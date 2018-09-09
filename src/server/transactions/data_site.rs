use bifrost::vector_clock::{StandardVectorClock};
use bifrost::utils::time::get_time;
use std::collections::{BTreeSet, BTreeMap, HashMap};
use utils::chashmap::{CHashMap, WriteGuard};
use ram::types::{Id, Value};
use ram::cell::{Cell, CellHeader, ReadError, WriteError};
use server::NebServer;
use linked_hash_map::LinkedHashMap;
use parking_lot::{Mutex, MutexGuard};
use super::*;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TXN_DATA_MANAGER_RPC_SERVICE) as u64;

type CommitHistory = BTreeMap<Id, CellHistory>;
type CellMetaMutex = Arc<Mutex<CellMeta>>;
type TxnMutex = Arc<Mutex<Transaction>>;

#[derive(Debug)]
pub struct CellMeta {
    read: TxnId,
    write: TxnId,
    owner: Option<TxnId>, // transaction that owns the cell in Committing state
    waiting: BTreeSet<(TxnId, u64)>, // transactions that waiting for owner to finish
}

struct Transaction {
    state: TxnState,
    affected_cells: Vec<Id>,
    last_activity: i64,
    history: CommitHistory
}

#[derive(Debug)]
struct CellHistory {
    cell: Option<Cell>,
    current_version: u64
}

impl CellHistory {
    pub fn new (cell: Option<Cell>, current_ver: u64) -> CellHistory {
        CellHistory {
            cell,
            current_version: current_ver,
        }
    }
}

pub struct DataManager {
    inner: Arc<DataManagerInner>
}

struct DataManagerInner {
    cells: Mutex<HashMap<Id, Arc<Mutex<CellMeta>>>>,
    cell_lru: Mutex<LinkedHashMap<Id, i64>>,
    txns: Mutex<HashMap<TxnId, Arc<Mutex<Transaction>>>>,
    tnxs_sorted: Mutex<BTreeSet<TxnId>>,
    managers: CHashMap<u64, Arc<manager::AsyncServiceClient>>,
    server: Arc<NebServer>,
    cleanup_sender: Mutex<Sender<()>>,
}

service! {
    rpc read(server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id) -> DataSiteResponse<TxnExecResult<Cell, ReadError>>;
    rpc read_selected(server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id, fields: Vec<u64>) -> DataSiteResponse<TxnExecResult<Vec<Value>, ReadError>>;
    rpc read_partial_raw(server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id, offset: usize, len: usize) -> DataSiteResponse<TxnExecResult<Vec<u8>, ReadError>>;
    rpc head(server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id) -> DataSiteResponse<TxnExecResult<CellHeader, ReadError>>;
    // two phase commit
    rpc prepare(server_id: u64, clock :StandardVectorClock, tid: TxnId, cell_ids: Vec<Id>) -> DataSiteResponse<DMPrepareResult>;
    rpc commit(clock :StandardVectorClock, tid: TxnId, cells: Vec<CommitOp>) -> DataSiteResponse<DMCommitResult>;

    // because there may be some exception on commit, abort have to handle 'committed' and 'committing' transactions
    // for committed transaction, abort need to recover the data according to it's cells history
    rpc abort(clock :StandardVectorClock, tid: TxnId) -> DataSiteResponse<AbortResult>;

    // there also should be a 'end' from transaction manager to inform data manager to clean up and release cell locks
    rpc end(clock :StandardVectorClock, tid: TxnId) -> DataSiteResponse<EndResult>;
}

dispatch_rpc_service_functions!(DataManager);

impl DataManager {
    pub fn new(server: &Arc<NebServer>) -> Arc<DataManager> {
        Arc::new(DataManager {
            inner: DataManagerInner::new(server)
        })
    }
}

impl Service for DataManager {
    fn read(&self, server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id)
        -> Box<Future<Item = DataSiteResponse<TxnExecResult<Cell, ReadError>>, Error = ()>>
    {
        let this = self.inner.clone();
        DataManagerInner::read(this, server_id, clock, tid, id)
    }
    fn read_selected(&self, server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id, fields: Vec<u64>)
        -> Box<Future<Item = DataSiteResponse<TxnExecResult<Vec<Value>, ReadError>>, Error = ()>>
    {
        let this = self.inner.clone();
        DataManagerInner::read_selected(this, server_id, clock, tid, id, fields)
    }
    fn read_partial_raw(&self, server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id, offset: usize, len: usize)
        -> Box<Future<Item = DataSiteResponse<TxnExecResult<Vec<u8>, ReadError>>, Error = ()>>
    {
        let this = self.inner.clone();
        DataManagerInner::read_partial_raw(this, server_id, clock, tid, id, offset, len)
    }
    fn head(&self, server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id)
        -> Box<Future<Item = DataSiteResponse<TxnExecResult<CellHeader, ReadError>>, Error = ()>>
    {
        let this = self.inner.clone();
        DataManagerInner::head(this, server_id, clock, tid, id)
    }
    fn prepare(&self, server_id: u64, clock :StandardVectorClock, tid: TxnId, cell_ids: Vec<Id>)
        -> Box<Future<Item = DataSiteResponse<DMPrepareResult>, Error = ()>>
    {
        let this = self.inner.clone();
        DataManagerInner::prepare(this, server_id, clock, tid, cell_ids)
    }
    fn commit(&self, clock :StandardVectorClock, tid: TxnId, cells: Vec<CommitOp>)
        -> Box<Future<Item = DataSiteResponse<DMCommitResult>, Error = ()>>
    {
        let this = self.inner.clone();
        DataManagerInner::commit(this, clock, tid, cells)
    }
    fn abort(&self, clock :StandardVectorClock, tid: TxnId)
        -> Box<Future<Item = DataSiteResponse<AbortResult>, Error = ()>>
    {
        let this = self.inner.clone();
        DataManagerInner::abort(this, clock, tid)
    }
    fn end(&self, clock :StandardVectorClock, tid: TxnId)
        -> Box<Future<Item = DataSiteResponse<EndResult>, Error = ()>>
    {
        let this = self.inner.clone();
        DataManagerInner::end(this, clock, tid)
    }
}

impl DataManagerInner {
    pub fn new(server: &Arc<NebServer>) -> Arc<DataManagerInner> {
        let (cleaup_sender, cleaup_recv) = channel();
        let manager = Arc::new(DataManagerInner {
            cells: Mutex::new(HashMap::new()),
            cell_lru: Mutex::new(LinkedHashMap::new()),
            txns: Mutex::new(HashMap::new()),
            tnxs_sorted: Mutex::new(BTreeSet::new()),
            managers: CHashMap::new(),
            server: server.clone(),
            cleanup_sender: Mutex::new(cleaup_sender),
        });
        let manager_clone = manager.clone();
        thread::spawn(move || { //TODO: give way to shutdown
            loop {
                cleaup_recv.recv();
                manager_clone.cell_meta_cleanup();
            }
        });
        return manager;
    }
    fn update_clock(&self, clock: &StandardVectorClock) {
        self.server.txn_peer.clock.merge_with(clock);
    }
    fn get_transaction(&self, tid: &TxnId) -> TxnMutex {
        let mut txns = self.txns.lock();
        txns.entry(tid.clone()).or_insert_with(|| {
            Arc::new(Mutex::new(Transaction {
                state: TxnState::Started,
                affected_cells: Vec::new(),
                last_activity: get_time(),
                history: BTreeMap::new(),
            }))
        }).clone()
    }
    fn cell_meta_mutex(&self, id: &Id) -> CellMetaMutex {
        {
            let mut lru = self.cell_lru.lock();
            *lru.entry(id.clone()).or_insert(0) = get_time();
            lru.get_refresh(id);
        }
        let mut cells = self.cells.lock();
        cells.entry(*id).or_insert_with(|| {
            Arc::new(Mutex::new(CellMeta {
                read: TxnId::new(),
                write: TxnId::new(),
                owner: None,
                waiting: BTreeSet::new(),
            }))
        }).clone()
    }
    fn response_with<T>(&self, data: T)
        -> Box<Future<Item = DataSiteResponse<T>, Error = ()>>
        where T: 'static
    {
        let res = DataSiteResponse::new(&self.server.txn_peer, data);
        box future::ok(res)
    }
    fn rollback(&self, history: &CommitHistory) -> Vec<RollbackFailure> {
        let mut failures: Vec<RollbackFailure> = Vec::new();
        for (id, history) in history.iter() {
            debug!("ROLLING BACK {:?} - {:?}", id, history);
            let cell = &history.cell;
            let current_ver = history.current_version;
            let error = if cell.is_none() { // the cell was created, need to remove
                self.server.chunks.remove_cell_by(&id, |cell| {
                    cell.header.version == current_ver
                }).err()
            } else if current_ver > 0 { // the cell was updated, need to update back
                self.server.chunks.update_cell_by(id, |cell_to_update|{
                    if cell_to_update.header.version == current_ver {
                        cell.clone()
                    } else { None }
                }).err()
            } else { // the cell was removed, need to put back
                let mut cell = cell.clone().unwrap();
                self.server.chunks.write_cell(&mut cell).err()
            };
            if let Some(error) = error {
                failures.push(RollbackFailure{id: *id, error: error});
            }
        }
        failures
    }
    fn update_cell_write(&self, cell_id: &Id, tid: &TxnId) {
        let meta_ref = self.cell_meta_mutex(cell_id);
        let mut meta = meta_ref.lock();
        meta.write = tid.clone();
    }
    fn get_tnx_manager(&self, server_id: u64) -> io::Result<Arc<manager::AsyncServiceClient>> {
        if !self.managers.contains_key(&server_id) {
            let client = self.server.get_member_by_server_id(server_id)?;
            self.managers.upsert(server_id, || {
                manager::AsyncServiceClient::new(manager::DEFAULT_SERVICE_ID, &client)
            }, |_| {});
        }
        Ok(self.managers.get(&server_id).unwrap().clone())
    }
    fn wipe_out_transaction(&self, tid: &TxnId) {
        self.txns.lock().remove(tid);
        self.tnxs_sorted.lock().remove(tid);
    }
    fn cell_meta_cleanup(&self) {
        let mut cell_lru = self.cell_lru.lock();
        let mut cells = self.cells.lock();
        let oldest_transaction = {
            let tnx_sorted = self.tnxs_sorted.lock();
            tnx_sorted.iter()
                .next()
                .cloned()
                .unwrap_or(
                    self.server.txn_peer.clock.to_clock()
                )
        };
        let now = get_time();
        let mut cell_to_evict = Vec::new();
        let mut need_break = false;
        for (cell_id, timestamp) in cell_lru.iter() {
            if let Some(cell_meta) = cells.get(cell_id) {
                let meta = cell_meta.lock();
                if meta.write < oldest_transaction &&
                    meta.read < oldest_transaction &&
                    now - timestamp > 5 * 60 * 1000 {
                    cell_to_evict.push(*cell_id);
                } else {
                    need_break = true;
                }
            } else {
                cell_to_evict.push(*cell_id);
            }
            if need_break {
                break;
            }
        }
        for evicted_cell in &cell_to_evict {
            cells.remove(evicted_cell);
            cell_lru.remove(evicted_cell);
        }
    }
    fn prepare_read<T>(&self, server_id: &u64, clock: &StandardVectorClock, tid: &TxnId, id: &Id)
        -> Result<(), Box<Future<Item = DataSiteResponse<TxnExecResult<T, ReadError>>, Error = ()>>>
        where T: 'static + Clone
    {
        self.update_clock(clock);
        let txn_lock = self.get_transaction(tid);
        let mut txn = txn_lock.lock();
        let meta_ref = self.cell_meta_mutex(id);
        let mut meta = meta_ref.lock();
        let committing = meta.owner.is_some();
        let read_too_late = &meta.write > tid;
        txn.last_activity = get_time();
        if txn.state != TxnState::Started {
            return Err(self.response_with(TxnExecResult::StateError(txn.state)))
        }
        if read_too_late { // not realizable
            return Err(self.response_with(TxnExecResult::Rejected));
        }
        if committing { // ts >= wt but committing, need to wait until it committed
            meta.waiting.insert((tid.clone(), *server_id));
            debug!("-> READ {:?} WAITING {:?}", tid, &meta.owner.clone());
            return Err(self.response_with(TxnExecResult::Wait));
        }
        if &meta.read < tid {
            meta.read = tid.clone()
        }
        return Ok(())
    }

    /////////////////////////////////////
    ///        Implement Services    ///
    ///////////////////////////////////

    fn read(this: Arc<Self>, server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id)
        -> Box<Future<Item = DataSiteResponse<TxnExecResult<Cell, ReadError>>, Error = ()>>
    {
        if let Err(r) = this.prepare_read(&server_id, &clock, &tid, &id) { return r; }
        match this.server.chunks.read_cell(&id) {
            Ok(cell) => this.response_with(TxnExecResult::Accepted(cell)),
            Err(read_error) => this.response_with(TxnExecResult::Error(read_error))
        }
    }
    fn read_selected(this: Arc<Self>, server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id, fields: Vec<u64>)
        -> Box<Future<Item = DataSiteResponse<TxnExecResult<Vec<Value>, ReadError>>, Error = ()>>
    {
        if let Err(r) = this.prepare_read(&server_id, &clock, &tid, &id) { return r; }
        match this.server.chunks.read_selected(&id, &fields[..]) {
            Ok(values) => this.response_with(TxnExecResult::Accepted(values)),
            Err(read_error) => this.response_with(TxnExecResult::Error(read_error))
        }
    }
    fn head(this: Arc<Self>, server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id)
        -> Box<Future<Item = DataSiteResponse<TxnExecResult<CellHeader, ReadError>>, Error = ()>>
    {
        if let Err(r) = this.prepare_read(&server_id, &clock, &tid, &id) { return r; }
        match this.server.chunks.head_cell(&id) {
            Ok(head) => this.response_with(TxnExecResult::Accepted(head)),
            Err(read_error) => this.response_with(TxnExecResult::Error(read_error))
        }
    }
    // TODO: Link this function in transaction manager
    fn read_partial_raw(this: Arc<Self>, server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id, offset: usize, len: usize)
        -> Box<Future<Item = DataSiteResponse<TxnExecResult<Vec<u8>, ReadError>>, Error = ()>>
    {
        if let Err(r) = this.prepare_read(&server_id, &clock, &tid, &id) { return r; }
        match this.server.chunks.read_partial_raw(&id, offset, len) {
            Ok(values) => this.response_with(TxnExecResult::Accepted(values)),
            Err(read_error) => this.response_with(TxnExecResult::Error(read_error))
        }
    }
    fn prepare(this: Arc<Self>, server_id: u64, clock :StandardVectorClock, tid: TxnId, cell_ids: Vec<Id>)
        -> Box<Future<Item = DataSiteResponse<DMPrepareResult>, Error = ()>>
    {
        // In this stage, data manager will not do any write operation but mark cell owner in their meta as a lock
        // It will also check if write are realizable. If not, transaction manager should retry with new id
        // cell_ids must be sorted to avoid deadlock. It can be done from data manager by using BTreeMap keys
        debug!("PREPARE FOR {:?}, {} cells", &tid, cell_ids.len());
        this.update_clock(&clock);
        let txn_lock = this.get_transaction(&tid);
        let mut txn = txn_lock.lock();
        if txn.state != TxnState::Started && txn.state != TxnState::Prepared {
            return this.response_with(DMPrepareResult::StateError(txn.state));
        }

        let mut cell_mutices = Vec::new();
        let mut cell_guards = Vec::new();

        for cell_id in &cell_ids {
            cell_mutices.push(this.cell_meta_mutex(cell_id));
        }
        for cell_mutex in &cell_mutices {
            let mut meta = cell_mutex.lock();
            if tid < meta.read || tid < meta.write { // write too late
                break;
            }
            cell_guards.push(meta);
        }
        if cell_guards.len() != cell_ids.len() {
            return this.response_with(DMPrepareResult::NotRealizable); // need retry
        } else {
            for mut meta in cell_guards {
                meta.owner = Some(tid.clone()) // set owner to lock this cell
            }
            txn.state = TxnState::Prepared;
            txn.affected_cells = cell_ids.clone(); // for cell number check
            txn.last_activity = get_time();    // check if transaction timeout
            return this.response_with(DMPrepareResult::Success);
        }
    }
    fn commit(this: Arc<Self>, clock :StandardVectorClock, tid: TxnId, cells: Vec<CommitOp>)
              -> Box<Future<Item = DataSiteResponse<DMCommitResult>, Error = ()>>
    {
        this.update_clock(&clock);
        let txn_lock = this.get_transaction(&tid);
        let mut txn = txn_lock.lock();
        txn.last_activity = get_time();
        // check state
        match txn.state {
            TxnState::Started => {return this.response_with(DMCommitResult::CheckFailed(CheckError::NotCommitted))},
            TxnState::Aborted => {return this.response_with(DMCommitResult::CheckFailed(CheckError::AlreadyAborted))},
            TxnState::Committed => {return this.response_with(DMCommitResult::CheckFailed(CheckError::AlreadyCommitted))},
            TxnState::Prepared => {}
        }
        // check cell list integrity
        let prepared_cells_num = txn.affected_cells.len();
        let arrived_cells_num = cells.len();
        if prepared_cells_num != arrived_cells_num {
            return this.response_with(
                DMCommitResult::CheckFailed(
                    CheckError::CellNumberDoesNotMatch(prepared_cells_num, arrived_cells_num)))
        }
        let mut write_error: Option<(Id, WriteError)> = None;
        {
            let mut commit_history = &mut txn.history; // for rollback in case of write error
            for cell_op in cells {
                match cell_op {
                    CommitOp::Read(id, version) => {}
                    CommitOp::Write(ref cell) => {
                        let mut cell = cell.clone();
                        let write_result = this.server.chunks.write_cell(&mut cell);
                        match write_result {
                            Ok(header) => {
                                commit_history.insert(cell.id(), CellHistory::new(None, header.version));
                                this.update_cell_write(&cell.id(), &tid);
                            },
                            Err(error) => {
                                write_error = Some((cell.id(), error));
                                break;
                            }
                        };
                    },
                    CommitOp::Remove(ref cell_id) => {
                        let original_cell  = {
                            match this.server.chunks.read_cell(cell_id) {
                                Ok(cell) => cell,
                                Err(re) => {
                                    write_error = Some((*cell_id, WriteError::ReadError(re)));
                                    break;
                                }
                            }
                        };
                        let write_result = this.server.chunks.remove_cell_by(cell_id, |cell|{
                            let version = cell.header.version;
                            version == original_cell.header.version
                        });
                        match write_result {
                            Ok(()) => {
                                commit_history.insert(*cell_id, CellHistory::new(Some(original_cell), 0));
                                this.update_cell_write(cell_id, &tid);
                            },
                            Err(error) => {
                                write_error = Some((*cell_id, error));
                                break;
                            }
                        }
                    },
                    CommitOp::Update(ref cell) => {
                        let cell_id = cell.id();
                        let original_cell = {
                            match this.server.chunks.read_cell(&cell_id) {
                                Ok(cell) => cell,
                                Err(re) => {
                                    write_error = Some((cell_id, WriteError::ReadError(re)));
                                    break;
                                }
                            }
                        };
                        let write_result = this.server.chunks.update_cell_by(&cell_id, |cell_to_update| {
                            if cell_to_update.header.version == original_cell.header.version {
                                Some(cell.clone())
                            } else {
                                None
                            }
                        });
                        match write_result {
                            Ok(cell) => {
                                commit_history.insert(cell_id, CellHistory::new(Some(original_cell), cell.header.version));
                                this.update_cell_write(&cell_id, &tid);
                            },
                            Err(error) => {
                                write_error = Some((cell_id, error));
                                break;
                            }
                        }
                    },
                    CommitOp::None => {
                        panic!("None CommitOp should not appear in data site");
                    }
                }
            }
        }
        txn.last_activity = get_time();
        // check if any of those operations failed, if yes, rollback and fail this commit
        if let Some((id, error)) = write_error {
            match error {
                WriteError::DeletionPredictionFailed | WriteError::UserCanceledUpdate => {
                    // in this case, we can inform transaction manager to try again
                    return this.response_with(DMCommitResult::CellChanged(id));
                }
                _ => {
                    // other failure due to unfixable error should abort without retry
                    return this.response_with(DMCommitResult::WriteError(id, error));
                }
            }
        } else {
            // all set, able to commit
            txn.state = TxnState::Committed;
            return this.response_with(DMCommitResult::Success)
        }
    }
    fn abort(this: Arc<Self>, clock :StandardVectorClock, tid: TxnId)
             -> Box<Future<Item = DataSiteResponse<AbortResult>, Error = ()>>
    {
        debug!(">> ABORT {:?}", tid);
        this.update_clock(&clock);
        let txn_lock = this.get_transaction(&tid);
        let mut txn = txn_lock.lock();
        if txn.state == TxnState::Aborted {
            return this.response_with(AbortResult::CheckFailed(CheckError::AlreadyAborted));
        }
        let rollback_failures = {
            debug!(">>>>>>>>>> ROLLING BACK FOR {:?} CELLS {:?}", txn.history.len(), tid);
            let failures = this.rollback(&txn.history);
            if failures.len() == 0 { None } else { Some(failures) }
        };
        txn.last_activity = get_time();
        txn.state = TxnState::Aborted;
        this.response_with(AbortResult::Success(rollback_failures))
    }
    fn end(this: Arc<Self>, clock :StandardVectorClock, tid: TxnId)
           -> Box<Future<Item = DataSiteResponse<EndResult>, Error = ()>>
    {
        debug!(">> END {:?}", tid);
        let this_clone = this.clone();
        let result = {
            this.update_clock(&clock);
            let txn_lock = this.get_transaction(&tid);
            let txn = txn_lock.lock();
            if !(txn.state == TxnState::Aborted || txn.state == TxnState::Committed) {
                return this.response_with(EndResult::CheckFailed(CheckError::CannotEnd))
            }
            debug!("AFFECTED: {}, {:?}, {:?}", txn.affected_cells.len(), txn.state, tid);
            let affected_cells = txn.affected_cells.len();
            let mut released_locks = 0;
            let mut waiting_list: BTreeMap<u64, BTreeSet<TxnId>> = BTreeMap::new();
            let mut cell_mutices = Vec::new();
            let mut cell_guards = Vec::new();
            {
                let cells = this.cells.lock();
                for cell_id in &txn.affected_cells {
                    if let Some(meta) = cells.get(cell_id){
                        cell_mutices.push(meta.clone());
                    }
                }
            }
            for cell_mutex in &cell_mutices {
                cell_guards.push(cell_mutex.lock()); // lock all affected cells on by on
            }
            for mut meta in cell_guards {
                if meta.owner == Some(tid.clone()) {
                    // collect waiting transactions
                    for &(ref waiting_tid, ref waiting_server_id) in &meta.waiting {
                        waiting_list
                            .entry(*waiting_server_id)
                            .or_insert_with(|| BTreeSet::new())
                            .insert(waiting_tid.clone());
                    }
                    meta.waiting.clear();
                    meta.owner = None;
                    released_locks += 1;
                } else {
                    warn!("affected txn does not own the cell");
                }
            }
            let mut wake_up_futures = Vec::with_capacity(waiting_list.len());
            debug!("RELEASE: {:?} for {:?}", tid, waiting_list);
            for (server_id, transactions) in waiting_list { // inform waiting servers to go on
                if let Ok(client) = this.get_tnx_manager(server_id) {
                    wake_up_futures.push(client.go_ahead(transactions, this.server.server_id));
                } else {
                    debug!("cannot inform server {} to continue its transactions", server_id);
                }
            }
            future::join_all(wake_up_futures)
                .then(move |_| {
                    if released_locks == affected_cells {
                        this.response_with(EndResult::Success)
                    } else {
                        this.response_with(EndResult::SomeLocksNotReleased)
                    }
                })
        };
        box result.then(move |r| {
            this_clone.wipe_out_transaction(&tid);
            this_clone.cleanup_sender.lock().send(());
            return r;
        })
    }
}
