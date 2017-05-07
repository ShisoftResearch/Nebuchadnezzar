use bifrost::vector_clock::{StandardVectorClock, Relation};
use bifrost::utils::time::get_time;
use std::collections::{BTreeSet, BTreeMap};
use chashmap::{CHashMap, WriteGuard};
use ram::types::{Id};
use ram::cell::{Cell, ReadError, WriteError};
use server::NebServer;
use linked_hash_map::LinkedHashMap;
use parking_lot::Mutex;
use super::*;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TNX_DATA_MANAGER_RPC_SERVICE) as u64;

pub type CellMetaGuard <'a> = WriteGuard<'a, Id, CellMeta>;
pub type CommitHistory = BTreeMap<Id, CellHistory>;

pub struct CellMeta {
    read: TransactionId,
    write: TransactionId,
    owner: Option<TransactionId>, // transaction that owns the cell in Committing state
    waiting: BTreeSet<TransactionId>, // transactions that waiting for owner to finish
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CommitOp {
    Write(Cell),
    Update(Cell),
    Remove(Id)
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EndResult {
    CheckFailed(CheckError),
    SomeLocksNotReleased,
    Success,
}

struct Transaction {
    server: u64,
    id: TransactionId,
    state: TransactionState,
    affected_cells: Vec<Id>,
    last_activity: i64,
    history: CommitHistory
}

struct CellHistory {
    cell: Option<Cell>,
    current_version: u64
}

impl CellHistory {
    pub fn new (cell: Option<Cell>, current_ver: u64) -> CellHistory {
        CellHistory {
            cell: cell,
            current_version: current_ver,
        }
    }
}

pub struct DataManager {
    cells: CHashMap<Id, CellMeta>,
    cell_lru: Mutex<LinkedHashMap<Id, i64>>,
    tnxs: CHashMap<TransactionId, Transaction>,
    tnxs_sorted: Mutex<BTreeSet<TransactionId>>,
    managers: CHashMap<u64, Arc<manager::AsyncServiceClient>>,
    server: Arc<NebServer>
}

service! {
    rpc read(server_id: u64, clock: StandardVectorClock, tid: TransactionId, id: Id) -> DataSiteResponse<TransactionExecResult<Cell, ReadError>>;

    // two phase commit
    rpc prepare(server_id: u64, clock :StandardVectorClock, tid: TransactionId, cell_ids: Vec<Id>) -> DataSiteResponse<PrepareResult>;
    rpc commit(clock :StandardVectorClock, tid: TransactionId, cells: Vec<CommitOp>) -> DataSiteResponse<CommitResult>;

    // because there may be some exception on commit, abort have to handle 'committed' and 'committing' transactions
    // for committed transaction, abort need to recover the data according to it's cells history
    rpc abort(clock :StandardVectorClock, tid: TransactionId) -> DataSiteResponse<AbortResult>;

    // there also should be a 'end' from transaction manager to inform data manager to clean up and release cell locks
    rpc end(clock :StandardVectorClock, tid: TransactionId) -> DataSiteResponse<EndResult>;
}

dispatch_rpc_service_functions!(DataManager);

impl DataManager {
    pub fn new(server: &Arc<NebServer>) -> Arc<DataManager> {
        Arc::new(DataManager {
            cells: CHashMap::new(),
            cell_lru: Mutex::new(LinkedHashMap::new()),
            tnxs: CHashMap::new(),
            tnxs_sorted: Mutex::new(BTreeSet::new()),
            managers: CHashMap::new(),
            server: server.clone(),
        })
    }
    fn local_clock(&self) -> StandardVectorClock {
        self.server.txn_peer.clock.to_clock()
    }
    fn update_clock(&self, clock: &StandardVectorClock) {
        self.server.txn_peer.clock.merge_with(clock);
    }
    fn get_transaction(&self, tid: &TransactionId, server: &u64) -> WriteGuard<TransactionId, Transaction> {
        if !self.tnxs.contains_key(tid) {
            self.tnxs.upsert(tid.clone(), ||{
                self.tnxs_sorted.lock().insert(tid.clone());
                Transaction {
                    id: tid.clone(),
                    server: *server,
                    state: TransactionState::Started,
                    affected_cells: Vec::new(),
                    last_activity: get_time(),
                    history: BTreeMap::new(),
                }
            }, |_|{});
        }
        match self.tnxs.get_mut(tid) {
            Some(tnx) => tnx,
            _ => self.get_transaction(tid, server) // it should be rare
        }
    }
    fn get_cell_meta(&self, id: &Id) -> CellMetaGuard {
        if !self.cells.contains_key(id) {
            self.cells.upsert(*id, || {
                CellMeta {
                    read: TransactionId::new(),
                    write: TransactionId::new(),
                    owner: None,
                    waiting: BTreeSet::new(),
                }
            }, |c|{});
        }
        {
            let mut lru = self.cell_lru.lock();
            *lru.entry(id.clone()).or_insert(0) = get_time();
            lru.get_refresh(id);
        }
        match self.cells.get_mut(id) {
            Some(meta) => meta,
            _ => self.get_cell_meta(id)
        }
    }
    fn response_with<T>(&self, data: T)
        -> Result<DataSiteResponse<T>, ()>{
        Ok(DataSiteResponse::new(&self.server.txn_peer, data))
    }
    fn rollback(&self, history: &CommitHistory) -> Vec<RollbackFailure> {
        let mut failures: Vec<RollbackFailure> = Vec::new();
        for (id, history) in history.iter() {
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
    fn update_cell_write(&self, cell_id: &Id, tid: &TransactionId) {
        let mut meta = self.get_cell_meta(cell_id);
        meta.write = tid.clone();
    }
    fn get_tnx_manager(&self, server_id: u64) -> io::Result<Arc<manager::AsyncServiceClient>> {
        if !self.managers.contains_key(&server_id) {
            let client = self.server.get_member_by_server_id(server_id)?;
            self.managers.upsert(server_id, || {
                manager::AsyncServiceClient::new(DEFAULT_SERVICE_ID, &client)
            }, |_| {});
        }
        Ok(self.managers.get(&server_id).unwrap().clone())
    }
    fn wipe_out_transaction(&self, tid: &TransactionId) {
        self.tnxs.remove(tid);
        self.tnxs_sorted.lock().remove(tid);
    }
    fn cell_meta_cleanup(&self) {
        let mut cell_lru = self.cell_lru.lock();
        let mut evicted_cells = Vec::new();
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
        for (cell_id, timestamp) in cell_lru.iter() {
            if let Some(meta) = self.cells.get(cell_id) {
                if meta.write < oldest_transaction &&
                   meta.read < oldest_transaction &&
                   now - timestamp > 5 * 60 * 1000 {
                    self.cells.remove(cell_id);
                    evicted_cells.push(*cell_id);
                } else {
                    break;
                }
            } else {
                evicted_cells.push(*cell_id);
            }
        }
        for evicted_cell in &evicted_cells {
            cell_lru.remove(evicted_cell);
        }
    }
}

impl Service for DataManager {
    fn read(&self, server_id: &u64, clock: &StandardVectorClock, tid: &TransactionId, id: &Id)
        -> Result<DataSiteResponse<TransactionExecResult<Cell, ReadError>>, ()> {
        self.update_clock(clock);
        let mut meta = self.get_cell_meta(id);
        let committing = meta.owner.is_some();
        let read_too_late = &meta.write > tid;
        let mut txn = self.get_transaction(tid, server_id);
        if txn.state != TransactionState::Started {
            return self.response_with(TransactionExecResult::Rejected)
        }
        if read_too_late { // not realizable
            return self.response_with(TransactionExecResult::Rejected);
        }
        if committing { // ts >= wt but committing, need to wait until it committed
            meta.waiting.insert(tid.clone());
            return self.response_with(TransactionExecResult::Wait);
        }
        let cell = match self.server.chunks.read_cell(id) {
            Ok(cell) => {cell}
            Err(read_error) => {
                // cannot read
                return self.response_with(TransactionExecResult::Error(read_error));
            }
        };
        let update_read = &meta.read < tid;
        if update_read {
            meta.read = tid.clone()
        }
        txn.last_activity = get_time();
        self.response_with(TransactionExecResult::Accepted(cell))
    }
    fn prepare(&self, server_id: &u64, clock :&StandardVectorClock, tid: &TransactionId, cell_ids: &Vec<Id>)
        -> Result<DataSiteResponse<PrepareResult>, ()> {
        // In this stage, data manager will not do any write operation but mark cell owner in their meta as a lock
        // It will also check if write are realizable. If not, transaction manager should retry with new id
        // cell_ids must be sorted to avoid deadlock. It can be done from data manager by using BTreeMap keys
        self.update_clock(clock);
        let mut txn = self.get_transaction(tid, server_id);
        if txn.state != TransactionState::Started && txn.state != TransactionState::Committing {
            return self.response_with(PrepareResult::TransactionStateError(txn.state))
        }
        let mut cell_metas: Vec<CellMetaGuard> = Vec::new();
        for cell_id in cell_ids {
            let mut meta = self.get_cell_meta(cell_id);
            if tid < &meta.read { // write too late
                break;
            }
            if tid < &meta.write {
                if meta.owner.is_some() { // not committed, should wait and try prepare again
                    meta.waiting.insert(tid.clone());
                    return self.response_with(PrepareResult::Wait)
                }
            }
            cell_metas.push(meta);
        }
        if cell_metas.len() != cell_ids.len() {
            return self.response_with(PrepareResult::NotRealizable); // need retry
        } else {
            for mut meta in cell_metas {
                meta.owner = Some(tid.clone()) // set owner to lock this cell
            }
            txn.state = TransactionState::Committing;
            txn.affected_cells = cell_ids.clone(); // for cell number check
            txn.last_activity = get_time();    // check if transaction timeout
            return self.response_with(PrepareResult::Success);
        }
    }
    fn commit(&self, clock :&StandardVectorClock, tid: &TransactionId, cells: &Vec<CommitOp>)
        -> Result<DataSiteResponse<CommitResult>, ()>  {
        self.update_clock(clock);
        let mut txn = match self.tnxs.get_mut(tid) {
            Some(tnx) => tnx,
            _ => { return self.response_with(CommitResult::CheckFailed(CheckError::TransactionNotExisted)); }
        };
        // check state
        match txn.state {
            TransactionState::Started => {return self.response_with(CommitResult::CheckFailed(CheckError::TransactionNotCommitted))},
            TransactionState::Aborted => {return self.response_with(CommitResult::CheckFailed(CheckError::TransactionAlreadyAborted))},
            TransactionState::Committed => {return self.response_with(CommitResult::CheckFailed(CheckError::TransactionAlreadyCommitted))},
            TransactionState::Committing => {}
        }
        // check cell list integrity
        let prepared_cells_num = txn.affected_cells.len();
        let arrived_cells_num = self.cells.len();
        if prepared_cells_num != arrived_cells_num {return self.response_with(CommitResult::CheckFailed(CheckError::CellNumberDoesNotMatch(prepared_cells_num, arrived_cells_num)))}
        let mut commit_history: CommitHistory = BTreeMap::new(); // for rollback in case of write error
        let mut write_error: Option<(Id, WriteError)> = None;
        for cell_op in cells {
            match cell_op {
                &CommitOp::Write(ref cell) => {
                    let mut cell = cell.clone();
                    let write_result = self.server.chunks.write_cell(&mut cell);
                    match write_result {
                        Ok(header) => {
                            commit_history.insert(cell.id(), CellHistory::new(None, header.version));
                            self.update_cell_write(&cell.id(), tid);
                        },
                        Err(error) => {
                            write_error = Some((cell.id(), error));
                            break;
                        }
                    };
                },
                &CommitOp::Remove(ref cell_id) => {
                    let original_cell = {
                        match self.server.chunks.read_cell(cell_id) {
                            Ok(cell) => cell,
                            Err(re) => {
                                write_error = Some((*cell_id, WriteError::ReadError(re)));
                                break;
                            }
                        }
                    };
                    let write_result = self.server.chunks.remove_cell_by(cell_id, |cell|{
                        let version = cell.header.version;
                        version == original_cell.header.version
                    });
                    match write_result {
                        Ok(()) => {
                            commit_history.insert(*cell_id, CellHistory::new(Some(original_cell), 0));
                            self.update_cell_write(cell_id, tid);
                        },
                        Err(error) => {
                            write_error = Some((*cell_id, error));
                            break;
                        }
                    }
                },
                &CommitOp::Update(ref cell) => {
                    let cell_id = cell.id();
                    let original_cell = {
                        match self.server.chunks.read_cell(&cell_id) {
                            Ok(cell) => cell,
                            Err(re) => {
                                write_error = Some((cell_id, WriteError::ReadError(re)));
                                break;
                            }
                        }
                    };
                    let write_result = self.server.chunks.update_cell_by(&cell_id, |cell_to_update| {
                        if cell_to_update.header.version == original_cell.header.version {
                            Some(cell.clone())
                        } else {
                            None
                        }
                    });
                    match write_result {
                        Ok(cell) => {
                            commit_history.insert(cell_id, CellHistory::new(Some(original_cell), cell.header.version));
                            self.update_cell_write(&cell_id, tid);
                        },
                        Err(error) => {
                            write_error = Some((cell_id, error));
                            break;
                        }
                    }
                }
            }
        }
        // check if any of those operations failed, if yes, rollback and fail this commit
        if let Some((id, error)) = write_error {
            let rollback_result = self.rollback(&commit_history);
            txn.state = TransactionState::Aborted; // set to abort so it can't be abort again
            txn.last_activity = get_time();
            match error {
                WriteError::DeletionPredictionFailed | WriteError::UserCanceledUpdate => {
                    // in this case, we can inform transaction manager to try again
                    return self.response_with(CommitResult::CellChanged(
                        id, rollback_result
                    ));
                }
                _ => {
                    // other failure due to unfixable error should abort without retry
                    return self.response_with(CommitResult::WriteError(
                        id, error, rollback_result
                    ));
                }
            }
        } else {
            // all set, able to commit
            txn.state = TransactionState::Committed;
            txn.history = commit_history;
            txn.last_activity = get_time();
            return self.response_with(CommitResult::Success)
        }
    }
    fn abort(&self, clock :&StandardVectorClock, tid: &TransactionId)
        -> Result<DataSiteResponse<AbortResult>, ()>  {
        self.update_clock(clock);
        let mut txn = match self.tnxs.get_mut(tid) {
            Some(tnx) => tnx,
            _ => { return self.response_with(AbortResult::CheckFailed(CheckError::TransactionNotExisted)); }
        };
        if txn.state == TransactionState::Aborted {
            return self.response_with(AbortResult::CheckFailed(CheckError::TransactionAlreadyAborted));
        }
        let rollback_failures = {
            let failures = self.rollback(&txn.history);
            if failures.len() == 0 { None } else { Some(failures) }
        };
        txn.last_activity = get_time();
        txn.state = TransactionState::Aborted;
        self.response_with(AbortResult::Success(rollback_failures))
    }
    fn end(&self, clock :&StandardVectorClock, tid: &TransactionId)
             -> Result<DataSiteResponse<EndResult>, ()>  {
        self.update_clock(clock);
        let mut txn = match self.tnxs.get_mut(tid) {
            Some(tnx) => tnx,
            _ => { return self.response_with(EndResult::CheckFailed(CheckError::TransactionNotExisted)); }
        };
        if !(txn.state == TransactionState::Aborted || txn.state == TransactionState::Committed) {
            return self.response_with(EndResult::CheckFailed(CheckError::TransactionCannotEnd))
        }
        let mut cell_metas: Vec<CellMetaGuard> = Vec::new();
        for cell_id in &txn.affected_cells {
            if let Some(meta) = self.cells.get_mut(cell_id){
                cell_metas.push(meta)
            }
        }
        let affected_cells = txn.affected_cells.len();
        let mut released_locks = 0;
        let mut waiting_list: BTreeMap<u64, BTreeSet<TransactionId>> = BTreeMap::new();
        for mut meta in cell_metas {
            if meta.owner == Some(tid.clone()) {
                for waiting_tid in &meta.waiting {
                    if let Some(t) = self.tnxs.get(waiting_tid) {
                        waiting_list
                            .entry(t.server)
                            .or_insert_with(|| BTreeSet::new())
                            .insert(waiting_tid.clone());
                    }
                }
                meta.waiting.clear();
                meta.owner = None;
                released_locks += 1;
            } else {
                warn!("affected txn does not own the cell");
            }
        }
        let mut wake_up_futures = Vec::with_capacity(waiting_list.len());
        for (server_id, transactions) in waiting_list { // inform waiting servers to go on
            if let Ok(client) = self.get_tnx_manager(server_id) {
                wake_up_futures.push(client.go_ahead(&transactions));
            } else {
                warn!("cannot inform server {} to continue it transactions", server_id);
            }
        }
        future::join_all(wake_up_futures).wait();
        self.wipe_out_transaction(tid);
        self.cell_meta_cleanup();
        if released_locks == affected_cells {
            self.response_with(EndResult::Success)
        } else {
            self.response_with(EndResult::SomeLocksNotReleased)
        }
    }
}