use bifrost::vector_clock::{StandardVectorClock, Relation};
use bifrost::utils::time::get_time;
use std::collections::{BTreeSet, BTreeMap};
use chashmap::{CHashMap, WriteGuard};
use ram::types::{Id};
use ram::cell::{Cell, ReadError, WriteError};
use server::NebServer;
use super::*;

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
pub enum PrepareResult {
    Wait,
    Success,
    TransactionNotExisted,
    NotRealizable,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CommitResult {
    Success,
    WriteError(Id, WriteError, Vec<RollbackFailure>),
    CellChanged(Id, Vec<RollbackFailure>),
    CheckFailed(CommitCheckError),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CommitCheckError {
    CellNumberDoesNotMatch(usize, usize),
    TransactionNotExisted,
    TransactionNotCommitted,
    TransactionAlreadyCommitted,
    TransactionAlreadyAborted,
}

#[derive(Debug, Eq, PartialEq)]
enum TransactionState {
    Started,
    Aborted,
    Committing,
    Committed,
}

struct Transaction {
    server: u64,
    id: TransactionId,
    state: TransactionState,
    affect_cells: usize,
    last_activity: i64,
    history: Option<CommitHistory>
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
#[derive(Debug, Serialize, Deserialize)]
pub struct RollbackFailure {
    id: Id,
    error: WriteError
}

pub struct DataManager {
    cells: CHashMap<Id, CellMeta>,
    tnxs: CHashMap<TransactionId, Transaction>,
    server: Arc<NebServer>
}

service! {
    rpc read(server_id: u64, clock: StandardVectorClock, tid: TransactionId, id: Id) -> DataSiteResponse<TransactionExecResult<Cell, ReadError>>;

    // two phase commit
    rpc prepare(clock :StandardVectorClock, tid: TransactionId, cell_ids: Vec<Id>) -> DataSiteResponse<PrepareResult>;
    rpc commit(clock :StandardVectorClock, tid: TransactionId, cells: Vec<CommitOp>) -> DataSiteResponse<CommitResult>;

    // because there may be some exception on commit, abort have to handle 'committed' and 'committing' transactions
    // for committed transaction, abort need to recover the data according to it's cells history
    rpc abort(clock :StandardVectorClock, tid: TransactionId) -> DataSiteResponse<()>;

    // there also should be a 'end' from transaction manager to inform data manager to clean up and release cell locks
    rpc end(clock :StandardVectorClock, tid: TransactionId) -> DataSiteResponse<()>;
}

dispatch_rpc_service_functions!(DataManager);

impl DataManager {
    fn local_clock(&self) -> StandardVectorClock {
        self.server.tnx_peer.clock.to_clock()
    }
    fn update_clock(&self, clock: &StandardVectorClock) {
        self.server.tnx_peer.clock.merge_with(clock);
    }
    fn get_transaction(&self, tid: &TransactionId, server: &u64) -> WriteGuard<TransactionId, Transaction> {
        if !self.tnxs.contains_key(tid) {
            self.tnxs.upsert(tid.clone(), ||{
                Transaction {
                    id: tid.clone(),
                    server: *server,
                    state: TransactionState::Started,
                    affect_cells: 0,
                    last_activity: get_time(),
                    history: None
                }
            }, |t|{});
        }
        match self.tnxs.get_mut(tid) {
            Some(tnx) => tnx,
            _ => self.get_transaction(tid, server) // it should be rare
        }
    }
    fn get_cell_meta(&self, id: &Id) -> CellMetaGuard {
        if !self.cells.contains_key(id) {
            self.cells.upsert(id.clone(), || {
                CellMeta {
                    read: TransactionId::new(),
                    write: TransactionId::new(),
                    owner: None,
                    waiting: BTreeSet::new()
                }
            }, |c|{});
        }
        match self.cells.get_mut(id) {
            Some(cell) => cell,
            _ => self.get_cell_meta(id)
        }
    }
    fn response_with<T>(&self, data: T)
        -> Result<DataSiteResponse<T>, ()>{
        Ok(DataSiteResponse::new(&self.server.tnx_peer, data))
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
}

impl Service for DataManager {
    fn read(&self, server_id: &u64, clock: &StandardVectorClock, tid: &TransactionId, id: &Id)
        -> Result<DataSiteResponse<TransactionExecResult<Cell, ReadError>>, ()> {
        self.update_clock(clock);
        let mut meta = self.get_cell_meta(id);
        let committing = meta.owner.is_some();
        let read_too_late = &meta.write > tid;
        let mut tnx = self.get_transaction(tid, server_id);
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
        tnx.last_activity = get_time();
        self.response_with(TransactionExecResult::Accepted(cell))
    }
    fn prepare(&self, clock :&StandardVectorClock, tid: &TransactionId, cell_ids: &Vec<Id>)
        -> Result<DataSiteResponse<PrepareResult>, ()> {
        // In this stage, data manager will not do any write operation but mark cell owner in their meta as a lock
        // It will also check if write are realizable. If not, transaction manager should retry with new id
        // cell_ids must be sorted to avoid deadlock. It can be done from data manager by using BTreeMap keys
        self.update_clock(clock);
        let mut tnx = match self.tnxs.get_mut(tid) {
            Some(tnx) => tnx,
            _ => { return self.response_with(PrepareResult::TransactionNotExisted); }
        };
        let mut cell_metas: Vec<CellMetaGuard> = Vec::new();
        for cell_id in cell_ids {
            let meta = self.get_cell_meta(cell_id);
            if tid < &meta.read { // write too late
                break;
            }
            if tid < &meta.write {
                if meta.owner.is_some() { // not committed, should wait and try prepare again
                    return self.response_with(PrepareResult::Wait)
                }
            }
            cell_metas.push(meta);
        }
        if cell_metas.len() < cell_ids.len() {
            return self.response_with(PrepareResult::NotRealizable); // need retry
        } else {
            for mut meta in cell_metas {
                meta.owner = Some(tid.clone()) // set owner to lock this cell
            }
            tnx.state = TransactionState::Committing;
            tnx.affect_cells = cell_ids.len(); // for cell number check
            tnx.last_activity = get_time();    // check if transaction timeout
            return self.response_with(PrepareResult::Success);
        }
    }
    fn commit(&self, clock :&StandardVectorClock, tid: &TransactionId, cells: &Vec<CommitOp>)
        -> Result<DataSiteResponse<CommitResult>, ()>  {
        self.update_clock(clock);
        let mut tnx = match self.tnxs.get_mut(tid) {
            Some(tnx) => tnx,
            _ => { return self.response_with(CommitResult::CheckFailed(CommitCheckError::TransactionNotExisted)); }
        };
        // check state
        match tnx.state {
            TransactionState::Started => {return self.response_with(CommitResult::CheckFailed(CommitCheckError::TransactionNotCommitted))},
            TransactionState::Aborted => {return self.response_with(CommitResult::CheckFailed(CommitCheckError::TransactionAlreadyAborted))},
            TransactionState::Committed => {return self.response_with(CommitResult::CheckFailed(CommitCheckError::TransactionAlreadyCommitted))},
            TransactionState::Committing => {}
        }
        // check cell list integrity
        let prepared_cells_num = tnx.affect_cells;
        let arrived_cells_num = self.cells.len();
        if prepared_cells_num != arrived_cells_num {return self.response_with(CommitResult::CheckFailed(CommitCheckError::CellNumberDoesNotMatch(prepared_cells_num, arrived_cells_num)))}
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
            tnx.state = TransactionState::Aborted; // set to abort so it can't be abort again
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
            tnx.state = TransactionState::Committed;
            tnx.history = Some(commit_history);
            return self.response_with(CommitResult::Success)
        }
    }
    fn abort(&self, clock :&StandardVectorClock, tid: &TransactionId)
        -> Result<DataSiteResponse<()>, ()>  {
        self.update_clock(clock);
        unimplemented!()
    }
    fn end(&self, clock :&StandardVectorClock, tid: &TransactionId)
             -> Result<DataSiteResponse<()>, ()>  {
        self.update_clock(clock);
        unimplemented!()
    }
}