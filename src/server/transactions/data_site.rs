use bifrost::vector_clock::{StandardVectorClock, Relation};
use bifrost::utils::time::get_time;
use std::collections::{BTreeSet, BTreeMap};
use chashmap::{CHashMap, WriteGuard};
use ram::types::{Id};
use ram::cell::{Cell, ReadError, WriteError};
use server::NebServer;
use super::*;

pub type CellMetaGuard <'a> = WriteGuard<'a, Id, CellMeta>;

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
    WriteError(Id, WriteError),
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

    rpc abort(clock :StandardVectorClock, tid: TransactionId) -> DataSiteResponse<()>;
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
                    last_activity: get_time()
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
        let cell_history: BTreeMap<Id, Option<Cell>> = BTreeMap::new(); // for rollback in case of write error
        for cell_op in cells {
            match cell_op {
                &CommitOp::Write(ref cell) => {

                },
                &CommitOp::Remove(ref cell_id) => {

                },
                &CommitOp::Update(ref cell) => {

                }
            }
        }
        unimplemented!()

    }
    fn abort(&self, clock :&StandardVectorClock, tid: &TransactionId)
        -> Result<DataSiteResponse<()>, ()>  {
        self.update_clock(clock);
        unimplemented!()
    }
}