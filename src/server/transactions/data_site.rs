use bifrost::vector_clock::{StandardVectorClock, Relation};
use std::collections::{BTreeSet, HashMap};
use chashmap::{CHashMap, WriteGuard};
use ram::types::{Id};
use ram::cell::{Cell, ReadError, WriteError};
use server::NebServer;
use super::*;

pub struct CellMeta {
    read: Option<TransactionId>,
    write: Option<TransactionId>,
    owner: Option<TransactionId>, // transaction that owns the cell in Committing state
    waiting: BTreeSet<TransactionId>, // transactions that waiting for owner to finish
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CellCommit {
    Write(Cell),
    Update(Cell),
    Remove(Id)
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CellPrepare {
    Write(Id),
    Update(Id),
    Remove(Id)
}

#[derive(Debug, Eq, PartialEq)]
enum TransactionState {
    Started,
    Aborted,
    Committing,
    Committed
}

struct Transaction {
    server: u64,
    id: TransactionId,
    state: TransactionState
}

pub struct DataManager {
    cells: CHashMap<Id, CellMeta>,
    tnxs: CHashMap<TransactionId, Transaction>,
    server: Arc<NebServer>
}

service! {
    rpc read(server_id: u64, clock: StandardVectorClock, tid: TransactionId, id: Id) -> DataSiteResponse<TransactionExecResult<Cell, ReadError>>;

    // two phase commit
    rpc prepare(clock :StandardVectorClock, tid: TransactionId, cells: Vec<CellPrepare>) -> DataSiteResponse<bool>;
    rpc commit(clock :StandardVectorClock, tid: TransactionId, cells: Vec<CellCommit>) -> DataSiteResponse<bool>;

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
                    state: TransactionState::Started
                }
            }, |t|{});
        }
        match self.tnxs.get_mut(tid) {
            Some(tnx) => tnx,
            _ => self.get_transaction(tid, server) // it should be rare
        }
    }
    fn get_cell_meta(&self, id: &Id) -> WriteGuard<Id, CellMeta > {
        if !self.cells.contains_key(id) {
            self.cells.upsert(id.clone(), || {
                CellMeta {
                    read: None,
                    write: None,
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
        let local_clock = self.local_clock();
        self.update_clock(clock);
        let mut tnx = self.get_transaction(tid, server_id);
        let mut meta = self.get_cell_meta(id);
        let committing = meta.owner.is_some();
        let read_too_late = !meta.write.is_none() && match meta.write {
            Some(ref w) => w > tid,
            _ => false
        };
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
        let update_read = {
            meta.read.is_none() || match meta.read {
                Some(ref r) => r < tid,
                None => true
            }
        };
        if update_read {
            meta.read = Some(tid.clone())
        }
        self.response_with(TransactionExecResult::Accepted(cell))
    }
    fn prepare(&self, clock :&StandardVectorClock, tid: &TransactionId, cells: &Vec<CellPrepare>)
        -> Result<DataSiteResponse<bool>, ()> {
        let local_clock = self.local_clock();
        self.update_clock(clock);
        unimplemented!()
    }
    fn commit(&self, clock :&StandardVectorClock, tid: &TransactionId, cells: &Vec<CellCommit>)
        -> Result<DataSiteResponse<bool>, ()>  {
        let local_clock = self.local_clock();
        self.update_clock(clock);
        unimplemented!()
    }
    fn abort(&self, clock :&StandardVectorClock, tid: &TransactionId)
        -> Result<DataSiteResponse<()>, ()>  {
        let local_clock = self.local_clock();
        self.update_clock(clock);
        unimplemented!()
    }
}