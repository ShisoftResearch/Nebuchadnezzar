use bifrost::vector_clock::{StandardVectorClock};
use std::collections::{BTreeSet, HashMap};
use chashmap::{CHashMap, WriteGuard};
use ram::types::{Id};
use ram::cell::{Cell, ReadError, WriteError};
use server::NebServer;
use super::*;

pub struct CellMeta {
    read: i64,
    owner: Option<TransactionId>, // transaction that owns the cell in Committing state
    waiting: BTreeSet<TransactionId>, // transactions that waiting for owner to finish
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
    rpc write(server_id: u64, clock: StandardVectorClock, tid: TransactionId, id: Id, cell: Cell) -> DataSiteResponse<TransactionExecResult<(), WriteError>>;
    rpc update(server_id: u64, clock: StandardVectorClock, tid: TransactionId, cell: Cell) -> DataSiteResponse<TransactionExecResult<(), WriteError>>;
    rpc remove(server_id: u64, clock: StandardVectorClock, tid: TransactionId, id: Id) -> DataSiteResponse<Result<(), WriteError>>;

    // two phase commit
    rpc prepare(clock :StandardVectorClock, tid: TransactionId) -> DataSiteResponse<bool>;
    rpc commit(clock :StandardVectorClock, tid: TransactionId, cells: Vec<Cell>) -> DataSiteResponse<bool>;

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
                    read: 0,
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
}

impl Service for DataManager {
    fn read(&self, server_id: &u64, clock: &StandardVectorClock, tid: &TransactionId, id: &Id)
        -> Result<DataSiteResponse<TransactionExecResult<Cell, ReadError>>, ()> {
        let local_clock = self.local_clock();
        self.update_clock(clock);
        let mut tnx = self.get_transaction(tid, server_id);
        let mut cell= self.get_cell_meta(id);

        unimplemented!()
    }
    fn write(&self, server_id: &u64, clock: &StandardVectorClock, tid: &TransactionId, id: &Id, cell: &Cell)
        -> Result<DataSiteResponse<TransactionExecResult<(), WriteError>>, ()> {
        let local_clock = self.local_clock();
        self.update_clock(clock);
        unimplemented!()
    }
    fn update(&self, server_id: &u64, clock: &StandardVectorClock, tid: &TransactionId, cell: &Cell)
        -> Result<DataSiteResponse<TransactionExecResult<(), WriteError>>, ()> {
        let local_clock = self.local_clock();
        self.update_clock(clock);
        unimplemented!()
    }
    fn remove(&self, server_id: &u64, clock: &StandardVectorClock, tid: &TransactionId, id: &Id)
        -> Result<DataSiteResponse<Result<(), WriteError>>, ()> {
        let local_clock = self.local_clock();
        self.update_clock(clock);
        unimplemented!()
    }

    fn prepare(&self, clock :&StandardVectorClock, tid: &TransactionId)
        -> Result<DataSiteResponse<bool>, ()> {
        let local_clock = self.local_clock();
        self.update_clock(clock);
        unimplemented!()
    }
    fn commit(&self, clock :&StandardVectorClock, tid: &TransactionId, cells: &Vec<Cell>)
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