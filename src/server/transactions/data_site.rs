use bifrost::vector_clock::{StandardVectorClock};
use std::collections::{HashSet, HashMap};
use concurrent_hashmap::ConcHashMap;
use ram::types::{Id};
use ram::cell::{Cell, ReadError, WriteError};
use server::NebServer;
use super::*;

pub struct Timestamp {
    read: i64,
    write: i64,
    committed: bool
}

#[derive(Debug, Eq, PartialEq)]
enum TransactionState {
    Started,
    Aborted,
    Committing,
    Committed
}

struct Transaction {
    id: TransactionId,
    server: u64,
    state: TransactionState
}

pub struct DataManager {
    timestamps: ConcHashMap<Id, Timestamp>,
    prev_cells: ConcHashMap<Id, Cell>,
    status: ConcHashMap<TransactionId, Transaction>,
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
//    fn get_transaction(&self, tid: &TransactionId) -> &Transaction {
//        self.status.
//    }
}

impl Service for DataManager {
    fn read(&self, server_id: &u64, clock: &StandardVectorClock, tid: &TransactionId, id: &Id)
        -> Result<DataSiteResponse<TransactionExecResult<Cell, ReadError>>, ()> {
        let local_clock = self.local_clock();
        self.update_clock(clock);

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