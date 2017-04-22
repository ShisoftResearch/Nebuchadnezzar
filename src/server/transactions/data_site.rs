use bifrost::vector_clock::{StandardVectorClock};
use std::collections::{HashSet, HashMap};
use concurrent_hashmap::ConcHashMap;
use ram::types::{Id};
use ram::cell::{Cell, ReadError, WriteError};
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
    peer: Arc<Peer>,
    timestamps: ConcHashMap<Id, Timestamp>,
    prev_cells: ConcHashMap<Id, Cell>,
    status: ConcHashMap<TransactionId, Transaction>
}

service! {
    rpc read(server_id: u64, clock :StandardVectorClock, tid: TransactionId, id: Id) -> DataSiteResponse<TransactionExecResult<Cell, ReadError>>;
    rpc write(server_id: u64, clock :StandardVectorClock, tid: TransactionId, id: Id, cell: Cell) -> DataSiteResponse<TransactionExecResult<(), WriteError>>;
    rpc update(server_id: u64, clock :StandardVectorClock, tid: TransactionId, cell: Cell) -> DataSiteResponse<TransactionExecResult<(), WriteError>>;
    rpc remove(server_id: u64, clock :StandardVectorClock, tid: TransactionId, id: Id) -> DataSiteResponse<Result<(), WriteError>>;

    // two phase commit
    rpc prepare(clock :StandardVectorClock, tid: TransactionId) -> DataSiteResponse<bool>;
    rpc commit(clock :StandardVectorClock, tid: TransactionId) -> DataSiteResponse<bool>;

    rpc abort(clock :StandardVectorClock, tid: TransactionId) -> StandardVectorClock;
}