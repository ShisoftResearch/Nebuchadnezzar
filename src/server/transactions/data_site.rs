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

pub struct DataManager {
    peer: Arc<Peer>,
    timestamps: ConcHashMap<Id, Timestamp>,
    prev_value: ConcHashMap<Id, Cell>
}

service! {
    rpc read(clock :StandardVectorClock, tid: TransactionId, id: Id) -> DataSiteResponse<TransactionExecResult<usize, ReadError>>;
    rpc write(clock :StandardVectorClock, tid: TransactionId, id: Id, cell: Cell) -> DataSiteResponse<TransactionExecResult<usize, WriteError>>;
    rpc update(clock :StandardVectorClock, tid: TransactionId, cell: Cell) -> DataSiteResponse<TransactionExecResult<usize, WriteError>>;
    rpc remove(clock :StandardVectorClock, tid: TransactionId, id: Id) -> DataSiteResponse<Result<(), WriteError>>;
    rpc prepare(clock :StandardVectorClock, tid: TransactionId) -> DataSiteResponse<bool>;
    rpc commit(clock :StandardVectorClock, tid: TransactionId) -> StandardVectorClock;
    rpc abort(clock :StandardVectorClock, tid: TransactionId) -> StandardVectorClock;
}