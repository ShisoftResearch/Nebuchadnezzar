use bifrost::vector_clock::{VectorClock, StandardVectorClock, ServerVectorClock};
use concurrent_hashmap::ConcHashMap;
use std::collections::{HashSet, HashMap};
use ram::types::{Id};
use ram::cell::{Cell, ReadError, WriteError};
use futures::sync::mpsc::{Sender, Receiver, channel};
use super::*;

struct DataObject {
    id: Id,
    server: u64,
}

impl PartialEq for DataObject {
    fn eq(&self, other: &DataObject) -> bool {
        self.id == other.id
    }
    fn ne(&self, other: &DataObject) -> bool {
        self.id != other.id
    }
}

struct Transaction {
    start_time: i64,
    id: TransactionId,
    reads: HashSet<DataObject>,
    writes: HashSet<DataObject>,
    await_chan: (Sender<AwaitResponse>, Receiver<AwaitResponse>)
}

service! {
    rpc begin() -> TransactionId;
    rpc read(tid: TransactionId, id: Id) -> TransactionExecResult<Cell, ReadError>;
    rpc write(tid: TransactionId, id: Id, cell: Cell) -> TransactionExecResult<(), WriteError>;
    rpc update(tid: TransactionId, cell: Cell) -> TransactionExecResult<(), WriteError>;
    rpc remove(tid: TransactionId, id: Id) -> Result<(), WriteError>;

    rpc commit(tid: TransactionId);
    rpc abort(tid: TransactionId);

    rpc go_ahead(tid: TransactionId, response: AwaitResponse); // invoked by data site to continue on it's transaction in case of waiting
}

pub struct TransactionManager {
    peer: Arc<Peer>,
    transactions: ConcHashMap<TransactionId, Transaction>
}
