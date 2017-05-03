use bifrost::vector_clock::{VectorClock, StandardVectorClock, ServerVectorClock};
use bifrost::utils::time::get_time;
use chashmap::CHashMap;
use std::collections::{BTreeMap, BTreeSet};
use ram::types::{Id};
use ram::cell::{Cell, ReadError, WriteError};
use server::NebServer;
use futures::sync::mpsc::{Sender, Receiver, channel};
use super::*;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TNX_MANAGER_RPC_SERVICE) as u64;

struct DataObject {
    id: Id,
    server: u64,
    cell: Cell
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
    start_time: i64, // use for timeout detecting
    id: TransactionId,
    reads: BTreeMap<Id, DataObject>,
    writes: BTreeMap<Id, DataObject>,
    await_chan: (Sender<AwaitResponse>, Receiver<AwaitResponse>)
}

service! {
    rpc begin() -> TransactionId;
    rpc read(tid: TransactionId, id: Id) -> TransactionExecResult<Cell, ReadError>;
    rpc write(tid: TransactionId, id: Id, cell: Cell) -> TransactionExecResult<(), WriteError>;
    rpc update(tid: TransactionId, cell: Cell) -> TransactionExecResult<(), WriteError>;
    rpc remove(tid: TransactionId, id: Id) -> TransactionExecResult<(), WriteError>;

    rpc commit(tid: TransactionId);
    rpc abort(tid: TransactionId);

    rpc go_ahead(tid: BTreeSet<TransactionId>); // invoked by data site to continue on it's transaction in case of waiting
}

pub struct TransactionManager {
    server: Arc<NebServer>,
    transactions: CHashMap<TransactionId, Transaction>,
}
dispatch_rpc_service_functions!(TransactionManager);

impl TransactionManager {
    pub fn new(server: &Arc<NebServer>) -> Arc<TransactionManager> {
        Arc::new(TransactionManager {
            server: server.clone(),
            transactions: CHashMap::new()
        })
    }
}

impl Service for TransactionManager {
    fn begin(&self) -> Result<TransactionId, ()> {
        let id = self.server.tnx_peer.clock.inc();
        self.transactions.insert(id.clone(), Transaction {
            start_time: get_time(),
            id: id.clone(),
            reads: BTreeMap::new(),
            writes: BTreeMap::new(),
            await_chan: channel(1)
        });
        Ok(id)
    }
    fn read(&self, tid: &TransactionId, id: &Id) -> Result<TransactionExecResult<Cell, ReadError>, ()> {
        if let Some(ref mut trans) = self.transactions.get_mut(&tid) {
            //trans.reads.entry(id.clone()).or_insert_with
        }
        Err(())
    }
    fn write(&self, tid: &TransactionId, id: &Id, cell: &Cell) -> Result<TransactionExecResult<(), WriteError>, ()> {
        Err(())
    }
    fn update(&self, tid: &TransactionId, cell: &Cell) -> Result<TransactionExecResult<(), WriteError>, ()> {
        Err(())
    }
    fn remove(&self, tid: &TransactionId, id: &Id) -> Result<TransactionExecResult<(), WriteError>, ()> {
        Err(())
    }
    fn commit(&self, tid: &TransactionId) -> Result<(), ()> {
        Err(())
    }
    fn abort(&self, tid: &TransactionId) -> Result<(), ()> {
        Err(())
    }
    fn go_ahead(&self, tid: &BTreeSet<TransactionId>) -> Result<(), ()> {
        Err(())
    }
}
