use bifrost::vector_clock::{VectorClock, StandardVectorClock, ServerVectorClock};
use bifrost::utils::time::get_time;
use chashmap::{CHashMap, WriteGuard};
use std::collections::{BTreeMap, BTreeSet};
use ram::types::{Id};
use ram::cell::{Cell, ReadError, WriteError};
use server::NebServer;
use futures::sync::mpsc::{Sender, Receiver, channel};
use super::*;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TNX_MANAGER_RPC_SERVICE) as u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum TMError {
    TransactionNotFound,
    CannotLocateCellServer,
    NoResponseFromCellServer,
}

struct DataObject {
    server: u64,
    cell: Option<Cell>,
    new: bool
}

struct Transaction {
    start_time: i64, // use for timeout detecting
    id: TransactionId,
    data: BTreeMap<Id, DataObject>,
    writes: BTreeSet<Id>,
    await_chan: (Sender<()>, Receiver<()>)
}

service! {
    rpc begin() -> TransactionId;
    rpc read(tid: TransactionId, id: Id) -> TransactionExecResult<Cell, ReadError> | TMError;
    rpc write(tid: TransactionId, id: Id, cell: Cell) -> TransactionExecResult<(), WriteError> | TMError;
    rpc update(tid: TransactionId, cell: Cell) -> TransactionExecResult<(), WriteError> | TMError;
    rpc remove(tid: TransactionId, id: Id) -> TransactionExecResult<(), WriteError> | TMError;

    rpc prepare(tid: TransactionId);
    rpc commit(tid: TransactionId);
    rpc abort(tid: TransactionId);

    rpc go_ahead(tid: BTreeSet<TransactionId>); // invoked by data site to continue on it's transaction in case of waiting
}

pub struct TransactionManager {
    server: Arc<NebServer>,
    transactions: CHashMap<TransactionId, Transaction>,
    data_sites: CHashMap<u64, Arc<data_site::AsyncServiceClient>>,
}
dispatch_rpc_service_functions!(TransactionManager);

impl TransactionManager {
    pub fn new(server: &Arc<NebServer>) -> Arc<TransactionManager> {
        Arc::new(TransactionManager {
            server: server.clone(),
            transactions: CHashMap::new(),
            data_sites: CHashMap::new()
        })
    }
}

impl TransactionManager {
    fn get_data_site(&self, server_id: u64) -> io::Result<(Arc<data_site::AsyncServiceClient>, u64)> {
        if !self.data_sites.contains_key(&server_id) {
            let client = self.server.get_member_by_server_id(server_id)?;
            self.data_sites.upsert(server_id, || {
                data_site::AsyncServiceClient::new(DEFAULT_SERVICE_ID, &client)
            }, |_| {});
        }
        Ok((self.data_sites .get(&server_id).unwrap().clone(), server_id))
    }
    fn get_data_site_by_id(&self, id: &Id) -> io::Result<(Arc<data_site::AsyncServiceClient>, u64)> {
        match self.server.get_server_id_by_id(id) {
            Some(id) => self.get_data_site(id),
            _ => Err(io::Error::new(io::ErrorKind::NotFound, "cannot find data site for this id"))
        }
    }
    fn get_clock(&self) -> StandardVectorClock {
        self.server.tnx_peer.clock.to_clock()
    }
    fn merge_clock(&self, clock: &StandardVectorClock) {
        self.server.tnx_peer.clock.merge_with(clock)
    }
    fn get_transaction(&self, tid: &TransactionId) -> Result<WriteGuard<TransactionId, Transaction>, TMError> {
        match self.transactions.get_mut(tid) {
            Some(tnx) => Ok(tnx),
            _ => {Err(TMError::TransactionNotFound)}
        }
    }
}

impl Service for TransactionManager {
    fn begin(&self) -> Result<TransactionId, ()> {
        let id = self.server.tnx_peer.clock.inc();
        self.transactions.insert(id.clone(), Transaction {
            start_time: get_time(),
            id: id.clone(),
            data: BTreeMap::new(),
            writes: BTreeSet::new(),
            await_chan: channel(1)
        });
        Ok(id)
    }
    fn read(&self, tid: &TransactionId, id: &Id) -> Result<TransactionExecResult<Cell, ReadError>, TMError> {
        let mut tnx = self.get_transaction(tid)?;
        if let Some(dataObj) = tnx.data.get(id) {
            match dataObj.cell {
                Some(ref cell) => {
                    return Ok(TransactionExecResult::Accepted(cell.clone())) // read from cache
                },
                None => {
                   return Ok(TransactionExecResult::Error(ReadError::CellDoesNotExisted))
                }
            }
        }
        let server = self.get_data_site_by_id(&id);
        match server {
            Ok((server, server_id)) => {
                let self_server_id = self.server.server_id;
                let read_response = server.read(
                    &self_server_id,
                    &self.get_clock(),
                    tid, id
                ).wait();
                match read_response {
                    Ok(dsr) => {
                        let dsr = dsr.unwrap();
                        self.merge_clock(&dsr.clock);
                        match dsr.payload {
                            TransactionExecResult::Accepted(ref cell) => {
                                tnx.data.insert(*id, DataObject {
                                    server: server_id,
                                    cell: Some(cell.clone()),
                                    new: false
                                });
                            },
                            TransactionExecResult::Error(ReadError::CellDoesNotExisted) => {
                                tnx.data.insert(*id, DataObject {
                                    server: server_id,
                                    cell: None,
                                    new: false
                                });
                            }
                            _ => {}
                        }
                        Ok(dsr.payload)
                    },
                    Err(e) => {
                        error!("{:?}", e);
                        Err(TMError::NoResponseFromCellServer)
                    }
                }
            },
            Err(e) => {
                error!("{:?}", e);
                Err(TMError::CannotLocateCellServer)
            }
        }
    }
    fn write(&self, tid: &TransactionId, id: &Id, cell: &Cell) -> Result<TransactionExecResult<(), WriteError>, TMError> {
        let mut tnx = self.get_transaction(tid)?;
        match self.server.get_server_id_by_id(id) {
            Some(server_id) => {
                let have_cached_cell = tnx.data.contains_key(id);
                if !have_cached_cell {
                    tnx.data.insert(*id, DataObject {
                        server: server_id,
                        cell: Some(cell.clone()),
                        new: true
                    });
                    Ok(TransactionExecResult::Accepted(()))
                } else {
                    let mut data_obj = tnx.data.get_mut(id).unwrap();
                    if !data_obj.cell.is_none() {
                        return Ok(TransactionExecResult::Error(WriteError::CellAlreadyExisted))
                    }
                    data_obj.cell = Some(cell.clone());
                    Ok(TransactionExecResult::Accepted(()))
                }
            },
            None => Err(TMError::CannotLocateCellServer)
        }
    }
    fn update(&self, tid: &TransactionId, cell: &Cell) -> Result<TransactionExecResult<(), WriteError>, TMError> {
        let mut tnx = self.get_transaction(tid)?;
        let id = cell.id();
        match self.server.get_server_id_by_id(&id) {
            Some(server_id) => {
                let cell = cell.clone();
                if tnx.data.contains_key(&id) {
                    tnx.data.get_mut(&id).unwrap().cell = Some(cell)
                } else {
                    tnx.data.insert(id, DataObject {
                        server: server_id,
                        cell: Some(cell),
                        new: false
                    });
                }
                Ok(TransactionExecResult::Accepted(()))
            },
            None => Err(TMError::CannotLocateCellServer)
        }
    }
    fn remove(&self, tid: &TransactionId, id: &Id) -> Result<TransactionExecResult<(), WriteError>, TMError> {
        let mut tnx = self.get_transaction(tid)?;
        match self.server.get_server_id_by_id(&id) {
            Some(server_id) => {
                if tnx.data.contains_key(&id) {
                    let data_obj = tnx.data.get_mut(&id).unwrap();
                    if data_obj.cell.is_none() {
                        return Ok(TransactionExecResult::Error(WriteError::CellDoesNotExisted))
                    }
                    data_obj.cell = None;
                } else {
                    tnx.data.insert(*id, DataObject {
                        server: server_id,
                        cell: None,
                        new: false
                    });
                }
                Ok(TransactionExecResult::Accepted(()))
            },
            None => Err(TMError::CannotLocateCellServer)
        }
    }
    fn prepare(&self, tid: &TransactionId) -> Result<(), ()> {
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
