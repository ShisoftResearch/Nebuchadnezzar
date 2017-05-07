use bifrost::vector_clock::{VectorClock, StandardVectorClock, ServerVectorClock};
use bifrost::utils::time::get_time;
use chashmap::{CHashMap, WriteGuard};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use ram::types::{Id};
use ram::cell::{Cell, ReadError, WriteError};
use server::NebServer;
use parking_lot::Mutex;
use std::sync::mpsc::{channel, Sender, Receiver};
use futures;
use super::*;

type TxnAwaits = Arc<Mutex<HashMap<u64, Arc<AwaitingServer>>>>;
type TxnGuard<'a> = WriteGuard<'a, TransactionId, Transaction>;
type ChangedObjs = BTreeMap<u64, Vec<(Id, DataObject)>>;
type DataSiteClients = HashMap<u64, Arc<data_site::AsyncServiceClient>>;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TNX_MANAGER_RPC_SERVICE) as u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum TMError {
    TransactionNotFound,
    CannotLocateCellServer,
    NoResponseFromCellServer,
    AssertionError
}

#[derive(Clone)]
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
}

service! {
    rpc begin() -> TransactionId;
    rpc read(tid: TransactionId, id: Id) -> TransactionExecResult<Cell, ReadError> | TMError;
    rpc write(tid: TransactionId, id: Id, cell: Cell) -> TransactionExecResult<(), WriteError> | TMError;
    rpc update(tid: TransactionId, cell: Cell) -> TransactionExecResult<(), WriteError> | TMError;
    rpc remove(tid: TransactionId, id: Id) -> TransactionExecResult<(), WriteError> | TMError;

    rpc prepare(tid: TransactionId) -> PrepareResult | TMError;
    rpc commit(tid: TransactionId);
    rpc abort(tid: TransactionId);

    rpc go_ahead(tid: BTreeSet<TransactionId>); // invoked by data site to continue on it's transaction in case of waiting
}

pub struct TransactionManager {
    server: Arc<NebServer>,
    transactions: CHashMap<TransactionId, Transaction>,
    data_sites: CHashMap<u64, Arc<data_site::AsyncServiceClient>>,
    await_manager: AwaitManager
}
dispatch_rpc_service_functions!(TransactionManager);

impl TransactionManager {
    pub fn new(server: &Arc<NebServer>) -> Arc<TransactionManager> {
        Arc::new(TransactionManager {
            server: server.clone(),
            transactions: CHashMap::new(),
            data_sites: CHashMap::new(),
            await_manager: AwaitManager::new()
        })
    }
}

impl TransactionManager {
    fn server_id(&self) -> u64 {
        self.server.server_id
    }
    fn get_data_site(&self, server_id: u64) -> io::Result<Arc<data_site::AsyncServiceClient>> {
        if !self.data_sites.contains_key(&server_id) {
            let client = self.server.get_member_by_server_id(server_id)?;
            self.data_sites.upsert(server_id, || {
                data_site::AsyncServiceClient::new(DEFAULT_SERVICE_ID, &client)
            }, |_| {});
        }
        Ok(self.data_sites .get(&server_id).unwrap().clone())
    }
    fn get_data_site_by_id(&self, id: &Id) -> io::Result<(u64, Arc<data_site::AsyncServiceClient>)> {
        match self.server.get_server_id_by_id(id) {
            Some(id) => match self.get_data_site(id) {
                Ok(site) => Ok((id, site.clone())),
                Err(e) => Err(e)
            },
            _ => Err(io::Error::new(io::ErrorKind::NotFound, "cannot find data site for this id"))
        }
    }
    fn get_clock(&self) -> StandardVectorClock {
        self.server.txn_peer.clock.to_clock()
    }
    fn merge_clock(&self, clock: &StandardVectorClock) {
        self.server.txn_peer.clock.merge_with(clock)
    }
    fn get_transaction(&self, tid: &TransactionId) -> Result<TxnGuard, TMError> {
        match self.transactions.get_mut(tid) {
            Some(tnx) => Ok(tnx),
            _ => {Err(TMError::TransactionNotFound)}
        }
    }
    fn changed_objs(&self, txn: &TxnGuard) -> ChangedObjs {
        let mut changed_objs: BTreeMap<u64, Vec<(Id, DataObject)>> = BTreeMap::new();
        for (id, data_obj) in &txn.data {
            changed_objs.entry(data_obj.server).or_insert_with(|| Vec::new()).push((*id, data_obj.clone()));
        }
        changed_objs
    }
    fn data_sites(&self, changed_objs: &ChangedObjs) -> Result<DataSiteClients, TMError>  {
        let data_sites: HashMap<u64, _> = changed_objs.iter().map(|(server_id, _)| {
            (*server_id, self.get_data_site(*server_id))
        }).collect();
        if data_sites.iter().any(|(_, data_site)| { data_site.is_err() }) {
            return Err(TMError::CannotLocateCellServer)
        }
        Ok(data_sites.into_iter().map(
            |(server_id, data_site)| {
                (server_id, data_site.unwrap())
            }
        ).collect())
    }
    fn site_prepare(
        server: Arc<NebServer>,
        tid: &TransactionId,
        objs: &Vec<(Id, DataObject)>,
        data_site: &Arc<data_site::AsyncServiceClient>
    ) -> Box<futures::Future<Item = PrepareResult, Error = TMError>> {
        let self_server_id = server.server_id;
        let cell_ids: Vec<_> = objs.iter().map(|&(id, _)| id).collect();
        let res_future = data_site
            .prepare(&self_server_id, &server.txn_peer.clock.to_clock(), tid, &cell_ids)
            .map_err(|_| -> TMError {
                TMError::NoResponseFromCellServer
            })
            .map(move |prepare_res| -> PrepareResult {
                let prepare_res = prepare_res.unwrap();
                server.txn_peer.clock.merge_with(&prepare_res.clock);
                prepare_res.payload
            })
            .then(|prepare_payload| {
                match prepare_payload {
                    Ok(payload) => {
                        match payload {
                            PrepareResult::Wait => {
                                return Ok(payload)
                            },
                            _ => {
                                return Ok(payload)
                            }
                        }
                    },
                    Err(e) => Err(e)
                }
            });
        Box::new(res_future)
    }
    fn sites_prepare(&self, tid: &TransactionId, txn: Transaction, changed_objs: &ChangedObjs, data_sites: DataSiteClients) {
        let prepare_futures: Vec<_> = changed_objs.iter().map(|(server, objs)| {
            let data_site = data_sites.get(&server).unwrap();
            TransactionManager::site_prepare(
                self.server.clone(), tid, objs, data_site
            )
        }).collect();
        let prepare_result = future::join_all(prepare_futures).wait();
    }
}

impl Service for TransactionManager {
    fn begin(&self) -> Result<TransactionId, ()> {
        let id = self.server.txn_peer.clock.inc();
        self.transactions.insert(id.clone(), Transaction {
            start_time: get_time(),
            id: id.clone(),
            data: BTreeMap::new(),
            writes: BTreeSet::new()
        });
        Ok(id)
    }
    fn read(&self, tid: &TransactionId, id: &Id) -> Result<TransactionExecResult<Cell, ReadError>, TMError> {
        let mut txn = self.get_transaction(tid)?;
        if let Some(dataObj) = txn.data.get(id) {
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
            Ok((server_id, server)) => {
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
                                txn.data.insert(*id, DataObject {
                                    server: server_id,
                                    cell: Some(cell.clone()),
                                    new: false,
                                });
                            },
                            TransactionExecResult::Error(ReadError::CellDoesNotExisted) => {
                                txn.data.insert(*id, DataObject {
                                    server: server_id,
                                    cell: None,
                                    new: false,
                                });
                            }
                            TransactionExecResult::Wait => {} // TODO: deal with wait
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
        let mut txn = self.get_transaction(tid)?;
        match self.server.get_server_id_by_id(id) {
            Some(server_id) => {
                let have_cached_cell = txn.data.contains_key(id);
                if !have_cached_cell {
                    txn.data.insert(*id, DataObject {
                        server: server_id,
                        cell: Some(cell.clone()),
                        new: true,
                    });
                    Ok(TransactionExecResult::Accepted(()))
                } else {
                    let mut data_obj = txn.data.get_mut(id).unwrap();
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
        let mut txn = self.get_transaction(tid)?;
        let id = cell.id();
        match self.server.get_server_id_by_id(&id) {
            Some(server_id) => {
                let cell = cell.clone();
                if txn.data.contains_key(&id) {
                    txn.data.get_mut(&id).unwrap().cell = Some(cell)
                } else {
                    txn.data.insert(id, DataObject {
                        server: server_id,
                        cell: Some(cell),
                        new: false,
                    });
                }
                Ok(TransactionExecResult::Accepted(()))
            },
            None => Err(TMError::CannotLocateCellServer)
        }
    }
    fn remove(&self, tid: &TransactionId, id: &Id) -> Result<TransactionExecResult<(), WriteError>, TMError> {
        let mut txn = self.get_transaction(tid)?;
        match self.server.get_server_id_by_id(&id) {
            Some(server_id) => {
                if txn.data.contains_key(&id) {
                    let data_obj = txn.data.get_mut(&id).unwrap();
                    if data_obj.cell.is_none() {
                        return Ok(TransactionExecResult::Error(WriteError::CellDoesNotExisted))
                    }
                    data_obj.cell = None;
                } else {
                    txn.data.insert(*id, DataObject {
                        server: server_id,
                        cell: None,
                        new: false,
                    });
                }
                Ok(TransactionExecResult::Accepted(()))
            },
            None => Err(TMError::CannotLocateCellServer)
        }
    }
    fn prepare(&self, tid: &TransactionId) -> Result<PrepareResult, TMError> {
        let mut txn = self.get_transaction(tid)?;
        let mut changed_objs = self.changed_objs(&txn);
        let data_sites = self.data_sites(&changed_objs)?;
        Err(TMError::CannotLocateCellServer)
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



struct AwaitingServer {
    sender: Mutex<Sender<()>>,
    receiver: Mutex<Receiver<()>>,
}

impl AwaitingServer {
    pub fn new() -> AwaitingServer {
        let (sender, receiver) = channel();
        AwaitingServer {
            sender: Mutex::new(sender),
            receiver: Mutex::new(receiver)
        }
    }
    pub fn send(&self) {
        self.sender.lock().send(());
    }
    pub fn wait(&self) {
        self.receiver.lock().recv();
    }
}

struct AwaitManager {
    channels: Mutex<HashMap<TransactionId, TxnAwaits>>
}

impl AwaitManager {
    pub fn new() -> AwaitManager {
        AwaitManager {
            channels: Mutex::new(HashMap::new())
        }
    }
    pub fn get_txn(&self, tid: &TransactionId) -> TxnAwaits {
        self.channels.lock()
            .entry(tid.clone())
            .or_insert_with(|| Arc::new(Mutex::new(HashMap::new())))
            .clone()
    }
    pub fn server_from_awaits(awaits: TxnAwaits, server_id: u64) -> Arc<AwaitingServer> {
        awaits.lock()
            .entry(server_id)
            .or_insert_with(|| Arc::new(AwaitingServer::new()))
            .clone()
    }
}