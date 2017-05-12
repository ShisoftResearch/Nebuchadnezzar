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
    RPCErrorFromCellServer,
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
    changed_objects: ChangedObjs
}

service! {
    rpc begin() -> TransactionId;
    rpc read(tid: TransactionId, id: Id) -> TransactionExecResult<Cell, ReadError> | TMError;
    rpc write(tid: TransactionId, cell: Cell) -> TransactionExecResult<(), WriteError> | TMError;
    rpc update(tid: TransactionId, cell: Cell) -> TransactionExecResult<(), WriteError> | TMError;
    rpc remove(tid: TransactionId, id: Id) -> TransactionExecResult<(), WriteError> | TMError;

    rpc prepare(tid: TransactionId) -> TMPrepareResult | TMError;
    rpc commit(tid: TransactionId) -> TMCommitResult | TMError;
    rpc abort(tid: TransactionId) -> AbortResult | TMError;

    rpc go_ahead(tids: BTreeSet<TransactionId>, server_id: u64); // invoked by data site to continue on it's transaction in case of waiting
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
    fn read_from_site(
        &self, server_id: u64, server: Arc<data_site::AsyncServiceClient>,
        tid: &TransactionId, id: &Id, mut txn: TxnGuard, await: TxnAwaits
    ) -> Result<TransactionExecResult<Cell, ReadError>, TMError> {
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
                    TransactionExecResult::Wait => {
                        AwaitManager::txn_wait(&await, server_id);
                        self.read_from_site(server_id, server, tid, id, txn, await);
                    }
                    _ => {}
                }
                Ok(dsr.payload)
            },
            Err(e) => {
                error!("{:?}", e);
                Err(TMError::RPCErrorFromCellServer)
            }
        }
    }
    fn generate_changed_objs(&self, txn: &mut TxnGuard) {
        let mut changed_objs = ChangedObjs::new();
        for (id, data_obj) in &txn.data {
            changed_objs.entry(data_obj.server).or_insert_with(|| Vec::new()).push((*id, data_obj.clone()));
        }
        txn.changed_objects = changed_objs;
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
        server: Arc<NebServer>, awaits: TxnAwaits, tid: TransactionId, objs: Vec<(Id, DataObject)>,
        data_site: Arc<data_site::AsyncServiceClient>
    ) -> Box<futures::Future<Item =DMPrepareResult, Error = TMError>> {
        let self_server_id = server.server_id;
        let cell_ids: Vec<_> = objs.iter().map(|&(id, _)| id).collect();
        let server_for_clock = server.clone();
        let res_future = data_site
            .prepare(&self_server_id, &server.txn_peer.clock.to_clock(), &tid, &cell_ids)
            .map_err(|_| -> TMError {
                TMError::RPCErrorFromCellServer
            })
            .map(move |prepare_res| -> DMPrepareResult {
                let prepare_res = prepare_res.unwrap();
                server_for_clock.txn_peer.clock.merge_with(&prepare_res.clock);
                prepare_res.payload
            })
            .then(move |prepare_payload| {
                match prepare_payload {
                    Ok(payload) => {
                        match payload {
                            DMPrepareResult::Wait => {
                                AwaitManager::txn_wait(&awaits, data_site.server_id);
                                return TransactionManager::site_prepare(
                                    server, awaits, tid, objs, data_site
                                ).wait() // after waiting, retry
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
    fn sites_prepare(&self, tid: &TransactionId, changed_objs: ChangedObjs, data_sites: DataSiteClients)
        -> Result<DMPrepareResult, TMError> {
        let prepare_futures: Vec<_> = changed_objs.into_iter().map(|(server, objs)| {
            let data_site = data_sites.get(&server).unwrap().clone();
            TransactionManager::site_prepare(
                self.server.clone(),
                self.await_manager.get_txn(tid),
                tid.clone(), objs, data_site
            )
        }).collect();
        let prepare_results = future::join_all(prepare_futures).wait()?;
        for result in prepare_results {
            match result {
                DMPrepareResult::Success => {},
                _ => {return Ok(result)}
            }
        }
        Ok(DMPrepareResult::Success)
    }
    fn sites_commit(&self, tid: &TransactionId, changed_objs: &ChangedObjs, data_sites: &DataSiteClients)
        -> Result<DMCommitResult, TMError> {
        let commit_futures: Vec<_> = changed_objs.iter().map(|(ref server_id, ref objs)| {
            let data_site = data_sites.get(server_id).unwrap().clone();
            let ops: Vec<CommitOp> = objs.iter()
                .map(|&(ref cell_id, ref data_obj)| {
                if data_obj.cell.is_none() && !data_obj.new {
                    CommitOp::Remove(*cell_id)
                } else if data_obj.new {
                    CommitOp::Write(data_obj.cell.clone().unwrap())
                } else if !data_obj.new {
                    CommitOp::Update(data_obj.cell.clone().unwrap())
                } else {
                    CommitOp::None
                }
            }).collect();
            data_site.commit(&self.get_clock(), tid, &ops)
        }).collect();
        let commit_results = future::join_all(commit_futures).wait();
        if let Ok(commit_results) = commit_results {
            for result in commit_results {
                if let Ok(dsr) = result {
                    self.merge_clock(&dsr.clock);
                    match dsr.payload {
                        DMCommitResult::Success => {},
                        _ => {
                            return Ok(dsr.payload);
                        }
                    }
                } else {
                    return Err(TMError::AssertionError)
                }
            }
        } else {
            return Err(TMError::RPCErrorFromCellServer)
        }
        Ok(DMCommitResult::Success)
    }
    fn sites_abort(&self, tid: &TransactionId, changed_objs: &ChangedObjs, data_sites: &DataSiteClients)
        -> Result<AbortResult, TMError> {
        let abort_futures: Vec<_> = changed_objs.iter().map(|(ref server_id, _)| {
            let data_site = data_sites.get(server_id).unwrap().clone();
            data_site.abort(&self.get_clock(), tid)
        }).collect();
        let abort_results = future::join_all(abort_futures).wait();
        if abort_results.is_err() {return Err(TMError::RPCErrorFromCellServer)}
        let abort_results = abort_results.unwrap();
        let mut rollback_failures = Vec::new();
        for result in abort_results {
            match result {
                Ok(asr) => {
                    let payload = asr.payload;
                    self.merge_clock(&asr.clock);
                    match payload {
                        AbortResult::Success(mut failures) => {
                            if let Some(mut failures) = failures {
                                rollback_failures.append(&mut failures);
                            }
                        },
                        _ => (return Ok(payload))
                    }
                },
                Err(_) => {return Err(TMError::AssertionError)}
            }
        }
        return
            Ok(AbortResult::Success(
                if rollback_failures.is_empty()
                    {None} else {Some(rollback_failures)}
            ))
    }
    fn sites_end(&self, tid: &TransactionId, changed_objs: &ChangedObjs, data_sites: &DataSiteClients)
        -> Result<EndResult, TMError> {
        let end_futures: Vec<_> = changed_objs.iter().map(|(ref server_id, _)| {
            let data_site = data_sites.get(server_id).unwrap().clone();
            data_site.end(&self.get_clock(), tid)
        }).collect();
        let end_results = future::join_all(end_futures).wait();
        if end_results.is_err() {return Err(TMError::RPCErrorFromCellServer)}
        let end_results = end_results.unwrap();
        for result in end_results {
            if let Ok(result) = result {
                self.merge_clock(&result.clock);
                let payload = result.payload;
                match payload {
                    EndResult::Success => {},
                    _ => {return Ok(payload);}
                }
            } else {
                return Err(TMError::AssertionError);
            }
        }
        Ok(EndResult::Success)
    }
}

impl Service for TransactionManager {
    fn begin(&self) -> Result<TransactionId, ()> {
        let id = self.server.txn_peer.clock.inc();
        self.transactions.insert(id.clone(), Transaction {
            start_time: get_time(),
            id: id.clone(),
            data: BTreeMap::new(),
            changed_objects: ChangedObjs::new()
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
                let await = self.await_manager.get_txn(tid);
                self.read_from_site(server_id, server, tid, id, txn, await)
            },
            Err(e) => {
                error!("{:?}", e);
                Err(TMError::CannotLocateCellServer)
            }
        }
    }
    fn write(&self, tid: &TransactionId, cell: &Cell) -> Result<TransactionExecResult<(), WriteError>, TMError> {
        let mut txn = self.get_transaction(tid)?;
        let id = cell.id();
        match self.server.get_server_id_by_id(&id) {
            Some(server_id) => {
                let have_cached_cell = txn.data.contains_key(&id);
                if !have_cached_cell {
                    txn.data.insert(id, DataObject {
                        server: server_id,
                        cell: Some(cell.clone()),
                        new: true,
                    });
                    Ok(TransactionExecResult::Accepted(()))
                } else {
                    let mut data_obj = txn.data.get_mut(&id).unwrap();
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
                    let mut new_obj = false;
                    {
                        let data_obj = txn.data.get_mut(&id).unwrap();
                        if data_obj.cell.is_none() {
                            return Ok(TransactionExecResult::Error(WriteError::CellDoesNotExisted))
                        }
                        if data_obj.new {
                            new_obj = true;
                        } else {
                            data_obj.cell = None;
                        }
                    }
                    if new_obj {
                        txn.data.remove(&id);
                    }
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
    fn prepare(&self, tid: &TransactionId) -> Result<TMPrepareResult, TMError> {
        let mut txn = self.get_transaction(tid)?;
        self.generate_changed_objs(&mut txn);
        let generated_objs = &txn.changed_objects;
        let changed_objs = generated_objs.clone();
        let data_sites = self.data_sites(&changed_objs)?;
        let sites_prepare_result = self.sites_prepare(tid, changed_objs, data_sites.clone())?;
        match sites_prepare_result {
            DMPrepareResult::Success => {},
            _ => {
                self.sites_abort(tid, generated_objs, &data_sites);
                return Ok(TMPrepareResult::DMPrepareError(sites_prepare_result));
            }
        }
        let sites_commit_result = self.sites_commit(tid, generated_objs, &data_sites)?;
        match sites_commit_result {
            DMCommitResult::Success => {},
            _ => {
                self.sites_abort(tid, generated_objs, &data_sites);
                return Ok(TMPrepareResult::DMCommitError(sites_commit_result))
            }
        }
        return Ok(TMPrepareResult::Success);
    }
    fn commit(&self, tid: &TransactionId) -> Result<TMCommitResult, TMError> {
        let mut txn = self.get_transaction(tid)?;
        let changed_objs = &txn.changed_objects;
        let data_sites = self.data_sites(changed_objs)?;
        let sites_end_result = self.sites_end(tid, changed_objs, &data_sites)?;
        match sites_end_result {
            EndResult::CheckFailed(ce) => {return Ok(TMCommitResult::CheckFailed(ce))},
            _ => {return Ok(TMCommitResult::Success)}
        }
    }
    fn abort(&self, tid: &TransactionId) -> Result<AbortResult, TMError> {
        let mut txn = self.get_transaction(tid)?;
        let changed_objs = &txn.changed_objects;
        let data_sites = self.data_sites(changed_objs)?;
        self.sites_abort(tid, changed_objs, &data_sites)
    }
    fn go_ahead(&self, tids: &BTreeSet<TransactionId>, server_id: &u64) -> Result<(), ()> {
        for tid in tids {
            let await_txn = self.await_manager.get_txn(tid);
            AwaitManager::txn_send(&await_txn, *server_id);
        }
        Ok(())
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
    pub fn server_from_txn_awaits(awaits: &TxnAwaits, server_id: u64) -> Arc<AwaitingServer> {
        awaits.lock()
            .entry(server_id)
            .or_insert_with(|| Arc::new(AwaitingServer::new()))
            .clone()
    }
    pub fn txn_send(awaits: &TxnAwaits, server_id: u64) {
        AwaitManager::server_from_txn_awaits(awaits, server_id).send();
    }
    pub fn txn_wait(awaits: &TxnAwaits, server_id: u64) {
        AwaitManager::server_from_txn_awaits(awaits, server_id).wait();
    }
}