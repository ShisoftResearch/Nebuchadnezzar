use bifrost::vector_clock::{StandardVectorClock};
use bifrost::utils::time::get_time;
use chashmap::{CHashMap, WriteGuard};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use ram::types::{Id, Value};
use ram::cell::{Cell, ReadError, WriteError};
use server::NebServer;
use parking_lot::{Mutex, MutexGuard};
use std::sync::mpsc::{channel, Sender, Receiver};
use futures;
use super::*;

type TxnAwaits = Arc<Mutex<HashMap<u64, Arc<AwaitingServer>>>>;
type TxnMutex = Arc<Mutex<Transaction>>;
type TxnGuard<'a> = MutexGuard<'a, Transaction>;
type AffectedObjs = BTreeMap<u64, BTreeMap<Id, DataObject>>; // server_id as key
type DataSiteClients = HashMap<u64, Arc<data_site::AsyncServiceClient>>;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TXN_MANAGER_RPC_SERVICE) as u64;

#[derive(Clone, Debug)]
struct DataObject {
    server: u64,
    cell: Option<Cell>,
    version: Option<u64>,
    changed: bool,
    new: bool
}

struct Transaction {
    start_time: i64, // use for timeout detecting
    data: HashMap<Id, DataObject>,
    affected_objects: AffectedObjs,
    state: TxnState
}

service! {
    rpc begin() -> TxnId;
    rpc read(tid: TxnId, id: Id) -> TxnExecResult<Cell, ReadError> | TMError;
    rpc read_selected(tid: TxnId, id: Id, fields: Vec<u64>) -> TxnExecResult<Vec<Value>, ReadError> | TMError;
    rpc write(tid: TxnId, cell: Cell) -> TxnExecResult<(), WriteError> | TMError;
    rpc update(tid: TxnId, cell: Cell) -> TxnExecResult<(), WriteError> | TMError;
    rpc remove(tid: TxnId, id: Id) -> TxnExecResult<(), WriteError> | TMError;

    rpc prepare(tid: TxnId) -> TMPrepareResult | TMError;
    rpc commit(tid: TxnId) -> EndResult | TMError;
    rpc abort(tid: TxnId) -> AbortResult | TMError;

    rpc go_ahead(tids: BTreeSet<TxnId>, server_id: u64); // invoked by data site to continue on it's transaction in case of waiting
}

pub struct TransactionManager {
    server: Arc<NebServer>,
    transactions: CHashMap<TxnId, TxnMutex>,
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
                data_site::AsyncServiceClient::new(data_site::DEFAULT_SERVICE_ID, &client)
            }, |_| {});
        }
        Ok(self.data_sites.get(&server_id).unwrap().clone())
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
    fn get_transaction(&self, tid: &TxnId) -> Result<TxnMutex, TMError> {
        match self.transactions.get(tid) {
            Some(txn) => Ok(txn.clone()),
            _ => {Err(TMError::TransactionNotFound)}
        }
    }
    fn read_from_site(
        &self, server_id: u64, server: Arc<data_site::AsyncServiceClient>,
        tid: &TxnId, id: &Id, mut txn: TxnGuard, await: TxnAwaits
    ) -> Result<TxnExecResult<Cell, ReadError>, TMError> {
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
                let payload = dsr.payload;
                match payload {
                    TxnExecResult::Accepted(ref cell) => {
                        txn.data.insert(*id, DataObject {
                            server: server_id,
                            cell: Some(cell.clone()),
                            new: false,
                            version: Some(cell.header.version),
                            changed: false,
                        });
                    },
                    TxnExecResult::Wait => {
                        AwaitManager::txn_wait(&await, server_id);
                        return self.read_from_site(server_id, server, tid, id, txn, await);
                    }
                    _ => {}
                }
                Ok(payload)
            },
            Err(e) => {
                error!("{:?}", e);
                Err(TMError::RPCErrorFromCellServer)
            }
        }
    }
    fn read_selected_from_site(
        &self, server_id: u64, server: Arc<data_site::AsyncServiceClient>,
        tid: &TxnId, id: &Id, fields: &Vec<u64>, mut txn: TxnGuard, await: TxnAwaits
    ) -> Result<TxnExecResult<Vec<Value>, ReadError>, TMError> {
        let self_server_id = self.server.server_id;
        let read_response = server.read_selected(
            &self_server_id,
            &self.get_clock(),
            tid, id, fields
        ).wait();
        match read_response {
            Ok(dsr) => {
                let dsr = dsr.unwrap();
                self.merge_clock(&dsr.clock);
                let payload = dsr.payload;
                match payload {
                    TxnExecResult::Wait => {
                        AwaitManager::txn_wait(&await, server_id);
                        return self.read_selected_from_site(server_id, server, tid, id, fields, txn, await);
                    }
                    _ => {}
                }
                Ok(payload)
            },
            Err(e) => {
                error!("{:?}", e);
                Err(TMError::RPCErrorFromCellServer)
            }
        }
    }
    fn generate_affected_objs(&self, txn: &mut TxnGuard) {
        let mut affected_objs = AffectedObjs::new();
        for (id, data_obj) in &txn.data {
            affected_objs.entry(data_obj.server).or_insert_with(|| BTreeMap::new()).insert(*id, data_obj.clone());
        }
        txn.data.clear(); // clean up data after transfered to changed/
        txn.affected_objects = affected_objs;
    }
    fn data_sites(&self, changed_objs: &AffectedObjs) -> Result<DataSiteClients, TMError>  {
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
        server: Arc<NebServer>, awaits: TxnAwaits, tid: TxnId, objs: BTreeMap<Id, DataObject>,
        data_site: Arc<data_site::AsyncServiceClient>
    ) -> Box<futures::Future<Item =DMPrepareResult, Error = TMError>> {
        let self_server_id = server.server_id;
        let cell_ids: Vec<_> = objs.iter().map(|(id, _)| *id).collect();
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
    fn sites_prepare(&self, tid: &TxnId, affected_objs: AffectedObjs, data_sites: DataSiteClients)
                     -> Result<DMPrepareResult, TMError> {
        let prepare_futures: Vec<_> = affected_objs.into_iter().map(|(server, objs)| {
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
    fn sites_commit(&self, tid: &TxnId, changed_objs: &AffectedObjs, data_sites: &DataSiteClients)
                    -> Result<DMCommitResult, TMError> {
        let commit_futures: Vec<_> = changed_objs.iter().map(|(ref server_id, ref objs)| {
            let data_site = data_sites.get(server_id).unwrap().clone();
            let ops: Vec<CommitOp> = objs.iter()
                .map(|(cell_id, data_obj)| {
                if data_obj.version.is_some() && !data_obj.changed {
                    CommitOp::Read(*cell_id,data_obj.version.unwrap())
                } else if data_obj.cell.is_none() && !data_obj.new {
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
    fn sites_abort(&self, tid: &TxnId, changed_objs: &AffectedObjs, data_sites: &DataSiteClients)
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
                        AbortResult::Success(failures) => {
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
        self.sites_end(tid, changed_objs, &data_sites);
        Ok(AbortResult::Success(
            if rollback_failures.is_empty()
                {None} else {Some(rollback_failures)}
        ))
    }
    fn sites_end(&self, tid: &TxnId, changed_objs: &AffectedObjs, data_sites: &DataSiteClients)
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
    fn ensure_txn_state(&self, txn: &TxnGuard, state: TxnState) -> Result<(), TMError> {
        if txn.state == state {
            return Ok(())
        } else {
            return Err(TMError::InvalidTransactionState(txn.state))
        }
    }
    fn ensure_rw_state(&self, txn: &TxnGuard) -> Result<(), TMError> {
        self.ensure_txn_state(txn, TxnState::Started)
    }
    fn cleanup_transaction(&self, tid: &TxnId) {
        self.transactions.remove(tid);
    }
}

impl Service for TransactionManager {
    fn begin(&self) -> Result<TxnId, ()> {
        let id = self.server.txn_peer.clock.inc();
        self.transactions.insert(id.clone(), Arc::new(Mutex::new(Transaction {
            start_time: get_time(),
            data: HashMap::new(),
            affected_objects: AffectedObjs::new(),
            state: TxnState::Started
        })));
        Ok(id)
    }
    fn read(&self, tid: &TxnId, id: &Id) -> Result<TxnExecResult<Cell, ReadError>, TMError> {
        let txn_mutex = self.get_transaction(tid)?;
        let txn = txn_mutex.lock();
        self.ensure_rw_state(&txn)?;
        if let Some(data_obj) = txn.data.get(id) {
            match data_obj.cell {
                Some(ref cell) => return Ok(TxnExecResult::Accepted(cell.clone())), // read from cache
                None => return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))
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
    fn read_selected(&self, tid: &TxnId, id: &Id, fields: &Vec<u64>) -> Result<TxnExecResult<Vec<Value>, ReadError>, TMError> {
        let txn_mutex = self.get_transaction(tid)?;
        let txn = txn_mutex.lock();
        self.ensure_rw_state(&txn)?;
        if let Some(data_obj) = txn.data.get(id) {
            match data_obj.cell {
                Some(ref cell) => {
                    let mut result = Vec::with_capacity(fields.len());
                    match cell.data {
                        Value::Map(ref map) => {
                            for field in fields {
                                result.push(map.get_by_key_id(*field).clone())
                            }
                        },
                        _ => return Ok(TxnExecResult::Error(ReadError::CellTypeIsNotMapForSelect))
                    }
                    return Ok(TxnExecResult::Accepted(result))
                }, // read from cache
                None => return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))
            }
        }
        let server = self.get_data_site_by_id(&id);
        match server {
            Ok((server_id, server)) => {
                let await = self.await_manager.get_txn(tid);
                self.read_selected_from_site(server_id, server, tid, id, fields, txn, await)
            },
            Err(e) => {
                error!("{:?}", e);
                Err(TMError::CannotLocateCellServer)
            }
        }
    }
    fn write(&self, tid: &TxnId, cell: &Cell) -> Result<TxnExecResult<(), WriteError>, TMError> {
        let txn_mutex = self.get_transaction(tid)?;
        let mut txn = txn_mutex.lock();
        let id = cell.id();
        self.ensure_rw_state(&txn)?;
        match self.server.get_server_id_by_id(&id) {
            Some(server_id) => {
                let have_cached_cell = txn.data.contains_key(&id);
                if !have_cached_cell {
                    txn.data.insert(id, DataObject {
                        server: server_id,
                        cell: Some(cell.clone()),
                        new: true,
                        version: None,
                        changed: true,
                    });
                    Ok(TxnExecResult::Accepted(()))
                } else {
                    let mut data_obj = txn.data.get_mut(&id).unwrap();
                    if !data_obj.cell.is_none() {
                        return Ok(TxnExecResult::Error(WriteError::CellAlreadyExisted))
                    }
                    data_obj.cell = Some(cell.clone());
                    data_obj.changed = true;
                    Ok(TxnExecResult::Accepted(()))
                }
            },
            None => Err(TMError::CannotLocateCellServer)
        }
    }
    fn update(&self, tid: &TxnId, cell: &Cell) -> Result<TxnExecResult<(), WriteError>, TMError> {
        let txn_mutex = self.get_transaction(tid)?;
        let mut txn = txn_mutex.lock();
        let id = cell.id();
        self.ensure_rw_state(&txn)?;
        match self.server.get_server_id_by_id(&id) {
            Some(server_id) => {
                let cell = cell.clone();
                if txn.data.contains_key(&id) {
                    let mut data_obj = txn.data.get_mut(&id).unwrap();
                    data_obj.cell = Some(cell);
                    data_obj.changed = true
                } else {
                    txn.data.insert(id, DataObject {
                        server: server_id,
                        cell: Some(cell),
                        new: false,
                        version: None,
                        changed: true,
                    });
                }
                Ok(TxnExecResult::Accepted(()))
            },
            None => Err(TMError::CannotLocateCellServer)
        }
    }
    fn remove(&self, tid: &TxnId, id: &Id) -> Result<TxnExecResult<(), WriteError>, TMError> {
        let txn_lock = self.get_transaction(tid)?;
        let mut txn = txn_lock.lock();
        self.ensure_rw_state(&txn)?;
        match self.server.get_server_id_by_id(&id) {
            Some(server_id) => {
                if txn.data.contains_key(&id) {
                    let mut new_obj = false;
                    {
                        let data_obj = txn.data.get_mut(&id).unwrap();
                        if data_obj.cell.is_none() {
                            return Ok(TxnExecResult::Error(WriteError::CellDoesNotExisted))
                        }
                        if data_obj.new {
                            new_obj = true;
                        } else {
                            data_obj.cell = None;
                        }
                        data_obj.changed = true;
                    }
                    if new_obj {
                        txn.data.remove(&id);
                    }
                } else {
                    txn.data.insert(*id, DataObject {
                        server: server_id,
                        cell: None,
                        new: false,
                        version: None,
                        changed: true
                    });
                }
                Ok(TxnExecResult::Accepted(()))
            },
            None => Err(TMError::CannotLocateCellServer)
        }
    }
    fn prepare(&self, tid: &TxnId) -> Result<TMPrepareResult, TMError> {
        let conclusion = {
            let txn_mutex = self.get_transaction(tid)?;
            let mut txn = txn_mutex.lock();
            let result = {
                self.ensure_rw_state(&txn)?;
                self.generate_affected_objs(&mut txn);
                let generated_objs = &txn.affected_objects;
                let affect_objs = generated_objs.clone();
                let data_sites = self.data_sites(&affect_objs)?;
                let sites_prepare_result = self.sites_prepare(tid, affect_objs, data_sites.clone())?;
                if sites_prepare_result == DMPrepareResult::Success {
                    let sites_commit_result = self.sites_commit(tid, generated_objs, &data_sites)?;
                    match sites_commit_result {
                        DMCommitResult::Success => {
                            TMPrepareResult::Success
                        },
                        _ => {
                            self.sites_abort(tid, generated_objs, &data_sites);
                            TMPrepareResult::DMCommitError(sites_commit_result)
                        }
                    }
                } else {
                    let _ = self.sites_abort(tid, generated_objs, &data_sites);
                    TMPrepareResult::DMPrepareError(sites_prepare_result)
                }
            };
            match result {
                TMPrepareResult::Success => {
                    txn.state = TxnState::Prepared;
                },
                _ => {}
            }
            result
        };
        match conclusion {
            TMPrepareResult::Success => {},
            _ => {self.cleanup_transaction(tid);}
        }
        return Ok(conclusion);
    }
    fn commit(&self, tid: &TxnId) -> Result<EndResult, TMError> {
        let result = {
            let txn_lock = self.get_transaction(tid)?;
            let txn = txn_lock.lock();
            self.ensure_txn_state(&txn, TxnState::Prepared)?;
            let affected_objs = &txn.affected_objects;
            let data_sites = self.data_sites(affected_objs)?;
            self.sites_end(tid, affected_objs, &data_sites)
        };
        self.cleanup_transaction(tid);
        return result;
    }
    fn abort(&self, tid: &TxnId) -> Result<AbortResult, TMError> {
        let result = {
            let txn_lock = self.get_transaction(tid)?; // cause of deadlock with L364(begin)?
            let txn = txn_lock.lock();
            if txn.state != TxnState::Aborted {
                let changed_objs = &txn.affected_objects;
                let data_sites = self.data_sites(changed_objs)?;
                self.sites_abort(tid, changed_objs, &data_sites) // with end
            } else {
                Err(TMError::InvalidTransactionState(txn.state))
            }
        };
        self.cleanup_transaction(tid);
        return result;
    }
    fn go_ahead(&self, tids: &BTreeSet<TxnId>, server_id: &u64) -> Result<(), ()> {
        debug!("=> TM WAKE UP TXN: {:?}", tids);
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
        let _ = self.sender.lock().send(());
    }
    pub fn wait(&self) {
        let _ = self.receiver.lock().recv();
    }
}

struct AwaitManager {
    channels: Mutex<HashMap<TxnId, TxnAwaits>>
}

impl AwaitManager {
    pub fn new() -> AwaitManager {
        AwaitManager {
            channels: Mutex::new(HashMap::new())
        }
    }
    pub fn get_txn(&self, tid: &TxnId) -> TxnAwaits {
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