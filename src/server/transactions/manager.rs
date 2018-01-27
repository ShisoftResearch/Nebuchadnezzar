use bifrost::vector_clock::{StandardVectorClock};
use bifrost::utils::time::get_time;
use chashmap::CHashMap;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::DerefMut;
use ram::types::{Id, Value};
use ram::cell::{Cell, ReadError, WriteError};
use server::NebServer;
use bifrost::utils::async_locks::{Mutex, MutexGuard, AsyncMutexGuard, RwLock};
use futures::sync::mpsc::{channel, Sender};
use futures::{Sink};
use futures::prelude::*;
use super::*;
use utils::stream::PollableStream;

type TxnAwaits = Arc<Mutex<HashMap<u64, Arc<AwaitingServer>>>>;
type TxnMutex = Arc<Mutex<Transaction>>;
type TxnGuard = MutexGuard<Transaction>;
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
    rpc begin() -> TxnId | TMError;
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
    inner: Arc<TransactionManagerInner>
}
dispatch_rpc_service_functions!(TransactionManager);

pub struct TransactionManagerInner {
    server: Arc<NebServer>,
    transactions: RwLock<HashMap<TxnId, TxnMutex>>,
    data_sites: CHashMap<u64, Arc<data_site::AsyncServiceClient>>,
    await_manager: AwaitManager
}

impl TransactionManager {
    pub fn new(server: &Arc<NebServer>) -> Arc<TransactionManager> {
        Arc::new(TransactionManager {
            inner: Arc::new(TransactionManagerInner {
                server: server.clone(),
                transactions: RwLock::new(HashMap::new()),
                data_sites: CHashMap::new(),
                await_manager: AwaitManager::new()
            })
        })
    }
}

impl Service for TransactionManager {
    fn begin(&self) -> Box<Future<Item = TxnId, Error = TMError>> {
        box future::result(self.inner.begin())
    }
    fn read(&self, tid: TxnId, id: Id) -> Box<Future<Item = TxnExecResult<Cell, ReadError>, Error = TMError>> {
        box TransactionManagerInner::read(self.inner.clone(), tid, id)
    }
    fn read_selected(&self, tid: TxnId, id: Id, fields: Vec<u64>)
                     -> Box<Future<Item = TxnExecResult<Vec<Value>, ReadError>, Error = TMError>>
    {
        box TransactionManagerInner::read_selected(self.inner.clone(), tid, id, fields)
    }
    fn write(&self, tid: TxnId, cell: Cell) -> Box<Future<Item = TxnExecResult<(), WriteError>, Error = TMError>> {
        box future::result(self.inner.write(tid, cell))
    }
    fn update(&self, tid: TxnId, cell: Cell) -> Box<Future<Item = TxnExecResult<(), WriteError>, Error = TMError>> {
        box future::result(self.inner.update(tid, cell))
    }
    fn remove(&self, tid: TxnId, id: Id) -> Box<Future<Item = TxnExecResult<(), WriteError>, Error = TMError>> {
        box future::result(self.inner.remove(tid, id))
    }
    fn prepare(&self, tid: TxnId) -> Box<Future<Item = TMPrepareResult, Error = TMError>> {
        box TransactionManagerInner::prepare(self.inner.clone(), tid)
    }
    fn commit(&self, tid: TxnId) -> Box<Future<Item = EndResult, Error = TMError>> {
        box TransactionManagerInner::commit(self.inner.clone(), tid)
    }
    fn abort(&self, tid: TxnId) -> Box<Future<Item = AbortResult, Error = TMError>> {
        box TransactionManagerInner::abort(self.inner.clone(), tid)
    }
    fn go_ahead(&self, tids: BTreeSet<TxnId>, server_id: u64) -> Box<Future<Item = (), Error = ()>> {
        box TransactionManagerInner::go_ahead(self.inner.clone(), tids, server_id)
    }
}

impl TransactionManagerInner {
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
        let txns = self.transactions.read();
        match txns.get(tid) {
            Some(txn) => Ok(txn.clone()),
            _ => {
                Err(TMError::TransactionNotFound)
            }
        }
    }
    fn read_from_site(
        this: Arc<Self>, server_id: u64, server: Arc<data_site::AsyncServiceClient>,
        tid: TxnId, id: Id, txn: Arc<TxnGuard>, await: TxnAwaits
    ) -> impl Future<Item = TxnExecResult<Cell, ReadError>, Error = TMError> {
        let self_server_id = this.server.server_id;
        let this_clone = this.clone();
        let txn_clone = txn.clone();
        server
            .read(
                &self_server_id,
                &this.get_clock(),
                &tid, &id
            )
            .then(move |read_response|
                match read_response {
                    Ok(dsr) => {
                        let dsr = dsr.unwrap();
                        this.merge_clock(&dsr.clock);
                        let payload = dsr.payload;
                        match payload {
                            TxnExecResult::Accepted(ref cell) => {
                                txn.mutate().data.insert(id, DataObject
                                    {
                                        server: server_id,
                                        version: Some(cell.header.version),
                                        cell: Some(cell.clone()),
                                        new: false,
                                        changed: false,
                                    });
                            },
                            TxnExecResult::Wait => {
                                return future::err(())
                            }
                            _ => {}
                        }
                        future::ok(Ok(payload))
                    },
                    Err(e) => {
                        error!("{:?}", e);
                        future::ok(Err(TMError::RPCErrorFromCellServer))
                    }
                })
            .or_else(move |_|
                AwaitManager::txn_wait(&await, server_id)
                    .then(move |_|
                        Self::read_from_site(this_clone, server_id, server, tid, id, txn_clone, await)
                            .then(|r|
                                future::ok(r))))
            .then(|r: Result<_, ()>|
                future::result(r.unwrap()))
    }
    fn read_selected_from_site(
        this: Arc<Self>, server_id: u64, server: Arc<data_site::AsyncServiceClient>,
        tid: TxnId, id: Id, fields: Vec<u64>, txn: Arc<TxnGuard>, await: TxnAwaits
    ) -> impl Future<Item = TxnExecResult<Vec<Value>, ReadError>, Error = TMError> {
        let self_server_id = this.server.server_id;
        let this_clone = this.clone();
        server
            .read_selected(
                &self_server_id,
                &this.get_clock(),
                &tid, &id, &fields
            ).then(move |read_response|
                match read_response {
                    Ok(dsr) => {
                        let dsr = dsr.unwrap();
                        this.merge_clock(&dsr.clock);
                        let payload = dsr.payload;
                        match payload {
                            TxnExecResult::Wait => {
                                return future::err(())
                            }
                            _ => {}
                        }
                        future::ok(Ok(payload))
                    },
                    Err(e) => {
                        error!("{:?}", e);
                        future::ok(Err(TMError::RPCErrorFromCellServer))
                    }
                })
            .or_else(move |_| {
                AwaitManager::txn_wait(&await, server_id)
                    .then(move |_|
                        Self::read_selected_from_site(this_clone, server_id, server, tid, id, fields, txn, await)
                            .then(|r| future::ok(r)))})
            .then(|r: Result<_, ()>|
                future::result(r.unwrap()))
    }
    fn generate_affected_objs(txn: &mut TxnGuard) {
        let mut affected_objs = AffectedObjs::new();
        for (id, data_obj) in &txn.data {
            affected_objs
                .entry(data_obj.server)
                .or_insert_with(|| BTreeMap::new())
                .insert(*id, data_obj.clone());
        }
        txn.data.clear(); // clean up data after transferred to changed
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
    ) -> impl Future<Item = DMPrepareResult, Error = TMError> {
        let self_server_id = server.server_id;
        let cell_ids: Vec<_> = objs.iter().map(|(id, _)| *id).collect();
        let server_for_clock = server.clone();
        data_site
            .prepare(&self_server_id, &server.txn_peer.clock.to_clock(), &tid, &cell_ids)
            .map_err(|_| -> TMError {
                TMError::RPCErrorFromCellServer
            })
            .map(move |prepare_res| -> DMPrepareResult {
                let prepare_res = prepare_res.unwrap();
                server_for_clock.txn_peer.clock.merge_with(&prepare_res.clock);
                prepare_res.payload
            })
            .then(|prepare_payload|
                match prepare_payload {
                    Ok(payload) => {
                        match payload {
                            DMPrepareResult::Wait => {
                                return future::err(())
                            },
                            _ => {
                                return future::ok(Ok(payload))
                            }
                        }
                    },
                    Err(e) => future::ok(Err(e))
                })
            .or_else(|_|
                AwaitManager::txn_wait(&awaits, data_site.server_id)
                    .then(|_|
                        TransactionManagerInner::site_prepare(server, awaits, tid, objs, data_site)
                            .then(|r| future::ok(r))))// after waiting, retry
            .then(|r: Result<_, ()>|
                future::result(r.unwrap()))
    }
    fn sites_prepare(this: Arc<Self>, tid: TxnId, affected_objs: AffectedObjs, data_sites: DataSiteClients)
        -> impl Future<Item = DMPrepareResult, Error = TMError>
    {
        let prepare_futures: Vec<_> = affected_objs.into_iter().map(|(server, objs)| {
            let data_site = data_sites.get(&server).unwrap().clone();
            TransactionManagerInner::site_prepare(
                this.server.clone(),
                this.await_manager.get_txn(&tid),
                tid.clone(), objs, data_site
            )
        }).collect();
        future::join_all(prepare_futures)
            .map(|prepare_results| {
                for result in prepare_results {
                    match result {
                        DMPrepareResult::Success => {},
                        _ => {return result}
                    }
                }
                DMPrepareResult::Success
            })
    }
    fn sites_commit(this: Arc<Self>, tid: TxnId, changed_objs: AffectedObjs, data_sites: DataSiteClients)
        -> impl Future<Item = DMCommitResult, Error = TMError>
    {
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
            data_site.commit(&this.get_clock(), &tid, &ops)
        }).collect();
        future::join_all(commit_futures)
            .then(move |commit_results| {
                if let Ok(commit_results) = commit_results {
                    for result in commit_results {
                        if let Ok(dsr) = result {
                            this.merge_clock(&dsr.clock);
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
            })
    }
    fn sites_abort(this: Arc<Self>, tid: TxnId, changed_objs: AffectedObjs, data_sites: DataSiteClients)
        -> impl Future<Item = AbortResult, Error = TMError>
    {
        let abort_futures: Vec<_> = changed_objs.iter().map(|(ref server_id, _)| {
            let data_site = data_sites.get(server_id).unwrap().clone();
            data_site.abort(&this.get_clock(), &tid)
        }).collect();
        let this_clone = this.clone();
        future::join_all(abort_futures)
            .map_err(|_| Err(TMError::RPCErrorFromCellServer))
            .and_then(move |abort_results| {
                let mut rollback_failures = Vec::new();
                for result in abort_results {
                    match result {
                        Ok(asr) => {
                            let payload = asr.payload;
                            this.merge_clock(&asr.clock);
                            match payload {
                                AbortResult::Success(failures) => {
                                    if let Some(mut failures) = failures {
                                        rollback_failures.append(&mut failures);
                                    }
                                },
                                _ => return Err(Ok(payload))
                            }
                        },
                        Err(_) => {return Err(Err(TMError::AssertionError))}
                    }
                }
                return Ok(rollback_failures)
            })
            .and_then(|rollback_failures|
                Self::sites_end(this_clone, tid, changed_objs, data_sites)
                    .map_err(|e| Err(e))
                    .and_then(|_: EndResult| {
                        Ok(AbortResult::Success(
                            if rollback_failures.is_empty()
                                {None} else {Some(rollback_failures)}
                        ))
                    }))
            .then(|results|{
                match results {
                    Err(Err(e)) => Err(e),
                    Err(Ok(r)) => Ok(r),
                    Ok(r) => Ok(r)
                }
            })

    }
    fn sites_end(this: Arc<Self>, tid: TxnId, changed_objs: AffectedObjs, data_sites: DataSiteClients)
        -> impl Future<Item = EndResult, Error = TMError>
    {
        let end_futures: Vec<_> = changed_objs.iter().map(|(ref server_id, _)| {
            let data_site = data_sites.get(server_id).unwrap().clone();
            data_site.end(&this.get_clock(), &tid)
        }).collect();
        future::join_all(end_futures)
            .then(move |end_results| {
                if end_results.is_err() {return Err(TMError::RPCErrorFromCellServer)}
                let end_results = end_results.unwrap();
                for result in end_results {
                    if let Ok(result) = result {
                        this.merge_clock(&result.clock);
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
            })
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
    fn cleanup_transaction(&self, tid: &TxnId) -> Result<(), ()> {
        let mut txn = self.transactions.write();
        txn.remove(tid);
        return Ok(());
    }
    ////////////////////////////
    // STARTING IMPL RPC CALLS//
    ////////////////////////////
    fn begin(&self) -> Result<TxnId, TMError> {
        let id = self.server.txn_peer.clock.inc();
        let mut txns = self.transactions.write();
        if txns.insert(id.clone(), Arc::new(Mutex::new(Transaction {
            start_time: get_time(),
            data: HashMap::new(),
            affected_objects: AffectedObjs::new(),
            state: TxnState::Started
        }))).is_some() {
            Err(TMError::TransactionIdExisted)
        } else {
            Ok(id)
        }
    }
    fn read(this: Arc<Self>, tid: TxnId, id: Id)
        -> impl Future<Item = TxnExecResult<Cell, ReadError>, Error = TMError>
    {
        let this_clone1 = this.clone();
        let this_clone2 = this.clone();
        future::result(this.get_transaction(&tid))
            .and_then(move |txn_mutex| {
                let txn = txn_mutex.lock();
                this.ensure_rw_state(&txn)
                    .map(|_| txn)
            })
            .and_then(move |txn| {
                future::result(txn.data.get(&id)
                    .map(|data_obj| {
                        // try read from cache
                        match data_obj.cell {
                            Some(ref cell) => TxnExecResult::Accepted(cell.clone()),
                            None => TxnExecResult::Error(ReadError::CellDoesNotExisted)
                        }
                    })
                    .ok_or(()))
                    .or_else(move |_| {
                        // not found, fetch from data site
                        future::result(this_clone1.get_data_site_by_id(&id))
                            .map_err(|e| {
                                error!("{:?}", e);
                                return TMError::CannotLocateCellServer
                            })
                            .and_then(move |(server_id, server)| {
                                let txn = Arc::new(txn);
                                let await = this_clone2.await_manager.get_txn(&tid);
                                Self::read_from_site(this_clone2, server_id, server, tid, id, txn, await)
                            })
                    })
            })
    }
    fn read_selected(this: Arc<Self>, tid: TxnId, id: Id, fields: Vec<u64>)
        -> impl Future<Item = TxnExecResult<Vec<Value>, ReadError>, Error = TMError>
    {
        let this_clone1 = this.clone();
        let fields_clone = fields.clone();
        future::result(this.get_transaction(&tid))
            .and_then(move |txn_mutex| {
                let txn = txn_mutex.lock();
                this.ensure_rw_state(&txn)
                    .map(|_| txn)
            })
            .and_then(move |txn| {
                future::result(txn.data.get(&id)
                    .map(|data_obj| {
                        // try read from cache
                        match data_obj.cell {
                            Some(ref cell) => {
                                let mut result = Vec::with_capacity(fields.len());
                                match cell.data {
                                    Value::Map(ref map) => {
                                        for field in fields {
                                            result.push(map.get_by_key_id(field).clone())
                                        }
                                    },
                                    _ => return TxnExecResult::Error(ReadError::CellTypeIsNotMapForSelect)
                                }
                                TxnExecResult::Accepted(result)
                            },
                            None => TxnExecResult::Error(ReadError::CellDoesNotExisted)
                        }
                    })
                    .ok_or(()))
                    .or_else(move |_| {
                        future::result(this_clone1.get_data_site_by_id(&id))
                            .map_err(|e| {
                                TMError::CannotLocateCellServer
                            })
                            .and_then(move |(server_id, server)| {
                                let await = this_clone1.await_manager.get_txn(&tid);
                                let txn = Arc::new(txn);
                                Self::read_selected_from_site(this_clone1, server_id, server, tid, id, fields_clone, txn, await)
                            })
                    })
            })
    }
    fn write(&self, tid: TxnId, cell: Cell) -> Result<TxnExecResult<(), WriteError>, TMError> {
        let txn_mutex = self.get_transaction(&tid)?;
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
    fn update(&self, tid: TxnId, cell: Cell) -> Result<TxnExecResult<(), WriteError>, TMError> {
        let txn_mutex = self.get_transaction(&tid)?;
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
    fn remove(&self, tid: TxnId, id: Id) -> Result<TxnExecResult<(), WriteError>, TMError> {
        let txn_lock = self.get_transaction(&tid)?;
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
                    txn.data.insert(id, DataObject {
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
    fn prepare(this: Arc<Self>, tid: TxnId)
        -> impl Future<Item = TMPrepareResult, Error = TMError>
    {
        let this_clone1 = this.clone();
        let this_clone2 = this.clone();
        let this_clone3 = this.clone();
        let tid_clone = tid.clone();
        future::result(this.get_transaction(&tid))
            .map(|txn_mutex| txn_mutex.lock())
            .and_then(move |txn| {
                this.ensure_rw_state(&txn)
                    .map(|_| txn)
            })
            .and_then(move |mut txn| {
                Self::generate_affected_objs(&mut txn);
                let affect_objs = txn.affected_objects.clone();
                this_clone1.data_sites(&affect_objs)
                    .map(|data_sites| {
                        (data_sites, affect_objs, txn)
                    })
            })
            .and_then(move |(data_sites, affect_objs, txn)| {
                Self::sites_prepare(this_clone2, tid, affect_objs.clone(), data_sites.clone())
                    .map(|sites_prepare_result|
                        (sites_prepare_result, data_sites, affect_objs, txn))
            })
            .and_then(move |(sites_prepare_result, data_sites, affect_objs, mut txn)| {
                future::result(
                    if sites_prepare_result == DMPrepareResult::Success {
                        Err(())
                    } else {
                        Ok(TMPrepareResult::DMPrepareError(sites_prepare_result))
                    }
                )
                .or_else(move |_|
                    Self::sites_commit(this_clone3, tid_clone, affect_objs, data_sites)
                        .map(move |sites_commit_result| {
                            match sites_commit_result {
                                DMCommitResult::Success => {
                                    txn.state = TxnState::Prepared;
                                    TMPrepareResult::Success
                                },
                                _ => {
                                    TMPrepareResult::DMCommitError(sites_commit_result)
                                }
                            }
                        }))
            })
    }
    fn commit(this: Arc<Self>, tid: TxnId)
        -> impl Future<Item = EndResult, Error = TMError>
    {
        let this_clone1 = this.clone();
        let this_clone2 = this.clone();
        let tid_clone = tid.clone();
        future::result(this.get_transaction(&tid))
            .and_then(move |txn_lock| {
                let txn = txn_lock.lock();
                this.ensure_txn_state(&txn, TxnState::Prepared)?;
                let affected_objs = txn.affected_objects.clone();
                let data_sites = this.data_sites(&affected_objs)?;
                Ok((affected_objs, data_sites, txn))
            })
            .and_then(move |(affected_objs, data_sites, txn)| {
                Self::sites_end(this_clone1, tid, affected_objs, data_sites)
            })
            .then(move |r| {
                this_clone2.cleanup_transaction(&tid_clone);
                return r;
            })
    }
    fn abort(this: Arc<Self>, tid: TxnId)
        -> impl Future<Item = AbortResult, Error = TMError>
    {
        let this_clone1 = this.clone();
        let tid_clone1 = tid.clone();
        debug!("TXN ABORT IN MGR {:?}", &tid);
        future::result(this.get_transaction(&tid))
            .and_then(|txn_lock| {
                Ok(txn_lock.lock())
            })
            .and_then(move |txn| {
                future::result(
                    if txn.state != TxnState::Aborted {
                        Err(txn)
                    } else {
                        Ok(AbortResult::Success(None))
                    }
                )
                .or_else(move |txn| {
                    let changed_objs = txn.affected_objects.clone();
                    debug!("ABORT AFFECTED OBJS: {:?}", changed_objs);
                    future::result(this.data_sites(&changed_objs))
                        .and_then(move |data_sites|
                            Self::sites_abort(this.clone(), tid.clone(), changed_objs, data_sites))
                })
            })
            .then(move |r| {
                this_clone1.cleanup_transaction(&tid_clone1);
                return r;
            })
    }
    fn go_ahead(this: Arc<Self>, tids: BTreeSet<TxnId>, server_id: u64)
        -> impl Future<Item = (), Error = ()>
    {
        debug!("=> TM WAKE UP TXN: {:?}", tids);
        let mut futures = Vec::new();
        for tid in tids {
            let await_txn = this.await_manager.get_txn(&tid);
            futures.push(AwaitManager::txn_send(&await_txn, server_id));
        }
        future::join_all(futures).map(|_| ())
    }
}



struct AwaitingServer {
    sender: Mutex<Sender<()>>,
    receiver: PollableStream<(), ()>,
}

impl AwaitingServer {
    pub fn new() -> AwaitingServer {
        let (sender, receiver) = channel(1);
        AwaitingServer {
            sender: Mutex::new(sender),
            receiver: PollableStream::from_stream(receiver)
        }
    }
    pub fn send(&self) -> impl Future<Item = (), Error = ()>
    {
        AwaitingServer::send_to_sender(self.sender.lock_async())
    }
    fn send_to_sender(sender: AsyncMutexGuard<Sender<()>>)
        -> impl Future<Item = (), Error = ()>
    {
        sender
            .map_err(|_| ())        
            .map(|s| s.clone())
            .and_then(|lock|
                lock.send(())
                    .map(|_| ())
                    .map_err(|_| ()))
    }
    pub fn wait(&self)
        -> impl Future<Item = (), Error = ()>
    {
        self.receiver.poll_future()
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
    pub fn txn_send(awaits: &TxnAwaits, server_id: u64)
        -> impl Future<Item = (), Error = ()>
    {
        AwaitManager::server_from_txn_awaits(awaits, server_id).send()
    }
    pub fn txn_wait(awaits: &TxnAwaits, server_id: u64)
        -> impl Future<Item = (), Error = ()>
    {
        AwaitManager::server_from_txn_awaits(awaits, server_id).wait()
    }
}