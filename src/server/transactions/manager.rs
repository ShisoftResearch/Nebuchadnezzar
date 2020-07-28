use super::*;
use bifrost::utils::time::get_time;
use bifrost::vector_clock::StandardVectorClock;
use bifrost_plugins::hash_ident;
use crate::ram::cell::CellHeader;
use crate::ram::cell::{Cell, ReadError, WriteError};
use crate::ram::types::{Id, Value};
use crate::server::NebServer;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use lightning::map::{ObjectMap, HashMap as LFMap, Map};
// Use async mutex because this module is a distributed coordinator
use async_std::sync::{Mutex, MutexGuard};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc::{channel, Sender, Receiver};

type TxnAwaits = ObjectMap<Arc<Mutex<AwaitingServer>>>;
type TxnMutex = Arc<Mutex<Transaction>>;
type TxnGuard<'a> = MutexGuard<'a, Transaction>;
type AffectedObjs = BTreeMap<u64, BTreeMap<Id, DataObject>>; // server_id as key
type DataSiteClients = ObjectMap<Arc<data_site::AsyncServiceClient>>;
type DataSitesMap = HashMap<u64, Arc<data_site::AsyncServiceClient>>;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TXN_MANAGER_RPC_SERVICE) as u64;

#[derive(Clone, Debug)]
struct DataObject {
    server: u64,
    cell: Option<Cell>,
    version: Option<u64>,
    changed: bool,
    new: bool,
}

struct Transaction {
    start_time: i64, // use for timeout detecting
    data: HashMap<Id, DataObject>,
    affected_objects: AffectedObjs,
    state: TxnState,
}

service! {
    rpc begin() -> Result<TxnId, TMError>;
    rpc read(tid: TxnId, id: Id) -> Result<TxnExecResult<Cell, ReadError>, TMError>;
    rpc read_selected(tid: TxnId, id: Id, fields: Vec<u64>) -> Result<TxnExecResult<Vec<Value>, ReadError>, TMError>;
    rpc head(tid: TxnId, id: Id) -> Result<TxnExecResult<CellHeader, ReadError>, TMError>;
    rpc write(tid: TxnId, cell: Cell) -> Result<TxnExecResult<(), WriteError>, TMError>;
    rpc update(tid: TxnId, cell: Cell) -> Result<TxnExecResult<(), WriteError>, TMError>;
    rpc remove(tid: TxnId, id: Id) -> Result<TxnExecResult<(), WriteError>, TMError>;

    rpc prepare(tid: TxnId) -> Result<TMPrepareResult, TMError>;
    rpc commit(tid: TxnId) -> Result<EndResult, TMError>;
    rpc abort(tid: TxnId) -> Result<AbortResult, TMError>;

    rpc go_ahead(tids: BTreeSet<TxnId>, server_id: u64); // invoked by data site to continue on it's transaction in case of waiting
}

dispatch_rpc_service_functions!(TransactionManager);

pub struct TransactionManager {
    server: Arc<NebServer>,
    transactions: LFMap<TxnId, TxnMutex>,
    data_sites: ObjectMap<Arc<data_site::AsyncServiceClient>>,
    await_manager: AwaitManager,
}

impl TransactionManager {
    pub fn new(server: &Arc<NebServer>) -> Arc<TransactionManager> {
        Arc::new(Self {
                server: server.clone(),
                transactions: LFMap::with_capacity(128),
                data_sites: ObjectMap::with_capacity(8),
                await_manager: AwaitManager::new(),
            })
    }
}

impl Service for TransactionManager {
    ////////////////////////////
    // STARTING IMPL RPC CALLS//
    ////////////////////////////
    fn read(
        &self,
        tid: TxnId,
        id: Id,
    ) -> BoxFuture<Result<TxnExecResult<Cell, ReadError>, TMError>> {
        async move {
            let txn_mutex = self.get_transaction(&tid)?;
            let mut txn = txn_mutex.lock().await;
            self.ensure_rw_state(&txn)?;
            if let Some(data_obj) = txn.data.get(&id) {
                match data_obj.cell {
                    Some(ref cell) => return Ok(TxnExecResult::Accepted(cell.clone())), // read from cache
                    None => return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted)),
                }
            }
            match self.get_data_site_by_id(&id).await {
                Ok((server_id, server)) => {
                    let awaits = self.await_manager.get_txn(&tid);
                    self.read_from_site(server_id, &server, &tid, &id, &mut txn, &awaits).await
                }
                Err(e) => {
                    error!("{:?}", e);
                    Err(TMError::CannotLocateCellServer)
                }
            }
        }.boxed()
    }

    fn head(
        &self,
        tid: TxnId,
        id: Id,
    ) -> BoxFuture<Result<TxnExecResult<CellHeader, ReadError>, TMError>> {
        async move {
            let txn_mutex = self.get_transaction(&tid)?;
            let txn = txn_mutex.lock().await;
            self.ensure_rw_state(&txn)?;
            if let Some(data_obj) = txn.data.get(&id) {
                match data_obj.cell {
                    Some(ref cell) => return Ok(TxnExecResult::Accepted(cell.header.clone())),
                    None => return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted)),
                }
            }
            match self.get_data_site_by_id(&id).await {
                Ok((server_id, server)) => {
                    let awaits = self.await_manager.get_txn(&tid);
                    self.head_from_site(server_id, &server, &tid, &id, &txn, &awaits).await
                }
                Err(e) => {
                    error!("{:?}", e);
                    Err(TMError::CannotLocateCellServer)
                }
            }
        }.boxed()
    }
    fn read_selected(
        &self,
        tid: TxnId,
        id: Id,
        fields: Vec<u64>,
    ) -> BoxFuture<Result<TxnExecResult<Vec<Value>, ReadError>, TMError>> {
        async move {
            let txn_mutex = self.get_transaction(&tid)?;
            let txn = txn_mutex.lock().await;
            self.ensure_rw_state(&txn)?;
            if let Some(data_obj) = txn.data.get(&id) {
                match data_obj.cell {
                    Some(ref cell) => {
                        let mut result = Vec::with_capacity(fields.len());
                        match cell.data {
                            Value::Map(ref map) => {
                                for field in fields {
                                    result.push(map.get_by_key_id(field).clone())
                                }
                            }
                            _ => return Ok(TxnExecResult::Error(ReadError::CellTypeIsNotMapForSelect)),
                        }
                        return Ok(TxnExecResult::Accepted(result));
                    } // read from cache
                    None => return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted)),
                }
            }
            match self.get_data_site_by_id(&id).await {
                Ok((server_id, server)) => {
                    let awaits = self.await_manager.get_txn(&tid);
                    self.read_selected_from_site(
                        server_id, &server, &tid, &id, &fields, &txn, &awaits
                    ).await
                }
                Err(e) => {
                    error!("{:?}", e);
                    Err(TMError::CannotLocateCellServer)
                }
            }
        }.boxed()
    }

    fn prepare(&self, tid: TxnId) -> BoxFuture<Result<TMPrepareResult, TMError>> {
        async move {
            let conclusion = {
                let txn_mutex = self.get_transaction(&tid)?;
                let mut txn = txn_mutex.lock().await;
                let result = {
                    self.ensure_rw_state(&txn)?;
                    self.generate_affected_objs(&mut txn);
                    let affect_objs = &txn.affected_objects;
                    let data_sites = self.data_sites_for_objs(&affect_objs).await?;
                    let sites_prepare_result = self.sites_prepare(
                        &tid,
                        affect_objs,
                        &data_sites
                    ).await?;
                    if sites_prepare_result == DMPrepareResult::Success {
                        let sites_commit_result = self.sites_commit(
                            &tid,
                            affect_objs,
                            &data_sites
                        ).await?;
                        match sites_commit_result {
                            DMCommitResult::Success => TMPrepareResult::Success,
                            _ => TMPrepareResult::DMCommitError(sites_commit_result),
                        }
                    } else {
                        TMPrepareResult::DMPrepareError(sites_prepare_result)
                    }
                };
                match result {
                    TMPrepareResult::Success => {
                        txn.state = TxnState::Prepared;
                    }
                    _ => {}
                }
                result
            };
            return Ok(conclusion);
        }.boxed()
    }
    fn commit(&self, tid: TxnId) -> BoxFuture<Result<EndResult, TMError>> {
        async move {
            let result = {
                let txn_lock = self.get_transaction(&tid)?;
                let txn = txn_lock.lock().await;
                self.ensure_txn_state(&txn, TxnState::Prepared)?;
                let affected_objs = &txn.affected_objects;
                let data_sites = self.data_sites_for_objs(&affected_objs).await?;
                self.sites_end(
                    &tid,
                    affected_objs,
                    &data_sites
                ).await
            };
            self.cleanup_transaction(&tid);
            return result;
        }.boxed()
    }
    fn abort(&self, tid: TxnId) -> BoxFuture<Result<AbortResult, TMError>> {
        debug!("TXN ABORT IN MGR {:?}", &tid);
        async move {
            let result = {
                let txn_lock = self.get_transaction(&tid)?;
                let txn = txn_lock.lock().await;
                if txn.state != TxnState::Aborted {
                    let changed_objs = &txn.affected_objects;
                    let data_sites = self.data_sites_for_objs(&changed_objs).await?;
                    debug!("ABORT AFFECTED OBJS: {:?}", changed_objs);
                    self.sites_abort(
                        &tid,
                        changed_objs,
                        &data_sites
                    ).await // with end
                } else {
                    Ok(AbortResult::Success(None))
                }
            };
            self.cleanup_transaction(&tid);
            return result;
        }.boxed()
    }
    fn begin(&self) -> BoxFuture<Result<TxnId, TMError>> {
        let id = self.server.txn_peer.clock.inc();
        if self.transactions.insert(
                &id,
                Arc::new(Mutex::new(Transaction {
                    start_time: get_time(),
                    data: HashMap::new(),
                    affected_objects: AffectedObjs::new(),
                    state: TxnState::Started,
                })),
            )
            .is_some()
        {
            future::ready(Err(TMError::TransactionIdExisted)).boxed()
        } else {
            future::ready(Ok(id)).boxed()
        }
    }
    
    fn write(&self, tid: TxnId, cell: Cell) -> BoxFuture<Result<TxnExecResult<(), WriteError>, TMError>> {
        async move {
            let txn_mutex = self.get_transaction(&tid)?;
            let mut txn = txn_mutex.lock().await;
            let id = cell.id();
            self.ensure_rw_state(&txn)?;
            match self.server.get_server_id_by_id(&id) {
                Some(server_id) => {
                    let have_cached_cell = txn.data.contains_key(&id);
                    if !have_cached_cell {
                        txn.data.insert(
                            id,
                            DataObject {
                                server: server_id,
                                cell: Some(cell.clone()),
                                new: true,
                                version: None,
                                changed: true,
                            },
                        );
                        Ok(TxnExecResult::Accepted(()))
                    } else {
                        let mut data_obj = txn.data.get_mut(&id).unwrap();
                        if !data_obj.cell.is_none() {
                            return Ok(TxnExecResult::Error(WriteError::CellAlreadyExisted));
                        }
                        data_obj.cell = Some(cell.clone());
                        data_obj.changed = true;
                        Ok(TxnExecResult::Accepted(()))
                    }
                }
                None => Err(TMError::CannotLocateCellServer),
            }
        }.boxed()
    }
    fn update(&self, tid: TxnId, cell: Cell) -> BoxFuture<Result<TxnExecResult<(), WriteError>, TMError>> {
        async move {
            let txn_mutex = self.get_transaction(&tid)?;
            let mut txn = txn_mutex.lock().await;
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
                        txn.data.insert(
                            id,
                            DataObject {
                                server: server_id,
                                cell: Some(cell),
                                new: false,
                                version: None,
                                changed: true,
                            },
                        );
                    }
                    Ok(TxnExecResult::Accepted(()))
                }
                None => Err(TMError::CannotLocateCellServer),
            }
        }.boxed()
    }
    fn remove(&self, tid: TxnId, id: Id) -> BoxFuture<Result<TxnExecResult<(), WriteError>, TMError>> {
        async move {
            let txn_lock = self.get_transaction(&tid)?;
            let mut txn = txn_lock.lock().await;
            self.ensure_rw_state(&txn)?;
            match self.server.get_server_id_by_id(&id) {
                Some(server_id) => {
                    if txn.data.contains_key(&id) {
                        let mut new_obj = false;
                        {
                            let data_obj = txn.data.get_mut(&id).unwrap();
                            if data_obj.cell.is_none() {
                                return Ok(TxnExecResult::Error(WriteError::CellDoesNotExisted));
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
                        txn.data.insert(
                            id,
                            DataObject {
                                server: server_id,
                                cell: None,
                                new: false,
                                version: None,
                                changed: true,
                            },
                        );
                    }
                    Ok(TxnExecResult::Accepted(()))
                }
                None => Err(TMError::CannotLocateCellServer),
            }
        }.boxed()
    }
    fn go_ahead(&self, tids: BTreeSet<TxnId>, server_id: u64) -> BoxFuture<()> {
        debug!("=> TM WAKE UP TXN: {:?}", tids);
        let futures = FuturesUnordered::new();
        for tid in tids {
            let await_txn = self.await_manager.get_txn(&tid);
            futures.push(tokio::spawn(async move {
                AwaitManager::txn_send(&await_txn, server_id).await;
            }));
        }
        async move {
            let _: Vec<_> = futures.collect().await;
        }.boxed()
    }
}

impl TransactionManager {
    fn server_id(&self) -> u64 {
        self.server.server_id
    }
    async fn get_data_site(&self, server_id: u64) -> io::Result<Arc<data_site::AsyncServiceClient>> {
        let server_id_usize = server_id as usize;
        if !self.data_sites.contains(&server_id_usize) {
            let client = self.server.get_member_by_server_id(server_id).await?;
            return Ok(
                self.data_sites.get_or_insert(
                    &server_id_usize, 
                    || data_site::AsyncServiceClient::new(data_site::DEFAULT_SERVICE_ID, &client
                    )
                )
            )
        }
        Ok(self.data_sites.get(&server_id_usize).unwrap().clone())
    }
    async fn get_data_site_by_id(
        &self,
        id: &Id,
    ) -> io::Result<(u64, Arc<data_site::AsyncServiceClient>)> {
        match self.server.get_server_id_by_id(id) {
            Some(id) => match self.get_data_site(id).await {
                Ok(site) => Ok((id, site.clone())),
                Err(e) => Err(e),
            },
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "cannot find data site for this id",
            )),
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
            _ => Err(TMError::TransactionNotFound),
        }
    }
    async fn read_from_site<'a>(
        &self,
        server_id: u64,
        server: &Arc<data_site::AsyncServiceClient>,
        tid: &TxnId,
        id: &Id,
        txn: &mut TxnGuard<'a>,
        awaits: &TxnAwaits,
    ) -> Result<TxnExecResult<Cell, ReadError>, TMError> {
        let self_server_id = self.server.server_id;
        loop {
            let read_response =
            server.read(self_server_id, self.get_clock(), tid.to_owned(), id.clone()).await;
            match read_response {
                Ok(dsr) => {
                    self.merge_clock(&dsr.clock);
                    let payload = dsr.payload;
                    let payload_out = payload.clone();
                    match payload {
                        TxnExecResult::Accepted(cell) => {
                            txn.data.insert(
                                id.clone(),
                                DataObject {
                                    server: server_id,
                                    version: Some(cell.header.version),
                                    cell: Some(cell),
                                    new: false,
                                    changed: false,
                                },
                            );
                        }
                        TxnExecResult::Wait => {
                            AwaitManager::txn_wait(&awaits, server_id).await;
                            continue;
                        }
                        _ => {}
                    }
                    return Ok(payload_out)
                }
                Err(e) => {
                    error!("{:?}", e);
                    return Err(TMError::RPCErrorFromCellServer)
                }
            }
        }
    }
    
    async fn head_from_site<'a>(
        &self,
        server_id: u64,
        server: &Arc<data_site::AsyncServiceClient>,
        tid: &TxnId,
        id: &Id,
        txn: &TxnGuard<'a>,
        awaits: &TxnAwaits,
    ) -> Result<TxnExecResult<CellHeader, ReadError>, TMError> {
        let self_server_id = self.server.server_id;
        loop {
            let head_response = server.head(self_server_id, self.get_clock(), tid.to_owned(), *id).await;
            match head_response {
                Ok(dsr) => {
                    self.merge_clock(&dsr.clock);
                    let payload = &dsr.payload;
                    match &payload {
                        &TxnExecResult::Wait => {
                            AwaitManager::txn_wait(&awaits, server_id).await;
                            continue;
                        }
                        _ => {}
                    }
                    return Ok(dsr.payload);
                }
                Err(e) => {
                    error!("{:?}", e);
                    return Err(TMError::RPCErrorFromCellServer);
                }
            }
        }
    }
    async fn read_selected_from_site<'a>(
        &self,
        server_id: u64,
        server: &Arc<data_site::AsyncServiceClient>,
        tid: &TxnId,
        id: &Id,
        fields: &Vec<u64>,
        txn: &TxnGuard<'a>,
        awaits: &TxnAwaits,
    ) -> Result<TxnExecResult<Vec<Value>, ReadError>, TMError> {
        let self_server_id = self.server.server_id;
        loop {
            let read_response = server.read_selected(
                self_server_id,
                self.get_clock(),
                tid.to_owned(),
                id.clone(),
                fields.to_owned()
            ).await;
            match read_response {
                Ok(dsr) => {
                    self.merge_clock(&dsr.clock);
                    let payload = dsr.payload;
                    match payload {
                        TxnExecResult::Wait => {
                            AwaitManager::txn_wait(&awaits, server_id).await;
                            continue;
                        }
                        _ => {}
                    }
                    return Ok(payload);
                }
                Err(e) => {
                    error!("{:?}", e);
                    return Err(TMError::RPCErrorFromCellServer);
                }
            }
        }
    }
    fn generate_affected_objs(&self, txn: &mut TxnGuard) {
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
    async fn data_sites_for_objs(&self, changed_objs: &AffectedObjs) -> Result<DataSitesMap, TMError> {
        let mut data_sites = HashMap::new();
        for (server_id, _) in changed_objs {
            data_sites.insert(*server_id, self.get_data_site(*server_id).await);
        }
        if data_sites.iter().any(|(_, data_site)| data_site.is_err()) {
            return Err(TMError::CannotLocateCellServer);
        }
        Ok(data_sites.into_iter().map(|(id, client)| (id, client.unwrap())).collect())
    }
    async fn site_prepare(
        server: &Arc<NebServer>,
        awaits: &TxnAwaits,
        tid: &TxnId,
        objs: &BTreeMap<Id, DataObject>,
        data_site: &Arc<data_site::AsyncServiceClient>,
    ) -> Result<DMPrepareResult, TMError> {
        loop {
            let self_server_id = server.server_id;
            let cell_ids: Vec<_> = objs.iter().map(|(id, _)| *id).collect();
            let server_for_clock = server.clone();
            let prepare_payload = data_site
                .prepare(
                    self_server_id,
                    server.txn_peer.clock.to_clock(),
                    tid.clone(),
                    cell_ids
                )
                .await
                .map_err(|_| -> TMError { TMError::RPCErrorFromCellServer })
                .map(move |prepare_res| -> DMPrepareResult {
                    server_for_clock
                        .txn_peer
                        .clock
                        .merge_with(&prepare_res.clock);
                    prepare_res.payload
                });
            match prepare_payload {
                Ok(payload) => {
                    match payload {
                        DMPrepareResult::Wait => {
                            AwaitManager::txn_wait(&awaits, data_site.server_id()).await;
                            continue; // after waiting, retry
                        }
                        _ => return Ok(payload),
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
    
    async fn sites_prepare(
        &self,
        tid: &TxnId,
        affected_objs: &AffectedObjs,
        data_sites: &DataSitesMap,
    ) -> Result<DMPrepareResult, TMError> {
        let mut prepare_futures: FuturesUnordered<_> = affected_objs
            .into_iter()
            .map(|(server, objs)| {
                async move {
                    let data_site = data_sites.get(server).unwrap().clone();
                    let awaits = self.await_manager.get_txn(&tid);
                    TransactionManager::site_prepare(
                        &self.server,
                        &awaits,
                        &tid,
                        &objs,
                        &data_site,
                    ).await
                }
            })
            .collect();
        while let Some(result) = prepare_futures.next().await {
            match result {
                Ok(DMPrepareResult::Success) => {}
                Ok(res) => return Ok(res),
                Err(e) => return Err(e)
            }
        }
        Ok(DMPrepareResult::Success)
    }
    async fn sites_commit(
        &self,
        tid: &TxnId,
        changed_objs: &AffectedObjs,
        data_sites: &DataSitesMap,
    ) -> Result<DMCommitResult, TMError> {
        let this_clone = self.clone();
        let commit_futures: FuturesUnordered<_> = changed_objs
            .iter()
            .map(move |(ref server_id, ref objs)| {
                let data_site = data_sites.get(server_id).unwrap().clone();
                let ops: Vec<CommitOp> = objs
                    .iter()
                    .map(|(cell_id, data_obj)| {
                        if data_obj.version.is_some() && !data_obj.changed {
                            CommitOp::Read(*cell_id, data_obj.version.unwrap())
                        } else if data_obj.cell.is_none() && !data_obj.new {
                            CommitOp::Remove(*cell_id)
                        } else if data_obj.new {
                            CommitOp::Write(data_obj.cell.clone().unwrap())
                        } else if !data_obj.new {
                            CommitOp::Update(data_obj.cell.clone().unwrap())
                        } else {
                            CommitOp::None
                        }
                    })
                    .collect();
                async move {
                    data_site.commit(this_clone.get_clock(), tid.to_owned(), ops).await
                }
            })
            .collect();
        let commit_results: Vec<_> = commit_futures.collect().await;
        for result in commit_results {
            if let Ok(dsr) = result {
                self.merge_clock(&dsr.clock);
                match dsr.payload {
                    DMCommitResult::Success => {}
                    _ => {
                        return Ok(dsr.payload);
                    }
                }
            } else {
                return Err(TMError::RPCErrorFromCellServer);
            }
        }
        Ok(DMCommitResult::Success)
    }
    async fn sites_abort(
        &self,
        tid: &TxnId,
        changed_objs: &AffectedObjs,
        data_sites: &DataSitesMap,
    ) -> Result<AbortResult, TMError> {
        let abort_futures: FuturesUnordered<_> = changed_objs
            .iter()
            .map(|(ref server_id, _)| {
                let data_site = data_sites.get(*server_id).unwrap();
                async move {
                    data_site.abort(self.get_clock(), tid.clone()).await
                }
            })
            .collect();
        let abort_results: Vec<_> = abort_futures.collect().await;
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
                        }
                        _ => (return Ok(payload)),
                    }
                }
                Err(_) => return Err(TMError::AssertionError),
            }
        }
        self.sites_end(tid, changed_objs, data_sites).await?;
        Ok(AbortResult::Success(if rollback_failures.is_empty() {
            None
        } else {
            Some(rollback_failures)
        }))
    }
    async fn sites_end(
        &self,
        tid: &TxnId,
        changed_objs: &AffectedObjs,
        data_sites: &DataSitesMap,
    ) -> Result<EndResult, TMError> {
        let end_futures: FuturesUnordered<_> = changed_objs
            .iter()
            .map(|(ref server_id, _)| {
                let data_site = data_sites.get(*server_id).unwrap();
                async move {
                    data_site.end(self.get_clock(), tid.clone()).await
                }
            })
            .collect();
        let end_results: Vec<_> = end_futures.collect().await;
        for result in end_results {
            match result {
                Ok(result) => {
                    self.merge_clock(&result.clock);
                    let payload = result.payload;
                    match payload {
                        EndResult::Success => {}
                        _ => {
                            return Ok(payload);
                        }
                    }
                },
                Err(e) => {
                    debug!("Error on site end {:?}", e);
                    return Err(TMError::RPCErrorFromCellServer);
                }
            }
        }
        Ok(EndResult::Success)
    }
    fn ensure_txn_state(&self, txn: &TxnGuard, state: TxnState) -> Result<(), TMError> {
        if txn.state == state {
            return Ok(());
        } else {
            return Err(TMError::InvalidTransactionState(txn.state));
        }
    }
    fn ensure_rw_state(&self, txn: &TxnGuard) -> Result<(), TMError> {
        self.ensure_txn_state(txn, TxnState::Started)
    }
    fn cleanup_transaction(&self, tid: &TxnId) {
        self.transactions.write(tid).map(|g| g.remove());
    }
}

struct AwaitingServer {
    sender: Sender<()>,
    receiver: Receiver<()>,
}

impl AwaitingServer {
    pub fn new() -> AwaitingServer {
        let (sender, receiver) = channel(1);
        AwaitingServer {
            sender: sender,
            receiver: receiver,
        }
    }
    pub async fn send(&mut self) {
        self.sender.send(()).await.unwrap();
    }
    pub async fn wait(&mut self) {
        self.receiver.recv().await.unwrap();
    }
}

struct AwaitManager {
    channels: LFMap<TxnId, TxnAwaits>,
}

impl AwaitManager {
    pub fn new() -> AwaitManager {
        AwaitManager {
            channels: LFMap::with_capacity(16),
        }
    }
    pub fn get_txn(&self, tid: &TxnId) -> TxnAwaits {
        self.channels.get_or_insert(tid, || ObjectMap::with_capacity(8))
    }
    pub fn server_from_txn_awaits(awaits: &TxnAwaits, server_id: u64) -> Arc<Mutex<AwaitingServer>> {
        awaits.get_or_insert(&(server_id as usize), || Arc::new(Mutex::new(AwaitingServer::new())))  
    }
    pub async fn txn_send(awaits: &TxnAwaits, server_id: u64) {
        let manager_lock = AwaitManager::server_from_txn_awaits(awaits, server_id);
        let mut manager = manager_lock.lock().await;
        manager.send().await
    }
    pub async fn txn_wait(awaits: &TxnAwaits, server_id: u64) {
        let manager_lock = AwaitManager::server_from_txn_awaits(awaits, server_id);
        let mut manager = manager_lock.lock().await;
        manager.wait().await
    }
}
