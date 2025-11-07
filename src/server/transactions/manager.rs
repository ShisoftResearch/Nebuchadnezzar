use super::*;
use crate::ram::cell::CellHeader;
use crate::ram::cell::{ReadError, WriteError};
use crate::ram::types::{Id, OwnedValue};
use crate::server::NebServer;
use bifrost::vector_clock::StandardVectorClock;
use bifrost_plugins::hash_ident;
use dovahkiin::types::Map;
use itertools::Itertools;
use lightning::map::{Map as LFMapT, PtrHashMap as LFMap};
use std::collections::{BTreeMap, HashMap};
// Use async mutex because this module is a distributed coordinator
use async_std::sync::{Mutex, MutexGuard};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use std::time::Duration;

type TxnMutex = Arc<Mutex<Transaction>>;
type TxnGuard<'a> = MutexGuard<'a, Transaction>;
type AffectedObjs = BTreeMap<u64, BTreeMap<Id, DataObject>>; // server_id as key
type DataSitesMap = HashMap<u64, Arc<data_site::AsyncServiceClient>>;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TXN_MANAGER_RPC_SERVICE) as u64;

/// Configuration for wait/retry behavior when transactions encounter conflicts
#[derive(Clone, Debug)]
pub struct WaitConfig {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub max_total_wait_ms: u64,
}

impl Default for WaitConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 1,   // Start with 1ms
            max_backoff_ms: 100,     // Cap at 100ms
            max_total_wait_ms: 5000, // Give up after 5 seconds
        }
    }
}

#[derive(Clone, Debug)]
struct DataObject {
    server: u64,
    cell: Option<OwnedCell>,
    version: Option<u64>,
    changed: bool,
    new: bool,
}

struct Transaction {
    data: HashMap<Id, DataObject>,
    affected_objects: AffectedObjs,
    state: TxnState,
}

service! {
    rpc begin() -> Result<TxnId, TMError>;
    rpc read(tid: TxnId, id: Id) -> Result<TxnExecResult<OwnedCell, ReadError>, TMError>;
    rpc read_selected(tid: TxnId, id: Id, fields: Vec<u64>) -> Result<TxnExecResult<OwnedCell, ReadError>, TMError>;
    rpc head(tid: TxnId, id: Id) -> Result<TxnExecResult<CellHeader, ReadError>, TMError>;
    rpc write(tid: TxnId, cell: OwnedCell) -> Result<TxnExecResult<(), WriteError>, TMError>;
    rpc update(tid: TxnId, cell: OwnedCell) -> Result<TxnExecResult<(), WriteError>, TMError>;
    rpc remove(tid: TxnId, id: Id) -> Result<TxnExecResult<(), WriteError>, TMError>;

    rpc prepare(tid: TxnId) -> Result<TMPrepareResult, TMError>;
    rpc commit(tid: TxnId) -> Result<EndResult, TMError>;
    rpc abort(tid: TxnId) -> Result<AbortResult, TMError>;
}

dispatch_rpc_service_functions!(TransactionManager);

service_with_id!(TransactionManager, DEFAULT_SERVICE_ID);

pub struct TransactionManager {
    server: Arc<NebServer>,
    transactions: LFMap<TxnId, TxnMutex>,
    data_sites: LFMap<u64, Arc<data_site::AsyncServiceClient>>,
    wait_config: WaitConfig,
}

impl TransactionManager {
    pub fn new(server: &Arc<NebServer>) -> Arc<TransactionManager> {
        Arc::new(Self {
            server: server.clone(),
            transactions: LFMap::with_capacity(128),
            data_sites: LFMap::with_capacity(8),
            wait_config: WaitConfig::default(),
        })
    }

    pub fn with_wait_config(
        server: &Arc<NebServer>,
        wait_config: WaitConfig,
    ) -> Arc<TransactionManager> {
        Arc::new(Self {
            server: server.clone(),
            transactions: LFMap::with_capacity(128),
            data_sites: LFMap::with_capacity(8),
            wait_config,
        })
    }

    /// Helper function for exponential backoff wait with timeout
    async fn backoff_wait(attempt: u32, config: &WaitConfig) -> Result<(), TMError> {
        let backoff_ms = config.initial_backoff_ms * 2u64.pow(attempt);
        let backoff_ms = backoff_ms.min(config.max_backoff_ms);

        debug!("Backing off for {}ms (attempt {})", backoff_ms, attempt);
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        Ok(())
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
    ) -> BoxFuture<'_, Result<TxnExecResult<OwnedCell, ReadError>, TMError>> {
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
                    self.read_from_site(server_id, &server, &tid, &id, &mut txn)
                        .await
                }
                Err(e) => {
                    error!("{:?}", e);
                    Err(TMError::CannotLocateCellServer)
                }
            }
        }
        .boxed()
    }

    fn head(
        &self,
        tid: TxnId,
        id: Id,
    ) -> BoxFuture<'_, Result<TxnExecResult<CellHeader, ReadError>, TMError>> {
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
                    self.head_from_site(server_id, &server, &tid, &id, &txn)
                        .await
                }
                Err(e) => {
                    error!("{:?}", e);
                    Err(TMError::CannotLocateCellServer)
                }
            }
        }
        .boxed()
    }
    fn read_selected(
        &self,
        tid: TxnId,
        id: Id,
        fields: Vec<u64>,
    ) -> BoxFuture<'_, Result<TxnExecResult<OwnedCell, ReadError>, TMError>> {
        async move {
            let txn_mutex = self.get_transaction(&tid)?;
            let txn = txn_mutex.lock().await;
            self.ensure_rw_state(&txn)?;
            if let Some(data_obj) = txn.data.get(&id) {
                match data_obj.cell {
                    Some(ref cell) => {
                        let schema_id = cell.header.schema;
                        if let Some(schema) = self.server.meta.schemas.get(&schema_id) {
                            if let OwnedValue::Map(map) = &cell.data {
                                let mut res = vec![];
                                'SEARCH: for field in &fields {
                                    if let Some(index_path) = schema.id_index.get(field) {
                                        let path =
                                            index_path.iter().map(|id| *id as u64).collect_vec();
                                        trace!("Get into map for txn select {:?}", path);
                                        let val = map.get_in_by_ids(path.iter()).clone();
                                        res.push(val);
                                        continue 'SEARCH;
                                    }
                                    res.push(OwnedValue::Null);
                                }
                                return Ok(TxnExecResult::Accepted(OwnedCell {
                                    header: cell.header,
                                    data: OwnedValue::Array(res),
                                }));
                            } else {
                                return Ok(TxnExecResult::Error(
                                    ReadError::CellTypeIsNotMapForSelect,
                                ));
                            }
                        } else {
                            return Ok(TxnExecResult::Error(ReadError::SchemaDoesNotExisted(
                                schema_id,
                            )));
                        }
                    } // read from cache
                    None => return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted)),
                }
            }
            match self.get_data_site_by_id(&id).await {
                Ok((server_id, server)) => {
                    self.read_selected_from_site(server_id, &server, &tid, &id, &fields, &txn)
                        .await
                }
                Err(e) => {
                    error!("{:?}", e);
                    Err(TMError::CannotLocateCellServer)
                }
            }
        }
        .boxed()
    }

    fn prepare(&self, tid: TxnId) -> BoxFuture<'_, Result<TMPrepareResult, TMError>> {
        async move {
            // Note: Automatic retry with fresh timestamps removed because it changes
            // the transaction ID mid-flight, breaking the client's reference.
            // For remaining NotRealizable errors, clients should retry with a new transaction.
            self.do_prepare(&tid).await
        }
        .boxed()
    }
    fn commit(&self, tid: TxnId) -> BoxFuture<'_, Result<EndResult, TMError>> {
        async move {
            let result = {
                let txn_lock = self.get_transaction(&tid)?;
                let txn = txn_lock.lock().await;
                self.ensure_txn_state(&txn, TxnState::Prepared)?;
                let affected_objs = &txn.affected_objects;
                let data_sites = self.data_sites_for_objs(&affected_objs).await?;
                self.sites_end(&tid, affected_objs, &data_sites).await
            };
            self.cleanup_transaction(&tid);
            return result;
        }
        .boxed()
    }
    fn abort(&self, tid: TxnId) -> BoxFuture<'_, Result<AbortResult, TMError>> {
        debug!("TXN ABORT IN MGR {:?}", &tid);
        async move {
            let result = {
                let txn_lock = self.get_transaction(&tid)?;
                let txn = txn_lock.lock().await;
                if txn.state != TxnState::Aborted {
                    let changed_objs = &txn.affected_objects;
                    let data_sites = self.data_sites_for_objs(&changed_objs).await?;
                    debug!("ABORT AFFECTED OBJS: {:?}", changed_objs);
                    self.sites_abort(&tid, changed_objs, &data_sites).await // with end
                } else {
                    Ok(AbortResult::Success(None))
                }
            };
            self.cleanup_transaction(&tid);
            return result;
        }
        .boxed()
    }
    fn begin(&self) -> BoxFuture<'_, Result<TxnId, TMError>> {
        let id = self.server.txn_peer.clock.inc();
        if self
            .transactions
            .insert(
                id.clone(),
                Arc::new(Mutex::new(Transaction {
                    data: HashMap::new(),
                    affected_objects: AffectedObjs::new(),
                    state: TxnState::Started,
                })),
            )
            .is_some()
        {
            error!("Transaction id existed: {:?}", id);
            future::ready(Err(TMError::TransactionIdExisted)).boxed()
        } else {
            future::ready(Ok(id)).boxed()
        }
    }

    fn write(
        &self,
        tid: TxnId,
        cell: OwnedCell,
    ) -> BoxFuture<'_, Result<TxnExecResult<(), WriteError>, TMError>> {
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
                        let data_obj = txn.data.get_mut(&id).unwrap();
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
        }
        .boxed()
    }
    fn update(
        &self,
        tid: TxnId,
        cell: OwnedCell,
    ) -> BoxFuture<'_, Result<TxnExecResult<(), WriteError>, TMError>> {
        async move {
            let txn_mutex = self.get_transaction(&tid)?;
            let mut txn = txn_mutex.lock().await;
            let id = cell.id();
            self.ensure_rw_state(&txn)?;
            match self.server.get_server_id_by_id(&id) {
                Some(server_id) => {
                    let cell = cell.clone();
                    if txn.data.contains_key(&id) {
                        let data_obj = txn.data.get_mut(&id).unwrap();
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
        }
        .boxed()
    }
    fn remove(
        &self,
        tid: TxnId,
        id: Id,
    ) -> BoxFuture<'_, Result<TxnExecResult<(), WriteError>, TMError>> {
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
        }
        .boxed()
    }
}

impl TransactionManager {
    async fn get_data_site(
        &self,
        server_id: u64,
    ) -> io::Result<Arc<data_site::AsyncServiceClient>> {
        if !self.data_sites.contains_key(&server_id) {
            let client = self.server.get_member_by_server_id(server_id).await?;
            return Ok(self
                .data_sites
                .get_or_insert(server_id, || data_site::AsyncServiceClient::new(&client)));
        }
        self.data_sites.get(&server_id).ok_or(io::Error::new(
            io::ErrorKind::NotFound,
            "data site not found",
        ))
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
    ) -> Result<TxnExecResult<OwnedCell, ReadError>, TMError> {
        let start_time = std::time::Instant::now();
        let mut attempt = 0u32;
        let self_server_id = self.server.server_id;

        loop {
            // Check timeout
            if start_time.elapsed().as_millis() > self.wait_config.max_total_wait_ms as u128 {
                warn!("Read timeout for transaction {:?} on cell {:?}", tid, id);
                return Ok(TxnExecResult::Rejected);
            }

            let read_response = server
                .read(self_server_id, self.get_clock(), tid.to_owned(), id.clone())
                .await;
            match read_response {
                Ok(dsr) => {
                    self.merge_clock(&dsr.clock);
                    let payload = dsr.payload;
                    match &payload {
                        TxnExecResult::Accepted(cell) => {
                            // Check if there's a pending update in the transaction cache
                            // If the cell was updated locally, we must return the cached updated version
                            // instead of overwriting it with the remote (stale) value
                            if let Some(data_obj) = txn.data.get_mut(id) {
                                // Entry exists in transaction cache
                                if data_obj.changed {
                                    // There's a pending update - return the cached updated cell instead
                                    // This ensures update-then-read visibility within the same transaction
                                    if let Some(ref cached_cell) = data_obj.cell {
                                        return Ok(TxnExecResult::Accepted(cached_cell.clone()));
                                    }
                                    // Changed but cell is None (removed) - return error
                                    return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted));
                                }
                                // Entry exists but not changed - only update if cell is missing
                                if data_obj.cell.is_none() {
                                    data_obj.cell = Some(cell.clone());
                                    data_obj.version = Some(cell.header.version);
                                }
                            } else {
                                // No entry exists - cache the remote value
                                txn.data.insert(
                                    id.clone(),
                                    DataObject {
                                        server: server_id,
                                        version: Some(cell.header.version),
                                        cell: Some(cell.clone()),
                                        new: false,
                                        changed: false,
                                    },
                                );
                            }
                        }
                        TxnExecResult::Wait => {
                            // Backoff and retry
                            Self::backoff_wait(attempt, &self.wait_config).await?;
                            attempt += 1;
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

    async fn head_from_site<'a>(
        &self,
        _server_id: u64,
        server: &Arc<data_site::AsyncServiceClient>,
        tid: &TxnId,
        id: &Id,
        _txn: &TxnGuard<'a>,
    ) -> Result<TxnExecResult<CellHeader, ReadError>, TMError> {
        let start_time = std::time::Instant::now();
        let mut attempt = 0u32;
        let self_server_id = self.server.server_id;

        loop {
            // Check timeout
            if start_time.elapsed().as_millis() > self.wait_config.max_total_wait_ms as u128 {
                warn!("Head timeout for transaction {:?} on cell {:?}", tid, id);
                return Ok(TxnExecResult::Rejected);
            }

            let head_response = server
                .head(self_server_id, self.get_clock(), tid.to_owned(), *id)
                .await;
            match head_response {
                Ok(dsr) => {
                    self.merge_clock(&dsr.clock);
                    let payload = &dsr.payload;
                    match &payload {
                        &TxnExecResult::Wait => {
                            // Backoff and retry
                            Self::backoff_wait(attempt, &self.wait_config).await?;
                            attempt += 1;
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
        _server_id: u64,
        server: &Arc<data_site::AsyncServiceClient>,
        tid: &TxnId,
        id: &Id,
        fields: &Vec<u64>,
        _txn: &TxnGuard<'a>,
    ) -> Result<TxnExecResult<OwnedCell, ReadError>, TMError> {
        let start_time = std::time::Instant::now();
        let mut attempt = 0u32;
        let self_server_id = self.server.server_id;

        loop {
            // Check timeout
            if start_time.elapsed().as_millis() > self.wait_config.max_total_wait_ms as u128 {
                warn!(
                    "Read selected timeout for transaction {:?} on cell {:?}",
                    tid, id
                );
                return Ok(TxnExecResult::Rejected);
            }

            let read_response = server
                .read_selected(
                    self_server_id,
                    self.get_clock(),
                    tid.to_owned(),
                    id.clone(),
                    fields.to_owned(),
                )
                .await;
            match read_response {
                Ok(dsr) => {
                    self.merge_clock(&dsr.clock);
                    let payload = dsr.payload;
                    match payload {
                        TxnExecResult::Wait => {
                            // Backoff and retry
                            Self::backoff_wait(attempt, &self.wait_config).await?;
                            attempt += 1;
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
        for (id, data_obj) in txn.data.drain() {
            if data_obj.changed {
                affected_objs
                    .entry(data_obj.server)
                    .or_insert_with(|| BTreeMap::new())
                    .insert(id, data_obj);
            }
        }
        txn.affected_objects = affected_objs;
    }
    async fn data_sites_for_objs(
        &self,
        changed_objs: &AffectedObjs,
    ) -> Result<DataSitesMap, TMError> {
        let mut data_sites = HashMap::new();
        for (server_id, _) in changed_objs {
            data_sites.insert(*server_id, self.get_data_site(*server_id).await);
        }
        if data_sites.iter().any(|(_, data_site)| data_site.is_err()) {
            return Err(TMError::CannotLocateCellServer);
        }
        Ok(data_sites
            .into_iter()
            .map(|(id, client)| (id, client.unwrap()))
            .collect())
    }
    async fn site_prepare(
        server: &Arc<NebServer>,
        config: &WaitConfig,
        tid: &TxnId,
        objs: &BTreeMap<Id, DataObject>,
        data_site: &Arc<data_site::AsyncServiceClient>,
    ) -> Result<DMPrepareResult, TMError> {
        let start_time = std::time::Instant::now();
        let mut attempt = 0u32;

        loop {
            // Check timeout
            if start_time.elapsed().as_millis() > config.max_total_wait_ms as u128 {
                warn!("Prepare timeout for transaction {:?}", tid);
                return Ok(DMPrepareResult::NotRealizable); // Give up
            }

            let self_server_id = server.server_id;
            let cell_ids: Vec<_> = objs.iter().map(|(id, _)| *id).collect();
            let server_for_clock = server.clone();
            let prepare_payload = data_site
                .prepare(
                    self_server_id,
                    server.txn_peer.clock.to_clock(),
                    tid.clone(),
                    cell_ids,
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
                            // Backoff and retry
                            Self::backoff_wait(attempt, config).await?;
                            attempt += 1;
                            continue;
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
            .map(|(server, objs)| async move {
                let data_site = data_sites.get(server).unwrap().clone();
                TransactionManager::site_prepare(
                    &self.server,
                    &self.wait_config,
                    &tid,
                    &objs,
                    &data_site,
                )
                .await
            })
            .collect();
        while let Some(result) = prepare_futures.next().await {
            match result {
                Ok(DMPrepareResult::Success) => {}
                Ok(res) => return Ok(res),
                Err(e) => return Err(e),
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
                    data_site
                        .commit(self.get_clock(), tid.to_owned(), ops)
                        .await
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
                async move { data_site.abort(self.get_clock(), tid.clone()).await }
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
                        _ => return Ok(payload),
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
                async move { data_site.end(self.get_clock(), tid.clone()).await }
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
                }
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
        self.transactions.remove(tid);
    }

    /// Performs the actual prepare operation (extracted for retry logic)
    async fn do_prepare(&self, tid: &TxnId) -> Result<TMPrepareResult, TMError> {
        let conclusion = {
            let txn_mutex = self.get_transaction(&tid)?;
            let mut txn = txn_mutex.lock().await;
            let result = {
                self.ensure_rw_state(&txn)?;
                self.generate_affected_objs(&mut txn);
                let affect_objs = &txn.affected_objects;
                let data_sites = self.data_sites_for_objs(&affect_objs).await?;
                let sites_prepare_result =
                    self.sites_prepare(&tid, affect_objs, &data_sites).await?;
                if sites_prepare_result == DMPrepareResult::Success {
                    let sites_commit_result =
                        self.sites_commit(&tid, affect_objs, &data_sites).await?;
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
        Ok(conclusion)
    }
}
