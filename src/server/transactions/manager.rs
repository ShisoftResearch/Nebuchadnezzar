use super::*;
use crate::ram::cell::CellHeader;
use crate::ram::cell::{ReadError, WriteError};
use crate::ram::types::{Id, OwnedValue};
use bifrost::conshash::ConsistentHashing;
use bifrost::hlc::Hlc;
use bifrost::rpc::{ClientPool, RPCClient};
use bifrost::utils::time::get_time;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use dovahkiin::types::Map;
use itertools::Itertools;
use lightning::map::{Map as LFMapT, PtrHashMap as LFMap};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
// Use async mutex because this module is a distributed coordinator
use async_std::sync::{Mutex, MutexGuard};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;
use tokio::sync::oneshot;
#[cfg(test)]
use tokio::sync::Notify;

type TxnMutex = Arc<Mutex<Transaction>>;
type TxnGuard<'a> = MutexGuard<'a, Transaction>;
type AffectedObjs = BTreeMap<u64, BTreeMap<Id, DataObject>>; // server_id as key

/// Transactions whose decision fan-out is cut short, and after how many
/// participants. Test-only: see `sites_end`.
#[cfg(test)]
static END_FANOUT_LIMIT: std::sync::OnceLock<parking_lot::Mutex<BTreeMap<TxnId, EndFanoutPlan>>> =
    std::sync::OnceLock::new();

/// Which participants a dying coordinator manages to tell.
#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) enum EndFanoutPlan {
    /// The first N in participant order.
    First(usize),
    /// One named server, so a test can choose WHICH participant is left in
    /// doubt rather than depending on map order.
    Only(u64),
}

#[cfg(test)]
fn end_fanout_limits() -> &'static parking_lot::Mutex<BTreeMap<TxnId, EndFanoutPlan>> {
    END_FANOUT_LIMIT.get_or_init(|| parking_lot::Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn end_fanout_limit(tid: &TxnId) -> Option<EndFanoutPlan> {
    end_fanout_limits().lock().get(tid).copied()
}

/// Make the coordinator stop after telling `sites` participants, as if it died
/// there. Removed when the handle drops.
#[cfg(test)]
pub(crate) struct EndFanoutLimit(TxnId);

#[cfg(test)]
impl Drop for EndFanoutLimit {
    fn drop(&mut self) {
        end_fanout_limits().lock().remove(&self.0);
    }
}

#[cfg(test)]
pub(crate) fn stop_end_fanout_after(tid: TxnId, sites: usize) -> EndFanoutLimit {
    end_fanout_limits()
        .lock()
        .insert(tid.clone(), EndFanoutPlan::First(sites));
    EndFanoutLimit(tid)
}

/// Tell exactly one named participant and stop. Everyone else is in doubt.
#[cfg(test)]
pub(crate) fn stop_end_fanout_after_server(tid: TxnId, server_id: u64) -> EndFanoutLimit {
    end_fanout_limits()
        .lock()
        .insert(tid.clone(), EndFanoutPlan::Only(server_id));
    EndFanoutLimit(tid)
}
type DataSitesMap = HashMap<u64, Arc<data_site::AsyncServiceClient>>;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TXN_MANAGER_RPC_SERVICE) as u64;

pub fn generate_scoped_service_id(group: &str, database_name: &str) -> u64 {
    hash_str(&format!(
        "TXN_MANAGER_RPC_SERVICE-{}-{}",
        group, database_name
    ))
}

#[cfg(test)]
struct PrepareResultObserverState {
    observed: AtomicBool,
    notify: Notify,
}

#[cfg(test)]
pub(crate) struct PrepareResultObserverHandle {
    key: (TxnId, Id),
    state: Arc<PrepareResultObserverState>,
}

#[cfg(test)]
impl PrepareResultObserverHandle {
    pub(crate) async fn wait_until_observed(&self) {
        let notified = self.state.notify.notified();
        if self.state.observed.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }
}

#[cfg(test)]
impl Drop for PrepareResultObserverHandle {
    fn drop(&mut self) {
        let mut observers = prepare_result_observers().lock();
        let owns_registration = observers
            .get(&self.key)
            .map(|state| Arc::ptr_eq(state, &self.state))
            .unwrap_or(false);
        if owns_registration {
            observers.remove(&self.key);
        }
        self.state.observed.store(true, Ordering::SeqCst);
        self.state.notify.notify_waiters();
    }
}

#[cfg(test)]
static PREPARE_RESULT_OBSERVERS: OnceLock<
    parking_lot::Mutex<BTreeMap<(TxnId, Id), Arc<PrepareResultObserverState>>>,
> = OnceLock::new();

#[cfg(test)]
fn prepare_result_observers(
) -> &'static parking_lot::Mutex<BTreeMap<(TxnId, Id), Arc<PrepareResultObserverState>>> {
    PREPARE_RESULT_OBSERVERS.get_or_init(|| parking_lot::Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn install_prepare_result_observer(tid: TxnId, id: Id) -> PrepareResultObserverHandle {
    let key = (tid, id);
    let state = Arc::new(PrepareResultObserverState {
        observed: AtomicBool::new(false),
        notify: Notify::new(),
    });
    prepare_result_observers()
        .lock()
        .insert(key.clone(), state.clone());
    PrepareResultObserverHandle { key, state }
}

#[cfg(test)]
struct AbortEntryDelayState {
    entered: AtomicBool,
    entered_notify: Notify,
    released: AtomicBool,
    released_notify: Notify,
}

#[cfg(test)]
pub(crate) struct AbortEntryDelayHandle {
    tid: TxnId,
    state: Arc<AbortEntryDelayState>,
}

#[cfg(test)]
impl AbortEntryDelayHandle {
    pub(crate) async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    pub(crate) fn release(&self) {
        if !self.state.released.swap(true, Ordering::SeqCst) {
            self.state.released_notify.notify_waiters();
        }
    }
}

#[cfg(test)]
impl Drop for AbortEntryDelayHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = abort_entry_delay_hooks().lock();
        let owns_registration = hooks
            .get(&self.tid)
            .map(|state| Arc::ptr_eq(state, &self.state))
            .unwrap_or(false);
        if owns_registration {
            hooks.remove(&self.tid);
        }
    }
}

#[cfg(test)]
static ABORT_ENTRY_DELAY_HOOKS: OnceLock<
    parking_lot::Mutex<BTreeMap<TxnId, Arc<AbortEntryDelayState>>>,
> = OnceLock::new();

#[cfg(test)]
fn abort_entry_delay_hooks(
) -> &'static parking_lot::Mutex<BTreeMap<TxnId, Arc<AbortEntryDelayState>>> {
    ABORT_ENTRY_DELAY_HOOKS.get_or_init(|| parking_lot::Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn install_abort_entry_delay(tid: TxnId) -> AbortEntryDelayHandle {
    let state = Arc::new(AbortEntryDelayState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: AtomicBool::new(false),
        released_notify: Notify::new(),
    });
    abort_entry_delay_hooks()
        .lock()
        .insert(tid.clone(), state.clone());
    AbortEntryDelayHandle { tid, state }
}

/// Dependencies needed by TransactionManager, extracted from NebServer to break cyclic dependency
pub struct TransactionManagerDeps {
    pub database_runtime: Arc<crate::server::DatabaseRuntime>,
    pub server_id: u64,
    pub consh: Arc<ConsistentHashing>,
    pub member_pool: Arc<ClientPool>,
    /// Per-server Hybrid Logical Clock source (node = server_id), shared with
    /// the participant-side `DataManager`. Sources transaction ids and the
    /// clock stamps carried on every transaction-layer RPC.
    pub hlc: Arc<bifrost::hlc::HlcSource>,
}

impl TransactionManagerDeps {
    /// Which member holds this cell. Slot-aware for the same reason
    /// `NebServer::get_server_id_by_id` is: a transaction that located a cell one
    /// way and wrote it another would corrupt exactly what it was protecting.
    pub fn get_server_id_by_id(&self, id: &Id) -> Option<u64> {
        self.consh.get_server_id_for_slot(id.locality() as u64)
    }

    pub async fn get_member_by_server_id(&self, server_id: u64) -> io::Result<Arc<RPCClient>> {
        let consh = self.consh.clone();
        self.member_pool
            .get_by_id(server_id, move |_| consh.try_server_name(server_id))
            .await
    }

    pub fn schemas(&self) -> &crate::ram::schema::LocalSchemasCache {
        self.database_runtime.schemas()
    }
}

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
    expectation: CellExpectation,
    changed: bool,
    new: bool,
    /// Coordinator-side pinned-read cache for size-gated repeatable-read.
    /// `None` selects the current (small-cell / clone) representation.
    /// `Some(..)` marks a deferred large-cell read whose full `cell` is not
    /// yet materialized (`cell: None` until an actual full read happens).
    pinned: Option<PinnedReadCache>,
}

/// Coordinator-side cache for a pinned (size-gated) repeatable-read version.
/// Keeps only small results — the header and any projections already
/// fetched — so a large pinned cell is not cloned whole into the
/// transaction cache. The full cell is materialized lazily on an actual
/// full read (wiring for that happens outside this representation).
#[derive(Clone, Debug, Default)]
struct PinnedReadCache {
    /// Cached header of the pinned version (small; serves repeated head reads).
    header: Option<CellHeader>,
    /// Cached projections keyed by the (sorted) field-id list served so far.
    projections: HashMap<Vec<u64>, OwnedCell>,
}

struct Transaction {
    data: HashMap<Id, DataObject>,
    affected_objects: AffectedObjs,
    state: TxnState,
    last_activity: i64, // Unix timestamp in milliseconds for detecting stale transactions
    /// Server ids on which this transaction pinned a read version (via a partial
    /// `head(pin=true)` / `read_selected`). Tracked separately from `data` so it
    /// survives `generate_affected_objs` draining `data`, letting commit/abort
    /// release those pins even on servers the transaction never wrote to.
    pinned_servers: HashSet<u64>,
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
    self_ref: Weak<TransactionManager>,
    deps: Arc<TransactionManagerDeps>,
    transactions: LFMap<TxnId, TxnMutex>,
    txn_ids: parking_lot::Mutex<BTreeSet<TxnId>>, // Track TxnIds for iteration (PtrHashMap doesn't support iteration)
    data_sites: LFMap<u64, Arc<data_site::AsyncServiceClient>>,
    wait_config: WaitConfig,
    shutdown: Arc<AtomicBool>, // Signal to stop background cleanup task
    // Completed transactions whose read pins await release, queued here so the
    // commit/abort paths pay only a lock+push; a periodic background flusher
    // sends the batched release RPCs. A per-commit `tokio::spawn` was measured
    // at 15-40us of worker-unpark overhead at low concurrency in this codebase,
    // which a queue push avoids entirely.
    pending_pin_releases: parking_lot::Mutex<Vec<(TxnId, HashSet<u64>)>>,
}

/// How often the background flusher sends queued read-pin releases. Pins are
/// memory retention, not locks, so a small bounded delay is harmless; the
/// participant's stale cleanup remains the backstop for lost releases.
const PIN_RELEASE_FLUSH_INTERVAL_MS: u64 = 50;

impl TransactionManager {
    pub fn new(deps: Arc<TransactionManagerDeps>) -> Arc<TransactionManager> {
        Self::new_with_config(deps, WaitConfig::default())
    }

    pub fn with_wait_config(
        deps: Arc<TransactionManagerDeps>,
        wait_config: WaitConfig,
    ) -> Arc<TransactionManager> {
        Self::new_with_config(deps, wait_config)
    }

    fn new_with_config(
        deps: Arc<TransactionManagerDeps>,
        wait_config: WaitConfig,
    ) -> Arc<TransactionManager> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let manager = Arc::new_cyclic(|self_ref| Self {
            self_ref: self_ref.clone(),
            deps,
            transactions: LFMap::with_capacity(128),
            txn_ids: parking_lot::Mutex::new(BTreeSet::new()),
            data_sites: LFMap::with_capacity(8),
            wait_config,
            shutdown: shutdown.clone(),
            pending_pin_releases: parking_lot::Mutex::new(Vec::new()),
        });

        // Periodically flush queued read-pin releases in batches, so completing
        // transactions never spawn tasks or await release RPCs themselves.
        let flusher = manager.clone();
        let flusher_shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                if flusher_shutdown.load(Ordering::Relaxed) {
                    debug!("TransactionManager pin-release flusher shutting down");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(PIN_RELEASE_FLUSH_INTERVAL_MS)).await;
                flusher.flush_pin_releases().await;
            }
        });

        // Spawn background cleanup task
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            loop {
                // Check if we should shutdown
                if shutdown.load(Ordering::Relaxed) {
                    debug!("TransactionManager cleanup task shutting down");
                    break;
                }

                // Sleep for 60 seconds
                tokio::time::sleep(Duration::from_secs(60)).await;

                // Clean up stale transactions (older than 5 minutes)
                let cleaned = manager_clone.cleanup_stale_transactions(5 * 60 * 1000);
                if cleaned > 0 {
                    warn!("Cleaned up {} stale transactions", cleaned);
                }
            }
        });

        manager
    }

    /// Returns the current number of living transactions tracked by this TransactionManager
    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    /// Helper function for exponential backoff wait with timeout
    async fn backoff_wait(attempt: u32, config: &WaitConfig) -> Result<(), TMError> {
        let backoff_ms = config.initial_backoff_ms * 2u64.pow(attempt);
        let backoff_ms = backoff_ms.min(config.max_backoff_ms);

        debug!("Backing off for {}ms (attempt {})", backoff_ms, attempt);
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        Ok(())
    }

    fn spawn_prepare_lifecycle(
        &self,
        tid: TxnId,
    ) -> Result<
        (
            oneshot::Receiver<Result<TMPrepareResult, TMError>>,
            oneshot::Sender<()>,
        ),
        TMError,
    > {
        let manager = self.self_ref.upgrade().ok_or(TMError::Other)?;
        let (result_sender, result_receiver) = oneshot::channel();
        let (ack_sender, ack_receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = manager.clone().run_prepare_lifecycle(tid.clone()).await;
            let prepared = matches!(&result, Ok(TMPrepareResult::Success));
            if result_sender.send(result).is_err() {
                if prepared {
                    let _ = manager.abort(tid).await;
                }
                return;
            }
            if prepared && ack_receiver.await.is_err() {
                let _ = manager.abort(tid).await;
            }
        });
        Ok((result_receiver, ack_sender))
    }

    #[cfg(test)]
    fn take_abort_entry_delay(tid: &TxnId) -> Option<Arc<AbortEntryDelayState>> {
        abort_entry_delay_hooks().lock().remove(tid)
    }

    #[cfg(test)]
    async fn await_abort_entry_delay(state: &Arc<AbortEntryDelayState>) {
        state.entered.store(true, Ordering::SeqCst);
        state.entered_notify.notify_waiters();
        let notified = state.released_notify.notified();
        if state.released.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    #[cfg(test)]
    fn notify_prepare_results_observed(tid: &TxnId, objects: &BTreeMap<Id, DataObject>) {
        let states: Vec<_> = {
            let mut observers = prepare_result_observers().lock();
            objects
                .keys()
                .filter_map(|id| observers.remove(&(tid.clone(), *id)))
                .collect()
        };
        for state in states {
            state.observed.store(true, Ordering::SeqCst);
            state.notify.notify_waiters();
        }
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
            self.full_read(&tid, &id, &mut txn).await
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
            let mut txn = txn_mutex.lock().await;
            self.ensure_rw_state(&txn)?;
            self.head_read(&tid, &id, &mut txn).await
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
            let mut txn = txn_mutex.lock().await;
            self.ensure_rw_state(&txn)?;
            self.selected_read(&tid, &id, &fields, &mut txn).await
        }
        .boxed()
    }

    fn prepare(&self, tid: TxnId) -> BoxFuture<'_, Result<TMPrepareResult, TMError>> {
        async move {
            let prepare_tid = tid.clone();
            let (result_receiver, ack_sender) = self.spawn_prepare_lifecycle(tid)?;
            match result_receiver.await {
                Ok(result) => {
                    let _ = ack_sender.send(());
                    result
                }
                Err(receive_error) => {
                    error!(
                        "prepare lifecycle task failed for {:?}: {:?}",
                        prepare_tid, receive_error
                    );
                    Err(TMError::Other)
                }
            }
        }
        .boxed()
    }
    fn commit(&self, tid: TxnId) -> BoxFuture<'_, Result<EndResult, TMError>> {
        async move {
            let txn_lock = self.get_transaction(&tid)?;
            let mut txn = txn_lock.lock().await;
            self.ensure_txn_state(&txn, TxnState::Prepared)?;
            let affected_objs = &txn.affected_objects;
            let result = match {
                #[cfg(feature = "occ_phase_profile")]
                let _phase_guard =
                    super::phase_profile::guard(super::phase_profile::Phase::EndParticipantLookup);
                self.data_sites_for_objs(affected_objs).await
            } {
                Ok(data_sites) => {
                    #[cfg(feature = "occ_phase_profile")]
                    let _phase_guard =
                        super::phase_profile::guard(super::phase_profile::Phase::EndCleanup);
                    self.sites_end(&tid, affected_objs, &data_sites).await
                }
                Err(error) => Err(error),
            };
            // Fire-and-forget release of read pins on servers this transaction
            // only read (pinned but not written). Write servers already dropped
            // their pins in `end`, so they are excluded. Queued so it never adds
            // latency to the commit path; the periodic flusher batches the RPCs.
            // Write-only transactions (no pins) skip this entirely.
            if !txn.pinned_servers.is_empty() {
                let mut pinned_servers = std::mem::take(&mut txn.pinned_servers);
                pinned_servers.retain(|server_id| !txn.affected_objects.contains_key(server_id));
                self.queue_pin_release(tid.clone(), pinned_servers);
            }
            self.cleanup_transaction_guarded(&tid, &mut txn);
            result
        }
        .boxed()
    }
    fn abort(&self, tid: TxnId) -> BoxFuture<'_, Result<AbortResult, TMError>> {
        debug!("TXN ABORT IN MGR {:?}", &tid);
        async move {
            let txn_lock = self.get_transaction(&tid)?;
            #[cfg(test)]
            if let Some(state) = Self::take_abort_entry_delay(&tid) {
                Self::await_abort_entry_delay(&state).await;
            }
            let mut txn = txn_lock.lock().await;
            match txn.state {
                TxnState::Cleanup => {
                    return Ok(AbortResult::CheckFailed(CheckError::AlreadyCleanup));
                }
                TxnState::Committed => {
                    return Ok(AbortResult::CheckFailed(CheckError::AlreadyCommitted));
                }
                TxnState::Started | TxnState::Prepared => {
                    // Once abort is accepted, commit must remain illegal even when
                    // participant rollback needs to be retried.
                    txn.state = TxnState::Aborted;
                }
                TxnState::Aborted => {}
            }
            txn.last_activity = get_time();
            let changed_objs = &txn.affected_objects;
            let result = match {
                #[cfg(feature = "occ_phase_profile")]
                let _phase_guard = super::phase_profile::guard(
                    super::phase_profile::Phase::AbortParticipantLookup,
                );
                self.data_sites_for_objs(changed_objs).await
            } {
                Ok(data_sites) => {
                    debug!("ABORT AFFECTED OBJS: {:?}", changed_objs);
                    #[cfg(feature = "occ_phase_profile")]
                    let _phase_guard =
                        super::phase_profile::guard(super::phase_profile::Phase::AbortCleanup);
                    self.sites_abort(&tid, changed_objs, &data_sites).await // with end
                }
                Err(error) => Err(error),
            };
            if Self::abort_cleanup_complete(&result) {
                // Fire-and-forget release of read pins on servers this transaction
                // only read (pinned but not written; write servers already dropped
                // their pins in the abort/end above). Queued so it never adds
                // latency to the abort path; the periodic flusher batches the RPCs.
                // Write-only transactions (no pins) skip this entirely.
                if !txn.pinned_servers.is_empty() {
                    let mut pinned_servers = std::mem::take(&mut txn.pinned_servers);
                    pinned_servers
                        .retain(|server_id| !txn.affected_objects.contains_key(server_id));
                    self.queue_pin_release(tid.clone(), pinned_servers);
                }
                self.cleanup_transaction_guarded(&tid, &mut txn);
            }
            result
        }
        .boxed()
    }
    fn begin(&self) -> BoxFuture<'_, Result<TxnId, TMError>> {
        let id = self.deps.hlc.now();
        let now = bifrost::utils::time::get_time();
        if self
            .transactions
            .insert(
                id.clone(),
                Arc::new(Mutex::new(Transaction {
                    data: HashMap::new(),
                    affected_objects: AffectedObjs::new(),
                    state: TxnState::Started,
                    last_activity: now,
                    pinned_servers: HashSet::new(),
                })),
            )
            .is_some()
        {
            error!("Transaction id existed: {:?}", id);
            future::ready(Err(TMError::TransactionIdExisted)).boxed()
        } else {
            self.txn_ids.lock().insert(id.clone());
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
            txn.last_activity = bifrost::utils::time::get_time(); // Update activity timestamp
            match self.deps.get_server_id_by_id(&id) {
                Some(server_id) => {
                    let have_cached_cell = txn.data.contains_key(&id);
                    if !have_cached_cell {
                        txn.data.insert(
                            id,
                            DataObject {
                                server: server_id,
                                cell: Some(cell),
                                expectation: CellExpectation::Absent,
                                new: true,
                                changed: true,
                                pinned: None,
                            },
                        );
                        Ok(TxnExecResult::Accepted(()))
                    } else {
                        let data_obj = txn.data.get_mut(&id).unwrap();
                        if data_obj.cell.is_some() {
                            return Ok(TxnExecResult::Error(WriteError::CellAlreadyExisted));
                        }
                        data_obj.cell = Some(cell);
                        data_obj.new = matches!(data_obj.expectation, CellExpectation::Absent)
                            && !data_obj.changed;
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
            txn.last_activity = bifrost::utils::time::get_time(); // Update activity timestamp
            match self.deps.get_server_id_by_id(&id) {
                Some(server_id) => {
                    // cell is already owned by the async block, no need to clone
                    if txn.data.contains_key(&id) {
                        let data_obj = txn.data.get_mut(&id).unwrap();
                        if data_obj.cell.is_none()
                            && matches!(data_obj.expectation, CellExpectation::Absent)
                        {
                            return Ok(TxnExecResult::Error(WriteError::CellDoesNotExisted));
                        }
                        data_obj.cell = Some(cell);
                        data_obj.changed = true;
                    } else {
                        let server = self
                            .get_data_site(server_id)
                            .await
                            .map_err(|_| TMError::CannotLocateCellServer)?;
                        let expectation = match self
                            .observe_version(&server, &tid, id.clone())
                            .await?
                        {
                            TxnExecResult::Accepted(version) => CellExpectation::Present(version),
                            TxnExecResult::Error(ReadError::CellDoesNotExisted) => {
                                return Ok(TxnExecResult::Error(WriteError::CellDoesNotExisted));
                            }
                            TxnExecResult::Error(error) => {
                                return Ok(TxnExecResult::Error(WriteError::ReadError(error)));
                            }
                            TxnExecResult::Rejected => return Ok(TxnExecResult::Rejected),
                            TxnExecResult::StateError(state) => {
                                return Ok(TxnExecResult::StateError(state));
                            }
                            TxnExecResult::Wait => {
                                unreachable!("observe_version retries waits before returning")
                            }
                        };
                        txn.data.insert(
                            id,
                            DataObject {
                                server: server_id,
                                cell: Some(cell),
                                expectation,
                                new: false,
                                changed: true,
                                pinned: None,
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
            match self.deps.get_server_id_by_id(&id) {
                Some(server_id) => {
                    if txn.data.contains_key(&id) {
                        let mut new_obj = false;
                        {
                            let data_obj = txn.data.get_mut(&id).unwrap();
                            // `cell: None` alone is not "does not exist": a
                            // pinned head-only read caches a present cell
                            // without materializing it. Only a prior in-txn
                            // removal (changed) or a certified-absent cell
                            // rejects the remove.
                            if data_obj.cell.is_none()
                                && (data_obj.changed
                                    || matches!(data_obj.expectation, CellExpectation::Absent))
                            {
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
                        let server = self
                            .get_data_site(server_id)
                            .await
                            .map_err(|_| TMError::CannotLocateCellServer)?;
                        let expectation = match self
                            .observe_version(&server, &tid, id.clone())
                            .await?
                        {
                            TxnExecResult::Accepted(version) => CellExpectation::Present(version),
                            TxnExecResult::Error(ReadError::CellDoesNotExisted) => {
                                return Ok(TxnExecResult::Error(WriteError::CellDoesNotExisted));
                            }
                            TxnExecResult::Error(error) => {
                                return Ok(TxnExecResult::Error(WriteError::ReadError(error)));
                            }
                            TxnExecResult::Rejected => return Ok(TxnExecResult::Rejected),
                            TxnExecResult::StateError(state) => {
                                return Ok(TxnExecResult::StateError(state));
                            }
                            TxnExecResult::Wait => {
                                unreachable!("observe_version retries waits before returning")
                            }
                        };
                        txn.data.insert(
                            id,
                            DataObject {
                                server: server_id,
                                cell: None,
                                expectation,
                                new: false,
                                changed: true,
                                pinned: None,
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
    /// A client for another server's data site.
    ///
    /// Public to the crate because the PARTICIPANT side needs it too: an
    /// in-doubt participant resolves its transaction by asking its peers, and
    /// the client pool that knows how to reach them lives here.
    pub(crate) async fn data_site_for(
        &self,
        server_id: u64,
    ) -> io::Result<Arc<data_site::AsyncServiceClient>> {
        self.get_data_site(server_id).await
    }

    async fn get_data_site(
        &self,
        server_id: u64,
    ) -> io::Result<Arc<data_site::AsyncServiceClient>> {
        if !self.data_sites.contains_key(&server_id) {
            let client = self.deps.get_member_by_server_id(server_id).await?;
            return Ok(self.data_sites.get_or_insert(server_id, || {
                data_site::AsyncServiceClient::new_with_service_id(
                    data_site::generate_scoped_service_id(
                        self.deps.database_runtime.group_name(),
                        self.deps.database_runtime.database_name(),
                    ),
                    &client,
                )
            }));
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
        match self.deps.get_server_id_by_id(id) {
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
    fn get_clock(&self) -> Hlc {
        self.deps.hlc.now()
    }
    fn merge_clock(&self, clock: Hlc) {
        self.deps.hlc.observe(clock);
    }
    fn get_transaction(&self, tid: &TxnId) -> Result<TxnMutex, TMError> {
        match self.transactions.get(tid) {
            Some(txn) => Ok(txn.clone()),
            _ => Err(TMError::TransactionNotFound),
        }
    }
    /// Coordinator full read. Read-your-writes and an already-materialized full
    /// cell shadow everything; a buffered remove and repeatable absence both
    /// read as missing. Otherwise the whole cell is fetched once via the
    /// participant `read` RPC — this is the single path that materializes the
    /// whole cell — and, if a prior partial read pinned the cell, the
    /// participant serves that pinned version so the full read is repeatable.
    async fn full_read<'a>(
        &self,
        tid: &TxnId,
        id: &Id,
        txn: &mut TxnGuard<'a>,
    ) -> Result<TxnExecResult<OwnedCell, ReadError>, TMError> {
        txn.last_activity = bifrost::utils::time::get_time();
        if let Some(data_obj) = txn.data.get(id) {
            if let Some(cell) = &data_obj.cell {
                return Ok(TxnExecResult::Accepted(cell.clone()));
            }
            // A buffered remove within this transaction, or repeatable absence.
            if data_obj.changed || matches!(data_obj.expectation, CellExpectation::Absent) {
                return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted));
            }
            // A prior partial read pinned the cell but never materialized the
            // whole cell: fall through to fetch it once from the pinned version.
        }

        match self.get_data_site_by_id(id).await {
            Ok((server_id, server)) => self.read_from_site(server_id, &server, tid, id, txn).await,
            Err(error) => {
                error!("{:?}", error);
                Err(TMError::CannotLocateCellServer)
            }
        }
    }

    /// Coordinator header read. Read-your-writes and an already-materialized full
    /// cell shadow everything; a buffered remove and repeatable absence read as
    /// missing; an already-pinned header is served from cache. Otherwise a
    /// `head(pin=true)` participant RPC pins the version and returns only the
    /// header — the whole cell is never transferred. The observed version is
    /// recorded as `Present(version)` so the read is certified at prepare.
    async fn head_read<'a>(
        &self,
        tid: &TxnId,
        id: &Id,
        txn: &mut TxnGuard<'a>,
    ) -> Result<TxnExecResult<CellHeader, ReadError>, TMError> {
        txn.last_activity = bifrost::utils::time::get_time();
        if let Some(data_obj) = txn.data.get(id) {
            if let Some(cell) = &data_obj.cell {
                return Ok(TxnExecResult::Accepted(cell.header.clone()));
            }
            if data_obj.changed || matches!(data_obj.expectation, CellExpectation::Absent) {
                return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted));
            }
            if let Some(header) = data_obj.pinned.as_ref().and_then(|p| p.header.as_ref()) {
                return Ok(TxnExecResult::Accepted(header.clone()));
            }
            // Pinned (e.g. only a projection was fetched so far) but the header
            // is not cached yet: fall through to fetch the header from the pin.
        }

        match self.get_data_site_by_id(id).await {
            Ok((server_id, server)) => self.head_from_site(server_id, &server, tid, id, txn).await,
            Err(error) => {
                error!("{:?}", error);
                Err(TMError::CannotLocateCellServer)
            }
        }
    }

    /// Coordinator projected read. Read-your-writes and an already-materialized
    /// full cell shadow everything (the projection is computed locally); a
    /// buffered remove and repeatable absence read as missing; an already-cached
    /// projection for these exact fields is served from cache. Otherwise a
    /// `read_selected` participant RPC pins the version and returns only the
    /// projection — the whole cell is never transferred. The projection's
    /// version is recorded as `Present(version)` so the read is certified.
    async fn selected_read<'a>(
        &self,
        tid: &TxnId,
        id: &Id,
        fields: &[u64],
        txn: &mut TxnGuard<'a>,
    ) -> Result<TxnExecResult<OwnedCell, ReadError>, TMError> {
        txn.last_activity = bifrost::utils::time::get_time();
        if let Some(data_obj) = txn.data.get(id) {
            if let Some(cell) = &data_obj.cell {
                return Ok(match self.select_from_cell(cell, fields) {
                    Ok(selected) => TxnExecResult::Accepted(selected),
                    Err(error) => TxnExecResult::Error(error),
                });
            }
            if data_obj.changed || matches!(data_obj.expectation, CellExpectation::Absent) {
                return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted));
            }
            if let Some(projection) = data_obj
                .pinned
                .as_ref()
                .and_then(|p| p.projections.get(fields))
            {
                return Ok(TxnExecResult::Accepted(projection.clone()));
            }
            // Pinned but this exact projection is not cached yet: fall through
            // to fetch it from the pinned version.
        }

        match self.get_data_site_by_id(id).await {
            Ok((server_id, server)) => {
                self.selected_from_site(server_id, &server, tid, id, fields, txn)
                    .await
            }
            Err(error) => {
                error!("{:?}", error);
                Err(TMError::CannotLocateCellServer)
            }
        }
    }

    fn select_from_cell(&self, cell: &OwnedCell, fields: &[u64]) -> Result<OwnedCell, ReadError> {
        if fields.is_empty() {
            return Ok(cell.clone());
        }

        let schema_id = cell.header.schema;
        let schema = self
            .deps
            .schemas()
            .get(&schema_id)
            .ok_or(ReadError::SchemaDoesNotExisted(schema_id.get()))?;

        let map = match &cell.data {
            OwnedValue::Map(map) => map,
            _ => return Err(ReadError::CellTypeIsNotMapForSelect),
        };

        let mut selected = Vec::with_capacity(fields.len());
        for field in fields {
            if let Some(index_path) = schema.id_index.get(field) {
                let path = index_path.iter().map(|id| *id as u64).collect_vec();
                trace!("Get into map for txn select {:?}", path);
                selected.push(map.get_in_by_ids(path.iter()).clone());
            } else {
                selected.push(map.get_by_key_id(*field).clone());
            }
        }

        Ok(OwnedCell {
            header: cell.header.clone(),
            data: OwnedValue::Array(selected),
        })
    }

    async fn read_from_site<'a>(
        &self,
        server_id: u64,
        server: &Arc<data_site::AsyncServiceClient>,
        tid: &TxnId,
        id: &Id,
        txn: &mut TxnGuard<'a>,
    ) -> Result<TxnExecResult<OwnedCell, ReadError>, TMError> {
        #[cfg(feature = "occ_phase_profile")]
        let _phase_guard = super::phase_profile::guard(super::phase_profile::Phase::ReadSiteRpc);
        let start_time = std::time::Instant::now();
        let mut attempt = 0u32;
        let self_server_id = self.deps.server_id;

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
                    self.merge_clock(dsr.clock);
                    let payload = dsr.payload;
                    match payload {
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
                                    let version = cell.header.version;
                                    data_obj.cell = Some(cell);
                                    data_obj.expectation = CellExpectation::Present(version);
                                }
                                return Ok(TxnExecResult::Accepted(
                                    data_obj.cell.as_ref().unwrap().clone(),
                                ));
                            } else {
                                // No entry exists - cache the remote value and return it
                                let version = cell.header.version;
                                let result = TxnExecResult::Accepted(cell.clone());
                                txn.data.insert(
                                    id.clone(),
                                    DataObject {
                                        server: server_id,
                                        expectation: CellExpectation::Present(version),
                                        cell: Some(cell),
                                        new: false,
                                        changed: false,
                                        pinned: None,
                                    },
                                );
                                return Ok(result);
                            }
                        }
                        TxnExecResult::Error(ReadError::CellDoesNotExisted) => {
                            let result = TxnExecResult::Error(ReadError::CellDoesNotExisted);
                            txn.data.insert(
                                id.clone(),
                                DataObject {
                                    server: server_id,
                                    cell: None,
                                    expectation: CellExpectation::Absent,
                                    changed: false,
                                    new: false,
                                    pinned: None,
                                },
                            );
                            return Ok(result);
                        }
                        TxnExecResult::Wait => {
                            // Backoff and retry
                            Self::backoff_wait(attempt, &self.wait_config).await?;
                            attempt += 1;
                            continue;
                        }
                        other => return Ok(other),
                    }
                }
                Err(e) => {
                    error!("{:?}", e);
                    return Err(TMError::RPCErrorFromCellServer);
                }
            }
        }
    }

    /// Issues a `head(pin=true)` participant RPC, pinning the served version so a
    /// later read in the same transaction repeats it, and caches only the header
    /// (never the whole cell). Records `Present(version)` for certification, or
    /// `Absent` on a missing cell (repeatable absence).
    async fn head_from_site<'a>(
        &self,
        server_id: u64,
        server: &Arc<data_site::AsyncServiceClient>,
        tid: &TxnId,
        id: &Id,
        txn: &mut TxnGuard<'a>,
    ) -> Result<TxnExecResult<CellHeader, ReadError>, TMError> {
        #[cfg(feature = "occ_phase_profile")]
        let _phase_guard = super::phase_profile::guard(super::phase_profile::Phase::ReadSiteRpc);
        let start_time = std::time::Instant::now();
        let mut attempt = 0u32;
        let self_server_id = self.deps.server_id;

        loop {
            if start_time.elapsed().as_millis() > self.wait_config.max_total_wait_ms as u128 {
                warn!("Head timeout for transaction {:?} on cell {:?}", tid, id);
                return Ok(TxnExecResult::Rejected);
            }

            // Partial read: pin=true snapshots the served version for repeatability.
            let head_response = server
                .head(self_server_id, self.get_clock(), tid.to_owned(), *id, true)
                .await;
            match head_response {
                Ok(dsr) => {
                    self.merge_clock(dsr.clock);
                    match dsr.payload {
                        TxnExecResult::Accepted(header) => {
                            let version = header.version;
                            // The participant pinned the served version and holds
                            // a segment guard for it. Remember this server so the
                            // pin is released promptly on commit/abort, even if a
                            // buffered write later shadows the read locally.
                            txn.pinned_servers.insert(server_id);
                            if let Some(data_obj) = txn.data.get_mut(id) {
                                // Read-your-writes: a buffered write/remove shadows the pin.
                                if data_obj.changed {
                                    return Ok(match &data_obj.cell {
                                        Some(cell) => TxnExecResult::Accepted(cell.header.clone()),
                                        None => TxnExecResult::Error(ReadError::CellDoesNotExisted),
                                    });
                                }
                                // Cache the header on the existing pinned entry;
                                // its recorded version is already Present(version).
                                let pinned =
                                    data_obj.pinned.get_or_insert_with(PinnedReadCache::default);
                                if pinned.header.is_none() {
                                    pinned.header = Some(header.clone());
                                }
                                return Ok(TxnExecResult::Accepted(header));
                            }
                            // Record the header-only read so it is in the certified
                            // read set as Present(version) — no whole-cell transfer.
                            txn.data.insert(
                                id.clone(),
                                DataObject {
                                    server: server_id,
                                    cell: None,
                                    expectation: CellExpectation::Present(version),
                                    changed: false,
                                    new: false,
                                    pinned: Some(PinnedReadCache {
                                        header: Some(header.clone()),
                                        projections: HashMap::new(),
                                    }),
                                },
                            );
                            return Ok(TxnExecResult::Accepted(header));
                        }
                        TxnExecResult::Error(ReadError::CellDoesNotExisted) => {
                            if !txn.data.contains_key(id) {
                                txn.data.insert(
                                    id.clone(),
                                    DataObject {
                                        server: server_id,
                                        cell: None,
                                        expectation: CellExpectation::Absent,
                                        changed: false,
                                        new: false,
                                        pinned: None,
                                    },
                                );
                            }
                            return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted));
                        }
                        TxnExecResult::Wait => {
                            Self::backoff_wait(attempt, &self.wait_config).await?;
                            attempt += 1;
                            continue;
                        }
                        other => return Ok(other),
                    }
                }
                Err(e) => {
                    error!("{:?}", e);
                    return Err(TMError::RPCErrorFromCellServer);
                }
            }
        }
    }

    /// Issues a `read_selected` participant RPC, pinning the served version so a
    /// later read in the same transaction repeats it, and caches only the
    /// returned projection (never the whole cell) keyed by the exact requested
    /// field order. Records `Present(version)` for certification, or `Absent` on
    /// a missing cell (repeatable absence).
    async fn selected_from_site<'a>(
        &self,
        server_id: u64,
        server: &Arc<data_site::AsyncServiceClient>,
        tid: &TxnId,
        id: &Id,
        fields: &[u64],
        txn: &mut TxnGuard<'a>,
    ) -> Result<TxnExecResult<OwnedCell, ReadError>, TMError> {
        #[cfg(feature = "occ_phase_profile")]
        let _phase_guard = super::phase_profile::guard(super::phase_profile::Phase::ReadSiteRpc);
        let start_time = std::time::Instant::now();
        let mut attempt = 0u32;
        let self_server_id = self.deps.server_id;

        loop {
            if start_time.elapsed().as_millis() > self.wait_config.max_total_wait_ms as u128 {
                warn!(
                    "Selected read timeout for transaction {:?} on cell {:?}",
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
                    fields.to_vec(),
                )
                .await;
            match read_response {
                Ok(dsr) => {
                    self.merge_clock(dsr.clock);
                    match dsr.payload {
                        TxnExecResult::Accepted(projection) => {
                            let version = projection.header.version;
                            // The participant pinned the served version and holds
                            // a segment guard for it. Remember this server so the
                            // pin is released promptly on commit/abort, even if a
                            // buffered write later shadows the read locally.
                            txn.pinned_servers.insert(server_id);
                            if let Some(data_obj) = txn.data.get_mut(id) {
                                // Read-your-writes: a buffered write/remove shadows the pin.
                                if data_obj.changed {
                                    return Ok(match &data_obj.cell {
                                        Some(cell) => match self.select_from_cell(cell, fields) {
                                            Ok(selected) => TxnExecResult::Accepted(selected),
                                            Err(error) => TxnExecResult::Error(error),
                                        },
                                        None => TxnExecResult::Error(ReadError::CellDoesNotExisted),
                                    });
                                }
                                // Cache the projection on the existing pinned entry;
                                // its recorded version is already Present(version).
                                let pinned =
                                    data_obj.pinned.get_or_insert_with(PinnedReadCache::default);
                                pinned
                                    .projections
                                    .entry(fields.to_vec())
                                    .or_insert_with(|| projection.clone());
                                return Ok(TxnExecResult::Accepted(projection));
                            }
                            // Record the projection-only read so it is in the
                            // certified read set as Present(version).
                            let mut projections = HashMap::new();
                            projections.insert(fields.to_vec(), projection.clone());
                            txn.data.insert(
                                id.clone(),
                                DataObject {
                                    server: server_id,
                                    cell: None,
                                    expectation: CellExpectation::Present(version),
                                    changed: false,
                                    new: false,
                                    pinned: Some(PinnedReadCache {
                                        header: None,
                                        projections,
                                    }),
                                },
                            );
                            return Ok(TxnExecResult::Accepted(projection));
                        }
                        TxnExecResult::Error(ReadError::CellDoesNotExisted) => {
                            if !txn.data.contains_key(id) {
                                txn.data.insert(
                                    id.clone(),
                                    DataObject {
                                        server: server_id,
                                        cell: None,
                                        expectation: CellExpectation::Absent,
                                        changed: false,
                                        new: false,
                                        pinned: None,
                                    },
                                );
                            }
                            return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted));
                        }
                        TxnExecResult::Wait => {
                            Self::backoff_wait(attempt, &self.wait_config).await?;
                            attempt += 1;
                            continue;
                        }
                        other => return Ok(other),
                    }
                }
                Err(e) => {
                    error!("{:?}", e);
                    return Err(TMError::RPCErrorFromCellServer);
                }
            }
        }
    }

    async fn observe_head_from_site(
        &self,
        server: &Arc<data_site::AsyncServiceClient>,
        tid: &TxnId,
        id: &Id,
    ) -> Result<TxnExecResult<CellHeader, ReadError>, TMError> {
        #[cfg(feature = "occ_phase_profile")]
        let _phase_guard = super::phase_profile::guard(super::phase_profile::Phase::ReadSiteRpc);
        let start_time = std::time::Instant::now();
        let mut attempt = 0u32;
        let self_server_id = self.deps.server_id;

        loop {
            // Check timeout
            if start_time.elapsed().as_millis() > self.wait_config.max_total_wait_ms as u128 {
                warn!("Head timeout for transaction {:?} on cell {:?}", tid, id);
                return Ok(TxnExecResult::Rejected);
            }

            // Use the transaction's own timestamp for internal blind observations so the
            // read timestamp recorded at the data site cannot advance beyond this tid.
            // Blind version observation: never pin. Blind update/remove targets
            // are written, not re-read, so pinning here would waste a segment
            // guard and needlessly materialize a participant transaction.
            let head_response = server
                .head(self_server_id, tid.clone(), tid.to_owned(), *id, false)
                .await;
            match head_response {
                Ok(dsr) => {
                    self.merge_clock(dsr.clock);
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
    async fn observe_version(
        &self,
        server: &Arc<data_site::AsyncServiceClient>,
        tid: &TxnId,
        id: Id,
    ) -> Result<TxnExecResult<u64, ReadError>, TMError> {
        Ok(match self.observe_head_from_site(server, tid, &id).await? {
            TxnExecResult::Accepted(header) => TxnExecResult::Accepted(header.version),
            TxnExecResult::Rejected => TxnExecResult::Rejected,
            TxnExecResult::Error(error) => TxnExecResult::Error(error),
            TxnExecResult::StateError(state) => TxnExecResult::StateError(state),
            TxnExecResult::Wait => unreachable!("observe_head_from_site retries waits"),
        })
    }
    fn generate_affected_objs(&self, txn: &mut TxnGuard) {
        let has_writes = txn.data.values().any(|data_obj| data_obj.changed);
        let mut affected_objs = AffectedObjs::new();
        if has_writes {
            for (id, data_obj) in txn.data.drain() {
                affected_objs
                    .entry(data_obj.server)
                    .or_insert_with(BTreeMap::new)
                    .insert(id, data_obj);
            }
        } else {
            txn.data.clear();
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
        deps: &Arc<TransactionManagerDeps>,
        config: &WaitConfig,
        tid: &TxnId,
        objs: &BTreeMap<Id, DataObject>,
        data_site: &Arc<data_site::AsyncServiceClient>,
        participants: &[u64],
    ) -> Result<DMPrepareResult, TMError> {
        let start_time = std::time::Instant::now();
        let mut attempt = 0u32;

        loop {
            // Check timeout
            if start_time.elapsed().as_millis() > config.max_total_wait_ms as u128 {
                warn!("Prepare timeout for transaction {:?}", tid);
                return Ok(DMPrepareResult::NotRealizable); // Give up
            }

            let coordinator_id = deps.server_id;
            let prepare_ops: Vec<_> = objs
                .iter()
                .map(|(id, data_obj)| PrepareOp {
                    id: *id,
                    expectation: data_obj.expectation.clone(),
                    intent: if data_obj.changed {
                        PrepareIntent::Write
                    } else {
                        PrepareIntent::Read
                    },
                })
                .collect();
            let deps_for_clock = deps.clone();
            let prepare_payload = data_site
                .prepare(
                    coordinator_id,
                    deps.hlc.now(),
                    tid.clone(),
                    prepare_ops,
                    participants.to_vec(),
                )
                .await
                .map_err(|_| -> TMError { TMError::RPCErrorFromCellServer })
                .map(move |prepare_res| -> DMPrepareResult {
                    deps_for_clock.hlc.observe(prepare_res.clock);
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
        // Who will hold a bracket. Exactly the servers `sites_commit` and
        // `sites_end` will address, which is the set an in-doubt participant
        // has to be able to ask: a server that only READ this transaction
        // writes no BEGIN and can answer nothing about its outcome, so
        // including it would only add peers that reply "I never heard of it"
        // -- indistinguishable from a peer that presumed abort.
        let participants: Vec<u64> = affected_objs
            .iter()
            .filter(|(_, objs)| objs.values().any(|obj| obj.changed))
            .map(|(server, _)| *server)
            .collect();
        let prepare_futures: FuturesUnordered<_> = affected_objs
            .iter()
            .map(|(server, objs)| {
                let participants = participants.clone();
                async move {
                    let data_site = data_sites.get(server).unwrap().clone();
                    let result = TransactionManager::site_prepare(
                        &self.deps,
                        &self.wait_config,
                        &tid,
                        &objs,
                        &data_site,
                        &participants,
                    )
                    .await;
                    #[cfg(test)]
                    Self::notify_prepare_results_observed(tid, objs);
                    result
                }
            })
            .collect();
        let results: Vec<Result<DMPrepareResult, TMError>> = prepare_futures.collect().await;
        Self::reduce_prepare_results(results)
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
                    .filter_map(|(cell_id, data_obj)| {
                        data_obj
                            .changed
                            .then(|| Self::commit_op_for_changed_data_obj(*cell_id, data_obj))
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
                self.merge_clock(dsr.clock);
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

    fn commit_op_for_changed_data_obj(cell_id: Id, data_obj: &DataObject) -> CommitOp {
        assert!(
            data_obj.changed,
            "unchanged observations should not be sent to commit"
        );
        match (&data_obj.cell, data_obj.new) {
            (None, false) => CommitOp::Remove(cell_id),
            (Some(cell), true) => CommitOp::Write(cell.clone()),
            (Some(cell), false) => CommitOp::Update(cell.clone()),
            (None, true) => panic!("invalid changed transaction state for {:?}", cell_id),
        }
    }

    async fn sites_abort(
        &self,
        tid: &TxnId,
        changed_objs: &AffectedObjs,
        data_sites: &DataSitesMap,
    ) -> Result<AbortResult, TMError> {
        let abort_futures: FuturesUnordered<_> = changed_objs
            .iter()
            .map(|(server_id, _)| {
                let server_id = *server_id;
                let data_site = data_sites.get(&server_id).unwrap().clone();
                async move {
                    (
                        server_id,
                        data_site.abort(self.get_clock(), tid.clone()).await,
                    )
                }
            })
            .collect();
        let abort_results: Vec<_> = abort_futures.collect().await;
        let mut rollback_failures = Vec::new();
        let mut sites_to_end = BTreeSet::new();
        let mut first_failure = None;
        let mut first_error = None;

        for (server_id, result) in abort_results {
            match result {
                Ok(asr) => {
                    let payload = asr.payload;
                    self.merge_clock(asr.clock);
                    match payload {
                        AbortResult::Success(failures) => {
                            sites_to_end.insert(server_id);
                            if let Some(mut failures) = failures {
                                rollback_failures.append(&mut failures);
                            }
                        }
                        AbortResult::CheckFailed(CheckError::AlreadyAborted) => {
                            sites_to_end.insert(server_id);
                        }
                        AbortResult::CheckFailed(CheckError::NotExisted) => {
                            // A prior attempt already ended this participant.
                        }
                        failure if first_failure.is_none() => first_failure = Some(failure),
                        _ => {}
                    }
                }
                Err(_) if first_error.is_none() => {
                    first_error = Some(TMError::RPCErrorFromCellServer)
                }
                Err(_) => {}
            }
        }

        let end_futures: FuturesUnordered<_> = sites_to_end
            .iter()
            .map(|server_id| {
                let server_id = *server_id;
                let data_site = data_sites.get(&server_id).unwrap().clone();
                async move {
                    (
                        server_id,
                        data_site.end(self.get_clock(), tid.clone()).await,
                    )
                }
            })
            .collect();
        let end_results: Vec<_> = end_futures.collect().await;
        for (server_id, result) in end_results {
            match result {
                Ok(response) => {
                    self.merge_clock(response.clock);
                    match response.payload {
                        EndResult::Success | EndResult::CheckFailed(CheckError::NotExisted) => {}
                        other => {
                            error!(
                                "Abort cleanup could not end participant {} for {:?}: {:?}",
                                server_id, tid, other
                            );
                            if first_error.is_none() {
                                first_error = Some(TMError::AssertionError);
                            }
                        }
                    }
                }
                Err(error) => {
                    debug!(
                        "Abort cleanup could not reach participant {} for {:?}: {:?}",
                        server_id, tid, error
                    );
                    if first_error.is_none() {
                        first_error = Some(TMError::RPCErrorFromCellServer);
                    }
                }
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }
        if let Some(failure) = first_failure {
            return Ok(failure);
        }
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
        // A coordinator that dies mid-decision, on demand.
        //
        // This is the one window 2PC cannot survive without a termination
        // protocol: the decision is made, some participants are told, and the
        // rest are left holding prepared entries they cannot resolve. It is
        // invisible from any single node, so without an injection point there
        // is no way to demonstrate the bug OR to prove a fix.
        //
        // `AffectedObjs` is a BTreeMap keyed by server id, so "the first N"
        // is a stable set across runs.
        #[cfg(test)]
        if let Some(limit) = end_fanout_limit(tid) {
            let told: Vec<u64> = match limit {
                EndFanoutPlan::First(sites) => changed_objs.keys().take(sites).copied().collect(),
                EndFanoutPlan::Only(server_id) => changed_objs
                    .keys()
                    .copied()
                    .filter(|id| *id == server_id)
                    .collect(),
            };
            for server_id in &told {
                let data_site = data_sites.get(server_id).unwrap();
                let _ = data_site.end(self.get_clock(), tid.clone()).await;
            }
            warn!(
                "TEST: coordinator for {:?} stops after telling {} of {} participants",
                tid,
                told.len(),
                changed_objs.len()
            );
            return Err(TMError::RPCErrorFromCellServer);
        }
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
                    self.merge_clock(result.clock);
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
    /// Best-effort, idempotent release of read pins on every server this
    /// transaction pinned a read on. Runs on commit/abort for both read-only and
    /// read-write transactions: a server the transaction only *read* (pinned) is
    /// never `sites_end`ed, so its pin (and the pin-only participant transaction)
    /// would otherwise linger until slow stale cleanup. Individual failures are
    /// logged at debug and ignored — the participant's stale cleanup remains the
    /// backstop, and a release must never fail commit/abort.
    /// Queue a completed transaction's read pins for release by the periodic
    /// flusher. Costs one lock+push on the commit/abort path; a lost release is
    /// reclaimed by the participant's stale cleanup.
    fn queue_pin_release(&self, tid: TxnId, pinned_servers: HashSet<u64>) {
        if pinned_servers.is_empty() {
            return;
        }
        self.pending_pin_releases.lock().push((tid, pinned_servers));
    }
    /// Drain the queued releases and send one batched RPC per participant.
    /// Best-effort: individual failures are logged and dropped; the
    /// participant's stale cleanup remains the backstop.
    async fn flush_pin_releases(&self) {
        let pending: Vec<(TxnId, HashSet<u64>)> = {
            let mut queue = self.pending_pin_releases.lock();
            if queue.is_empty() {
                return;
            }
            std::mem::take(&mut *queue)
        };
        let mut tids_by_server: HashMap<u64, Vec<TxnId>> = HashMap::new();
        for (tid, servers) in pending {
            for server_id in servers {
                tids_by_server
                    .entry(server_id)
                    .or_default()
                    .push(tid.clone());
            }
        }
        for (server_id, tids) in tids_by_server {
            match self.get_data_site(server_id).await {
                Ok(data_site) => match data_site.release_read_pins(tids).await {
                    Ok(response) => self.merge_clock(response.clock),
                    Err(error) => debug!(
                        "batched release_read_pins RPC to server {} failed: {:?}",
                        server_id, error
                    ),
                },
                Err(error) => debug!(
                    "cannot locate server {} to release read pins: {:?}",
                    server_id, error
                ),
            }
        }
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

    fn reduce_prepare_results<I>(results: I) -> Result<DMPrepareResult, TMError>
    where
        I: IntoIterator<Item = Result<DMPrepareResult, TMError>>,
    {
        let mut first_failure = None;
        let mut first_error = None;

        for result in results {
            match result {
                Ok(DMPrepareResult::Success) => {}
                Ok(other) if first_failure.is_none() => first_failure = Some(other),
                Ok(_) => {}
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(first_failure.unwrap_or(DMPrepareResult::Success))
        }
    }

    fn cleanup_transaction_guarded(&self, tid: &TxnId, txn: &mut Transaction) {
        txn.data.clear();
        txn.affected_objects.clear();
        txn.pinned_servers.clear();
        txn.state = TxnState::Cleanup;
        txn.last_activity = get_time();
        let _ = self.transactions.remove(tid);
        self.txn_ids.lock().remove(tid);
    }

    fn cleanup_transaction(&self, tid: &TxnId) {
        if let Some(txn) = self.transactions.get(tid) {
            let mut txn_guard = txn.lock_blocking();
            self.cleanup_transaction_guarded(tid, &mut txn_guard);
        } else {
            self.txn_ids.lock().remove(tid);
        }
    }

    fn abort_cleanup_complete(result: &Result<AbortResult, TMError>) -> bool {
        matches!(result, Ok(AbortResult::Success(None)))
    }

    /// Clean up stale transactions that have been abandoned by clients
    /// Should be called periodically by a background task
    /// Returns the number of transactions cleaned up
    pub fn cleanup_stale_transactions(&self, max_age_ms: i64) -> usize {
        let now = bifrost::utils::time::get_time();
        let cutoff = now - max_age_ms;
        let mut cleaned = 0;

        // Collect stale transaction IDs
        let stale_txns: Vec<TxnId> = {
            let txn_ids = self.txn_ids.lock();
            txn_ids
                .iter()
                .filter_map(|tid| {
                    if let Some(txn_mutex) = self.transactions.get(tid) {
                        // Try to get the lock without blocking
                        if let Some(txn_guard) = txn_mutex.try_lock() {
                            // Only Started transactions can be abandoned safely.
                            // Prepared transactions need a commit/abort decision, and
                            // Aborted transactions may still need rollback retries.
                            if txn_guard.last_activity < cutoff
                                && txn_guard.state == TxnState::Started
                            {
                                return Some(tid.clone());
                            }
                        }
                    }
                    None
                })
                .collect()
        };

        // Remove the stale transactions
        for tid in stale_txns {
            warn!(
                "Cleaning up stale transaction {:?} (likely client didn't call prepare/abort)",
                tid
            );
            self.cleanup_transaction(&tid);
            cleaned += 1;
        }

        cleaned
    }

    /// Performs the actual prepare operation (extracted for retry logic)
    async fn do_prepare(&self, tid: &TxnId) -> Result<TMPrepareResult, TMError> {
        let conclusion = {
            let txn_mutex = self.get_transaction(&tid)?;
            let mut txn = txn_mutex.lock().await;
            let result = {
                self.ensure_rw_state(&txn)?;
                {
                    #[cfg(feature = "occ_phase_profile")]
                    let _phase_guard = super::phase_profile::guard(
                        super::phase_profile::Phase::AffectedObjectGrouping,
                    );
                    self.generate_affected_objs(&mut txn);
                }
                let affect_objs = &txn.affected_objects;
                let data_sites = {
                    #[cfg(feature = "occ_phase_profile")]
                    let _phase_guard = super::phase_profile::guard(
                        super::phase_profile::Phase::PrepareParticipantLookup,
                    );
                    self.data_sites_for_objs(affect_objs).await?
                };
                let sites_prepare_result = {
                    #[cfg(feature = "occ_phase_profile")]
                    let _phase_guard =
                        super::phase_profile::guard(super::phase_profile::Phase::PrepareBarrier);
                    self.sites_prepare(&tid, affect_objs, &data_sites).await?
                };
                if sites_prepare_result == DMPrepareResult::Success {
                    let sites_commit_result = {
                        #[cfg(feature = "occ_phase_profile")]
                        let _phase_guard =
                            super::phase_profile::guard(super::phase_profile::Phase::CommitBarrier);
                        self.sites_commit(&tid, affect_objs, &data_sites).await?
                    };
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

    async fn run_prepare_lifecycle(
        self: Arc<Self>,
        tid: TxnId,
    ) -> Result<TMPrepareResult, TMError> {
        // Note: Automatic retry with fresh timestamps removed because it changes
        // the transaction ID mid-flight, breaking the client's reference.
        // For remaining NotRealizable errors, clients should retry with a new transaction.
        let result = self.do_prepare(&tid).await;
        match &result {
            Ok(TMPrepareResult::Success) => {}
            Ok(_) | Err(_) => {
                let _ = self.abort(tid.clone()).await;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::schema::Schema;
    use crate::ram::schema::SchemaVid;
    use crate::ram::tests::default_fields;
    use crate::ram::types::{OwnedMap, OwnedValue};
    use crate::server::transactions;
    use crate::server::{NebServer, ServerOptions, Service};
    use dovahkiin::types::custom_types::id::Id;
    use dovahkiin::types::Map;
    use futures::future::join_all;

    #[test]
    fn scoped_transaction_manager_service_ids_differ_between_databases() {
        let group = "group_a";
        assert_ne!(
            generate_scoped_service_id(group, "db_a"),
            generate_scoped_service_id(group, "db_b")
        );
    }

    #[test]
    fn data_object_pinned_representation_defers_full_cell() {
        let obj = DataObject {
            server: 1,
            cell: None,
            expectation: CellExpectation::Absent,
            changed: false,
            new: false,
            pinned: Some(PinnedReadCache {
                header: None,
                projections: HashMap::new(),
            }),
        };
        assert!(obj.pinned.is_some());
        assert!(obj.cell.is_none());
        assert!(obj.pinned.as_ref().unwrap().header.is_none());
        assert!(obj.pinned.as_ref().unwrap().projections.is_empty());
    }

    #[cfg(feature = "occ_phase_profile")]
    #[test]
    fn coordinator_profile_covers_every_existing_protocol_boundary() {
        let source = include_str!("manager.rs");
        let production_source = source
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .map(|(production_source, _)| production_source)
            .unwrap_or(source);
        for phase in [
            "Phase::ReadSiteRpc",
            "Phase::AffectedObjectGrouping",
            "Phase::PrepareParticipantLookup",
            "Phase::PrepareBarrier",
            "Phase::CommitBarrier",
            "Phase::AbortParticipantLookup",
            "Phase::AbortCleanup",
            "Phase::EndParticipantLookup",
            "Phase::EndCleanup",
        ] {
            assert!(
                production_source.contains(phase),
                "expected manager.rs to reference {}",
                phase
            );
        }
    }

    async fn scoped_txn_client_for_database(
        address: &str,
        group_name: &str,
        database_name: &str,
    ) -> Arc<transactions::manager::AsyncServiceClient> {
        transactions::new_async_client_for_database(&address.to_string(), group_name, database_name)
            .await
            .unwrap()
    }

    async fn start_manager_test_server(address: &str, group: &str) -> Arc<NebServer> {
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: crate::ram::segs::SEGMENT_SIZE,
                db_size: crate::ram::segs::SEGMENT_SIZE,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell, Service::Transaction],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &address.to_string(),
            &group.to_string(),
            async |_| {},
        )
        .await
        .unwrap()
    }

    fn install_basic_schema(runtime: &Arc<crate::server::DatabaseRuntime>) -> Schema {
        let schema = Schema::new_with_id(
            1,
            &String::from("txn_test"),
            None,
            default_fields(),
            false,
            false,
        );
        runtime.meta().schemas.debug_only_new_schema(schema.clone());
        schema
    }

    async fn seed_counter_cell(
        runtime: &Arc<crate::server::DatabaseRuntime>,
        schema_id: u32,
        id: Id,
        score: u64,
    ) {
        let mut data = OwnedMap::new();
        data.insert(&String::from("id"), OwnedValue::I64(id.bits() as i64));
        data.insert(&String::from("score"), OwnedValue::U64(score));
        data.insert(
            &String::from("name"),
            OwnedValue::String(format!("cell-{score}")),
        );
        let mut cell = crate::ram::cell::OwnedCell::new_with_id(
            SchemaVid(schema_id),
            &id,
            OwnedValue::Map(data),
        );
        runtime.chunks().write_cell(&mut cell).unwrap();
    }

    fn counter_cell(schema_id: u32, id: Id, score: u64) -> OwnedCell {
        let mut data = OwnedMap::new();
        data.insert(&String::from("id"), OwnedValue::I64(id.bits() as i64));
        data.insert(&String::from("score"), OwnedValue::U64(score));
        data.insert(
            &String::from("name"),
            OwnedValue::String(format!("cell-{score}")),
        );
        OwnedCell::new_with_id(SchemaVid(schema_id), &id, OwnedValue::Map(data))
    }

    #[test]
    fn changed_existing_replacement_builds_update_commit_op() {
        let id = Id::allocated(0, 0, 7001);
        let cell = counter_cell(1, id, 9);
        let commit_op = TransactionManager::commit_op_for_changed_data_obj(
            id,
            &DataObject {
                server: 1,
                cell: Some(cell.clone()),
                expectation: CellExpectation::Present(3),
                changed: true,
                new: false,
                pinned: None,
            },
        );

        match commit_op {
            CommitOp::Update(updated) => assert_eq!(updated.data, cell.data),
            other => panic!("expected update commit op, got {:?}", other),
        }
    }

    #[test]
    fn prepare_result_reduction_waits_for_all_and_returns_first_failure() {
        let results = vec![
            Ok(DMPrepareResult::Success),
            Ok(DMPrepareResult::NotRealizable),
            Ok(DMPrepareResult::Success),
        ];

        assert_eq!(
            TransactionManager::reduce_prepare_results(results).unwrap(),
            DMPrepareResult::NotRealizable
        );
    }

    #[test]
    fn prepare_result_reduction_prefers_rpc_error_after_all_votes_settle() {
        let results = vec![
            Ok(DMPrepareResult::NotRealizable),
            Err(TMError::RPCErrorFromCellServer),
            Ok(DMPrepareResult::Success),
        ];

        assert_eq!(
            TransactionManager::reduce_prepare_results(results),
            Err(TMError::RPCErrorFromCellServer)
        );
    }

    #[test]
    fn abort_cleanup_requires_empty_rollback_failures() {
        assert!(TransactionManager::abort_cleanup_complete(&Ok(
            AbortResult::Success(None)
        )));
        assert!(!TransactionManager::abort_cleanup_complete(&Ok(
            AbortResult::Success(Some(vec![]))
        )));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_cleanup_preserves_abort_decisions_for_retry() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_manager_stale_abort_retry";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let txn = scoped_txn_client_for_database(address, group, group).await;
        let tid = txn.begin().await.unwrap().unwrap();

        let txn_lock = manager.get_transaction(&tid).unwrap();
        {
            let mut txn_guard = txn_lock.lock().await;
            txn_guard.state = TxnState::Aborted;
            txn_guard.last_activity = 0;
        }

        assert_eq!(manager.cleanup_stale_transactions(1), 0);
        assert_eq!(manager.transaction_count(), 1);
        assert_eq!(txn_lock.lock().await.state, TxnState::Aborted);

        manager.cleanup_transaction(&tid);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn affected_objs_retains_read_dependencies_for_rw_transaction() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_manager_affected_objs_rw";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let read_id = Id::allocated(0, 0, 7101);
        let write_id = Id::allocated(0, 0, 7102);
        let txn_mutex = Mutex::new(Transaction {
            data: HashMap::from([
                (
                    read_id,
                    DataObject {
                        server: 1,
                        cell: Some(counter_cell(1, read_id, 1)),
                        expectation: CellExpectation::Present(3),
                        changed: false,
                        new: false,
                        pinned: None,
                    },
                ),
                (
                    write_id,
                    DataObject {
                        server: 1,
                        cell: Some(counter_cell(1, write_id, 2)),
                        expectation: CellExpectation::Present(4),
                        changed: true,
                        new: false,
                        pinned: None,
                    },
                ),
            ]),
            affected_objects: AffectedObjs::new(),
            state: TxnState::Started,
            last_activity: get_time(),
            pinned_servers: HashSet::new(),
        });

        let mut txn = txn_mutex.lock().await;
        manager.generate_affected_objs(&mut txn);

        assert!(
            txn.data.is_empty(),
            "drained transaction data should be cleared"
        );
        let participant = txn
            .affected_objects
            .get(&1)
            .expect("read-write transaction should keep participant dependencies");
        assert_eq!(participant.len(), 2);
        assert!(participant.contains_key(&read_id));
        assert!(participant.contains_key(&write_id));

        drop(txn);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn affected_objs_read_only_transaction_clears_cached_data_locally() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_manager_affected_objs_ro";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let read_id = Id::allocated(0, 0, 7201);
        let txn_mutex = Mutex::new(Transaction {
            data: HashMap::from([(
                read_id,
                DataObject {
                    server: 1,
                    cell: Some(counter_cell(1, read_id, 3)),
                    expectation: CellExpectation::Present(8),
                    changed: false,
                    new: false,
                    pinned: None,
                },
            )]),
            affected_objects: AffectedObjs::new(),
            state: TxnState::Started,
            last_activity: get_time(),
            pinned_servers: HashSet::new(),
        });

        let mut txn = txn_mutex.lock().await;
        manager.generate_affected_objs(&mut txn);

        assert!(
            txn.data.is_empty(),
            "read-only transaction cache should be cleared"
        );
        assert!(
            txn.affected_objects.is_empty(),
            "read-only transaction should not retain participants for prepare"
        );

        drop(txn);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blind_mutation_records_update_version() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_occ_blind_update_version";
        let server = start_manager_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_basic_schema(&runtime);
        let cell_id = Id::allocated(0, 0, 7301);

        let mut seeded = counter_cell(schema.vid.get(), cell_id, 2);
        runtime.chunks().write_cell(&mut seeded).unwrap();
        let original = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();

        let txn = scoped_txn_client_for_database(address, group, group).await;
        let tid = txn.begin().await.unwrap().unwrap();

        let blind_update = counter_cell(schema.vid.get(), cell_id, 7);
        assert_eq!(
            txn.update(tid.clone(), blind_update)
                .await
                .unwrap()
                .unwrap(),
            TxnExecResult::Accepted(())
        );

        let txn_lock = runtime
            .txn_manager()
            .unwrap()
            .transactions
            .get(&tid)
            .expect("transaction should still be live after blind update");
        let txn_state = txn_lock.lock().await;
        let data_obj = txn_state
            .data
            .get(&cell_id)
            .expect("blind update should cache the target cell");
        assert_eq!(
            data_obj.expectation,
            CellExpectation::Present(original.header.version)
        );
        assert!(data_obj.changed);

        drop(txn_state);
        let _ = txn.abort(tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blind_mutation_records_remove_version() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_occ_blind_remove_version";
        let server = start_manager_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_basic_schema(&runtime);
        let cell_id = Id::allocated(0, 0, 7302);

        let mut seeded = counter_cell(schema.vid.get(), cell_id, 4);
        runtime.chunks().write_cell(&mut seeded).unwrap();
        let original = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();

        let txn = scoped_txn_client_for_database(address, group, group).await;
        let tid = txn.begin().await.unwrap().unwrap();

        assert_eq!(
            txn.remove(tid.clone(), cell_id).await.unwrap().unwrap(),
            TxnExecResult::Accepted(())
        );

        let txn_lock = runtime
            .txn_manager()
            .unwrap()
            .transactions
            .get(&tid)
            .expect("transaction should still be live after blind remove");
        let txn_state = txn_lock.lock().await;
        let data_obj = txn_state
            .data
            .get(&cell_id)
            .expect("blind remove should cache the target cell");
        assert_eq!(
            data_obj.expectation,
            CellExpectation::Present(original.header.version)
        );
        assert!(data_obj.changed);

        drop(txn_state);
        let _ = txn.abort(tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_transaction_commits_leave_manager_empty() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_manager_cleanup_single_db";
        let server = start_manager_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_basic_schema(&runtime);
        let cell_ids = (0..8)
            .map(|index| Id::allocated(0, 0, (index + 1) as u64))
            .collect::<Vec<_>>();

        for (index, cell_id) in cell_ids.iter().enumerate() {
            seed_counter_cell(&runtime, schema.vid.get(), *cell_id, index as u64).await;
        }

        let results = join_all((0..48).map(|worker| {
            let cell_ids = cell_ids.clone();
            let address = address.to_string();
            let group = group.to_string();
            async move {
                let txn_client = scoped_txn_client_for_database(&address, &group, &group).await;
                let txn_id = txn_client.begin().await.unwrap().unwrap();
                let target = cell_ids[worker % cell_ids.len()];
                match txn_client
                    .read(txn_id.clone(), target)
                    .await
                    .unwrap()
                    .unwrap()
                {
                    TxnExecResult::Accepted(mut cell) => {
                        let mut data = cell.data.Map().unwrap().clone();
                        let next_score = *data.get("score").u64().unwrap() + 1;
                        data.insert(&String::from("score"), OwnedValue::U64(next_score));
                        cell.data = OwnedValue::Map(data);
                        txn_client
                            .update(txn_id.clone(), cell)
                            .await
                            .unwrap()
                            .unwrap();
                        match txn_client.prepare(txn_id.clone()).await.unwrap().unwrap() {
                            TMPrepareResult::Success => {
                                let end = txn_client.commit(txn_id.clone()).await.unwrap().unwrap();
                                assert!(
                                    matches!(
                                        end,
                                        EndResult::Success | EndResult::SomeLocksNotReleased { .. }
                                    ),
                                    "unexpected commit result: {:?}",
                                    end
                                );
                            }
                            TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable) => {}
                            other => panic!("unexpected prepare result: {:?}", other),
                        }
                    }
                    other => panic!("unexpected read result: {:?}", other),
                }
            }
        }))
        .await;

        for result in results {
            result;
        }

        assert_eq!(
            runtime.txn_manager().unwrap().transaction_count(),
            0,
            "all committed transactions should be cleaned from manager state"
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_database_transaction_cleanup_stays_database_local() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_manager_cleanup_multi_db";
        let server = start_manager_test_server(address, group).await;
        let analytics_runtime = server.ensure_database_runtime("analytics").await.unwrap();
        let default_runtime = server.current_database();
        let default_schema = install_basic_schema(&default_runtime);
        let analytics_schema = install_basic_schema(&analytics_runtime);
        let default_cell = Id::allocated(0, 0, 1001);
        let analytics_cell = Id::allocated(0, 0, 2001);

        seed_counter_cell(&default_runtime, default_schema.vid.get(), default_cell, 1).await;
        seed_counter_cell(
            &analytics_runtime,
            analytics_schema.vid.get(),
            analytics_cell,
            1,
        )
        .await;

        let tasks = join_all((0..40).map(|index| {
            let address = address.to_string();
            let group = group.to_string();
            let database_name = if index % 2 == 0 {
                group.to_string()
            } else {
                "analytics".to_string()
            };
            let cell_id = if index % 2 == 0 {
                default_cell
            } else {
                analytics_cell
            };
            async move {
                let txn_client =
                    scoped_txn_client_for_database(&address, &group, &database_name).await;
                let txn_id = txn_client.begin().await.unwrap().unwrap();
                match txn_client
                    .read(txn_id.clone(), cell_id)
                    .await
                    .unwrap()
                    .unwrap()
                {
                    TxnExecResult::Accepted(mut cell) => {
                        let mut data = cell.data.Map().unwrap().clone();
                        let next_score = *data.get("score").u64().unwrap() + 1;
                        data.insert(&String::from("score"), OwnedValue::U64(next_score));
                        cell.data = OwnedValue::Map(data);
                        txn_client
                            .update(txn_id.clone(), cell)
                            .await
                            .unwrap()
                            .unwrap();
                        if index % 3 == 0 {
                            let abort_result =
                                txn_client.abort(txn_id.clone()).await.unwrap().unwrap();
                            assert!(matches!(abort_result, AbortResult::Success(_)));
                        } else {
                            match txn_client.prepare(txn_id.clone()).await.unwrap().unwrap() {
                                TMPrepareResult::Success => {
                                    let end =
                                        txn_client.commit(txn_id.clone()).await.unwrap().unwrap();
                                    assert!(
                                        matches!(
                                            end,
                                            EndResult::Success
                                                | EndResult::SomeLocksNotReleased { .. }
                                        ),
                                        "unexpected commit result: {:?}",
                                        end
                                    );
                                }
                                TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable) => {
                                }
                                other => panic!("unexpected prepare result: {:?}", other),
                            }
                        }
                    }
                    other => panic!("unexpected read result: {:?}", other),
                }
            }
        }))
        .await;

        for task in tasks {
            task;
        }

        assert_eq!(
            default_runtime.txn_manager().unwrap().transaction_count(),
            0
        );
        assert_eq!(
            analytics_runtime.txn_manager().unwrap().transaction_count(),
            0
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_failure_racing_with_explicit_abort_leaves_manager_empty() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_manager_prepare_abort_race";
        let server = start_manager_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_basic_schema(&runtime);
        let hot_cell = Id::allocated(0, 0, 3001);
        seed_counter_cell(&runtime, schema.vid.get(), hot_cell, 1).await;

        let txn_client = scoped_txn_client_for_database(address, group, group).await;
        for iteration in 0..48 {
            let writer_tid = txn_client.begin().await.unwrap().unwrap();
            let reader_tid = txn_client.begin().await.unwrap().unwrap();

            match txn_client
                .read(reader_tid.clone(), hot_cell)
                .await
                .unwrap()
                .unwrap()
            {
                TxnExecResult::Accepted(_) => {}
                other => panic!("unexpected reader result: {:?}", other),
            }

            let mut data = OwnedMap::new();
            data.insert(&String::from("id"), OwnedValue::I64(hot_cell.bits() as i64));
            data.insert(
                &String::from("score"),
                OwnedValue::U64((iteration + 2) as u64),
            );
            data.insert(
                &String::from("name"),
                OwnedValue::String(format!("writer-{iteration}")),
            );
            let updated_cell = crate::ram::cell::OwnedCell::new_with_id(
                schema.vid,
                &hot_cell,
                OwnedValue::Map(data),
            );
            match txn_client
                .update(writer_tid.clone(), updated_cell)
                .await
                .unwrap()
                .unwrap()
            {
                TxnExecResult::Accepted(()) => {}
                other => panic!("unexpected writer update result: {:?}", other),
            }

            let prepare_client = txn_client.clone();
            let abort_client = txn_client.clone();
            let prepare_tid = writer_tid.clone();
            let abort_tid = writer_tid.clone();

            let (prepare_result, abort_result) = tokio::join!(
                async move { prepare_client.prepare(prepare_tid).await.unwrap() },
                async move { abort_client.abort(abort_tid).await.unwrap() }
            );

            match prepare_result {
                Ok(TMPrepareResult::Success)
                | Ok(TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable))
                | Ok(TMPrepareResult::DMPrepareError(DMPrepareResult::Wait))
                | Ok(TMPrepareResult::DMPrepareError(DMPrepareResult::TransactionNotExisted))
                | Err(TMError::InvalidTransactionState(TxnState::Cleanup))
                | Err(TMError::TransactionNotFound) => {}
                other => panic!("unexpected prepare result in race: {:?}", other),
            }

            match abort_result {
                Ok(AbortResult::Success(_))
                | Ok(AbortResult::CheckFailed(CheckError::AlreadyAborted))
                | Ok(AbortResult::CheckFailed(CheckError::AlreadyCleanup))
                | Err(TMError::TransactionNotFound) => {}
                other => panic!("unexpected abort result in race: {:?}", other),
            }

            let _ = txn_client.abort(reader_tid.clone()).await;
        }

        assert_eq!(
            runtime.txn_manager().unwrap().transaction_count(),
            0,
            "prepare/abort races should not leak manager transaction state"
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_database_prepare_abort_races_stay_isolated() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_manager_prepare_abort_multi_db";
        let server = start_manager_test_server(address, group).await;
        let analytics_runtime = server.ensure_database_runtime("analytics").await.unwrap();
        let default_runtime = server.current_database();
        let default_schema = install_basic_schema(&default_runtime);
        let analytics_schema = install_basic_schema(&analytics_runtime);
        for index in 0..24 {
            let cell = Id::allocated(0, 0, 4001 + index);
            seed_counter_cell(&default_runtime, default_schema.vid.get(), cell, 1).await;
            seed_counter_cell(&analytics_runtime, analytics_schema.vid.get(), cell, 1).await;
        }

        let results = join_all((0..48).map(|iteration| {
            let address = address.to_string();
            let group = group.to_string();
            async move {
                let (database_name, schema_id) = if iteration % 2 == 0 {
                    (group.clone(), default_schema.vid)
                } else {
                    ("analytics".to_string(), analytics_schema.vid)
                };
                // Each writer/reader pair shares a cell so the newer reader forces the
                // older writer's prepare failure. Using the same IDs in both databases
                // exercises service scoping without introducing unrelated intra-database
                // lock contention between pairs.
                let hot_cell = Id::allocated(0, 0, 4001 + (iteration / 2) as u64);
                let txn_client =
                    scoped_txn_client_for_database(&address, &group, &database_name).await;
                let writer_tid = txn_client.begin().await.unwrap().unwrap();
                let reader_tid = txn_client.begin().await.unwrap().unwrap();

                match txn_client
                    .read(reader_tid.clone(), hot_cell)
                    .await
                    .unwrap()
                    .unwrap()
                {
                    TxnExecResult::Accepted(_) => {}
                    other => panic!("unexpected reader result: {:?}", other),
                }

                let mut data = OwnedMap::new();
                data.insert(&String::from("id"), OwnedValue::I64(hot_cell.bits() as i64));
                data.insert(
                    &String::from("score"),
                    OwnedValue::U64((iteration + 10) as u64),
                );
                data.insert(
                    &String::from("name"),
                    OwnedValue::String(format!("{database_name}-{iteration}")),
                );
                let updated_cell = crate::ram::cell::OwnedCell::new_with_id(
                    schema_id,
                    &hot_cell,
                    OwnedValue::Map(data),
                );
                match txn_client
                    .update(writer_tid.clone(), updated_cell)
                    .await
                    .unwrap()
                    .unwrap()
                {
                    TxnExecResult::Accepted(()) => {}
                    other => panic!("unexpected update result: {:?}", other),
                }

                let prepare_client = txn_client.clone();
                let abort_client = txn_client.clone();
                let prepare_tid = writer_tid.clone();
                let abort_tid = writer_tid.clone();

                let (prepare_result, abort_result) = tokio::join!(
                    async move { prepare_client.prepare(prepare_tid).await.unwrap() },
                    async move { abort_client.abort(abort_tid).await.unwrap() }
                );

                match prepare_result {
                    Ok(TMPrepareResult::Success)
                    | Ok(TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable))
                    | Ok(TMPrepareResult::DMPrepareError(DMPrepareResult::Wait))
                    | Ok(TMPrepareResult::DMPrepareError(DMPrepareResult::TransactionNotExisted))
                    | Err(TMError::InvalidTransactionState(TxnState::Cleanup))
                    | Err(TMError::TransactionNotFound) => {}
                    other => panic!("unexpected prepare result in race: {:?}", other),
                }

                match abort_result {
                    Ok(AbortResult::Success(_))
                    | Ok(AbortResult::CheckFailed(CheckError::AlreadyAborted))
                    | Ok(AbortResult::CheckFailed(CheckError::AlreadyCleanup))
                    | Err(TMError::TransactionNotFound) => {}
                    other => panic!("unexpected abort result in race: {:?}", other),
                }

                let _ = txn_client.abort(reader_tid.clone()).await;
            }
        }))
        .await;

        for result in results {
            result;
        }

        assert_eq!(
            default_runtime.txn_manager().unwrap().transaction_count(),
            0
        );
        assert_eq!(
            analytics_runtime.txn_manager().unwrap().transaction_count(),
            0
        );
        server.shutdown().await;
    }
}

impl Drop for TransactionManager {
    fn drop(&mut self) {
        // Signal background cleanup task to shutdown
        self.shutdown.store(true, Ordering::Relaxed);
        debug!("TransactionManager dropped, signaling cleanup task to stop");
    }
}
