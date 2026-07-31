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
use std::ops::Bound::{Excluded, Unbounded};
// Use async mutex because this module is a distributed coordinator
use async_std::sync::{Mutex, MutexGuard};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
#[cfg(test)]
use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::{mpsc, oneshot};

#[cfg(test)]
struct CompletionCleanupPauseState {
    entered: AtomicBool,
    entered_notify: Notify,
    released: parking_lot::Mutex<bool>,
    released_condvar: parking_lot::Condvar,
}

#[cfg(test)]
struct CompletionCleanupPauseHandle {
    tid: TxnId,
    state: Arc<CompletionCleanupPauseState>,
}

#[cfg(test)]
impl CompletionCleanupPauseHandle {
    async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    fn release(&self) {
        let mut released = self.state.released.lock();
        if !*released {
            *released = true;
            self.state.released_condvar.notify_all();
        }
    }
}

#[cfg(test)]
impl Drop for CompletionCleanupPauseHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = completion_cleanup_pause_hooks().lock();
        if hooks
            .get(&self.tid)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            hooks.remove(&self.tid);
        }
    }
}

#[cfg(test)]
static COMPLETION_CLEANUP_PAUSE_HOOKS: OnceLock<
    parking_lot::Mutex<BTreeMap<TxnId, Arc<CompletionCleanupPauseState>>>,
> = OnceLock::new();

#[cfg(test)]
fn completion_cleanup_pause_hooks(
) -> &'static parking_lot::Mutex<BTreeMap<TxnId, Arc<CompletionCleanupPauseState>>> {
    COMPLETION_CLEANUP_PAUSE_HOOKS.get_or_init(|| parking_lot::Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn install_completion_cleanup_pause(tid: TxnId) -> CompletionCleanupPauseHandle {
    let state = Arc::new(CompletionCleanupPauseState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: parking_lot::Mutex::new(false),
        released_condvar: parking_lot::Condvar::new(),
    });
    completion_cleanup_pause_hooks()
        .lock()
        .insert(tid, state.clone());
    CompletionCleanupPauseHandle { tid, state }
}

#[cfg(test)]
fn pause_after_completion_cleanup(tid: &TxnId) {
    let Some(state) = completion_cleanup_pause_hooks().lock().remove(tid) else {
        return;
    };
    state.entered.store(true, Ordering::SeqCst);
    state.entered_notify.notify_waiters();
    let mut released = state.released.lock();
    while !*released {
        state.released_condvar.wait(&mut released);
    }
}

#[cfg(test)]
struct RetirementRetryPauseState {
    entered: AtomicBool,
    entered_notify: Notify,
    released: AtomicBool,
    released_notify: Notify,
}

#[cfg(test)]
struct RetirementRetryPauseHandle {
    tid: TxnId,
    state: Arc<RetirementRetryPauseState>,
}

#[cfg(test)]
impl RetirementRetryPauseHandle {
    async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    fn release(&self) {
        if !self.state.released.swap(true, Ordering::SeqCst) {
            self.state.released_notify.notify_waiters();
        }
    }
}

#[cfg(test)]
impl Drop for RetirementRetryPauseHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = retirement_retry_pause_hooks().lock();
        if hooks
            .get(&self.tid)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            hooks.remove(&self.tid);
        }
    }
}

#[cfg(test)]
static RETIREMENT_RETRY_PAUSE_HOOKS: OnceLock<
    parking_lot::Mutex<BTreeMap<TxnId, Arc<RetirementRetryPauseState>>>,
> = OnceLock::new();

#[cfg(test)]
fn retirement_retry_pause_hooks(
) -> &'static parking_lot::Mutex<BTreeMap<TxnId, Arc<RetirementRetryPauseState>>> {
    RETIREMENT_RETRY_PAUSE_HOOKS.get_or_init(|| parking_lot::Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn install_retirement_retry_pause(tid: TxnId) -> RetirementRetryPauseHandle {
    let state = Arc::new(RetirementRetryPauseState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: AtomicBool::new(false),
        released_notify: Notify::new(),
    });
    retirement_retry_pause_hooks()
        .lock()
        .insert(tid, state.clone());
    RetirementRetryPauseHandle { tid, state }
}

#[cfg(test)]
async fn pause_after_retirement_lookup(tid: &TxnId) {
    let Some(state) = retirement_retry_pause_hooks().lock().remove(tid) else {
        return;
    };
    state.entered.store(true, Ordering::SeqCst);
    state.entered_notify.notify_waiters();
    while !state.released.load(Ordering::SeqCst) {
        state.released_notify.notified().await;
    }
}

type TxnMutex = Arc<Mutex<Transaction>>;
type TxnGuard<'a> = MutexGuard<'a, Transaction>;
type AffectedObjs = BTreeMap<u64, BTreeMap<Id, DataObject>>; // server_id as key
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

#[cfg(test)]
struct CompletionBoundaryClockState {
    now_ms: AtomicI64,
    after_cleanup_ms: i64,
}

#[cfg(test)]
struct CompletionBoundaryClockHandle {
    tid: TxnId,
    state: Arc<CompletionBoundaryClockState>,
}

#[cfg(test)]
impl Drop for CompletionBoundaryClockHandle {
    fn drop(&mut self) {
        let mut clocks = completion_boundary_clocks().lock();
        if clocks
            .get(&self.tid)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            clocks.remove(&self.tid);
        }
    }
}

#[cfg(test)]
static COMPLETION_BOUNDARY_CLOCKS: OnceLock<
    parking_lot::Mutex<BTreeMap<TxnId, Arc<CompletionBoundaryClockState>>>,
> = OnceLock::new();

#[cfg(test)]
fn completion_boundary_clocks(
) -> &'static parking_lot::Mutex<BTreeMap<TxnId, Arc<CompletionBoundaryClockState>>> {
    COMPLETION_BOUNDARY_CLOCKS.get_or_init(|| parking_lot::Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn install_completion_boundary_clock(
    tid: TxnId,
    before_cleanup_ms: i64,
    after_cleanup_ms: i64,
) -> CompletionBoundaryClockHandle {
    let state = Arc::new(CompletionBoundaryClockState {
        now_ms: AtomicI64::new(before_cleanup_ms),
        after_cleanup_ms,
    });
    completion_boundary_clocks()
        .lock()
        .insert(tid, state.clone());
    CompletionBoundaryClockHandle { tid, state }
}

#[cfg(test)]
fn completion_boundary_time(tid: &TxnId) -> i64 {
    completion_boundary_clocks()
        .lock()
        .get(tid)
        .map(|state| state.now_ms.load(Ordering::SeqCst))
        .unwrap_or_else(get_time)
}

#[cfg(not(test))]
fn completion_boundary_time(_tid: &TxnId) -> i64 {
    get_time()
}

#[cfg(test)]
fn note_completion_cleanup_boundary(tid: &TxnId) {
    if let Some(state) = completion_boundary_clocks().lock().get(tid) {
        state.now_ms.store(state.after_cleanup_ms, Ordering::SeqCst);
    }
}

#[cfg(not(test))]
fn note_completion_cleanup_boundary(_tid: &TxnId) {}

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
    pub fn get_server_id_by_id(&self, id: &Id) -> Option<u64> {
        self.consh.get_server_id(id.higher)
    }

    pub async fn get_member_by_server_id(&self, server_id: u64) -> io::Result<Arc<RPCClient>> {
        let server_name = self
            .consh
            .to_server_name_option(Some(server_id))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("server id {server_id} is not in the membership table"),
                )
            })?;
        self.member_pool
            .get_by_id(server_id, move |_| server_name.clone())
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
    point_cache: PointReadCache,
}

#[derive(Clone, Debug, Default)]
struct PointReadCache {
    header: Option<CellHeader>,
    projections: HashMap<Vec<u64>, OwnedCell>,
}

const COMPLETED_DECISION_RETENTION_MS: i64 = 300_000;
const RETIREMENT_DISCOVERY_BATCH: usize = 256;
const ABORT_CLEANUP_QUEUE_CAPACITY: usize = 256;
const ABORT_CLEANUP_DISCOVERY_BATCH: usize = 256;

#[derive(Default)]
struct RetirementDiscoveryState {
    members: BTreeSet<TxnId>,
    cursor: Option<TxnId>,
}

impl RetirementDiscoveryState {
    fn insert(&mut self, tid: TxnId) {
        self.members.insert(tid);
    }

    fn remove(&mut self, tid: &TxnId) {
        self.members.remove(tid);
        if self.members.is_empty() {
            self.cursor = None;
        }
    }

    fn candidates(&mut self, limit: usize) -> Vec<TxnId> {
        if self.members.is_empty() || limit == 0 {
            return Vec::new();
        }
        let mut candidates = Vec::with_capacity(limit.min(self.members.len()));
        if let Some(after) = self.cursor {
            candidates.extend(
                self.members
                    .range((Excluded(after), Unbounded))
                    .take(limit)
                    .copied(),
            );
            if candidates.len() < limit {
                candidates.extend(
                    self.members
                        .range(..=after)
                        .take(limit - candidates.len())
                        .copied(),
                );
            }
        } else {
            candidates.extend(self.members.iter().take(limit).copied());
        }
        if let Some(last) = candidates.last() {
            self.cursor = Some(*last);
        }
        candidates
    }

    #[cfg(test)]
    fn storage_len(&self) -> usize {
        self.members.len()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DecisionRecord {
    resolution: TxnResolution,
    expires_at_ms: Option<i64>,
}

impl DecisionRecord {
    fn cleanup_pending(resolution: TxnResolution) -> Self {
        Self {
            resolution,
            expires_at_ms: None,
        }
    }

    fn completed_at(resolution: TxnResolution, completed_at_ms: i64) -> Self {
        Self {
            resolution,
            expires_at_ms: Some(completed_at_ms.saturating_add(COMPLETED_DECISION_RETENTION_MS)),
        }
    }

    fn resolution_at(&self, now_ms: i64) -> Option<TxnResolution> {
        self.expires_at_ms
            .is_none_or(|expires_at_ms| now_ms < expires_at_ms)
            .then_some(self.resolution)
    }
}

const PREPARE_DISPATCH_CLOSED: usize = 1usize << (usize::BITS - 1);

struct PrepareDispatchState {
    word: AtomicUsize,
    abort_requested: AtomicBool,
}

impl PrepareDispatchState {
    fn new() -> Self {
        Self {
            word: AtomicUsize::new(0),
            abort_requested: AtomicBool::new(false),
        }
    }

    fn request_abort(&self) {
        self.abort_requested.store(true, Ordering::Release);
    }

    fn abort_requested(&self) -> bool {
        self.abort_requested.load(Ordering::Acquire)
    }

    fn acquire(
        self: &Arc<Self>,
        manager: Weak<TransactionManager>,
        tid: TxnId,
    ) -> Option<PrepareDispatchGuard> {
        let mut current = self.word.load(Ordering::Acquire);
        loop {
            if current & PREPARE_DISPATCH_CLOSED != 0 || current == PREPARE_DISPATCH_CLOSED - 1 {
                return None;
            }
            match self.word.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(PrepareDispatchGuard {
                        state: self.clone(),
                        manager,
                        tid,
                        cleanup_armed: true,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn try_close(&self) -> bool {
        self.word
            .compare_exchange(
                0,
                PREPARE_DISPATCH_CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn release(&self) {
        let previous = self.word.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0 && previous & PREPARE_DISPATCH_CLOSED == 0);
    }
}

struct PrepareDispatchGuard {
    state: Arc<PrepareDispatchState>,
    manager: Weak<TransactionManager>,
    tid: TxnId,
    cleanup_armed: bool,
}

impl PrepareDispatchGuard {
    fn disarm(&mut self) {
        self.cleanup_armed = false;
    }
}

impl Drop for PrepareDispatchGuard {
    fn drop(&mut self) {
        if self.cleanup_armed {
            self.state.request_abort();
            if let Some(manager) = self.manager.upgrade() {
                manager.queue_abort_cleanup(self.tid);
            }
        }
        self.state.release();
    }
}

struct Transaction {
    data: HashMap<Id, DataObject>,
    affected_objects: AffectedObjs,
    state: TxnState,
    prepare_dispatch: Arc<PrepareDispatchState>,
    dispatch_participants: BTreeSet<u64>,
    commit_dispatch_started: bool,
    commit_hlc: Option<Hlc>,
    coordinator_decision_durable: bool,
    abort_cleanup_finished: BTreeSet<u64>,
    completed_participants: BTreeSet<u64>,
    last_activity: i64, // Unix timestamp in milliseconds for detecting stale transactions
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
    rpc resolve(tid: TxnId) -> Result<TxnResolution, TMError>;
}

dispatch_rpc_service_functions!(TransactionManager);

service_with_id!(TransactionManager, DEFAULT_SERVICE_ID);

pub struct TransactionManager {
    self_ref: Weak<TransactionManager>,
    deps: Arc<TransactionManagerDeps>,
    transactions: LFMap<TxnId, TxnMutex>,
    txn_ids: parking_lot::Mutex<BTreeSet<TxnId>>, // Track TxnIds for iteration (PtrHashMap doesn't support iteration)
    completed_decisions: parking_lot::Mutex<HashMap<TxnId, DecisionRecord>>,
    retiring_transactions: parking_lot::Mutex<HashSet<TxnId>>,
    retirement_records: parking_lot::Mutex<HashMap<TxnId, undo_log::CoordinatorCompletionRecord>>,
    volatile_retirement_discovery: parking_lot::Mutex<RetirementDiscoveryState>,
    retirement_sender: mpsc::Sender<TxnId>,
    abort_cleanup_sender: mpsc::Sender<TxnId>,
    abort_cleanup_scheduled: parking_lot::Mutex<HashSet<TxnId>>,
    abort_cleanup_cursor: parking_lot::Mutex<Option<TxnId>>,
    replay_locks: parking_lot::Mutex<HashMap<TxnId, Weak<Mutex<()>>>>,
    data_sites: LFMap<u64, Arc<data_site::AsyncServiceClient>>,
    wait_config: WaitConfig,
    shutdown: Arc<AtomicBool>, // Signal to stop background cleanup task
    #[cfg(test)]
    drop_next_retirement_prepare_response: AtomicBool,
    #[cfg(test)]
    drop_next_end_response: AtomicBool,
    #[cfg(test)]
    fail_next_resolution_request: AtomicBool,
}

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
        let (retirement_sender, mut retirement_receiver) = mpsc::channel(256);
        let (abort_cleanup_sender, mut abort_cleanup_receiver) =
            mpsc::channel(ABORT_CLEANUP_QUEUE_CAPACITY);
        let manager = Arc::new_cyclic(|self_ref| Self {
            self_ref: self_ref.clone(),
            deps,
            transactions: LFMap::with_capacity(128),
            txn_ids: parking_lot::Mutex::new(BTreeSet::new()),
            completed_decisions: parking_lot::Mutex::new(HashMap::new()),
            retiring_transactions: parking_lot::Mutex::new(HashSet::new()),
            retirement_records: parking_lot::Mutex::new(HashMap::new()),
            volatile_retirement_discovery: parking_lot::Mutex::new(
                RetirementDiscoveryState::default(),
            ),
            retirement_sender,
            abort_cleanup_sender,
            abort_cleanup_scheduled: parking_lot::Mutex::new(HashSet::new()),
            abort_cleanup_cursor: parking_lot::Mutex::new(None),
            replay_locks: parking_lot::Mutex::new(HashMap::new()),
            data_sites: LFMap::with_capacity(8),
            wait_config,
            shutdown: shutdown.clone(),
            #[cfg(test)]
            drop_next_retirement_prepare_response: AtomicBool::new(false),
            #[cfg(test)]
            drop_next_end_response: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_resolution_request: AtomicBool::new(false),
        });

        // Spawn background cleanup task
        let cleanup_manager = manager.self_ref.clone();
        tokio::spawn(async move {
            loop {
                // Check if we should shutdown
                if shutdown.load(Ordering::Relaxed) {
                    debug!("TransactionManager cleanup task shutting down");
                    break;
                }

                #[cfg(test)]
                tokio::time::sleep(Duration::from_millis(50)).await;
                #[cfg(not(test))]
                tokio::time::sleep(Duration::from_secs(60)).await;

                let Some(manager) = cleanup_manager.upgrade() else {
                    return;
                };
                if manager.shutdown.load(Ordering::Relaxed) {
                    return;
                }
                manager.prune_completed_decisions_at(get_time());
                manager.prune_replay_locks();
                manager.restart_retirement_jobs();
                manager.restart_abort_cleanup_jobs();

                // Clean up stale transactions (older than 5 minutes)
                let cleaned = manager.cleanup_stale_transactions(5 * 60 * 1000);
                if cleaned > 0 {
                    warn!("Cleaned up {} stale transactions", cleaned);
                }
                drop(manager);
            }
        });

        let retirement_manager = manager.self_ref.clone();
        tokio::spawn(async move {
            while let Some(tid) = retirement_receiver.recv().await {
                let Some(manager) = retirement_manager.upgrade() else {
                    return;
                };
                if manager.shutdown.load(Ordering::Relaxed) {
                    manager.retiring_transactions.lock().remove(&tid);
                    return;
                }
                let finished = manager.retire_completion_once(&tid).await;
                if finished {
                    manager.retiring_transactions.lock().remove(&tid);
                    manager.retirement_records.lock().remove(&tid);
                    manager.volatile_retirement_discovery.lock().remove(&tid);
                    continue;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                if manager.retirement_sender.try_send(tid).is_err() {
                    manager.retiring_transactions.lock().remove(&tid);
                }
            }
        });

        let abort_cleanup_manager = manager.self_ref.clone();
        tokio::spawn(async move {
            while let Some(tid) = abort_cleanup_receiver.recv().await {
                let Some(manager) = abort_cleanup_manager.upgrade() else {
                    return;
                };
                if manager.shutdown.load(Ordering::Relaxed) {
                    manager.abort_cleanup_scheduled.lock().remove(&tid);
                    return;
                }
                let result = <TransactionManager as Service>::abort(&manager, tid).await;
                if let Err(error) = &result {
                    debug!(
                        "Background Abort cleanup attempt failed for {:?}: {:?}",
                        tid, error
                    );
                }
                manager.abort_cleanup_scheduled.lock().remove(&tid);
            }
        });

        let retirement_discovery_manager = manager.self_ref.clone();
        tokio::spawn(async move {
            loop {
                #[cfg(test)]
                tokio::time::sleep(Duration::from_millis(50)).await;
                #[cfg(not(test))]
                tokio::time::sleep(Duration::from_secs(1)).await;
                let Some(manager) = retirement_discovery_manager.upgrade() else {
                    return;
                };
                if manager.shutdown.load(Ordering::Relaxed) {
                    return;
                }
                manager.restart_retirement_jobs();
            }
        });

        manager.restart_retirement_jobs();
        manager.restart_abort_cleanup_jobs();

        manager
    }

    /// Returns the current number of living transactions tracked by this TransactionManager
    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    fn durable_resolution_at(status: &undo_log::CoordinatorStatus, now_ms: i64) -> TxnResolution {
        match status {
            undo_log::CoordinatorStatus::Decided(record) => record.resolution,
            undo_log::CoordinatorStatus::Completed(record) if now_ms < record.expires_at_ms => {
                record.resolution
            }
            undo_log::CoordinatorStatus::Completed(_) => TxnResolution::Unknown,
        }
    }

    fn cached_resolution_at(&self, tid: &TxnId, now_ms: i64) -> Option<TxnResolution> {
        let mut completed = self.completed_decisions.lock();
        match completed.get(tid).copied() {
            Some(record) => match record.resolution_at(now_ms) {
                Some(resolution) => Some(resolution),
                None => {
                    completed.remove(tid);
                    None
                }
            },
            None => None,
        }
    }

    fn prune_completed_decision_records_at(
        completed: &mut HashMap<TxnId, DecisionRecord>,
        now_ms: i64,
    ) {
        completed.retain(|_, record| record.resolution_at(now_ms).is_some());
    }

    fn prune_completed_decisions_at(&self, now_ms: i64) {
        Self::prune_completed_decision_records_at(&mut self.completed_decisions.lock(), now_ms);
    }

    pub(crate) async fn resolve_at_coordinator(
        &self,
        owner: &TxnPriority,
    ) -> Result<TxnResolution, TMError> {
        #[cfg(test)]
        if self
            .fail_next_resolution_request
            .swap(false, Ordering::SeqCst)
        {
            return Err(TMError::RPCErrorFromCellServer);
        }
        if owner.coordinator_id == self.deps.server_id {
            return <Self as Service>::resolve(self, owner.tid).await;
        }

        let client = self
            .deps
            .get_member_by_server_id(owner.coordinator_id)
            .await
            .map_err(|_| TMError::RPCErrorFromCellServer)?;
        let coordinator = AsyncServiceClient::new_with_service_id(
            generate_scoped_service_id(
                self.deps.database_runtime.group_name(),
                self.deps.database_runtime.database_name(),
            ),
            &client,
        );
        match coordinator.resolve(owner.tid).await {
            Ok(result) => result,
            Err(_) => Err(TMError::RPCErrorFromCellServer),
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_next_resolution_request_for_test(&self) {
        self.fail_next_resolution_request
            .store(true, Ordering::SeqCst);
    }

    fn queue_retirement(&self, tid: TxnId) {
        if !self.retiring_transactions.lock().insert(tid) {
            return;
        }
        if self.retirement_sender.try_send(tid).is_err() {
            self.retiring_transactions.lock().remove(&tid);
        }
    }

    fn queue_abort_cleanup(&self, tid: TxnId) {
        if !self.abort_cleanup_scheduled.lock().insert(tid) {
            return;
        }
        if self.abort_cleanup_sender.try_send(tid).is_err() {
            self.abort_cleanup_scheduled.lock().remove(&tid);
        }
    }

    fn live_abort_cleanup_candidates(&self, limit: usize) -> Vec<TxnId> {
        let tids = self.txn_ids.lock();
        if tids.is_empty() || limit == 0 {
            return Vec::new();
        }
        let mut cursor = self.abort_cleanup_cursor.lock();
        let mut candidates = Vec::with_capacity(limit.min(tids.len()));
        if let Some(after) = *cursor {
            candidates.extend(
                tids.range((Excluded(after), Unbounded))
                    .take(limit)
                    .copied(),
            );
            if candidates.len() < limit {
                candidates.extend(tids.range(..=after).take(limit - candidates.len()).copied());
            }
        } else {
            candidates.extend(tids.iter().take(limit).copied());
        }
        if let Some(last) = candidates.last() {
            *cursor = Some(*last);
        }
        candidates
    }

    fn restart_abort_cleanup_jobs(&self) {
        let available_capacity = self
            .abort_cleanup_sender
            .capacity()
            .min(ABORT_CLEANUP_DISCOVERY_BATCH);
        if available_capacity == 0 {
            return;
        }
        if let Some(undo_log) = self.deps.database_runtime.undo_log() {
            match undo_log.coordinator_abort_cleanup_candidates(available_capacity) {
                Ok(candidates) => {
                    for tid in candidates {
                        if let Some(txn) = self.transactions.get(&tid) {
                            let should_abort = txn.try_lock().is_some_and(|txn| {
                                txn.state == TxnState::Aborted
                                    || txn.prepare_dispatch.abort_requested()
                            });
                            if should_abort {
                                self.queue_abort_cleanup(tid);
                            }
                            continue;
                        }
                        self.queue_abort_cleanup(tid);
                    }
                }
                Err(error) => {
                    error!(
                        "Failed to rediscover durable coordinator Abort cleanup: {:?}",
                        error
                    );
                }
            }
        }
        let remaining_capacity = self
            .abort_cleanup_sender
            .capacity()
            .min(ABORT_CLEANUP_DISCOVERY_BATCH);
        for tid in self.live_abort_cleanup_candidates(remaining_capacity) {
            let Some(txn) = self.transactions.get(&tid) else {
                self.txn_ids.lock().remove(&tid);
                continue;
            };
            let Some(txn) = txn.try_lock() else {
                continue;
            };
            if txn.state == TxnState::Aborted || txn.prepare_dispatch.abort_requested() {
                drop(txn);
                self.queue_abort_cleanup(tid);
            }
        }
    }

    fn replay_lock(&self, tid: &TxnId) -> Arc<Mutex<()>> {
        let mut locks = self.replay_locks.lock();
        if let Some(lock) = locks.get(tid).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(*tid, Arc::downgrade(&lock));
        lock
    }

    fn prune_replay_locks(&self) {
        self.replay_locks
            .lock()
            .retain(|_, lock| lock.strong_count() != 0);
    }

    fn restart_retirement_jobs(&self) {
        let available_capacity = self
            .retirement_sender
            .capacity()
            .min(RETIREMENT_DISCOVERY_BATCH);
        if available_capacity == 0 {
            return;
        }
        if let Some(undo_log) = self.deps.database_runtime.undo_log() {
            match undo_log.coordinator_retirement_candidates(available_capacity) {
                Ok(candidates) => {
                    for tid in candidates {
                        self.queue_retirement(tid);
                    }
                }
                Err(error) => {
                    error!(
                        "Failed to restart durable coordinator retirement jobs: {:?}",
                        error
                    );
                }
            }
        }
        let remaining_capacity = self
            .retirement_sender
            .capacity()
            .min(RETIREMENT_DISCOVERY_BATCH);
        let volatile_candidates = self
            .volatile_retirement_discovery
            .lock()
            .candidates(remaining_capacity);
        for tid in volatile_candidates {
            self.queue_retirement(tid);
        }
    }

    #[cfg(test)]
    fn drop_next_retirement_prepare_response_for_test(&self) {
        self.drop_next_retirement_prepare_response
            .store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn drop_next_end_response_for_test(&self) {
        self.drop_next_end_response.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn take_dropped_end_response_for_test(&self) -> bool {
        self.drop_next_end_response.swap(false, Ordering::SeqCst)
    }

    #[cfg(not(test))]
    fn take_dropped_end_response_for_test(&self) -> bool {
        false
    }

    async fn retire_completion_once(&self, tid: &TxnId) -> bool {
        let undo_log = self.deps.database_runtime.undo_log();
        let mut record = match undo_log {
            Some(undo_log) => match undo_log.coordinator_status(tid) {
                Ok(Some(undo_log::CoordinatorStatus::Completed(record))) => record,
                Ok(_) => return true,
                Err(error) => {
                    error!(
                        "Failed to read coordinator completion for retirement {:?}: {:?}",
                        tid, error
                    );
                    return false;
                }
            },
            None => match self.retirement_records.lock().get(tid).cloned() {
                Some(record) => record,
                None => return true,
            },
        };

        #[cfg(test)]
        pause_after_retirement_lookup(tid).await;

        if record.finalized_participants == record.participants {
            return true;
        }

        let prepare_targets: Vec<_> = record
            .participants
            .difference(&record.retired_participants)
            .copied()
            .collect();
        let mut newly_retired = BTreeSet::new();
        for server_id in prepare_targets {
            let Ok(data_site) = self.get_data_site(server_id).await else {
                continue;
            };
            match data_site
                .retire(self.get_clock(), *tid, record.resolution)
                .await
            {
                Ok(response) => {
                    self.merge_clock(response.clock);
                    if matches!(response.payload, EndResult::Success) {
                        #[cfg(test)]
                        if self
                            .drop_next_retirement_prepare_response
                            .swap(false, Ordering::SeqCst)
                        {
                            continue;
                        }
                        newly_retired.insert(server_id);
                    }
                }
                Err(error) => {
                    debug!(
                        "Participant {} retirement prepare failed for {:?}: {:?}",
                        server_id, tid, error
                    );
                }
            }
        }
        if !newly_retired.is_empty() {
            record.retired_participants.extend(newly_retired);
            if let Some(undo_log) = undo_log {
                if let Err(error) = undo_log.write_coordinator_completion_record(tid, &record) {
                    error!(
                        "Failed to persist retirement prepare acknowledgements for {:?}: {:?}",
                        tid, error
                    );
                    return false;
                }
            }
            self.retirement_records.lock().insert(*tid, record.clone());
        }

        let finalize_targets: Vec<_> = record
            .retired_participants
            .difference(&record.finalized_participants)
            .copied()
            .collect();
        let mut newly_finalized = BTreeSet::new();
        for server_id in finalize_targets {
            let Ok(data_site) = self.get_data_site(server_id).await else {
                continue;
            };
            match data_site
                .finalize_retirement(self.get_clock(), *tid, record.resolution)
                .await
            {
                Ok(response) => {
                    self.merge_clock(response.clock);
                    if matches!(response.payload, EndResult::Success) {
                        newly_finalized.insert(server_id);
                    }
                }
                Err(error) => {
                    debug!(
                        "Participant {} retirement finalize failed for {:?}: {:?}",
                        server_id, tid, error
                    );
                }
            }
        }
        if !newly_finalized.is_empty() {
            record.finalized_participants.extend(newly_finalized);
            if let Some(undo_log) = undo_log {
                if let Err(error) = undo_log.write_coordinator_completion_record(tid, &record) {
                    error!(
                        "Failed to persist retirement finalize acknowledgements for {:?}: {:?}",
                        tid, error
                    );
                    return false;
                }
            }
            self.retirement_records.lock().insert(*tid, record.clone());
        }

        record.finalized_participants == record.participants
    }

    #[cfg(test)]
    pub(crate) fn forget_transaction_for_test(&self, tid: &TxnId) {
        let _ = self.transactions.remove(tid);
        self.txn_ids.lock().remove(tid);
    }

    #[cfg(test)]
    pub(crate) async fn coordinator_expectation_for_test(
        &self,
        tid: &TxnId,
        id: &Id,
    ) -> Option<CellExpectation> {
        let txn = self.get_transaction(tid).ok()?;
        let expectation = txn
            .lock()
            .await
            .data
            .get(id)
            .map(|data_obj| data_obj.expectation.clone());
        expectation
    }

    /// Helper function for exponential backoff wait with timeout
    async fn backoff_wait(attempt: u32, config: &WaitConfig) -> Result<(), TMError> {
        let backoff_ms = config.initial_backoff_ms * 2u64.pow(attempt);
        let backoff_ms = backoff_ms.min(config.max_backoff_ms);

        debug!("Backing off for {}ms (attempt {})", backoff_ms, attempt);
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        Ok(())
    }

    async fn spawn_prepare_lifecycle(
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
        let txn_mutex = self.get_transaction(&tid)?;
        let txn = txn_mutex.lock().await;
        self.ensure_rw_state(&txn)?;
        let mut dispatch_guard = txn
            .prepare_dispatch
            .acquire(self.self_ref.clone(), tid)
            .ok_or(TMError::TransactionNotFound)?;
        drop(txn);
        let (result_sender, result_receiver) = oneshot::channel();
        let (ack_sender, ack_receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = manager.clone().run_prepare_lifecycle(tid.clone()).await;
            let prepared = matches!(&result, Ok(TMPrepareResult::Success));
            if result_sender.send(result).is_err() {
                if prepared {
                    let _ = manager.abort(tid).await;
                }
                dispatch_guard.disarm();
                return;
            }
            if prepared && ack_receiver.await.is_err() {
                let _ = manager.abort(tid).await;
            }
            dispatch_guard.disarm();
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
            let (result_receiver, ack_sender) = self.spawn_prepare_lifecycle(tid).await?;
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
            let txn_lock = match self.get_transaction(&tid) {
                Ok(txn_lock) => txn_lock,
                Err(TMError::TransactionNotFound) => {
                    return self
                        .retry_durable_commit_after_coordinator_restart(&tid)
                        .await;
                }
                Err(error) => return Err(error),
            };
            let mut txn = txn_lock.lock().await;
            match txn.state {
                TxnState::Prepared => {
                    // The commit choice becomes irrevocable before the first
                    // decision-record attempt. An I/O error can be
                    // outcome-uncertain, so abort must never be allowed after
                    // this transition.
                    txn.state = TxnState::Committed;
                }
                TxnState::Committed => {}
                state => return Err(TMError::InvalidTransactionState(state)),
            }
            txn.last_activity = get_time();
            if !txn.coordinator_decision_durable {
                if txn.affected_objects.is_empty() {
                    txn.coordinator_decision_durable = true;
                } else {
                    let Some(commit_hlc) = txn.commit_hlc else {
                        return Ok(EndResult::CheckFailed(CheckError::CannotEnd));
                    };
                    if self
                        .deps
                        .database_runtime
                        .chunks()
                        .durable_storage_configured()
                        && self.deps.database_runtime.undo_log().is_none()
                    {
                        return Ok(EndResult::CheckFailed(CheckError::CannotEnd));
                    }
                    if let Some(undo_log) = self.deps.database_runtime.undo_log() {
                        let participants: Vec<_> = txn.affected_objects.keys().copied().collect();
                        if let Err(error) = undo_log.write_coordinator_commit_decision(
                            &tid,
                            commit_hlc,
                            &participants,
                        ) {
                            error!(
                                "Failed to persist coordinator commit decision for {:?}: {:?}",
                                tid, error
                            );
                            return Ok(EndResult::CheckFailed(CheckError::CannotEnd));
                        }
                    }
                    txn.coordinator_decision_durable = true;
                }
            }
            let affected_objs: AffectedObjs = txn
                .affected_objects
                .iter()
                .filter(|(server_id, _)| !txn.completed_participants.contains(server_id))
                .map(|(server_id, objects)| (*server_id, objects.clone()))
                .collect();
            let mut result = match {
                #[cfg(feature = "occ_phase_profile")]
                let _phase_guard =
                    super::phase_profile::guard(super::phase_profile::Phase::EndParticipantLookup);
                self.data_sites_for_objs(&affected_objs).await
            } {
                Ok(data_sites) => {
                    #[cfg(feature = "occ_phase_profile")]
                    let _phase_guard =
                        super::phase_profile::guard(super::phase_profile::Phase::EndCleanup);
                    let (result, completed) =
                        self.sites_end(&tid, &affected_objs, &data_sites).await;
                    txn.completed_participants.extend(completed);
                    result
                }
                Err(error) => Err(error),
            };
            if matches!(result, Ok(EndResult::Success))
                && txn.completed_participants.len() == txn.affected_objects.len()
            {
                let resolution = TxnResolution::Commit(txn.commit_hlc.unwrap_or(tid));
                match self
                    .finish_transaction_guarded(&tid, &txn_lock, &mut txn, resolution)
                    .await
                {
                    Ok(completion) => {
                        if !completion.participants.is_empty() {
                            self.queue_retirement(tid);
                        }
                    }
                    Err(error) => {
                        error!(
                            "Failed to persist coordinator completion for {:?}: {:?}",
                            tid, error
                        );
                        result = Ok(EndResult::CheckFailed(CheckError::CannotEnd));
                    }
                }
            }
            result
        }
        .boxed()
    }
    fn abort(&self, tid: TxnId) -> BoxFuture<'_, Result<AbortResult, TMError>> {
        debug!("TXN ABORT IN MGR {:?}", &tid);
        async move {
            let txn_lock = match self.get_transaction(&tid) {
                Ok(txn_lock) => txn_lock,
                Err(TMError::TransactionNotFound) => {
                    return self
                        .retry_durable_abort_after_coordinator_restart(&tid)
                        .await;
                }
                Err(error) => return Err(error),
            };
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
            txn.prepare_dispatch.request_abort();
            txn.last_activity = get_time();
            if !txn.coordinator_decision_durable {
                if txn.dispatch_participants.is_empty() {
                    txn.coordinator_decision_durable = true;
                } else {
                    if self
                        .deps
                        .database_runtime
                        .chunks()
                        .durable_storage_configured()
                        && self.deps.database_runtime.undo_log().is_none()
                    {
                        self.queue_abort_cleanup(tid);
                        return Ok(AbortResult::CheckFailed(CheckError::CannotEnd));
                    }
                    if let Some(undo_log) = self.deps.database_runtime.undo_log() {
                        let participants: Vec<_> =
                            txn.dispatch_participants.iter().copied().collect();
                        if let Err(error) =
                            undo_log.write_coordinator_abort_decision(&tid, &participants)
                        {
                            error!(
                                "Failed to persist coordinator abort decision for {:?}: {:?}",
                                tid, error
                            );
                            self.queue_abort_cleanup(tid);
                            return Ok(AbortResult::CheckFailed(CheckError::CannotEnd));
                        }
                    }
                    txn.coordinator_decision_durable = true;
                }
            }
            let changed_objs: AffectedObjs = txn
                .dispatch_participants
                .difference(&txn.abort_cleanup_finished)
                .filter_map(|server_id| {
                    txn.affected_objects
                        .get(server_id)
                        .cloned()
                        .map(|objects| (*server_id, objects))
                })
                .collect();
            let (mut result, cleanup_finished, ended_participants) = match {
                #[cfg(feature = "occ_phase_profile")]
                let _phase_guard = super::phase_profile::guard(
                    super::phase_profile::Phase::AbortParticipantLookup,
                );
                self.data_sites_for_objs(&changed_objs).await
            } {
                Ok(data_sites) => {
                    debug!("ABORT AFFECTED OBJS: {:?}", changed_objs);
                    #[cfg(feature = "occ_phase_profile")]
                    let _phase_guard =
                        super::phase_profile::guard(super::phase_profile::Phase::AbortCleanup);
                    self.sites_abort(&tid, &changed_objs, &data_sites).await // with end
                }
                Err(error) => (Err(error), BTreeSet::new(), BTreeSet::new()),
            };
            txn.abort_cleanup_finished.extend(cleanup_finished);
            txn.completed_participants.extend(ended_participants);
            if Self::abort_cleanup_complete(&result)
                && txn.abort_cleanup_finished == txn.dispatch_participants
            {
                match self
                    .finish_transaction_guarded(&tid, &txn_lock, &mut txn, TxnResolution::Abort)
                    .await
                {
                    Ok(completion) => {
                        if !completion.participants.is_empty() {
                            self.queue_retirement(tid);
                        }
                    }
                    Err(error) => {
                        error!(
                            "Failed to persist coordinator abort completion for {:?}: {:?}",
                            tid, error
                        );
                        self.queue_abort_cleanup(tid);
                        result = Ok(AbortResult::CheckFailed(CheckError::CannotEnd));
                    }
                }
            } else {
                self.queue_abort_cleanup(tid);
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
                    prepare_dispatch: Arc::new(PrepareDispatchState::new()),
                    dispatch_participants: BTreeSet::new(),
                    commit_dispatch_started: false,
                    commit_hlc: None,
                    coordinator_decision_durable: false,
                    abort_cleanup_finished: BTreeSet::new(),
                    completed_participants: BTreeSet::new(),
                    last_activity: now,
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
    fn resolve(&self, tid: TxnId) -> BoxFuture<'_, Result<TxnResolution, TMError>> {
        async move {
            if let Ok(txn_lock) = self.get_transaction(&tid) {
                let txn = txn_lock.lock().await;
                let live_resolution = match txn.state {
                    TxnState::Committed if txn.coordinator_decision_durable => {
                        Some(TxnResolution::Commit(txn.commit_hlc.unwrap_or(tid)))
                    }
                    TxnState::Committed => Some(TxnResolution::InProgress),
                    TxnState::Aborted if txn.coordinator_decision_durable => {
                        Some(TxnResolution::Abort)
                    }
                    TxnState::Aborted => Some(TxnResolution::InProgress),
                    TxnState::Started | TxnState::Prepared => Some(TxnResolution::InProgress),
                    TxnState::Cleanup => None,
                };
                drop(txn);
                if let Some(resolution) = live_resolution {
                    return Ok(resolution);
                }
            }
            let now_ms = get_time();
            if let Some(resolution) = self.cached_resolution_at(&tid, now_ms) {
                return Ok(resolution);
            }
            match self.deps.database_runtime.undo_log() {
                Some(undo_log) => undo_log
                    .coordinator_status(&tid)
                    .map(|status| {
                        status
                            .as_ref()
                            .map(|status| Self::durable_resolution_at(status, now_ms))
                            .unwrap_or(TxnResolution::Unknown)
                    })
                    .map_err(|error| {
                        error!(
                            "Failed to resolve durable coordinator decision for {:?}: {:?}",
                            tid, error
                        );
                        TMError::Other
                    }),
                None => Ok(TxnResolution::Unknown),
            }
        }
        .boxed()
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
                                expectation: CellExpectation::UnobservedAbsent,
                                new: true,
                                changed: true,
                                point_cache: PointReadCache::default(),
                            },
                        );
                        Ok(TxnExecResult::Accepted(()))
                    } else {
                        let data_obj = txn.data.get_mut(&id).unwrap();
                        if data_obj.cell.is_some() {
                            return Ok(TxnExecResult::Error(WriteError::CellAlreadyExisted));
                        }
                        data_obj.cell = Some(cell);
                        data_obj.new = matches!(
                            data_obj.expectation,
                            CellExpectation::Absent(_) | CellExpectation::UnobservedAbsent
                        ) && !data_obj.changed;
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
                            && matches!(
                                data_obj.expectation,
                                CellExpectation::Absent(_) | CellExpectation::UnobservedAbsent
                            )
                        {
                            return Ok(TxnExecResult::Error(WriteError::CellDoesNotExisted));
                        }
                        data_obj.cell = Some(cell);
                        data_obj.changed = true;
                    } else {
                        // Blind write: prepare observes and certifies the
                        // head under the cell guards, so no read round trip
                        // is needed here. A missing cell surfaces at prepare
                        // as NotRealizable instead of failing this call.
                        txn.data.insert(
                            id,
                            DataObject {
                                server: server_id,
                                cell: Some(cell),
                                expectation: CellExpectation::UnobservedPresent,
                                new: false,
                                changed: true,
                                point_cache: PointReadCache::default(),
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
                        // Blind remove: same deferred observation as blind
                        // update above.
                        txn.data.insert(
                            id,
                            DataObject {
                                server: server_id,
                                cell: None,
                                expectation: CellExpectation::UnobservedPresent,
                                new: false,
                                changed: true,
                                point_cache: PointReadCache::default(),
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
    /// whole cell. A prior partial observation keeps only its logical
    /// expectation, so a later full shape resolves the same fixed snapshot.
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
            if data_obj.changed
                || matches!(
                    data_obj.expectation,
                    CellExpectation::Absent(_) | CellExpectation::UnobservedAbsent
                )
            {
                return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted));
            }
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
    /// missing; an already-owned header is served from cache. Otherwise the
    /// participant resolves the fixed snapshot and returns only the header.
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
            if data_obj.changed
                || matches!(
                    data_obj.expectation,
                    CellExpectation::Absent(_) | CellExpectation::UnobservedAbsent
                )
            {
                return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted));
            }
            if let Some(header) = data_obj.point_cache.header.as_ref() {
                return Ok(TxnExecResult::Accepted(header.clone()));
            }
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
    /// participant resolves the fixed snapshot and returns only the projection.
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
            if data_obj.changed
                || matches!(
                    data_obj.expectation,
                    CellExpectation::Absent(_) | CellExpectation::UnobservedAbsent
                )
            {
                return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted));
            }
            if let Some(projection) = data_obj.point_cache.projections.get(fields) {
                return Ok(TxnExecResult::Accepted(projection.clone()));
            }
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
            .ok_or(ReadError::SchemaDoesNotExisted(schema_id))?;

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

    fn visible_point<T>(value: Option<T>) -> TxnExecResult<T, ReadError>
    where
        T: Send + Clone,
    {
        match value {
            Some(value) => TxnExecResult::Accepted(value),
            None => TxnExecResult::Error(ReadError::CellDoesNotExisted),
        }
    }

    fn observation_agrees(
        recorded: &CellExpectation,
        observed: &CellExpectation,
    ) -> Result<(), ReadError> {
        if recorded == observed {
            Ok(())
        } else {
            Err(ReadError::NotMatch)
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
                    match dsr.payload {
                        TxnExecResult::Accepted(observed) => {
                            if let Some(data_obj) = txn.data.get_mut(id) {
                                if data_obj.changed {
                                    if let Some(ref cached_cell) = data_obj.cell {
                                        return Ok(TxnExecResult::Accepted(cached_cell.clone()));
                                    }
                                    return Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted));
                                }
                                if let Err(error) = Self::observation_agrees(
                                    &data_obj.expectation,
                                    &observed.expectation,
                                ) {
                                    return Ok(TxnExecResult::Error(error));
                                }
                                if data_obj.cell.is_none() {
                                    data_obj.cell = observed.value;
                                }
                                return Ok(Self::visible_point(data_obj.cell.clone()));
                            } else {
                                let result = Self::visible_point(observed.value.clone());
                                txn.data.insert(
                                    *id,
                                    DataObject {
                                        server: server_id,
                                        expectation: observed.expectation,
                                        cell: observed.value,
                                        new: false,
                                        changed: false,
                                        point_cache: PointReadCache::default(),
                                    },
                                );
                                return Ok(result);
                            }
                        }
                        TxnExecResult::Wait => {
                            Self::backoff_wait(attempt, &self.wait_config).await?;
                            attempt += 1;
                            continue;
                        }
                        TxnExecResult::Rejected => return Ok(TxnExecResult::Rejected),
                        TxnExecResult::Error(error) => {
                            return Ok(TxnExecResult::Error(error));
                        }
                        TxnExecResult::StateError(state) => {
                            return Ok(TxnExecResult::StateError(state));
                        }
                    }
                }
                Err(e) => {
                    error!("{:?}", e);
                    return Err(TMError::RPCErrorFromCellServer);
                }
            }
        }
    }

    /// Resolves the participant snapshot header and caches the owned result.
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

            let head_response = server
                .head(self_server_id, self.get_clock(), tid.to_owned(), *id)
                .await;
            match head_response {
                Ok(dsr) => {
                    self.merge_clock(dsr.clock);
                    match dsr.payload {
                        TxnExecResult::Accepted(observed) => {
                            if let Some(data_obj) = txn.data.get_mut(id) {
                                if data_obj.changed {
                                    return Ok(match &data_obj.cell {
                                        Some(cell) => TxnExecResult::Accepted(cell.header.clone()),
                                        None => TxnExecResult::Error(ReadError::CellDoesNotExisted),
                                    });
                                }
                                if let Err(error) = Self::observation_agrees(
                                    &data_obj.expectation,
                                    &observed.expectation,
                                ) {
                                    return Ok(TxnExecResult::Error(error));
                                }
                                if data_obj.point_cache.header.is_none() {
                                    data_obj.point_cache.header = observed.value.clone();
                                }
                                return Ok(Self::visible_point(observed.value));
                            }
                            let result = Self::visible_point(observed.value.clone());
                            txn.data.insert(
                                *id,
                                DataObject {
                                    server: server_id,
                                    cell: None,
                                    expectation: observed.expectation,
                                    changed: false,
                                    new: false,
                                    point_cache: PointReadCache {
                                        header: observed.value,
                                        projections: HashMap::new(),
                                    },
                                },
                            );
                            return Ok(result);
                        }
                        TxnExecResult::Wait => {
                            Self::backoff_wait(attempt, &self.wait_config).await?;
                            attempt += 1;
                            continue;
                        }
                        TxnExecResult::Rejected => return Ok(TxnExecResult::Rejected),
                        TxnExecResult::Error(error) => {
                            return Ok(TxnExecResult::Error(error));
                        }
                        TxnExecResult::StateError(state) => {
                            return Ok(TxnExecResult::StateError(state));
                        }
                    }
                }
                Err(e) => {
                    error!("{:?}", e);
                    return Err(TMError::RPCErrorFromCellServer);
                }
            }
        }
    }

    /// Resolves a projected participant snapshot and caches the owned result by
    /// exact requested field order.
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
                        TxnExecResult::Accepted(observed) => {
                            if let Some(data_obj) = txn.data.get_mut(id) {
                                if data_obj.changed {
                                    return Ok(match &data_obj.cell {
                                        Some(cell) => match self.select_from_cell(cell, fields) {
                                            Ok(selected) => TxnExecResult::Accepted(selected),
                                            Err(error) => TxnExecResult::Error(error),
                                        },
                                        None => TxnExecResult::Error(ReadError::CellDoesNotExisted),
                                    });
                                }
                                if let Err(error) = Self::observation_agrees(
                                    &data_obj.expectation,
                                    &observed.expectation,
                                ) {
                                    return Ok(TxnExecResult::Error(error));
                                }
                                if let Some(projection) = observed.value.as_ref() {
                                    data_obj
                                        .point_cache
                                        .projections
                                        .entry(fields.to_vec())
                                        .or_insert_with(|| projection.clone());
                                }
                                return Ok(Self::visible_point(observed.value));
                            }
                            let mut projections = HashMap::new();
                            if let Some(projection) = observed.value.as_ref() {
                                projections.insert(fields.to_vec(), projection.clone());
                            }
                            let result = Self::visible_point(observed.value);
                            txn.data.insert(
                                *id,
                                DataObject {
                                    server: server_id,
                                    cell: None,
                                    expectation: observed.expectation,
                                    changed: false,
                                    new: false,
                                    point_cache: PointReadCache {
                                        header: None,
                                        projections,
                                    },
                                },
                            );
                            return Ok(result);
                        }
                        TxnExecResult::Wait => {
                            Self::backoff_wait(attempt, &self.wait_config).await?;
                            attempt += 1;
                            continue;
                        }
                        TxnExecResult::Rejected => return Ok(TxnExecResult::Rejected),
                        TxnExecResult::Error(error) => {
                            return Ok(TxnExecResult::Error(error));
                        }
                        TxnExecResult::StateError(state) => {
                            return Ok(TxnExecResult::StateError(state));
                        }
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
    ) -> Result<TxnExecResult<ObservedPoint<CellHeader>, ReadError>, TMError> {
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

            let head_response = server
                .head(self_server_id, self.get_clock(), *tid, *id)
                .await;
            match head_response {
                Ok(dsr) => {
                    self.merge_clock(dsr.clock);
                    match &dsr.payload {
                        TxnExecResult::Wait => {
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
    async fn observe_revision(
        &self,
        server: &Arc<data_site::AsyncServiceClient>,
        tid: &TxnId,
        id: Id,
    ) -> Result<TxnExecResult<u64, ReadError>, TMError> {
        Ok(match self.observe_head_from_site(server, tid, &id).await? {
            TxnExecResult::Accepted(observed) => match observed.value {
                Some(header) => {
                    debug_assert_eq!(
                        observed.expectation,
                        CellExpectation::Present(header.revision_ts)
                    );
                    TxnExecResult::Accepted(header.revision_ts)
                }
                None => TxnExecResult::Error(ReadError::CellDoesNotExisted),
            },
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
                .prepare(coordinator_id, deps.hlc.now(), tid.clone(), prepare_ops)
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
        let prepare_futures: FuturesUnordered<_> = affected_objs
            .iter()
            .map(|(server, objs)| async move {
                let data_site = data_sites.get(server).unwrap().clone();
                let result = TransactionManager::site_prepare(
                    &self.deps,
                    &self.wait_config,
                    &tid,
                    &objs,
                    &data_site,
                )
                .await;
                #[cfg(test)]
                Self::notify_prepare_results_observed(tid, objs);
                result
            })
            .collect();
        let results: Vec<Result<DMPrepareResult, TMError>> = prepare_futures.collect().await;
        Self::reduce_prepare_results(results)
    }
    async fn sites_commit(
        &self,
        tid: &TxnId,
        commit_hlc: Hlc,
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
                async move { data_site.commit(commit_hlc, tid.to_owned(), ops).await }
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
    ) -> (Result<AbortResult, TMError>, BTreeSet<u64>, BTreeSet<u64>) {
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
        let mut cleanup_finished = BTreeSet::new();
        let mut ended_participants = BTreeSet::new();
        let mut first_failure = None;
        let mut first_error = None;

        for (server_id, result) in abort_results {
            match result {
                Ok(asr) => {
                    let payload = asr.payload;
                    self.merge_clock(asr.clock);
                    match payload {
                        AbortResult::Success(Some(mut failures)) => {
                            if !failures.is_empty() {
                                rollback_failures.append(&mut failures);
                            } else {
                                sites_to_end.insert(server_id);
                            }
                        }
                        AbortResult::Success(None) => {
                            sites_to_end.insert(server_id);
                        }
                        AbortResult::CheckFailed(CheckError::AlreadyAborted) => {
                            sites_to_end.insert(server_id);
                        }
                        AbortResult::CheckFailed(CheckError::NotExisted) => {
                            cleanup_finished.insert(server_id);
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

        // Releasing any successful participant before every participant has
        // compensated would break the shared owner barrier. A retry can safely
        // observe AlreadyAborted and end all compensated participants together.
        if first_error.is_some() || first_failure.is_some() || !rollback_failures.is_empty() {
            sites_to_end.clear();
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
                    let payload = response.payload;
                    match payload {
                        EndResult::Success => {
                            if self.take_dropped_end_response_for_test() {
                                if first_error.is_none() {
                                    first_error = Some(TMError::RPCErrorFromCellServer);
                                }
                            } else {
                                cleanup_finished.insert(server_id);
                                ended_participants.insert(server_id);
                            }
                        }
                        EndResult::CheckFailed(CheckError::NotExisted) => {
                            cleanup_finished.insert(server_id);
                        }
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

        let result = if let Some(error) = first_error {
            Err(error)
        } else if let Some(failure) = first_failure {
            Ok(failure)
        } else if !rollback_failures.is_empty() {
            Ok(AbortResult::Success(Some(rollback_failures)))
        } else {
            Ok(AbortResult::Success(None))
        };
        (result, cleanup_finished, ended_participants)
    }
    async fn sites_end(
        &self,
        tid: &TxnId,
        changed_objs: &AffectedObjs,
        data_sites: &DataSitesMap,
    ) -> (Result<EndResult, TMError>, BTreeSet<u64>) {
        let end_futures: FuturesUnordered<_> = changed_objs
            .iter()
            .map(|(server_id, _)| {
                let server_id = *server_id;
                let data_site = data_sites.get(&server_id).unwrap();
                async move {
                    (
                        server_id,
                        data_site.end(self.get_clock(), tid.clone()).await,
                    )
                }
            })
            .collect();
        let end_results: Vec<_> = end_futures.collect().await;
        let mut completed = BTreeSet::new();
        let mut first_failure = None;
        let mut first_error = None;
        for (server_id, result) in end_results {
            match result {
                Ok(result) => {
                    self.merge_clock(result.clock);
                    let payload = result.payload;
                    match payload {
                        EndResult::Success => {
                            if self.take_dropped_end_response_for_test() {
                                if first_error.is_none() {
                                    first_error = Some(TMError::RPCErrorFromCellServer);
                                }
                            } else {
                                completed.insert(server_id);
                            }
                        }
                        _ if first_failure.is_none() => first_failure = Some(payload),
                        _ => {}
                    }
                }
                Err(e) => {
                    debug!("Error on site end {:?}", e);
                    if first_error.is_none() {
                        first_error = Some(TMError::RPCErrorFromCellServer);
                    }
                }
            }
        }
        let result = if let Some(error) = first_error {
            Err(error)
        } else if let Some(failure) = first_failure {
            Ok(failure)
        } else {
            Ok(EndResult::Success)
        };
        (result, completed)
    }

    fn recorded_participant_targets(participants: &BTreeSet<u64>) -> AffectedObjs {
        participants
            .iter()
            .map(|server_id| (*server_id, BTreeMap::new()))
            .collect()
    }

    async fn retry_durable_commit_after_coordinator_restart(
        &self,
        tid: &TxnId,
    ) -> Result<EndResult, TMError> {
        let replay_lock = self.replay_lock(tid);
        let _replay_guard = replay_lock.lock().await;
        let Some(undo_log) = self.deps.database_runtime.undo_log() else {
            return Err(TMError::TransactionNotFound);
        };
        let Some(status) = undo_log.coordinator_status(tid).map_err(|error| {
            error!(
                "Failed to read durable coordinator decision for {:?}: {:?}",
                tid, error
            );
            TMError::Other
        })?
        else {
            return Err(TMError::TransactionNotFound);
        };
        #[cfg(test)]
        tokio::task::yield_now().await;
        match status {
            undo_log::CoordinatorStatus::Completed(record) => {
                if record.finalized_participants != record.participants {
                    self.queue_retirement(*tid);
                }
                if get_time() >= record.expires_at_ms {
                    return Err(TMError::TransactionNotFound);
                }
                match record.resolution {
                    TxnResolution::Commit(_) => Ok(EndResult::Success),
                    TxnResolution::Abort => {
                        Err(TMError::InvalidTransactionState(TxnState::Aborted))
                    }
                    TxnResolution::InProgress | TxnResolution::Unknown => {
                        Err(TMError::TransactionNotFound)
                    }
                }
            }
            undo_log::CoordinatorStatus::Decided(record)
                if matches!(record.resolution, TxnResolution::Commit(_)) =>
            {
                let affected_objs = Self::recorded_participant_targets(&record.participants);
                let mut completion_participants = BTreeSet::new();
                if !affected_objs.is_empty() {
                    let data_sites = self.data_sites_for_objs(&affected_objs).await?;
                    let (result, completed) =
                        self.sites_end(tid, &affected_objs, &data_sites).await;
                    if !matches!(result, Ok(EndResult::Success))
                        || completed.len() != record.participants.len()
                    {
                        return result;
                    }
                    completion_participants = completed;
                }
                self.complete_replayed_decision(tid, &record, completion_participants)
                    .map_err(|error| {
                        error!(
                            "Failed to persist replayed commit completion for {:?}: {:?}",
                            tid, error
                        );
                        TMError::Other
                    })?;
                Ok(EndResult::Success)
            }
            undo_log::CoordinatorStatus::Decided(record) => match record.resolution {
                TxnResolution::Abort => Err(TMError::InvalidTransactionState(TxnState::Aborted)),
                TxnResolution::Commit(_) => unreachable!(),
                TxnResolution::InProgress | TxnResolution::Unknown => {
                    Err(TMError::TransactionNotFound)
                }
            },
        }
    }

    fn complete_replayed_decision(
        &self,
        tid: &TxnId,
        decision: &undo_log::CoordinatorDecisionRecord,
        participants: BTreeSet<u64>,
    ) -> io::Result<undo_log::CoordinatorCompletionRecord> {
        let undo_log =
            self.deps.database_runtime.undo_log().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "undo log not configured")
            })?;
        if self.transactions.get(tid).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("transaction {tid:?} was restored while replaying completion"),
            ));
        }
        let prior_completed = self
            .completed_decisions
            .lock()
            .insert(*tid, DecisionRecord::cleanup_pending(decision.resolution));
        note_completion_cleanup_boundary(tid);
        #[cfg(test)]
        pause_after_completion_cleanup(tid);
        let completed =
            DecisionRecord::completed_at(decision.resolution, completion_boundary_time(tid));
        let completion = undo_log::CoordinatorCompletionRecord {
            resolution: decision.resolution,
            participants,
            expires_at_ms: completed
                .expires_at_ms
                .expect("a completed decision must have an expiry"),
            retired_participants: BTreeSet::new(),
            finalized_participants: BTreeSet::new(),
        };
        if let Err(error) = undo_log.write_coordinator_completion_record(tid, &completion) {
            let mut completed_decisions = self.completed_decisions.lock();
            if let Some(prior_completed) = prior_completed {
                completed_decisions.insert(*tid, prior_completed);
            } else {
                completed_decisions.remove(tid);
            }
            return Err(error);
        }
        self.completed_decisions.lock().insert(*tid, completed);
        if !completion.participants.is_empty() {
            self.retirement_records
                .lock()
                .insert(*tid, completion.clone());
            self.queue_retirement(*tid);
        }
        Ok(completion)
    }

    async fn retry_durable_abort_after_coordinator_restart(
        &self,
        tid: &TxnId,
    ) -> Result<AbortResult, TMError> {
        let replay_lock = self.replay_lock(tid);
        let _replay_guard = replay_lock.lock().await;
        let Some(undo_log) = self.deps.database_runtime.undo_log() else {
            return Err(TMError::TransactionNotFound);
        };
        let Some(status) = undo_log.coordinator_status(tid).map_err(|error| {
            error!(
                "Failed to read durable coordinator decision for {:?}: {:?}",
                tid, error
            );
            TMError::Other
        })?
        else {
            return Err(TMError::TransactionNotFound);
        };
        #[cfg(test)]
        tokio::task::yield_now().await;
        match status {
            undo_log::CoordinatorStatus::Completed(record) => {
                if record.finalized_participants != record.participants {
                    self.queue_retirement(*tid);
                }
                if get_time() >= record.expires_at_ms {
                    return Err(TMError::TransactionNotFound);
                }
                match record.resolution {
                    TxnResolution::Commit(_) => {
                        Ok(AbortResult::CheckFailed(CheckError::AlreadyCommitted))
                    }
                    TxnResolution::Abort => Ok(AbortResult::Success(None)),
                    TxnResolution::InProgress | TxnResolution::Unknown => {
                        Err(TMError::TransactionNotFound)
                    }
                }
            }
            undo_log::CoordinatorStatus::Decided(mut record)
                if matches!(
                    record.resolution,
                    TxnResolution::InProgress | TxnResolution::Abort
                ) =>
            {
                if record.resolution == TxnResolution::InProgress {
                    let participants: Vec<_> = record.participants.iter().copied().collect();
                    if let Err(error) =
                        undo_log.write_coordinator_abort_decision(tid, &participants)
                    {
                        error!(
                            "Failed to turn coordinator dispatch intent into Abort for {:?}: {:?}",
                            tid, error
                        );
                        return Ok(AbortResult::CheckFailed(CheckError::CannotEnd));
                    }
                    record.resolution = TxnResolution::Abort;
                }
                let affected_objs = Self::recorded_participant_targets(&record.participants);
                let mut completion_participants = BTreeSet::new();
                if !affected_objs.is_empty() {
                    let data_sites = self.data_sites_for_objs(&affected_objs).await?;
                    let (result, cleanup_finished, ended_participants) =
                        self.sites_abort(tid, &affected_objs, &data_sites).await;
                    if !Self::abort_cleanup_complete(&result) {
                        return result;
                    }
                    if cleanup_finished.len() != record.participants.len() {
                        return Ok(AbortResult::CheckFailed(CheckError::CannotEnd));
                    }
                    completion_participants = ended_participants;
                }
                self.complete_replayed_decision(tid, &record, completion_participants)
                    .map_err(|error| {
                        error!(
                            "Failed to persist replayed abort completion for {:?}: {:?}",
                            tid, error
                        );
                        TMError::Other
                    })?;
                Ok(AbortResult::Success(None))
            }
            undo_log::CoordinatorStatus::Decided(record) => match record.resolution {
                TxnResolution::Commit(_) => {
                    Ok(AbortResult::CheckFailed(CheckError::AlreadyCommitted))
                }
                TxnResolution::Abort => unreachable!(),
                TxnResolution::InProgress => unreachable!(),
                TxnResolution::Unknown => Err(TMError::TransactionNotFound),
            },
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
        txn.state = TxnState::Cleanup;
        txn.last_activity = get_time();
        let _ = self.transactions.remove(tid);
        self.txn_ids.lock().remove(tid);
        note_completion_cleanup_boundary(tid);
        #[cfg(test)]
        pause_after_completion_cleanup(tid);
    }

    async fn finish_transaction_guarded(
        &self,
        tid: &TxnId,
        txn_lock: &TxnMutex,
        txn: &mut Transaction,
        resolution: TxnResolution,
    ) -> io::Result<undo_log::CoordinatorCompletionRecord> {
        // The replay lock serializes completion against undo-log replay for
        // the same tid. Without an undo log replay can never run, and an
        // aborted transaction that never dispatched to a participant has no
        // record replay could visit, so those finishes skip the lock.
        let skip_replay_lock = self.deps.database_runtime.undo_log().is_none()
            && matches!(resolution, TxnResolution::Abort)
            && txn.completed_participants.is_empty()
            && txn.dispatch_participants.is_empty();
        if skip_replay_lock {
            return self.finish_transaction_guarded_with_replay_lock(tid, txn_lock, txn, resolution);
        }
        let replay_lock = self.replay_lock(tid);
        let _replay_guard = replay_lock.lock().await;
        self.finish_transaction_guarded_with_replay_lock(tid, txn_lock, txn, resolution)
    }

    fn finish_transaction_guarded_with_replay_lock(
        &self,
        tid: &TxnId,
        txn_lock: &TxnMutex,
        txn: &mut Transaction,
        resolution: TxnResolution,
    ) -> io::Result<undo_log::CoordinatorCompletionRecord> {
        let participants = txn.completed_participants.clone();
        let pending = DecisionRecord::cleanup_pending(resolution);
        let prior_completed = {
            let mut completed_decisions = self.completed_decisions.lock();
            let current = self.transactions.get(tid).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("transaction {tid:?} is not live during completion"),
                )
            })?;
            if !Arc::ptr_eq(&current, txn_lock) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("transaction {tid:?} was replaced during completion"),
                ));
            }
            let removed = self.transactions.remove(tid).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("transaction {tid:?} disappeared during completion"),
                )
            })?;
            if !Arc::ptr_eq(&removed, txn_lock) {
                let _ = self.transactions.try_insert(*tid, removed);
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("transaction {tid:?} changed identity during completion"),
                ));
            }
            self.txn_ids.lock().remove(tid);
            completed_decisions.insert(*tid, pending)
        };
        note_completion_cleanup_boundary(tid);
        #[cfg(test)]
        pause_after_completion_cleanup(tid);
        let completed = DecisionRecord::completed_at(resolution, completion_boundary_time(tid));
        let completion = undo_log::CoordinatorCompletionRecord {
            resolution,
            participants,
            expires_at_ms: completed
                .expires_at_ms
                .expect("a completed decision must have an expiry"),
            retired_participants: BTreeSet::new(),
            finalized_participants: BTreeSet::new(),
        };
        let undo_log = self.deps.database_runtime.undo_log();
        if let Some(undo_log) = undo_log {
            if let Err(error) = undo_log.write_coordinator_completion_record(tid, &completion) {
                let mut completed_decisions = self.completed_decisions.lock();
                match self.transactions.try_insert(*tid, txn_lock.clone()) {
                    None => {
                        self.txn_ids.lock().insert(*tid);
                    }
                    Some(existing) if Arc::ptr_eq(&existing, txn_lock) => {
                        self.txn_ids.lock().insert(*tid);
                    }
                    Some(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            format!(
                                "transaction {tid:?} could not restore its exact completion state"
                            ),
                        ));
                    }
                }
                if let Some(prior_completed) = prior_completed {
                    completed_decisions.insert(*tid, prior_completed);
                } else {
                    completed_decisions.remove(tid);
                }
                return Err(error);
            }
        }
        {
            let mut completed_decisions = self.completed_decisions.lock();
            txn.data.clear();
            txn.affected_objects.clear();
            txn.state = TxnState::Cleanup;
            txn.last_activity = get_time();
            completed_decisions.insert(*tid, completed);
        }
        if !completion.participants.is_empty() {
            self.retirement_records
                .lock()
                .insert(*tid, completion.clone());
            if undo_log.is_none() {
                self.volatile_retirement_discovery.lock().insert(*tid);
            }
        }
        Ok(completion)
    }

    fn cleanup_transaction(&self, tid: &TxnId) {
        if let Some(txn) = self.transactions.get(tid) {
            let mut txn_guard = txn.lock_blocking();
            self.cleanup_transaction_guarded(tid, &mut txn_guard);
        } else {
            self.txn_ids.lock().remove(tid);
        }
    }

    fn cleanup_stale_transaction_if_eligible(&self, tid: &TxnId, cutoff: i64) -> bool {
        let Some(txn) = self.transactions.get(tid) else {
            self.txn_ids.lock().remove(tid);
            return false;
        };
        let Some(mut txn_guard) = txn.try_lock() else {
            return false;
        };
        if txn_guard.last_activity >= cutoff
            || txn_guard.state != TxnState::Started
            || txn_guard.commit_dispatch_started
            || txn_guard.prepare_dispatch.abort_requested()
            || !txn_guard.prepare_dispatch.try_close()
        {
            return false;
        }
        self.cleanup_transaction_guarded(tid, &mut txn_guard);
        true
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
        let txn_ids: Vec<_> = self.txn_ids.lock().iter().copied().collect();
        let mut cleaned = 0;
        for tid in txn_ids {
            if self.cleanup_stale_transaction_if_eligible(&tid, cutoff) {
                warn!(
                    "Cleaning up stale transaction {:?} (likely client didn't call prepare/abort)",
                    tid
                );
                cleaned += 1;
            }
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
                if txn.prepare_dispatch.abort_requested() {
                    return Err(TMError::InvalidTransactionState(TxnState::Aborted));
                }
                if txn.commit_dispatch_started {
                    return Err(TMError::InvalidTransactionState(TxnState::Committed));
                }
                {
                    #[cfg(feature = "occ_phase_profile")]
                    let _phase_guard = super::phase_profile::guard(
                        super::phase_profile::Phase::AffectedObjectGrouping,
                    );
                    self.generate_affected_objs(&mut txn);
                }
                let data_sites = {
                    #[cfg(feature = "occ_phase_profile")]
                    let _phase_guard = super::phase_profile::guard(
                        super::phase_profile::Phase::PrepareParticipantLookup,
                    );
                    self.data_sites_for_objs(&txn.affected_objects).await?
                };
                if txn.dispatch_participants.is_empty() && !txn.affected_objects.is_empty() {
                    let participants: Vec<_> = txn.affected_objects.keys().copied().collect();
                    if self
                        .deps
                        .database_runtime
                        .chunks()
                        .durable_storage_configured()
                        && self.deps.database_runtime.undo_log().is_none()
                    {
                        return Err(TMError::Other);
                    }
                    if let Some(undo_log) = self.deps.database_runtime.undo_log() {
                        undo_log
                            .write_coordinator_dispatch_intent(tid, &participants)
                            .map_err(|error| {
                                error!(
                                    "Failed to persist coordinator dispatch intent for {:?}: {:?}",
                                    tid, error
                                );
                                TMError::Other
                            })?;
                    }
                    txn.dispatch_participants.extend(participants);
                }
                let sites_prepare_result = {
                    #[cfg(feature = "occ_phase_profile")]
                    let _phase_guard =
                        super::phase_profile::guard(super::phase_profile::Phase::PrepareBarrier);
                    self.sites_prepare(&tid, &txn.affected_objects, &data_sites)
                        .await?
                };
                if sites_prepare_result == DMPrepareResult::Success {
                    if txn.affected_objects.is_empty() {
                        TMPrepareResult::Success
                    } else {
                        let commit_hlc = self
                            .deps
                            .hlc
                            .try_now()
                            .map_err(|_| TMError::ClockExhausted)?;
                        debug_assert!(commit_hlc.ts > tid.ts);
                        txn.commit_dispatch_started = true;
                        txn.commit_hlc = Some(commit_hlc);
                        let sites_commit_result = {
                            #[cfg(feature = "occ_phase_profile")]
                            let _phase_guard = super::phase_profile::guard(
                                super::phase_profile::Phase::CommitBarrier,
                            );
                            self.sites_commit(&tid, commit_hlc, &txn.affected_objects, &data_sites)
                                .await?
                        };
                        match sites_commit_result {
                            DMCommitResult::Success => TMPrepareResult::Success,
                            _ => TMPrepareResult::DMCommitError(sites_commit_result),
                        }
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
        let failed = !matches!(&result, Ok(TMPrepareResult::Success));
        if failed {
            if let Ok(txn) = self.get_transaction(&tid) {
                txn.lock().await.prepare_dispatch.request_abort();
            }
            self.queue_abort_cleanup(tid);
            let _ = self.abort(tid.clone()).await;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::durable_fs::fail_next_directory_sync_for_test;
    use crate::ram::schema::Schema;
    use crate::ram::tests::default_fields;
    use crate::ram::types::{OwnedMap, OwnedValue};
    use crate::server::transactions;
    use crate::server::transactions::test_hlc;
    use crate::server::{NebServer, ServerOptions, Service};
    use dovahkiin::types::custom_types::id::Id;
    use dovahkiin::types::Map;
    use futures::future::join_all;
    use tempfile::TempDir;

    #[test]
    fn scoped_transaction_manager_service_ids_differ_between_databases() {
        let group = "group_a";
        assert_ne!(
            generate_scoped_service_id(group, "db_a"),
            generate_scoped_service_id(group, "db_b")
        );
    }

    #[test]
    fn volatile_retirement_discovery_physically_removes_completed_work_and_rotates_live_work() {
        let mut discovery = RetirementDiscoveryState::default();
        for offset in 0..(RETIREMENT_DISCOVERY_BATCH * 8) {
            let tid = test_hlc(20_000 + offset as u64, 70);
            discovery.insert(tid);
            discovery.remove(&tid);
        }
        let live = BTreeSet::from([
            test_hlc(40_001, 70),
            test_hlc(40_002, 70),
            test_hlc(40_003, 70),
        ]);
        for tid in &live {
            discovery.insert(*tid);
        }

        assert_eq!(
            discovery.storage_len(),
            live.len(),
            "successful retirement must physically remove all scheduling storage"
        );
        let mut observed = BTreeSet::new();
        for _ in 0..3 {
            observed.extend(discovery.candidates(1));
        }
        assert_eq!(
            observed, live,
            "bounded rotation must not starve live work behind completed history"
        );
    }

    #[test]
    fn data_object_point_cache_owns_only_logical_observations() {
        let obj = DataObject {
            server: 1,
            cell: None,
            expectation: CellExpectation::Absent(None),
            changed: false,
            new: false,
            point_cache: PointReadCache::default(),
        };
        assert!(obj.cell.is_none());
        assert!(obj.point_cache.header.is_none());
        assert!(obj.point_cache.projections.is_empty());
    }

    #[test]
    fn completed_decision_retention_uses_strict_expiry_boundary() {
        assert_eq!(
            COMPLETED_DECISION_RETENTION_MS, 300_000,
            "completed decisions must be retained for exactly 300 seconds"
        );
        let commit_hlc = test_hlc(500, 21);
        let completed_at_ms = 1_000_000;
        let record =
            DecisionRecord::completed_at(TxnResolution::Commit(commit_hlc), completed_at_ms);

        assert_eq!(
            record.resolution_at(completed_at_ms + COMPLETED_DECISION_RETENTION_MS - 1),
            Some(TxnResolution::Commit(commit_hlc))
        );
        assert_eq!(
            record.resolution_at(completed_at_ms + COMPLETED_DECISION_RETENTION_MS),
            None,
            "the completed decision must become Unknown exactly at 300 seconds"
        );
    }

    #[test]
    fn expired_resolution_can_retain_incomplete_retirement_metadata() {
        let expires_at_ms = 10_000;
        let status =
            undo_log::CoordinatorStatus::Completed(undo_log::CoordinatorCompletionRecord {
                resolution: TxnResolution::Abort,
                participants: BTreeSet::from([7]),
                expires_at_ms,
                retired_participants: BTreeSet::new(),
                finalized_participants: BTreeSet::new(),
            });

        assert_eq!(
            TransactionManager::durable_resolution_at(&status, expires_at_ms),
            TxnResolution::Unknown,
            "coordinator resolution visibility ends at the exact 300-second deadline"
        );
        assert!(matches!(
            status,
            undo_log::CoordinatorStatus::Completed(ref record)
                if record.finalized_participants != record.participants
        ));
    }

    #[test]
    fn completed_decision_cache_prunes_at_strict_expiry_boundary() {
        let deadline_ms = 1_300_000;
        let expired_tid = test_hlc(502, 21);
        let live_tid = test_hlc(503, 21);
        let mut records = HashMap::from([
            (
                expired_tid,
                DecisionRecord {
                    resolution: TxnResolution::Abort,
                    expires_at_ms: Some(deadline_ms),
                },
            ),
            (
                live_tid,
                DecisionRecord {
                    resolution: TxnResolution::Abort,
                    expires_at_ms: Some(deadline_ms + 1),
                },
            ),
        ]);

        TransactionManager::prune_completed_decision_records_at(&mut records, deadline_ms);

        assert!(!records.contains_key(&expired_tid));
        assert!(records.contains_key(&live_tid));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_lock_lookup_is_targeted_and_dead_entry_pruning_is_maintenance_only() {
        let address = "127.0.0.1:5486";
        let group = "txn_manager_targeted_replay_lock";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let dead_entries = 1_024;
        {
            let mut locks = manager.replay_locks.lock();
            for node in 0..dead_entries {
                locks.insert(test_hlc(10_000 + node, node), Weak::new());
            }
        }
        let target = test_hlc(20_000, 99);

        let live = manager.replay_lock(&target);
        assert_eq!(
            manager.replay_locks.lock().len(),
            dead_entries as usize + 1,
            "a normal targeted lookup must not sweep unrelated historical weak entries"
        );
        manager.prune_replay_locks();
        assert_eq!(
            manager.replay_locks.lock().len(),
            1,
            "periodic maintenance owns whole-map dead-entry pruning"
        );
        drop(live);
        manager.prune_replay_locks();
        assert!(manager.replay_locks.lock().is_empty());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn manager_resolve_unknown_retries_use_only_targeted_canonical_lookups() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5487";
        let group = "txn_manager_targeted_unknown_resolution";
        let server = start_durable_manager_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = runtime.txn_manager().unwrap().clone();
        let undo = runtime.undo_log().unwrap();
        let scans_after_startup = undo.full_log_scan_count_for_test();

        for offset in 0..256 {
            let unknown_tid = test_hlc(30_000 + offset, 98);
            assert_eq!(
                <TransactionManager as super::Service>::resolve(&manager, unknown_tid)
                    .await
                    .unwrap(),
                TxnResolution::Unknown
            );
        }
        let decided_tid = test_hlc(40_000, server.server_id);
        undo.write_coordinator_abort_decision(&decided_tid, &[])
            .unwrap();
        assert_eq!(
            <TransactionManager as super::Service>::resolve(&manager, decided_tid)
                .await
                .unwrap(),
            TxnResolution::Abort
        );
        assert_eq!(
            undo.full_log_scan_count_for_test(),
            scans_after_startup,
            "manager cache misses, Unknown retries, and a normal decision append must not rescan log files"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn paused_retirement_retry_does_not_hold_canonical_index_against_append() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5488";
        let group = "txn_manager_retirement_retry_append";
        let server = start_durable_manager_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = runtime.txn_manager().unwrap().clone();
        let undo = runtime.undo_log().unwrap().clone();
        let retirement_tid = test_hlc(41_000, server.server_id);
        let appended_tid = test_hlc(41_001, server.server_id);
        undo.write_coordinator_completion_record(
            &retirement_tid,
            &undo_log::CoordinatorCompletionRecord {
                resolution: TxnResolution::Abort,
                participants: BTreeSet::new(),
                expires_at_ms: get_time() + COMPLETED_DECISION_RETENTION_MS,
                retired_participants: BTreeSet::new(),
                finalized_participants: BTreeSet::new(),
            },
        )
        .unwrap();
        let pause = install_retirement_retry_pause(retirement_tid);
        let retry_manager = manager.clone();
        let retry =
            tokio::spawn(
                async move { retry_manager.retire_completion_once(&retirement_tid).await },
            );
        pause.wait_until_entered().await;

        let append_undo = undo.clone();
        let append = tokio::task::spawn_blocking(move || {
            append_undo.write_coordinator_abort_decision(&appended_tid, &[])
        });
        tokio::time::timeout(Duration::from_secs(1), append)
            .await
            .expect("normal append must not wait for paused retirement work")
            .unwrap()
            .unwrap();
        assert!(matches!(
            undo.coordinator_status(&appended_tid).unwrap(),
            Some(undo_log::CoordinatorStatus::Decided(_))
        ));

        pause.release();
        assert!(retry.await.unwrap());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn maintenance_worker_eventually_prunes_idle_expired_cache_entries() {
        let address = "127.0.0.1:5489";
        let group = "txn_manager_periodic_cache_prune";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let expired_tid = test_hlc(42_000, server.server_id);
        manager.completed_decisions.lock().insert(
            expired_tid,
            DecisionRecord {
                resolution: TxnResolution::Abort,
                expires_at_ms: Some(get_time().saturating_sub(1)),
            },
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !manager
                    .completed_decisions
                    .lock()
                    .contains_key(&expired_tid)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("periodic maintenance must prune an idle expired decision without a lookup");

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn coordinator_retention_starts_only_after_live_cleanup_boundary() {
        let address = "127.0.0.1:5480";
        let group = "txn_manager_completion_cleanup_boundary";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let tid = <TransactionManager as super::Service>::begin(&manager)
            .await
            .unwrap();
        let before_cleanup_ms = 1_000_000;
        let after_cleanup_ms = before_cleanup_ms + COMPLETED_DECISION_RETENTION_MS + 17;
        let _clock = install_completion_boundary_clock(tid, before_cleanup_ms, after_cleanup_ms);
        let txn_lock = manager.get_transaction(&tid).unwrap();
        let mut txn = txn_lock.lock().await;

        let completion = manager
            .finish_transaction_guarded(&tid, &txn_lock, &mut txn, TxnResolution::Abort)
            .await
            .unwrap();
        assert_eq!(
            completion.expires_at_ms,
            after_cleanup_ms + COMPLETED_DECISION_RETENTION_MS,
            "the exact retention deadline must be allocated at the post-cleanup boundary"
        );
        assert_eq!(
            manager.cached_resolution_at(
                &tid,
                after_cleanup_ms + COMPLETED_DECISION_RETENTION_MS - 1,
            ),
            Some(TxnResolution::Abort)
        );
        assert_eq!(
            manager.cached_resolution_at(&tid, after_cleanup_ms + COMPLETED_DECISION_RETENTION_MS,),
            None
        );
        assert_eq!(manager.transaction_count(), 0);
        drop(txn);

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resolve_never_observes_a_gap_between_live_cleanup_and_completion_publication() {
        let address = "127.0.0.1:5485";
        let group = "txn_manager_completion_publication_handoff";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let tid = <TransactionManager as super::Service>::begin(&manager)
            .await
            .unwrap();
        let pause = install_completion_cleanup_pause(tid);
        let abort_manager = manager.clone();
        let abort = tokio::spawn(async move {
            <TransactionManager as super::Service>::abort(&abort_manager, tid).await
        });
        pause.wait_until_entered().await;
        assert_eq!(manager.transaction_count(), 0);

        let resolve_manager = manager.clone();
        let mut resolve = tokio::spawn(async move {
            <TransactionManager as super::Service>::resolve(&resolve_manager, tid).await
        });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), &mut resolve)
                .await
                .expect("cleanup-pending resolution must remain immediately discoverable")
                .unwrap()
                .unwrap(),
            TxnResolution::Abort
        );

        pause.release();
        assert_eq!(abort.await.unwrap().unwrap(), AbortResult::Success(None));

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn completion_persistence_failure_keeps_cleanup_pending_until_exact_retry_boundary() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5481";
        let group = "txn_manager_completion_failure_replay";
        let server = start_durable_manager_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = runtime.txn_manager().unwrap().clone();
        let undo = runtime.undo_log().unwrap();
        let tid = <TransactionManager as super::Service>::begin(&manager)
            .await
            .unwrap();
        undo.write_coordinator_abort_decision(&tid, &[]).unwrap();
        let failed_cleanup_boundary = 1_000_000;
        let failed_clock = install_completion_boundary_clock(tid, 0, failed_cleanup_boundary);
        undo.fail_next_coordinator_completion_for_test();
        let txn_lock = manager.get_transaction(&tid).unwrap();
        let mut txn = txn_lock.lock().await;
        txn.state = TxnState::Aborted;
        txn.coordinator_decision_durable = true;

        assert!(manager
            .finish_transaction_guarded(&tid, &txn_lock, &mut txn, TxnResolution::Abort)
            .await
            .is_err());
        assert_eq!(
            manager.transaction_count(),
            1,
            "failed completion persistence must restore the exact live coordinator"
        );
        assert_eq!(
            manager.cached_resolution_at(
                &tid,
                failed_cleanup_boundary + COMPLETED_DECISION_RETENTION_MS,
            ),
            None,
            "a failed cleanup attempt must not leave any completed-retention entry"
        );
        drop(txn);
        assert_eq!(
            <TransactionManager as super::Service>::resolve(&manager, tid)
                .await
                .unwrap(),
            TxnResolution::Abort,
            "the restored live coordinator must remain explicitly resolvable"
        );
        assert!(matches!(
            undo.coordinator_status(&tid).unwrap(),
            Some(undo_log::CoordinatorStatus::Decided(
                undo_log::CoordinatorDecisionRecord {
                    resolution: TxnResolution::Abort,
                    ..
                }
            ))
        ));
        drop(failed_clock);

        let successful_cleanup_boundary =
            failed_cleanup_boundary + COMPLETED_DECISION_RETENTION_MS + 37;
        let _successful_clock = install_completion_boundary_clock(
            tid,
            failed_cleanup_boundary + COMPLETED_DECISION_RETENTION_MS,
            successful_cleanup_boundary,
        );
        assert_eq!(
            <TransactionManager as super::Service>::abort(&manager, tid)
                .await
                .unwrap(),
            AbortResult::Success(None),
            "retry must replay the older irrevocable decision and reconstruct completion"
        );
        let completed = match undo.coordinator_status(&tid).unwrap() {
            Some(undo_log::CoordinatorStatus::Completed(record)) => record,
            other => panic!("expected a durable completion after retry, got {other:?}"),
        };
        assert_eq!(
            completed.expires_at_ms,
            successful_cleanup_boundary + COMPLETED_DECISION_RETENTION_MS
        );
        assert_eq!(
            TransactionManager::durable_resolution_at(
                &undo_log::CoordinatorStatus::Completed(completed.clone()),
                successful_cleanup_boundary + COMPLETED_DECISION_RETENTION_MS - 1,
            ),
            TxnResolution::Abort
        );
        assert_eq!(
            TransactionManager::durable_resolution_at(
                &undo_log::CoordinatorStatus::Completed(completed),
                successful_cleanup_boundary + COMPLETED_DECISION_RETENTION_MS,
            ),
            TxnResolution::Unknown
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replayed_completion_retention_starts_after_cleanup_pending_publication() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5486";
        let group = "txn_manager_replayed_completion_boundary";
        let server = start_durable_manager_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = runtime.txn_manager().unwrap().clone();
        let undo = runtime.undo_log().unwrap();
        let tid = test_hlc(700, manager.deps.server_id);
        let commit_hlc = test_hlc(701, manager.deps.server_id);
        undo.write_coordinator_commit_decision(&tid, commit_hlc, &[])
            .unwrap();
        let before_pending_ms = 1_000_000;
        let after_pending_ms = before_pending_ms + COMPLETED_DECISION_RETENTION_MS + 23;
        let _clock = install_completion_boundary_clock(tid, before_pending_ms, after_pending_ms);

        assert_eq!(
            manager
                .retry_durable_commit_after_coordinator_restart(&tid)
                .await,
            Ok(EndResult::Success)
        );
        let completed = match undo.coordinator_status(&tid).unwrap() {
            Some(undo_log::CoordinatorStatus::Completed(record)) => record,
            other => panic!("expected replayed durable completion, got {other:?}"),
        };
        assert_eq!(
            completed.expires_at_ms,
            after_pending_ms + COMPLETED_DECISION_RETENTION_MS,
            "replay must allocate its exact window only after cleanup-pending publication"
        );
        assert_eq!(
            manager.cached_resolution_at(
                &tid,
                after_pending_ms + COMPLETED_DECISION_RETENTION_MS - 1,
            ),
            Some(TxnResolution::Commit(commit_hlc))
        );
        assert_eq!(
            manager.cached_resolution_at(&tid, after_pending_ms + COMPLETED_DECISION_RETENTION_MS,),
            None
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delayed_completion_sync_keeps_pending_explicit_and_deadline_at_logical_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5490";
        let group = "txn_manager_delayed_completion_sync";
        let server = start_durable_manager_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = runtime.txn_manager().unwrap().clone();
        let undo = runtime.undo_log().unwrap();
        let tid = <TransactionManager as super::Service>::begin(&manager)
            .await
            .unwrap();
        undo.write_coordinator_abort_decision(&tid, &[]).unwrap();
        let cleanup_boundary_ms = 1_000_000;
        let _clock = install_completion_boundary_clock(tid, 0, cleanup_boundary_ms);
        let pause = install_completion_cleanup_pause(tid);
        let txn_lock = manager.get_transaction(&tid).unwrap();
        {
            let mut txn = txn_lock.lock().await;
            txn.state = TxnState::Aborted;
            txn.coordinator_decision_durable = true;
        }

        let finish_manager = manager.clone();
        let finish_lock = txn_lock.clone();
        let finish = tokio::spawn(async move {
            let mut txn = finish_lock.lock().await;
            finish_manager
                .finish_transaction_guarded(&tid, &finish_lock, &mut txn, TxnResolution::Abort)
                .await
        });
        pause.wait_until_entered().await;
        assert_eq!(
            manager.transaction_count(),
            0,
            "cleanup-pending publication must atomically remove the live coordinator"
        );
        assert_eq!(
            manager
                .cached_resolution_at(&tid, cleanup_boundary_ms + COMPLETED_DECISION_RETENTION_MS),
            Some(TxnResolution::Abort),
            "a completion sync stalled past the nominal window must stay explicitly resolvable"
        );
        assert_eq!(
            manager.cached_resolution_at(&tid, i64::MAX),
            Some(TxnResolution::Abort),
            "cleanup-pending resolution must never expire while persistence is in flight"
        );
        let resolve_manager = manager.clone();
        let mut resolve = tokio::spawn(async move {
            <TransactionManager as super::Service>::resolve(&resolve_manager, tid).await
        });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), &mut resolve)
                .await
                .expect("cleanup-pending resolution must not wait for the stalled sync")
                .unwrap()
                .unwrap(),
            TxnResolution::Abort
        );

        pause.release();
        let completion = finish.await.unwrap().unwrap();
        assert_eq!(
            completion.expires_at_ms,
            cleanup_boundary_ms + COMPLETED_DECISION_RETENTION_MS,
            "the retention deadline must stay anchored at logical cleanup publication"
        );
        let completed = match undo.coordinator_status(&tid).unwrap() {
            Some(undo_log::CoordinatorStatus::Completed(record)) => record,
            other => panic!("expected a durable completion after the delayed sync, got {other:?}"),
        };
        assert_eq!(
            completed.expires_at_ms,
            cleanup_boundary_ms + COMPLETED_DECISION_RETENTION_MS,
            "a sync returning after the nominal window must not extend the durable deadline"
        );
        assert_eq!(
            TransactionManager::durable_resolution_at(
                &undo_log::CoordinatorStatus::Completed(completed.clone()),
                cleanup_boundary_ms + COMPLETED_DECISION_RETENTION_MS - 1,
            ),
            TxnResolution::Abort
        );
        assert_eq!(
            TransactionManager::durable_resolution_at(
                &undo_log::CoordinatorStatus::Completed(completed),
                cleanup_boundary_ms + COMPLETED_DECISION_RETENTION_MS,
            ),
            TxnResolution::Unknown,
            "a resolution first attempted after the late sync is Unknown by contract"
        );
        assert_eq!(
            manager
                .cached_resolution_at(&tid, cleanup_boundary_ms + COMPLETED_DECISION_RETENTION_MS),
            None,
            "the published cache record honors the same logical-cleanup deadline"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_prepare_dispatch_guard_hands_off_abort_without_leaking_inflight_state() {
        let address = "127.0.0.1:5482";
        let group = "txn_manager_dispatch_drop_handoff";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let tid = <TransactionManager as super::Service>::begin(&manager)
            .await
            .unwrap();
        let txn_lock = manager.get_transaction(&tid).unwrap();
        let txn = txn_lock.lock().await;
        let dispatch_state = txn.prepare_dispatch.clone();
        let dispatch_guard = dispatch_state
            .acquire(manager.self_ref.clone(), tid)
            .expect("open transaction must admit prepare dispatch");
        drop(txn);

        drop(dispatch_guard);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.transaction_count() == 0
                    && dispatch_state.word.load(Ordering::SeqCst) == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Drop must enqueue Abort cleanup and release the in-flight count");
        assert_eq!(
            <TransactionManager as super::Service>::resolve(&manager, tid)
                .await
                .unwrap(),
            TxnResolution::Abort
        );

        server.shutdown().await;
    }

    #[test]
    fn unresolved_durable_decision_never_uses_completed_retention_deadline() {
        let commit_hlc = test_hlc(501, 21);
        let durable = undo_log::CoordinatorStatus::Decided(undo_log::CoordinatorDecisionRecord {
            resolution: TxnResolution::Commit(commit_hlc),
            participants: BTreeSet::from([21, 22]),
        });

        assert_eq!(
            TransactionManager::durable_resolution_at(&durable, i64::MAX,),
            TxnResolution::Commit(commit_hlc),
            "an unresolved decision must remain replayable regardless of age"
        );
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

    async fn try_start_manager_test_server(
        address: &str,
        group: &str,
    ) -> Result<Arc<NebServer>, crate::server::ServerError> {
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: crate::ram::segs::SEGMENT_SIZE,
                db_size: crate::ram::segs::SEGMENT_SIZE,
                history_retention_ms: 300_000,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
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
    }

    async fn start_manager_test_server(address: &str, group: &str) -> Arc<NebServer> {
        try_start_manager_test_server(address, group).await.unwrap()
    }

    async fn start_durable_manager_test_server(
        address: &str,
        group: &str,
        temp_dir: &TempDir,
    ) -> Arc<NebServer> {
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: crate::ram::segs::SEGMENT_SIZE,
                db_size: crate::ram::segs::SEGMENT_SIZE,
                history_retention_ms: 300_000,
                tiered_config: None,
                backup_storage: None,
                wal_storage: Some(temp_dir.path().join("wal").to_string_lossy().into_owned()),
                undo_log_storage: Some(temp_dir.path().join("undo").to_string_lossy().into_owned()),
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

    async fn subscription_callback_snapshot() -> Option<(String, usize)> {
        let callback = bifrost::raft::client::CALLBACK
            .read()
            .await
            .as_ref()?
            .upgrade()?;
        Some((
            callback.server_address.clone(),
            Arc::strong_count(&callback) - 1,
        ))
    }

    async fn subscription_callback_shortcut_is_registered(address: &str) -> bool {
        bifrost::raft::state_machine::callback::get_local(
            hash_str(address),
            bifrost::raft::state_machine::callback::DEFAULT_SERVICE_ID,
        )
        .await
        .is_some()
    }

    #[test]
    fn sequential_manager_servers_rebind_subscription_callback_after_shutdown() {
        let _ = env_logger::try_init();
        let first_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let after_first_shutdown = first_runtime.block_on(async {
            let server =
                start_manager_test_server("127.0.0.1:5496", "txn_manager_callback_rebind_a").await;
            assert_eq!(
                subscription_callback_snapshot().await.map(|state| state.0),
                Some("127.0.0.1:5496".to_string())
            );
            server.shutdown().await;
            (
                subscription_callback_snapshot().await,
                subscription_callback_shortcut_is_registered("127.0.0.1:5496").await,
            )
        });
        drop(first_runtime);

        let after_first_runtime_drop =
            futures::executor::block_on(subscription_callback_snapshot());

        let second_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        second_runtime.block_on(async {
            let before_second_start = subscription_callback_snapshot().await;
            let server = try_start_manager_test_server(
                "127.0.0.1:5497",
                "txn_manager_callback_rebind_b",
            )
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "second startup failed with {error:?}; callback after first shutdown={after_first_shutdown:?}, after first runtime drop={after_first_runtime_drop:?}, before second startup={before_second_start:?}"
                )
            });
            assert_eq!(
                subscription_callback_snapshot().await.map(|state| state.0),
                Some("127.0.0.1:5497".to_string())
            );
            server.shutdown().await;
        });
    }

    #[test]
    fn graceful_shutdown_releases_complete_manager_server_graph() {
        let _ = env_logger::try_init();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let (
            server_ref,
            raft_service_ref,
            rpc_ref,
            database_runtime_ref,
            transaction_runtime_ref,
            manager_ref,
            consh_ref,
            membership_ref,
            raft_client_ref,
            callback_ref,
        ) = runtime.block_on(async {
            let server =
                start_manager_test_server("127.0.0.1:5498", "txn_manager_server_graph_drop").await;
            let database_runtime = server.current_database();
            let manager = database_runtime.txn_manager().unwrap().clone();
            let callback_ref = bifrost::raft::client::CALLBACK.read().await.clone();
            let refs = (
                Arc::downgrade(&server),
                Arc::downgrade(&server.raft_service),
                Arc::downgrade(&server.rpc),
                Arc::downgrade(&database_runtime),
                Arc::downgrade(&manager.deps.database_runtime),
                Arc::downgrade(&manager),
                Arc::downgrade(&server.consh),
                Arc::downgrade(&server.membership),
                Arc::downgrade(&server.raft_client),
                callback_ref,
            );

            server.shutdown().await;
            drop(manager);
            drop(database_runtime);
            drop(server);
            refs
        });
        drop(runtime);

        for _ in 0..100 {
            if server_ref.upgrade().is_none()
                && raft_service_ref.upgrade().is_none()
                && rpc_ref.upgrade().is_none()
                && database_runtime_ref.upgrade().is_none()
                && transaction_runtime_ref.upgrade().is_none()
                && manager_ref.upgrade().is_none()
                && consh_ref.upgrade().is_none()
                && membership_ref.upgrade().is_none()
                && raft_client_ref.upgrade().is_none()
                && callback_ref
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade)
                    .is_none()
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        let mut survivors = Vec::new();
        if server_ref.upgrade().is_some() {
            survivors.push("NebServer");
        }
        if raft_service_ref.upgrade().is_some() {
            survivors.push("RaftService");
        }
        if rpc_ref.upgrade().is_some() {
            survivors.push("rpc::Server");
        }
        if database_runtime_ref.upgrade().is_some() {
            survivors.push("final DatabaseRuntime");
        }
        if transaction_runtime_ref.upgrade().is_some() {
            survivors.push("transaction DatabaseRuntime");
        }
        if manager_ref.upgrade().is_some() {
            survivors.push("TransactionManager");
        }
        if consh_ref.upgrade().is_some() {
            survivors.push("ConsistentHashing");
        }
        if membership_ref.upgrade().is_some() {
            survivors.push("ObserverClient");
        }
        if raft_client_ref.upgrade().is_some() {
            survivors.push("RaftClient");
        }
        if callback_ref
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .is_some()
        {
            survivors.push("SubscriptionService");
        }
        assert!(
            survivors.is_empty(),
            "graceful shutdown retained {survivors:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_manager_releases_periodic_cleanup_worker_and_deps() {
        let address = "127.0.0.1:5495";
        let group = "txn_manager_cleanup_worker_drop";
        let server = start_manager_test_server(address, group).await;
        let deps = Arc::new(TransactionManagerDeps {
            database_runtime: server.current_database(),
            server_id: server.server_id,
            consh: server.consh.clone(),
            member_pool: server.member_pool.clone(),
            hlc: server.hlc.clone(),
        });
        let deps_ref = Arc::downgrade(&deps);
        let manager = TransactionManager::new(deps);
        let manager_ref = Arc::downgrade(&manager);

        drop(manager);
        tokio::time::timeout(Duration::from_millis(250), async {
            while manager_ref.upgrade().is_some() || deps_ref.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the periodic cleanup worker must not retain its manager or dependencies");

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn completed_abort_remains_resolvable_after_live_cleanup() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5352";
        let group = "txn_manager_completed_abort_retention";
        let server = start_manager_test_server(address, group).await;
        let txn = scoped_txn_client_for_database(address, group, group).await;
        let tid = txn.begin().await.unwrap().unwrap();

        assert_eq!(
            txn.abort(tid).await.unwrap().unwrap(),
            AbortResult::Success(None)
        );
        assert_eq!(
            txn.resolve(tid).await.unwrap().unwrap(),
            TxnResolution::Abort,
            "cleanup must install the bounded completed decision before removing live state"
        );
        assert_eq!(
            server
                .current_database()
                .txn_manager()
                .unwrap()
                .transaction_count(),
            0
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn active_read_only_commit_resolves_to_its_transaction_timestamp() {
        let address = "127.0.0.1:5450";
        let group = "txn_manager_active_read_only_resolution";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let tid = <TransactionManager as super::Service>::begin(&manager)
            .await
            .unwrap();
        {
            let txn_lock = manager.get_transaction(&tid).unwrap();
            let mut txn = txn_lock.lock().await;
            txn.state = TxnState::Committed;
            txn.coordinator_decision_durable = true;
            assert!(txn.commit_hlc.is_none());
        }

        assert_eq!(
            <TransactionManager as super::Service>::resolve(&manager, tid)
                .await
                .unwrap(),
            TxnResolution::Commit(tid),
            "an empty committed transaction still has an explicit final decision"
        );

        manager.cleanup_transaction(&tid);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn durable_completion_retires_participant_evidence_in_background() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5353";
        let group = "txn_manager_background_retirement";
        let server = start_durable_manager_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = runtime.txn_manager().unwrap().clone();
        let undo = runtime.undo_log().expect("durable transaction undo log");
        let tid = test_hlc(600, manager.deps.server_id);
        let expires_at_ms = get_time() + COMPLETED_DECISION_RETENTION_MS;

        undo.write_commit_marker(&tid).unwrap();
        undo.write_coordinator_completion_record(
            &tid,
            &undo_log::CoordinatorCompletionRecord {
                resolution: TxnResolution::Commit(tid),
                participants: BTreeSet::from([manager.deps.server_id]),
                expires_at_ms,
                retired_participants: BTreeSet::new(),
                finalized_participants: BTreeSet::new(),
            },
        )
        .unwrap();

        manager.queue_retirement(tid);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = undo.coordinator_status(&tid).unwrap();
                let participant = undo.participant_retirement(&tid).unwrap();
                if matches!(
                    status,
                    Some(undo_log::CoordinatorStatus::Completed(ref record))
                        if record.finalized_participants
                            == BTreeSet::from([manager.deps.server_id])
                ) && participant.as_ref().is_some_and(|record| record.finalized)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background retirement should durably prepare and finalize");

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retirement_discovery_worker_retries_after_transient_index_error() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5483";
        let group = "txn_manager_retirement_discovery_retry";
        let server = start_durable_manager_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = runtime.txn_manager().unwrap().clone();
        let undo = runtime.undo_log().unwrap();
        let tid = test_hlc(604, manager.deps.server_id);
        undo.write_abort_marker(&tid).unwrap();
        undo.write_coordinator_completion_record(
            &tid,
            &undo_log::CoordinatorCompletionRecord {
                resolution: TxnResolution::Abort,
                participants: BTreeSet::from([manager.deps.server_id]),
                expires_at_ms: get_time() + COMPLETED_DECISION_RETENTION_MS,
                retired_participants: BTreeSet::new(),
                finalized_participants: BTreeSet::new(),
            },
        )
        .unwrap();
        undo.fail_next_coordinator_completions_read_for_test();
        manager.restart_retirement_jobs();
        assert!(
            manager.retiring_transactions.lock().is_empty(),
            "the injected discovery failure must leave the record for a later sweep"
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(
                    undo.coordinator_status(&tid).unwrap(),
                    Some(undo_log::CoordinatorStatus::Completed(ref record))
                        if record.finalized_participants == record.participants
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the maintenance discovery worker must retry and finish retirement");

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lost_retirement_prepare_response_retries_from_durable_participant_evidence() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5355";
        let group = "txn_manager_lost_retirement_response";
        let server = start_durable_manager_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = runtime.txn_manager().unwrap().clone();
        let undo = runtime.undo_log().expect("durable transaction undo log");
        let tid = test_hlc(602, manager.deps.server_id);
        let expires_at_ms = get_time() + COMPLETED_DECISION_RETENTION_MS;

        undo.write_commit_marker(&tid).unwrap();
        undo.write_coordinator_completion_record(
            &tid,
            &undo_log::CoordinatorCompletionRecord {
                resolution: TxnResolution::Commit(tid),
                participants: BTreeSet::from([manager.deps.server_id]),
                expires_at_ms,
                retired_participants: BTreeSet::new(),
                finalized_participants: BTreeSet::new(),
            },
        )
        .unwrap();
        manager.drop_next_retirement_prepare_response_for_test();
        manager.queue_retirement(tid);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if undo
                    .participant_retirement(&tid)
                    .unwrap()
                    .is_some_and(|record| !record.finalized)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("lost response must leave durable unfinalized participant evidence");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = undo.coordinator_status(&tid).unwrap();
                let participant = undo.participant_retirement(&tid).unwrap();
                if matches!(
                    status,
                    Some(undo_log::CoordinatorStatus::Completed(ref record))
                        if record.finalized_participants
                            == BTreeSet::from([manager.deps.server_id])
                ) && participant.as_ref().is_some_and(|record| record.finalized)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("retry must idempotently acknowledge and finalize durable evidence");

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_resumes_incomplete_durable_retirement() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5354";
        let group = "txn_manager_restart_retirement";
        let tid;
        let server_id;

        {
            let server = start_durable_manager_test_server(address, group, &temp_dir).await;
            let runtime = server.current_database();
            let manager = runtime.txn_manager().unwrap().clone();
            let undo = runtime.undo_log().expect("durable transaction undo log");
            tid = test_hlc(601, manager.deps.server_id);
            server_id = manager.deps.server_id;
            undo.write_abort_marker(&tid).unwrap();
            undo.write_coordinator_completion_record(
                &tid,
                &undo_log::CoordinatorCompletionRecord {
                    resolution: TxnResolution::Abort,
                    participants: BTreeSet::from([server_id]),
                    expires_at_ms: get_time() + COMPLETED_DECISION_RETENTION_MS,
                    retired_participants: BTreeSet::new(),
                    finalized_participants: BTreeSet::new(),
                },
            )
            .unwrap();
            server.shutdown().await;
        }

        let restarted = start_durable_manager_test_server(address, group, &temp_dir).await;
        let runtime = restarted.current_database();
        let undo = runtime.undo_log().expect("reopened transaction undo log");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = undo.coordinator_status(&tid).unwrap();
                let participant = undo.participant_retirement(&tid).unwrap();
                if matches!(
                    status,
                    Some(undo_log::CoordinatorStatus::Completed(ref record))
                        if record.finalized_participants == BTreeSet::from([server_id])
                ) && participant.as_ref().is_some_and(|record| record.finalized)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("restart should resume incomplete durable retirement");

        restarted.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_restart_replay_writes_one_completion_record() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5356";
        let group = "txn_manager_concurrent_restart_replay";
        let server = start_durable_manager_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = runtime.txn_manager().unwrap().clone();
        let undo = runtime.undo_log().expect("durable transaction undo log");
        let tid = test_hlc(603, manager.deps.server_id);
        undo.write_coordinator_commit_decision(&tid, tid, &[])
            .unwrap();

        let (left, right) = tokio::join!(
            manager.retry_durable_commit_after_coordinator_restart(&tid),
            manager.retry_durable_commit_after_coordinator_restart(&tid),
        );
        assert_eq!(left, Ok(EndResult::Success));
        assert_eq!(right, Ok(EndResult::Success));
        assert_eq!(
            undo.coordinator_completion_write_count_for_test(),
            1,
            "per-transaction replay serialization must preserve the first completion"
        );

        server.shutdown().await;
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
        data.insert(&String::from("id"), OwnedValue::I64(id.lower as i64));
        data.insert(&String::from("score"), OwnedValue::U64(score));
        data.insert(
            &String::from("name"),
            OwnedValue::String(format!("cell-{score}")),
        );
        let mut cell =
            crate::ram::cell::OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(data));
        runtime.chunks().write_cell(&mut cell).unwrap();
    }

    fn counter_cell(schema_id: u32, id: Id, score: u64) -> OwnedCell {
        let mut data = OwnedMap::new();
        data.insert(&String::from("id"), OwnedValue::I64(id.lower as i64));
        data.insert(&String::from("score"), OwnedValue::U64(score));
        data.insert(
            &String::from("name"),
            OwnedValue::String(format!("cell-{score}")),
        );
        OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(data))
    }

    #[test]
    fn changed_existing_replacement_builds_update_commit_op() {
        let id = Id::new(0, 7001);
        let cell = counter_cell(1, id, 9);
        let commit_op = TransactionManager::commit_op_for_changed_data_obj(
            id,
            &DataObject {
                server: 1,
                cell: Some(cell.clone()),
                expectation: CellExpectation::Present(3),
                changed: true,
                new: false,
                point_cache: PointReadCache::default(),
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
    async fn coordinator_rotation_directory_failure_prevents_commit_success() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5439";
        let group = "txn_manager_coordinator_directory_sync_failure";
        let server = start_durable_manager_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = runtime.txn_manager().unwrap().clone();
        let client = scoped_txn_client_for_database(address, group, group).await;
        let tid = client.begin().await.unwrap().unwrap();
        let id = Id::new(0, 99055);
        {
            let txn_lock = manager.get_transaction(&tid).unwrap();
            let mut txn = txn_lock.lock().await;
            txn.state = TxnState::Prepared;
            txn.commit_hlc = Some(manager.deps.hlc.now());
            txn.affected_objects.insert(
                77,
                BTreeMap::from([(
                    id,
                    DataObject {
                        server: 77,
                        cell: Some(counter_cell(1, id, 2)),
                        expectation: CellExpectation::Present(1),
                        changed: true,
                        new: false,
                        point_cache: PointReadCache::default(),
                    },
                )]),
            );
        }
        let undo = runtime.undo_log().expect("durable coordinator undo log");
        undo.rotate_before_next_record_for_test();
        fail_next_directory_sync_for_test(&undo.log_directory_for_test());

        assert_eq!(
            client.commit(tid).await.unwrap().unwrap(),
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        let txn_lock = manager.get_transaction(&tid).unwrap();
        let txn = txn_lock.lock().await;
        assert_eq!(txn.state, TxnState::Committed);
        assert!(!txn.coordinator_decision_durable);
        drop(txn);
        assert_eq!(undo.coordinator_decision(&tid).unwrap(), None);

        manager.cleanup_transaction(&tid);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_cleanup_preserves_abort_decisions_for_retry() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5370";
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
    async fn stale_cleanup_rechecks_state_while_holding_the_same_transaction_guard() {
        let address = "127.0.0.1:5371";
        let group = "txn_manager_stale_cleanup_recheck";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let tid = <TransactionManager as super::Service>::begin(&manager)
            .await
            .unwrap();
        let txn_lock = manager.get_transaction(&tid).unwrap();
        {
            let mut txn = txn_lock.lock().await;
            txn.last_activity = 0;
            txn.state = TxnState::Prepared;
        }

        assert!(!manager.cleanup_stale_transaction_if_eligible(&tid, 1));
        assert!(manager.get_transaction(&tid).is_ok());
        assert_eq!(txn_lock.lock().await.state, TxnState::Prepared);

        manager.cleanup_transaction(&tid);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn affected_objs_retains_read_dependencies_for_rw_transaction() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5288";
        let group = "txn_manager_affected_objs_rw";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let read_id = Id::new(0, 7101);
        let write_id = Id::new(0, 7102);
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
                        point_cache: PointReadCache::default(),
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
                        point_cache: PointReadCache::default(),
                    },
                ),
            ]),
            affected_objects: AffectedObjs::new(),
            state: TxnState::Started,
            prepare_dispatch: Arc::new(PrepareDispatchState::new()),
            dispatch_participants: BTreeSet::new(),
            commit_dispatch_started: false,
            commit_hlc: None,
            coordinator_decision_durable: false,
            abort_cleanup_finished: BTreeSet::new(),
            completed_participants: BTreeSet::new(),
            last_activity: get_time(),
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
        let address = "127.0.0.1:5298";
        let group = "txn_manager_affected_objs_ro";
        let server = start_manager_test_server(address, group).await;
        let manager = server.current_database().txn_manager().unwrap().clone();
        let read_id = Id::new(0, 7201);
        let txn_mutex = Mutex::new(Transaction {
            data: HashMap::from([(
                read_id,
                DataObject {
                    server: 1,
                    cell: Some(counter_cell(1, read_id, 3)),
                    expectation: CellExpectation::Present(8),
                    changed: false,
                    new: false,
                    point_cache: PointReadCache::default(),
                },
            )]),
            affected_objects: AffectedObjs::new(),
            state: TxnState::Started,
            prepare_dispatch: Arc::new(PrepareDispatchState::new()),
            dispatch_participants: BTreeSet::new(),
            commit_dispatch_started: false,
            commit_hlc: None,
            coordinator_decision_durable: false,
            abort_cleanup_finished: BTreeSet::new(),
            completed_participants: BTreeSet::new(),
            last_activity: get_time(),
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
        let address = "127.0.0.1:5338";
        let group = "txn_occ_blind_update_version";
        let server = start_manager_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_basic_schema(&runtime);
        let cell_id = Id::new(0, 7301);

        let mut seeded = counter_cell(schema.id, cell_id, 2);
        runtime.chunks().write_cell(&mut seeded).unwrap();
        let original = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();

        let txn = scoped_txn_client_for_database(address, group, group).await;
        let tid = txn.begin().await.unwrap().unwrap();

        let blind_update = counter_cell(schema.id, cell_id, 7);
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
        assert_eq!(data_obj.expectation, CellExpectation::UnobservedPresent);
        assert!(data_obj.changed);

        drop(txn_state);
        let _ = txn.abort(tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blind_mutation_records_remove_version() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5339";
        let group = "txn_occ_blind_remove_version";
        let server = start_manager_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_basic_schema(&runtime);
        let cell_id = Id::new(0, 7302);

        let mut seeded = counter_cell(schema.id, cell_id, 4);
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
        assert_eq!(data_obj.expectation, CellExpectation::UnobservedPresent);
        assert!(data_obj.changed);

        drop(txn_state);
        let _ = txn.abort(tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_transaction_commits_leave_manager_empty() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5293";
        let group = "txn_manager_cleanup_single_db";
        let server = start_manager_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_basic_schema(&runtime);
        let cell_ids = (0..8)
            .map(|index| Id::new(0, (index + 1) as u64))
            .collect::<Vec<_>>();

        for (index, cell_id) in cell_ids.iter().enumerate() {
            seed_counter_cell(&runtime, schema.id, *cell_id, index as u64).await;
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
        let address = "127.0.0.1:5294";
        let group = "txn_manager_cleanup_multi_db";
        let server = start_manager_test_server(address, group).await;
        let analytics_runtime = server.ensure_database_runtime("analytics").await.unwrap();
        let default_runtime = server.current_database();
        let default_schema = install_basic_schema(&default_runtime);
        let analytics_schema = install_basic_schema(&analytics_runtime);
        let default_cell = Id::new(0, 1001);
        let analytics_cell = Id::new(0, 2001);

        seed_counter_cell(&default_runtime, default_schema.id, default_cell, 1).await;
        seed_counter_cell(&analytics_runtime, analytics_schema.id, analytics_cell, 1).await;

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
        let address = "127.0.0.1:5295";
        let group = "txn_manager_prepare_abort_race";
        let server = start_manager_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_basic_schema(&runtime);
        let hot_cell = Id::new(0, 3001);
        seed_counter_cell(&runtime, schema.id, hot_cell, 1).await;

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
            data.insert(&String::from("id"), OwnedValue::I64(hot_cell.lower as i64));
            data.insert(
                &String::from("score"),
                OwnedValue::U64((iteration + 2) as u64),
            );
            data.insert(
                &String::from("name"),
                OwnedValue::String(format!("writer-{iteration}")),
            );
            let updated_cell = crate::ram::cell::OwnedCell::new_with_id(
                schema.id,
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
        let address = "127.0.0.1:5296";
        let group = "txn_manager_prepare_abort_multi_db";
        let server = start_manager_test_server(address, group).await;
        let analytics_runtime = server.ensure_database_runtime("analytics").await.unwrap();
        let default_runtime = server.current_database();
        let default_schema = install_basic_schema(&default_runtime);
        let analytics_schema = install_basic_schema(&analytics_runtime);
        for index in 0..24 {
            let cell = Id::new(0, 4001 + index);
            seed_counter_cell(&default_runtime, default_schema.id, cell, 1).await;
            seed_counter_cell(&analytics_runtime, analytics_schema.id, cell, 1).await;
        }

        let results = join_all((0..48).map(|iteration| {
            let address = address.to_string();
            let group = group.to_string();
            async move {
                let (database_name, schema_id) = if iteration % 2 == 0 {
                    (group.clone(), default_schema.id)
                } else {
                    ("analytics".to_string(), analytics_schema.id)
                };
                // Each writer/reader pair shares a cell so the newer reader forces the
                // older writer's prepare failure. Using the same IDs in both databases
                // exercises service scoping without introducing unrelated intra-database
                // lock contention between pairs.
                let hot_cell = Id::new(0, 4001 + (iteration / 2) as u64);
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
                data.insert(&String::from("id"), OwnedValue::I64(hot_cell.lower as i64));
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
