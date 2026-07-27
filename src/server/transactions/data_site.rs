use super::*;
use crate::ram::cell::{InstalledRevision, OwnedCellRef, RevisionWrite};
use crate::ram::history::RevisionState;
use crate::ram::segs::SegmentReferenceGuard;
use crate::ram::types::{FromHeader, Id, OwnedMap, OwnedPrimArray, OwnedValue};
use crate::server::DatabaseRuntime;
use crate::{
    index::builder::IndexBuilder,
    ram::cell::{CellHeader, OwnedCell, ReadError, SnapshotRead, WriteError},
};
use bifrost::hlc::Hlc;
use bifrost::utils::time::get_time;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use futures::future::BoxFuture;
use lightning::linked_list::LinkedList;
use lightning::map::Map;
use lightning::map::PtrHashMap as LFMap;
#[cfg(test)]
use parking_lot::Condvar;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
#[cfg(test)]
use std::sync::{OnceLock, Weak};
use std::time::Duration;
#[cfg(test)]
use tokio::sync::Notify;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TXN_DATA_MANAGER_RPC_SERVICE) as u64;

pub fn generate_scoped_service_id(group: &str, database_name: &str) -> u64 {
    hash_str(&format!(
        "TXN_DATA_MANAGER_RPC_SERVICE-{}-{}",
        group, database_name
    ))
}

// Test-only instrumentation: counts how many full-cell participant `read` RPCs
// this process has served. Coordinator tests assert that a `head`/`read_selected`
// (shape-gated partial read) does NOT transfer the whole cell, i.e. does not
// increment this counter. The increment is a cheap relaxed add always compiled
// in; only tests read it via `full_read_rpc_count`.
static FULL_READ_RPC_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Number of full-cell participant `read` RPCs served since process start.
/// Test-only accessor; used to prove header/projection-only reads never fetch
/// the whole cell.
#[cfg(test)]
pub(crate) fn full_read_rpc_count() -> usize {
    FULL_READ_RPC_COUNT.load(Relaxed)
}

// Test age used to prove that elapsed wall-clock time never authorizes owner
// takeover. Stale-owner resolution is an explicit later protocol phase.
#[cfg(test)]
const LOCK_TIMEOUT_MS: i64 = 30_000;

#[cfg(test)]
struct PrepareDelayState {
    entered: AtomicBool,
    entered_notify: Notify,
    released: AtomicBool,
    released_notify: Notify,
}

#[cfg(test)]
pub(crate) struct PrepareDelayHandle {
    key: (TxnId, Id),
    state: Arc<PrepareDelayState>,
}

#[cfg(test)]
impl PrepareDelayHandle {
    pub(crate) async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    pub(crate) fn release(&self) {
        if !self
            .state
            .released
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.state.released_notify.notify_waiters();
        }
    }
}

#[cfg(test)]
impl Drop for PrepareDelayHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = prepare_delay_hooks().lock();
        let owns_registration = hooks
            .get(&self.key)
            .map(|state| Arc::ptr_eq(state, &self.state))
            .unwrap_or(false);
        if owns_registration {
            hooks.remove(&self.key);
        }
    }
}

#[cfg(test)]
static PREPARE_DELAY_HOOKS: OnceLock<Mutex<BTreeMap<(TxnId, Id), Arc<PrepareDelayState>>>> =
    OnceLock::new();

#[cfg(test)]
fn prepare_delay_hooks() -> &'static Mutex<BTreeMap<(TxnId, Id), Arc<PrepareDelayState>>> {
    PREPARE_DELAY_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn install_prepare_delay_for_cell(tid: TxnId, id: Id) -> PrepareDelayHandle {
    let key = (tid, id);
    let state = Arc::new(PrepareDelayState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: AtomicBool::new(false),
        released_notify: Notify::new(),
    });
    prepare_delay_hooks()
        .lock()
        .insert(key.clone(), state.clone());
    PrepareDelayHandle { key, state }
}

#[cfg(test)]
pub(crate) struct CommitDelayHandle {
    key: (TxnId, Id),
    state: Arc<PrepareDelayState>,
}

#[cfg(test)]
impl CommitDelayHandle {
    pub(crate) async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    pub(crate) fn release(&self) {
        if !self
            .state
            .released
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.state.released_notify.notify_waiters();
        }
    }
}

#[cfg(test)]
impl Drop for CommitDelayHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = commit_delay_hooks().lock();
        if hooks
            .get(&self.key)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            hooks.remove(&self.key);
        }
    }
}

#[cfg(test)]
static COMMIT_DELAY_HOOKS: OnceLock<Mutex<BTreeMap<(TxnId, Id), Arc<PrepareDelayState>>>> =
    OnceLock::new();

#[cfg(test)]
fn commit_delay_hooks() -> &'static Mutex<BTreeMap<(TxnId, Id), Arc<PrepareDelayState>>> {
    COMMIT_DELAY_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn install_commit_delay_for_cell(tid: TxnId, id: Id) -> CommitDelayHandle {
    let key = (tid, id);
    let state = Arc::new(PrepareDelayState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: AtomicBool::new(false),
        released_notify: Notify::new(),
    });
    commit_delay_hooks()
        .lock()
        .insert(key.clone(), state.clone());
    CommitDelayHandle { key, state }
}

#[cfg(test)]
struct BeforeStorageMutationState {
    entered: AtomicBool,
    entered_notify: Notify,
    released: Mutex<bool>,
    released_condvar: Condvar,
}

#[cfg(test)]
pub(crate) struct BeforeStorageMutationHandle {
    key: (TxnId, Id),
    state: Arc<BeforeStorageMutationState>,
}

#[cfg(test)]
impl BeforeStorageMutationHandle {
    pub(crate) async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    pub(crate) fn release(&self) {
        let mut released = self.state.released.lock();
        if !*released {
            *released = true;
            self.state.released_condvar.notify_all();
        }
    }
}

#[cfg(test)]
impl Drop for BeforeStorageMutationHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = before_storage_mutation_hooks().lock();
        if hooks
            .get(&self.key)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            hooks.remove(&self.key);
        }
    }
}

#[cfg(test)]
static BEFORE_STORAGE_MUTATION_HOOKS: OnceLock<
    Mutex<BTreeMap<(TxnId, Id), Arc<BeforeStorageMutationState>>>,
> = OnceLock::new();

#[cfg(test)]
fn before_storage_mutation_hooks(
) -> &'static Mutex<BTreeMap<(TxnId, Id), Arc<BeforeStorageMutationState>>> {
    BEFORE_STORAGE_MUTATION_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn install_before_storage_mutation_pause(
    tid: TxnId,
    id: Id,
) -> BeforeStorageMutationHandle {
    let key = (tid, id);
    let state = Arc::new(BeforeStorageMutationState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: Mutex::new(false),
        released_condvar: Condvar::new(),
    });
    before_storage_mutation_hooks()
        .lock()
        .insert(key.clone(), state.clone());
    BeforeStorageMutationHandle { key, state }
}

#[cfg(test)]
fn pause_before_storage_mutation(tid: &TxnId, id: &Id) {
    let state = before_storage_mutation_hooks()
        .lock()
        .remove(&(tid.clone(), *id));
    let Some(state) = state else {
        return;
    };
    state
        .entered
        .store(true, std::sync::atomic::Ordering::SeqCst);
    state.entered_notify.notify_waiters();
    let mut released = state.released.lock();
    while !*released {
        state.released_condvar.wait(&mut released);
    }
}

#[cfg(test)]
pub(crate) struct AbortCannotEndHandle {
    key: (TxnId, Id),
}

#[cfg(test)]
impl Drop for AbortCannotEndHandle {
    fn drop(&mut self) {
        abort_cannot_end_hooks().lock().remove(&self.key);
    }
}

#[cfg(test)]
static ABORT_CANNOT_END_HOOKS: OnceLock<Mutex<BTreeSet<(TxnId, Id)>>> = OnceLock::new();

#[cfg(test)]
fn abort_cannot_end_hooks() -> &'static Mutex<BTreeSet<(TxnId, Id)>> {
    ABORT_CANNOT_END_HOOKS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[cfg(test)]
pub(crate) fn install_abort_cannot_end_for_cell(tid: TxnId, id: Id) -> AbortCannotEndHandle {
    let key = (tid, id);
    abort_cannot_end_hooks().lock().insert(key.clone());
    AbortCannotEndHandle { key }
}

#[cfg(test)]
pub(crate) struct EndPromotionFailureHandle {
    key: (TxnId, Id),
}

#[cfg(test)]
impl Drop for EndPromotionFailureHandle {
    fn drop(&mut self) {
        end_promotion_failure_hooks().lock().remove(&self.key);
    }
}

#[cfg(test)]
static END_PROMOTION_FAILURE_HOOKS: OnceLock<Mutex<BTreeSet<(TxnId, Id)>>> = OnceLock::new();

#[cfg(test)]
fn end_promotion_failure_hooks() -> &'static Mutex<BTreeSet<(TxnId, Id)>> {
    END_PROMOTION_FAILURE_HOOKS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[cfg(test)]
pub(crate) fn install_end_promotion_failure(tid: TxnId, id: Id) -> EndPromotionFailureHandle {
    let key = (tid, id);
    end_promotion_failure_hooks().lock().insert(key.clone());
    EndPromotionFailureHandle { key }
}

#[cfg(test)]
fn should_fail_end_promotion(tid: &TxnId, id: &Id) -> bool {
    end_promotion_failure_hooks()
        .lock()
        .contains(&(tid.clone(), *id))
}

type CommitHistory = BTreeMap<Id, CellHistory>;
type CellMetaMutex = Arc<Mutex<CellMeta>>;
type TxnMutex = Arc<Mutex<Transaction>>;

/// Per-cell metadata for concurrency control
///
/// Implements a hybrid timestamp-ordering + lock-based protocol with Wait-Die:
/// - `read` / `write`: Track timestamps for timestamp-ordering validation
/// - `owner`: Acts as a write lock during prepare/commit phases
/// - `lock_acquired_at`: Timestamp used for diagnostics only
///
/// Wait-Die Protocol:
/// - When a transaction wants to acquire a cell already owned by another:
///   - If requester is YOUNGER (higher timestamp): DIE (abort immediately)
///   - If requester is OLDER (lower timestamp): WAIT (backoff and retry)
/// - This prevents deadlock while reducing contention on hot cells
/// - Waiters poll via backoff rather than blocking on a condition variable
///
/// Owner age never authorizes takeover. Task 10 adds explicit stale-owner
/// resolution; until then every owner participates in Wait-Die.
#[derive(Debug)]
pub struct CellMeta {
    read: TxnId,
    write: TxnId,
    owner: Option<TxnPriority>, // transaction that owns the cell during prepare/commit
    lock_acquired_at: Option<i64>, // timestamp when lock was acquired (milliseconds since epoch)
}

struct Transaction {
    state: TxnState,
    affected_cells: Vec<Id>,
    certified: BTreeMap<Id, PrepareOp>,
    coordinator_id: Option<u64>,
    installed: BTreeMap<Id, InstalledRevision>,
    commit_hlc: Option<Hlc>,
    commit_ops: Option<Vec<CommitOp>>,
    installed_output_durable: bool,
    compensation_output_durable: bool,
    durable_decision: Option<TxnState>,
    last_activity: i64,
    history: CommitHistory,
    /// RAII guards that hold segment references during this transaction
    /// Automatically released when guards are dropped (no leak risk)
    rollback_guards: Vec<SegmentReferenceGuard>,
}

struct CellHistory {
    cell: Option<OwnedCellRef>,
    compensation: Option<InstalledRevision>,
}

impl CellHistory {
    pub fn new(cell: Option<OwnedCellRef>) -> CellHistory {
        CellHistory {
            cell,
            compensation: None,
        }
    }
}

impl Transaction {
    fn certified_op(&self, id: &Id) -> Option<&PrepareOp> {
        self.certified.get(id)
    }

    fn certified_present_revision_ts(&self, id: &Id) -> Option<u64> {
        match self.certified_op(id).map(|op| &op.expectation) {
            Some(CellExpectation::Present(revision_ts)) => Some(*revision_ts),
            _ => None,
        }
    }

    fn certified_expects_absent_write(&self, id: &Id) -> bool {
        matches!(
            self.certified_op(id),
            Some(PrepareOp {
                intent: PrepareIntent::Write,
                expectation: CellExpectation::Absent(_),
                ..
            })
        )
    }
}

pub struct DataManager {
    cells: LFMap<Id, Arc<Mutex<CellMeta>>>,
    txns: LFMap<TxnId, Arc<Mutex<Transaction>>>,
    cell_list: LinkedList<Id>,
    txns_sorted: Mutex<BTreeSet<TxnId>>,
    database_runtime: Arc<DatabaseRuntime>,
    cleanup_signal: Arc<AtomicBool>,
    /// Per-server Hybrid Logical Clock source (node = server_id), shared with
    /// the coordinator-side `TransactionManager`. Stamps every participant
    /// response clock and observes the coordinator's incoming clock.
    hlc: Arc<bifrost::hlc::HlcSource>,
    #[cfg(test)]
    fail_next_undo_availability: AtomicBool,
}

#[cfg(test)]
type TestDataManagerKey = (u64, String, String);

#[cfg(test)]
static TEST_DATA_MANAGERS: OnceLock<Mutex<BTreeMap<TestDataManagerKey, Weak<DataManager>>>> =
    OnceLock::new();

#[cfg(test)]
fn test_data_managers() -> &'static Mutex<BTreeMap<TestDataManagerKey, Weak<DataManager>>> {
    TEST_DATA_MANAGERS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn register_data_manager_for_test(manager: &Arc<DataManager>) {
    let key = (
        manager.hlc.node(),
        manager.database_runtime.group_name().to_string(),
        manager.database_runtime.database_name().to_string(),
    );
    test_data_managers()
        .lock()
        .insert(key, Arc::downgrade(manager));
}

#[cfg(test)]
pub(crate) fn participant_owner_for_test(
    server_id: u64,
    group_name: &str,
    database_name: &str,
    id: &Id,
) -> Option<TxnPriority> {
    let key = (server_id, group_name.to_string(), database_name.to_string());
    let manager = test_data_managers().lock().get(&key)?.upgrade()?;
    let cell_meta = manager.cells.get(id)?;
    let owner = cell_meta.lock().owner.clone();
    owner
}

service! {
    rpc read(server_id: u64, clock: Hlc, tid: TxnId, id: Id) -> DataSiteResponse<TxnExecResult<ObservedPoint<OwnedCell>, ReadError>>;
    rpc read_selected(server_id: u64, clock: Hlc, tid: TxnId, id: Id, fields: Vec<u64>) -> DataSiteResponse<TxnExecResult<ObservedPoint<OwnedCell>, ReadError>>;
    rpc read_partial_raw(server_id: u64, clock: Hlc, tid: TxnId, id: Id, offset: usize, len: usize) -> DataSiteResponse<TxnExecResult<ObservedPoint<Vec<u8>>, ReadError>>;
    rpc head(server_id: u64, clock: Hlc, tid: TxnId, id: Id) -> DataSiteResponse<TxnExecResult<ObservedPoint<CellHeader>, ReadError>>;
    // two phase commit
    rpc prepare(coordinator_id: u64, clock: Hlc, tid: TxnId, ops: Vec<PrepareOp>) -> DataSiteResponse<DMPrepareResult>;
    rpc commit(commit_hlc: Hlc, tid: TxnId, cells: Vec<CommitOp>) -> DataSiteResponse<DMCommitResult>;

    // because there may be some exception on commit, abort have to handle 'committed' and 'committing' transactions
    // for committed transaction, abort need to recover the data according to it's cells history
    rpc abort(clock: Hlc, tid: TxnId) -> DataSiteResponse<AbortResult>;

    // there also should be a 'end' from transaction manager to inform data manager to clean up and release cell locks
    rpc end(clock: Hlc, tid: TxnId) -> DataSiteResponse<EndResult>;
}

dispatch_rpc_service_functions!(DataManager);

service_with_id!(DataManager, DEFAULT_SERVICE_ID);

impl DataManager {
    pub fn new(
        database_runtime: Arc<DatabaseRuntime>,
        hlc: Arc<bifrost::hlc::HlcSource>,
    ) -> Arc<Self> {
        let cleanup_signal = Arc::new(AtomicBool::new(false));
        let manager = Arc::new(Self {
            cells: LFMap::with_capacity(256),
            txns: LFMap::with_capacity(128),
            cell_list: LinkedList::new(),
            txns_sorted: Mutex::new(BTreeSet::new()),
            database_runtime,
            cleanup_signal: cleanup_signal.clone(),
            hlc,
            #[cfg(test)]
            fail_next_undo_availability: AtomicBool::new(false),
        });
        #[cfg(test)]
        register_data_manager_for_test(&manager);

        let manager_clone = manager.clone();
        tokio::spawn(async move {
            loop {
                if cleanup_signal.load(Relaxed) {
                    manager_clone.cell_meta_cleanup().await;
                    cleanup_signal.store(false, Relaxed);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        // Spawn undo log trimming task if undo log is enabled
        if manager.undo_log().is_some() {
            let manager_clone = manager.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(300)).await; // Trim every 5 minutes
                    if let Some(undo_log) = manager_clone.undo_log() {
                        if let Err(e) = undo_log.trim_old_logs() {
                            error!("Failed to trim undo logs: {:?}", e);
                        } else {
                            debug!("Successfully trimmed old undo logs");
                        }
                    }
                }
            });
        }

        return manager;
    }
    fn update_clock(&self, clock: Hlc) {
        self.hlc.observe(clock);
    }
    #[inline]
    fn get_or_create_transaction(&self, tid: &TxnId) -> TxnMutex {
        // Fast path: transaction already exists
        if let Some(txn) = self.txns.get(tid) {
            return txn;
        }

        // Slow path: create new transaction
        self.create_transaction(tid)
    }
    #[inline]
    fn find_transaction(&self, tid: &TxnId) -> Option<TxnMutex> {
        self.txns.get(tid)
    }

    #[cold]
    fn create_transaction(&self, tid: &TxnId) -> TxnMutex {
        loop {
            if let Some(txn) = self.txns.get(tid) {
                return txn;
            }

            let txn = Arc::new(Mutex::new(Transaction {
                state: TxnState::Started,
                affected_cells: Vec::with_capacity(8), // Pre-allocate for common case
                certified: BTreeMap::new(),
                coordinator_id: None,
                installed: BTreeMap::new(),
                commit_hlc: None,
                commit_ops: None,
                installed_output_durable: false,
                compensation_output_durable: false,
                durable_decision: None,
                last_activity: get_time(),
                history: BTreeMap::new(),
                rollback_guards: Vec::with_capacity(4), // Pre-allocate for common case
            }));

            if self.txns.insert(tid.clone(), txn.clone()).is_none() {
                self.txns_sorted.lock().insert(tid.clone());
                return txn;
            }
        }
    }
    fn cell_meta_mutex(&self, id: &Id) -> CellMetaMutex {
        // Check if entry exists to avoid duplicate list insertions
        let is_new = self.cells.get(id).is_none();
        let arc = self.cells.get_or_insert(*id, || {
            Arc::new(Mutex::new(CellMeta {
                read: TxnId::default(),
                write: TxnId::default(),
                owner: None,
                lock_acquired_at: None,
            }))
        });
        // Only push to list if this was a new insertion
        if is_new {
            self.cell_list.push_back(*id);
        }
        arc
    }
    fn response_with<T: Send>(&self, data: T) -> BoxFuture<'_, DataSiteResponse<T>>
    where
        T: 'static,
    {
        future::ready(DataSiteResponse::new(self.hlc.now(), data)).boxed()
    }

    #[cfg(test)]
    fn take_matching_prepare_delay(
        tid: &TxnId,
        prepared_ops: &[PrepareOp],
    ) -> Option<Arc<PrepareDelayState>> {
        let mut hooks = prepare_delay_hooks().lock();
        let delayed_key = prepared_ops
            .iter()
            .map(|op| (tid.clone(), op.id))
            .find(|key| hooks.contains_key(key))?;
        hooks.remove(&delayed_key)
    }

    #[cfg(test)]
    fn take_matching_abort_cannot_end(tid: &TxnId, affected_cells: &[Id]) -> bool {
        let mut hooks = abort_cannot_end_hooks().lock();
        let Some(key) = affected_cells
            .iter()
            .map(|id| (tid.clone(), *id))
            .find(|key| hooks.contains(key))
        else {
            return false;
        };
        hooks.remove(&key)
    }

    #[cfg(test)]
    async fn await_prepare_delay(state: &Arc<PrepareDelayState>) {
        state
            .entered
            .store(true, std::sync::atomic::Ordering::SeqCst);
        state.entered_notify.notify_waiters();
        let notified = state.released_notify.notified();
        if state.released.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    #[cfg(test)]
    fn take_matching_commit_delay(
        tid: &TxnId,
        cells: &[CommitOp],
    ) -> Option<Arc<PrepareDelayState>> {
        let mut hooks = commit_delay_hooks().lock();
        let delayed_key = cells
            .iter()
            .filter_map(|op| Self::commit_op_cell_id(op).ok())
            .map(|id| (*tid, id))
            .find(|key| hooks.contains_key(key))?;
        hooks.remove(&delayed_key)
    }

    fn canonical_prepare_ops(ops: Vec<PrepareOp>) -> Result<Vec<PrepareOp>, DMPrepareResult> {
        let mut by_id = BTreeMap::new();
        for op in ops {
            if by_id.insert(op.id, op).is_some() {
                return Err(DMPrepareResult::NotRealizable);
            }
        }
        if by_id.is_empty() {
            return Err(DMPrepareResult::NotRealizable);
        }
        Ok(by_id.into_values().collect())
    }

    fn current_expectation(&self, id: &Id) -> Result<CellExpectation, ReadError> {
        match self.chunks().head_snapshot(id, u64::MAX)? {
            SnapshotRead::Present(header) => Ok(CellExpectation::Present(header.revision_ts)),
            SnapshotRead::Absent(tombstone_revision_ts) => {
                Ok(CellExpectation::Absent(tombstone_revision_ts))
            }
            SnapshotRead::Wait => Err(ReadError::NotMatch),
        }
    }

    fn prepare_expectation_matches(&self, op: &PrepareOp) -> bool {
        self.current_expectation(&op.id)
            .is_ok_and(|current| current == op.expectation)
    }

    #[inline]
    fn chunks(&self) -> &Arc<crate::ram::chunk::Chunks> {
        self.database_runtime.chunks()
    }

    #[inline]
    fn undo_log(&self) -> Option<&Arc<super::undo_log::UndoLogger>> {
        self.database_runtime.undo_log()
    }

    #[cfg(test)]
    fn fail_next_undo_availability_for_test(&self) {
        self.fail_next_undo_availability
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Create a segment reference guard to prevent eviction during transaction.
    /// Returns None if segment not found (already freed/evicted).
    /// The guard is RAII - reference is automatically released when dropped.
    #[inline]
    fn acquire_segment_guard(
        &self,
        chunk_idx: usize,
        segment_id: u64,
    ) -> Option<SegmentReferenceGuard> {
        if let Some(chunk) = self.chunks().list.get(chunk_idx) {
            if let Some(segment) = chunk.segs.get(&(segment_id as usize)) {
                return Some(SegmentReferenceGuard::new(segment));
            }
        }
        warn!(
            "Could not find segment {} in chunk {} to acquire guard",
            segment_id, chunk_idx
        );
        None
    }
    fn rollback(
        &self,
        history: &mut CommitHistory,
        installed_revisions: &BTreeMap<Id, InstalledRevision>,
    ) -> Vec<RollbackFailure> {
        let mut failures = Vec::new();
        for (id, history) in history.iter_mut() {
            debug!("ROLLING BACK {:?}", id);
            let install_result = if history.compensation.is_none() {
                installed_revisions
                    .get(id)
                    .ok_or(WriteError::CellRevisionMismatch)
                    .and_then(|installed| {
                        self.chunks().compensate(
                            installed,
                            history.cell.as_ref().map(|cell| cell.clone_referred()),
                        )
                    })
                    .map(|compensation| {
                        history.compensation = Some(compensation);
                    })
            } else {
                Ok(())
            };
            let result = install_result.and_then(|()| {
                let compensation = history
                    .compensation
                    .as_ref()
                    .expect("successful compensation install retains its exact handle");
                self.chunks()
                    .force_sync_installed_revisions([compensation])
                    .map_err(|error| WriteError::DurabilityFailure(error.to_string()))
            });
            let error = result.err();
            if let Some(error) = error {
                failures.push(RollbackFailure { id: *id, error });
            }
        }
        failures
    }
    #[inline]
    fn guarded_txn_cell_ids(txn: &Transaction) -> Vec<Id> {
        txn.affected_cells
            .iter()
            .copied()
            .chain(txn.certified.keys().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    #[inline]
    fn wipe_out_transaction(&self, tid: &TxnId) {
        let _ = self.txns.remove(tid);
        self.txns_sorted.lock().remove(tid);
    }
    async fn cell_meta_cleanup(&self) {
        let oldest_transaction = {
            self.txns_sorted
                .lock()
                .iter()
                .next()
                .cloned()
                .unwrap_or_else(|| self.hlc.now())
        };
        let mut cells_to_evict = Vec::new();

        // First pass: collect cells with their metadata (while holding locks)
        for cell_id_ref in self.cell_list.iter_front() {
            let cell_id = cell_id_ref.deref();
            if let Some(cell_meta) = self.cells.get(&cell_id) {
                let meta = cell_meta.lock();
                if meta.write < oldest_transaction && meta.read < oldest_transaction {
                    cells_to_evict.push((cell_id_ref, cell_meta.clone()));
                } else {
                    // Cells are ordered by activity, stop at first active one
                    break;
                }
            } else {
                // Cell not in map but in list - just remove from list
                cell_id_ref.remove();
            }
        }

        // Second pass: re-check and remove (prevents TOCTOU race)
        let empty_clock = TxnId::default();
        for (cell_id_ref, cell_meta) in cells_to_evict {
            let cell_id = cell_id_ref.deref();

            // Re-check timestamps while holding lock before removal
            // This prevents evicting cells that became active after first check
            let should_evict = {
                let meta = cell_meta.lock();
                meta.write < oldest_transaction
                    && meta.read < oldest_transaction
                    && meta.owner.is_none() // Don't evict if locked by a transaction
                    // Skip metas that have never been stamped (both clocks still
                    // empty). Such a meta belongs to an in-flight prepare that has
                    // created it via `cell_meta_mutex` but not yet assigned its
                    // owner (an insert prepare stamps neither `read` nor `write`
                    // before acquiring the owner). The owner check above cannot see
                    // that pending acquisition, and the removal below runs after
                    // this lock is released, so evicting here would race the
                    // prepare and orphan the lock it is about to take. Read/observe
                    // paths stamp `read` before prepare, so update/remove metas are
                    // never empty at this point; the only cost is that a genuinely
                    // abandoned empty meta is reclaimed on a later pass (once it is
                    // stamped or re-accessed) rather than now.
                    && !(meta.read == empty_clock && meta.write == empty_clock)
            };

            if should_evict {
                self.cells.remove(&cell_id);
                cell_id_ref.remove();
            }
        }
    }
    fn prepare_read<T: Send>(
        &self,
        clock: &Hlc,
        tid: &TxnId,
        id: &Id,
    ) -> Result<(), BoxFuture<'_, DataSiteResponse<TxnExecResult<T, ReadError>>>>
    where
        T: 'static + Clone,
    {
        self.update_clock(*clock);
        let meta_ref = self.cell_meta_mutex(id);
        let mut meta = meta_ref.lock();
        if meta.owner.is_some() {
            debug!(
                "-> READ {:?} WAITING for {:?} to finish commit on cell {:?}",
                tid, &meta.owner, id
            );
            return Err(self.response_with(TxnExecResult::Wait));
        }

        if meta.read < *tid {
            meta.read = *tid;
        }
        Ok(())
    }

    fn observed_snapshot<T, F>(
        snapshot: Result<SnapshotRead<T>, ReadError>,
        revision_ts: F,
    ) -> TxnExecResult<ObservedPoint<T>, ReadError>
    where
        T: Send + Clone,
        F: FnOnce(&T) -> u64,
    {
        match snapshot {
            Ok(SnapshotRead::Present(value)) => {
                let expectation = CellExpectation::Present(revision_ts(&value));
                TxnExecResult::Accepted(ObservedPoint {
                    value: Some(value),
                    expectation,
                })
            }
            Ok(SnapshotRead::Absent(delete_revision_ts)) => {
                TxnExecResult::Accepted(ObservedPoint {
                    value: None,
                    expectation: CellExpectation::Absent(delete_revision_ts),
                })
            }
            Ok(SnapshotRead::Wait) => TxnExecResult::Wait,
            Err(error) => TxnExecResult::Error(error),
        }
    }

    fn warn_on_index_wait_results<I>(&self, tid: &TxnId, results: I)
    where
        I: IntoIterator<
            Item = Result<Result<(), crate::index::builder::IndexError>, tokio::task::JoinError>,
        >,
    {
        for result in results {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!(
                        "Index task failed during transaction commit {:?}: {:?}",
                        tid, error
                    );
                }
                Err(error) => {
                    warn!(
                        "Index task join failed during transaction commit {:?}: {:?}",
                        tid, error
                    );
                }
            }
        }
    }

    fn exact_slice_eq<T>(left: &[T], right: &[T], elements_equal: impl Fn(&T, &T) -> bool) -> bool {
        left.len() == right.len()
            && left
                .iter()
                .zip(right)
                .all(|(left, right)| elements_equal(left, right))
    }

    fn exact_owned_map_eq(left: &OwnedMap, right: &OwnedMap) -> bool {
        left.fields == right.fields
            && left.map.len() == right.map.len()
            && left.map.iter().all(|(key, left_value)| {
                right
                    .map
                    .get(key)
                    .is_some_and(|right_value| Self::exact_owned_value_eq(left_value, right_value))
            })
    }

    fn exact_owned_primitive_array_eq(left: &OwnedPrimArray, right: &OwnedPrimArray) -> bool {
        match left {
            OwnedPrimArray::Bool(left) => {
                matches!(right, OwnedPrimArray::Bool(right) if left == right)
            }
            OwnedPrimArray::Char(left) => {
                matches!(right, OwnedPrimArray::Char(right) if left == right)
            }
            OwnedPrimArray::I8(left) => {
                matches!(right, OwnedPrimArray::I8(right) if left == right)
            }
            OwnedPrimArray::I16(left) => {
                matches!(right, OwnedPrimArray::I16(right) if left == right)
            }
            OwnedPrimArray::I32(left) => {
                matches!(right, OwnedPrimArray::I32(right) if left == right)
            }
            OwnedPrimArray::I64(left) => {
                matches!(right, OwnedPrimArray::I64(right) if left == right)
            }
            OwnedPrimArray::U8(left) => {
                matches!(right, OwnedPrimArray::U8(right) if left == right)
            }
            OwnedPrimArray::U16(left) => {
                matches!(right, OwnedPrimArray::U16(right) if left == right)
            }
            OwnedPrimArray::U32(left) => {
                matches!(right, OwnedPrimArray::U32(right) if left == right)
            }
            OwnedPrimArray::U64(left) => {
                matches!(right, OwnedPrimArray::U64(right) if left == right)
            }
            OwnedPrimArray::F32(left) => matches!(right, OwnedPrimArray::F32(right)
                if Self::exact_slice_eq(left, right, |left, right| left.to_bits() == right.to_bits())),
            OwnedPrimArray::F64(left) => matches!(right, OwnedPrimArray::F64(right)
                if Self::exact_slice_eq(left, right, |left, right| left.to_bits() == right.to_bits())),
            OwnedPrimArray::Pos2d32(left) => matches!(right, OwnedPrimArray::Pos2d32(right)
            if Self::exact_slice_eq(left, right, |left, right| {
                left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
            })),
            OwnedPrimArray::Pos2d64(left) => matches!(right, OwnedPrimArray::Pos2d64(right)
            if Self::exact_slice_eq(left, right, |left, right| {
                left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
            })),
            OwnedPrimArray::Pos3d32(left) => matches!(right, OwnedPrimArray::Pos3d32(right)
            if Self::exact_slice_eq(left, right, |left, right| {
                left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
                    && left.z.to_bits() == right.z.to_bits()
            })),
            OwnedPrimArray::Pos3d64(left) => matches!(right, OwnedPrimArray::Pos3d64(right)
            if Self::exact_slice_eq(left, right, |left, right| {
                left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
                    && left.z.to_bits() == right.z.to_bits()
            })),
            OwnedPrimArray::Id(left) => {
                matches!(right, OwnedPrimArray::Id(right) if left == right)
            }
            OwnedPrimArray::String(left) => {
                matches!(right, OwnedPrimArray::String(right) if left == right)
            }
            OwnedPrimArray::Bytes(left) => {
                matches!(right, OwnedPrimArray::Bytes(right) if left.iter().map(|value| &value.data).eq(right.iter().map(|value| &value.data)))
            }
            OwnedPrimArray::SmallBytes(left) => {
                matches!(right, OwnedPrimArray::SmallBytes(right) if left.iter().map(|value| &value.data).eq(right.iter().map(|value| &value.data)))
            }
        }
    }

    fn exact_owned_value_eq(left: &OwnedValue, right: &OwnedValue) -> bool {
        match left {
            OwnedValue::Bool(left) => matches!(right, OwnedValue::Bool(right) if left == right),
            OwnedValue::Char(left) => matches!(right, OwnedValue::Char(right) if left == right),
            OwnedValue::I8(left) => matches!(right, OwnedValue::I8(right) if left == right),
            OwnedValue::I16(left) => matches!(right, OwnedValue::I16(right) if left == right),
            OwnedValue::I32(left) => matches!(right, OwnedValue::I32(right) if left == right),
            OwnedValue::I64(left) => matches!(right, OwnedValue::I64(right) if left == right),
            OwnedValue::U8(left) => matches!(right, OwnedValue::U8(right) if left == right),
            OwnedValue::U16(left) => matches!(right, OwnedValue::U16(right) if left == right),
            OwnedValue::U32(left) => matches!(right, OwnedValue::U32(right) if left == right),
            OwnedValue::U64(left) => matches!(right, OwnedValue::U64(right) if left == right),
            OwnedValue::F32(left) => {
                matches!(right, OwnedValue::F32(right) if left.to_bits() == right.to_bits())
            }
            OwnedValue::F64(left) => {
                matches!(right, OwnedValue::F64(right) if left.to_bits() == right.to_bits())
            }
            OwnedValue::Pos2d32(left) => matches!(right, OwnedValue::Pos2d32(right)
                if left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()),
            OwnedValue::Pos2d64(left) => matches!(right, OwnedValue::Pos2d64(right)
                if left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()),
            OwnedValue::Pos3d32(left) => matches!(right, OwnedValue::Pos3d32(right)
                if left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
                    && left.z.to_bits() == right.z.to_bits()),
            OwnedValue::Pos3d64(left) => matches!(right, OwnedValue::Pos3d64(right)
                if left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
                    && left.z.to_bits() == right.z.to_bits()),
            OwnedValue::Id(left) => matches!(right, OwnedValue::Id(right) if left == right),
            OwnedValue::String(left) => {
                matches!(right, OwnedValue::String(right) if left == right)
            }
            OwnedValue::Bytes(left) => {
                matches!(right, OwnedValue::Bytes(right) if left.data == right.data)
            }
            OwnedValue::SmallBytes(left) => {
                matches!(right, OwnedValue::SmallBytes(right) if left.data == right.data)
            }
            OwnedValue::Map(left) => {
                matches!(right, OwnedValue::Map(right) if Self::exact_owned_map_eq(left, right))
            }
            OwnedValue::Array(left) => matches!(right, OwnedValue::Array(right)
                if Self::exact_slice_eq(left, right, Self::exact_owned_value_eq)),
            OwnedValue::PrimArray(left) => matches!(right, OwnedValue::PrimArray(right)
                if Self::exact_owned_primitive_array_eq(left, right)),
            OwnedValue::Null => matches!(right, OwnedValue::Null),
            OwnedValue::NA => matches!(right, OwnedValue::NA),
        }
    }

    fn commit_cells_equal(left: &OwnedCell, right: &OwnedCell) -> bool {
        left.header.revision_ts == right.header.revision_ts
            && left.header.flags == right.header.flags
            && left.header.schema == right.header.schema
            && left.header.partition == right.header.partition
            && left.header.hash == right.header.hash
            && Self::exact_owned_value_eq(&left.data, &right.data)
    }

    fn commit_ops_equal(left: &CommitOp, right: &CommitOp) -> bool {
        match (left, right) {
            (CommitOp::Write(left), CommitOp::Write(right))
            | (CommitOp::Update(left), CommitOp::Update(right)) => {
                Self::commit_cells_equal(left, right)
            }
            (CommitOp::Remove(left), CommitOp::Remove(right)) => left == right,
            (CommitOp::Read(left_id, left_revision), CommitOp::Read(right_id, right_revision)) => {
                left_id == right_id && left_revision == right_revision
            }
            (CommitOp::None, CommitOp::None) => true,
            _ => false,
        }
    }

    fn canonical_commit_ops(mut cells: Vec<CommitOp>) -> Result<Vec<CommitOp>, DMCommitResult> {
        let mut keyed = Vec::with_capacity(cells.len());
        for cell in cells.drain(..) {
            let id = Self::commit_op_cell_id(&cell).map_err(DMCommitResult::CheckFailed)?;
            keyed.push((id, cell));
        }
        keyed.sort_by_key(|(id, _)| *id);
        Ok(keyed.into_iter().map(|(_, cell)| cell).collect())
    }

    fn commit_requests_equal(left: &[CommitOp], right: &[CommitOp]) -> bool {
        left.len() == right.len()
            && left
                .iter()
                .zip(right)
                .all(|(left, right)| Self::commit_ops_equal(left, right))
    }

    fn apply_commit_ops(
        &self,
        txn_lock: &TxnMutex,
        tid: &TxnId,
        commit_hlc: Hlc,
        cells: Vec<CommitOp>,
    ) -> DMCommitResult {
        let cells = match Self::canonical_commit_ops(cells) {
            Ok(cells) => cells,
            Err(result) => return result,
        };
        let mut txn = txn_lock.lock();
        txn.last_activity = get_time();
        match txn.state {
            TxnState::Started => {
                return DMCommitResult::CheckFailed(CheckError::NotCommitted);
            }
            TxnState::Aborted => {
                return DMCommitResult::CheckFailed(CheckError::AlreadyAborted);
            }
            TxnState::Committed => {
                if txn.commit_hlc == Some(commit_hlc)
                    && txn
                        .commit_ops
                        .as_ref()
                        .is_some_and(|stored| Self::commit_requests_equal(stored, &cells))
                    && self.installed_revisions_agree(&txn)
                {
                    return DMCommitResult::Success;
                }
                return DMCommitResult::CheckFailed(CheckError::AlreadyCommitted);
            }
            TxnState::Cleanup => {
                return DMCommitResult::CheckFailed(CheckError::AlreadyCleanup);
            }
            TxnState::Prepared => {}
        };

        let undo_available = self.undo_log().is_some();
        #[cfg(test)]
        let undo_available = {
            let failure_injected = self
                .fail_next_undo_availability
                .swap(false, std::sync::atomic::Ordering::SeqCst);
            undo_available && !failure_injected
        };
        if self.chunks().durable_storage_configured()
            && (!undo_available || !self.chunks().wal_storage_configured())
        {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }

        if let Err(result) = Self::validate_commit_subset(&txn, &cells) {
            return result;
        }
        if let Err(result) = Self::validate_commit_payload(&txn, &cells) {
            return result;
        }
        if let Err(result) = Self::validate_commit_hlc(&txn, tid, commit_hlc) {
            return result;
        }
        if txn
            .commit_hlc
            .is_some_and(|installed_hlc| installed_hlc != commit_hlc)
        {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }
        if txn
            .commit_ops
            .as_ref()
            .is_some_and(|stored| !Self::commit_requests_equal(stored, &cells))
        {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }

        let Some(coordinator_id) = txn.coordinator_id else {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        };
        let expected_owner = TxnPriority::new(tid.clone(), coordinator_id);
        let guarded_cell_ids = Self::guarded_txn_cell_ids(&txn);
        let mut cell_mutexes = Vec::with_capacity(guarded_cell_ids.len());
        for cell_id in &guarded_cell_ids {
            cell_mutexes.push(self.cell_meta_mutex(cell_id));
        }
        let mut cell_guards = Vec::with_capacity(cell_mutexes.len());
        for cell_mutex in &cell_mutexes {
            cell_guards.push(cell_mutex.lock());
        }
        if cell_guards
            .iter()
            .any(|meta| meta.owner.as_ref() != Some(&expected_owner))
        {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }
        if let Err(result) = self.validate_commit_storage_state(&txn, &cells) {
            return result;
        }

        txn.commit_hlc = Some(commit_hlc);
        txn.commit_ops = Some(cells.clone());
        crate::ram::chunk::set_transaction_context(true);
        let mut write_error: Option<(Id, WriteError)> = None;
        let mut commit_failure: Option<DMCommitResult> = None;
        {
            for cell_op in cells {
                let cell_id = Self::commit_op_cell_id(&cell_op)
                    .expect("commit payload validation rejects non-mutation ops");
                if txn.installed.contains_key(&cell_id) {
                    continue;
                }

                let meta_index = guarded_cell_ids
                    .binary_search(&cell_id)
                    .expect("prepared cell metadata must exist");
                let meta = &mut cell_guards[meta_index];
                match cell_op {
                    CommitOp::Write(mut cell) => {
                        if let Some(undo_log) = self.undo_log() {
                            let undo_entry = super::undo_log::UndoLogEntry::new_write(
                                tid.clone(),
                                cell_id,
                                commit_hlc.ts,
                            );
                            if let Err(error) = undo_log.write_undo_entry(undo_entry) {
                                error!("Failed to write undo log entry for new cell: {:?}", error);
                                commit_failure =
                                    Some(DMCommitResult::CheckFailed(CheckError::CannotEnd));
                                break;
                            }
                            #[cfg(test)]
                            pause_before_storage_mutation(tid, &cell_id);
                        }
                        match self.chunks().write_cell_at_revision(
                            &mut cell,
                            RevisionWrite::pending(commit_hlc.ts),
                        ) {
                            Ok(installed) => {
                                txn.history.insert(cell_id, CellHistory::new(None));
                                txn.installed.insert(cell_id, installed);
                                meta.write = commit_hlc;
                            }
                            Err(error) => {
                                write_error = Some((cell_id, error));
                                break;
                            }
                        };
                    }
                    CommitOp::Remove(ref cell_id) => {
                        let expected_revision_ts = txn
                            .certified_present_revision_ts(cell_id)
                            .expect("commit payload validation requires present certification");

                        let (cell_addr, old_cell_ref) = {
                            let shared_cell = match self.chunks().read_cell(cell_id) {
                                Ok(cell) => cell,
                                Err(read_error) => {
                                    write_error =
                                        Some((*cell_id, WriteError::ReadError(read_error)));
                                    break;
                                }
                            };
                            if shared_cell.header.revision_ts != expected_revision_ts {
                                write_error = Some((*cell_id, WriteError::CellRevisionMismatch));
                                break;
                            }
                            let addr = shared_cell.cell_guard().get_ptr();
                            let cell_ref = shared_cell.to_owned().into_ref();
                            (addr, cell_ref)
                        };
                        let chunk = self.chunks().locate_chunk_by_partition(cell_id.higher);
                        let chunk_idx = chunk.id;
                        let (segment_id, seq_id) = chunk.get_cell_segment_info(cell_addr);
                        let segment_base_addr = chunk.allocator.addr_by_id(segment_id as usize);
                        let cell_offset = (cell_addr - segment_base_addr) as u64;
                        let guard = match self.acquire_segment_guard(chunk_idx, segment_id) {
                            Some(guard) => guard,
                            None => {
                                write_error = Some((
                                    *cell_id,
                                    WriteError::ReadError(ReadError::CellDoesNotExisted),
                                ));
                                break;
                            }
                        };

                        if let Some(undo_log) = self.undo_log() {
                            let undo_entry = super::undo_log::UndoLogEntry::new_restore(
                                tid.clone(),
                                *cell_id,
                                super::undo_log::UndoOpType::Remove,
                                commit_hlc.ts,
                                expected_revision_ts,
                                chunk_idx as u64,
                                seq_id,
                                cell_offset,
                            );
                            if let Err(error) = undo_log.write_undo_entry(undo_entry) {
                                error!("Failed to write undo log entry: {:?}", error);
                                commit_failure =
                                    Some(DMCommitResult::CheckFailed(CheckError::CannotEnd));
                                break;
                            }
                            #[cfg(test)]
                            pause_before_storage_mutation(tid, cell_id);
                        }

                        match self
                            .chunks()
                            .remove_cell_at_revision(cell_id, RevisionWrite::pending(commit_hlc.ts))
                        {
                            Ok(installed) => {
                                txn.history
                                    .insert(*cell_id, CellHistory::new(Some(old_cell_ref)));
                                txn.installed.insert(*cell_id, installed);
                                meta.write = commit_hlc;
                                txn.rollback_guards.push(guard);
                            }
                            Err(error) => {
                                write_error = Some((*cell_id, error));
                                break;
                            }
                        }
                    }
                    CommitOp::Update(mut cell) => {
                        let cell_id = cell.id();
                        let expected_revision_ts = txn
                            .certified_present_revision_ts(&cell_id)
                            .expect("commit payload validation requires present certification");

                        let (cell_addr, old_cell_ref) = {
                            let shared_cell = match self.chunks().read_cell(&cell_id) {
                                Ok(cell) => cell,
                                Err(read_error) => {
                                    write_error =
                                        Some((cell_id, WriteError::ReadError(read_error)));
                                    break;
                                }
                            };
                            if shared_cell.header.revision_ts != expected_revision_ts {
                                write_error = Some((cell_id, WriteError::CellRevisionMismatch));
                                break;
                            }
                            (
                                shared_cell.cell_guard().get_ptr(),
                                shared_cell.to_owned().into_ref(),
                            )
                        };
                        let chunk = self.chunks().locate_chunk_by_partition(cell_id.higher);
                        let chunk_idx = chunk.id;
                        let (segment_id, seq_id) = chunk.get_cell_segment_info(cell_addr);
                        let segment_base_addr = chunk.allocator.addr_by_id(segment_id as usize);
                        let cell_offset = (cell_addr - segment_base_addr) as u64;
                        let guard = match self.acquire_segment_guard(chunk_idx, segment_id) {
                            Some(guard) => guard,
                            None => {
                                write_error = Some((
                                    cell_id,
                                    WriteError::ReadError(ReadError::CellDoesNotExisted),
                                ));
                                break;
                            }
                        };

                        if let Some(undo_log) = self.undo_log() {
                            let undo_entry = super::undo_log::UndoLogEntry::new_restore(
                                tid.clone(),
                                cell_id,
                                super::undo_log::UndoOpType::Update,
                                commit_hlc.ts,
                                expected_revision_ts,
                                chunk_idx as u64,
                                seq_id,
                                cell_offset,
                            );
                            if let Err(error) = undo_log.write_undo_entry(undo_entry) {
                                error!("Failed to write undo log entry: {:?}", error);
                                commit_failure =
                                    Some(DMCommitResult::CheckFailed(CheckError::CannotEnd));
                                break;
                            }
                            #[cfg(test)]
                            pause_before_storage_mutation(tid, &cell_id);
                        }
                        match self.chunks().update_cell_at_revision(
                            &mut cell,
                            RevisionWrite::pending(commit_hlc.ts),
                        ) {
                            Ok(installed) => {
                                txn.history
                                    .insert(cell_id, CellHistory::new(Some(old_cell_ref)));
                                txn.installed.insert(cell_id, installed);
                                meta.write = commit_hlc;
                                txn.rollback_guards.push(guard);
                            }
                            Err(error) => {
                                write_error = Some((cell_id, error));
                                break;
                            }
                        }
                    }
                    CommitOp::Read(_, _) | CommitOp::None => {
                        unreachable!("commit payload validation rejects non-mutation ops")
                    }
                }
            }
        }
        txn.last_activity = get_time();
        crate::ram::chunk::set_transaction_context(false);

        if let Some(failure) = commit_failure {
            return failure;
        }
        if let Some((id, error)) = write_error {
            return Self::map_commit_write_error(&txn, id, error);
        }
        if !self.installed_revisions_agree(&txn) {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }
        if let Err(error) = self
            .chunks()
            .force_sync_installed_revisions(txn.installed.values())
        {
            error!(
                "Failed to sync exact installed output before participant commit success for transaction {:?}: {:?}",
                tid, error
            );
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }
        txn.installed_output_durable = true;

        txn.state = TxnState::Committed;

        DMCommitResult::Success
    }

    fn validate_commit_hlc(
        txn: &Transaction,
        tid: &TxnId,
        commit_hlc: Hlc,
    ) -> Result<(), DMCommitResult> {
        if commit_hlc.ts <= tid.ts
            || txn.certified.values().any(|op| {
                let certified_revision = match op.expectation {
                    CellExpectation::Present(revision_ts)
                    | CellExpectation::Absent(Some(revision_ts)) => Some(revision_ts),
                    CellExpectation::Absent(None) => None,
                };
                certified_revision.is_some_and(|revision_ts| commit_hlc.ts <= revision_ts)
            })
        {
            Err(DMCommitResult::CheckFailed(CheckError::CannotEnd))
        } else {
            Ok(())
        }
    }

    fn installed_revision_agrees(&self, installed: &InstalledRevision, commit_hlc: Hlc) -> bool {
        if installed.node.revision_ts != commit_hlc.ts {
            return false;
        }
        let (state, location) = installed.node.load();
        if self.chunks().history_location(&installed.id, commit_hlc.ts) != Some(location) {
            return false;
        }
        match state {
            RevisionState::PendingPresent | RevisionState::CommittedPresent => {
                self.chunks().head_cell(&installed.id).is_ok_and(|header| {
                    Id::from_header(&header) == installed.id && header.revision_ts == commit_hlc.ts
                })
            }
            RevisionState::PendingDeleted | RevisionState::CommittedDeleted => matches!(
                self.chunks().head_cell(&installed.id),
                Err(ReadError::CellDoesNotExisted)
            ),
            RevisionState::Aborted | RevisionState::Expired => false,
        }
    }

    fn installed_revisions_agree(&self, txn: &Transaction) -> bool {
        let write_ids: Vec<_> = txn
            .certified
            .iter()
            .filter(|(_, op)| op.intent == PrepareIntent::Write)
            .map(|(id, _)| *id)
            .collect();
        if write_ids.is_empty() {
            return txn.installed.is_empty();
        }
        let Some(commit_hlc) = txn.commit_hlc else {
            return false;
        };
        write_ids.into_iter().all(|id| {
            txn.installed
                .get(&id)
                .is_some_and(|installed| self.installed_revision_agrees(installed, commit_hlc))
        })
    }

    fn validate_commit_subset(txn: &Transaction, cells: &[CommitOp]) -> Result<(), DMCommitResult> {
        let prepared_cells_num = txn.certified.len();
        let arrived_cells_num = cells.len();
        if arrived_cells_num > prepared_cells_num {
            return Err(DMCommitResult::CheckFailed(
                CheckError::CellNumberDoesNotMatch(prepared_cells_num, arrived_cells_num),
            ));
        }

        let mut committed_cell_ids = BTreeSet::new();
        for op in cells {
            let cell_id = match Self::commit_op_cell_id(op) {
                Ok(cell_id) => cell_id,
                Err(error) => return Err(DMCommitResult::CheckFailed(error)),
            };
            if !txn.certified.contains_key(&cell_id) || !committed_cell_ids.insert(cell_id) {
                return Err(DMCommitResult::CheckFailed(
                    CheckError::CellNumberDoesNotMatch(prepared_cells_num, arrived_cells_num),
                ));
            }
        }

        if txn.certified.iter().any(|(cell_id, op)| {
            op.intent == PrepareIntent::Write && !committed_cell_ids.contains(cell_id)
        }) {
            return Err(DMCommitResult::CheckFailed(
                CheckError::CellNumberDoesNotMatch(prepared_cells_num, arrived_cells_num),
            ));
        }

        Ok(())
    }

    fn validate_commit_payload(
        txn: &Transaction,
        cells: &[CommitOp],
    ) -> Result<(), DMCommitResult> {
        for op in cells {
            let cell_id =
                Self::commit_op_cell_id(op).map_err(|error| DMCommitResult::CheckFailed(error))?;
            let Some(certified) = txn.certified_op(&cell_id) else {
                return Err(DMCommitResult::CheckFailed(CheckError::CannotEnd));
            };

            let valid = match op {
                CommitOp::Write(_) => {
                    certified.intent == PrepareIntent::Write
                        && matches!(certified.expectation, CellExpectation::Absent(_))
                }
                CommitOp::Update(_) | CommitOp::Remove(_) => {
                    certified.intent == PrepareIntent::Write
                        && matches!(certified.expectation, CellExpectation::Present(_))
                }
                CommitOp::Read(_, _) | CommitOp::None => false,
            };

            if !valid {
                return Err(DMCommitResult::CheckFailed(CheckError::CannotEnd));
            }
        }

        Ok(())
    }

    fn validate_commit_storage_state(
        &self,
        txn: &Transaction,
        cells: &[CommitOp],
    ) -> Result<(), DMCommitResult> {
        for op in cells {
            let cell_id = Self::commit_op_cell_id(op).map_err(DMCommitResult::CheckFailed)?;
            if txn.installed.contains_key(&cell_id) {
                continue;
            }
            let certified = txn
                .certified_op(&cell_id)
                .ok_or(DMCommitResult::CheckFailed(CheckError::CannotEnd))?;
            if self.current_expectation(&cell_id).ok().as_ref() != Some(&certified.expectation) {
                return Err(DMCommitResult::CellChanged(cell_id));
            }
        }

        Ok(())
    }

    fn map_commit_write_error(txn: &Transaction, id: Id, error: WriteError) -> DMCommitResult {
        let expected_present = txn.certified_present_revision_ts(&id).is_some();
        let expected_absent_write = txn.certified_expects_absent_write(&id);
        let is_cell_changed = match &error {
            WriteError::DeletionPredictionFailed
            | WriteError::UserCanceledUpdate
            | WriteError::CellRevisionMismatch => true,
            WriteError::CellDoesNotExisted
            | WriteError::ReadError(ReadError::CellDoesNotExisted) => expected_present,
            WriteError::CellAlreadyExisted => expected_absent_write,
            _ => false,
        };

        if is_cell_changed {
            DMCommitResult::CellChanged(id)
        } else {
            DMCommitResult::WriteError(id, error)
        }
    }

    fn commit_op_cell_id(op: &CommitOp) -> Result<Id, CheckError> {
        match op {
            CommitOp::Write(cell) | CommitOp::Update(cell) => Ok(cell.id()),
            CommitOp::Remove(id) | CommitOp::Read(id, _) => Ok(*id),
            CommitOp::None => Err(CheckError::CannotEnd),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::cell::OwnedCell;
    use crate::ram::schema::Schema;
    use crate::ram::segs::SEGMENT_SIZE;
    use crate::ram::tests::default_fields;
    use crate::ram::types::{Id, OwnedMap, OwnedPrimArray, OwnedValue, Pos3d32};
    use crate::server::transactions::test_hlc;
    use crate::server::{NebServer, ServerOptions, Service as NebService};
    use bifrost::rpc::DEFAULT_CLIENT_POOL;
    use dovahkiin::types::Map as OwnedMapTrait;
    use futures::future::join_all;
    use lightning::map::Map as LFMapTrait;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn scoped_data_manager_service_ids_differ_between_databases() {
        let group = "group_a";
        assert_ne!(
            generate_scoped_service_id(group, "db_a"),
            generate_scoped_service_id(group, "db_b")
        );
    }

    #[test]
    fn canonical_commit_identity_covers_kind_full_id_header_and_payload() {
        let first_id = Id::new(7, 11);
        let second_id = Id::new(8, 11);
        let mut first = counter_cell(990, first_id, 1, "first");
        first.header.revision_ts = 17;
        first.header.flags = 3;
        let second = counter_cell(990, second_id, 2, "second");
        let expected = DataManager::canonical_commit_ops(vec![
            CommitOp::Update(second.clone()),
            CommitOp::Update(first.clone()),
        ])
        .unwrap();
        let reordered = DataManager::canonical_commit_ops(vec![
            CommitOp::Update(first.clone()),
            CommitOp::Update(second.clone()),
        ])
        .unwrap();
        assert!(DataManager::commit_requests_equal(&expected, &reordered));

        let changed_kind = DataManager::canonical_commit_ops(vec![
            CommitOp::Remove(first_id),
            CommitOp::Update(second.clone()),
        ])
        .unwrap();
        assert!(!DataManager::commit_requests_equal(
            &expected,
            &changed_kind
        ));

        let changed_id = DataManager::canonical_commit_ops(vec![
            CommitOp::Update(counter_cell(990, Id::new(9, 11), 1, "first")),
            CommitOp::Update(second.clone()),
        ])
        .unwrap();
        assert!(!DataManager::commit_requests_equal(&expected, &changed_id));

        let mut header_variants = Vec::new();
        for mutate in [
            |cell: &mut OwnedCell| cell.header.revision_ts += 1,
            |cell: &mut OwnedCell| cell.header.flags += 1,
            |cell: &mut OwnedCell| cell.header.schema += 1,
        ] {
            let mut changed = first.clone();
            mutate(&mut changed);
            header_variants.push(changed);
        }
        for changed in header_variants {
            let changed_header = DataManager::canonical_commit_ops(vec![
                CommitOp::Update(changed),
                CommitOp::Update(second.clone()),
            ])
            .unwrap();
            assert!(!DataManager::commit_requests_equal(
                &expected,
                &changed_header
            ));
        }

        let changed_payload = DataManager::canonical_commit_ops(vec![
            CommitOp::Update(counter_cell(990, first_id, 99, "changed")),
            CommitOp::Update(second),
        ])
        .unwrap();
        assert!(!DataManager::commit_requests_equal(
            &expected,
            &changed_payload
        ));
    }

    fn commit_identity_for_values(left: OwnedValue, right: OwnedValue) -> bool {
        let id = Id::new(7, 12);
        let left = DataManager::canonical_commit_ops(vec![CommitOp::Update(
            OwnedCell::new_with_id(990, &id, left),
        )])
        .unwrap();
        let right = DataManager::canonical_commit_ops(vec![CommitOp::Update(
            OwnedCell::new_with_id(990, &id, right),
        )])
        .unwrap();
        DataManager::commit_requests_equal(&left, &right)
    }

    #[test]
    fn commit_identity_accepts_same_nan_bits_and_rejects_changed_payload_bits() {
        let nan_bits = 0x7ff8_0000_0000_0042;
        let same_left = OwnedValue::F64(f64::from_bits(nan_bits));
        let same_right = OwnedValue::F64(f64::from_bits(nan_bits));
        assert!(
            commit_identity_for_values(same_left, same_right),
            "the same NaN payload bits are the same request"
        );

        assert!(
            !commit_identity_for_values(
                OwnedValue::F64(f64::from_bits(nan_bits)),
                OwnedValue::F64(f64::from_bits(nan_bits + 1)),
            ),
            "different NaN payload bits are different requests"
        );
    }

    #[test]
    fn commit_identity_distinguishes_signed_zero() {
        assert!(!commit_identity_for_values(
            OwnedValue::F32(0.0),
            OwnedValue::F32(-0.0),
        ));
    }

    #[test]
    fn commit_identity_compares_complete_ordered_map_fields() {
        let mut left = OwnedMap::new();
        left.insert("score", OwnedValue::U64(9));
        left.insert("name", OwnedValue::String("cell".to_string()));
        let mut right = left.clone();
        right.fields = vec!["renamed".to_string()];

        assert!(!commit_identity_for_values(
            OwnedValue::Map(left.clone()),
            OwnedValue::Map(right),
        ));

        let mut reverse_inserted = OwnedMap::new();
        reverse_inserted.insert("name", OwnedValue::String("cell".to_string()));
        reverse_inserted.insert("score", OwnedValue::U64(9));
        reverse_inserted.fields = left.fields.clone();
        assert!(
            commit_identity_for_values(OwnedValue::Map(left), OwnedValue::Map(reverse_inserted)),
            "map entry iteration order is not request identity"
        );
    }

    #[test]
    fn commit_identity_applies_bit_exact_comparison_recursively() {
        let nan_bits = 0x7fc0_0042;
        let nested = OwnedValue::Array(vec![OwnedValue::Map(OwnedMap {
            map: [(
                17,
                OwnedValue::PrimArray(OwnedPrimArray::Pos3d32(vec![Pos3d32 {
                    x: f32::from_bits(nan_bits),
                    y: 0.0,
                    z: -0.0,
                }])),
            )]
            .into_iter()
            .collect(),
            fields: vec!["position".to_string()],
        })]);
        assert!(
            commit_identity_for_values(nested.clone(), nested.clone()),
            "bit-identical nested NaNs must compare equal"
        );

        let changed = OwnedValue::Array(vec![OwnedValue::Map(OwnedMap {
            map: [(
                17,
                OwnedValue::PrimArray(OwnedPrimArray::Pos3d32(vec![Pos3d32 {
                    x: f32::from_bits(nan_bits),
                    y: -0.0,
                    z: -0.0,
                }])),
            )]
            .into_iter()
            .collect(),
            fields: vec!["position".to_string()],
        })]);
        assert!(
            !commit_identity_for_values(nested, changed),
            "nested primitive-array element order and float bits are request identity"
        );
    }

    #[cfg(feature = "occ_phase_profile")]
    #[test]
    fn participant_profile_covers_every_existing_protocol_boundary() {
        let source = include_str!("data_site.rs");
        let (before_tests, after_test_marker) = source
            .split_once("\n#[cfg(test)]\nmod tests {")
            .expect("data_site.rs should contain the private test module");
        let (_, after_tests) = after_test_marker
            .split_once("\n}\n\nimpl Service for DataManager {")
            .expect("data_site.rs should resume production code after the private test module");
        let production_source =
            format!("{before_tests}\n\nimpl Service for DataManager {{{after_tests}");
        let end_method_tail = production_source
            .split_once("\n    fn end(")
            .map(|(_, end_method_tail)| end_method_tail)
            .expect("expected data_site.rs to define Service::end");
        let async_move_offset = end_method_tail
            .find("async move {")
            .expect("expected Service::end to return an async cleanup future");
        let before_async_move = &end_method_tail[..async_move_offset];
        let async_move_and_after = &end_method_tail[async_move_offset..];

        for phase in [
            "Phase::ParticipantPrepare",
            "Phase::ParticipantCommit",
            "Phase::ParticipantAbort",
            "Phase::ParticipantEnd",
        ] {
            assert!(
                production_source.contains(phase),
                "missing participant guard for {phase}"
            );
        }

        assert!(
            before_async_move.contains("let phase_guard =")
                && before_async_move.contains("Phase::ParticipantEnd"),
            "expected Service::end to bind a named participant end guard before its async cleanup future",
        );
        assert!(
            async_move_and_after.contains(
                "async move {\n            #[cfg(feature = \"occ_phase_profile\")]\n            let _phase_guard = phase_guard;"
            ),
            "expected Service::end async cleanup future to retain the participant end guard",
        );
    }

    #[test]
    fn dropped_prepare_delay_handle_removes_its_registration() {
        let id = Id::new(0, 99012);
        let tid = test_hlc(12, 31);
        let key = (tid.clone(), id);
        {
            let _handle = install_prepare_delay_for_cell(tid, id);
            assert!(prepare_delay_hooks().lock().contains_key(&key));
        }

        assert!(
            !prepare_delay_hooks().lock().contains_key(&key),
            "dropping the handle should remove its global registration"
        );
    }

    async fn start_transaction_test_server(
        address: &str,
        group: &str,
    ) -> Arc<crate::server::NebServer> {
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE,
                db_size: SEGMENT_SIZE,
                history_retention_ms: 300_000,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![NebService::Cell, NebService::Transaction],
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

    async fn start_durable_transaction_test_server(
        address: &str,
        group: &str,
        temp_dir: &TempDir,
    ) -> Arc<crate::server::NebServer> {
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE,
                db_size: SEGMENT_SIZE,
                history_retention_ms: 300_000,
                tiered_config: None,
                backup_storage: None,
                wal_storage: Some(temp_dir.path().join("wal").to_string_lossy().into_owned()),
                undo_log_storage: Some(temp_dir.path().join("undo").to_string_lossy().into_owned()),
                raft_storage: None,
                index_enabled: false,
                services: vec![NebService::Cell, NebService::Transaction],
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

    async fn start_backup_without_wal_test_server(
        address: &str,
        group: &str,
        temp_dir: &TempDir,
    ) -> Arc<crate::server::NebServer> {
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE,
                db_size: SEGMENT_SIZE,
                history_retention_ms: 300_000,
                tiered_config: None,
                backup_storage: Some(
                    temp_dir
                        .path()
                        .join("backup")
                        .to_string_lossy()
                        .into_owned(),
                ),
                wal_storage: None,
                undo_log_storage: Some(temp_dir.path().join("undo").to_string_lossy().into_owned()),
                raft_storage: None,
                index_enabled: false,
                services: vec![NebService::Cell],
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

    async fn data_manager_for_database(
        server: &Arc<NebServer>,
        address: &str,
        database_name: &str,
    ) -> Arc<DataManager> {
        let runtime = server
            .ensure_database_runtime(database_name)
            .await
            .expect("database runtime");
        DataManager::new(runtime, server.hlc.clone())
    }

    async fn data_site_client_for_database(
        address: &str,
        group: &str,
        database_name: &str,
    ) -> Arc<AsyncServiceClient> {
        let client = DEFAULT_CLIENT_POOL.get(&address.to_string()).await.unwrap();
        AsyncServiceClient::new_with_service_id(
            generate_scoped_service_id(group, database_name),
            &client,
        )
    }

    fn install_prepare_test_schema(runtime: &Arc<crate::server::DatabaseRuntime>) -> Schema {
        let schema = Schema::new_with_id(
            990,
            &String::from("txn_prepare_cert"),
            None,
            default_fields(),
            false,
            false,
        );
        runtime.meta().schemas.debug_only_new_schema(schema.clone());
        schema
    }

    fn counter_cell(schema_id: u32, id: Id, score: u64, name: &str) -> OwnedCell {
        let mut data = OwnedMap::new();
        data.insert(&String::from("id"), OwnedValue::I64(id.lower as i64));
        data.insert(&String::from("score"), OwnedValue::U64(score));
        data.insert(&String::from("name"), OwnedValue::String(name.to_string()));
        OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(data))
    }

    fn seed_cell_revision(
        runtime: &Arc<crate::server::DatabaseRuntime>,
        schema_id: u32,
        id: Id,
        score: u64,
        revision_steps: usize,
    ) -> u64 {
        let mut cell = counter_cell(schema_id, id, score, "prepare-seed-0");
        let mut header = runtime.chunks().write_cell(&mut cell).unwrap();
        for step in 0..revision_steps {
            let next_score = score + step as u64 + 1;
            let mut updated = counter_cell(
                schema_id,
                id,
                next_score,
                &format!("prepare-seed-{}", step + 1),
            );
            header = runtime.chunks().update_cell(&mut updated).unwrap();
        }
        header.revision_ts
    }

    fn prepare_local_txn(manager: &Arc<DataManager>, tid: &TxnId, affected_cells: Vec<Id>) {
        let owner = TxnPriority::new(tid.clone(), 0);
        let txn_lock = manager.get_or_create_transaction(tid);
        let mut txn = txn_lock.lock();
        txn.state = TxnState::Prepared;
        txn.certified = affected_cells
            .iter()
            .copied()
            .map(|id| {
                (
                    id,
                    PrepareOp {
                        id,
                        expectation: CellExpectation::Absent(None),
                        intent: PrepareIntent::Write,
                    },
                )
            })
            .collect();
        txn.affected_cells = txn.certified.keys().copied().collect();
        txn.coordinator_id = Some(0);
        txn.history.clear();
        txn.last_activity = get_time();
        drop(txn);

        let lock_time = get_time();
        for id in affected_cells {
            let meta = manager.cell_meta_mutex(&id);
            let mut meta = meta.lock();
            meta.owner = Some(owner.clone());
            meta.lock_acquired_at = Some(lock_time);
        }
    }

    async fn prepare_ops_local(
        manager: &Arc<DataManager>,
        coordinator_id: u64,
        tid: &TxnId,
        ops: Vec<PrepareOp>,
    ) -> DMPrepareResult {
        <DataManager as Service>::prepare(manager, coordinator_id, tid.clone(), tid.clone(), ops)
            .await
            .payload
    }

    async fn commit_ops_local(
        manager: &Arc<DataManager>,
        tid: &TxnId,
        ops: Vec<CommitOp>,
    ) -> DMCommitResult {
        <DataManager as Service>::commit(manager, manager.hlc.now(), tid.clone(), ops)
            .await
            .payload
    }

    async fn abort_and_end_local(manager: &Arc<DataManager>, tid: &TxnId) {
        let abort = <DataManager as Service>::abort(manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort, AbortResult::Success(None));
        let end = <DataManager as Service>::end(manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end, EndResult::Success);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_a_stale_present_version() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5323";
        let group = "txn_data_site_prepare_stale_present";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let client = data_site_client_for_database(address, group, group).await;
        let cell_id = Id::new(0, 99001);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 3, 0);
        let tid = test_hlc(1, 11);

        let result = client
            .prepare(
                11,
                tid.clone(),
                tid.clone(),
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(revision_ts + 1),
                    intent: PrepareIntent::Write,
                }],
            )
            .await
            .unwrap()
            .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn first_snapshot_read_waits_for_owner_before_timestamp_rejection() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5390";
        let group = "txn_data_site_snapshot_owner_wait";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99024);

        let mut cell = counter_cell(schema.id, cell_id, 1, "owner-wait-seed");
        runtime.chunks().write_cell(&mut cell).unwrap();

        let reader_tid = test_hlc(cell.header.revision_ts + 1, 51);
        let writer_tid = test_hlc(reader_tid.ts + 1, 52);
        let meta = manager.cell_meta_mutex(&cell_id);
        {
            let mut meta = meta.lock();
            meta.write = writer_tid;
            meta.owner = Some(TxnPriority::new(writer_tid, 52));
            meta.lock_acquired_at = Some(get_time());
        }

        let response =
            <DataManager as Service>::read(&manager, 51, reader_tid, reader_tid, cell_id)
                .await
                .payload;
        assert!(matches!(response, TxnExecResult::Wait));

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn point_read_shapes_share_snapshot_without_participant_state() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5362";
        let group = "txn_data_site_snapshot_shapes";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99022);

        let mut first = counter_cell(schema.id, cell_id, 300, "A");
        let first_revision_ts = runtime.chunks().write_cell(&mut first).unwrap().revision_ts;
        let tid = manager.hlc.now();
        let mut second = counter_cell(schema.id, cell_id, 400, "B");
        runtime.chunks().update_cell(&mut second).unwrap();

        let head = <DataManager as Service>::head(&manager, 43, tid, tid, cell_id)
            .await
            .payload
            .unwrap();
        let selected = <DataManager as Service>::read_selected(
            &manager,
            43,
            tid,
            tid,
            cell_id,
            vec![hash_str("score")],
        )
        .await
        .payload
        .unwrap();
        let full = <DataManager as Service>::read(&manager, 43, tid, tid, cell_id)
            .await
            .payload
            .unwrap();
        let partial =
            <DataManager as Service>::read_partial_raw(&manager, 43, tid, tid, cell_id, 0, 8)
                .await
                .payload
                .unwrap();

        let expectation = CellExpectation::Present(first_revision_ts);
        assert_eq!(head.expectation, expectation);
        assert_eq!(selected.expectation, expectation);
        assert_eq!(full.expectation, expectation);
        assert_eq!(partial.expectation, expectation);
        assert_eq!(head.value.unwrap().revision_ts, first_revision_ts);
        assert_eq!(full.value.unwrap().header.revision_ts, first_revision_ts);
        assert!(partial.value.is_some());
        assert!(manager.find_transaction(&tid).is_none());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_a_present_cell_when_absence_was_observed() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5324";
        let group = "txn_data_site_prepare_present_vs_absent";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let client = data_site_client_for_database(address, group, group).await;
        let cell_id = Id::new(0, 99002);
        seed_cell_revision(&runtime, schema.id, cell_id, 4, 0);
        let tid = test_hlc(1, 12);

        let result = client
            .prepare(
                12,
                tid.clone(),
                tid.clone(),
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Absent(None),
                    intent: PrepareIntent::Write,
                }],
            )
            .await
            .unwrap()
            .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_present_expectation_when_cell_is_missing_without_publishing_owner() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5326";
        let group = "txn_data_site_prepare_missing_present";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99003);
        let tid = test_hlc(1, 13);

        let result = <DataManager as Service>::prepare(
            &manager,
            13,
            tid.clone(),
            tid.clone(),
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(1),
                intent: PrepareIntent::Read,
            }],
        )
        .await
        .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        let txn = manager.get_or_create_transaction(&tid);
        let txn = txn.lock();
        assert_ne!(txn.state, TxnState::Prepared);
        assert!(txn.affected_cells.is_empty());
        drop(txn);

        let meta = manager.cell_meta_mutex(&cell_id);
        let meta = meta.lock();
        assert!(meta.owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_duplicate_prepare_ops_without_publishing_owner() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5327";
        let group = "txn_data_site_prepare_duplicate_ops";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99004);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 5, 0);
        let tid = test_hlc(1, 14);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(revision_ts),
            intent: PrepareIntent::Write,
        };

        let result = <DataManager as Service>::prepare(
            &manager,
            14,
            tid.clone(),
            tid.clone(),
            vec![op.clone(), op],
        )
        .await
        .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        let meta = manager.cell_meta_mutex(&cell_id);
        assert!(meta.lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_empty_prepare_ops_without_creating_transaction_state() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5345";
        let group = "txn_data_site_prepare_empty_ops";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = test_hlc(1, 19);

        assert!(manager.find_transaction(&tid).is_none());

        let result =
            <DataManager as Service>::prepare(&manager, 19, tid.clone(), tid.clone(), vec![])
                .await
                .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        assert!(manager.find_transaction(&tid).is_none());
        assert_eq!(manager.cells.len(), 0);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_retry_rejects_different_ops_without_overwriting_original_prepare() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5328";
        let group = "txn_data_site_prepare_retry_ops_mismatch";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::new(0, 99006);
        let cell_b = Id::new(0, 99007);
        let version_a = seed_cell_revision(&runtime, schema.id, cell_a, 7, 0);
        let version_b = seed_cell_revision(&runtime, schema.id, cell_b, 8, 0);
        let tid = test_hlc(1, 15);
        let coordinator_id = 15;
        let requester = TxnPriority::new(tid.clone(), coordinator_id);
        let op_a = PrepareOp {
            id: cell_a,
            expectation: CellExpectation::Present(version_a),
            intent: PrepareIntent::Write,
        };
        let op_b = PrepareOp {
            id: cell_b,
            expectation: CellExpectation::Present(version_b),
            intent: PrepareIntent::Write,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op_a.clone()],
        )
        .await
        .payload;
        assert_eq!(first, DMPrepareResult::Success);

        let second = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op_b.clone()],
        )
        .await
        .payload;

        assert_eq!(second, DMPrepareResult::StateError(TxnState::Prepared));

        let txn = manager.get_or_create_transaction(&tid);
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert_eq!(txn.coordinator_id, Some(coordinator_id));
        assert_eq!(txn.affected_cells, vec![cell_a]);
        assert_eq!(txn.certified.len(), 1);
        assert_eq!(txn.certified.get(&cell_a), Some(&op_a));
        assert_eq!(txn.certified.get(&cell_b), None);
        drop(txn);

        assert_eq!(
            manager.cell_meta_mutex(&cell_a).lock().owner,
            Some(requester)
        );
        assert_eq!(
            manager
                .cells
                .get(&cell_b)
                .map(|meta| meta.lock().owner.clone()),
            None
        );

        let abort_result = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort_result, AbortResult::Success(None));
        let end_result = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_a).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_retry_rejects_different_coordinator_without_state_or_owner_change() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5329";
        let group = "txn_data_site_prepare_retry_coordinator_mismatch";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99008);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 9, 0);
        let tid = test_hlc(1, 16);
        let original_coordinator = 16;
        let requester = TxnPriority::new(tid.clone(), original_coordinator);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(revision_ts),
            intent: PrepareIntent::Write,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            original_coordinator,
            tid.clone(),
            tid.clone(),
            vec![op.clone()],
        )
        .await
        .payload;
        assert_eq!(first, DMPrepareResult::Success);

        let second = <DataManager as Service>::prepare(
            &manager,
            99,
            tid.clone(),
            tid.clone(),
            vec![op.clone()],
        )
        .await
        .payload;

        assert_eq!(second, DMPrepareResult::StateError(TxnState::Prepared));

        let txn = manager.get_or_create_transaction(&tid);
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert_eq!(txn.coordinator_id, Some(original_coordinator));
        assert_eq!(txn.affected_cells, vec![cell_id]);
        assert_eq!(txn.certified.len(), 1);
        assert_eq!(txn.certified.get(&cell_id), Some(&op));
        drop(txn);

        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(requester)
        );

        let abort_result = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort_result, AbortResult::Success(None));
        let end_result = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_retry_accepts_exact_same_payload_and_reacquires_missing_owner() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5343";
        let group = "txn_data_site_prepare_retry_idempotent";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::new(0, 99009);
        let cell_b = Id::new(0, 99010);
        let version_a = seed_cell_revision(&runtime, schema.id, cell_a, 10, 0);
        let version_b = seed_cell_revision(&runtime, schema.id, cell_b, 11, 0);
        let tid = test_hlc(1, 17);
        let coordinator_id = 17;
        let requester = TxnPriority::new(tid.clone(), coordinator_id);
        let op_a = PrepareOp {
            id: cell_a,
            expectation: CellExpectation::Present(version_a),
            intent: PrepareIntent::Write,
        };
        let op_b = PrepareOp {
            id: cell_b,
            expectation: CellExpectation::Present(version_b),
            intent: PrepareIntent::Read,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op_b.clone(), op_a.clone()],
        )
        .await
        .payload;
        assert_eq!(first, DMPrepareResult::Success);

        {
            let meta = manager.cell_meta_mutex(&cell_a);
            let mut meta = meta.lock();
            meta.owner = None;
            meta.lock_acquired_at = None;
        }

        let second = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op_a.clone(), op_b.clone()],
        )
        .await
        .payload;

        assert_eq!(second, DMPrepareResult::Success);

        let txn = manager.get_or_create_transaction(&tid);
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert_eq!(txn.coordinator_id, Some(coordinator_id));
        assert_eq!(txn.affected_cells, vec![cell_a, cell_b]);
        assert_eq!(txn.certified.len(), 2);
        assert_eq!(txn.certified.get(&cell_a), Some(&op_a));
        assert_eq!(txn.certified.get(&cell_b), Some(&op_b));
        drop(txn);

        assert_eq!(
            manager.cell_meta_mutex(&cell_a).lock().owner,
            Some(requester.clone())
        );
        assert_eq!(
            manager.cell_meta_mutex(&cell_b).lock().owner,
            Some(requester)
        );

        let abort_result = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort_result, AbortResult::Success(None));
        let end_result = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_a).lock().owner.is_none());
        assert!(manager.cell_meta_mutex(&cell_b).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_retry_exact_payload_does_not_blindly_succeed_with_foreign_owner() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5344";
        let group = "txn_data_site_prepare_retry_foreign_owner";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99011);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 12, 0);
        let tid = test_hlc(1, 18);
        let coordinator_id = 18;
        let requester = TxnPriority::new(tid.clone(), coordinator_id);
        let foreign_owner = TxnPriority::new(test_hlc(1, 1), 1);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(revision_ts),
            intent: PrepareIntent::Write,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op.clone()],
        )
        .await
        .payload;
        assert_eq!(first, DMPrepareResult::Success);

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.owner = Some(foreign_owner.clone());
            meta.lock_acquired_at = Some(get_time());
        }

        let second = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op.clone()],
        )
        .await
        .payload;

        assert_eq!(second, DMPrepareResult::NotRealizable);

        let txn = manager.get_or_create_transaction(&tid);
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert_eq!(txn.coordinator_id, Some(coordinator_id));
        assert_eq!(txn.affected_cells, vec![cell_id]);
        assert_eq!(txn.certified.len(), 1);
        assert_eq!(txn.certified.get(&cell_id), Some(&op));
        drop(txn);

        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(foreign_owner)
        );

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.owner = Some(requester);
            meta.lock_acquired_at = Some(get_time());
        }

        let abort_result = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort_result, AbortResult::Success(None));
        let end_result = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_clock_wait_die_has_one_younger_requester() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5325";
        let group = "txn_data_site_prepare_wait_die_clock";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let client = data_site_client_for_database(address, group, group).await;
        let cell_id = Id::new(0, 99005);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 6, 0);
        let older_tid = test_hlc(1, 11);
        let younger_tid = test_hlc(1, 22);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(revision_ts),
            intent: PrepareIntent::Write,
        };

        let first = client
            .prepare(11, older_tid.clone(), older_tid.clone(), vec![op.clone()])
            .await
            .unwrap()
            .payload;
        let second = client
            .prepare(22, younger_tid.clone(), younger_tid.clone(), vec![op])
            .await
            .unwrap()
            .payload;

        assert_eq!(first, DMPrepareResult::Success);
        assert_eq!(second, DMPrepareResult::NotRealizable);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_get_transaction_returns_single_shared_entry_per_tid() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5290";
        let group = "txn_data_site_same_tid";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = manager.hlc.now();

        let results = join_all((0..32).map(|_| {
            let manager = manager.clone();
            let tid = tid.clone();
            async move {
                let txn = manager.get_or_create_transaction(&tid);
                Arc::as_ptr(&txn) as usize
            }
        }))
        .await;

        let first = results[0];
        assert!(
            results.iter().all(|ptr| *ptr == first),
            "all concurrent lookups of the same txn id should return the same transaction entry"
        );
        assert_eq!(manager.txns.len(), 1);
        assert_eq!(manager.txns_sorted.lock().len(), 1);
        assert!(manager.txns.get(&tid).is_some());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_only_transaction_creates_no_data_site_transaction() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5358";
        let group = "txn_data_site_read_only_stateless";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99011);
        seed_cell_revision(&runtime, schema.id, cell_id, 7, 0);
        let tid = test_hlc(1, 31);

        let response =
            <DataManager as Service>::read(&manager, 31, tid.clone(), tid.clone(), cell_id)
                .await
                .payload;

        assert!(matches!(response, TxnExecResult::Accepted(_)));
        assert!(manager.find_transaction(&tid).is_none());
        assert_eq!(manager.txns.len(), 0);
        assert!(manager.txns_sorted.lock().is_empty());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_read_only_point_shape_is_participant_stateless() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5391";
        let group = "txn_data_site_all_point_shapes_stateless";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99025);
        seed_cell_revision(&runtime, schema.id, cell_id, 7, 0);
        let tid = manager.hlc.now();

        let head = <DataManager as Service>::head(&manager, 31, tid, tid, cell_id)
            .await
            .payload;
        assert!(matches!(head, TxnExecResult::Accepted(_)));

        let selected = <DataManager as Service>::read_selected(
            &manager,
            31,
            tid,
            tid,
            cell_id,
            vec![hash_str("score")],
        )
        .await
        .payload;
        assert!(matches!(selected, TxnExecResult::Accepted(_)));

        let partial =
            <DataManager as Service>::read_partial_raw(&manager, 31, tid, tid, cell_id, 0, 8)
                .await
                .payload;
        assert!(matches!(partial, TxnExecResult::Accepted(_)));

        assert!(manager.find_transaction(&tid).is_none());
        assert_eq!(manager.txns.len(), 0);
        assert!(manager.txns_sorted.lock().is_empty());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_cleans_up_many_active_transactions_without_leaking_bookkeeping() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5291";
        let group = "txn_data_site_cleanup";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tids = (0..64).map(|_| manager.hlc.now()).collect::<Vec<_>>();

        for tid in &tids {
            let txn = manager.get_or_create_transaction(tid);
            txn.lock().state = TxnState::Committed;
        }
        assert_eq!(manager.txns.len(), tids.len());
        assert_eq!(manager.txns_sorted.lock().len(), tids.len());

        let results = join_all(tids.iter().cloned().map(|tid| {
            let manager = manager.clone();
            async move {
                <DataManager as Service>::end(&manager, tid.clone(), tid)
                    .await
                    .payload
            }
        }))
        .await;

        for result in results {
            assert_eq!(result, EndResult::Success);
        }
        assert_eq!(manager.txns.len(), 0);
        assert!(manager.txns_sorted.lock().is_empty());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ending_one_database_transaction_does_not_wipe_same_tid_in_another_database() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5292";
        let group = "txn_data_site_multidb";
        let server = start_transaction_test_server(address, group).await;
        let default_manager = data_manager_for_database(&server, address, group).await;
        let analytics_manager = data_manager_for_database(&server, address, "analytics").await;
        let shared_tid = default_manager.hlc.now();

        default_manager
            .get_or_create_transaction(&shared_tid)
            .lock()
            .state = TxnState::Committed;
        analytics_manager
            .get_or_create_transaction(&shared_tid)
            .lock()
            .state = TxnState::Committed;

        let end_result =
            <DataManager as Service>::end(&default_manager, shared_tid.clone(), shared_tid.clone())
                .await
                .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(default_manager.txns.get(&shared_tid).is_none());
        assert!(!default_manager.txns_sorted.lock().contains(&shared_tid));

        assert!(
            analytics_manager.txns.get(&shared_tid).is_some(),
            "ending one database transaction must not remove another database's transaction entry"
        );
        assert!(
            analytics_manager.txns_sorted.lock().contains(&shared_tid),
            "ending one database transaction must not remove another database's sorted bookkeeping"
        );

        let analytics_end = <DataManager as Service>::end(
            &analytics_manager,
            shared_tid.clone(),
            shared_tid.clone(),
        )
        .await
        .payload;
        assert_eq!(analytics_end, EndResult::Success);
        assert!(analytics_manager.txns.get(&shared_tid).is_none());
        assert!(!analytics_manager.txns_sorted.lock().contains(&shared_tid));

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_end_calls_on_same_transaction_cleanup_idempotently() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5297";
        let group = "txn_data_site_end_race";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = manager.hlc.now();
        manager.get_or_create_transaction(&tid).lock().state = TxnState::Aborted;

        let end_clock = manager.hlc.now();
        let left_manager = manager.clone();
        let right_manager = manager.clone();
        let left_tid = tid.clone();
        let right_tid = tid.clone();
        let left_clock = end_clock.clone();
        let right_clock = end_clock.clone();

        let (left, right) = tokio::join!(
            async move {
                <DataManager as Service>::end(&left_manager, left_clock, left_tid)
                    .await
                    .payload
            },
            async move {
                <DataManager as Service>::end(&right_manager, right_clock, right_tid)
                    .await
                    .payload
            }
        );

        for result in [left, right] {
            assert!(
                matches!(
                    result,
                    EndResult::Success
                        | EndResult::CheckFailed(CheckError::NotExisted)
                        | EndResult::CheckFailed(CheckError::CannotEnd)
                        | EndResult::LockReleaseRetriesExhausted { .. }
                        | EndResult::SomeLocksNotReleased { .. }
                ),
                "unexpected end result in duplicate end race: {:?}",
                result
            );
        }

        assert!(manager.txns.get(&tid).is_none());
        assert!(!manager.txns_sorted.lock().contains(&tid));

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_validation_rejects_non_mutation_ops_without_panicking() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5299";
        let group = "txn_data_site_commit_variant_validation";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let affected_id = Id::new(0, 8101);

        for ops in [vec![CommitOp::Read(affected_id, 1)], vec![CommitOp::None]] {
            let tid = manager.hlc.now();
            prepare_local_txn(&manager, &tid, vec![affected_id]);

            let result =
                <DataManager as Service>::commit(&manager, manager.hlc.now(), tid.clone(), ops)
                    .await
                    .payload;
            assert_eq!(result, DMCommitResult::CheckFailed(CheckError::CannotEnd));

            let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
            let txn = txn_lock.lock();
            assert_eq!(txn.state, TxnState::Prepared);
            assert_eq!(txn.affected_cells, vec![affected_id]);
            assert!(txn.history.is_empty());
        }

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_validation_requires_certified_writes_and_allows_read_only_empty_commits() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5287";
        let group = "txn_data_site_commit_subset_validation";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let write_id = Id::new(0, 8201);
        let read_id = Id::new(0, 8202);
        let read_only_id = Id::new(0, 8203);
        let unprepared = Id::new(0, 8204);
        let write_version = seed_cell_revision(&runtime, schema.id, write_id, 7, 0);
        let read_version = seed_cell_revision(&runtime, schema.id, read_id, 9, 0);
        let read_only_version = seed_cell_revision(&runtime, schema.id, read_only_id, 11, 0);

        let read_only_tid = manager.hlc.now();
        let read_only_prepare = prepare_ops_local(
            &manager,
            0,
            &read_only_tid,
            vec![PrepareOp {
                id: read_only_id,
                expectation: CellExpectation::Present(read_only_version),
                intent: PrepareIntent::Read,
            }],
        )
        .await;
        assert_eq!(read_only_prepare, DMPrepareResult::Success);

        let read_only_before = runtime
            .chunks()
            .read_cell(&read_only_id)
            .unwrap()
            .to_owned();
        let read_only_result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            read_only_tid.clone(),
            vec![],
        )
        .await
        .payload;
        assert_eq!(read_only_result, DMCommitResult::Success);
        assert_eq!(
            manager.txns.get(&read_only_tid).unwrap().lock().state,
            TxnState::Committed
        );
        let read_only_after = runtime
            .chunks()
            .read_cell(&read_only_id)
            .unwrap()
            .to_owned();
        assert_eq!(read_only_after.data, read_only_before.data);
        assert_eq!(
            read_only_after.header.revision_ts,
            read_only_before.header.revision_ts
        );

        let empty_tid = manager.hlc.now();
        let empty_prepare = prepare_ops_local(
            &manager,
            0,
            &empty_tid,
            vec![
                PrepareOp {
                    id: write_id,
                    expectation: CellExpectation::Present(write_version),
                    intent: PrepareIntent::Write,
                },
                PrepareOp {
                    id: read_id,
                    expectation: CellExpectation::Present(read_version),
                    intent: PrepareIntent::Read,
                },
            ],
        )
        .await;
        assert_eq!(empty_prepare, DMPrepareResult::Success);

        let empty_before_write = runtime.chunks().read_cell(&write_id).unwrap().to_owned();
        let empty_before_read = runtime.chunks().read_cell(&read_id).unwrap().to_owned();
        let empty_result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            empty_tid.clone(),
            vec![],
        )
        .await
        .payload;
        assert_eq!(
            empty_result,
            DMCommitResult::CheckFailed(CheckError::CellNumberDoesNotMatch(2, 0))
        );
        assert_eq!(
            manager.txns.get(&empty_tid).unwrap().lock().state,
            TxnState::Prepared
        );
        let empty_after_write = runtime.chunks().read_cell(&write_id).unwrap().to_owned();
        let empty_after_read = runtime.chunks().read_cell(&read_id).unwrap().to_owned();
        assert_eq!(empty_after_write.data, empty_before_write.data);
        assert_eq!(
            empty_after_write.header.revision_ts,
            empty_before_write.header.revision_ts
        );
        assert_eq!(empty_after_read.data, empty_before_read.data);
        assert_eq!(
            empty_after_read.header.revision_ts,
            empty_before_read.header.revision_ts
        );
        assert!(manager
            .txns
            .get(&empty_tid)
            .unwrap()
            .lock()
            .history
            .is_empty());
        abort_and_end_local(&manager, &empty_tid).await;

        let partial_tid = manager.hlc.now();
        let partial_prepare = prepare_ops_local(
            &manager,
            0,
            &partial_tid,
            vec![
                PrepareOp {
                    id: write_id,
                    expectation: CellExpectation::Present(write_version),
                    intent: PrepareIntent::Write,
                },
                PrepareOp {
                    id: read_id,
                    expectation: CellExpectation::Present(read_version),
                    intent: PrepareIntent::Read,
                },
            ],
        )
        .await;
        assert_eq!(partial_prepare, DMPrepareResult::Success);

        let partial_before_write = runtime.chunks().read_cell(&write_id).unwrap().to_owned();
        let partial_before_read = runtime.chunks().read_cell(&read_id).unwrap().to_owned();
        let partial_result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            partial_tid.clone(),
            vec![CommitOp::Read(read_id, read_version)],
        )
        .await
        .payload;
        assert_eq!(
            partial_result,
            DMCommitResult::CheckFailed(CheckError::CellNumberDoesNotMatch(2, 1))
        );
        assert_eq!(
            manager.txns.get(&partial_tid).unwrap().lock().state,
            TxnState::Prepared
        );
        let partial_after_write = runtime.chunks().read_cell(&write_id).unwrap().to_owned();
        let partial_after_read = runtime.chunks().read_cell(&read_id).unwrap().to_owned();
        assert_eq!(partial_after_write.data, partial_before_write.data);
        assert_eq!(
            partial_after_write.header.revision_ts,
            partial_before_write.header.revision_ts
        );
        assert_eq!(partial_after_read.data, partial_before_read.data);
        assert_eq!(
            partial_after_read.header.revision_ts,
            partial_before_read.header.revision_ts
        );
        assert!(manager
            .txns
            .get(&partial_tid)
            .unwrap()
            .lock()
            .history
            .is_empty());
        abort_and_end_local(&manager, &partial_tid).await;

        let extra_tid = manager.hlc.now();
        prepare_local_txn(&manager, &extra_tid, vec![write_id, read_id]);
        let extra_result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            extra_tid.clone(),
            vec![CommitOp::Remove(unprepared)],
        )
        .await
        .payload;
        assert_eq!(
            extra_result,
            DMCommitResult::CheckFailed(CheckError::CellNumberDoesNotMatch(2, 1))
        );
        assert_eq!(
            manager.txns.get(&extra_tid).unwrap().lock().state,
            TxnState::Prepared
        );

        let duplicate_tid = manager.hlc.now();
        prepare_local_txn(&manager, &duplicate_tid, vec![write_id, read_id]);
        let duplicate_result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            duplicate_tid.clone(),
            vec![CommitOp::Remove(write_id), CommitOp::Remove(write_id)],
        )
        .await
        .payload;
        assert_eq!(
            duplicate_result,
            DMCommitResult::CheckFailed(CheckError::CellNumberDoesNotMatch(2, 2))
        );
        assert_eq!(
            manager.txns.get(&duplicate_tid).unwrap().lock().state,
            TxnState::Prepared
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_validation_rejects_missing_coordinator_without_mutating_storage() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5347";
        let group = "txn_data_site_commit_missing_coordinator";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8204);
        let initial_score = 13;
        let initial_revision_ts =
            seed_cell_revision(&runtime, schema.id, cell_id, initial_score, 0);
        let tid = manager.hlc.now();
        let expected_owner = TxnPriority::new(tid.clone(), 0);

        prepare_local_txn(&manager, &tid, vec![cell_id]);
        {
            let txn_lock = manager.txns.get(&tid).expect("txn should exist");
            let mut txn = txn_lock.lock();
            txn.coordinator_id = None;
            txn.certified.insert(
                cell_id,
                PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision_ts),
                    intent: PrepareIntent::Write,
                },
            );
        }

        let result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            tid.clone(),
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                99,
                "counter_missing_coordinator_updated",
            ))],
        )
        .await
        .payload;

        assert_eq!(result, DMCommitResult::CheckFailed(CheckError::CannotEnd));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), initial_score);
        assert_eq!(persisted.header.revision_ts, initial_revision_ts);

        let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn_lock.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(expected_owner)
        );

        {
            let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
            let mut txn = txn_lock.lock();
            txn.coordinator_id = Some(0);
        }

        let abort_result = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort_result, AbortResult::Success(None));
        let end_result = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_does_not_reclaim_aged_foreign_owner() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5346";
        let group = "txn_data_site_aged_owner_wait_die";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8205);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 21, 0);
        let t1 = test_hlc(1, 21);
        let t2 = test_hlc(1, 22);
        let t1_owner = TxnPriority::new(t1.clone(), 21);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            21,
            t1.clone(),
            t1.clone(),
            vec![prepare_op.clone()],
        )
        .await
        .payload;
        assert_eq!(first, DMPrepareResult::Success);
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t1_owner.clone())
        );

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.lock_acquired_at = Some(get_time() - LOCK_TIMEOUT_MS - 1);
        }

        let second = <DataManager as Service>::prepare(
            &manager,
            22,
            t2.clone(),
            t2.clone(),
            vec![prepare_op],
        )
        .await
        .payload;
        assert_eq!(second, DMPrepareResult::NotRealizable);
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t1_owner.clone()),
            "owner age must not permit blind takeover"
        );

        let abort_t2 = <DataManager as Service>::abort(&manager, t2.clone(), t2.clone())
            .await
            .payload;
        assert_eq!(abort_t2, AbortResult::Success(None));
        let end_t2 = <DataManager as Service>::end(&manager, t2.clone(), t2.clone())
            .await
            .payload;
        assert_eq!(end_t2, EndResult::Success);
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t1_owner)
        );

        let abort_t1 = <DataManager as Service>::abort(&manager, t1.clone(), t1.clone())
            .await
            .payload;
        assert_eq!(abort_t1, AbortResult::Success(None));
        let end_t1 = <DataManager as Service>::end(&manager, t1.clone(), t1.clone())
            .await
            .payload;
        assert_eq!(end_t1, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn abort_validation_rejects_missing_coordinator_without_rollback() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5348";
        let group = "txn_data_site_abort_missing_coordinator";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8206);
        let initial_score = 41;
        let committed_score = 55;
        let initial_revision_ts =
            seed_cell_revision(&runtime, schema.id, cell_id, initial_score, 0);
        let tid = test_hlc(1, 23);
        let owner = TxnPriority::new(tid.clone(), 23);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        let prepare = <DataManager as Service>::prepare(
            &manager,
            23,
            tid.clone(),
            tid.clone(),
            vec![prepare_op],
        )
        .await
        .payload;
        assert_eq!(prepare, DMPrepareResult::Success);

        let commit = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            tid.clone(),
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                committed_score,
                "counter_abort_missing_coordinator_commit",
            ))],
        )
        .await
        .payload;
        assert_eq!(commit, DMCommitResult::Success);

        let committed = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        let committed_version = committed.header.revision_ts;
        assert_eq!(*committed.data["score"].u64().unwrap(), committed_score);
        assert!(committed_version > initial_revision_ts);

        {
            let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
            let mut txn = txn_lock.lock();
            assert_eq!(txn.state, TxnState::Committed);
            assert!(txn.history.contains_key(&cell_id));
            txn.coordinator_id = None;
        }

        let abort = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;

        assert_eq!(abort, AbortResult::CheckFailed(CheckError::CannotEnd));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), committed_score);
        assert_eq!(persisted.header.revision_ts, committed_version);

        let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn_lock.lock();
        assert_eq!(txn.state, TxnState::Committed);
        assert!(txn.history.contains_key(&cell_id));
        drop(txn);

        assert_eq!(manager.cell_meta_mutex(&cell_id).lock().owner, Some(owner));

        {
            let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
            let mut txn = txn_lock.lock();
            txn.coordinator_id = Some(23);
        }

        let end = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn abort_validation_rejects_stale_committed_owner_before_rollback() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5349";
        let group = "txn_data_site_abort_stale_owner";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8207);
        let initial_score = 61;
        let committed_score = 77;
        let initial_revision_ts =
            seed_cell_revision(&runtime, schema.id, cell_id, initial_score, 0);
        let t1 = test_hlc(1, 24);
        let t1_owner = TxnPriority::new(t1.clone(), 24);
        let t2_owner = TxnPriority::new(test_hlc(1, 25), 25);
        let t1_prepare = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        let prepare_t1 = <DataManager as Service>::prepare(
            &manager,
            24,
            t1.clone(),
            t1.clone(),
            vec![t1_prepare],
        )
        .await
        .payload;
        assert_eq!(prepare_t1, DMPrepareResult::Success);

        let commit_t1 = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            t1.clone(),
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                committed_score,
                "counter_abort_stale_owner_commit",
            ))],
        )
        .await
        .payload;
        assert_eq!(commit_t1, DMCommitResult::Success);

        let committed = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        let committed_version = committed.header.revision_ts;
        assert_eq!(*committed.data["score"].u64().unwrap(), committed_score);
        assert!(committed_version > initial_revision_ts);

        {
            let txn_lock = manager.txns.get(&t1).expect("t1 should remain tracked");
            let txn = txn_lock.lock();
            assert_eq!(txn.state, TxnState::Committed);
            assert!(txn.history.contains_key(&cell_id));
        }

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            // Simulate stale-owner replacement directly. A real prepare must not
            // observe or certify t1's pending revision before end promotes it.
            meta.owner = Some(t2_owner.clone());
            meta.lock_acquired_at = Some(get_time());
        }
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t2_owner.clone())
        );

        let abort_t1 = <DataManager as Service>::abort(&manager, t1.clone(), t1.clone())
            .await
            .payload;

        assert_eq!(abort_t1, AbortResult::CheckFailed(CheckError::CannotEnd));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), committed_score);
        assert_eq!(persisted.header.revision_ts, committed_version);

        let t1_txn_lock = manager.txns.get(&t1).expect("t1 should remain tracked");
        let t1_txn = t1_txn_lock.lock();
        assert_eq!(t1_txn.state, TxnState::Committed);
        assert!(t1_txn.history.contains_key(&cell_id));
        drop(t1_txn);

        assert_ne!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t1_owner.clone())
        );
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t2_owner.clone())
        );

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.owner = Some(t1_owner);
            meta.lock_acquired_at = Some(get_time());
        }

        let end_t1 = <DataManager as Service>::end(&manager, t1.clone(), t1.clone())
            .await
            .payload;
        assert_eq!(end_t1, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_vector_clock_stale_update_rejected_after_committed_peer_changes_version() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5351";
        let group = "txn_data_site_concurrent_stale_update";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8210);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 0, 0);
        let t1 = test_hlc(1, 11);
        let t2 = test_hlc(1, 22);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        // (Dropped the `t1.relation(&t2) == Concurrent` precondition: HLC is a
        // total order with no `Concurrent` relation. The behaviour under test —
        // a stale-versioned update from a peer being rejected after another peer
        // commits a new revision_ts — does not depend on the two tids' relative age.)

        let prepare_t1 = prepare_ops_local(&manager, 11, &t1, vec![prepare_op.clone()]).await;
        assert_eq!(prepare_t1, DMPrepareResult::Success);

        let commit_t1 = commit_ops_local(
            &manager,
            &t1,
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                1,
                "counter_concurrent_stale_t1",
            ))],
        )
        .await;
        assert_eq!(commit_t1, DMCommitResult::Success);

        let end_t1 = <DataManager as Service>::end(&manager, t1.clone(), t1.clone())
            .await
            .payload;
        assert_eq!(end_t1, EndResult::Success);

        let prepare_t2 = prepare_ops_local(&manager, 22, &t2, vec![prepare_op]).await;
        assert_eq!(prepare_t2, DMPrepareResult::NotRealizable);

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), 1);
        assert!(persisted.header.revision_ts > initial_revision_ts);

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_rejects_change_after_certification() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5352";
        let group = "txn_data_site_commit_rechecks_certified_version";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8211);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = test_hlc(1, 31);

        let prepare = prepare_ops_local(
            &manager,
            31,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(initial_revision_ts),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let mut external = counter_cell(schema.id, cell_id, 7, "counter_commit_external");
        let external_header = runtime.chunks().update_cell(&mut external).unwrap();
        assert!(external_header.revision_ts > initial_revision_ts);

        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                99,
                "counter_commit_after_certification",
            ))],
        )
        .await;
        assert_eq!(commit, DMCommitResult::CellChanged(cell_id));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), 7);
        assert_eq!(persisted.header.revision_ts, external_header.revision_ts);

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_remove_rejects_change_after_certification() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5353";
        let group = "txn_data_site_remove_rechecks_certified_version";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8212);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 2, 0);
        let tid = test_hlc(1, 32);

        let prepare = prepare_ops_local(
            &manager,
            32,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(initial_revision_ts),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let mut external = counter_cell(schema.id, cell_id, 8, "counter_remove_external");
        let external_header = runtime.chunks().update_cell(&mut external).unwrap();
        assert!(external_header.revision_ts > initial_revision_ts);

        let commit = commit_ops_local(&manager, &tid, vec![CommitOp::Remove(cell_id)]).await;
        assert_eq!(commit, DMCommitResult::CellChanged(cell_id));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), 8);
        assert_eq!(persisted.header.revision_ts, external_header.revision_ts);

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_update_rejects_missing_after_certification() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5356";
        let group = "txn_data_site_update_rechecks_missing_certification";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8216);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 6, 0);
        let tid = test_hlc(1, 35);

        let prepare = prepare_ops_local(
            &manager,
            35,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(initial_revision_ts),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        runtime
            .chunks()
            .remove_cell_by(&cell_id, |_| true)
            .expect("external direct removal should succeed");

        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                18,
                "counter_update_after_external_remove",
            ))],
        )
        .await;
        assert_eq!(commit, DMCommitResult::CellChanged(cell_id));
        assert!(
            matches!(
                runtime.chunks().read_cell(&cell_id),
                Err(ReadError::CellDoesNotExisted)
            ),
            "cell should remain missing after stale certified update"
        );

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_write_rejects_insert_after_absent_certification() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5354";
        let group = "txn_data_site_write_rechecks_absent_certification";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8213);
        let tid = test_hlc(1, 33);

        let prepare = prepare_ops_local(
            &manager,
            33,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Absent(None),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let mut external = counter_cell(schema.id, cell_id, 5, "counter_write_external");
        let external_header = runtime.chunks().write_cell(&mut external).unwrap();

        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![CommitOp::Write(counter_cell(
                schema.id,
                cell_id,
                11,
                "counter_write_after_absent_prepare",
            ))],
        )
        .await;
        assert_eq!(commit, DMCommitResult::CellChanged(cell_id));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), 5);
        assert_eq!(persisted.header.revision_ts, external_header.revision_ts);

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_prevalidates_full_payload_before_any_storage_mutation() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5355";
        let group = "txn_data_site_commit_prevalidation";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::new(0, 8214);
        let cell_b = Id::new(0, 8215);
        let version_a = seed_cell_revision(&runtime, schema.id, cell_a, 3, 0);
        let version_b = seed_cell_revision(&runtime, schema.id, cell_b, 4, 0);
        let tid = test_hlc(1, 34);

        let prepare = prepare_ops_local(
            &manager,
            34,
            &tid,
            vec![
                PrepareOp {
                    id: cell_a,
                    expectation: CellExpectation::Present(version_a),
                    intent: PrepareIntent::Write,
                },
                PrepareOp {
                    id: cell_b,
                    expectation: CellExpectation::Present(version_b),
                    intent: PrepareIntent::Read,
                },
            ],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let before_a = runtime.chunks().read_cell(&cell_a).unwrap().to_owned();
        let before_b = runtime.chunks().read_cell(&cell_b).unwrap().to_owned();
        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![
                CommitOp::Update(counter_cell(schema.id, cell_a, 13, "counter_prevalidate_a")),
                CommitOp::Update(counter_cell(schema.id, cell_b, 14, "counter_prevalidate_b")),
            ],
        )
        .await;

        assert_eq!(commit, DMCommitResult::CheckFailed(CheckError::CannotEnd));

        let after_a = runtime.chunks().read_cell(&cell_a).unwrap().to_owned();
        let after_b = runtime.chunks().read_cell(&cell_b).unwrap().to_owned();
        assert_eq!(after_a.data, before_a.data);
        assert_eq!(after_a.header.revision_ts, before_a.header.revision_ts);
        assert_eq!(after_b.data, before_b.data);
        assert_eq!(after_b.header.revision_ts, before_b.header.revision_ts);

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_prevalidates_current_storage_state_before_partial_write() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5357";
        let group = "txn_data_site_commit_storage_prevalidation";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::new(0, 8217);
        let cell_b = Id::new(0, 8218);
        let version_a = seed_cell_revision(&runtime, schema.id, cell_a, 15, 0);
        let version_b = seed_cell_revision(&runtime, schema.id, cell_b, 25, 0);
        let tid = test_hlc(1, 36);

        let prepare = prepare_ops_local(
            &manager,
            36,
            &tid,
            vec![
                PrepareOp {
                    id: cell_a,
                    expectation: CellExpectation::Present(version_a),
                    intent: PrepareIntent::Write,
                },
                PrepareOp {
                    id: cell_b,
                    expectation: CellExpectation::Present(version_b),
                    intent: PrepareIntent::Write,
                },
            ],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let before_a = runtime.chunks().read_cell(&cell_a).unwrap().to_owned();
        let before_b = runtime.chunks().read_cell(&cell_b).unwrap().to_owned();

        let mut external_b = counter_cell(schema.id, cell_b, 26, "counter_prevalidate_external_b");
        let external_b_header = runtime.chunks().update_cell(&mut external_b).unwrap();
        assert!(external_b_header.revision_ts > version_b);

        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![
                CommitOp::Update(counter_cell(
                    schema.id,
                    cell_a,
                    115,
                    "counter_prevalidate_storage_a",
                )),
                CommitOp::Update(counter_cell(
                    schema.id,
                    cell_b,
                    126,
                    "counter_prevalidate_storage_b",
                )),
            ],
        )
        .await;

        assert_eq!(commit, DMCommitResult::CellChanged(cell_b));

        let after_a = runtime.chunks().read_cell(&cell_a).unwrap().to_owned();
        let after_b = runtime.chunks().read_cell(&cell_b).unwrap().to_owned();
        assert_eq!(after_a.data, before_a.data);
        assert_eq!(after_a.header.revision_ts, before_a.header.revision_ts);
        assert_eq!(after_b.data, external_b.data);
        assert_eq!(after_b.header.revision_ts, external_b_header.revision_ts);
        assert_ne!(after_b.header.revision_ts, before_b.header.revision_ts);

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    // Regression guard for the `cell_meta_cleanup` orphan race.
    //
    // `cell_meta_cleanup`'s second pass checks `owner.is_none()` under the meta
    // lock but then calls `self.cells.remove` AFTER releasing that lock. An
    // in-flight INSERT prepare creates its cell meta via `cell_meta_mutex` (empty
    // read/write clocks, `owner = None`, pushed onto `cell_list`) and only later,
    // in the same synchronous region, assigns `owner = Some(..)`. Because the
    // background cleanup task runs on another worker thread (it is signalled on
    // every transaction `end`, see `cleanup_signal.store(true, ..)`), it could
    // lock the fresh meta first, evict it, and remove it from `self.cells` before
    // the prepare acquires the owner — orphaning the acquired lock (the cell map
    // would then hold no entry, and the next `cell_meta_mutex` would mint a fresh
    // unowned meta). The orphan is fail-safe rather than a lost update (`write_cell`
    // rejects a duplicate insert with `CellAlreadyExisted` and the participant
    // `commit` re-checks `owner`, so the orphaned transaction spuriously aborts
    // with `CannotEnd`), but it is still an incorrect spurious abort.
    //
    // The fix leaves metas whose `read` and `write` clocks are both still empty
    // (the in-flight-insert window) for a later pass. This test drives the window
    // deterministically and asserts the owned meta stays map-resident.
    #[tokio::test(flavor = "multi_thread")]
    async fn cell_meta_cleanup_must_not_orphan_a_lock_being_acquired_by_insert_prepare() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5372";
        let group = "txn_ds_cleanup_insert_race";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;

        // The preparing insert transaction is registered exactly as real prepare
        // does via `get_or_create_transaction`, so the cleanup watermark `oldest`
        // is this (non-empty) clock. A freshly created insert meta carries empty
        // read/write clocks, which sort strictly below `oldest`, so only the
        // racy `owner.is_none()` check stands between it and eviction.
        let insert_tid = test_hlc(9, 7);
        let _txn = manager.get_or_create_transaction(&insert_tid);

        // Model prepare mid-flight: the meta exists (owner not yet acquired).
        let insert_id = Id::new(0, 90501);
        let meta_in_prepare = manager.cell_meta_mutex(&insert_id);
        assert!(
            manager.cells.get(&insert_id).is_some(),
            "precondition: the in-flight insert meta is registered in the cell map"
        );
        assert!(
            meta_in_prepare.lock().owner.is_none(),
            "precondition: prepare has not yet acquired the owner"
        );

        // The background cleanup pass runs during that window.
        manager.cell_meta_cleanup().await;

        // Prepare now finishes acquiring the owner on the meta it created.
        {
            let mut meta = meta_in_prepare.lock();
            meta.owner = Some(TxnPriority::new(insert_tid.clone(), 0));
            meta.lock_acquired_at = Some(get_time());
        }

        // Safety property: the cell map must still resolve to the meta prepare
        // acquired its owner on. If cleanup removed the entry during the window,
        // a subsequent `cell_meta_mutex` would mint a fresh, unowned meta and the
        // acquired lock is orphaned.
        let visible = manager.cells.get(&insert_id);
        let same_meta = visible
            .as_ref()
            .is_some_and(|m| Arc::ptr_eq(m, &meta_in_prepare));
        assert!(
            same_meta,
            "cell_meta_cleanup orphaned the in-flight insert lock: after prepare \
             acquired its owner, the cell map no longer resolves to that owned meta \
             (present={}, same_arc={})",
            visible.is_some(),
            same_meta
        );
        assert_eq!(
            visible.unwrap().lock().owner,
            Some(TxnPriority::new(insert_tid, 0)),
            "the meta visible to other transactions must reflect the acquired owner"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_certifies_exact_tombstone_never_absence_and_full_id() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5397";
        let group = "txn_data_site_exact_absence";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let deleted_id = Id::new(0, 99030);
        let never_id = Id::new(0, 99031);
        let colliding_id = Id::new(1, deleted_id.lower);

        let deleted_revision = seed_cell_revision(&runtime, schema.id, deleted_id, 1, 0);
        runtime.chunks().remove_cell(&deleted_id).unwrap();
        let tombstone_revision = match manager.current_expectation(&deleted_id).unwrap() {
            CellExpectation::Absent(Some(revision_ts)) => revision_ts,
            other => panic!("expected exact tombstone, got {other:?}"),
        };
        assert!(tombstone_revision > deleted_revision);

        let wrong_tid = manager.hlc.now();
        let wrong = prepare_ops_local(
            &manager,
            41,
            &wrong_tid,
            vec![PrepareOp {
                id: deleted_id,
                expectation: CellExpectation::Absent(Some(tombstone_revision - 1)),
                intent: PrepareIntent::Read,
            }],
        )
        .await;
        assert_eq!(wrong, DMPrepareResult::NotRealizable);
        assert!(manager.cell_meta_mutex(&deleted_id).lock().owner.is_none());

        let exact_tid = manager.hlc.now();
        let exact = prepare_ops_local(
            &manager,
            42,
            &exact_tid,
            vec![
                PrepareOp {
                    id: deleted_id,
                    expectation: CellExpectation::Absent(Some(tombstone_revision)),
                    intent: PrepareIntent::Read,
                },
                PrepareOp {
                    id: never_id,
                    expectation: CellExpectation::Absent(None),
                    intent: PrepareIntent::Write,
                },
            ],
        )
        .await;
        assert_eq!(exact, DMPrepareResult::Success);
        abort_and_end_local(&manager, &exact_tid).await;

        let collision_tid = manager.hlc.now();
        let collision = prepare_ops_local(
            &manager,
            43,
            &collision_tid,
            vec![PrepareOp {
                id: colliding_id,
                expectation: CellExpectation::Present(tombstone_revision),
                intent: PrepareIntent::Read,
            }],
        )
        .await;
        assert_eq!(collision, DMPrepareResult::NotRealizable);
        assert!(manager
            .cell_meta_mutex(&colliding_id)
            .lock()
            .owner
            .is_none());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partial_point_observation_is_a_prepare_certificate() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5398";
        let group = "txn_data_site_partial_certificate";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let read_id = Id::new(0, 99032);
        let write_id = Id::new(0, 99033);
        let read_revision = seed_cell_revision(&runtime, schema.id, read_id, 1, 0);
        let write_revision = seed_cell_revision(&runtime, schema.id, write_id, 2, 0);
        let tid = manager.hlc.now();

        let observed =
            <DataManager as Service>::read_partial_raw(&manager, 44, tid, tid, read_id, 0, 8)
                .await
                .payload
                .unwrap();
        assert_eq!(
            observed.expectation,
            CellExpectation::Present(read_revision)
        );

        let mut external = counter_cell(schema.id, read_id, 7, "partial-cert-external");
        runtime.chunks().update_cell(&mut external).unwrap();

        let prepare = prepare_ops_local(
            &manager,
            44,
            &tid,
            vec![
                PrepareOp {
                    id: read_id,
                    expectation: observed.expectation,
                    intent: PrepareIntent::Read,
                },
                PrepareOp {
                    id: write_id,
                    expectation: CellExpectation::Present(write_revision),
                    intent: PrepareIntent::Write,
                },
            ],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::NotRealizable);
        assert!(manager.cell_meta_mutex(&read_id).lock().owner.is_none());
        assert!(manager.cell_meta_mutex(&write_id).lock().owner.is_none());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_installs_pending_retries_same_hlc_and_end_promotes() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5399";
        let group = "txn_data_site_pending_commit";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99034);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision),
            intent: PrepareIntent::Write,
        };
        assert_eq!(
            prepare_ops_local(&manager, 45, &tid, vec![op]).await,
            DMPrepareResult::Success
        );

        let commit_hlc = manager.hlc.now();
        let commit_op = CommitOp::Update(counter_cell(schema.id, cell_id, 9, "pending-commit-new"));
        let first =
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![commit_op.clone()])
                .await
                .payload;
        assert_eq!(first, DMCommitResult::Success);

        let txn = manager.txns.get(&tid).unwrap();
        {
            let txn = txn.lock();
            assert_eq!(txn.commit_hlc, Some(commit_hlc));
            assert_eq!(txn.installed.len(), 1);
            assert_eq!(
                txn.installed[&cell_id].node.load().0,
                RevisionState::PendingPresent
            );
        }
        assert!(matches!(
            runtime
                .chunks()
                .read_cell_snapshot(&cell_id, u64::MAX)
                .unwrap(),
            SnapshotRead::Wait
        ));
        let reader_tid = manager.hlc.now();
        assert!(matches!(
            <DataManager as Service>::read(&manager, 46, reader_tid, reader_tid, cell_id,)
                .await
                .payload,
            TxnExecResult::Wait
        ));

        let retry =
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![commit_op.clone()])
                .await
                .payload;
        assert_eq!(retry, DMCommitResult::Success);
        let conflicting =
            <DataManager as Service>::commit(&manager, manager.hlc.now(), tid, vec![commit_op])
                .await
                .payload;
        assert_eq!(
            conflicting,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );

        let end = <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
            .await
            .payload;
        assert_eq!(end, EndResult::Success);
        let current = runtime
            .chunks()
            .read_cell_snapshot(&cell_id, u64::MAX)
            .unwrap();
        match current {
            SnapshotRead::Present(cell) => {
                assert_eq!(cell.header.revision_ts, commit_hlc.ts);
                assert_eq!(*cell.data["score"].u64().unwrap(), 9);
            }
            other => panic!("expected promoted commit, got {other:?}"),
        }
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::NotExisted)
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_retry_same_hlc_accepts_identical_nan_payload_bits() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5410";
        let group = "txn_data_site_commit_retry_identical_nan";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99041);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                51,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );

        let nan_bits = 0x7ff8_0000_0000_0042;
        let mut cell = counter_cell(schema.id, cell_id, 9, "exact-nan-payload");
        let OwnedValue::Map(data) = &mut cell.data else {
            unreachable!()
        };
        data.insert("measurement", OwnedValue::F64(f64::from_bits(nan_bits)));
        let commit_op = CommitOp::Update(cell);
        let commit_hlc = manager.hlc.now();
        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![commit_op.clone()],)
                .await
                .payload,
            DMCommitResult::Success
        );
        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![commit_op])
                .await
                .payload,
            DMCommitResult::Success,
            "an exact same-HLC NaN retry must be idempotently accepted"
        );

        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_retry_same_hlc_requires_exact_operation_and_cell_without_mutation() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5403";
        let group = "txn_data_site_commit_retry_exact_payload";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99035);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                47,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );

        let commit_hlc = manager.hlc.now();
        let mut original_cell = counter_cell(schema.id, cell_id, 9, "exact-payload-original");
        let OwnedValue::Map(original_data) = &mut original_cell.data else {
            unreachable!()
        };
        original_data.insert("signed_zero", OwnedValue::F64(0.0));
        let original = CommitOp::Update(original_cell);
        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![original.clone()],)
                .await
                .payload,
            DMCommitResult::Success
        );

        let txn_lock = manager.txns.get(&tid).expect("transaction should exist");
        let installed_before = txn_lock.lock().installed[&cell_id].node.load();
        let mut changed_signed_zero = original.clone();
        let CommitOp::Update(changed_cell) = &mut changed_signed_zero else {
            unreachable!()
        };
        let OwnedValue::Map(changed_data) = &mut changed_cell.data else {
            unreachable!()
        };
        changed_data
            .map
            .insert(hash_str("signed_zero"), OwnedValue::F64(-0.0));
        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![changed_signed_zero],)
                .await
                .payload,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        let changed = CommitOp::Update(counter_cell(
            schema.id,
            cell_id,
            10,
            "exact-payload-changed",
        ));
        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![changed])
                .await
                .payload,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        assert_eq!(
            <DataManager as Service>::commit(
                &manager,
                commit_hlc,
                tid,
                vec![CommitOp::Remove(cell_id)],
            )
            .await
            .payload,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        let mut changed_header = counter_cell(schema.id, cell_id, 9, "exact-payload-original");
        changed_header.header.flags = 1;
        assert_eq!(
            <DataManager as Service>::commit(
                &manager,
                commit_hlc,
                tid,
                vec![CommitOp::Update(changed_header)],
            )
            .await
            .payload,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        assert_eq!(
            <DataManager as Service>::commit(
                &manager,
                commit_hlc,
                tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    Id::new(1, cell_id.lower),
                    9,
                    "exact-payload-original",
                ))],
            )
            .await
            .payload,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        assert_eq!(
            txn_lock.lock().installed[&cell_id].node.load(),
            installed_before,
            "a conflicting retry must not mutate the installed revision"
        );

        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        let current = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*current.data["score"].u64().unwrap(), 9);

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_owner_loss_preserves_pending_state_and_can_retry() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5404";
        let group = "txn_data_site_end_owner_loss";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99036);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        let coordinator_id = 48;
        let owner = TxnPriority::new(tid, coordinator_id);
        assert_eq!(
            prepare_ops_local(
                &manager,
                coordinator_id,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "end-owner-loss-pending",
                ))],
            )
            .await,
            DMCommitResult::Success
        );

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.owner = None;
            meta.lock_acquired_at = None;
        }
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        let txn_lock = manager
            .txns
            .get(&tid)
            .expect("failed end must preserve transaction state");
        assert_eq!(txn_lock.lock().state, TxnState::Committed);
        assert_eq!(
            txn_lock.lock().installed[&cell_id].node.load().0,
            RevisionState::PendingPresent,
            "failed end must not promote before validating ownership"
        );

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.owner = Some(owner);
            meta.lock_acquired_at = Some(get_time());
        }
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        match runtime
            .chunks()
            .read_cell_snapshot(&cell_id, u64::MAX)
            .unwrap()
        {
            SnapshotRead::Present(cell) => {
                assert_eq!(*cell.data["score"].u64().unwrap(), 9);
            }
            other => panic!("expected promoted retry, got {other:?}"),
        }

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_validates_all_cell_owners_before_multi_cell_promotion() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5405";
        let group = "txn_data_site_end_multi_cell_barrier";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let first_id = Id::new(0, 99037);
        let second_id = Id::new(0, 99038);
        let first_revision = seed_cell_revision(&runtime, schema.id, first_id, 1, 0);
        let second_revision = seed_cell_revision(&runtime, schema.id, second_id, 2, 0);
        let tid = manager.hlc.now();
        let coordinator_id = 49;
        let owner = TxnPriority::new(tid, coordinator_id);
        assert_eq!(
            prepare_ops_local(
                &manager,
                coordinator_id,
                &tid,
                vec![
                    PrepareOp {
                        id: first_id,
                        expectation: CellExpectation::Present(first_revision),
                        intent: PrepareIntent::Write,
                    },
                    PrepareOp {
                        id: second_id,
                        expectation: CellExpectation::Present(second_revision),
                        intent: PrepareIntent::Write,
                    },
                ],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![
                    CommitOp::Update(counter_cell(schema.id, first_id, 11, "end-barrier-first",)),
                    CommitOp::Update(counter_cell(schema.id, second_id, 12, "end-barrier-second",)),
                ],
            )
            .await,
            DMCommitResult::Success
        );

        let foreign_owner = TxnPriority::new(manager.hlc.now(), 99);
        {
            let meta = manager.cell_meta_mutex(&second_id);
            let mut meta = meta.lock();
            meta.owner = Some(foreign_owner.clone());
            meta.lock_acquired_at = Some(get_time());
        }
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        let txn_lock = manager
            .txns
            .get(&tid)
            .expect("failed end must preserve transaction state");
        for id in [first_id, second_id] {
            assert_eq!(
                txn_lock.lock().installed[&id].node.load().0,
                RevisionState::PendingPresent,
                "owner validation failure must precede every promotion"
            );
        }
        assert_eq!(
            manager.cell_meta_mutex(&first_id).lock().owner,
            Some(owner.clone())
        );
        assert_eq!(
            manager.cell_meta_mutex(&second_id).lock().owner,
            Some(foreign_owner)
        );

        {
            let meta = manager.cell_meta_mutex(&second_id);
            let mut meta = meta.lock();
            meta.owner = Some(owner);
            meta.lock_acquired_at = Some(get_time());
        }
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_marker_failure_keeps_pending_revision_owner_and_transaction_for_retry() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5407";
        let group = "txn_data_site_end_marker_failure";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99041);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        let coordinator_id = 51;
        let owner = TxnPriority::new(tid, coordinator_id);
        assert_eq!(
            prepare_ops_local(
                &manager,
                coordinator_id,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "end-marker-failure",
                ))],
            )
            .await,
            DMCommitResult::Success
        );
        let installed_location = manager.txns.get(&tid).unwrap().lock().installed[&cell_id]
            .node
            .load()
            .1;
        let installed_segment = runtime.chunks().list[0]
            .locate_segment(installed_location)
            .unwrap();
        let syncs_before_end = installed_segment.force_wal_sync_count_for_test();
        assert!(
            manager
                .txns
                .get(&tid)
                .unwrap()
                .lock()
                .installed_output_durable,
            "participant commit success must already prove exact output durability"
        );

        runtime
            .undo_log()
            .unwrap()
            .fail_next_commit_marker_for_test();
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        assert_eq!(
            installed_segment.force_wal_sync_count_for_test(),
            syncs_before_end,
            "end must reuse the participant commit durability proof instead of double-fsyncing"
        );
        let txn_lock = manager
            .txns
            .get(&tid)
            .expect("marker failure must preserve transaction state");
        {
            let txn = txn_lock.lock();
            assert_eq!(txn.state, TxnState::Committed);
            assert_eq!(txn.durable_decision, None);
            assert_eq!(
                txn.installed[&cell_id].node.load().0,
                RevisionState::PendingPresent
            );
        }
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner.clone())
        );
        assert!(
            runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "participant marker failure must retain undo until end succeeds"
        );

        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        assert_eq!(
            installed_segment.force_wal_sync_count_for_test(),
            syncs_before_end,
            "retry must continue reusing the participant commit durability proof"
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_retry_after_durable_commit_completion_is_idempotently_successful() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5425";
        let group = "txn_data_site_end_response_loss_retry";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99045);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                55,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "response-was-lost-after-durable-end",
                ))],
            )
            .await,
            DMCommitResult::Success
        );

        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success,
            "a retry must use durable participant outcome evidence after live state is gone"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn participant_commit_syncs_exact_output_before_success_without_emitting_decision() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5426";
        let group = "txn_data_site_commit_output_durability";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99046);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                56,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        let output_segment = runtime.chunks().list[0]
            .locate_segment(runtime.chunks().address_of(&cell_id))
            .unwrap();
        output_segment.fail_next_force_wal_sync_for_test();
        let update = CommitOp::Update(counter_cell(
            schema.id,
            cell_id,
            9,
            "commit-output-must-be-durable",
        ));
        let commit_hlc = manager.hlc.now();

        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![update.clone()],)
                .await
                .payload,
            DMCommitResult::CheckFailed(CheckError::CannotEnd)
        );
        let txn_lock = manager
            .txns
            .get(&tid)
            .expect("output sync failure must retain participant state");
        assert_eq!(
            txn_lock.lock().installed[&cell_id].node.load().0,
            RevisionState::PendingPresent
        );
        {
            let txn = txn_lock.lock();
            assert_eq!(txn.state, TxnState::Prepared);
            assert!(!txn.installed_output_durable);
            assert_eq!(txn.durable_decision, None);
        }
        let owner = manager.cells.get(&cell_id).unwrap();
        assert_eq!(
            owner.lock().owner,
            Some(TxnPriority::new(tid, 56)),
            "retryable sync contention must retain the participant owner"
        );
        assert!(
            runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "participant commit durability is not a transaction decision"
        );
        assert_eq!(
            runtime
                .undo_log()
                .unwrap()
                .participant_completion(&tid)
                .unwrap(),
            None,
            "retryable sync contention must not emit a participant marker"
        );

        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![update])
                .await
                .payload,
            DMCommitResult::Success
        );
        assert!(
            runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "successful output preparation must not emit a completion marker"
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn durable_mutation_without_available_undo_is_rejected_before_cell_change() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5409";
        let group = "txn_data_site_durable_requires_undo";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99043);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                53,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );

        manager.fail_next_undo_availability_for_test();
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "must-not-be-installed",
                ))],
            )
            .await,
            DMCommitResult::CheckFailed(CheckError::CannotEnd)
        );
        let retained = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(retained.header.revision_ts, initial_revision);
        assert_eq!(*retained.data["score"].u64().unwrap(), 1);

        assert_eq!(
            <DataManager as Service>::abort(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            AbortResult::Success(None)
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backup_only_direct_participant_rejects_mutation_without_wal_before_any_change() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5424";
        let group = "txn_data_site_backup_without_wal";
        let server = start_backup_without_wal_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = DataManager::new(runtime.clone(), server.hlc.clone());
        let cell_id = Id::new(0, 99044);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                54,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );

        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "backup-only-must-not-install",
                ))],
            )
            .await,
            DMCommitResult::CheckFailed(CheckError::CannotEnd)
        );
        let retained = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(retained.header.revision_ts, initial_revision);
        assert_eq!(*retained.data["score"].u64().unwrap(), 1);
        assert!(
            !runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "rejection must precede undo output and every durable decision"
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        assert!(!runtime
            .undo_log()
            .unwrap()
            .recover()
            .unwrap()
            .contains_key(&tid));

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compensation_sync_failure_retries_the_exact_handle_without_duplicate_output() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5408";
        let group = "txn_data_site_compensation_sync_retry";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99042);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        let coordinator_id = 52;
        let owner = TxnPriority::new(tid, coordinator_id);
        assert_eq!(
            prepare_ops_local(
                &manager,
                coordinator_id,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "compensation-sync-failed-output",
                ))],
            )
            .await,
            DMCommitResult::Success
        );
        let installed_location = manager.txns.get(&tid).unwrap().lock().installed[&cell_id]
            .node
            .load()
            .1;
        runtime.chunks().list[0]
            .locate_segment(installed_location)
            .unwrap()
            .fail_next_force_wal_sync_for_test();

        assert_eq!(
            <DataManager as Service>::abort(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            AbortResult::CheckFailed(CheckError::CannotEnd)
        );
        let compensation = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*compensation.data["score"].u64().unwrap(), 1);
        let compensation_ts = compensation.header.revision_ts;
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner.clone())
        );

        assert_eq!(
            <DataManager as Service>::abort(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            AbortResult::Success(None)
        );
        assert_eq!(
            runtime
                .chunks()
                .read_cell(&cell_id)
                .unwrap()
                .header
                .revision_ts,
            compensation_ts,
            "retry must sync the retained exact compensation instead of installing another"
        );
        let compensation_location = manager.txns.get(&tid).unwrap().lock().history[&cell_id]
            .compensation
            .as_ref()
            .unwrap()
            .node
            .load()
            .1;
        let compensation_segment = runtime.chunks().list[0]
            .locate_segment(compensation_location)
            .unwrap();
        let syncs_after_abort = compensation_segment.force_wal_sync_count_for_test();
        runtime
            .undo_log()
            .unwrap()
            .fail_next_abort_marker_for_test();
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        assert_eq!(
            compensation_segment.force_wal_sync_count_for_test(),
            syncs_after_abort,
            "end must reuse the participant abort durability proof instead of double-fsyncing"
        );
        assert!(
            manager.txns.get(&tid).is_some(),
            "abort marker failure must preserve retryable transaction state"
        );
        assert!(
            runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "a failed abort marker must not suppress recovery"
        );
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner),
            "abort marker failure must preserve the owner barrier"
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        assert_eq!(
            compensation_segment.force_wal_sync_count_for_test(),
            syncs_after_abort,
            "end retry must continue reusing the participant abort durability proof"
        );
        assert!(
            !runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "the synced abort marker must suppress recovery only after exact compensation sync"
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_partial_promotion_failure_restores_pending_barrier_for_retry() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5406";
        let group = "txn_data_site_end_partial_promotion";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let first_id = Id::new(0, 99039);
        let second_id = Id::new(0, 99040);
        let first_revision = seed_cell_revision(&runtime, schema.id, first_id, 1, 0);
        let second_revision = seed_cell_revision(&runtime, schema.id, second_id, 2, 0);
        let tid = manager.hlc.now();
        let coordinator_id = 50;
        let owner = TxnPriority::new(tid, coordinator_id);
        assert_eq!(
            prepare_ops_local(
                &manager,
                coordinator_id,
                &tid,
                vec![
                    PrepareOp {
                        id: first_id,
                        expectation: CellExpectation::Present(first_revision),
                        intent: PrepareIntent::Write,
                    },
                    PrepareOp {
                        id: second_id,
                        expectation: CellExpectation::Present(second_revision),
                        intent: PrepareIntent::Write,
                    },
                ],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![
                    CommitOp::Update(counter_cell(
                        schema.id,
                        first_id,
                        11,
                        "partial-promotion-first",
                    )),
                    CommitOp::Update(counter_cell(
                        schema.id,
                        second_id,
                        12,
                        "partial-promotion-second",
                    )),
                ],
            )
            .await,
            DMCommitResult::Success
        );

        let failure = install_end_promotion_failure(tid, second_id);
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        let txn_lock = manager
            .txns
            .get(&tid)
            .expect("failed promotion must preserve transaction state");
        assert_eq!(txn_lock.lock().state, TxnState::Committed);
        assert_eq!(
            txn_lock.lock().durable_decision,
            Some(TxnState::Committed),
            "the synced marker remains the irrevocable commit decision"
        );
        assert!(
            runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .get(&tid)
                .is_none(),
            "the durable commit marker must suppress undo before promotion retry"
        );
        assert_eq!(
            <DataManager as Service>::abort(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            AbortResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        for id in [first_id, second_id] {
            assert_eq!(
                txn_lock.lock().installed[&id].node.load().0,
                RevisionState::PendingPresent,
                "partial failure must restore the complete pending barrier"
            );
            assert_eq!(
                manager.cell_meta_mutex(&id).lock().owner,
                Some(owner.clone()),
                "partial failure must preserve every owner"
            );
        }

        drop(failure);
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        for (id, expected_score) in [(first_id, 11), (second_id, 12)] {
            match runtime.chunks().read_cell_snapshot(&id, u64::MAX).unwrap() {
                SnapshotRead::Present(cell) => {
                    assert_eq!(*cell.data["score"].u64().unwrap(), expected_score);
                }
                other => panic!("expected promoted retry, got {other:?}"),
            }
        }

        server.shutdown().await;
    }
}

impl Service for DataManager {
    /////////////////////////////////////
    ///        Implement Services    ///
    ///////////////////////////////////

    fn read(
        &self,
        _server_id: u64,
        clock: Hlc,
        tid: TxnId,
        id: Id,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<ObservedPoint<OwnedCell>, ReadError>>> {
        FULL_READ_RPC_COUNT.fetch_add(1, Relaxed);
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        self.response_with(Self::observed_snapshot(
            self.chunks().read_cell_snapshot(&id, tid.ts),
            |cell| cell.header.revision_ts,
        ))
    }
    fn read_selected(
        &self,
        _server_id: u64,
        clock: Hlc,
        tid: TxnId,
        id: Id,
        fields: Vec<u64>,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<ObservedPoint<OwnedCell>, ReadError>>> {
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        self.response_with(Self::observed_snapshot(
            self.chunks().read_selected_snapshot(&id, tid.ts, &fields),
            |cell| cell.header.revision_ts,
        ))
    }
    fn head(
        &self,
        _server_id: u64,
        clock: Hlc,
        tid: TxnId,
        id: Id,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<ObservedPoint<CellHeader>, ReadError>>> {
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        self.response_with(Self::observed_snapshot(
            self.chunks().head_snapshot(&id, tid.ts),
            |header| header.revision_ts,
        ))
    }
    fn read_partial_raw(
        &self,
        _server_id: u64,
        clock: Hlc,
        tid: TxnId,
        id: Id,
        offset: usize,
        len: usize,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<ObservedPoint<Vec<u8>>, ReadError>>> {
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        let payload = match self
            .chunks()
            .read_partial_raw_snapshot(&id, tid.ts, offset, len)
        {
            Ok(SnapshotRead::Present(value)) => match self.chunks().head_snapshot(&id, tid.ts) {
                Ok(SnapshotRead::Present(header)) => TxnExecResult::Accepted(ObservedPoint {
                    value: Some(value),
                    expectation: CellExpectation::Present(header.revision_ts),
                }),
                Ok(SnapshotRead::Wait) => TxnExecResult::Wait,
                Err(error) => TxnExecResult::Error(error),
                Ok(SnapshotRead::Absent(_)) => TxnExecResult::Error(ReadError::NotMatch),
            },
            Ok(SnapshotRead::Absent(delete_revision_ts)) => {
                TxnExecResult::Accepted(ObservedPoint {
                    value: None,
                    expectation: CellExpectation::Absent(delete_revision_ts),
                })
            }
            Ok(SnapshotRead::Wait) => TxnExecResult::Wait,
            Err(error) => TxnExecResult::Error(error),
        };
        self.response_with(payload)
    }
    fn prepare(
        &self,
        coordinator_id: u64,
        clock: Hlc,
        tid: TxnId,
        ops: Vec<PrepareOp>,
    ) -> BoxFuture<'_, DataSiteResponse<DMPrepareResult>> {
        debug!("PREPARE FOR {:?}, {} ops", &tid, ops.len());
        async move {
            #[cfg(feature = "occ_phase_profile")]
            let _phase_guard =
                super::phase_profile::guard(super::phase_profile::Phase::ParticipantPrepare);
            self.update_clock(clock);

            let prepared_ops = match Self::canonical_prepare_ops(ops) {
                Ok(prepared_ops) => prepared_ops,
                Err(result) => return self.response_with(result).await,
            };
            #[cfg(test)]
            if let Some(state) = Self::take_matching_prepare_delay(&tid, &prepared_ops) {
                Self::await_prepare_delay(&state).await;
            }

            let prepared_ops_by_id: BTreeMap<Id, PrepareOp> =
                prepared_ops.iter().cloned().map(|op| (op.id, op)).collect();
            let requester = TxnPriority::new(tid.clone(), coordinator_id);

            let result = 'result: {
                let txn_lock = self.get_or_create_transaction(&tid);
                let mut txn = txn_lock.lock();
                match txn.state {
                    TxnState::Started => {}
                    TxnState::Prepared => {
                        if txn.coordinator_id != Some(coordinator_id)
                            || txn.certified != prepared_ops_by_id
                        {
                            break 'result DMPrepareResult::StateError(TxnState::Prepared);
                        }
                    }
                    _ => break 'result DMPrepareResult::StateError(txn.state),
                }

                let mut cell_mutices = Vec::with_capacity(prepared_ops.len());
                let mut cell_guards = Vec::with_capacity(prepared_ops.len());

                for op in &prepared_ops {
                    cell_mutices.push(self.cell_meta_mutex(&op.id));
                }
                for cell_mutex in &cell_mutices {
                    let meta = cell_mutex.lock();

                    if let Some(owner) = meta.owner.clone() {
                        if owner != requester {
                            let lock_age = meta
                                .lock_acquired_at
                                .map(|acquired| get_time() - acquired)
                                .unwrap_or(0);

                            if requester.compare_age(&owner).is_gt() {
                                debug!(
                                    "PREPARE Wait-Die: younger txn {:?} aborted, cell owned by older {:?} (lock age: {}ms)",
                                    requester, owner, lock_age
                                );
                                break 'result DMPrepareResult::NotRealizable;
                            } else {
                                debug!(
                                    "PREPARE Wait-Die: older txn {:?} waits for younger owner {:?} (lock age: {}ms)",
                                    requester, owner, lock_age
                                );
                                break 'result DMPrepareResult::Wait;
                            }
                        }
                    }

                    cell_guards.push(meta);
                }

                let lock_time = get_time();
                for meta in &mut cell_guards {
                    meta.owner = Some(requester.clone());
                    meta.lock_acquired_at = Some(lock_time);
                }

                for op in &prepared_ops {
                    if !self.prepare_expectation_matches(op) {
                        debug!(
                            "PREPARE expectation mismatch for {:?} on cell {:?}: {:?}",
                            requester, op.id, op
                        );
                        for meta in &mut cell_guards {
                            if meta.owner.as_ref() == Some(&requester) {
                                meta.owner = None;
                                meta.lock_acquired_at = None;
                            }
                        }
                        break 'result DMPrepareResult::NotRealizable;
                    }
                }

                txn.certified = prepared_ops_by_id;
                txn.affected_cells = txn.certified.keys().copied().collect();
                txn.coordinator_id = Some(coordinator_id);
                txn.state = TxnState::Prepared;
                txn.last_activity = get_time();
                debug!("SITE PREPARE SUCCESSFUL FOR {:?}", requester);
                DMPrepareResult::Success
            };

            self.response_with(result).await
        }
        .boxed()
    }
    fn commit(
        &self,
        commit_hlc: Hlc,
        tid: TxnId,
        cells: Vec<CommitOp>,
    ) -> BoxFuture<'_, DataSiteResponse<DMCommitResult>> {
        #[cfg(feature = "occ_phase_profile")]
        let phase_guard =
            super::phase_profile::guard(super::phase_profile::Phase::ParticipantCommit);
        self.update_clock(commit_hlc);

        #[cfg(test)]
        if let Some(state) = Self::take_matching_commit_delay(&tid, &cells) {
            return async move {
                #[cfg(feature = "occ_phase_profile")]
                let _phase_guard = phase_guard;
                Self::await_prepare_delay(&state).await;
                let payload = match self.find_transaction(&tid) {
                    Some(txn_lock) => self.apply_commit_ops(&txn_lock, &tid, commit_hlc, cells),
                    None => DMCommitResult::CheckFailed(CheckError::NotExisted),
                };
                DataSiteResponse::new(self.hlc.now(), payload)
            }
            .boxed();
        }

        let Some(txn_lock) = self.find_transaction(&tid) else {
            return self.response_with(DMCommitResult::CheckFailed(CheckError::NotExisted));
        };

        if self.database_runtime.indexer().is_some() {
            let tid_for_logs = tid.clone();
            let scoped_commit = IndexBuilder::with_request_index_scope({
                let txn_lock = txn_lock.clone();
                let tid = tid.clone();
                move || self.apply_commit_ops(&txn_lock, &tid, commit_hlc, cells)
            });
            return async move {
                #[cfg(feature = "occ_phase_profile")]
                let _phase_guard = phase_guard;
                let (payload, request_results) = scoped_commit.await;
                let pending_results = IndexBuilder::await_indices().await;
                self.warn_on_index_wait_results(
                    &tid_for_logs,
                    request_results
                        .into_iter()
                        .chain(pending_results.into_iter()),
                );
                DataSiteResponse::new(self.hlc.now(), payload)
            }
            .boxed();
        }

        #[cfg(feature = "occ_phase_profile")]
        let _phase_guard = phase_guard;
        self.response_with(self.apply_commit_ops(&txn_lock, &tid, commit_hlc, cells))
    }
    fn abort(&self, clock: Hlc, tid: TxnId) -> BoxFuture<'_, DataSiteResponse<AbortResult>> {
        debug!(">> ABORT {:?}", tid);
        #[cfg(feature = "occ_phase_profile")]
        let _phase_guard =
            super::phase_profile::guard(super::phase_profile::Phase::ParticipantAbort);
        self.update_clock(clock);
        let Some(txn_lock) = self.find_transaction(&tid) else {
            return self.response_with(AbortResult::CheckFailed(CheckError::NotExisted));
        };
        let mut txn = txn_lock.lock();
        if txn.durable_decision == Some(TxnState::Committed) {
            return self.response_with(AbortResult::CheckFailed(CheckError::AlreadyCommitted));
        }
        if txn.state == TxnState::Aborted {
            return self.response_with(AbortResult::CheckFailed(CheckError::AlreadyAborted));
        }
        #[cfg(test)]
        if Self::take_matching_abort_cannot_end(&tid, &txn.affected_cells) {
            return self.response_with(AbortResult::CheckFailed(CheckError::CannotEnd));
        }

        if txn.history.is_empty() {
            let guards_to_drop = std::mem::take(&mut txn.rollback_guards);
            txn.last_activity = get_time();
            txn.compensation_output_durable = true;
            txn.state = TxnState::Aborted;
            drop(txn);
            drop(guards_to_drop);
            return self.response_with(AbortResult::Success(None));
        }

        let Some(coordinator_id) = txn.coordinator_id else {
            return self.response_with(AbortResult::CheckFailed(CheckError::CannotEnd));
        };
        let expected_owner = TxnPriority::new(tid.clone(), coordinator_id);
        let guarded_cell_ids = Self::guarded_txn_cell_ids(&txn);
        let mut cell_mutexes = Vec::with_capacity(guarded_cell_ids.len());
        for cell_id in &guarded_cell_ids {
            cell_mutexes.push(self.cell_meta_mutex(cell_id));
        }
        let mut cell_guards = Vec::with_capacity(cell_mutexes.len());
        for cell_mutex in &cell_mutexes {
            cell_guards.push(cell_mutex.lock());
        }
        if cell_guards
            .iter()
            .any(|meta| meta.owner.as_ref() != Some(&expected_owner))
        {
            return self.response_with(AbortResult::CheckFailed(CheckError::CannotEnd));
        }

        let rollback_failures = {
            debug!(
                ">>>>>>>>>> ROLLING BACK FOR {:?} CELLS {:?}",
                txn.history.len(),
                tid
            );
            let Transaction {
                history, installed, ..
            } = &mut *txn;
            let failures = self.rollback(history, installed);
            if failures.len() == 0 {
                None
            } else {
                Some(failures)
            }
        };
        if let Some(failures) = rollback_failures {
            for failure in failures {
                error!(
                    "Compensation for cell {:?} in transaction {:?} failed: {:?}",
                    failure.id, tid, failure.error
                );
            }
            txn.last_activity = get_time();
            return self.response_with(AbortResult::CheckFailed(CheckError::CannotEnd));
        }

        // Keep the old immutable locations protected until every compensation
        // is complete. A failed attempt retains both these guards and the cell
        // owners so a later abort call can resume an already-aborted installed
        // node without duplicating completed compensation.
        let guards_to_drop = std::mem::take(&mut txn.rollback_guards);
        txn.last_activity = get_time();
        txn.compensation_output_durable = true;
        txn.state = TxnState::Aborted;

        drop(cell_guards);
        drop(txn);
        drop(guards_to_drop);

        self.response_with(AbortResult::Success(None))
    }
    fn end(&self, clock: Hlc, tid: TxnId) -> BoxFuture<'_, DataSiteResponse<EndResult>> {
        debug!(">> END {:?}", tid);
        #[cfg(feature = "occ_phase_profile")]
        let phase_guard = super::phase_profile::guard(super::phase_profile::Phase::ParticipantEnd);
        self.update_clock(clock);

        let result = 'end: {
            let Some(txn_lock) = self.find_transaction(&tid) else {
                let completion = match self.undo_log() {
                    Some(undo_log) => undo_log.participant_completion(&tid),
                    None => Ok(None),
                };
                return self.response_with(match completion {
                    Ok(Some(TxnState::Committed | TxnState::Aborted)) => EndResult::Success,
                    Ok(_) => EndResult::CheckFailed(CheckError::NotExisted),
                    Err(error) => {
                        error!(
                            "Failed to read durable participant completion for transaction {:?}: {:?}",
                            tid, error
                        );
                        EndResult::CheckFailed(CheckError::CannotEnd)
                    }
                });
            };
            let mut txn = txn_lock.lock();
            if !(txn.state == TxnState::Aborted || txn.state == TxnState::Committed) {
                break 'end EndResult::CheckFailed(CheckError::CannotEnd);
            }

            let guarded_cell_ids = Self::guarded_txn_cell_ids(&txn);
            let expected_owner = if guarded_cell_ids.is_empty() {
                None
            } else {
                let Some(coordinator_id) = txn.coordinator_id else {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                };
                Some(TxnPriority::new(tid, coordinator_id))
            };
            let mut cell_mutexes = Vec::with_capacity(guarded_cell_ids.len());
            for cell_id in &guarded_cell_ids {
                let Some(cell_mutex) = self.cells.get(cell_id) else {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                };
                cell_mutexes.push(cell_mutex);
            }
            let mut cell_guards: Vec<_> = cell_mutexes.iter().map(|cell| cell.lock()).collect();
            if let Some(expected_owner) = expected_owner.as_ref() {
                if cell_guards
                    .iter()
                    .any(|meta| meta.owner.as_ref() != Some(expected_owner))
                {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
            }

            if txn.state == TxnState::Committed {
                if !txn.installed.is_empty() && !txn.installed_output_durable {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
                if !self.installed_revisions_agree(&txn) {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
                if txn.installed.values().any(|installed| {
                    !matches!(
                        installed.node.load().0,
                        RevisionState::PendingPresent
                            | RevisionState::PendingDeleted
                            | RevisionState::CommittedPresent
                            | RevisionState::CommittedDeleted
                    )
                }) {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
            }
            if txn.state == TxnState::Aborted {
                if !txn.history.is_empty() && !txn.compensation_output_durable {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
                if txn
                    .history
                    .values()
                    .any(|history| history.compensation.is_none())
                {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
            }

            if txn.durable_decision.is_none() {
                if let Some(undo_log) = self.undo_log() {
                    let log_result = match txn.state {
                        TxnState::Committed => undo_log.write_commit_marker(&tid),
                        TxnState::Aborted => undo_log.write_abort_marker(&tid),
                        _ => Ok(()),
                    };
                    if let Err(error) = log_result {
                        error!("Failed to write transaction completion marker: {:?}", error);
                        break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                    }
                    txn.durable_decision = Some(txn.state);
                }
            }

            if txn.state == TxnState::Committed {
                let mut promoted = Vec::new();
                let mut promotion_failed = false;
                for installed in txn.installed.values() {
                    match installed.node.load().0 {
                        state @ (RevisionState::PendingPresent | RevisionState::PendingDeleted) => {
                            #[cfg(test)]
                            if should_fail_end_promotion(&tid, &installed.id) {
                                promotion_failed = true;
                                break;
                            }
                            if self.chunks().promote_revision(installed).is_err() {
                                promotion_failed = true;
                                break;
                            }
                            let committed = match state {
                                RevisionState::PendingPresent => RevisionState::CommittedPresent,
                                RevisionState::PendingDeleted => RevisionState::CommittedDeleted,
                                _ => unreachable!(),
                            };
                            promoted.push((installed.node.clone(), state, committed));
                        }
                        RevisionState::CommittedPresent | RevisionState::CommittedDeleted => {}
                        RevisionState::Aborted | RevisionState::Expired => unreachable!(
                            "installed revision states were prevalidated before promotion"
                        ),
                    }
                }
                if promotion_failed || !self.installed_revisions_agree(&txn) {
                    for (node, pending, committed) in promoted.into_iter().rev() {
                        if !node.compare_exchange_state(committed, pending) {
                            error!("Could not restore pending promotion barrier for {:?}", tid);
                        }
                    }
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
            }

            debug!(
                "AFFECTED: {}, {:?}, {:?}",
                guarded_cell_ids.len(),
                txn.state,
                tid
            );

            for meta in &mut cell_guards {
                meta.owner = None;
                meta.lock_acquired_at = None;
            }

            let guards_to_drop = std::mem::take(&mut txn.rollback_guards);
            self.wipe_out_transaction(&tid);
            drop(txn);
            drop(guards_to_drop);
            self.cleanup_signal.store(true, Relaxed);
            debug!("ENDED: {:?} after atomic owner barrier", tid);
            EndResult::Success
        };
        async move {
            #[cfg(feature = "occ_phase_profile")]
            let _phase_guard = phase_guard;
            self.response_with(result).await
        }
        .boxed()
    }
}
