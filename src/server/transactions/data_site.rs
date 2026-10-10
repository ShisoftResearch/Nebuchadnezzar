use super::*;
use crate::ram::cell::OwnedCellRef;
use crate::ram::segs::SegmentReferenceGuard;
use crate::ram::types::Id;
use crate::server::DatabaseRuntime;
use crate::{
    index::builder::IndexBuilder,
    ram::cell::{CellHeader, OwnedCell, ReadError, WriteError},
};
use bifrost::hlc::Hlc;
use bifrost::utils::time::get_time;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use futures::future::BoxFuture;
use lightning::linked_list::LinkedList;
use lightning::map::Map;
use lightning::map::PtrHashMap as LFMap;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
#[cfg(test)]
use std::sync::OnceLock;
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

// Lock timeout in milliseconds - locks held longer than this are considered stale
// and can be reclaimed (default: 30 seconds)
const LOCK_TIMEOUT_MS: i64 = 30_000;

// Maximum retries for lock release (Option 6: Two-Phase Lock Release)
const MAX_LOCK_RELEASE_RETRIES: usize = 3;

// Backoff between lock release retries in milliseconds
const LOCK_RELEASE_RETRY_BACKOFF_MS: u64 = 100;

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

/// Test hook: panic inside the apply burst right after this cell is written,
/// to drive the abort-on-unwind path through the real write loop.
#[cfg(test)]
static APPLY_PANIC_HOOKS: OnceLock<Mutex<BTreeSet<(TxnId, Id)>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn install_apply_panic_after_cell(tid: TxnId, id: Id) {
    APPLY_PANIC_HOOKS
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
        .insert((tid, id));
}

#[cfg(test)]
fn take_apply_panic_hook(tid: &TxnId, id: &Id) -> bool {
    APPLY_PANIC_HOOKS
        .get()
        .is_some_and(|hooks| hooks.lock().remove(&(tid.clone(), *id)))
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

type CommitHistory = BTreeMap<Id, CellHistory>;
type CellMetaMutex = Arc<Mutex<CellMeta>>;
type TxnMutex = Arc<Mutex<Transaction>>;

/// Per-cell metadata for concurrency control
///
/// Implements a hybrid timestamp-ordering + lock-based protocol with Wait-Die:
/// - `read` / `write`: Track timestamps for timestamp-ordering validation
/// - `owner`: Acts as a write lock during prepare/commit phases
/// - `lock_acquired_at`: Timestamp when lock was acquired (for timeout detection)
///
/// Wait-Die Protocol:
/// - When a transaction wants to acquire a cell already owned by another:
///   - If requester is YOUNGER (higher timestamp): DIE (abort immediately)
///   - If requester is OLDER (lower timestamp): WAIT (backoff and retry)
/// - This prevents deadlock while reducing contention on hot cells
/// - Waiters poll via backoff rather than blocking on a condition variable
///
/// Lock Timeout:
/// - Locks are considered stale after LOCK_TIMEOUT_MS milliseconds
/// - Stale locks can be reclaimed by any transaction (logged as warning)
#[derive(Debug)]
pub struct CellMeta {
    read: TxnId,
    write: TxnId,
    owner: Option<TxnPriority>, // transaction that owns the cell during prepare/commit
    lock_acquired_at: Option<i64>, // timestamp when lock was acquired (milliseconds since epoch)
}

/// A single pinned read: the location of a specific version of a cell within
/// its segment, together with the version number that was pinned.
#[derive(Clone, Copy, Debug)]
struct PinnedRead {
    location: usize,
    version: u64,
}

/// Per-transaction record of pinned reads for large cells. When a transaction
/// reads a LARGE cell under repeatable-read semantics, the participant pins
/// that specific version (holding a segment reference guard) and remembers
/// its location here, instead of the coordinator cloning the whole cell.
///
/// The guard is `Option` so a pin can be recorded (e.g. in tests) without
/// necessarily holding a live segment reference.
#[derive(Default)]
struct PinnedReadSet {
    entries: HashMap<Id, (PinnedRead, Option<SegmentReferenceGuard>)>,
}

impl PinnedReadSet {
    fn insert(&mut self, id: Id, pin: PinnedRead, guard: Option<SegmentReferenceGuard>) {
        self.entries.insert(id, (pin, guard));
    }

    fn get(&self, id: &Id) -> Option<&PinnedRead> {
        self.entries.get(id).map(|(pin, _)| pin)
    }

    /// Empties the set, dropping any held segment guards.
    fn drain(&mut self) {
        self.entries.clear();
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// How long a transaction may hold its write heads without a decision.
///
/// Deliberately generous and deliberately a knob: this is the in-doubt
/// window, and its right value is coupled to the distributed termination
/// protocol that is still design-only. Too short steals heads from
/// transactions that are merely slow; too long is a writer slot held hostage
/// by a coordinator that is never coming back.
/// Cooperative termination outcomes, so a soak can tell "nothing was in
/// doubt" from "everything in doubt was presumed abort".
pub static TERMINATION_COMMITTED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static TERMINATION_ABORTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How long an in-doubt transaction keeps its write heads while a
/// participant stays silent, before this node gives up and presumes abort.
fn termination_answer_grace() -> std::time::Duration {
    std::env::var("NEB_TERMINATION_ANSWER_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(30))
}

fn txn_lease_timeout() -> std::time::Duration {
    std::env::var("NEB_TXN_LEASE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(120))
}

struct Transaction {
    state: TxnState,
    affected_cells: Vec<Id>,
    certified: BTreeMap<Id, PrepareOp>,
    coordinator_id: Option<u64>,
    last_activity: i64,
    history: CommitHistory,
    /// RAII guards that hold segment references during this transaction
    /// Automatically released when guards are dropped (no leak risk)
    segment_guards: Vec<SegmentReferenceGuard>,
    /// Pinned versions of LARGE cells read under repeatable-read semantics.
    pinned_reads: PinnedReadSet,
    /// Every server that will hold a bracket for this transaction, learned
    /// from the coordinator at prepare.
    ///
    /// IN MEMORY ONLY, and deliberately so. A participant that survives to be
    /// in doubt still has this and can ask its peers directly; one that
    /// restarts has lost it and must fall back to asking every member of the
    /// cluster. Persisting it would mean a second durable write in the
    /// prepare path -- paid by every transaction, to serve a case that is
    /// already covered by the broadcast.
    participants: Vec<u64>,
    /// This site was told to COMMIT and has started writing COMMIT records.
    ///
    /// From here the transaction can only be finished, never aborted: one
    /// durable COMMIT anywhere decides the transaction at recovery, so a
    /// presumed abort that rolled it back and padded over its spans would
    /// tear it. Set under the transaction lock before the first record is
    /// written, and checked by every abort path under the same lock.
    decided_commit: bool,
}

#[derive(Debug)]
struct CellHistory {
    cell: Option<OwnedCellRef>,
    current_version: u64,
}

impl CellHistory {
    pub fn new(cell: Option<OwnedCellRef>, current_ver: u64) -> CellHistory {
        CellHistory {
            cell,
            current_version: current_ver,
        }
    }
}

impl Transaction {
    fn certified_op(&self, id: &Id) -> Option<&PrepareOp> {
        self.certified.get(id)
    }

    fn certified_present_version(&self, id: &Id) -> Option<u64> {
        match self.certified_op(id).map(|op| &op.expectation) {
            Some(CellExpectation::Present(version)) => Some(*version),
            _ => None,
        }
    }

    fn certified_expects_absent_write(&self, id: &Id) -> bool {
        matches!(
            self.certified_op(id),
            Some(PrepareOp {
                intent: PrepareIntent::Write,
                expectation: CellExpectation::Absent,
                ..
            })
        )
    }
}

pub struct DataManager {
    cells: parking_lot::RwLock<HashMap<Id, CellMetaMutex>>,
    txns: LFMap<TxnId, Arc<Mutex<Transaction>>>,
    cell_list: LinkedList<Id>,
    txns_sorted: Mutex<BTreeSet<TxnId>>,
    database_runtime: Arc<DatabaseRuntime>,
    cleanup_signal: Arc<AtomicBool>,
    /// Per-server Hybrid Logical Clock source (node = server_id), shared with
    /// the coordinator-side `TransactionManager`. Stamps every participant
    /// response clock and observes the coordinator's incoming clock.
    hlc: Arc<bifrost::hlc::HlcSource>,
    /// What this site decided about transactions it has already finished.
    ///
    /// The point of keeping it after the transaction object is gone: a peer
    /// left in doubt asks "what happened to T", and the honest answer lives
    /// here. Without it every finished transaction reads as `Unknown`, which
    /// is presume-abort -- exactly the answer that tears a transaction whose
    /// other half committed.
    ///
    /// Bounded and ordered. `TxnId` is an HLC, so oldest-first eviction is
    /// just the front of the map, and the retention that matters is measured
    /// against how long a coordinator can be gone before the lease sweeper
    /// gives up -- seconds, against tens of thousands of entries.
    decided: Mutex<BTreeMap<TxnId, TxnOutcome>>,
    /// When each in-doubt transaction was first found unresolvable because a
    /// participant would not answer. Bounds how long its write heads stay
    /// held: a hostage head is a writer slot gone from its chunk with no
    /// refill path, so waiting forever trades one wrong answer for a
    /// different one.
    in_doubt_since: Mutex<BTreeMap<TxnId, std::time::Instant>>,
}

/// How many finished transactions a site remembers the fate of. Sized to
/// outlast the in-doubt window by orders of magnitude rather than tuned.
const DECISION_MEMORY: usize = 64 * 1024;

service! {
    rpc read(server_id: u64, clock: Hlc, tid: TxnId, id: Id) -> DataSiteResponse<TxnExecResult<OwnedCell, ReadError>>;
    rpc read_selected(server_id: u64, clock: Hlc, tid: TxnId, id: Id, fields: Vec<u64>) -> DataSiteResponse<TxnExecResult<OwnedCell, ReadError>>;
    rpc read_partial_raw(server_id: u64, clock: Hlc, tid: TxnId, id: Id, offset: usize, len: usize) -> DataSiteResponse<TxnExecResult<Vec<u8>, ReadError>>;
    rpc head(server_id: u64, clock: Hlc, tid: TxnId, id: Id, pin: bool) -> DataSiteResponse<TxnExecResult<CellHeader, ReadError>>;
    // Release any read pins held for a transaction (partial reads pin versions).
    // Best-effort and idempotent: called by the coordinator on commit/abort so a
    // transaction that merely pinned reads on a server does not linger there.
    rpc release_read_pins(tids: Vec<TxnId>) -> DataSiteResponse<()>;
    // two phase commit
    rpc prepare(coordinator_id: u64, clock: Hlc, tid: TxnId, ops: Vec<PrepareOp>, participants: Vec<u64>) -> DataSiteResponse<DMPrepareResult>;
    rpc commit(clock: Hlc, tid: TxnId, cells: Vec<CommitOp>) -> DataSiteResponse<DMCommitResult>;

    // because there may be some exception on commit, abort have to handle 'committed' and 'committing' transactions
    // for committed transaction, abort need to recover the data according to it's cells history
    rpc abort(clock: Hlc, tid: TxnId) -> DataSiteResponse<AbortResult>;

    // there also should be a 'end' from transaction manager to inform data manager to clean up and release cell locks
    rpc end(clock: Hlc, tid: TxnId) -> DataSiteResponse<EndResult>;

    // Cooperative termination: what does this site know about T's fate?
    //
    // Answered from this site's own decision record, never from a
    // coordinator. That is the whole point -- the coordinator is the thing
    // that went away.
    rpc txn_status(tid: TxnId) -> DataSiteResponse<TxnOutcome>;
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
            cells: parking_lot::RwLock::new(HashMap::with_capacity(256)),
            txns: LFMap::with_capacity(128),
            cell_list: LinkedList::new(),
            txns_sorted: Mutex::new(BTreeSet::new()),
            database_runtime,
            cleanup_signal: cleanup_signal.clone(),
            decided: Mutex::new(BTreeMap::new()),
            in_doubt_since: Mutex::new(BTreeMap::new()),
            hlc,
        });
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            loop {
                if cleanup_signal.load(Relaxed) {
                    manager_clone.cell_meta_cleanup().await;
                    cleanup_signal.store(false, Relaxed);
                }
                manager_clone
                    .sweep_idle_transactions(txn_lease_timeout(), termination_answer_grace())
                    .await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

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
                last_activity: get_time(),
                history: BTreeMap::new(),
                segment_guards: Vec::with_capacity(4), // Pre-allocate for common case
                pinned_reads: PinnedReadSet::default(),
                participants: Vec::new(),
                decided_commit: false,
            }));

            if self.txns.insert(tid.clone(), txn.clone()).is_none() {
                self.txns_sorted.lock().insert(tid.clone());
                return txn;
            }
        }
    }
    /// Prepared transactions that carry at least one write. A fencer drains
    /// these before it reports the fence engaged, because each can still commit.
    pub fn prepared_writer_count(&self) -> usize {
        let tids: Vec<TxnId> = self.txns_sorted.lock().iter().cloned().collect();
        tids.iter()
            .filter(|tid| {
                self.find_transaction(tid).is_some_and(|txn| {
                    let txn = txn.lock();
                    txn.state == TxnState::Prepared
                        && txn
                            .certified
                            .values()
                            .any(|op| op.intent == PrepareIntent::Write)
                })
            })
            .count()
    }

    /// Whether preparing `ops` would write a cell the engaged fence protects.
    ///
    /// A prepare names cells by id and carries no schema, so the family is read
    /// off the stored cell. A write to a cell that does not exist yet (an
    /// insert) has no stored header to read, and is treated as protected: an
    /// insert is exactly what a dump must not miss, and refusing a derived
    /// transactional insert for the length of a dump is the cheaper error.
    fn prepare_writes_fenced_cell(&self, ops: &[PrepareOp]) -> bool {
        let fence = self.database_runtime.write_fence();
        ops.iter()
            .filter(|op| op.intent == PrepareIntent::Write)
            .any(|op| match op.expectation {
                CellExpectation::Absent => true,
                CellExpectation::Present(_) => {
                    // A cell that cannot be examined (it vanished since the
                    // transaction read it, or its schema is unknown here) is
                    // refused like an insert: it would fail certification
                    // anyway, and "nothing to protect" is a check-then-act race.
                    match self
                        .chunks()
                        .head_cell(&op.id)
                        .ok()
                        .and_then(|header| self.database_runtime.schemas().uid_of_vid(&header.schema))
                    {
                        Some(family) => fence.would_refuse(Some(family)),
                        None => true,
                    }
                }
            })
    }

    fn existing_cell_meta_mutex(&self, id: &Id) -> Option<CellMetaMutex> {
        self.cells.read().get(id).cloned()
    }

    fn cell_meta_mutex(&self, id: &Id) -> CellMetaMutex {
        if let Some(meta) = self.existing_cell_meta_mutex(id) {
            return meta;
        }
        let mut cells = self.cells.write();
        cells
            .entry(*id)
            .or_insert_with(|| {
                self.cell_list.push_back(*id);
                Arc::new(Mutex::new(CellMeta {
                    read: TxnId::default(),
                    write: TxnId::default(),
                    owner: None,
                    lock_acquired_at: None,
                }))
            })
            .clone()
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

    fn prepare_expectation_matches(&self, op: &PrepareOp) -> bool {
        match (&op.expectation, self.chunks().head_cell(&op.id)) {
            (CellExpectation::Present(expected_version), Ok(head)) => {
                head.version == *expected_version
            }
            (CellExpectation::Absent, Err(ReadError::CellDoesNotExisted)) => true,
            _ => false,
        }
    }

    #[inline]
    fn chunks(&self) -> &Arc<crate::ram::chunk::Chunks> {
        self.database_runtime.chunks()
    }

    #[inline]

    /// Create a segment reference guard to prevent eviction during transaction.
    /// Returns None if the segment is not found (already freed/evicted) or is
    /// held exclusively by an evictor, promoter or the cleaner -- in which case
    /// its pages are about to go and a transaction must not pin a read there.
    /// The guard is RAII - reference is automatically released when dropped.
    #[inline]
    fn acquire_segment_guard(
        &self,
        chunk_idx: usize,
        segment_id: u64,
    ) -> Option<SegmentReferenceGuard> {
        if let Some(chunk) = self.chunks().list.get(chunk_idx) {
            if let Some(segment) = chunk.segs.get(&(segment_id as usize)) {
                return SegmentReferenceGuard::new(segment);
            }
        }
        warn!(
            "Could not find segment {} in chunk {} to acquire guard",
            segment_id, chunk_idx
        );
        None
    }
    fn rollback(&self, history: &CommitHistory) -> Vec<RollbackFailure> {
        let mut failures = Vec::new();
        for (id, history) in history.iter() {
            debug!("ROLLING BACK {:?} - {:?}", id, history);
            let cell = &history.cell;
            let current_ver = history.current_version;
            let error = if cell.is_none() {
                // the cell was created, need to remove
                self.chunks()
                    .remove_cell_by(id, |cell| cell.header.version == current_ver)
                    .err()
            } else if current_ver > 0 {
                // the cell was updated, need to update back
                self.chunks()
                    .update_cell_by(id, |cell_to_update| {
                        if cell_to_update.header.version == current_ver {
                            cell.as_ref().map(|r| r.clone_referred())
                        } else {
                            None
                        }
                    })
                    .err()
            } else {
                // the cell was removed, need to put back
                let mut cell = cell.as_ref().unwrap().clone_referred();
                self.chunks().write_cell(&mut cell).err()
            };
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
        // Give back the write heads this transaction leased, whatever ended
        // it. One site rather than one per outcome: a head that outlives its
        // transaction is a writer slot removed from its chunk's pool with no
        // refill path, so the release has to be impossible to forget on some
        // path nobody thought about.
        //
        // RETIRE, not a plain release: by now a commit has closed every
        // bracket and an abort has rewound every span, so normally there is
        // nothing left to do but hand the heads back. Anything still open is
        // a path that ended the transaction without deciding it physically,
        // and the one thing that must not happen to such a head is reaching
        // another writer -- so it is filled instead, and its bracket left to
        // recovery's discard.
        if let Err(error) =
            crate::ram::chunk::retire_transaction_leases(tid, &self.chunks().address_range())
        {
            error!(
                "transaction {:?} did not give its write heads back cleanly: {}",
                tid, error
            );
        }
        // Give back the segment references the transaction holds -- its read
        // pins and its write guards -- NOW, and do not wait for the record to
        // be freed. `txns` is a lock-free map: `remove` hands back a clone and
        // the map frees the original later, at its own reclamation point, so
        // the record (and every reference inside it) can outlive the
        // transaction by an unbounded time. A segment holding a reference is
        // never a combine candidate, so each such leak keeps a segment of
        // dead data out of the cleaner's reach for good, until the chunk is
        // full.
        if let Some(txn_lock) = self.txns.get(tid) {
            let (guards, pins) = {
                let mut txn = txn_lock.lock();
                (
                    std::mem::take(&mut txn.segment_guards),
                    std::mem::take(&mut txn.pinned_reads),
                )
            };
            drop(guards);
            drop(pins);
        }
        let _ = self.txns.remove(tid);
        self.txns_sorted.lock().remove(tid);
    }

    /// Undo an applied transaction: restore the cell index from its history,
    /// then rewind its spans, then let its heads go. The caller holds the
    /// transaction lock and the cells' meta locks.
    ///
    /// In THAT order, and the order is the fix. The heads stay leased through
    /// the rollback, so nothing can be appended behind the transaction's open
    /// BEGIN while it runs; the rollback's own writes are ordinary and go to
    /// other heads, which a lease can never all take
    /// (`can_spare_a_head_for_lease`). Only once the index names nothing in
    /// the spans are they padded over, journaled, and only then are the heads
    /// released -- with no bracket left open in them.
    ///
    /// A rollback that failed for any cell may leave the index naming that
    /// cell's entry inside the span, and padding over an indexed entry would
    /// leave the index pointing into space the cleaner then frees. So the
    /// spans are left as uncommitted brackets in RETIRED heads instead:
    /// recovery discards them, and no writer appends behind them.
    fn rollback_and_rewind(
        &self,
        tid: &TxnId,
        txn: &mut Transaction,
    ) -> Option<Vec<RollbackFailure>> {
        debug!(
            ">>>>>>>>>> ROLLING BACK FOR {:?} CELLS {:?}",
            txn.history.len(),
            tid
        );
        #[cfg(feature = "abort_profile")]
        let rollback_timer = crate::ram::abort_profile::guard(crate::ram::abort_profile::Phase::Rollback);
        let failures = self.rollback(&txn.history);
        #[cfg(feature = "abort_profile")]
        drop(rollback_timer);
        let failures = (!failures.is_empty()).then_some(failures);
        txn.last_activity = get_time();
        txn.state = TxnState::Aborted;
        let store = self.chunks().address_range();
        #[cfg(feature = "abort_profile")]
        let _rewind_timer = crate::ram::abort_profile::guard(crate::ram::abort_profile::Phase::Rewind);
        let ended = match failures {
            None => crate::ram::chunk::rewind_transaction_writes(tid, &store),
            Some(_) => crate::ram::chunk::retire_transaction_leases(tid, &store),
        };
        if let Err(error) = ended {
            error!(
                "transaction {:?} aborted but its write heads did not end cleanly: {}",
                tid, error
            );
        }
        failures
    }

    /// The abort decision, with the transaction lock held.
    fn abort_locked(&self, tid: &TxnId, txn: &mut Transaction) -> AbortResult {
        if txn.state == TxnState::Aborted {
            return AbortResult::CheckFailed(CheckError::AlreadyAborted);
        }
        if txn.decided_commit {
            // Told to commit, and possibly already durable as committed: an
            // abort here would tear the transaction, whoever asks for it.
            return AbortResult::CheckFailed(CheckError::AlreadyCommitted);
        }
        #[cfg(test)]
        if Self::take_matching_abort_cannot_end(tid, &txn.affected_cells) {
            return AbortResult::CheckFailed(CheckError::CannotEnd);
        }

        if txn.history.is_empty() {
            let guards_to_drop = std::mem::take(&mut txn.segment_guards);
            txn.last_activity = get_time();
            txn.state = TxnState::Aborted;
            // Nothing was applied, but a bracket may still have been OPENED:
            // a write that failed after its BEGIN landed leaves a BEGIN-only
            // span, and its head must not go back to the pool with it open.
            // With nothing in the history, nothing in the index names the
            // span, so it is rewound like any other.
            if let Err(error) =
                crate::ram::chunk::rewind_transaction_writes(tid, &self.chunks().address_range())
            {
                error!(
                    "transaction {:?} aborted with nothing applied, but its write heads did not \
                     end cleanly: {}",
                    tid, error
                );
            }
            drop(guards_to_drop);
            return AbortResult::Success(None);
        }

        let Some(coordinator_id) = txn.coordinator_id else {
            return AbortResult::CheckFailed(CheckError::CannotEnd);
        };
        let expected_owner = TxnPriority::new(tid.clone(), coordinator_id);
        let guarded_cell_ids = Self::guarded_txn_cell_ids(txn);
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
            return AbortResult::CheckFailed(CheckError::CannotEnd);
        }

        // Keep segment references and metadata guards alive throughout rollback so the owner
        // cannot be reclaimed between validation and the storage mutations.
        let guards_to_drop = std::mem::take(&mut txn.segment_guards);
        let rollback_failures = self.rollback_and_rewind(tid, txn);
        drop(cell_guards);
        drop(guards_to_drop);
        AbortResult::Success(rollback_failures)
    }

    /// One pass of the participant's lease sweeper.
    ///
    /// A transaction holding write heads past `idle` has lost its
    /// coordinator, or is about to be told something late. In order:
    ///
    /// 1. Ask the other participants (cooperative termination) and finish
    ///    the ones a peer reports committed. Runs first because closing a
    ///    bracket needs the leases every later step gives up.
    /// 2. Presume abort for the rest this site knows: the same rollback,
    ///    rewind and release an explicit abort performs, under the same
    ///    transaction lock -- which is also what makes a slow owner safe. An
    ///    apply still running holds that lock, so the sweep waits for it
    ///    rather than padding over a span that is still growing; and a late
    ///    `end` finds the transaction gone and is refused, rather than told
    ///    it committed something whose spans were just disowned.
    /// 3. Retire whatever is left: leases no participant object knows about.
    pub(crate) async fn sweep_idle_transactions(
        &self,
        idle: std::time::Duration,
        grace: std::time::Duration,
    ) {
        let hold = self.terminate_in_doubt_transactions(idle, grace).await;
        let store = self.chunks().address_range();
        for tid in crate::ram::chunk::transactions_with_idle_leases(idle, &store) {
            if hold.contains(&tid) {
                continue;
            }
            self.presume_abort(&tid);
        }
        for tid in crate::ram::chunk::expire_transaction_leases(idle, &store, &hold) {
            self.wipe_out_transaction(&tid);
        }
    }

    /// Presumed abort for a transaction whose coordinator went silent.
    /// Returns whether the transaction was ended here.
    fn presume_abort(&self, tid: &TxnId) -> bool {
        let Some(txn_lock) = self.find_transaction(tid) else {
            // Nothing to roll back here; the retire pass reclaims the heads.
            return false;
        };
        let mut txn = txn_lock.lock();
        if txn.decided_commit {
            // Finishing it is the only option; termination retries the
            // close, and the heads stay held until it succeeds.
            return false;
        }
        let outcome = self.abort_locked(tid, &mut txn);
        drop(txn);
        match outcome {
            AbortResult::Success(None) | AbortResult::CheckFailed(CheckError::AlreadyAborted) => {
                warn!(
                    "transaction {:?} held its write heads past the lease timeout without a \
                     decision; presumed ABORT, rolled back and rewound",
                    tid
                );
            }
            other => {
                // Could not roll back (ownership moved, no coordinator, or a
                // rollback failure). The wipe below retires whatever is still
                // open, so no head leaves with its bracket open either way.
                error!(
                    "transaction {:?} held its write heads past the lease timeout; its presumed \
                     abort ended with {:?}",
                    tid, other
                );
            }
        }
        self.wipe_out_transaction(tid);
        true
    }
    /// The commit decision's physical half: every bracket gets its COMMIT,
    /// fsynced, and only then do the heads go back to the pool.
    ///
    /// Under the transaction lock, and re-checked there, because the decision
    /// can race the lease sweeper: a sweep that already presumed abort has
    /// rolled the transaction back and padded over its spans, and a COMMIT
    /// written after that would commit nothing -- while a ram layer that finds
    /// no leases answers `Ok`. So a transaction that is gone, or no longer in
    /// the applied-and-undecided state, is refused here and the coordinator
    /// is not told it committed. `decided_commit` is set before the first
    /// record is written, which from then on keeps every abort path away.
    fn close_committed_brackets(&self, tid: &TxnId) -> Result<(), String> {
        let Some(txn_lock) = self.find_transaction(tid) else {
            return Err("the transaction was ended here before its decision arrived".into());
        };
        let mut txn = txn_lock.lock();
        if txn.state != TxnState::Committed {
            return Err(format!(
                "the transaction is {:?} here, not applied-and-undecided",
                txn.state
            ));
        }
        txn.decided_commit = true;
        let store = self.chunks().address_range();
        self.chunks()
            .commit_transaction_brackets(tid)
            .map_err(|error| format!("{error:?}"))?;
        if let Err(error) = crate::ram::chunk::release_transaction_leases(tid, &store) {
            // Every bracket carries its COMMIT, so this is unreachable; if it
            // is reached, the final wipe retires the head rather than hand it
            // over open.
            error!(
                "transaction {:?} committed but its heads could not be released: {}",
                tid, error
            );
        }
        drop(txn);
        Ok(())
    }

    /// Cooperative termination, participant side.
    ///
    /// A participant whose coordinator went silent knows its entries are
    /// durable and knows nothing else. Presuming abort -- what this site did
    /// unconditionally before -- is safe only if no OTHER participant was
    /// told to commit, and that is precisely what it cannot see on its own.
    ///
    /// So it asks. The participant set came from the coordinator at prepare
    /// and lives in memory; a peer that was told answers `Committed` from its
    /// decision record, and this site then closes its bracket instead of
    /// abandoning it.
    ///
    /// What this does NOT fix: a site that has RESTARTED has lost both the
    /// transaction object and the participant set, so it never gets here.
    /// That case is recovery's, and it broadcasts rather than asks.
    pub(crate) async fn terminate_in_doubt_transactions(
        &self,
        idle: std::time::Duration,
        grace: std::time::Duration,
    ) -> std::collections::BTreeSet<TxnId> {
        let store = self.chunks().address_range();
        let candidates = crate::ram::chunk::transactions_with_idle_leases(idle, &store);
        // Transactions whose leases must NOT be released this round, because
        // the question "did anyone commit this?" is still open. Releasing
        // them would throw away the leases a commit needs.
        let mut hold = std::collections::BTreeSet::new();
        for tid in candidates {
            // The transaction lock is READ and dropped before the network
            // call. Holding it across the await would also make this future
            // non-Send, which is how the first version announced itself.
            let Some((state, participants, decided_commit)) =
                self.find_transaction(&tid).map(|txn_lock| {
                    let txn = txn_lock.lock();
                    (txn.state, txn.participants.clone(), txn.decided_commit)
                })
            else {
                // No transaction object: this site cannot be the one holding
                // a decision open, and the sweeper below reclaims the heads.
                continue;
            };
            if decided_commit {
                // This site was TOLD to commit and could not finish closing
                // its brackets. There is nothing to ask anyone: finish it.
                // Until that succeeds the heads stay held -- presuming abort
                // now would tear a transaction one COMMIT may already decide.
                let ended = <DataManager as Service>::end(self, self.hlc.now(), tid.clone())
                    .await
                    .payload;
                if !matches!(ended, EndResult::Success) {
                    error!(
                        "transaction {:?} was decided COMMIT here and still cannot close its \
                         brackets: {:?}; holding its heads and retrying",
                        tid, ended
                    );
                    hold.insert(tid);
                }
                continue;
            }
            // `Committed` here means "entries durable", not "decided" -- see
            // `TxnOutcome`. Both it and `Prepared` are in doubt.
            if !matches!(state, TxnState::Prepared | TxnState::Committed) {
                continue;
            }
            let (outcome, all_answered) = self.ask_participants(&tid, &participants).await;
            match outcome {
                TxnOutcome::Committed => {
                    warn!(
                        "transaction {:?} was in doubt here and a participant reports it \
                         COMMITTED; committing rather than presuming abort",
                        tid
                    );
                    // Scoped, not just dropped: `Transaction` is not `Send`,
                    // and the async capture analysis keeps a `let ... else`
                    // binding alive across the await that follows even with an
                    // explicit `drop`.
                    let still_here = match self.find_transaction(&tid) {
                        Some(txn_lock) => {
                            txn_lock.lock().state = TxnState::Committed;
                            true
                        }
                        None => false,
                    };
                    if !still_here {
                        error!(
                            "transaction {:?} was resolved COMMITTED by its peers but vanished \
                             from this site before it could be committed",
                            tid
                        );
                        continue;
                    }
                    let ended = <DataManager as Service>::end(self, self.hlc.now(), tid.clone())
                        .await
                        .payload;
                    if !matches!(ended, EndResult::Success) {
                        error!(
                            "transaction {:?} was resolved COMMITTED by its peers but this site \
                             could not end it: {:?}. Its bracket stays open and the next start \
                             will ask again.",
                            tid, ended
                        );
                        continue;
                    }
                    self.in_doubt_since.lock().remove(&tid);
                    TERMINATION_COMMITTED.fetch_add(1, Relaxed);
                }
                _ if !all_answered => {
                    // SILENCE IS NOT A VOTE. A participant that could not be
                    // reached has not said "not committed", so the heads stay
                    // held and this runs again next second -- but not
                    // forever: a hostage head is a writer slot gone from its
                    // chunk with no refill path, so the wait is bounded and
                    // the give-up is loud.
                    let waited = {
                        let mut since = self.in_doubt_since.lock();
                        let first = since
                            .entry(tid.clone())
                            .or_insert_with(std::time::Instant::now);
                        first.elapsed()
                    };
                    if waited < grace {
                        debug!(
                            "transaction {:?} is in doubt and not every participant has answered; \
                             holding its heads ({:?} of {:?})",
                            tid, waited, grace
                        );
                        hold.insert(tid);
                        continue;
                    }
                    error!(
                        "transaction {:?} stayed in doubt for {:?}: not every participant \
                         answered. Presuming abort, which is the only thing this node can do \
                         alone -- if a peer did commit it, this store has lost its half.",
                        tid, waited
                    );
                    self.in_doubt_since.lock().remove(&tid);
                    TERMINATION_ABORTED.fetch_add(1, Relaxed);
                }
                TxnOutcome::Aborted | TxnOutcome::Unknown | TxnOutcome::InDoubt => {
                    // Every participant answered and none had committed it.
                    // Presume abort, which is what happened before -- but now
                    // it is a conclusion rather than an assumption, and the
                    // log says which.
                    warn!(
                        "transaction {:?} was in doubt here and all {} participant(s) report \
                         {:?}; presuming abort",
                        tid,
                        participants.len(),
                        outcome
                    );
                    self.in_doubt_since.lock().remove(&tid);
                    TERMINATION_ABORTED.fetch_add(1, Relaxed);
                }
            }
        }
        // Anything not held this round is no longer in doubt -- decided, or
        // gone because its `end` finally arrived. Without this the map keeps
        // an entry per transaction that was ever waiting on a silent peer,
        // for the life of the process.
        self.in_doubt_since
            .lock()
            .retain(|tid, _| hold.contains(tid));
        hold
    }

    /// Ask every other participant what became of `tid`.
    ///
    /// Resolution rule, in order: one `Committed` decides the transaction, so
    /// the first is returned immediately. Failing that an explicit `Aborted`
    /// is a real decision and is reported as one. Everything else -- peers in
    /// doubt, peers that never heard of it, peers that cannot be reached --
    /// is `Unknown`, which the caller presumes abort on.
    ///
    /// A `Committed` cannot be wrong: only `end` writes a decision record,
    /// and only a coordinator that decided commit sends `end`.
    async fn ask_participants(&self, tid: &TxnId, participants: &[u64]) -> (TxnOutcome, bool) {
        let Some(txn_manager) = self.database_runtime.txn_manager() else {
            return (TxnOutcome::Unknown, true);
        };
        let me = self.database_runtime.rpc.server_id;
        let mut saw_abort = false;
        let peers: Vec<u64> = participants
            .iter()
            .copied()
            .filter(|peer| *peer != me)
            .collect();
        let mut answered = 0usize;
        for peer in peers.iter().copied() {
            let site = match txn_manager.data_site_for(peer).await {
                Ok(site) => site,
                Err(error) => {
                    debug!(
                        "cannot reach participant {} to resolve {:?}: {}",
                        peer, tid, error
                    );
                    continue;
                }
            };
            match site.txn_status(tid.clone()).await {
                Ok(response) => {
                    self.update_clock(response.clock);
                    answered += 1;
                    match response.payload {
                        TxnOutcome::Committed => return (TxnOutcome::Committed, true),
                        TxnOutcome::Aborted => saw_abort = true,
                        TxnOutcome::InDoubt | TxnOutcome::Unknown => {}
                    }
                }
                Err(error) => debug!(
                    "participant {} did not answer about {:?}: {:?}",
                    peer, tid, error
                ),
            }
        }
        let all_answered = answered == peers.len();
        if saw_abort {
            (TxnOutcome::Aborted, all_answered)
        } else {
            (TxnOutcome::Unknown, all_answered)
        }
    }

    /// Remember what this site decided, so a peer in doubt can be told.
    ///
    /// Recorded at `end` and at `abort`, which are the only two places a
    /// decision reaches a participant. NOT at `commit`: that runs in the
    /// prepare phase and makes entries durable without deciding anything, so
    /// recording there would answer "committed" for a transaction that is
    /// about to be aborted.
    fn record_decision(&self, tid: &TxnId, outcome: TxnOutcome) {
        let mut decided = self.decided.lock();
        decided.insert(tid.clone(), outcome);
        while decided.len() > DECISION_MEMORY {
            let Some(oldest) = decided.keys().next().cloned() else {
                break;
            };
            decided.remove(&oldest);
        }
    }

    /// What this site can say about a transaction, for a peer resolving it.
    pub(crate) fn txn_outcome(&self, tid: &TxnId) -> TxnOutcome {
        if let Some(outcome) = self.decided.lock().get(tid) {
            return *outcome;
        }
        // A RESTART empties the live record but not the segments. Without
        // this a site that durably committed answers "never heard of it"
        // about its own committed transaction, and the peer asking reads that
        // as abort -- tearing the transaction the durable COMMIT existed to
        // preserve.
        if self.chunks().committed_at_recovery(tid) {
            return TxnOutcome::Committed;
        }
        // Still live here. Anything short of a decision is in doubt --
        // including `TxnState::Committed`, which only means the entries are
        // durable.
        match self.find_transaction(tid).map(|txn| txn.lock().state) {
            Some(TxnState::Aborted) => TxnOutcome::Aborted,
            Some(_) => TxnOutcome::InDoubt,
            None => TxnOutcome::Unknown,
        }
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
            if let Some(cell_meta) = self.existing_cell_meta_mutex(&cell_id) {
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

        // Reference acquisition and removal share the map lock. A reference
        // borrowed before prepare must stay mapped even before it has an owner.
        for (cell_id_ref, cell_meta) in cells_to_evict {
            let cell_id = cell_id_ref.deref();
            #[cfg(test)]
            {
                let delay = prepare_delay_hooks()
                    .lock()
                    .remove(&(TxnId::default(), cell_id));
                if let Some(delay) = delay {
                    Self::await_prepare_delay(&delay).await;
                }
            }
            let meta = cell_meta.lock();
            let mut cells = self.cells.write();
            let same_meta = cells
                .get(&cell_id)
                .is_some_and(|current| Arc::ptr_eq(current, &cell_meta));
            // These 2 references belong to the map and this cleanup candidate.
            // Any other reference belongs to an operation that can acquire an owner.
            if same_meta
                && Arc::strong_count(&cell_meta) == 2
                && meta.write < oldest_transaction
                && meta.read < oldest_transaction
                && meta.owner.is_none()
            {
                cells.remove(&cell_id);
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
        let committing = meta.owner.is_some();

        // Use the more recent timestamp between the transaction ID and the incoming clock
        // This allows transactions to proceed even if their original timestamp is stale
        // due to clock updates from concurrent transactions
        let effective_ts = clock.max(tid);
        let read_too_late = &meta.write > effective_ts;

        // Check timestamp constraints first
        if read_too_late {
            // Write timestamp is newer - not realizable even if still committing
            // The timestamp constraint won't change after waiting
            warn!(
                "ReadTooLate: Transaction {:?} (effective ts: {:?}) trying to read cell {:?} but write timestamp {:?} is newer. Transaction timestamp is older than cell's write timestamp.",
                tid, effective_ts, id, meta.write
            );
            return Err(self.response_with(TxnExecResult::Rejected));
        }

        if committing {
            // Timestamp is OK but another transaction is still committing
            // Return Wait so caller can retry with backoff
            debug!(
                "-> READ {:?} WAITING for {:?} to finish commit on cell {:?}",
                tid, &meta.owner, id
            );
            return Err(self.response_with(TxnExecResult::Wait));
        }

        // Cell is available, update read timestamp using the effective timestamp
        if &meta.read < effective_ts {
            meta.read = effective_ts.clone()
        }
        return Ok(());
    }

    /// Returns the pinned location for `id` IF this transaction already holds a
    /// pin for it, creating nothing. Used by the full-read path, which must
    /// repeat an already-pinned version (from a prior partial read) but must
    /// never create a pin of its own.
    ///
    /// Must be called AFTER `prepare_read` has succeeded. The transaction lock
    /// is taken briefly and released before returning.
    fn existing_read_pin(&self, tid: &TxnId, id: &Id) -> Option<usize> {
        let txn_lock = self.find_transaction(tid)?;
        let location = txn_lock.lock().pinned_reads.get(id).map(|pin| pin.location);
        location
    }

    /// Shape-gated repeatable-read pinning. Called by the partial-read paths
    /// (`head` with `pin == true`, `read_selected`), which always pin so a later
    /// read in the same transaction repeats the same version.
    ///
    /// Must be called AFTER `prepare_read` has succeeded (so the cell-meta lock
    /// is already released). Returns `Some(location)` when the read should be
    /// served from a specific stored version (a raw segment address), or `None`
    /// when the caller should fall back to a normal current read.
    ///
    /// Behavior:
    /// - If this transaction already pinned the cell, returns that pinned
    ///   location so the read repeats the original version even if the current
    ///   cell has since advanced.
    /// - Otherwise it captures the current version's address, acquires a segment
    ///   reference guard to keep it alive, records the pin on a
    ///   (created-if-needed) participant transaction, and returns that address.
    ///   There is no size gate: partial reads pin regardless of cell size.
    /// - If the cell does not exist or the guard cannot be acquired, returns
    ///   `None` (no pin) so the caller falls back to a normal current read.
    fn ensure_read_pin(&self, tid: &TxnId, id: &Id) -> Option<usize> {
        // Serve an already-pinned cell from its pin (repeatability). The
        // transaction lock is taken briefly and released before returning.
        if let Some(location) = self.existing_read_pin(tid, id) {
            return Some(location);
        }

        // Capture the current version's address and version cheaply: an index
        // lookup plus a header decode. Materializing the cell value here (e.g.
        // via `read_cell`) would parse the whole payload just to learn where it
        // lives, defeating the point of pinning partial reads. The index guard
        // is released inside the call, before we take the transaction lock.
        let (addr, version) = self.chunks().cell_location_and_version(id).ok()?;
        let chunk = self
            .chunks()
            .locate_chunk_by_partition(id.locality() as u64);
        let chunk_idx = chunk.id;
        let (segment_id, _seq_id) = chunk.get_cell_segment_info(addr);

        // Keep the pinned version's segment alive for the transaction's lifetime.
        // If it cannot be acquired (segment freed/evicted), fall back gracefully.
        let guard = self.acquire_segment_guard(chunk_idx, segment_id)?;

        let txn_lock = self.get_or_create_transaction(tid);
        let mut txn = txn_lock.lock();
        // A concurrent read for the same transaction may have pinned first; keep
        // the earliest pin so all reads remain mutually repeatable.
        if let Some(existing) = txn.pinned_reads.get(id).copied() {
            return Some(existing.location);
        }
        txn.pinned_reads.insert(
            *id,
            PinnedRead {
                location: addr,
                version,
            },
            Some(guard),
        );
        Some(addr)
    }

    /// Attempt to release locks for a transaction
    /// Returns (number of locks released, Vec of failures)
    /// Option 5: Ensures metadata isn't cleaned up while locks are held
    /// Option 6: Provides detailed failure information for retry logic
    fn attempt_lock_release(
        &self,
        expected_owner: Option<&TxnPriority>,
        affected_cell_ids: &[Id],
    ) -> (usize, Vec<LockReleaseFailure>) {
        let mut released_count = 0;
        let mut failures = Vec::new();
        let current_time = get_time();

        for cell_id in affected_cell_ids {
            if let Some(cell_mutex) = self.existing_cell_meta_mutex(cell_id) {
                let mut meta = cell_mutex.lock();

                match (expected_owner, &meta.owner) {
                    (Some(expected_owner), Some(owner)) if owner == expected_owner => {
                        // Release the lock
                        let lock_age = meta
                            .lock_acquired_at
                            .map(|acquired| current_time - acquired)
                            .unwrap_or(0);

                        meta.owner = None;
                        meta.lock_acquired_at = None;
                        released_count += 1;

                        debug!(
                            "Released lock on cell {:?} owned by {:?} (held for {}ms)",
                            cell_id, expected_owner, lock_age
                        );
                    }
                    (Some(expected_owner), Some(owner)) => {
                        let reason = format!(
                            "Cell lock owned by different transaction: expected {:?}, found {:?}",
                            expected_owner, owner
                        );
                        warn!(
                            "Cannot release lock on cell {:?} for {:?}: {}",
                            cell_id, expected_owner, reason
                        );
                        failures.push(LockReleaseFailure {
                            cell_id: *cell_id,
                            reason,
                        });
                    }
                    (Some(expected_owner), None) => {
                        debug!(
                            "Lock on cell {:?} not held by {:?} (already released or never acquired)",
                            cell_id, expected_owner
                        );
                        released_count += 1;
                    }
                    (None, Some(owner)) => {
                        let reason = format!(
                            "Missing coordinator id for release; cell still owned by {:?}",
                            owner
                        );
                        warn!(
                            "Cannot release lock on cell {:?} without expected owner: {}",
                            cell_id, reason
                        );
                        failures.push(LockReleaseFailure {
                            cell_id: *cell_id,
                            reason,
                        });
                    }
                    (None, None) => {
                        debug!(
                            "Lock on cell {:?} already released before coordinator was known",
                            cell_id
                        );
                        released_count += 1;
                    }
                }
            } else {
                let reason = "Cell metadata not found (may have been cleaned up)".to_string();
                warn!(
                    "Cannot release lock on cell {:?} for {:?}: {}",
                    cell_id, expected_owner, reason
                );
                failures.push(LockReleaseFailure {
                    cell_id: *cell_id,
                    reason,
                });
            }
        }

        (released_count, failures)
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

    fn apply_commit_ops(
        &self,
        txn_lock: &TxnMutex,
        tid: &TxnId,
        effective_ts: &TxnId,
        cells: Vec<CommitOp>,
    ) -> DMCommitResult {
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
                return DMCommitResult::CheckFailed(CheckError::AlreadyCommitted);
            }
            TxnState::Cleanup => {
                return DMCommitResult::CheckFailed(CheckError::AlreadyCleanup);
            }
            TxnState::Prepared => {}
        };

        if let Err(result) = Self::validate_commit_subset(&txn, &cells) {
            return result;
        }
        if let Err(result) = Self::validate_commit_payload(&txn, &cells) {
            return result;
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

        // Name the transaction, not just "in a transaction": its writes lease
        // a head per chunk and hold it until the decision, so the write path
        // has to know WHOSE lease to write behind.
        // A scope rather than a set/reset pair: an unwind skips a reset, and
        // a thread left naming a dead transaction leases its next ORDINARY
        // write into that transaction's bracket, where recovery discards it.
        let txn_scope = crate::ram::chunk::TransactionScope::enter(tid.clone());
        // A panic between BEGIN and the decision is an ABORT of this
        // transaction, and is carried out as one -- index rolled back, spans
        // rewound, heads released -- before the panic continues. Left to
        // unwind on its own it held the heads with their BEGINs open until a
        // sweeper came, while the index served cells recovery would discard.
        let applied = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut write_error: Option<(Id, WriteError)> = None;
            {
                for cell_op in cells {
                    match cell_op {
                        CommitOp::Read(_, _) => continue,
                        CommitOp::None => {
                            unreachable!("commit payload validation rejects None ops")
                        }
                        CommitOp::Write(mut cell) => {
                            let cell_id = cell.id();
                            let meta_index = guarded_cell_ids
                                .binary_search(&cell_id)
                                .expect("prepared cell metadata must exist");
                            let meta = &mut cell_guards[meta_index];
                            let should_skip = effective_ts < &meta.write;
                            let write_ts = meta.write.clone();

                            if should_skip {
                                debug!(
                                    "Thomas Write Rule: Skipping obsolete write for cell {:?} (effective ts {:?} < write timestamp {:?})",
                                    cell_id, effective_ts, write_ts
                                );
                                continue;
                            }

                            match self.chunks().write_cell(&mut cell) {
                                Ok(header) => {
                                    txn.history
                                        .insert(cell_id, CellHistory::new(None, header.version));
                                    meta.write = effective_ts.clone();
                                    #[cfg(test)]
                                    if take_apply_panic_hook(tid, &cell_id) {
                                        panic!("test hook: apply panics after writing {cell_id:?}");
                                    }
                                }
                                Err(error) => {
                                    write_error = Some((cell_id, error));
                                    break;
                                }
                            };
                        }
                        CommitOp::Remove(ref cell_id) => {
                            let expected_version = txn
                                .certified_present_version(cell_id)
                                .expect("commit payload validation requires present certification");
                            let meta_index = guarded_cell_ids
                                .binary_search(cell_id)
                                .expect("prepared cell metadata must exist");
                            let meta = &mut cell_guards[meta_index];
                            let should_skip = effective_ts < &meta.write;
                            let write_ts = meta.write.clone();

                            if should_skip {
                                debug!(
                                    "Thomas Write Rule: Skipping obsolete write for cell {:?} (effective ts {:?} < write timestamp {:?})",
                                    cell_id, effective_ts, write_ts
                                );
                                continue;
                            }

                            let (cell_addr, old_cell_ref) = {
                                let shared_cell = match self.chunks().read_cell(cell_id) {
                                    Ok(cell) => cell,
                                    Err(read_error) => {
                                        write_error =
                                            Some((*cell_id, WriteError::ReadError(read_error)));
                                        break;
                                    }
                                };
                                if shared_cell.header.version != expected_version {
                                    write_error = Some((*cell_id, WriteError::CellVersionMismatch));
                                    break;
                                }
                                let addr = shared_cell.cell_guard().get_ptr();
                                let cell_ref = shared_cell.to_owned().into_ref();
                                (addr, cell_ref)
                            };
                            let chunk = self
                                .chunks()
                                .locate_chunk_by_partition(cell_id.locality() as u64);
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

                            match self.chunks().remove_cell_by(cell_id, |cell| {
                                cell.header.version == expected_version
                            }) {
                                Ok(()) => {
                                    txn.history
                                        .insert(*cell_id, CellHistory::new(Some(old_cell_ref), 0));
                                    meta.write = effective_ts.clone();
                                    txn.segment_guards.push(guard);
                                }
                                Err(error) => {
                                    write_error = Some((*cell_id, error));
                                    break;
                                }
                            }
                        }
                        CommitOp::Update(mut cell) => {
                            let cell_id = cell.id();
                            let expected_version = txn
                                .certified_present_version(&cell_id)
                                .expect("commit payload validation requires present certification");
                            let meta_index = guarded_cell_ids
                                .binary_search(&cell_id)
                                .expect("prepared cell metadata must exist");
                            let meta = &mut cell_guards[meta_index];
                            let should_skip = effective_ts < &meta.write;
                            let write_ts = meta.write.clone();

                            if should_skip {
                                debug!(
                                    "Thomas Write Rule: Skipping obsolete write for cell {:?} (effective ts {:?} < write timestamp {:?})",
                                    cell_id, effective_ts, write_ts
                                );
                                continue;
                            }

                            let cell_addr = {
                                let shared_cell = match self.chunks().read_cell(&cell_id) {
                                    Ok(cell) => cell,
                                    Err(read_error) => {
                                        write_error =
                                            Some((cell_id, WriteError::ReadError(read_error)));
                                        break;
                                    }
                                };
                                if shared_cell.header.version != expected_version {
                                    write_error = Some((cell_id, WriteError::CellVersionMismatch));
                                    break;
                                }
                                shared_cell.cell_guard().get_ptr()
                            };
                            let chunk = self
                                .chunks()
                                .locate_chunk_by_partition(cell_id.locality() as u64);
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

                            let mut old_cell_ref = None;
                            match self.chunks().update_cell_by(&cell_id, |cell_to_update| {
                                if cell_to_update.header.version == expected_version {
                                    old_cell_ref = Some((*cell_to_update).to_owned().into_ref());
                                    cell.header.version = expected_version;
                                    Some(cell)
                                } else {
                                    None
                                }
                            }) {
                                Ok(updated_cell) => {
                                    debug_assert!(old_cell_ref.is_some());
                                    txn.history.insert(
                                        cell_id,
                                        CellHistory::new(old_cell_ref, updated_cell.header.version),
                                    );
                                    meta.write = effective_ts.clone();
                                    txn.segment_guards.push(guard);
                                }
                                Err(error) => {
                                    write_error = Some((cell_id, error));
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            write_error
        }));
        drop(txn_scope);
        let write_error = match applied {
            Ok(write_error) => write_error,
            Err(panic) => {
                error!(
                    "transaction {:?} panicked while applying its writes; aborting it before \
                     the panic continues",
                    tid
                );
                let _ = self.rollback_and_rewind(tid, &mut txn);
                drop(crate::ram::chunk::take_transactional_segments());
                std::panic::resume_unwind(panic);
            }
        };
        txn.last_activity = get_time();

        if let Some((id, error)) = write_error {
            // Drop what this attempt recorded: the next transaction on this
            // thread must not inherit segments (nor pin them alive).
            drop(crate::ram::chunk::take_transactional_segments());
            return Self::map_commit_write_error(&txn, id, error);
        }

        txn.state = TxnState::Committed;

        // Sync the segments this transaction actually APPENDED to.
        //
        // The old loop walked `segment_guards`, which hold the segments of
        // the OLD versions of updated and removed cells -- not where the new
        // entries went. A transaction of pure inserts pushed no guard at all
        // and so synced nothing, while its writes had skipped group commit
        // on the promise that commit would sync them. Both halves of that
        // promise were broken. The list now comes from the write path
        // itself, recorded at the point where the sync was skipped.
        // ENTRIES FIRST, then the COMMIT records, then those.
        //
        // The order is the whole cross-chunk rule. A transaction spanning
        // several chunks writes a bracket in each, and N fsyncs cannot be made
        // atomic -- so a crash can leave one chunk's COMMIT durable and
        // another's not. That is survivable only if every ENTRY was already
        // durable before any COMMIT was written: then a single surviving
        // COMMIT, plus the manifest naming every member, is enough to apply
        // the whole transaction, and recovery needs no agreement between
        // chunks about which COMMIT it found.
        //
        // Syncing entries and COMMITs together instead -- which is what this
        // did first -- makes a surviving COMMIT prove nothing about the other
        // chunks, and the fuzzer's transaction lane found exactly that: a
        // four-cell transaction spread over four chunks came back 3/4.
        let mut sync_failure = None;
        let entry_segments = crate::ram::chunk::take_transactional_segments();
        for segment in &entry_segments {
            if let Err(error) = segment.force_wal_sync() {
                error!(
                    "Failed to sync WAL for segment {} (chunk {}) during commit: {:?}",
                    segment.id, segment.chunk_id, error
                );
                sync_failure.get_or_insert(error);
            } else {
                debug!(
                    "Synced segment {} (chunk {}) WAL to disk for transaction commit",
                    segment.id, segment.chunk_id
                );
            }
        }
        // NO bracket COMMIT here, for the same reason there is no commit
        // marker here: despite its name this runs in the coordinator's
        // PREPARE phase, and a durable COMMIT is the participant saying "this
        // transaction happened". Writing it now would make the transaction
        // recoverable-as-committed before anyone decided to commit it, so a
        // crash in the window would install a transaction the coordinator
        // might have been about to abort -- and an abort that could not
        // physically rewind (a sealed segment) would leave the COMMIT
        // standing. Prepare makes the ENTRIES durable; `end` closes the
        // bracket.
        if let Some(error) = sync_failure {
            // Reporting success here would promise durability that does not
            // exist: the writes are in memory and in the index, but their
            // log is not on disk, so a crash loses a committed transaction.
            //
            // And this site now treats the transaction as ABORTED, so it is
            // aborted for real, here: the index is rolled back and the spans
            // rewound before the heads go. Setting the state alone left the
            // writes visible in memory (a later `abort` answers
            // AlreadyAborted and rolls nothing back) and the heads to be
            // released with their BEGINs open.
            error!(
                "Transaction {:?} could not be made durable at commit: {:?}. Reporting failure \
                 so it is not treated as committed.",
                tid, error
            );
            let _ = self.rollback_and_rewind(tid, &mut txn);
            return DMCommitResult::NotDurable(format!("WAL sync failed: {error:?}"));
        }

        // NO commit marker here, deliberately.
        //
        // Despite its name, this runs in the coordinator's PREPARE phase
        // (`manager::sites_commit`, whose success becomes
        // `TMPrepareResult::Success`): it applies the writes and records the
        // undo entries that let them be taken back. The commit DECISION
        // comes later, in `end`, and the marker belongs with the decision --
        // writing it here marks the transaction complete before anyone has
        // decided to commit it, so a crash in between leaves writes that
        // recovery will never roll back. That is durability for something
        // that was never agreed.
        DMCommitResult::Success
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
                        && certified.expectation == CellExpectation::Absent
                }
                CommitOp::Update(_) | CommitOp::Remove(_) => {
                    certified.intent == PrepareIntent::Write
                        && matches!(certified.expectation, CellExpectation::Present(_))
                }
                CommitOp::Read(_, version) => {
                    certified.intent == PrepareIntent::Read
                        && certified.expectation == CellExpectation::Present(*version)
                }
                CommitOp::None => false,
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
            match op {
                CommitOp::Write(cell) => match self.chunks().read_cell(&cell.id()) {
                    Ok(_) => {
                        return Err(Self::map_commit_write_error(
                            txn,
                            cell.id(),
                            WriteError::CellAlreadyExisted,
                        ));
                    }
                    Err(ReadError::CellDoesNotExisted) => {}
                    Err(read_error) => {
                        return Err(DMCommitResult::WriteError(
                            cell.id(),
                            WriteError::ReadError(read_error),
                        ));
                    }
                },
                CommitOp::Update(cell) => {
                    let cell_id = cell.id();
                    let expected_version = txn
                        .certified_present_version(&cell_id)
                        .expect("commit payload validation requires present certification");
                    let current_version = match self.chunks().read_cell(&cell_id) {
                        Ok(shared_cell) => shared_cell.header.version,
                        Err(read_error) => {
                            return Err(Self::map_commit_write_error(
                                txn,
                                cell_id,
                                WriteError::ReadError(read_error),
                            ));
                        }
                    };
                    if current_version != expected_version {
                        return Err(Self::map_commit_write_error(
                            txn,
                            cell_id,
                            WriteError::CellVersionMismatch,
                        ));
                    }
                }
                CommitOp::Remove(cell_id) => {
                    let expected_version = txn
                        .certified_present_version(cell_id)
                        .expect("commit payload validation requires present certification");
                    let current_version = match self.chunks().read_cell(cell_id) {
                        Ok(shared_cell) => shared_cell.header.version,
                        Err(read_error) => {
                            return Err(Self::map_commit_write_error(
                                txn,
                                *cell_id,
                                WriteError::ReadError(read_error),
                            ));
                        }
                    };
                    if current_version != expected_version {
                        return Err(Self::map_commit_write_error(
                            txn,
                            *cell_id,
                            WriteError::CellVersionMismatch,
                        ));
                    }
                }
                CommitOp::Read(cell_id, version) => {
                    let current_version = match self.chunks().read_cell(cell_id) {
                        Ok(shared_cell) => shared_cell.header.version,
                        Err(read_error) => {
                            return Err(Self::map_commit_write_error(
                                txn,
                                *cell_id,
                                WriteError::ReadError(read_error),
                            ));
                        }
                    };
                    if current_version != *version {
                        return Err(Self::map_commit_write_error(
                            txn,
                            *cell_id,
                            WriteError::CellVersionMismatch,
                        ));
                    }
                }
                CommitOp::None => {
                    return Err(DMCommitResult::CheckFailed(CheckError::CannotEnd));
                }
            }
        }

        Ok(())
    }

    fn map_commit_write_error(txn: &Transaction, id: Id, error: WriteError) -> DMCommitResult {
        let expected_present = txn.certified_present_version(&id).is_some();
        let expected_absent_write = txn.certified_expects_absent_write(&id);
        let is_cell_changed = match &error {
            WriteError::DeletionPredictionFailed
            | WriteError::UserCanceledUpdate
            | WriteError::CellVersionMismatch => true,
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
    use crate::ram::schema::SchemaVid;
    use crate::ram::segs::SEGMENT_SIZE;
    use crate::ram::tests::default_fields;
    use crate::ram::types::{Id, OwnedMap, OwnedValue};
    use crate::server::transactions::test_hlc;
    use crate::server::{NebServer, ServerOptions, Service as NebService};
    use bifrost::rpc::DEFAULT_CLIENT_POOL;
    use dovahkiin::types::Map as OwnedMapTrait;
    use futures::future::join_all;
    use lightning::map::Map as LFMapTrait;
    use std::sync::Arc;

    #[test]
    fn pinned_read_set_records_and_returns_entry() {
        let mut set = PinnedReadSet::default();
        set.insert(
            Id::allocated(0, 0, 42),
            PinnedRead {
                location: 0x1000,
                version: 7,
            },
            None,
        );
        assert_eq!(
            set.get(&Id::allocated(0, 0, 42)).map(|p| p.version),
            Some(7)
        );
        assert_eq!(
            set.get(&Id::allocated(0, 0, 42)).map(|p| p.location),
            Some(0x1000)
        );
        assert!(set.get(&Id::allocated(0, 0, 99)).is_none());
        assert!(!set.is_empty());
        set.drain();
        assert!(set.is_empty());
    }

    #[test]
    fn scoped_data_manager_service_ids_differ_between_databases() {
        let group = "group_a";
        assert_ne!(
            generate_scoped_service_id(group, "db_a"),
            generate_scoped_service_id(group, "db_b")
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
        let id = Id::allocated(0, 0, 99012);
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
                chunk_size: 4 * SEGMENT_SIZE,
                db_size: 4 * SEGMENT_SIZE,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
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
        data.insert(&String::from("id"), OwnedValue::I64(id.bits() as i64));
        data.insert(&String::from("score"), OwnedValue::U64(score));
        data.insert(&String::from("name"), OwnedValue::String(name.to_string()));
        OwnedCell::new_with_id(SchemaVid(schema_id), &id, OwnedValue::Map(data))
    }

    // A cell whose stored length is well above the default 4096-byte read-pin
    // threshold. The multi-KiB `name` payload makes the participant pin the read
    // version; `score` is a small, distinct marker used to tell versions apart.
    fn large_cell(schema_id: u32, id: Id, score: u64, marker: &str) -> OwnedCell {
        let mut data = OwnedMap::new();
        data.insert(&String::from("id"), OwnedValue::I64(id.bits() as i64));
        data.insert(&String::from("score"), OwnedValue::U64(score));
        let payload = format!("{}-{}", marker, "x".repeat(8192));
        data.insert(&String::from("name"), OwnedValue::String(payload));
        OwnedCell::new_with_id(SchemaVid(schema_id), &id, OwnedValue::Map(data))
    }

    fn seed_cell_version(
        runtime: &Arc<crate::server::DatabaseRuntime>,
        schema_id: u32,
        id: Id,
        score: u64,
        version_steps: usize,
    ) -> u64 {
        let mut cell = counter_cell(schema_id, id, score, "prepare-seed-0");
        let mut header = runtime.chunks().write_cell(&mut cell).unwrap();
        for step in 0..version_steps {
            let next_score = score + step as u64 + 1;
            let mut updated = counter_cell(
                schema_id,
                id,
                next_score,
                &format!("prepare-seed-{}", step + 1),
            );
            header = runtime.chunks().update_cell(&mut updated).unwrap();
        }
        header.version
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
                        expectation: CellExpectation::Absent,
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
        <DataManager as Service>::prepare(
            manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            ops,
            vec![],
        )
        .await
        .payload
    }

    async fn commit_ops_local(
        manager: &Arc<DataManager>,
        tid: &TxnId,
        ops: Vec<CommitOp>,
    ) -> DMCommitResult {
        <DataManager as Service>::commit(manager, tid.clone(), tid.clone(), ops)
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

    /// A server whose chunk has room for several segments. A transaction only
    /// leases a head when its chunk can spare one, so the lease lifecycle is
    /// untestable on the one-segment chunks the other tests use (there every
    /// transactional write falls back to unbracketed).
    async fn start_lease_test_server(address: &str, group: &str) -> Arc<NebServer> {
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 4 * SEGMENT_SIZE,
                db_size: 4 * SEGMENT_SIZE,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
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

    /// Prepare `cells` as inserts for `tid` and apply them: the participant's
    /// state right before the decision, each write behind a leased head.
    async fn apply_inserts(manager: &Arc<DataManager>, schema: &Schema, tid: &TxnId, cells: &[Id]) {
        prepare_local_txn(manager, tid, cells.to_vec());
        let ops = cells
            .iter()
            .map(|id| CommitOp::Write(counter_cell(schema.vid.get(), *id, 1, "lease")))
            .collect();
        assert_eq!(
            commit_ops_local(manager, tid, ops).await,
            DMCommitResult::Success
        );
    }

    fn segment_holding(
        runtime: &Arc<crate::server::DatabaseRuntime>,
        id: &Id,
    ) -> lightning::aarc::Arc<crate::ram::segs::Segment> {
        let chunks = runtime.chunks();
        chunks.list[0]
            .locate_segment(chunks.address_of(id))
            .expect("segment of an indexed cell")
    }

    fn entry_type_at(addr: usize) -> (crate::ram::entry::EntryType, usize) {
        let (header, _) = crate::ram::entry::Entry::decode_from(addr, |_, header| header);
        (
            header.entry_type,
            crate::ram::entry::ENTRY_HEAD_SIZE + header.content_length as usize,
        )
    }

    /// No head of this store may be owned with a BEGIN open in it once the
    /// transactions touching it are decided.
    fn assert_no_open_bracket(runtime: &Arc<crate::server::DatabaseRuntime>) {
        for chunk in runtime.chunks().list.iter() {
            for seg in chunk.segments() {
                assert!(
                    !seg.bracket_is_open(),
                    "segment {} still holds an open bracket",
                    seg.id
                );
                assert!(!seg.is_head_owned(), "segment {} is still owned", seg.id);
            }
        }
    }

    /// An abort after apply rewinds the span BEFORE the head goes back.
    ///
    /// It used to release the heads first, BEGIN still open, so the next
    /// ordinary writer's acknowledged entries landed inside a bracket that
    /// recovery discards (H1).
    #[tokio::test(flavor = "multi_thread")]
    async fn abort_after_apply_rewinds_the_span_before_releasing_the_head() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_abort_rewinds";
        let server = start_lease_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let store = runtime.chunks().address_range();
        let tid = test_hlc(1, 9101);
        let cell = Id::allocated(0, 0, 9101);

        apply_inserts(&manager, &schema, &tid, &[cell]).await;
        assert_eq!(
            crate::ram::chunk::transaction_lease_count(&tid, &store),
            1,
            "the apply must hold its head until the decision"
        );
        let seg = segment_holding(&runtime, &cell);
        assert!(seg.bracket_is_open());
        let begin_at = runtime.chunks().address_of(&cell)
            - (crate::ram::entry::ENTRY_HEAD_SIZE + crate::ram::bracket::TXN_ID_SIZE);
        assert_eq!(
            entry_type_at(begin_at).0,
            crate::ram::entry::EntryType::BEGIN
        );
        let cursor = seg.append_header.load(std::sync::atomic::Ordering::Acquire);

        let abort = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort, AbortResult::Success(None));
        assert_eq!(crate::ram::chunk::transaction_lease_count(&tid, &store), 0);
        assert!(runtime.chunks().read_cell(&cell).is_err(), "rolled back");
        assert_eq!(
            entry_type_at(begin_at),
            (crate::ram::entry::EntryType::PADDING, cursor - begin_at),
            "the whole span, BEGIN included, must be one padding entry"
        );
        assert_no_open_bracket(&runtime);
        assert!(seg.try_own(), "the head must be back in the pool");
        seg.release_own();

        let end = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end, EndResult::Success);
        server.shutdown().await;
    }

    /// An abort restores what every kind of write replaced: an updated cell
    /// reads its old value, a removed cell is back, an inserted cell is gone
    /// -- and the transaction's span is rewound, not left open.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_abort_restores_updates_removes_and_inserts() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_abort_restores";
        let server = start_lease_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let updated = Id::allocated(0, 0, 9201);
        let removed = Id::allocated(0, 0, 9202);
        let inserted = Id::allocated(0, 0, 9203);
        let updated_version = seed_cell_version(&runtime, schema.vid.get(), updated, 5, 0);
        let removed_version = seed_cell_version(&runtime, schema.vid.get(), removed, 6, 0);
        let tid = test_hlc(1, 9201);
        let prepare = prepare_ops_local(
            &manager,
            9201,
            &tid,
            vec![
                PrepareOp {
                    id: updated,
                    expectation: CellExpectation::Present(updated_version),
                    intent: PrepareIntent::Write,
                },
                PrepareOp {
                    id: removed,
                    expectation: CellExpectation::Present(removed_version),
                    intent: PrepareIntent::Write,
                },
                PrepareOp {
                    id: inserted,
                    expectation: CellExpectation::Absent,
                    intent: PrepareIntent::Write,
                },
            ],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);
        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![
                CommitOp::Update(counter_cell(schema.vid.get(), updated, 50, "new")),
                CommitOp::Remove(removed),
                CommitOp::Write(counter_cell(schema.vid.get(), inserted, 70, "new")),
            ],
        )
        .await;
        assert_eq!(commit, DMCommitResult::Success);
        let store = runtime.chunks().address_range();
        assert!(crate::ram::chunk::transaction_lease_count(&tid, &store) >= 1);

        let abort = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort, AbortResult::Success(None));
        let chunks = runtime.chunks();
        assert_eq!(
            chunks.read_cell(&updated).unwrap().to_owned().data["score"]
                .u64()
                .copied(),
            Some(5),
            "the update must be undone"
        );
        assert_eq!(
            chunks.read_cell(&removed).unwrap().to_owned().data["score"]
                .u64()
                .copied(),
            Some(6),
            "the remove must be undone"
        );
        assert!(
            chunks.read_cell(&inserted).is_err(),
            "the insert must be undone"
        );
        assert_eq!(crate::ram::chunk::transaction_lease_count(&tid, &store), 0);
        assert_no_open_bracket(&runtime);
        let end = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end, EndResult::Success);
        server.shutdown().await;
    }

    /// The deadlock that made abort release its heads first, as a regression
    /// test: the aborting transaction holds its heads through the rollback,
    /// with every head the chunk can spare taken by transactions, and the
    /// abort still completes at once. A lease never takes the last head that
    /// is not a transaction's, so the rollback always has one to write to.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_abort_under_head_pressure_completes_without_waiting() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_abort_pressure";
        let server = start_lease_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let store = runtime.chunks().address_range();
        let victim_cell = Id::allocated(0, 0, 9301);
        let victim_version = seed_cell_version(&runtime, schema.vid.get(), victim_cell, 7, 0);
        let victim = test_hlc(1, 9301);
        let prepare = prepare_ops_local(
            &manager,
            9301,
            &victim,
            vec![PrepareOp {
                id: victim_cell,
                expectation: CellExpectation::Present(victim_version),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);
        assert_eq!(
            commit_ops_local(
                &manager,
                &victim,
                vec![CommitOp::Update(counter_cell(
                    schema.vid.get(),
                    victim_cell,
                    70,
                    "new"
                ))],
            )
            .await,
            DMCommitResult::Success
        );
        // Others take every head the chunk can spare; the first one refused
        // marks the pressure point.
        let mut holders = Vec::new();
        for k in 0..16u64 {
            let tid = test_hlc(2, 9400 + k);
            let cell = Id::allocated(0, 0, 9400 + k);
            prepare_local_txn(&manager, &tid, vec![cell]);
            let applied = commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Write(counter_cell(
                    schema.vid.get(),
                    cell,
                    1,
                    "holder",
                ))],
            )
            .await;
            if applied != DMCommitResult::Success {
                assert_eq!(
                    applied,
                    DMCommitResult::WriteError(cell, WriteError::TransactionHeadUnavailable),
                    "under head pressure a transaction must fail typed, not go unbracketed"
                );
                abort_and_end_local(&manager, &tid).await;
                break;
            }
            holders.push(tid);
        }
        assert!(!holders.is_empty());

        let started = std::time::Instant::now();
        let abort = <DataManager as Service>::abort(&manager, victim.clone(), victim.clone())
            .await
            .payload;
        assert_eq!(abort, AbortResult::Success(None));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the abort waited {:?} for a head",
            started.elapsed()
        );
        assert_eq!(
            runtime
                .chunks()
                .read_cell(&victim_cell)
                .unwrap()
                .to_owned()
                .data["score"]
                .u64()
                .copied(),
            Some(7)
        );
        assert_eq!(
            crate::ram::chunk::transaction_lease_count(&victim, &store),
            0
        );
        for tid in &holders {
            abort_and_end_local(&manager, tid).await;
        }
        let end = <DataManager as Service>::end(&manager, victim.clone(), victim.clone())
            .await
            .payload;
        assert_eq!(end, EndResult::Success);
        assert_no_open_bracket(&runtime);
        server.shutdown().await;
    }

    /// A panic between BEGIN and the decision aborts the transaction, rewinds
    /// its span, and leaves the thread naming nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicking_apply_is_aborted_and_rewound() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_apply_panic";
        let server = start_lease_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let store = runtime.chunks().address_range();
        let tid = test_hlc(1, 9102);
        let first = Id::allocated(0, 0, 91021);
        let second = Id::allocated(0, 0, 91022);

        prepare_local_txn(&manager, &tid, vec![first, second]);
        install_apply_panic_after_cell(tid.clone(), first);
        let ops = vec![
            CommitOp::Write(counter_cell(schema.vid.get(), first, 1, "panic")),
            CommitOp::Write(counter_cell(schema.vid.get(), second, 2, "panic")),
        ];
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Without an indexer the apply runs inside the call itself.
            drop(<DataManager as Service>::commit(
                &manager,
                tid.clone(),
                tid.clone(),
                ops,
            ));
        }));
        assert!(panicked.is_err(), "the hook must have fired");
        assert!(
            !crate::ram::chunk::is_in_transaction(),
            "the unwind must not leave this thread naming the dead transaction"
        );
        assert_eq!(crate::ram::chunk::transaction_lease_count(&tid, &store), 0);
        assert!(runtime.chunks().read_cell(&first).is_err(), "rolled back");
        assert!(runtime.chunks().read_cell(&second).is_err());
        assert_eq!(
            manager.txns.get(&tid).unwrap().lock().state,
            TxnState::Aborted
        );
        assert_no_open_bracket(&runtime);

        let end = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end, EndResult::Success);
        server.shutdown().await;
    }

    /// A slow owner and the lease sweeper.
    ///
    /// The sweep must wait while the owner still holds the transaction (an
    /// apply in progress), then presume abort properly -- roll back, rewind,
    /// release -- and the late decision that follows must NOT be told it
    /// committed: its spans are gone.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_slow_owner_after_lease_expiry_is_never_told_it_committed() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_slow_owner";
        let server = start_lease_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let store = runtime.chunks().address_range();
        let tid = test_hlc(1, 9103);
        let cell = Id::allocated(0, 0, 9103);

        apply_inserts(&manager, &schema, &tid, &[cell]).await;
        let txn_lock = manager.find_transaction(&tid).unwrap();
        let owner = txn_lock.lock();
        let sweep = {
            let manager = manager.clone();
            tokio::spawn(async move {
                manager
                    .sweep_idle_transactions(Duration::ZERO, Duration::ZERO)
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!sweep.is_finished(), "the sweep must wait for the owner");
        assert_eq!(
            crate::ram::chunk::transaction_lease_count(&tid, &store),
            1,
            "and must not touch the owner's heads meanwhile"
        );
        drop(owner);
        sweep.await.unwrap();

        assert_eq!(crate::ram::chunk::transaction_lease_count(&tid, &store), 0);
        assert!(
            runtime.chunks().read_cell(&cell).is_err(),
            "presumed abort rolls back"
        );
        assert!(manager.find_transaction(&tid).is_none());
        assert_no_open_bracket(&runtime);

        // The owner wakes up with a commit decision. It must be refused.
        let end = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_ne!(
            end,
            EndResult::Success,
            "a late decision must not be acknowledged"
        );
        assert!(runtime.chunks().read_cell(&cell).is_err());
        server.shutdown().await;
    }

    /// The commit decision closes every bracket with a COMMIT before the
    /// heads go back, and once it has started no abort can tear it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_commit_decision_closes_the_bracket_and_cannot_be_aborted() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_commit_closes";
        let server = start_lease_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let store = runtime.chunks().address_range();
        let tid = test_hlc(1, 9104);
        let cell = Id::allocated(0, 0, 9104);

        apply_inserts(&manager, &schema, &tid, &[cell]).await;
        // Decided commit (as `end` marks it before writing any record): an
        // abort from anywhere must now be refused, leases untouched.
        manager
            .find_transaction(&tid)
            .unwrap()
            .lock()
            .decided_commit = true;
        let abort = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(
            abort,
            AbortResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        assert_eq!(crate::ram::chunk::transaction_lease_count(&tid, &store), 1);

        let end = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end, EndResult::Success);
        assert_eq!(crate::ram::chunk::transaction_lease_count(&tid, &store), 0);
        let addr = runtime.chunks().address_of(&cell);
        let (_, size) = entry_type_at(addr);
        assert_eq!(
            entry_type_at(addr + size).0,
            crate::ram::entry::EntryType::COMMIT,
            "the bracket must be closed right behind the transaction's entry"
        );
        assert_eq!(
            runtime.chunks().durable_txn_status(&tid),
            crate::ram::chunk::DurableTxnStatus::Committed
        );
        assert!(runtime.chunks().read_cell(&cell).is_ok());
        assert_no_open_bracket(&runtime);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_a_stale_present_version() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_stale_present";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let client = data_site_client_for_database(address, group, group).await;
        let cell_id = Id::allocated(0, 0, 99001);
        let version = seed_cell_version(&runtime, schema.vid.get(), cell_id, 3, 0);
        let tid = test_hlc(1, 11);

        let result = client
            .prepare(
                11,
                tid.clone(),
                tid.clone(),
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(version + 1),
                    intent: PrepareIntent::Write,
                }],
                vec![],
            )
            .await
            .unwrap()
            .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_fenced_database_refuses_to_prepare_writes_to_protected_cells() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_fenced_prepare";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = runtime.data_site().expect("registered data site");
        let present_id = Id::allocated(0, 0, 99101);
        let version = seed_cell_version(&runtime, schema.vid.get(), present_id, 1, 0);
        let absent_id = Id::allocated(0, 0, 99102);

        let hold = runtime
            .engage_write_fence([schema.uid].into_iter().collect(), Duration::from_secs(5))
            .await
            .unwrap();

        let write_present = PrepareOp {
            id: present_id,
            expectation: CellExpectation::Present(version),
            intent: PrepareIntent::Write,
        };
        let tid = test_hlc(1, 71);
        assert_eq!(
            prepare_ops_local(&manager, 71, &tid, vec![write_present.clone()]).await,
            DMPrepareResult::DatabaseFenced
        );
        // An insert has no stored header to name its family, so it is refused too.
        let tid = test_hlc(1, 72);
        assert_eq!(
            prepare_ops_local(
                &manager,
                72,
                &tid,
                vec![PrepareOp {
                    id: absent_id,
                    expectation: CellExpectation::Absent,
                    intent: PrepareIntent::Write,
                }]
            )
            .await,
            DMPrepareResult::DatabaseFenced
        );
        // Reads are not writes.
        let tid = test_hlc(1, 73);
        assert_eq!(
            prepare_ops_local(
                &manager,
                73,
                &tid,
                vec![PrepareOp {
                    id: present_id,
                    expectation: CellExpectation::Present(version),
                    intent: PrepareIntent::Read,
                }]
            )
            .await,
            DMPrepareResult::Success
        );
        abort_and_end_local(&manager, &tid).await;

        drop(hold);
        let tid = test_hlc(1, 74);
        assert_eq!(
            prepare_ops_local(&manager, 74, &tid, vec![write_present]).await,
            DMPrepareResult::Success,
            "the fence lifts when the hold drops"
        );
        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn engaging_the_fence_waits_for_a_prepared_writer_to_commit() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_fence_drains_prepared";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = runtime.data_site().expect("registered data site");
        let cell_id = Id::allocated(0, 0, 99111);
        let version = seed_cell_version(&runtime, schema.vid.get(), cell_id, 1, 0);
        let tid = test_hlc(1, 81);

        assert_eq!(
            prepare_ops_local(
                &manager,
                81,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(version),
                    intent: PrepareIntent::Write,
                }]
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(manager.prepared_writer_count(), 1);

        let engaging = {
            let runtime = runtime.clone();
            let uid = schema.uid;
            tokio::spawn(async move {
                runtime
                    .engage_write_fence([uid].into_iter().collect(), Duration::from_secs(10))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !engaging.is_finished(),
            "the fence must not report engaged while a prepared writer can still commit"
        );

        // The prepared transaction is allowed to finish, fence or no fence.
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.vid.get(),
                    cell_id,
                    2,
                    "committed-through-the-fence",
                ))],
            )
            .await,
            DMCommitResult::Success
        );
        let end = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end, EndResult::Success);

        let hold = engaging.await.unwrap().expect("engages once the writer is done");
        let stored = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*stored.data["score"].u64().unwrap(), 2);
        drop(hold);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn engaging_the_fence_times_out_on_a_prepared_writer_that_never_finishes() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_fence_drain_timeout";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = runtime.data_site().expect("registered data site");
        let cell_id = Id::allocated(0, 0, 99121);
        let version = seed_cell_version(&runtime, schema.vid.get(), cell_id, 1, 0);
        let tid = test_hlc(1, 91);
        assert_eq!(
            prepare_ops_local(
                &manager,
                91,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(version),
                    intent: PrepareIntent::Write,
                }]
            )
            .await,
            DMPrepareResult::Success
        );
        let err = runtime
            .engage_write_fence([schema.uid].into_iter().collect(), Duration::from_millis(100))
            .await
            .err()
            .expect("must time out");
        assert!(matches!(
            err,
            crate::server::write_fence::FenceError::DrainTimeout { .. }
        ));
        assert!(
            !runtime.write_fence().is_engaged(),
            "a timed-out engage leaves nothing fenced"
        );
        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partial_read_pins_and_is_repeatable_across_an_update() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_pinned_partial_read";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 99021);

        // Seed version A. Size no longer matters: a partial read (head with
        // pin=true) pins the read version regardless of cell size, so a small
        // (normal) cell pins just as a large one would (shape-gated, not size).
        let score_a: u64 = 100;
        let mut cell_a = counter_cell(schema.vid.get(), cell_id, score_a, "A");
        let version_a = runtime.chunks().write_cell(&mut cell_a).unwrap().version;

        let tid = test_hlc(1, 41);

        // A partial read (head with pin=true) pins version A for this transaction.
        let head_a =
            <DataManager as Service>::head(&manager, 41, tid.clone(), tid.clone(), cell_id, true)
                .await
                .payload;
        let header_a = match head_a {
            TxnExecResult::Accepted(header) => header,
            other => panic!("expected head to be accepted, got {:?}", other),
        };
        assert_eq!(header_a.version, version_a);

        // Externally advance the cell to version B.
        let score_b: u64 = 200;
        let mut cell_b = counter_cell(schema.vid.get(), cell_id, score_b, "B");
        let version_b = runtime.chunks().update_cell(&mut cell_b).unwrap().version;
        assert_ne!(version_a, version_b);

        // The pinning transaction must still observe version A on a full read.
        let read_pinned =
            <DataManager as Service>::read(&manager, 41, tid.clone(), tid.clone(), cell_id)
                .await
                .payload;
        let pinned_cell = match read_pinned {
            TxnExecResult::Accepted(cell) => cell,
            other => panic!("expected read to be accepted, got {:?}", other),
        };
        assert_eq!(
            *pinned_cell.data["score"].u64().unwrap(),
            score_a,
            "pinned read must serve version A's data, not version B's"
        );
        assert_eq!(
            pinned_cell.header.version, version_a,
            "pinned read must serve version A, not version B"
        );

        // And a subsequent head from the same transaction is still version A.
        let head_again =
            <DataManager as Service>::head(&manager, 41, tid.clone(), tid.clone(), cell_id, true)
                .await
                .payload;
        let header_again = match head_again {
            TxnExecResult::Accepted(header) => header,
            other => panic!("expected head to be accepted, got {:?}", other),
        };
        assert_eq!(header_again.version, version_a);

        // A different transaction, never having pinned, sees the current version B.
        let other_tid = test_hlc(1, 42);
        let read_current = <DataManager as Service>::read(
            &manager,
            42,
            other_tid.clone(),
            other_tid.clone(),
            cell_id,
        )
        .await
        .payload;
        let current_cell = match read_current {
            TxnExecResult::Accepted(cell) => cell,
            other => panic!("expected read to be accepted, got {:?}", other),
        };
        assert_eq!(
            *current_cell.data["score"].u64().unwrap(),
            score_b,
            "a non-pinning transaction must see the current version B"
        );
        assert_eq!(current_cell.header.version, version_b);

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn release_read_pins_drains_pins_and_wipes_pin_only_transaction() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_release_read_pins";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 99023);

        // Seed a version, then pin it with a partial read (head with pin=true),
        // which creates a participant transaction holding a segment guard.
        let mut cell = counter_cell(schema.vid.get(), cell_id, 500, "A");
        let version = runtime.chunks().write_cell(&mut cell).unwrap().version;

        let tid = test_hlc(1, 44);
        let head =
            <DataManager as Service>::head(&manager, 44, tid.clone(), tid.clone(), cell_id, true)
                .await
                .payload;
        assert!(
            matches!(head, TxnExecResult::Accepted(ref h) if h.version == version),
            "partial read must observe the seeded version"
        );

        // The pin created a participant transaction with a non-empty pin set.
        // Clone the Arc so we can still inspect the pin set after a wipe removes
        // the map entry.
        let txn = manager
            .find_transaction(&tid)
            .expect("partial read must create a participant transaction");
        assert!(
            !txn.lock().pinned_reads.is_empty(),
            "partial read must record a pinned read"
        );

        // Releasing the read pins drains the pin set and, because the transaction
        // is pin-only (Started, no certified/affected state), wipes it entirely.
        let released = <DataManager as Service>::release_read_pins(&manager, vec![tid.clone()])
            .await
            .payload;
        assert_eq!(released, ());
        assert!(
            txn.lock().pinned_reads.is_empty(),
            "release_read_pins must drain the pinned reads"
        );
        assert!(
            manager.find_transaction(&tid).is_none(),
            "release_read_pins must wipe a pin-only participant transaction"
        );
        assert!(
            !manager.txns_sorted.lock().contains(&tid),
            "release_read_pins must drop the txns_sorted entry too"
        );

        // Releasing an unknown transaction is idempotent and still succeeds.
        let unknown_tid = test_hlc(9, 45);
        let released_unknown =
            <DataManager as Service>::release_read_pins(&manager, vec![unknown_tid.clone()])
                .await
                .payload;
        assert_eq!(released_unknown, ());
        assert!(manager.find_transaction(&unknown_tid).is_none());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn head_without_pin_does_not_snapshot_the_read() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_head_no_pin";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 99022);

        // Seed version A.
        let score_a: u64 = 300;
        let mut cell_a = counter_cell(schema.vid.get(), cell_id, score_a, "A");
        let version_a = runtime.chunks().write_cell(&mut cell_a).unwrap().version;

        let tid = test_hlc(1, 43);

        // A blind head (pin=false, the version-observation path) must NOT pin and
        // must NOT create a participant transaction.
        let head_a =
            <DataManager as Service>::head(&manager, 43, tid.clone(), tid.clone(), cell_id, false)
                .await
                .payload;
        assert!(
            matches!(head_a, TxnExecResult::Accepted(ref h) if h.version == version_a),
            "blind head must observe the current version"
        );
        assert!(
            manager.find_transaction(&tid).is_none(),
            "head(pin=false) must not create a participant transaction"
        );

        // Externally advance the cell to version B.
        let score_b: u64 = 400;
        let mut cell_b = counter_cell(schema.vid.get(), cell_id, score_b, "B");
        let version_b = runtime.chunks().update_cell(&mut cell_b).unwrap().version;
        assert_ne!(version_a, version_b);

        // With no prior partial-read pin, a full read in the same transaction sees
        // the current version B (a blind head created no snapshot, and full reads
        // never create a pin themselves).
        let read_current =
            <DataManager as Service>::read(&manager, 43, tid.clone(), tid.clone(), cell_id)
                .await
                .payload;
        let current_cell = match read_current {
            TxnExecResult::Accepted(cell) => cell,
            other => panic!("expected read to be accepted, got {:?}", other),
        };
        assert_eq!(
            *current_cell.data["score"].u64().unwrap(),
            score_b,
            "head(pin=false) must not snapshot; the later full read sees version B"
        );
        assert_eq!(current_cell.header.version, version_b);
        assert!(
            manager.find_transaction(&tid).is_none(),
            "a full read with no prior partial pin must not create a participant transaction"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_a_present_cell_when_absence_was_observed() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_present_vs_absent";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let client = data_site_client_for_database(address, group, group).await;
        let cell_id = Id::allocated(0, 0, 99002);
        seed_cell_version(&runtime, schema.vid.get(), cell_id, 4, 0);
        let tid = test_hlc(1, 12);

        let result = client
            .prepare(
                12,
                tid.clone(),
                tid.clone(),
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Absent,
                    intent: PrepareIntent::Write,
                }],
                vec![],
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_missing_present";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 99003);
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
            vec![],
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_duplicate_ops";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 99004);
        let version = seed_cell_version(&runtime, schema.vid.get(), cell_id, 5, 0);
        let tid = test_hlc(1, 14);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(version),
            intent: PrepareIntent::Write,
        };

        let result = <DataManager as Service>::prepare(
            &manager,
            14,
            tid.clone(),
            tid.clone(),
            vec![op.clone(), op],
            vec![],
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_empty_ops";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = test_hlc(1, 19);

        assert!(manager.find_transaction(&tid).is_none());

        let result = <DataManager as Service>::prepare(
            &manager,
            19,
            tid.clone(),
            tid.clone(),
            vec![],
            vec![],
        )
        .await
        .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        assert!(manager.find_transaction(&tid).is_none());
        assert_eq!(manager.cells.read().len(), 0);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_retry_rejects_different_ops_without_overwriting_original_prepare() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_retry_ops_mismatch";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::allocated(0, 0, 99006);
        let cell_b = Id::allocated(0, 0, 99007);
        let version_a = seed_cell_version(&runtime, schema.vid.get(), cell_a, 7, 0);
        let version_b = seed_cell_version(&runtime, schema.vid.get(), cell_b, 8, 0);
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
            vec![],
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
            vec![],
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
                .existing_cell_meta_mutex(&cell_b)
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_retry_coordinator_mismatch";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 99008);
        let version = seed_cell_version(&runtime, schema.vid.get(), cell_id, 9, 0);
        let tid = test_hlc(1, 16);
        let original_coordinator = 16;
        let requester = TxnPriority::new(tid.clone(), original_coordinator);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(version),
            intent: PrepareIntent::Write,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            original_coordinator,
            tid.clone(),
            tid.clone(),
            vec![op.clone()],
            vec![],
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
            vec![],
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_retry_idempotent";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::allocated(0, 0, 99009);
        let cell_b = Id::allocated(0, 0, 99010);
        let version_a = seed_cell_version(&runtime, schema.vid.get(), cell_a, 10, 0);
        let version_b = seed_cell_version(&runtime, schema.vid.get(), cell_b, 11, 0);
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
            vec![],
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
            vec![],
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_retry_foreign_owner";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 99011);
        let version = seed_cell_version(&runtime, schema.vid.get(), cell_id, 12, 0);
        let tid = test_hlc(1, 18);
        let coordinator_id = 18;
        let requester = TxnPriority::new(tid.clone(), coordinator_id);
        let foreign_owner = TxnPriority::new(test_hlc(1, 1), 1);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(version),
            intent: PrepareIntent::Write,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op.clone()],
            vec![],
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
            vec![],
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_wait_die_clock";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let client = data_site_client_for_database(address, group, group).await;
        let cell_id = Id::allocated(0, 0, 99005);
        let version = seed_cell_version(&runtime, schema.vid.get(), cell_id, 6, 0);
        let older_tid = test_hlc(1, 11);
        let younger_tid = test_hlc(1, 22);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(version),
            intent: PrepareIntent::Write,
        };

        let first = client
            .prepare(
                11,
                older_tid.clone(),
                older_tid.clone(),
                vec![op.clone()],
                vec![],
            )
            .await
            .unwrap()
            .payload;
        let second = client
            .prepare(
                22,
                younger_tid.clone(),
                younger_tid.clone(),
                vec![op],
                vec![],
            )
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
        let address = &crate::utils::test_port::unique_localhost_addr();
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_read_only_stateless";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 99011);
        seed_cell_version(&runtime, schema.vid.get(), cell_id, 7, 0);
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
    async fn end_cleans_up_many_active_transactions_without_leaking_bookkeeping() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
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
        let address = &crate::utils::test_port::unique_localhost_addr();
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
        let address = &crate::utils::test_port::unique_localhost_addr();
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_commit_variant_validation";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let affected_id = Id::allocated(0, 0, 8101);

        for ops in [vec![CommitOp::Read(affected_id, 1)], vec![CommitOp::None]] {
            let tid = manager.hlc.now();
            prepare_local_txn(&manager, &tid, vec![affected_id]);

            let result = <DataManager as Service>::commit(&manager, tid.clone(), tid.clone(), ops)
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_commit_subset_validation";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let write_id = Id::allocated(0, 0, 8201);
        let read_id = Id::allocated(0, 0, 8202);
        let read_only_id = Id::allocated(0, 0, 8203);
        let unprepared = Id::allocated(0, 0, 8204);
        let write_version = seed_cell_version(&runtime, schema.vid.get(), write_id, 7, 0);
        let read_version = seed_cell_version(&runtime, schema.vid.get(), read_id, 9, 0);
        let read_only_version = seed_cell_version(&runtime, schema.vid.get(), read_only_id, 11, 0);

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
            read_only_tid.clone(),
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
            read_only_after.header.version,
            read_only_before.header.version
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
            empty_tid.clone(),
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
            empty_after_write.header.version,
            empty_before_write.header.version
        );
        assert_eq!(empty_after_read.data, empty_before_read.data);
        assert_eq!(
            empty_after_read.header.version,
            empty_before_read.header.version
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
            partial_tid.clone(),
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
            partial_after_write.header.version,
            partial_before_write.header.version
        );
        assert_eq!(partial_after_read.data, partial_before_read.data);
        assert_eq!(
            partial_after_read.header.version,
            partial_before_read.header.version
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
            extra_tid.clone(),
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
            duplicate_tid.clone(),
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_commit_missing_coordinator";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 8204);
        let initial_score = 13;
        let initial_version =
            seed_cell_version(&runtime, schema.vid.get(), cell_id, initial_score, 0);
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
                    expectation: CellExpectation::Present(initial_version),
                    intent: PrepareIntent::Write,
                },
            );
        }

        let result = <DataManager as Service>::commit(
            &manager,
            tid.clone(),
            tid.clone(),
            vec![CommitOp::Update(counter_cell(
                schema.vid.get(),
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
        assert_eq!(persisted.header.version, initial_version);

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
    async fn commit_validation_rejects_stale_prepared_owner_before_storage_mutation() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_commit_stale_owner";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 8205);
        let initial_score = 21;
        let initial_version =
            seed_cell_version(&runtime, schema.vid.get(), cell_id, initial_score, 0);
        let t1 = test_hlc(1, 21);
        let t2 = test_hlc(1, 22);
        let t1_owner = TxnPriority::new(t1.clone(), 21);
        let t2_owner = TxnPriority::new(t2.clone(), 22);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_version),
            intent: PrepareIntent::Write,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            21,
            t1.clone(),
            t1.clone(),
            vec![prepare_op.clone()],
            vec![],
        )
        .await
        .payload;
        assert_eq!(first, DMPrepareResult::Success);
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t1_owner)
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
            vec![],
        )
        .await
        .payload;
        assert_eq!(second, DMPrepareResult::Success);
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t2_owner.clone())
        );

        let commit = <DataManager as Service>::commit(
            &manager,
            t1.clone(),
            t1.clone(),
            vec![CommitOp::Update(counter_cell(
                schema.vid.get(),
                cell_id,
                99,
                "counter_stale_owner_commit",
            ))],
        )
        .await
        .payload;

        assert_eq!(commit, DMCommitResult::CheckFailed(CheckError::CannotEnd));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), initial_score);
        assert_eq!(persisted.header.version, initial_version);

        let t1_txn_lock = manager.txns.get(&t1).expect("t1 should remain tracked");
        let t1_txn = t1_txn_lock.lock();
        assert_eq!(t1_txn.state, TxnState::Prepared);
        assert!(t1_txn.history.is_empty());
        drop(t1_txn);

        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t2_owner.clone())
        );

        let abort_t2 = <DataManager as Service>::abort(&manager, t2.clone(), t2.clone())
            .await
            .payload;
        assert_eq!(abort_t2, AbortResult::Success(None));
        let end_t2 = <DataManager as Service>::end(&manager, t2.clone(), t2.clone())
            .await
            .payload;
        assert_eq!(end_t2, EndResult::Success);

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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_abort_missing_coordinator";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 8206);
        let initial_score = 41;
        let committed_score = 55;
        let initial_version =
            seed_cell_version(&runtime, schema.vid.get(), cell_id, initial_score, 0);
        let tid = test_hlc(1, 23);
        let owner = TxnPriority::new(tid.clone(), 23);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_version),
            intent: PrepareIntent::Write,
        };

        let prepare = <DataManager as Service>::prepare(
            &manager,
            23,
            tid.clone(),
            tid.clone(),
            vec![prepare_op],
            vec![],
        )
        .await
        .payload;
        assert_eq!(prepare, DMPrepareResult::Success);

        let commit = <DataManager as Service>::commit(
            &manager,
            tid.clone(),
            tid.clone(),
            vec![CommitOp::Update(counter_cell(
                schema.vid.get(),
                cell_id,
                committed_score,
                "counter_abort_missing_coordinator_commit",
            ))],
        )
        .await
        .payload;
        assert_eq!(commit, DMCommitResult::Success);

        let committed = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        let committed_version = committed.header.version;
        assert_eq!(*committed.data["score"].u64().unwrap(), committed_score);
        assert!(committed_version > initial_version);

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
        assert_eq!(persisted.header.version, committed_version);

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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_abort_stale_owner";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 8207);
        let initial_score = 61;
        let committed_score = 77;
        let initial_version =
            seed_cell_version(&runtime, schema.vid.get(), cell_id, initial_score, 0);
        let t1 = test_hlc(1, 24);
        let t2 = test_hlc(1, 25);
        let t1_owner = TxnPriority::new(t1.clone(), 24);
        let t2_owner = TxnPriority::new(t2.clone(), 25);
        let t1_prepare = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_version),
            intent: PrepareIntent::Write,
        };

        let prepare_t1 = <DataManager as Service>::prepare(
            &manager,
            24,
            t1.clone(),
            t1.clone(),
            vec![t1_prepare],
            vec![],
        )
        .await
        .payload;
        assert_eq!(prepare_t1, DMPrepareResult::Success);

        let commit_t1 = <DataManager as Service>::commit(
            &manager,
            t1.clone(),
            t1.clone(),
            vec![CommitOp::Update(counter_cell(
                schema.vid.get(),
                cell_id,
                committed_score,
                "counter_abort_stale_owner_commit",
            ))],
        )
        .await
        .payload;
        assert_eq!(commit_t1, DMCommitResult::Success);

        let committed = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        let committed_version = committed.header.version;
        assert_eq!(*committed.data["score"].u64().unwrap(), committed_score);
        assert!(committed_version > initial_version);

        {
            let txn_lock = manager.txns.get(&t1).expect("t1 should remain tracked");
            let txn = txn_lock.lock();
            assert_eq!(txn.state, TxnState::Committed);
            assert!(txn.history.contains_key(&cell_id));
        }

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.lock_acquired_at = Some(get_time() - LOCK_TIMEOUT_MS - 1);
        }

        let prepare_t2 = <DataManager as Service>::prepare(
            &manager,
            25,
            t2.clone(),
            t2.clone(),
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(committed_version),
                intent: PrepareIntent::Write,
            }],
            vec![],
        )
        .await
        .payload;
        assert_eq!(prepare_t2, DMPrepareResult::Success);
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
        assert_eq!(persisted.header.version, committed_version);

        let t1_txn_lock = manager.txns.get(&t1).expect("t1 should remain tracked");
        let t1_txn = t1_txn_lock.lock();
        assert_eq!(t1_txn.state, TxnState::Committed);
        assert!(t1_txn.history.contains_key(&cell_id));
        drop(t1_txn);

        assert_ne!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t1_owner)
        );
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t2_owner.clone())
        );

        let abort_t2 = <DataManager as Service>::abort(&manager, t2.clone(), t2.clone())
            .await
            .payload;
        assert_eq!(abort_t2, AbortResult::Success(None));
        let end_t2 = <DataManager as Service>::end(&manager, t2.clone(), t2.clone())
            .await
            .payload;
        assert_eq!(end_t2, EndResult::Success);

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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_concurrent_stale_update";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 8210);
        let initial_version = seed_cell_version(&runtime, schema.vid.get(), cell_id, 0, 0);
        let t1 = test_hlc(1, 11);
        let t2 = test_hlc(1, 22);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_version),
            intent: PrepareIntent::Write,
        };

        // (Dropped the `t1.relation(&t2) == Concurrent` precondition: HLC is a
        // total order with no `Concurrent` relation. The behaviour under test —
        // a stale-versioned update from a peer being rejected after another peer
        // commits a new version — does not depend on the two tids' relative age.)

        let prepare_t1 = prepare_ops_local(&manager, 11, &t1, vec![prepare_op.clone()]).await;
        assert_eq!(prepare_t1, DMPrepareResult::Success);

        let commit_t1 = commit_ops_local(
            &manager,
            &t1,
            vec![CommitOp::Update(counter_cell(
                schema.vid.get(),
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
        assert!(persisted.header.version > initial_version);

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_rejects_change_after_certification() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_commit_rechecks_certified_version";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 8211);
        let initial_version = seed_cell_version(&runtime, schema.vid.get(), cell_id, 1, 0);
        let tid = test_hlc(1, 31);

        let prepare = prepare_ops_local(
            &manager,
            31,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(initial_version),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let mut external = counter_cell(schema.vid.get(), cell_id, 7, "counter_commit_external");
        let external_header = runtime.chunks().update_cell(&mut external).unwrap();
        assert!(external_header.version > initial_version);

        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![CommitOp::Update(counter_cell(
                schema.vid.get(),
                cell_id,
                99,
                "counter_commit_after_certification",
            ))],
        )
        .await;
        assert_eq!(commit, DMCommitResult::CellChanged(cell_id));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), 7);
        assert_eq!(persisted.header.version, external_header.version);

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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_remove_rechecks_certified_version";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 8212);
        let initial_version = seed_cell_version(&runtime, schema.vid.get(), cell_id, 2, 0);
        let tid = test_hlc(1, 32);

        let prepare = prepare_ops_local(
            &manager,
            32,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(initial_version),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let mut external = counter_cell(schema.vid.get(), cell_id, 8, "counter_remove_external");
        let external_header = runtime.chunks().update_cell(&mut external).unwrap();
        assert!(external_header.version > initial_version);

        let commit = commit_ops_local(&manager, &tid, vec![CommitOp::Remove(cell_id)]).await;
        assert_eq!(commit, DMCommitResult::CellChanged(cell_id));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), 8);
        assert_eq!(persisted.header.version, external_header.version);

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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_update_rechecks_missing_certification";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 8216);
        let initial_version = seed_cell_version(&runtime, schema.vid.get(), cell_id, 6, 0);
        let tid = test_hlc(1, 35);

        let prepare = prepare_ops_local(
            &manager,
            35,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(initial_version),
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
                schema.vid.get(),
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_write_rechecks_absent_certification";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 8213);
        let tid = test_hlc(1, 33);

        let prepare = prepare_ops_local(
            &manager,
            33,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Absent,
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let mut external = counter_cell(schema.vid.get(), cell_id, 5, "counter_write_external");
        let external_header = runtime.chunks().write_cell(&mut external).unwrap();

        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![CommitOp::Write(counter_cell(
                schema.vid.get(),
                cell_id,
                11,
                "counter_write_after_absent_prepare",
            ))],
        )
        .await;
        assert_eq!(commit, DMCommitResult::CellChanged(cell_id));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), 5);
        assert_eq!(persisted.header.version, external_header.version);

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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_commit_prevalidation";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::allocated(0, 0, 8214);
        let cell_b = Id::allocated(0, 0, 8215);
        let version_a = seed_cell_version(&runtime, schema.vid.get(), cell_a, 3, 0);
        let version_b = seed_cell_version(&runtime, schema.vid.get(), cell_b, 4, 0);
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
                CommitOp::Update(counter_cell(
                    schema.vid.get(),
                    cell_a,
                    13,
                    "counter_prevalidate_a",
                )),
                CommitOp::Update(counter_cell(
                    schema.vid.get(),
                    cell_b,
                    14,
                    "counter_prevalidate_b",
                )),
            ],
        )
        .await;

        assert_eq!(commit, DMCommitResult::CheckFailed(CheckError::CannotEnd));

        let after_a = runtime.chunks().read_cell(&cell_a).unwrap().to_owned();
        let after_b = runtime.chunks().read_cell(&cell_b).unwrap().to_owned();
        assert_eq!(after_a.data, before_a.data);
        assert_eq!(after_a.header.version, before_a.header.version);
        assert_eq!(after_b.data, before_b.data);
        assert_eq!(after_b.header.version, before_b.header.version);

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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_commit_storage_prevalidation";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::allocated(0, 0, 8217);
        let cell_b = Id::allocated(0, 0, 8218);
        let version_a = seed_cell_version(&runtime, schema.vid.get(), cell_a, 15, 0);
        let version_b = seed_cell_version(&runtime, schema.vid.get(), cell_b, 25, 0);
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

        let mut external_b = counter_cell(
            schema.vid.get(),
            cell_b,
            26,
            "counter_prevalidate_external_b",
        );
        let external_b_header = runtime.chunks().update_cell(&mut external_b).unwrap();
        assert!(external_b_header.version > version_b);

        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![
                CommitOp::Update(counter_cell(
                    schema.vid.get(),
                    cell_a,
                    115,
                    "counter_prevalidate_storage_a",
                )),
                CommitOp::Update(counter_cell(
                    schema.vid.get(),
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
        assert_eq!(after_a.header.version, before_a.header.version);
        assert_eq!(after_b.data, external_b.data);
        assert_eq!(after_b.header.version, external_b_header.version);
        assert_ne!(after_b.header.version, before_b.header.version);

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cell_meta_cleanup_must_not_remove_an_update_prepared_after_its_check() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_cleanup_update_race";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let id = Id::allocated(0, 0, 90502);
        let version = seed_cell_version(&runtime, schema.vid.get(), id, 0, 0);
        {
            let meta = manager.cell_meta_mutex(&id);
            meta.lock().read = test_hlc(1, 1);
        }
        // The empty transaction ID reserves this hook for metadata cleanup.
        let delay = install_prepare_delay_for_cell(TxnId::default(), id);
        let cleaning = tokio::spawn({
            let manager = manager.clone();
            async move { manager.cell_meta_cleanup().await }
        });
        delay.wait_until_entered().await;
        let tid = manager.hlc.now();
        let prepared = prepare_ops_local(
            &manager,
            0,
            &tid,
            vec![PrepareOp {
                id,
                expectation: CellExpectation::Present(version),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        delay.release();
        cleaning.await.unwrap();
        assert_eq!(prepared, DMPrepareResult::Success);
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.vid.get(),
                    id,
                    1,
                    "cleanup_update"
                ),)]
            )
            .await,
            DMCommitResult::Success
        );
        assert_eq!(
            *runtime.chunks().read_cell(&id).unwrap().data["score"]
                .u64()
                .unwrap(),
            1
        );
        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cell_meta_cleanup_must_not_orphan_a_borrowed_update_meta() {
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_cleanup_borrowed_update";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let id = Id::allocated(0, 0, 90503);
        let borrowed = manager.cell_meta_mutex(&id);
        borrowed.lock().read = test_hlc(1, 1);
        manager.cell_meta_cleanup().await;
        let visible = manager.cell_meta_mutex(&id);
        assert!(
            Arc::ptr_eq(&borrowed, &visible),
            "cleanup orphaned the metadata borrowed before prepare"
        );
        drop(visible);
        drop(borrowed);
        manager.cell_meta_cleanup().await;
        assert!(manager.existing_cell_meta_mutex(&id).is_none());
        server.shutdown().await;
    }

    // A prepare borrows metadata before it acquires ownership. Cleanup must
    // retain that reference for both inserts and updates.
    #[tokio::test(flavor = "multi_thread")]
    async fn cell_meta_cleanup_must_not_orphan_a_lock_being_acquired_by_insert_prepare() {
        let _ = env_logger::try_init();
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_ds_cleanup_insert_race";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;

        // The preparing insert transaction is registered exactly as real prepare
        // does via `get_or_create_transaction`, so the cleanup watermark `oldest`
        // is this (non-empty) clock. A freshly created insert meta has empty
        // clocks that sort below the watermark. Its borrowed reference must
        // protect it until prepare publishes the owner.
        let insert_tid = test_hlc(9, 7);
        let _txn = manager.get_or_create_transaction(&insert_tid);

        // Model prepare mid-flight: the meta exists (owner not yet acquired).
        let insert_id = Id::allocated(0, 0, 90501);
        let meta_in_prepare = manager.cell_meta_mutex(&insert_id);
        assert!(
            manager.existing_cell_meta_mutex(&insert_id).is_some(),
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
        let visible = manager.existing_cell_meta_mutex(&insert_id);
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
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<OwnedCell, ReadError>>> {
        // Instrumentation: this is the whole-cell transfer path. Coordinator
        // tests assert header/projection-only reads never reach here.
        FULL_READ_RPC_COUNT.fetch_add(1, Relaxed);
        // A full read never creates a pin. It only serves from an existing pin
        // when a prior partial read (head/read_selected) in this transaction
        // already snapshotted the cell, so both reads repeat the same version.
        // Pinned serves deliberately skip `prepare_read`: the version was
        // admitted for this transaction when the pin was created, so re-running
        // the ReadTooLate check or waiting on a committing owner could only
        // spuriously reject or delay a read whose (immutable) outcome cannot
        // change. The clock merge `prepare_read` would have done happens here;
        // the fall-through path merges inside `prepare_read` (never twice).
        if let Some(location) = self.existing_read_pin(&tid, &id) {
            self.update_clock(clock);
            return match self.chunks().read_cell_at(&id, location) {
                Ok(cell) => self.response_with(TxnExecResult::Accepted(cell)),
                Err(read_error) => self.response_with(TxnExecResult::Error(read_error)),
            };
        }
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        match self.chunks().read_cell(&id) {
            Ok(cell) => self.response_with(TxnExecResult::Accepted(cell.to_owned())),
            Err(read_error) => self.response_with(TxnExecResult::Error(read_error)),
        }
    }
    fn read_selected(
        &self,
        _server_id: u64,
        clock: Hlc,
        tid: TxnId,
        id: Id,
        fields: Vec<u64>,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<OwnedCell, ReadError>>> {
        // Serve an already-pinned cell from its immutable snapshot without
        // `prepare_read` (see `read` for why re-checking could only spuriously
        // reject or delay it; the clock merge happens exactly once per path).
        if let Some(location) = self.existing_read_pin(&tid, &id) {
            self.update_clock(clock);
            return match self.chunks().read_selected_at(&id, location, &fields[..]) {
                Ok(values) => self.response_with(TxnExecResult::Accepted(values)),
                Err(read_error) => self.response_with(TxnExecResult::Error(read_error)),
            };
        }
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        if let Some(location) = self.ensure_read_pin(&tid, &id) {
            return match self.chunks().read_selected_at(&id, location, &fields[..]) {
                Ok(values) => self.response_with(TxnExecResult::Accepted(values)),
                Err(read_error) => self.response_with(TxnExecResult::Error(read_error)),
            };
        }
        match self.chunks().read_selected(&id, &fields[..], true) {
            // Need header for version check
            Ok(values) => self.response_with(TxnExecResult::Accepted(values.to_owned())),
            Err(read_error) => self.response_with(TxnExecResult::Error(read_error)),
        }
    }
    fn head(
        &self,
        _server_id: u64,
        clock: Hlc,
        tid: TxnId,
        id: Id,
        pin: bool,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<CellHeader, ReadError>>> {
        // A partial read (pin=true) snapshots the read version so the same
        // transaction repeats it. The blind version-observation path (pin=false,
        // e.g. `observe_head_from_site`) must NOT pin or create a transaction; it
        // serves the current header as the original (pre-pinning) code did.
        if pin {
            // Serve an already-pinned cell from its immutable snapshot without
            // `prepare_read` (see `read` for why re-checking could only
            // spuriously reject or delay it; the clock merge happens exactly
            // once per path).
            if let Some(location) = self.existing_read_pin(&tid, &id) {
                self.update_clock(clock);
                return match self.chunks().head_at(&id, location) {
                    Ok(head) => self.response_with(TxnExecResult::Accepted(head)),
                    Err(read_error) => self.response_with(TxnExecResult::Error(read_error)),
                };
            }
        }
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        if pin {
            if let Some(location) = self.ensure_read_pin(&tid, &id) {
                return match self.chunks().head_at(&id, location) {
                    Ok(head) => self.response_with(TxnExecResult::Accepted(head)),
                    Err(read_error) => self.response_with(TxnExecResult::Error(read_error)),
                };
            }
        }
        match self.chunks().head_cell(&id) {
            Ok(head) => self.response_with(TxnExecResult::Accepted(head)),
            Err(read_error) => self.response_with(TxnExecResult::Error(read_error)),
        }
    }
    /// Releases any read pins held for the given transactions, dropping their
    /// segment guards.
    ///
    /// Called by the coordinator's periodic release flusher with a batch of
    /// completed transactions that pinned reads on this server without writing
    /// to it (write servers drop their pins in `end`). Best-effort and
    /// idempotent per transaction:
    /// - If the transaction is absent (stale cleanup already reclaimed it, or it
    ///   was ended after a write), this is a no-op and still succeeds.
    /// - If present, its pinned reads are drained. When the transaction is
    ///   pin-only (state `Started`, with no certified/affected state), it was
    ///   created solely to hold pins, so it is wiped out entirely — leaving no
    ///   lingering `Transaction` or `txns_sorted` entry.
    ///
    /// Never fails, so a best-effort coordinator release cannot break commit or
    /// abort. It does not touch cell locks, history, or two-phase-commit state.
    fn release_read_pins(&self, tids: Vec<TxnId>) -> BoxFuture<'_, DataSiteResponse<()>> {
        for tid in tids {
            if let Some(txn_lock) = self.find_transaction(&tid) {
                let pin_only = {
                    let mut txn = txn_lock.lock();
                    // Dropping the guards here releases the pinned segment references.
                    txn.pinned_reads.drain();
                    txn.state == TxnState::Started
                        && txn.certified.is_empty()
                        && txn.affected_cells.is_empty()
                };
                if pin_only {
                    self.wipe_out_transaction(&tid);
                }
            }
        }
        self.response_with(())
    }
    // TODO: Link this function in transaction manager
    fn read_partial_raw(
        &self,
        _server_id: u64,
        clock: Hlc,
        tid: TxnId,
        id: Id,
        offset: usize,
        len: usize,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<Vec<u8>, ReadError>>> {
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        match self.chunks().read_partial_raw(&id, offset, len) {
            Ok(values) => self.response_with(TxnExecResult::Accepted(values)),
            Err(read_error) => self.response_with(TxnExecResult::Error(read_error)),
        }
    }
    fn prepare(
        &self,
        coordinator_id: u64,
        clock: Hlc,
        tid: TxnId,
        ops: Vec<PrepareOp>,
        participants: Vec<u64>,
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
            // The write fence. Counted for the whole prepare, so a fencer that
            // is draining sees this transaction either not yet started (and
            // then refuses it here) or already `Prepared` (and waits for it
            // through `prepared_writer_count`). A prepare that is fenced out is
            // refused before it takes any cell lock.
            let fence = self.database_runtime.write_fence();
            let _fence_guard = fence.count_if_open();
            if fence.is_engaged() && self.prepare_writes_fenced_cell(&prepared_ops) {
                return self.response_with(DMPrepareResult::DatabaseFenced).await;
            }
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
                    let mut meta = cell_mutex.lock();
                    let current_time = get_time();

                    if let Some(owner) = meta.owner.clone() {
                        if owner != requester {
                            let lock_age = meta
                                .lock_acquired_at
                                .map(|acquired| current_time - acquired)
                                .unwrap_or(0);

                            if lock_age > LOCK_TIMEOUT_MS {
                                warn!(
                                    "PREPARE: Reclaiming stale lock on cell (held for {}ms by {:?}), now claimed by {:?}",
                                    lock_age, owner, requester
                                );
                                meta.owner = None;
                                meta.lock_acquired_at = None;
                            } else if requester.compare_age(&owner).is_gt() {
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

                for op in &prepared_ops {
                    if !self.prepare_expectation_matches(op) {
                        debug!(
                            "PREPARE expectation mismatch for {:?} on cell {:?}: {:?}",
                            requester, op.id, op
                        );
                        break 'result DMPrepareResult::NotRealizable;
                    }
                }

                let lock_time = get_time();
                for meta in &mut cell_guards {
                    meta.owner = Some(requester.clone());
                    meta.lock_acquired_at = Some(lock_time);
                }

                txn.certified = prepared_ops_by_id;
                txn.affected_cells = txn.certified.keys().copied().collect();
                txn.coordinator_id = Some(coordinator_id);
                txn.participants = participants;
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
        clock: Hlc,
        tid: TxnId,
        cells: Vec<CommitOp>,
    ) -> BoxFuture<'_, DataSiteResponse<DMCommitResult>> {
        #[cfg(feature = "occ_phase_profile")]
        let phase_guard =
            super::phase_profile::guard(super::phase_profile::Phase::ParticipantCommit);
        self.update_clock(clock);
        // A commit is a write. It is admitted even while a fence is engaged
        // (the transaction prepared before the fence, and aborting a prepared
        // transaction here would leave its coordinator in doubt), but it is
        // counted, so the fencer's drain waits for it.
        let fence_guard = self.database_runtime.write_fence().count_if_open();

        let effective_ts = clock.max(tid);

        let Some(txn_lock) = self.find_transaction(&tid) else {
            return self.response_with(DMCommitResult::CheckFailed(CheckError::NotExisted));
        };

        if self.database_runtime.indexer().is_some() {
            let tid_for_logs = tid.clone();
            let scoped_commit = IndexBuilder::with_request_index_scope({
                let txn_lock = txn_lock.clone();
                let tid = tid.clone();
                let effective_ts = effective_ts.clone();
                move || self.apply_commit_ops(&txn_lock, &tid, &effective_ts, cells)
            });
            return async move {
                let _fence_guard = fence_guard;
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
        self.response_with(self.apply_commit_ops(&txn_lock, &tid, &effective_ts, cells))
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
        // The leases are held through the rollback and released only after
        // the spans are rewound; see `rollback_and_rewind`. They used to be
        // released FIRST, which handed every head back with the
        // transaction's BEGIN still open, so the next ordinary writer's
        // acknowledged entries landed inside a bracket recovery discards.
        let mut txn = txn_lock.lock();
        let result = self.abort_locked(&tid, &mut txn);
        drop(txn);
        self.response_with(result)
    }
    fn txn_status(&self, tid: TxnId) -> BoxFuture<'_, DataSiteResponse<TxnOutcome>> {
        let outcome = self.txn_outcome(&tid);
        async move { self.response_with(outcome).await }.boxed()
    }

    fn end(&self, clock: Hlc, tid: TxnId) -> BoxFuture<'_, DataSiteResponse<EndResult>> {
        debug!(">> END {:?}", tid);
        #[cfg(feature = "occ_phase_profile")]
        let phase_guard = super::phase_profile::guard(super::phase_profile::Phase::ParticipantEnd);
        self.update_clock(clock);

        // Option 6: Two-Phase Lock Release with Verification and Retry
        let (affected_cell_ids, txn_state, expected_owner) = {
            let Some(txn_lock) = self.find_transaction(&tid) else {
                return self.response_with(EndResult::CheckFailed(CheckError::NotExisted));
            };
            let txn = txn_lock.lock();
            if !(txn.state == TxnState::Aborted || txn.state == TxnState::Committed) {
                return self.response_with(EndResult::CheckFailed(CheckError::CannotEnd));
            }
            debug!(
                "AFFECTED: {}, {:?}, {:?}",
                txn.affected_cells.len(),
                txn.state,
                tid
            );
            (
                txn.affected_cells.clone(),
                txn.state,
                txn.coordinator_id
                    .map(|coordinator_id| TxnPriority::new(tid.clone(), coordinator_id)),
            )
        };

        // Attempt lock release with retries
        let mut retry_attempt = 0;
        let mut lock_release_result = None;

        while retry_attempt <= MAX_LOCK_RELEASE_RETRIES {
            if retry_attempt > 0 {
                debug!(
                    "Retrying lock release for {:?} (attempt {}/{})",
                    tid, retry_attempt, MAX_LOCK_RELEASE_RETRIES
                );
                std::thread::sleep(Duration::from_millis(LOCK_RELEASE_RETRY_BACKOFF_MS));
            }

            let (released_count, failed_releases) =
                self.attempt_lock_release(expected_owner.as_ref(), &affected_cell_ids);
            let total_cells = affected_cell_ids.len();

            if failed_releases.is_empty() {
                // All locks released successfully
                debug!(
                    "Successfully released all {} locks for {:?}, total locks: {}",
                    released_count,
                    tid,
                    self.txns.len()
                );
                lock_release_result = Some(Ok(()));
                break;
            } else if retry_attempt == MAX_LOCK_RELEASE_RETRIES {
                // Retries exhausted - CRITICAL ERROR
                error!(
                    "CRITICAL: Lock release retries exhausted for {:?}: {}/{} locks released, {} failures",
                    tid,
                    released_count,
                    total_cells,
                    failed_releases.len()
                );
                for failure in &failed_releases {
                    error!(
                        "  - Cell {:?} lock release failed: {}",
                        failure.cell_id, failure.reason
                    );
                }
                lock_release_result = Some(Err((released_count, total_cells, failed_releases)));
                break;
            } else {
                // Partial failure on this attempt, will retry
                error!(
                    "Lock release failed for {:?} on attempt {}/{}: {}/{} locks released, {} failures - retrying",
                    tid,
                    retry_attempt + 1,
                    MAX_LOCK_RELEASE_RETRIES + 1,
                    released_count,
                    total_cells,
                    failed_releases.len()
                );
                for failure in &failed_releases {
                    error!(
                        "  - Cell {:?} lock release failed: {}",
                        failure.cell_id, failure.reason
                    );
                }
            }

            retry_attempt += 1;
        }
        async move {
            #[cfg(feature = "occ_phase_profile")]
            let _phase_guard = phase_guard;
            // Release all segment references before wiping out transaction
            let guards_to_drop = {
                if let Some(txn_lock) = self.find_transaction(&tid) {
                    let mut txn = txn_lock.lock();
                    std::mem::take(&mut txn.segment_guards)
                } else {
                    Vec::new()
                }
            };
            drop(guards_to_drop); // Drop guards, releasing all segment references

            // The bracket closes HERE, with the decision.
            //
            // This is where a transaction is actually committed or aborted;
            // the misleadingly named `commit` above runs in the PREPARE phase
            // and only makes the ENTRIES durable. Writing the COMMIT there
            // would make the participant recoverable-as-committed before
            // anyone decided to commit -- and an abort that then could not
            // physically rewind (a sealed segment) would leave that COMMIT
            // standing, installing a transaction that was explicitly
            // refused.
            //
            // So a durable COMMIT now means exactly what 2PC needs it to
            // mean: this participant was told to commit. A participant that
            // crashes before its `end` arrives has no COMMIT, and recovery
            // discards its bracket -- the presumed-abort outcome, which is
            // safe on its own only while no OTHER participant has committed.
            // Closing that last gap needs the coordinator's decision to be
            // durable, or the participant set to be resolvable; both are
            // Phase 6 and neither is fixed by moving this write.
            if txn_state == TxnState::Committed {
                if let Err(reason) = self.close_committed_brackets(&tid) {
                    // Refusing is the honest answer: without a durable COMMIT
                    // the next restart discards this transaction, so telling
                    // the coordinator "ended" would be telling it something
                    // recovery will contradict.
                    error!(
                        "transaction {:?} could not close its bracket at the decision: {}",
                        tid, reason
                    );
                    self.cleanup_signal.store(true, Relaxed);
                    return self
                        .response_with(EndResult::CheckFailed(CheckError::CannotEnd))
                        .await;
                }
            }

            // Recorded only once the outcome is real: for a commit that means
            // after the bracket closed, because a failed close is refused
            // above and this site is then NOT committed.
            self.record_decision(
                &tid,
                if txn_state == TxnState::Committed {
                    TxnOutcome::Committed
                } else {
                    TxnOutcome::Aborted
                },
            );
            self.wipe_out_transaction(&tid);
            self.cleanup_signal.store(true, Relaxed);

            // Return appropriate result based on lock release outcome
            match lock_release_result {
                Some(Ok(())) => {
                    debug!("ENDED: {:?} with all locks released", tid);
                    self.response_with(EndResult::Success).await
                }
                Some(Err((released, total, failures))) => {
                    // Option 1: Hard error on lock release failure
                    error!(
                        "ENDED: {:?} with lock release failures ({}/{} released)",
                        tid, released, total
                    );
                    if failures.len() == total {
                        // Complete failure - retries exhausted
                        self.response_with(EndResult::LockReleaseRetriesExhausted { failures })
                            .await
                    } else {
                        // Partial failure
                        self.response_with(EndResult::SomeLocksNotReleased {
                            released,
                            total,
                            failures,
                        })
                        .await
                    }
                }
                None => {
                    // This shouldn't happen, but handle it gracefully
                    error!("ENDED: {:?} with unknown lock release status", tid);
                    self.response_with(EndResult::LockReleaseRetriesExhausted { failures: vec![] })
                        .await
                }
            }
        }
        .boxed()
    }
}
