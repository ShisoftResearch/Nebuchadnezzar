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
}

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
    rpc prepare(coordinator_id: u64, clock: Hlc, tid: TxnId, ops: Vec<PrepareOp>) -> DataSiteResponse<DMPrepareResult>;
    rpc commit(clock: Hlc, tid: TxnId, cells: Vec<CommitOp>) -> DataSiteResponse<DMCommitResult>;

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
        });
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            loop {
                if cleanup_signal.load(Relaxed) {
                    manager_clone.cell_meta_cleanup().await;
                    cleanup_signal.store(false, Relaxed);
                }
                // Reclaim write heads from transactions that went silent.
                //
                // A participant that aborted keeps its transaction (and its
                // heads) until `end` arrives; a coordinator that dies never
                // sends one. Without this the head is held for the life of
                // the process, and since the pool has no refill path that is
                // one writer slot gone from that chunk permanently. The
                // entries themselves stay exactly where they are and are
                // settled discard-unless-COMMIT, which is the same rule a
                // crash in the same window already gets.
                for tid in crate::ram::chunk::expire_transaction_leases(
                    txn_lease_timeout(),
                    &manager_clone.chunks().address_range(),
                ) {
                    manager_clone.wipe_out_transaction(&tid);
                }
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
        crate::ram::chunk::release_transaction_leases(tid, &self.chunks().address_range());
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
        let chunk = self.chunks().locate_chunk_by_partition(id.locality() as u64);
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
        txn.pinned_reads
            .insert(*id, PinnedRead { location: addr, version }, Some(guard));
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
            if let Some(cell_mutex) = self.cells.get(cell_id) {
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
        crate::ram::chunk::set_transaction_id(Some(tid.clone()));
        let mut write_error: Option<(Id, WriteError)> = None;
        {
            for cell_op in cells {
                match cell_op {
                    CommitOp::Read(_, _) => continue,
                    CommitOp::None => unreachable!("commit payload validation rejects None ops"),
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
                        let chunk = self.chunks().locate_chunk_by_partition(cell_id.locality() as u64);
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


                        match self
                            .chunks()
                            .remove_cell_by(cell_id, |cell| cell.header.version == expected_version)
                        {
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
                        let chunk = self.chunks().locate_chunk_by_partition(cell_id.locality() as u64);
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
        txn.last_activity = get_time();
        crate::ram::chunk::set_transaction_id(None);

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
            // The undo entries are still there and still incomplete, so
            // recovery rolls this transaction back -- which is the outcome
            // the caller is now told about.
            error!(
                "Transaction {:?} could not be made durable at commit: {:?}. Reporting failure \
                 so it is not treated as committed.",
                tid, error
            );
            txn.state = TxnState::Aborted;
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
    use crate::server::transactions::test_hlc;
    use crate::ram::cell::OwnedCell;
    use crate::ram::schema::Schema;
    use crate::ram::segs::SEGMENT_SIZE;
    use crate::ram::tests::default_fields;
    use crate::ram::types::{Id, OwnedMap, OwnedValue};
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
        assert_eq!(set.get(&Id::allocated(0, 0, 42)).map(|p| p.version), Some(7));
        assert_eq!(set.get(&Id::allocated(0, 0, 42)).map(|p| p.location), Some(0x1000));
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
                chunk_size: SEGMENT_SIZE,
                db_size: SEGMENT_SIZE,
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
        OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(data))
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
        OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(data))
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
        <DataManager as Service>::prepare(manager, coordinator_id, tid.clone(), tid.clone(), ops)
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
        let version = seed_cell_version(&runtime, schema.id, cell_id, 3, 0);
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
            )
            .await
            .unwrap()
            .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
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
        let mut cell_a = counter_cell(schema.id, cell_id, score_a, "A");
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
        let mut cell_b = counter_cell(schema.id, cell_id, score_b, "B");
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
        let read_current =
            <DataManager as Service>::read(&manager, 42, other_tid.clone(), other_tid.clone(), cell_id)
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
        let mut cell = counter_cell(schema.id, cell_id, 500, "A");
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
        let mut cell_a = counter_cell(schema.id, cell_id, score_a, "A");
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
        let mut cell_b = counter_cell(schema.id, cell_id, score_b, "B");
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
        seed_cell_version(&runtime, schema.id, cell_id, 4, 0);
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
        let version = seed_cell_version(&runtime, schema.id, cell_id, 5, 0);
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_retry_ops_mismatch";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::allocated(0, 0, 99006);
        let cell_b = Id::allocated(0, 0, 99007);
        let version_a = seed_cell_version(&runtime, schema.id, cell_a, 7, 0);
        let version_b = seed_cell_version(&runtime, schema.id, cell_b, 8, 0);
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_retry_coordinator_mismatch";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 99008);
        let version = seed_cell_version(&runtime, schema.id, cell_id, 9, 0);
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_retry_idempotent";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::allocated(0, 0, 99009);
        let cell_b = Id::allocated(0, 0, 99010);
        let version_a = seed_cell_version(&runtime, schema.id, cell_a, 10, 0);
        let version_b = seed_cell_version(&runtime, schema.id, cell_b, 11, 0);
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_retry_foreign_owner";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::allocated(0, 0, 99011);
        let version = seed_cell_version(&runtime, schema.id, cell_id, 12, 0);
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
        let address = &crate::utils::test_port::unique_localhost_addr();
        let group = "txn_data_site_prepare_wait_die_clock";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let client = data_site_client_for_database(address, group, group).await;
        let cell_id = Id::allocated(0, 0, 99005);
        let version = seed_cell_version(&runtime, schema.id, cell_id, 6, 0);
        let older_tid = test_hlc(1, 11);
        let younger_tid = test_hlc(1, 22);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(version),
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
        seed_cell_version(&runtime, schema.id, cell_id, 7, 0);
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
        let tids = (0..64)
            .map(|_| manager.hlc.now())
            .collect::<Vec<_>>();

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
        let write_version = seed_cell_version(&runtime, schema.id, write_id, 7, 0);
        let read_version = seed_cell_version(&runtime, schema.id, read_id, 9, 0);
        let read_only_version = seed_cell_version(&runtime, schema.id, read_only_id, 11, 0);

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
        let initial_version = seed_cell_version(&runtime, schema.id, cell_id, initial_score, 0);
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
        let initial_version = seed_cell_version(&runtime, schema.id, cell_id, initial_score, 0);
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
                schema.id,
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
        let initial_version = seed_cell_version(&runtime, schema.id, cell_id, initial_score, 0);
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
        )
        .await
        .payload;
        assert_eq!(prepare, DMPrepareResult::Success);

        let commit = <DataManager as Service>::commit(
            &manager,
            tid.clone(),
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
        let initial_version = seed_cell_version(&runtime, schema.id, cell_id, initial_score, 0);
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
        )
        .await
        .payload;
        assert_eq!(prepare_t1, DMPrepareResult::Success);

        let commit_t1 = <DataManager as Service>::commit(
            &manager,
            t1.clone(),
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
        let initial_version = seed_cell_version(&runtime, schema.id, cell_id, 0, 0);
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
        let initial_version = seed_cell_version(&runtime, schema.id, cell_id, 1, 0);
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

        let mut external = counter_cell(schema.id, cell_id, 7, "counter_commit_external");
        let external_header = runtime.chunks().update_cell(&mut external).unwrap();
        assert!(external_header.version > initial_version);

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
        let initial_version = seed_cell_version(&runtime, schema.id, cell_id, 2, 0);
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

        let mut external = counter_cell(schema.id, cell_id, 8, "counter_remove_external");
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
        let initial_version = seed_cell_version(&runtime, schema.id, cell_id, 6, 0);
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
        let version_a = seed_cell_version(&runtime, schema.id, cell_a, 3, 0);
        let version_b = seed_cell_version(&runtime, schema.id, cell_b, 4, 0);
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
        let version_a = seed_cell_version(&runtime, schema.id, cell_a, 15, 0);
        let version_b = seed_cell_version(&runtime, schema.id, cell_b, 25, 0);
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
        assert!(external_b_header.version > version_b);

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
        let address = &crate::utils::test_port::unique_localhost_addr();
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
        let insert_id = Id::allocated(0, 0, 90501);
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
    fn abort(
        &self,
        clock: Hlc,
        tid: TxnId,
    ) -> BoxFuture<'_, DataSiteResponse<AbortResult>> {
        debug!(">> ABORT {:?}", tid);
        #[cfg(feature = "occ_phase_profile")]
        let _phase_guard =
            super::phase_profile::guard(super::phase_profile::Phase::ParticipantAbort);
        self.update_clock(clock);
        let Some(txn_lock) = self.find_transaction(&tid) else {
            return self.response_with(AbortResult::CheckFailed(CheckError::NotExisted));
        };
        let mut txn = txn_lock.lock();
        if txn.state == TxnState::Aborted {
            return self.response_with(AbortResult::CheckFailed(CheckError::AlreadyAborted));
        }
        #[cfg(test)]
        if Self::take_matching_abort_cannot_end(&tid, &txn.affected_cells) {
            return self.response_with(AbortResult::CheckFailed(CheckError::CannotEnd));
        }

        if txn.history.is_empty() {
            let guards_to_drop = std::mem::take(&mut txn.segment_guards);
            txn.last_activity = get_time();
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

        // Keep segment references and metadata guards alive throughout rollback so the owner
        // cannot be reclaimed between validation and the storage mutations.
        let guards_to_drop = std::mem::take(&mut txn.segment_guards);

        // Give the write heads back BEFORE rolling back.
        //
        // The lease keeps a transaction's entries contiguous until its
        // decision, so that a COMMIT closes a bracket containing nothing but
        // its own entries. This IS the decision and it is an abort: no COMMIT
        // will ever be written, the bracket stays uncommitted, and recovery
        // discards it whatever lands after it. There is nothing left to
        // protect.
        //
        // Holding on, meanwhile, starves the rollback that must run right
        // now: restoring old versions and writing tombstones are ordinary
        // writes, and in a chunk whose heads this transaction owns they have
        // nowhere to go. That is a participant deadlocking itself -- found
        // with gdb on a transaction suite stuck for over an hour, one thread
        // spinning for a head its own aborting transaction still held.
        crate::ram::chunk::release_transaction_leases(&tid, &self.chunks().address_range());

        let rollback_failures = {
            debug!(
                ">>>>>>>>>> ROLLING BACK FOR {:?} CELLS {:?}",
                txn.history.len(),
                tid
            );
            let failures = self.rollback(&txn.history);
            if failures.len() == 0 {
                None
            } else {
                Some(failures)
            }
        };
        txn.last_activity = get_time();
        txn.state = TxnState::Aborted;

        // NO physical rewind here any more.
        //
        // Padding over the transaction's span and resetting the cursor was
        // only safe while the transaction still OWNED the segment, and the
        // lease now ends above, before rollback, because holding it that long
        // deadlocked the rollback. Rewinding after the release would pad over
        // whatever another writer appended in between.
        //
        // Nothing is lost but the space: an aborted transaction writes no
        // COMMIT, so recovery discards its bracket, and its entries are dead
        // space the cleaner reclaims like any other.

        drop(cell_guards);
        drop(txn);
        drop(guards_to_drop);

        self.response_with(AbortResult::Success(rollback_failures))
    }
    fn end(
        &self,
        clock: Hlc,
        tid: TxnId,
    ) -> BoxFuture<'_, DataSiteResponse<EndResult>> {
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
                crate::ram::chunk::set_transaction_id(Some(tid.clone()));
                let bracket_result = self.chunks().commit_transaction_brackets(&tid);
                crate::ram::chunk::set_transaction_id(None);
                if let Err(error) = bracket_result {
                    // Refusing is the honest answer: without a durable COMMIT
                    // the next restart discards this transaction, so telling
                    // the coordinator "ended" would be telling it something
                    // recovery will contradict.
                    error!(
                        "transaction {:?} could not close its bracket at the decision: {:?}",
                        tid, error
                    );
                    self.cleanup_signal.store(true, Relaxed);
                    return self.response_with(EndResult::CheckFailed(CheckError::CannotEnd)).await;
                }
            }

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
