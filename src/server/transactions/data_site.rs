use super::*;
use crate::ram::cell::{header_from_chunk_raw, OwnedCellRef};
use crate::ram::segs::SegmentReferenceGuard;
use crate::ram::types::Id;
use crate::server::{DatabaseRuntime, Peer};
use crate::{
    index::builder::IndexBuilder,
    ram::cell::{CellHeader, OwnedCell, ReadError, WriteError},
};
use bifrost::utils::time::get_time;
use bifrost::vector_clock::StandardVectorClock;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use futures::future::BoxFuture;
use lightning::linked_list::LinkedList;
use lightning::map::Map;
use lightning::map::PtrHashMap as LFMap;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::Duration;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TXN_DATA_MANAGER_RPC_SERVICE) as u64;

pub fn generate_scoped_service_id(group: &str, database_name: &str) -> u64 {
    hash_str(&format!(
        "TXN_DATA_MANAGER_RPC_SERVICE-{}-{}",
        group, database_name
    ))
}

// Lock timeout in milliseconds - locks held longer than this are considered stale
// and can be reclaimed (default: 30 seconds)
const LOCK_TIMEOUT_MS: i64 = 30_000;

// Maximum retries for lock release (Option 6: Two-Phase Lock Release)
const MAX_LOCK_RELEASE_RETRIES: usize = 3;

// Backoff between lock release retries in milliseconds
const LOCK_RELEASE_RETRY_BACKOFF_MS: u64 = 100;

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
    owner: Option<TxnId>, // transaction that owns the cell (write lock) during prepare/commit
    lock_acquired_at: Option<i64>, // timestamp when lock was acquired (milliseconds since epoch)
}

struct Transaction {
    state: TxnState,
    affected_cells: Vec<Id>,
    last_activity: i64,
    history: CommitHistory,
    /// RAII guards that hold segment references during this transaction
    /// Automatically released when guards are dropped (no leak risk)
    segment_guards: Vec<SegmentReferenceGuard>,
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

pub struct DataManager {
    cells: LFMap<Id, Arc<Mutex<CellMeta>>>,
    txns: LFMap<TxnId, Arc<Mutex<Transaction>>>,
    cell_list: LinkedList<Id>,
    txns_sorted: Mutex<BTreeSet<TxnId>>,
    database_runtime: Arc<DatabaseRuntime>,
    txn_peer: Peer,
    cleanup_signal: Arc<AtomicBool>,
}

service! {
    rpc read(server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id) -> DataSiteResponse<TxnExecResult<OwnedCell, ReadError>>;
    rpc read_selected(server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id, fields: Vec<u64>) -> DataSiteResponse<TxnExecResult<OwnedCell, ReadError>>;
    rpc read_partial_raw(server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id, offset: usize, len: usize) -> DataSiteResponse<TxnExecResult<Vec<u8>, ReadError>>;
    rpc head(server_id: u64, clock: StandardVectorClock, tid: TxnId, id: Id) -> DataSiteResponse<TxnExecResult<CellHeader, ReadError>>;
    // two phase commit
    rpc prepare(server_id: u64, clock :StandardVectorClock, tid: TxnId, cell_ids: Vec<Id>) -> DataSiteResponse<DMPrepareResult>;
    rpc commit(clock :StandardVectorClock, tid: TxnId, cells: Vec<CommitOp>) -> DataSiteResponse<DMCommitResult>;

    // because there may be some exception on commit, abort have to handle 'committed' and 'committing' transactions
    // for committed transaction, abort need to recover the data according to it's cells history
    rpc abort(clock :StandardVectorClock, tid: TxnId) -> DataSiteResponse<AbortResult>;

    // there also should be a 'end' from transaction manager to inform data manager to clean up and release cell locks
    rpc end(clock :StandardVectorClock, tid: TxnId) -> DataSiteResponse<EndResult>;
}

dispatch_rpc_service_functions!(DataManager);

service_with_id!(DataManager, DEFAULT_SERVICE_ID);

impl DataManager {
    pub fn new(database_runtime: Arc<DatabaseRuntime>, txn_peer: Peer) -> Arc<Self> {
        let cleanup_signal = Arc::new(AtomicBool::new(false));
        let manager = Arc::new(Self {
            cells: LFMap::with_capacity(256),
            txns: LFMap::with_capacity(128),
            cell_list: LinkedList::new(),
            txns_sorted: Mutex::new(BTreeSet::new()),
            database_runtime,
            txn_peer,
            cleanup_signal: cleanup_signal.clone(),
        });
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
    fn update_clock(&self, clock: &StandardVectorClock) {
        self.txn_peer.clock.merge_with(clock);
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
                last_activity: get_time(),
                history: BTreeMap::new(),
                segment_guards: Vec::with_capacity(4), // Pre-allocate for common case
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
                read: TxnId::new(),
                write: TxnId::new(),
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
        future::ready(DataSiteResponse::new(&self.txn_peer, data)).boxed()
    }

    #[inline]
    fn chunks(&self) -> &Arc<crate::ram::chunk::Chunks> {
        self.database_runtime.chunks()
    }

    #[inline]
    fn undo_log(&self) -> Option<&Arc<super::undo_log::UndoLogger>> {
        self.database_runtime.undo_log()
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
    fn update_cell_write(&self, cell_id: &Id, tid: &TxnId) {
        let meta_ref = self.cell_meta_mutex(cell_id);
        let mut meta = meta_ref.lock();
        meta.write = tid.clone();
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
                .unwrap_or_else(|| self.txn_peer.clock.to_clock())
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
        for (cell_id_ref, cell_meta) in cells_to_evict {
            let cell_id = cell_id_ref.deref();

            // Re-check timestamps while holding lock before removal
            // This prevents evicting cells that became active after first check
            let should_evict = {
                let meta = cell_meta.lock();
                meta.write < oldest_transaction
                    && meta.read < oldest_transaction
                    && meta.owner.is_none() // Don't evict if locked by a transaction
            };

            if should_evict {
                self.cells.remove(&cell_id);
                cell_id_ref.remove();
            }
        }
    }
    fn prepare_read<T: Send>(
        &self,
        clock: &StandardVectorClock,
        tid: &TxnId,
        id: &Id,
    ) -> Result<(), BoxFuture<'_, DataSiteResponse<TxnExecResult<T, ReadError>>>>
    where
        T: 'static + Clone,
    {
        self.update_clock(clock);
        let txn_lock = self.get_or_create_transaction(tid);
        let mut txn = txn_lock.lock();
        let meta_ref = self.cell_meta_mutex(id);
        let mut meta = meta_ref.lock();
        let committing = meta.owner.is_some();

        // Use the more recent timestamp between the transaction ID and the incoming clock
        // This allows transactions to proceed even if their original timestamp is stale
        // due to clock updates from concurrent transactions
        let effective_ts = if clock > tid { clock } else { tid };
        let read_too_late = &meta.write > effective_ts;

        txn.last_activity = get_time();
        if txn.state != TxnState::Started {
            return Err(self.response_with(TxnExecResult::StateError(txn.state)));
        }

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

    /// Attempt to release locks for a transaction
    /// Returns (number of locks released, Vec of failures)
    /// Option 5: Ensures metadata isn't cleaned up while locks are held
    /// Option 6: Provides detailed failure information for retry logic
    fn attempt_lock_release(
        &self,
        tid: &TxnId,
        affected_cell_ids: &[Id],
    ) -> (usize, Vec<LockReleaseFailure>) {
        let mut released_count = 0;
        let mut failures = Vec::new();
        let current_time = get_time();

        for cell_id in affected_cell_ids {
            if let Some(cell_mutex) = self.cells.get(cell_id) {
                let mut meta = cell_mutex.lock();

                // Verify this transaction owns the lock
                match &meta.owner {
                    Some(owner_tid) if owner_tid == tid => {
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
                            cell_id, tid, lock_age
                        );
                    }
                    Some(other_tid) => {
                        // Lock owned by different transaction - this is a problem
                        let reason =
                            format!("Cell lock owned by different transaction: {:?}", other_tid);
                        warn!(
                            "Cannot release lock on cell {:?} for {:?}: {}",
                            cell_id, tid, reason
                        );
                        failures.push(LockReleaseFailure {
                            cell_id: *cell_id,
                            reason,
                        });
                    }
                    None => {
                        // Lock not held - might have been released already or never acquired
                        debug!(
                            "Lock on cell {:?} not held by {:?} (already released or never acquired)",
                            cell_id, tid
                        );
                        // Don't count as failure if lock was already released
                        released_count += 1;
                    }
                }
            } else {
                // Cell metadata not found - Option 5: Metadata cleanup protection
                let reason = "Cell metadata not found (may have been cleaned up)".to_string();
                warn!(
                    "Cannot release lock on cell {:?} for {:?}: {}",
                    cell_id, tid, reason
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

        let prepared_cells_num = txn.affected_cells.len();
        let arrived_cells_num = cells.len();
        if arrived_cells_num > prepared_cells_num {
            return DMCommitResult::CheckFailed(CheckError::CellNumberDoesNotMatch(
                prepared_cells_num,
                arrived_cells_num,
            ));
        }
        let prepared_cell_ids: BTreeSet<_> = txn.affected_cells.iter().copied().collect();
        let mut committed_cell_ids = BTreeSet::new();
        for cell_id in cells.iter().map(Self::commit_op_cell_id) {
            if !prepared_cell_ids.contains(&cell_id) || !committed_cell_ids.insert(cell_id) {
                return DMCommitResult::CheckFailed(CheckError::CellNumberDoesNotMatch(
                    prepared_cells_num,
                    arrived_cells_num,
                ));
            }
        }

        crate::ram::chunk::set_transaction_context(true);
        let mut write_error: Option<(Id, WriteError)> = None;
        {
            for cell_op in cells {
                match cell_op {
                    CommitOp::Read(_id, _version) => {}
                    CommitOp::Write(mut cell) => {
                        let cell_id = cell.id();
                        let (should_skip, write_ts) = {
                            let meta_ref = self.cell_meta_mutex(&cell_id);
                            let meta = meta_ref.lock();
                            (effective_ts < &meta.write, meta.write.clone())
                        };

                        if should_skip {
                            debug!(
                                "Thomas Write Rule: Skipping obsolete write for cell {:?} (effective ts {:?} < write timestamp {:?})",
                                cell_id, effective_ts, write_ts
                            );
                            continue;
                        }

                        match self.chunks().write_cell(&mut cell) {
                            Ok(header) => {
                                if let Some(undo_log) = self.undo_log() {
                                    let undo_entry = super::undo_log::UndoLogEntry::new_write(
                                        tid.clone(),
                                        cell_id,
                                        header.version,
                                    );
                                    if let Err(error) = undo_log.write_undo_entry(undo_entry) {
                                        error!(
                                            "Failed to write undo log entry for new cell: {:?}",
                                            error
                                        );
                                    }
                                }
                                txn.history
                                    .insert(cell_id, CellHistory::new(None, header.version));
                                self.update_cell_write(&cell_id, effective_ts);
                            }
                            Err(error) => {
                                write_error = Some((cell.id(), error));
                                break;
                            }
                        };
                    }
                    CommitOp::Remove(ref cell_id) => {
                        let (cell_addr, orig_version, old_cell_ref) = {
                            let shared_cell = match self.chunks().read_cell(cell_id) {
                                Ok(cell) => cell,
                                Err(read_error) => {
                                    write_error =
                                        Some((*cell_id, WriteError::ReadError(read_error)));
                                    break;
                                }
                            };
                            let addr = shared_cell.cell_guard().get_ptr();
                            let version = shared_cell.header.version;
                            let cell_ref = shared_cell.to_owned().into_ref();
                            (addr, version, cell_ref)
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
                                orig_version,
                                chunk_idx as u64,
                                seq_id,
                                cell_offset,
                            );
                            if let Err(error) = undo_log.write_undo_entry(undo_entry) {
                                error!("Failed to write undo log entry: {:?}", error);
                            }
                        }

                        match self
                            .chunks()
                            .remove_cell_by(cell_id, |cell| cell.header.version == orig_version)
                        {
                            Ok(()) => {
                                txn.history
                                    .insert(*cell_id, CellHistory::new(Some(old_cell_ref), 0));
                                self.update_cell_write(cell_id, effective_ts);
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
                        let (cell_addr, orig_version) = {
                            match self.chunks().location_for_read(&cell_id).and_then(|loc| {
                                let addr = *loc;
                                let header = header_from_chunk_raw(addr);
                                header.map(|(header, _)| (header, addr))
                            }) {
                                Ok((header, addr)) => (addr, header.version),
                                Err(read_error) => {
                                    write_error =
                                        Some((cell_id, WriteError::ReadError(read_error)));
                                    break;
                                }
                            }
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
                                orig_version,
                                chunk_idx as u64,
                                seq_id,
                                cell_offset,
                            );
                            if let Err(error) = undo_log.write_undo_entry(undo_entry) {
                                error!("Failed to write undo log entry: {:?}", error);
                            }
                        }
                        cell.header.version = orig_version;
                        let mut old_cell_ref = None;
                        match self.chunks().update_cell_by(&cell_id, |cell_to_update| {
                            if cell_to_update.header.version == orig_version {
                                old_cell_ref = Some((*cell_to_update).to_owned().into_ref());
                                Some(cell)
                            } else {
                                None
                            }
                        }) {
                            Ok(cell) => {
                                debug_assert!(old_cell_ref.is_some());
                                txn.history.insert(
                                    cell_id,
                                    CellHistory::new(old_cell_ref, cell.header.version),
                                );
                                self.update_cell_write(&cell_id, effective_ts);
                                txn.segment_guards.push(guard);
                            }
                            Err(error) => {
                                write_error = Some((cell_id, error));
                                break;
                            }
                        }
                    }
                    CommitOp::None => panic!("None CommitOp should not appear in data site"),
                }
            }
        }
        txn.last_activity = get_time();
        crate::ram::chunk::set_transaction_context(false);

        if let Some((id, error)) = write_error {
            let guards_to_drop = std::mem::take(&mut txn.segment_guards);
            drop(txn);
            drop(guards_to_drop);
            return match error {
                WriteError::DeletionPredictionFailed | WriteError::UserCanceledUpdate => {
                    DMCommitResult::CellChanged(id)
                }
                _ => DMCommitResult::WriteError(id, error),
            };
        }

        txn.state = TxnState::Committed;
        for guard in &txn.segment_guards {
            let chunk_idx = guard.chunk_id();
            let seg_id = guard.segment_id();
            let chunk = &self.chunks().list[chunk_idx];
            if let Some(segment) = chunk.segs.get(&(seg_id as usize)) {
                if let Err(error) = segment.force_wal_sync() {
                    error!(
                        "Failed to sync WAL for segment {} during commit: {:?}",
                        seg_id, error
                    );
                } else {
                    debug!(
                        "Synced segment {} (chunk {}) WAL to disk for transaction commit",
                        seg_id, chunk_idx
                    );
                }
            }
        }

        DMCommitResult::Success
    }

    fn commit_op_cell_id(op: &CommitOp) -> Id {
        match op {
            CommitOp::Write(cell) | CommitOp::Update(cell) => cell.id(),
            CommitOp::Remove(id) | CommitOp::Read(id, _) => *id,
            CommitOp::None => panic!("None CommitOp should not appear in data site"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::segs::SEGMENT_SIZE;
    use crate::server::{NebServer, ServerOptions, Service as NebService};
    use futures::future::join_all;
    use std::sync::Arc;

    #[test]
    fn scoped_data_manager_service_ids_differ_between_databases() {
        let group = "group_a";
        assert_ne!(
            generate_scoped_service_id(group, "db_a"),
            generate_scoped_service_id(group, "db_b")
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

    async fn data_manager_for_database(
        server: &Arc<NebServer>,
        address: &str,
        database_name: &str,
    ) -> Arc<DataManager> {
        let runtime = server
            .ensure_database_runtime(database_name)
            .await
            .expect("database runtime");
        DataManager::new(runtime, crate::server::Peer::new(&address.to_string()))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_get_transaction_returns_single_shared_entry_per_tid() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5290";
        let group = "txn_data_site_same_tid";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = manager.txn_peer.clock.inc();

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
    async fn end_cleans_up_many_active_transactions_without_leaking_bookkeeping() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5291";
        let group = "txn_data_site_cleanup";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tids = (0..64)
            .map(|_| manager.txn_peer.clock.inc())
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
        let address = "127.0.0.1:5292";
        let group = "txn_data_site_multidb";
        let server = start_transaction_test_server(address, group).await;
        let default_manager = data_manager_for_database(&server, address, group).await;
        let analytics_manager = data_manager_for_database(&server, address, "analytics").await;
        let shared_tid = default_manager.txn_peer.clock.inc();

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
        let tid = manager.txn_peer.clock.inc();
        manager.get_or_create_transaction(&tid).lock().state = TxnState::Aborted;

        let end_clock = manager.txn_peer.clock.to_clock();
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
}

impl Service for DataManager {
    /////////////////////////////////////
    ///        Implement Services    ///
    ///////////////////////////////////

    fn read(
        &self,
        _server_id: u64,
        clock: StandardVectorClock,
        tid: TxnId,
        id: Id,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<OwnedCell, ReadError>>> {
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            r
        } else {
            match self.chunks().read_cell(&id) {
                Ok(cell) => self.response_with(TxnExecResult::Accepted(cell.to_owned())),
                Err(read_error) => self.response_with(TxnExecResult::Error(read_error)),
            }
        }
    }
    fn read_selected(
        &self,
        _server_id: u64,
        clock: StandardVectorClock,
        tid: TxnId,
        id: Id,
        fields: Vec<u64>,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<OwnedCell, ReadError>>> {
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
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
        clock: StandardVectorClock,
        tid: TxnId,
        id: Id,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<CellHeader, ReadError>>> {
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        match self.chunks().head_cell(&id) {
            Ok(head) => self.response_with(TxnExecResult::Accepted(head)),
            Err(read_error) => self.response_with(TxnExecResult::Error(read_error)),
        }
    }
    // TODO: Link this function in transaction manager
    fn read_partial_raw(
        &self,
        _server_id: u64,
        clock: StandardVectorClock,
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
        _server_id: u64,
        clock: StandardVectorClock,
        tid: TxnId,
        cell_ids: Vec<Id>,
    ) -> BoxFuture<'_, DataSiteResponse<DMPrepareResult>> {
        // PREPARE PHASE: Two-Phase Commit with Wait-Die Concurrency Control
        //
        // This function implements the first phase of 2PC with a hybrid protocol:
        // 1. Wait-Die lock-based conflict resolution (NEW)
        // 2. Timestamp-ordering validation (EXISTING)
        //
        // Wait-Die Protocol (prevents deadlock on lock conflicts):
        // - If cell.owner exists and != tid:
        //   - Younger txn (tid > owner): DIE → return NotRealizable (abort)
        //   - Older txn (tid < owner): WAIT → return Wait (TM will backoff & retry)
        // - This reduces cascading aborts on hot cells vs pure timestamp ordering
        //
        // Timestamp Ordering (validates serializability and linearizability - STRICT):
        // - Check tid >= meta.read (write-after-read constraint)
        // - Check tid >= meta.write (write-after-write constraint)
        // - Ensures strict ordering and prevents lost updates
        //
        // Lock Acquisition:
        // - If all checks pass, set meta.owner = Some(tid) for each cell
        // - cell_ids must be sorted to avoid deadlock on mutex acquisition
        // - Locks are released in the `end` phase after commit/abort
        debug!("PREPARE FOR {:?}, {} cells", &tid, cell_ids.len());
        self.update_clock(&clock);

        // A write-only transaction may reach a data site for the first time at prepare.
        // In that case we still need local state to acquire locks and track commit history.
        let txn_lock = self.get_or_create_transaction(&tid);
        let mut txn = txn_lock.lock();
        if txn.state != TxnState::Started && txn.state != TxnState::Prepared {
            return self.response_with(DMPrepareResult::StateError(txn.state));
        }

        let mut cell_mutices = Vec::with_capacity(cell_ids.len());
        let mut cell_guards = Vec::with_capacity(cell_ids.len());

        for cell_id in &cell_ids {
            cell_mutices.push(self.cell_meta_mutex(cell_id));
        }
        for cell_mutex in &cell_mutices {
            let mut meta = cell_mutex.lock();
            let current_time = get_time();

            // Wait-Die Protocol: Check if another transaction owns this cell
            // This implements lock-based concurrency control to reduce timestamp-ordering aborts
            if let Some(ref owner_tid) = meta.owner {
                if owner_tid != &tid {
                    // Option 2: Check if lock has timed out (stale lock detection)
                    let lock_age = meta
                        .lock_acquired_at
                        .map(|acquired| current_time - acquired)
                        .unwrap_or(0);

                    if lock_age > LOCK_TIMEOUT_MS {
                        // Lock is stale - reclaim it
                        warn!(
                            "PREPARE: Reclaiming stale lock on cell (held for {}ms by {:?}), now claimed by {:?}",
                            lock_age, owner_tid, tid
                        );
                        meta.owner = None;
                        meta.lock_acquired_at = None;
                        // Continue to acquire the lock below
                    } else if tid > *owner_tid {
                        // Younger transaction "dies" (aborts immediately)
                        debug!(
                            "PREPARE Wait-Die: younger txn {:?} aborted, cell owned by older {:?} (lock age: {}ms)",
                            tid, owner_tid, lock_age
                        );
                        return self.response_with(DMPrepareResult::NotRealizable);
                    } else {
                        // Older transaction waits for younger owner to release
                        debug!(
                            "PREPARE Wait-Die: older txn {:?} waits for younger owner {:?} (lock age: {}ms)",
                            tid, owner_tid, lock_age
                        );
                        return self.response_with(DMPrepareResult::Wait);
                    }
                }
            }

            // Timestamp ordering validation (STRICT)
            // Ensures strict serializability and linearizability
            //
            // Read-Write Conflict Check:
            // Enforce write-after-read constraint - prevents writing with older
            // timestamp than existing reads
            if tid < meta.read {
                debug!(
                    "PREPARE: Write too late for {:?} (tid: {:?}), cell read timestamp: {:?}",
                    tid, tid, meta.read
                );
                break;
            }

            // Write-Write Conflict Check (STRICT):
            // Enforce write-after-write constraint - prevents timestamp inversions
            // This ensures:
            // - Linearizability (real-time ordering preserved)
            // - No lost updates for counters
            // - No lost edges in edge lists
            // - Safe for quota enforcement
            if tid < meta.write {
                debug!(
                    "PREPARE: Write conflict for {:?} (tid: {:?}), cell write timestamp: {:?}",
                    tid, tid, meta.write
                );
                break;
            }

            cell_guards.push(meta);
        }
        if cell_guards.len() != cell_ids.len() {
            debug!(
                "SITE PREPARE CELL GUARD MISMATCH: {} expecting {}",
                cell_ids.len(),
                cell_guards.len()
            );
            return self.response_with(DMPrepareResult::NotRealizable); // need retry
        } else {
            let lock_time = get_time();
            for mut meta in cell_guards {
                meta.owner = Some(tid.clone()); // set owner to lock this cell
                meta.lock_acquired_at = Some(lock_time); // Option 1: Record lock acquisition time
                debug!("Lock acquired for cell by {:?} at {}", tid, lock_time);
            }
            txn.state = TxnState::Prepared;
            txn.affected_cells = cell_ids; // for cell number check
            txn.last_activity = get_time(); // check if transaction timeout
            debug!("SITE PREPARE SUCCESSFUL FOR {:?}", tid);
            return self.response_with(DMPrepareResult::Success);
        }
    }
    fn commit(
        &self,
        clock: StandardVectorClock,
        tid: TxnId,
        cells: Vec<CommitOp>,
    ) -> BoxFuture<'_, DataSiteResponse<DMCommitResult>> {
        self.update_clock(&clock);

        let effective_ts = if clock > tid {
            clock.clone()
        } else {
            tid.clone()
        };

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
                let (payload, request_results) = scoped_commit.await;
                let pending_results = IndexBuilder::await_indices().await;
                self.warn_on_index_wait_results(
                    &tid_for_logs,
                    request_results
                        .into_iter()
                        .chain(pending_results.into_iter()),
                );
                DataSiteResponse::new(&self.txn_peer, payload)
            }
            .boxed();
        }

        self.response_with(self.apply_commit_ops(&txn_lock, &tid, &effective_ts, cells))
    }
    fn abort(
        &self,
        clock: StandardVectorClock,
        tid: TxnId,
    ) -> BoxFuture<'_, DataSiteResponse<AbortResult>> {
        debug!(">> ABORT {:?}", tid);
        self.update_clock(&clock);
        let Some(txn_lock) = self.find_transaction(&tid) else {
            return self.response_with(AbortResult::CheckFailed(CheckError::NotExisted));
        };
        let mut txn = txn_lock.lock();
        if txn.state == TxnState::Aborted {
            return self.response_with(AbortResult::CheckFailed(CheckError::AlreadyAborted));
        }

        // Move guards out before rollback (they need to stay alive during rollback)
        let guards_to_drop = std::mem::take(&mut txn.segment_guards);

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

        // Release segment references after marking as aborted and rollback complete
        drop(txn); // Release the lock first
        drop(guards_to_drop); // Then drop guards, releasing all segment references

        self.response_with(AbortResult::Success(rollback_failures))
    }
    fn end(
        &self,
        clock: StandardVectorClock,
        tid: TxnId,
    ) -> BoxFuture<'_, DataSiteResponse<EndResult>> {
        debug!(">> END {:?}", tid);
        self.update_clock(&clock);

        // Option 6: Two-Phase Lock Release with Verification and Retry
        let (affected_cell_ids, txn_state) = {
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
            (txn.affected_cells.clone(), txn.state)
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
                self.attempt_lock_release(&tid, &affected_cell_ids);
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

            // Write commit/abort marker to undo log based on transaction state
            if let Some(undo_log) = self.undo_log() {
                let log_result = match txn_state {
                    TxnState::Committed => undo_log.write_commit_marker(&tid),
                    TxnState::Aborted => undo_log.write_abort_marker(&tid),
                    _ => Ok(()), // No marker needed for other states
                };
                if let Err(e) = log_result {
                    error!("Failed to write transaction completion marker: {:?}", e);
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
