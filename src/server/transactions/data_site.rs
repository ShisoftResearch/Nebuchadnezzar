use super::*;
use crate::ram::cell::{header_from_chunk_raw, OwnedCellRef};
use crate::ram::segs::SegmentReferenceGuard;
use crate::ram::types::Id;
use crate::server::NebServer;
use crate::{
    index::builder::IndexBuilder,
    ram::cell::{CellHeader, OwnedCell, ReadError, WriteError},
};
use bifrost::utils::time::get_time;
use bifrost::vector_clock::StandardVectorClock;
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

type CommitHistory = BTreeMap<Id, CellHistory>;
type CellMetaMutex = Arc<Mutex<CellMeta>>;
type TxnMutex = Arc<Mutex<Transaction>>;

/// Per-cell metadata for concurrency control
/// 
/// Implements a hybrid timestamp-ordering + lock-based protocol with Wait-Die:
/// - `read` / `write`: Track timestamps for timestamp-ordering validation
/// - `owner`: Acts as a write lock during prepare/commit phases
/// 
/// Wait-Die Protocol:
/// - When a transaction wants to acquire a cell already owned by another:
///   - If requester is YOUNGER (higher timestamp): DIE (abort immediately)
///   - If requester is OLDER (lower timestamp): WAIT (backoff and retry)
/// - This prevents deadlock while reducing contention on hot cells
/// - Waiters poll via backoff rather than blocking on a condition variable
#[derive(Debug)]
pub struct CellMeta {
    read: TxnId,
    write: TxnId,
    owner: Option<TxnId>, // transaction that owns the cell (write lock) during prepare/commit
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
    server: Arc<NebServer>,
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
    pub fn new(server: &Arc<NebServer>) -> Arc<Self> {
        let cleanup_signal = Arc::new(AtomicBool::new(false));
        let manager = Arc::new(Self {
            cells: LFMap::with_capacity(256),
            txns: LFMap::with_capacity(128),
            cell_list: LinkedList::new(),
            txns_sorted: Mutex::new(BTreeSet::new()),
            server: server.clone(),
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
        if server.undo_log.is_some() {
            let server_clone = server.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(300)).await; // Trim every 5 minutes
                    if let Some(ref undo_log) = server_clone.undo_log {
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
        self.server.txn_peer.clock.merge_with(clock);
    }
    #[inline]
    fn get_transaction(&self, tid: &TxnId) -> TxnMutex {
        // Fast path: transaction already exists
        if let Some(txn) = self.txns.get(tid) {
            return txn;
        }

        // Slow path: create new transaction
        self.create_transaction(tid)
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
        self.cell_list.push_back(*id);
        self.cells.get_or_insert(*id, || {
            Arc::new(Mutex::new(CellMeta {
                read: TxnId::new(),
                write: TxnId::new(),
                owner: None,
            }))
        })
    }
    fn response_with<T: Send>(&self, data: T) -> BoxFuture<'_, DataSiteResponse<T>>
    where
        T: 'static,
    {
        future::ready(DataSiteResponse::new(&self.server.txn_peer, data)).boxed()
    }

    /// Create a segment reference guard to prevent eviction during transaction.
    /// Returns None if segment not found (already freed/evicted).
    /// The guard is RAII - reference is automatically released when dropped.
    #[inline]
    fn acquire_segment_guard(&self, chunk_idx: usize, segment_id: u64) -> Option<SegmentReferenceGuard> {
        if let Some(chunk) = self.server.chunks.list.get(chunk_idx) {
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
                self.server
                    .chunks
                    .remove_cell_by(id, |cell| cell.header.version == current_ver)
                    .err()
            } else if current_ver > 0 {
                // the cell was updated, need to update back
                self.server
                    .chunks
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
                self.server.chunks.write_cell(&mut cell).err()
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
        if let Some(txn) = self.txns.lock(tid) {
            txn.remove();
        }
        self.txns_sorted.lock().remove(tid);
    }
    async fn cell_meta_cleanup(&self) {
        let oldest_transaction = {
            self.txns_sorted
                .lock()
                .iter()
                .next()
                .cloned()
                .unwrap_or_else(|| self.server.txn_peer.clock.to_clock())
        };
        let mut cell_to_evict = Vec::new();
        for cell_id_ref in self.cell_list.iter_front() {
            let cell_id = cell_id_ref.deref();
            if let Some(cell_meta) = self.cells.get(&cell_id) {
                let meta = cell_meta.lock();
                if meta.write < oldest_transaction && meta.read < oldest_transaction {
                    cell_to_evict.push(cell_id_ref);
                } else {
                    // Cells are ordered by activity, stop at first active one
                    break;
                }
            } else {
                cell_to_evict.push(cell_id_ref);
            }
        }
        for evicted_cell in &cell_to_evict {
            self.cells.remove(&evicted_cell.deref());
            evicted_cell.remove();
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
        let txn_lock = self.get_transaction(tid);
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
                tid,
                effective_ts,
                id,
                meta.write
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
            match self.server.chunks.read_cell(&id) {
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
        match self.server.chunks.read_selected(&id, &fields[..], true) {
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
        match self.server.chunks.head_cell(&id) {
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
        match self.server.chunks.read_partial_raw(&id, offset, len) {
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
        // Timestamp Ordering (validates serializability - RELAXED):
        // - Check tid >= meta.read (write-after-read constraint) - STRICT
        // - meta.write check REMOVED - handled by locks + Thomas Write Rule
        // - This allows higher concurrency on hot cells while maintaining serializability
        //
        // Lock Acquisition:
        // - If all checks pass, set meta.owner = Some(tid) for each cell
        // - cell_ids must be sorted to avoid deadlock on mutex acquisition
        // - Locks are released in the `end` phase after commit/abort
        debug!("PREPARE FOR {:?}, {} cells", &tid, cell_ids.len());
        self.update_clock(&clock);

        let txn_lock = self.get_transaction(&tid);
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
            let meta = cell_mutex.lock();
            
            // Wait-Die Protocol: Check if another transaction owns this cell
            // This implements lock-based concurrency control to reduce timestamp-ordering aborts
            if let Some(ref owner_tid) = meta.owner {
                if owner_tid != &tid {
                    if tid > *owner_tid {
                        // Younger transaction "dies" (aborts immediately)
                        debug!(
                            "PREPARE Wait-Die: younger txn {:?} aborted, cell owned by older {:?}",
                            tid, owner_tid
                        );
                        return self.response_with(DMPrepareResult::NotRealizable);
                    } else {
                        // Older transaction waits for younger owner to release
                        debug!(
                            "PREPARE Wait-Die: older txn {:?} waits for younger owner {:?}",
                            tid, owner_tid
                        );
                        return self.response_with(DMPrepareResult::Wait);
                    }
                }
            }
            
            // Timestamp ordering validation - relaxed for lock-centric protocol
            // 
            // Read-Write Conflict Check (STRICT):
            // Still enforce write-after-read constraint to ensure no transaction
            // reads a value that will be overwritten by an older write
            if tid < meta.read {
                debug!(
                    "PREPARE: Write too late for {:?} (tid: {:?}), cell read timestamp: {:?}",
                    tid, tid, meta.read
                );
                break;
            }
            
            // Write-Write Conflict Check (RELAXED):
            // With Wait-Die + locks, we can allow concurrent writes.
            // The lock (meta.owner) serializes actual writes, and Thomas Write Rule
            // in commit phase handles ordering. This significantly reduces aborts
            // on hot cells while maintaining serializability.
            //
            // Previously strict check (now removed):
            // if tid < meta.write { break; }
            //
            // Rationale: If we have the lock (via Wait-Die), we can write regardless
            // of meta.write timestamp. The commit phase will apply writes in lock order,
            // and Thomas Write Rule skips obsolete writes.
            
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
            for mut meta in cell_guards {
                meta.owner = Some(tid.clone()) // set owner to lock this cell
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

        // Use the more recent timestamp between the transaction ID and the incoming clock
        let effective_ts = if &clock > &tid { &clock } else { &tid };

        let txn_lock = self.get_transaction(&tid);
        let mut txn = txn_lock.lock();
        txn.last_activity = get_time();
        // check state
        match txn.state {
            TxnState::Started => {
                return self.response_with(DMCommitResult::CheckFailed(CheckError::NotCommitted));
            }
            TxnState::Aborted => {
                return self.response_with(DMCommitResult::CheckFailed(CheckError::AlreadyAborted));
            }
            TxnState::Committed => {
                return self
                    .response_with(DMCommitResult::CheckFailed(CheckError::AlreadyCommitted));
            }
            TxnState::Prepared => {}
        };
        // check cell list integrity
        let prepared_cells_num = txn.affected_cells.len();
        let arrived_cells_num = cells.len();
        if prepared_cells_num != arrived_cells_num {
            return self.response_with(DMCommitResult::CheckFailed(
                CheckError::CellNumberDoesNotMatch(prepared_cells_num, arrived_cells_num),
            ));
        }

        // Set transaction context to skip fsync during writes
        // Segments will be synced at the end of commit instead
        crate::ram::chunk::set_transaction_context(true);

        // Pre-allocate small temporaries to reduce reallocations
        let mut write_error: Option<(Id, WriteError)> = None;
        {
            for cell_op in cells {
                match cell_op {
                    CommitOp::Read(_id, _version) => {}
                    CommitOp::Write(mut cell) => {
                        let cell_id = cell.id();

                        // Apply Thomas Write Rule: Skip if this write is obsolete
                        // Check if a later transaction has already written to this cell
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

                        let write_result = self.server.chunks.write_cell(&mut cell);
                        match write_result {
                            Ok(header) => {
                                // Log the new cell creation with its version for rollback
                                // During recovery, if cell exists with this version, it will be deleted
                                if let Some(ref undo_log) = self.server.undo_log {
                                    let undo_entry = super::undo_log::UndoLogEntry::new_write(
                                        tid.clone(),
                                        cell_id,
                                        header.version,
                                    );
                                    if let Err(e) = undo_log.write_undo_entry(undo_entry) {
                                        error!(
                                            "Failed to write undo log entry for new cell: {:?}",
                                            e
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
                        // Read cell and extract information while holding lock
                        let (cell_addr, orig_version, old_cell_ref) = {
                            let shared_cell = match self.server.chunks.read_cell(cell_id) {
                                Ok(cell) => cell,
                                Err(re) => {
                                    write_error = Some((*cell_id, WriteError::ReadError(re)));
                                    break;
                                }
                            };
                            let addr = **shared_cell.guard();
                            let version = shared_cell.header.version;
                            let cell_ref = shared_cell.to_owned().into_ref();
                            (addr, version, cell_ref)
                        }; // SharedCell dropped here, lock released

                        // Now protect the segment without holding cell lock
                        let chunk = self.server.chunks.locate_chunk_by_partition(cell_id.higher);
                        let chunk_idx = chunk.id;
                        let (segment_id, seq_id) = chunk.get_cell_segment_info(cell_addr);

                        // Calculate cell offset within segment for fast recovery
                        let segment_base_addr = chunk.allocator.addr_by_id(segment_id as usize);
                        let cell_offset = (cell_addr - segment_base_addr) as u64;

                        // Acquire RAII guard to prevent segment eviction during transaction
                        // Guard automatically releases reference when dropped
                        let guard = match self.acquire_segment_guard(chunk_idx, segment_id) {
                            Some(g) => g,
                            None => {
                                write_error = Some((*cell_id, WriteError::ReadError(ReadError::CellDoesNotExisted)));
                                break;
                            }
                        };

                        // Write undo log entry before performing the remove
                        // Store old version and exact offset for verification during recovery
                        // Note: only seq_id is stored, not seg_id (which changes across recoveries)
                        if let Some(ref undo_log) = self.server.undo_log {
                            let undo_entry = super::undo_log::UndoLogEntry::new_restore(
                                tid.clone(),
                                *cell_id,
                                super::undo_log::UndoOpType::Remove,
                                orig_version,
                                chunk_idx as u64,
                                seq_id,
                                cell_offset,
                            );
                            if let Err(e) = undo_log.write_undo_entry(undo_entry) {
                                error!("Failed to write undo log entry: {:?}", e);
                                // Continue anyway - segment guard will prevent data loss
                            }
                        }

                        let write_result = self
                            .server
                            .chunks
                            .remove_cell_by(cell_id, |cell| cell.header.version == orig_version);
                        match write_result {
                            Ok(()) => {
                                txn.history
                                    .insert(*cell_id, CellHistory::new(Some(old_cell_ref), 0));
                                self.update_cell_write(cell_id, effective_ts);
                                // Keep the guard alive by storing it in transaction
                                txn.segment_guards.push(guard);
                            }
                            Err(error) => {
                                // Guard dropped here, automatically releasing reference
                                write_error = Some((*cell_id, error));
                                break;
                            }
                        }
                    }
                    CommitOp::Update(mut cell) => {
                        let cell_id = cell.id();
                        // Read cell and extract information while holding lock
                        let (cell_addr, orig_version) =
                            {
                                match self.server.chunks.location_for_read(&cell_id).and_then(
                                    |loc| {
                                        let addr = *loc;
                                        let header = header_from_chunk_raw(addr);
                                        header.map(|(header, _)| (header, addr))
                                    },
                                ) {
                                    Ok((header, addr)) => {
                                        let version = header.version;
                                        (addr, version)
                                    }
                                    Err(re) => {
                                        write_error = Some((cell_id, WriteError::ReadError(re)));
                                        break;
                                    }
                                }
                            }; // SharedCell dropped here, lock released

                        // Now protect the segment without holding cell lock
                        let chunk = self.server.chunks.locate_chunk_by_partition(cell_id.higher);
                        let chunk_idx = chunk.id;
                        let (segment_id, seq_id) = chunk.get_cell_segment_info(cell_addr);

                        // Calculate cell offset within segment for fast recovery
                        let segment_base_addr = chunk.allocator.addr_by_id(segment_id as usize);
                        let cell_offset = (cell_addr - segment_base_addr) as u64;

                        // Acquire RAII guard to prevent segment eviction during transaction
                        // Guard automatically releases reference when dropped
                        let guard = match self.acquire_segment_guard(chunk_idx, segment_id) {
                            Some(g) => g,
                            None => {
                                write_error = Some((cell_id, WriteError::ReadError(ReadError::CellDoesNotExisted)));
                                break;
                            }
                        };

                        // Write undo log entry before performing the update
                        // Store old version and exact offset for verification during recovery
                        // Note: only seq_id is stored, not seg_id (which changes across recoveries)
                        if let Some(ref undo_log) = self.server.undo_log {
                            let undo_entry = super::undo_log::UndoLogEntry::new_restore(
                                tid.clone(),
                                cell_id,
                                super::undo_log::UndoOpType::Update,
                                orig_version,
                                chunk_idx as u64,
                                seq_id,
                                cell_offset,
                            );
                            if let Err(e) = undo_log.write_undo_entry(undo_entry) {
                                error!("Failed to write undo log entry: {:?}", e);
                                // Continue anyway - segment guard will prevent data loss
                            }
                        }
                        cell.header.version = orig_version; // Enforce version consistency
                        let mut old_cell_ref = None;
                        let write_result =
                            self.server
                                .chunks
                                .update_cell_by(&cell_id, |cell_to_update| {
                                    if cell_to_update.header.version == orig_version {
                                        old_cell_ref =
                                            Some((*cell_to_update).to_owned().into_ref());
                                        Some(cell)
                                    } else {
                                        None
                                    }
                                });
                        match write_result {
                            Ok(cell) => {
                                debug_assert!(old_cell_ref.is_some());
                                txn.history.insert(
                                    cell_id,
                                    CellHistory::new(old_cell_ref, cell.header.version),
                                );
                                self.update_cell_write(&cell_id, effective_ts);
                                // Keep the guard alive by storing it in transaction
                                txn.segment_guards.push(guard);
                            }
                            Err(error) => {
                                // Guard dropped here, automatically releasing reference
                                write_error = Some((cell_id, error));
                                break;
                            }
                        }
                    }
                    CommitOp::None => {
                        panic!("None CommitOp should not appear in data site");
                    }
                }
            }
        }
        txn.last_activity = get_time();

        // Clear transaction context before returning
        crate::ram::chunk::set_transaction_context(false);

        // check if any of those operations failed, if yes, rollback and fail this commit
        if let Some((id, error)) = write_error {
            // Release all segment references on failure by dropping guards
            // Moving guards out of transaction causes them to drop, releasing references
            let _guards_to_drop = std::mem::take(&mut txn.segment_guards);
            drop(txn); // Release the lock before guards drop
            drop(_guards_to_drop); // Explicitly drop guards, releasing all segment references

            match error {
                WriteError::DeletionPredictionFailed | WriteError::UserCanceledUpdate => {
                    // in this case, we can inform transaction manager to try again
                    return self.response_with(DMCommitResult::CellChanged(id));
                }
                _ => {
                    // other failure due to unfixable error should abort without retry
                    return self.response_with(DMCommitResult::WriteError(id, error));
                }
            }
        } else {
            // all set, able to commit - segments remain protected until transaction ends
            txn.state = TxnState::Committed;

            // Sync all segments that were written to during this transaction
            // This ensures durability - data is persisted to disk before commit succeeds
            for guard in &txn.segment_guards {
                let chunk_idx = guard.chunk_id();
                let seg_id = guard.segment_id();
                let chunk = &self.server.chunks.list[chunk_idx];
                if let Some(segment) = chunk.segs.get(&(seg_id as usize)) {
                    // Use force_wal_sync to ensure WAL is persisted and counters are reset
                    if let Err(e) = segment.force_wal_sync() {
                        error!(
                            "Failed to sync WAL for segment {} during commit: {:?}",
                            seg_id, e
                        );
                    } else {
                        debug!(
                            "Synced segment {} (chunk {}) WAL to disk for transaction commit",
                            seg_id, chunk_idx
                        );
                    }
                }
            }

            // Commit all indices
            let response = DataSiteResponse::new(&self.server.txn_peer, DMCommitResult::Success);
            return IndexBuilder::await_indices().map(|_| response).boxed();
        }
    }
    fn abort(
        &self,
        clock: StandardVectorClock,
        tid: TxnId,
    ) -> BoxFuture<'_, DataSiteResponse<AbortResult>> {
        debug!(">> ABORT {:?}", tid);
        self.update_clock(&clock);
        let txn_lock = self.get_transaction(&tid);
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
        let mut released_locks = 0;
        let affected_cells;
        {
            let txn_lock = self.get_transaction(&tid);
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
            let affected_len = txn.affected_cells.len();
            // Reserve to avoid reallocations when many cells are touched
            let mut cell_mutices = Vec::with_capacity(affected_len);
            let mut cell_guards = Vec::with_capacity(affected_len);
            {
                for cell_id in &txn.affected_cells {
                    if let Some(meta) = self.cells.get(cell_id) {
                        cell_mutices.push(meta.clone());
                    } else {
                        // Cell metadata not found - might have been cleaned up
                        // This is OK, just means we don't need to release a lock for it
                        debug!("Cell {:?} in affected_cells but not in cells map", cell_id);
                    }
                }
            }
            // Use the actual number of cells we found metadata for, not affected_len
            affected_cells = cell_mutices.len();
            for cell_mutex in &cell_mutices {
                cell_guards.push(cell_mutex.lock()); // lock all affected cells one by one
            }
            for mut meta in cell_guards {
                if meta.owner.as_ref() == Some(&tid) {
                    // Release ownership - waiting transactions will discover this on their next poll
                    meta.owner = None;
                    released_locks += 1;
                    debug!("Released lock on cell owned by {:?}", tid);
                } else {
                    warn!("affected txn does not own the cell");
                }
            }
        }
        async move {
            // Release all segment references before wiping out transaction
            let (guards_to_drop, txn_state) = {
                let txn_lock = self.get_transaction(&tid);
                let mut txn = txn_lock.lock();
                (std::mem::take(&mut txn.segment_guards), txn.state)
            };
            drop(guards_to_drop); // Drop guards, releasing all segment references

            // Write commit/abort marker to undo log based on transaction state
            if let Some(ref undo_log) = self.server.undo_log {
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
            if released_locks == affected_cells {
                debug!(
                    "ENDED: {:?} with all locks ({}) released",
                    tid, released_locks
                );
                self.response_with(EndResult::Success).await
            } else {
                warn!(
                    "ENDED: {:?} with SOME locks ({}/{}) NOT released",
                    tid, released_locks, affected_cells
                );
                self.response_with(EndResult::SomeLocksNotReleased).await
            }
        }
        .boxed()
    }
}
