use crate::query::statistics::{
    merge_statistics, schema_tracks_statistics, ChunkStatistics, SchemaStatistics,
};
use crate::ram::entry::{Entry, EntryContent, EntryType, ENTRY_HEAD_SIZE};
use crate::ram::file_manager::SegmentFileManager;
use crate::ram::history::{
    HistoryIndex, RevisionChain, RevisionNode, RevisionState, SnapshotRevision,
};
use crate::ram::schema::LocalSchemasCache;
use crate::ram::segment_list::SegmentList;
use crate::ram::segs::{
    Segment, SegmentAllocator, SegmentClass, SegmentReferenceGuard, SEGMENT_SIZE, SEGMENT_SIZE_U32,
};
use crate::ram::tombstone::{Tombstone, TOMBSTONE_ENTRY_SIZE};
use crate::ram::types::{FromHeader, Id};
use crate::server::ServerMeta;
use crate::{index::builder::IndexBuilder, ram::cell::*};
use crate::{
    index::builder::{probe_cell_indices, IndexRes},
    ram::cleaner::Cleaner,
};

use super::schema::Schema;
use bifrost::hlc::HlcSource;
use dovahkiin::types::OwnedValue;
use lightning::aarc::Arc as AArc;
use lightning::map::{Map, WordMap, WordMutexGuard};
use lightning::spin_hint::Backoff;
use lightning::ttl_cache::TTLCache;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub type CellReadGuard<'a> = WordMutexGuard<'a>;
pub type CellWriteGuard<'a> = WordMutexGuard<'a>;

#[cfg(test)]
type SnapshotReadLeaseHook = Arc<dyn Fn(Id, usize, bool) + Send + Sync>;
#[cfg(test)]
type CellGuardRetryHook = Arc<dyn Fn(u64) + Send + Sync>;
#[cfg(test)]
type ExactSyncLeaseHook = Arc<dyn Fn(Id, usize) + Send + Sync>;

// Global chunk allocation state for unified address space
static GLOBAL_CHUNK_BASE: AtomicUsize = AtomicUsize::new(0);
static GLOBAL_CHUNK_SIZE_BITS: AtomicUsize = AtomicUsize::new(0);
static GLOBAL_CHUNK_COUNT: AtomicUsize = AtomicUsize::new(0);
static GLOBAL_ALLOCATED_SIZE: AtomicUsize = AtomicUsize::new(0);
static GLOBAL_CHUNKS_PTR: AtomicUsize = AtomicUsize::new(0);

static MAX_SEGMENTS_FOR_CLEANER: usize = 16;

static DEAD_RATE_FOR_COMBINE_CLEANER: f32 = 0.50f32;

const HEAD_SEG_ID_EMPTY: u64 = u64::MAX;
const HEAD_SEG_ID_ALLOCATING: u64 = u64::MAX - 1;

/// Get the current global chunk base address
pub fn get_global_chunk_base() -> usize {
    GLOBAL_CHUNK_BASE.load(Ordering::Acquire)
}

/// Get chunk size as power-of-2 bits
pub fn get_chunk_size_bits() -> usize {
    GLOBAL_CHUNK_SIZE_BITS.load(Ordering::Acquire)
}

/// Calculate chunk ID and segment ID from a fault address
/// Returns: Some((chunk_id, segment_id)) or None if address not in range
pub fn chunk_and_segment_from_addr(fault_addr: usize) -> Option<(usize, usize)> {
    use crate::ram::segs::SEGMENT_BITS_SHIFT;

    let base = GLOBAL_CHUNK_BASE.load(Ordering::Acquire);
    if base == 0 || fault_addr < base {
        return None;
    }

    let offset = fault_addr - base;
    let total_size = GLOBAL_ALLOCATED_SIZE.load(Ordering::Acquire);

    if offset >= total_size {
        return None;
    }

    let chunk_size_bits = GLOBAL_CHUNK_SIZE_BITS.load(Ordering::Acquire);
    let chunk_id = offset >> chunk_size_bits;
    let offset_in_chunk = offset & ((1 << chunk_size_bits) - 1);
    let segment_id = offset_in_chunk >> SEGMENT_BITS_SHIFT;

    Some((chunk_id, segment_id))
}

/// Set the global Chunks pointer when a chunk collection is constructed.
pub fn set_global_chunks(chunks: &Arc<Chunks>) {
    let ptr = Arc::as_ptr(chunks) as usize;
    GLOBAL_CHUNKS_PTR.store(ptr, Ordering::Release);
}

/// Get a reference to the global Chunks instance
/// SAFETY: Only safe to call if Chunks instance is still alive
pub unsafe fn get_global_chunks() -> Option<&'static Chunks> {
    let ptr = GLOBAL_CHUNKS_PTR.load(Ordering::Acquire);
    if ptr == 0 {
        None
    } else {
        Some(&*(ptr as *const Chunks))
    }
}

/// Access a segment by chunk_id and segment_id from the global Chunks
/// Used by signal handler to flip reference bits
pub fn get_segment_for_fault(
    chunk_id: usize,
    segment_id: usize,
) -> Option<AArc<crate::ram::segs::Segment>> {
    unsafe {
        get_global_chunks().and_then(|chunks| {
            chunks
                .list
                .get(chunk_id)
                .and_then(|chunk| chunk.segs.get(&segment_id))
        })
    }
}

// /// Reset global chunk allocation (for tests)
// ///
// /// IMPORTANT: Reset GLOBAL_CHUNKS_PTR BEFORE unmapping memory to prevent
// /// the signal handler from accessing unmapped memory during cleanup.
// pub fn reset_global_chunk_allocation() {
//     // Reset GLOBAL_CHUNKS_PTR first to prevent signal handler from accessing chunks
//     // This must happen BEFORE unmapping memory to avoid SIGSEGV in signal handler
//     GLOBAL_CHUNKS_PTR.store(0, Ordering::Release);

//     let base = GLOBAL_CHUNK_BASE.swap(0, Ordering::AcqRel);
//     let size = GLOBAL_ALLOCATED_SIZE.swap(0, Ordering::AcqRel);

//     // Reset other globals before unmapping
//     GLOBAL_CHUNK_SIZE_BITS.store(0, Ordering::Release);
//     GLOBAL_CHUNK_COUNT.store(0, Ordering::Release);

//     // Now safe to unmap memory - signal handler won't try to access it
//     if base != 0 && size != 0 {
//         unsafe {
//             println!("unmapping memory from {}", base);
//             libc::munmap(base as *mut libc::c_void, size);
//         }
//     }
// }

// Thread-local flag to indicate if we're currently in a transaction
// When true, WAL writes will skip fsync (will be synced at commit instead)
thread_local! {
    static IN_TRANSACTION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Set the transaction context for the current thread
pub fn set_transaction_context(in_txn: bool) {
    IN_TRANSACTION.with(|flag| flag.set(in_txn));
}

/// Check if the current thread is in a transaction context
pub fn is_in_transaction() -> bool {
    IN_TRANSACTION.with(|flag| flag.get())
}

pub struct Chunk {
    pub id: usize,
    pub cell_index: WordMap,
    pub segs: SegmentList,
    pub head_seg_id: AtomicU64,
    pub blob_head_seg_id: AtomicU64,
    pub meta: Arc<ServerMeta>,
    pub backup_storage: Option<String>,
    pub wal_storage: Option<String>,
    pub file_manager: Arc<SegmentFileManager>,
    pub total_space: AtomicUsize,
    pub capacity: usize,
    pub gc_lock: Mutex<()>,
    pub allocator: SegmentAllocator,
    pub index_builder: Option<Arc<IndexBuilder>>,
    pub statistics: ChunkStatistics,
    pub revision_clock: Arc<HlcSource>,
    pub history_retention_ms: u64,
    pub history: Arc<HistoryIndex>,
    /// Shared tiered memory manager for eviction/promotion
    pub tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
    #[cfg(test)]
    fail_next_allocation: AtomicUsize,
    #[cfg(test)]
    secondary_index_removal_attempts: AtomicUsize,
    #[cfg(test)]
    snapshot_read_lease_hook: Mutex<Option<SnapshotReadLeaseHook>>,
    #[cfg(test)]
    cell_guard_retry_hook: Mutex<Option<CellGuardRetryHook>>,
    #[cfg(test)]
    exact_sync_lease_hook: Mutex<Option<ExactSyncLeaseHook>>,
}

impl Chunk {
    #[inline]
    fn refresh_statistics_for_schema(&self, schema_id: u32) {
        if schema_tracks_statistics(schema_id) {
            self.refresh_statistics();
        }
    }

    /// Debug-only validation for cell locations
    /// Checks alignment and basic sanity of addresses stored in cell index
    #[cfg(debug_assertions)]
    fn validate_cell_location(&self, addr: usize, context: &str) -> bool {
        // Check for obviously invalid addresses
        if addr == 0 {
            error!(
                "[Chunk {}] Invalid cell location at {}: address is NULL (0x0)",
                self.id, context
            );
            return false;
        }

        // Cell data should be at least 8-byte aligned for proper struct access
        if addr % 8 != 0 {
            error!(
                "[Chunk {}] Invalid cell location at {}: address 0x{:x} is not 8-byte aligned",
                self.id, context, addr
            );
            return false;
        }

        // Check if address looks suspicious (too high bits set)
        // Valid pointers on x86-64 typically use only lower 48 bits
        if addr > 0x0000_FFFF_FFFF_FFFF {
            error!(
                "[Chunk {}] Invalid cell location at {}: address 0x{:x} has invalid high bits",
                self.id, context, addr
            );
            return false;
        }

        // Check if the address is within reasonable segment bounds
        // We can't do precise bounds checking without segment info, but we can check basic sanity
        if let Some(segment) = self.locate_segment(addr) {
            let seg_start = segment.addr;
            let seg_end = seg_start + SEGMENT_SIZE;

            if addr < seg_start || addr >= seg_end {
                error!(
                    "[Chunk {}] Invalid cell location at {}: address 0x{:x} outside segment bounds [0x{:x}, 0x{:x})",
                    self.id, context, addr, seg_start, seg_end
                );
                return false;
            }
        } else {
            warn!(
                "[Chunk {}] Cannot validate cell location at {}: address 0x{:x} - segment not found (may be valid for new writes)",
                self.id, context, addr
            );
            // Don't fail validation if segment not found - might be a newly allocated address
        }

        true
    }

    /// Validate address before storing it in cell_index (WRITE operation)
    #[cfg(debug_assertions)]
    #[inline]
    fn assert_address_aligned_for_write(&self, addr: usize, operation: &str, hash: u64) {
        debug_assert!(
            addr % 8 == 0,
            "WRITE POINT: {} attempting to store MISALIGNED address 0x{:016x} (offset: {}) for hash {}",
            operation, addr, addr % 8, hash
        );
        if addr % 8 != 0 {
            error!(
                "WRITE POINT: {} attempting to store misaligned address 0x{:016x} (offset: {}) in cell_index for hash {}",
                operation, addr, addr % 8, hash
            );
        }
    }

    /// Validate address after retrieving it from cell_index (READ operation)
    #[cfg(debug_assertions)]
    #[inline]
    fn assert_address_aligned_for_read(&self, addr: usize, operation: &str, hash: u64) {
        debug_assert!(
            addr % 8 == 0,
            "READ POINT: {} retrieved MISALIGNED address 0x{:016x} (offset: {}) for hash {}",
            operation,
            addr,
            addr % 8,
            hash
        );
        if addr % 8 != 0 {
            error!(
                "READ POINT: {} retrieved misaligned address 0x{:016x} (offset: {}) from cell_index for hash {}",
                operation, addr, addr % 8, hash
            );
        }
    }

    fn new_with_base(
        id: usize,
        base_addr: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
        revision_clock: Arc<HlcSource>,
        history_retention_ms: u64,
    ) -> Chunk {
        let allocate_memory = base_addr == 0;
        let allocator = SegmentAllocator::new_with_base(id, base_addr, size, allocate_memory);

        // Create file manager
        let file_manager = Arc::new(SegmentFileManager::new(
            backup_storage.clone(),
            wal_storage.clone(),
        ));

        // Initialize storage directories
        if let Err(e) = file_manager.init_directories() {
            panic!("Failed to initialize storage directories: {}", e);
        }

        let bootstrap_segment = allocator
            .alloc_seg(&file_manager)
            .expect(&format!("No space left for first segment in chunk {}", id));
        let num_segs = {
            let n = size / SEGMENT_SIZE;
            if n > 0 {
                n
            } else {
                n + 1
            }
        };
        assert!(
            !(base_addr == 0 && tiered_manager.is_some()),
            "Should not enable tiered memory if the memory is not allocated by Chunks"
        );
        debug!("Creating chunk {}, num segments {}", id, num_segs);
        let segs = SegmentList::new(num_segs);
        // Recovery rebuilds this index very aggressively; starting too small causes
        // repeated lock-free migration storms during startup.
        let index_capacity = num_segs
            .saturating_mul(64)
            .clamp(4_096, 1 << 20)
            .next_power_of_two();
        let index = WordMap::with_capacity(index_capacity);
        let history = HistoryIndex::new_for_chunk(id, history_retention_ms);
        let chunk = Chunk {
            id,
            segs,
            cell_index: index,
            meta,
            backup_storage,
            wal_storage,
            file_manager,
            allocator,
            index_builder,
            capacity: size,
            total_space: AtomicUsize::new(0),
            head_seg_id: AtomicU64::new(bootstrap_segment.id),
            blob_head_seg_id: AtomicU64::new(HEAD_SEG_ID_EMPTY),
            gc_lock: Mutex::new(()),
            statistics: ChunkStatistics::new(),
            revision_clock,
            history_retention_ms,
            history,
            tiered_manager,
            #[cfg(test)]
            fail_next_allocation: AtomicUsize::new(0),
            #[cfg(test)]
            secondary_index_removal_attempts: AtomicUsize::new(0),
            #[cfg(test)]
            snapshot_read_lease_hook: Mutex::new(None),
            #[cfg(test)]
            cell_guard_retry_hook: Mutex::new(None),
            #[cfg(test)]
            exact_sync_lease_hook: Mutex::new(None),
        };
        chunk.put_segment(bootstrap_segment);
        return chunk;
    }

    #[inline]
    fn head_slot(&self, segment_class: SegmentClass) -> &AtomicU64 {
        match segment_class {
            SegmentClass::Regular => &self.head_seg_id,
            SegmentClass::Blob => &self.blob_head_seg_id,
        }
    }

    pub fn get_head_seg_id(&self) -> u64 {
        self.head_seg_id.load(Ordering::Acquire)
    }

    pub fn is_active_head(&self, seg_id: u64) -> bool {
        self.head_seg_id.load(Ordering::Acquire) == seg_id
            || self.blob_head_seg_id.load(Ordering::Acquire) == seg_id
    }

    #[inline]
    pub fn has_blob_head(&self) -> bool {
        self.blob_head_seg_id.load(Ordering::Acquire) != HEAD_SEG_ID_EMPTY
    }

    pub fn try_acquire(&self, size: u32, full_gc: bool) -> Result<PendingEntry, WriteError> {
        self.try_acquire_in_class(size, full_gc, SegmentClass::Regular)
    }

    pub fn try_acquire_in_class(
        &self,
        size: u32,
        full_gc: bool,
        segment_class: SegmentClass,
    ) -> Result<PendingEntry, WriteError> {
        #[cfg(test)]
        if self.fail_next_allocation.swap(0, Ordering::AcqRel) != 0 {
            return Err(WriteError::CannotAllocateSpace);
        }
        let mut tried_gc = false;
        let backoff = Backoff::new();
        let head_slot = self.head_slot(segment_class);
        loop {
            let head_seg_id = head_slot.load(Ordering::Acquire);
            if head_seg_id == HEAD_SEG_ID_ALLOCATING {
                // Allocating new segment in progress, wait for it to complete
                backoff.spin();
                continue;
            }
            if head_seg_id != HEAD_SEG_ID_EMPTY {
                // Try to get the head segment. If it's been removed (e.g., by cleaner after
                // a new head was allocated), retry with the updated head_seg_id.
                let head = match self.segs.get(&(head_seg_id as usize)) {
                    Some(seg) => seg,
                    None => {
                        debug!(
                            "Head segment {} was removed, retrying with current head",
                            head_seg_id
                        );
                        backoff.spin();
                        continue;
                    }
                };
                if let Some(addr) = head.try_acquire(size) {
                    trace!(
                        "Chunk {} acquired address {} for size {} in segment {} ({:?})",
                        self.id,
                        addr,
                        size,
                        head.id,
                        segment_class
                    );
                    head.incr_references();
                    return Ok(PendingEntry {
                        addr,
                        seg: head,
                        size,
                        skip_sync: is_in_transaction(),
                    });
                }
            }

            let total_space = self.segs.len() * SEGMENT_SIZE;
            if total_space >= self.capacity - SEGMENT_SIZE {
                if tried_gc {
                    debug!(
                        "chunk-allocation-failure: chunk={}, total_space={}, capacity={}, head_seg_id={}, seg_count={}, full_gc={}, segment_class={:?}",
                        self.id,
                        total_space,
                        self.capacity,
                        head_slot.load(Ordering::Relaxed),
                        self.segs.len(),
                        full_gc,
                        segment_class
                    );
                    error!("No space left for chunk {}, cannot allocate space", self.id);
                    return Err(WriteError::CannotAllocateSpace);
                } else if full_gc {
                    warn!("No space left for chunk {}, emergency full GC", self.id);
                    let _ = Cleaner::clean(self, true, true);
                    tried_gc = true;
                    continue;
                } else {
                    warn!(
                        "No space left for chunk {}, emergency best effort GC",
                        self.id
                    );
                    let _ = Cleaner::clean(self, true, false);
                    tried_gc = true;
                    continue;
                }
            }
            if self.allocator.meet_gc_threshold() {
                debug!("Allocator meet GC threshold, will try partial GC");
                let _ = Cleaner::clean(self, false, false);
            }

            if head_slot
                .compare_exchange(
                    head_seg_id,
                    HEAD_SEG_ID_ALLOCATING,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                backoff.spin();
                continue;
            }

            let new_seg_opt = self
                .allocator
                .alloc_seg_with_class(&self.file_manager, segment_class);
            let new_seg = new_seg_opt.expect("No space left after full GCs");
            let new_seg_id = new_seg.id;

            // Publish new head segment id FIRST
            // This creates a window where the head_seg_id points to a segment not yet in self.segs.
            // Readers of head_seg_id must handle this by retrying.
            head_slot.store(new_seg_id, Ordering::Release);

            self.put_segment(new_seg);

            if head_seg_id != HEAD_SEG_ID_EMPTY {
                if let Some(old_head) = self.segs.get(&(head_seg_id as usize)) {
                    if let Err(e) = old_head.force_wal_sync() {
                        warn!(
                            "Failed to sync WAL for old head segment {}: {}",
                            head_seg_id, e
                        );
                    }
                    let mut state = old_head.file_state.lock();
                    if let Some(wal) = state.wal.take() {
                        if let Err(e) = wal.sync_all() {
                            warn!(
                                "Failed to sync WAL during close for old head segment {}: {}",
                                head_seg_id, e
                            );
                        }
                        drop(wal);
                        debug!(
                            "Closed WAL file for old head segment {} (freed file descriptor)",
                            head_seg_id
                        );
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub fn head_seg_ids_for_test(&self) -> (u64, Option<u64>) {
        let blob_head_id = self.blob_head_seg_id.load(Ordering::Acquire);
        (
            self.get_head_seg_id(),
            if blob_head_id == HEAD_SEG_ID_EMPTY {
                None
            } else {
                Some(blob_head_id)
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn set_snapshot_read_lease_hook_for_test(
        &self,
        hook: Option<SnapshotReadLeaseHook>,
    ) {
        *self.snapshot_read_lease_hook.lock() = hook;
    }

    #[cfg(test)]
    pub(crate) fn set_cell_guard_retry_hook_for_test(&self, hook: Option<CellGuardRetryHook>) {
        *self.cell_guard_retry_hook.lock() = hook;
    }

    #[cfg(test)]
    pub(crate) fn set_exact_sync_lease_hook_for_test(&self, hook: Option<ExactSyncLeaseHook>) {
        *self.exact_sync_lease_hook.lock() = hook;
    }

    pub(crate) fn reset_write_heads_after_recovery(&self) -> io::Result<()> {
        self.blob_head_seg_id
            .store(HEAD_SEG_ID_EMPTY, Ordering::Release);

        let regular_head_id = self
            .segments()
            .into_iter()
            .filter(|segment| {
                segment.segment_class() == SegmentClass::Regular
                    && segment.is_hot()
                    && segment.append_header.load(Ordering::Acquire) < segment.bound
            })
            .max_by_key(|segment| segment.seq_id)
            .map(|segment| segment.id)
            .unwrap_or(HEAD_SEG_ID_EMPTY);

        self.head_seg_id.store(regular_head_id, Ordering::Release);

        Ok(())
    }

    pub fn location_for_read<'a>(&self, hash: u64) -> Result<CellReadGuard<'_>, ReadError> {
        let guard = self.cell_index.lock(hash as usize);
        match guard {
            Some(index) => {
                if *index == 0 {
                    warn!("Cannot find cell with hash {} for index is zero", hash);
                    return Err(ReadError::CellDoesNotExisted);
                }
                return Ok(index);
            }
            None => {
                if hash == 0 {
                    Err(ReadError::CellIdIsUnitId)
                } else {
                    trace!(
                        "Cannot find cell with hash {} for it is not in the map",
                        hash
                    );
                    Err(ReadError::CellDoesNotExisted)
                }
            }
        }
    }

    pub fn location_for_write(&self, hash: u64, has_read: bool) -> Option<CellWriteGuard<'_>> {
        let guard = self.cell_index.lock(hash as usize);
        match guard {
            Some(index) => {
                if *index == 0 {
                    return None;
                }

                #[cfg(debug_assertions)]
                self.assert_address_aligned_for_read(*index, "location_for_write", hash);
                Some(index)
            }
            None => None,
        }
    }

    pub fn lock_or_insert_cell(&self, hash: u64) -> CellGuard<'_> {
        let backoff = Backoff::new();
        loop {
            let guard = self.cell_index.lock_or_insert(hash as usize, 0);
            if let Some(guard) = CellGuard::from_guard(hash, guard, self) {
                return guard;
            }
            backoff.spin();
        }
    }

    pub(crate) fn head_cell(&self, hash: u64) -> Result<CellHeader, ReadError> {
        header_from_chunk_raw(*CellGuard::for_read(hash, self)?).map(|pair| pair.0)
    }

    // Cheap capture of the current cell's raw address and revision: an index
    // lookup plus a header decode, with no value materialization. Used by
    // repeatable-read pinning, where parsing the payload just to learn where it
    // lives would defeat the point of pinning.
    pub(crate) fn cell_location_and_revision(&self, hash: u64) -> Result<(usize, u64), ReadError> {
        let location = *CellGuard::for_read(hash, self)?;
        let (header, _) = header_from_chunk_raw(location)?;
        Ok((location, header.revision_ts))
    }

    // By-address header read: decodes the header stored at a caller-pinned raw
    // `location` instead of resolving through the cell index. Used by
    // repeatable-read pinning, where the caller already holds a segment guard
    // that keeps the bytes at `location` alive even after the cell index has
    // moved on to a newer revision.
    pub(crate) fn head_at(&self, location: usize) -> Result<CellHeader, ReadError> {
        header_from_chunk_raw(location).map(|pair| pair.0)
    }

    pub fn read_cell(&self, hash: u64) -> Result<SharedCell<'_>, ReadError> {
        SharedCell::from_chunk_raw(hash, CellGuard::for_read(hash, self)?, self).map(|(c, _)| c)
    }

    // By-address full-cell read: materializes the cell exactly as stored at
    // `location`, bypassing the cell index entirely. See `head_at`.
    pub fn read_cell_at(&self, hash: u64, location: usize) -> Result<OwnedCell, ReadError> {
        SharedCellData::from_chunk_raw(hash, location, self).map(|(cell, _)| cell.to_owned())
    }

    fn read_selected(
        &self,
        hash: u64,
        fields: &[u64],
        need_header: bool,
    ) -> Result<SharedCell<'_>, ReadError> {
        let loc = CellGuard::for_read(hash, self)?;
        let (val, hdr) = select_from_chunk_raw(*loc, self, fields, need_header)?;
        Ok(SharedCell::compose(
            SharedCellData::from_data(hdr, val),
            loc,
        ))
    }

    // By-address projected read: same field-projection logic as `read_selected`,
    // but pinned to `location` instead of following the cell index.
    fn read_selected_at(
        &self,
        location: usize,
        fields: &[u64],
        need_header: bool,
    ) -> Result<OwnedCell, ReadError> {
        let (val, hdr) = select_from_chunk_raw(location, self, fields, need_header)?;
        Ok(SharedCellData::from_data(hdr, val).to_owned())
    }

    fn read_partial_raw(&self, hash: u64, offset: usize, len: usize) -> Result<Vec<u8>, ReadError> {
        let loc = CellGuard::for_read(hash, self)?;
        let entry_size = self.entry_size_at(*loc);
        self.read_partial_raw_at(*loc, entry_size, offset, len)
    }

    fn read_partial_raw_at(
        &self,
        location: usize,
        entry_size: u32,
        offset: usize,
        len: usize,
    ) -> Result<Vec<u8>, ReadError> {
        let end_offset = offset
            .checked_add(len)
            .ok_or(ReadError::CellDoesNotExisted)?;
        if end_offset > entry_size as usize {
            return Err(ReadError::CellDoesNotExisted);
        }
        let head_ptr = location
            .checked_add(offset)
            .ok_or(ReadError::CellDoesNotExisted)?;
        let end_ptr = location
            .checked_add(end_offset)
            .ok_or(ReadError::CellDoesNotExisted)?;
        let mut data = Vec::with_capacity(len);
        for ptr in head_ptr..end_ptr {
            data.push(unsafe { *(ptr as *const u8) });
        }
        Ok(data)
    }

    pub fn write_cell_to_chunk<'a>(
        &self,
        cell: &OwnedCell,
        write_plan: &WritePlan,
        pending_entry: &PendingEntry,
        revision_ts: u64,
    ) -> Result<WriteToChunkResult, WriteError> {
        cell.write_to_chunk_with(write_plan, pending_entry, revision_ts)
    }

    fn next_revision_ts(&self, previous: u64) -> Result<u64, WriteError> {
        let next = self
            .revision_clock
            .try_now()
            .map_err(|_| WriteError::RevisionClockExhausted)?
            .ts;
        if next <= previous {
            return Err(WriteError::RevisionClockExhausted);
        }
        Ok(next)
    }

    fn ensure_indices(&self, new_cell: &OwnedCell, old_cell: Option<&SharedCell>, schema: &Schema) {
        if let Some(index_builder) = &self.index_builder {
            let old_indices = old_cell.map(|cell| probe_cell_indices(cell, &*schema));
            index_builder.ensure_indices(new_cell, &*schema, old_indices);
        }
    }

    fn remove_indices(&self, cell: &SharedCell, schema: &Schema) {
        if let Some(indexer) = &self.index_builder {
            indexer.remove_indices(&cell, &*schema)
        }
    }

    fn ensure_indices_with_res(
        &self,
        cell: &OwnedCell,
        old_indices: Option<Vec<IndexRes>>,
        schema: &Schema,
    ) {
        if let Some(index_builder) = &self.index_builder {
            index_builder.ensure_indices(cell, schema, old_indices)
        }
    }

    fn entry_size_at(&self, location: usize) -> u32 {
        let (entry, _) = Entry::decode_from(location, |_, _| {});
        entry
            .content_length
            .checked_add(ENTRY_HEAD_SIZE as u32)
            .expect("entry size exceeds u32")
    }

    fn ensure_present_predecessor(
        &self,
        id: Id,
        header: &CellHeader,
        location: usize,
    ) -> Result<Arc<RevisionNode>, WriteError> {
        self.ensure_present_predecessor_with_chain(id, header, location)
            .map(|(_, predecessor)| predecessor)
    }

    fn ensure_present_predecessor_with_chain(
        &self,
        id: Id,
        header: &CellHeader,
        location: usize,
    ) -> Result<(Arc<RevisionChain>, Arc<RevisionNode>), WriteError> {
        if let Some(chain) = self.history.chain(&id) {
            let current = chain.current().ok_or(WriteError::CellRevisionMismatch)?;
            let (state, current_location) = current.load();
            if current.revision_ts != header.revision_ts
                || current_location != location
                || !matches!(
                    state,
                    RevisionState::PendingPresent
                        | RevisionState::CommittedPresent
                        | RevisionState::Aborted
                )
            {
                return Err(WriteError::CellRevisionMismatch);
            }
            return Ok((chain, current));
        }

        let predecessor = Arc::new(RevisionNode::new(
            header.revision_ts,
            RevisionState::CommittedPresent,
            location,
            self.entry_size_at(location),
        ));
        let (chain, _) = self
            .history
            .install(id, predecessor.clone(), None)
            .map_err(|_| WriteError::CellRevisionMismatch)?;
        Ok((chain, predecessor))
    }

    fn revision_state(visibility: InstallVisibility, deleted: bool) -> RevisionState {
        match (visibility, deleted) {
            (InstallVisibility::Pending, false) => RevisionState::PendingPresent,
            (InstallVisibility::Pending, true) => RevisionState::PendingDeleted,
            (InstallVisibility::Committed, false) => RevisionState::CommittedPresent,
            (InstallVisibility::Committed, true) => RevisionState::CommittedDeleted,
        }
    }

    fn validate_assigned_revision(write: RevisionWrite) -> Result<(), WriteError> {
        if write.revision_ts == 0 {
            Err(WriteError::CellRevisionMismatch)
        } else {
            Ok(())
        }
    }

    fn install_written_revision(
        &self,
        id: Id,
        node: Arc<RevisionNode>,
        expected_predecessor: Option<&Arc<RevisionNode>>,
        orphan_segment: &Segment,
    ) -> Result<
        (
            Arc<crate::ram::history::RevisionChain>,
            Option<Arc<RevisionNode>>,
        ),
        WriteError,
    > {
        match self.history.install(id, node.clone(), expected_predecessor) {
            Ok(installed) => Ok(installed),
            Err(()) => {
                self.mark_dead_entry_with_size(node.load().1, node.entry_size, orphan_segment);
                Err(WriteError::CellRevisionMismatch)
            }
        }
    }

    fn write_cell_at_revision(
        &self,
        cell: &mut OwnedCell,
        write: RevisionWrite,
    ) -> Result<InstalledRevision, WriteError> {
        Self::validate_assigned_revision(write)?;
        let id = cell.id();
        let hash = id.lower;
        let raw_guard = self
            .cell_index
            .try_insert_locked(hash as usize)
            .ok_or(WriteError::CellAlreadyExisted)?;
        let mut guard = CellGuard::from_guard(hash, raw_guard, self)
            .expect("empty cell reservation cannot require promotion");
        self.write_cell_with_guard_at_revision(&mut guard, cell, write)
    }

    fn write_cell_with_guard_at_revision(
        &self,
        guard: &mut CellGuard<'_>,
        cell: &mut OwnedCell,
        write: RevisionWrite,
    ) -> Result<InstalledRevision, WriteError> {
        if !guard.is_unassigned() {
            return Err(WriteError::CellAlreadyExisted);
        }
        let id = cell.id();
        if id.lower != guard.hash {
            return Err(WriteError::CellRevisionMismatch);
        }
        let hash = id.lower;
        let predecessor = self.history.current(&id);
        if predecessor
            .as_ref()
            .is_some_and(|current| current.revision_ts >= write.revision_ts)
        {
            return Err(WriteError::CellRevisionMismatch);
        }
        if predecessor.as_ref().is_some_and(|current| {
            !matches!(
                current.load().0,
                RevisionState::CommittedDeleted | RevisionState::Aborted
            )
        }) {
            return Err(WriteError::CellAlreadyExisted);
        }

        let write_plan = cell.plan_write(self)?;
        let pending_entry = write_plan.allocate(self, true)?;
        let entry_size = write_plan.total_size();
        let orphan_segment = pending_entry.seg.clone();
        let write_result =
            self.write_cell_to_chunk(cell, &write_plan, &pending_entry, write.revision_ts)?;
        pending_entry.finish()?;
        let node = Arc::new(RevisionNode::new(
            write.revision_ts,
            Self::revision_state(write.visibility, false),
            write_result.addr,
            entry_size,
        ));
        let (chain, installed_predecessor) =
            self.install_written_revision(id, node.clone(), predecessor.as_ref(), &orphan_segment)?;

        #[cfg(debug_assertions)]
        self.assert_address_aligned_for_write(write_result.addr, "write_cell_at_revision", hash);
        guard.set_ptr(write_result.addr);
        self.ensure_indices(cell, None, &write_plan.schema);
        self.refresh_statistics_for_schema(write_plan.schema.id);
        if let Some(predecessor) = installed_predecessor {
            self.history.retire(&chain, &predecessor);
        }
        cell.header.revision_ts = write.revision_ts;
        Ok(InstalledRevision { id, node })
    }

    fn update_cell_at_revision(
        &self,
        cell: &mut OwnedCell,
        write: RevisionWrite,
    ) -> Result<InstalledRevision, WriteError> {
        Self::validate_assigned_revision(write)?;
        let id = cell.id();
        let hash = id.lower;
        let mut guard =
            CellGuard::for_write(hash, true, self).ok_or(WriteError::CellDoesNotExisted)?;
        self.update_cell_with_guard_at_revision(&mut guard, cell, write)
    }

    fn update_cell_with_guard_at_revision(
        &self,
        guard: &mut CellGuard<'_>,
        cell: &mut OwnedCell,
        write: RevisionWrite,
    ) -> Result<InstalledRevision, WriteError> {
        if guard.is_unassigned() || cell.header.hash != guard.hash {
            return Err(WriteError::CellDoesNotExisted);
        }
        let id = cell.id();
        let old_location = guard.get_ptr();
        let old_header = guard.head_cell().map_err(WriteError::ReadError)?;
        if Id::from_header(&old_header) != id {
            return Err(WriteError::CellDoesNotExisted);
        }
        let predecessor = self.ensure_present_predecessor(id, &old_header, old_location)?;
        if write.revision_ts <= predecessor.revision_ts {
            return Err(WriteError::CellRevisionMismatch);
        }

        let write_plan = cell.plan_write(self)?;
        let old_indices = guard.old_index_res(&write_plan.schema)?;
        let pending_entry = write_plan.allocate(self, true)?;
        let entry_size = write_plan.total_size();
        let orphan_segment = pending_entry.seg.clone();
        let write_result =
            self.write_cell_to_chunk(cell, &write_plan, &pending_entry, write.revision_ts)?;
        pending_entry.finish()?;
        let node = Arc::new(RevisionNode::new(
            write.revision_ts,
            Self::revision_state(write.visibility, false),
            write_result.addr,
            entry_size,
        ));
        let (chain, _) =
            self.install_written_revision(id, node.clone(), Some(&predecessor), &orphan_segment)?;

        guard.set_ptr(write_result.addr);
        self.ensure_indices_with_res(cell, old_indices, &write_plan.schema);
        self.refresh_statistics_for_schema(write_plan.schema.id);
        self.history.retire(&chain, &predecessor);
        cell.header.revision_ts = write.revision_ts;
        Ok(InstalledRevision { id, node })
    }

    fn update_cell_with_guard(
        &self,
        guard: &mut CellGuard<'_>,
        cell: &mut OwnedCell,
    ) -> Result<(), WriteError> {
        // Direct updates allocate their revision while holding the cell guard
        // and do not need an InstalledRevision token. Keep this path separate
        // from transaction-assigned revisions so the cached chain can consume
        // the new node without an otherwise-discarded Arc clone.
        if guard.is_unassigned() || cell.header.hash != guard.hash {
            return Err(WriteError::CellDoesNotExisted);
        }
        let id = cell.id();
        let old_location = guard.get_ptr();
        let old_header = guard.head_cell().map_err(WriteError::ReadError)?;
        if Id::from_header(&old_header) != id {
            return Err(WriteError::CellDoesNotExisted);
        }
        let (chain, predecessor) =
            self.ensure_present_predecessor_with_chain(id, &old_header, old_location)?;
        let revision_ts = self.next_revision_ts(predecessor.revision_ts)?;

        let write_plan = cell.plan_write(self)?;
        let old_indices = guard.old_index_res(&write_plan.schema)?;
        let pending_entry = write_plan.allocate(self, true)?;
        let entry_size = write_plan.total_size();
        let orphan_segment = pending_entry.seg.clone();
        let write_result =
            self.write_cell_to_chunk(cell, &write_plan, &pending_entry, revision_ts)?;
        pending_entry.finish()?;
        let node = Arc::new(RevisionNode::new(
            revision_ts,
            RevisionState::CommittedPresent,
            write_result.addr,
            entry_size,
        ));
        if self
            .history
            .install_on_chain(&chain, node, &predecessor)
            .is_err()
        {
            self.mark_dead_entry_with_size(write_result.addr, entry_size, &orphan_segment);
            return Err(WriteError::CellRevisionMismatch);
        }

        guard.set_ptr(write_result.addr);
        self.ensure_indices_with_res(cell, old_indices, &write_plan.schema);
        self.refresh_statistics_for_schema(write_plan.schema.id);
        self.history.retire(&chain, &predecessor);
        cell.header.revision_ts = revision_ts;
        Ok(())
    }

    fn remove_cell_at_revision(
        &self,
        id: &Id,
        write: RevisionWrite,
    ) -> Result<InstalledRevision, WriteError> {
        Self::validate_assigned_revision(write)?;
        let hash = id.lower;
        let guard = CellGuard::for_write(hash, true, self).ok_or(WriteError::CellDoesNotExisted)?;
        self.remove_cell_with_guard_at_revision(guard, id, write)
    }

    fn remove_cell_with_guard_at_revision(
        &self,
        guard: CellGuard<'_>,
        id: &Id,
        write: RevisionWrite,
    ) -> Result<InstalledRevision, WriteError> {
        self.remove_cell_with_guard_at_revision_for_indexing(
            guard,
            id,
            write,
            self.index_builder.is_some(),
        )
    }

    fn remove_cell_with_guard_at_revision_for_indexing(
        &self,
        mut guard: CellGuard<'_>,
        id: &Id,
        write: RevisionWrite,
        indexing_required: bool,
    ) -> Result<InstalledRevision, WriteError> {
        if guard.is_unassigned() || guard.hash != id.lower {
            return Err(WriteError::CellDoesNotExisted);
        }
        let old_location = guard.get_ptr();
        let old_header = guard.head_cell().map_err(WriteError::ReadError)?;
        if Id::from_header(&old_header) != *id {
            return Err(WriteError::CellDoesNotExisted);
        }
        let indexed_old_cell = if indexing_required {
            let schema = self
                .meta
                .schemas
                .get(&old_header.schema)
                .ok_or(WriteError::SchemaDoesNotExisted(old_header.schema))?;
            let (_, data_ptr) =
                header_from_chunk_raw(old_location).map_err(WriteError::ReadError)?;
            let compression_plan = if schema.compression_plan.is_empty() {
                None
            } else {
                Some(schema.compression_plan.clone())
            };
            let old_cell = SharedCellData::from_data_with_plan(
                old_header,
                crate::ram::io::reader::read_by_schema(data_ptr, &schema),
                compression_plan,
            );
            Some((old_cell, schema))
        } else {
            None
        };
        let predecessor = self.ensure_present_predecessor(*id, &old_header, old_location)?;
        if write.revision_ts <= predecessor.revision_ts {
            return Err(WriteError::CellRevisionMismatch);
        }

        let old_segment = self.locate_segment_ensured(old_location, id);
        let pending_entry = self.try_acquire(TOMBSTONE_ENTRY_SIZE as u32, true)?;
        let tombstone_segment = pending_entry.seg.clone();
        Tombstone::put(
            pending_entry.addr,
            old_segment.seq_id,
            write.revision_ts,
            id.higher,
            id.lower,
        );
        let tombstone_location = pending_entry.addr;
        pending_entry.finish()?;
        tombstone_segment.tombstones.fetch_add(1, Ordering::Relaxed);
        tombstone_segment.note_dead_bytes_change();
        let node = Arc::new(RevisionNode::new(
            write.revision_ts,
            Self::revision_state(write.visibility, true),
            tombstone_location,
            TOMBSTONE_ENTRY_SIZE as u32,
        ));
        let (chain, _) = self.install_written_revision(
            *id,
            node.clone(),
            Some(&predecessor),
            &tombstone_segment,
        )?;

        // History publication is the last fallible step. From here through the
        // current-index transition and secondary-index scheduling, deletion
        // cannot return an error.
        if let Some((old_cell, schema)) = indexed_old_cell {
            #[cfg(test)]
            if !schema.index_fields.is_empty() || schema.is_scannable {
                self.secondary_index_removal_attempts
                    .fetch_add(1, Ordering::Relaxed);
            }
            let shared = SharedCell::compose(old_cell, guard);
            self.index_builder
                .as_ref()
                .expect("indexing preflight requires an index builder")
                .remove_indices(&shared, &schema);
            guard = shared.into_cell_guard();
        }
        guard.remove_index_entry();
        self.refresh_statistics_for_schema(old_header.schema);
        self.history.retire(&chain, &predecessor);
        Ok(InstalledRevision { id: *id, node })
    }

    fn read_snapshot_at<T, F>(
        &self,
        id: &Id,
        snapshot_ts: u64,
        materialize: F,
    ) -> Result<SnapshotRead<T>, ReadError>
    where
        F: Fn(usize, u32) -> Result<T, ReadError>,
    {
        let recovery_floor = self.history.recovery_floor();
        if recovery_floor != 0 && snapshot_ts < recovery_floor {
            return Err(ReadError::SnapshotTooOld);
        }
        match CellGuard::for_read(id.lower, self) {
            Ok(mut guard) => {
                let location = guard.get_ptr();
                let header = guard.head_cell()?;
                if header.revision_ts < snapshot_ts {
                    if let Some(current) = self.history.current(id) {
                        let (state, node_location) = current.load();
                        if current.revision_ts == header.revision_ts
                            && state == RevisionState::CommittedPresent
                            && node_location == location
                        {
                            return materialize(location, current.entry_size)
                                .map(SnapshotRead::Present);
                        }
                    }
                }
            }
            Err(ReadError::CellDoesNotExisted) => {}
            Err(error) => return Err(error),
        }

        let backoff = Backoff::new();
        loop {
            match self.history.resolve(id, snapshot_ts) {
                SnapshotRevision::Present(node) => {
                    let expected = node.load();
                    if expected.0 != RevisionState::CommittedPresent {
                        backoff.spin();
                        continue;
                    }
                    let Some(segment) = self.locate_segment(expected.1) else {
                        backoff.spin();
                        continue;
                    };
                    let lease = SegmentReferenceGuard::try_new(segment);
                    #[cfg(test)]
                    if let Some(hook) = self.snapshot_read_lease_hook.lock().clone() {
                        hook(*id, expected.1, lease.is_some());
                    }
                    let Some(_lease) = lease else {
                        backoff.spin();
                        continue;
                    };
                    if node.load() != expected {
                        backoff.spin();
                        continue;
                    }
                    return materialize(expected.1, node.entry_size).map(SnapshotRead::Present);
                }
                SnapshotRevision::Deleted(node) => {
                    return Ok(SnapshotRead::Absent(Some(node.revision_ts)));
                }
                SnapshotRevision::NeverExisted => return Ok(SnapshotRead::Absent(None)),
                SnapshotRevision::Wait => return Ok(SnapshotRead::Wait),
                SnapshotRevision::TooOld => return Err(ReadError::SnapshotTooOld),
            }
        }
    }

    fn read_cell_snapshot(
        &self,
        id: &Id,
        snapshot_ts: u64,
    ) -> Result<SnapshotRead<OwnedCell>, ReadError> {
        self.read_snapshot_at(id, snapshot_ts, |location, _| {
            self.read_cell_at(id.lower, location)
        })
    }

    fn read_selected_snapshot(
        &self,
        id: &Id,
        snapshot_ts: u64,
        fields: &[u64],
    ) -> Result<SnapshotRead<OwnedCell>, ReadError> {
        self.read_snapshot_at(id, snapshot_ts, |location, _| {
            self.read_selected_at(location, fields, true)
        })
    }

    fn head_snapshot(
        &self,
        id: &Id,
        snapshot_ts: u64,
    ) -> Result<SnapshotRead<CellHeader>, ReadError> {
        self.read_snapshot_at(id, snapshot_ts, |location, _| self.head_at(location))
    }

    fn read_partial_raw_snapshot(
        &self,
        id: &Id,
        snapshot_ts: u64,
        offset: usize,
        len: usize,
    ) -> Result<SnapshotRead<Vec<u8>>, ReadError> {
        self.read_snapshot_at(id, snapshot_ts, |location, entry_size| {
            self.read_partial_raw_at(location, entry_size, offset, len)
        })
    }

    fn promote_revision(&self, installed: &InstalledRevision) -> Result<(), WriteError> {
        if self
            .history
            .current(&installed.id)
            .is_some_and(|current| Arc::ptr_eq(&current, &installed.node))
            && installed.node.promote()
        {
            Ok(())
        } else {
            Err(WriteError::CellRevisionMismatch)
        }
    }

    fn abort_revision(&self, installed: &InstalledRevision) -> Result<(), WriteError> {
        if self
            .history
            .current(&installed.id)
            .is_some_and(|current| Arc::ptr_eq(&current, &installed.node))
            && installed.node.abort()
        {
            Ok(())
        } else {
            Err(WriteError::CellRevisionMismatch)
        }
    }

    fn invalidate_recovered_revision(
        &self,
        id: &Id,
        installed_revision_ts: u64,
    ) -> Result<InstalledRevision, WriteError> {
        let current = self
            .history
            .current(id)
            .filter(|current| current.revision_ts == installed_revision_ts)
            .ok_or(WriteError::CellRevisionMismatch)?;
        let (state, location) = current.load();
        let mirror_matches = match state {
            RevisionState::CommittedPresent => CellGuard::for_read(id.lower, self)
                .ok()
                .and_then(|mut guard| {
                    let header = guard.head_cell().ok()?;
                    Some(
                        guard.get_ptr() == location
                            && Id::from_header(&header) == *id
                            && header.revision_ts == installed_revision_ts,
                    )
                })
                .unwrap_or(false),
            RevisionState::CommittedDeleted => matches!(
                CellGuard::for_read(id.lower, self),
                Err(ReadError::CellDoesNotExisted)
            ),
            RevisionState::PendingPresent
            | RevisionState::PendingDeleted
            | RevisionState::Aborted
            | RevisionState::Expired => false,
        };
        if !mirror_matches || !current.compare_exchange_state(state, RevisionState::Aborted) {
            return Err(WriteError::CellRevisionMismatch);
        }
        Ok(InstalledRevision {
            id: *id,
            node: current,
        })
    }

    fn write_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let revision_ts = self.next_revision_ts(0)?;
        self.write_cell_at_revision(cell, RevisionWrite::committed(revision_ts))?;
        Ok(cell.header)
    }

    fn update_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.hash;
        let mut guard =
            CellGuard::for_write(hash, true, self).ok_or(WriteError::CellDoesNotExisted)?;
        self.update_cell_with_guard(&mut guard, cell)?;
        Ok(cell.header)
    }

    pub fn upsert_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.hash;
        let mut guard = self.lock_or_insert_cell(hash);
        if guard.is_unassigned() {
            let previous_revision_ts = self
                .history
                .current(&cell.id())
                .map(|node| node.revision_ts)
                .unwrap_or(0);
            let revision_ts = self.next_revision_ts(previous_revision_ts)?;
            self.write_cell_with_guard_at_revision(
                &mut guard,
                cell,
                RevisionWrite::committed(revision_ts),
            )?;
        } else {
            self.update_cell_with_guard(&mut guard, cell)?;
        }
        Ok(cell.header)
    }

    fn upsert_cell_at_revision(
        &self,
        cell: &mut OwnedCell,
        write: RevisionWrite,
    ) -> Result<InstalledRevision, WriteError> {
        Self::validate_assigned_revision(write)?;
        let hash = cell.header.hash;
        let mut guard = self.lock_or_insert_cell(hash);
        if guard.is_unassigned() {
            self.write_cell_with_guard_at_revision(&mut guard, cell, write)
        } else {
            self.update_cell_with_guard_at_revision(&mut guard, cell, write)
        }
    }

    fn update_cell_by<U>(&self, hash: u64, update: U) -> Result<OwnedCell, WriteError>
    where
        U: FnOnce(&SharedCellData) -> Option<OwnedCell>,
    {
        let mut guard =
            CellGuard::for_write(hash, true, self).ok_or(WriteError::CellDoesNotExisted)?;
        let (current, _) = SharedCellData::from_chunk_raw(hash, guard.get_ptr(), self)
            .map_err(WriteError::ReadError)?;
        let mut new_cell = update(&current).ok_or(WriteError::UserCanceledUpdate)?;
        if new_cell.id() != current.id() {
            return Err(WriteError::CellRevisionMismatch);
        }
        self.update_cell_with_guard(&mut guard, &mut new_cell)?;
        Ok(new_cell)
    }

    fn remove_cell(&self, hash: u64) -> Result<(), WriteError> {
        let mut guard =
            CellGuard::for_write(hash, true, self).ok_or(WriteError::CellDoesNotExisted)?;
        let header = guard.head_cell().map_err(WriteError::ReadError)?;
        let revision_ts = self.next_revision_ts(header.revision_ts)?;
        self.remove_cell_with_guard_at_revision(
            guard,
            &header.id(),
            RevisionWrite::committed(revision_ts),
        )?;
        Ok(())
    }

    fn remove_cell_by<P>(&self, hash: u64, predict: P) -> Result<(), WriteError>
    where
        P: Fn(&SharedCell) -> bool,
    {
        let guard = CellGuard::for_write(hash, true, self).ok_or(WriteError::CellDoesNotExisted)?;
        let (cell, _) =
            SharedCell::from_chunk_raw(hash, guard, self).map_err(WriteError::ReadError)?;
        if !predict(&cell) {
            return Err(WriteError::CellDoesNotExisted);
        }
        let header = cell.header;
        let revision_ts = self.next_revision_ts(header.revision_ts)?;
        self.remove_cell_with_guard_at_revision(
            cell.into_cell_guard(),
            &header.id(),
            RevisionWrite::committed(revision_ts),
        )?;
        Ok(())
    }

    #[inline(always)]
    pub fn put_segment(&self, segment: Segment) {
        debug!(
            "Putting segment for chunk {} with id {}",
            self.id, segment.id
        );
        let segment_id = segment.id;
        let segment_key = segment_id as usize;
        let is_hot = segment.is_hot();

        // Update cached hot count BEFORE adding to list to avoid race with full scan
        // If we increment after adding, a full scan could count the new segment
        // and update the cache, then we'd increment again, leading to over-counting
        if is_hot {
            if let Some(ref tiered_manager) = self.tiered_manager {
                tiered_manager.increment_hot_count();
            }
        }

        self.segs.insert_back(segment_key, AArc::new(segment));
    }

    pub fn remove_segment(&self, segment_id: u64) -> io::Result<bool> {
        debug!(
            "Removing segment for chunk {} with id {}",
            self.id, segment_id
        );

        // Check if segment is hot BEFORE removing to avoid race with full scan
        // If we decrement after removing, a full scan could miss the removed segment
        // and update the cache, then we'd decrement again, leading to under-counting
        let Some(segment) = self.segs.get(&(segment_id as usize)) else {
            error!(
                "Segment {} not found in chunk {} to remove",
                segment_id, self.id
            );
            return Ok(false);
        };
        let should_decrement = segment.is_hot();
        if !should_decrement {
            error!(
                "Segment {} is not hot in chunk {} to remove",
                segment_id, self.id
            );
        }

        // Keep the segment registered and its memory readable until every
        // source filename removal is durably published.
        segment.dispense()?;

        // Decrement cache BEFORE removing from list
        if should_decrement {
            if let Some(ref tiered_manager) = self.tiered_manager {
                tiered_manager.decrement_hot_count();
            }
        }

        // Now safe to remove and dispose
        if let Some(seg) = self.segs.remove(&(segment_id as usize)) {
            // Free the segment memory
            seg.free_memory();
            Ok(true)
        } else {
            error!(
                "Segment {} not found in chunk {} to remove",
                segment_id, self.id
            );
            Ok(false)
        }
    }

    pub fn locate_segment(&self, addr: usize) -> Option<AArc<Segment>> {
        let seg_id = self.allocator.id_by_addr(addr);
        let res = self.segs.get(&seg_id);
        if res.is_none() {
            // Segment doesn't exist - this can happen when the cleaner combines segments
            // and removes old ones. The address in cell_index may be stale.
            // Callers should handle this by re-reading from cell_index or retrying.
            debug!(
                "Cannot locate segment for {:?}, got id {}, chunk segs {:?} (segment may have been removed by cleaner)",
                addr,
                seg_id,
                self.segs.iter_front_keys().collect::<Vec<_>>()
            );
        }
        return res;
    }

    fn locate_segment_ensured(&self, cell_location: usize, cell_id: &Id) -> AArc<Segment> {
        self.locate_segment(cell_location).expect(
            format!(
                "Cannot locate cell segment for cell id: {:?} at {}",
                cell_id, cell_location
            )
            .as_str(),
        )
    }

    // Mark entry as dead with explicit size (safer - doesn't need to decode)
    #[inline]
    pub fn mark_dead_entry_with_size(&self, addr: usize, size: u32, seg: &Segment) {
        trace!(
            "Marking {} bytes as dead at addr 0x{:016x} in segment {}",
            size,
            addr,
            seg.id
        );
        seg.dead_space.fetch_add(size, Ordering::Relaxed);
        seg.note_dead_bytes_change();
    }

    pub fn drain_history_dead(&self) {
        while let Some(dead) = self.history.pop_dead() {
            if let Some(segment) = self.locate_segment(dead.location) {
                self.mark_dead_entry_with_size(dead.location, dead.entry_size, &segment);
            } else {
                warn!(
                    "Cannot account expired revision at 0x{:016x}: segment is no longer present in chunk {}",
                    dead.location, self.id
                );
            }
        }
    }

    pub(crate) fn compare_exchange_current_address(
        &self,
        id: Id,
        revision_ts: u64,
        old_location: usize,
        new_location: usize,
    ) -> CurrentAddressRelocation {
        let index = self.cell_index.lock(id.lower as usize);
        if let Some(mut current_location) = index {
            if *current_location == old_location {
                let Ok((header, _)) = header_from_chunk_raw(old_location) else {
                    return CurrentAddressRelocation::Inconsistent;
                };
                if Id::from_header(&header) != id || header.revision_ts != revision_ts {
                    return CurrentAddressRelocation::Inconsistent;
                }
                *current_location = new_location;
                return CurrentAddressRelocation::Moved;
            }

            // Keep the lower-hash guard through the history check. A writer
            // that won before cleaner publication has already installed a
            // successor; a writer that starts after the history-location CAS
            // cannot validate the stale mirror until this guard is released.
            if self.history.current(&id).is_some_and(|current| {
                current.revision_ts == revision_ts && current.load().1 == new_location
            }) {
                return CurrentAddressRelocation::Inconsistent;
            }
            return CurrentAddressRelocation::NoLongerCurrent;
        }

        // A concurrent delete removes the mirror only after publishing its
        // tombstone successor. Prove that transition while the absent-key
        // guard is held before allowing source reclamation.
        if self.history.current(&id).is_some_and(|current| {
            current.revision_ts == revision_ts && current.load().1 == new_location
        }) {
            CurrentAddressRelocation::Inconsistent
        } else {
            CurrentAddressRelocation::NoLongerCurrent
        }
    }

    pub(crate) fn compare_exchange_current_only_address(
        &self,
        id: Id,
        revision_ts: u64,
        old_location: usize,
        new_location: usize,
        source_segment_ids: &HashSet<u64>,
    ) -> CurrentAddressRelocation {
        let index = self.cell_index.lock(id.lower as usize);
        let Some(mut current_location) = index else {
            return CurrentAddressRelocation::Inconsistent;
        };

        if *current_location == old_location {
            let Ok((source_header, _)) = header_from_chunk_raw(old_location) else {
                return CurrentAddressRelocation::Inconsistent;
            };
            let Ok((destination_header, _)) = header_from_chunk_raw(new_location) else {
                return CurrentAddressRelocation::Inconsistent;
            };
            if Id::from_header(&source_header) != id
                || source_header.revision_ts != revision_ts
                || Id::from_header(&destination_header) != id
                || destination_header.revision_ts != revision_ts
            {
                return CurrentAddressRelocation::Inconsistent;
            }
            *current_location = new_location;
            return CurrentAddressRelocation::Moved;
        }

        if *current_location == 0 {
            return CurrentAddressRelocation::Inconsistent;
        }
        let Some(current_segment) = self
            .segments()
            .into_iter()
            .find(|segment| segment.contains_address(*current_location))
        else {
            return CurrentAddressRelocation::Inconsistent;
        };
        if source_segment_ids.contains(&current_segment.id) {
            return CurrentAddressRelocation::Inconsistent;
        }
        let Some(header_end) = current_location
            .checked_add(ENTRY_HEAD_SIZE)
            .and_then(|entry_content| entry_content.checked_add(CELL_HEADER_SIZE))
        else {
            return CurrentAddressRelocation::Inconsistent;
        };
        if header_end > current_segment.append_header.load(Ordering::Acquire) {
            return CurrentAddressRelocation::Inconsistent;
        }
        let Ok((current_header, _)) = header_from_chunk_raw(*current_location) else {
            return CurrentAddressRelocation::Inconsistent;
        };
        if current_header.hash != id.lower {
            return CurrentAddressRelocation::Inconsistent;
        }
        let current_id = Id::from_header(&current_header);
        if *current_location == new_location {
            return if current_id == id && current_header.revision_ts == revision_ts {
                CurrentAddressRelocation::Moved
            } else {
                CurrentAddressRelocation::Inconsistent
            };
        }
        if current_id == id && current_header.revision_ts < revision_ts {
            return CurrentAddressRelocation::Inconsistent;
        }
        CurrentAddressRelocation::NoLongerCurrent
    }

    // Decodes entry to get size and marks it dead
    // WARNING: Will panic if memory at addr is corrupted!
    // Prefer mark_dead_entry_with_size when size is known
    #[inline]
    pub fn mark_dead_entry_with_seg(&self, addr: usize, seg: &Segment) {
        #[cfg(debug_assertions)]
        {
            if addr % 8 != 0 {
                panic!(
                    "CORRUPTION: mark_dead_entry_with_seg received misaligned addr=0x{:016x} (offset: {}) for segment {}. \
                    This address should have been validated earlier.",
                    addr, addr % 8, seg.id
                );
            }
        }

        // Decode entry to get its content_length
        // This will PANIC if memory is corrupted - which is intentional!
        // We want to know about memory corruption issues immediately
        let (entry, _) = Entry::decode_from(addr, |_, _| {});
        self.mark_dead_entry_with_size(addr, entry.content_length, seg);
    }

    pub fn mark_dead_entry_with_cell<C: Cell>(&self, addr: usize, cell: &C) {
        let seg = self.locate_segment_ensured(addr, &cell.id());
        self.mark_dead_entry_with_seg(addr, &seg)
    }

    pub fn contains_seg(&self, seg_id: u64) -> bool {
        self.segs.contains_key(&(seg_id as usize))
    }

    pub fn segment_ids(&self) -> Vec<usize> {
        self.segs.iter_front_keys().collect()
    }

    pub fn segments(&self) -> Vec<AArc<Segment>> {
        self.segs.iter_front_values().collect()
    }

    pub fn segs_for_combine_cleaner(&self) -> Vec<(AArc<Segment>, f32)> {
        self.segs_for_combine_cleaner_impl(false)
    }

    pub fn segs_for_combine_cleaner_full(&self) -> Vec<(AArc<Segment>, f32)> {
        self.segs_for_combine_cleaner_impl(true)
    }

    fn choose_combine_candidate_class(mapping: &[(AArc<Segment>, f32)]) -> Option<SegmentClass> {
        let preferred_class = mapping.first().map(|(seg, _)| seg.segment_class())?;
        let mut blob_count = 0;
        let mut regular_count = 0;

        for (seg, _) in mapping {
            match seg.segment_class() {
                SegmentClass::Blob => blob_count += 1,
                SegmentClass::Regular => regular_count += 1,
            }
        }

        let preferred_count = match preferred_class {
            SegmentClass::Blob => blob_count,
            SegmentClass::Regular => regular_count,
        };

        if preferred_count >= 2 {
            return Some(preferred_class);
        }

        if blob_count >= 2 {
            return Some(SegmentClass::Blob);
        }

        if regular_count >= 2 {
            return Some(SegmentClass::Regular);
        }

        None
    }

    fn segs_for_combine_cleaner_impl(&self, full: bool) -> Vec<(AArc<Segment>, f32)> {
        let mut mapping: Vec<_> = self
            .segments()
            .into_iter()
            .map(|seg| {
                let living = seg.living_space() as f32;
                let segment_utilization = living / SEGMENT_SIZE_U32 as f32;
                (seg, segment_utilization)
            })
            .filter(|(seg, utilization)| {
                // Always require some dead space (utilization < 100%)
                // For full GC, accept any segment with dead space
                // For partial GC, only consider high-dead segments
                *utilization < 1.0
                    && (full || *utilization < DEAD_RATE_FOR_COMBINE_CLEANER)
                    && !self.is_active_head(seg.id)
                    && seg.is_hot() // Don't clean cold segments (tiered memory)
                    && !seg.cleaned_without_progress()
            })
            .collect();
        mapping.sort_by(|(_, util1), (_, util2)| util1.partial_cmp(util2).unwrap());

        let Some(preferred_class) = Self::choose_combine_candidate_class(&mapping) else {
            return Vec::new();
        };

        mapping.retain(|(seg, _)| seg.segment_class() == preferred_class);

        let max_segments = if full {
            mapping.len()
        } else {
            MAX_SEGMENTS_FOR_CLEANER
        };
        mapping.truncate(max_segments);
        return mapping;
    }

    /// Get segment information for a cell based on its memory address.
    /// Returns (segment_id, seq_id) for the segment containing the cell.
    pub fn get_cell_segment_info(&self, cell_addr: usize) -> (u64, u64) {
        let segment_id = self.allocator.id_by_addr(cell_addr) as u64;
        if let Some(segment) = self.segs.get(&(segment_id as usize)) {
            (segment.id, segment.seq_id)
        } else {
            unreachable!("Cannot find segment for cell at address {}", cell_addr)
        }
    }

    pub fn live_entries<'a>(&'a self, seg: &'a Segment) -> impl Iterator<Item = Entry> + 'a {
        seg.entry_iter()
            .filter_map(move |entry_meta| {
                let chunk_id = &self.id;
                let chunk_index = &self.cell_index;
                let chunk_segs = &self.segs;
                let entry_size = entry_meta.entry_size;
                let entry_header = entry_meta.entry_header;
                trace!("Iterating live entries on chunk {} segment {}. Got {:?} at {} size {}",
                       chunk_id, seg.id, entry_header.entry_type, entry_meta.entry_pos, entry_size);
                // Validate entry type is a known valid type (CELL or TOMBSTONE)
                // Invalid types indicate we're reading garbage (possibly from inside another entry)
                // which can happen if append_header was set incorrectly by a previous operation
                debug_assert!(entry_header.entry_type == EntryType::CELL || entry_header.entry_type == EntryType::TOMBSTONE);

                // Validate that entry size is reasonable (must be at least header size and 8-byte aligned)
                // Real entries are always 8-byte aligned; non-aligned sizes indicate corruption
                debug_assert!(entry_meta.entry_size % 8 == 0);
                debug_assert!(entry_meta.entry_size >= ENTRY_HEAD_SIZE);
                if entry_header.entry_type == EntryType::CELL {
                        trace!("Entry at {} is a cell", entry_meta.entry_pos);
                        let cell_header =
                            cell_header_from_entry_content_addr(entry_meta.body_pos);
                        trace!("Cell header read, id is {:?}", cell_header.id());
                        let expect = Some(entry_meta.entry_pos);
                        let actual = chunk_index.get_from_mutex(&(cell_header.hash as usize));
                        if expect == actual {
                            trace!(
                                "Cell entry {:?} is valid", cell_header.id()
                            );
                            return Some(Entry {
                                meta: entry_meta,
                                content: EntryContent::Cell(cell_header)
                            });
                        } else {
                            trace!(
                                "Cell entry index mismatch for {:?}. Expect {:?}, actual {:?}, will be ditched", 
                                cell_header.id(), expect, actual
                            );
                        }
                } else if entry_header.entry_type == EntryType::TOMBSTONE {
                        trace!("Entry at {} is a tombstone", entry_meta.entry_pos);
                        let tombstone =
                            Tombstone::read_from_entry_content_addr(entry_meta.body_pos);
                        let contains_seg = chunk_segs.contains_seq_id(tombstone.segment_seq_id);
                        if contains_seg {
                            trace!("Tomestone entry {:?} - {:?} at seq_id {} is valid",
                                   tombstone.partition, tombstone.hash, tombstone.segment_seq_id);
                            return Some(Entry {
                                meta: entry_meta,
                                content: EntryContent::Tombstone(tombstone)
                            });
                        } else {
                            trace!("Tombstone target at seq_id {} have been removed, will be ditched", tombstone.segment_seq_id)
                        }
                } else {
                    unreachable!(
                        "Unexpected cell type on getting live entries at {}: type {:?}, size {}, append header {}, ends at {}",
                        entry_meta.entry_pos,
                        entry_header.entry_type.bits(),
                        entry_size,
                        seg.append_header.load(Ordering::Relaxed),
                        entry_meta.entry_pos + entry_size
                    )
                }
                return None
            })
    }

    pub fn cell_count(&self) -> usize {
        self.cell_index.len()
    }

    pub fn seg_count(&self) -> usize {
        self.segs.len()
    }

    pub fn count(&self) -> usize {
        self.cell_index.len()
    }

    #[inline]
    fn refresh_statistics(&self) {
        self.statistics.refresh_from_chunk(self)
    }

    pub fn lock_cell_for_read(&self, hash: u64) -> Result<CellGuard<'_>, ReadError> {
        CellGuard::for_read(hash, self)
    }

    pub fn lock_cell_for_write(
        &self,
        hash: u64,
        has_read: bool,
    ) -> Result<CellGuard<'_>, ReadError> {
        CellGuard::for_write(hash, has_read, self).ok_or(ReadError::CellDoesNotExisted)
    }

    pub fn compare_revision_and_update_cell(
        &self,
        hash: u64,
        revision_ts: u64,
        cell: &mut OwnedCell,
    ) -> Result<CellHeader, WriteError> {
        let mut guard = self
            .lock_cell_for_write(hash, true)
            .map_err(WriteError::ReadError)?;
        let cell_revision_ts = guard.cell_revision_ts().map_err(WriteError::ReadError)?;
        if cell_revision_ts == revision_ts {
            cell.header.revision_ts = revision_ts;
            guard.update_cell(cell)?;
            return Ok(cell.header);
        }
        return Err(WriteError::CellRevisionMismatch);
    }

    pub fn compare_revision_and_set_field(
        &self,
        hash: u64,
        revision_ts: u64,
        field: u64,
        value: OwnedValue,
    ) -> Result<CellHeader, WriteError> {
        let mut guard = self
            .lock_cell_for_write(hash, true)
            .map_err(WriteError::ReadError)?;
        let mut cell = guard.read_cell_owned().map_err(WriteError::ReadError)?;
        if cell.header.revision_ts == revision_ts {
            cell.data[field] = value;
            guard.update_cell(&mut cell)?;
            return Ok(cell.header);
        }
        return Err(WriteError::CellRevisionMismatch);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CurrentAddressRelocation {
    Moved,
    NoLongerCurrent,
    Inconsistent,
}

pub struct PendingEntry {
    pub seg: AArc<Segment>,
    pub addr: usize,
    pub size: u32,
    pub skip_sync: bool, // Skip fsync if part of a transaction (will be synced at commit)
}

impl PendingEntry {
    /// Publish the newly written entry to its segment WAL. Callers must finish
    /// this fallible step before installing the entry in revision history.
    pub fn finish(self) -> Result<(), WriteError> {
        if let Err(error) = self.seg.write_wal(self.addr, self.size, self.skip_sync) {
            // The segment append cursor has already reserved these bytes, but
            // no revision points at them. Account the orphan immediately so
            // repeated retryable durability failures cannot leak live space.
            self.seg.dead_space.fetch_add(self.size, Ordering::Relaxed);
            self.seg.note_dead_bytes_change();
            return Err(WriteError::DurabilityFailure(error.to_string()));
        }
        self.seg.set_dirty();
        Ok(())
    }
}

impl Drop for PendingEntry {
    fn drop(&mut self) {
        self.seg.decr_references();
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        self.history.shutdown();
    }
}

pub struct Chunks {
    pub list: Vec<Chunk>,
    pub statistics: TTLCache<Arc<SchemaStatistics>>,
    pub tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
    pub revision_clock: Arc<HlcSource>,
    pub history_retention_ms: u64,
}

enum ChunkConstruction {
    Empty,
    Recover { raft_storage: Option<String> },
}

impl Chunks {
    pub fn new(
        count: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
    ) -> Arc<Chunks> {
        Self::new_with_clock(
            count,
            size,
            meta,
            index_builder,
            backup_storage,
            wal_storage,
            tiered_manager,
            Arc::new(HlcSource::new(0)),
            300_000,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_clock(
        count: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
        revision_clock: Arc<HlcSource>,
        history_retention_ms: u64,
    ) -> Arc<Chunks> {
        Self::try_construct_with_clock(
            count,
            size,
            meta,
            index_builder,
            backup_storage,
            wal_storage,
            tiered_manager,
            revision_clock,
            history_retention_ms,
            ChunkConstruction::Empty,
        )
        .expect("chunk construction failed")
        .0
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn recover_with_clock(
        count: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
        raft_storage: Option<String>,
        revision_clock: Arc<HlcSource>,
        history_retention_ms: u64,
    ) -> io::Result<(Arc<Chunks>, crate::ram::recovery::RecoverySummary)> {
        Self::try_construct_with_clock(
            count,
            size,
            meta,
            index_builder,
            backup_storage,
            wal_storage,
            tiered_manager,
            revision_clock,
            history_retention_ms,
            ChunkConstruction::Recover { raft_storage },
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_recovery_for_test(
        count: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
        enable_recovery: bool,
        raft_storage: Option<String>,
    ) -> Arc<Chunks> {
        if !enable_recovery {
            return Self::new_with_clock(
                count,
                size,
                meta,
                index_builder,
                backup_storage,
                wal_storage,
                tiered_manager,
                Arc::new(HlcSource::new(0)),
                300_000,
            );
        }
        let (chunks, recovery) = Self::recover_with_clock(
            count,
            size,
            meta,
            index_builder,
            backup_storage,
            wal_storage,
            tiered_manager,
            raft_storage,
            Arc::new(HlcSource::new(0)),
            300_000,
        )
        .expect("test segment recovery failed");
        chunks
            .revision_clock()
            .try_observe(bifrost::hlc::Hlc {
                ts: recovery.max_revision_ts,
                node: chunks.revision_clock().node(),
            })
            .expect("test recovered revision clock exhausted");
        chunks
            .establish_recovery_floor()
            .expect("test recovery snapshot floor clock exhausted");
        chunks
    }

    #[allow(clippy::too_many_arguments)]
    fn try_construct_with_clock(
        count: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
        revision_clock: Arc<HlcSource>,
        history_retention_ms: u64,
        construction: ChunkConstruction,
    ) -> io::Result<(Arc<Chunks>, crate::ram::recovery::RecoverySummary)> {
        use libc::{MAP_ANONYMOUS, MAP_PRIVATE, PROT_READ, PROT_WRITE};
        use std::ptr;

        // Calculate exact chunk size
        let chunk_size = size.next_power_of_two();
        let chunk_size_bits = chunk_size.trailing_zeros() as usize;

        // Allocate one giant mmap for all chunks
        let total_size = chunk_size * count;
        let global_base = unsafe {
            libc::mmap(
                ptr::null_mut(),
                total_size,
                PROT_READ | PROT_WRITE,
                MAP_ANONYMOUS | MAP_PRIVATE,
                -1,
                0,
            )
        };

        if global_base == libc::MAP_FAILED {
            let errno = std::io::Error::last_os_error();
            panic!(
                "Failed to allocate {} bytes for {} chunks (chunk_size: {} bytes). \
                Error: {} (errno: {}). \
                This could be due to: insufficient memory, system limits (ulimit -v), \
                or memory fragmentation. Try reducing total_size or chunk_count.",
                total_size,
                count,
                chunk_size,
                errno,
                errno.raw_os_error().unwrap_or(-1)
            );
        }

        let global_base_addr = global_base as usize;

        // Store global state
        GLOBAL_CHUNK_BASE.store(global_base_addr, Ordering::Release);
        GLOBAL_CHUNK_SIZE_BITS.store(chunk_size_bits, Ordering::Release);
        GLOBAL_CHUNK_COUNT.store(count, Ordering::Release);
        GLOBAL_ALLOCATED_SIZE.store(total_size, Ordering::Release);

        info!(
            "Allocated global chunk space: base={:#x}, chunk_size={} (2^{}), count={}, total={}",
            global_base_addr, chunk_size, chunk_size_bits, count, total_size
        );

        if let Some(ref manager) = tiered_manager {
            info!(
                "Tiered memory enabled: threshold={}, limit={} MB, shared across all {} chunks",
                manager.shared_pool().threshold,
                manager.shared_pool().physical_memory_limit / (1024 * 1024),
                count,
            );
        }

        let mut chunks = Vec::new();
        assert!(size >= SEGMENT_SIZE);
        debug!(
            "Creating chunks, count {} , chunk_size {} bytes",
            count, size
        );
        for i in 0..count {
            let chunk_base = global_base_addr + (i * chunk_size);
            let backup_storage = backup_storage
                .clone()
                .map(|dir| format!("{}/chunk-bk-{}", dir, i));
            let wal_storage = wal_storage
                .clone()
                .map(|dir| format!("{}/chunk-wal-{}", dir, i));
            chunks.push(Chunk::new_with_base(
                i,
                chunk_base,
                chunk_size,
                meta.clone(),
                index_builder.clone(),
                backup_storage,
                wal_storage,
                tiered_manager.clone(),
                revision_clock.clone(),
                history_retention_ms,
            ));
        }
        let num_schemas = meta.schemas.count() + 1;
        let chunks_arc = Arc::new(Chunks {
            list: chunks,
            statistics: TTLCache::with_capacity(num_schemas.next_power_of_two()),
            tiered_manager,
            revision_clock,
            history_retention_ms,
        });

        if let Some(ref manager) = chunks_arc.tiered_manager {
            manager.register_chunks(&chunks_arc);
        }

        // Store global pointer for signal handler access
        set_global_chunks(&chunks_arc);

        let recovery = match construction {
            ChunkConstruction::Empty => crate::ram::recovery::RecoverySummary::default(),
            ChunkConstruction::Recover { raft_storage } => {
                info!("Recovery enabled, attempting to recover from storage");

                let config = crate::ram::recovery::RecoveryConfig {
                    num_chunks: count,
                    chunk_size,
                    max_threads: Some(64), // Cap recovery parallelism to reduce contention storms
                };

                let recovery = crate::ram::recovery::recover_chunks(
                    &config,
                    &backup_storage,
                    &wal_storage,
                    &raft_storage,
                    &chunks_arc.list,
                )?;
                info!("Recovery completed successfully");
                recovery
            }
        };

        Ok((chunks_arc, recovery))
    }

    pub fn next_revision_ts(&self, previous: u64) -> Result<u64, WriteError> {
        let next = self
            .revision_clock
            .try_now()
            .map_err(|_| WriteError::RevisionClockExhausted)?
            .ts;
        if next <= previous {
            return Err(WriteError::RevisionClockExhausted);
        }
        Ok(next)
    }

    pub fn revision_clock(&self) -> &Arc<HlcSource> {
        &self.revision_clock
    }

    pub(crate) fn durable_storage_configured(&self) -> bool {
        self.list
            .iter()
            .any(|chunk| chunk.wal_storage.is_some() || chunk.backup_storage.is_some())
    }

    pub(crate) fn wal_storage_configured(&self) -> bool {
        !self.list.is_empty() && self.list.iter().all(|chunk| chunk.wal_storage.is_some())
    }

    pub fn establish_recovery_floor(&self) -> Result<u64, WriteError> {
        let recovery_floor = self
            .revision_clock
            .try_now()
            .map_err(|_| WriteError::RevisionClockExhausted)?
            .ts;
        for chunk in &self.list {
            chunk.history.set_recovery_floor(recovery_floor);
        }
        Ok(recovery_floor)
    }

    /// Sync all buffered WAL data to disk across all chunks
    pub fn sync_all(&self) {
        info!("Syncing WAL for all chunks...");
        for chunk in &self.list {
            for segment in chunk.segs.iter_values() {
                if let Err(error) = segment.force_wal_sync() {
                    error!(
                        "Failed to sync WAL for chunk {} segment {}: {}",
                        chunk.id, segment.seq_id, error
                    );
                }
            }
        }
        info!("All WAL data synced to disk.");
    }

    /// Archive all dirty segments to backup storage across all chunks
    /// This ensures all in-memory data is persisted to backup files before shutdown
    pub fn archive_all(&self) {
        info!("Archiving all dirty segments to backup storage...");
        let mut total_archived = 0;
        let mut total_skipped = 0;
        let mut total_errors = 0;

        for chunk in &self.list {
            for segment in chunk.segs.iter_values() {
                // Check if segment needs archiving
                let is_clean = !segment.is_dirty();

                if is_clean {
                    total_skipped += 1;
                    continue;
                }

                // Archive the segment
                match segment.archive() {
                    Ok(true) => {
                        debug!("Archived segment {} (chunk {})", segment.id, chunk.id);
                        total_archived += 1;
                    }
                    Ok(false) => {
                        debug!("Segment {} already archived", segment.id);
                        total_skipped += 1;
                    }
                    Err(e) => {
                        error!(
                            "Failed to archive segment {} (chunk {}): {}",
                            segment.id, chunk.id, e
                        );
                        total_errors += 1;
                    }
                }
            }
        }

        info!(
            "Segment archiving complete: {} archived, {} skipped (clean), {} errors",
            total_archived, total_skipped, total_errors
        );
    }

    pub fn new_dummy(count: usize, size: usize) -> Arc<Chunks> {
        // Dummy doesn't use tiered memory or recovery
        Chunks::new(
            count,
            size,
            Arc::<ServerMeta>::new(ServerMeta {
                schemas: LocalSchemasCache::new_local(""),
            }),
            None,
            None,
            None,
            None,
        )
    }
    pub fn locate_chunk_by_partition(&self, partition: u64) -> &Chunk {
        let chunk_id = partition as usize % self.list.len();
        return &self.list[chunk_id];
    }
    fn locate_chunk_by_key(&self, key: &Id) -> (&Chunk, u64) {
        return (self.locate_chunk_by_partition(key.higher), key.lower);
    }
    pub fn read_cell(&self, key: &Id) -> Result<SharedCell<'_>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_cell(hash);
    }
    pub fn read_cell_snapshot(
        &self,
        key: &Id,
        snapshot_ts: u64,
    ) -> Result<SnapshotRead<OwnedCell>, ReadError> {
        let chunk = self.locate_chunk_by_partition(key.higher);
        chunk.read_cell_snapshot(key, snapshot_ts)
    }
    // By-address full-cell read: materializes the cell exactly as stored at
    // `location`, regardless of where the cell index currently points. Used by
    // repeatable-read pinning to re-read a specific version whose address and
    // segment guard were captured earlier.
    pub fn read_cell_at(&self, key: &Id, location: usize) -> Result<OwnedCell, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_cell_at(hash, location);
    }
    pub fn read_selected(
        &self,
        key: &Id,
        fields: &[u64],
        need_header: bool,
    ) -> Result<SharedCell<'_>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_selected(hash, fields, need_header);
    }
    pub fn read_selected_snapshot(
        &self,
        key: &Id,
        snapshot_ts: u64,
        fields: &[u64],
    ) -> Result<SnapshotRead<OwnedCell>, ReadError> {
        let chunk = self.locate_chunk_by_partition(key.higher);
        chunk.read_selected_snapshot(key, snapshot_ts, fields)
    }
    // By-address projected read: same field-projection logic as `read_selected`,
    // pinned to `location` instead of the cell index.
    pub fn read_selected_at(
        &self,
        key: &Id,
        location: usize,
        fields: &[u64],
    ) -> Result<OwnedCell, ReadError> {
        let chunk = self.locate_chunk_by_partition(key.higher);
        return chunk.read_selected_at(location, fields, true);
    }
    pub fn read_partial_raw(
        &self,
        key: &Id,
        offset: usize,
        len: usize,
    ) -> Result<Vec<u8>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_partial_raw(hash, offset, len);
    }
    pub fn read_partial_raw_snapshot(
        &self,
        key: &Id,
        snapshot_ts: u64,
        offset: usize,
        len: usize,
    ) -> Result<SnapshotRead<Vec<u8>>, ReadError> {
        let chunk = self.locate_chunk_by_partition(key.higher);
        chunk.read_partial_raw_snapshot(key, snapshot_ts, offset, len)
    }
    pub fn head_cell(&self, key: &Id) -> Result<CellHeader, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.head_cell(hash);
    }
    pub fn head_snapshot(
        &self,
        key: &Id,
        snapshot_ts: u64,
    ) -> Result<SnapshotRead<CellHeader>, ReadError> {
        let chunk = self.locate_chunk_by_partition(key.higher);
        chunk.head_snapshot(key, snapshot_ts)
    }
    // By-address header read: same as `head_cell` but pinned to `location`
    // instead of resolving through the cell index.
    pub fn head_at(&self, key: &Id, location: usize) -> Result<CellHeader, ReadError> {
        let chunk = self.locate_chunk_by_partition(key.higher);
        return chunk.head_at(location);
    }
    // Cheap capture of the current cell's raw address and revision (index lookup
    // + header decode, no value materialization). See `Chunk::cell_location_and_revision`.
    pub fn cell_location_and_revision(&self, key: &Id) -> Result<(usize, u64), ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.cell_location_and_revision(hash);
    }
    pub fn location_for_read(&self, key: &Id) -> Result<CellReadGuard<'_>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        chunk.location_for_read(hash)
    }
    pub fn write_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.write_cell(cell);
    }
    pub fn write_cell_at_revision(
        &self,
        cell: &mut OwnedCell,
        write: RevisionWrite,
    ) -> Result<InstalledRevision, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        chunk.write_cell_at_revision(cell, write)
    }
    pub fn update_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.update_cell(cell);
    }
    pub fn update_cell_at_revision(
        &self,
        cell: &mut OwnedCell,
        write: RevisionWrite,
    ) -> Result<InstalledRevision, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        chunk.update_cell_at_revision(cell, write)
    }
    pub fn update_cell_by<U>(&self, key: &Id, update: U) -> Result<OwnedCell, WriteError>
    where
        U: FnOnce(&SharedCellData) -> Option<OwnedCell>,
    {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.update_cell_by(hash, update);
    }
    pub fn upsert_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.upsert_cell(cell);
    }

    fn upsert_cell_at_revision(
        &self,
        cell: &mut OwnedCell,
        write: RevisionWrite,
    ) -> Result<InstalledRevision, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        chunk.upsert_cell_at_revision(cell, write)
    }
    pub fn remove_cell(&self, key: &Id) -> Result<(), WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell(hash);
    }
    pub fn remove_cell_at_revision(
        &self,
        key: &Id,
        write: RevisionWrite,
    ) -> Result<InstalledRevision, WriteError> {
        let chunk = self.locate_chunk_by_partition(key.higher);
        chunk.remove_cell_at_revision(key, write)
    }
    #[cfg(test)]
    pub(crate) fn fail_next_allocation_for_test(&self, key: &Id) {
        self.locate_chunk_by_partition(key.higher)
            .fail_next_allocation
            .store(1, Ordering::Release);
    }
    #[cfg(test)]
    pub(crate) fn secondary_index_removal_attempts_for_test(&self, key: &Id) -> usize {
        self.locate_chunk_by_partition(key.higher)
            .secondary_index_removal_attempts
            .load(Ordering::Acquire)
    }
    pub fn remove_cell_by<P>(&self, key: &Id, predict: P) -> Result<(), WriteError>
    where
        P: Fn(&SharedCell) -> bool,
    {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell_by(hash, predict);
    }
    pub fn address_of(&self, key: &Id) -> usize {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return *chunk.location_for_read(hash).unwrap();
    }

    pub fn history_location(&self, key: &Id, revision_ts: u64) -> Option<usize> {
        self.locate_chunk_by_partition(key.higher)
            .history
            .location(key, revision_ts)
    }

    pub(crate) fn current_revision_ts(&self, key: &Id) -> Option<u64> {
        self.locate_chunk_by_partition(key.higher)
            .history
            .current(key)
            .map(|node| node.revision_ts)
    }

    pub fn promote_revision(&self, installed: &InstalledRevision) -> Result<(), WriteError> {
        self.locate_chunk_by_partition(installed.id.higher)
            .promote_revision(installed)
    }

    pub fn abort_revision(&self, installed: &InstalledRevision) -> Result<(), WriteError> {
        self.locate_chunk_by_partition(installed.id.higher)
            .abort_revision(installed)
    }

    pub(crate) fn force_sync_installed_revisions<'a>(
        &self,
        installed_revisions: impl IntoIterator<Item = &'a InstalledRevision>,
    ) -> io::Result<()> {
        let mut segment_keys = HashSet::new();
        let mut guarded_segments = Vec::new();
        for installed in installed_revisions {
            let chunk = self.locate_chunk_by_partition(installed.id.higher);
            let current = chunk.history.current(&installed.id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "installed revision {:?} has no current chain head",
                        installed.id
                    ),
                )
            })?;
            if !Arc::ptr_eq(&current, &installed.node) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "installed revision {:?} is no longer the current chain head",
                        installed.id
                    ),
                ));
            }
            let expected = installed.node.load();
            let location = expected.1;
            let segment = chunk.locate_segment(location).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "installed revision {:?} moved before its segment could be leased",
                        installed.id
                    ),
                )
            })?;
            let lease = SegmentReferenceGuard::try_new(segment.clone()).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "installed revision {:?} segment is being relocated",
                        installed.id
                    ),
                )
            })?;
            let registered = chunk.segs.get(&(segment.id as usize)).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "installed revision {:?} segment was reclaimed while acquiring its lease",
                        installed.id
                    ),
                )
            })?;
            if !std::ptr::eq::<Segment>(&*registered, &*segment)
                || installed.node.load() != expected
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "installed revision {:?} moved while acquiring its segment lease",
                        installed.id
                    ),
                ));
            }
            #[cfg(test)]
            if let Some(hook) = chunk.exact_sync_lease_hook.lock().clone() {
                hook(installed.id, location);
            }
            if chunk
                .history
                .location(&installed.id, installed.node.revision_ts)
                != Some(location)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "installed revision {:?} location no longer matches its chain node",
                        installed.id
                    ),
                ));
            }
            match expected.0 {
                RevisionState::PendingPresent | RevisionState::CommittedPresent => {
                    let header = cell_header_from_entry_content_addr(Entry::content_pos(location));
                    if Id::from_header(&header) != installed.id
                        || header.revision_ts != installed.node.revision_ts
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "installed present revision {:?} does not match its output",
                                installed.id
                            ),
                        ));
                    }
                }
                RevisionState::PendingDeleted | RevisionState::CommittedDeleted => {
                    let tombstone = Tombstone::read(location);
                    if Id::new(tombstone.partition, tombstone.hash) != installed.id
                        || tombstone.revision_ts != installed.node.revision_ts
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "installed tombstone revision {:?} does not match its output",
                                installed.id
                            ),
                        ));
                    }
                }
                RevisionState::Aborted | RevisionState::Expired => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("installed revision {:?} is not live", installed.id),
                    ));
                }
            }
            if segment_keys.insert((chunk.id, segment.id)) {
                guarded_segments.push((segment, lease));
            }
        }

        let require_wal = self.durable_storage_configured();
        for (segment, _lease) in guarded_segments {
            if require_wal {
                segment.force_wal_sync_required()?;
            } else {
                segment.force_wal_sync()?;
            }
        }
        Ok(())
    }

    fn install_compensation(
        &self,
        installed: &InstalledRevision,
        prior: Option<OwnedCell>,
    ) -> Result<InstalledRevision, WriteError> {
        let compensation_ts = self.next_revision_ts(installed.node.revision_ts)?;
        match prior {
            Some(mut cell) => {
                self.upsert_cell_at_revision(&mut cell, RevisionWrite::committed(compensation_ts))
            }
            None => self
                .remove_cell_at_revision(&installed.id, RevisionWrite::committed(compensation_ts)),
        }
    }

    pub fn compensate(
        &self,
        installed: &InstalledRevision,
        prior: Option<OwnedCell>,
    ) -> Result<InstalledRevision, WriteError> {
        let chunk = self.locate_chunk_by_partition(installed.id.higher);
        let current = chunk
            .history
            .current(&installed.id)
            .ok_or(WriteError::CellRevisionMismatch)?;
        if Arc::ptr_eq(&current, &installed.node) {
            match installed.node.load().0 {
                RevisionState::PendingPresent | RevisionState::PendingDeleted => {
                    chunk.abort_revision(installed)?;
                }
                RevisionState::Aborted => {}
                RevisionState::CommittedPresent
                | RevisionState::CommittedDeleted
                | RevisionState::Expired => return Err(WriteError::CellRevisionMismatch),
            }
            return self.install_compensation(installed, prior);
        }

        Err(WriteError::CellRevisionMismatch)
    }

    pub(crate) fn compensate_recovered(
        &self,
        id: &Id,
        installed_revision_ts: u64,
        prior: Option<OwnedCell>,
    ) -> Result<InstalledRevision, WriteError> {
        let installed = self
            .locate_chunk_by_partition(id.higher)
            .invalidate_recovered_revision(id, installed_revision_ts)?;
        self.install_compensation(&installed, prior)
    }

    pub fn count(&self) -> usize {
        self.list.iter().map(|c| c.count()).sum()
    }

    pub fn clear_cell_index(&self) -> usize {
        let mut removed = 0usize;
        for chunk in &self.list {
            removed += chunk.cell_index.len();
            chunk.cell_index.clear();
            chunk.statistics.ensured_refresh_chunk(chunk);
        }
        removed
    }

    pub fn all_chunk_statistics(&self, schema_id: u32) -> Vec<Option<Arc<SchemaStatistics>>> {
        self.list
            .iter()
            .map(|c| c.statistics.schemas.get(&schema_id))
            .collect()
    }
    pub fn ensure_statistics(&self) {
        self.list
            .iter()
            .for_each(|c| c.statistics.ensured_refresh_chunk(c));
    }
    pub fn overall_statistics(&self, schema: u32) -> Arc<SchemaStatistics> {
        self.statistics
            .get(schema as usize, 5 * 60, |schema| {
                let schema = schema as u32;
                let all_stats = self
                    .all_chunk_statistics(schema)
                    .into_iter()
                    .filter_map(|s| s)
                    .collect::<Vec<_>>();
                merge_statistics(all_stats).map(|s| Arc::new(s))
            })
            .unwrap()
    }

    pub fn lock_cell_for_read(&self, key: &Id) -> Result<CellGuard<'_>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.lock_cell_for_read(hash);
    }
    pub fn lock_cell_for_write(
        &self,
        key: &Id,
        has_read: bool,
    ) -> Result<CellGuard<'_>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.lock_cell_for_write(hash, has_read);
    }

    pub fn compare_revision_and_update_cell(
        &self,
        key: &Id,
        revision_ts: u64,
        cell: &mut OwnedCell,
    ) -> Result<CellHeader, WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.compare_revision_and_update_cell(hash, revision_ts, cell);
    }

    pub fn compare_revision_and_set_field(
        &self,
        key: &Id,
        revision_ts: u64,
        field: u64,
        value: OwnedValue,
    ) -> Result<CellHeader, WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.compare_revision_and_set_field(hash, revision_ts, field, value);
    }
}

pub struct CellGuard<'a> {
    segment: Option<AArc<Segment>>,
    guard: Option<WordMutexGuard<'a>>,
    chunk: &'a Chunk,
    hash: u64,
    revision_ts: u64,
}

impl<'a> CellGuard<'a> {
    pub fn from_guard(hash: u64, guard: WordMutexGuard<'a>, chunk: &'a Chunk) -> Option<Self> {
        let mut segment = None;
        let mut revision_ts = 0;
        if *guard != 0 {
            #[cfg(feature = "tiered_memory")]
            {
                segment = chunk.locate_segment(*guard);
                if let Some(seg) = &segment {
                    if seg.is_cold() {
                        // CRITICAL: Release the cell lock BEFORE promotion to avoid deadlock.
                        // The deadlock scenario:
                        // - Thread A holds cell lock, calls promote_segment, waits for segment exclusive access
                        // - Thread B holds segment reference (via another cell), waits for the same cell lock
                        // By releasing the guard first, we break this circular wait.
                        // The caller's retry loop will re-acquire the lock after promotion completes.
                        drop(guard);

                        if let Some(ref tiered_manager) = chunk.tiered_manager {
                            if let Err(e) = tiered_manager.promote(chunk, seg) {
                                warn!(
                                    "Failed to promote segment {} in chunk {}: {}",
                                    seg.id, chunk.id, e
                                );
                            }
                        } else {
                            crate::ram::tiered::promotion::promote_segment(&seg);
                        }
                        // Return None to signal caller to retry (now segment should be hot)
                        return None;
                    }
                    if !seg.incr_references() {
                        return None;
                    }
                    seg.mark_referenced();
                } else {
                    trace!(
                        "Segment not found for cell at {:?} for chunk {}. Should retry.",
                        *guard,
                        chunk.id
                    );
                    return None;
                }
            }
            revision_ts = cell_revision_ts_from_chunk_raw(*guard).unwrap();
        }

        Some(Self {
            guard: Some(guard),
            chunk,
            hash,
            segment,
            revision_ts,
        })
    }

    pub fn for_read(hash: u64, chunk: &'a Chunk) -> Result<Self, ReadError> {
        let backoff = Backoff::new();
        loop {
            let guard = chunk.location_for_read(hash)?;
            if let Some(guard) = CellGuard::from_guard(hash, guard, chunk) {
                return Ok(guard);
            }
            backoff.spin();
        }
    }

    pub fn for_write(hash: u64, has_read: bool, chunk: &'a Chunk) -> Option<Self> {
        let backoff = Backoff::new();
        loop {
            let guard = chunk.location_for_write(hash, has_read)?;
            if let Some(guard) = CellGuard::from_guard(hash, guard, chunk) {
                return Some(guard);
            }
            #[cfg(test)]
            if let Some(hook) = chunk.cell_guard_retry_hook.lock().clone() {
                hook(hash);
            }
            backoff.spin();
        }
    }

    fn update_revision_ts(&mut self, revision_ts: u64) {
        if self.revision_ts < revision_ts {
            self.revision_ts = revision_ts;
        }
    }

    pub fn head_cell(&mut self) -> Result<CellHeader, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let (header, _) = header_from_chunk_raw(self.get_ptr())?;
        self.update_revision_ts(header.revision_ts);
        Ok(header)
    }

    pub fn cell_revision_ts(&mut self) -> Result<u64, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let revision_ts = cell_revision_ts_from_chunk_raw(self.get_ptr())?;
        self.update_revision_ts(revision_ts);
        Ok(revision_ts)
    }

    pub fn read_cell_owned(&mut self) -> Result<OwnedCell, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let (data, _) = SharedCellData::from_chunk_raw(self.hash, self.get_ptr(), self.chunk)?;
        self.update_revision_ts(data.header.revision_ts);
        Ok(data.to_owned())
    }

    pub fn read_cell_shared(&mut self) -> Result<SharedCellData<'_>, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let (data, _) = SharedCellData::from_chunk_raw(self.hash, self.get_ptr(), self.chunk)?;
        self.update_revision_ts(data.header.revision_ts);
        Ok(data)
    }

    pub fn is_unassigned(&self) -> bool {
        self.get_ptr() == 0
    }

    pub fn update_cell(&mut self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        if self.is_unassigned() {
            return Err(WriteError::CellDoesNotExisted);
        }
        self.chunk.update_cell_with_guard(self, cell)?;
        Ok(cell.header)
    }

    /// Upsert a cell - updates if the guard points to an existing cell, inserts if empty.
    /// This is useful when you have a guard from `try_insert_locked` which may point to
    /// an empty slot (insert case) or an existing cell (update case).
    pub fn upsert_cell(&mut self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        if self.is_unassigned() {
            let current_revision_ts = self
                .chunk
                .history
                .current(&cell.id())
                .map(|node| node.revision_ts)
                .unwrap_or(0);
            let revision_ts = self.chunk.next_revision_ts(current_revision_ts)?;
            self.chunk.write_cell_with_guard_at_revision(
                self,
                cell,
                RevisionWrite::committed(revision_ts),
            )?;
        } else {
            self.chunk.update_cell_with_guard(self, cell)?;
        }
        Ok(cell.header)
    }

    pub fn word_mutex_guard(&mut self) -> &mut WordMutexGuard<'a> {
        self.guard.as_mut().unwrap()
    }

    pub fn get_ptr(&self) -> usize {
        **self.guard.as_ref().unwrap() as usize
    }

    fn remove_index_entry(mut self) {
        self.decrement_segment_references();
        self.segment = None;
        self.guard.take().unwrap().remove();
    }

    #[inline(always)]
    fn decrement_segment_references(&self) {
        if let Some(segment) = &self.segment {
            segment.decr_references();
        }
    }

    fn old_index_res(&self, schema: &Schema) -> Result<Option<Vec<IndexRes>>, WriteError> {
        if self.chunk.index_builder.is_some() {
            SharedCellData::from_chunk_raw(self.hash, self.get_ptr(), self.chunk)
                .map(|(c, _)| Some(probe_cell_indices(&c, schema)))
                .map_err(|e| WriteError::ReadError(e))
        } else {
            Ok(None)
        }
    }

    fn set_ptr(&mut self, ptr: usize) {
        let guard = self.guard.as_mut().unwrap();
        **guard = ptr;
    }
}

impl<'a> Drop for CellGuard<'a> {
    fn drop(&mut self) {
        #[cfg(feature = "tiered_memory")]
        self.decrement_segment_references();
    }
}

impl<'a> Deref for CellGuard<'a> {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &**self.guard.as_ref().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::schema::{Field, LocalSchemasCache};
    use crate::ram::types::Map;
    use bifrost_hasher::hash_str;
    use dovahkiin::types::Type;
    use env_logger;
    use tempfile::TempDir;

    const TEST_CHUNK_SIZE: usize = 8 * 1024 * 1024;

    fn setup_test_chunks() -> (Arc<Chunks>, Schema) {
        let fields = Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed_array("data", Type::U8),
        ]);
        let schema = Schema::new("cell_stored_len_test", None, fields, false, false);
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            1,
            TEST_CHUNK_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        (chunks, schema)
    }

    fn payload_cell(schema_id: u32, id: &Id, payload_len: usize) -> OwnedCell {
        let data: Vec<u8> = std::iter::repeat(id.lower as u8)
            .take(payload_len)
            .collect();
        OwnedCell {
            header: CellHeader::new(schema_id, id),
            data: data_map_value!(id: id.lower as i32, data: data),
        }
    }

    #[test]
    fn backup_only_chunks_reject_exact_transaction_output_sync_without_wal() {
        let fields = Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed_array("data", Type::U8),
        ]);
        let schema = Schema::new("backup_only_exact_sync", None, fields, false, false);
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let backup = TempDir::new().unwrap();
        let chunks = Chunks::new(
            1,
            TEST_CHUNK_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            Some(backup.path().to_string_lossy().into_owned()),
            None,
            None,
        );
        let id = Id::new(1, 39);
        let mut cell = payload_cell(schema.id, &id, 16);

        set_transaction_context(true);
        let installed = chunks
            .write_cell_at_revision(&mut cell, RevisionWrite::pending(7))
            .unwrap();
        set_transaction_context(false);

        let error = chunks
            .force_sync_installed_revisions([&installed])
            .expect_err("backup storage without WAL cannot prove exact output durability");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    fn replace_stored_schema_id(chunks: &Chunks, id: &Id, schema_id: u32) {
        let entry_addr = chunks.address_of(id);
        let schema_addr = Entry::content_pos(entry_addr) + size_of::<u64>() + size_of::<u32>();
        unsafe {
            (schema_addr as *mut u32).write_unaligned(schema_id);
        }
    }

    #[test]
    fn schema_less_remove_without_indexer_publishes_tombstone() {
        let (chunks, schema) = setup_test_chunks();
        let id = Id::new(1, 41);
        let mut cell = payload_cell(schema.id, &id, 16);
        chunks.write_cell(&mut cell).unwrap();
        replace_stored_schema_id(&chunks, &id, schema.id + 10_000);

        chunks.remove_cell(&id).unwrap();

        assert!(matches!(
            chunks.read_cell(&id),
            Err(ReadError::CellDoesNotExisted)
        ));
        let current = chunks.list[0].history.current(&id).unwrap();
        assert_eq!(current.load().0, RevisionState::CommittedDeleted);
        assert!(current.revision_ts > cell.header.revision_ts);
    }

    #[test]
    fn indexed_remove_with_missing_schema_is_side_effect_free() {
        let (chunks, schema) = setup_test_chunks();
        let id = Id::new(1, 43);
        let mut cell = payload_cell(schema.id, &id, 16);
        chunks.write_cell(&mut cell).unwrap();
        let missing_schema_id = schema.id + 10_000;
        replace_stored_schema_id(&chunks, &id, missing_schema_id);

        let chunk = &chunks.list[0];
        let original_addr = chunks.address_of(&id);
        let original_current = chunk.history.current(&id).unwrap();
        let original_segment = chunk.locate_segment(original_addr).unwrap();
        let original_append = original_segment.append_header.load(Ordering::Acquire);
        let original_tombstones = original_segment.tombstones.load(Ordering::Acquire);
        let guard = CellGuard::for_write(id.lower, true, chunk).unwrap();

        let result = chunk.remove_cell_with_guard_at_revision_for_indexing(
            guard,
            &id,
            RevisionWrite::committed(cell.header.revision_ts + 1),
            true,
        );

        assert!(matches!(
            result,
            Err(WriteError::SchemaDoesNotExisted(id)) if id == missing_schema_id
        ));
        assert_eq!(chunks.address_of(&id), original_addr);
        assert!(Arc::ptr_eq(
            &chunk.history.current(&id).unwrap(),
            &original_current
        ));
        assert_eq!(
            original_segment.append_header.load(Ordering::Acquire),
            original_append
        );
        assert_eq!(
            original_segment.tombstones.load(Ordering::Acquire),
            original_tombstones
        );
        assert_eq!(chunks.secondary_index_removal_attempts_for_test(&id), 0);
    }

    #[test]
    fn read_at_returns_pinned_version_after_update() {
        let _ = env_logger::try_init();
        let (chunks, schema) = setup_test_chunks();

        let id = Id::new(1, 42);
        let mut cell = payload_cell(schema.id, &id, 16);
        chunks.write_cell(&mut cell).unwrap();

        // Capture version A's raw address and its full/selected contents.
        let addr = {
            let sc = chunks.read_cell(&id).unwrap();
            sc.cell_guard().get_ptr()
        };
        let full_before = chunks.read_cell(&id).unwrap().to_owned();
        let selected_before = chunks
            .read_selected(&id, &[hash_str("data")], true)
            .unwrap()
            .to_owned();

        // Update the cell in place: the cell index now serves version B at a
        // different address.
        let mut updated = payload_cell(schema.id, &id, 32);
        chunks.update_cell(&mut updated).unwrap();

        // Sanity check: by-id reads now observe the new version.
        let full_after = chunks.read_cell(&id).unwrap().to_owned();
        assert_ne!(full_after.data, full_before.data);
        assert!(full_after.header.revision_ts > full_before.header.revision_ts);

        // Reading BY ADDRESS still returns the OLD version (copy-on-write).
        let pinned = chunks.read_cell_at(&id, addr).unwrap();
        assert_eq!(pinned.data, full_before.data);
        assert_eq!(pinned.header.revision_ts, full_before.header.revision_ts);

        // head_at agrees with the pinned header.
        let h = chunks.head_at(&id, addr).unwrap();
        assert_eq!(h.revision_ts, full_before.header.revision_ts);

        // read_selected_at returns the pinned projection, not the new one.
        let selected_pinned = chunks
            .read_selected_at(&id, addr, &[hash_str("data")])
            .unwrap();
        assert_eq!(selected_pinned.data, selected_before.data);

        // An invalid (unit) address still errors like the by-id path.
        assert!(chunks.head_at(&id, 0).is_err());
        assert!(chunks.read_cell_at(&id, 0).is_err());
    }
}
