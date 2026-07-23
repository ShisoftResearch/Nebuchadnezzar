use crate::query::statistics::{
    merge_statistics, schema_tracks_statistics, ChunkStatistics, SchemaStatistics,
};
use crate::ram::entry::{Entry, EntryContent, EntryType, ENTRY_HEAD_SIZE};
use crate::ram::file_manager::SegmentFileManager;
use crate::ram::schema::LocalSchemasCache;
use crate::ram::segment_list::SegmentList;
use crate::ram::segs::{Segment, SegmentAllocator, SegmentClass, SEGMENT_SIZE, SEGMENT_SIZE_U32};
use crate::ram::tombstone::{Tombstone, TOMBSTONE_ENTRY_SIZE};
use crate::ram::types::Id;
use crate::server::ServerMeta;
use crate::{index::builder::IndexBuilder, ram::cell::*};
use crate::{
    index::builder::{probe_cell_indices, IndexRes},
    ram::cleaner::Cleaner,
};

use super::schema::Schema;
use dovahkiin::types::OwnedValue;
use lightning::aarc::Arc as AArc;
use lightning::map::{Map, WordMap, WordMutexGuard};
use lightning::spin_hint::Backoff;
use lightning::ttl_cache::TTLCache;
use parking_lot::Mutex;
use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub type CellReadGuard<'a> = WordMutexGuard<'a>;
pub type CellWriteGuard<'a> = WordMutexGuard<'a>;

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

/// Set the global Chunks pointer (called by Chunks::new_with_recovery)
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
    /// Shared tiered memory manager for eviction/promotion
    pub tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
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

    fn new(
        id: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
    ) -> Chunk {
        // Call new_with_base with base_addr=0 to use old allocation behavior
        Self::new_with_base(
            id,
            0,
            size,
            meta,
            index_builder,
            backup_storage,
            wal_storage,
            tiered_manager,
        )
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
            tiered_manager,
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

    // By-address header read: decodes the header stored at a caller-pinned raw
    // `location` instead of resolving through the cell index. Used by
    // repeatable-read pinning, where the caller already holds a segment guard
    // that keeps the bytes at `location` alive even after the cell index has
    // moved on to a newer version.
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
        let head_ptr = *loc + offset;
        let mut data = Vec::with_capacity(len);
        for ptr in head_ptr..(head_ptr + len) {
            data.push(unsafe { *(ptr as *const u8) });
        }
        Ok(data.to_vec())
    }

    pub fn write_cell_to_chunk<'a>(
        &self,
        cell: &OwnedCell,
        write_plan: &WritePlan,
        pending_entry: &PendingEntry,
        old_version: u64,
    ) -> Result<WriteToChunkResult, WriteError> {
        cell.write_to_chunk_with(write_plan, pending_entry, old_version)
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

    fn write_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        debug!("Writing cell {:?} to chunk {}", cell.id(), self.id);
        let write_plan = cell.plan_write(self)?;
        let pending_entry = write_plan.allocate(self, true)?;
        let write_result =
            self.write_cell_to_chunk(cell, &write_plan, &pending_entry, cell.header.version)?;
        let cell_loc = write_result.addr;
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                self.validate_cell_location(
                    cell_loc,
                    &format!("write_cell(hash={})", cell.header.hash)
                ),
                "Attempting to store invalid cell location 0x{:x} in cell index for hash {}",
                cell_loc,
                cell.header.hash
            );
        }

        match self.cell_index.try_insert_locked(cell.header.hash as usize) {
            Some(mut guard) => {
                #[cfg(debug_assertions)]
                self.assert_address_aligned_for_write(cell_loc, "write_cell", cell.header.hash);

                *guard = cell_loc;
                drop(guard);
                self.ensure_indices(cell, None, &*write_plan.schema);
                self.refresh_statistics_for_schema(write_plan.schema.id);
            }
            None => return Err(WriteError::CellAlreadyExisted),
        }
        cell.header.version = write_result.new_version;
        cell.header.timestamp = write_result.new_timestamp;
        Ok(cell.header)
    }

    fn update_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.hash;
        let write_plan = cell.plan_write(self)?;
        let pending_entry = write_plan.allocate(self, true)?;
        if let Some(mut cell_guard) = CellGuard::for_write(hash, true, self) {
            let cell_location = cell_guard.get_ptr();
            let cell_version =
                cell_version_from_chunk_raw(cell_location).map_err(|e| WriteError::ReadError(e))?;
            let write_result =
                self.write_cell_to_chunk(cell, &write_plan, &pending_entry, cell_version)?;
            let new_cell_loc = write_result.addr;
            #[cfg(debug_assertions)]
            {
                if cell_location != 0 {
                    self.assert_address_aligned_for_read(cell_location, "update_cell(old)", hash);
                }
                self.assert_address_aligned_for_write(new_cell_loc, "update_cell", hash);
            }

            let schema = &*write_plan.schema;
            let old_indices = cell_guard.old_index_res(schema)?;
            cell_guard.set_ptr(new_cell_loc);
            drop(cell_guard);
            self.ensure_indices_with_res(cell, old_indices, schema);
            self.mark_dead_entry_with_cell(cell_location, cell);
            self.refresh_statistics_for_schema(schema.id);
            drop(write_plan);
            cell.header.version = write_result.new_version;
            cell.header.timestamp = write_result.new_timestamp;
        } else {
            // Optimistic update will remove the new inserted one
            let write_result =
                self.write_cell_to_chunk(cell, &write_plan, &pending_entry, cell.header.version)?;
            let new_cell_loc = write_result.addr;
            self.mark_dead_entry_with_cell(new_cell_loc, cell);
            return Err(WriteError::CellDoesNotExisted);
        }
        Ok(cell.header)
    }

    pub fn upsert_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.hash;
        let write_plan = cell.plan_write(self)?;
        let pending_entry = write_plan.allocate(self, true)?;
        loop {
            if let Some(mut cell_guard) = CellGuard::for_write(hash, true, self) {
                trace!("Cell {} exists, will update for upsert", hash);
                let cell_location = cell_guard.get_ptr();
                let cell_version = cell_version_from_chunk_raw(cell_location)
                    .map_err(|e| WriteError::ReadError(e))?;
                let write_result =
                    self.write_cell_to_chunk(cell, &write_plan, &pending_entry, cell_version)?;
                let new_cell_loc = write_result.addr;
                #[cfg(debug_assertions)]
                {
                    if cell_location != 0 {
                        self.assert_address_aligned_for_read(
                            cell_location,
                            "upsert_cell(update/old)",
                            hash,
                        );
                    }
                    self.assert_address_aligned_for_write(
                        new_cell_loc,
                        "upsert_cell(update)",
                        hash,
                    );
                }

                let old_indices = cell_guard.old_index_res(&*write_plan.schema)?;
                cell_guard.set_ptr(new_cell_loc);
                drop(cell_guard);
                self.ensure_indices_with_res(cell, old_indices, &*write_plan.schema);
                self.mark_dead_entry_with_cell(cell_location, cell);
                self.refresh_statistics_for_schema(write_plan.schema.id);
                drop(write_plan);
                cell.header.version = write_result.new_version;
                cell.header.timestamp = write_result.new_timestamp;
            } else {
                let reservation = self.cell_index.try_insert_locked(hash as usize);
                if let Some(mut guard) = reservation {
                    // New cell
                    trace!("Cell {} does not exists, will insert for upsert", hash);
                    let write_result = self.write_cell_to_chunk(
                        cell,
                        &write_plan,
                        &pending_entry,
                        cell.header.version,
                    )?;
                    let new_cell_loc = write_result.addr;
                    #[cfg(debug_assertions)]
                    self.assert_address_aligned_for_write(
                        new_cell_loc,
                        "upsert_cell(insert)",
                        hash,
                    );

                    *guard = new_cell_loc;
                    drop(guard);
                    self.ensure_indices(cell, None, &*write_plan.schema);
                    self.refresh_statistics_for_schema(write_plan.schema.id);
                    drop(write_plan);
                    cell.header.version = write_result.new_version;
                    cell.header.timestamp = write_result.new_timestamp;
                } else {
                    trace!("Cell {} was not exists, but found exists, will try", hash);
                    continue;
                }
            }
            return Ok(cell.header);
        }
    }

    fn update_cell_by<U>(&self, hash: u64, update: U) -> Result<OwnedCell, WriteError>
    where
        U: FnOnce(&SharedCellData) -> Option<OwnedCell>,
    {
        if let Some(mut cell_guard) = CellGuard::for_write(hash, true, self) {
            let old_loc = cell_guard.get_ptr();
            match SharedCellData::from_chunk_raw(hash, *cell_guard, self) {
                Ok((cell, schema)) => {
                    let old_indices = self
                        .index_builder
                        .as_ref()
                        .map(|_| probe_cell_indices(&cell, &*schema));

                    // Get old entry size BEFORE releasing lock to avoid race condition
                    // where old_loc could be corrupted after we update cell_index
                    let old_entry_size = if old_loc != 0 {
                        match Entry::decode_from(old_loc, |_, _| {}) {
                            (entry, _) => Some(entry.content_length),
                        }
                    } else {
                        None
                    };

                    let new_cell = update(&cell);
                    if let Some(mut new_cell) = new_cell {
                        let write_plan = new_cell.plan_write(self)?;
                        let pending_entry = write_plan.allocate(self, false)?;
                        let write_result = self.write_cell_to_chunk(
                            &new_cell,
                            &write_plan,
                            &pending_entry,
                            cell.header.version,
                        )?;
                        let new_cell_loc = write_result.addr;

                        #[cfg(debug_assertions)]
                        self.assert_address_aligned_for_write(new_cell_loc, "update_cell_by", hash);

                        **cell_guard.word_mutex_guard() = new_cell_loc;
                        if let Some(indexer) = &self.index_builder {
                            indexer.ensure_indices(&new_cell, &*schema, old_indices);
                        }

                        // Mark old entry as dead using size we captured earlier
                        // This avoids decoding old_loc after lock is released (race condition)
                        if let Some(size) = old_entry_size {
                            let seg = self.locate_segment_ensured(old_loc, &new_cell.id());
                            self.mark_dead_entry_with_size(old_loc, size, &seg);
                        }

                        self.refresh_statistics_for_schema(write_plan.schema.id);
                        drop(write_plan);
                        new_cell.header.version = write_result.new_version;
                        new_cell.header.timestamp = write_result.new_timestamp;
                        return Ok(new_cell);
                    } else {
                        return Err(WriteError::UserCanceledUpdate);
                    }
                }
                Err(e) => return Err(WriteError::ReadError(e)),
            }
        } else {
            return Err(WriteError::CellDoesNotExisted);
        }
    }

    fn remove_cell(&self, hash: u64) -> Result<(), WriteError> {
        // Use location_for_read to ensure promotion happens if segment is cold
        let guard = match CellGuard::for_read(hash, self) {
            Ok(guard) => guard,
            Err(ReadError::CellDoesNotExisted) => return Err(WriteError::CellDoesNotExisted),
            Err(ReadError::CellIdIsUnitId) => return Err(WriteError::CellDoesNotExisted),
            Err(e) => return Err(WriteError::ReadError(e)),
        };
        let cell_location = guard.get_ptr();

        if let Some(indexer) = &self.index_builder {
            match SharedCell::from_chunk_raw(hash, guard, self) {
                Ok((cell, schema)) => {
                    indexer.remove_indices(&cell, &*schema);
                    cell.into_cell_guard().remove_cell();
                }
                Err(e) => return Err(WriteError::ReadError(e)),
            }
        } else {
            guard.remove_cell();
        }
        self.put_tombstone_by_cell_loc(cell_location)?;
        Ok(())
    }

    fn remove_cell_by<P>(&self, hash: u64, predict: P) -> Result<(), WriteError>
    where
        P: Fn(&SharedCell) -> bool,
    {
        // Use location_for_read to ensure promotion happens if segment is cold
        let guard = match CellGuard::for_read(hash, self) {
            Ok(guard) => guard,
            Err(ReadError::CellDoesNotExisted) => return Err(WriteError::CellDoesNotExisted),
            Err(ReadError::CellIdIsUnitId) => return Err(WriteError::CellDoesNotExisted),
            Err(e) => return Err(WriteError::ReadError(e)),
        };
        let cell_location = *guard;

        match SharedCell::from_chunk_raw(hash, guard, self) {
            Ok((cell, schema)) => {
                if predict(&cell) {
                    self.remove_indices(&cell, &schema);
                    cell.into_cell_guard().remove_cell();
                    self.put_tombstone_by_cell_loc(cell_location)?;
                    return Ok(());
                }
                Err(WriteError::CellDoesNotExisted)
            }
            Err(e) => Err(WriteError::ReadError(e)),
        }
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

    pub fn remove_segment(&self, segment_id: u64) {
        debug!(
            "Removing segment for chunk {} with id {}",
            self.id, segment_id
        );

        // Check if segment is hot BEFORE removing to avoid race with full scan
        // If we decrement after removing, a full scan could miss the removed segment
        // and update the cache, then we'd decrement again, leading to under-counting
        let should_decrement = if let Some(seg) = self.segs.get(&(segment_id as usize)) {
            let is_hot = seg.is_hot();
            if !is_hot {
                error!(
                    "Segment {} is not hot in chunk {} to remove",
                    segment_id, self.id
                );
            }
            is_hot
        } else {
            error!(
                "Segment {} not found in chunk {} to remove",
                segment_id, self.id
            );
            false
        };

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
            // Free the segment files
            seg.dispense();
        } else {
            error!(
                "Segment {} not found in chunk {} to remove",
                segment_id, self.id
            );
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

    #[inline]
    fn put_tombstone(
        &self,
        cell_header: &CellHeader,
        cell_seg: &AArc<Segment>,
    ) -> Result<(), WriteError> {
        let pending_entry = (|| loop {
            if let Ok(pending_entry) = self.try_acquire(TOMBSTONE_ENTRY_SIZE as u32, true) {
                return pending_entry;
            }
            warn!(
                "Chunk {} is too full to put a tombstone. Will retry.",
                self.id
            )
        })();
        Tombstone::put(
            pending_entry.addr,
            cell_seg.seq_id,
            cell_header.version,
            cell_header.partition,
            cell_header.hash,
        );
        pending_entry.seg.tombstones.fetch_add(1, Ordering::Relaxed);
        pending_entry.seg.note_dead_bytes_change();
        Ok(())
    }

    pub fn put_tombstone_by_cell_loc(&self, cell_location: usize) -> Result<(), WriteError> {
        debug!(
            "Put tombstone for chunk {} for cell {}",
            self.id, cell_location
        );
        let header = header_from_chunk_raw(cell_location)
            .map_err(|e| WriteError::ReadError(e))?
            .0;

        // Get entry size while we know the memory is still valid
        let entry_size = {
            let (entry, _) = Entry::decode_from(cell_location, |_, _| {});
            entry.content_length
        };

        let cell_seg = self.locate_segment_ensured(cell_location, &header.id());
        self.put_tombstone(&header, &cell_seg)?;
        self.mark_dead_entry_with_size(cell_location, entry_size, &cell_seg);
        Ok(())
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
                    && seg.no_references() // Includes transaction protection via SegmentReferenceGuards
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

    pub fn compare_version_and_update_cell(
        &self,
        hash: u64,
        version: u64,
        cell: &mut OwnedCell,
    ) -> Result<CellHeader, WriteError> {
        let mut guard = self
            .lock_cell_for_write(hash, true)
            .map_err(WriteError::ReadError)?;
        let cell_version = guard.cell_version().map_err(WriteError::ReadError)?;
        if cell_version == version {
            cell.header.version = version; // update version to the latest version
            guard.update_cell(cell)?;
            return Ok(cell.header);
        }
        return Err(WriteError::CellVersionMismatch);
    }

    pub fn compare_version_and_set_field(
        &self,
        hash: u64,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> Result<CellHeader, WriteError> {
        let mut guard = self
            .lock_cell_for_write(hash, true)
            .map_err(WriteError::ReadError)?;
        let mut cell = guard.read_cell_owned().map_err(WriteError::ReadError)?;
        if cell.header.version == version {
            cell.data[field] = value;
            guard.update_cell(&mut cell)?;
            return Ok(cell.header);
        }
        return Err(WriteError::CellVersionMismatch);
    }
}

pub struct PendingEntry {
    pub seg: AArc<Segment>,
    pub addr: usize,
    pub size: u32,
    pub skip_sync: bool, // Skip fsync if part of a transaction (will be synced at commit)
}

impl Drop for PendingEntry {
    // dealing with entry write ahead log
    fn drop(&mut self) {
        self.seg
            .write_wal(self.addr, self.size, self.skip_sync)
            .unwrap();
        self.seg.set_dirty();
        self.seg.decr_references();
    }
}

pub struct Chunks {
    pub list: Vec<Chunk>,
    pub statistics: TTLCache<Arc<SchemaStatistics>>,
    pub tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
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
        Self::new_with_recovery(
            count,
            size,
            meta,
            index_builder,
            backup_storage,
            wal_storage,
            tiered_manager,
            false,
            None,
        )
    }

    pub fn new_with_recovery(
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
            ));
        }
        let num_schemas = meta.schemas.count() + 1;
        let chunks_arc = Arc::new(Chunks {
            list: chunks,
            statistics: TTLCache::with_capacity(num_schemas.next_power_of_two()),
            tiered_manager,
        });

        if let Some(ref manager) = chunks_arc.tiered_manager {
            manager.register_chunks(&chunks_arc);
        }

        // Store global pointer for signal handler access
        set_global_chunks(&chunks_arc);

        // Attempt recovery if enabled
        if enable_recovery {
            info!("Recovery enabled, attempting to recover from storage");

            let config = crate::ram::recovery::RecoveryConfig {
                num_chunks: count,
                chunk_size,
                max_threads: Some(64), // Cap recovery parallelism to reduce contention storms
            };

            match crate::ram::recovery::recover_chunks(
                &config,
                &backup_storage,
                &wal_storage,
                &raft_storage,
                &chunks_arc.list,
            ) {
                Ok(()) => {
                    info!("Recovery completed successfully");
                }
                Err(e) => {
                    error!("Recovery failed: {:?}", e);
                    error!("Starting with fresh storage");
                }
            }
        }

        chunks_arc
    }

    /// Sync all buffered WAL data to disk across all chunks
    pub fn sync_all(&self) {
        info!("Syncing WAL for all chunks...");
        for chunk in &self.list {
            for segment in chunk.segs.iter_values() {
                segment.force_wal_sync();
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
    pub fn head_cell(&self, key: &Id) -> Result<CellHeader, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.head_cell(hash);
    }
    // By-address header read: same as `head_cell` but pinned to `location`
    // instead of resolving through the cell index.
    pub fn head_at(&self, key: &Id, location: usize) -> Result<CellHeader, ReadError> {
        let chunk = self.locate_chunk_by_partition(key.higher);
        return chunk.head_at(location);
    }
    pub fn location_for_read(&self, key: &Id) -> Result<CellReadGuard<'_>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        chunk.location_for_read(hash)
    }
    pub fn write_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.write_cell(cell);
    }
    pub fn update_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.update_cell(cell);
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
    pub fn remove_cell(&self, key: &Id) -> Result<(), WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell(hash);
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

    pub fn compare_version_and_update_cell(
        &self,
        key: &Id,
        version: u64,
        cell: &mut OwnedCell,
    ) -> Result<CellHeader, WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.compare_version_and_update_cell(hash, version, cell);
    }

    pub fn compare_version_and_set_field(
        &self,
        key: &Id,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> Result<CellHeader, WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.compare_version_and_set_field(hash, version, field, value);
    }
}

pub struct CellGuard<'a> {
    segment: Option<AArc<Segment>>,
    guard: Option<WordMutexGuard<'a>>,
    chunk: &'a Chunk,
    hash: u64,
    version: u64,
}

impl<'a> CellGuard<'a> {
    pub fn from_guard(hash: u64, guard: WordMutexGuard<'a>, chunk: &'a Chunk) -> Option<Self> {
        let mut segment = None;
        let mut version = 0;
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
            version = cell_version_from_chunk_raw(*guard).unwrap();
        }

        Some(Self {
            guard: Some(guard),
            chunk,
            hash,
            segment,
            version,
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
            backoff.spin();
        }
    }

    fn update_version(&mut self, version: u64) {
        if self.version < version {
            self.version = version;
        }
    }

    pub fn head_cell(&mut self) -> Result<CellHeader, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let (header, _) = header_from_chunk_raw(self.get_ptr())?;
        self.update_version(header.version);
        Ok(header)
    }

    pub fn cell_version(&mut self) -> Result<u64, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let version = cell_version_from_chunk_raw(self.get_ptr())?;
        self.update_version(version);
        Ok(version)
    }

    pub fn read_cell_owned(&mut self) -> Result<OwnedCell, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let (data, _) = SharedCellData::from_chunk_raw(self.hash, self.get_ptr(), self.chunk)?;
        self.update_version(data.header.version);
        Ok(data.to_owned())
    }

    pub fn read_cell_shared(&mut self) -> Result<SharedCellData<'_>, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let (data, _) = SharedCellData::from_chunk_raw(self.hash, self.get_ptr(), self.chunk)?;
        self.update_version(data.header.version);
        Ok(data)
    }

    pub fn is_unassigned(&self) -> bool {
        self.get_ptr() == 0
    }

    pub fn update_cell(&mut self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let old_cell_loc = self.get_ptr();
        if cell.header.version < self.version {
            cell.header.version = self.version;
        }
        if self.is_unassigned() {
            return Err(WriteError::CellDoesNotExisted);
        }
        let write_plan = cell.plan_write(self.chunk)?;
        let pending_entry = write_plan.allocate(self.chunk, false)?;
        let write_result = self.chunk.write_cell_to_chunk(
            cell,
            &write_plan,
            &pending_entry,
            cell.header.version,
        )?;
        let new_cell_loc = write_result.addr;
        let schema = &*write_plan.schema;
        let old_indices = self.old_index_res(schema)?;
        let guard = self.guard.as_mut().unwrap();
        **guard = new_cell_loc;
        self.chunk
            .ensure_indices_with_res(cell, old_indices, schema);
        self.chunk.mark_dead_entry_with_cell(old_cell_loc, cell);
        self.chunk.refresh_statistics_for_schema(schema.id);
        drop(write_plan);
        cell.header.version = write_result.new_version;
        cell.header.timestamp = write_result.new_timestamp;
        Ok(cell.header)
    }

    /// Upsert a cell - updates if the guard points to an existing cell, inserts if empty.
    /// This is useful when you have a guard from `try_insert_locked` which may point to
    /// an empty slot (insert case) or an existing cell (update case).
    pub fn upsert_cell(&mut self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let old_cell_loc = self.get_ptr();
        if cell.header.version < self.version {
            cell.header.version = self.version;
        }
        let write_plan = cell.plan_write(self.chunk)?;
        let pending_entry = write_plan.allocate(self.chunk, false)?;
        let write_result = self.chunk.write_cell_to_chunk(
            cell,
            &write_plan,
            &pending_entry,
            cell.header.version,
        )?;
        let new_cell_loc = write_result.addr;
        let schema = &*write_plan.schema;
        let schema_id = schema.id;
        if old_cell_loc != 0 {
            // Update case - cell already exists
            let old_indices = self.old_index_res(&*schema)?;
            let guard = self.guard.as_mut().unwrap();
            **guard = new_cell_loc;
            self.chunk
                .ensure_indices_with_res(cell, old_indices, &*schema);
            self.chunk.mark_dead_entry_with_cell(old_cell_loc, cell);
        } else {
            // Insert case - new cell
            let guard = self.guard.as_mut().unwrap();
            **guard = new_cell_loc;
            self.chunk.ensure_indices(cell, None, &*schema);
        }

        drop(write_plan);
        cell.header.version = write_result.new_version;
        cell.header.timestamp = write_result.new_timestamp;

        self.chunk.refresh_statistics_for_schema(schema_id);
        Ok(cell.header)
    }

    pub fn word_mutex_guard(&mut self) -> &mut WordMutexGuard<'a> {
        self.guard.as_mut().unwrap()
    }

    pub fn get_ptr(&self) -> usize {
        **self.guard.as_ref().unwrap() as usize
    }

    pub fn remove_cell(mut self) {
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
        let data: Vec<u8> = std::iter::repeat(id.lower as u8).take(payload_len).collect();
        OwnedCell {
            header: CellHeader::new(schema_id, id),
            data: data_map_value!(id: id.lower as i32, data: data),
        }
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
        assert!(full_after.header.version > full_before.header.version);

        // Reading BY ADDRESS still returns the OLD version (copy-on-write).
        let pinned = chunks.read_cell_at(&id, addr).unwrap();
        assert_eq!(pinned.data, full_before.data);
        assert_eq!(pinned.header.version, full_before.header.version);

        // head_at agrees with the pinned header.
        let h = chunks.head_at(&id, addr).unwrap();
        assert_eq!(h.version, full_before.header.version);

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
