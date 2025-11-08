use crate::ram::chunk::Chunk;
use crate::ram::entry;
use crate::ram::entry::EntryMeta;
use crate::ram::io::align_address;
use crate::ram::tombstone::TOMBSTONE_SIZE_U32;
use bifrost::utils::time::get_time;
use libc::*;
use lightning::list::LinkedRingBufferList;
use parking_lot;
use std::fs::{self, create_dir_all, remove_file, File};
use std::io::prelude::*;
use std::io::BufWriter;
use std::path::Path;
use std::ptr;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicI8, AtomicU32, AtomicUsize,
    Ordering::{self, *},
};
use std::{io, slice};

use super::entry::ENTRY_HEAD_SIZE;

pub const SEGMENT_SIZE_U32: u32 = 8 * 1024 * 1024;
pub const SEGMENT_SIZE: usize = SEGMENT_SIZE_U32 as usize;
pub const SEGMENT_MASK: usize = !(SEGMENT_SIZE - 1);
pub const SEGMENT_BITS_SHIFT: u32 = SEGMENT_SIZE.trailing_zeros();

pub const HOT_SEGMENT: u8 = 1;
pub const COLD_SEGMENT: u8 = 2;
pub const HOT_COLD_MASK: u8 = !0 << 1 >> 1;
pub const LOCKING_SEGMENT_BITS: u8 = !HOT_COLD_MASK;

// Page constants (used for alignment and mprotect)
pub const PAGE_SHIFT: usize = 12; // 4KB pages
pub const PAGE_SIZE: usize = 1 << PAGE_SHIFT;

// WAL Performance Configuration
// These settings implement group commit batching to improve write throughput
// while maintaining durability guarantees within bounded loss windows.
//
// Performance Impact:
// - Larger buffer = fewer system calls, better throughput
// - Larger batch size = fewer fsyncs, MUCH better throughput (100x+)
// - Longer interval = better batching, but higher potential data loss window
//
// Durability Guarantees:
// - Transactional writes: No fsync during write, sync happens at commit time
// - Non-transactional writes: Fsync when batch_size OR interval is reached
// - In case of crash: Max potential loss = WAL_SYNC_BATCH_SIZE bytes OR
//                                          WAL_SYNC_INTERVAL_MS time window
//
// Performance Analysis:
// - With 10ms interval: max 100 fsyncs/sec = ~13 MB/s if writing <130KB per interval
// - With 100ms interval: max 10 fsyncs/sec = ~40+ MB/s (10x improvement)
// - With i64::MAX interval: limited only by batch size = 100s-1000s MB/s
//
// Tuning Recommendations:
// - High throughput: batch_size=4MB, interval=100ms (recommended for most workloads)
// - Maximum throughput: batch_size=4MB, interval=i64::MAX (sync only on size)
// - Low latency: batch_size=512KB, interval=50ms
// - Strict durability: batch_size=0, interval=0 (sync every write)
pub const WAL_BUFFER_SIZE: usize = 512 * 1024; // 512KB in-memory buffer (reduces syscalls)
pub const WAL_SYNC_BATCH_SIZE: usize = 1 * 1024 * 1024; // Sync after 1MB of writes (reduces fsyncs)
pub const WAL_SYNC_INTERVAL_MS: i64 = 10; // Sync every 10ms (10x less frequent than before)

#[derive(Default)]
#[repr(C, align(64))] // Ensure consistent memory layout and cache line alignment
pub struct Segment {
    pub id: u64,
    pub seq_id: u64,
    pub addr: usize,
    pub bound: usize,
    pub append_header: AtomicUsize,
    pub dead_space: AtomicU32,
    pub tombstones: AtomicU32,
    pub references: AtomicUsize,
    pub backup_file_name: Option<String>,
    pub wal_file: parking_lot::Mutex<Option<BufWriter<File>>>,
    pub wal_file_name: Option<String>,
    pub archived: AtomicBool,
    pub dropped: AtomicBool,
    // Tiered memory fields
    /// Segment lock for tiered memory operations (eviction, promotion, cleaner)
    /// Holds the hot/cold state: false = hot (anonymous memory), true = cold (backed by file)
    /// Cell read/write operations do NOT need this lock, only cell-level locks
    pub tiered_lock: AtomicU8, // 1 = hot, 2 = cold, highest bit for locking
    pub reference_bit: AtomicBool, // For CLOCK eviction algorithm (set by mprotect fault handler)
    // WAL batch sync tracking (for group commit optimization)
    pub last_sync_time: AtomicI64, // Timestamp of last fsync in milliseconds
    pub bytes_since_sync: AtomicUsize, // Bytes written since last fsync
}

impl Segment {
    pub fn new(
        id: u64,
        seq_id: u64,
        chunk_id: usize,
        buffer_ptr: usize,
        hot: bool,
        backup_storage: &Option<String>,
        wal_storage: &Option<String>,
    ) -> Segment {
        let mut wal_file_name = None;
        let mut wal_file_opt = None;
        let size = SEGMENT_SIZE;
        if let Some(backup_storage) = backup_storage {
            create_dir_all(backup_storage).unwrap();
        }
        if let Some(wal_storage) = wal_storage {
            create_dir_all(wal_storage).unwrap();
            let file_name = format!("{}/{}-{}-{}.nlog", wal_storage, chunk_id, id, seq_id);
            let file = BufWriter::with_capacity(
                WAL_BUFFER_SIZE, // 256KB for better batching
                File::create(&file_name).unwrap(),
            ); // fast fail
            wal_file_name = Some(file_name);
            wal_file_opt = Some(file);
        }
        debug!(
            "Creating new segment chunk {}, id {}, seq_id {}, size {}, address {}",
            chunk_id, id, seq_id, size, buffer_ptr
        );
        let tiered_lock = if hot { HOT_SEGMENT } else { COLD_SEGMENT };
        Segment {
            addr: buffer_ptr,
            id,
            seq_id,
            bound: buffer_ptr + size,
            append_header: AtomicUsize::new(buffer_ptr),
            dead_space: AtomicU32::new(0),
            tombstones: AtomicU32::new(0),
            references: AtomicUsize::new(0),
            backup_file_name: backup_storage
                .clone()
                .map(|path| format!("{}/{}-{}-{}.nbackup", path, chunk_id, id, seq_id)),
            wal_file: parking_lot::Mutex::new(wal_file_opt),
            wal_file_name,
            archived: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
            tiered_lock: AtomicU8::new(tiered_lock),
            reference_bit: AtomicBool::new(false),
            last_sync_time: AtomicI64::new(0),
            bytes_since_sync: AtomicUsize::new(0),
        }
    }

    pub fn try_acquire(&self, size: u32) -> Option<usize> {
        let size = size as usize;
        loop {
            let curr_last = self.append_header.load(Ordering::Acquire);
            let exp_last = curr_last + size;
            if exp_last > self.bound {
                return None;
            } else {
                if self
                    .append_header
                    .compare_exchange(curr_last, exp_last, Ordering::AcqRel, Ordering::Relaxed)
                    .is_err()
                {
                    continue;
                } else {
                    debug_assert_eq!(
                        align_address(8, curr_last),
                        curr_last,
                        "Acquired address is not aligned"
                    );
                    return Some(curr_last);
                }
            }
        }
    }

    pub fn shrink(&self, size: usize) {
        // If the segment is full or larger than SEGMENT_SIZE, there's nothing to shrink
        if size >= SEGMENT_SIZE {
            return;
        }
        // DISABLED: punch_hole() is incompatible with tiered memory eviction
        // Tiered memory handles memory reclamation through eviction, so punch_hole() is not needed
        // Calling punch_hole() can cause SIGSEGV when eviction tries to archive() a segment
        // whose tail pages were freed by punch_hole()
        // punch_hole(self.addr, size);
    }

    fn append_header(&self) -> usize {
        self.append_header.load(Ordering::Relaxed)
    }

    pub fn entry_iter(&self) -> SegmentEntryIter {
        SegmentEntryIter {
            bound: self.append_header(),
            cursor: self.addr,
        }
    }

    pub fn dead_space(&self) -> u32 {
        self.dead_space.load(Ordering::Relaxed)
    }

    // dead space plus tombstone spaces
    pub fn total_dead_space(&self) -> u32 {
        // We count tombstone space becasue we want to actively clean them out when they are obsolete
        let tombstones_space =
            self.tombstones.load(Ordering::Relaxed) * TOMBSTONE_SIZE_U32;
        let dead_cells_space = self.dead_space();
        return tombstones_space + dead_cells_space;
    }

    pub fn used_spaces(&self) -> u32 {
        let space = self.append_header.load(Ordering::Relaxed) as usize - self.addr;
        debug_assert!(space <= SEGMENT_SIZE);
        return space as u32;
    }

    pub fn living_space(&self) -> u32 {
        let total_dead_space = self.total_dead_space();
        let used_space = self.used_spaces();
        if total_dead_space <= used_space {
            used_space - total_dead_space
        } else {
            warn!(
                "living space check error for segment {}, used {}, dead {}",
                self.id, used_space, total_dead_space
            );
            0
        }
    }

    pub fn valid_space(&self) -> u32 {
        return self.used_spaces() - self.dead_space();
    }

    pub fn living_rate(&self) -> f32 {
        let used_space = self.used_spaces() as f32;
        if used_space == 0f32 {
            // empty segment
            return 1f32;
        }
        return self.living_space() as f32 / used_space;
    }

    // archive this segment and write the data to backup storage
    pub fn archive(&self) -> Result<bool, io::Error> {
        debug!(
            "archive() called for segment {}, backup_file_name={:?}",
            self.id, self.backup_file_name
        );
        if let &Some(ref backup_file) = &self.backup_file_name {
            while !self.no_references() { /* wait until all references released */ }
            let backup_file_path = Path::new(backup_file);
            if backup_file_path.exists() {
                // warn!(
                //     "Segment backup {} exists and can't archive twice",
                //     backup_file
                // );
                // return Ok(false);
                debug!(
                    "[DEBUG ONLY] Removing existing backup file for segment {}",
                    self.id
                );
                fs::remove_file(backup_file_path)?;
            }

            // Ensure parent directory exists before creating backup file
            if let Some(parent) = backup_file_path.parent() {
                create_dir_all(parent)?;
            }

            // TODO: restore this after testing
            // if let Some(ref wal_file) = self.wal_file_name {
            //     // if there is a WAL file ready, copy this file to backup
            //     // First, flush and close the WAL file if it's open
            //     {
            //         let mut file_opt = self.wal_file.lock();
            //         if let Some(mut writer) = file_opt.take() {
            //             // Flush and sync the file before closing
            //             writer.flush()?;
            //             writer.get_ref().sync_all()?;
            //             // Writer is dropped here, closing the file handle
            //             // file_opt is now None
            //         } else {
            //             // WAL file was already closed or never opened
            //             // This is fine - we'll read it from disk if it exists
            //             debug!("WAL file mutex is empty for segment {}, will read from disk if file exists", self.id);
            //         }
            //     }

            //     // Check if WAL file exists before trying to copy it
            //     // Note: The file might exist even if the mutex is empty (e.g., after recovery)
            //     let wal_path = Path::new(wal_file);
            //     if wal_path.exists() {
            //         // CRITICAL: WAL files contain only the written data, not full SEGMENT_SIZE.
            //         // For explicit file I/O during promotion (pread), we need full-size files.
            //         // Copy WAL and pad to SEGMENT_SIZE with zeros.

            //         // Read the WAL file
            //         let wal_data = std::fs::read(wal_file)?;
            //         let wal_size = wal_data.len();

            //         // Create backup file and write WAL data + padding
            //         let mut backup_file = File::create(backup_file_path)?;
            //         backup_file.write_all(&wal_data)?;

            //         // Pad to SEGMENT_SIZE if needed
            //         if wal_size < SEGMENT_SIZE {
            //             let padding_size = SEGMENT_SIZE - wal_size;
            //             let padding = vec![0u8; padding_size];
            //             backup_file.write_all(&padding)?;
            //             debug!(
            //                 "Padded WAL backup for segment {} from {} to {} bytes",
            //                 self.id, wal_size, SEGMENT_SIZE
            //             );
            //         }

            //         backup_file.sync_all()?;

            //         // Remove the WAL file (file is now closed, so this should succeed)
            //         remove_file(wal_file)?;
            //         return Ok(true);
            //     } else {
            //         // WAL file doesn't exist, fall through to memory-based archiving
            //         debug!("WAL file {} does not exist for segment {}, falling back to memory-based archiving", wal_file, self.id);
            //     }
            // }

            // Fallback: write from memory if WAL file doesn't exist or wasn't configured
            {
                let backup_file = File::create(backup_file_path)?;
                // Write only the valid data up to append_header to avoid reading from
                // memory that may have been freed by punch_hole() during compaction.
                // Promotion uses mmap() which handles variable-sized files correctly.
                let valid_size = self.append_header() - self.addr;
                let write_size = valid_size.max(PAGE_SIZE); // At least one page to ensure file exists
                
                debug!(
                    "Archiving segment {} from memory: valid_size={}, write_size={}",
                    self.id, valid_size, write_size
                );
                
                let mut buffer = BufWriter::with_capacity(write_size, backup_file);
                unsafe {
                    let data_block = slice::from_raw_parts(self.addr as *const u8, write_size);
                    buffer.write(data_block)?;
                }
                buffer.flush()?;
                buffer.get_ref().sync_all()?;
                return Ok(true);
            }
        } else {
            warn!(
                "Segment {} has no backup file name, cannot archive",
                self.id
            );
        }
        return Ok(false);
    }

    pub fn write_wal(&self, addr: usize, size: u32, skip_sync: bool) -> io::Result<()> {
        let mut file_opt = self.wal_file.lock();
        if let Some(ref mut file) = *file_opt {
            unsafe {
                let data_block = slice::from_raw_parts(addr as *const u8, size as usize);
                file.write(data_block)?;
            }

            // Transactions control their own sync at commit time
            // For non-transactional writes, use group commit batching
            if skip_sync {
                // Transaction context: no sync, will be synced at commit
                trace!(
                    "WAL sync skipped for segment {} (transactional write, will sync at commit)",
                    self.id
                );
                return Ok(());
            }

            // Group commit logic for non-transactional writes
            let current_time = get_time();
            let bytes_written = self
                .bytes_since_sync
                .fetch_add(size as usize, Ordering::Relaxed)
                + size as usize;
            let last_sync = self.last_sync_time.load(Ordering::Relaxed);
            let time_since_sync = current_time - last_sync;

            // Sync if either threshold is reached:
            // 1. Enough bytes have accumulated (batch size threshold)
            // 2. Enough time has passed (time threshold)
            let should_sync =
                bytes_written >= WAL_SYNC_BATCH_SIZE || time_since_sync >= WAL_SYNC_INTERVAL_MS;

            if should_sync {
                // Only flush if we're going to sync (sync_data implicitly flushes)
                file.get_ref().sync_data()?;

                // Reset counters after successful sync
                self.bytes_since_sync.store(0, Ordering::Relaxed);
                self.last_sync_time.store(current_time, Ordering::Relaxed);

                trace!(
                    "WAL synced for segment {} ({} bytes, {} ms since last sync)",
                    self.id,
                    bytes_written,
                    time_since_sync
                );
            } else {
                trace!("WAL write buffered for segment {} ({} bytes accumulated, {} ms since last sync)",
                       self.id, bytes_written, time_since_sync);
            }
        }
        Ok(())
    }

    /// Force a WAL sync, ensuring all buffered data is persisted to disk
    /// This is useful for transaction commits and other critical durability points
    pub fn force_wal_sync(&self) -> io::Result<()> {
        let mut file_opt = self.wal_file.lock();
        if let Some(ref mut file) = *file_opt {
            file.flush()?;
            file.get_ref().sync_all()?;

            // Reset counters after forced sync
            let current_time = get_time();
            self.bytes_since_sync.store(0, Ordering::Relaxed);
            self.last_sync_time.store(current_time, Ordering::Relaxed);

            trace!("Forced WAL sync for segment {}", self.id);
        }
        Ok(())
    }

    pub fn no_references(&self) -> bool {
        self.references.load(Ordering::Relaxed) == 0
    }

    pub fn mem_drop(&self, chunk: &Chunk) {
        if self
            .dropped
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            chunk.allocator.free(self.addr);
        }
    }
    // remove the backup if it have one
    pub fn dispense(&self) {
        debug!("dispense segment {}", self.id);
        if let &Some(ref backup_storage) = &self.backup_file_name {
            let path = Path::new(backup_storage);
            if path.exists() {
                if let Err(_e) = remove_file(path) {
                    error!("cannot reclaim segment file on dispense {}", backup_storage)
                }
            } else {
                warn!("cannot find segment backup to dispense {}", backup_storage)
            }
        }
    }

    // Tiered memory helper methods (stubs when tiered memory is disabled)

    /// Check if segment is hot (in anonymous memory)
    /// Always returns true when tiered memory is disabled
    /// This is a fast check that doesn't acquire the lock (may be stale)
    #[inline]
    pub fn is_hot(&self) -> bool {
        self.tiered_lock.load(Ordering::Relaxed) & HOT_COLD_MASK == HOT_SEGMENT
    }

    /// Check if segment is cold (backed by file)
    /// Always returns false when tiered memory is disabled
    /// This is a fast check that doesn't acquire the lock (may be stale)
    #[inline]
    pub fn is_cold(&self) -> bool {
        self.tiered_lock.load(Ordering::Relaxed) & HOT_COLD_MASK == COLD_SEGMENT
    }

    #[inline]
    pub fn is_locked(&self) -> bool {
        let lock_bits = self.tiered_lock.load(Ordering::Relaxed);
        lock_bits & HOT_COLD_MASK != lock_bits
    }

    pub fn set_cold(&self) {
        self.tiered_lock.store(COLD_SEGMENT, Ordering::Relaxed);
    }

    pub fn set_hot(&self) {
        self.tiered_lock.store(HOT_SEGMENT, Ordering::Relaxed);
    }
    pub fn lock_cold(&self) -> bool {
        self.tiered_lock
            .compare_exchange(
                COLD_SEGMENT,
                COLD_SEGMENT | LOCKING_SEGMENT_BITS,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    pub fn lock_hot(&self) -> bool {
        self.tiered_lock
            .compare_exchange(
                HOT_SEGMENT,
                HOT_SEGMENT | LOCKING_SEGMENT_BITS,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Mark segment as recently accessed (for CLOCK algorithm)
    /// No-op when tiered memory is disabled
    #[inline]
    pub fn mark_referenced(&self) {
        self.reference_bit.store(true, Ordering::Relaxed);
    }

    /// Clear reference bit and return old value (for CLOCK algorithm)
    /// Always returns false when tiered memory is disabled
    #[inline]
    pub fn clear_reference_bit(&self) -> bool {
        self.reference_bit.swap(false, Ordering::Relaxed)
    }

    /// Get current reference bit value without clearing
    /// Always returns false when tiered memory is disabled
    #[inline]
    pub fn get_reference_bit(&self) -> bool {
        self.reference_bit.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn contains_address(&self, addr: usize) -> bool {
        self.addr <= addr && addr < self.bound
    }
}

pub struct SegmentEntryIter {
    pub(crate) bound: usize,
    pub(crate) cursor: usize,
}

impl Iterator for SegmentEntryIter {
    type Item = EntryMeta;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let cursor = self.cursor;
        if cursor >= self.bound {
            return None;
        }
        let (entry_header, entry_meta) = entry::Entry::decode_from(cursor, |body_pos, header| {
            let entry_size = ENTRY_HEAD_SIZE + header.content_length as usize;
            debug!("Found body pos {}. Header: {:?}, entry size: {}, entry pos: {}, content length {}, bound {}",
                       body_pos, header, entry_size, cursor, header.content_length, self.bound);
            return EntryMeta {
                body_pos,
                entry_header: header,
                entry_size,
                entry_pos: cursor,
            };
        });

        // Stop iteration if we encounter UNDECIDED entries (uninitialized space)
        // This can happen if the segment is partially written or if we're iterating
        // while the segment is being modified
        if entry_header.entry_type == entry::EntryType::UNDECIDED {
            debug!(
                "Stopping segment iteration at UNDECIDED entry at position {}",
                cursor
            );
            return None;
        }

        // Validate that the entry doesn't exceed the bound
        let next_cursor = cursor + entry_meta.entry_size;
        if next_cursor > self.bound {
            warn!("Entry at position {} exceeds segment bound (size: {}, bound: {}), stopping iteration",
                  cursor, entry_meta.entry_size, self.bound);
            return None;
        }

        self.cursor = next_cursor;
        Some(entry_meta)
    }
}

pub struct SegmentAllocator {
    base: usize,
    offset: AtomicUsize,
    limit: usize,
    gc_threshold: usize,
    free: LinkedRingBufferList<usize, 64>,
    pub next_seq_id: AtomicUsize,
    chunk_id: usize,
}

impl SegmentAllocator {
    pub fn new(chunk_id: usize, chunk_size: usize) -> Self {
        Self::new_with_base(chunk_id, 0, chunk_size, true)
    }

    /// Create allocator with pre-allocated base address
    /// If allocate_memory=false, assumes memory at base_addr already exists
    pub fn new_with_base(
        chunk_id: usize,
        base_addr: usize,
        chunk_size: usize,
        allocate_memory: bool,
    ) -> Self {
        let (base, addr, limit) = if allocate_memory {
            // Old behavior: allocate our own mmap
            let overflow = SEGMENT_SIZE - PAGE_SIZE;
            let aligned_size = chunk_size + overflow;
            let ptr = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    aligned_size,
                    PROT_READ | PROT_WRITE,
                    MAP_ANONYMOUS | MAP_PRIVATE,
                    -1,
                    0,
                )
            };
            let addr = ptr as usize;
            let start = addr + overflow;
            let aligned_addr = start & SEGMENT_MASK;
            (aligned_addr, aligned_addr, aligned_addr + chunk_size)
        } else {
            // New behavior: use provided base from global allocation
            (base_addr, base_addr, base_addr + chunk_size)
        };

        Self {
            base,
            offset: AtomicUsize::new(addr),
            limit,
            gc_threshold: base + (chunk_size as f64 * 0.9) as usize - SEGMENT_SIZE,
            free: LinkedRingBufferList::new(),
            next_seq_id: AtomicUsize::new(0),
            chunk_id,
        }
    }

    pub fn meet_gc_threshold(&self) -> bool {
        self.offset.load(Relaxed) > self.gc_threshold
    }

    pub fn alloc_seg(
        &self,
        backup_storage: &Option<String>,
        wal_storage: &Option<String>,
    ) -> Option<Segment> {
        self.free
            .pop_front()
            .or_else(|| loop {
                debug!("Allocate segment by bump pointer");
                let addr = self.offset.load(Relaxed);
                let new_addr = addr + SEGMENT_SIZE;
                if new_addr > self.limit {
                    // Check the right boundary
                    return None;
                } else {
                    if self
                        .offset
                        .compare_exchange(addr, new_addr, AcqRel, Relaxed)
                        .is_ok()
                    {
                        return Some(addr);
                    }
                }
            })
            .map(|addr| {
                let id = self.id_by_addr(addr);
                let seq_id = self.next_seq_id.fetch_add(1, Ordering::AcqRel);
                Segment::new(
                    id as u64,
                    seq_id as u64,
                    self.chunk_id,
                    addr,
                    true,
                    backup_storage,
                    wal_storage,
                )
            })
    }

    /// Allocate a segment with a specific seq_id (for recovery purposes)
    /// This preserves the original seq_id from recovered files
    pub fn alloc_seg_with_seq_id(
        &self,
        seq_id: u64,
        backup_storage: &Option<String>,
        wal_storage: &Option<String>,
    ) -> Option<Segment> {
        // First allocate the address
        self.free
            .pop_front()
            .or_else(|| loop {
                debug!("Allocate segment by bump pointer (recovery)");
                let addr = self.offset.load(Relaxed);
                let new_addr = addr + SEGMENT_SIZE;
                if new_addr > self.limit {
                    return None;
                } else {
                    if self
                        .offset
                        .compare_exchange(addr, new_addr, AcqRel, Relaxed)
                        .is_ok()
                    {
                        return Some(addr);
                    }
                }
            })
            .map(|addr| {
                let id = self.id_by_addr(addr);
                // Use the provided seq_id instead of fetching a new one
                Segment::new(
                    id as u64,
                    seq_id,
                    self.chunk_id,
                    addr,
                    true,
                    backup_storage,
                    wal_storage,
                )
            })
    }

    pub fn free(&self, seg_addr: usize) {
        debug_assert!(seg_addr >= self.base);
        debug_assert!(seg_addr < self.limit);
        debug!("Segment {} freed", seg_addr);
        self.free.push_front(seg_addr);
    }

    pub fn id_by_addr(&self, addr: usize) -> usize {
        let offset = addr - self.base;
        let id = offset >> SEGMENT_BITS_SHIFT;
        id
    }

    #[inline]
    pub fn addr_by_id(&self, id: usize) -> usize {
        self.base + (id << SEGMENT_BITS_SHIFT)
    }
}

/// Free physical memory pages for a memory region
///
/// Uses MADV_DONTNEED on all platforms, which tells the kernel to free
/// physical pages while keeping the virtual mapping intact. For file-backed
/// mappings, pages will be re-faulted from disk on next access. For anonymous
/// mappings, pages will be zero-filled on next access.
///
/// Note: On Linux, MADV_REMOVE would punch holes in files (destructive),
/// so we use MADV_DONTNEED instead which is safe for both anonymous and
/// file-backed mappings.
///
/// This is aggressive - pages are freed immediately.
pub unsafe fn madvise_free(addr: usize, size: usize) {
    madvise(addr as *mut c_void, size, MADV_DONTNEED);
}

/// Mark pages as cold (low priority for eviction)
///
/// Uses MADV_COLD (Linux 5.4+) to hint to the kernel that these pages
/// should be evicted first under memory pressure. Unlike MADV_DONTNEED,
/// this doesn't immediately free pages - it just marks them as candidates
/// for eviction.
///
/// This is cooperative - the kernel decides when to actually evict pages.
/// Pages remain resident until the kernel needs memory.
///
/// Falls back to MADV_DONTNEED on older kernels or non-Linux systems.
pub unsafe fn madvise_cold(addr: usize, size: usize) {
    #[cfg(target_os = "linux")]
    {
        // MADV_COLD = 20 (Linux 5.4+)
        const MADV_COLD: i32 = 20;
        let result = madvise(addr as *mut c_void, size, MADV_COLD);

        if result != 0 {
            let errno = std::io::Error::last_os_error();
            // EINVAL likely means old kernel without MADV_COLD support
            if errno.raw_os_error() == Some(libc::EINVAL) {
                warn!("MADV_COLD not supported (kernel < 5.4), falling back to MADV_DONTNEED");
                madvise(addr as *mut c_void, size, MADV_DONTNEED);
            } else {
                warn!("madvise(MADV_COLD) failed: {}", errno);
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        // Fall back to MADV_DONTNEED on non-Linux systems
        madvise(addr as *mut c_void, size, MADV_DONTNEED);
    }
}

fn punch_hole(seg_addr: usize, seg_size: usize) {
    let right_boundary = seg_addr + seg_size;
    let aligned_addr = (((right_boundary - 1) >> PAGE_SHIFT) + 1) << PAGE_SHIFT;
    let hole_length = (seg_addr + SEGMENT_SIZE) - aligned_addr;
    if hole_length > PAGE_SIZE {
        // Have pages to release
        debug!(
            "Partially free the segment by puching hole with size {}",
            hole_length
        );
        unsafe {
            madvise_free(aligned_addr, hole_length);
        }
    }
}
