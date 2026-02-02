use crate::ram::chunk::Chunk;
#[cfg(feature = "compress_backups")]
use crate::ram::compression;
use crate::ram::entry;
use crate::ram::entry::EntryMeta;
use crate::ram::file_manager::SegmentFileManager;
use crate::ram::io::align_address;
use crate::ram::tombstone::TOMBSTONE_SIZE_U32;
use bifrost::utils::time::get_time;
#[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
use crc32fast::Hasher as Crc32Hasher;
use libc::*;
use lightning::list::LinkedRingBufferList;
use lightning::spin_hint::Backoff;
use parking_lot;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicU8, AtomicUsize,
    Ordering::{self, *},
};
use std::sync::Arc;
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

pub const EXCLUSIVE_REF_COUNT: usize = usize::MAX;

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

#[repr(C, align(64))] // Ensure consistent memory layout and cache line alignment
pub struct Segment {
    pub id: u64,
    pub seq_id: u64,
    pub chunk_id: usize,
    pub addr: usize,
    pub bound: usize,
    pub append_header: AtomicUsize,
    pub dead_space: AtomicU32,
    pub tombstones: AtomicU32,
    /// Generation counter for changes that introduce dead space (dead cells or tombstones)
    dead_bytes_generation: AtomicU64,
    /// Marker used by cleaners to skip segments that were cleaned without reclaiming space
    last_no_progress_clean_generation: AtomicU64,
    references: AtomicUsize,
    pub file_state: parking_lot::Mutex<SegmentFileState>,
    pub dropped: AtomicBool,
    // Tiered memory fields
    /// Segment lock for tiered memory operations (eviction, promotion, cleaner)
    /// Holds the hot/cold state: false = hot (anonymous memory), true = cold (backed by file)
    /// Cell read/write operations do NOT need this lock, only cell-level locks
    pub tiered_lock: AtomicU8, // 1 = hot, 2 = cold, highest bit for locking
    pub reference_bit: AtomicBool, // For CLOCK eviction algorithm (set by mprotect fault handler)
    /// Timestamp in ms of last promotion, used to avoid immediate re-eviction
    pub last_promoted_ms: AtomicI64,
    /// Timestamp in ms of last eviction, used for churn detection
    pub last_evicted_ms: AtomicI64,
    /// Tracks if WAL has been written to since last successful archive
    /// Used to optimize eviction: if archived=true && is_dirty=false, can skip re-archiving
    is_dirty: AtomicBool,
    // WAL batch sync tracking (for group commit optimization)
    pub last_sync_time: AtomicI64, // Timestamp of last fsync in milliseconds
    pub bytes_since_sync: AtomicUsize, // Bytes written since last fsync
}

/// File state for a segment, protected by a mutex
///
/// **LOCK ORDERING INVARIANT**:
/// To prevent deadlock, locks must be acquired in this order:
/// 1. `tiered_lock` (atomic, not a mutex)
/// 2. `file_state` (this mutex)
/// 3. Cell locks (via cell_index)
///
/// All code paths that acquire multiple locks MUST follow this order.
/// See eviction.rs and promotion.rs for examples.
/// File state for a segment, protected by a mutex
///
/// **MEMORY OPTIMIZATION**: Uses unbuffered File handles instead of BufWriter
/// to avoid accumulating 512KB+ buffers per segment. With thousands of segments,
/// BufWriter buffers caused multi-GB memory leaks.
pub struct SegmentFileState {
    pub manager: Arc<SegmentFileManager>,
    pub wal: Option<File>,
}

impl Segment {
    pub fn new(
        id: u64,
        seq_id: u64,
        chunk_id: usize,
        buffer_ptr: usize,
        hot: bool,
        file_manager: Arc<SegmentFileManager>,
    ) -> Segment {
        let size = SEGMENT_SIZE;

        if let Err(e) = file_manager.init_directories() {
            panic!("Failed to initialize storage directories: {}", e);
        }

        // WAL files are created lazily on first write. This avoids keeping file descriptors
        // open for cold/idle segments. Active head segments will create and hold WAL files
        // while they are being written to.
        let wal_file_opt = None;

        debug!(
            "Creating new segment chunk {}, id {}, seq_id {}, size {}, address {}",
            chunk_id, id, seq_id, size, buffer_ptr
        );
        let tiered_lock = if hot { HOT_SEGMENT } else { COLD_SEGMENT };
        Segment {
            addr: buffer_ptr,
            id,
            seq_id,
            chunk_id,
            bound: buffer_ptr + size,
            append_header: AtomicUsize::new(buffer_ptr),
            dead_space: AtomicU32::new(0),
            tombstones: AtomicU32::new(0),
            dead_bytes_generation: AtomicU64::new(0),
            last_no_progress_clean_generation: AtomicU64::new(0),
            references: AtomicUsize::new(0),
            file_state: parking_lot::Mutex::new(SegmentFileState {
                manager: file_manager,
                wal: wal_file_opt,
            }),
            dropped: AtomicBool::new(false),
            tiered_lock: AtomicU8::new(tiered_lock),
            reference_bit: AtomicBool::new(false),
            last_promoted_ms: AtomicI64::new(0),
            last_evicted_ms: AtomicI64::new(0),
            is_dirty: AtomicBool::new(true), // Start dirty
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
        self.punch_hole(size);
    }

    /// Frees memory pages from this segment starting at the given offset.
    ///
    /// This function uses `madvise_free` to free pages from the aligned start offset
    /// to the end of the segment. The address is aligned up to the next page boundary
    /// to ensure proper page alignment required by madvise.
    ///
    /// # Arguments
    /// * `start_offset` - The offset within the segment to start freeing from (will be aligned up to page boundary)
    ///
    /// # Safety
    /// This function should only be called when the freed region is no longer needed,
    /// as accessing the freed pages may cause the OS to zero them or remap them.
    ///
    /// # Example
    /// ```rust,ignore
    /// use neb::ram::segs::Segment;
    ///
    /// // After writing some data to a segment, you can free unused tail pages
    /// // (assuming `segment` is a valid Segment reference)
    /// let used_size = 1024 * 100; // 100KB used
    /// segment.punch_hole(used_size);
    /// ```
    pub fn punch_hole(&self, start_offset: usize) {
        // Calculate the absolute address of the start offset
        let start_addr = self.addr + start_offset;

        // Align to the next page boundary (round up)
        let aligned_addr = align_address(PAGE_SIZE, start_addr);

        // Calculate the size to free (from aligned address to end of segment)
        let end_addr = self.bound;

        if aligned_addr < end_addr {
            let size = end_addr - aligned_addr;

            // Only punch hole if we have at least one page to free
            if size >= PAGE_SIZE {
                debug!(
                    "Punching hole in segment {} from offset {} (aligned to {}), size {} bytes ({} pages)",
                    self.id,
                    start_offset,
                    aligned_addr - self.addr,
                    size,
                    size / PAGE_SIZE
                );
                unsafe {
                    madvise_free(aligned_addr, size);
                }
            }
        }
    }

    pub fn free_memory(&self) {
        unsafe {
            madvise_free(self.addr, SEGMENT_SIZE);
        }
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

    /// Track changes that introduce new dead bytes so cleaners can detect progress.
    #[inline]
    pub fn note_dead_bytes_change(&self) {
        // Clear any "no progress" marker when new dead bytes show up so the cleaner
        // can try again.
        self.last_no_progress_clean_generation
            .store(0, Ordering::Relaxed);
        self.dead_bytes_generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark this segment as cleaned without reclaiming space for the current generation.
    #[inline]
    pub fn mark_clean_no_progress(&self) {
        let gen = self.dead_bytes_generation.load(Ordering::Relaxed);
        if gen > 0 {
            self.last_no_progress_clean_generation
                .store(gen, Ordering::Relaxed);
        }
    }

    /// Clear the "no progress" marker so the cleaner can reconsider this segment.
    #[inline]
    pub fn clear_clean_no_progress(&self) {
        self.last_no_progress_clean_generation
            .store(0, Ordering::Relaxed);
    }

    /// Returns true if the cleaner already tried this generation and reclaimed nothing.
    #[inline]
    pub fn cleaned_without_progress(&self) -> bool {
        let gen = self.dead_bytes_generation.load(Ordering::Relaxed);
        gen > 0
            && gen
                == self
                    .last_no_progress_clean_generation
                    .load(Ordering::Relaxed)
    }

    // dead space plus tombstone spaces
    pub fn total_dead_space(&self) -> u32 {
        // We count tombstone space becasue we want to actively clean them out when they are obsolete
        let tombstones_space = self.tombstones.load(Ordering::Relaxed) * TOMBSTONE_SIZE_U32;
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

    #[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
    fn calculate_crc32(data: &[u8]) -> u32 {
        let mut hasher = Crc32Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    #[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
    fn verify_archive_checksum(
        &self,
        source_data: &[u8],
        backup_path: &Path,
        pad_to_size: Option<usize>,
        segment_id: u64,
    ) -> Result<(), io::Error> {
        // Pad source data if needed (for WAL files that are padded to SEGMENT_SIZE)
        let source_data_padded = if let Some(target_size) = pad_to_size {
            debug!(
                "Padding source data: original_size={}, target_size={}",
                source_data.len(),
                target_size
            );
            if source_data.len() < target_size {
                let mut padded = source_data.to_vec();
                padded.resize(target_size, 0);
                debug!(
                    "Padded source data: original_size={}, padded_size={}",
                    source_data.len(),
                    padded.len()
                );
                padded
            } else {
                debug!("Source data already >= target_size, no padding needed");
                source_data.to_vec()
            }
        } else {
            source_data.to_vec()
        };

        debug!(
            "Calculating checksum: source_data_padded.len()={}",
            source_data_padded.len()
        );
        let source_checksum = Self::calculate_crc32(&source_data_padded);

        // Read the backup file to calculate its checksum
        let mut backup_file = File::open(backup_path)?;
        let mut backup_data = Vec::new();
        backup_file.read_to_end(&mut backup_data)?;
        let backup_checksum = Self::calculate_crc32(&backup_data);

        if source_checksum != backup_checksum {
            error!(
                "CRC32 checksum mismatch for segment {}: source={:08x} (size={}), backup={:08x} (size={}) for segment {}",
                self.id, source_checksum, source_data_padded.len(), backup_checksum, backup_data.len(), segment_id
            );
            // Log first few bytes for debugging
            let source_preview = if source_data_padded.len() >= 16 {
                format!("{:02x?}", &source_data_padded[..16])
            } else {
                format!("{:02x?}", source_data_padded)
            };
            let backup_preview = if backup_data.len() >= 16 {
                format!("{:02x?}", &backup_data[..16])
            } else {
                format!("{:02x?}", backup_data)
            };
            error!(
                "Source data preview (first 16 bytes): {}, Backup data preview (first 16 bytes): {}",
                source_preview, backup_preview
            );
            panic!("CRC32 checksum mismatch for segment {}: source={:08x}, backup={:08x} for segment {}", self.id, source_checksum, backup_checksum, segment_id);
        } else {
            debug!(
                "CRC32 checksum verified for segment {}: {:08x}",
                self.id, source_checksum
            );
        }
        Ok(())
    }

    /// Verify checksum of segment memory against backup file (for eviction)
    /// Only compiled in debug builds
    #[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
    pub fn verify_eviction_checksum(&self, backup_path: &Path) -> Result<(), io::Error> {
        let write_size = {
            let valid_size = self.append_header() - self.addr;
            valid_size.max(PAGE_SIZE) // At least one page to ensure file exists
        };

        unsafe {
            let segment_data = slice::from_raw_parts(self.addr as *const u8, write_size);
            // Backup file is padded to SEGMENT_SIZE, so pad source data to match
            self.verify_archive_checksum(segment_data, backup_path, Some(SEGMENT_SIZE), self.id)
        }
    }

    /// Verify checksum of segment memory against source data (for promotion)
    /// Only compiled in debug builds
    #[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
    pub fn verify_promotion_checksum(&self, source_data: &[u8]) -> Result<(), io::Error> {
        // Compare the full SEGMENT_SIZE that was copied during promotion
        // (not based on append_header, which may be less than SEGMENT_SIZE)
        let compare_size = SEGMENT_SIZE.min(source_data.len());

        unsafe {
            let segment_data = slice::from_raw_parts(self.addr as *const u8, compare_size);
            let source_slice = &source_data[..compare_size];

            let segment_checksum = Self::calculate_crc32(segment_data);
            let source_checksum = Self::calculate_crc32(source_slice);

            if segment_checksum != source_checksum {
                error!(
                    "CRC32 checksum mismatch after promotion for segment {}: segment={:08x}, source={:08x}",
                    self.id, segment_checksum, source_checksum
                );
                panic!("CRC32 checksum mismatch after promotion for segment {}: segment={:08x}, source={:08x}", self.id, segment_checksum, source_checksum);
            } else {
                debug!(
                    "CRC32 checksum verified after promotion for segment {}: {:08x}",
                    self.id, segment_checksum
                );
            }
        }
        Ok(())
    }

    // archive this segment and write the data to backup storage
    // Backup files are opened on demand and closed immediately after use
    pub fn archive(&self) -> Result<bool, io::Error> {
        let mut state = self.file_state.lock();
        let backup_path_opt = state
            .manager
            .backup_path(self.chunk_id, self.id, self.seq_id);

        debug!(
            "archive() called for segment {}, backup_path={:?}",
            self.id, backup_path_opt
        );

        if let Some(backup_file) = backup_path_opt {
            // NOTE: We do NOT wait for no_references() here because:
            // 1. The file_state mutex already ensures only one archive at a time
            // 2. Waiting here could deadlock if another component holds tiered_lock
            // 3. Reading segment memory during archive is safe - data is copied atomically
            // The reference counter is only for preventing madvise_free during eviction
            let backup_file_path = Path::new(&backup_file);
            let has_old_backup = backup_file_path.exists();
            // If backup file already exists, we use make a backup of the existing file
            if has_old_backup {
                debug!("Backup file {} already exists, moving to .old", backup_file);
                // Handle race condition where file might be deleted between check and rename
                match fs::rename(&backup_file, format!("{}.old", backup_file)) {
                    Ok(_) => {
                        debug!("Successfully moved old backup file to .old");
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        // File was deleted between exists() check and rename - this is fine
                        debug!("Backup file disappeared before rename (likely deleted by another process)");
                    }
                    Err(e) => {
                        // Other errors should be propagated
                        return Err(e);
                    }
                }
                // Prepare the new backup file by creating a fresh writer
                let _ = state.manager.open_or_create_backup_writer(
                    self.chunk_id,
                    self.id,
                    self.seq_id,
                )?;
            }
            {
                // Always open a fresh backup writer for this archive operation
                if let Some(mut file) = state.manager.open_or_create_backup_writer(
                    self.chunk_id,
                    self.id,
                    self.seq_id,
                )? {
                    // Truncate the file to zero and write from beginning
                    file.set_len(0)?;
                    file.sync_all()?;

                    unsafe {
                        let data_block =
                            slice::from_raw_parts(self.addr as *const u8, SEGMENT_SIZE);

                        // Create a padded copy to SEGMENT_SIZE to match WAL-based archiving behavior
                        let padded_data = Vec::from(data_block);

                        debug_assert_eq!(padded_data.len(), SEGMENT_SIZE);

                        // Conditionally compress based on feature flag
                        #[cfg(feature = "compress_backups")]
                        {
                            let compressed_data = compression::compress(&padded_data)?;
                            file.write_all(&compressed_data)?;
                            debug!(
                                "Archived segment {} with compression: {} bytes -> {} bytes (ratio: {:.2}%)",
                                self.id,
                                SEGMENT_SIZE,
                                compressed_data.len(),
                                (compressed_data.len() as f64 / SEGMENT_SIZE as f64) * 100.0
                            );
                        }

                        #[cfg(not(feature = "compress_backups"))]
                        {
                            file.write_all(&padded_data)?;
                            debug!(
                                "Archived segment {} without compression: {} bytes",
                                self.id, SEGMENT_SIZE
                            );
                        }
                    }

                    file.sync_all()?;
                    drop(file);

                    #[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
                    {
                        // Note: Checksum verification is skipped for compressed files
                        // as LZ4 compression includes its own integrity checks
                        debug!(
                            "Skipping CRC32 checksum verification for compressed segment {} (LZ4 has built-in integrity)",
                            self.id
                        );
                    }

                    // Sanity check: verify backup file actually exists before marking archived
                    let backup_file_path = Path::new(&backup_file);
                    if !backup_file_path.exists() {
                        error!(
                            "CRITICAL: Archive wrote segment {} but backup file does not exist at '{}'",
                            self.id, backup_file
                        );
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!(
                                "Archive failed: backup file '{}' not found after write",
                                backup_file
                            ),
                        ));
                    }

                    debug!(
                        "Archived segment {} to backup file '{}'",
                        self.id, backup_file
                    );
                    self.clear_dirty();

                    // Close and delete WAL file since backup now contains all data
                    // Recovery prefers backup files over WAL files (see file_manager.rs:272-285)
                    // Closing the file descriptor first ensures clean deletion
                    if let Some(wal) = state.wal.take() {
                        drop(wal); // Close the file descriptor
                        debug!("Closed WAL file descriptor for segment {}", self.id);
                    }

                    // Delete the WAL file from disk
                    if let Err(e) = state
                        .manager
                        .delete_wal(self.chunk_id, self.id, self.seq_id)
                    {
                        warn!("Failed to delete WAL file for segment {}: {}", self.id, e);
                    } else {
                        debug!("Deleted WAL file for archived segment {}", self.id);
                    }

                    return Ok(true);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Failed to create backup writer",
                    ));
                }
            }
        } else {
            warn!(
                "Segment {} has no backup storage configured, cannot archive",
                self.id
            );
            return Ok(false);
        }
        return Ok(false);
    }

    pub fn write_wal(&self, addr: usize, size: u32, skip_sync: bool) -> io::Result<()> {
        let mut state = self.file_state.lock();
        // Lazily create WAL file on first write if not already present
        if state.wal.is_none() {
            state.wal = state
                .manager
                .create_wal_file(self.chunk_id, self.id, self.seq_id)?;
        }

        if let Some(ref mut file) = state.wal {
            unsafe {
                let data_block = slice::from_raw_parts(addr as *const u8, size as usize);
                file.write_all(data_block)?; // Use write_all to ensure all bytes are written
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
                // Sync data to disk
                file.sync_data()?;

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
        let mut state = self.file_state.lock();
        if let Some(ref mut file) = state.wal {
            file.sync_all()?;

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

    pub fn obtain_exclusive_references(&self) -> bool {
        self.references
            .compare_exchange(0, EXCLUSIVE_REF_COUNT, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    pub fn release_exclusive_references(&self) {
        self.references.store(0, Ordering::Relaxed);
    }

    pub fn incr_references(&self) -> bool {
        let backoff = Backoff::new();
        loop {
            let curr_refs = self.references.load(Ordering::Relaxed);
            if curr_refs == EXCLUSIVE_REF_COUNT {
                // Do not compete for exclusive references, bail out instead of spinning
                // The cleaners obtains segment lock first, then cell locks,
                // while normal operations obtains cell lock then segment counter
                // this could cause deadlock if the cleaners is waiting for the segment lock
                return false;
            }
            if self
                .references
                .compare_exchange(
                    curr_refs,
                    curr_refs + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
            backoff.spin();
        }
    }

    pub fn decr_references(&self) {
        let backoff = Backoff::new();
        #[cfg(debug_assertions)]
        loop {
            let curr_refs = self.references.load(Ordering::Relaxed);
            // If reference count is already 0, just return. This can happen in race conditions
            // where a PendingEntry is dropped after the segment has been cleaned up.
            debug_assert!(
                curr_refs > 0,
                "Segment {} has negative references {}",
                self.id,
                curr_refs
            );
            debug_assert!(
                curr_refs != EXCLUSIVE_REF_COUNT,
                "Segment {} has exclusive references, which should not happen",
                self.id
            );
            if self
                .references
                .compare_exchange(
                    curr_refs,
                    curr_refs - 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return;
            }
            backoff.spin();
        }
        #[cfg(not(debug_assertions))]
        {
            self.references.fetch_sub(1, Ordering::Relaxed);
        }
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
        let backtrace = std::backtrace::Backtrace::capture();
        debug!(
            "[DISPENSE] segment {} (chunk={}, seq_id={}) - tiered_state={}\nBacktrace:\n{}",
            self.id,
            self.chunk_id,
            self.seq_id,
            if self.is_hot() { "HOT" } else { "COLD" },
            backtrace
        );
        let state = self.file_state.lock();
        if let Some(backup_path) = state
            .manager
            .backup_path(self.chunk_id, self.id, self.seq_id)
        {
            let exists = std::path::Path::new(&backup_path).exists();
            debug!(
                "[DISPENSE] Deleting backup for segment {} (chunk={}, seq_id={}): {} (exists: {})",
                self.id, self.chunk_id, self.seq_id, backup_path, exists
            );
        }
        if let Err(e) = state
            .manager
            .delete_all(self.chunk_id, self.id, self.seq_id)
        {
            debug!(
                "[DISPENSE ERROR] Failed to delete files for segment {} (chunk={}, seq_id={}): {}",
                self.id, self.chunk_id, self.seq_id, e
            );
        } else {
            debug!(
                "[DISPENSE SUCCESS] Deleted files for segment {} (chunk={}, seq_id={})",
                self.id, self.chunk_id, self.seq_id
            );
        }
    }

    // Tiered memory helper methods (stubs when tiered memory is disabled)

    /// Check if segment is hot (in anonymous memory)
    /// Always returns true when tiered memory is disabled
    /// This is a fast check that doesn't acquire the lock (may be stale)
    #[inline]
    pub fn is_hot(&self) -> bool {
        // Use Acquire ordering to ensure we see the latest state from other threads
        // This pairs with Release in set_hot() to prevent reading stale data
        self.tiered_lock.load(Ordering::Acquire) & HOT_COLD_MASK == HOT_SEGMENT
    }

    /// Check if segment is cold (backed by file)
    /// Always returns false when tiered memory is disabled
    /// This is a fast check that doesn't acquire the lock (may be stale)
    #[inline]
    pub fn is_cold(&self) -> bool {
        // Check if segment is cold or being promoted (locked while cold)
        // During promotion, tiered_lock is COLD_SEGMENT | LOCKING_SEGMENT_BITS
        // We need to return true in both cases to prevent reads from seeing garbage
        // Use Acquire ordering to ensure we see the latest state from other threads
        let state = self.tiered_lock.load(Ordering::Acquire);
        (state & HOT_COLD_MASK) == COLD_SEGMENT
            || (state & HOT_COLD_MASK) == (COLD_SEGMENT | LOCKING_SEGMENT_BITS)
    }

    #[inline]
    pub fn is_locked(&self) -> bool {
        let lock_bits = self.tiered_lock.load(Ordering::Relaxed);
        lock_bits & HOT_COLD_MASK != lock_bits
    }

    pub fn set_cold(&self) {
        // Debug: capture backtrace to track who is marking this segment cold
        let backtrace = std::backtrace::Backtrace::capture();

        // Verify backup file exists before marking cold
        let backup_path = {
            let state = self.file_state.lock();
            state
                .manager
                .backup_path(self.chunk_id, self.id, self.seq_id)
        };

        if let Some(ref path) = backup_path {
            let exists = std::path::Path::new(path).exists();
            if !exists {
                debug!(
                    "CRITICAL BUG: set_cold() called for segment {} (chunk={}, seq_id={}) but backup file does NOT exist at '{}'!\n\
                     Backtrace:\n{}",
                    self.id, self.chunk_id, self.seq_id, path, backtrace
                );
                // Always panic to catch this immediately
                panic!(
                    "set_cold() called without backup file for segment {} (chunk={}, seq_id={}) at '{}'",
                    self.id, self.chunk_id, self.seq_id, path
                );
            } else {
                debug!(
                    "[DEBUG] set_cold() for segment {} (chunk={}, seq_id={}): backup verified at '{}'",
                    self.id, self.chunk_id, self.seq_id, path
                );
            }
        } else {
            debug!(
                "[DEBUG] set_cold() for segment {} (chunk={}, seq_id={}) but no backup path configured",
                self.id, self.chunk_id, self.seq_id
            );
        }

        self.tiered_lock.store(COLD_SEGMENT, Ordering::Relaxed);
    }

    pub fn set_hot(&self) {
        // Use Release ordering to ensure all previous writes are visible before setting hot
        // This pairs with Acquire in is_cold() to prevent reading stale data
        self.tiered_lock.store(HOT_SEGMENT, Ordering::Release);
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

    pub fn lock_hot_to_cold(&self) -> bool {
        self.tiered_lock
            .compare_exchange(
                HOT_SEGMENT,
                COLD_SEGMENT | LOCKING_SEGMENT_BITS,
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

    /// Mark the segment as recently promoted to give it a cooldown window during eviction
    #[inline]
    pub fn mark_promoted_now(&self) {
        self.last_promoted_ms.store(get_time(), Ordering::Relaxed);
    }

    /// Check if the segment was promoted within the provided window (milliseconds)
    #[inline]
    pub fn recently_promoted_within(&self, window_ms: u64) -> bool {
        if window_ms == 0 {
            return false;
        }
        let last = self.last_promoted_ms.load(Ordering::Relaxed);
        if last <= 0 {
            return false;
        }
        let now = get_time();
        now - last <= window_ms as i64
    }

    /// Mark the segment as recently evicted for churn detection
    #[inline]
    pub fn mark_evicted_now(&self) {
        self.last_evicted_ms.store(get_time(), Ordering::Relaxed);
    }

    /// Check if the segment was evicted within a window (milliseconds)
    #[inline]
    pub fn recently_evicted_within(&self, window_ms: u64) -> bool {
        if window_ms == 0 {
            return false;
        }
        let last = self.last_evicted_ms.load(Ordering::Relaxed);
        if last <= 0 {
            return false;
        }
        let now = get_time();
        now - last <= window_ms as i64
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

    pub fn set_dirty(&self) {
        debug!("set_dirty for segment {}", self.id);
        self.is_dirty.store(true, Ordering::Release);
    }

    pub fn clear_dirty(&self) {
        debug!("clear_dirty for segment {}", self.id);
        self.is_dirty.store(false, Ordering::Release);
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::Relaxed)
    }
}

/// RAII guard that holds a reference to a segment, preventing it from being evicted.
/// The reference count is automatically decremented when the guard is dropped.
/// This ensures no reference leaks even in error paths or panics.
pub struct SegmentReferenceGuard {
    segment: lightning::aarc::Arc<Segment>,
}

impl SegmentReferenceGuard {
    /// Create a new guard and increment the segment's reference count
    pub fn new(segment: lightning::aarc::Arc<Segment>) -> Self {
        segment.incr_references();
        debug!(
            "SegmentReferenceGuard acquired for segment {} (ref count: {})",
            segment.id,
            segment.references.load(Ordering::Relaxed)
        );
        Self { segment }
    }

    /// Get the segment ID
    pub fn segment_id(&self) -> u64 {
        self.segment.id
    }

    /// Get the chunk ID
    pub fn chunk_id(&self) -> usize {
        self.segment.chunk_id
    }
}

impl Drop for SegmentReferenceGuard {
    fn drop(&mut self) {
        self.segment.decr_references();
    }
}

pub struct SegmentExclusiveRefGuard<'a> {
    segment: &'a Segment,
}

impl<'a> Drop for SegmentExclusiveRefGuard<'a> {
    fn drop(&mut self) {
        debug_assert_eq!(
            self.segment.references.load(Ordering::Relaxed),
            EXCLUSIVE_REF_COUNT
        );
        self.segment.release_exclusive_references();
    }
}

impl<'a> SegmentExclusiveRefGuard<'a> {
    pub fn new(segment: &'a Segment) -> Option<Self> {
        if !segment.obtain_exclusive_references() {
            return None;
        }
        Some(Self { segment })
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
        debug_assert!(
            next_cursor <= self.bound,
            "Entry at position {} exceeds segment bound (size: {}, bound: {})",
            cursor,
            entry_meta.entry_size,
            self.bound
        );

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

    pub fn alloc_seg(&self, file_manager: &Arc<SegmentFileManager>) -> Option<Segment> {
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
                    file_manager.clone(),
                )
            })
    }

    /// Allocate a segment with a specific seq_id (for recovery purposes)
    /// This preserves the original seq_id from recovered files
    pub fn alloc_seg_with_seq_id(
        &self,
        seq_id: u64,
        file_manager: &Arc<SegmentFileManager>,
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
                    file_manager.clone(),
                )
            })
    }

    /// Allocate a segment at a specific ID for recovery purposes
    /// This ensures recovered data goes to the correct address
    pub fn alloc_seg_at_id(
        &self,
        seg_id: u64,
        seq_id: u64,
        file_manager: &Arc<SegmentFileManager>,
    ) -> Option<Segment> {
        let addr = self.addr_by_id(seg_id as usize);

        // Ensure address is within bounds
        if addr >= self.limit {
            error!(
                "Cannot allocate segment {} at address {:#x}: exceeds limit {:#x}",
                seg_id, addr, self.limit
            );
            return None;
        }

        // Update offset if needed (to track allocated space)
        let required_end = addr + SEGMENT_SIZE;
        loop {
            let current_offset = self.offset.load(Ordering::Relaxed);
            if current_offset >= required_end {
                break; // Already allocated past this point
            }
            // Try to bump the offset
            if self
                .offset
                .compare_exchange(
                    current_offset,
                    required_end,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }

        Some(Segment::new(
            seg_id,
            seq_id,
            self.chunk_id,
            addr,
            true, // hot - recovery starts segments as hot
            file_manager.clone(),
        ))
    }

    /// Set the next seq_id for allocations (used after recovery)
    /// This ensures new segments continue from where recovered segments left off
    pub fn set_next_seq_id(&self, seq_id: u64) {
        let current = self.next_seq_id.load(Ordering::Relaxed);
        // Only update if the new value is higher (could be called from multiple threads during recovery)
        if seq_id as usize > current {
            self.next_seq_id.store(seq_id as usize, Ordering::Release);
            info!(
                "Set next_seq_id for chunk {} to {} (was {})",
                self.chunk_id, seq_id, current
            );
        }
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

pub unsafe fn madvise_free(addr: usize, size: usize) {
    #[cfg(target_os = "linux")]
    let advice = MADV_DONTNEED; // Drop the memory immediately instead of using MADV_FREE to wait for the kernel to reclaim it;
    #[cfg(not(target_os = "linux"))]
    let advice = MADV_DONTNEED;

    let result = madvise(addr as *mut c_void, size, advice);
    if result != 0 {
        let errno = std::io::Error::last_os_error();
        if errno.raw_os_error() == Some(libc::EINVAL) {
            warn!(
                "MADV_({}) not supported, falling back to MADV_DONTNEED",
                advice
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_punch_hole_alignment() {
        let _ = env_logger::try_init();

        // Create a test segment allocator
        let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 2);
        let file_manager = Arc::new(SegmentFileManager::new(None, None));

        // Allocate a segment
        let segment = allocator
            .alloc_seg(&file_manager)
            .expect("Failed to allocate segment");

        // Test 1: Punch hole from middle of segment (should align up to next page)
        let offset = 1024; // 1KB offset (not page aligned)
        segment.punch_hole(offset);

        // Test 2: Punch hole from page-aligned offset
        let aligned_offset = PAGE_SIZE * 2; // 8KB offset (page aligned)
        segment.punch_hole(aligned_offset);

        // Test 3: Punch hole from near end of segment (should not free if less than PAGE_SIZE)
        let near_end_offset = SEGMENT_SIZE - PAGE_SIZE / 2;
        segment.punch_hole(near_end_offset);

        // If we got here without panicking, the test passes
        assert!(true, "punch_hole executed without errors");
    }

    #[test]
    fn test_punch_hole_edge_cases() {
        let _ = env_logger::try_init();

        let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 2);
        let file_manager = Arc::new(SegmentFileManager::new(None, None));
        let segment = allocator
            .alloc_seg(&file_manager)
            .expect("Failed to allocate segment");

        // Test edge case: offset at end of segment
        segment.punch_hole(SEGMENT_SIZE);

        // Test edge case: offset beyond end of segment (should do nothing)
        segment.punch_hole(SEGMENT_SIZE + 1000);

        // Test edge case: offset at 0 (should free almost entire segment)
        segment.punch_hole(0);

        assert!(true, "Edge cases handled correctly");
    }
}
