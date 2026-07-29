use crate::ram::cell::cell_hash_from_entry_content_addr;
use crate::ram::chunk::Chunk;
use crate::ram::entry::EntryType;
use crate::ram::segs::{Segment, SegmentEntryIter};
use lightning::map::WordMutexGuard;
use std::collections::HashSet;
use std::io;

/// Result of locking all cells in a segment
pub struct CellLocks {
    seg_id: u64,
    pub locks: Vec<WordMutexGuard<'static>>,
}

impl CellLocks {
    /// Create a new empty CellLocks (for when no locks are needed)
    pub fn empty(seg_id: u64) -> Self {
        Self {
            seg_id,
            locks: Vec::new(),
        }
    }
}

/// Lock all cells in a segment to prevent concurrent access during critical operations
///
/// This function scans a segment, finds all cell IDs, and locks them iteratively.
/// It prevents readers from acquiring cell locks during operations like eviction/promotion.
///
/// # Arguments
/// * `segment` - The segment to lock cells in
/// * `chunk` - The chunk containing the cell index
/// * `entry_iter` - Iterator over segment entries (can be from segment itself or temporary mapping)
///
/// # Returns
/// * `Ok(CellLocks)` - Successfully locked all cells, containing the locks
/// * `Err(io::Error)` - Failed to lock cells after maximum retries
pub fn lock_all_cells_in_segment(
    segment: &Segment,
    chunk: &Chunk,
    entry_iter: SegmentEntryIter,
    promoting: bool,
) -> Result<CellLocks, io::Error> {
    debug!(
        "Scanning segment {} for promotion={} to find all cells",
        segment.id, promoting
    );

    // Collect all cell hashes from the segment and deduplicate by hash
    let mut cell_hashes_set: HashSet<u64> = HashSet::with_capacity(1024);
    for entry_meta in entry_iter {
        if entry_meta.entry_header.entry_type == EntryType::CELL {
            let hash = cell_hash_from_entry_content_addr(entry_meta.body_pos);
            cell_hashes_set.insert(hash);
        }
    }

    // Convert to Vec for iteration
    let mut unique_cell_hashes: Vec<u64> = cell_hashes_set.into_iter().collect();
    unique_cell_hashes.sort();

    if cfg!(debug_assertions) && unique_cell_hashes.is_empty() {
        warn!(
            "No cells found in segment {} for promotion={}",
            segment.id, promoting
        );
    }

    debug!(
        "Found {} unique cells in segment {} for promotion={} to lock",
        unique_cell_hashes.len(),
        segment.id,
        promoting
    );

    // Lock all cell IDs iteratively
    let mut locks: Vec<WordMutexGuard> = Vec::with_capacity(unique_cell_hashes.len());
    let mut cell_hashes: Vec<u64> = unique_cell_hashes;

    const MAX_RETRY_ATTEMPTS: usize = 100;
    let backoff = crossbeam::utils::Backoff::new();
    let mut retry_count = 0;
    let mut stale_cells = 0;
    let mut skipped_locking_cells = 0;

    while !cell_hashes.is_empty() {
        let mut still_unlocked = Vec::new();

        for hash in cell_hashes {
            // Check the cell eearly without locking such that the cell is not in the segment, we can skip it.
            if let Some(addr) = chunk.cell_index.get_from_mutex(&(hash as usize)) {
                if !segment.contains_address(addr) {
                    stale_cells += 1;
                    continue;
                }
            }
            match chunk.cell_index.try_lock(hash as usize) {
                Some(Some(lock)) => {
                    let addr = *lock;
                    // Verify the cell is actually in this segment
                    if segment.contains_address(addr) {
                        // SAFETY: We're extending the lifetime to 'static, which is safe because:
                        // 1. The locks are dropped when CellLocks is dropped, before any memory becomes invalid
                        // 2. The segment address never changes (stable memory layout)
                        // 3. The chunk and cell_index outlive the locks (they're passed by reference)
                        unsafe {
                            locks.push(std::mem::transmute(lock));
                        }
                    } else {
                        debug!(
                            "Cell {} is not in segment {}, dropping lock",
                            hash, segment.id
                        );
                        stale_cells += 1;
                        drop(lock);
                    }
                    retry_count = 0;
                    backoff.reset();
                }
                Some(None) if promoting => {
                    // When promoting, we can skip the cell that is locked.
                    skipped_locking_cells += 1;
                    continue;
                }
                Some(None) => {
                    // Cell is currently locked by another thread, retry
                    still_unlocked.push(hash);
                    retry_count += 1;
                    if retry_count >= MAX_RETRY_ATTEMPTS {
                        warn!("Failed to lock cell {} after {} retries for segment {} for promotion={}", hash, retry_count, segment.id, promoting);
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("Failed to lock cell {} after {} retries for segment {} for promotion={}", hash, retry_count, segment.id, promoting),
                        ));
                    }
                    trace!("Failed to lock cell {} after {} retries for segment {} for promotion={}, retrying", hash, retry_count, segment.id, promoting);
                    backoff.spin();
                }
                None => {
                    error!(
                        "Cell {} not found in chunk {} index for segment {} for promotion={}",
                        hash, chunk.id, segment.id, promoting
                    );
                }
            }
        }

        if promoting {
            // When promoting, only cells that are locked by read/write operations should be locked.
            // In such case we just return the locks that are already locked.
            break;
        }

        cell_hashes = still_unlocked;

        if !cell_hashes.is_empty() {
            std::thread::yield_now();
        }
    }

    debug!(
        "Successfully locked {} cells, stale {} cells, skipped locking {} cells in segment {}",
        locks.len(),
        stale_cells,
        skipped_locking_cells,
        segment.id
    );

    Ok(CellLocks {
        locks,
        seg_id: segment.id,
    })
}

#[cfg(debug_assertions)]
impl Drop for CellLocks {
    fn drop(&mut self) {
        debug!(
            "Dropping CellLocks with {} locks for segment {}",
            self.locks.len(),
            self.seg_id
        );
    }
}
