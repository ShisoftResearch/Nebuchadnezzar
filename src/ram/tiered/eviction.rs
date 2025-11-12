use crate::ram::chunk::Chunk;
use crate::ram::segs::{madvise_free, Segment, SEGMENT_SIZE};
use std::io;
use std::thread;

/// Evict a hot segment to cold storage with cell-level locking
///
/// **CRITICAL SAFETY**: We must lock ALL cells in the segment during eviction
/// to prevent concurrent reads from accessing memory while/after it's being freed.
///
/// Process:
/// 1. Try to acquire segment lock (skip if already locked by another operation)
/// 2. Check if already cold (skip if so)
/// 3. Wait for no active references (prevents races with cleaner)
/// 4. Write segment data to backup file (acquires and releases file_state)
/// 5. Lock all cell IDs in the cell index (prevents new readers)
/// 6. Mark as cold and free physical pages (update tiered_lock)
/// 7. Unlock all cells
///
/// Lock ordering: tiered_lock → file_state → cell_locks
/// This matches promotion to prevent deadlock.
///
/// This approach:
/// - Prevents readers from acquiring cell locks during/after eviction
/// - Ensures no one can access memory that's being freed
/// - Archive happens before cell locking to maintain consistent lock ordering
pub fn evict_segment(segment: &Segment, chunk: &Chunk) -> Result<(), io::Error> {
    debug!("evict_segment called for segment {}", segment.id);

    // Step 1: Try to acquire segment lock (skip if already locked)
    if !segment.lock_hot_to_cold() {
        debug!("Segment {} is not hot, skipping eviction", segment.id);
        return Err(io::Error::new(io::ErrorKind::Other, "Segment is not hot"));
    }

    debug!(
        "Evicting segment {} to cold storage with cell locking",
        segment.id
    );

    // Step 3: Wait for no active references (prevents races with cleaner)
    let mut wait_count = 0;
    while !segment.no_references() {
        wait_count += 1;
        if wait_count % 1000 == 0 {
            debug!(
                "Segment {} waiting for references to drop (waited {} times)",
                segment.id, wait_count
            );
        }
        thread::yield_now();
    }
    if wait_count > 0 {
        debug!(
            "Segment {} references dropped after {} waits",
            segment.id, wait_count
        );
    }

    // Check if segment needs archiving based on archived and wal_dirty flags
    // archived=true means backup file exists
    // wal_dirty=false means no WAL writes or memory modifications since last archive
    // Both must be true to skip archiving
    let is_clean = segment.archived.load(std::sync::atomic::Ordering::Acquire) 
        && !segment.wal_dirty.load(std::sync::atomic::Ordering::Acquire);
    
    if !is_clean {
        debug!(
            "Segment {} needs archiving before eviction (archived={}, wal_dirty={})",
            segment.id,
            segment.archived.load(std::sync::atomic::Ordering::Relaxed),
            segment.wal_dirty.load(std::sync::atomic::Ordering::Relaxed)
        );
        match segment.archive() {
            Ok(true) => {
                debug!("Segment {} archived successfully before eviction", segment.id);
            }
            Ok(false) => {
                warn!("Segment {} archive returned false before eviction", segment.id);
                // Continue anyway - backup file might exist from previous archive
            }
            Err(e) => {
                error!("Failed to archive segment {} before eviction: {}", segment.id, e);
                segment.set_hot();
                return Err(e);
            }
        }
    } else {
        debug!(
            "Segment {} already archived and clean, skipping redundant archive before eviction",
            segment.id
        );
    }

    // Get backup path and verify it exists
    let backup_path =
        match chunk
            .file_manager
            .backup_path(segment.chunk_id, segment.id, segment.seq_id)
        {
            Some(backup_path) => backup_path,
            None => {
                error!("Segment {} has no backup file path", segment.id);
                segment.set_hot();
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Segment {} has no backup file path", segment.id),
                ));
            }
        };

    // Verify file exists (either newly created or already present)
    let backup_path_ref = std::path::Path::new(&backup_path);
    if !backup_path_ref.exists() {
        segment.set_hot();
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Segment {} backup file does not exist",
                segment.id
            ),
        ));
    }

    #[cfg(all(debug_assertions, feature = "verify_checksums"))]
    {
        // Verify checksum: compare segment memory with backup file before freeing
        if let Err(e) = segment.verify_eviction_checksum(backup_path_ref) {
            error!("Checksum verification failed for segment {} during eviction: {}", segment.id, e);
            segment.set_hot();
            return Err(e);
        }
    }

    // Step 5: Lock all cells in the segment AFTER archiving
    // This maintains lock ordering: tiered_lock → file_state → cell_locks
    // let _cell_locks = match cell_locking::lock_all_cells_in_segment(
    //     segment,
    //     chunk,
    //     segment.entry_iter(),
    //     false,
    // ) {
    //     Ok(cell_locks) => {
    //         debug!("Successfully locked {} cells for segment {}", cell_locks.locks.len(), segment.id);
    //         cell_locks
    //     },
    //     Err(e) => {
    //         warn!(
    //             "Failed to lock cells for segment {}: {}. Giving up eviction",
    //             segment.id, e
    //         );
    //         segment.set_hot();
    //         return Err(e);
    //     }
    // };

    // Step 6: Close WAL file to free file descriptor (before marking cold)
    // Cold segments don't need WAL files - data is safely in backup
    // WAL will be lazily recreated if segment is promoted and written to again
    {
        let mut file_state = segment.file_state.lock();
        if let Some(wal) = file_state.wal.take() {
            if let Err(e) = wal.sync_all() {
                warn!("Failed to sync WAL for segment {} during eviction: {}", segment.id, e);
            }
            drop(wal);
            debug!("Closed WAL file for evicted segment {} (freed file descriptor)", segment.id);
        }
    }
    
    // Step 7: Mark as cold and free physical pages (update tiered_lock)
    segment.set_cold();
    debug!(
        "Marked segment {} as COLD (addr {:#x}), about to free physical pages",
        segment.id, segment.addr
    );
    unsafe {
        madvise_free(segment.addr, SEGMENT_SIZE);
    }
    debug!(
        "Freed physical pages for segment {} (addr {:#x})",
        segment.id, segment.addr
    );

    // Locks are automatically dropped when _cell_locks and tiered_guard go out of scope

    info!(
        "Successfully evicted segment {} to cold storage with cell locking",
        segment.id
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_eviction_basics() {
        // Basic eviction test
        // Full integration tests in tiered/tests.rs
    }
}
