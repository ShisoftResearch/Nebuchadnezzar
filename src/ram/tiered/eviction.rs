use crate::ram::chunk::Chunk;
use crate::ram::segs::{madvise_free, Segment, SEGMENT_SIZE};
use crate::ram::tiered::cell_locking;
use std::io;
use std::sync::atomic::Ordering;
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
/// 4. Lock all cell IDs in the cell index (prevents new readers)
/// 5. Write segment data to backup file
/// 6. Mark as cold and free physical pages (update tiered_lock)
/// 7. Unlock all cells
///
/// This approach:
/// - Prevents readers from acquiring cell locks during/after eviction
/// - Ensures no one can access memory that's being freed
/// - Archive happens while cells are locked (safe)
pub fn evict_segment(segment: &Segment, chunk: &Chunk) -> Result<(), io::Error> {
    debug!("evict_segment called for segment {}", segment.id);

    // Step 1: Try to acquire segment lock (skip if already locked)
    if !segment.lock_hot() {
        debug!("Segment {} is not hot, skipping eviction", segment.id);
        return Ok(());
    }

    debug!(
        "Evicting segment {} to cold storage with cell locking",
        segment.id
    );

    // Step 3: Wait for no active references (prevents races with cleaner)
    while !segment.no_references() {
        thread::yield_now();
    }

    // Step 4: Lock all cells in the segment
    let _cell_locks = match cell_locking::lock_all_cells_in_segment(
        segment,
        chunk,
        segment.entry_iter(),
        false,
    ) {
        Ok(cell_locks) => cell_locks,
        Err(e) => {
            error!("Failed to lock cells for segment {}: {}", segment.id, e);
            segment.set_hot();
            return Err(e);
        }
    };

    // Step 5: Write segment data to backup file while cells are locked
    let archived = match segment.archive() {
        Ok(archived) => archived,
        Err(e) => {
            error!("Failed to archive segment {}: {}", segment.id, e);
            segment.set_hot();
            return Err(e);
        }
    };
    debug!(
        "Archive result for segment {}: archived={}",
        segment.id, archived
    );

    // Get backup path and verify it exists
    let backup_path = match segment.backup_file_name.as_ref() {
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
    if !archived && !std::path::Path::new(backup_path).exists() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Segment {} backup file does not exist and archive failed",
                segment.id
            ),
        ));
    }

    // Step 6: Mark as cold and free physical pages (update tiered_lock)
    segment.set_cold();
    unsafe {
        madvise_free(segment.addr, SEGMENT_SIZE);
    }
    debug!(
        "Marked segment {} as cold and freed physical pages",
        segment.id
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
