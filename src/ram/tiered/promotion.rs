use crate::ram::chunk::Chunk;
use crate::ram::recovery::find_append_header;
use crate::ram::segs::{Segment, SegmentEntryIter, SEGMENT_SIZE};
use crate::ram::tiered::cell_locking;
use std::io::{Read, Seek};
use std::ptr;
use std::sync::atomic::Ordering;
use std::thread;

/// Promote a cold segment to hot storage with cell-level locking
///
/// This operation uses cell-level locking to ensure safety:
///
/// **CRITICAL SAFETY**: We must lock ALL cells in the segment during promotion
/// to prevent concurrent reads from accessing memory while it's being overwritten.
///
/// Process:
/// 1. Acquire segment lock (blocks until available) - ensures only one promotion at a time
/// 2. Check if already hot (skip if so)
/// 3. Wait for no active references (prevents races with cleaner)
/// 4. Check if segment is transaction-protected (skip if so)
/// 5. Read file into a TEMPORARY buffer (NOT the segment address)
/// 6. Scan temporary buffer to find all cell IDs
/// 7. Lock all cell IDs in the cell index (prevents new readers)
/// 8. Copy data from temporary buffer to segment address
/// 9. Mark as hot (update tiered_lock)
/// 10. Unlock all cells
/// 11. Clean up temporary buffer
///
/// This approach:
/// - Prevents readers from acquiring cell locks during data copy
/// - Uses temporary buffer to avoid corrupting segment address
/// - No "empty window" - data is copied atomically from reader's perspective
/// This function have to succeed or panic.
pub fn promote_segment(segment: &Segment, chunk: &Chunk) {
    debug!(
        "Promoting segment {} to hot storage with cell locking",
        segment.id
    );

    // Step 1: Acquire segment lock (blocks until available)
    // This ensures only one promotion/eviction/cleaner operation at a time
    loop {
        if segment.is_hot() {
            // Already hot, skip promotion
            debug!("Segment {} is already hot, skipping promotion", segment.id);
            return;
        }
        if segment.lock_cold() {
            // Locked cold, proceed with promotion
            break;
        }
        // Locked for any reason, wait
        thread::yield_now();
    }

    debug!("Segment {} is cold, proceeding with promotion", segment.id);

    // Step 3: Wait for no active references (prevents races with cleaner)
    // We hold the tiered_lock while waiting - this is safe because:
    // - Cell read/write operations don't need tiered_lock, only cell locks
    // - Eviction will skip if lock is held
    // - Other promotions will block on the lock (which is what we want)
    while !segment.no_references() {
        thread::yield_now();
    }
    debug!(
        "Promotion: all references released for segment {}",
        segment.id
    );

    // Step 4: Check if segment is protected by transactions
    if chunk.is_segment_protected(segment.id) {
        // Well, it should not happen, but if it does, there is nothing we can do about it.
        warn!(
            "Segment {} is transaction-protected, cannot promote",
            segment.id
        );
    }

    // Step 5: Get backup file handler from segment's file_state
    // Lock file_state to access or create the backup file handler
    let mut file_state = segment.file_state.lock();
    
    // Start with empty buffer - read_to_end will fill it with file contents
    let mut temp_buffer = Vec::with_capacity(SEGMENT_SIZE);
    
    let backup_file = match &mut file_state.backup {
        Some(file) => file,
        None => {
            error!(
                "CRITICAL: Segment {} is marked COLD but backup file does not exist", segment.id
            );
            segment.set_cold();
            panic!("Cannot promote segment {}: failed to obtain backup file", segment.id);
        }
    };

    match backup_file.rewind() {
        Ok(_) => {},
        Err(e) => {
            error!("Failed to rewind backup file for segment {}: {}", segment.id, e);
            segment.set_cold();
            panic!("Cannot promote segment {}: failed to rewind backup file: {}", segment.id, e);
        }
    }

    // Read all file contents (may be less than SEGMENT_SIZE)
    if let Err(e) = backup_file.get_mut().read_to_end(&mut temp_buffer) {
        error!(
            "Failed to read backup file for segment {}: {}",
            segment.id, e
        );
        segment.set_cold();
        panic!(
            "Cannot promote segment {}: failed to read backup file: {}",
            segment.id, e
        );
    }
    
    let bytes_read = temp_buffer.len();
    debug!(
        "Read {} bytes from backup file for segment {}",
        bytes_read, segment.id
    );
    
    // Resize to SEGMENT_SIZE, padding with zeros if needed
    // This ensures the segment memory is fully initialized
    temp_buffer.resize(SEGMENT_SIZE, 0);
    debug!(
        "Resized buffer to {} bytes (padded {} zero bytes) for segment {}",
        SEGMENT_SIZE, SEGMENT_SIZE - bytes_read, segment.id
    );

    // Always scan the buffer to find the actual valid data boundary
    // We need to treat the buffer as a memory region for scanning
    let temp_start = temp_buffer.as_ptr() as usize;
    let scanned_boundary = find_append_header(temp_start, SEGMENT_SIZE);
    let scanned_size = scanned_boundary - temp_start;

    debug!(
        "Scanned file for segment {}: found valid data boundary at offset {} (size: {})",
        segment.id, scanned_size, scanned_size
    );

    let scan_boundary = scanned_boundary;

    let temp_entry_iter = SegmentEntryIter {
        bound: scan_boundary,
        cursor: temp_start,
    };

    let _locks =
        cell_locking::lock_all_cells_in_segment(segment, chunk, temp_entry_iter, true).unwrap();

    let segment_addr = segment.addr;
    unsafe {
        ptr::copy_nonoverlapping(temp_buffer.as_ptr(), segment_addr as *mut u8, SEGMENT_SIZE);
    }

    let restored_append_header = segment.addr + scanned_size;
    segment
        .append_header
        .store(restored_append_header, Ordering::Release);
    debug!(
        "Restored append_header for segment {} to {} (offset {}) based on scanned file boundary",
        segment.id, restored_append_header, scanned_size
    );

    debug!("Data copied successfully for segment {}", segment.id);

    #[cfg(debug_assertions)]
    {
        // Verify checksum: compare segment memory with source backup data after promotion
        // Compare the full SEGMENT_SIZE since that's what was copied (including padding)
        if let Err(e) = segment.verify_promotion_checksum(&temp_buffer) {
            error!("Checksum verification failed for segment {} after promotion: {}", segment.id, e);
            segment.set_cold();
            panic!("Cannot promote segment {}: checksum verification failed: {}", segment.id, e);
        }
    }

    // Step 9: Mark as hot (update tiered_lock)
    segment.set_hot();
    debug!(
        "Marked segment {} as HOT (addr {:#x}) after promotion",
        segment.id, segment.addr
    );

    // Step 10: Set reference bit
    segment.mark_referenced();

    // Locks are automatically dropped when _cell_locks goes out of scope

    // Step 11: Buffer cleanup
    // temp_buffer will be automatically dropped and deallocated when it goes out of scope
    debug!("Cleaned up temporary buffer for segment {}", segment.id);
    
    info!(
        "Successfully promoted segment {} to hot storage with cell locking",
        segment.id
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_promotion_basics() {
        // Basic promotion test
        // Full integration tests in tiered/tests.rs
    }
}
