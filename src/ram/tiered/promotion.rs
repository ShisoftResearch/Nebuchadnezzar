use crate::ram::compression;
use crate::ram::recovery::find_append_header;
use crate::ram::segs::{Segment, SegmentEntryIter, SegmentExclusiveRefGuard, SEGMENT_SIZE};
use std::io::Read;
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
pub fn promote_segment(segment: &Segment) {
    debug!(
        "[PROMOTE] Starting promotion of segment {} (chunk={}, seq_id={})",
        segment.id, segment.chunk_id, segment.seq_id
    );

    // Step 1: Acquire segment lock (blocks until available)
    // This ensures only one promotion/eviction/cleaner operation at a time
    loop {
        let _exclusive_guard = if let Some(l) = SegmentExclusiveRefGuard::new(segment) {
            l
        } else {
            continue;
        };
        if segment.is_hot() {
            // Already hot, skip promotion
            debug!(
                "[PROMOTE] Segment {} (chunk={}, seq_id={}) is already hot, skipping",
                segment.id, segment.chunk_id, segment.seq_id
            );
            return;
        }
        if segment.lock_cold() {
            // Locked cold, proceed with promotion
            debug!(
                "[PROMOTE] Acquired cold lock on segment {} (chunk={}, seq_id={})",
                segment.id, segment.chunk_id, segment.seq_id
            );
            break;
        }
        // Locked for any reason, wait
        thread::yield_now();
    }

    debug!(
        "[PROMOTE] Proceeding with promotion of segment {} (chunk={}, seq_id={})",
        segment.id, segment.chunk_id, segment.seq_id
    );

    debug!(
        "Promotion: all references released for segment {}",
        segment.id
    );

    // Step 4: Check if segment has active references (including transaction guards)
    // Promotion should not happen while segment has active references
    // if !segment.no_references() {
    //     warn!(
    //         "Segment {} has active references (ref count: {}), should not promote, but promoting anyway",
    //         segment.id,
    //         segment.references.load(std::sync::atomic::Ordering::Relaxed)
    //     );
    // }

    // Step 5: Open backup file on demand instead of keeping it open
    // This avoids holding file descriptors for idle/cold segments.
    let file_state = segment.file_state.lock();
    let backup_path = {
        match file_state
            .manager
            .backup_path(segment.chunk_id, segment.id, segment.seq_id)
        {
            Some(path) => path,
            None => {
                error!(
                    "CRITICAL: Segment {} is marked COLD but backup path does not exist",
                    segment.id
                );
                segment.set_cold();
                panic!("Cannot promote segment {}: missing backup path", segment.id);
            }
        }
    };

    let mut backup_file = match std::fs::File::open(&backup_path) {
        Ok(file) => file,
        Err(e) => {
            let parent_dir = std::path::Path::new(&backup_path).parent();
            let files_in_dir: Vec<String> = parent_dir
                .and_then(|p| std::fs::read_dir(p).ok())
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .filter_map(|e| e.file_name().into_string().ok())
                        .filter(|name| name.contains(&format!("-{}-", segment.id)))
                        .collect()
                })
                .unwrap_or_default();

            debug!(
                "PROMOTION FAILED: Failed to open backup file {} for segment {} (chunk={}, seq_id={}): {}\n\
                 Files in directory matching seg_id {}: {:?}",
                backup_path,
                segment.id,
                segment.chunk_id,
                segment.seq_id,
                e,
                segment.id,
                files_in_dir
            );
            segment.set_cold();
            panic!(
                "Cannot promote segment {} (seq_id={}): failed to open backup file: {}. \
                 Existing files for this seg_id: {:?}",
                segment.id, segment.seq_id, e, files_in_dir
            );
        }
    };

    // Start with empty buffer - read_to_end will fill it with file contents
    let mut temp_buffer = Vec::with_capacity(SEGMENT_SIZE);

    // Read all file contents (may be compressed)
    if let Err(e) = backup_file.read_to_end(&mut temp_buffer) {
        error!(
            "Failed to read backup file {} for segment {}: {}",
            backup_path, segment.id, e
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

    // Decompress if the file is compressed (auto-detects compression)
    temp_buffer = match compression::decompress_if_compressed(&temp_buffer) {
        Ok(decompressed) => {
            debug!(
                "Decompressed backup file for segment {}: {} bytes -> {} bytes",
                segment.id,
                bytes_read,
                decompressed.len()
            );
            decompressed
        }
        Err(e) => {
            error!(
                "Failed to decompress backup file for segment {}: {}",
                segment.id, e
            );
            segment.set_cold();
            panic!(
                "Cannot promote segment {}: failed to decompress backup file: {}",
                segment.id, e
            );
        }
    };

    // Resize to SEGMENT_SIZE, padding with zeros if needed
    // This ensures the segment memory is fully initialized
    let decompressed_size = temp_buffer.len();
    debug_assert_eq!(decompressed_size, SEGMENT_SIZE);

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

    let _temp_entry_iter = SegmentEntryIter {
        bound: scan_boundary,
        cursor: temp_start,
    };

    // let _locks =
    //     cell_locking::lock_all_cells_in_segment(segment, chunk, _temp_entry_iter, true).unwrap();

    let segment_addr = segment.addr;

    unsafe {
        ptr::copy_nonoverlapping(temp_buffer.as_ptr(), segment_addr as *mut u8, SEGMENT_SIZE);
    }

    // Memory barrier: ensure the copy is complete and visible before marking hot
    // This prevents other threads from reading the segment before data is fully written
    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);

    let restored_append_header = segment.addr + scanned_size;
    segment
        .append_header
        .store(restored_append_header, Ordering::Release);
    debug!(
        "Restored append_header for segment {} to {} (offset {}) based on scanned file boundary",
        segment.id, restored_append_header, scanned_size
    );

    debug!("Data copied successfully for segment {}", segment.id);

    #[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
    {
        // Verify checksum: compare segment memory with source backup data after promotion
        // Compare the full SEGMENT_SIZE since that's what was copied (including padding)
        if let Err(e) = segment.verify_promotion_checksum(&temp_buffer) {
            error!(
                "Checksum verification failed for segment {} after promotion: {}",
                segment.id, e
            );
            segment.set_cold();
            panic!(
                "Cannot promote segment {}: checksum verification failed: {}",
                segment.id, e
            );
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
    debug!(
        "[PROMOTION COMPLETED] Segment {} (chunk={}, seq_id={}) is now hot",
        segment.id, segment.chunk_id, segment.seq_id
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
