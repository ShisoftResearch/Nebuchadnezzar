use crate::ram::chunk::Chunk;
use crate::ram::recovery::find_append_header;
use crate::ram::segs::{Segment, SegmentEntryIter, SEGMENT_SIZE};
use crate::ram::tiered::cell_locking;
use std::fs::File;
use std::io::{Read, Write};
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
        error!(
            "Segment {} is transaction-protected, cannot promote",
            segment.id
        );
        segment.set_cold();
        panic!();
    }

    // Step 5: Get backup file handler from segment's file_state
    // Lock file_state to access or create the backup file handler
    let mut file_state = segment.file_state.lock();
    
    // Get backup path and verify it exists BEFORE opening
    let backup_path = file_state
        .manager
        .backup_path(segment.chunk_id, segment.id, segment.seq_id)
        .unwrap_or_else(|| String::from("<unknown>"));
    
    // CRITICAL: Verify backup file exists before attempting to open/mmap
    // A cold segment MUST have a backup file - if it doesn't exist, this is a bug
    let backup_path_obj = std::path::Path::new(&backup_path);
    if !backup_path_obj.exists() {
        error!(
            "CRITICAL: Segment {} is marked COLD but backup file does not exist: {}",
            segment.id, backup_path
        );
        error!(
            "This indicates the segment was evicted without proper archiving, or the backup file was deleted"
        );
        segment.set_cold();
        panic!(
            "Cannot promote segment {}: backup file does not exist at {}",
            segment.id, backup_path
        );
    }
    
    // Verify backup file is readable and non-empty
    match std::fs::metadata(&backup_path) {
        Ok(metadata) => {
            if metadata.len() == 0 {
                error!(
                    "CRITICAL: Backup file for segment {} exists but is empty: {}",
                    segment.id, backup_path
                );
                segment.set_cold();
                panic!(
                    "Cannot promote segment {}: backup file is empty at {}",
                    segment.id, backup_path
                );
            }
            debug!(
                "Backup file for segment {} exists and has size {} bytes: {}",
                segment.id,
                metadata.len(),
                backup_path
            );
        }
        Err(e) => {
            error!(
                "Failed to get metadata for backup file of segment {}: {} (path: {})",
                segment.id, e, backup_path
            );
            segment.set_cold();
            panic!(
                "Cannot promote segment {}: failed to access backup file at {}: {}",
                segment.id, backup_path, e
            );
        }
    }
    
    // If a backup writer is currently open for this segment (e.g. due to archiving), flush it so
    // that the on-disk contents are up-to-date before we read the file. Promotion itself does not
    // require a persistent file handle.
    if let Some(ref mut writer) = file_state.backup {
        if let Err(e) = writer.flush() {
            error!(
                "Failed to flush backup file for segment {}: {} (path: {})",
                segment.id, e, backup_path
            );
            segment.set_cold();
            panic!(
                "Cannot promote segment {}: failed to flush backup file: {}",
                segment.id, e
            );
        }
        debug!("Flushed backup file for segment {} before reading", segment.id);
    }

    // Step 6: Read file into a buffer instead of using mmap
    // Get the actual file size to determine how much to read
    let file_size = match std::fs::metadata(&backup_path) {
        Ok(metadata) => {
            let size = metadata.len() as usize;
            debug!(
                "Backup file for segment {}: size={} bytes",
                segment.id, size
            );
            size
        }
        Err(e) => {
            error!("Failed to get file size for segment {}: {}", segment.id, e);
            segment.set_cold();
            panic!("Failed to get file size for segment {}: {}", segment.id, e);
        }
    };

    // Limit read size to SEGMENT_SIZE
    let read_size = file_size.min(SEGMENT_SIZE);

    debug!(
        "Reading cold file (path={}) for segment {} into buffer (file_size={}, read_size={})",
        backup_path, segment.id, file_size, read_size
    );

    // Release the file_state lock before performing long operations (cell locking, data copy)
    drop(file_state);

    // Read file contents into a buffer from a fresh file handle so we don't retain any lingering
    // descriptors beyond this promotion.
    let mut temp_buffer = vec![0u8; read_size];
    if read_size > 0 {
        let mut backup_file = match File::open(&backup_path) {
            Ok(file) => file,
            Err(e) => {
                error!(
                    "Failed to open backup file for segment {}: {}",
                    segment.id, e
                );
                segment.set_cold();
                panic!(
                    "Cannot promote segment {}: failed to open backup file: {}",
                    segment.id, e
                );
            }
        };

        if let Err(e) = backup_file.read_exact(&mut temp_buffer) {
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
    }

    debug!(
        "Successfully read {} bytes from backup file for segment {}",
        read_size, segment.id
    );

    // Always scan the buffer to find the actual valid data boundary
    // We need to treat the buffer as a memory region for scanning
    let temp_start = temp_buffer.as_ptr() as usize;
    let scanned_boundary = find_append_header(temp_start, read_size);
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

    // Copy data from buffer (up to read_size) and zero-fill the rest if needed
    let copy_size = scanned_size.min(read_size);
    unsafe {
        ptr::copy_nonoverlapping(temp_buffer.as_ptr(), segment_addr as *mut u8, copy_size);
        // Zero-fill the rest of the segment if file was shorter than SEGMENT_SIZE
        if copy_size < SEGMENT_SIZE {
            ptr::write_bytes(
                (segment_addr + copy_size) as *mut u8,
                0,
                SEGMENT_SIZE - copy_size,
            );
        }
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
