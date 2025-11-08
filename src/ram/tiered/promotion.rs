use crate::ram::chunk::Chunk;
use crate::ram::recovery::find_append_header;
use crate::ram::segs::{Segment, SegmentEntryIter, SEGMENT_SIZE};
use crate::ram::tiered::cell_locking;
use libc::{c_void, close, mmap, munmap, MAP_PRIVATE, PROT_READ, PROT_WRITE};
use std::io;
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
/// 5. mmap file to a TEMPORARY location (NOT the segment address)
/// 6. Scan temporary mapping to find all cell IDs
/// 7. Lock all cell IDs in the cell index (prevents new readers)
/// 8. Copy data from temporary mapping to segment address
/// 9. Mark as hot and close file descriptor (update tiered_lock)
/// 10. Unlock all cells
/// 11. Unmap temporary file
///
/// This approach:
/// - Prevents readers from acquiring cell locks during data copy
/// - Uses temporary mapping to avoid corrupting segment address
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

    // Step 5: Get backup file path and open it
    let backup_path = match segment.backup_file_name.as_ref() {
        Some(backup_path) => backup_path,
        None => {
            error!("Segment {} has no backup file path", segment.id);
            segment.set_cold();
            panic!();
        }
    };

    let path_cstr = match std::ffi::CString::new(backup_path.as_str()) {
        Ok(path_cstr) => path_cstr,
        Err(e) => {
            error!("Invalid path: {}", e);
            segment.set_cold();
            panic!();
        }
    };

    let fd = unsafe { libc::open(path_cstr.as_ptr(), libc::O_RDONLY) };
    if fd < 0 {
        let err = io::Error::last_os_error();
        error!(
            "Failed to open backup file for segment {}: {}",
            segment.id, err
        );
        segment.set_cold();
        panic!();
    }

    // Step 6: mmap file to a TEMPORARY location (NOT the segment address)
    // This allows us to scan the data without corrupting the existing segment
    // Get the actual file size BEFORE mmap to avoid mapping beyond the file
    let file_size = match std::fs::metadata(backup_path) {
        Ok(metadata) => metadata.len() as usize,
        Err(e) => {
            error!("Failed to get file size for segment {}: {}", segment.id, e);
            unsafe { close(fd) };
            panic!("Failed to get file size for segment {}: {}", segment.id, e);
        }
    };
    
    // Limit mapping size to actual file size to avoid SIGBUS
    let map_size = file_size.min(SEGMENT_SIZE);
    
    debug!(
        "Mapping cold file (fd={}) for segment {} to temporary location (file_size={}, map_size={})",
        fd, segment.id, file_size, map_size
    );
    let temp_addr = unsafe {
        mmap(
            std::ptr::null_mut(),
            map_size,  // Use actual file size, not SEGMENT_SIZE
            PROT_READ,
            MAP_PRIVATE,
            fd,
            0,
        )
    };

    if temp_addr == libc::MAP_FAILED {
        let err = io::Error::last_os_error();
        error!(
            "Failed to mmap cold file for segment {}: {}",
            segment.id, err
        );
        unsafe { close(fd) };
        // tiered_guard will be dropped automatically, releasing the lock
        segment.set_cold();
        panic!(
            "Failed to mmap cold file for segment {}: {}",
            segment.id, err
        );
    }
    
    debug!(
        "Temporary mapping created at {:#x} for segment {} (file_size={}, map_size={})",
        temp_addr as usize, segment.id, file_size, map_size
    );

    // Always scan the file to find the actual valid data boundary
    let temp_start = temp_addr as usize;
    let scanned_boundary = find_append_header(temp_start, map_size);
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

    let _locks = cell_locking::lock_all_cells_in_segment(segment, chunk, temp_entry_iter, true).unwrap();

    let segment_addr = segment.addr;

    // Copy data from file (up to map_size) and zero-fill the rest if needed
    let copy_size = scanned_size.min(map_size);
    unsafe {
        ptr::copy_nonoverlapping(
            temp_addr as *const u8,
            segment_addr as *mut u8,
            copy_size,
        );
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
    segment.append_header.store(restored_append_header, Ordering::Release);
    debug!(
        "Restored append_header for segment {} to {} (offset {}) based on scanned file boundary",
        segment.id, restored_append_header, scanned_size
    );

    debug!("Data copied successfully for segment {}", segment.id);

    // Step 9: Mark as hot and close file descriptor (update tiered_lock)
    unsafe { close(fd) };
    segment.set_hot();
    debug!(
        "Marked segment {} as HOT (addr {:#x}) after promotion and closed fd",
        segment.id, segment.addr
    );

    // Step 10: Set reference bit
    segment.mark_referenced();

    // Locks are automatically dropped when _cell_locks goes out of scope

    // Step 11: Unmap temporary file
    unsafe { munmap(temp_addr, map_size) };
    debug!("Unmapped temporary mapping for segment {}", segment.id);
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
