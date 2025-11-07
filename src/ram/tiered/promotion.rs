use crate::ram::chunk::Chunk;
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
        panic!();
    }

    // Step 5: Get backup file path and open it
    let backup_path = match segment.backup_file_name.as_ref() {
        Some(backup_path) => backup_path,
        None => {
            error!("Segment {} has no backup file path", segment.id);
            panic!();
        }
    };

    let path_cstr = match std::ffi::CString::new(backup_path.as_str()) {
        Ok(path_cstr) => path_cstr,
        Err(e) => {
            error!("Invalid path: {}", e);
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
        panic!();
    }

    // Step 6: mmap file to a TEMPORARY location (NOT the segment address)
    // This allows us to scan the data without corrupting the existing segment
    debug!(
        "Mapping cold file (fd={}) for segment {} to temporary location",
        fd, segment.id
    );
    let temp_addr = unsafe {
        mmap(
            std::ptr::null_mut(),
            SEGMENT_SIZE,
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
        panic!(
            "Failed to mmap cold file for segment {}: {}",
            segment.id, err
        );
    }

    debug!(
        "Temporary mapping created at {:#x} for segment {}",
        temp_addr as usize, segment.id
    );

    // Create an entry iterator for the temporary mapping
    let temp_start = temp_addr as usize;

    let temp_entry_iter = SegmentEntryIter {
        bound: SEGMENT_SIZE, // Scan to the end of the temporary mapping
        cursor: temp_start,
    };

    // Step 6-7: Lock all cells in the segment
    let _locks = cell_locking::lock_all_cells_in_segment(segment, chunk, temp_entry_iter, true).unwrap();

    // Handle result and cleanup
    // Step 8: Copy data from temporary mapping to segment address
    // All cells are now locked, safe to overwrite segment memory
    debug!(
        "Copying data from temporary mapping to segment {} address",
        segment.id
    );
    let segment_addr = segment.addr;

    // Ensure segment memory is writable
    let mprotect_result = unsafe {
        libc::mprotect(
            segment_addr as *mut c_void,
            SEGMENT_SIZE,
            PROT_READ | PROT_WRITE,
        )
    };
    if mprotect_result != 0 {
        let err = io::Error::last_os_error();
        error!("Failed to make segment {} writable: {}", segment.id, err);
        unsafe { munmap(temp_addr, SEGMENT_SIZE) };
        panic!("Failed to make segment {} writable: {}", segment.id, err);
    }

    // Copy full segment
    unsafe {
        ptr::copy_nonoverlapping(
            temp_addr as *const u8,
            segment_addr as *mut u8,
            SEGMENT_SIZE,
        );
    }

    debug!("Data copied successfully for segment {}", segment.id);

    // Step 9: Mark as hot and close file descriptor (update tiered_lock)
    unsafe { close(fd) };
    segment.set_hot();
    debug!("Marked segment {} as hot and closed fd", segment.id);

    // Step 10: Set reference bit
    segment.mark_referenced();

    // Locks are automatically dropped when _cell_locks goes out of scope

    // Step 11: Unmap temporary file
    unsafe { munmap(temp_addr, SEGMENT_SIZE) };
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
