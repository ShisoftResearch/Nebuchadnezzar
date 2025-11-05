use crate::ram::chunk::Chunk;
use crate::ram::segs::{madvise_cold, Segment, SEGMENT_SIZE};
use libc::{c_void, mmap, open, MAP_FIXED, MAP_PRIVATE, O_RDONLY, PROT_READ};
use std::ffi::CString;
use std::io;
use std::sync::atomic::Ordering;
use std::thread;

/// Evict a hot segment to cold (file-backed mmap)
/// 
/// This operation is safe without cell-level locking because:
/// - The file contains identical data to what's in memory
/// - MAP_FIXED atomically replaces the mapping
/// - Data at segment.addr remains unchanged from the reader's perspective
/// 
/// Process:
/// 1. Wait for no active references
/// 2. Archive segment to backup file
/// 3. Open backup file read-only
/// 4. mmap with MAP_FIXED (replaces anonymous mapping with file-backed)
/// 5. Store file descriptor (marks segment as cold)
pub fn evict_segment(segment: &Segment, _chunk: &Chunk) -> Result<(), io::Error> {
    debug!("evict_segment called for segment {}, is_cold={}, is_hot={}", 
           segment.id, segment.is_cold(), segment.is_hot());
    
    // Sanity check: don't evict if already cold
    if segment.is_cold() {
        warn!("Attempted to evict already-cold segment {}", segment.id);
        return Ok(());
    }
    
    debug!("Evicting segment {} to cold storage", segment.id);
    
    // Step 1: Wait for no active references
    // This ensures no one is actively dereferencing pointers into this segment
    while !segment.no_references() {
        thread::yield_now();
    }
    
    // Step 2: Ensure backup file exists
    // Try to archive - if it returns false, the backup might already exist
    let archived = segment.archive()?;
    debug!("Archive result for segment {}: archived={}", segment.id, archived);
    
    // Get backup path and verify it exists
    let backup_path = segment.backup_file_name.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Segment {} has no backup file path", segment.id),
        )
    })?;
    
    // If archive() returned false but backup exists (e.g., from previous write),
    // that's fine - we can still proceed with eviction
    if !archived && !std::path::Path::new(backup_path).exists() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Segment {} backup file does not exist and archive failed", segment.id),
        ));
    }
    
    // Step 3: Open backup file read-only
    // IMPORTANT: Do NOT call madvise_free() before mmap!
    // Calling MADV_DONTNEED on anonymous memory causes the kernel to zero pages,
    // which would corrupt data if accessed during the mmap transition.
    // Instead, we'll apply madvise AFTER the file-backed mapping is established.
    
    let path_cstr = CString::new(backup_path.as_str()).map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid path: {}", e))
    })?;
    
    let fd = unsafe { open(path_cstr.as_ptr(), O_RDONLY) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    
    // Step 4: mmap with MAP_FIXED at the original address
    // This atomically replaces the anonymous mapping with file-backed mapping
    // MAP_FIXED ensures atomic replacement: readers see either old data (anonymous)
    // or new data (file-backed), never zeros or corrupted data.
    let result = unsafe {
        mmap(
            segment.addr as *mut c_void,
            SEGMENT_SIZE,
            PROT_READ,
            MAP_PRIVATE | MAP_FIXED,
            fd,
            0,
        )
    };
    
    if result == libc::MAP_FAILED {
        // Failed to mmap - close fd and return error
        unsafe { libc::close(fd) };
        return Err(io::Error::last_os_error());
    }
    
    // Verify mmap returned the same address (MAP_FIXED should guarantee this)
    if result as usize != segment.addr {
        error!(
            "MAP_FIXED returned different address: expected {}, got {}",
            segment.addr, result as usize
        );
        unsafe { libc::close(fd) };
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "MAP_FIXED returned unexpected address",
        ));
    }
    
    debug!("Atomically replaced anonymous mapping with file-backed mapping for segment {}", segment.id);
    
    // Step 5: Store file descriptor (marks segment as cold)
    // CRITICAL: This MUST happen BEFORE mprotect(PROT_NONE)!
    // 
    // Ordering rationale:
    // 1. mmap(MAP_FIXED) established file-backed mapping (completed above)
    // 2. Store fd → segment is now "cold" (this step)
    // 3. mprotect(PROT_NONE) → segment is protected (next step)
    // 4. madvise_cold() → hint to evict pages (later step)
    //
    // If we protect (step 3) before storing fd (step 2):
    // - SIGSEGV occurs between steps
    // - Handler sees cold_file_fd=-1 (thinks segment is hot)
    // - But memory is actually file-backed → mismatch → handler confusion → deadlock!
    //
    // Correct order prevents this race:
    // - Store fd first → handler always sees consistent state
    // - Then protect → handler knows segment is cold and can handle properly
    segment.cold_file_fd.store(fd, Ordering::Release);
    debug!("Marked segment {} as cold (fd={})", segment.id, fd);
    
    // Step 6: Protect the segment with mprotect(PROT_NONE)
    // Now that cold_file_fd is set, the handler will correctly detect this as a cold segment
    if let Err(e) = crate::ram::tiered::page_fault_tracker::protect_segment(segment.addr) {
        warn!("Failed to protect evicted segment {}: {}", segment.id, e);
        // If we can't protect, close fd and revert to hot
        segment.cold_file_fd.store(-1, Ordering::Release);
        unsafe { libc::close(fd) };
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to protect segment during eviction: {}", e),
        ));
    }
    debug!("Protected evicted segment {} with mprotect(PROT_NONE)", segment.id);
    
    // Step 7: Mark file-backed pages as COLD (low priority for eviction)
    // NOW it's safe to call madvise_cold because:
    // - The segment is protected (PROT_NONE) - no unsynchronized access possible
    // - The segment is marked as cold (fd stored) - handler can detect and promote
    // - File-backed mapping is established - faulting back in reads from file
    //
    // MADV_COLD cooperatively hints to the kernel that these pages should be evicted
    // first under memory pressure. For file-backed mappings, faulting back in means
    // reading from the file, not zeroing pages (unlike anonymous memory).
    // This gives the kernel flexibility to keep some frequently-accessed cold pages
    // in the page cache while still prioritizing their eviction.
    unsafe {
        madvise_cold(segment.addr, SEGMENT_SIZE);
    }
    debug!("Marked file-backed segment {} as cold (low priority for page cache)", segment.id);
    
    info!(
        "Successfully evicted segment {} to cold storage (fd: {})",
        segment.id, fd
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

