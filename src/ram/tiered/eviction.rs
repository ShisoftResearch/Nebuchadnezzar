use crate::ram::chunk::Chunk;
use crate::ram::segs::{madvise_cold, madvise_free, Segment, SEGMENT_SIZE};
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
    
    // Step 3: Aggressively free physical pages from hot anonymous memory BEFORE remapping
    // This immediately releases the physical pages while the memory is still anonymous
    // After we remap to file-backed, we'll use a gentler MADV_COLD approach
    unsafe {
        madvise_free(segment.addr, SEGMENT_SIZE);
    }
    debug!("Freed physical pages for hot segment {} before file-backed remapping", segment.id);
    
    // Step 4: Open backup file read-only
    
    let path_cstr = CString::new(backup_path.as_str()).map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid path: {}", e))
    })?;
    
    let fd = unsafe { open(path_cstr.as_ptr(), O_RDONLY) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    
    // Step 5: mmap with MAP_FIXED at the original address
    // This atomically replaces the anonymous mapping with file-backed mapping
    // The data at segment.addr remains unchanged (same bytes)
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
    
    // Step 6: Mark file-backed pages as COLD (low priority for eviction)
    // MADV_COLD cooperatively hints to the kernel that these pages should be evicted
    // first under memory pressure, but doesn't force immediate eviction.
    // This gives the kernel flexibility to keep some frequently-accessed cold pages
    // in the page cache while still prioritizing their eviction.
    unsafe {
        madvise_cold(segment.addr, SEGMENT_SIZE);
    }
    debug!("Marked file-backed segment {} as cold (low priority for page cache)", segment.id);
    
    // Step 7: Store file descriptor (marks segment as cold)
    segment.cold_file_fd.store(fd, Ordering::Release);
    
    // Step 8: Protect the segment with mprotect(PROT_NONE) to track future accesses
    // Even though it's file-backed now, mprotect still works!
    // This allows us to detect when cold segments are accessed and promote them
    if let Err(e) = crate::ram::tiered::page_fault_tracker::protect_segment(segment.addr) {
        warn!("Failed to protect evicted segment {}: {}", segment.id, e);
    } else {
        debug!("Protected evicted segment {} with mprotect(PROT_NONE)", segment.id);
    }
    
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

