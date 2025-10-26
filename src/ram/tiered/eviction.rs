use crate::ram::chunk::Chunk;
use crate::ram::segs::{Segment, SEGMENT_SIZE};
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
    
    // Step 2: Archive segment to backup file
    // This writes the current memory contents to disk
    let archived = segment.archive()?;
    if !archived {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to archive segment {} for eviction", segment.id),
        ));
    }
    
    // Step 3: Open backup file read-only
    let backup_path = segment.backup_file_name.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Segment {} has no backup file path", segment.id),
        )
    })?;
    
    let path_cstr = CString::new(backup_path.as_str()).map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid path: {}", e))
    })?;
    
    let fd = unsafe { open(path_cstr.as_ptr(), O_RDONLY) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    
    // Step 4: mmap with MAP_FIXED at the original address
    // This atomically replaces the anonymous mapping with file-backed mapping
    // The data at segment.addr remains unchanged (same bytes)
    // Physical memory pages are freed automatically by the kernel
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
    
    // Step 5: Store file descriptor (marks segment as cold)
    segment.cold_file_fd.store(fd, Ordering::Release);
    
    info!(
        "Successfully evicted segment {} to cold storage (fd: {})",
        segment.id, fd
    );
    
    Ok(())
}

/// Evict multiple segments in batch
/// 
/// This is useful for evicting several segments at once to reach a memory target.
/// Each segment is evicted independently - if one fails, others may still succeed.
pub fn evict_segments(
    segments: &[&Segment],
    chunk: &Chunk,
) -> Result<usize, Vec<io::Error>> {
    let mut evicted_count = 0;
    let mut errors = Vec::new();
    
    for segment in segments {
        match evict_segment(segment, chunk) {
            Ok(()) => evicted_count += 1,
            Err(e) => {
                warn!("Failed to evict segment {}: {}", segment.id, e);
                errors.push(e);
            }
        }
    }
    
    if errors.is_empty() {
        Ok(evicted_count)
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_eviction_basics() {
        // Basic eviction test
        // Full integration tests in tiered/tests.rs
    }
}

