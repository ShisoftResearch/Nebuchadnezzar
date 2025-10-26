use crate::ram::chunk::Chunk;
use crate::ram::segs::{Segment, SEGMENT_SIZE};
use libc::{c_void, close, mmap, MAP_ANONYMOUS, MAP_FIXED, MAP_PRIVATE, PROT_READ, PROT_WRITE};
use std::io;
use std::ptr;
use std::sync::atomic::Ordering;
use std::thread;

/// Promote a cold segment to hot (anonymous memory)
/// 
/// **CRITICAL**: This function uses atomic state to serialize promotions and prevent 
/// concurrent access during the "empty window" created by MAP_FIXED remapping.
/// 
/// Why protection is required:
/// - MAP_FIXED with MAP_ANONYMOUS creates a new zero-filled anonymous mapping
/// - Between the mmap call and memcpy completion, the segment contains zeros
/// - The promoting flag prevents concurrent promotions and signals readers to wait
/// - location_for_read() checks is_cold() OR promoting and blocks/waits
/// 
/// Process:
/// 1. Set promoting flag (serializes promotions, signals readers)
/// 2. Wait for active references to drain
/// 3. Copy data to temp buffer (while file is still mapped)
/// 4. Remap as anonymous with MAP_FIXED (creates empty window - protected by promoting flag!)
/// 5. Copy data back to anonymous mapping (fills empty window)
/// 6. Close file descriptor and mark as hot
/// 7. Clear promoting flag (allow reads again)
///
/// Note: Individual cell locking was considered but WordMap doesn't expose iteration.
/// This approach is simpler and equally safe - the promoting flag prevents all access.
pub fn promote_segment(segment: &Segment, _chunk: &Chunk) -> Result<(), io::Error> {
    // Sanity check: don't promote if already hot
    if segment.is_hot() {
        warn!("Attempted to promote already-hot segment {}", segment.id);
        return Ok(());
    }
    
    // Try to set promoting flag - only one thread should promote at a time
    if segment.promoting.compare_exchange(
        false,
        true,
        Ordering::AcqRel,
        Ordering::Acquire
    ).is_err() {
        // Another thread is already promoting - wait for it to complete
        debug!("Segment {} already being promoted by another thread, waiting...", segment.id);
        while segment.promoting.load(Ordering::Acquire) {
            thread::yield_now();
        }
        // Check if it's now hot
        if segment.is_hot() {
            return Ok(());
        }
        // Still cold? This shouldn't happen but be safe
        warn!("Segment {} still cold after promotion by another thread", segment.id);
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "Promotion race condition detected",
        ));
    }
    
    debug!("Promoting segment {} to hot storage", segment.id);
    
    let segment_start = segment.addr;
    
    // Step 1: Wait for all active references to drain
    // This ensures no one is actively dereferencing pointers into this segment
    debug!("Waiting for active references to drain for segment {}", segment.id);
    while !segment.no_references() {
        thread::yield_now();
    }
    
    // Step 2: Copy data to temp buffer BEFORE unmapping
    // The file is still mapped at this point, so reads are safe
    debug!("Copying segment {} data to temp buffer", segment.id);
    let mut data = vec![0u8; SEGMENT_SIZE];
    unsafe {
        ptr::copy_nonoverlapping(
            segment_start as *const u8,
            data.as_mut_ptr(),
            SEGMENT_SIZE,
        );
    }
    
    // Step 3: Remap as anonymous with MAP_FIXED
    // ⚠️ CRITICAL SECTION: This creates an "empty window"
    // The segment now points to zeros until step 4 completes
    // The promoting flag prevents concurrent access during this window
    debug!(
        "Remapping segment {} as anonymous (empty window protected by promoting flag)",
        segment.id
    );
    let result = unsafe {
        mmap(
            segment_start as *mut c_void,
            SEGMENT_SIZE,
            PROT_READ | PROT_WRITE,
            MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED,
            -1,
            0,
        )
    };
    
    if result == libc::MAP_FAILED {
        // Failed to remap - clear promoting flag and return error
        segment.promoting.store(false, Ordering::Release);
        return Err(io::Error::last_os_error());
    }
    
    // Verify MAP_FIXED returned the same address
    if result as usize != segment_start {
        error!(
            "MAP_FIXED returned different address during promotion: expected {}, got {}",
            segment_start, result as usize
        );
        segment.promoting.store(false, Ordering::Release);
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "MAP_FIXED returned unexpected address during promotion",
        ));
    }
    
    // Step 4: Copy data back to new anonymous mapping
    // This fills the empty window - segment now contains correct data again
    debug!("Copying data back to segment {} anonymous mapping", segment.id);
    unsafe {
        ptr::copy_nonoverlapping(
            data.as_ptr(),
            segment_start as *mut u8,
            SEGMENT_SIZE,
        );
    }
    
    // Step 5: Close file descriptor and mark segment as hot
    let fd = segment.cold_file_fd.load(Ordering::Acquire);
    if fd >= 0 {
        unsafe { close(fd) };
    }
    segment.cold_file_fd.store(-1, Ordering::Release);
    
    // Step 6: Clear promoting flag - segment is now hot and accessible
    segment.promoting.store(false, Ordering::Release);
    
    info!("Successfully promoted segment {} to hot storage", segment.id);
    
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_promotion_basics() {
        // Basic promotion test
        // Full integration tests in tiered/tests.rs
    }
}
