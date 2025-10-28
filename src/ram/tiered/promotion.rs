use crate::ram::chunk::Chunk;
use crate::ram::cell::cell_header_from_entry_content_addr;
use crate::ram::entry::EntryType;
use crate::ram::segs::{Segment, SEGMENT_SIZE};
use libc::{c_void, close, mmap, MAP_ANONYMOUS, MAP_FIXED, MAP_PRIVATE, PROT_READ, PROT_WRITE};
use std::io;
use std::ptr;
use std::sync::atomic::Ordering;
use std::thread;

/// Promote a cold segment to hot (anonymous memory)
/// 
/// **CRITICAL**: This function locks ALL cells in the segment during promotion to prevent 
/// concurrent access during the "empty window" created by MAP_FIXED remapping.
/// 
/// Why cell locking is required:
/// - MAP_FIXED with MAP_ANONYMOUS creates a new zero-filled anonymous mapping
/// - Between the mmap call and memcpy completion, the segment contains zeros
/// - All cells in the segment must be locked to prevent reads during this window
/// 
/// Process:
/// 1. Scan segment to find all cells (like the cleaner does)
/// 2. Lock all cells iteratively using try_lock (retry those that fail)
/// 3. Copy data to temp buffer (while file is still mapped)
/// 4. Remap as anonymous with MAP_FIXED (creates empty window - cells are locked!)
/// 5. Copy data back to anonymous mapping (fills empty window)
/// 6. Close file descriptor and mark as hot
/// 7. Unlock all cells
///
/// The iterative locking ensures we eventually lock all cells without deadlock.
pub fn promote_segment(segment: &Segment, chunk: &Chunk) -> Result<(), io::Error> {
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
    
    // Step 1: Scan segment to collect all cell hashes (like the cleaner does)
    debug!("Scanning segment {} to find all cells", segment.id);
    let mut cell_hashes: Vec<usize> = Vec::new();
    
    for entry_meta in segment.entry_iter() {
        if entry_meta.entry_header.entry_type == EntryType::CELL {
            let cell_header = cell_header_from_entry_content_addr(entry_meta.body_pos);
            let hash = cell_header.hash as usize;
            cell_hashes.push(hash);
        }
    }
    
    debug!("Found {} cells in segment {} to lock", cell_hashes.len(), segment.id);
    
    // Step 2: Lock all cells iteratively using try_lock
    // Keep trying to lock cells that couldn't be locked until all are locked
    let mut locks: Vec<lightning::map::WordMutexGuard> = Vec::with_capacity(cell_hashes.len());
    let mut unlocked_indices: Vec<usize> = (0..cell_hashes.len()).collect();
    
    while !unlocked_indices.is_empty() {
        let mut still_unlocked = Vec::new();
        
        for &idx in &unlocked_indices {
            let hash = cell_hashes[idx];
            match chunk.cell_index.try_lock(hash as usize) {
                Some(Some(lock)) => {
                    // Successfully locked this cell
                    locks.push(lock);
                }
                Some(None) => {
                    // Couldn't lock (busy or doesn't exist), try again in next iteration
                    still_unlocked.push(idx);
                }
                None => {
                    error!("Cell {} not found in chunk {} index during promotion", hash, chunk.id);
                }
            }
        }
        
        unlocked_indices = still_unlocked;
        
        if !unlocked_indices.is_empty() {
            // Some cells still locked by others, yield and retry
            thread::yield_now();
        }
    }
    
    debug!("Successfully locked all {} cells in segment {}", locks.len(), segment.id);
    
    let segment_start = segment.addr;
    
    // Step 3: Copy data to temp buffer BEFORE unmapping
    // All cells are now locked, safe to copy
    debug!("Copying segment {} data to temp buffer", segment.id);
    let mut data = vec![0u8; SEGMENT_SIZE];
    unsafe {
        ptr::copy_nonoverlapping(
            segment_start as *const u8,
            data.as_mut_ptr(),
            SEGMENT_SIZE,
        );
    }
    
    // Step 4: Remap as anonymous with MAP_FIXED
    // ⚠️ CRITICAL SECTION: This creates an "empty window"
    // The segment now points to zeros until step 5 completes
    // All cells are locked so no concurrent reads can happen
    debug!(
        "Remapping segment {} as anonymous (empty window protected by cell locks)",
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
        // Failed to remap - clear promoting flag and drop locks
        segment.promoting.store(false, Ordering::Release);
        drop(locks);
        return Err(io::Error::last_os_error());
    }
    
    // Verify MAP_FIXED returned the same address
    if result as usize != segment_start {
        error!(
            "MAP_FIXED returned different address during promotion: expected {}, got {}",
            segment_start, result as usize
        );
        segment.promoting.store(false, Ordering::Release);
        drop(locks);
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "MAP_FIXED returned unexpected address during promotion",
        ));
    }
    
    // Step 5: Copy data back to new anonymous mapping
    // This fills the empty window - segment now contains correct data again
    debug!("Copying data back to segment {} anonymous mapping", segment.id);
    unsafe {
        ptr::copy_nonoverlapping(
            data.as_ptr(),
            segment_start as *mut u8,
            SEGMENT_SIZE,
        );
    }
    
    // Step 6: Close file descriptor and mark segment as hot
    let fd = segment.cold_file_fd.load(Ordering::Acquire);
    if fd >= 0 {
        unsafe { close(fd) };
    }
    segment.cold_file_fd.store(-1, Ordering::Release);
    
    // Step 7: Clear promoting flag
    segment.promoting.store(false, Ordering::Release);
    
    // Step 8: Drop all locks - cells are now accessible again
    drop(locks);
    
    info!("Successfully promoted segment {} to hot storage (locked {} cells during promotion)", 
          segment.id, cell_hashes.len());
    
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
