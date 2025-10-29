/// Segment-level reference bit tracking using mprotect + SIGSEGV
/// 
/// This module implements zero-overhead memory access tracking by:
/// 1. Setting entire segments to PROT_NONE when CLOCK clears reference bits
/// 2. Catching SIGSEGV/SIGBUS on first access to protected segments
/// 3. Setting reference bit and re-enabling access (PROT_READ|PROT_WRITE)
/// 4. CLOCK re-arms segments when sweeping by setting them back to PROT_NONE
/// 
/// Granularity: One bit per segment (8MB), matching natural data structure
/// Overhead: One signal + syscall on first access after arming, then zero-cost

use crate::ram::chunk::{chunk_and_segment_from_addr, get_segment_for_fault};
use crate::ram::segs::SEGMENT_SIZE;
use libc::{PROT_READ, PROT_WRITE, PROT_NONE};
use std::sync::atomic::{AtomicBool, Ordering};

static SIGNAL_HANDLER_INSTALLED: AtomicBool = AtomicBool::new(false);

/// Install SIGSEGV and SIGBUS handlers for segment fault tracking
/// Safe to call multiple times (only installs once)
pub fn install_fault_handlers() {
    if SIGNAL_HANDLER_INSTALLED.swap(true, Ordering::AcqRel) {
        return; // Already installed
    }
    
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handle_segfault as usize;
        sa.sa_flags = libc::SA_SIGINFO | libc::SA_RESTART;
        libc::sigemptyset(&mut sa.sa_mask as *mut libc::sigset_t);
        
        // Install handler for SIGSEGV
        if libc::sigaction(libc::SIGSEGV, &sa, std::ptr::null_mut()) != 0 {
            panic!("Failed to install SIGSEGV handler");
        }
        
        // Install handler for SIGBUS (can happen on some systems)
        if libc::sigaction(libc::SIGBUS, &sa, std::ptr::null_mut()) != 0 {
            panic!("Failed to install SIGBUS handler");
        }
    }
    
    info!("Segment fault handlers installed for reference bit tracking");
}

/// Signal handler for SIGSEGV/SIGBUS
/// 
/// SAFETY: This function must be signal-safe:
/// - No memory allocation
/// - No locks (except lock-free atomics)
/// - No I/O operations
/// - Only async-signal-safe functions
extern "C" fn handle_segfault(
    _sig: libc::c_int,
    info: *mut libc::siginfo_t,
    _context: *mut libc::c_void,
) {
    unsafe {
        // Get the faulting address
        let fault_addr = (*info).si_addr() as usize;
        
        // Try to resolve to chunk and segment
        if let Some((chunk_id, segment_id)) = chunk_and_segment_from_addr(fault_addr) {
            // Get the segment
            if let Some(segment) = get_segment_for_fault(chunk_id, segment_id) {
                // Check if this segment is protected
                if segment.protected.load(Ordering::Relaxed) {
                    // Set reference bit
                    segment.mark_referenced();
                    
                    // Clear protected flag
                    segment.protected.store(false, Ordering::Release);
                    
                    // Re-enable access to the entire segment
                    let segment_addr = segment.addr;
                    let result = libc::mprotect(
                        segment_addr as *mut libc::c_void,
                        SEGMENT_SIZE,
                        PROT_READ | PROT_WRITE,
                    );
                    
                    if result == 0 {
                        // Success - return to retry the faulting instruction
                        return;
                    }
                }
                
                // If we get here, either not protected or mprotect failed
                // Fall through to crash
            }
        }
        
        // If we get here, this is a real segfault, not one of ours
        // Re-raise with default handler
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = libc::SIG_DFL;
        libc::sigaction(libc::SIGSEGV, &sa, std::ptr::null_mut());
        libc::raise(libc::SIGSEGV);
    }
}

/// Protect a segment (set to PROT_NONE)
/// Called by CLOCK when clearing reference bits
/// Returns true if successfully protected
pub fn protect_segment(segment_addr: usize) -> Result<(), String> {
    unsafe {
        let result = libc::mprotect(
            segment_addr as *mut libc::c_void,
            SEGMENT_SIZE,
            PROT_NONE,
        );
        
        if result == 0 {
            Ok(())
        } else {
            Err(format!(
                "mprotect(PROT_NONE) failed for segment addr {:#x}: errno {}",
                segment_addr,
                *libc::__errno_location()
            ))
        }
    }
}

/// Unprotect a segment (set to PROT_READ|PROT_WRITE)
/// Called when manually accessing a protected segment
pub fn unprotect_segment(segment_addr: usize) -> Result<(), String> {
    unsafe {
        let result = libc::mprotect(
            segment_addr as *mut libc::c_void,
            SEGMENT_SIZE,
            PROT_READ | PROT_WRITE,
        );
        
        if result == 0 {
            Ok(())
        } else {
            Err(format!(
                "mprotect(PROT_READ|PROT_WRITE) failed for segment addr {:#x}: errno {}",
                segment_addr,
                *libc::__errno_location()
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signal_handler_installation() {
        // Test that we can install handlers without crashing
        install_fault_handlers();
        
        // Installing again should be idempotent
        install_fault_handlers();
        
        info!("Signal handlers installed successfully");
    }
    
    // NOTE: Full integration tests for segment fault tracking are in
    // src/ram/tiered/tests.rs where we have proper chunk/segment setup
}
