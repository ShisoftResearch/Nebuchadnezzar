/// Segment-level reference bit tracking using mprotect + SIGSEGV
///
/// This module implements zero-overhead memory access tracking by:
/// 1. CLOCK clears reference bit → calls protect_segment() to set PROT_NONE
/// 2. First access triggers SIGSEGV → handler sets reference bit + unprotects
/// 3. Subsequent accesses have zero overhead until CLOCK re-arms
///
/// Design:
/// - Segment granularity (8MB): simpler than page-level, lower syscall overhead
/// - No extra state tracking: mprotect itself tracks protection state
/// - Signal-safe handler: lock-free, no allocation, minimal atomic operations
///
/// Overhead: One signal + one syscall (~1-2μs) on first access after protection
///
/// This feature can be disabled via Cargo.toml to use direct reference marking instead.
#[cfg(feature = "page_fault_tracking")]
use crate::ram::segs::SEGMENT_SIZE;
#[cfg(feature = "page_fault_tracking")]
use libc::{PROT_NONE, PROT_READ, PROT_WRITE};

#[cfg(feature = "page_fault_tracking")]
use crate::ram::chunk::{chunk_and_segment_from_addr, get_segment_for_fault};
#[cfg(feature = "page_fault_tracking")]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "page_fault_tracking")]
static SIGNAL_HANDLER_INSTALLED: AtomicBool = AtomicBool::new(false);

/// Install SIGSEGV and SIGBUS handlers for segment fault tracking
/// Safe to call multiple times (only installs once)
///
/// If page_fault_tracking feature is disabled, this is a no-op.
#[cfg(feature = "page_fault_tracking")]
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

/// No-op when page_fault_tracking feature is disabled
#[cfg(not(feature = "page_fault_tracking"))]
pub fn install_fault_handlers() {
    debug!("Page fault tracking disabled - using direct reference marking");
}

/// Signal handler for SIGSEGV/SIGBUS
///
/// SAFETY: This function must be signal-safe:
/// - No memory allocation
/// - No locks (except lock-free atomics)
/// - No I/O operations
/// - Only async-signal-safe functions
#[cfg(feature = "page_fault_tracking")]
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
                // Set reference bit
                segment.mark_referenced();

                #[cfg(all(debug_assertions, feature = "debug_page_faults"))]
                {
                    // Log the reference (signal-safe: write to stderr fd directly)
                    // Note: This is expensive but useful for debugging. Only enabled with debug build AND feature flag.
                    let msg = format!(
                        "[PAGE_FAULT] Segment referenced: chunk={}, seg_id={}, hot={}, addr={:#x}\n",
                        chunk_id, segment_id, segment.is_hot(), segment.addr
                    );
                    libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
                }

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

                // If mprotect failed, fall through to crash
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
/// When page_fault_tracking is enabled, uses mprotect
/// When disabled, this is a no-op (reference tracking happens directly on read)
#[cfg(feature = "page_fault_tracking")]
pub fn protect_segment(segment_addr: usize) -> Result<(), String> {
    unsafe {
        let result = libc::mprotect(segment_addr as *mut libc::c_void, SEGMENT_SIZE, PROT_NONE);

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
/// When page_fault_tracking is enabled, uses mprotect
/// When disabled, this is a no-op
#[cfg(feature = "page_fault_tracking")]
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
