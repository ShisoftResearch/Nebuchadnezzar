/// Test to verify that segment allocation maintains 8-byte alignment
/// even under concurrent stress conditions
use crate::ram::segs::{Segment, SegmentAllocator, SEGMENT_SIZE};
use std::sync::Arc;
use std::thread;

#[test]
fn test_segment_initial_alignment() {
    println!("Testing that newly allocated segments have 8-byte aligned addresses");
    println!();

    let allocator = Arc::new(SegmentAllocator::new(0, SEGMENT_SIZE * 100));

    // Allocate several segments and verify alignment
    for i in 0..10 {
        let seg = allocator
            .alloc_seg(&None, &None)
            .expect("Failed to allocate segment");
        println!(
            "Segment {} address: 0x{:016x} (8-byte aligned: {})",
            i,
            seg.addr,
            seg.addr % 8 == 0
        );
        assert_eq!(seg.addr % 8, 0, "Segment address must be 8-byte aligned");
        assert_eq!(seg.bound % 8, 0, "Segment bound must be 8-byte aligned");
        assert_eq!(
            seg.append_header.load(std::sync::atomic::Ordering::Acquire) % 8,
            0,
            "Segment append_header must be 8-byte aligned"
        );
    }
    println!("✅ All segments are properly aligned");
}

#[test]
fn test_segment_try_acquire_alignment() {
    println!("Testing that Segment::try_acquire maintains alignment with REALISTIC sizes");
    println!("(sizes that the actual system would use - all 8-byte aligned)");
    println!();

    let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 10);
    let seg = allocator
        .alloc_seg(&None, &None)
        .expect("Failed to allocate segment");

    println!("Initial segment address: 0x{:016x}", seg.addr);
    println!();

    // Use REALISTIC sizes that the actual system uses - all 8-byte aligned
    // ENTRY_HEAD_SIZE (8) + entry_body_size (8-byte aligned) = always 8-byte aligned
    let sizes = vec![
        8,    // Minimum (just entry header)
        16,   // Small entry
        24,   // Small entry
        32,   // TOMBSTONE_ENTRY_SIZE (32 + 8 = 40)
        40,   // TOMBSTONE_ENTRY_SIZE
        48,   // Medium entry
        64,   // Medium entry
        256,  // Larger entry
        512,  // Large entry
        1024, // Very large entry
        4096, // Max entry
    ];

    for size in sizes {
        assert_eq!(size % 8, 0, "Test size {} must be 8-byte aligned", size);
        let addr = seg
            .try_acquire(size)
            .expect(&format!("Failed to acquire {} bytes", size));
        println!(
            "Acquired {} bytes at 0x{:016x} (8-byte aligned: {})",
            size,
            addr,
            addr % 8 == 0
        );
        assert_eq!(
            addr % 8,
            0,
            "Acquired address must be 8-byte aligned (size={})",
            size
        );
    }
    println!();
    println!("✅ All acquired addresses are properly aligned");
}

#[test]
fn test_segment_concurrent_allocation_alignment() {
    println!("Testing segment allocation alignment under concurrent stress");
    println!();

    let allocator = Arc::new(SegmentAllocator::new(0, SEGMENT_SIZE * 1000));
    let num_threads = 10;
    let allocations_per_thread = 50;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let allocator = Arc::clone(&allocator);
            thread::spawn(move || {
                let mut misaligned_count = 0;
                for i in 0..allocations_per_thread {
                    if let Some(seg) = allocator.alloc_seg(&None, &None) {
                        if seg.addr % 8 != 0 {
                            eprintln!(
                                "❌ Thread {} allocation {} got misaligned address: 0x{:016x}",
                                thread_id, i, seg.addr
                            );
                            misaligned_count += 1;
                        }
                    }
                }
                misaligned_count
            })
        })
        .collect();

    let mut total_misaligned = 0;
    for (thread_id, handle) in handles.into_iter().enumerate() {
        let misaligned = handle.join().expect("Thread panicked");
        if misaligned > 0 {
            println!(
                "❌ Thread {} had {} misaligned allocations",
                thread_id, misaligned
            );
        }
        total_misaligned += misaligned;
    }

    println!();
    println!(
        "Total allocations: {}",
        num_threads * allocations_per_thread
    );
    println!("Misaligned allocations: {}", total_misaligned);

    assert_eq!(
        total_misaligned, 0,
        "Found {} misaligned segment allocations",
        total_misaligned
    );
    println!("✅ All concurrent segment allocations maintained alignment");
}

#[test]
fn test_segment_try_acquire_concurrent_alignment() {
    println!("Testing Segment::try_acquire alignment under concurrent stress");
    println!();

    let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 10);
    let seg = Arc::new(
        allocator
            .alloc_seg(&None, &None)
            .expect("Failed to allocate segment"),
    );

    let num_threads = 8;
    let tries_per_thread = 100;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let seg = Arc::clone(&seg);
            thread::spawn(move || {
                let mut acquired = Vec::new();
                let mut misaligned_count = 0;

                // Try to acquire various sizes concurrently
                for i in 0..tries_per_thread {
                    let size = ((i % 10) + 1) * 8; // Sizes: 8, 16, 24, ..., 80
                    if let Some(addr) = seg.try_acquire(size) {
                        if addr % 8 != 0 {
                            eprintln!(
                                "❌ Thread {} try {} got misaligned address: 0x{:016x} (size={})",
                                thread_id, i, addr, size
                            );
                            misaligned_count += 1;
                        }
                        acquired.push((addr, size));
                    } else {
                        // Segment is full
                        break;
                    }
                }
                (acquired.len(), misaligned_count)
            })
        })
        .collect();

    let mut total_acquired = 0;
    let mut total_misaligned = 0;

    for (thread_id, handle) in handles.into_iter().enumerate() {
        let (acquired, misaligned) = handle.join().expect("Thread panicked");
        println!(
            "Thread {} acquired {} addresses ({} misaligned)",
            thread_id, acquired, misaligned
        );
        total_acquired += acquired;
        total_misaligned += misaligned;
    }

    println!();
    println!("Total addresses acquired: {}", total_acquired);
    println!("Misaligned addresses: {}", total_misaligned);

    assert_eq!(
        total_misaligned, 0,
        "Found {} misaligned addresses from try_acquire",
        total_misaligned
    );
    println!("✅ All concurrent try_acquire calls maintained alignment");
}

#[test]
fn test_segment_append_header_increments() {
    println!("Testing that append_header increments maintain alignment");
    println!("NOTE: The system should ONLY call try_acquire with 8-byte aligned sizes");
    println!();

    let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 10);
    let seg = allocator
        .alloc_seg(&None, &None)
        .expect("Failed to allocate segment");

    println!("Testing REALISTIC allocation sizes (8-byte aligned):");
    println!();

    // Use ONLY 8-byte aligned sizes as the actual system does
    let sizes = vec![
        (8, "8 bytes (entry header only)"),
        (16, "16 bytes (small cell)"),
        (24, "24 bytes (small cell)"),
        (32, "32 bytes (medium cell)"),
        (40, "40 bytes (TOMBSTONE_ENTRY_SIZE)"),
        (48, "48 bytes (medium cell)"),
        (1000, "1000 bytes (large cell - 8-aligned)"),
        (1024, "1024 bytes (large cell)"),
    ];

    for (size, description) in sizes {
        assert_eq!(size % 8, 0, "Test size must be 8-byte aligned");

        let addr_before = seg.append_header.load(std::sync::atomic::Ordering::Acquire);

        if let Some(addr) = seg.try_acquire(size) {
            let addr_after = seg.append_header.load(std::sync::atomic::Ordering::Acquire);

            println!("{}", description);
            println!(
                "  Before:   0x{:016x} (offset: {})",
                addr_before,
                addr_before % 8
            );
            println!("  Acquired: 0x{:016x} (offset: {})", addr, addr % 8);
            println!(
                "  After:    0x{:016x} (offset: {})",
                addr_after,
                addr_after % 8
            );
            println!("  Increment: {} bytes", addr_after - addr_before);

            assert_eq!(
                addr, addr_before,
                "Acquired address should match pre-increment header"
            );
            assert_eq!(addr % 8, 0, "Acquired address must be 8-byte aligned");
            assert_eq!(
                addr_after % 8,
                0,
                "After increment, header must remain 8-byte aligned"
            );
            assert_eq!(
                addr_after - addr_before,
                size as usize,
                "Increment should match requested size"
            );
            println!("  ✓");
            println!();
        }
    }

    println!("✅ append_header maintains 8-byte alignment through all increments");
    println!();
    println!("=== IMPORTANT ===");
    println!("If try_acquire is ever called with NON-8-byte-aligned sizes,");
    println!("the alignment will break! The cell.rs code ensures sizes are");
    println!("always 8-byte aligned before calling try_acquire.");
}

#[test]
fn test_detect_address_corruption_pattern() {
    println!("Testing detection of corruption patterns like 0xE6 endings");
    println!();

    // These are patterns seen in actual crashes
    let corrupted_addresses = vec![
        0x6c395a4000e6_usize,   // First crash address
        0x6c51974000e6_usize,   // Second crash address
        0x0000_0000_00e6_usize, // Pattern: offset 6
        0x0000_0000_01e6_usize, // Pattern: offset 6
        0x0000_0000_10e6_usize, // Pattern: offset 6
    ];

    println!("Analyzing crash address patterns:");
    println!();

    for addr in corrupted_addresses {
        let offset_2 = addr % 2;
        let offset_4 = addr % 4;
        let offset_8 = addr % 8;

        println!("Address: 0x{:016x}", addr);
        println!("  Last byte: 0x{:02x} ({})", addr & 0xFF, addr & 0xFF);
        println!(
            "  2-byte alignment: offset={} {}",
            offset_2,
            if offset_2 == 0 { "✓" } else { "✗" }
        );
        println!(
            "  4-byte alignment: offset={} {}",
            offset_4,
            if offset_4 == 0 { "✓" } else { "✗" }
        );
        println!(
            "  8-byte alignment: offset={} {}",
            offset_8,
            if offset_8 == 0 { "✓" } else { "✗" }
        );

        // All crash addresses should have the same pattern
        assert_eq!(offset_8, 6, "Crash addresses have 8-byte offset of 6");
        assert_eq!(offset_4, 2, "Crash addresses have 4-byte offset of 2");
        assert_eq!(offset_2, 0, "Crash addresses have 2-byte offset of 0");

        // Show what they SHOULD be
        let should_be_rounded_down = addr & !7; // Clear last 3 bits
        let should_be_rounded_up = (addr & !7) + 8; // Clear last 3 bits and add 8

        println!(
            "  Should be (round down): 0x{:016x}",
            should_be_rounded_down
        );
        println!("  Should be (round up):   0x{:016x}", should_be_rounded_up);
        println!();
    }

    println!("=== PATTERN IDENTIFIED ===");
    println!("All crash addresses end in 0xE6 (230 decimal)");
    println!("This is consistently 6 bytes off from an 8-byte boundary");
    println!();
    println!("Hypothesis: Something is adding 6 to valid addresses, OR");
    println!("            Reading from 6 bytes before where pointer points");
}
