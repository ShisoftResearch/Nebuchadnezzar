/// Test WordMap to see if it corrupts addresses
/// Testing the hypothesis that WordMap might have a 6-byte offset issue

use lightning::map::WordMap;
use std::alloc::System;

#[test]
fn test_wordmap_stores_correct_addresses() {
    println!("Testing if WordMap correctly stores and retrieves addresses");
    println!();
    
    let map: WordMap<System> = WordMap::with_capacity(1024);
    
    // Test various addresses, especially 8-byte aligned ones
    let test_addresses = vec![
        0x0000_7f00_0000_0000_usize,  // 8-byte aligned
        0x0000_7f00_0000_0008_usize,  // 8-byte aligned
        0x0000_7f00_0000_0010_usize,  // 8-byte aligned
        0x0000_7f00_1234_5678_usize,  // 8-byte aligned
        0x0000_6c39_5a40_00e0_usize,  // Similar to crash address (but aligned)
    ];
    
    println!("Storing addresses:");
    for (i, &addr) in test_addresses.iter().enumerate() {
        println!("  Hash {}: storing 0x{:016x}", i, addr);
        if let Some(mut guard) = map.try_insert_locked(i) {
            *guard = addr;
            drop(guard);
        }
    }
    println!();
    
    println!("Retrieving and verifying addresses:");
    let mut corrupted_count = 0;
    for (i, &expected_addr) in test_addresses.iter().enumerate() {
        if let Some(guard) = map.lock(i) {
            let retrieved_addr = *guard;
            let is_correct = retrieved_addr == expected_addr;
            let is_aligned = retrieved_addr % 8 == 0;
            
            println!("  Hash {}: expected 0x{:016x}, got 0x{:016x} {}{}",
                     i, expected_addr, retrieved_addr,
                     if is_correct { "✓" } else { "✗ WRONG!" },
                     if !is_aligned { " (MISALIGNED!)" } else { "" });
            
            if retrieved_addr != expected_addr {
                let diff = if retrieved_addr > expected_addr {
                    retrieved_addr - expected_addr
                } else {
                    expected_addr - retrieved_addr
                };
                println!("    Difference: {} bytes (0x{:x})", diff, diff);
                corrupted_count += 1;
            }
            
            assert_eq!(retrieved_addr, expected_addr, 
                      "WordMap corrupted address! Expected 0x{:016x}, got 0x{:016x}",
                      expected_addr, retrieved_addr);
        } else {
            println!("  Hash {}: NOT FOUND!", i);
            corrupted_count += 1;
        }
    }
    println!();
    
    println!("Summary: {} / {} addresses retrieved correctly",
             test_addresses.len() - corrupted_count, test_addresses.len());
    
    assert_eq!(corrupted_count, 0, "Found {} corrupted addresses in WordMap", corrupted_count);
}

#[test]
fn test_wordmap_concurrent_stress() {
    use std::sync::Arc;
    use std::thread;
    
    println!("Testing WordMap under concurrent stress");
    println!();
    
    let map: Arc<WordMap<System>> = Arc::new(WordMap::with_capacity(10000));
    let num_threads = 10;
    let writes_per_thread = 1000;
    
    // Write phase: concurrent writes
    let write_handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let map = Arc::clone(&map);
            thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let hash = (thread_id * writes_per_thread + i) as usize;
                    // Create an 8-byte aligned address
                    let addr = 0x0000_7f00_0000_0000_usize + (hash * 8);
                    
                    if let Some(mut guard) = map.try_insert_locked(hash) {
                        *guard = addr;
                        drop(guard);
                    }
                }
            })
        })
        .collect();
    
    for handle in write_handles {
        handle.join().expect("Thread panicked");
    }
    
    println!("Wrote {} addresses across {} threads", 
             num_threads * writes_per_thread, num_threads);
    println!();
    
    // Read phase: verify all addresses
    println!("Verifying all addresses...");
    let mut corrupted = Vec::new();
    
    for thread_id in 0..num_threads {
        for i in 0..writes_per_thread {
            let hash = (thread_id * writes_per_thread + i) as usize;
            let expected_addr = 0x0000_7f00_0000_0000_usize + (hash * 8);
            
            if let Some(guard) = map.lock(hash) {
                let retrieved_addr = *guard;
                if retrieved_addr != expected_addr {
                    let diff = if retrieved_addr > expected_addr {
                        retrieved_addr - expected_addr
                    } else {
                        expected_addr - retrieved_addr
                    };
                    corrupted.push((hash, expected_addr, retrieved_addr, diff));
                }
            } else {
                corrupted.push((hash, expected_addr, 0, expected_addr));
            }
        }
    }
    
    println!();
    if corrupted.is_empty() {
        println!("✅ All {} addresses retrieved correctly!", num_threads * writes_per_thread);
    } else {
        println!("❌ Found {} corrupted addresses:", corrupted.len());
        for (hash, expected, actual, diff) in corrupted.iter().take(10) {
            println!("  Hash {}: expected 0x{:016x}, got 0x{:016x}, diff {} bytes (0x{:x})",
                     hash, expected, actual, diff, diff);
        }
        if corrupted.len() > 10 {
            println!("  ... and {} more", corrupted.len() - 10);
        }
    }
    
    assert_eq!(corrupted.len(), 0, "Found {} corrupted addresses in concurrent test", corrupted.len());
}

#[test]
fn test_wordmap_with_actual_segment_addresses() {
    println!("Testing WordMap with real segment addresses");
    println!();
    
    use crate::ram::segs::{Segment, SegmentAllocator};
    
    let allocator = SegmentAllocator::new(0, 1024 * 1024 * 100); // 100MB
    let map: WordMap<System> = WordMap::with_capacity(1024);
    
    // Allocate some segments and store their addresses in WordMap
    let mut segments = Vec::new();
    for i in 0..10 {
        if let Some(seg) = allocator.alloc_seg(&None, &None) {
            println!("Allocated segment {}: addr=0x{:016x}, aligned={}", 
                     i, seg.addr, seg.addr % 8 == 0);
            
            // Store segment address in WordMap
            if let Some(mut guard) = map.try_insert_locked(i) {
                *guard = seg.addr;
                drop(guard);
            }
            
            segments.push(seg);
        }
    }
    println!();
    
    // Retrieve and verify
    println!("Verifying stored addresses:");
    let mut mismatches = 0;
    for (i, seg) in segments.iter().enumerate() {
        if let Some(guard) = map.lock(i) {
            let retrieved = *guard;
            let expected = seg.addr;
            let matches = retrieved == expected;
            let aligned = retrieved % 8 == 0;
            
            println!("  Segment {}: expected 0x{:016x}, got 0x{:016x} {}{}",
                     i, expected, retrieved,
                     if matches { "✓" } else { "✗" },
                     if !aligned { " (MISALIGNED!)" } else { "" });
            
            if !matches {
                let diff = if retrieved > expected {
                    retrieved - expected
                } else {
                    expected - retrieved
                };
                println!("    Difference: {} bytes", diff);
                mismatches += 1;
            }
            
            assert_eq!(retrieved, expected, 
                      "WordMap corrupted segment address! Diff: {}",
                      if retrieved > expected { retrieved - expected } else { expected - retrieved });
        }
    }
    println!();
    
    if mismatches == 0 {
        println!("✅ All segment addresses stored and retrieved correctly!");
    } else {
        println!("❌ Found {} mismatched addresses!", mismatches);
    }
}

