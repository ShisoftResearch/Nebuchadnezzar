/// Test to reproduce memory corruption during concurrent compaction and reads
/// 
/// This test creates high load with:
/// - Many concurrent updates to trigger compaction
/// - Heavy read traffic during compaction
/// - Should reproduce "SchemaDoesNotExisted" errors with garbage schema IDs

use crate::ram::cell::*;
use crate::ram::chunk::{Chunk, Chunks};
use crate::ram::cleaner::Cleaner;
use crate::ram::schema::{Field, LocalSchemasCache, Schema};
use crate::ram::segs::{SEGMENT_SIZE, Segment};
use crate::ram::types::*;
use crate::server::ServerMeta;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const CHUNK_SIZE: usize = 8 * 1024 * 1024;

fn create_test_schema() -> Schema {
    let fields = Field::new_schema(vec![
        Field::new_unindexed("id", Type::I32),
        Field::new_unindexed("value", Type::I32),
        Field::new_unindexed_array("data", Type::U8), // Array of bytes instead of Bytes type
    ]);
    Schema::new("corruption_test", None, fields, false, false)
}

fn create_cell(schema_id: u32, id: u64, value: i32) -> OwnedCell {
    let data_bytes = vec![0xAAu8; 1000]; // 1KB of data
    OwnedCell {
        header: CellHeader::new(schema_id, &Id::new(0, id)),
        data: data_map_value!(id: value, value: value, data: OwnedValue::PrimArray(OwnedPrimArray::U8(data_bytes))),
    }
}

#[test]
fn test_compaction_with_concurrent_reads_and_updates() {
    let _ = env_logger::try_init();

    info!("=== Starting compaction corruption stress test ===");

    // Setup
    let schema = create_test_schema();
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema.clone());
    let schema_id = schema.id;

    // Use multiple chunks to increase concurrency
    // Configure tiered memory with small physical limit to force eviction/promotion
    let chunk_size = CHUNK_SIZE * 32; // 256MB per chunk (total 1GB)
    let num_chunks = 4;
    
    #[cfg(feature = "tiered_memory")]
    let tiered_config = Some(crate::ram::tiered::TieredConfig {
        threshold: 0.8,  // Start evicting at 80% of physical limit
        physical_memory_limit: 64 * 1024 * 1024, // Only 64MB physical for 1GB virtual - forces heavy eviction!
    });
    #[cfg(not(feature = "tiered_memory"))]
    let tiered_config = None;
    
    let chunks = Arc::new(Chunks::new(
        num_chunks,
        chunk_size,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        tiered_config,
    ));

    #[cfg(feature = "tiered_memory")]
    info!("Created {} chunks with {}MB each (1GB total, 64MB physical - TIERED MEMORY ENABLED)", 
          num_chunks, chunk_size / (1024 * 1024));
    #[cfg(not(feature = "tiered_memory"))]
    info!("Created {} chunks with {}MB each (tiered memory disabled)", 
          num_chunks, chunk_size / (1024 * 1024));

    // Shared state
    let stop_flag = Arc::new(AtomicBool::new(false));
    let error_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let update_count = Arc::new(AtomicUsize::new(0));
    let corruption_count = Arc::new(AtomicUsize::new(0));

    // Phase 1: Fill up segments with cells to trigger multiple segments per chunk
    info!("Phase 1: Filling segments with initial data");
    let num_initial_cells = 1000; // Each cell ~1KB, so 1000 cells = 1MB, plenty to fill segments
    for i in 0..num_initial_cells {
        let mut cell = create_cell(schema_id, i, i as i32);
        if let Err(e) = chunks.write_cell(&mut cell) {
            error!("Failed to write initial cell {}: {:?}", i, e);
        }
    }

    let total_cells = chunks.count();
    let total_segments: usize = chunks.list.iter().map(|c| c.seg_count()).sum();
    info!("Initial state: {} cells across {} segments", total_cells, total_segments);

    // Phase 2: Start aggressive cleaner that triggers compaction
    let chunks_for_cleaner = Arc::clone(&chunks);
    let stop_flag_for_cleaner = Arc::clone(&stop_flag);
    let cleaner_handle = thread::spawn(move || {
        let mut iterations = 0;
        while !stop_flag_for_cleaner.load(Ordering::Relaxed) {
            iterations += 1;
            // Run aggressive GC on all chunks
            for chunk in &chunks_for_cleaner.list {
                Cleaner::clean(chunk, false);
            }
            // Small sleep to allow other threads to make progress
            thread::sleep(Duration::from_millis(5));
            
            if iterations % 20 == 0 {
                debug!("Cleaner completed {} iterations", iterations);
            }
        }
        info!("Cleaner thread completed {} iterations", iterations);
    });

    // Phase 3: Start writer threads that continuously update cells
    let num_writer_threads = 8;
    let mut writer_handles = vec![];
    for thread_id in 0..num_writer_threads {
        let chunks_clone = Arc::clone(&chunks);
        let stop_flag_clone = Arc::clone(&stop_flag);
        let update_count_clone = Arc::clone(&update_count);
        let error_count_clone = Arc::clone(&error_count);

        let handle = thread::spawn(move || {
            let mut local_updates = 0;
            let mut local_errors = 0;
            
            while !stop_flag_clone.load(Ordering::Relaxed) {
                // Update existing cells (triggers compaction when old versions become dead space)
                let cell_id = (thread_id * 100 + (local_updates % 100)) as u64;
                let mut cell = create_cell(schema_id, cell_id, (local_updates % 1000) as i32);
                
                match chunks_clone.update_cell(&mut cell) {
                    Ok(_) => {
                        local_updates += 1;
                    }
                    Err(WriteError::CellDoesNotExisted) => {
                        // Cell doesn't exist yet, write it
                        if let Err(e) = chunks_clone.write_cell(&mut cell) {
                            error!("Writer {} failed to write cell {}: {:?}", thread_id, cell_id, e);
                            local_errors += 1;
                        } else {
                            local_updates += 1;
                        }
                    }
                    Err(e) => {
                        error!("Writer {} update failed: {:?}", thread_id, e);
                        local_errors += 1;
                    }
                }

                // Occasional small sleep to increase concurrency window
                if local_updates % 10 == 0 {
                    thread::yield_now();
                }
            }
            
            update_count_clone.fetch_add(local_updates, Ordering::Relaxed);
            error_count_clone.fetch_add(local_errors, Ordering::Relaxed);
            info!("Writer {} completed: {} updates, {} errors", thread_id, local_updates, local_errors);
        });
        writer_handles.push(handle);
    }

    // Phase 4: Start reader threads that continuously read cells
    let num_reader_threads = 16;
    let mut reader_handles = vec![];
    for thread_id in 0..num_reader_threads {
        let chunks_clone = Arc::clone(&chunks);
        let stop_flag_clone = Arc::clone(&stop_flag);
        let read_count_clone = Arc::clone(&read_count);
        let corruption_count_clone = Arc::clone(&corruption_count);
        let error_count_clone = Arc::clone(&error_count);

        let handle = thread::spawn(move || {
            let mut local_reads = 0;
            let mut local_errors = 0;
            let mut local_corruptions = 0;
            
            while !stop_flag_clone.load(Ordering::Relaxed) {
                // Read random cells
                let cell_id = (thread_id * 50 + (local_reads % 500)) as u64;
                let id = Id::new(0, cell_id);
                
                match chunks_clone.read_cell(&id) {
                    Ok(cell) => {
                        // Verify schema is correct
                        if cell.header.schema != schema_id {
                            error!(
                                "CORRUPTION DETECTED! Reader {} read cell {} with invalid schema ID: {} (expected {})",
                                thread_id, cell_id, cell.header.schema, schema_id
                            );
                            local_corruptions += 1;
                        }
                        local_reads += 1;
                    }
                    Err(ReadError::SchemaDoesNotExisted(bad_schema_id)) => {
                        error!(
                            "SCHEMA CORRUPTION! Reader {} encountered invalid schema {} reading cell {} (expected {})",
                            thread_id, bad_schema_id, cell_id, schema_id
                        );
                        local_corruptions += 1;
                        local_errors += 1;
                    }
                    Err(ReadError::CellDoesNotExisted) => {
                        // Cell was deleted or doesn't exist yet - this is fine
                        local_reads += 1;
                    }
                    Err(e) => {
                        warn!("Reader {} read error for cell {}: {:?}", thread_id, cell_id, e);
                        local_errors += 1;
                    }
                }

                // Yield occasionally to increase concurrency
                if local_reads % 20 == 0 {
                    thread::yield_now();
                }
            }
            
            read_count_clone.fetch_add(local_reads, Ordering::Relaxed);
            error_count_clone.fetch_add(local_errors, Ordering::Relaxed);
            corruption_count_clone.fetch_add(local_corruptions, Ordering::Relaxed);
            info!("Reader {} completed: {} reads, {} errors, {} corruptions", 
                  thread_id, local_reads, local_errors, local_corruptions);
        });
        reader_handles.push(handle);
    }

    // Phase 5: Let it run for a while
    info!("Running stress test for 10 seconds...");
    thread::sleep(Duration::from_secs(10));

    // Phase 6: Stop all threads
    info!("Stopping all threads...");
    stop_flag.store(true, Ordering::Relaxed);

    // Wait for all threads
    for handle in writer_handles {
        handle.join().expect("Writer thread panicked");
    }
    for handle in reader_handles {
        handle.join().expect("Reader thread panicked");
    }
    cleaner_handle.join().expect("Cleaner thread panicked");

    // Results
    let total_reads = read_count.load(Ordering::Relaxed);
    let total_updates = update_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);
    let total_corruptions = corruption_count.load(Ordering::Relaxed);

    info!("=== Test Results ===");
    info!("Total reads: {}", total_reads);
    info!("Total updates: {}", total_updates);
    info!("Total errors: {}", total_errors);
    info!("Total corruptions: {}", total_corruptions);
    info!("Final cell count: {}", chunks.count());
    
    let final_segments: usize = chunks.list.iter().map(|c| c.seg_count()).sum();
    info!("Final segments: {}", final_segments);

    // Assert no corruptions
    assert_eq!(
        total_corruptions, 0,
        "Found {} schema corruption errors during concurrent compaction and reads!",
        total_corruptions
    );

    info!("✓ No corruptions detected - test passed!");
}

