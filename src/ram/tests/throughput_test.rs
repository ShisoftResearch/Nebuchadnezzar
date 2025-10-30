/// Throughput test for write_cell with WAL and backup enabled
/// This test is designed to measure the actual write throughput and observe
/// the impact of group commit batching optimizations.

use super::*;
use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::schema::*;
use crate::ram::types::*;
use crate::server::ServerMeta;
use dovahkiin::types::Type;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

pub const CHUNK_SIZE: usize = 16 * 1024 * 1024 * 1024; // 16GB
pub const CHUNK_COUNT: usize = 2;
pub const NUM_THREADS: usize = 16;

#[test]
fn test_write_throughput_with_wal_and_backup() {
    let _ = env_logger::try_init();
    
    // Setup: Create temporary directories for WAL and backup
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let backup_dir = temp_dir.path().join("backup");
    
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&backup_dir).unwrap();
    
    let wal_path = wal_dir.to_str().unwrap().to_string();
    let backup_path = backup_dir.to_str().unwrap().to_string();
    
    println!("\n=== Parallel Write Throughput Test with WAL and Backup ===");
    println!("WAL directory: {}", wal_path);
    println!("Backup directory: {}", backup_path);
    println!("Chunk size: {} GB", CHUNK_SIZE / (1024 * 1024 * 1024));
    println!("Chunk count: {}", CHUNK_COUNT);
    println!("Worker threads: {}", NUM_THREADS);
    
    // Create schema using the same pattern as existing tests
    let fields = default_fields();
    let schema = Schema::new("throughput_test", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema.clone());
    
    let meta = Arc::new(ServerMeta { schemas });
    
    // Create chunks with WAL and backup enabled
    let chunks = Arc::new(Chunks::new(
        CHUNK_COUNT,
        CHUNK_SIZE * CHUNK_COUNT,
        meta,
        None, // no index builder
        Some(backup_path),
        Some(wal_path),
        None, // no tiered memory
    ));
    
    // Test parameters
    let total_writes = 1740000usize; // ~8GB with 1KB cells
    let writes_per_thread = total_writes / NUM_THREADS;
    let cell_data_size = 1024; // 1KB of data per cell
    
    let target_gb = (total_writes * (cell_data_size + 100)) as f64 / (1024.0 * 1024.0 * 1024.0);
    println!("\n=== Writing {:.2} GB of data ({} cells) ===", target_gb, total_writes);
    println!("Each thread writes {} cells", writes_per_thread);
    
    // Shared progress counter
    let progress_counter = Arc::new(AtomicUsize::new(0));
    
    // Start timing
    let start = Instant::now();
    
    // Spawn worker threads
    let mut handles = vec![];
    
    for thread_id in 0..NUM_THREADS {
        let chunks = Arc::clone(&chunks);
        let progress = Arc::clone(&progress_counter);
        let schema_id = schema.id;
        let start_id = thread_id * writes_per_thread;
        let end_id = start_id + writes_per_thread;
        
        let handle = thread::spawn(move || {
            let mut local_bytes = 0usize;
            
            for i in start_id..end_id {
                // Use thread_id as partition to distribute across chunks
                let partition = (thread_id % CHUNK_COUNT) as u64;
                let cell_id = Id::new(partition, i as u64);
                
                // Create cell data
                let mut data_map = OwnedMap::new();
                data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
                data_map.insert(&String::from("score"), OwnedValue::U64(i as u64 * 100));
                data_map.insert(&String::from("name"), OwnedValue::String("x".repeat(cell_data_size)));
                
                let mut cell = OwnedCell {
                    header: CellHeader::new(schema_id, &cell_id),
                    data: OwnedValue::Map(data_map),
                };
                
                // Calculate size
                let cell_size = cell_data_size + 100;
                local_bytes += cell_size;
                
                chunks.write_cell(&mut cell).expect("Write should succeed");
                
                // Update progress counter
                let completed = progress.fetch_add(1, Ordering::Relaxed) + 1;
                
                // Print progress every 50000 writes (from any thread)
                if completed % 50000 == 0 {
                    let elapsed = start.elapsed();
                    let elapsed_secs = elapsed.as_secs_f64();
                    let total_bytes = completed * cell_size;
                    let throughput_mbs = (total_bytes as f64 / elapsed_secs) / (1024.0 * 1024.0);
                    println!(
                        "Progress: {}/{} cells ({:.1}%), {:.2} GB written, {:.2} MB/s",
                        completed,
                        total_writes,
                        (completed as f64 / total_writes as f64) * 100.0,
                        total_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                        throughput_mbs
                    );
                }
            }
            
            local_bytes
        });
        
        handles.push(handle);
    }
    
    // Wait for all threads to complete and sum bytes written
    let mut total_bytes = 0usize;
    for handle in handles {
        total_bytes += handle.join().expect("Thread should complete successfully");
    }
    
    let elapsed = start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let throughput_mbs = (total_bytes as f64 / elapsed_secs) / (1024.0 * 1024.0);
    let throughput_gbs = throughput_mbs / 1024.0;
    let writes_per_sec = total_writes as f64 / elapsed_secs;
    
    println!("\n=== Write Performance Results ===");
    println!("Total writes: {}", total_writes);
    println!("Total data written: {:.2} GB", total_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
    println!("Time elapsed: {:.2} seconds", elapsed_secs);
    println!("Throughput: {:.2} MB/s ({:.2} GB/s)", throughput_mbs, throughput_gbs);
    println!("Write rate: {:.0} writes/second", writes_per_sec);
    println!("Threads: {}", NUM_THREADS);
    
    // Performance expectations
    println!("\n=== Performance Analysis ===");
    if throughput_mbs < 20.0 {
        println!("⚠️  WARNING: Throughput is low ({:.2} MB/s)", throughput_mbs);
        println!("   This suggests the WAL sync is still a bottleneck.");
        println!("   Current thresholds:");
        println!("   - Batch size: 4MB");
        println!("   - Interval: 100ms");
        println!("   Consider increasing these values in src/ram/segs.rs");
    } else if throughput_mbs < 100.0 {
        println!("✓ Good: Throughput is reasonable ({:.2} MB/s)", throughput_mbs);
        println!("  Group commit batching is working with {} threads.", NUM_THREADS);
    } else {
        println!("✓✓ Excellent: High throughput ({:.2} MB/s)", throughput_mbs);
        println!("   Group commit batching is very effective with parallel writes!");
    }
    
    // Verify all cells were written (allow small variance due to concurrent operations)
    let cell_count = chunks.count();
    assert!(
        cell_count >= total_writes && cell_count <= total_writes + NUM_THREADS,
        "Cell count should be close to expected, expected {}, got {} (diff: {})",
        total_writes, cell_count, cell_count as i64 - total_writes as i64
    );
    
    println!("\n=== Test Completed Successfully ===\n");
}


