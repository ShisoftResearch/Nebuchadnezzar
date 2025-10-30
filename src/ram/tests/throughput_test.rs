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
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

pub const CHUNK_SIZE: usize = 64 * 1024 * 1024; // 64MB
pub const CHUNK_COUNT: usize = 2;

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
    
    println!("\n=== Write Throughput Test with WAL and Backup ===");
    println!("WAL directory: {}", wal_path);
    println!("Backup directory: {}", backup_path);
    println!("Chunk size: {} MB", CHUNK_SIZE / (1024 * 1024));
    println!("Chunk count: {}", CHUNK_COUNT);
    
    // Create schema using the same pattern as existing tests
    let fields = default_fields();
    let schema = Schema::new("throughput_test", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema.clone());
    
    let meta = Arc::new(ServerMeta { schemas });
    
    // Create chunks with WAL and backup enabled
    let chunks = Chunks::new(
        CHUNK_COUNT,
        CHUNK_SIZE * CHUNK_COUNT,
        meta,
        None, // no index builder
        Some(backup_path),
        Some(wal_path),
        None, // no tiered memory
    );
    
    // Test parameters
    let num_writes = 10000usize;
    let cell_data_size = 1024; // 1KB of data per cell
    
    println!("\n=== Writing {} cells ===", num_writes);
    
    // Perform writes and measure throughput
    let start = Instant::now();
    let mut total_bytes = 0usize;
    
    for i in 0..num_writes {
        let cell_id = Id::new(1, i as u64);
        
        // Create cell data - reuse pattern from existing tests
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("score"), OwnedValue::U64(i as u64 * 100));
        data_map.insert(&String::from("name"), OwnedValue::String("x".repeat(cell_data_size)));
        
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &cell_id),
            data: OwnedValue::Map(data_map),
        };
        
        // Calculate approximate size
        let cell_size = cell_data_size + 100;
        total_bytes += cell_size;
        
        chunks.write_cell(&mut cell).expect("Write should succeed");
        
        // Print progress every 1000 writes
        if (i + 1) % 1000 == 0 {
            let elapsed = start.elapsed();
            let elapsed_secs = elapsed.as_secs_f64();
            let throughput_mbs = (total_bytes as f64 / elapsed_secs) / (1024.0 * 1024.0);
            println!(
                "Progress: {}/{} cells, {:.2} MB written, {:.2} MB/s",
                i + 1,
                num_writes,
                total_bytes as f64 / (1024.0 * 1024.0),
                throughput_mbs
            );
        }
    }
    
    let elapsed = start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let throughput_mbs = (total_bytes as f64 / elapsed_secs) / (1024.0 * 1024.0);
    let writes_per_sec = num_writes as f64 / elapsed_secs;
    
    println!("\n=== Write Performance Results ===");
    println!("Total writes: {}", num_writes);
    println!("Total data written: {:.2} MB", total_bytes as f64 / (1024.0 * 1024.0));
    println!("Time elapsed: {:.2} seconds", elapsed_secs);
    println!("Throughput: {:.2} MB/s", throughput_mbs);
    println!("Write rate: {:.0} writes/second", writes_per_sec);
    
    // Performance expectations
    println!("\n=== Performance Analysis ===");
    if throughput_mbs < 20.0 {
        println!("⚠️  WARNING: Throughput is low ({:.2} MB/s)", throughput_mbs);
        println!("   This suggests the WAL sync is still a bottleneck.");
        println!("   Current thresholds:");
        println!("   - Batch size: 4MB");
        println!("   - Interval: 100ms");
        println!("   Consider increasing these values in src/ram/segs.rs");
    } else if throughput_mbs < 50.0 {
        println!("✓ Good: Throughput is reasonable ({:.2} MB/s)", throughput_mbs);
        println!("  Group commit batching is working.");
    } else {
        println!("✓✓ Excellent: High throughput ({:.2} MB/s)", throughput_mbs);
        println!("   Group commit batching is very effective!");
    }
    
    // Verify all cells were written
    let cell_count = chunks.count();
    assert_eq!(
        cell_count, num_writes,
        "All cells should be written, expected {}, got {}",
        num_writes, cell_count
    );
    
    println!("\n=== Test Completed Successfully ===\n");
}


