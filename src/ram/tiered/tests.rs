/// Tests for the tiered memory system
/// 
/// These tests verify:
/// - Automatic eviction when memory limit is exceeded
/// - Promotion of cold segments on access
/// - Configuration via environment variables

use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::schema::*;
use crate::ram::segs::SEGMENT_SIZE;
use crate::ram::types::*;
use crate::server::ServerMeta;
use std::sync::Arc;

/// Helper to create default test fields
fn default_fields() -> Field {
    use dovahkiin::types::Type;
    Field::new_schema(vec![
        Field::new_indexed("id", Type::I64, vec![IndexType::Statistics]),
        Field::new_unindexed("name", Type::String),
        Field::new_unindexed("data", Type::String), // Large field for filling memory
    ])
}

/// Test automatic eviction when physical memory limit is exceeded
#[test]
fn test_eviction_on_memory_overflow() {
    let _ = env_logger::try_init();
    
    // Configure tiered memory with a small physical memory limit
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.7"); // 70% threshold
    std::env::set_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT", &format!("{}", 3 * SEGMENT_SIZE)); // 3 segments = 24MB
    
    let chunk_capacity = 10 * SEGMENT_SIZE; // 80MB virtual capacity
    let fields = default_fields();
    let schema = Schema::new("test_overflow", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("/tmp/neb_test_overflow_schema");
    schemas.new_schema(schema.clone());
    
    // Create temp directories for this test
    let backup_dir = "/tmp/neb_test_overflow_bk";
    let wal_dir = "/tmp/neb_test_overflow_wal";
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);
    
    let chunks = Chunks::new(
        1,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
    );
    
    // Verify tiered manager is enabled in at least one chunk
    let has_tiered_manager = chunks.list.iter().any(|c| c.tiered_manager.is_some());
    assert!(has_tiered_manager, "Tiered memory manager should be enabled");
    
    // Fill with enough data to exceed the physical memory limit
    // Each cell will be ~1KB (plus overhead), so we need multiple segments worth
    let large_data = "x".repeat(1024); // 1KB string
    let cells_per_segment = SEGMENT_SIZE / 2048; // Conservative estimate
    let num_cells = cells_per_segment * 5; // 5 segments worth of data
    
    info!("Filling with {} cells to trigger eviction", num_cells);
    
    for i in 0..num_cells {
        let id = Id::new(schema.id as u64, i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("test_{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String(large_data.clone()));
        
        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };
        
        match chunks.write_cell(&mut cell) {
            Ok(_) => {
                // Periodically trigger eviction check
                if i > 0 && i % (cells_per_segment / 2) == 0 {
                    for chunk in &chunks.list {
                        if let Some(ref manager) = chunk.tiered_manager {
                            match manager.check_and_evict(chunk) {
                                Ok(evicted) if evicted > 0 => {
                                    info!("Evicted {} segments at cell {}", evicted, i);
                                }
                                Err(e) => {
                                    error!("Eviction failed: {:?}", e);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Write failed at cell {} (may be expected if virtual capacity full): {:?}", i, e);
                break;
            }
        }
    }
    
    // Check that some segments are cold
    let mut total_hot = 0;
    let mut total_cold = 0;
    
    for chunk in &chunks.list {
        let segments = chunk.segments();
        let hot = segments.iter().filter(|s| s.is_hot()).count();
        let cold = segments.iter().filter(|s| s.is_cold()).count();
        total_hot += hot;
        total_cold += cold;
        
        info!("Chunk {}: {} hot segments, {} cold segments", chunk.id, hot, cold);
        
        // Verify cold segments have file descriptors
        for seg in segments.iter().filter(|s| s.is_cold()) {
            let fd = seg.cold_file_fd.load(std::sync::atomic::Ordering::Relaxed);
            assert!(fd >= 0, "Cold segment {} should have valid file descriptor", seg.id);
        }
    }
    
    info!("Total: {} hot, {} cold segments", total_hot, total_cold);
    assert!(total_cold > 0, "Expected some segments to be evicted to cold storage");
    
    // Test that we can still read data from cold segments (promotion)
    // Read a few cells to trigger promotion
    for i in 0..(num_cells.min(10)) {
        let id = Id::new(schema.id as u64, i as u64);
        match chunks.read_cell(&id) {
            Ok(cell) => {
                assert_eq!(cell.data["id"].i64().unwrap(), &(i as i64));
                info!("Successfully read cell {} (may have triggered promotion)", i);
            }
            Err(e) => {
                panic!("Failed to read cell {}: {:?}", i, e);
            }
        }
    }
    
    // Clean up environment variables
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    
    // Clean up test directories
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all("/tmp/neb_test_overflow_schema");
}

/// Test that reads from cold segments trigger promotion and data is still intact
#[test]
fn test_cold_segment_promotion() {
    let _ = env_logger::try_init();
    
    // Configure with tight memory limit
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.6");
    std::env::set_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT", &format!("{}", 2 * SEGMENT_SIZE));
    
    let chunk_capacity = 8 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("test_promotion", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("/tmp/neb_test_promotion_schema");
    schemas.new_schema(schema.clone());
    
    let backup_dir = "/tmp/neb_test_promotion_bk";
    let wal_dir = "/tmp/neb_test_promotion_wal";
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);
    
    let chunks = Chunks::new(
        1,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
    );
    
    // Write cells
    let large_data = "testdata_".repeat(128); // ~1KB
    let num_cells = (SEGMENT_SIZE / 2048) * 4; // 4 segments worth
    
    let mut written_ids = Vec::new();
    for i in 0..num_cells {
        let id = Id::new(schema.id as u64, 1000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(1000 + i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("promotion_test_{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String(large_data.clone()));
        
        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };
        
        if chunks.write_cell(&mut cell).is_ok() {
            written_ids.push(id);
            
            // Trigger eviction periodically
            if i % 100 == 0 {
                for chunk in &chunks.list {
                    if let Some(ref manager) = chunk.tiered_manager {
                        let _ = manager.check_and_evict(chunk);
                    }
                }
            }
        }
    }
    
    info!("Wrote {} cells", written_ids.len());
    
    // Force eviction to make sure we have cold segments
    for chunk in &chunks.list {
        if let Some(ref manager) = chunk.tiered_manager {
            match manager.explicit_evict(chunk, 2) {
                Ok(evicted) => info!("Explicitly evicted {} segments", evicted),
                Err(e) => error!("Explicit eviction failed: {:?}", e),
            }
        }
    }
    
    // Verify cold segments exist
    let total_cold: usize = chunks.list.iter()
        .map(|c| c.segments().iter().filter(|s| s.is_cold()).count())
        .sum();
    assert!(total_cold > 0, "Should have cold segments after explicit eviction");
    info!("Have {} cold segments", total_cold);
    
    // Read all cells back - this should promote cold segments as needed
    for (idx, id) in written_ids.iter().enumerate() {
        match chunks.read_cell(id) {
            Ok(cell) => {
                let expected_id = 1000 + idx as i64;
                assert_eq!(cell.data["id"].i64().unwrap(), &expected_id, 
                          "Data should be intact after promotion");
            }
            Err(e) => {
                panic!("Failed to read cell after promotion: {:?}", e);
            }
        }
    }
    
    info!("Successfully read all cells, promotion working correctly");
    
    // Clean up
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all("/tmp/neb_test_promotion_schema");
}

/// Test that cleaners skip cold segments
#[test]
fn test_cleaner_ignores_cold_segments() {
    let _ = env_logger::try_init();
    
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.5");
    std::env::set_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT", &format!("{}", 2 * SEGMENT_SIZE));
    
    let chunk_capacity = 8 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("test_cleaner", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("/tmp/neb_test_cleaner_schema");
    schemas.new_schema(schema.clone());
    
    let backup_dir = "/tmp/neb_test_cleaner_bk";
    let wal_dir = "/tmp/neb_test_cleaner_wal";
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);
    
    let chunks = Chunks::new(
        1,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
    );
    
    // Write data and force eviction
    let num_cells = (SEGMENT_SIZE / 2048) * 4;
    for i in 0..num_cells {
        let id = Id::new(schema.id as u64, 2000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(2000 + i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("cleaner_test_{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String("data".repeat(256)));
        
        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };
        
        let _ = chunks.write_cell(&mut cell);
        
        if i % 100 == 0 {
            for chunk in &chunks.list {
                if let Some(ref manager) = chunk.tiered_manager {
                    let _ = manager.check_and_evict(chunk);
                }
            }
        }
    }
    
    // Force some evictions
    for chunk in &chunks.list {
        if let Some(ref manager) = chunk.tiered_manager {
            let _ = manager.explicit_evict(chunk, 2);
        }
    }
    
    // Check cleaner segment lists
    for chunk in &chunks.list {
        let compact_segs = chunk.segs_for_compact_cleaner();
        let combine_segs = chunk.segs_for_combine_cleaner();
        
        // Verify no cold segments in cleaner lists
        for seg in &compact_segs {
            assert!(seg.is_hot(), "Compact cleaner should only see hot segments, but saw cold segment {}", seg.id);
        }
        
        for (seg, _) in &combine_segs {
            assert!(seg.is_hot(), "Combine cleaner should only see hot segments, but saw cold segment {}", seg.id);
        }
        
        info!("Chunk {}: {} segments for compact cleaner, {} for combine cleaner", 
              chunk.id, compact_segs.len(), combine_segs.len());
    }
    
    // Clean up
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all("/tmp/neb_test_cleaner_schema");
}
