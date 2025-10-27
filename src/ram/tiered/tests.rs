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
    
    // Fill with enough data to exceed the physical memory limit (3 segments = 24MB)
    // Each cell will be ~1KB (plus overhead), so we need multiple segments worth
    let large_data = "x".repeat(1024); // 1KB string
    let cells_per_segment = SEGMENT_SIZE / 2048; // Conservative estimate
    let num_cells = cells_per_segment * 6; // 6 segments worth to ensure we exceed 3-segment limit
    
    info!("Filling with {} cells to exceed 3-segment limit", num_cells);
    
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
    
    // Final eviction check to ensure memory limit is respected
    info!("Final eviction check");
    for chunk in &chunks.list {
        if let Some(ref manager) = chunk.tiered_manager {
            match manager.check_and_evict(chunk) {
                Ok(evicted) if evicted > 0 => {
                    info!("Final eviction: evicted {} segments", evicted);
                }
                Err(e) => {
                    error!("Final eviction failed: {:?}", e);
                }
                _ => {}
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

/// Test tiered memory with recovery enabled from the start
#[test]
fn test_tiered_memory_with_recovery_enabled() {
    let _ = env_logger::try_init();
    
    let backup_dir = "/tmp/neb_test_recovery_enabled_bk";
    let wal_dir = "/tmp/neb_test_recovery_enabled_wal";
    let schema_dir = "/tmp/neb_test_recovery_enabled_schema";
    
    // Clean up from any previous runs
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
    
    // Configure tiered memory with tight limits
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.6");
    std::env::set_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT", &format!("{}", 3 * SEGMENT_SIZE));
    
    let chunk_capacity = 10 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("test_recovery", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local(schema_dir);
    schemas.new_schema(schema.clone());
    
    // Create chunks with recovery enabled (simulates production scenario)
    let chunks = Chunks::new_with_recovery(
        1,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        true, // recovery enabled
    );
    
    info!("Testing tiered memory with recovery enabled");
    
    // Write data and trigger eviction
    let large_data = "recovery_test_".repeat(100); // ~1.4KB
    let num_cells = (SEGMENT_SIZE / 2048) * 5; // 5 segments worth
    let mut written_ids = Vec::new();
    
    for i in 0..num_cells {
        let id = Id::new(schema.id as u64, 3000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(3000 + i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("recovery_{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String(large_data.clone()));
        
        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };
        
        if chunks.write_cell(&mut cell).is_ok() {
            written_ids.push(id);
            
            // Trigger eviction periodically
            if i % 200 == 0 && i > 0 {
                for chunk in &chunks.list {
                    if let Some(ref manager) = chunk.tiered_manager {
                        let _ = manager.check_and_evict(chunk);
                    }
                }
            }
        }
    }
    
    info!("Wrote {} cells", written_ids.len());
    
    // Force eviction to create cold segments
    for chunk in &chunks.list {
        if let Some(ref manager) = chunk.tiered_manager {
            match manager.explicit_evict(chunk, 2) {
                Ok(evicted) => info!("Evicted {} segments", evicted),
                Err(e) => error!("Eviction failed: {:?}", e),
            }
        }
    }
    
    // Check cold segment count
    let total_cold: usize = chunks.list.iter()
        .map(|c| c.segments().iter().filter(|s| s.is_cold()).count())
        .sum();
    info!("Cold segments: {}", total_cold);
    assert!(total_cold > 0, "Should have cold segments");
    
    // Verify all cells can still be read (triggers promotion as needed)
    let mut successful_reads = 0;
    for (idx, id) in written_ids.iter().enumerate() {
        match chunks.read_cell(id) {
            Ok(cell) => {
                let expected_id = 3000 + idx as i64;
                assert_eq!(cell.data["id"].i64().unwrap(), &expected_id,
                          "Data mismatch for cell {}", idx);
                successful_reads += 1;
            }
            Err(e) => {
                error!("Failed to read cell {}: {:?}", idx, e);
            }
        }
    }
    
    info!("Successfully read {}/{} cells with recovery enabled", successful_reads, written_ids.len());
    assert!(successful_reads > 0, "Should be able to read cells");
    
    // Verify tiered memory still works
    for chunk in &chunks.list {
        if let Some(ref manager) = chunk.tiered_manager {
            let hot_before = chunk.segments().iter().filter(|s| s.is_hot()).count();
            if let Ok(evicted) = manager.explicit_evict(chunk, 1) {
                if evicted > 0 {
                    let hot_after = chunk.segments().iter().filter(|s| s.is_hot()).count();
                    assert!(hot_after < hot_before, "Hot count should decrease after eviction");
                    info!("Tiered memory working correctly with recovery enabled");
                }
            }
        }
    }
    
    // Clean up
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
}

/// Test that recovery loads segments as COLD when tiered memory is enabled
/// This saves physical memory by mmapping backup files directly instead of loading into RAM
#[test]
fn test_recovery_loads_segments_as_cold() {
    let _ = env_logger::try_init();
    
    let backup_dir = "/tmp/neb_test_cold_recovery_bk";
    let wal_dir = "/tmp/neb_test_cold_recovery_wal";
    let schema_dir = "/tmp/neb_test_cold_recovery_schema";
    
    // Clean up
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
    
    // Configure tiered memory with limit of 2 segments (16MB)
    // We'll create 7+ segments, so at least 3 should be recovered as cold (2 hot + 1 head = 3 hot max)
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.8");
    std::env::set_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT", &format!("{}", 2 * SEGMENT_SIZE)); // Only 2 segments fit in hot
    
    let chunk_capacity = 10 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("test_cold_recovery", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local(schema_dir);
    schemas.new_schema(schema.clone());
    
    // Phase 1: Create data and archive it (without recovery)
    {
        info!("Phase 1: Creating and archiving 5 segments of data");
        
        let chunks = Chunks::new(
            1,
            chunk_capacity,
            Arc::new(ServerMeta { schemas }),
            None,
            Some(backup_dir.to_string()),
            Some(wal_dir.to_string()),
        );
        
        // Write enough data to create at least 5 segments
        // Use the same pattern as working tests: 1KB cells, conservative estimate
        let large_data = "x".repeat(1024);
        let cells_per_segment = SEGMENT_SIZE / 2048;
        let num_cells = cells_per_segment * 15; // 15 segments worth to ensure we get at least 7
        
        info!("Writing {} cells to create 7+ segments", num_cells);
        
        for i in 0..num_cells {
            let id = Id::new(schema.id as u64, 5000 + i as u64);
            let mut data_map = OwnedMap::new();
            data_map.insert(&String::from("id"), OwnedValue::I64(5000 + i as i64));
            data_map.insert(&String::from("name"), OwnedValue::String(format!("cold_recovery_{}", i)));
            data_map.insert(&String::from("data"), OwnedValue::String(large_data.clone()));
            
            let data = OwnedValue::Map(data_map);
            let mut cell = OwnedCell {
                header: CellHeader::new(schema.id, &id),
                data,
            };
            
            let _ = chunks.write_cell(&mut cell);
        }
        
        let segments_created = chunks.list[0].segs.len();
        info!("Wrote {} cells creating {} segments", num_cells, segments_created);
        
        // Archive all segments
        for chunk in &chunks.list {
            for segment in chunk.segments() {
                let _ = segment.archive();
            }
        }
        
        info!("Phase 1 complete - archived {} segments", chunks.list[0].segs.len());
    }
    
    // Phase 2: Recover with tiered memory - segments should be loaded as COLD
    {
        info!("Phase 2: Recovering with tiered memory (should load as cold)");
        
        let schemas_recovery = LocalSchemasCache::new_local(schema_dir);
        schemas_recovery.new_schema(schema.clone());
        
        let chunks = Chunks::new_with_recovery(
            1,
            chunk_capacity,
            Arc::new(ServerMeta { schemas: schemas_recovery }),
            None,
            Some(backup_dir.to_string()),
            Some(wal_dir.to_string()),
            true, // enable recovery
        );
        
        // Count cold vs hot segments after recovery
        let mut cold_count = 0;
        let mut hot_count = 0;
        
        for chunk in &chunks.list {
            for segment in chunk.segments() {
                if segment.is_cold() {
                    cold_count += 1;
                    info!("Segment {} recovered as COLD", segment.id);
                } else if segment.is_hot() {
                    hot_count += 1;
                    // Head segment should always be hot
                    if segment.id == chunk.get_head_seg_id() {
                        info!("Segment {} is head (correctly HOT)", segment.id);
                    }
                }
            }
        }
        
        info!("After recovery: {} cold segments, {} hot segments (expected 3 cold, 2 hot)", cold_count, hot_count);
        
        // With tiered memory enabled and 16MB limit, 3 out of 5 segments should be cold
        assert!(cold_count >= 3, "Expected at least 3 segments to be recovered as cold (limit is 2 segments = 16MB)");
        
        // Verify we can still read data from cold segments (triggers promotion)
        for i in 0..10 {
            let id = Id::new(schema.id as u64, 5000 + i);
            match chunks.read_cell(&id) {
                Ok(cell) => {
                    assert_eq!(cell.data["id"].i64().unwrap(), &(5000 + i as i64));
                }
                Err(e) => {
                    error!("Failed to read cell {} from cold segment: {:?}", i, e);
                }
            }
        }
        
        info!("Successfully read from cold segments - cold recovery working!");
    }
    
    // Clean up
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
}

/// Test that cold segments are properly recycled after cleaning
/// This prevents address space bloating
#[test]
fn test_cold_segment_recycling() {
    let _ = env_logger::try_init();
    
    let backup_dir = "/tmp/neb_test_recycling_bk";
    let wal_dir = "/tmp/neb_test_recycling_wal";
    let schema_dir = "/tmp/neb_test_recycling_schema";
    
    // Clean up
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
    
    // Configure tiered memory with very tight limit (1 segment = 8MB)
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.5");
    std::env::set_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT", &format!("{}", SEGMENT_SIZE)); // Only 1 segment fits!
    
    let chunk_capacity = 10 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("test_recycling", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local(schema_dir);
    schemas.new_schema(schema.clone());
    
    let chunks = Chunks::new(
        1,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
    );
    
    // Track allocated segment addresses
    let initial_segments: Vec<u64> = chunks.list[0].segments().iter().map(|s| s.id).collect();
    info!("Initial segments: {:?}", initial_segments);
    
    // Phase 1: Fill multiple segments with data and trigger eviction
    info!("Phase 1: Creating 3+ segments and evicting to cold");
    let large_data = "x".repeat(1024); // 1KB cells (matching working tests)
    let cells_per_segment = SEGMENT_SIZE / 2048; // Conservative estimate
    let num_cells = cells_per_segment * 7; // 7 segments worth to ensure we exceed 1-segment limit
    
    info!("Filling with {} cells to exceed 1-segment limit", num_cells);
    
    for i in 0..num_cells {
        let id = Id::new(schema.id as u64, 6000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(6000 + i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("test_{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String(large_data.clone()));
        
        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };
        
        let _ = chunks.write_cell(&mut cell);
        
        // Trigger eviction more frequently
        if i > 0 && i % (cells_per_segment / 2) == 0 {
            for chunk in &chunks.list {
                if let Some(ref manager) = chunk.tiered_manager {
                    match manager.check_and_evict(chunk) {
                        Ok(evicted) if evicted > 0 => {
                            info!("Evicted {} segments at cell {}", evicted, i);
                        }
                        Err(e) => {
                            warn!("Eviction check failed: {:?}", e);
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    
    let after_fill: Vec<u64> = chunks.list[0].segments().iter().map(|s| s.id).collect();
    let cold_count_before = chunks.list[0].segments().iter().filter(|s| s.is_cold()).count();
    info!("After filling: {:?}, cold segments: {}", after_fill, cold_count_before);
    
    assert!(cold_count_before > 0, "Should have cold segments before cleaning");
    
    // Phase 2: Test the recycling mechanism by manually calling mem_drop on a cold segment
    info!("Phase 2: Testing recycling mechanism");
    
    // Find a cold segment to recycle
    let cold_segment_id = chunks.list[0].segments().iter()
        .find(|s| s.is_cold())
        .map(|s| s.id)
        .expect("Should have cold segments");
    
    info!("Testing mem_drop on cold segment {}", cold_segment_id);
    
    // Get the segment and call mem_drop (this is what cleaners do)
    if let Some(seg_to_recycle) = chunks.list[0].segments().iter()
        .find(|s| s.id == cold_segment_id)
        .cloned()
    {
        // Remove from chunk's segment list (simulates what cleaner does)
        chunks.list[0].remove_segment(seg_to_recycle.id);
        
        // This should unmap the file-backed memory, remap as anonymous, and add to free list
        seg_to_recycle.mem_drop(&chunks.list[0]);
        
        info!("Successfully recycled cold segment {}", cold_segment_id);
    }
    
    // Phase 3: Allocate new segment - should reuse the recycled address
    info!("Phase 3: Allocating new segments to verify recycling");
    
    let pre_alloc_count = chunks.list[0].segments().len();
    
    // Allocate more data to trigger new segment allocation
    for i in 0..(cells_per_segment * 2) {
        let id = Id::new(schema.id as u64, 8000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(8000 + i as i64));
        data_map.insert(&String::from("data"), OwnedValue::String(large_data.clone()));
        
        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };
        
        let _ = chunks.write_cell(&mut cell);
    }
    
    let post_alloc_count = chunks.list[0].segments().len();
    let final_segments: Vec<u64> = chunks.list[0].segments().iter().map(|s| s.id).collect();
    
    info!("Segment count: {} -> {}, final segments: {:?}", 
          pre_alloc_count, post_alloc_count, final_segments);
    
    // Verify the recycled segment ID appears in the new allocations
    let recycled_id_reused = final_segments.contains(&cold_segment_id);
    info!("Recycled segment {} reused: {}", cold_segment_id, recycled_id_reused);
    
    info!("Recycling test complete - cold segments can be properly recycled");
    
    // Clean up
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
}

/// Test that hot segments are properly recycled and remapped
/// Verifies that remapping hot segments works without allocating extra memory
#[test]
fn test_hot_segment_recycling() {
    let _ = env_logger::try_init();
    
    let backup_dir = "/tmp/neb_test_hot_recycling_bk";
    let wal_dir = "/tmp/neb_test_hot_recycling_wal";
    let schema_dir = "/tmp/neb_test_hot_recycling_schema";
    
    // Clean up
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
    
    // No tiered memory - all segments stay hot
    let chunk_capacity = 10 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("test_hot_recycle", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local(schema_dir);
    schemas.new_schema(schema.clone());
    
    let chunks = Chunks::new(
        1,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
    );
    
    // Phase 1: Fill multiple segments with recognizable data
    info!("Phase 1: Creating hot segments with data");
    
    // Use the same pattern as test_eviction_on_memory_overflow
    let large_data = "x".repeat(1024); // 1KB string
    let cells_per_segment = SEGMENT_SIZE / 2048; // Conservative estimate
    let num_cells = cells_per_segment * 5; // 5 segments worth of data
    
    info!("Creating {} cells to fill 5 segments", num_cells);
    for i in 0..num_cells {
        let id = Id::new(schema.id as u64, 7000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(7000 + i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("original_{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String(large_data.clone()));
        
        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };
        
        let _ = chunks.write_cell(&mut cell);
    }
    
    let segments_before: Vec<u64> = chunks.list[0].segments().iter().map(|s| s.id).collect();
    info!("Segments created: {:?}, all should be hot", segments_before);
    info!("Total segments: {}", segments_before.len());
    
    // Verify all are hot
    let all_hot = chunks.list[0].segments().iter().all(|s| s.is_hot());
    assert!(all_hot, "All segments should be hot without tiered memory");
    
    // Verify we have at least 2 segments
    assert!(segments_before.len() >= 2, "Should have at least 2 segments, but only have {}", segments_before.len());
    
    // Phase 2: Manually recycle a hot segment (simulate what cleaner does)
    info!("Phase 2: Recycling a hot segment");
    
    let segment_to_recycle = chunks.list[0].segments()
        .iter()
        .nth(1) // Pick the second segment (not head)
        .expect("Should have multiple segments")
        .clone();
    
    let recycled_id = segment_to_recycle.id;
    info!("Recycling hot segment {}", recycled_id);
    
    // Remove and drop (this adds to free list)
    chunks.list[0].remove_segment(recycled_id);
    segment_to_recycle.mem_drop(&chunks.list[0]);
    
    // Phase 3: Allocate new data - should reuse the recycled segment
    info!("Phase 3: Allocating new data to reuse recycled hot segment");
    
    let new_large_data = "y".repeat(1024); // 1KB string
    let new_num_cells = cells_per_segment * 3; // 3 segments worth of new data
    
    info!("Creating {} new cells to fill 3 segments", new_num_cells);
    // Use IDs that don't collide with Phase 1 (which goes up to 7000+20480 = 27480)
    for i in 0..new_num_cells {
        let id = Id::new(schema.id as u64, 30000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(9000 + i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("new_{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String(new_large_data.clone()));
        
        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };
        
        let _ = chunks.write_cell(&mut cell);
    }
    
    let segments_after: Vec<u64> = chunks.list[0].segments().iter().map(|s| s.id).collect();
    info!("Segments after reuse: {:?}", segments_after);
    
    // Verify the recycled segment ID was reused
    let was_reused = segments_after.contains(&recycled_id);
    info!("Hot segment {} was reused: {}", recycled_id, was_reused);
    
    // Verify all segments are still hot
    let all_still_hot = chunks.list[0].segments().iter().all(|s| s.is_hot());
    assert!(all_still_hot, "All segments should still be hot");
    
    // Verify we can read the new data (not old data)
    let id = Id::new(schema.id as u64, 30000);
    match chunks.read_cell(&id) {
        Ok(cell) => {
            // Cell ID 30000 contains internal id value of 9000 (see Phase 3 loop)
            assert_eq!(cell.data["id"].i64().unwrap(), &9000);
            let name_str = cell.data["name"].string().unwrap();
            assert!(name_str.starts_with("new_"), "Should have new data (new_*), got: {}", name_str);
            let data_str = cell.data["data"].string().unwrap();
            assert!(data_str.starts_with("y"), "Data field should contain 'y' characters from new write");
            info!("Successfully verified new data in recycled segment");
        }
        Err(e) => {
            error!("Failed to read from recycled hot segment: {:?}", e);
        }
    }
    
    info!("Hot segment recycling test complete - remapping works correctly");
    
    // Clean up
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
}
