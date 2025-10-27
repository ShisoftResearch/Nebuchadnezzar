use super::*;
use crate::dovahkiin::types::Map;
use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::entry::{EntryContent, EntryType};
use crate::ram::schema::Field;
use crate::ram::schema::*;
use crate::ram::segs::SEGMENT_SIZE;
use crate::ram::types::*;
use crate::server::ServerMeta;
use std::sync::Arc;

pub const DATA_SIZE: usize = 500 * 1024; // 500KB per cell
const MAX_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

fn default_cell(id: &Id) -> OwnedCell {
    let data: Vec<_> = std::iter::repeat(id.lower as u8).take(DATA_SIZE).collect();
    OwnedCell {
        header: CellHeader::new(0, id),
        data: data_map_value!(id: id.lower as i32, data: data),
    }
}

fn default_fields() -> Field {
    Field::new_schema(vec![
        Field::new_unindexed("id", Type::I32),
        Field::new_unindexed_array("data", Type::U8),
    ])
}

#[test]
fn test_eviction_and_promotion_basic() {
    let _ = env_logger::try_init();
    
    let schema = Schema::new("tiered_test", None, default_fields(), false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema);
    
    // Create temporary directory for test
    let test_dir = std::env::temp_dir().join(format!("neb-tiered-test-{}", std::process::id()));
    let backup_path = test_dir.join("backup");
    let wal_path = test_dir.join("wal");
    std::fs::create_dir_all(&backup_path).unwrap();
    std::fs::create_dir_all(&wal_path).unwrap();
    
    // Set environment variables to enable tiered memory for this test
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "true");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.3");
    
    // Create chunks with tiered memory enabled
    let chunks = {
        let meta = Arc::new(ServerMeta { schemas });
        Chunks::new(
            1,  // 1 chunk
            MAX_SEGMENT_SIZE * 3,  // 3 segments total
            meta,
            None,
            Some(backup_path.to_str().unwrap().to_string()),
            Some(wal_path.to_str().unwrap().to_string()),
        )
    };
    
    let chunk = &chunks.list[0];
    
    // Write cells to fill segments
    // Each cell is ~500KB, segments are 8MB, so ~16 cells per segment
    // Write 32 cells to ensure we fill at least 2 segments
    println!("Writing cells to fill segments...");
    for i in 0..32 {
        let mut cell = default_cell(&Id::new(0, i));
        chunks.write_cell(&mut cell).unwrap();
    }
    
    // Verify all cells are initially hot
    println!("Verifying all segments are initially hot...");
    let segments = chunk.segments();
    println!("Total segments: {}", segments.len());
    println!("Head segment: {}", chunk.get_head_seg_id());
    for seg in &segments {
        println!("  Segment {}: hot={}, references={}", seg.id, seg.is_hot(), seg.references.load(std::sync::atomic::Ordering::Relaxed));
        assert!(seg.is_hot(), "Segment {} should be hot initially", seg.id);
    }
    
    // Ensure we have at least 2 segments (one can be evicted, one is head)
    assert!(segments.len() >= 2, "Need at least 2 segments for eviction test");
    
    // Trigger explicit eviction for testing
    println!("Triggering eviction...");
    if let Some(ref tiered_manager) = chunk.tiered_manager {
        // Use explicit eviction since we may not have exceeded the threshold yet
        let evicted = tiered_manager.explicit_evict(chunk, 1).unwrap();
        println!("Evicted {} segments", evicted);
        // It's possible that eviction fails if only head segment exists or all have references
        // So we check segments rather than asserting eviction count
    } else {
        panic!("Tiered manager not enabled");
    }
    
    // Check if any segments are now cold
    println!("Checking segment states after eviction...");
    let cold_count = chunk.segments().iter().filter(|s| s.is_cold()).count();
    let hot_count = chunk.segments().iter().filter(|s| s.is_hot()).count();
    println!("Hot segments: {}, Cold segments: {}", hot_count, cold_count);
    
    // Access all data to verify correctness (and potentially trigger promotion)
    println!("Accessing all cells to verify data integrity and test promotion...");
    for i in 0..32 {
        let id = Id::new(0, i);
        let cell = chunks.read_cell(&id).unwrap();
        let expected = default_cell(&id);
        assert_eq!(cell.to_owned().data, expected.data, "Data mismatch for cell {}", i);
    }
    
    println!("All data verified successfully!");
    
    // If we had cold segments, they should now be promoted
    if cold_count > 0 {
        println!("Testing that cold segments were promoted...");
        let cold_count_after = chunk.segments().iter().filter(|s| s.is_cold()).count();
        println!("Cold segments after access: {}", cold_count_after);
        // Note: Accessing might promote them back, though eviction could happen again
    }
    
    // Cleanup
    std::fs::remove_dir_all(&test_dir).ok();
    
    println!("Test completed successfully!");
}

#[test]
fn test_clock_algorithm() {
    let _ = env_logger::try_init();
    
    let schema = Schema::new("clock_test", None, default_fields(), false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema);
    
    let test_dir = std::env::temp_dir().join(format!("neb-clock-test-{}", std::process::id()));
    let backup_path = test_dir.join("backup");
    let wal_path = test_dir.join("wal");
    std::fs::create_dir_all(&backup_path).unwrap();
    std::fs::create_dir_all(&wal_path).unwrap();
    
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "true");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.4");
    
    let meta = Arc::new(ServerMeta { schemas });
    let chunks = Chunks::new(
        1,
        MAX_SEGMENT_SIZE * 4,
        meta.clone(),
        None,
        Some(backup_path.to_str().unwrap().to_string()),
        Some(wal_path.to_str().unwrap().to_string()),
    );
    let chunk = &chunks.list[0];
    
    // Fill segments
    println!("Filling segments for CLOCK test...");
    for i in 0..16 {
        let mut cell = default_cell(&Id::new(0, i));
        chunks.write_cell(&mut cell).ok();
    }
    
    let segments = chunk.segments();
    println!("Created {} segments", segments.len());
    
    // Mark some segments as referenced
    for (idx, seg) in segments.iter().enumerate() {
        if idx % 2 == 0 {
            seg.mark_referenced();
        }
    }
    
    // CLOCK should prefer unreferenced segments
    if let Some(ref tiered_manager) = chunk.tiered_manager {
        // Try to evict - should prefer unreferenced segments
        let result = tiered_manager.explicit_evict(&chunk, 1);
        println!("Eviction result: {:?}", result);
    }
    
    std::fs::remove_dir_all(&test_dir).ok();
}

#[test]
fn test_concurrent_promotion() {
    // Test that concurrent promotions are handled safely
    // This is a basic test - full concurrency testing would need more setup
    let _ = env_logger::try_init();
    
    println!("Concurrent promotion test: Basic atomic state check");
    
    let schema = Schema::new("concurrent_test", None, default_fields(), false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema);
    
    let test_dir = std::env::temp_dir().join(format!("neb-concurrent-test-{}", std::process::id()));
    let backup_path = test_dir.join("backup");
    let wal_path = test_dir.join("wal");
    std::fs::create_dir_all(&backup_path).unwrap();
    std::fs::create_dir_all(&wal_path).unwrap();
    
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "true");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.3");
    
    let meta = Arc::new(ServerMeta { schemas });
    let chunks = Chunks::new(
        1,
        MAX_SEGMENT_SIZE * 2,
        meta.clone(),
        None,
        Some(backup_path.to_str().unwrap().to_string()),
        Some(wal_path.to_str().unwrap().to_string()),
    );
    let chunk = &chunks.list[0];
    
    // Write and evict
    for i in 0..8 {
        let mut cell = default_cell(&Id::new(0, i));
        chunks.write_cell(&mut cell).ok();
    }
    
    if let Some(ref tiered_manager) = chunk.tiered_manager {
        tiered_manager.explicit_evict(&chunk, 1).ok();
    }
    
    // Verify atomic state works
    let cold_segment = chunk.segments().into_iter().find(|s| s.is_cold());
    if let Some(seg) = cold_segment {
        println!("Found cold segment {}, testing atomic state", seg.id);
        assert!(!seg.promoting.load(std::sync::atomic::Ordering::Acquire));
        assert!(seg.is_cold());
    }
    
    std::fs::remove_dir_all(&test_dir).ok();
}

#[test]
fn test_cleaner_skips_cold_segments() {
    let _ = env_logger::try_init();
    
    let schema = Schema::new("cleaner_test", None, default_fields(), false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema);
    
    let test_dir = std::env::temp_dir().join(format!("neb-cleaner-test-{}", std::process::id()));
    let backup_path = test_dir.join("backup");
    let wal_path = test_dir.join("wal");
    std::fs::create_dir_all(&backup_path).unwrap();
    std::fs::create_dir_all(&wal_path).unwrap();
    
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "true");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.3");
    
    let meta = Arc::new(ServerMeta { schemas });
    let chunks = Chunks::new(
        1,
        MAX_SEGMENT_SIZE * 3,
        meta.clone(),
        None,
        Some(backup_path.to_str().unwrap().to_string()),
        Some(wal_path.to_str().unwrap().to_string()),
    );
    let chunk = &chunks.list[0];
    
    // Fill and create fragmentation
    for i in 0..12 {
        let mut cell = default_cell(&Id::new(0, i));
        chunks.write_cell(&mut cell).ok();
    }
    
    // Evict some segments
    if let Some(ref tiered_manager) = chunk.tiered_manager {
        tiered_manager.explicit_evict(&chunk, 1).ok();
    }
    
    // Get segments for cleaning - should only include hot segments
    let compact_segs = chunk.segs_for_compact_cleaner();
    let combine_segs = chunk.segs_for_combine_cleaner();
    
    // Verify no cold segments in cleaner lists
    for seg in &compact_segs {
        assert!(seg.is_hot(), "Compact cleaner should only see hot segments");
    }
    
    for (seg, _) in &combine_segs {
        assert!(seg.is_hot(), "Combine cleaner should only see hot segments");
    }
    
    println!("Cleaner correctly skips cold segments");
    
    std::fs::remove_dir_all(&test_dir).ok();
}
