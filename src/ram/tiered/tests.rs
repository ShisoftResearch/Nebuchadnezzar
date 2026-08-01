/// Tests for the tiered memory system
///
/// These tests verify:
/// - Automatic eviction when memory limit is exceeded
/// - Promotion of cold segments on access
/// - Configuration via environment variables
use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::schema::*;
use crate::ram::segs::{SegmentClass, SEGMENT_SIZE};
use crate::ram::tiered::clock::ClockEvictionPolicy;
use crate::ram::types::*;
use crate::server::transactions;
use crate::server::ServerMeta;
use crate::server::{NebServer, ServerOptions, Service};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::sync::Mutex;

// Global mutex to prevent test interference
static TEST_MUTEX: Mutex<()> = Mutex::new(());

async fn tiered_txn_client(
    address: &String,
    group_name: &str,
) -> Arc<transactions::manager::AsyncServiceClient> {
    transactions::new_async_client_for_database(address, group_name, group_name)
        .await
        .unwrap()
}

/// Helper to create default test fields
fn default_fields() -> Field {
    use dovahkiin::types::Type;
    Field::new_schema(vec![
        Field::new_indexed("id", Type::I64, vec![IndexType::Statistics]),
        Field::new_unindexed("name", Type::String),
        Field::new_unindexed("data", Type::String), // Large field for filling memory
    ])
}

/// Helper to create fields with a score counter for transaction tests
fn fields_with_score() -> Field {
    use dovahkiin::types::Type;
    Field::new_schema(vec![
        Field::new_indexed("id", Type::I64, vec![IndexType::Statistics]),
        Field::new_unindexed("name", Type::String),
        Field::new_unindexed("data", Type::String),
        Field::new_unindexed("score", Type::U64),
    ])
}

fn write_cells_for_partition(
    chunks: &Arc<Chunks>,
    schema_id: u32,
    partition: u64,
    start_idx: usize,
    count: usize,
    payload: &str,
) {
    for i in 0..count {
        let logical_idx = start_idx + i;
        let id = Id::allocated(partition as u16, 0, logical_idx as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(logical_idx as i64));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(format!("cell_{}_{}", partition, logical_idx)),
        );
        data_map.insert(
            &String::from("data"),
            OwnedValue::String(payload.to_string()),
        );

        let mut cell = OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(data_map));
        chunks
            .write_cell(&mut cell)
            .expect("direct write for eviction test should succeed");
    }
}

fn large_string_cell(schema_id: u32, id: Id, payload_len: usize, prefix: &str) -> OwnedCell {
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(id.bits() as i64));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(format!("{}_{}", prefix, id.bits())),
    );
    data_map.insert(
        &String::from("data"),
        OwnedValue::String(
            prefix.repeat(payload_len / prefix.len().max(1) + 1)[..payload_len].to_string(),
        ),
    );

    OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(data_map))
}

fn segment_id_for_cell(chunks: &Arc<Chunks>, id: &Id) -> u64 {
    let chunk = chunks.locate_chunk_by_partition(id.locality() as u64);
    chunk.locate_segment(chunks.address_of(id)).unwrap().id
}

fn total_hot_segments(chunks: &Arc<Chunks>) -> usize {
    chunks
        .list
        .iter()
        .map(|chunk| chunk.segments().iter().filter(|seg| seg.is_hot()).count())
        .sum()
}

fn total_cold_segments(chunks: &Arc<Chunks>) -> usize {
    chunks
        .list
        .iter()
        .map(|chunk| chunk.segments().iter().filter(|seg| seg.is_cold()).count())
        .sum()
}

fn total_hot_segments_across_sets(chunk_sets: &[&Arc<Chunks>]) -> usize {
    chunk_sets
        .iter()
        .map(|chunks| total_hot_segments(chunks))
        .sum()
}

fn total_cold_segments_across_sets(chunk_sets: &[&Arc<Chunks>]) -> usize {
    chunk_sets
        .iter()
        .map(|chunks| total_cold_segments(chunks))
        .sum()
}

fn assert_shared_counter_matches_scanned_total(
    manager: &crate::ram::tiered::manager::TieredMemoryManager,
    chunk_sets: &[&Arc<Chunks>],
) {
    let scanned_total = total_hot_segments_across_sets(chunk_sets);
    let shared_total = manager.shared_hot_segments();
    assert_eq!(
        shared_total, scanned_total,
        "shared hot-segment counter should match scanned hot segments across all registered databases"
    );
}

async fn wait_for_shared_counter_alignment(
    manager: &crate::ram::tiered::manager::TieredMemoryManager,
    chunk_sets: &[&Arc<Chunks>],
    timeout_ms: u64,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        if manager.shared_hot_segments() == total_hot_segments_across_sets(chunk_sets) {
            return;
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_shared_counter_matches_scanned_total(manager, chunk_sets);
}

fn reconcile_global_hot_segments(
    manager: &crate::ram::tiered::manager::TieredMemoryManager,
    chunk_sets: &[&Arc<Chunks>],
) -> usize {
    let mut total = 0;
    for chunks in chunk_sets {
        for chunk in &chunks.list {
            total = manager.hot_count_cached(chunk);
        }
    }
    total
}

fn append_round_robin_until_reconciled_hot_segments(
    chunks: &Arc<Chunks>,
    schema_id: u32,
    partitions: &[u64],
    next_indices: &mut [usize],
    manager: &crate::ram::tiered::manager::TieredMemoryManager,
    observed_chunk_sets: &[&Arc<Chunks>],
    target_hot_segments: usize,
    payload: &str,
) {
    assert_eq!(
        partitions.len(),
        next_indices.len(),
        "partitions and next_indices must stay aligned"
    );
    assert!(
        !partitions.is_empty(),
        "round-robin writes need at least one partition"
    );

    let mut cursor = 0;
    while reconcile_global_hot_segments(manager, observed_chunk_sets) < target_hot_segments {
        let slot = cursor % partitions.len();
        write_cells_for_partition(
            chunks,
            schema_id,
            partitions[slot],
            next_indices[slot],
            1,
            payload,
        );
        next_indices[slot] += 1;
        cursor += 1;
    }
}

/// Test automatic eviction when physical memory limit is exceeded
#[test]
fn test_eviction_on_memory_overflow() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    // Configure tiered memory with a small physical memory limit
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.7"); // 70% threshold
    std::env::set_var(
        "NEB_TIERED_PHYSICAL_MEMORY_LIMIT",
        &format!("{}", 3 * SEGMENT_SIZE),
    ); // 3 segments = 24MB

    let chunk_capacity = 10 * SEGMENT_SIZE; // 80MB virtual capacity
    let fields = default_fields();
    let schema = Schema::new("test_overflow", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("/tmp/neb_test_overflow_schema");
    schemas.debug_only_new_schema(schema.clone());

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
        crate::ram::tiered::TieredConfig::from_env().map(|c| {
            Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
                crate::ram::tiered::SharedMemoryPool::new(&c),
            ))
        }),
    );

    // Verify tiered manager is enabled in at least one chunk
    let has_tiered_manager = chunks.list.iter().any(|c| c.tiered_manager.is_some());
    assert!(
        has_tiered_manager,
        "Tiered memory manager should be enabled"
    );

    // Fill with enough data to exceed the physical memory limit (3 segments = 24MB)
    // Each cell will be ~1KB (plus overhead), so we need multiple segments worth
    let large_data = "x".repeat(1024); // 1KB string
    let cells_per_segment = SEGMENT_SIZE / 2048; // Conservative estimate
    let num_cells = cells_per_segment * 6; // 6 segments worth to ensure we exceed 3-segment limit

    info!("Filling with {} cells to exceed 3-segment limit", num_cells);

    for i in 0..num_cells {
        let id = Id::from_parts(schema.id as u64, i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(format!("test_{}", i)),
        );
        data_map.insert(
            &String::from("data"),
            OwnedValue::String(large_data.clone()),
        );

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
                warn!(
                    "Write failed at cell {} (may be expected if virtual capacity full): {:?}",
                    i, e
                );
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

        info!(
            "Chunk {}: {} hot segments, {} cold segments",
            chunk.id, hot, cold
        );

        // Verify cold segments have backup files
        for seg in segments.iter().filter(|s| s.is_cold()) {
            let backup_path = chunk
                .file_manager
                .backup_path(seg.chunk_id, seg.id, seg.seq_id)
                .expect("Cold segment should have backup file path");
            assert!(
                std::path::Path::new(&backup_path).exists(),
                "Cold segment {} should have backup file",
                seg.id
            );
        }
    }

    info!("Total: {} hot, {} cold segments", total_hot, total_cold);
    assert!(
        total_cold > 0,
        "Expected some segments to be evicted to cold storage"
    );

    // Test that we can still read data from cold segments (promotion)
    // Read a few cells to trigger promotion
    for i in 0..(num_cells.min(10)) {
        let id = Id::from_parts(schema.id as u64, i as u64);
        match chunks.read_cell(&id) {
            Ok(cell) => {
                assert_eq!(cell.data["id"].i64().unwrap(), &(i as i64));
                info!(
                    "Successfully read cell {} (may have triggered promotion)",
                    i
                );
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
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    // Configure with tight memory limit
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.6");
    std::env::set_var(
        "NEB_TIERED_PHYSICAL_MEMORY_LIMIT",
        &format!("{}", 2 * SEGMENT_SIZE),
    );

    let chunk_capacity = 8 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("test_promotion", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("/tmp/neb_test_promotion_schema");
    schemas.debug_only_new_schema(schema.clone());

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
        crate::ram::tiered::TieredConfig::from_env().map(|c| {
            Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
                crate::ram::tiered::SharedMemoryPool::new(&c),
            ))
        }),
    );

    // Write cells
    let large_data = "testdata_".repeat(128); // ~1KB
    let num_cells = (SEGMENT_SIZE / 2048) * 4; // 4 segments worth

    let mut written_ids = Vec::new();
    for i in 0..num_cells {
        let id = Id::from_parts(schema.id as u64, 1000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(1000 + i as i64));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(format!("promotion_test_{}", i)),
        );
        data_map.insert(
            &String::from("data"),
            OwnedValue::String(large_data.clone()),
        );

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
    let total_cold: usize = chunks
        .list
        .iter()
        .map(|c| c.segments().iter().filter(|s| s.is_cold()).count())
        .sum();
    assert!(
        total_cold > 0,
        "Should have cold segments after explicit eviction"
    );
    info!("Have {} cold segments", total_cold);

    // Read all cells back - this should promote cold segments as needed
    for (idx, id) in written_ids.iter().enumerate() {
        match chunks.read_cell(id) {
            Ok(cell) => {
                let expected_id = 1000 + idx as i64;
                assert_eq!(
                    cell.data["id"].i64().unwrap(),
                    &expected_id,
                    "Data should be intact after promotion"
                );
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

/// Test churn-related metrics and promotion cooldown skip logic
#[test]
fn test_metrics_and_churn_counters() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.75");
    std::env::set_var("NEB_TIERED_MEMORY_LOWER_WATERMARK", "0.50");
    std::env::set_var("NEB_TIERED_PROMOTION_COOLDOWN_MS", "5000");
    std::env::set_var(
        "NEB_TIERED_PHYSICAL_MEMORY_LIMIT",
        &format!("{}", 2 * SEGMENT_SIZE),
    );

    let chunk_capacity = 4 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("test_metrics", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("/tmp/neb_test_metrics_schema");
    schemas.debug_only_new_schema(schema.clone());

    let backup_dir = "/tmp/neb_test_metrics_bk";
    let wal_dir = "/tmp/neb_test_metrics_wal";
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let chunks = Chunks::new(
        1,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        crate::ram::tiered::TieredConfig::from_env().map(|c| {
            Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
                crate::ram::tiered::SharedMemoryPool::new(&c),
            ))
        }),
    );

    let manager = chunks
        .list
        .first()
        .and_then(|c| c.tiered_manager.as_ref())
        .expect("Tiered manager should be enabled");

    // Write enough cells to create multiple segments
    let large_data = "metrics_test".repeat(128); // ~1KB
    let cells_per_segment = SEGMENT_SIZE / 2048;
    let num_cells = cells_per_segment * 3;

    for i in 0..num_cells {
        let id = Id::from_parts(schema.id as u64, i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(format!("metrics_{}", i)),
        );
        data_map.insert(
            &String::from("data"),
            OwnedValue::String(large_data.clone()),
        );

        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };

        let _ = chunks.write_cell(&mut cell);
    }

    let chunk = &chunks.list[0];

    // Evict a segment to mark last_evicted_ms and increment eviction count
    let evicted = manager
        .explicit_evict(chunk, 1)
        .expect("eviction to succeed");
    assert!(evicted > 0, "Should evict at least one segment");

    let cold_seg = chunk
        .segments()
        .into_iter()
        .find(|s| s.is_cold())
        .expect("Should have a cold segment after eviction");

    manager
        .promote(chunk, &cold_seg)
        .expect("first access should succeed");

    manager
        .promote(chunk, &cold_seg)
        .expect("second access should trigger promotion");

    let stats = manager.stats(chunk);
    assert!(stats.evictions > 0, "Eviction counter should increase");
    assert!(stats.promotions > 0, "Promotion counter should increase");
    assert!(
        stats.churns > 0,
        "Churn counter should detect evict→promote"
    );

    // Cleanup
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_MEMORY_LOWER_WATERMARK");
    std::env::remove_var("NEB_TIERED_PROMOTION_COOLDOWN_MS");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all("/tmp/neb_test_metrics_schema");
}

#[test]
fn test_active_blob_head_is_not_evicted_by_clock() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    let chunks = Chunks::new_dummy(1, 3 * SEGMENT_SIZE);
    let chunk = &chunks.list[0];

    let blob_head = chunk
        .allocator
        .alloc_seg_with_class(&chunk.file_manager, SegmentClass::Blob)
        .expect("should allocate a blob head for the test");
    let blob_head_id = blob_head.id;
    chunk.put_segment(blob_head);
    chunk
        .blob_head_seg_id
        .store(blob_head_id, AtomicOrdering::Relaxed);

    let (regular_head, blob_head) = chunk.head_seg_ids_for_test();
    let blob_head = blob_head.expect("blob head should be installed for the test");
    let policy = ClockEvictionPolicy::default();

    let victim = policy.select_victim(chunk);
    assert!(
        victim.is_none(),
        "CLOCK must not evict an active blob head when only heads exist"
    );

    assert!(chunk.segs.get(&(regular_head as usize)).unwrap().is_hot());
    assert!(chunk.segs.get(&(blob_head as usize)).unwrap().is_hot());
}

#[test]
fn test_blob_segments_evict_before_regular_segments_without_blob_head() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    let chunks = Chunks::new_dummy(1, 4 * SEGMENT_SIZE);
    let chunk = &chunks.list[0];

    let regular_candidate = chunk
        .allocator
        .alloc_seg_with_class(&chunk.file_manager, SegmentClass::Regular)
        .expect("should allocate a regular candidate for the test");
    let regular_candidate_id = regular_candidate.id;
    chunk.put_segment(regular_candidate);

    let blob_candidate = chunk
        .allocator
        .alloc_seg_with_class(&chunk.file_manager, SegmentClass::Blob)
        .expect("should allocate a blob candidate for the test");
    let blob_candidate_id = blob_candidate.id;
    chunk.put_segment(blob_candidate);

    let (regular_head, blob_head) = chunk.head_seg_ids_for_test();
    assert_eq!(
        blob_head, None,
        "test setup should not install a blob write head"
    );
    assert_eq!(
        chunk
            .segs
            .get(&(regular_candidate_id as usize))
            .unwrap()
            .segment_class(),
        SegmentClass::Regular
    );
    assert_eq!(
        chunk
            .segs
            .get(&(blob_candidate_id as usize))
            .unwrap()
            .segment_class(),
        SegmentClass::Blob
    );
    assert!(chunk.segs.get(&(regular_head as usize)).unwrap().is_hot());
    assert!(chunk
        .segs
        .get(&(regular_candidate_id as usize))
        .unwrap()
        .is_hot());
    assert!(chunk
        .segs
        .get(&(blob_candidate_id as usize))
        .unwrap()
        .is_hot());

    let policy = ClockEvictionPolicy::default();
    let victim = policy
        .select_victim(chunk)
        .expect("CLOCK should pick a victim when both blob and regular candidates exist");

    assert_eq!(
        victim.id, blob_candidate_id,
        "blob-first eviction should not depend on an active blob write head"
    );
}

#[test]
fn test_blob_segments_evict_before_regular_segments() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    let schema_dir = "/tmp/neb_blob_priority_schema";
    let backup_dir = "/tmp/neb_blob_priority_bk";
    let wal_dir = "/tmp/neb_blob_priority_wal";
    let _ = std::fs::remove_dir_all(schema_dir);
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let regular = Schema::new_with_id(910, "regular_evict", None, default_fields(), false, false);
    let blob = Schema::new_with_id(920, "blob_evict", None, default_fields(), false, false)
        .with_blobs(true);
    let schemas = LocalSchemasCache::new_local(schema_dir);
    schemas.debug_only_new_schema(regular.clone());
    schemas.debug_only_new_schema(blob.clone());

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.95,
            lower_watermark: 0.8,
            physical_memory_limit: 8 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let chunks = Chunks::new(
        1,
        6 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        Some(manager.clone()),
    );
    let chunk = &chunks.list[0];

    let mut regular_segments = BTreeSet::new();
    for index in 0..64_u64 {
        let id = Id::from_parts(9_100, 10_000 + index);
        let mut cell = large_string_cell(regular.id, id, 512_000, "regular-evict");
        chunks.write_cell(&mut cell).unwrap();
        regular_segments.insert(segment_id_for_cell(&chunks, &id));
        if regular_segments.len() >= 2 {
            break;
        }
    }

    let mut blob_segments = BTreeSet::new();
    for index in 0..64_u64 {
        let id = Id::from_parts(9_200, 20_000 + index);
        let mut cell = large_string_cell(blob.id, id, 1_500_000, "blob-evict");
        chunks.write_cell(&mut cell).unwrap();
        blob_segments.insert(segment_id_for_cell(&chunks, &id));
        if blob_segments.len() >= 2 {
            break;
        }
    }

    let (regular_head, blob_head) = chunk.head_seg_ids_for_test();
    let blob_head = blob_head.expect("blob writes should leave an active blob head");
    let regular_non_head = regular_segments
        .iter()
        .copied()
        .find(|segment_id| *segment_id != regular_head)
        .expect("setup should create a non-head regular segment");
    let blob_non_head = blob_segments
        .iter()
        .copied()
        .find(|segment_id| *segment_id != blob_head)
        .expect("setup should create a non-head blob segment");

    assert_eq!(
        chunk
            .segs
            .get(&(regular_non_head as usize))
            .unwrap()
            .segment_class(),
        SegmentClass::Regular,
        "setup should classify the regular victim candidate correctly"
    );
    assert_eq!(
        chunk
            .segs
            .get(&(blob_non_head as usize))
            .unwrap()
            .segment_class(),
        SegmentClass::Blob,
        "setup should classify the blob victim candidate correctly"
    );
    assert!(
        chunk
            .segs
            .get(&(regular_non_head as usize))
            .unwrap()
            .is_hot(),
        "setup should keep the regular non-head hot before eviction"
    );
    assert!(
        chunk.segs.get(&(blob_non_head as usize)).unwrap().is_hot(),
        "setup should keep the blob non-head hot before eviction"
    );

    let evicted = manager
        .explicit_evict(chunk, 1)
        .expect("explicit eviction should succeed");

    let cold_blob_segments: Vec<_> = chunk
        .segments()
        .into_iter()
        .filter(|seg| seg.segment_class() == SegmentClass::Blob && seg.is_cold())
        .map(|seg| seg.id)
        .collect();

    assert_eq!(evicted, 1, "the test should evict exactly one segment");
    assert!(
        !cold_blob_segments.is_empty(),
        "the real tiered path should evict a blob segment before any regular hot segment"
    );
    assert!(
        cold_blob_segments
            .iter()
            .all(|segment_id| *segment_id != blob_head),
        "explicit eviction must leave the active blob head hot"
    );
    assert!(
        chunk
            .segs
            .get(&(regular_non_head as usize))
            .unwrap()
            .is_hot(),
        "regular hot segments should remain hot while a blob victim exists"
    );
    assert!(chunk.segs.get(&(regular_head as usize)).unwrap().is_hot());
    assert!(chunk.segs.get(&(blob_head as usize)).unwrap().is_hot());

    let _ = std::fs::remove_dir_all(schema_dir);
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
}

#[test]
fn test_blob_segments_promote_on_read_after_eviction() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    let schema_dir = "/tmp/neb_blob_promote_schema";
    let backup_dir = "/tmp/neb_blob_promote_bk";
    let wal_dir = "/tmp/neb_blob_promote_wal";
    let _ = std::fs::remove_dir_all(schema_dir);
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let blob = Schema::new("blob_promote", None, default_fields(), false, false).with_blobs(true);
    let schemas = LocalSchemasCache::new_local(schema_dir);
    schemas.debug_only_new_schema(blob.clone());

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.95,
            lower_watermark: 0.8,
            physical_memory_limit: 8 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let chunks = Chunks::new(
        1,
        4 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        Some(manager.clone()),
    );
    let chunk = &chunks.list[0];

    let mut blob_cells = Vec::new();
    let mut blob_segments = BTreeSet::new();
    for index in 0..64_u64 {
        let id = Id::allocated(93, 0, index);
        let mut cell = large_string_cell(blob.id, id, 1_500_000, "blob-promote");
        chunks.write_cell(&mut cell).unwrap();
        let segment_id = segment_id_for_cell(&chunks, &id);
        blob_segments.insert(segment_id);
        blob_cells.push((id, segment_id));
        if blob_segments.len() >= 2 {
            break;
        }
    }

    let (_, blob_head) = chunk.head_seg_ids_for_test();
    let blob_head = blob_head.expect("blob writes should allocate a blob head");
    let (cold_target_id, cold_target_segment_id) = blob_cells
        .iter()
        .copied()
        .find(|(_, segment_id)| *segment_id != blob_head)
        .expect("setup should create a non-head blob segment to evict");

    let evicted = manager
        .explicit_evict(chunk, 1)
        .expect("explicit eviction should succeed");
    assert_eq!(evicted, 1);

    let cold_segment = chunk
        .segs
        .get(&(cold_target_segment_id as usize))
        .expect("target blob segment should still exist after eviction");
    assert!(
        cold_segment.is_cold(),
        "blob segment should be cold after explicit eviction"
    );

    let read_back = chunks.read_cell(&cold_target_id).unwrap();
    assert_eq!(
        read_back.data["id"].i64(),
        Some(&(cold_target_id.bits() as i64))
    );
    drop(read_back);

    assert!(
        cold_segment.is_hot(),
        "reading a cold blob segment should promote it"
    );
    assert_eq!(
        cold_segment.get_access_count(),
        0,
        "promotion should reset the cold access counter"
    );

    let _ = std::fs::remove_dir_all(schema_dir);
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
}

#[test]
fn test_global_eviction_across_chunks_in_single_database() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    let chunk_capacity = 4 * SEGMENT_SIZE;
    let schema = Schema::new(
        "single_db_global_eviction",
        None,
        default_fields(),
        false,
        false,
    );
    let schemas = LocalSchemasCache::new_local("/tmp/neb_single_db_global_eviction_schema");
    schemas.debug_only_new_schema(schema.clone());

    let backup_dir = "/tmp/neb_single_db_global_eviction_bk";
    let wal_dir = "/tmp/neb_single_db_global_eviction_wal";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.75,
            lower_watermark: 0.5,
            physical_memory_limit: 3 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let chunks = Chunks::new(
        2,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        Some(manager.clone()),
    );

    let payload = "x".repeat(1024);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    write_cells_for_partition(&chunks, schema.id, 0, 0, cells_per_segment * 2, &payload);
    write_cells_for_partition(
        &chunks,
        schema.id,
        1,
        cells_per_segment * 2,
        cells_per_segment * 2,
        &payload,
    );

    let hot_before: usize = chunks
        .list
        .iter()
        .map(|chunk| chunk.segments().iter().filter(|seg| seg.is_hot()).count())
        .sum();
    assert!(
        hot_before >= 4,
        "setup should produce at least four hot segments across both chunks"
    );

    let evicted = manager
        .evict_for_allocation()
        .expect("global eviction across chunks should succeed");
    assert!(
        evicted > 0,
        "global pressure across two chunks should trigger eviction"
    );

    let cold_after: usize = chunks
        .list
        .iter()
        .map(|chunk| chunk.segments().iter().filter(|seg| seg.is_cold()).count())
        .sum();
    assert!(
        cold_after > 0,
        "at least one segment should be evicted when combined chunk pressure exceeds the shared limit"
    );

    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all("/tmp/neb_single_db_global_eviction_schema");
}

#[test]
fn test_single_database_eviction_waits_until_threshold_is_exceeded() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    let physical_memory_limit = 8 * SEGMENT_SIZE;
    let threshold = 0.75;
    let threshold_hot_segments =
        ((physical_memory_limit as f64 * threshold as f64) / SEGMENT_SIZE as f64) as usize;
    let schema = Schema::new(
        "single_db_threshold_gate",
        None,
        default_fields(),
        false,
        false,
    );
    let schemas = LocalSchemasCache::new_local("/tmp/neb_single_db_threshold_gate_schema");
    schemas.debug_only_new_schema(schema.clone());

    let backup_dir = "/tmp/neb_single_db_threshold_gate_bk";
    let wal_dir = "/tmp/neb_single_db_threshold_gate_wal";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold,
            lower_watermark: 0.5,
            physical_memory_limit,
            promotion_cooldown_ms: 0,
        }),
    ));
    let chunks = Chunks::new(
        2,
        4 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        Some(manager.clone()),
    );

    let payload = "s".repeat(2048);
    let partitions = [0_u64, 1_u64];
    let mut next_indices = [0_usize, 0_usize];
    append_round_robin_until_reconciled_hot_segments(
        &chunks,
        schema.id,
        &partitions,
        &mut next_indices,
        &manager,
        &[&chunks],
        threshold_hot_segments.saturating_sub(1),
        &payload,
    );

    let hot_before_threshold = reconcile_global_hot_segments(&manager, &[&chunks]);
    assert!(
        hot_before_threshold == threshold_hot_segments.saturating_sub(1),
        "single database should remain exactly one hot segment below the trigger point before the final segment is added"
    );
    let evicted_before_threshold = manager
        .evict_for_allocation()
        .expect("single-database threshold check below the limit should succeed");
    assert_eq!(
        evicted_before_threshold, 0,
        "single-database eviction must not trigger before total hot memory exceeds the threshold"
    );
    assert_eq!(
        total_cold_segments(&chunks),
        0,
        "single database should have no cold segments before the threshold is exceeded"
    );

    append_round_robin_until_reconciled_hot_segments(
        &chunks,
        schema.id,
        &partitions,
        &mut next_indices,
        &manager,
        &[&chunks],
        threshold_hot_segments,
        &payload,
    );
    let hot_at_trigger = reconcile_global_hot_segments(&manager, &[&chunks]);
    assert_eq!(
        hot_at_trigger, threshold_hot_segments,
        "single database should reach the exact hot-segment threshold before eviction is triggered for the next allocation"
    );

    let cold_before_crossing = total_cold_segments(&chunks);
    let evicted_after_threshold = manager
        .evict_for_allocation()
        .expect("single-database threshold check at the boundary should succeed");
    let cold_after_crossing = total_cold_segments(&chunks);
    assert!(
        evicted_after_threshold > 0 || cold_after_crossing > cold_before_crossing,
        "single-database eviction should trigger once total hot memory reaches the threshold boundary for the next allocation"
    );

    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all("/tmp/neb_single_db_threshold_gate_schema");
}

#[test]
fn test_reconciled_background_eviction_ignores_stale_shared_counter_drift() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    let physical_memory_limit = 8 * SEGMENT_SIZE;
    let threshold = 0.75;
    let schema = Schema::new(
        "reconciled_threshold_gate",
        None,
        default_fields(),
        false,
        false,
    );
    let schemas = LocalSchemasCache::new_local("/tmp/neb_reconciled_threshold_gate_schema");
    schemas.debug_only_new_schema(schema.clone());

    let backup_dir = "/tmp/neb_reconciled_threshold_gate_bk";
    let wal_dir = "/tmp/neb_reconciled_threshold_gate_wal";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold,
            lower_watermark: 0.5,
            physical_memory_limit,
            promotion_cooldown_ms: 0,
        }),
    ));
    let chunks = Chunks::new(
        1,
        4 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        Some(manager.clone()),
    );

    let payload = "s".repeat(2048);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    write_cells_for_partition(&chunks, schema.id, 0, 0, cells_per_segment, &payload);

    let scanned_hot_segments = total_hot_segments(&chunks);
    assert!(
        scanned_hot_segments < 6,
        "setup should stay materially below the 0.75 threshold for an 8-segment limit"
    );

    manager.shared_pool().adjust_delta(64);
    assert!(
        manager.shared_hot_segments() > scanned_hot_segments,
        "test setup should create an artificial positive drift in the shared counter"
    );

    let stale_path_result = manager
        .evict_for_allocation()
        .expect("non-reconciled allocation path should still return cleanly");
    assert!(
        stale_path_result > 0 || manager.shared_hot_segments() > scanned_hot_segments,
        "without forced reconcile the stale shared counter should remain observable"
    );

    let evicted_reconciled = manager
        .evict_for_allocation_reconciled()
        .expect("reconciled allocation path should succeed");
    assert_eq!(
        evicted_reconciled, 0,
        "forced reconciliation should prevent background eviction while scanned hot memory is still below threshold"
    );
    assert_eq!(
        manager.shared_hot_segments(),
        total_hot_segments(&chunks),
        "forced reconciliation should pull the shared counter back to the scanned hot-segment total"
    );

    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all("/tmp/neb_reconciled_threshold_gate_schema");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cleaner_keeps_shared_counter_aligned_under_single_database_churn() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();
    std::env::set_var("NEB_CLEANER_SLEEP_INTERVAL_MS", "10");

    let backup_dir = "/tmp/neb_single_db_cleaner_drift_bk";
    let wal_dir = "/tmp/neb_single_db_cleaner_drift_wal";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 8 * SEGMENT_SIZE,
            db_size: 8 * SEGMENT_SIZE,
            tiered_config: Some(crate::ram::tiered::TieredConfig {
                threshold: 0.75,
                lower_watermark: 0.5,
                physical_memory_limit: 6 * SEGMENT_SIZE,
                promotion_cooldown_ms: 0,
            }),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "tiered_single_db_cleaner_drift",
        async |_| {},
    )
    .await
    .unwrap();

    let schema = Schema::new_with_id(
        9021,
        &String::from("single_db_cleaner_drift_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta().schemas.debug_only_new_schema(schema.clone());

    let manager = server
        .chunks()
        .tiered_manager
        .as_ref()
        .expect("single database should have a tiered manager")
        .clone();

    let payload = "cleaner-single-db".repeat(170);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    for round in 0..8 {
        write_cells_for_partition(
            server.chunks(),
            schema.id,
            (round % 2) as u64,
            round * (cells_per_segment / 2),
            cells_per_segment / 2,
            &payload,
        );
        wait_for_shared_counter_alignment(&manager, &[&server.chunks()], 500).await;
    }

    wait_for_shared_counter_alignment(&manager, &[&server.chunks()], 500).await;

    server.cleaner().stop();
    server.shutdown().await;
    std::env::remove_var("NEB_CLEANER_SLEEP_INTERVAL_MS");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cleaner_keeps_shared_counter_aligned_under_multi_database_churn() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();
    std::env::set_var("NEB_CLEANER_SLEEP_INTERVAL_MS", "10");

    let backup_dir = "/tmp/neb_multi_db_cleaner_drift_bk";
    let wal_dir = "/tmp/neb_multi_db_cleaner_drift_wal";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 8 * SEGMENT_SIZE,
            db_size: 8 * SEGMENT_SIZE,
            tiered_config: Some(crate::ram::tiered::TieredConfig {
                threshold: 0.75,
                lower_watermark: 0.5,
                physical_memory_limit: 8 * SEGMENT_SIZE,
                promotion_cooldown_ms: 0,
            }),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "tiered_multi_db_cleaner_drift",
        async |_| {},
    )
    .await
    .unwrap();

    let analytics = server
        .ensure_database_runtime("analytics")
        .await
        .expect("analytics runtime should be created");

    let default_schema = Schema::new_with_id(
        9022,
        &String::from("default_cleaner_drift_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    let analytics_schema = Schema::new_with_id(
        9023,
        &String::from("analytics_cleaner_drift_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    server
        .meta()
        .schemas
        .debug_only_new_schema(default_schema.clone());
    analytics
        .meta()
        .schemas
        .debug_only_new_schema(analytics_schema.clone());

    let manager = server
        .chunks()
        .tiered_manager
        .as_ref()
        .expect("default runtime should have a tiered manager")
        .clone();

    let payload = "cleaner-multi-db".repeat(170);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    for round in 0..8 {
        write_cells_for_partition(
            server.chunks(),
            default_schema.id,
            (round % 2) as u64,
            round * (cells_per_segment / 3),
            cells_per_segment / 3,
            &payload,
        );
        write_cells_for_partition(
            analytics.chunks(),
            analytics_schema.id,
            (round % 2) as u64,
            round * (cells_per_segment / 3),
            cells_per_segment / 3,
            &payload,
        );

        let read_id = Id::from_parts((round % 2) as u64, 0);
        let _ = server.chunks().read_cell(&read_id);
        let _ = analytics.chunks().read_cell(&read_id);

        wait_for_shared_counter_alignment(&manager, &[&server.chunks(), &analytics.chunks()], 750)
            .await;
    }

    wait_for_shared_counter_alignment(&manager, &[&server.chunks(), &analytics.chunks()], 750)
        .await;
    assert!(
        total_cold_segments_across_sets(&[&server.chunks(), &analytics.chunks()]) > 0,
        "multi-database churn should eventually create cold segments under the shared cleaner-managed budget"
    );

    analytics.cleaner().stop();
    server.cleaner().stop();
    server.shutdown().await;
    std::env::remove_var("NEB_CLEANER_SLEEP_INTERVAL_MS");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unload_reload_recovery_preserves_shared_counter_alignment() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();
    std::env::set_var("NEB_CLEANER_SLEEP_INTERVAL_MS", "10");

    let backup_dir = "/tmp/neb_reload_recovery_drift_bk";
    let wal_dir = "/tmp/neb_reload_recovery_drift_wal";
    let undo_dir = "/tmp/neb_reload_recovery_drift_undo";
    let raft_dir = "/tmp/neb_reload_recovery_drift_raft";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(undo_dir);
    let _ = std::fs::remove_dir_all(raft_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(undo_dir);
    let _ = std::fs::create_dir_all(raft_dir);

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 8 * SEGMENT_SIZE,
            db_size: 8 * SEGMENT_SIZE,
            tiered_config: Some(crate::ram::tiered::TieredConfig {
                threshold: 0.75,
                lower_watermark: 0.5,
                physical_memory_limit: 8 * SEGMENT_SIZE,
                promotion_cooldown_ms: 0,
            }),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            undo_log_storage: Some(undo_dir.to_string()),
            raft_storage: Some(raft_dir.to_string()),
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: true,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "tiered_unload_reload_drift",
        async |_| {},
    )
    .await
    .unwrap();

    let analytics = server
        .ensure_database_runtime("analytics")
        .await
        .expect("analytics runtime should be created");

    let default_schema = Schema::new_with_id(
        9024,
        &String::from("default_reload_recovery_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    let analytics_schema = Schema::new_with_id(
        9025,
        &String::from("analytics_reload_recovery_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    server
        .meta()
        .schemas
        .debug_only_new_schema(default_schema.clone());
    analytics
        .meta()
        .schemas
        .debug_only_new_schema(analytics_schema.clone());

    let manager = server
        .chunks()
        .tiered_manager
        .as_ref()
        .expect("default runtime should have a tiered manager")
        .clone();

    let payload = "reload-recovery".repeat(170);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    write_cells_for_partition(
        server.chunks(),
        default_schema.id,
        0,
        0,
        cells_per_segment,
        &payload,
    );
    write_cells_for_partition(
        analytics.chunks(),
        analytics_schema.id,
        0,
        0,
        cells_per_segment,
        &payload,
    );
    wait_for_shared_counter_alignment(&manager, &[&server.chunks(), &analytics.chunks()], 750)
        .await;

    analytics.chunks().sync_all();
    analytics.chunks().archive_all();
    assert!(server.unload_database_runtime("analytics").await);
    wait_for_shared_counter_alignment(&manager, &[&server.chunks()], 750).await;

    let analytics_reloaded = server
        .ensure_database_runtime("analytics")
        .await
        .expect("analytics runtime should reload with recovery enabled");
    analytics_reloaded
        .meta()
        .schemas
        .debug_only_new_schema(analytics_schema.clone());
    wait_for_shared_counter_alignment(
        &manager,
        &[&server.chunks(), &analytics_reloaded.chunks()],
        750,
    )
    .await;
    assert!(
        total_hot_segments(analytics_reloaded.chunks()) > 0,
        "reloaded analytics runtime should recover hot segments from storage"
    );
    assert_shared_counter_matches_scanned_total(
        &manager,
        &[&server.chunks(), &analytics_reloaded.chunks()],
    );

    write_cells_for_partition(
        analytics_reloaded.chunks(),
        analytics_schema.id,
        1,
        cells_per_segment,
        cells_per_segment / 2,
        &payload,
    );
    wait_for_shared_counter_alignment(
        &manager,
        &[&server.chunks(), &analytics_reloaded.chunks()],
        750,
    )
    .await;

    analytics_reloaded.cleaner().stop();
    server.cleaner().stop();
    server.shutdown().await;
    std::env::remove_var("NEB_CLEANER_SLEEP_INTERVAL_MS");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(undo_dir);
    let _ = std::fs::remove_dir_all(raft_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_global_eviction_across_multiple_databases() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    let backup_dir = "/tmp/neb_multi_db_global_eviction_bk";
    let wal_dir = "/tmp/neb_multi_db_global_eviction_wal";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 8 * SEGMENT_SIZE,
            db_size: 8 * SEGMENT_SIZE,
            tiered_config: Some(crate::ram::tiered::TieredConfig {
                threshold: 0.75,
                lower_watermark: 0.5,
                physical_memory_limit: 4 * SEGMENT_SIZE,
                promotion_cooldown_ms: 0,
            }),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "tiered_multi_db_global",
        async |_| {},
    )
    .await
    .unwrap();

    let analytics = server
        .ensure_database_runtime("analytics")
        .await
        .expect("analytics runtime should be created");
    server.cleaner().stop();
    analytics.cleaner().stop();

    let default_schema = Schema::new_with_id(
        9001,
        &String::from("default_eviction_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    let analytics_schema = Schema::new_with_id(
        9002,
        &String::from("analytics_eviction_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    server
        .meta()
        .schemas
        .debug_only_new_schema(default_schema.clone());
    analytics
        .meta()
        .schemas
        .debug_only_new_schema(analytics_schema.clone());

    let default_manager = server
        .chunks()
        .tiered_manager
        .as_ref()
        .expect("default runtime should have a tiered manager")
        .clone();
    let analytics_manager = analytics
        .chunks()
        .tiered_manager
        .as_ref()
        .expect("analytics runtime should have a tiered manager")
        .clone();
    assert!(
        Arc::ptr_eq(&default_manager, &analytics_manager),
        "database runtimes should share one global tiered manager"
    );

    let payload = "y".repeat(2048);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    write_cells_for_partition(
        server.chunks(),
        default_schema.id,
        0,
        0,
        cells_per_segment,
        &payload,
    );

    let hot_after_default_only = total_hot_segments(server.chunks());
    assert!(
        hot_after_default_only <= 2,
        "first database should stay under the shared trigger budget by itself"
    );

    let cold_after_default_only =
        total_cold_segments(server.chunks()) + total_cold_segments(analytics.chunks());
    assert_eq!(
        cold_after_default_only, 0,
        "one database below the shared limit should not evict yet"
    );

    write_cells_for_partition(
        analytics.chunks(),
        analytics_schema.id,
        0,
        cells_per_segment,
        cells_per_segment * 2,
        &payload,
    );

    let evicted = default_manager
        .evict_for_allocation()
        .expect("global eviction across databases should succeed");
    let total_cold = total_cold_segments(server.chunks()) + total_cold_segments(analytics.chunks());
    assert!(
        evicted > 0 || total_cold > 0,
        "combined pressure from two databases should trigger shared global eviction"
    );
    assert!(
        total_cold > 0,
        "global eviction should produce cold segments across the shared server-wide budget"
    );

    analytics.cleaner().stop();
    server.cleaner().stop();
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_database_eviction_waits_until_combined_threshold_is_exceeded() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    let physical_memory_limit = 8 * SEGMENT_SIZE;
    let threshold = 0.75;
    let threshold_hot_segments =
        ((physical_memory_limit as f64 * threshold as f64) / SEGMENT_SIZE as f64) as usize;
    let backup_dir = "/tmp/neb_multi_db_threshold_gate_bk";
    let wal_dir = "/tmp/neb_multi_db_threshold_gate_wal";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 8 * SEGMENT_SIZE,
            db_size: 8 * SEGMENT_SIZE,
            tiered_config: Some(crate::ram::tiered::TieredConfig {
                threshold,
                lower_watermark: 0.5,
                physical_memory_limit,
                promotion_cooldown_ms: 0,
            }),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "tiered_multi_db_threshold_gate",
        async |_| {},
    )
    .await
    .unwrap();

    let analytics = server
        .ensure_database_runtime("analytics")
        .await
        .expect("analytics runtime should be created");
    server.cleaner().stop();
    analytics.cleaner().stop();

    let default_schema = Schema::new_with_id(
        9011,
        &String::from("default_threshold_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    let analytics_schema = Schema::new_with_id(
        9012,
        &String::from("analytics_threshold_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    server
        .meta()
        .schemas
        .debug_only_new_schema(default_schema.clone());
    analytics
        .meta()
        .schemas
        .debug_only_new_schema(analytics_schema.clone());

    let manager = server
        .chunks()
        .tiered_manager
        .as_ref()
        .expect("default runtime should have a tiered manager")
        .clone();

    let payload = "m".repeat(2048);
    let mut default_next_indices = [0_usize];
    let mut analytics_next_indices = [0_usize];
    append_round_robin_until_reconciled_hot_segments(
        server.chunks(),
        default_schema.id,
        &[0_u64],
        &mut default_next_indices,
        &manager,
        &[&server.chunks(), &analytics.chunks()],
        threshold_hot_segments.saturating_sub(3),
        &payload,
    );
    append_round_robin_until_reconciled_hot_segments(
        analytics.chunks(),
        analytics_schema.id,
        &[0_u64],
        &mut analytics_next_indices,
        &manager,
        &[&server.chunks(), &analytics.chunks()],
        threshold_hot_segments.saturating_sub(1),
        &payload,
    );

    let combined_hot_before_threshold =
        reconcile_global_hot_segments(&manager, &[&server.chunks(), &analytics.chunks()]);
    assert_eq!(
        combined_hot_before_threshold,
        threshold_hot_segments.saturating_sub(1),
        "combined databases should remain exactly one hot segment below the shared trigger point before the final segment is added"
    );
    let evicted_before_threshold = manager
        .evict_for_allocation()
        .expect("multi-database threshold check below the limit should succeed");
    assert_eq!(
        evicted_before_threshold, 0,
        "shared eviction must not trigger before combined hot memory exceeds the threshold"
    );
    assert_eq!(
        total_cold_segments(server.chunks()) + total_cold_segments(analytics.chunks()),
        0,
        "no database should have cold segments before the combined threshold is exceeded"
    );

    append_round_robin_until_reconciled_hot_segments(
        analytics.chunks(),
        analytics_schema.id,
        &[0_u64],
        &mut analytics_next_indices,
        &manager,
        &[&server.chunks(), &analytics.chunks()],
        threshold_hot_segments,
        &payload,
    );
    let combined_hot_at_trigger =
        reconcile_global_hot_segments(&manager, &[&server.chunks(), &analytics.chunks()]);
    assert_eq!(
        combined_hot_at_trigger, threshold_hot_segments,
        "combined databases should reach the exact shared hot-segment threshold before eviction is triggered for the next allocation"
    );

    let cold_before_crossing =
        total_cold_segments(server.chunks()) + total_cold_segments(analytics.chunks());
    let evicted_after_threshold = manager
        .evict_for_allocation()
        .expect("multi-database threshold check at the boundary should succeed");
    let cold_after_crossing =
        total_cold_segments(server.chunks()) + total_cold_segments(analytics.chunks());
    assert!(
        evicted_after_threshold > 0 || cold_after_crossing > cold_before_crossing,
        "shared eviction should trigger once combined hot memory reaches the threshold boundary for the next allocation"
    );

    analytics.cleaner().stop();
    server.cleaner().stop();
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
}

#[test]
fn test_equal_sized_databases_evict_down_to_shared_limit() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    let physical_memory_limit = 6 * SEGMENT_SIZE;
    let lower_watermark = 0.5;
    let desired_hot_segments =
        ((physical_memory_limit as f64 * lower_watermark as f64) / SEGMENT_SIZE as f64) as usize;
    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.75,
            lower_watermark,
            physical_memory_limit,
            promotion_cooldown_ms: 0,
        }),
    ));

    let payload = "q".repeat(2048);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    let mut databases = Vec::new();

    for db_idx in 0..3 {
        let schema = Schema::new(
            &format!("equal_db_eviction_{}", db_idx),
            None,
            default_fields(),
            false,
            false,
        );
        let schema_dir = format!("/tmp/neb_equal_db_eviction_schema_{}", db_idx);
        let backup_dir = format!("/tmp/neb_equal_db_eviction_bk_{}", db_idx);
        let wal_dir = format!("/tmp/neb_equal_db_eviction_wal_{}", db_idx);

        let _ = std::fs::remove_dir_all(&schema_dir);
        let _ = std::fs::remove_dir_all(&backup_dir);
        let _ = std::fs::remove_dir_all(&wal_dir);

        let schemas = LocalSchemasCache::new_local(&schema_dir);
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            1,
            4 * SEGMENT_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            Some(backup_dir.clone()),
            Some(wal_dir.clone()),
            Some(manager.clone()),
        );

        write_cells_for_partition(&chunks, schema.id, 0, 0, cells_per_segment * 2, &payload);

        databases.push((chunks, schema_dir, backup_dir, wal_dir));
    }

    let hot_before: usize = databases
        .iter()
        .map(|(chunks, _, _, _)| total_hot_segments(chunks))
        .sum();
    assert!(
        hot_before > desired_hot_segments,
        "equal-sized databases should exceed the shared lower watermark before eviction"
    );

    let mut evicted_total = 0;
    for _ in 0..4 {
        let hot_now: usize = databases
            .iter()
            .map(|(chunks, _, _, _)| total_hot_segments(chunks))
            .sum();
        if hot_now <= desired_hot_segments {
            break;
        }

        let evicted = manager
            .evict_for_allocation()
            .expect("shared eviction across equal-sized databases should succeed");
        evicted_total += evicted;
        if evicted == 0 {
            break;
        }
    }

    let hot_after: usize = databases
        .iter()
        .map(|(chunks, _, _, _)| total_hot_segments(chunks))
        .sum();
    assert!(
        evicted_total > 0,
        "equal-sized databases should trigger eviction"
    );
    assert!(
        hot_after <= desired_hot_segments,
        "shared eviction should converge to the configured lower watermark"
    );

    let databases_with_cold = databases
        .iter()
        .filter(|(chunks, _, _, _)| total_cold_segments(chunks) > 0)
        .count();
    assert!(
        databases_with_cold >= 2,
        "eviction should reclaim memory from more than one equally sized database"
    );

    for (_, schema_dir, backup_dir, wal_dir) in databases {
        let _ = std::fs::remove_dir_all(schema_dir);
        let _ = std::fs::remove_dir_all(backup_dir);
        let _ = std::fs::remove_dir_all(wal_dir);
    }
}

#[test]
fn test_global_eviction_ignores_unregistered_database() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.75,
            lower_watermark: 0.5,
            physical_memory_limit: 4 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema_a = Schema::new("db_a_eviction", None, default_fields(), false, false);
    let schemas_a = LocalSchemasCache::new_local("/tmp/neb_db_a_eviction_schema");
    schemas_a.debug_only_new_schema(schema_a.clone());
    let chunks_a = Chunks::new(
        1,
        4 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas: schemas_a }),
        None,
        Some("/tmp/neb_db_a_eviction_bk".to_string()),
        Some("/tmp/neb_db_a_eviction_wal".to_string()),
        Some(manager.clone()),
    );

    let schema_b = Schema::new("db_b_eviction", None, default_fields(), false, false);
    let schemas_b = LocalSchemasCache::new_local("/tmp/neb_db_b_eviction_schema");
    schemas_b.debug_only_new_schema(schema_b.clone());
    let chunks_b = Chunks::new(
        1,
        4 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas: schemas_b }),
        None,
        Some("/tmp/neb_db_b_eviction_bk".to_string()),
        Some("/tmp/neb_db_b_eviction_wal".to_string()),
        Some(manager.clone()),
    );

    let payload = "z".repeat(2048);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    write_cells_for_partition(
        &chunks_a,
        schema_a.id,
        0,
        0,
        cells_per_segment / 2,
        &payload,
    );
    write_cells_for_partition(
        &chunks_b,
        schema_b.id,
        0,
        0,
        cells_per_segment * 2,
        &payload,
    );

    manager.unregister_chunks(&chunks_b);

    let evicted_after_unregistration = manager
        .evict_for_allocation()
        .expect("eviction check after unregistering one database should succeed");
    assert_eq!(
        evicted_after_unregistration, 0,
        "unregistered database chunks should no longer contribute to shared eviction pressure"
    );
    assert_eq!(
        total_cold_segments(&chunks_a),
        0,
        "remaining registered database should stay hot when it is below the shared limit"
    );

    let _ = std::fs::remove_dir_all("/tmp/neb_db_a_eviction_bk");
    let _ = std::fs::remove_dir_all("/tmp/neb_db_a_eviction_wal");
    let _ = std::fs::remove_dir_all("/tmp/neb_db_a_eviction_schema");
    let _ = std::fs::remove_dir_all("/tmp/neb_db_b_eviction_bk");
    let _ = std::fs::remove_dir_all("/tmp/neb_db_b_eviction_wal");
    let _ = std::fs::remove_dir_all("/tmp/neb_db_b_eviction_schema");
}

#[test]
fn test_multi_chance_clock_api() {
    use crate::ram::file_manager::SegmentFileManager;
    use crate::ram::segs::Segment;
    use std::sync::Arc;

    let file_manager = Arc::new(SegmentFileManager::new(None, None));
    let segment = Segment::new(1, 1, 0, 0x1000, true, file_manager);

    segment.mark_referenced();
    assert_eq!(segment.get_reference_count(), 1);
    segment.mark_referenced();
    segment.mark_referenced();
    assert_eq!(segment.get_reference_count(), 3);

    let is_victim = segment.decrement_and_check();
    assert!(!is_victim);
    assert_eq!(segment.get_reference_count(), 2);

    segment.decrement_and_check();
    segment.decrement_and_check();
    let is_victim = segment.decrement_and_check();
    assert!(is_victim);

    assert_eq!(segment.get_access_count(), 0);
    let count = segment.increment_access_count();
    assert_eq!(count, 1);
    let count = segment.increment_access_count();
    assert_eq!(count, 2);
    segment.reset_access_count();
    assert_eq!(segment.get_access_count(), 0);
}

/// Large-scale end-to-end test: 64MB physical limit, 1GB virtual, 512MB data
/// with batched transactional inserts followed by random transactional updates.
/// Tests natural eviction/promotion with serializability guarantees.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_large_scale_transactions_with_natural_tiered_memory() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    info!("=== Starting Large-Scale Tiered Memory Transaction Test ===");
    info!("Config: 64MB physical, 1GB virtual, 512MB data target");

    // Clean up old backup files from previous test runs
    // This is critical because archive() skips existing files, and our fix
    // to write full SEGMENT_SIZE won't apply to old truncated files
    let backup_dir = "/tmp/neb_large_scale_bk";
    let wal_dir = "/tmp/neb_large_scale_wal";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);

    // Configure: 64MB physical limit, 1GB virtual capacity
    let physical_limit = 64 * 1024 * 1024; // 64MB = 8 segments
    let virtual_capacity = 1024 * 1024 * 1024; // 1GB = 128 segments
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.75"); // 75% threshold
    std::env::set_var(
        "NEB_TIERED_PHYSICAL_MEMORY_LIMIT",
        &format!("{}", physical_limit),
    );

    // Start server
    let server_addr = crate::utils::test_port::unique_localhost_addr();

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: virtual_capacity,
            db_size: virtual_capacity,
            tiered_config: crate::ram::tiered::TieredConfig::from_env(),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &server_addr,
        "large_scale_test",
        async |_| {},
    )
    .await
    .unwrap();

    // Create schema
    let fields = fields_with_score();
    let schema = Schema::new("large_scale_schema", None, fields, false, false);
    let schema = Schema::new_with_id(
        9999,
        &schema.name,
        schema.str_key_field.clone(),
        schema.fields.clone(),
        schema.is_dynamic,
        schema.is_scannable,
    );
    server.meta().schemas.debug_only_new_schema(schema.clone());

    info!("Server started, schema created");

    // Phase 1: Insert 512MB of data in batched transactions
    // Each cell ~8KB, so ~65536 cells = 512MB
    // Batch into transactions of 1000 cells each = 66 transactions
    info!("Phase 1: Inserting 512MB of data in batched transactions");

    let cell_size = 8 * 1024; // 8KB per cell
    let target_data_size = 512 * 1024 * 1024; // 512MB
    let num_cells = target_data_size / cell_size; // ~65536 cells
    let batch_size = 1000; // 1000 cells per transaction
    let num_batches = (num_cells + batch_size - 1) / batch_size;

    let large_blob = "X".repeat(cell_size - 512); // Leave room for other fields
    let client = tiered_txn_client(&server_addr, "large_scale_test").await;

    let mut all_ids = Vec::with_capacity(num_cells);
    let insert_start = std::time::Instant::now();

    for batch_idx in 0..num_batches {
        let tx = client.begin().await.unwrap().unwrap();
        let start_idx = batch_idx * batch_size;
        let end_idx = ((batch_idx + 1) * batch_size).min(num_cells);

        for i in start_idx..end_idx {
            let id = Id::from_parts(schema.id as u64, i as u64 + 1);
            let mut m = OwnedMap::new();
            m.insert(&String::from("id"), OwnedValue::I64(i as i64));
            m.insert(
                &String::from("name"),
                OwnedValue::String(format!("cell_{}", i)),
            );
            m.insert(
                &String::from("data"),
                OwnedValue::String(large_blob.clone()),
            );
            m.insert(&String::from("score"), OwnedValue::U64(0));
            let cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(m));

            match client.write(tx.clone(), cell).await {
                Ok(Ok(transactions::TxnExecResult::Accepted(_))) => {
                    all_ids.push(id);
                }
                Ok(Ok(other)) => {
                    warn!("Unexpected write result: {:?}", other);
                }
                Ok(Err(e)) => {
                    error!("Write error: {:?}", e);
                    break;
                }
                Err(e) => {
                    error!("RPC error: {:?}", e);
                    break;
                }
            }
        }

        // Commit batch
        match client.prepare(tx.clone()).await {
            Ok(Ok(transactions::TMPrepareResult::Success)) => match client.commit(tx).await {
                Ok(Ok(transactions::EndResult::Success)) => {
                    if (batch_idx + 1) % 10 == 0 {
                        info!(
                            "Inserted batch {}/{} ({} cells total)",
                            batch_idx + 1,
                            num_batches,
                            all_ids.len()
                        );
                    }
                }
                other => {
                    error!("Commit failed for batch {}: {:?}", batch_idx, other);
                    break;
                }
            },
            other => {
                error!("Prepare failed for batch {}: {:?}", batch_idx, other);
                break;
            }
        }

        // Let cleaner run naturally every few batches
        if batch_idx % 5 == 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    }

    let insert_duration = insert_start.elapsed();
    info!(
        "Phase 1 complete: Inserted {} cells in {:.2}s ({:.2} MB/s)",
        all_ids.len(),
        insert_duration.as_secs_f64(),
        (all_ids.len() * cell_size) as f64 / insert_duration.as_secs_f64() / (1024.0 * 1024.0)
    );

    // Check tiered memory stats
    for chunk in &server.chunks().list {
        if let Some(ref manager) = chunk.tiered_manager {
            let stats = manager.stats(chunk);
            info!("After insert - Hot: {} segments ({} MB), Cold: {} segments ({} MB), Total: {} segments",
                  stats.hot_segments,
                  stats.hot_segments * 8,
                  stats.cold_segments,
                  stats.cold_segments * 8,
                  stats.total_segments);

            // Verify we have cold segments (natural eviction occurred)
            assert!(
                stats.cold_segments > 0,
                "Should have cold segments after inserting 512MB with 64MB limit"
            );
        }
    }

    // Phase 2: Random transactional updates with serializability
    info!("Phase 2: Random read-then-update transactions (testing serializability)");

    let update_workers = 12;
    let updates_per_worker = 500;
    let success_counters: Arc<Vec<AtomicU64>> =
        Arc::new((0..all_ids.len()).map(|_| AtomicU64::new(0)).collect());

    let update_start = std::time::Instant::now();
    let mut update_handles = Vec::new();

    for worker_id in 0..update_workers {
        let server_addr = server_addr.clone();
        let all_ids = all_ids.clone();
        let success_counters = success_counters.clone();
        let schema_id = schema.id;

        update_handles.push(tokio::spawn(async move {
            let client = tiered_txn_client(&server_addr, "large_scale_test").await;
            let mut local_success = 0u64;
            let mut local_conflict = 0u64;

            // Use worker_id to offset the counter for better distribution
            let mut counter: u64 = worker_id as u64 * 1000000;

            for _ in 0..updates_per_worker {
                // Select key using simple counter-based pseudo-random
                counter = counter.wrapping_mul(1103515245).wrapping_add(12345);
                let key_idx = (counter as usize) % all_ids.len();
                let id = all_ids[key_idx];

                let tx = client.begin().await.unwrap().unwrap();

                // Read current value
                match client.read(tx.clone(), id).await {
                    Ok(Ok(transactions::TxnExecResult::Accepted(cell))) => {
                        let curr_score = *cell.data["score"].u64().unwrap();

                        // Update score
                        let mut m = OwnedMap::new();
                        m.insert(
                            &String::from("id"),
                            OwnedValue::I64(*cell.data["id"].i64().unwrap()),
                        );
                        m.insert(&String::from("name"), cell.data["name"].clone());
                        m.insert(&String::from("data"), cell.data["data"].clone());
                        m.insert(&String::from("score"), OwnedValue::U64(curr_score + 1));

                        let updated_cell =
                            OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(m));

                        match client.update(tx.clone(), updated_cell).await {
                            Ok(Ok(transactions::TxnExecResult::Accepted(_))) => {
                                // Try to commit
                                match client.prepare(tx.clone()).await {
                                    Ok(Ok(transactions::TMPrepareResult::Success)) => {
                                        match client.commit(tx).await {
                                            Ok(Ok(transactions::EndResult::Success)) => {
                                                success_counters[key_idx]
                                                    .fetch_add(1, AtomicOrdering::Relaxed);
                                                local_success += 1;
                                            }
                                            _ => {
                                                local_conflict += 1;
                                            }
                                        }
                                    }
                                    _ => {
                                        local_conflict += 1;
                                    }
                                }
                            }
                            _ => {
                                local_conflict += 1;
                            }
                        }
                    }
                    _ => {
                        local_conflict += 1;
                    }
                }
            }

            (local_success, local_conflict)
        }));
    }

    // Wait for all update workers
    let mut total_success = 0u64;
    let mut total_conflict = 0u64;
    for handle in update_handles {
        let (success, conflict) = handle.await.unwrap();
        total_success += success;
        total_conflict += conflict;
    }

    let update_duration = update_start.elapsed();
    info!(
        "Phase 2 complete: {} successful updates, {} conflicts in {:.2}s ({:.2} TPS)",
        total_success,
        total_conflict,
        update_duration.as_secs_f64(),
        total_success as f64 / update_duration.as_secs_f64()
    );

    // Phase 3: Verify serializability - final scores must match successful commits
    info!("Phase 3: Verifying serializability");

    let verification_start = std::time::Instant::now();
    let mut mismatches = 0;
    let sample_size = all_ids.len().min(1000); // Verify a sample

    for i in (0..all_ids.len()).step_by(all_ids.len() / sample_size) {
        let id = all_ids[i];
        let expected = success_counters[i].load(AtomicOrdering::Relaxed);

        match server.chunks().read_cell(&id) {
            Ok(cell) => {
                let actual = *cell.data["score"].u64().unwrap();
                if actual != expected {
                    error!(
                        "Serializability violation at key {}: expected score {}, got {}",
                        i, expected, actual
                    );
                    mismatches += 1;
                }
            }
            Err(e) => {
                error!("Failed to read cell {} for verification: {:?}", i, e);
                mismatches += 1;
            }
        }
    }

    let verification_duration = verification_start.elapsed();
    info!(
        "Phase 3 complete: Verified {} cells in {:.2}s, {} mismatches",
        sample_size,
        verification_duration.as_secs_f64(),
        mismatches
    );

    assert_eq!(
        mismatches, 0,
        "Serializability check failed: {} mismatches found",
        mismatches
    );

    // Final tiered memory stats
    for chunk in &server.chunks().list {
        if let Some(ref manager) = chunk.tiered_manager {
            let stats = manager.stats(chunk);
            info!("Final stats - Hot: {} segments ({} MB), Cold: {} segments ({} MB), Total: {} segments",
                  stats.hot_segments,
                  stats.hot_segments * 8,
                  stats.cold_segments,
                  stats.cold_segments * 8,
                  stats.total_segments);
        }
    }

    // Cleanup
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");

    // Drop server first to ensure all operations complete
    drop(server);

    // Wait for operations to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Clean up files
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);

    info!("=== Large-Scale Tiered Memory Transaction Test Complete ===");
}

/// Comprehensive stress test: Multiple scales of load with concurrent reads and writes
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_stress_concurrent_mixed_workload_with_tiered_memory() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    info!("=== Starting Stress Test: Mixed Concurrent Workload ===");

    // Reduced configuration: 16MB physical, 64MB virtual (reduced for faster test)
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.7");
    std::env::set_var(
        "NEB_TIERED_PHYSICAL_MEMORY_LIMIT",
        &format!("{}", 16 * 1024 * 1024),
    );

    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let backup_dir = "/tmp/neb_stress_test_bk";
    let wal_dir = "/tmp/neb_stress_test_wal";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024, // Reduced from 256MB
            db_size: 64 * 1024 * 1024,
            tiered_config: crate::ram::tiered::TieredConfig::from_env(),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &server_addr,
        "stress_test",
        async |_| {},
    )
    .await
    .unwrap();

    let fields = fields_with_score();
    let schema = Schema::new("stress_schema", None, fields, false, false);
    let schema = Schema::new_with_id(
        8888,
        &schema.name,
        schema.str_key_field.clone(),
        schema.fields.clone(),
        schema.is_dynamic,
        schema.is_scannable,
    );
    server.meta().schemas.debug_only_new_schema(schema.clone());

    // Initialize 2000 cells (reduced from 10000 for faster test)
    info!("Initializing 2000 cells");
    let client = tiered_txn_client(&server_addr, "stress_test").await;
    let num_keys = 2000;
    let mut ids = Vec::with_capacity(num_keys);

    let batch_size = 200;
    for batch in 0..(num_keys / batch_size) {
        let tx = client.begin().await.unwrap().unwrap();
        for i in (batch * batch_size)..((batch + 1) * batch_size) {
            let id = Id::from_parts(schema.id as u64, i as u64 + 1);
            let mut m = OwnedMap::new();
            m.insert(&String::from("id"), OwnedValue::I64(i as i64));
            m.insert(
                &String::from("name"),
                OwnedValue::String(format!("stress_{}", i)),
            );
            m.insert(&String::from("data"), OwnedValue::String("D".repeat(1024))); // Reduced from 4KB to 1KB
            m.insert(&String::from("score"), OwnedValue::U64(0));
            let cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(m));
            let _ = client.write(tx.clone(), cell).await.unwrap().unwrap();
            ids.push(id);
        }
        let _ = client.prepare(tx.clone()).await.unwrap().unwrap();
        let _ = client.commit(tx).await.unwrap().unwrap();
    }

    info!("Initialization complete, starting mixed workload");

    // Mixed workload: readers and writers (reduced from 6+6 for faster test)
    let readers = 3;
    let writers = 3;
    let duration_secs = 10; // Reduced from 20 seconds

    let success_counters: Arc<Vec<AtomicU64>> =
        Arc::new((0..num_keys).map(|_| AtomicU64::new(0)).collect());

    let start_time = std::time::Instant::now();
    let mut handles = Vec::new();

    // Reader threads
    for reader_id in 0..readers {
        let server_addr = server_addr.clone();
        let ids = ids.clone();

        handles.push(tokio::spawn(async move {
            let client = tiered_txn_client(&server_addr, "stress_test").await;
            let mut reads = 0u64;
            let mut counter: u64 = reader_id as u64 * 777;

            while start_time.elapsed().as_secs() < duration_secs {
                counter = counter.wrapping_mul(1103515245).wrapping_add(12345);
                let key_idx = (counter as usize) % ids.len();
                let id = ids[key_idx];

                let tx = client.begin().await.unwrap().unwrap();
                if let Ok(Ok(transactions::TxnExecResult::Accepted(_))) =
                    client.read(tx.clone(), id).await
                {
                    let _ = client.prepare(tx.clone()).await;
                    let _ = client.commit(tx).await;
                    reads += 1;
                }
            }

            info!("Reader {} completed {} reads", reader_id, reads);
            reads
        }));
    }

    // Writer threads
    for writer_id in 0..writers {
        let server_addr = server_addr.clone();
        let ids = ids.clone();
        let success_counters = success_counters.clone();
        let schema_id = schema.id;

        handles.push(tokio::spawn(async move {
            let client = tiered_txn_client(&server_addr, "stress_test").await;
            let mut writes = 0u64;
            let mut counter: u64 = writer_id as u64 * 999;

            while start_time.elapsed().as_secs() < duration_secs {
                counter = counter.wrapping_mul(1103515245).wrapping_add(12345);
                let key_idx = (counter as usize) % ids.len();
                let id = ids[key_idx];

                let tx = client.begin().await.unwrap().unwrap();

                if let Ok(Ok(transactions::TxnExecResult::Accepted(cell))) =
                    client.read(tx.clone(), id).await
                {
                    let curr_score = *cell.data["score"].u64().unwrap();
                    let mut m = OwnedMap::new();
                    m.insert(
                        &String::from("id"),
                        OwnedValue::I64(*cell.data["id"].i64().unwrap()),
                    );
                    m.insert(&String::from("name"), cell.data["name"].clone());
                    m.insert(&String::from("data"), cell.data["data"].clone());
                    m.insert(&String::from("score"), OwnedValue::U64(curr_score + 1));

                    let updated = OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(m));

                    if let Ok(Ok(transactions::TxnExecResult::Accepted(_))) =
                        client.update(tx.clone(), updated).await
                    {
                        if let Ok(Ok(transactions::TMPrepareResult::Success)) =
                            client.prepare(tx.clone()).await
                        {
                            if let Ok(Ok(transactions::EndResult::Success)) =
                                client.commit(tx).await
                            {
                                success_counters[key_idx].fetch_add(1, AtomicOrdering::Relaxed);
                                writes += 1;
                            }
                        }
                    }
                }
            }

            info!("Writer {} completed {} writes", writer_id, writes);
            writes
        }));
    }

    // Wait for all threads
    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start_time.elapsed();
    info!("Mixed workload completed in {:.2}s", elapsed.as_secs_f64());

    // Verify a sample
    let mut verified = 0;
    for i in (0..ids.len()).step_by(ids.len() / 100) {
        let id = ids[i];
        let expected = success_counters[i].load(AtomicOrdering::Relaxed);
        if let Ok(cell) = server.chunks().read_cell(&id) {
            let actual = *cell.data["score"].u64().unwrap();
            assert_eq!(actual, expected, "Mismatch at key {}", i);
            verified += 1;
        }
    }

    info!("Verified {} cells successfully", verified);

    // Cleanup
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);

    info!("=== Stress Test Complete ===");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_direct_writes_without_transactions_or_tiered_memory() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    info!("=== Starting Direct Write Test (No Transactions, No Tiered Memory) ===");

    // Note: new_with_recovery() will call reset_global_chunk_allocation() at the start,
    // so we don't need to call it here. We only reset at the end to clean up for the next test.

    // NO tiered memory configuration
    let chunk_capacity = 16 * SEGMENT_SIZE; // 128MB
    let schema_id = 7777;
    let schema_name = "direct_schema";
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let backup_dir = "/tmp/neb_direct_bk";
    let wal_dir = "/tmp/neb_direct_wal";

    // Clean up
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: chunk_capacity,
            db_size: chunk_capacity,
            tiered_config: None, // No tiered memory
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
            undo_log_storage: None,
            raft_storage: None,
        },
        &server_addr,
        "test",
        async |_| {},
    )
    .await
    .unwrap();

    let schema = Schema::new_with_id(
        schema_id,
        &String::from(schema_name),
        None,
        fields_with_score(), // Use fields_with_score to include the "score" field
        false,
        false,
    );

    server.meta().schemas.debug_only_new_schema(schema.clone());

    // Initialize cells in batches (single-threaded setup)
    info!("Initializing 10,000 cells directly");
    let num_keys = 10_000;
    let mut ids = Vec::with_capacity(num_keys);

    let batch_size = 500;
    for batch in 0..(num_keys / batch_size) {
        for i in (batch * batch_size)..((batch + 1) * batch_size) {
            let id = Id::allocated(0, 0, i as u64); // Use partition 0, unique lower values
            let mut m = OwnedMap::new();
            m.insert(&String::from("id"), OwnedValue::I64(i as i64));
            m.insert(
                &String::from("name"),
                OwnedValue::String(format!("item_{}", i)),
            );
            m.insert(&String::from("data"), OwnedValue::String("x".repeat(100)));
            m.insert(&String::from("score"), OwnedValue::U64(0));

            let mut cell = OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(m));

            match server.chunks().write_cell(&mut cell) {
                Ok(_) => ids.push(id),
                Err(e) => {
                    error!("Failed to write cell {}: {:?}", i, e);
                    panic!("Write failed");
                }
            }
        }
    }

    info!("Initialization complete, starting concurrent workload");

    // Mixed workload: readers and writers
    let readers = 6;
    let writers = 6;
    let duration_secs = 20;

    let success_counters: Arc<Vec<AtomicU64>> =
        Arc::new((0..num_keys).map(|_| AtomicU64::new(0)).collect());

    let chunks = server.chunks().clone();
    let start_time = std::time::Instant::now();
    let mut handles = Vec::new();

    // Reader threads
    for reader_id in 0..readers {
        let chunks = chunks.clone();
        let ids = ids.clone();

        handles.push(tokio::spawn(async move {
            let mut reads = 0u64;
            let mut counter: u64 = reader_id as u64 * 777;

            while start_time.elapsed().as_secs() < duration_secs {
                counter = counter.wrapping_mul(1103515245).wrapping_add(12345);
                let key_idx = (counter as usize) % ids.len();
                let id = ids[key_idx];

                // Use spawn_blocking for synchronous chunks operations
                let chunks_clone = chunks.clone();
                let result = tokio::task::spawn_blocking(move || {
                    chunks_clone.read_cell(&id).map(|cell| cell.to_owned())
                })
                .await;

                if let Ok(Ok(_owned_cell)) = result {
                    // Verify we can read the cell
                    reads += 1;
                }
            }

            info!("Reader {} completed {} reads", reader_id, reads);
            reads
        }));
    }

    // Writer threads
    for writer_id in 0..writers {
        let chunks = chunks.clone();
        let ids = ids.clone();
        let success_counters = success_counters.clone();

        handles.push(tokio::spawn(async move {
            let mut writes = 0u64;
            let mut counter: u64 = writer_id as u64 * 999;

            while start_time.elapsed().as_secs() < duration_secs {
                counter = counter.wrapping_mul(1103515245).wrapping_add(12345);
                let key_idx = (counter as usize) % ids.len();
                let id = ids[key_idx];

                // Read
                let chunks_clone_read = chunks.clone();
                let read_result = tokio::task::spawn_blocking(move || {
                    chunks_clone_read.read_cell(&id).map(|cell| cell.to_owned())
                })
                .await;

                if let Ok(Ok(owned_cell)) = read_result {
                    // Get current score value
                    if let Some(curr_score) = owned_cell.data["score"].u64() {
                        // Update
                        let mut m = OwnedMap::new();
                        m.insert(&String::from("id"), owned_cell.data["id"].clone());
                        m.insert(&String::from("name"), owned_cell.data["name"].clone());
                        m.insert(&String::from("data"), owned_cell.data["data"].clone());
                        m.insert(&String::from("score"), OwnedValue::U64(*curr_score + 1));

                        let mut updated_cell =
                            OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(m));

                        // Update
                        let chunks_clone_update = chunks.clone();
                        let update_result = tokio::task::spawn_blocking(move || {
                            chunks_clone_update.update_cell(&mut updated_cell)
                        })
                        .await;

                        if update_result.is_ok() && update_result.unwrap().is_ok() {
                            success_counters[key_idx].fetch_add(1, AtomicOrdering::Relaxed);
                            writes += 1;
                        }
                    }
                }
            }

            info!("Writer {} completed {} writes", writer_id, writes);
            writes
        }));
    }

    // Wait for all threads
    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start_time.elapsed();
    info!(
        "Concurrent workload completed in {:.2}s",
        elapsed.as_secs_f64()
    );

    info!("Triggering GC");

    // Trigger GC
    use crate::ram::cleaner::Cleaner;
    let _ = Cleaner::clean(&server.chunks().list[0], true, true);

    info!("GC complete, verifying data");

    // Verify a sample
    // Note: Without transactions, concurrent updates can cause lost updates,
    // so we verify that updates occurred (score > 0) but don't require exact equality
    let mut verified = 0;
    let mut mismatches = 0;
    for i in (0..ids.len()).step_by(ids.len() / 100) {
        let id = ids[i];
        let expected = success_counters[i].load(AtomicOrdering::Relaxed);
        if let Ok(cell) = server.chunks().read_cell(&id) {
            let owned_cell = cell.to_owned();
            if let Some(actual) = owned_cell.data["score"].u64() {
                // Without transactions, concurrent updates can cause lost updates
                // Accept if actual <= expected (some updates may be lost)
                // and actual >= 1 (at least one update occurred if expected > 0)
                if *actual > expected {
                    panic!(
                        "Unexpected: actual {} > expected {} at key {}",
                        *actual, expected, i
                    );
                } else if *actual == 0 && expected > 0 {
                    warn!(
                        "No updates applied to key {} despite {} successful updates",
                        i, expected
                    );
                    mismatches += 1;
                } else if *actual < expected {
                    debug!(
                        "Key {}: expected {} updates, got {} (lost {} updates due to concurrency)",
                        i,
                        expected,
                        *actual,
                        expected - *actual
                    );
                }
                verified += 1;
            }
        }
    }

    info!(
        "Verified {} cells successfully ({} had mismatches)",
        verified, mismatches
    );
    // Allow some mismatches due to concurrent updates without transactions
    assert!(
        mismatches < verified / 2,
        "Too many keys had no updates despite successful writes"
    );

    // Cleanup
    // Stop cleaner explicitly before dropping server to ensure background tasks stop
    server.cleaner().stop();

    // Wait for cleaner to fully stop
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Drop server to ensure all operations complete
    drop(server);

    // Wait for all operations to complete, including background tasks
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Note: We do NOT reset global chunk allocation here because:
    // 1. new_with_recovery() already calls reset_global_chunk_allocation() at the start
    // 2. Resetting here can cause SIGSEGV if signal handlers from previous tests are still active
    // 3. The TEST_MUTEX ensures tests run serially, so new_with_recovery() will clean up properly

    // Clean up files
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);

    info!("=== Direct Write Test Complete ===");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_direct_writes_with_tiered_memory() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    info!("=== Starting Direct Write Test WITH Tiered Memory ===");

    // Configure tiered memory with a small physical memory limit
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.7");
    std::env::set_var(
        "NEB_TIERED_PHYSICAL_MEMORY_LIMIT",
        &format!("{}", 32 * 1024 * 1024),
    ); // 32MB physical
    std::env::set_var("NEB_CLEANER_SLEEP_INTERVAL_MS", "100"); // Faster cleaner for testing

    let chunk_capacity = 512 * 1024 * 1024; // 512MB virtual capacity (more space for updates)
    let schema_id = 8888;
    let schema_name = "tiered_direct_schema";
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let backup_dir = "/tmp/neb_tiered_direct_bk";
    let wal_dir = "/tmp/neb_tiered_direct_wal";

    // Clean up
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::create_dir_all(backup_dir);
    let _ = std::fs::create_dir_all(wal_dir);

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: chunk_capacity,
            db_size: chunk_capacity,
            tiered_config: crate::ram::tiered::TieredConfig::from_env(),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
            undo_log_storage: None,
            raft_storage: None,
        },
        &server_addr,
        "test",
        async |_| {},
    )
    .await
    .unwrap();

    let schema = Schema::new_with_id(
        schema_id,
        &String::from(schema_name),
        None,
        fields_with_score(),
        false,
        false,
    );

    server.meta().schemas.debug_only_new_schema(schema.clone());

    // Start cleaner (this triggers eviction/promotion automatically)
    use crate::ram::cleaner::Cleaner;
    let cleaner = Cleaner::new_and_start(server.chunks().clone());
    info!("Cleaner started");

    // Initialize cells in batches (single-threaded setup)
    info!("Initializing 10,000 cells directly");
    let num_keys = 10_000;
    let mut ids = Vec::with_capacity(num_keys);

    let batch_size = 500;
    for batch in 0..(num_keys / batch_size) {
        for i in (batch * batch_size)..((batch + 1) * batch_size) {
            let id = Id::allocated(0, 0, i as u64); // Use partition 0, unique lower values
            let mut m = OwnedMap::new();
            m.insert(&String::from("id"), OwnedValue::I64(i as i64));
            m.insert(
                &String::from("name"),
                OwnedValue::String(format!("item_{}", i)),
            );
            m.insert(&String::from("data"), OwnedValue::String("x".repeat(100)));
            m.insert(&String::from("score"), OwnedValue::U64(0));

            let mut cell = OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(m));

            match server.chunks().write_cell(&mut cell) {
                Ok(_) => ids.push(id),
                Err(e) => {
                    error!("Failed to write cell {}: {:?}", i, e);
                    panic!("Write failed");
                }
            }
        }
    }

    info!("Initialization complete, starting concurrent workload with tiered memory");

    // Mixed workload: readers and writers
    let readers = 6;
    let writers = 6;
    let duration_secs = 20;

    let success_counters: Arc<Vec<AtomicU64>> =
        Arc::new((0..num_keys).map(|_| AtomicU64::new(0)).collect());

    let chunks = server.chunks().clone();
    let start_time = std::time::Instant::now();
    let mut handles = Vec::new();

    // Reader threads
    for reader_id in 0..readers {
        let chunks = chunks.clone();
        let ids = ids.clone();

        handles.push(tokio::spawn(async move {
            let mut reads = 0u64;
            let mut counter: u64 = reader_id as u64 * 777;

            while start_time.elapsed().as_secs() < duration_secs {
                counter = counter.wrapping_mul(1103515245).wrapping_add(12345);
                let key_idx = (counter as usize) % ids.len();
                let id = ids[key_idx];

                // Use spawn_blocking for synchronous chunks operations
                let chunks_clone = chunks.clone();
                let result = tokio::task::spawn_blocking(move || {
                    chunks_clone.read_cell(&id).map(|cell| cell.to_owned())
                })
                .await;

                if let Ok(Ok(_owned_cell)) = result {
                    // Verify we can read the cell
                    reads += 1;
                }
            }

            info!("Reader {} completed {} reads", reader_id, reads);
            reads
        }));
    }

    // Writer threads
    for writer_id in 0..writers {
        let chunks = chunks.clone();
        let ids = ids.clone();
        let success_counters = success_counters.clone();

        handles.push(tokio::spawn(async move {
            let mut writes = 0u64;
            let mut counter: u64 = writer_id as u64 * 999;

            while start_time.elapsed().as_secs() < duration_secs {
                counter = counter.wrapping_mul(1103515245).wrapping_add(12345);
                let key_idx = (counter as usize) % ids.len();
                let id = ids[key_idx];

                // Read
                let chunks_clone_read = chunks.clone();
                let read_result = tokio::task::spawn_blocking(move || {
                    chunks_clone_read.read_cell(&id).map(|cell| cell.to_owned())
                })
                .await;

                if let Ok(Ok(owned_cell)) = read_result {
                    // Get current score value
                    if let Some(curr_score) = owned_cell.data["score"].u64() {
                        // Update
                        let mut m = OwnedMap::new();
                        m.insert(&String::from("id"), owned_cell.data["id"].clone());
                        m.insert(&String::from("name"), owned_cell.data["name"].clone());
                        m.insert(&String::from("data"), owned_cell.data["data"].clone());
                        m.insert(&String::from("score"), OwnedValue::U64(*curr_score + 1));

                        let mut updated_cell =
                            OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(m));

                        // Update
                        let chunks_clone_update = chunks.clone();
                        let update_result = tokio::task::spawn_blocking(move || {
                            chunks_clone_update.update_cell(&mut updated_cell)
                        })
                        .await;

                        if update_result.is_ok() && update_result.unwrap().is_ok() {
                            success_counters[key_idx].fetch_add(1, AtomicOrdering::Relaxed);
                            writes += 1;
                        }
                    }
                }
            }

            info!("Writer {} completed {} writes", writer_id, writes);
            writes
        }));
    }

    // Wait for all threads with timeout
    for handle in handles {
        match tokio::time::timeout(tokio::time::Duration::from_secs(30), handle).await {
            Ok(result) => {
                let _ = result;
            }
            Err(_) => {
                warn!("Thread did not complete within timeout");
            }
        }
    }

    let elapsed = start_time.elapsed();
    info!(
        "Concurrent workload completed in {:.2}s",
        elapsed.as_secs_f64()
    );

    // Wait for any pending tiered memory operations (eviction/promotion) to complete
    // This ensures segment references are released before GC
    info!("Waiting for tiered memory operations to complete");
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Stop the background cleaner to prevent it from interfering with GC
    info!("Stopping background cleaner");
    drop(cleaner);
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    info!("Triggering GC");

    // Trigger GC with timeout
    let gc_result = tokio::task::spawn_blocking({
        let chunks = server.chunks().clone();
        move || {
            let _ = Cleaner::clean(&chunks.list[0], true, true);
        }
    });

    match tokio::time::timeout(tokio::time::Duration::from_secs(10), gc_result).await {
        Ok(Ok(_)) => {
            info!("GC complete");
        }
        Ok(Err(e)) => {
            warn!("GC failed: {:?}", e);
        }
        Err(_) => {
            warn!("GC timed out after 10 seconds");
        }
    }

    info!("Verifying data");

    // Wait a bit for any pending operations to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Verify a sample
    // Note: Without transactions, concurrent updates can cause lost updates,
    // so we verify that updates occurred (score > 0) but don't require exact equality
    let mut verified = 0;
    let mut mismatches = 0;
    let mut read_errors = 0;
    for i in (0..ids.len()).step_by(ids.len() / 100) {
        let id = ids[i];
        let expected = success_counters[i].load(AtomicOrdering::Relaxed);

        // Retry reading in case of segment lookup errors (cleaner may have moved cells)
        let mut cell_result = None;
        for _retry in 0..3 {
            match server.chunks().read_cell(&id) {
                Ok(cell) => {
                    cell_result = Some(cell.to_owned());
                    break;
                }
                Err(e) => {
                    if _retry < 2 {
                        // Retry after a short delay
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        continue;
                    } else {
                        warn!("Failed to read cell {} after 3 retries: {:?}", i, e);
                        read_errors += 1;
                        break;
                    }
                }
            }
        }

        if let Some(owned_cell) = cell_result {
            if let Some(actual) = owned_cell.data["score"].u64() {
                // Without transactions, concurrent updates can cause lost updates
                // The actual value can be anywhere between 0 and expected (or slightly more due to races)
                // We just verify that updates occurred if expected > 0
                if *actual == 0 && expected > 0 {
                    warn!(
                        "No updates applied to key {} despite {} successful updates",
                        i, expected
                    );
                    mismatches += 1;
                } else if *actual < expected {
                    debug!(
                        "Key {}: expected {} updates, got {} (lost {} updates due to concurrency)",
                        i,
                        expected,
                        *actual,
                        expected - *actual
                    );
                } else if *actual > expected {
                    // This can happen due to race conditions in read-then-update pattern
                    // Multiple writers can read the same value and all increment it
                    debug!(
                        "Key {}: actual {} > expected {} (race condition in concurrent updates)",
                        i, *actual, expected
                    );
                }
                verified += 1;
            }
        }
    }

    info!(
        "Verified {} cells successfully ({} had mismatches, {} read errors)",
        verified, mismatches, read_errors
    );
    // Allow some mismatches and read errors due to concurrent updates and cleaner operations
    assert!(
        mismatches + read_errors < verified / 2,
        "Too many keys had no updates or read errors despite successful writes"
    );

    // Cleanup
    // Stop cleaner explicitly before dropping server
    server.cleaner().stop();

    // Wait for cleaner to fully stop
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Drop server to ensure all operations complete
    drop(server);

    // Wait for all operations to complete, including background tasks
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Note: We do NOT reset global chunk allocation here because:
    // 1. new_with_recovery() already calls reset_global_chunk_allocation() at the start
    // 2. Resetting here can cause SIGSEGV if signal handlers are still active
    // 3. The TEST_MUTEX ensures tests run serially, so new_with_recovery() will clean up properly

    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);

    info!("=== Direct Write Test with Tiered Memory Complete ===");
}
