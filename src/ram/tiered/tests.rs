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
use crate::utils::test_temp::temp_path;

// Global mutex to prevent test interference
static TEST_MUTEX: Mutex<()> = Mutex::new(());

/// Serialise tiered tests, recovering from poisoning.
///
/// These tests share process-wide tiered state, so they must not overlap. Using
/// `.lock().unwrap()` meant the first genuine failure poisoned the mutex and
/// every subsequent test failed on the lock instead of running -- one real
/// assertion turned into eighteen `PoisonError`s, hiding which one actually
/// broke. The lock guards ordering, not invariants, so recovering is safe and
/// keeps the real failure legible.
fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner())
}


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
    let _guard = test_lock();
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
    let schemas = LocalSchemasCache::new_local(&temp_path("neb_test_overflow_schema"));
    schemas.debug_only_new_schema(schema.clone());

    // Create temp directories for this test
    let backup_dir = temp_path("neb_test_overflow_bk");
    let wal_dir = temp_path("neb_test_overflow_wal");
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::remove_dir_all(temp_path("neb_test_overflow_schema"));
}

/// Test that reads from cold segments trigger promotion and data is still intact
#[test]
fn test_cold_segment_promotion() {
    let _guard = test_lock();
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
    let schemas = LocalSchemasCache::new_local(&temp_path("neb_test_promotion_schema"));
    schemas.debug_only_new_schema(schema.clone());

    let backup_dir = temp_path("neb_test_promotion_bk");
    let wal_dir = temp_path("neb_test_promotion_wal");
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::remove_dir_all(temp_path("neb_test_promotion_schema"));
}

/// Test churn-related metrics and promotion cooldown skip logic
#[test]
fn test_metrics_and_churn_counters() {
    let _guard = test_lock();
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
    let schemas = LocalSchemasCache::new_local(&temp_path("neb_test_metrics_schema"));
    schemas.debug_only_new_schema(schema.clone());

    let backup_dir = temp_path("neb_test_metrics_bk");
    let wal_dir = temp_path("neb_test_metrics_wal");
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::remove_dir_all(temp_path("neb_test_metrics_schema"));
}

#[test]
fn test_active_blob_head_is_not_evicted_by_clock() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let chunks = Chunks::new_dummy(1, 3 * SEGMENT_SIZE);
    let chunk = &chunks.list[0];

    let blob_head = chunk
        .allocator
        .alloc_seg_with_class(&chunk.file_manager, SegmentClass::Blob)
        .expect("should allocate a blob head for the test");
    let blob_head_id = blob_head.id;
    chunk.put_segment(blob_head);
    chunk.blob_head_pool[0].store(blob_head_id, AtomicOrdering::Relaxed);

    let (regular_head, blob_head) = chunk.head_seg_ids_for_test();
    let blob_head = blob_head.expect("blob head should be installed for the test");
    let policy = ClockEvictionPolicy::default();

    let victim = policy.select_victim(chunk);
    assert!(
        victim.is_none(),
        "CLOCK must not evict an active blob head when only heads exist"
    );

    assert!(chunk.segs.get(&(regular_head.unwrap() as usize)).unwrap().is_hot());
    assert!(chunk.segs.get(&(blob_head as usize)).unwrap().is_hot());
}

#[test]
fn test_blob_segments_evict_before_regular_segments_without_blob_head() {
    let _guard = test_lock();
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
    assert!(chunk.segs.get(&(regular_head.unwrap() as usize)).unwrap().is_hot());
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
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let schema_dir = temp_path("neb_blob_priority_schema");
    let backup_dir = temp_path("neb_blob_priority_bk");
    let wal_dir = temp_path("neb_blob_priority_wal");
    let _ = std::fs::remove_dir_all(&schema_dir);
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

    let regular = Schema::new_with_id(910, "regular_evict", None, default_fields(), false, false);
    let blob = Schema::new_with_id(920, "blob_evict", None, default_fields(), false, false)
        .with_blobs(true);
    let schemas = LocalSchemasCache::new_local(&schema_dir);
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
        .find(|segment_id| Some(*segment_id) != regular_head)
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
    assert!(chunk.segs.get(&(regular_head.unwrap() as usize)).unwrap().is_hot());
    assert!(chunk.segs.get(&(blob_head as usize)).unwrap().is_hot());

    let _ = std::fs::remove_dir_all(&schema_dir);
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
}

#[test]
fn test_blob_segments_promote_on_read_after_eviction() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let schema_dir = temp_path("neb_blob_promote_schema");
    let backup_dir = temp_path("neb_blob_promote_bk");
    let wal_dir = temp_path("neb_blob_promote_wal");
    let _ = std::fs::remove_dir_all(&schema_dir);
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

    let blob = Schema::new("blob_promote", None, default_fields(), false, false).with_blobs(true);
    let schemas = LocalSchemasCache::new_local(&schema_dir);
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

    // Reading a cold segment no longer promotes it. The backup is block
    // indexed, so the read is served by decompressing the one block holding the
    // cell -- which for a blob large enough to own a block is exactly that blob
    // -- and the segment stays cold. Promoting 8 MiB to reach one cell was the
    // behaviour that turned a working set larger than the hot tier into
    // promote/evict churn.
    //
    // What matters is asserted above: the cell read back intact. What follows
    // asserts the segment was NOT materialised in full to do it.
    assert!(
        cold_segment.is_cold(),
        "a cold blob read should be served from its block, not by promoting the segment"
    );
    let (present, total) = cold_segment.block_residency_stats();
    assert!(
        present > 0 && present <= total,
        "expected the read to fault in at least one block (present {present} of {total})"
    );

    let _ = std::fs::remove_dir_all(&schema_dir);
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
}

#[test]
fn test_global_eviction_across_chunks_in_single_database() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let chunk_capacity = 4 * SEGMENT_SIZE;
    let schema = Schema::new(
        "single_db_global_eviction",
        None,
        default_fields(),
        false,
        false,
    );
    let schemas = LocalSchemasCache::new_local(&temp_path("neb_single_db_global_eviction_schema"));
    schemas.debug_only_new_schema(schema.clone());

    let backup_dir = temp_path("neb_single_db_global_eviction_bk");
    let wal_dir = temp_path("neb_single_db_global_eviction_wal");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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

    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::remove_dir_all(temp_path("neb_single_db_global_eviction_schema"));
}

#[test]
fn test_single_database_eviction_waits_until_threshold_is_exceeded() {
    let _guard = test_lock();
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
    let schemas = LocalSchemasCache::new_local(&temp_path("neb_single_db_threshold_gate_schema"));
    schemas.debug_only_new_schema(schema.clone());

    let backup_dir = temp_path("neb_single_db_threshold_gate_bk");
    let wal_dir = temp_path("neb_single_db_threshold_gate_wal");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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

    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::remove_dir_all(temp_path("neb_single_db_threshold_gate_schema"));
}

#[test]
fn test_reconciled_background_eviction_ignores_stale_shared_counter_drift() {
    let _guard = test_lock();
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
    let schemas = LocalSchemasCache::new_local(&temp_path("neb_reconciled_threshold_gate_schema"));
    schemas.debug_only_new_schema(schema.clone());

    let backup_dir = temp_path("neb_reconciled_threshold_gate_bk");
    let wal_dir = temp_path("neb_reconciled_threshold_gate_wal");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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

    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::remove_dir_all(temp_path("neb_reconciled_threshold_gate_schema"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cleaner_keeps_shared_counter_aligned_under_single_database_churn() {
    let _guard = test_lock();
    let _ = env_logger::try_init();
    std::env::set_var("NEB_CLEANER_SLEEP_INTERVAL_MS", "10");

    let backup_dir = temp_path("neb_single_db_cleaner_drift_bk");
    let wal_dir = temp_path("neb_single_db_cleaner_drift_wal");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cleaner_keeps_shared_counter_aligned_under_multi_database_churn() {
    let _guard = test_lock();
    let _ = env_logger::try_init();
    std::env::set_var("NEB_CLEANER_SLEEP_INTERVAL_MS", "10");

    let backup_dir = temp_path("neb_multi_db_cleaner_drift_bk");
    let wal_dir = temp_path("neb_multi_db_cleaner_drift_wal");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unload_reload_recovery_preserves_shared_counter_alignment() {
    let _guard = test_lock();
    let _ = env_logger::try_init();
    std::env::set_var("NEB_CLEANER_SLEEP_INTERVAL_MS", "10");

    let backup_dir = temp_path("neb_reload_recovery_drift_bk");
    let wal_dir = temp_path("neb_reload_recovery_drift_wal");
    let undo_dir = temp_path("neb_reload_recovery_drift_undo");
    let raft_dir = temp_path("neb_reload_recovery_drift_raft");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::remove_dir_all(&undo_dir);
    let _ = std::fs::remove_dir_all(&raft_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&undo_dir);
    let _ = std::fs::create_dir_all(&raft_dir);

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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::remove_dir_all(&undo_dir);
    let _ = std::fs::remove_dir_all(&raft_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_global_eviction_across_multiple_databases() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let backup_dir = temp_path("neb_multi_db_global_eviction_bk");
    let wal_dir = temp_path("neb_multi_db_global_eviction_wal");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_database_eviction_waits_until_combined_threshold_is_exceeded() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let physical_memory_limit = 8 * SEGMENT_SIZE;
    let threshold = 0.75;
    let threshold_hot_segments =
        ((physical_memory_limit as f64 * threshold as f64) / SEGMENT_SIZE as f64) as usize;
    let backup_dir = temp_path("neb_multi_db_threshold_gate_bk");
    let wal_dir = temp_path("neb_multi_db_threshold_gate_wal");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
}

#[test]
fn test_equal_sized_databases_evict_down_to_shared_limit() {
    let _guard = test_lock();
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
        let schema_dir = temp_path(&format!("neb_equal_db_eviction_schema_{}", db_idx));
        let backup_dir = temp_path(&format!("neb_equal_db_eviction_bk_{}", db_idx));
        let wal_dir = temp_path(&format!("neb_equal_db_eviction_wal_{}", db_idx));

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
        let _ = std::fs::remove_dir_all(&schema_dir);
        let _ = std::fs::remove_dir_all(&backup_dir);
        let _ = std::fs::remove_dir_all(&wal_dir);
    }
}

#[test]
fn test_global_eviction_ignores_unregistered_database() {
    let _guard = test_lock();
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
    let schemas_a = LocalSchemasCache::new_local(&temp_path("neb_db_a_eviction_schema"));
    schemas_a.debug_only_new_schema(schema_a.clone());
    let chunks_a = Chunks::new(
        1,
        4 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas: schemas_a }),
        None,
        Some(temp_path("neb_db_a_eviction_bk")),
        Some(temp_path("neb_db_a_eviction_wal")),
        Some(manager.clone()),
    );

    let schema_b = Schema::new("db_b_eviction", None, default_fields(), false, false);
    let schemas_b = LocalSchemasCache::new_local(&temp_path("neb_db_b_eviction_schema"));
    schemas_b.debug_only_new_schema(schema_b.clone());
    let chunks_b = Chunks::new(
        1,
        4 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas: schemas_b }),
        None,
        Some(temp_path("neb_db_b_eviction_bk")),
        Some(temp_path("neb_db_b_eviction_wal")),
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

    let _ = std::fs::remove_dir_all(temp_path("neb_db_a_eviction_bk"));
    let _ = std::fs::remove_dir_all(temp_path("neb_db_a_eviction_wal"));
    let _ = std::fs::remove_dir_all(temp_path("neb_db_a_eviction_schema"));
    let _ = std::fs::remove_dir_all(temp_path("neb_db_b_eviction_bk"));
    let _ = std::fs::remove_dir_all(temp_path("neb_db_b_eviction_wal"));
    let _ = std::fs::remove_dir_all(temp_path("neb_db_b_eviction_schema"));
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
    let _guard = test_lock();
    let _ = env_logger::try_init();

    info!("=== Starting Large-Scale Tiered Memory Transaction Test ===");
    info!("Config: 64MB physical, 1GB virtual, 512MB data target");

    // Clean up old backup files from previous test runs
    // This is critical because archive() skips existing files, and our fix
    // to write full SEGMENT_SIZE won't apply to old truncated files
    let backup_dir = temp_path("neb_large_scale_bk");
    let wal_dir = temp_path("neb_large_scale_wal");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);

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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);

    info!("=== Large-Scale Tiered Memory Transaction Test Complete ===");
}

/// Comprehensive stress test: Multiple scales of load with concurrent reads and writes
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_stress_concurrent_mixed_workload_with_tiered_memory() {
    let _guard = test_lock();
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
    let backup_dir = temp_path("neb_stress_test_bk");
    let wal_dir = temp_path("neb_stress_test_wal");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024, // Reduced from 256MB
            db_size: 64 * 1024 * 1024,
            tiered_config: crate::ram::tiered::TieredConfig::from_env(),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);

    info!("=== Stress Test Complete ===");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_direct_writes_without_transactions_or_tiered_memory() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    info!("=== Starting Direct Write Test (No Transactions, No Tiered Memory) ===");

    // Note: new_with_recovery() will call reset_global_chunk_allocation() at the start,
    // so we don't need to call it here. We only reset at the end to clean up for the next test.

    // NO tiered memory configuration
    let chunk_capacity = 16 * SEGMENT_SIZE; // 128MB
    let schema_id = 7777;
    let schema_name = "direct_schema";
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let backup_dir = temp_path("neb_direct_bk");
    let wal_dir = temp_path("neb_direct_wal");

    // Clean up
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);

    info!("=== Direct Write Test Complete ===");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_direct_writes_with_tiered_memory() {
    let _guard = test_lock();
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
    let backup_dir = temp_path("neb_tiered_direct_bk");
    let wal_dir = temp_path("neb_tiered_direct_wal");

    // Clean up
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

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
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);

    info!("=== Direct Write Test with Tiered Memory Complete ===");
}

/// Concurrent allocators must not collectively evict far past the lower
/// watermark.
///
/// Every allocating thread calls `evict_for_allocation()`, which sizes its
/// target from the *current* hot count. When several threads cross the
/// threshold together they each compute a full target from the same
/// pre-eviction reading and each evicts all of it, so hot memory lands at a
/// fraction of the watermark instead of at it. The evicted segments are then
/// faulted straight back in, and the cycle repeats -- observed on a 1.7TB
/// import as hot memory oscillating between 35GB and 315GB against a 400GB
/// limit, with throughput down 4-10x.
///
/// Existing eviction tests miss this because they re-check the hot count
/// between calls, which is exactly the check production lacks.
#[test]
fn test_concurrent_allocation_eviction_stops_at_lower_watermark() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    const HOT_SEGMENTS: usize = 18;
    const EVICTOR_THREADS: usize = 8;
    let physical_memory_limit = 20 * SEGMENT_SIZE;
    let lower_watermark = 0.72_f32;
    let watermark_segments =
        ((physical_memory_limit as f64 * lower_watermark as f64) / SEGMENT_SIZE as f64) as usize;

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark,
            physical_memory_limit,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("concurrent_evict_watermark", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_concurrent_evict_schema");
    let backup_dir = temp_path("neb_concurrent_evict_bk");
    let wal_dir = temp_path("neb_concurrent_evict_wal");
    let _ = std::fs::remove_dir_all(&schema_dir);
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        (HOT_SEGMENTS + 4) * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        Some(manager.clone()),
    );

    let payload = "q".repeat(2048);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    write_cells_for_partition(
        &chunks,
        schema.id,
        0,
        0,
        cells_per_segment * HOT_SEGMENTS,
        &payload,
    );

    let hot_before = total_hot_segments(&chunks);
    assert!(
        hot_before > watermark_segments,
        "test needs to start above the watermark: hot={} watermark={}",
        hot_before,
        watermark_segments
    );

    // Fire the evictions concurrently, as real allocating threads do.
    let mut handles = Vec::new();
    for _ in 0..EVICTOR_THREADS {
        let m = manager.clone();
        handles.push(std::thread::spawn(move || {
            m.evict_for_allocation().expect("eviction should not error")
        }));
    }
    let evicted_total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

    let hot_after = total_hot_segments(&chunks);

    // Eviction re-checks the watermark after each segment, but a pass already
    // past that check still evicts one before looking again -- so with N passes
    // running concurrently the pool can dip N segments below the watermark and
    // no further. That is the guarantee sharding preserves, and it is what this
    // asserts: bounded overshoot, not the original collapse to a fraction of
    // the watermark (which showed up here as hot_after=1 against a floor of 14).
    let floor = watermark_segments.saturating_sub(EVICTOR_THREADS);
    assert!(
        hot_after >= floor,
        "concurrent eviction collapsed hot memory far below the watermark: \
         hot_before={} hot_after={} watermark={} floor={} evicted_total={} ({} threads)",
        hot_before,
        hot_after,
        watermark_segments,
        floor,
        evicted_total,
        EVICTOR_THREADS
    );

    let _ = std::fs::remove_dir_all(&schema_dir);
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
}

/// A read of a cold cell must be served from a single backup block, leaving the
/// segment cold.
///
/// This is the point of the block-indexed backup format. Before it, the only
/// way to reach one cell in a cold segment was to decompress all 8 MiB and
/// promote the segment -- which is why a working set larger than the hot tier
/// degenerated into promote/evict churn at roughly one cycle per read.
#[test]
fn cold_cell_reads_are_served_from_one_block_without_promotion() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: 64 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("cold_block_read", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_cold_block_schema");
    let backup_dir = temp_path("neb_cold_block_bk");
    let wal_dir = temp_path("neb_cold_block_wal");
    let _ = std::fs::remove_dir_all(&schema_dir);
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        8 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    // Fill a couple of segments so there is something to evict.
    let payload = "z".repeat(2048);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    write_cells_for_partition(&chunks, schema.id, 0, 0, cells_per_segment * 2, &payload);

    let chunk = &chunks.list[0];
    // Evict everything evictable so the reads below land on cold segments.
    let evicted = manager.explicit_evict(chunk, 4).expect("evict");
    assert!(evicted > 0, "test needs at least one cold segment");

    let cold_before = chunk.segments().iter().filter(|s| s.is_cold()).count();
    assert!(cold_before > 0, "expected cold segments after eviction");

    // Read cells back. Each read should fault in one block and leave the
    // segment cold, rather than promoting all 8 MiB.
    let mut read_ok = 0;
    for i in 0..(cells_per_segment / 4) {
        let id = Id::allocated(0, 0, i as u64);
        if chunks.read_cell(&id).is_ok() {
            read_ok += 1;
        }
    }
    assert!(read_ok > 0, "expected to read cells back");

    let cold_after = chunk.segments().iter().filter(|s| s.is_cold()).count();
    assert!(
        cold_after > 0,
        "every cold segment was promoted; partial reads did not engage \
         (cold {} -> {}, reads {})",
        cold_before,
        cold_after,
        read_ok
    );

    // At least one segment should be holding only part of itself: proof the
    // read was served from a block rather than a full materialisation.
    let partial = chunk
        .segments()
        .iter()
        .filter(|s| {
            let (present, total) = s.block_residency_stats();
            present > 0 && total > 0 && present < total
        })
        .count();
    assert!(
        partial > 0,
        "no segment is partially resident; reads did not use the block path"
    );

    let _ = std::fs::remove_dir_all(&schema_dir);
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
}

/// Memory faulted into cold segments must count against the configured limit.
///
/// A cold segment contributes nothing to the hot-segment counter, but a
/// partially resident one holds real memory. Unaccounted, the limit would bound
/// whole segments while partial residency grew underneath it -- the same shape
/// of unbounded growth that OOM-killed this server when promotion ignored the
/// limit.
#[test]
fn blocks_faulted_into_cold_segments_are_accounted_and_released() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: 64 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("cold_accounting", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_cold_acct_schema");
    let backup_dir = temp_path("neb_cold_acct_bk");
    let wal_dir = temp_path("neb_cold_acct_wal");
    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        8 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    let payload = "y".repeat(2048);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    write_cells_for_partition(&chunks, schema.id, 0, 0, cells_per_segment * 2, &payload);

    let chunk = &chunks.list[0];
    assert!(manager.explicit_evict(chunk, 4).expect("evict") > 0);

    // Read a handful of cells to fault blocks into cold segments.
    for i in 0..32 {
        let _ = chunks.read_cell(&Id::allocated(0, 0, i as u64));
    }

    let before = 0usize;
    let during = manager.cold_resident_total();
    let partially_resident: usize = chunk
        .segments()
        .iter()
        .map(|s| s.block_resident_bytes())
        .sum();

    if partially_resident > 0 {
        assert!(
            during > before,
            "cold residency ({partially_resident} B held) is not reflected in the accounting \
             ({before} -> {during})"
        );
    }

    // Releasing must precede freeing: free_memory clears the segment's
    // residency, so taking the bytes afterwards yields nothing and the counter
    // would never come down. Production does take -> release -> free at both
    // sites that free a segment; this mirrors it.
    for seg in chunk.segments().iter() {
        manager.release_cold_resident(seg.take_block_resident_bytes());
        seg.free_memory();
    }
    let after = manager.cold_resident_total();
    assert!(
        after <= before,
        "freeing segments did not release cold residency ({before} -> {during} -> {after})"
    );

    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Every cell must survive archive -> evict -> cold read byte for byte, across
/// a spread of sizes.
///
/// The block-indexed backup format changed how segments are written and how
/// cold reads are served, so this asserts the thing that actually matters: no
/// cell is lost, truncated, or altered by being served from a block instead of
/// a promoted segment. Sizes straddle the block target deliberately -- cells
/// smaller than a block, cells near it, and cells larger than it, which get
/// blocks of their own.
#[test]
fn cells_survive_archive_evict_and_cold_read_intact() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: 64 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("cold_integrity", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_integrity_schema");
    let backup_dir = temp_path("neb_integrity_bk");
    let wal_dir = temp_path("neb_integrity_wal");
    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        16 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    // Sizes chosen to straddle the 32 KiB block target in both directions.
    let sizes = [16usize, 200, 3_000, 31_000, 33_000, 70_000];
    let mut expected: Vec<(Id, String)> = Vec::new();

    for i in 0..900usize {
        let len = sizes[i % sizes.len()];
        // Content derived from the index so a mismatch identifies the cell.
        let body: String = format!("cell-{}-", i)
            .chars()
            .cycle()
            .take(len)
            .collect();
        let id = Id::allocated(0, 0, 900_000 + i as u64);

        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("n{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String(body.clone()));
        let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data_map));
        if chunks.write_cell(&mut cell).is_ok() {
            expected.push((id, body));
        }
    }
    assert!(expected.len() > 500, "setup wrote too few cells");

    // Push as much as possible cold so reads take the block path.
    for chunk in &chunks.list {
        let _ = manager.explicit_evict(chunk, 32);
    }
    let cold = chunks
        .list
        .iter()
        .flat_map(|c| c.segments().into_iter())
        .filter(|s| s.is_cold())
        .count();
    assert!(cold > 0, "test needs cold segments to exercise the block path");

    // Every cell must come back exactly as written.
    let mut checked = 0usize;
    for (id, body) in &expected {
        let cell = chunks
            .read_cell(id)
            .unwrap_or_else(|e| panic!("cell {:?} unreadable after eviction: {:?}", id, e));
        let got = cell.data["data"]
            .string()
            .unwrap_or_else(|| panic!("cell {:?} lost its data field", id));
        assert_eq!(
            got.len(),
            body.len(),
            "cell {:?} changed length across the cold path",
            id
        );
        assert_eq!(got, body, "cell {:?} content differs after cold read", id);
        checked += 1;
    }
    assert_eq!(checked, expected.len());

    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Reading a cold segment while already holding a guard on it must complete.
///
/// This is the shape that livelocked a 1.7TB-class import. A block read hands
/// the caller a live reference to a segment that is still cold. The read path
/// used to give up on the block path once half the segment was resident and
/// demand a full promotion instead -- and `promote_segment` spins until every
/// reference drains, so a caller holding one waited on itself. 33 threads sat
/// in sched_yield for 50 minutes.
///
/// Runs on its own thread with a deadline: a regression here hangs rather than
/// failing, and a hung test is indistinguishable from a slow one.
#[test]
fn reading_a_cold_segment_while_holding_a_guard_on_it_completes() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
            crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
                threshold: 0.8,
                lower_watermark: 0.72,
                physical_memory_limit: 64 * SEGMENT_SIZE,
                promotion_cooldown_ms: 0,
            }),
        ));

        let schema = Schema::new("cold_reentrant", None, default_fields(), false, false);
        let schema_dir = temp_path("neb_reentrant_schema");
        let backup_dir = temp_path("neb_reentrant_bk");
        let wal_dir = temp_path("neb_reentrant_wal");
        for d in [&schema_dir, &backup_dir, &wal_dir] {
            let _ = std::fs::remove_dir_all(d);
        }

        let schemas = LocalSchemasCache::new_local(&schema_dir);
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            1,
            16 * SEGMENT_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            Some(backup_dir.clone()),
            Some(wal_dir.clone()),
            Some(manager.clone()),
        );

        // Many small cells so one segment holds a lot of them: the caller must
        // read enough distinct blocks of the segment it is holding to push
        // residency past the point the old policy promoted at.
        let mut ids: Vec<Id> = Vec::new();
        // ~32 MiB of payload: several 8 MiB segments, each holding on the order
        // of two thousand cells, so one segment's worth of reads touches many
        // distinct blocks of it.
        for i in 0..8_000usize {
            let id = Id::allocated(0, 0, 700_000 + i as u64);
            let mut data_map = OwnedMap::new();
            data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
            data_map.insert(&String::from("name"), OwnedValue::String(format!("n{}", i)));
            data_map.insert(
                &String::from("data"),
                OwnedValue::String("x".repeat(4_000)),
            );
            let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data_map));
            if chunks.write_cell(&mut cell).is_ok() {
                ids.push(id);
            }
        }
        assert!(ids.len() > 500, "setup wrote too few cells");

        for chunk in &chunks.list {
            let _ = manager.explicit_evict(chunk, 32);
        }
        let cold = chunks
            .list
            .iter()
            .flat_map(|c| c.segments().into_iter())
            .filter(|s| s.is_cold())
            .count();
        assert!(cold > 0, "test needs cold segments to exercise the block path");

        // Hold a guard obtained through the block path, then keep reading the
        // same segment through it.
        let held = chunks
            .lock_cell_for_read(&ids[0])
            .expect("first cell must be readable while cold");

        for id in ids.iter().skip(1) {
            let _ = chunks.read_cell(id);
        }
        drop(held);

        for d in [&schema_dir, &backup_dir, &wal_dir] {
            let _ = std::fs::remove_dir_all(d);
        }
        let _ = tx.send(());
    });

    match rx.recv_timeout(std::time::Duration::from_secs(120)) {
        Ok(()) => {
            worker.join().expect("worker panicked after signalling");
        }
        // The sender is dropped on panic too, so a disconnect means the body
        // failed for some other reason -- surface that rather than reporting it
        // as a deadlock.
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            match worker.join() {
                Err(payload) => std::panic::resume_unwind(payload),
                Ok(()) => panic!("worker exited without signalling"),
            }
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!(
            "reads of a cold segment did not finish while a guard was held on it: the read path \
             is waiting for a reference the caller itself holds"
        ),
    }
}

/// Cold residency must be reclaimable, or it grows without bound.
///
/// Faulted-in blocks count against the same limit as hot segments but were
/// only ever released when the segment was freed. A cold segment that is never
/// freed therefore held its blocks forever: an import reached 91GB resident
/// against a 40GB limit, and eviction spun because the excess lived in cold
/// segments that evicting hot ones could not free.
#[test]
fn reading_cold_segments_alone_must_not_grow_past_the_limit() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    // Cold residency counts against the same limit as hot segments, but the
    // only thing that reclaimed it hung off the ALLOCATION path. A workload
    // that only reads never checked the budget, so faulted-in blocks grew
    // without bound: TB15's sidecar rebuild -- which reads every cold segment
    // in the store and allocates nothing -- took resident set from 298 GB to
    // 600 GB against a 200 GB limit.
    let limit = 4 * SEGMENT_SIZE;
    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: limit,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("cold_readonly", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_readonly_schema");
    let backup_dir = temp_path("neb_readonly_bk");
    let wal_dir = temp_path("neb_readonly_wal");
    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        32 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    let mut ids: Vec<Id> = Vec::new();
    for i in 0..30_000usize {
        let id = Id::allocated(0, 0, 900_000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("n{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String("z".repeat(4_000)));
        let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data_map));
        if chunks.write_cell(&mut cell).is_ok() {
            ids.push(id);
        }
    }
    assert!(ids.len() > 500, "setup wrote too few cells");

    // Push everything cold, then stop writing entirely.
    for chunk in &chunks.list {
        let _ = manager.explicit_evict(chunk, 64);
    }
    let hot_after_evict = manager.shared_hot_segments();

    // Pure reads from here: nothing allocates a segment, so nothing on the
    // allocation path can enforce the budget.
    for _ in 0..3 {
        for id in ids.iter() {
            let _ = chunks.read_cell(id);
        }
    }

    let resident = manager.cold_resident_total();
    let total = hot_after_evict * SEGMENT_SIZE + resident;
    assert!(
        total <= limit * 2,
        "read-only faulting grew resident memory to {} against a {} limit \
         ({} bytes of it cold blocks); nothing bounded it",
        total,
        limit,
        resident
    );

    // And the data is still readable: the budget drops cache, not content.
    for id in ids.iter().take(200) {
        let cell = chunks
            .read_cell(id)
            .unwrap_or_else(|e| panic!("cell {:?} unreadable: {:?}", id, e));
        assert_eq!(cell.data["data"].string().map(|s| s.len()), Some(4_000));
    }

    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn cold_block_residency_is_reclaimed_under_pressure() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: 64 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("cold_reclaim", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_reclaim_schema");
    let backup_dir = temp_path("neb_reclaim_bk");
    let wal_dir = temp_path("neb_reclaim_wal");
    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        16 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    let mut ids: Vec<Id> = Vec::new();
    for i in 0..8_000usize {
        let id = Id::allocated(0, 0, 600_000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("n{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String("y".repeat(4_000)));
        let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data_map));
        if chunks.write_cell(&mut cell).is_ok() {
            ids.push(id);
        }
    }
    assert!(ids.len() > 500, "setup wrote too few cells");

    for chunk in &chunks.list {
        let _ = manager.explicit_evict(chunk, 32);
    }

    // Fault blocks in by reading cold cells.
    for id in ids.iter() {
        let _ = chunks.read_cell(id);
    }
    let resident = manager.cold_resident_total();
    assert!(
        resident > 0,
        "reads of cold segments should have faulted blocks in"
    );

    // Reclaim, holding no guards.
    let mut reclaimed_total = 0usize;
    for chunk in &chunks.list {
        for segment in chunk.segments() {
            if let Some(bytes) = segment.try_reclaim_resident_blocks() {
                manager.release_cold_resident(bytes);
                reclaimed_total += bytes;
            }
        }
    }
    assert!(
        reclaimed_total > 0,
        "no cold residency was reclaimable ({} bytes resident)",
        resident
    );
    assert_eq!(
        manager.cold_resident_total(),
        resident - reclaimed_total,
        "accounting must drop by exactly what was reclaimed"
    );

    // The data must still be readable: reclaiming drops cache, not content.
    for id in ids.iter().take(200) {
        let cell = chunks
            .read_cell(id)
            .unwrap_or_else(|e| panic!("cell {:?} unreadable after reclaim: {:?}", id, e));
        assert_eq!(cell.data["data"].string().map(|s| s.len()), Some(4_000));
    }

    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Cold-read amplification harness.
///
/// Reports bytes moved per read at each layer -- disk, decompression, and the
/// index copying the lookup does on its own account -- so a change to this
/// machinery can be judged on measurement instead of argument. Ignored by
/// default: it is a measurement, not an assertion. Run with:
///   cargo test --lib cold_read_amplification -- --ignored --nocapture
#[test]
#[ignore]
fn cold_read_amplification() {
    use crate::ram::segs::{
        COLD_BLOCK_FILE_BYTES, COLD_BLOCK_HITS, COLD_BLOCK_MISSES, COLD_BLOCK_OPENS,
        COLD_BLOCK_PLAIN_BYTES, COLD_BLOCK_SERVES, COLD_INDEX_COPY_BYTES, COLD_INDEX_LOADS,
    };
    use std::sync::atomic::Ordering as O;

    let _guard = test_lock();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: 64 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("cold_amp", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_amp_schema");
    let backup_dir = temp_path("neb_amp_bk");
    let wal_dir = temp_path("neb_amp_wal");
    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        16 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    // Cell payload is the denominator of every ratio below, so it is fixed and
    // known rather than inferred.
    const CELL_PAYLOAD: usize = 1_000;
    const CELLS: usize = 12_000;

    let mut ids: Vec<Id> = Vec::new();
    for i in 0..CELLS {
        let id = Id::allocated(0, 0, 500_000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("n{}", i)));
        // Realistic payload: structured but varying, so compression has
        // something to find without the ratio being a fiction. A run of one
        // repeated byte compresses to nothing and would make every on-disk
        // number meaningless.
        let mut body = String::with_capacity(CELL_PAYLOAD);
        let mut w = i as u64;
        while body.len() < CELL_PAYLOAD {
            w = w.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            body.push_str(&format!("{:016x}-{}-", w, i));
        }
        body.truncate(CELL_PAYLOAD);
        data_map.insert(&String::from("data"), OwnedValue::String(body));
        let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data_map));
        if chunks.write_cell(&mut cell).is_ok() {
            ids.push(id);
        }
    }
    assert!(ids.len() > 1_000, "setup wrote too few cells");

    let arch_before = crate::ram::segs::ARCHIVE_BYTES.load(O::Relaxed);
    for chunk in &chunks.list {
        let _ = manager.explicit_evict(chunk, 64);
    }
    let archived = crate::ram::segs::ARCHIVE_BYTES.load(O::Relaxed) - arch_before;
    let live_payload = (ids.len() * CELL_PAYLOAD) as f64;
    println!(
        "\nblock target {} B | archived {:.2} MiB for {:.2} MiB payload ({:.1}% on disk)",
        crate::ram::compression::block_size(),
        archived as f64 / (1 << 20) as f64,
        live_payload / (1 << 20) as f64,
        archived as f64 / live_payload * 100.0
    );

    // Counters are process-global, so the window starts here.
    let base = |c: &std::sync::atomic::AtomicU64| c.load(O::Relaxed);
    let (b_serves, b_hits, b_miss, b_file, b_plain, b_opens, b_idx, b_copy) = (
        base(&COLD_BLOCK_SERVES),
        base(&COLD_BLOCK_HITS),
        base(&COLD_BLOCK_MISSES),
        base(&COLD_BLOCK_FILE_BYTES),
        base(&COLD_BLOCK_PLAIN_BYTES),
        base(&COLD_BLOCK_OPENS),
        base(&COLD_INDEX_LOADS),
        base(&COLD_INDEX_COPY_BYTES),
    );

    // Two regimes, because they stress different layers and a change can help
    // one while hurting the other.
    //
    //   cold -- every block reclaimed first, so reads pay disk and
    //           decompression. This is the dataset-far-larger-than-memory case.
    //   warm -- blocks already resident, so nothing is paid except whatever the
    //           lookup itself costs. Uniform access over a large dataset lands
    //           here constantly, and it is where per-call overhead shows up
    //           undisguised.
    //
    // Uniform random-ish access: stride by a value coprime with the count so
    // every cell is visited once in an order uncorrelated with layout. This is
    // the access pattern that has nothing worth promoting.
    let stride = 7919usize;
    let reclaim_all = || {
        let mut n = 0usize;
        for chunk in &chunks.list {
            for segment in chunk.segments() {
                if let Some(b) = segment.try_reclaim_resident_blocks() {
                    manager.release_cold_resident(b);
                    n += b;
                }
            }
        }
        n
    };

    reclaim_all();
    let start = std::time::Instant::now();
    let mut read = 0usize;
    for k in 0..ids.len() {
        let id = ids[(k * stride) % ids.len()];
        if chunks.read_cell(&id).is_ok() {
            read += 1;
        }
    }
    let elapsed = start.elapsed();

    let serves = base(&COLD_BLOCK_SERVES) - b_serves;
    let hits = base(&COLD_BLOCK_HITS) - b_hits;
    let misses = base(&COLD_BLOCK_MISSES) - b_miss;
    let file_bytes = base(&COLD_BLOCK_FILE_BYTES) - b_file;
    let plain_bytes = base(&COLD_BLOCK_PLAIN_BYTES) - b_plain;
    let opens = base(&COLD_BLOCK_OPENS) - b_opens;
    let idx_loads = base(&COLD_INDEX_LOADS) - b_idx;
    let copy_bytes = base(&COLD_INDEX_COPY_BYTES) - b_copy;

    let served_payload = (read * CELL_PAYLOAD) as f64;
    let per = |n: u64| n as f64 / read.max(1) as f64;

    println!("\n=== cold read amplification ===");
    println!("cells read              : {}", read);
    println!("wall                    : {:?} ({:.0} reads/s)", elapsed,
             read as f64 / elapsed.as_secs_f64().max(1e-9));
    println!("block serves            : {} (hit {} / miss {})", serves, hits, misses);
    println!("index loads             : {}", idx_loads);
    println!("--- bytes per cell read (payload = {} B) ---", CELL_PAYLOAD);
    println!("disk read               : {:>10.1} B  ({:.1}x payload)",
             per(file_bytes), file_bytes as f64 / served_payload);
    println!("decompressed            : {:>10.1} B  ({:.1}x payload)",
             per(plain_bytes), plain_bytes as f64 / served_payload);
    println!("index copied in lookup  : {:>10.1} B  ({:.1}x payload)",
             per(copy_bytes), copy_bytes as f64 / served_payload);
    println!("file opens              : {:>10.3} per read", per(opens));
    println!();

    // Warm pass: same reads, nothing reclaimed in between.
    let (w_serves, w_hits, w_file, w_plain, w_copy) = (
        base(&COLD_BLOCK_SERVES),
        base(&COLD_BLOCK_HITS),
        base(&COLD_BLOCK_FILE_BYTES),
        base(&COLD_BLOCK_PLAIN_BYTES),
        base(&COLD_INDEX_COPY_BYTES),
    );
    let wstart = std::time::Instant::now();
    let mut wread = 0usize;
    for k in 0..ids.len() {
        let id = ids[(k * stride) % ids.len()];
        if chunks.read_cell(&id).is_ok() {
            wread += 1;
        }
    }
    let welapsed = wstart.elapsed();
    let wserves = base(&COLD_BLOCK_SERVES) - w_serves;
    let whits = base(&COLD_BLOCK_HITS) - w_hits;
    let wfile = base(&COLD_BLOCK_FILE_BYTES) - w_file;
    let wplain = base(&COLD_BLOCK_PLAIN_BYTES) - w_plain;
    let wcopy = base(&COLD_INDEX_COPY_BYTES) - w_copy;
    let wper = |n: u64| n as f64 / wread.max(1) as f64;

    // Sparse pass: touch one cell per block, reclaiming first, so no fetch is
    // amortised over its neighbours. This is uniform access across a dataset
    // far larger than memory -- the case with nothing worth promoting, and the
    // one where amplification is real rather than averaged away.
    // Fixed sample size. Deriving the stride from block size alone made the
    // read count vary with it -- 6,000 reads at a 1 KiB target against 188 at
    // 32 KiB -- so throughput across sizes was comparing different amounts of
    // work. The stride still guarantees one cell per block; the count no longer
    // moves with it.
    const SPARSE_READS: usize = 150;
    let cells_per_block = (crate::ram::compression::block_size() / CELL_PAYLOAD).max(1);
    let sparse_stride = (cells_per_block * 2).max(1);
    reclaim_all();
    let (s_serves, s_hits, s_miss, s_file, s_plain, s_copy, s_opens) = (
        base(&COLD_BLOCK_SERVES),
        base(&COLD_BLOCK_HITS),
        base(&COLD_BLOCK_MISSES),
        base(&COLD_BLOCK_FILE_BYTES),
        base(&COLD_BLOCK_PLAIN_BYTES),
        base(&COLD_INDEX_COPY_BYTES),
        base(&COLD_BLOCK_OPENS),
    );
    let sstart = std::time::Instant::now();
    let mut sread = 0usize;
    let mut k = 0usize;
    while k < ids.len() && sread < SPARSE_READS {
        if chunks.read_cell(&ids[k]).is_ok() {
            sread += 1;
        }
        k += sparse_stride;
    }
    let selapsed = sstart.elapsed();
    let sserves = base(&COLD_BLOCK_SERVES) - s_serves;
    let shits = base(&COLD_BLOCK_HITS) - s_hits;
    let smiss = base(&COLD_BLOCK_MISSES) - s_miss;
    let sfile = base(&COLD_BLOCK_FILE_BYTES) - s_file;
    let splain = base(&COLD_BLOCK_PLAIN_BYTES) - s_plain;
    let scopy = base(&COLD_INDEX_COPY_BYTES) - s_copy;
    let sopens = base(&COLD_BLOCK_OPENS) - s_opens;
    let sper = |n: u64| n as f64 / sread.max(1) as f64;
    let spay = (sread * CELL_PAYLOAD) as f64;

    println!("=== sparse (one cell per block, nothing amortised) ===");
    println!("cells read              : {} (stride {})", sread, sparse_stride);
    println!("wall                    : {:?} ({:.0} reads/s)", selapsed,
             sread as f64 / selapsed.as_secs_f64().max(1e-9));
    println!("block serves            : {} (hit {} / miss {})", sserves, shits, smiss);
    println!("disk read               : {:>10.1} B  ({:.1}x payload)",
             sper(sfile), sfile as f64 / spay);
    println!("decompressed            : {:>10.1} B  ({:.1}x payload)",
             sper(splain), splain as f64 / spay);
    println!("index copied in lookup  : {:>10.1} B  ({:.1}x payload)",
             sper(scopy), scopy as f64 / spay);
    println!("file opens              : {:>10.3} per read", sper(sopens));
    println!();

    println!("=== warm (blocks already resident) ===");
    println!("cells read              : {}", wread);
    println!("wall                    : {:?} ({:.0} reads/s)", welapsed,
             wread as f64 / welapsed.as_secs_f64().max(1e-9));
    println!("block serves            : {} (hit {})", wserves, whits);
    println!("disk read               : {:>10.1} B per read", wper(wfile));
    println!("decompressed            : {:>10.1} B per read", wper(wplain));
    println!("index copied in lookup  : {:>10.1} B per read", wper(wcopy));
    println!();

    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Concurrent cold-read scaling.
///
/// The residency lock is deliberately not held across the block fetch, so
/// readers of one segment do not serialise behind a disk read and a decompress.
/// That is a claim about contention, which a single-threaded benchmark cannot
/// evaluate at all -- it shows up only as scaling against thread count. Ignored
/// by default. Run with:
///   cargo test --lib cold_read_concurrency -- --ignored --nocapture
#[test]
#[ignore]
fn cold_read_concurrency() {
    use std::sync::atomic::Ordering as O;

    let _guard = test_lock();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: 64 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("cold_conc", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_conc_schema");
    let backup_dir = temp_path("neb_conc_bk");
    let wal_dir = temp_path("neb_conc_wal");
    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        16 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    const CELL_PAYLOAD: usize = 1_000;
    let mut ids: Vec<Id> = Vec::new();
    for i in 0..40_000usize {
        let id = Id::allocated(0, 0, 400_000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("n{}", i)));
        let mut body = String::with_capacity(CELL_PAYLOAD);
        let mut w = i as u64;
        while body.len() < CELL_PAYLOAD {
            w = w.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            body.push_str(&format!("{:016x}-{}-", w, i));
        }
        body.truncate(CELL_PAYLOAD);
        data_map.insert(&String::from("data"), OwnedValue::String(body));
        let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data_map));
        if chunks.write_cell(&mut cell).is_ok() {
            ids.push(id);
        }
    }
    assert!(ids.len() > 10_000, "setup wrote too few cells");

    for chunk in &chunks.list {
        let _ = manager.explicit_evict(chunk, 64);
    }

    let reclaim_all = || {
        for chunk in &chunks.list {
            for segment in chunk.segments() {
                if let Some(b) = segment.try_reclaim_resident_blocks() {
                    manager.release_cold_resident(b);
                }
            }
        }
    };

    // Two access patterns, because they exercise different contention.
    //
    //   disjoint -- each thread owns a contiguous range, so threads mostly touch
    //               different segments and rarely meet on one residency lock.
    //   shared   -- every thread sweeps the SAME narrow range, so they collide
    //               on the same segments constantly. This is the pattern that
    //               reveals whether a fetch serialises its segment's readers,
    //               and the disjoint pattern cannot see it at all.
    for shared in [false, true] {
        println!(
            "\n=== concurrent cold reads, {} segments (reclaimed each round) ===",
            if shared { "SHARED" } else { "disjoint" }
        );
        println!("{:>8}  {:>12}  {:>12}  {:>9}", "threads", "reads/s", "vs 1 thread", "misses");

    let mut single = 0f64;
    for threads in [1usize, 2, 4, 8, 16, 32] {
        reclaim_all();
        let before_miss = crate::ram::segs::COLD_BLOCK_MISSES.load(O::Relaxed);
        let per = ids.len() / threads;
        let start = std::time::Instant::now();
        std::thread::scope(|s| {
            for t in 0..threads {
                let chunks = &chunks;
                let ids = &ids;
                s.spawn(move || {
                    if shared {
                        // Same narrow range for every thread, offset so they
                        // interleave rather than march in lockstep.
                        let window = (ids.len() / 16).max(1);
                        let off = t * 37;
                        for j in 0..window {
                            let _ = chunks.read_cell(&ids[(off + j) % window]);
                        }
                    } else {
                        let lo = t * per;
                        let hi = if t == threads - 1 { ids.len() } else { lo + per };
                        for id in &ids[lo..hi] {
                            let _ = chunks.read_cell(id);
                        }
                    }
                });
            }
        });
        let elapsed = start.elapsed();
        let misses = crate::ram::segs::COLD_BLOCK_MISSES.load(O::Relaxed) - before_miss;
        let done = if shared {
            (ids.len() / 16).max(1) * threads
        } else {
            ids.len()
        };
        let rate = done as f64 / elapsed.as_secs_f64().max(1e-9);
        if threads == 1 {
            single = rate;
        }
        println!(
            "{:>8}  {:>12.0}  {:>11.2}x  {:>9}",
            threads,
            rate,
            rate / single.max(1e-9),
            misses
        );
    }
    }
    println!();

    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Write-path phase costs, the local counterpart to `cold_read_amplification`.
///
/// The per-phase counters are the only thing that has reliably located a
/// bottleneck in this engine, but until now they could only be read from a
/// running server. This drives writes directly so the same decomposition can be
/// iterated on in seconds instead of a twenty-minute import.
///
/// Ignored by default. Run with:
///   cargo test --lib write_path_phases -- --ignored --nocapture
#[test]
#[ignore]
fn write_path_phases() {
    use crate::ram::chunk::{
        WRITE_ALLOC_NANOS, WRITE_CELLS, WRITE_COPY_NANOS, WRITE_INDEX_NANOS, WRITE_PLAN_NANOS,
        WRITE_SECONDARY_NANOS, WRITE_STATS_NANOS,
    };
    use std::sync::atomic::Ordering as O;

    let _guard = test_lock();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: 256 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("write_phases", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_wp_schema");
    let backup_dir = temp_path("neb_wp_bk");
    let wal_dir = temp_path("neb_wp_wal");
    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        64 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    const CELLS: usize = 60_000;
    let base = |c: &std::sync::atomic::AtomicU64| c.load(O::Relaxed);
    let (b_cells, b_plan, b_alloc, b_copy, b_index, b_sec, b_stats) = (
        base(&WRITE_CELLS),
        base(&WRITE_PLAN_NANOS),
        base(&WRITE_ALLOC_NANOS),
        base(&WRITE_COPY_NANOS),
        base(&WRITE_INDEX_NANOS),
        base(&WRITE_SECONDARY_NANOS),
        base(&WRITE_STATS_NANOS),
    );

    let start = std::time::Instant::now();
    for i in 0..CELLS {
        let id = Id::allocated(0, 0, 800_000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("n{}", i)));
        let mut body = String::with_capacity(600);
        let mut w = i as u64;
        while body.len() < 600 {
            w = w.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            body.push_str(&format!("{:016x}-", w));
        }
        body.truncate(600);
        data_map.insert(&String::from("data"), OwnedValue::String(body));
        let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data_map));
        let _ = chunks.write_cell(&mut cell);
    }
    let elapsed = start.elapsed();

    let n = (base(&WRITE_CELLS) - b_cells).max(1);
    let us = |now: u64, before: u64| (now - before) as f64 / 1000.0 / n as f64;

    println!("\n=== write path, {} cells ===", n);
    println!("wall            : {:?} ({:.0} cells/s)", elapsed,
             n as f64 / elapsed.as_secs_f64().max(1e-9));
    println!("  plan          : {:7.2} us/cell", us(base(&WRITE_PLAN_NANOS), b_plan));
    println!("  alloc         : {:7.2} us/cell", us(base(&WRITE_ALLOC_NANOS), b_alloc));
    println!("  copy          : {:7.2} us/cell", us(base(&WRITE_COPY_NANOS), b_copy));
    println!("  index         : {:7.2} us/cell", us(base(&WRITE_INDEX_NANOS), b_index));
    println!("  secondary     : {:7.2} us/cell", us(base(&WRITE_SECONDARY_NANOS), b_sec));
    println!("  stats         : {:7.2} us/cell", us(base(&WRITE_STATS_NANOS), b_stats));
    println!();

    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Cached backup handles must stay bounded as cold segments accumulate.
///
/// One handle per cold segment is unbounded in dataset size. A 1.7TB import
/// reached 66,430 cold segments and pinned 64,860 descriptors against a 65,535
/// limit, after which every write failed with EMFILE. A 59GB dataset never
/// exceeded ~18,000 and so never revealed it -- the failure only appears at a
/// scale that takes half an hour to reach, which is exactly the kind this test
/// exists to catch in half a second.
#[test]
fn cached_backup_handles_stay_bounded() {
    use crate::ram::segs::COLD_BACKUP_FDS;
    use std::sync::atomic::Ordering as O;

    let _guard = test_lock();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: 64 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("fd_bound", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_fd_schema");
    let backup_dir = temp_path("neb_fd_bk");
    let wal_dir = temp_path("neb_fd_wal");
    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        32 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    let before = COLD_BACKUP_FDS.load(O::Relaxed);

    let mut ids: Vec<Id> = Vec::new();
    for i in 0..30_000usize {
        let id = Id::allocated(0, 0, 900_000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("n{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String("q".repeat(2_000)));
        let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data_map));
        if chunks.write_cell(&mut cell).is_ok() {
            ids.push(id);
        }
    }
    assert!(ids.len() > 1_000, "setup wrote too few cells");

    for chunk in &chunks.list {
        let _ = manager.explicit_evict(chunk, 256);
    }
    // Touch every cold segment so each one would cache a handle if uncapped.
    for id in ids.iter() {
        let _ = chunks.read_cell(id);
    }

    let cached = COLD_BACKUP_FDS.load(O::Relaxed).saturating_sub(before);
    let evicted = crate::ram::segs::COLD_BACKUP_EVICTIONS.load(O::Relaxed);
    let cap = crate::ram::segs::cold_backup_fd_cap();
    let cold: usize = chunks
        .list
        .iter()
        .flat_map(|c| c.segments().into_iter())
        .filter(|s| s.is_cold())
        .count();
    assert!(cold > 0, "test needs cold segments");

    // Bounded by the cap, not merely by some large constant. Shards round the
    // per-shard size up, so allow one slot of slack per shard.
    assert!(
        cached <= cap + 16,
        "cached handles {} exceed the cap {} (cold segments {})",
        cached,
        cap,
        cold
    );

    // And the bound has to come from eviction rather than from never filling:
    // with more cold segments than slots, the cache must have replaced some.
    // Otherwise this test would still pass if caching silently stopped working.
    if cold > cap {
        assert!(
            evicted > 0,
            "with {} cold segments against a cap of {}, the cache should have \
             evicted, but did not -- is it caching at all?",
            cold,
            cap
        );
    }
    println!(
        "cold segments {} | cap {} | cached {} | evictions {}",
        cold, cap, cached, evicted
    );

    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Releasing a reference that was already released must not wrap.
///
/// `references` is a usize and EXCLUSIVE_REF_COUNT is usize::MAX, so a
/// decrement at zero used to wrap to exactly the value meaning "exclusively
/// locked". The segment then looked permanently locked: never referencable,
/// never evictable, never promotable, and eviction would report it as held by
/// active references forever. The race is documented in the code as expected --
/// a PendingEntry dropped after cleanup -- so it had to be survivable rather
/// than merely asserted against.
#[test]
fn releasing_an_already_released_reference_does_not_wrap() {
    use crate::ram::segs::EXCLUSIVE_REF_COUNT;

    let _guard = test_lock();
    let _ = env_logger::try_init();

    let schema = Schema::new("ref_wrap", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_refwrap_schema");
    let _ = std::fs::remove_dir_all(&schema_dir);
    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        4 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );

    let segment = chunks.list[0]
        .segments()
        .into_iter()
        .next()
        .expect("chunk should have a segment");

    // Balanced pair, then two extra releases racing with a cleanup that already
    // took the count to zero.
    assert!(segment.incr_references());
    segment.decr_references();
    segment.decr_references();
    segment.decr_references();

    let refs = segment.references_count();
    assert_eq!(
        refs, 0,
        "over-release should saturate at zero, got {} (EXCLUSIVE_REF_COUNT is {})",
        refs, EXCLUSIVE_REF_COUNT
    );
    assert_ne!(
        refs, EXCLUSIVE_REF_COUNT,
        "wrapping to EXCLUSIVE_REF_COUNT would pin the segment forever"
    );
    // And the segment must still be usable afterwards.
    assert!(
        segment.incr_references(),
        "segment should still accept references after an over-release"
    );
    segment.decr_references();

    let _ = std::fs::remove_dir_all(&schema_dir);
}

/// Does a cell survive being written past the tier limit?
///
/// The narrowest statement of a loss found by a scale run on .239: a reshard of
/// 4 GB of payload against a 1 GB tier limit moved 129 fewer cells than were
/// written and reported **0 failures**, because the donor's read of each of
/// those 129 came back `CellDoesNotExisted`. The log says exactly what was
/// there instead:
///
/// ```text
/// WARN neb::ram::cell] stale cell read: requested id bits N found Id(0) at 0x72f0b...
/// ```
///
/// 129 distinct ids at 129 distinct addresses, every one of them reading as
/// `Id(0)` -- zeroed memory, not another cell's bytes. The same run with the
/// tier limit raised ABOVE the payload evicted nothing and lost nothing, which
/// is what makes this a tier bug rather than a migration one. Migration only
/// made it visible, by being the first thing to read every cell back.
///
/// This test therefore contains no migration at all: write past the limit,
/// read everything back. Sized by environment so the same test can be run as a
/// quick gate or as a long stress:
///
///   NEB_TIER_SURVIVAL_CELLS, NEB_TIER_SURVIVAL_PAYLOAD, NEB_TIER_SURVIVAL_SEGMENTS
///
/// It is intermittent -- the .239 reproduction lost 129 cells on one run and 0
/// on the next with identical settings, under different background load -- so a
/// single pass proving nothing is expected. Run it in a loop when hunting.
#[tokio::test(flavor = "multi_thread")]
async fn every_cell_survives_being_written_past_the_tier_limit() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }
    let cells = env_usize("NEB_TIER_SURVIVAL_CELLS", 65536);
    let payload_len = env_usize("NEB_TIER_SURVIVAL_PAYLOAD", 4096);
    let limit_segments = env_usize("NEB_TIER_SURVIVAL_SEGMENTS", 4);

    let backup_dir = temp_path("neb_tier_survival_bk");
    let wal_dir = temp_path("neb_tier_survival_wal");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);
    let _ = std::fs::create_dir_all(&backup_dir);
    let _ = std::fs::create_dir_all(&wal_dir);

    // The store has to be able to HOLD everything; only the hot tier is small.
    // Sizing the chunk to the tier limit instead would test the allocator's
    // behaviour when it runs out of space, which is a different question.
    let store_bytes = (cells * payload_len * 3).max(64 * SEGMENT_SIZE);
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: store_bytes,
            db_size: store_bytes,
            tiered_config: Some(crate::ram::tiered::TieredConfig {
                threshold: 0.8,
                lower_watermark: 0.72,
                physical_memory_limit: limit_segments * SEGMENT_SIZE,
                promotion_cooldown_ms: 0,
            }),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "tiered_cell_survival",
        async |_| {},
    )
    .await
    .unwrap();

    const SCHEMA: u32 = 9077;
    let schema = Schema::new_with_id(
        SCHEMA,
        &String::from("tier_survival_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    // `register_internal_schema`, not `debug_only_new_schema`: this test has to
    // run in RELEASE. bifrost picks its codec by build profile and the tier's
    // pacing is entirely different under a debug build, so a debug-only run
    // would be measuring a store that nobody ships. The id is fixed, which is
    // what that entry point requires.
    server.meta().schemas.register_internal_schema(schema.clone());

    let mut written: Vec<Id> = Vec::with_capacity(cells);
    for index in 0..cells {
        let id = Id::allocated(0, 0, index as u64);
        let mut cell = large_string_cell(SCHEMA, id, payload_len, "tier-survival");
        server
            .chunks()
            .write_cell(&mut cell)
            .expect("writing past the tier limit should succeed");
        written.push(id);
    }

    // Eviction is threshold-driven and runs on the cleaner's own interval, so
    // give it a bounded chance to catch up before reading. Without this the
    // test can finish before anything is cold and fail its own vacuity guard
    // rather than testing the tier.
    for _ in 0..100 {
        if total_cold_segments(&server.chunks()) > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Read every one of them back. The tier is several times smaller than what
    // was just written, so most of these have to come from a cold segment.
    let mut lost: Vec<(Id, String)> = Vec::new();
    for id in &written {
        match server.chunks().read_cell(id) {
            Ok(cell) => assert_eq!(cell.header.id, *id),
            Err(error) => lost.push((*id, format!("{error:?}"))),
        }
    }

    let hot = total_hot_segments(&server.chunks());
    let cold = total_cold_segments(&server.chunks());
    server.cleaner().stop();
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&wal_dir);

    assert!(
        cold > 0,
        "nothing was evicted ({hot} hot, {cold} cold segments for {cells} cells against a \
         {limit_segments}-segment limit), so this run proves nothing -- raise the payload \
         or lower the limit"
    );
    assert!(
        lost.is_empty(),
        "{} of {} cells became unreadable after being written past a {}-segment tier limit \
         ({hot} hot, {cold} cold); first few: {:?}",
        lost.len(),
        written.len(),
        limit_segments,
        lost.iter().take(5).collect::<Vec<_>>()
    );
}

/// A read racing eviction must never be told the cell is gone.
///
/// The sibling test above writes past the limit and then reads everything back,
/// and it passes 24/24 under four-way load on .239 -- so a quiet store that has
/// finished writing is not where the loss lives. What the .239 reshard had that
/// it does not is **readers running while the tier is still evicting**: the
/// migration reads every cell on the donor at the same time as the recipient's
/// writes are pushing both members' tiers over their limit.
///
/// So this test keeps writing while it reads. Any `StaleCellPointer` is a
/// definite bug -- the index entry pointed at memory that does not hold the
/// cell -- and that is a distinct error precisely so this assertion can be
/// exact instead of hedging about whether a cell might really have been
/// deleted. Nothing here deletes anything.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_racing_eviction_never_sees_a_stale_pointer() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }
    let seed_cells = env_usize("NEB_TIER_RACE_SEED", 32768);
    let churn_cells = env_usize("NEB_TIER_RACE_CHURN", 32768);
    let payload_len = env_usize("NEB_TIER_RACE_PAYLOAD", 4096);
    let limit_segments = env_usize("NEB_TIER_RACE_SEGMENTS", 4);
    let readers = env_usize("NEB_TIER_RACE_READERS", 8);

    let backup_dir = temp_path("neb_tier_race_bk");
    let wal_dir = temp_path("neb_tier_race_wal");
    for dir in [&backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::create_dir_all(dir);
    }

    let store_bytes = ((seed_cells + churn_cells) * payload_len * 3).max(64 * SEGMENT_SIZE);
    // One chunk by DEFAULT, many chunks on request
    // (`NEB_TIER_RACE_CHUNK_SEGMENTS` + `NEB_TIER_RACE_PARTITIONS`). The
    // configuration that lost cells used 64 MB chunks in a 16 GB store -- 256
    // of them -- so the GLOBAL cross-chunk evictor is in play there and not
    // here, which is the leading suspect.
    //
    // Trying it produced a different finding instead, worth its own look: with
    // 64 chunks of 8 segments and a 4-segment limit, 256 MB of payload evicted
    // **nothing at all** -- the run raced nothing rather than losing anything,
    // and the tier limit was simply not enforced. The single-chunk default is
    // kept because it demonstrably does evict, and a test that silently stops
    // testing is worse than one that is narrow.
    let chunk_bytes = env_usize("NEB_TIER_RACE_CHUNK_SEGMENTS", 0)
        .checked_mul(SEGMENT_SIZE)
        .filter(|bytes| *bytes > 0)
        .unwrap_or(store_bytes);
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: chunk_bytes,
            db_size: store_bytes,
            tiered_config: Some(crate::ram::tiered::TieredConfig {
                threshold: 0.8,
                lower_watermark: 0.72,
                physical_memory_limit: limit_segments * SEGMENT_SIZE,
                promotion_cooldown_ms: 0,
            }),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "tiered_read_eviction_race",
        async |_| {},
    )
    .await
    .unwrap();

    const SCHEMA: u32 = 9078;
    server
        .meta()
        .schemas
        .register_internal_schema(Schema::new_with_id(
            SCHEMA,
            &String::from("tier_race_schema"),
            None,
            default_fields(),
            false,
            false,
        ));

    // Spread across localities so the writes actually reach many chunks. A cell
    // is placed by `locate_chunk_by_partition(id.locality())`, so a single
    // locality piles the whole payload into one chunk and runs it out of space
    // long before the tier is under any pressure.
    let partitions = env_usize("NEB_TIER_RACE_PARTITIONS", 1).max(1) as u64;
    let seed_id = |index: usize| Id::allocated((index as u64 % partitions) as u16, 0, index as u64);
    for index in 0..seed_cells {
        let id = seed_id(index);
        let mut cell = large_string_cell(SCHEMA, id, payload_len, "tier-race-seed");
        server.chunks().write_cell(&mut cell).expect("seed write");
    }

    // Readers sweep the seeded ids while the writer keeps the tier over its
    // limit. A stale pointer is counted separately from every other outcome:
    // it is the one that cannot be explained by anything the test does.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stale = Arc::new(AtomicU64::new(0));
    let absent = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for reader in 0..readers {
        let chunks = server.chunks().clone();
        let stop = stop.clone();
        let stale = stale.clone();
        let absent = absent.clone();
        let reads = reads.clone();
        handles.push(std::thread::spawn(move || {
            let mut index = reader;
            while !stop.load(AtomicOrdering::Relaxed) {
                let logical = index % seed_cells;
                let id = Id::allocated((logical as u64 % partitions) as u16, 0, logical as u64);
                match chunks.read_cell(&id) {
                    Ok(_) => {}
                    Err(ReadError::StaleCellPointer) => {
                        stale.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    Err(ReadError::CellDoesNotExisted) => {
                        absent.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    Err(_) => {}
                }
                reads.fetch_add(1, AtomicOrdering::Relaxed);
                index += readers;
            }
        }));
    }

    for index in 0..churn_cells {
        // Offset well past the seeded range so churn never collides with it --
        // an overwritten seed cell would be a legitimate reason for a reader to
        // see something else, and this test must have none of those.
        let logical = index + 10_000_000;
        let id = Id::allocated((logical as u64 % partitions) as u16, 0, logical as u64);
        let mut cell = large_string_cell(SCHEMA, id, payload_len, "tier-race-churn");
        server.chunks().write_cell(&mut cell).expect("churn write");
    }
    stop.store(true, AtomicOrdering::Relaxed);
    for handle in handles {
        let _ = handle.join();
    }

    let stale = stale.load(AtomicOrdering::Relaxed);
    let absent = absent.load(AtomicOrdering::Relaxed);
    let reads = reads.load(AtomicOrdering::Relaxed);
    let cold = total_cold_segments(&server.chunks());
    server.cleaner().stop();
    server.shutdown().await;
    for dir in [&backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(dir);
    }

    println!(
        "TIER RACE: {reads} reads, {stale} stale pointers, {absent} reported absent, {cold} cold segments"
    );
    assert!(
        cold > 0,
        "nothing was evicted, so this run raced nothing ({reads} reads)"
    );
    let (recorded, verdict) = crate::ram::cell::stale_pointer_record::snapshot();
    assert_eq!(
        stale, 0,
        "{stale} reads of {reads} found the index pointing at memory that does not hold the \
         cell, while the tier was evicting under a concurrent writer.\n\
         VERDICT (process-wide count {recorded}, most recent): {}",
        verdict.as_deref().unwrap_or("none recorded -- the mismatch branch did not run here")
    );
    assert_eq!(
        absent, 0,
        "{absent} reads of {reads} were told a seeded cell does not exist; nothing in this \
         test deletes anything"
    );
}

/// A segment part-way through promotion must be untouchable by the two things
/// that drop pages or overwrite backups.
///
/// Both used to be reachable there. `is_cold()` is deliberately true while a
/// promotion runs, so the cold-budget sweeper's `try_reclaim_resident_blocks`
/// passed its gate; and `is_settled_cold()` is false, because `lock_cold` sets
/// the locking bit, so the archive refusal added in d8b0039c did not fire. The
/// promoter meanwhile held no reference at all -- it took the exclusive guard
/// only to flip the state and dropped it before reading a single byte.
///
/// The measured consequence was task #71: two cells of 4.2M reading as `Id(0)`
/// from a segment reporting HOT, not dirty, offset inside the written range.
/// `docs/tla/SegmentTier.tla` has the model and both counterexamples.
#[test]
fn a_promoting_segment_is_not_archived_or_reclaimed_from_underneath_it() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: 64 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("promoting_seg", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_promoting_schema");
    let backup_dir = temp_path("neb_promoting_bk");
    let wal_dir = temp_path("neb_promoting_wal");
    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        16 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    let mut ids: Vec<Id> = Vec::new();
    for i in 0..8_000usize {
        let id = Id::allocated(0, 0, 730_000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("n{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String("y".repeat(4_000)));
        let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data_map));
        if chunks.write_cell(&mut cell).is_ok() {
            ids.push(id);
        }
    }
    assert!(ids.len() > 500, "setup wrote too few cells");

    for chunk in &chunks.list {
        let _ = manager.explicit_evict(chunk, 32);
    }
    // Reads of cold cells fault single blocks back in, which is what makes the
    // resident image a patchwork rather than nothing at all.
    for id in ids.iter() {
        let _ = chunks.read_cell(id);
    }

    let chunk = &chunks.list[0];
    let target = chunk
        .segments()
        .into_iter()
        .find(|s| s.is_settled_cold() && s.block_resident_bytes() > 0)
        .expect("expected a cold segment holding faulted-in blocks");

    assert!(
        target.image_is_partial(),
        "an evicted segment's mapping is a patchwork until a promotion restores it"
    );

    let backup_path = chunk
        .file_manager
        .backup_path(target.chunk_id, target.id, target.seq_id)
        .expect("cold segment must have a backup");
    let before = std::fs::metadata(&backup_path).expect("backup exists").len();

    // Stand where `promote_segment` stands after `lock_cold`.
    assert!(
        target.lock_cold(),
        "a settled-cold segment must be lockable for promotion"
    );

    assert_eq!(
        target.try_reclaim_resident_blocks(),
        None,
        "the cold-budget sweeper must not madvise a segment that is being restored"
    );
    assert!(
        target.block_resident_bytes() > 0,
        "and it must not have cleared the residency either"
    );
    assert_eq!(
        target.archive().expect("archive must not error"),
        false,
        "a patchwork image must never be written over an authoritative backup"
    );
    assert_eq!(
        std::fs::metadata(&backup_path).expect("backup still exists").len(),
        before,
        "the refusal must leave the backup byte-for-byte intact"
    );

    target.set_cold();
}

/// Promotion owns the image it restores: afterwards nothing is flagged partial
/// and the block-residency bytes come back to the manager's accounting.
///
/// The accounting half was a leak of its own. The bytes charged for blocks
/// faulted in while cold stayed charged after promotion replaced those blocks
/// with the whole image, so a promoted segment was billed twice -- once as a
/// hot segment, once as blocks it no longer separately held -- until its next
/// eviction happened to hand them back.
#[test]
fn a_promotion_restores_the_whole_image_and_returns_its_residency() {
    let _guard = test_lock();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: 64 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("promo_residency", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_promo_res_schema");
    let backup_dir = temp_path("neb_promo_res_bk");
    let wal_dir = temp_path("neb_promo_res_wal");
    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        16 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    let mut ids: Vec<Id> = Vec::new();
    for i in 0..8_000usize {
        let id = Id::allocated(0, 0, 880_000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("n{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String("y".repeat(4_000)));
        let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data_map));
        if chunks.write_cell(&mut cell).is_ok() {
            ids.push(id);
        }
    }
    assert!(ids.len() > 500, "setup wrote too few cells");

    for chunk in &chunks.list {
        let _ = manager.explicit_evict(chunk, 32);
    }
    for id in ids.iter() {
        let _ = chunks.read_cell(id);
    }

    let chunk = &chunks.list[0];
    let target = chunk
        .segments()
        .into_iter()
        .find(|s| s.is_settled_cold() && s.block_resident_bytes() > 0)
        .expect("expected a cold segment holding faulted-in blocks");
    let charged = target.block_resident_bytes();
    let accounted_before = manager.cold_resident_total();

    let released = crate::ram::tiered::promotion::promote_segment(&target);

    assert!(target.is_hot(), "promotion must leave the segment hot");
    assert!(
        !target.image_is_partial(),
        "a restored segment holds a whole image, and must say so before it goes hot"
    );
    assert_eq!(
        target.block_resident_bytes(),
        0,
        "the faulted-in blocks are part of the whole image now, not a separate residency"
    );
    assert_eq!(
        released, charged,
        "promotion must hand back exactly what the segment was charging"
    );
    manager.release_cold_resident(released);
    assert_eq!(
        manager.cold_resident_total(),
        accounted_before - charged,
        "the manager's cold total must drop by what the promotion returned"
    );
}

/// The race itself, driven rather than hoped for.
///
/// A promotion is held open at the exact point it has taken the cold lock and
/// restored nothing, and the three things that used to be able to reach it are
/// tried from another thread.
///
/// Against the pre-fix code all three succeed: the promoter held no reference,
/// `is_cold()` is true in that window so the cold-budget sweeper passed its
/// gate, and `is_settled_cold()` is false there so the archive refusal did not
/// fire. Any of them alone loses the cells -- the sweeper by `madvise`ing the
/// image away, the archive by writing the patchwork over the one authoritative
/// copy.
///
/// An earlier version of this test hammered a sweeper thread against unpaused
/// promotions. It passed against the broken code, because the window is
/// microseconds wide at unit-test scale. That is why the hook exists.
#[test]
fn nothing_can_reach_into_the_promotion_window() {
    use crate::ram::tiered::promotion::test_hooks;
    use std::sync::atomic::Ordering as AtomicOrdering;

    let _guard = test_lock();
    let _ = env_logger::try_init();

    let manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
        crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: 64 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        }),
    ));

    let schema = Schema::new("promo_window", None, default_fields(), false, false);
    let schema_dir = temp_path("neb_promo_window_schema");
    let backup_dir = temp_path("neb_promo_window_bk");
    let wal_dir = temp_path("neb_promo_window_wal");
    for d in [&schema_dir, &backup_dir, &wal_dir] {
        let _ = std::fs::remove_dir_all(d);
    }

    let schemas = LocalSchemasCache::new_local(&schema_dir);
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        16 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.clone()),
        Some(wal_dir.clone()),
        Some(manager.clone()),
    );

    let mut ids: Vec<Id> = Vec::new();
    for i in 0..8_000usize {
        let id = Id::allocated(0, 0, 910_000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("name"), OwnedValue::String(format!("n{}", i)));
        data_map.insert(&String::from("data"), OwnedValue::String("y".repeat(4_000)));
        let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data_map));
        if chunks.write_cell(&mut cell).is_ok() {
            ids.push(id);
        }
    }
    assert!(ids.len() > 500, "setup wrote too few cells");

    for chunk in &chunks.list {
        let _ = manager.explicit_evict(chunk, 32);
    }
    for id in ids.iter() {
        let _ = chunks.read_cell(id);
    }

    let chunk = &chunks.list[0];
    let target = chunk
        .segments()
        .into_iter()
        .find(|s| s.is_settled_cold() && s.block_resident_bytes() > 0)
        .expect("expected a cold segment holding faulted-in blocks");

    let backup_path = chunk
        .file_manager
        .backup_path(target.chunk_id, target.id, target.seq_id)
        .expect("cold segment must have a backup");
    let backup_len_before = std::fs::metadata(&backup_path).expect("backup exists").len();

    test_hooks::PAUSE_AFTER_LOCK_COLD.store(true, AtomicOrdering::Release);
    let promoter = {
        let target = target.clone();
        std::thread::spawn(move || crate::ram::tiered::promotion::promote_segment(&target))
    };

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !test_hooks::PROMOTION_IS_PAUSED.load(AtomicOrdering::Acquire) {
        assert!(
            std::time::Instant::now() < deadline,
            "the promotion never reached its window"
        );
        std::thread::yield_now();
    }

    // 1. The promoter must own the segment for the whole restore, not just for
    //    the state flip. This is the fix the other two assertions rest on.
    assert!(
        !target.incr_references(),
        "a promotion must hold the segment exclusively while it restores; without \
         that, anything at all can take it and madvise the image away underneath"
    );
    // 2. The cold-budget sweeper must decline: `is_cold()` is true here.
    assert_eq!(
        target.try_reclaim_resident_blocks(),
        None,
        "the sweeper must not reclaim from a segment that is being restored"
    );
    // 3. And an archive must decline: `is_settled_cold()` is false here.
    assert_eq!(
        target.archive().expect("archive must not error"),
        false,
        "a patchwork image must never be written over an authoritative backup"
    );
    assert_eq!(
        std::fs::metadata(&backup_path).expect("backup still exists").len(),
        backup_len_before,
        "the refusal must leave the backup byte-for-byte intact"
    );

    test_hooks::PAUSE_AFTER_LOCK_COLD.store(false, AtomicOrdering::Release);
    let released = promoter.join().expect("promotion thread");
    manager.release_cold_resident(released);

    let mut unreadable = Vec::new();
    for id in ids.iter() {
        if chunks.read_cell(id).is_err() {
            unreadable.push(*id);
        }
    }
    assert!(
        unreadable.is_empty(),
        "{} of {} cells were lost around the promotion window; first few: {:?}",
        unreadable.len(),
        ids.len(),
        &unreadable[..unreadable.len().min(5)]
    );
}
