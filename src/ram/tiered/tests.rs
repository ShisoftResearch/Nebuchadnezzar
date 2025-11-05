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
use crate::ram::tiered::page_fault_tracker;
use crate::server::{NebServer, Service, ServerOptions};
use crate::server::transactions;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

// Global mutex to prevent test interference
static TEST_MUTEX: Mutex<()> = Mutex::new(());

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

/// Test automatic eviction when physical memory limit is exceeded
#[test]
fn test_eviction_on_memory_overflow() {
    let _guard = TEST_MUTEX.lock().unwrap();
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
        crate::ram::tiered::TieredConfig::from_env(),
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
    let _guard = TEST_MUTEX.lock().unwrap();
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
        crate::ram::tiered::TieredConfig::from_env(),
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

/// Large-scale end-to-end test: 64MB physical limit, 1GB virtual, 512MB data
/// with batched transactional inserts followed by random transactional updates.
/// Tests natural eviction/promotion with serializability guarantees.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_large_scale_transactions_with_natural_tiered_memory() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();
    
    info!("=== Starting Large-Scale Tiered Memory Transaction Test ===");
    info!("Config: 64MB physical, 1GB virtual, 512MB data target");
    
    // Install page fault handlers
    page_fault_tracker::install_fault_handlers();
    
    // Configure: 64MB physical limit, 1GB virtual capacity
    let physical_limit = 64 * 1024 * 1024; // 64MB = 8 segments
    let virtual_capacity = 1024 * 1024 * 1024; // 1GB = 128 segments
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.75"); // 75% threshold
    std::env::set_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT", &format!("{}", physical_limit));
    
    // Start server
    let server_addr = String::from("127.0.0.1:5400");
    let backup_dir = "/tmp/neb_large_scale_bk";
    let wal_dir = "/tmp/neb_large_scale_wal";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: virtual_capacity,
            tiered_config: crate::ram::tiered::TieredConfig::from_env(),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
        },
        &server_addr,
        "large_scale_test",
        async |_| {},
    )
    .await;
    
    // Create schema
    let fields = fields_with_score();
    let schema = Schema::new("large_scale_schema", None, fields, false, false);
    let schema = Schema::new_with_id(9999, &schema.name, schema.str_key_field.clone(), 
                                     schema.fields.clone(), schema.is_dynamic, schema.is_scannable);
    server.meta.schemas.new_schema(schema.clone());
    
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
    let client = transactions::new_async_client(&server_addr).await.unwrap();
    
    let mut all_ids = Vec::with_capacity(num_cells);
    let insert_start = std::time::Instant::now();
    
    for batch_idx in 0..num_batches {
        let tx = client.begin().await.unwrap().unwrap();
        let start_idx = batch_idx * batch_size;
        let end_idx = ((batch_idx + 1) * batch_size).min(num_cells);
        
        for i in start_idx..end_idx {
            let id = Id::new(schema.id as u64, i as u64);
            let mut m = OwnedMap::new();
            m.insert(&String::from("id"), OwnedValue::I64(i as i64));
            m.insert(&String::from("name"), OwnedValue::String(format!("cell_{}", i)));
            m.insert(&String::from("data"), OwnedValue::String(large_blob.clone()));
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
            Ok(Ok(transactions::TMPrepareResult::Success)) => {
                match client.commit(tx).await {
                    Ok(Ok(transactions::EndResult::Success)) => {
                        if (batch_idx + 1) % 10 == 0 {
                            info!("Inserted batch {}/{} ({} cells total)", 
                                  batch_idx + 1, num_batches, all_ids.len());
                        }
                    }
                    other => {
                        error!("Commit failed for batch {}: {:?}", batch_idx, other);
                        break;
                    }
                }
            }
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
    info!("Phase 1 complete: Inserted {} cells in {:.2}s ({:.2} MB/s)", 
          all_ids.len(), 
          insert_duration.as_secs_f64(),
          (all_ids.len() * cell_size) as f64 / insert_duration.as_secs_f64() / (1024.0 * 1024.0));
    
    // Check tiered memory stats
    for chunk in &server.chunks.list {
        if let Some(ref manager) = chunk.tiered_manager {
            let stats = manager.stats(chunk);
            info!("After insert - Hot: {} segments ({} MB), Cold: {} segments ({} MB), Total: {} segments",
                  stats.hot_segments,
                  stats.hot_segments * 8,
                  stats.cold_segments,
                  stats.cold_segments * 8,
                  stats.total_segments);
            
            // Verify we have cold segments (natural eviction occurred)
            assert!(stats.cold_segments > 0, 
                   "Should have cold segments after inserting 512MB with 64MB limit");
        }
    }
    
    // Phase 2: Random transactional updates with serializability
    info!("Phase 2: Random read-then-update transactions (testing serializability)");
    
    let update_workers = 12;
    let updates_per_worker = 500;
    let success_counters: Arc<Vec<AtomicU64>> = Arc::new(
        (0..all_ids.len()).map(|_| AtomicU64::new(0)).collect()
    );
    
    let update_start = std::time::Instant::now();
    let mut update_handles = Vec::new();
    
    for worker_id in 0..update_workers {
        let server_addr = server_addr.clone();
        let all_ids = all_ids.clone();
        let success_counters = success_counters.clone();
        let schema_id = schema.id;
        
        update_handles.push(tokio::spawn(async move {
            let client = transactions::new_async_client(&server_addr).await.unwrap();
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
                        m.insert(&String::from("id"), OwnedValue::I64(*cell.data["id"].i64().unwrap()));
                        m.insert(&String::from("name"), cell.data["name"].clone());
                        m.insert(&String::from("data"), cell.data["data"].clone());
                        m.insert(&String::from("score"), OwnedValue::U64(curr_score + 1));
                        
                        let updated_cell = OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(m));
                        
                        match client.update(tx.clone(), updated_cell).await {
                            Ok(Ok(transactions::TxnExecResult::Accepted(_))) => {
                                // Try to commit
                                match client.prepare(tx.clone()).await {
                                    Ok(Ok(transactions::TMPrepareResult::Success)) => {
                                        match client.commit(tx).await {
                                            Ok(Ok(transactions::EndResult::Success)) => {
                                                success_counters[key_idx].fetch_add(1, AtomicOrdering::Relaxed);
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
    info!("Phase 2 complete: {} successful updates, {} conflicts in {:.2}s ({:.2} TPS)",
          total_success,
          total_conflict,
          update_duration.as_secs_f64(),
          total_success as f64 / update_duration.as_secs_f64());
    
    // Phase 3: Verify serializability - final scores must match successful commits
    info!("Phase 3: Verifying serializability");
    
    let verification_start = std::time::Instant::now();
    let mut mismatches = 0;
    let sample_size = all_ids.len().min(1000); // Verify a sample
    
    for i in (0..all_ids.len()).step_by(all_ids.len() / sample_size) {
        let id = all_ids[i];
        let expected = success_counters[i].load(AtomicOrdering::Relaxed);
        
        match server.chunks.read_cell(&id) {
            Ok(cell) => {
                let actual = *cell.data["score"].u64().unwrap();
                if actual != expected {
                    error!("Serializability violation at key {}: expected score {}, got {}", 
                           i, expected, actual);
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
    info!("Phase 3 complete: Verified {} cells in {:.2}s, {} mismatches",
          sample_size, verification_duration.as_secs_f64(), mismatches);
    
    assert_eq!(mismatches, 0, "Serializability check failed: {} mismatches found", mismatches);
    
    // Final tiered memory stats
    for chunk in &server.chunks.list {
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
    
    page_fault_tracker::install_fault_handlers();
    
    // Medium configuration: 32MB physical, 256MB virtual
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.7");
    std::env::set_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT", &format!("{}", 32 * 1024 * 1024));
    
    let server_addr = String::from("127.0.0.1:5401");
    let backup_dir = "/tmp/neb_stress_test_bk";
    let wal_dir = "/tmp/neb_stress_test_wal";
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: 256 * 1024 * 1024,
            tiered_config: crate::ram::tiered::TieredConfig::from_env(),
            backup_storage: Some(backup_dir.to_string()),
            wal_storage: Some(wal_dir.to_string()),
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
        },
        &server_addr,
        "stress_test",
        async |_| {},
    )
    .await;
    
    let fields = fields_with_score();
    let schema = Schema::new("stress_schema", None, fields, false, false);
    let schema = Schema::new_with_id(8888, &schema.name, schema.str_key_field.clone(),
                                     schema.fields.clone(), schema.is_dynamic, schema.is_scannable);
    server.meta.schemas.new_schema(schema.clone());
    
    // Initialize 10000 cells
    info!("Initializing 10000 cells");
    let client = transactions::new_async_client(&server_addr).await.unwrap();
    let num_keys = 10000;
    let mut ids = Vec::with_capacity(num_keys);
    
    let batch_size = 500;
    for batch in 0..(num_keys / batch_size) {
        let tx = client.begin().await.unwrap().unwrap();
        for i in (batch * batch_size)..((batch + 1) * batch_size) {
            let id = Id::new(schema.id as u64, i as u64);
            let mut m = OwnedMap::new();
            m.insert(&String::from("id"), OwnedValue::I64(i as i64));
            m.insert(&String::from("name"), OwnedValue::String(format!("stress_{}", i)));
            m.insert(&String::from("data"), OwnedValue::String("D".repeat(4096)));
            m.insert(&String::from("score"), OwnedValue::U64(0));
            let cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(m));
            let _ = client.write(tx.clone(), cell).await.unwrap().unwrap();
            ids.push(id);
        }
        let _ = client.prepare(tx.clone()).await.unwrap().unwrap();
        let _ = client.commit(tx).await.unwrap().unwrap();
    }
    
    info!("Initialization complete, starting mixed workload");
    
    // Mixed workload: readers and writers
    let readers = 6;
    let writers = 6;
    let duration_secs = 20;
    
    let success_counters: Arc<Vec<AtomicU64>> = Arc::new(
        (0..num_keys).map(|_| AtomicU64::new(0)).collect()
    );
    
    let start_time = std::time::Instant::now();
    let mut handles = Vec::new();
    
    // Reader threads
    for reader_id in 0..readers {
        let server_addr = server_addr.clone();
        let ids = ids.clone();
        
        handles.push(tokio::spawn(async move {
            let client = transactions::new_async_client(&server_addr).await.unwrap();
            let mut reads = 0u64;
            let mut counter: u64 = reader_id as u64 * 777;
            
            while start_time.elapsed().as_secs() < duration_secs {
                counter = counter.wrapping_mul(1103515245).wrapping_add(12345);
                let key_idx = (counter as usize) % ids.len();
                let id = ids[key_idx];
                
                let tx = client.begin().await.unwrap().unwrap();
                if let Ok(Ok(transactions::TxnExecResult::Accepted(_))) = client.read(tx.clone(), id).await {
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
            let client = transactions::new_async_client(&server_addr).await.unwrap();
            let mut writes = 0u64;
            let mut counter: u64 = writer_id as u64 * 999;
            
            while start_time.elapsed().as_secs() < duration_secs {
                counter = counter.wrapping_mul(1103515245).wrapping_add(12345);
                let key_idx = (counter as usize) % ids.len();
                let id = ids[key_idx];
                
                let tx = client.begin().await.unwrap().unwrap();
                
                if let Ok(Ok(transactions::TxnExecResult::Accepted(cell))) = client.read(tx.clone(), id).await {
                    let curr_score = *cell.data["score"].u64().unwrap();
                    let mut m = OwnedMap::new();
                    m.insert(&String::from("id"), OwnedValue::I64(*cell.data["id"].i64().unwrap()));
                    m.insert(&String::from("name"), cell.data["name"].clone());
                    m.insert(&String::from("data"), cell.data["data"].clone());
                    m.insert(&String::from("score"), OwnedValue::U64(curr_score + 1));
                    
                    let updated = OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(m));
                    
                    if let Ok(Ok(transactions::TxnExecResult::Accepted(_))) = client.update(tx.clone(), updated).await {
                        if let Ok(Ok(transactions::TMPrepareResult::Success)) = client.prepare(tx.clone()).await {
                            if let Ok(Ok(transactions::EndResult::Success)) = client.commit(tx).await {
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
        if let Ok(cell) = server.chunks.read_cell(&id) {
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
