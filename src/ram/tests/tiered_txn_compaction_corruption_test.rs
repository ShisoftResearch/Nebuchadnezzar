/// Tiered memory transactional stress test to reproduce corruption
///
/// This test creates high load with:
/// - Many concurrent transactional updates
/// - Heavy read traffic during compaction
/// - Transaction rollbacks to stress undo log  
/// - Small physical memory (128MB) vs large virtual (1GB) to force eviction/promotion
/// - Should reproduce "SchemaDoesNotExisted" errors with garbage schema IDs if corruption exists

use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::cleaner::Cleaner;
use crate::ram::schema::{Field, Schema};
use crate::ram::segs::SEGMENT_SIZE;
use crate::ram::types::*;
use crate::server::transactions::{self, TxnExecResult};
use crate::server::{NebServer, ServerOptions, Service};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn create_test_schema() -> Schema {
    let fields = Field::new_schema(vec![
        Field::new_unindexed("id", Type::I32),
        Field::new_unindexed("value", Type::I32),
        Field::new_unindexed_array("data", Type::U8),
    ]);
    Schema::new_with_id(1, "txn_corruption_test", None, fields, false, false)
}

fn create_cell_data(value: i32) -> OwnedValue {
    let data_bytes = vec![0xAAu8; 1000]; // 1KB of data
    data_map_value!(
        id: value,
        value: value,
        data: OwnedValue::PrimArray(OwnedPrimArray::U8(data_bytes))
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_txn_compaction_with_concurrent_reads_and_updates() {
    let _ = env_logger::try_init();

    info!("=== Starting TRANSACTIONAL compaction corruption stress test ===");

    // Setup server with transactions and tiered memory enabled
    // Small physical memory (64MB) with large virtual (1GB) forces constant eviction/promotion
    let server_addr = String::from("127.0.0.1:5555");
    
    // Create temporary directories for backup storage (required for tiered memory)
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let backup_storage_path = temp_dir.path().join("backup");
    let wal_storage_path = temp_dir.path().join("wal");
    std::fs::create_dir_all(&backup_storage_path).expect("Failed to create backup dir");
    std::fs::create_dir_all(&wal_storage_path).expect("Failed to create WAL dir");
    
    #[cfg(feature = "tiered_memory")]
    let tiered_config = Some(crate::ram::tiered::TieredConfig {
        threshold: 0.8,  // Start evicting at 80% of physical limit
        physical_memory_limit: 128 * 1024 * 1024, // 128MB physical for 1GB virtual
    });
    #[cfg(not(feature = "tiered_memory"))]
    let tiered_config = None;
    
    let server = Arc::new(
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 4,
                total_size: SEGMENT_SIZE * 128, // 1GB total virtual memory
                tiered_config,
                backup_storage: Some(backup_storage_path.to_str().unwrap().to_string()),
                wal_storage: Some(wal_storage_path.to_str().unwrap().to_string()),
                index_enabled: false,
                services: vec![Service::Cell, Service::Transaction],
                enable_recovery: false,
                undo_log_storage: None,
                raft_storage: None,
            },
            &server_addr,
            "test",
            async |_| {},
        )
        .await,
    );

    let schema = create_test_schema();
    server.meta.schemas.new_schema(schema.clone());
    let schema_id = schema.id;

    #[cfg(feature = "tiered_memory")]
    info!("Server started with {} chunks (1GB virtual, 128MB physical - TIERED MEMORY ENABLED)", 
          server.chunks.list.len());
    #[cfg(not(feature = "tiered_memory"))]
    info!("Server started with {} chunks (tiered memory disabled)", server.chunks.list.len());
    
    info!("Backup storage: {:?}", backup_storage_path);
    info!("WAL storage: {:?}", wal_storage_path);

    // Shared state
    let stop_flag = Arc::new(AtomicBool::new(false));
    let error_count = Arc::new(AtomicUsize::new(0));
    let corruption_count = Arc::new(AtomicUsize::new(0));
    let txn_count = Arc::new(AtomicUsize::new(0));
    let committed_count = Arc::new(AtomicUsize::new(0));
    let aborted_count = Arc::new(AtomicUsize::new(0));

    // Phase 1: Fill with initial data (outside transactions)
    info!("Phase 1: Filling with initial data");
    for i in 0..1000 {
        let mut cell = OwnedCell::new_with_id(
            schema_id,
            &Id::new(0, i),
            create_cell_data(i as i32),
        );
        if let Err(e) = server.chunks.write_cell(&mut cell) {
            error!("Failed to write initial cell {}: {:?}", i, e);
        }
    }

    let total_cells = server.chunks.count();
    let total_segments: usize = server.chunks.list.iter().map(|c| c.seg_count()).sum();
    info!("Initial state: {} cells across {} segments", total_cells, total_segments);

    // Phase 2: Start aggressive cleaner
    let chunks_for_cleaner = Arc::clone(&server.chunks);
    let stop_flag_for_cleaner = Arc::clone(&stop_flag);
    let cleaner_handle = thread::spawn(move || {
        let mut iterations = 0;
        while !stop_flag_for_cleaner.load(Ordering::Relaxed) {
            iterations += 1;
            for chunk in &chunks_for_cleaner.list {
                Cleaner::clean(chunk, false);
            }
            thread::sleep(Duration::from_millis(5));
            
            if iterations % 20 == 0 {
                debug!("Cleaner completed {} iterations", iterations);
            }
        }
        info!("Cleaner thread completed {} iterations", iterations);
    });

    // Phase 3: Start transactional writer threads
    let num_txn_threads = 8;
    let mut txn_handles = vec![];

    for thread_id in 0..num_txn_threads {
        let server_addr_clone = server_addr.clone();
        let stop_flag_clone = Arc::clone(&stop_flag);
        let txn_count_clone = Arc::clone(&txn_count);
        let committed_count_clone = Arc::clone(&committed_count);
        let aborted_count_clone = Arc::clone(&aborted_count);
        let error_count_clone = Arc::clone(&error_count);

        let handle = tokio::spawn(async move {
            let txn_client = transactions::new_async_client(&server_addr_clone)
                .await
                .expect("Failed to create transaction client");

            let mut local_txns = 0usize;
            let mut local_commits = 0usize;
            let mut local_aborts = 0usize;
            let mut local_errors = 0usize;

            while !stop_flag_clone.load(Ordering::Relaxed) {
                // Start a new transaction
                let txn_id = match txn_client.begin().await {
                    Ok(Ok(id)) => id,
                    Ok(Err(e)) => {
                        error!("TxnWriter {} failed to begin: {:?}", thread_id, e);
                        local_errors += 1;
                        continue;
                    }
                    Err(e) => {
                        error!("TxnWriter {} RPC error on begin: {:?}", thread_id, e);
                        local_errors += 1;
                        continue;
                    }
                };

                local_txns += 1;

                // Do 5-10 updates in this transaction
                let num_ops = 5 + (local_txns % 5);
                let mut had_error = false;

                for i in 0..num_ops {
                    let cell_id_base = (thread_id * 100 + (local_txns % 100)) as u64;
                    let cell_id = cell_id_base + (i as u64);
                    
                    let cell = OwnedCell::new_with_id(
                        schema_id,
                        &Id::new(0, cell_id),
                        create_cell_data((local_txns % 1000) as i32),
                    );

                    // Use update (creates if doesn't exist in this system)
                    match txn_client.update(txn_id.clone(), cell).await {
                        Ok(Ok(TxnExecResult::Accepted(()))) => {}
                        Ok(Ok(other)) => {
                            warn!("TxnWriter {} update unexpected result: {:?}", thread_id, other);
                        }
                        Ok(Err(e)) => {
                            warn!("TxnWriter {} update error: {:?}", thread_id, e);
                            had_error = true;
                            break;
                        }
                        Err(e) => {
                            error!("TxnWriter {} RPC error on update: {:?}", thread_id, e);
                            had_error = true;
                            break;
                        }
                    }
                }

                // Decide whether to commit or abort (10% abort rate)
                let should_abort = (local_txns % 10) == 0;

                if had_error || should_abort {
                    // Abort the transaction
                    match txn_client.abort(txn_id).await {
                        Ok(Ok(_)) => {
                            local_aborts += 1;
                        }
                        Ok(Err(e)) => {
                            warn!("TxnWriter {} abort error: {:?}", thread_id, e);
                            local_errors += 1;
                        }
                        Err(e) => {
                            error!("TxnWriter {} RPC error on abort: {:?}", thread_id, e);
                            local_errors += 1;
                        }
                    }
                } else {
                    // Prepare and commit
                    match txn_client.prepare(txn_id.clone()).await {
                        Ok(Ok(_)) => {},
                        Ok(Err(e)) => {
                            warn!("TxnWriter {} prepare error: {:?}", thread_id, e);
                            local_errors += 1;
                            continue;
                        }
                        Err(e) => {
                            error!("TxnWriter {} RPC error on prepare: {:?}", thread_id, e);
                            local_errors += 1;
                            continue;
                        }
                    }

                    match txn_client.commit(txn_id).await {
                        Ok(Ok(_)) => {
                            local_commits += 1;
                        }
                        Ok(Err(e)) => {
                            warn!("TxnWriter {} commit error: {:?}", thread_id, e);
                            local_errors += 1;
                        }
                        Err(e) => {
                            error!("TxnWriter {} RPC error on commit: {:?}", thread_id, e);
                            local_errors += 1;
                        }
                    }
                }

                // Small yield to allow other threads
                if local_txns % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            txn_count_clone.fetch_add(local_txns, Ordering::Relaxed);
            committed_count_clone.fetch_add(local_commits, Ordering::Relaxed);
            aborted_count_clone.fetch_add(local_aborts, Ordering::Relaxed);
            error_count_clone.fetch_add(local_errors, Ordering::Relaxed);

            info!("TxnWriter {} completed: {} txns, {} commits, {} aborts, {} errors",
                  thread_id, local_txns, local_commits, local_aborts, local_errors);
        });

        txn_handles.push(handle);
    }

    // Phase 4: Start reader threads that check for corruption
    let num_reader_threads = 16;
    let mut reader_handles = vec![];

    for thread_id in 0..num_reader_threads {
        let chunks_clone = Arc::clone(&server.chunks);
        let stop_flag_clone = Arc::clone(&stop_flag);
        let corruption_count_clone = Arc::clone(&corruption_count);
        let error_count_clone = Arc::clone(&error_count);

        let handle = tokio::spawn(async move {
            let mut local_reads = 0usize;
            let mut local_errors = 0usize;
            let mut local_corruptions = 0usize;

            while !stop_flag_clone.load(Ordering::Relaxed) {
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
                        // Cell was deleted or doesn't exist - this is fine
                        local_reads += 1;
                    }
                    Err(e) => {
                        warn!("Reader {} read error for cell {}: {:?}", thread_id, cell_id, e);
                        local_errors += 1;
                    }
                }

                // Yield occasionally
                if local_reads % 20 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            corruption_count_clone.fetch_add(local_corruptions, Ordering::Relaxed);
            error_count_clone.fetch_add(local_errors, Ordering::Relaxed);

            info!("Reader {} completed: {} reads, {} errors, {} corruptions",
                  thread_id, local_reads, local_errors, local_corruptions);
        });

        reader_handles.push(handle);
    }

    // Phase 5: Let it run for a while
    info!("Running transactional stress test for 15 seconds...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Phase 6: Stop all threads
    info!("Stopping all threads...");
    stop_flag.store(true, Ordering::Relaxed);

    // Wait for all threads
    for handle in txn_handles {
        handle.await.expect("Transaction thread panicked");
    }
    for handle in reader_handles {
        handle.await.expect("Reader thread panicked");
    }
    cleaner_handle.join().expect("Cleaner thread panicked");

    // Results
    let total_txns = txn_count.load(Ordering::Relaxed);
    let total_commits = committed_count.load(Ordering::Relaxed);
    let total_aborts = aborted_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);
    let total_corruptions = corruption_count.load(Ordering::Relaxed);

    info!("=== Test Results ===");
    info!("Total transactions: {}", total_txns);
    info!("Total commits: {}", total_commits);
    info!("Total aborts: {}", total_aborts);
    info!("Total errors: {}", total_errors);
    info!("Total corruptions: {}", total_corruptions);
    info!("Final cell count: {}", server.chunks.count());

    let final_segments: usize = server.chunks.list.iter().map(|c| c.seg_count()).sum();
    info!("Final segments: {}", final_segments);

    // Assert no corruptions
    assert_eq!(
        total_corruptions, 0,
        "Found {} schema corruption errors during concurrent transactional compaction and reads!",
        total_corruptions
    );

    info!("✓ No corruptions detected - transactional test passed!");
}

