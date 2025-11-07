/// Tests specifically designed to detect data corruption issues during transactions
/// Focuses on the panic: "Cannot decode entry header" in mark_dead_entry_with_cell
use super::*;
use crate::ram::cell::*;
use crate::ram::schema::*;
use crate::ram::tests::default_fields;
use crate::ram::types::*;
use crate::server::transactions;
use crate::server::*;
use env_logger;
use tokio::time::{sleep, Duration};

/// Test rapid concurrent updates to the same cell
/// This can trigger race conditions in cell location tracking
#[allow(unused_variables)]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_rapid_concurrent_updates_same_cell() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5300");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
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
    .await;

    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    // Create initial cell
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(1));
    data_map.insert(&String::from("score"), OwnedValue::U64(0));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Test")),
    );
    let mut cell =
        OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map.clone()));
    server.chunks.write_cell(&mut cell).unwrap();
    let cell_id = cell.id();

    println!("Starting rapid concurrent updates on cell: {:?}", cell_id);

    // Launch many concurrent transactions updating the same cell
    let txn_count = 100;
    let mut tasks = Vec::with_capacity(txn_count);

    for i in 0..txn_count {
        let txn_client = transactions::new_async_client(&server_addr).await.unwrap();
        let cid = cell_id.clone();
        let sid = schema.id;

        tasks.push(tokio::spawn(async move {
            let txn_id = txn_client.begin().await.unwrap().unwrap();

            // Read the cell
            match txn_client.read(txn_id.clone(), cid.clone()).await {
                Ok(Ok(TxnExecResult::Accepted(mut cell))) => {
                    // Update score
                    let mut data = cell.data.Map().unwrap().clone();
                    data.insert(&String::from("score"), OwnedValue::U64(i as u64));
                    cell.data = OwnedValue::Map(data);

                    // Update the cell
                    if let Ok(Ok(TxnExecResult::Accepted(_))) =
                        txn_client.update(txn_id.clone(), cell).await
                    {
                        // Try to prepare and commit
                        if let Ok(Ok(TMPrepareResult::Success)) =
                            txn_client.prepare(txn_id.clone()).await
                        {
                            match txn_client.commit(txn_id.clone()).await {
                                Ok(Ok(EndResult::Success)) => {
                                    println!("Transaction {} committed successfully", i);
                                }
                                Ok(Ok(_)) => {
                                    // Other results (CheckFailed, SomeLocksNotReleased)
                                }
                                Ok(Err(e)) => {
                                    println!("Transaction {} commit failed: {:?}", i, e);
                                }
                                Err(e) => {
                                    println!("Transaction {} commit error: {:?}", i, e);
                                }
                            }
                        }
                    }
                }
                _ => {
                    // Failed to read, that's ok in concurrent scenario
                }
            }
        }));
    }

    // Wait for all transactions to complete
    for task in tasks {
        let _ = task.await;
    }

    println!("All transactions completed");
}

/// Test high-frequency updates with varying data sizes
/// Can trigger segment allocation issues and corruption
#[allow(unused_variables)]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_varying_size_concurrent_updates() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5301");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
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
    .await;

    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    // Create initial cell
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(1));
    data_map.insert(&String::from("score"), OwnedValue::U64(0));
    data_map.insert(&String::from("name"), OwnedValue::String(String::from("X")));
    let mut cell =
        OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map.clone()));
    server.chunks.write_cell(&mut cell).unwrap();
    let cell_id = cell.id();

    println!("Starting varying-size updates on cell: {:?}", cell_id);

    let txn_count = 50;
    let mut tasks = Vec::with_capacity(txn_count);

    for i in 0..txn_count {
        let txn_client = transactions::new_async_client(&server_addr).await.unwrap();
        let cid = cell_id.clone();
        let sid = schema.id;

        tasks.push(tokio::spawn(async move {
            let txn_id = txn_client.begin().await.unwrap().unwrap();

            match txn_client.read(txn_id.clone(), cid.clone()).await {
                Ok(Ok(TxnExecResult::Accepted(mut cell))) => {
                    let mut data = cell.data.Map().unwrap().clone();

                    // Vary the size dramatically - from small to large strings
                    let string_size = if i % 3 == 0 {
                        100 // Small
                    } else if i % 3 == 1 {
                        10000 // Medium
                    } else {
                        100000 // Large
                    };

                    let large_string = "X".repeat(string_size);
                    data.insert(&String::from("name"), OwnedValue::String(large_string));
                    data.insert(&String::from("score"), OwnedValue::U64(i as u64));
                    cell.data = OwnedValue::Map(data);

                    if let Ok(Ok(TxnExecResult::Accepted(_))) =
                        txn_client.update(txn_id.clone(), cell).await
                    {
                        if let Ok(Ok(TMPrepareResult::Success)) =
                            txn_client.prepare(txn_id.clone()).await
                        {
                            let _ = txn_client.commit(txn_id.clone()).await;
                            println!("Transaction {} (size: {}) committed", i, string_size);
                        }
                    }
                }
                _ => {}
            }
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    println!("All varying-size updates completed");
}

/// Test multiple cells being updated concurrently within transactions
/// Tests cell index integrity under concurrent modifications
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_multi_cell_concurrent_transactions() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5302");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: 128 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
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
    .await;

    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    // Create multiple cells
    let cell_count = 20;
    let mut cell_ids = Vec::new();

    for i in 0..cell_count {
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("score"), OwnedValue::U64(0));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(format!("Cell{}", i)),
        );
        let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));
        server.chunks.write_cell(&mut cell).unwrap();
        cell_ids.push(cell.id());
    }

    println!("Starting multi-cell concurrent transactions");

    let txn_count = 100;
    let mut tasks = Vec::with_capacity(txn_count);

    for i in 0..txn_count {
        let txn_client = transactions::new_async_client(&server_addr).await.unwrap();
        let cells = cell_ids.clone();

        tasks.push(tokio::spawn(async move {
            let txn_id = txn_client.begin().await.unwrap().unwrap();
            let mut updated = 0;

            // Try to update multiple cells in this transaction
            for (idx, cid) in cells.iter().enumerate().take(5) {
                match txn_client.read(txn_id.clone(), cid.clone()).await {
                    Ok(Ok(TxnExecResult::Accepted(mut cell))) => {
                        let mut data = cell.data.Map().unwrap().clone();
                        data.insert(
                            &String::from("score"),
                            OwnedValue::U64((i as u64) + (idx as u64)),
                        );
                        cell.data = OwnedValue::Map(data);

                        if let Ok(Ok(TxnExecResult::Accepted(_))) =
                            txn_client.update(txn_id.clone(), cell).await
                        {
                            updated += 1;
                        }
                    }
                    _ => {}
                }
            }

            if updated > 0 {
                if let Ok(Ok(TMPrepareResult::Success)) = txn_client.prepare(txn_id.clone()).await {
                    let _ = txn_client.commit(txn_id.clone()).await;
                    println!("Transaction {} committed with {} updates", i, updated);
                }
            }
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    println!("All multi-cell transactions completed");
}

/// Test rapid commit sequence - commits happening in quick succession
/// Can expose race conditions in mark_dead_entry_with_cell
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rapid_commit_sequence() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5303");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
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
    .await;

    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    // Create initial cell
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(1));
    data_map.insert(&String::from("score"), OwnedValue::U64(0));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Test")),
    );
    let mut cell =
        OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map.clone()));
    server.chunks.write_cell(&mut cell).unwrap();
    let cell_id = cell.id();

    println!("Starting rapid commit sequence test on cell: {:?}", cell_id);

    // Execute transactions sequentially but with minimal delays
    for i in 0..50 {
        let txn_client = transactions::new_async_client(&server_addr).await.unwrap();
        let txn_id = txn_client.begin().await.unwrap().unwrap();

        if let Ok(Ok(TxnExecResult::Accepted(mut cell))) =
            txn_client.read(txn_id.clone(), cell_id.clone()).await
        {
            let mut data = cell.data.Map().unwrap().clone();
            data.insert(&String::from("score"), OwnedValue::U64(i as u64));
            cell.data = OwnedValue::Map(data);

            if let Ok(Ok(TxnExecResult::Accepted(_))) =
                txn_client.update(txn_id.clone(), cell).await
            {
                if let Ok(Ok(TMPrepareResult::Success)) = txn_client.prepare(txn_id.clone()).await {
                    match txn_client.commit(txn_id.clone()).await {
                        Ok(Ok(EndResult::Success)) => {
                            println!("Rapid commit {} succeeded", i);
                        }
                        e => {
                            println!("Rapid commit {} failed: {:?}", i, e);
                        }
                    }
                }
            }
        }

        // Minimal delay between transactions
        sleep(Duration::from_micros(100)).await;
    }

    println!("Rapid commit sequence completed");
}

/// Test interleaved prepare and commit operations
/// Can expose timing issues in transaction state management
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn test_interleaved_prepare_commit() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5304");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
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
    .await;

    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    // Create multiple cells
    let cell_count = 10;
    let mut cell_ids = Vec::new();

    for i in 0..cell_count {
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("score"), OwnedValue::U64(0));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(format!("Cell{}", i)),
        );
        let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));
        server.chunks.write_cell(&mut cell).unwrap();
        cell_ids.push(cell.id());
    }

    println!("Starting interleaved prepare/commit test");

    let txn_count = 50;
    let mut tasks = Vec::with_capacity(txn_count);

    for i in 0..txn_count {
        let txn_client = transactions::new_async_client(&server_addr).await.unwrap();
        let cid = cell_ids[i % cell_count].clone();

        tasks.push(tokio::spawn(async move {
            let txn_id = txn_client.begin().await.unwrap().unwrap();

            if let Ok(Ok(TxnExecResult::Accepted(mut cell))) =
                txn_client.read(txn_id.clone(), cid.clone()).await
            {
                let mut data = cell.data.Map().unwrap().clone();
                data.insert(&String::from("score"), OwnedValue::U64(i as u64));
                cell.data = OwnedValue::Map(data);

                if let Ok(Ok(TxnExecResult::Accepted(_))) =
                    txn_client.update(txn_id.clone(), cell).await
                {
                    // Prepare
                    if let Ok(Ok(TMPrepareResult::Success)) =
                        txn_client.prepare(txn_id.clone()).await
                    {
                        // Small delay before commit
                        sleep(Duration::from_micros((i % 1000) as u64)).await;

                        // Commit
                        match txn_client.commit(txn_id.clone()).await {
                            Ok(Ok(EndResult::Success)) => {
                                println!("Interleaved txn {} committed", i);
                            }
                            e => {
                                println!("Interleaved txn {} failed: {:?}", i, e);
                            }
                        }
                    }
                }
            }
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    println!("Interleaved prepare/commit test completed");
}

/// Stress test with maximum concurrency on a single cell
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_maximum_concurrency_stress() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5305");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: 128 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
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
    .await;

    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    // Create initial cell
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(1));
    data_map.insert(&String::from("score"), OwnedValue::U64(0));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("StressTest")),
    );
    let mut cell =
        OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map.clone()));
    server.chunks.write_cell(&mut cell).unwrap();
    let cell_id = cell.id();

    println!(
        "Starting maximum concurrency stress test on cell: {:?}",
        cell_id
    );

    let txn_count = 500;
    let mut tasks = Vec::with_capacity(txn_count);

    for i in 0..txn_count {
        let txn_client = transactions::new_async_client(&server_addr).await.unwrap();
        let cid = cell_id.clone();

        tasks.push(tokio::spawn(async move {
            let txn_id = match txn_client.begin().await {
                Ok(Ok(tid)) => tid,
                _ => return,
            };

            match txn_client.read(txn_id.clone(), cid.clone()).await {
                Ok(Ok(TxnExecResult::Accepted(mut cell))) => {
                    let mut data = cell.data.Map().unwrap().clone();

                    // Add varying data
                    data.insert(&String::from("score"), OwnedValue::U64(i as u64));
                    data.insert(&String::from("iteration"), OwnedValue::I64(i as i64));
                    cell.data = OwnedValue::Map(data);

                    if let Ok(Ok(TxnExecResult::Accepted(_))) =
                        txn_client.update(txn_id.clone(), cell).await
                    {
                        if let Ok(Ok(TMPrepareResult::Success)) =
                            txn_client.prepare(txn_id.clone()).await
                        {
                            match txn_client.commit(txn_id.clone()).await {
                                Ok(Ok(EndResult::Success)) => {
                                    if i % 50 == 0 {
                                        println!("Stress test: {} transactions completed", i);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            }
        }));
    }

    let mut completed = 0;
    for task in tasks {
        if let Ok(_) = task.await {
            completed += 1;
        }
    }

    println!(
        "Maximum concurrency stress test completed: {} tasks finished",
        completed
    );
}

/// Test that mimics the exact wikidata import scenario
/// Based on the actual panic stack trace from wikidata import
#[tokio::test(flavor = "multi_thread", worker_threads = 64)]
async fn test_wikidata_import_scenario() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5306");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 4,                // Multiple chunks like production
            total_size: 512 * 1024 * 1024, // Larger memory pool
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: true, // Enable indexing like production
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            undo_log_storage: None,
            raft_storage: None,
        },
        &server_addr,
        "test",
        async |_| {},
    )
    .await;

    let schema = Schema::new_with_id(
        1,
        &String::from("wikidata"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    println!("Starting wikidata import scenario test");
    println!("This test mimics high-concurrency batch imports");

    // Simulate batched imports with 64 workers (matching wikidata import)
    let batch_count = 20;
    let items_per_batch = 50;

    for batch in 0..batch_count {
        println!("Processing batch {}/{}", batch + 1, batch_count);

        let mut batch_tasks = Vec::new();

        // Each batch processes multiple items concurrently
        for item in 0..items_per_batch {
            let txn_client = transactions::new_async_client(&server_addr).await.unwrap();
            let sid = schema.id;

            batch_tasks.push(tokio::spawn(async move {
                let txn_id = match txn_client.begin().await {
                    Ok(Ok(tid)) => tid,
                    _ => return false,
                };

                // Create multiple related cells (like statements in wikidata)
                let mut success = true;
                let base_id = Id::rand();

                for stmt in 0..5 {
                    // 5 statements per item
                    let mut data_map = OwnedMap::new();
                    data_map.insert(
                        &String::from("id"),
                        OwnedValue::I64(((batch * items_per_batch) as i64) + (item as i64)),
                    );
                    data_map.insert(&String::from("score"), OwnedValue::U64(stmt as u64));
                    data_map.insert(
                        &String::from("name"),
                        OwnedValue::String(format!("Item_{}_{}", item, stmt)),
                    );

                    let cell = OwnedCell::new_with_id(sid, &base_id, OwnedValue::Map(data_map));

                    // Use update (upsert pattern) like wikidata import
                    match txn_client.update(txn_id.clone(), cell).await {
                        Ok(Ok(TxnExecResult::Accepted(_))) => {}
                        _ => {
                            success = false;
                            break;
                        }
                    }
                }

                if success {
                    if let Ok(Ok(TMPrepareResult::Success)) =
                        txn_client.prepare(txn_id.clone()).await
                    {
                        match txn_client.commit(txn_id.clone()).await {
                            Ok(Ok(EndResult::Success)) => return true,
                            _ => return false,
                        }
                    }
                }

                false
            }));
        }

        // Wait for batch to complete
        let mut batch_success = 0;
        for task in batch_tasks {
            if let Ok(true) = task.await {
                batch_success += 1;
            }
        }

        println!(
            "Batch {} completed: {}/{} successful",
            batch + 1,
            batch_success,
            items_per_batch
        );

        // Small delay between batches
        sleep(Duration::from_millis(10)).await;
    }

    println!("Wikidata import scenario test completed");
}

/// Test update_cell_by specifically - this is where the panic occurs
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_update_cell_by_stress() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5307");
    // Use unique group name to avoid conflicts with other tests
    let group_name = "test_update_cell_by_stress";
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            undo_log_storage: None,
            raft_storage: None,
        },
        &server_addr,
        group_name,
        async |_| {},
    )
    .await;

    // Wait for Raft to stabilize before starting stress test
    // This prevents overwhelming the Raft heartbeat mechanism
    tokio::time::sleep(Duration::from_millis(500)).await;

    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    // Create initial cell
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(1));
    data_map.insert(&String::from("score"), OwnedValue::U64(0));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Test")),
    );
    let mut cell =
        OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map.clone()));
    server.chunks.write_cell(&mut cell).unwrap();
    let cell_id = cell.id();

    println!("Testing update_cell_by path that triggers mark_dead_entry_with_cell");

    // Reduced concurrency to avoid overwhelming Raft
    // Still high enough to stress test the corruption path
    let update_count = 100;
    let mut tasks = Vec::new();

    // Batch spawn tasks with small delays to avoid overwhelming Raft
    for i in 0..update_count {
        // Small delay between spawning tasks to reduce Raft load
        if i > 0 && i % 20 == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let txn_client = transactions::new_async_client(&server_addr).await.unwrap();
        let cid = cell_id.clone();

        tasks.push(tokio::spawn(async move {
            let txn_id = match txn_client.begin().await {
                Ok(Ok(tid)) => tid,
                _ => return,
            };

            // Read current cell
            if let Ok(Ok(TxnExecResult::Accepted(mut cell))) =
                txn_client.read(txn_id.clone(), cid.clone()).await
            {
                // Modify it
                let mut data = cell.data.Map().unwrap().clone();
                data.insert(&String::from("score"), OwnedValue::U64(i as u64));
                data.insert(&String::from("iteration"), OwnedValue::I64(i as i64));
                cell.data = OwnedValue::Map(data);

                // Update (this calls update_cell_by which calls mark_dead_entry_with_cell)
                if let Ok(Ok(TxnExecResult::Accepted(_))) =
                    txn_client.update(txn_id.clone(), cell).await
                {
                    if let Ok(Ok(TMPrepareResult::Success)) =
                        txn_client.prepare(txn_id.clone()).await
                    {
                        // The panic occurs during commit
                        let _ = txn_client.commit(txn_id.clone()).await;
                    }
                }
            }
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    println!("update_cell_by stress test completed");
}
