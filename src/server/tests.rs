use crate::ram::schema::Field;
use crate::ram::schema::Schema;
use crate::ram::types::*;
use crate::server::*;
use crate::{client, ram::cell::OwnedCell};
use dovahkiin::types::custom_types::id::Id;
use futures::stream::FuturesUnordered;
use rand::prelude::*;
use std::env;
use std::sync::Arc;
use test::Bencher;
use tokio_stream::StreamExt;

#[bench]
fn cell_construct(b: &mut Bencher) {
    b.iter(|| {
        let id = Id::new(0, 1);
        let mut value = OwnedValue::Map(OwnedMap::new());
        value["DATA"] = OwnedValue::U64(2);
        OwnedCell::new_with_id(1, &id, value);
    })
}

#[bench]
fn cell_clone(b: &mut Bencher) {
    let id = Id::new(0, 1);
    let mut value = OwnedValue::Map(OwnedMap::new());
    value["DATA"] = OwnedValue::U64(2);
    let cell = OwnedCell::new_with_id(1, &id, value);
    b.iter(|| {
        let _ = cell.clone();
    })
}

#[tokio::test]
pub async fn init() {
    let _ = env_logger::try_init();
    NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: 64 * 1024 * 1024, // 64 MB - must be >= SEGMENT_SIZE (8 MB)
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
            index_enabled: false,
            services: vec![],
            enable_recovery: false,
        },
        &String::from("127.0.0.1:5100"),
        &String::from("test"),
        async |_| {},
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn smoke_test() {
    let _ = env_logger::try_init();
    const DATA: &'static str = "DATA";
    let num = env::var("NEB_KV_SMOKE_TEST_ITEMS")
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .unwrap();
    let server_addr = String::from("127.0.0.1:5500");
    let server_group = String::from("smoke_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: 512 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
        },
        &server_addr,
        &server_group,
        async |_| {},
    )
    .await;
    let schema_id = 123;
    let schema = Schema::new_with_id(
        schema_id,
        &String::from("schema"),
        None,
        Field::new_schema(vec![Field::new_unindexed(DATA, Type::U64)]),
        false,
        false,
    );

    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            &server_group,
        )
        .await
        .unwrap(),
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();

    for i in 0..num {
        // intense upsert, half delete
        let id = Id::new(1, i / 2);
        let mut value = OwnedValue::Map(OwnedMap::new());
        value[DATA] = OwnedValue::U64(i);
        let cell = OwnedCell::new_with_id(schema_id, &id, value);
        client.upsert_cell(cell).await.unwrap().unwrap();

        // read
        let read_cell = client.read_cell(id).await.unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].u64().unwrap()), i);

        if i % 2 == 0 {
            client.remove_cell(id).await.unwrap().unwrap();
        }
    }

    for i in 0..num {
        let id = Id::new(1, i);
        let mut value = OwnedValue::Map(OwnedMap::new());
        value[DATA] = OwnedValue::U64(i * 2);
        let cell = OwnedCell::new_with_id(schema_id, &id, value);
        client.upsert_cell(cell).await.unwrap().unwrap();

        // verify
        let read_cell = client.read_cell(id).await.unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].u64().unwrap()), i * 2);
    }
}

#[tokio::test(flavor = "multi_thread")]
pub async fn smoke_test_parallel() {
    let _ = env_logger::try_init();
    const DATA: &'static str = "DATA";
    const ARRAY: &'static str = "ARRAY";
    let num = env::var("NEB_KV_SMOKE_TEST_ITEMS")
        .unwrap_or("256".to_string())
        .parse::<u64>()
        .unwrap();
    let server_addr = String::from("127.0.0.1:5301");
    let server_group = String::from("smoke_parallel_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 4,
            total_size: 16 * 1024 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
        },
        &server_addr,
        &server_group,
        async |_| {},
    )
    .await;
    let schema_id = 123;
    let schema = Schema::new_with_id(
        schema_id,
        "schema",
        None,
        Field::new_schema(vec![
            Field::new_unindexed(DATA, Type::U64),
            Field::new_unindexed_array(ARRAY, Type::U64),
        ]),
        false,
        false,
    );

    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            &server_group,
        )
        .await
        .unwrap(),
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();

    let num_tasks = 1024;
    let mut tasks: FuturesUnordered<_> = FuturesUnordered::new();

    info!("Schduling test cases");

    for i in 0..num_tasks {
        let client_clone = client.clone();
        info!("Schduling test task {}", i);
        tasks.push(tokio::spawn(async move {
            let id = Id::new(1, i as u64);
            let mut rng = SmallRng::from_rng(&mut rand::rng());
            for j in 0..num {
                debug!("Smoke test i {}, j {}", i, j);
                if j > 1 && rng.gen_range(0..8) == 4 {
                    debug!("Removing i {}, j {}", i, j);
                    client_clone.remove_cell(id).await.unwrap().unwrap();
                }
                let mut value = OwnedValue::Map(OwnedMap::new());
                value[DATA] = OwnedValue::U64(j);
                value[ARRAY] = (1..rng.gen_range(1..1024)).collect::<Vec<u64>>().value();
                let cell = OwnedCell::new_with_id(schema_id, &id, value);
                debug!("Upsert {:?}, i {}, j {}", id, i, j);
                client_clone.upsert_cell(cell).await.unwrap().unwrap();
                // read
                let read_cell = client_clone
                    .read_cell(id)
                    .await
                    .unwrap()
                    .expect(&format!("Finally expecting {:?} at {}", id, j));
                assert_eq!(
                    *(read_cell.data[DATA].u64().unwrap()),
                    j,
                    "Parallel final read i {}, j {}",
                    i,
                    j
                );
                debug!("Iteration i {}, j {} completed", i, j);
            }
            true
        }));
    }
    info!("Waiting for all tasks to finish");
    while let Some(r) = tasks.next().await {
        assert!(r.unwrap());
    }
}

#[tokio::test(flavor = "multi_thread")]
pub async fn txn() {
    let _ = env_logger::try_init();
    const DATA: &'static str = "DATA";
    let num = env::var("NEB_KV_TXN_TEST_ITEMS")
        .unwrap_or("2000".to_string())
        .parse::<u64>()
        .unwrap();
    let server_addr = String::from("127.0.0.1:5303");
    let server_group = String::from("bench_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: 512 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
        },
        &server_addr,
        &server_group,
        async |_| {},
    )
    .await;
    let schema_id = 123;
    let schema = Schema::new_with_id(
        schema_id,
        &String::from("schema"),
        None,
        Field::new_schema(vec![Field::new_unindexed(DATA, Type::U64)]),
        false,
        false,
    );

    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            &server_group,
        )
        .await
        .unwrap(),
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();

    for _ in 0..num {
        client
            .transaction(|txn| async move {
                let id = Id::new(0, 1);
                let mut value = OwnedValue::Map(OwnedMap::new());
                value[DATA] = OwnedValue::U64(2);
                let cell = OwnedCell::new_with_id(schema_id, &id, value);
                txn.upsert(cell).await
            })
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
pub async fn schema_wal_recovery_test() {
    let _ = env_logger::try_init();

    // Test WAL-only recovery (no snapshot)
    let temp_dir = tempfile::TempDir::new().unwrap();
    let raft_path = temp_dir.path().join("raft_wal_only");
    let raft_path_str = raft_path.to_str().unwrap().to_string();

    info!("Using raft storage path: {}", raft_path_str);
    info!("Testing WAL log replay recovery (NO snapshot)");

    let server_addr = String::from("127.0.0.1:18900");
    let server_group = String::from("wal_test");

    // === PHASE 1: Create server and add schemas (< 1000 to avoid snapshot) ===
    info!("PHASE 1: Creating server and adding 3 schemas (below snapshot threshold)");
    {
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()),
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await;

        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.clone()],
                &server_group,
            )
            .await
            .unwrap(),
        );

        let schema1 = Schema::new_with_id(
            100,
            "users",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("name", Type::String),
                Field::new_unindexed("age", Type::U32),
            ]),
            false,
            false,
        );

        let schema2 = Schema::new_with_id(
            200,
            "products",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("title", Type::String),
                Field::new_unindexed("price", Type::F64),
            ]),
            false,
            false,
        );

        let schema3 = Schema::new_with_id(
            300,
            "orders",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("customer_id", Type::U64),
                Field::new_unindexed("total", Type::F64),
            ]),
            false,
            false,
        );

        client
            .new_schema_with_id(schema1.clone())
            .await
            .unwrap()
            .unwrap();
        client
            .new_schema_with_id(schema2.clone())
            .await
            .unwrap()
            .unwrap();
        client
            .new_schema_with_id(schema3.clone())
            .await
            .unwrap()
            .unwrap();

        info!("Created 3 schemas, waiting for WAL commit...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        // Verify schemas exist before shutdown
        let retrieved = client
            .schema_by_name(&"users".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.id, 100);
        info!("✓ Schemas verified before shutdown");

        // Check files - should have WAL but NO snapshot
        let log_path = temp_dir.path().join("raft_wal_only").join("log.dat");
        let snapshot_path = temp_dir.path().join("raft_wal_only").join("snapshot.dat");
        info!("Log file exists: {}", log_path.exists());
        info!("Snapshot file exists: {}", snapshot_path.exists());
        info!("This test expects WAL recovery, not snapshot recovery");

        // Shutdown
        info!("Shutting down server...");
        drop(client);
        server.raft_service.shutdown().await;
        server.rpc.shutdown().await;
        drop(server);

        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    }

    // === PHASE 2: Restart and verify WAL replay ===
    info!("PHASE 2: Restarting server - should replay WAL logs to rebuild schemas");
    {
        let server2 = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()),
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await;

        let client2 = Arc::new(
            client::AsyncClient::new(
                &server2.rpc,
                &server2.membership,
                &vec![server_addr.clone()],
                &server_group,
            )
            .await
            .unwrap(),
        );

        info!("Waiting for WAL replay to complete...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        info!("Verifying schemas recovered from WAL...");
        let all_schemas = client2.get_all_schema().await.unwrap();
        info!("Found {} schemas after WAL replay", all_schemas.len());

        if all_schemas.len() == 0 {
            error!("WAL log replay FAILED - no schemas recovered");
            error!("This means bifrost does NOT replay WAL logs to state machines on recovery");
            panic!("WAL recovery test failed: expected 3 schemas, found 0");
        }

        // Verify the specific schemas
        let s1 = client2
            .schema_by_name(&"users".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(s1.id, 100);
        info!("✓ Schema 'users' recovered from WAL");

        let s2 = client2
            .schema_by_name(&"products".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(s2.id, 200);
        info!("✓ Schema 'products' recovered from WAL");

        let s3 = client2
            .schema_by_name(&"orders".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(s3.id, 300);
        info!("✓ Schema 'orders' recovered from WAL");

        info!("✓ WAL log replay recovery works!");
    }
}

#[tokio::test(flavor = "multi_thread")]
pub async fn schema_snapshot_recovery_test() {
    let _ = env_logger::try_init();

    // Create a temporary directory for raft storage
    let temp_dir = tempfile::TempDir::new().unwrap();
    let raft_path = temp_dir.path().join("raft_storage");
    let raft_path_str = raft_path.to_str().unwrap().to_string();

    info!("Using raft storage path: {}", raft_path_str);

    // Use unique port to avoid conflicts with other tests
    let server_addr = String::from("127.0.0.1:18800");
    let server_group = String::from("persistence_test");

    // === PHASE 1: Create server and add schemas ===
    info!("PHASE 1: Creating initial server and adding schemas");
    {
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()), // Enable persistence for this test
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await;

        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.clone()],
                &server_group,
            )
            .await
            .unwrap(),
        );

        // Create multiple schemas to test
        info!("Creating test schemas...");

        let schema1 = Schema::new_with_id(
            100,
            "users",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("name", Type::String),
                Field::new_unindexed("age", Type::U32),
            ]),
            false,
            false,
        );

        let schema2 = Schema::new_with_id(
            200,
            "products",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("title", Type::String),
                Field::new_unindexed("price", Type::F64),
            ]),
            false,
            false,
        );

        let schema3 = Schema::new_with_id(
            300,
            "orders",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("customer_id", Type::U64),
                Field::new_unindexed("total", Type::F64),
            ]),
            false,
            false,
        );

        client
            .new_schema_with_id(schema1.clone())
            .await
            .unwrap()
            .unwrap();
        client
            .new_schema_with_id(schema2.clone())
            .await
            .unwrap()
            .unwrap();
        client
            .new_schema_with_id(schema3.clone())
            .await
            .unwrap()
            .unwrap();

        info!("Created 3 schemas, waiting for them to be committed...");

        // Give Raft time to commit and potentially snapshot
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        // Verify schemas exist before shutdown
        let retrieved_schema1 = client
            .schema_by_name(&"users".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved_schema1.id, 100);
        assert_eq!(retrieved_schema1.name, "users");

        let retrieved_schema2 = client
            .schema_by_name(&"products".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved_schema2.id, 200);

        let retrieved_schema3 = client
            .schema_by_name(&"orders".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved_schema3.id, 300);

        info!("All schemas verified before shutdown");

        // Check if snapshot/log files were created
        let raft_log_path = temp_dir.path().join("raft_storage").join("log.dat");
        let raft_snapshot_path = temp_dir.path().join("raft_storage").join("snapshot.dat");
        info!("Log file exists: {}", raft_log_path.exists());
        info!("Snapshot file exists: {}", raft_snapshot_path.exists());

        // Gracefully shutdown the server
        info!("Shutting down server...");
        drop(client);
        server.raft_service.shutdown().await;
        server.rpc.shutdown().await;
        drop(server);

        info!("Server shut down, waiting before restart...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    }

    // === PHASE 2: Restart server and verify recovery ===
    info!("PHASE 2: Restarting server and verifying schema recovery");
    {
        let server2 = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()), // Resume from persisted state
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await;

        let client2 = Arc::new(
            client::AsyncClient::new(
                &server2.rpc,
                &server2.membership,
                &vec![server_addr.clone()],
                &server_group,
            )
            .await
            .unwrap(),
        );

        // Wait for recovery to complete - needs time for Raft to load snapshot and sync
        info!("Waiting for Raft recovery to complete...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        info!("Verifying recovered schemas...");

        // Check if any schemas exist
        let all_schemas = client2.get_all_schema().await.unwrap();
        info!("Found {} schemas after recovery", all_schemas.len());
        for s in &all_schemas {
            info!("  - Schema: id={}, name={}", s.id, s.name);
        }

        // Verify all schemas were recovered
        let recovered_schema1 = client2
            .schema_by_name(&"users".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered_schema1.id, 100);
        assert_eq!(recovered_schema1.name, "users");
        info!("✓ Schema 'users' recovered successfully");

        let recovered_schema2 = client2
            .schema_by_name(&"products".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered_schema2.id, 200);
        assert_eq!(recovered_schema2.name, "products");
        info!("✓ Schema 'products' recovered successfully");

        let recovered_schema3 = client2
            .schema_by_name(&"orders".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered_schema3.id, 300);
        assert_eq!(recovered_schema3.name, "orders");
        info!("✓ Schema 'orders' recovered successfully");

        // Test that new schemas get IDs that don't conflict with recovered ones
        info!("Creating new schema after recovery to test ID allocation...");
        let schema4 = Schema::new(
            "inventory",
            None,
            Field::new_schema(vec![Field::new_unindexed("stock", Type::U64)]),
            false,
            false,
        );

        let new_id = client2.new_schema(schema4).await.unwrap().unwrap();
        assert!(
            new_id > 300,
            "New schema should have ID > 300, got {}",
            new_id
        );
        info!("✓ New schema created with ID {} (correctly > 300)", new_id);

        info!("All persistence tests passed!");
    }

    // Cleanup happens automatically when temp_dir is dropped
}

#[tokio::test(flavor = "multi_thread")]
pub async fn schema_persistence_multiple_restarts() {
    let _ = env_logger::try_init();

    let temp_dir = tempfile::TempDir::new().unwrap();
    let raft_path = temp_dir.path().join("raft_storage_multi");
    let raft_path_str = raft_path.to_str().unwrap().to_string();

    let server_group = String::from("multi_restart_test");

    for restart_num in 1..=3 {
        info!("=== RESTART CYCLE {} ===", restart_num);

        // Use unique port for each cycle to avoid "Address already in use" errors
        let server_addr = format!("127.0.0.1:{}", 19000 + restart_num);

        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()), // Enable persistence for this test
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await;

        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.clone()],
                &server_group,
            )
            .await
            .unwrap(),
        );

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Verify schemas from previous cycles still exist
        for i in 1..restart_num {
            let schema_name = format!("schema_cycle_{}", i);
            let schema = client.schema_by_name(&schema_name).await.unwrap();
            assert!(schema.is_some(), "Schema from cycle {} should exist", i);
            info!("✓ Verified schema from cycle {}", i);
        }

        // Add a new schema for this cycle
        let new_schema = Schema::new(
            &format!("schema_cycle_{}", restart_num),
            None,
            Field::new_schema(vec![Field::new_unindexed("data", Type::U64)]),
            false,
            false,
        );

        client.new_schema(new_schema).await.unwrap().unwrap();
        info!("✓ Created schema for cycle {}", restart_num);

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Properly shutdown server
        info!("Shutting down cycle {}...", restart_num);
        drop(client);
        server.raft_service.shutdown().await;
        server.rpc.shutdown().await;
        drop(server);

        // Wait for cleanup (using unique ports so this can be shorter)
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    // Final verification: restart one more time and check all schemas exist
    info!("=== FINAL VERIFICATION ===");
    {
        let server_addr = String::from("127.0.0.1:19004"); // Unique port for final verification
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()), // Enable persistence for this test
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await;

        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.clone()],
                &server_group,
            )
            .await
            .unwrap(),
        );

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // All 3 schemas should exist
        for i in 1..=3 {
            let schema_name = format!("schema_cycle_{}", i);
            let schema = client.schema_by_name(&schema_name).await.unwrap();
            assert!(
                schema.is_some(),
                "Schema from cycle {} should exist after all restarts",
                i
            );
            info!("✓ Final verification: schema_cycle_{} exists", i);
        }

        info!("All multiple restart tests passed!");
    }
}

#[tokio::test]
pub async fn memory_status_test() {
    let _ = env_logger::try_init();

    // Create server with tiered memory enabled
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 4,
            total_size: 128 * 1024 * 1024, // 128 MB
            tiered_config: Some(crate::ram::tiered::TieredConfig::with_memory_limit(
                64 * 1024 * 1024, // 64 MB physical limit
            )),
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            services: vec![],
            index_enabled: false,
            enable_recovery: false,
        },
        "127.0.0.1:5400",
        "memory_status_test",
        async |_| {},
    )
    .await;

    // Get memory status
    let status = server.memory_status();

    // Verify basic statistics
    assert_eq!(status.total_chunks, 4, "Should have 4 chunks");
    assert_eq!(
        status.chunk_details.len(),
        4,
        "Should have details for 4 chunks"
    );
    assert!(
        status.total_segments > 0,
        "Should have at least one segment (bootstrap)"
    );
    assert!(
        status.tiered_memory_enabled,
        "Tiered memory should be enabled"
    );
    assert_eq!(
        status.physical_memory_limit_bytes,
        Some(64 * 1024 * 1024),
        "Physical limit should be 64 MB"
    );

    // Each chunk should start with at least one hot segment (bootstrap)
    for chunk_status in &status.chunk_details {
        assert!(
            chunk_status.hot_segments > 0,
            "Chunk {} should have at least one hot segment",
            chunk_status.chunk_id
        );
        assert_eq!(
            chunk_status.cold_segments, 0,
            "Chunk {} should have no cold segments initially",
            chunk_status.chunk_id
        );
    }

    // Test print_summary (just ensure it doesn't panic)
    status.print_summary();

    // Test JSON serialization
    let json = serde_json::to_string(&status).expect("Should serialize to JSON");
    assert!(
        json.contains("total_chunks"),
        "JSON should contain total_chunks"
    );
    assert!(
        json.contains("tiered_memory_enabled"),
        "JSON should contain tiered_memory_enabled"
    );

    info!("Memory status test completed successfully");
}
