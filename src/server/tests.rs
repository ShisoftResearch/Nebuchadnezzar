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
use std::time::Duration;
use test::Bencher;
use tokio_stream::StreamExt;

#[bench]
fn cell_construct(b: &mut Bencher) {
    b.iter(|| {
        let id = Id::from_parts(0, 1);
        let mut value = OwnedValue::Map(OwnedMap::new());
        value["DATA"] = OwnedValue::U64(2);
        OwnedCell::new_with_id(1, &id, value);
    })
}

#[bench]
fn cell_clone(b: &mut Bencher) {
    let id = Id::from_parts(0, 1);
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
            chunk_size: 64 * 1024 * 1024, // 64 MB - must be >= SEGMENT_SIZE (8 MB)
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
            index_enabled: false,
            services: vec![],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        &String::from("test"),
        async |_| {},
    )
    .await
    .unwrap();
}

#[tokio::test]
pub async fn explicit_database_binding_scopes_storage_roots() {
    let _ = env_logger::try_init();
    let temp_dir = tempfile::TempDir::new().unwrap();
    let backup_root = temp_dir.path().join("backup");
    let wal_root = temp_dir.path().join("wal");
    let undo_root = temp_dir.path().join("undo");
    let raft_root = temp_dir.path().join("raft");
    let database_name = "analytics/db";
    let scoped_db_dir = "analytics_db";

    let server = NebServer::new_from_opts_in_database(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: Some(backup_root.to_string_lossy().to_string()),
            wal_storage: Some(wal_root.to_string_lossy().to_string()),
            undo_log_storage: Some(undo_root.to_string_lossy().to_string()),
            raft_storage: Some(raft_root.to_string_lossy().to_string()),
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "storage_scope_group",
        database_name,
        async |_| {},
    )
    .await
    .unwrap();

    assert_eq!(server.database_name(), database_name);
    assert!(
        backup_root.join("databases").join(scoped_db_dir).exists(),
        "backup path should be scoped per database"
    );
    assert!(
        wal_root.join("databases").join(scoped_db_dir).exists(),
        "wal path should be scoped per database"
    );
    assert!(
        undo_root.join("databases").join(scoped_db_dir).exists(),
        "undo path should be scoped per database"
    );
    assert!(
        raft_root.join("databases").join(scoped_db_dir).exists(),
        "raft path should be scoped per database"
    );

    server.shutdown().await;
}

#[tokio::test]
pub async fn resolves_bound_database_runtime_by_name() {
    let _ = env_logger::try_init();
    let database_name = "analytics";

    let server = NebServer::new_from_opts_in_database(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "database_runtime_lookup_group",
        database_name,
        async |_| {},
    )
    .await
    .unwrap();

    let looked_up_runtime = server
        .database(database_name)
        .expect("database runtime should be registered under its database name");
    assert!(Arc::ptr_eq(&looked_up_runtime, &server.database_runtime));
    assert!(Arc::ptr_eq(
        &server.current_database(),
        &server.database_runtime
    ));
    assert_eq!(server.database_names(), vec![database_name.to_string()]);
    assert_eq!(looked_up_runtime.database_name(), database_name);
    assert_eq!(
        looked_up_runtime.group_name(),
        "database_runtime_lookup_group"
    );
    let _ = looked_up_runtime
        .data_client(&vec![server.rpc.address.clone()])
        .await
        .expect("database runtime should create a bound async client");
    let _ = looked_up_runtime.indexed_data_client();
    assert!(
        server.database("missing").is_none(),
        "unknown database names must not resolve to a runtime"
    );

    server.shutdown().await;
}

#[tokio::test]
pub async fn ensure_database_runtime_creates_new_database_runtime_on_live_host() {
    let _ = env_logger::try_init();

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "dynamic_database_runtime_group",
        async |_| {},
    )
    .await
    .unwrap();

    let default_runtime = server.current_database();
    let analytics_runtime = server
        .ensure_database_runtime("analytics")
        .await
        .expect("new database runtime should be created on demand");

    assert_eq!(analytics_runtime.database_name(), "analytics");
    assert_eq!(
        analytics_runtime.group_name(),
        "dynamic_database_runtime_group"
    );
    assert!(
        !Arc::ptr_eq(&default_runtime, &analytics_runtime),
        "new database runtime should be distinct from the default runtime"
    );
    assert!(Arc::ptr_eq(
        &analytics_runtime,
        &server
            .database("analytics")
            .expect("new runtime should be inserted into the runtime registry")
    ));

    let all_databases = server
        .neb_client
        .get_all_databases()
        .await
        .expect("database catalog should stay readable");
    assert!(
        all_databases.iter().any(|entry| entry.name == "analytics"),
        "new runtime creation should settle the database catalog entry"
    );

    let names = server.database_names();
    assert!(names
        .iter()
        .any(|name| name == "dynamic_database_runtime_group"));
    assert!(names.iter().any(|name| name == "analytics"));

    let analytics_client = analytics_runtime
        .data_client(&vec![server.rpc.address.clone()])
        .await
        .expect("new database runtime should create a bound client");
    assert_eq!(analytics_client.database_name(), "analytics");

    let analytics_runtime_again = server
        .ensure_database_runtime("analytics")
        .await
        .expect("database runtime creation should be idempotent");
    assert!(Arc::ptr_eq(&analytics_runtime, &analytics_runtime_again));

    server.shutdown().await;
}

#[tokio::test]
pub async fn unload_database_runtime_evicts_non_default_runtime() {
    let _ = env_logger::try_init();

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction, Service::RangedIndexer],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "runtime_unload_group",
        async |_| {},
    )
    .await
    .unwrap();

    let analytics_runtime = server
        .ensure_database_runtime("analytics")
        .await
        .expect("database runtime should be created before unload");
    assert!(server.database("analytics").is_some());

    assert!(
        server.unload_database_runtime("analytics").await,
        "unload should return true for a registered non-default runtime"
    );
    assert!(
        server.database("analytics").is_none(),
        "runtime should be removed from the registry after unload"
    );
    assert!(
        !server.unload_database_runtime("runtime_unload_group").await,
        "default runtime must not be unloaded"
    );

    let analytics_runtime_reloaded = server
        .ensure_database_runtime("analytics")
        .await
        .expect("database runtime should be recreated after unload");
    assert!(
        !Arc::ptr_eq(&analytics_runtime, &analytics_runtime_reloaded),
        "reloaded runtime should be a fresh Arc after unload"
    );

    server.shutdown().await;
}

#[tokio::test]
pub async fn delete_database_storage_removes_scoped_paths() {
    let _ = env_logger::try_init();
    let temp_dir = tempfile::TempDir::new().unwrap();
    let backup_root = temp_dir.path().join("backup");
    let wal_root = temp_dir.path().join("wal");
    let undo_root = temp_dir.path().join("undo");
    let raft_root = temp_dir.path().join("raft");

    let server = NebServer::new_from_opts_in_database(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: Some(backup_root.to_string_lossy().to_string()),
            wal_storage: Some(wal_root.to_string_lossy().to_string()),
            undo_log_storage: Some(undo_root.to_string_lossy().to_string()),
            raft_storage: Some(raft_root.to_string_lossy().to_string()),
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "runtime_delete_storage_group",
        "runtime_delete_storage_group",
        async |_| {},
    )
    .await
    .unwrap();

    server
        .ensure_database_runtime("analytics/db")
        .await
        .expect("database runtime should be created before deleting storage");
    assert!(server.unload_database_runtime("analytics/db").await);

    let scoped_db_dir = "analytics_db";
    let backup_path = backup_root.join("databases").join(scoped_db_dir);
    let wal_path = wal_root.join("databases").join(scoped_db_dir);
    let undo_path = undo_root.join("databases").join(scoped_db_dir);
    let raft_path = raft_root.join("databases").join(scoped_db_dir);

    std::fs::create_dir_all(&backup_path).unwrap();
    std::fs::create_dir_all(&wal_path).unwrap();
    std::fs::create_dir_all(&undo_path).unwrap();
    std::fs::create_dir_all(&raft_path).unwrap();

    assert!(backup_path.exists());
    assert!(wal_path.exists());
    assert!(undo_path.exists());
    assert!(raft_path.exists());

    server
        .delete_database_storage("analytics/db")
        .expect("storage delete should succeed for scoped runtime");

    assert!(!backup_path.exists());
    assert!(!wal_path.exists());
    assert!(!undo_path.exists());
    assert!(!raft_path.exists());

    server.shutdown().await;
}

#[tokio::test]
pub async fn unload_database_runtime_unchecked_allows_default() {
    let _ = env_logger::try_init();

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "runtime_unchecked_unload_group",
        async |_| {},
    )
    .await
    .unwrap();

    let default_name = server.database_name().to_string();

    // Regular unload must be blocked for the default database
    assert!(
        !server.unload_database_runtime(&default_name).await,
        "regular unload must not evict the default runtime"
    );
    assert!(
        server.database(&default_name).is_some(),
        "default runtime must still be present after blocked unload"
    );

    // Unchecked unload must succeed
    assert!(
        server
            .unload_database_runtime_unchecked(&default_name)
            .await,
        "unchecked unload must evict the default runtime"
    );
    assert!(
        server.database(&default_name).is_none(),
        "default runtime must be gone after unchecked unload"
    );

    server.shutdown().await;
}

#[tokio::test]
pub async fn delete_database_storage_unchecked_allows_default() {
    let _ = env_logger::try_init();
    let temp_dir = tempfile::TempDir::new().unwrap();
    let backup_root = temp_dir.path().join("backup");
    let wal_root = temp_dir.path().join("wal");
    let undo_root = temp_dir.path().join("undo");
    let raft_root = temp_dir.path().join("raft");

    let group = "default_storage_unchecked_group";
    let server = NebServer::new_from_opts_in_database(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: Some(backup_root.to_string_lossy().to_string()),
            wal_storage: Some(wal_root.to_string_lossy().to_string()),
            undo_log_storage: Some(undo_root.to_string_lossy().to_string()),
            raft_storage: Some(raft_root.to_string_lossy().to_string()),
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        group,
        group,
        async |_| {},
    )
    .await
    .unwrap();

    let default_name = server.database_name().to_string();

    // Regular delete must be blocked for the default database
    assert!(
        server.delete_database_storage(&default_name).is_err(),
        "regular storage delete must be blocked for the default database"
    );

    // Unload the default runtime first so storage can be wiped
    server
        .unload_database_runtime_unchecked(&default_name)
        .await;

    // Create the scoped storage directories as the runtime would have
    let scoped_db_dir = default_name.replace('/', "_");
    let backup_path = backup_root.join("databases").join(&scoped_db_dir);
    let wal_path = wal_root.join("databases").join(&scoped_db_dir);
    std::fs::create_dir_all(&backup_path).unwrap();
    std::fs::create_dir_all(&wal_path).unwrap();

    // Unchecked delete must succeed
    server
        .delete_database_storage_unchecked(&default_name)
        .expect("unchecked storage delete must succeed for the default database");

    assert!(
        !backup_path.exists(),
        "backup storage should be removed after unchecked delete"
    );
    assert!(
        !wal_path.exists(),
        "WAL storage should be removed after unchecked delete"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn smoke_test() {
    let _ = env_logger::try_init();
    const DATA: &'static str = "DATA";
    let num = env::var("NEB_KV_SMOKE_TEST_ITEMS")
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .unwrap();
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let server_group = String::from("smoke_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 512 * 1024 * 1024,
            db_size: 512 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &server_addr,
        &server_group,
        async |_| {},
    )
    .await
    .unwrap();
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
        let id = Id::from_parts(1, i / 2);
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
        let id = Id::from_parts(1, i);
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
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let server_group = String::from("smoke_parallel_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 4 * 1024 * 1024 * 1024,
            db_size: 16 * 1024 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &server_addr,
        &server_group,
        async |_| {},
    )
    .await
    .unwrap();
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
            let id = Id::from_parts(1, i as u64);
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
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let server_group = String::from("bench_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 512 * 1024 * 1024,
            db_size: 512 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &server_addr,
        &server_group,
        async |_| {},
    )
    .await
    .unwrap();
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
                let id = Id::from_parts(0, 1);
                let mut value = OwnedValue::Map(OwnedMap::new());
                value[DATA] = OwnedValue::U64(2);
                let cell = OwnedCell::new_with_id(schema_id, &id, value);
                txn.upsert(cell).await
            })
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
pub async fn indexed_parallel_rpc_writes_complete_without_global_index_barrier() {
    let _ = env_logger::try_init();
    const CONTENT: &'static str = "content";
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let server_group = String::from("indexed_parallel_rpc_test");

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 128 * 1024 * 1024,
            db_size: 512 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: true,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &server_addr,
        &server_group,
        async |_| {},
    )
    .await
    .unwrap();

    let schema_id = 321;
    let schema = Schema::new_with_id(
        schema_id,
        "indexed_parallel_schema",
        None,
        Field::new_schema(vec![Field::new_indexed(
            CONTENT,
            Type::String,
            vec![crate::ram::schema::IndexType::Fulltext],
        )]),
        false,
        false,
    );

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
    client.new_schema_with_id(schema).await.unwrap().unwrap();

    let mut tasks: FuturesUnordered<_> = FuturesUnordered::new();
    let num_tasks = 128u64;
    let writes_per_task = 16u64;

    for worker in 0..num_tasks {
        let client_clone = client.clone();
        tasks.push(tokio::spawn(async move {
            for seq in 0..writes_per_task {
                let id = Id::from_parts(worker + 1, seq + 1);
                let mut value = OwnedValue::Map(OwnedMap::new());
                value[CONTENT] = OwnedValue::String(format!(
                    "shared-term worker-{worker} sequence-{seq} shared-term"
                ));
                let cell = OwnedCell::new_with_id(schema_id, &id, value);
                client_clone.upsert_cell(cell).await.unwrap().unwrap();
            }
            true
        }));
    }

    tokio::time::timeout(tokio::time::Duration::from_secs(20), async {
        while let Some(res) = tasks.next().await {
            assert!(res.unwrap());
        }
    })
    .await
    .expect(
        "concurrent indexed RPC writes should finish without stalling on unrelated index backlog",
    );

    let sample = client.read_cell(Id::from_parts(1, 1)).await.unwrap().unwrap();
    let content = sample.data[CONTENT].string().unwrap();
    assert!(content.contains("shared-term"));

    server.shutdown().await;
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

    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let server_group = String::from("wal_test");

    // === PHASE 1: Create server and add schemas (< 1000 to avoid snapshot) ===
    info!("PHASE 1: Creating server and adding 3 schemas (below snapshot threshold)");
    {
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()),
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

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
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()),
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

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
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let server_group = String::from("persistence_test");

    // === PHASE 1: Create server and add schemas ===
    info!("PHASE 1: Creating initial server and adding schemas");
    {
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()), // Enable persistence for this test
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

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
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()), // Resume from persisted state
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

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

        // Use a unique port for each cycle to avoid "Address already in use" errors
        let server_addr = crate::utils::test_port::unique_localhost_addr();

        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()), // Enable persistence for this test
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

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
        let server_addr = crate::utils::test_port::unique_localhost_addr(); // Unique port for final verification
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: Some(raft_path_str.clone()), // Enable persistence for this test
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

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
            chunk_size: 32 * 1024 * 1024,
            db_size: 128 * 1024 * 1024, // 128 MB
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
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "memory_status_test",
        async |_| {},
    )
    .await
    .unwrap();

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
    assert_eq!(
        status.shared_hot_segments, status.total_hot_segments,
        "shared hot counter should match scanned hot segments in the initial steady state"
    );
    assert_eq!(
        status.hot_segment_counter_drift, 0,
        "initial memory status should report no hot-segment counter drift"
    );
    assert_eq!(
        status.shared_hot_memory_bytes, status.total_hot_memory_bytes,
        "shared hot memory bytes should match scanned hot memory bytes when there is no drift"
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
    assert!(
        json.contains("shared_hot_segments"),
        "JSON should contain shared_hot_segments"
    );

    info!("Memory status test completed successfully");
}

#[tokio::test(flavor = "multi_thread")]
pub async fn compact_id_allocator_end_to_end() {
    let _ = env_logger::try_init();

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "compact_id_alloc_group",
        async |_| {},
    )
    .await
    .unwrap();

    let runtime = server.current_database();
    let allocator = runtime
        .id_allocator()
        .await
        .expect("allocator claims an origin through the lease authority");

    // Uniform ids: allocated class, distinct, spread over localities.
    let mut seen = std::collections::HashSet::new();
    let mut localities = std::collections::HashSet::new();
    for _ in 0..1000 {
        let id = allocator.take_uniform().await.unwrap();
        assert!(!id.is_hashed());
        assert_eq!(id.origin(), allocator.origin());
        assert!(seen.insert(id), "allocator issued a duplicate id");
        localities.insert(id.locality());
    }
    assert!(localities.len() > 500, "uniform locality clumped");

    // Affinity ids: co-located with the anchor, still unique.
    let anchor = allocator.take_uniform().await.unwrap();
    for _ in 0..100 {
        let id = allocator.take_near(&anchor).await.unwrap();
        assert_eq!(id.locality(), anchor.locality());
        assert!(seen.insert(id), "affinity id collided");
    }

    // A second database claims a distinct origin under its own authority.
    let analytics = server
        .ensure_database_runtime("analytics")
        .await
        .expect("analytics runtime");
    let analytics_alloc = analytics
        .id_allocator()
        .await
        .expect("second database allocator");
    let a_id = analytics_alloc.take_uniform().await.unwrap();
    assert!(!a_id.is_hashed());

    server.shutdown().await;
}

/// Round-trips a dynamic-schema cell whose static part carries an id array
/// of every small length. The dynamic tail begins where the array data
/// ends, so any writer/reader disagreement about the walk corrupts the
/// type-tagged dynamic fields (regression: invalid-UTF-8 panics reading
/// graph id-list cells under the 8-byte id layout).
#[tokio::test(flavor = "multi_thread")]
pub async fn dynamic_tail_layout_roundtrip_across_array_lengths() {
    let _ = env_logger::try_init();
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
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
    let fields = Field::new_schema(vec![
        Field::new_unindexed("next", Type::Id),
        Field::new_unindexed_array("list", Type::Id),
    ]);
    let schema = Schema::new_with_id(91, &String::from("dyn_layout"), None, fields, true, false);
    server.meta().schemas.debug_only_new_schema(schema.clone());
    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            "test",
        )
        .await
        .unwrap(),
    );

    for len in 0..12usize {
        let list_ids: Vec<Id> = (0..len).map(|_| Id::rand()).collect();
        let mut map = OwnedMap::new();
        map.insert("next", OwnedValue::Id(Id::rand()));
        map.insert(
            "list",
            OwnedValue::PrimArray(OwnedPrimArray::Id(list_ids.clone())),
        );
        map.insert("extra_note", OwnedValue::String(format!("note-{len}")));
        map.insert("extra_num", OwnedValue::U64(len as u64));
        // A dynamic entry inserted by key id only has no name: the writer
        // cannot persist it, but its presence must not desynchronize the
        // serialized entry count from the payload (regression: readers
        // overran into adjacent memory and decoded garbage strings).
        map.insert_key_id(0xdead_beef_dead_beef, OwnedValue::U64(42));
        let id = Id::rand();
        let cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(map));
        client.write_cell(cell).await.unwrap().unwrap();
        let read = client.read_cell(id).await.unwrap().unwrap();
        assert_eq!(
            read.data["extra_note"].string().unwrap(),
            &format!("note-{len}"),
            "dynamic string corrupted at list len {len}"
        );
        assert_eq!(read.data["extra_num"].u64().unwrap(), &(len as u64));
        match &read.data["list"] {
            OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) => {
                assert_eq!(ids, &list_ids, "list mismatch at len {len}")
            }
            other => panic!("unexpected list value at len {len}: {:?}", other),
        }
    }
}

/// Thread census for this process, by thread name.
#[cfg(target_os = "linux")]
fn threads_by_name() -> std::collections::BTreeMap<String, usize> {
    let mut census = std::collections::BTreeMap::new();
    if let Ok(entries) = std::fs::read_dir("/proc/self/task") {
        for entry in entries.flatten() {
            let comm = entry.path().join("comm");
            if let Ok(name) = std::fs::read_to_string(comm) {
                *census.entry(name.trim().to_string()).or_insert(0) += 1;
            }
        }
    }
    census
}

#[cfg(target_os = "linux")]
fn thread_count() -> usize {
    std::fs::read_dir("/proc/self/task")
        .map(|entries| entries.count())
        .unwrap_or(0)
}

/// Does a server give its threads back when it goes away?
///
/// It did not, and the graceful path hid it. Measured on the Morpheus suite:
/// after ~2950 tests the process held **9100 threads** and ~22 GB across
/// resident and swap -- 3456 `raft-server`, 2416 `cleaner-clean-t`,
/// 1208 `cleaner-evict-t`, 802 `stats-sweeper`, 302 `Cleaner main` -- and the
/// last test in the run, 11-18 s standalone, had not finished in 40 minutes.
///
/// The server itself always dropped. What survived was the graph it hands to
/// others: `rpc::Server` keeps a strong `Arc` to every service it hosts, and
/// the Raft service is held by its own plane tasks. Awaiting `shutdown` broke
/// both; dropping did not.
///
/// **The assertion is per-server, not a thread census.** A census is
/// meaningless inside a suite where hundreds of other tests are creating and
/// dropping servers concurrently -- it failed exactly that way. What is
/// deterministic is that a dropped server told its own cleaner and its own
/// store to stop. Set `NEB_THREAD_CENSUS=1` to also count process threads;
/// only do that when running this test alone.
///
/// Both paths are checked, and the dropped one is the point: a test that only
/// exercised `shutdown` passed throughout the entire period the leak existed.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn a_server_that_goes_away_gives_its_threads_back() {
    let _ = env_logger::try_init();
    const ROUNDS: usize = 3;
    let census = std::env::var("NEB_THREAD_CENSUS").is_ok();

    async fn spawn_server(label: &str) -> Arc<NebServer> {
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 16 * 1024 * 1024,
                db_size: 16 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &crate::utils::test_port::unique_localhost_addr(),
            label,
            async |_| {},
        )
        .await
        .unwrap()
    }

    spawn_server("thread_lifecycle_warmup").await.shutdown().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    for graceful in [true, false] {
        let before = thread_count();
        let census_before = threads_by_name();
        for round in 0..ROUNDS {
            let server = spawn_server(&format!("thread_lifecycle_{graceful}_{round}")).await;
            // Held across the drop so the teardown can be observed after it.
            let cleaner = server.cleaner().clone();
            let chunks = server.chunks().clone();
            let weak_runtime = Arc::downgrade(&server.database_runtime);
            assert!(
                !cleaner.is_stopped() && !chunks.is_background_stopped(),
                "a running server should not already be torn down"
            );
            if graceful {
                server.shutdown().await;
            }
            drop(server);
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(
                cleaner.is_stopped(),
                "graceful={graceful} round={round}: the server went away without stopping its \
                 cleaner, which is 13 threads per server"
            );
            assert!(
                chunks.is_background_stopped(),
                "graceful={graceful} round={round}: the server went away without stopping its \
                 store's background work"
            );
            drop(cleaner);
            drop(chunks);
            tokio::time::sleep(Duration::from_millis(300)).await;
            // Reported, NOT asserted, and the difference is the honest part.
            // Dropping a server stops its threads; it does not free its store,
            // because the RPC services that hold the store cannot be removed
            // safely while other servers share a `(group, database)` name. A
            // graceful shutdown does free it. See `stop_owned_background_work`.
            println!(
                "STILL REACHABLE (graceful={graceful} round={round}): runtime={}",
                weak_runtime.strong_count()
            );
            if graceful {
                assert_eq!(
                    weak_runtime.strong_count(),
                    0,
                    "a gracefully shut down server must let its database runtime go"
                );
            }
        }

        if !census {
            continue;
        }
        // Bounded wait: threads exit on their own schedule, and a census taken
        // the instant the last server dropped would report scheduling as a leak.
        let mut after = thread_count();
        for _ in 0..30 {
            if after <= before {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            after = thread_count();
        }
        let census_after = threads_by_name();
        let mut grew: Vec<(String, usize, usize)> = census_after
            .iter()
            .filter_map(|(name, count)| {
                let was = census_before.get(name).copied().unwrap_or(0);
                (*count > was).then(|| (name.clone(), was, *count))
            })
            .collect();
        grew.sort_by_key(|(_, was, now)| std::cmp::Reverse(now - was));
        println!(
            "THREADS (graceful={graceful}): {before} before, {after} after {ROUNDS} lifecycles ({:+})",
            after as i64 - before as i64
        );
        for (name, was, now) in &grew {
            println!("THREADS:   {name}: {was} -> {now}");
        }
        assert!(
            after <= before,
            "graceful={graceful}: {ROUNDS} servers were created and dropped, and the process \
             kept {} extra threads ({before} -> {after}); by name: {grew:?}",
            after as i64 - before as i64
        );
    }
}

/// Does UNLOADING a database give its threads back?
///
/// This is the production-shaped question, and the reason the sibling test
/// above is not the whole story. A whole server going away is a process
/// exiting, where nothing is reclaimed and nothing needs to be. What a
/// long-lived server really does is load and unload DATABASES -- Morpheus
/// exposes exactly that as `unload_runtime` and `drop_database`, clusterwide --
/// and each one carries a cleaner, its two rayon pools, and a statistics
/// sweeper.
///
/// So this test churns database runtimes on one server. Like its sibling, it
/// asserts PER-RUNTIME facts -- this runtime's cleaner stopped, this runtime's
/// store stopped -- rather than a process-wide thread census, which is
/// meaningless while hundreds of other tests create servers concurrently.
/// `NEB_THREAD_CENSUS=1` adds the census for running it alone.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn unloading_a_database_gives_its_threads_back() {
    let _ = env_logger::try_init();
    const DATABASES: usize = 4;

    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &crate::utils::test_port::unique_localhost_addr(),
        "database_runtime_churn",
        async |_| {},
    )
    .await
    .unwrap();

    // One load/unload first, so the baseline includes anything a database
    // initialises once per process rather than once per database.
    server.ensure_database_runtime("churn_warmup").await.unwrap();
    assert!(server.unload_database_runtime("churn_warmup").await);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let census = std::env::var("NEB_THREAD_CENSUS").is_ok();
    let before = thread_count();
    let census_before = threads_by_name();
    // The cleaner and the store are held across the unload so the teardown can
    // be observed after it -- exactly what the unload cannot rely on itself,
    // since the runtime is not dropped (see below).
    let mut loaded_dbs = Vec::new();
    for index in 0..DATABASES {
        let name = format!("churn_db_{index}");
        let runtime = server.ensure_database_runtime(&name).await.unwrap();
        loaded_dbs.push((
            name,
            runtime.cleaner().clone(),
            runtime.chunks().clone(),
            Arc::downgrade(&runtime),
        ));
    }
    let loaded = thread_count();
    if census {
        assert!(
            loaded > before,
            "loading {DATABASES} databases added no threads ({before} -> {loaded}), so this run \
             cannot say anything about unloading them"
        );
    }

    for (name, cleaner, chunks, weak) in &loaded_dbs {
        assert!(
            !cleaner.is_stopped() && !chunks.is_background_stopped(),
            "{name} is loaded and should not already be torn down"
        );
        let before_refs = weak.strong_count();
        assert!(
            server.unload_database_runtime(name).await,
            "{name} should have been loaded and unloadable"
        );
        assert!(
            server.database(name).is_none(),
            "{name} was unloaded but is still in the runtime map"
        );
        assert!(
            cleaner.is_stopped(),
            "{name} was unloaded without stopping its cleaner, which is 13 threads"
        );
        assert!(
            chunks.is_background_stopped(),
            "{name} was unloaded without stopping its store's background work, which is the \
             statistics sweeper -- and it cannot notice on its own, because unloading does \
             not drop the store"
        );
        println!(
            "DB CHURN: {name} refs {before_refs} -> {} after unload",
            weak.strong_count()
        );
    }
    let weak_runtimes: Vec<(String, std::sync::Weak<DatabaseRuntime>)> = loaded_dbs
        .into_iter()
        .map(|(name, _, _, weak)| (name, weak))
        .collect();

    // Is the residue a LEAK or just lazy? `PtrHashMap::remove` clones the value
    // out and retires the node, so the map's own copy is destroyed by
    // epoch-based reclamation, which only advances with further activity. Churn
    // the map and see whether the earlier runtimes are reclaimed.
    let alive_before_churn = weak_runtimes
        .iter()
        .filter(|(_, weak)| weak.strong_count() > 0)
        .count();
    for round in 0..10 {
        let name = format!("churn_extra_{round}");
        server.ensure_database_runtime(&name).await.unwrap();
        server.unload_database_runtime(&name).await;
    }
    let alive_after_churn = weak_runtimes
        .iter()
        .filter(|(_, weak)| weak.strong_count() > 0)
        .count();
    println!(
        "DB CHURN: {alive_before_churn} of {DATABASES} runtimes alive before extra churn,          {alive_after_churn} after 10 more load/unload cycles"
    );

    let mut after = thread_count();
    for _ in 0..30 {
        if after <= before {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        after = thread_count();
    }
    let census_after = if census {
        threads_by_name()
    } else {
        census_before.clone()
    };
    let still_alive: Vec<&String> = weak_runtimes
        .iter()
        .filter(|(_, weak)| weak.strong_count() > 0)
        .map(|(name, _)| name)
        .collect();
    let mut grew: Vec<(String, usize, usize)> = census_after
        .iter()
        .filter_map(|(name, count)| {
            let was = census_before.get(name).copied().unwrap_or(0);
            (*count > was).then(|| (name.clone(), was, *count))
        })
        .collect();
    grew.sort_by_key(|(_, was, now)| std::cmp::Reverse(now - was));
    println!(
        "DB CHURN: {before} threads before, {loaded} with {DATABASES} loaded, {after} after \
         unloading ({:+})",
        after as i64 - before as i64
    );
    for (name, was, now) in &grew {
        println!("DB CHURN:   {name}: {was} -> {now}");
    }

    server.shutdown().await;

    // ASSERTED, and it used to be a `println!` reporting 4 of 4 still reachable.
    // `database_runtimes` was a `PtrHashMap`, whose `remove` returns a CLONE and
    // leaves the original in the retired node -- so removal was not a release and
    // every database ever unloaded stayed resident for the life of the process,
    // memory store included. It is a plain `RwLock<HashMap>` now, which drops on
    // removal like anything else; the map was never on a request path, so there
    // was no lock-free property to trade away.
    assert!(
        still_alive.is_empty(),
        "unloading a database must release its runtime, and these are still reachable: \
         {still_alive:?}"
    );
    assert!(
        !census || after <= before,
        "{DATABASES} databases were loaded and unloaded, and the server kept {} extra threads \
         ({before} -> {after}); by name: {grew:?}",
        after as i64 - before as i64
    );
}

/// `PtrHashMap::remove` is not a release, and this server depends on knowing it.
///
/// The full semantics -- including that 900 further insert/remove cycles reclaim
/// nothing and only dropping the map does -- are pinned where they belong, in
/// Lightning's own `value_release_semantics`. This is the consumer-side half: the
/// one fact `unload_database_runtime_unchecked` is built on, next to the code
/// built on it.
///
/// Leaving the value in the retired node is documented, intended and correct: a
/// concurrent reader may still be dereferencing that node, so dropping at removal
/// time would be a use-after-free. QSBR makes the eventual drop safe; it does not
/// make it happen. Treating `remove` as a release was OUR mistake, and
/// `NebServer.database_runtimes` is a `PtrHashMap<String, Arc<DatabaseRuntime>>`
/// that lives as long as the server -- so an unloaded database stays resident,
/// and the unload has to stop its store's background work explicitly rather than
/// wait for a drop that never comes.
///
/// If this test ever starts failing because `remove` began releasing eagerly,
/// that is good news and the compensation in the unload can go.
#[test]
fn ptr_hash_map_remove_does_not_release_the_value() {
    use lightning::map::{Map, PtrHashMap};

    let map: PtrHashMap<usize, Arc<String>> = PtrHashMap::with_capacity(64);
    let value = Arc::new(String::from("owned"));
    map.insert(1, value.clone());
    assert_eq!(Arc::strong_count(&value), 2, "the map holds one");

    let removed = map.remove(&1).expect("the entry was inserted");
    assert!(map.get(&1).is_none(), "the key is gone from the map");
    drop(removed);
    assert_eq!(
        Arc::strong_count(&value),
        2,
        "the key is gone and what remove returned is dropped, and the retired node STILL \
         holds the value -- which is why unloading a database cannot wait for its store to \
         be dropped"
    );
}

/// MEASUREMENT for task #64 (the Morpheus suite ends holding ~9100 threads
/// across 302 in-process servers): how many OS threads does one full
/// `NebServer` boot + `shutdown()` + drop cycle retain?
///
/// ```text
/// cargo test --release --lib server_shutdown_thread_retention_probe -- --ignored --nocapture
/// ```
///
/// A probe, not a gate: it prints the per-cycle retention so the fix (or the
/// decision that production does not care -- one server per process exits
/// anyway) can be made from numbers. The first cycle is excluded from the
/// signal, as anything process-global initializes there.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement probe; run with --ignored --nocapture"]
async fn server_shutdown_thread_retention_probe() {
    let _ = env_logger::try_init();
    let opts = ServerOptions {
        chunk_size: 16 * 1024 * 1024,
        db_size: 16 * 1024 * 1024,
        tiered_config: None,
        backup_storage: None,
        wal_storage: None,
        undo_log_storage: None,
        raft_storage: None,
        index_enabled: false,
        services: vec![Service::Cell],
        enable_recovery: false,
        disable_storage_locks: true,
    };

    let mut after_prev = 0usize;
    for cycle in 0..4 {
        let before = thread_count();
        let server = NebServer::new_from_opts(
            &opts,
            &crate::utils::test_port::unique_localhost_addr(),
            "shutdown_thread_probe",
            async |_| {},
        )
        .await
        .unwrap();
        let peak = thread_count();
        server.shutdown().await;
        drop(server);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let after = thread_count();
        println!(
            "PROBE cycle={cycle} before={before} peak={peak} after={after} \
             retained_vs_before={} retained_vs_prev_cycle={}",
            after.saturating_sub(before),
            if cycle == 0 { 0 } else { after.saturating_sub(after_prev) }
        );
        after_prev = after;
    }
}
