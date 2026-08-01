use super::*;
use crate::ram::schema::*;
use crate::ram::tests::default_fields;
use crate::ram::types::*;
use crate::ram::{cell::*, segs::SEGMENT_SIZE};
use crate::server::transactions;
use crate::server::*;
use bifrost_hasher::hash_str;
use dovahkiin::types::Type;
use env_logger;

async fn scoped_txn_client(
    address: &String,
    group_name: &str,
) -> Arc<transactions::manager::AsyncServiceClient> {
    transactions::new_async_client_for_database(address, group_name, group_name)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
pub async fn workspace_wr() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5200");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: SEGMENT_SIZE,
            db_size: SEGMENT_SIZE,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
        },
        &server_addr,
        "test",
        async |_| {},
    )
    .await
    .unwrap();
    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta().schemas.debug_only_new_schema(schema.clone());
    let txn = scoped_txn_client(&server_addr, "test").await;
    let txn_id = txn.begin().await.unwrap().unwrap();
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(100));
    data_map.insert(&String::from("score"), OwnedValue::U64(70));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Jack")),
    );
    let cell_1 = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map.clone()));
    let cell_1_w_res = txn
        .write(txn_id.to_owned(), cell_1.to_owned())
        .await
        .unwrap()
        .unwrap();
    match cell_1_w_res {
        TxnExecResult::Accepted(()) => {}
        _ => panic!("write cell 1 not accepted {:?}", cell_1_w_res),
    }
    let cell_1_r_res = txn
        .read(txn_id.to_owned(), cell_1.id())
        .await
        .unwrap()
        .unwrap();
    match cell_1_r_res {
        TxnExecResult::Accepted(cell) => {
            assert_eq!(cell.id(), cell_1.id());
            assert_eq!(cell.data["id"].i64().unwrap(), &100);
            assert_eq!(cell.data["name"].string().unwrap(), "Jack");
            assert_eq!(cell.data["score"].u64().unwrap(), &70);
        }
        _ => panic!("read cell 1 not accepted {:?}", cell_1_r_res),
    }
    data_map.insert(&String::from("score"), OwnedValue::U64(90));
    let cell_1_w2 =
        OwnedCell::new_with_id(schema.id, &cell_1.id(), OwnedValue::Map(data_map.clone()));
    let cell_1_w_res = txn
        .write(txn_id.to_owned(), cell_1_w2.to_owned())
        .await
        .unwrap()
        .unwrap();
    match cell_1_w_res {
        TxnExecResult::Accepted(()) => panic!("Write existed cell should fail"),
        TxnExecResult::Error(WriteError::CellAlreadyExisted) => {}
        _ => panic!("Wrong feedback {:?}", cell_1_w_res),
    }
    let cell_1_r_res = txn
        .read(txn_id.to_owned(), cell_1.id())
        .await
        .unwrap()
        .unwrap();
    match cell_1_r_res {
        TxnExecResult::Accepted(cell) => {
            assert_eq!(cell.id(), cell_1.id());
            assert_eq!(cell.data["score"].u64().unwrap(), &70);
        }
        _ => panic!("read cell 1 not accepted {:?}", cell_1_r_res),
    }
    let cell_1_u_res = txn
        .update(txn_id.to_owned(), cell_1_w2.to_owned())
        .await
        .unwrap()
        .unwrap();
    match cell_1_u_res {
        TxnExecResult::Accepted(()) => {}
        _ => panic!("update cell 1 not accepted"),
    }
    let cell_1_r_res = txn
        .read(txn_id.to_owned(), cell_1.id())
        .await
        .unwrap()
        .unwrap();
    match cell_1_r_res {
        TxnExecResult::Accepted(cell) => {
            assert_eq!(cell.id(), cell_1.id());
            assert_eq!(cell.data["score"].u64().unwrap(), &90);
        }
        _ => panic!("read cell 1 not accepted {:?}", cell_1_r_res),
    }
    let cell_1_rm_res = txn
        .remove(txn_id.to_owned(), cell_1.id())
        .await
        .unwrap()
        .unwrap();
    match cell_1_rm_res {
        TxnExecResult::Accepted(()) => {}
        _ => panic!("remove cell 1 not accepted {:?}", cell_1_rm_res),
    }
    let cell_1_r_res = txn
        .read(txn_id.to_owned(), cell_1.id())
        .await
        .unwrap()
        .unwrap();
    match cell_1_r_res {
        TxnExecResult::Error(ReadError::CellDoesNotExisted) => {}
        _ => panic!("read cell 1 not accepted {:?}", cell_1_r_res),
    }
    assert_eq!(
        txn.prepare(txn_id.to_owned()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(txn_id.to_owned()).await.unwrap().unwrap(),
        EndResult::Success
    );
    assert_eq!(
        txn.commit(txn_id.to_owned()).await.unwrap(),
        Err(TMError::TransactionNotFound)
    );
    // committed transaction should have been disposed
}

#[tokio::test(flavor = "multi_thread")]
pub async fn data_site_wr() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5201");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
        },
        &server_addr,
        "test",
        async |_| {},
    )
    .await
    .unwrap();
    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        true,
        false,
    );
    server.meta().schemas.debug_only_new_schema(schema.clone());
    let txn = scoped_txn_client(&server_addr, "test").await;
    let txn_id = txn.begin().await.unwrap().unwrap();
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(100));
    data_map.insert(&String::from("score"), OwnedValue::U64(70));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Jack")),
    );
    let cell_1 = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map.clone()));
    let cell_1_non_exists_read = txn
        .read(txn_id.to_owned(), cell_1.id())
        .await
        .unwrap()
        .unwrap();
    match cell_1_non_exists_read {
        TxnExecResult::Error(ReadError::CellDoesNotExisted) => {}
        _ => panic!(
            "read non-existed cell should fail but got {:?}",
            cell_1_non_exists_read
        ),
    }
    let _cell_1_write = txn
        .write(txn_id.to_owned(), cell_1.to_owned())
        .await
        .unwrap()
        .unwrap();
    let cell_1_r_res = txn
        .read(txn_id.to_owned(), cell_1.id())
        .await
        .unwrap()
        .unwrap();
    match cell_1_r_res {
        TxnExecResult::Accepted(cell) => {
            assert_eq!(cell.id(), cell_1.id());
            assert_eq!(cell.data["id"].i64().unwrap(), &100);
            assert_eq!(cell.data["name"].string().unwrap(), "Jack");
            assert_eq!(cell.data["score"].u64().unwrap(), &70);
        }
        _ => panic!("read cell 1 not accepted {:?}", cell_1_r_res),
    }
    data_map.insert(&String::from("score"), OwnedValue::U64(90));
    let cell_1_w2 =
        OwnedCell::new_with_id(schema.id, &cell_1.id(), OwnedValue::Map(data_map.clone()));
    let cell_1_w_res = txn
        .update(txn_id.to_owned(), cell_1_w2.to_owned())
        .await
        .unwrap()
        .unwrap();
    match cell_1_w_res {
        TxnExecResult::Accepted(()) => {}
        _ => panic!("Wrong feedback {:?}", cell_1_w_res),
    }
    assert!(server.chunks().read_cell(&cell_1.id()).is_err()); // isolation test
    assert_eq!(
        txn.prepare(txn_id.to_owned()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(txn_id.to_owned()).await.unwrap().unwrap(),
        EndResult::Success
    );
    let cell_r2 = server.chunks().read_cell(&cell_1.id()).unwrap();
    assert_eq!(cell_r2.id(), cell_1.id());
    assert_eq!(cell_r2.data["id"].i64().unwrap(), &100);
    assert_eq!(cell_r2.data["name"].string().unwrap(), "Jack");
    assert_eq!(cell_r2.data["score"].u64().unwrap(), &90);
}

#[tokio::test(flavor = "multi_thread")]
pub async fn data_site_commit_waits_for_hashed_indices() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5208");
    let server_group = "txn_hash_commit";
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: true,
            services: vec![
                Service::Cell,
                Service::Transaction,
                Service::Query,
                Service::HashIndexer,
            ],
            enable_recovery: false,
            disable_storage_locks: true,
            undo_log_storage: None,
            raft_storage: None,
        },
        &server_addr,
        server_group,
        async |_| {},
    )
    .await
    .unwrap();

    let fields = Field::new_schema(vec![
        Field::new_indexed("id", Type::U64, vec![IndexType::Hashed]),
        Field::new_unindexed("name", Type::String),
    ]);
    let schema = Schema::new_with_id(1208, "txn_hash_commit", None, fields, false, false);
    let client = server
        .data_client(&vec![server_addr.clone()])
        .await
        .unwrap();
    client
        .new_schema_with_id(schema.clone())
        .await
        .unwrap()
        .unwrap();

    let txn = scoped_txn_client(&server_addr, server_group).await;
    let txn_id = txn.begin().await.unwrap().unwrap();
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::U64(100));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Jack")),
    );
    let cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));

    let write_result = txn
        .write(txn_id.to_owned(), cell.to_owned())
        .await
        .unwrap()
        .unwrap();
    match write_result {
        TxnExecResult::Accepted(()) => {}
        _ => panic!("write cell not accepted: {:?}", write_result),
    }

    assert_eq!(
        txn.prepare(txn_id.to_owned()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(txn_id.to_owned()).await.unwrap().unwrap(),
        EndResult::Success
    );

    let query_result = server
        .indexed_data_client()
        .hashed_query(schema.id, hash_str("id"), &OwnedValue::U64(100))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(query_result, vec![cell.id()]);
}

#[tokio::test(flavor = "multi_thread")]
pub async fn multi_transaction() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5202");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
        },
        &server_addr,
        "test",
        async |_| {},
    )
    .await
    .unwrap();
    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta().schemas.debug_only_new_schema(schema.clone());
    let txn = scoped_txn_client(&server_addr, "test").await;
    let txn_1_id = txn.begin().await.unwrap().unwrap();
    let txn_2_id = txn.begin().await.unwrap().unwrap();
    let mut data_map_1 = OwnedMap::new();
    data_map_1.insert(&String::from("id"), OwnedValue::I64(100));
    data_map_1.insert(&String::from("score"), OwnedValue::U64(70));
    data_map_1.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Jack")),
    );
    let cell_1 =
        OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map_1.clone()));
    assert_eq!(
        txn.update(txn_1_id.to_owned(), cell_1.to_owned())
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Error(WriteError::CellDoesNotExisted)
    );
    let data_map_2 = data_map_1.clone();
    data_map_1.insert(&String::from("score"), OwnedValue::U64(90));
    let cell_2 =
        OwnedCell::new_with_id(schema.id, &cell_1.id(), OwnedValue::Map(data_map_2.clone()));
    let _cell_1_t2_write = txn
        .write(txn_2_id.to_owned(), cell_2.to_owned())
        .await
        .unwrap()
        .unwrap();
    txn.prepare(txn_2_id.to_owned()).await.unwrap().unwrap();
    txn.commit(txn_2_id.to_owned()).await.unwrap().unwrap();
    assert!(matches!(
        txn.abort(txn_1_id.to_owned()).await.unwrap().unwrap(),
        AbortResult::Success(_)
    ));
    ///////////////// PHASE 2 //////////////////
    let txn_1_id = txn.begin().await.unwrap().unwrap();
    let txn_2_id = txn.begin().await.unwrap().unwrap();
    let mut txn_2_cell = match txn
        .read(txn_2_id.to_owned(), cell_1.id())
        .await
        .unwrap()
        .unwrap()
    {
        TxnExecResult::Accepted(cell) => cell,
        _ => {
            panic!("Cannot read cell 1 for txn 2");
        }
    };
    txn.update(txn_1_id.to_owned(), cell_1.to_owned())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        txn.prepare(txn_1_id.to_owned()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(txn_1_id.to_owned()).await.unwrap().unwrap(),
        EndResult::Success
    );

    let mut stale_data = txn_2_cell.data.Map().unwrap().clone();
    stale_data.insert(&String::from("score"), OwnedValue::U64(80));
    txn_2_cell.data = OwnedValue::Map(stale_data);
    txn.update(txn_2_id.to_owned(), txn_2_cell)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        txn.prepare(txn_2_id.to_owned()).await.unwrap().unwrap(),
        TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
    );
    // Note: When prepare fails, the transaction is automatically aborted and cleaned up
    // to prevent memory leaks. No need to explicitly call abort or commit.
    let txn_1_id = txn.begin().await.unwrap().unwrap();
    txn.update(txn_1_id.to_owned(), cell_1.to_owned())
        .await
        .unwrap()
        .unwrap(); // txn_1_id > txn_2_id, realizable
    assert_eq!(
        txn.prepare(txn_1_id.to_owned()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(txn_1_id.to_owned()).await.unwrap().unwrap(),
        EndResult::Success
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn smoke_rw() {
    let _ = env_logger::try_init();
    // this test is likely to have unrealizable transactions and
    // should not cause any deadlock even if they failed
    let server_addr = String::from("127.0.0.1:5203");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
        },
        &server_addr,
        "test",
        async |_| {},
    )
    .await
    .unwrap();
    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta().schemas.debug_only_new_schema(schema.clone());
    let txn = scoped_txn_client(&server_addr, "test").await;
    let mut data_map_1 = OwnedMap::new();
    data_map_1.insert(&String::from("id"), OwnedValue::I64(100));
    data_map_1.insert(&String::from("score"), OwnedValue::U64(0));
    data_map_1.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Jack")),
    );
    let mut cell_1 =
        OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map_1.clone()));
    server.chunks().write_cell(&mut cell_1).unwrap();
    let cell_id = cell_1.id();
    let thread_count = 200;
    let mut futs: Vec<_> = Vec::with_capacity(thread_count);
    for _ in 0..thread_count {
        let txn = txn.clone();
        futs.push(tokio::spawn(async move {
            let txn_id = txn.begin().await.unwrap().unwrap();
            let read_result = txn
                .read(txn_id.to_owned(), cell_id.to_owned())
                .await
                .unwrap();
            if let Ok(TxnExecResult::Accepted(mut cell)) = read_result {
                let mut score = *cell.data["score"].u64().unwrap();
                score += 1;
                let mut data = cell.data.Map().unwrap().clone();
                data.insert(&String::from("score"), OwnedValue::U64(score));
                cell.data = OwnedValue::Map(data);
                txn.update(txn_id.to_owned(), cell.to_owned())
                    .await
                    .unwrap()
                    .unwrap();
            } else {
                // println!("Failed read, {:?}", read_result);
            }
            if txn.prepare(txn_id.to_owned()).await.unwrap() == Ok(TMPrepareResult::Success) {
                assert_eq!(
                    txn.commit(txn_id.to_owned()).await.unwrap().unwrap(),
                    EndResult::Success
                );
            }
        }));
    }
    for f in futs {
        f.await.unwrap();
    }
}

/// A pinned head-only read must not make a later same-txn remove report
/// `CellDoesNotExisted` — the head observation certifies the cell present
/// (regression: remove treated any `cell: None` entry as already removed).
#[tokio::test(flavor = "multi_thread")]
pub async fn head_then_remove_commits() {
    let _ = env_logger::try_init();
    let server_addr = String::from("127.0.0.1:5209");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
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
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta().schemas.debug_only_new_schema(schema.clone());
    let txn = scoped_txn_client(&server_addr, "test").await;

    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(100));
    data_map.insert(&String::from("score"), OwnedValue::U64(70));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Jack")),
    );
    let cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));
    let cell_id = cell.id();

    let setup_txn = txn.begin().await.unwrap().unwrap();
    txn.write(setup_txn.to_owned(), cell.to_owned())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        txn.prepare(setup_txn.to_owned()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(setup_txn.to_owned()).await.unwrap().unwrap(),
        EndResult::Success
    );

    let remove_txn = txn.begin().await.unwrap().unwrap();
    match txn
        .head(remove_txn.to_owned(), cell_id.to_owned())
        .await
        .unwrap()
        .unwrap()
    {
        TxnExecResult::Accepted(header) => assert_eq!(header.id(), cell_id),
        other => panic!("head should observe the cell: {:?}", other),
    }
    assert_eq!(
        txn.remove(remove_txn.to_owned(), cell_id.to_owned())
            .await
            .unwrap()
            .unwrap(),
        TxnExecResult::Accepted(())
    );
    assert_eq!(
        txn.prepare(remove_txn.to_owned()).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(remove_txn.to_owned()).await.unwrap().unwrap(),
        EndResult::Success
    );

    let verify_txn = txn.begin().await.unwrap().unwrap();
    assert!(matches!(
        txn.read(verify_txn.to_owned(), cell_id).await.unwrap().unwrap(),
        TxnExecResult::Error(ReadError::CellDoesNotExisted)
    ));
    assert!(matches!(
        txn.abort(verify_txn.to_owned()).await.unwrap().unwrap(),
        AbortResult::Success(_)
    ));
}
