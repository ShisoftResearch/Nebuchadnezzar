use crate::client;
use crate::client::transaction::TxnError;
use crate::ram::cell::*;
use crate::ram::schema::*;
use crate::ram::tests::default_fields;
use crate::ram::types;
use crate::ram::types::*;
use crate::server::*;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::*;

#[tokio::test(flavor = "multi_thread")]
pub async fn database_catalog() {
    let _ = env_logger::try_init();
    let server_group = "database_catalog_test";
    let server_addr = String::from("127.0.0.1:5399");
    let database_name = "testdb_database_catalog";
    let server = NebServer::new_from_opts_in_database(
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
        },
        &server_addr,
        server_group,
        database_name,
        async |_| {},
    )
    .await;

    assert_eq!(server.database_name(), database_name);

    let client = Arc::new(
        client::AsyncClient::new_for_database(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            server_group,
            database_name,
        )
        .await
        .unwrap(),
    );

    client.ensure_database().await.unwrap();

    let database = client
        .get_database(database_name)
        .await
        .unwrap()
        .expect("database should exist");
    assert_eq!(database.name, database_name);

    client.create_database("another_db").await.unwrap().unwrap();
    let mut databases = client
        .get_all_databases()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    databases.sort();

    assert_eq!(
        databases,
        vec!["another_db".to_string(), database_name.to_string()]
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn general() {
    let _ = env_logger::try_init();
    let server_group = "general_test";
    let server_addr = String::from("127.0.0.1:5400");
    debug!("Creating new neb server");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
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
    let schema = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            server_group,
        )
        .await
        .unwrap(),
    );
    let schema_id = client.new_schema(schema).await.unwrap().unwrap();
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(100));
    data_map.insert(&String::from("score"), OwnedValue::U64(0));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Jack")),
    );
    let cell_1 = OwnedCell::new_with_id(schema_id, &Id::rand(), OwnedValue::Map(data_map.clone()));
    client.write_cell(cell_1.clone()).await.unwrap().unwrap();
    let read_cell = client
        .read_cell(cell_1.clone().id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_cell["score"].u64().unwrap(), &0);
    assert_eq!(read_cell["id"].i64().unwrap(), &100);
    client
        .transaction(|ref mut _trans| {
            future::ready(Ok(())) // empty transaction
        })
        .await
        .unwrap();
    let should_aborted = client
        .transaction(|trans| async move { trans.abort().await })
        .await;
    match should_aborted {
        Err(TxnError::Aborted(_)) => {}
        _ => panic!("{:?}", should_aborted),
    }

    // TODO: investigate dead lock
    //    client.transaction(|ref mut trans| {
    //        trans.write(&cell_1) // regular fail case
    //    }).err().unwrap();
    client
        .transaction(move |trans| {
            async move {
                let empty_cell = OwnedCell::new_with_id(
                    schema_id,
                    &Id::rand(),
                    OwnedValue::Map(OwnedMap::new()),
                );
                trans.write(empty_cell.to_owned()).await // empty cell write should fail
            }
        })
        .await
        .err()
        .unwrap();

    let cell_1_id = cell_1.id();
    let thread_count = 50;
    let futs: FuturesUnordered<_> = FuturesUnordered::new();
    for _ in 0..thread_count {
        let client = client.clone();
        futs.push(async move {
            client
                .transaction(|txn| {
                    async move {
                        let mut cell = txn.read(cell_1_id.to_owned()).await?.unwrap();
                        // WARNING: read_selected is subject to dirty read
                        let selected = txn
                            .read_selected(
                                cell_1_id.to_owned(),
                                types::key_hashes(&vec![String::from("score")]),
                            )
                            .await?
                            .unwrap()
                            .data;
                        let mut score = *cell.data["score"].u64().unwrap();
                        assert_eq!(
                            selected.uni_array().unwrap()[0].u64(),
                            Some(&score),
                            "Selected value {:?}",
                            selected
                        );
                        score += 1;
                        let mut data = cell.data.Map().unwrap().clone();
                        data.insert(&String::from("score"), OwnedValue::U64(score));
                        cell.data = OwnedValue::Map(data);
                        txn.update(cell.to_owned()).await?;
                        let selected = txn
                            .read_selected(
                                cell_1_id.to_owned(),
                                types::key_hashes(&vec![String::from("score")]),
                            )
                            .await?
                            .unwrap()
                            .data;
                        assert_eq!(selected.uni_array().unwrap()[0].u64().unwrap(), &score);

                        let header = txn.head(cell.id()).await?.unwrap();
                        assert_eq!(header.id(), cell.id());
                        assert!(header.version > 1);

                        Ok(())
                    }
                })
                .await
                .unwrap()
        });
    }
    let _: Vec<_> = futs.collect().await;
    let cell_1_r = client.read_cell(cell_1.id()).await.unwrap().unwrap();
    assert_eq!(
        cell_1_r.data["score"].u64().unwrap(),
        &(thread_count as u64)
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn multi_cell_update() {
    let _ = env_logger::try_init();
    let server_group = "multi_cell_update_test";
    let server_addr = String::from("127.0.0.1:5401");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
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
        server_group,
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
    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            server_group,
        )
        .await
        .unwrap(),
    );
    let thread_count = 100;
    let schema_id = schema.id;
    let _ = client.new_schema_with_id(schema).await.unwrap();
    let all_schemas = client.get_all_schema().await.unwrap();
    assert!(!all_schemas.is_empty());
    info!("Schema id {}, all schemas: {:?}", schema_id, all_schemas);
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(100));
    data_map.insert(&String::from("score"), OwnedValue::U64(0));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Jack")),
    );
    let cell_1 = OwnedCell::new_with_id(schema_id, &Id::rand(), OwnedValue::Map(data_map.clone()));
    client.write_cell(cell_1.clone()).await.unwrap().unwrap();
    client.read_cell(cell_1.id()).await.unwrap().unwrap();
    let cell_1_id = cell_1.id();
    let mut cell_2 = cell_1.clone();
    cell_2.set_id(&Id::rand());
    client.write_cell(cell_2.clone()).await.unwrap().unwrap();
    client.read_cell(cell_2.id()).await.unwrap().unwrap();
    let cell_2_id = cell_2.id();
    let futs: FuturesUnordered<_> = FuturesUnordered::new();
    for _i in 0..thread_count {
        let client = client.clone();
        futs.push(async move {
            client
                .transaction(|txn| async move {
                    let mut score_1;
                    let mut score_2;
                    let mut cell_1 = txn.read(cell_1_id.to_owned()).await?.unwrap();
                    let mut cell_2 = txn.read(cell_2_id.to_owned()).await?.unwrap();
                    score_1 = *cell_1.data["score"].u64().unwrap();
                    score_2 = *cell_2.data["score"].u64().unwrap();
                    score_1 += 1;
                    score_2 += 1;
                    let mut data_1 = cell_1.data.Map().unwrap().clone();
                    data_1.insert(&String::from("score"), OwnedValue::U64(score_1));
                    cell_1.data = OwnedValue::Map(data_1);
                    let mut data_2 = cell_2.data.Map().unwrap().clone();
                    data_2.insert(&String::from("score"), OwnedValue::U64(score_2));
                    cell_2.data = OwnedValue::Map(data_2);
                    txn.update(cell_1.to_owned()).await?;
                    txn.update(cell_2.to_owned()).await?;
                    Ok(())
                })
                .await
                .unwrap();
        });
    }
    let _: Vec<_> = futs.collect().await;
    let cell_1_r = client.read_cell(cell_1_id).await.unwrap().unwrap();
    let cell_2_r = client.read_cell(cell_2_id).await.unwrap().unwrap();
    let cell_1_score = cell_1_r.data["score"].u64().unwrap();
    let cell_2_score = cell_2_r.data["score"].u64().unwrap();
    assert_eq!(cell_1_score + cell_2_score, (thread_count * 2) as u64);
}

fn schema_validation_server_options() -> ServerOptions {
    ServerOptions {
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
    }
}

async fn schema_validation_context(
    server_group: &str,
    port: u16,
) -> (Arc<NebServer>, Arc<client::AsyncClient>) {
    let server_addr = format!("127.0.0.1:{port}");
    let server = NebServer::new_from_opts(
        &schema_validation_server_options(),
        &server_addr,
        server_group,
        async |_| {},
    )
    .await;

    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            server_group,
        )
        .await
        .unwrap(),
    );

    (server, client)
}

async fn expect_schema_registration_ok(client: &Arc<client::AsyncClient>, schema: Schema) {
    client.new_schema_with_id(schema).await.unwrap().unwrap();
    let schemas = client.get_all_schema().await.unwrap();
    assert_eq!(schemas.len(), 1, "schema should pass pre-SM validation");
}

async fn expect_invalid_schema_registration(
    client: &Arc<client::AsyncClient>,
    schema: Schema,
) -> String {
    let res = client.new_schema_with_id(schema).await.unwrap();
    let msg = match res {
        Err(NewSchemaError::InvalidSchema(msg)) => msg,
        other => panic!("unexpected result: {other:?}"),
    };

    let schemas = client.get_all_schema().await.unwrap();
    assert!(
        schemas.is_empty(),
        "schema should be rejected before SM call"
    );
    msg
}

fn cosine_vector_index() -> IndexType {
    IndexType::Vector(crate::index::vector::VectorIndexConfig::new(
        crate::index::vector::MetricEncoding::Cosine,
    ))
}

fn test_embedding_index() -> IndexType {
    IndexType::Embedding(crate::index::embedding::EmbeddingModel::from("test-model"))
}

#[tokio::test(flavor = "multi_thread")]
pub async fn rejects_indexed_map_schema_before_state_machine() {
    let _ = env_logger::try_init();
    let (_server, client) =
        schema_validation_context("rejects_indexed_map_schema_before_state_machine", 5411).await;

    let schema = Schema::new_with_id(
        1,
        "invalid_indexed_map",
        None,
        Field::new_schema(vec![Field::new(
            "payload",
            Type::Map,
            false,
            false,
            Some(vec![Field::new_unindexed("value", Type::U64)]),
            vec![IndexType::Hashed],
        )]),
        false,
        false,
    );

    let msg = expect_invalid_schema_registration(&client, schema).await;
    assert!(msg.contains("payload"), "unexpected message: {msg}");
    assert!(
        msg.contains("only supports null indexing"),
        "unexpected message: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn allows_null_index_on_map_before_state_machine() {
    let _ = env_logger::try_init();
    let (_server, client) =
        schema_validation_context("allows_null_index_on_map_before_state_machine", 5413).await;

    let schema = Schema::new_with_id(
        1,
        "null_indexed_map",
        None,
        Field::new_schema(vec![Field::new(
            "payload",
            Type::Map,
            false,
            false,
            Some(vec![Field::new_unindexed("value", Type::U64)]),
            vec![IndexType::Null],
        )]),
        false,
        false,
    );

    expect_schema_registration_ok(&client, schema).await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn rejects_ranged_string_before_state_machine() {
    let _ = env_logger::try_init();
    let (_server, client) =
        schema_validation_context("rejects_ranged_string_before_state_machine", 5414).await;

    let schema = Schema::new_with_id(
        1,
        "invalid_ranged_string",
        None,
        Field::new_schema(vec![Field::new_indexed(
            "name",
            Type::String,
            vec![IndexType::Ranged],
        )]),
        false,
        false,
    );

    let msg = expect_invalid_schema_registration(&client, schema).await;
    assert!(msg.contains("name"), "unexpected message: {msg}");
    assert!(
        msg.contains("does not support ranged indexing"),
        "unexpected message: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn allows_string_hash_and_embedding_before_state_machine() {
    let _ = env_logger::try_init();
    let (_server, client) = schema_validation_context(
        "allows_string_hash_and_embedding_before_state_machine",
        5415,
    )
    .await;

    let schema = Schema::new_with_id(
        1,
        "valid_string_indices",
        None,
        Field::new_schema(vec![
            Field::new_indexed("tag", Type::String, vec![IndexType::Hashed]),
            Field::new_indexed("body", Type::String, vec![test_embedding_index()]),
        ]),
        false,
        false,
    );

    expect_schema_registration_ok(&client, schema).await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn allows_string_fulltext_before_state_machine() {
    let _ = env_logger::try_init();
    let (_server, client) =
        schema_validation_context("allows_string_fulltext_before_state_machine", 5416).await;

    let schema = Schema::new_with_id(
        1,
        "valid_string_fulltext",
        None,
        Field::new_schema(vec![Field::new_indexed(
            "body",
            Type::String,
            vec![IndexType::Fulltext],
        )]),
        false,
        false,
    );

    expect_schema_registration_ok(&client, schema).await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn rejects_non_string_fulltext_before_state_machine() {
    let _ = env_logger::try_init();
    let (_server, client) =
        schema_validation_context("rejects_non_string_fulltext_before_state_machine", 5417).await;

    let schema = Schema::new_with_id(
        1,
        "invalid_u64_fulltext",
        None,
        Field::new_schema(vec![Field::new_indexed(
            "body",
            Type::U64,
            vec![IndexType::Fulltext],
        )]),
        false,
        false,
    );

    let msg = expect_invalid_schema_registration(&client, schema).await;
    assert!(msg.contains("body"), "unexpected message: {msg}");
    assert!(
        msg.contains("does not support Fulltext indexing"),
        "unexpected message: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn allows_vector_field_before_state_machine() {
    let _ = env_logger::try_init();
    let (_server, client) =
        schema_validation_context("allows_vector_field_before_state_machine", 5418).await;

    let schema = Schema::new_with_id(
        1,
        "valid_vector_field",
        None,
        Field::new_schema(vec![Field::new_indexed_vector(
            "embedding",
            Type::F32,
            8,
            vec![cosine_vector_index()],
        )]),
        false,
        false,
    );

    expect_schema_registration_ok(&client, schema).await;
}

#[tokio::test(flavor = "multi_thread")]
pub async fn rejects_non_vector_field_with_vector_index_before_state_machine() {
    let _ = env_logger::try_init();
    let (_server, client) = schema_validation_context(
        "rejects_non_vector_field_with_vector_index_before_state_machine",
        5419,
    )
    .await;

    let schema = Schema::new_with_id(
        1,
        "invalid_vector_field",
        None,
        Field::new_schema(vec![Field::new_indexed(
            "embedding",
            Type::F32,
            vec![cosine_vector_index()],
        )]),
        false,
        false,
    );

    let msg = expect_invalid_schema_registration(&client, schema).await;
    assert!(msg.contains("embedding"), "unexpected message: {msg}");
    assert!(
        msg.contains("requires vector_size for vector indexing"),
        "unexpected message: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn rejects_non_numeric_vector_field_before_state_machine() {
    let _ = env_logger::try_init();
    let (_server, client) = schema_validation_context(
        "rejects_non_numeric_vector_field_before_state_machine",
        5420,
    )
    .await;

    let schema = Schema::new_with_id(
        1,
        "invalid_string_vector_field",
        None,
        Field::new_schema(vec![Field::new_indexed_vector(
            "embedding",
            Type::String,
            8,
            vec![cosine_vector_index()],
        )]),
        false,
        false,
    );

    let msg = expect_invalid_schema_registration(&client, schema).await;
    assert!(msg.contains("embedding"), "unexpected message: {msg}");
    assert!(
        msg.contains("does not support vector indexing"),
        "unexpected message: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn rejects_non_embedding_compound_index_before_state_machine() {
    let _ = env_logger::try_init();
    let (_server, client) = schema_validation_context(
        "rejects_non_embedding_compound_index_before_state_machine",
        5412,
    )
    .await;

    let mut schema = Schema::new_with_id(
        1,
        "invalid_compound_index",
        None,
        Field::new_schema(vec![
            Field::new_unindexed("title", Type::String),
            Field::new_unindexed("body", Type::String),
        ]),
        false,
        false,
    );
    schema.add_compound_index(
        "title_body_hash",
        vec!["title".to_string(), "body".to_string()],
        vec![IndexType::Hashed],
    );

    let msg = expect_invalid_schema_registration(&client, schema).await;
    assert!(
        msg.contains("only supports embedding indices"),
        "unexpected message: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
pub async fn write_skew() {
    let _ = env_logger::try_init();
    let server_group = "write_skew_test";
    let server_addr = String::from("127.0.0.1:5402");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
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
        server_group,
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
    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            server_group,
        )
        .await
        .unwrap(),
    );
    let schema_id = client.new_schema(schema).await.unwrap().unwrap();
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(100));
    data_map.insert(&String::from("score"), OwnedValue::U64(0));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Jack")),
    );
    let cell_1 = OwnedCell::new_with_id(schema_id, &Id::rand(), OwnedValue::Map(data_map.clone()));
    client.write_cell(cell_1.clone()).await.unwrap().unwrap();
    client.read_cell(cell_1.id()).await.unwrap().unwrap();
    let cell_1_id = cell_1.id();
    let client_c1 = client.clone();

    let t1 = tokio::spawn(async move {
        client_c1
            .transaction(|txn| {
                async move {
                    let mut cell_1 = txn.read(cell_1_id.to_owned()).await?.unwrap();
                    let mut score_1 = *cell_1.data["score"].u64().unwrap();
                    thread::sleep(Duration::new(2, 0)); // wait 2 secs to let late write occur
                    score_1 += 1;
                    let mut data_1 = cell_1.data.Map().unwrap().clone();
                    data_1.insert(&String::from("score"), OwnedValue::U64(score_1));
                    cell_1.data = OwnedValue::Map(data_1);
                    txn.update(cell_1.to_owned()).await?;
                    Ok(())
                }
            })
            .await
            .unwrap();
    });
    let client_c2 = client.clone();
    let t2 = tokio::spawn(async move {
        client_c2
            .transaction(|txn| async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let mut cell_1 = txn.read(cell_1_id.to_owned()).await?.unwrap();
                let mut score_1 = *cell_1.data["score"].u64().unwrap();
                score_1 += 1;
                let mut data_1 = cell_1.data.Map().unwrap().clone();
                data_1.insert(&String::from("score"), OwnedValue::U64(score_1));
                cell_1.data = OwnedValue::Map(data_1);
                txn.update(cell_1.to_owned()).await?;
                Ok(())
            })
            .await
            .unwrap();
    });
    t2.await.unwrap();
    t1.await.unwrap();
    let cell_1_r = client.read_cell(cell_1_id).await.unwrap().unwrap();
    let cell_1_score = *cell_1_r.data["score"].u64().unwrap();
    assert_eq!(cell_1_score, 2);
}

#[tokio::test(flavor = "multi_thread")]
pub async fn server_isolation() {
    let _ = env_logger::try_init();
    let server_1_group = "server_isolation_test_1";
    let server_2_group = "server_isolation_test_2";
    let server_address_1 = "127.0.0.1:5403";
    let server_address_2 = "127.0.0.1:5404";

    let server_1 = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
        },
        server_address_1,
        server_1_group,
        async |_| {},
    )
    .await;
    let client1 = Arc::new(
        client::AsyncClient::new(
            &server_1.rpc,
            &server_1.membership,
            &vec![server_address_1.to_string()],
            server_1_group,
        )
        .await
        .unwrap(),
    );

    let server_2 = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None, // No persistence for regular tests
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
        },
        server_address_2,
        server_2_group,
        async |_| {},
    )
    .await;
    let client2 = Arc::new(
        client::AsyncClient::new(
            &server_2.rpc,
            &server_2.membership,
            &vec![server_address_2.to_string()],
            server_2_group,
        )
        .await
        .unwrap(),
    );

    let schema1 = Schema::new_with_id(
        1,
        &String::from("test"),
        None,
        default_fields(),
        false,
        false,
    );
    let schema2 = Schema::new_with_id(
        2,
        "test",
        None,
        Field::new_schema(vec![
            Field::new_unindexed("-id", Type::U32),
            Field::new_unindexed("-name", Type::String),
            Field::new_unindexed("-score", Type::U8),
        ]),
        false,
        false,
    );
    client1
        .new_schema_with_id(schema1.clone())
        .await
        .unwrap()
        .unwrap();
    client2
        .new_schema_with_id(schema2.clone())
        .await
        .unwrap()
        .unwrap();

    // println!("{:?}", client1.schema_client.get(&schema1.id));

    let schema_1_got: Schema = client1
        .get_all_schema()
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone();
    assert_eq!(schema_1_got.id, 1);
    let schema_1_fields = schema1.fields;
    assert_eq!(
        schema_1_fields
            .clone()
            .sub_fields
            .unwrap()
            .first()
            .unwrap()
            .name,
        default_fields().sub_fields.unwrap().first().unwrap().name
    );
    assert_eq!(
        schema_1_fields
            .clone()
            .sub_fields
            .unwrap()
            .get(1)
            .unwrap()
            .name,
        default_fields().sub_fields.unwrap().get(1).unwrap().name
    );
    assert_eq!(
        schema_1_fields
            .clone()
            .sub_fields
            .unwrap()
            .get(2)
            .unwrap()
            .name,
        default_fields().sub_fields.unwrap().get(2).unwrap().name
    );

    let schema_2_got: Schema = client2
        .get_all_schema()
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone();
    assert_eq!(schema_2_got.id, 2);
    let schema_2_fields = schema2.fields;
    assert_eq!(
        schema_2_fields
            .clone()
            .sub_fields
            .unwrap()
            .first()
            .unwrap()
            .name,
        "-id"
    );
    assert_eq!(
        schema_2_fields
            .clone()
            .sub_fields
            .unwrap()
            .get(1)
            .unwrap()
            .name,
        "-name"
    );
    assert_eq!(
        schema_2_fields
            .clone()
            .sub_fields
            .unwrap()
            .get(2)
            .unwrap()
            .name,
        "-score"
    );
}
