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
    let server_addr = crate::utils::test_port::unique_localhost_addr();
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
            disable_storage_locks: true,
        },
        &server_addr,
        server_group,
        database_name,
        async |_| {},
    )
    .await
    .unwrap();

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
    let server_addr = crate::utils::test_port::unique_localhost_addr();
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
            disable_storage_locks: true,
        },
        &server_addr,
        &server_group,
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
    let server_addr = crate::utils::test_port::unique_localhost_addr();
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
            disable_storage_locks: true,
        },
        &server_addr,
        server_group,
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

#[tokio::test(flavor = "multi_thread")]
pub async fn read_all_cells_selected_returns_requested_fields_in_input_order() {
    let _ = env_logger::try_init();
    let (_server, client) = schema_validation_context("read_all_cells_selected_test", 5412).await;

    let schema = Schema::new_with_id(
        1,
        &String::from("selected_read_test"),
        None,
        default_fields(),
        false,
        false,
    );
    let schema_id = schema.id;
    client.new_schema_with_id(schema).await.unwrap().unwrap();

    let mut first = OwnedMap::new();
    first.insert(&String::from("id"), OwnedValue::I64(1));
    first.insert(&String::from("score"), OwnedValue::U64(10));
    first.insert(
        &String::from("name"),
        OwnedValue::String(String::from("First")),
    );
    let first_cell = OwnedCell::new_with_id(schema_id, &Id::rand(), OwnedValue::Map(first));
    client
        .write_cell(first_cell.clone())
        .await
        .unwrap()
        .unwrap();

    let mut second = OwnedMap::new();
    second.insert(&String::from("id"), OwnedValue::I64(2));
    second.insert(&String::from("score"), OwnedValue::U64(20));
    second.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Second")),
    );
    let second_cell = OwnedCell::new_with_id(schema_id, &Id::rand(), OwnedValue::Map(second));
    client
        .write_cell(second_cell.clone())
        .await
        .unwrap()
        .unwrap();

    let selected = client
        .read_all_cells_selected(
            &vec![second_cell.id(), first_cell.id()],
            &types::key_hashes(&vec![String::from("score"), String::from("name")]),
            true,
        )
        .await
        .unwrap();

    let second_selected = selected[0].as_ref().expect("second cell should read");
    assert_eq!(
        second_selected.data.uni_array().unwrap()[0].u64(),
        Some(&20)
    );
    assert_eq!(
        second_selected.data.uni_array().unwrap()[1]
            .string()
            .unwrap(),
        "Second"
    );

    let first_selected = selected[1].as_ref().expect("first cell should read");
    assert_eq!(first_selected.data.uni_array().unwrap()[0].u64(), Some(&10));
    assert_eq!(
        first_selected.data.uni_array().unwrap()[1]
            .string()
            .unwrap(),
        "First"
    );
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
        disable_storage_locks: true,
    }
}

async fn schema_validation_context(
    server_group: &str,
    _port: u16,
) -> (Arc<NebServer>, Arc<client::AsyncClient>) {
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let server = NebServer::new_from_opts(
        &schema_validation_server_options(),
        &server_addr,
        server_group,
        async |_| {},
    )
    .await
    .unwrap();

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
    IndexType::Vector(crate::index::vector::VectorIndexConfig::hnsw(
        crate::index::vector::MetricEncoding::Cosine,
        crate::index::vector::HnswConfig::default(),
    ))
}

fn test_embedding_index() -> IndexType {
    IndexType::Embedding(crate::index::embedding::EmbeddingIndexConfig::for_model(
        crate::index::embedding::EmbeddingModel::from("test-model"),
    ))
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
    let server_addr = crate::utils::test_port::unique_localhost_addr();
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
            disable_storage_locks: true,
        },
        &server_addr,
        server_group,
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
    let server_address_1 = &crate::utils::test_port::unique_localhost_addr();
    let server_address_2 = &crate::utils::test_port::unique_localhost_addr();

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
            disable_storage_locks: true,
        },
        server_address_1,
        server_1_group,
        async |_| {},
    )
    .await
    .unwrap();
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
            disable_storage_locks: true,
        },
        server_address_2,
        server_2_group,
        async |_| {},
    )
    .await
    .unwrap();
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

/// The slot table is seeded from the ring at startup, and agrees with it.
///
/// This is the property that lets the table be introduced at all: if adoption
/// produced *different* placement than `jump_hash` already computes, switching
/// over would relocate live data. So it is not enough that the table is
/// populated — every entry has to match what the ring would have said.
#[tokio::test(flavor = "multi_thread")]
pub async fn slot_table_is_seeded_to_agree_with_the_ring() {
    let _ = env_logger::try_init();
    let server_group = "slot_adoption_test";
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let database_name = "testdb_slot_adoption";

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
            disable_storage_locks: true,
        },
        &server_addr,
        server_group,
        database_name,
        async |_| {},
    )
    .await
    .unwrap();

    let (table, _applied_index) = crate::slots::load_table(
        &server_group.to_string(),
        &server.raft_client,
        crate::server::SLOTS_SM_ID,
    )
    .await
    .expect("slot table command should succeed");
    let table = table.expect("startup should have seeded a table, not left it absent");

    assert_eq!(
        table.len(),
        crate::slots::SLOT_COUNT,
        "every slot must be placed; a partial table would leave some ids unroutable"
    );

    // Agreement with the ring, entry by entry. Anything else means adopting the
    // table silently moves data.
    let conshash = server.conshash();
    for slot in 0..crate::slots::SLOT_COUNT {
        let expected = conshash
            .get_server_id(slot as u64)
            .expect("the ring places every slot on a single-member cluster");
        let placed = table
            .get(&(slot as u32))
            .expect("slot should be in the table");
        assert_eq!(
            placed.serving_owner(),
            expected,
            "slot {slot} was adopted onto {} but the ring says {expected}",
            placed.serving_owner()
        );
        assert!(
            !placed.is_migrating(),
            "a freshly seeded slot must not be in flight: slot {slot}"
        );
    }

    // Re-adopting claims nothing: a restart must not disturb placement, and
    // every member runs this concurrently at startup.
    let readopted = crate::slots::adopt_from_ring(
        &server_group.to_string(),
        &server.consh,
        &server.raft_client,
        crate::server::SLOTS_SM_ID,
    )
    .await
    .expect("re-adoption should succeed");
    assert_eq!(
        readopted, 0,
        "adoption must be idempotent; claiming slots again would move data"
    );
}



/// Routing through the table must agree with routing through the ring, and must
/// still work when there is no table.
///
/// This is the switchover safety property. The table only becomes authoritative
/// if it answers identically to what it replaces — otherwise adopting it
/// relocates live data. And a group that never seeded one has to keep working
/// exactly as before, which is what makes the change safe to land.
#[tokio::test(flavor = "multi_thread")]
pub async fn placement_reads_the_table_and_falls_back_to_the_ring() {
    let _ = env_logger::try_init();
    let server_group = "slot_routing_test";
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let database_name = "testdb_slot_routing";

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
            disable_storage_locks: true,
        },
        &server_addr,
        server_group,
        database_name,
        async |_| {},
    )
    .await
    .unwrap();

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

    // Spread across the slot space, including both id classes and the extremes,
    // so this is not just testing one lucky slot.
    let probes = [
        Id::from_parts(0, 1),
        Id::from_parts(1, 1),
        Id::from_parts(12345, 99),
        Id::from_parts(dovahkiin::types::custom_types::id::ID_LOCALITY_MASK, 7),
        Id::hashed(0x1234_5678_9abc_def0),
        Id::hashed(u64::MAX),
    ];

    for id in probes {
        let by_table = client.locate_server_id(&id).expect("table routing");
        let by_ring = server
            .conshash()
            .get_server_id(id.locality() as u64)
            .expect("ring routing");
        assert_eq!(
            by_table, by_ring,
            "id {id:?} routes to {by_table} through the table but {by_ring} through the ring; \
             adopting the table must not move anything"
        );
    }

    // With no table at all, the ring still answers -- a group that never seeded
    // one behaves exactly as it did before this existed.
    client.refresh_slot_owners(None, u64::MAX - 2);
    for id in probes {
        let by_ring = server
            .conshash()
            .get_server_id(id.locality() as u64)
            .expect("ring routing");
        assert_eq!(
            client.locate_server_id(&id).expect("fallback routing"),
            by_ring,
            "with no table, placement must fall back to the ring"
        );
    }

    // An unplaced slot inside an otherwise-present table also falls back,
    // rather than routing to server id 0, which is not a server.
    let mut sparse = vec![0u64; crate::slots::SLOT_COUNT];
    let probe = Id::from_parts(4242, 1);
    let expected = server
        .conshash()
        .get_server_id(probe.locality() as u64)
        .expect("ring routing");
    sparse[crate::slots::slot_of(&probe) as usize] = 0;
    client.refresh_slot_owners(Some(sparse), u64::MAX - 1);
    assert_eq!(
        client.locate_server_id(&probe).expect("sparse routing"),
        expected,
        "a zero entry is an unplaced slot, not a server id"
    );

    // And the table genuinely overrides the ring when it says something else --
    // otherwise none of the above proves the table is being consulted at all.
    let mut overridden = vec![0u64; crate::slots::SLOT_COUNT];
    const SENTINEL: u64 = 0xDEAD_BEEF;
    overridden[crate::slots::slot_of(&probe) as usize] = SENTINEL;
    client.refresh_slot_owners(Some(overridden), u64::MAX);
    assert_eq!(
        client.locate_server_id(&probe).expect("override routing"),
        SENTINEL,
        "the table must be authoritative where it has an entry"
    );
}

/// Slot enumeration must find exactly the cells in the requested slots.
///
/// This is the primitive migration is built on, so its precision matters more
/// than its speed: a missed cell is data left behind on the donor, and an extra
/// one is a cell moved that nobody asked to move. It reads no cell bodies —
/// `cell_index` is keyed by `id.bits()`, so a slot is bits 62..48 of the key.
#[tokio::test(flavor = "multi_thread")]
pub async fn slot_enumeration_finds_exactly_the_requested_slots() {
    use std::collections::HashSet;

    let _ = env_logger::try_init();
    let server_group = "slot_enumeration_test";
    let server_addr = crate::utils::test_port::unique_localhost_addr();
    let database_name = "testdb_slot_enumeration";

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
            disable_storage_locks: true,
        },
        &server_addr,
        server_group,
        database_name,
        async |_| {},
    )
    .await
    .unwrap();

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

    let schema = Schema::new_with_id(
        1200,
        &String::from("slot_enum_schema"),
        None,
        default_fields(),
        false,
        false,
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();

    // Three slots, several cells each, written with explicit ids so the expected
    // answer is known rather than inferred.
    const SLOTS: [u16; 3] = [11, 22, 33];
    const PER_SLOT: u64 = 4;
    let mut expected: std::collections::HashMap<u16, HashSet<Id>> = Default::default();
    for slot in SLOTS {
        for seq in 0..PER_SLOT {
            let id = Id::from_parts(slot as u64, 5000 + seq);
            let mut value = OwnedMap::new();
            value.insert(&String::from("id"), OwnedValue::I64(seq as i64));
            value.insert(&String::from("score"), OwnedValue::U64(0));
            value.insert(
                &String::from("name"),
                OwnedValue::String(format!("slot{slot}-{seq}")),
            );
            let cell = OwnedCell::new_with_id(1200, &id, OwnedValue::Map(value));
            client.write_cell(cell).await.unwrap().unwrap();
            expected.entry(slot).or_default().insert(id);
        }
    }

    let chunks = server.chunks();

    // One slot at a time: exact set, nothing from its neighbours.
    for slot in SLOTS {
        let found: HashSet<Id> = chunks
            .cell_ids_in_slots(&HashSet::from([slot]))
            .into_iter()
            .collect();
        assert_eq!(
            found, expected[&slot],
            "slot {slot} enumerated {found:?}, expected {:?}",
            expected[&slot]
        );
    }

    // Several slots in one pass: the union, which is the form a migration plan
    // actually uses.
    let all_requested: HashSet<u16> = SLOTS.into_iter().collect();
    let found: HashSet<Id> = chunks
        .cell_ids_in_slots(&all_requested)
        .into_iter()
        .collect();
    let union: HashSet<Id> = expected.values().flatten().copied().collect();
    assert_eq!(
        found, union,
        "a multi-slot pass must return the union and nothing else"
    );

    // A slot nobody wrote to is empty, not "everything" -- a filter that fails
    // open would migrate the whole server.
    assert!(
        chunks
            .cell_ids_in_slots(&HashSet::from([9999u16]))
            .is_empty(),
        "an unused slot must enumerate empty"
    );
    assert!(
        chunks.cell_ids_in_slots(&HashSet::new()).is_empty(),
        "an empty slot set must enumerate empty"
    );
}

/// The campaign's headline claim, as a deterministic test: **a member joining a
/// running cluster does not make existing cells unreachable.**
///
/// This is the failure that five startup-ordering patches tried and failed to
/// fix. Its mechanism was arithmetic, not timing: placement was
/// `nodes[jump_hash(n, locality)]`, so the instant `n` changed, roughly half of
/// all localities pointed at a different member — with their cells still on the
/// old one. Nothing was corrupted and nothing was deleted; the data was simply
/// looked up at an address it had never been written to.
///
/// The test is deliberately built so that it would *fail* without the slot
/// table rather than passing for free, and it says so out loud: it counts how
/// many of its own localities the ring now maps somewhere else, and refuses to
/// conclude anything if that count is zero.
#[tokio::test(flavor = "multi_thread")]
pub async fn cells_stay_addressable_when_a_member_joins_a_running_cluster() {
    use bifrost::conshash::slots::client::SMClient as SlotsSMClient;

    let _ = env_logger::try_init();
    let server_group = "slot_join_stability_test";
    let addresses = vec![
        crate::utils::test_port::unique_localhost_addr(),
        crate::utils::test_port::unique_localhost_addr(),
    ];
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

    // One member, which is how every cluster necessarily starts: the first node
    // cannot wait for the second, because the second cannot join until the
    // first is up.
    let first = NebServer::new_cluster_from_opts(
        &opts,
        &addresses[0],
        &addresses,
        server_group,
        async |_| {},
    )
    .await
    .unwrap();

    let client = Arc::new(
        client::AsyncClient::new(&first.rpc, &first.membership, &addresses, server_group)
            .await
            .unwrap(),
    );
    client
        .new_schema_with_id(Schema::new_with_id(
            1400,
            &String::from("join_stability_schema"),
            None,
            default_fields(),
            false,
            false,
        ))
        .await
        .unwrap()
        .unwrap();

    // This test is about placement NOT being recomputed when a member joins.
    // The automatic join fill would confound that by legitimately assigning
    // slots to the joiner, so freeze migrations for THIS GROUP -- the same
    // persistent per-group kill switch an operator uses, rather than a global
    // flag that would leak into every other test in the process.
    {
        let placement = SlotsSMClient::new(crate::server::SLOTS_SM_ID, &client.raft_client);
        placement
            .set_migration_freeze(&crate::slots::slot_group_id(server_group), &true)
            .await
            .expect("freezing migrations for this group should succeed");
    }

    // Spread across the slot space rather than clustered, so this cannot pass by
    // landing entirely in localities the ring happens not to reassign.
    let ids: Vec<Id> = (0..96u64).map(|i| Id::from_parts(i * 331, 1)).collect();
    for (seq, id) in ids.iter().enumerate() {
        let mut value = types::OwnedMap::new();
        value.insert(&String::from("id"), OwnedValue::I64(seq as i64));
        value.insert(&String::from("score"), OwnedValue::U64(seq as u64));
        value.insert(
            &String::from("name"),
            OwnedValue::String(format!("pre-join-{seq}")),
        );
        client
            .write_cell(OwnedCell::new_with_id(
                1400,
                id,
                OwnedValue::Map(value),
            ))
            .await
            .unwrap()
            .unwrap();
    }
    for id in &ids {
        client.read_cell(*id).await.unwrap().unwrap();
    }

    // The second member joins a cluster that is already holding data.
    let second = NebServer::new_cluster_from_opts(
        &opts,
        &addresses[1],
        &addresses,
        server_group,
        async |_| {},
    )
    .await
    .unwrap();

    // Wait for the join to be visible in the ring, because the whole point is
    // what happens *after* membership changes. A test that ran before the ring
    // noticed would prove nothing.
    let mut converged = false;
    for _ in 0..100 {
        if first.conshash().server_count() >= 2 {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        converged,
        "the ring never saw the second member, so this test cannot say anything about joins"
    );

    // How many of these localities would the RING now send elsewhere? This is
    // the test's own proof that it is not passing for free: if placement were
    // still computed, this many cells would have become unreachable.
    let reassigned_by_ring = ids
        .iter()
        .filter(|id| {
            first.conshash().get_server_id(id.locality() as u64) != Some(first.server_id)
        })
        .count();
    assert!(
        reassigned_by_ring > 0,
        "the ring reassigned none of {} localities on this join, so the test is vacuous",
        ids.len()
    );

    // The authoritative table, re-read from the state machine rather than from
    // the cache the client happened to load before the join.
    client.reload_slot_owners().await;
    let placement = SlotsSMClient::new(crate::server::SLOTS_SM_ID, &client.raft_client);
    let group = crate::slots::slot_group_id(server_group);
    assert_eq!(
        placement
            .slots_owned_by(&group, &first.server_id)
            .await
            .unwrap()
            .len(),
        crate::slots::SLOT_COUNT,
        "a joining member must own nothing: the table is not recomputed, so every slot \
         stays where it was"
    );
    assert!(
        placement
            .slots_owned_by(&group, &second.server_id)
            .await
            .unwrap()
            .is_empty(),
        "the new member should own no slots until something explicitly assigns them"
    );

    // And the part that actually matters: every cell written before the join is
    // still readable through the ordinary hashed path afterwards.
    for id in &ids {
        let cell = client
            .read_cell(*id)
            .await
            .unwrap()
            .unwrap_or_else(|error| {
                panic!(
                    "{id:?} (locality {}) became unreachable when a member joined: {error:?}",
                    id.locality()
                )
            });
        assert_eq!(cell.header.id, *id);
    }

    // Placement and the ring now genuinely disagree, which is the point -- and
    // the table is the one being obeyed.
    let disagreements = ids
        .iter()
        .filter(|id| {
            Some(client.locate_server_id(id).unwrap())
                != first.conshash().get_server_id(id.locality() as u64)
        })
        .count();
    assert_eq!(
        disagreements, reassigned_by_ring,
        "every locality the ring reassigned should be one where the table overrides it"
    );
}

/// The continuation of the join test above: a joiner owns nothing by design,
/// and this is the machinery that stops "nothing" from being permanent. The
/// leader fills the joining member toward the cluster's mean live bytes, using
/// the per-slot byte counters -- and stops BEFORE the move that would push it
/// past the mean, so a re-run moves nothing.
#[tokio::test(flavor = "multi_thread")]
pub async fn a_joining_member_is_filled_toward_the_mean() {
    use bifrost::conshash::fill::FillStop;
    use bifrost::conshash::slots::client::SMClient as SlotsSMClient;

    let _ = env_logger::try_init();
    let server_group = "join_fill_test";
    let addresses = vec![
        crate::utils::test_port::unique_localhost_addr(),
        crate::utils::test_port::unique_localhost_addr(),
    ];
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

    let first = NebServer::new_cluster_from_opts(
        &opts,
        &addresses[0],
        &addresses,
        server_group,
        async |_| {},
    )
    .await
    .unwrap();
    let client = Arc::new(
        client::AsyncClient::new(&first.rpc, &first.membership, &addresses, server_group)
            .await
            .unwrap(),
    );
    client
        .new_schema_with_id(Schema::new_with_id(
            1401,
            &String::from("join_fill_schema"),
            None,
            default_fields(),
            false,
            false,
        ))
        .await
        .unwrap()
        .unwrap();

    // Freeze migrations for THIS GROUP while the cluster is set up. The join
    // fill is automatic now, so without this the watcher fills the joiner
    // before the manual call below and the manual call's report no longer
    // describes what moved. Frozen, the manual path is deterministic again,
    // and the automatic path has its own test.
    let placement = SlotsSMClient::new(crate::server::SLOTS_SM_ID, &client.raft_client);
    let group = crate::slots::slot_group_id(server_group);
    placement
        .set_migration_freeze(&group, &true)
        .await
        .expect("freezing migrations for this group should succeed");

    // Cells across many distinct slots, so the fill has slots worth moving.
    let ids: Vec<Id> = (0..96u64).map(|i| Id::from_parts(i * 331, 7)).collect();
    for (seq, id) in ids.iter().enumerate() {
        let mut value = types::OwnedMap::new();
        value.insert(&String::from("id"), OwnedValue::I64(seq as i64));
        value.insert(&String::from("score"), OwnedValue::U64(seq as u64));
        value.insert(
            &String::from("name"),
            OwnedValue::String(format!("fill-{seq}")),
        );
        client
            .write_cell(OwnedCell::new_with_id(1401, id, OwnedValue::Map(value)))
            .await
            .unwrap()
            .unwrap();
    }
    let donor_bytes_before = first.chunks().total_live_bytes();
    assert!(donor_bytes_before > 0);

    let second = NebServer::new_cluster_from_opts(
        &opts,
        &addresses[1],
        &addresses,
        server_group,
        async |_| {},
    )
    .await
    .unwrap();
    let mut converged = false;
    for _ in 0..100 {
        if first.conshash().server_count() >= 2 {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(converged, "the ring never saw the second member");

    // Wait for the schema to reach the joiner. In production the watcher's
    // stability window (10 s) absorbs exactly this: a fresh member needs a
    // moment before it can accept cells of a schema created before it joined.
    // A transfer that races it anyway fails SAFELY -- the donor keeps the
    // cells and the fill stops with the refusal -- but this test is about the
    // fill, not the race.
    let mut schema_synced = false;
    for _ in 0..300 {
        if second.chunks().list[0].meta.schemas.get(&1401).is_some() {
            schema_synced = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        schema_synced,
        "the joiner never learned the schema -- historically this was the plane \
         split-brain: the joiner bootstrapped its own copy of the schema plane \
         rather than joining the existing one"
    );

    // The schema plane must be ONE cluster. Before the join fix this was
    // permanently one-sided, and only query-routing luck decided whether
    // anything worked -- including in runs that PASSED.
    {
        let plane_id = crate::server::database_meta_plane_id(server_group, server_group);
        let mut expected = addresses.clone();
        expected.sort();
        for (name, server) in [("first", &first), ("second", &second)] {
            let plane = server
                .raft_service
                .ensure_plane(bifrost::raft::PlaneSpec { plane_id })
                .await
                .unwrap_or_else(|e| panic!("{name} cannot handle the schema plane: {e:?}"));
            let mut members = plane
                .member_addresses()
                .await
                .unwrap_or_else(|e| panic!("{name} cannot read plane members: {e:?}"));
            members.sort();
            assert_eq!(
                members, expected,
                "{name} must see the schema plane as one two-member cluster"
            );
        }
    }

    // Let the AUTOMATIC fill fire and be refused by the freeze before thawing.
    //
    // The watcher fires once per join, after its stability window. Thawing
    // immediately leaves that pending attempt to land in the middle of the
    // manual fill below, and the manual call's report then no longer accounts
    // for everything the joiner gained -- a 1-in-5 failure under parallel
    // load. Waiting out the window means the automatic attempt has already
    // happened and been refused, so after the thaw the manual call is the only
    // mover.
    let auto_window_ms = std::env::var("NEB_JOIN_FILL_DELAY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(10_000);
    tokio::time::sleep(Duration::from_millis(auto_window_ms + 2_000)).await;
    placement
        .set_migration_freeze(&group, &false)
        .await
        .expect("unfreezing migrations should succeed");

    // The leader runs the fill; anyone else declines and says so.
    if !second.raft_service.is_leader_for_real().await {
        let refused = second
            .fill_joining_member(second.server_id)
            .await
            .expect_err("a non-leader must not run the fill");
        assert!(refused.contains("not the placement leader"), "{refused}");
    }

    let report = first
        .fill_joining_member(second.server_id)
        .await
        .expect("the leader's fill should run");
    assert!(
        matches!(report.stopped, FillStop::Filled),
        "expected a completed fill, got {:?}",
        report.stopped
    );
    assert!(report.moved_slots > 0, "a loaded cluster must donate slots");
    assert!(report.moved_bytes > 0);

    // The joiner now holds real bytes, and no more than the mean -- the stop
    // rule is "never overshoot", so overshooting is the failure mode to catch.
    let joiner_bytes = second.chunks().total_live_bytes();
    let donor_bytes_after = first.chunks().total_live_bytes();
    assert_eq!(joiner_bytes, report.moved_bytes);
    let mean = (joiner_bytes + donor_bytes_after) / 2;
    assert!(
        joiner_bytes <= mean,
        "the fill overshot: joiner {} vs mean {}",
        joiner_bytes,
        mean
    );
    assert!(
        donor_bytes_after >= joiner_bytes,
        "the donor must not end below the member it just filled"
    );

    // The placement table agrees with the counters.
    let joiner_slots = placement
        .slots_owned_by(&group, &second.server_id)
        .await
        .unwrap();
    assert_eq!(joiner_slots.len(), report.moved_slots);

    // Every cell is still readable through the ordinary path afterwards.
    client.reload_slot_owners().await;
    for id in &ids {
        let cell = client
            .read_cell(*id)
            .await
            .unwrap()
            .unwrap_or_else(|error| panic!("{id:?} unreachable after the fill: {error:?}"));
        assert_eq!(cell.header.id, *id);
    }

    // Hysteresis is the stop rule: a second fill moves nothing.
    let again = first
        .fill_joining_member(second.server_id)
        .await
        .expect("a re-run is always legal");
    assert_eq!(
        again.moved_slots, 0,
        "a filled member must not be filled again"
    );
}

/// The automatic path: nobody calls the fill, a member simply joins and the
/// leader fills it.
///
/// The manual test above proves the fill is correct; this proves it is
/// CONNECTED -- that the watcher is subscribed at startup, that the join event
/// reaches it, that the leader gate lets exactly one member act, and that the
/// stability window elapses rather than hanging. Shortened here through
/// `NEB_JOIN_FILL_DELAY_MS` so the test does not sit through the production
/// 10 s; the variable is set before either server starts and left set, since
/// its only effect on any other test is making a fill they do not perform
/// happen sooner.
#[tokio::test(flavor = "multi_thread")]
pub async fn a_joining_member_is_filled_automatically() {
    let _ = env_logger::try_init();
    // Deliberately does NOT shorten the stability window. That knob is a
    // process-global env var, and setting it here changed the behaviour of
    // whatever else was running in the same test process -- including the
    // manual fill test, which reads it to know how long to wait out this very
    // watcher. Two tests mutating and reading one global made each flake about
    // one run in six. Waiting the real 10 s costs this test ten seconds and
    // tests the production default instead of a special case.
    let server_group = "auto_join_fill_test";
    let addresses = vec![
        crate::utils::test_port::unique_localhost_addr(),
        crate::utils::test_port::unique_localhost_addr(),
    ];
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

    let first = NebServer::new_cluster_from_opts(
        &opts,
        &addresses[0],
        &addresses,
        server_group,
        async |_| {},
    )
    .await
    .unwrap();
    let client = Arc::new(
        client::AsyncClient::new(&first.rpc, &first.membership, &addresses, server_group)
            .await
            .unwrap(),
    );
    client
        .new_schema_with_id(Schema::new_with_id(
            1402,
            &String::from("auto_join_fill_schema"),
            None,
            default_fields(),
            false,
            false,
        ))
        .await
        .unwrap()
        .unwrap();

    let ids: Vec<Id> = (0..96u64).map(|i| Id::from_parts(i * 331, 11)).collect();
    for (seq, id) in ids.iter().enumerate() {
        let mut value = types::OwnedMap::new();
        value.insert(&String::from("id"), OwnedValue::I64(seq as i64));
        value.insert(&String::from("score"), OwnedValue::U64(seq as u64));
        value.insert(
            &String::from("name"),
            OwnedValue::String(format!("auto-{seq}")),
        );
        client
            .write_cell(OwnedCell::new_with_id(1402, id, OwnedValue::Map(value)))
            .await
            .unwrap()
            .unwrap();
    }
    assert!(first.chunks().total_live_bytes() > 0);

    // Nothing below calls the fill. The join is the only trigger.
    let second = NebServer::new_cluster_from_opts(
        &opts,
        &addresses[1],
        &addresses,
        server_group,
        async |_| {},
    )
    .await
    .unwrap();

    let mut filled_bytes = 0;
    // Generous: the production stability window is 10 s and the fill follows.
    for _ in 0..600 {
        filled_bytes = second.chunks().total_live_bytes();
        if filled_bytes > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        filled_bytes > 0,
        "the joining member was never filled: the watcher is not connected to the \
         join event, or the leader gate let nobody act"
    );

    // Filled to the mean, not past it -- the same stop rule, reached without
    // anyone asking.
    let donor_bytes = first.chunks().total_live_bytes();
    assert!(
        filled_bytes <= (filled_bytes + donor_bytes) / 2,
        "the automatic fill overshot: joiner {filled_bytes} vs donor {donor_bytes}"
    );

    // And the cluster still serves every cell it started with.
    client.reload_slot_owners().await;
    for id in &ids {
        let cell = client
            .read_cell(*id)
            .await
            .unwrap()
            .unwrap_or_else(|e| panic!("{id:?} unreachable after an automatic fill: {e:?}"));
        assert_eq!(cell.header.id, *id);
    }
}
