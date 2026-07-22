use super::*;
use crate::ram::cell::{OwnedCell, ReadError};
use crate::ram::schema::Schema;
use crate::ram::tests::default_fields;
use crate::ram::types::{Id, OwnedMap, OwnedValue};
use crate::server::transactions;
use crate::server::{DatabaseRuntime, NebServer, ServerOptions, Service};
use bifrost_hasher::hash_str;
use dovahkiin::types::{Map, Value};
use std::sync::Arc;

async fn scoped_txn_client_for_database(
    address: &str,
    group_name: &str,
    database_name: &str,
) -> Arc<transactions::manager::AsyncServiceClient> {
    transactions::new_async_client_for_database(&address.to_string(), group_name, database_name)
        .await
        .unwrap()
}

async fn start_occ_test_server(address: &str, group: &str) -> Arc<NebServer> {
    NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: crate::ram::segs::SEGMENT_SIZE,
            db_size: crate::ram::segs::SEGMENT_SIZE,
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
        &address.to_string(),
        &group.to_string(),
        async |_| {},
    )
    .await
    .unwrap()
}

fn install_occ_schema(runtime: &Arc<DatabaseRuntime>) -> Schema {
    let schema = Schema::new_with_id(
        901,
        &String::from("txn_occ_repeatable"),
        None,
        default_fields(),
        false,
        false,
    );
    runtime.meta().schemas.debug_only_new_schema(schema.clone());
    schema
}

fn counter_cell(schema_id: u32, id: Id, score: u64, name: &str) -> OwnedCell {
    let mut data = OwnedMap::new();
    data.insert(&String::from("id"), OwnedValue::I64(id.lower as i64));
    data.insert(&String::from("score"), OwnedValue::U64(score));
    data.insert(&String::from("name"), OwnedValue::String(name.to_string()));
    OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(data))
}

fn score_of(cell: &OwnedCell) -> u64 {
    *cell.data["score"].u64().unwrap()
}

fn selected_score_of(cell: &OwnedCell) -> u64 {
    *cell.data.uni_array().unwrap()[0].u64().unwrap()
}

fn accepted_cell(result: TxnExecResult<OwnedCell, ReadError>) -> OwnedCell {
    match result {
        TxnExecResult::Accepted(cell) => cell,
        other => panic!("expected accepted cell, got {:?}", other),
    }
}

fn accepted_head(
    result: TxnExecResult<crate::ram::cell::CellHeader, ReadError>,
) -> crate::ram::cell::CellHeader {
    match result {
        TxnExecResult::Accepted(head) => head,
        other => panic!("expected accepted header, got {:?}", other),
    }
}

fn assert_missing(result: TxnExecResult<OwnedCell, ReadError>) {
    assert!(
        matches!(result, TxnExecResult::Error(ReadError::CellDoesNotExisted)),
        "expected missing cell result, got {:?}",
        result
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_full_read_uses_first_snapshot() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5310";
    let group = "txn_occ_repeatable_full";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90101);

    let mut initial = counter_cell(schema.id, cell_id, 0, "counter_full_initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    let first = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(score_of(&first), 0);

    let mut updated = counter_cell(schema.id, cell_id, 9, "counter_full_updated");
    let updated_header = runtime.chunks().update_cell(&mut updated).unwrap();
    assert!(updated_header.version > first.header.version);

    let second = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(second.header.version, first.header.version);
    assert_eq!(score_of(&second), 0);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_missing_read_caches_absence() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5311";
    let group = "txn_occ_repeatable_missing";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let missing_id = Id::new(0, 90102);

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    assert_missing(txn.read(tid.clone(), missing_id).await.unwrap().unwrap());

    let mut inserted = counter_cell(schema.id, missing_id, 9, "counter_missing_inserted");
    runtime.chunks().write_cell(&mut inserted).unwrap();

    assert_missing(txn.read(tid.clone(), missing_id).await.unwrap().unwrap());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_selected_and_head_share_full_snapshot() {
    let _ = env_logger::try_init();
    let address = "127.0.0.1:5312";
    let group = "txn_occ_repeatable_select_head";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let cell_id = Id::new(0, 90103);

    let mut initial = counter_cell(schema.id, cell_id, 0, "counter_select_initial");
    runtime.chunks().write_cell(&mut initial).unwrap();

    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    let selected = accepted_cell(
        txn.read_selected(tid.clone(), cell_id, vec![hash_str("score")])
            .await
            .unwrap()
            .unwrap(),
    );
    let head = accepted_head(txn.head(tid.clone(), cell_id).await.unwrap().unwrap());

    assert_eq!(selected.header.version, head.version);
    assert_eq!(selected_score_of(&selected), 0);

    let mut updated = counter_cell(schema.id, cell_id, 9, "counter_select_updated");
    let updated_header = runtime.chunks().update_cell(&mut updated).unwrap();
    assert!(updated_header.version > head.version);

    let full = accepted_cell(txn.read(tid.clone(), cell_id).await.unwrap().unwrap());
    assert_eq!(full.header.version, head.version);
    assert_eq!(score_of(&full), 0);

    server.shutdown().await;
}
