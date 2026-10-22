//! A read that a server fails must never answer with fewer rows.
//!
//! The faults are injected at the send of `CellReader`, per server: batch
//! sends and one-by-one sends fail a set number of times, or always.
use super::read::read_faults;
use super::*;
use crate::{
    ram::{
        cell::OwnedCell,
        schema::{Field, IndexType, Schema, SchemaVid},
        types::{Map, OwnedMap, Type},
    },
    server::{NebServer, ServerOptions, Service},
};
use bifrost_hasher::hash_str;
use std::sync::Arc;
use tempfile::TempDir;

const SCHEMA: u32 = 4951;
const PRICE: &str = "price";
const NOTE: &str = "note";
const ROWS: u64 = 40;

async fn fixture(name: &str) -> (Arc<NebServer>, IndexedDataClient, u64, TempDir) {
    let dir = TempDir::new().unwrap();
    let address = crate::utils::test_port::unique_localhost_addr();
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            raft_storage: Some(dir.path().to_str().unwrap().into()),
            index_enabled: true,
            services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &address,
        &name.to_string(),
        async |_| {},
    )
    .await
    .unwrap();
    let client = server.data_client(&vec![address]).await.unwrap();
    let schema = Schema::new_with_id(
        SCHEMA,
        name,
        None,
        Field::new_schema(vec![
            Field::new_indexed(PRICE, Type::U64, vec![IndexType::Ranged]),
            Field::new_unindexed(NOTE, Type::U64),
        ]),
        false,
        true,
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();
    for seq in 1..=ROWS {
        let mut data = OwnedValue::Map(OwnedMap::new());
        data[PRICE] = OwnedValue::U64(7);
        data[NOTE] = OwnedValue::U64(seq);
        let cell = OwnedCell::new_with_id(SchemaVid(SCHEMA), &Id::from_parts(5, seq), data);
        client.write_cell(cell).await.unwrap().unwrap();
    }
    server.await_index_tasks().await;
    let data_client = server.indexed_data_client();
    let owner = data_client
        .conshash
        .get_server_id_for_slot(crate::slots::slot_of(&Id::from_parts(5, 1)) as u64)
        .unwrap();
    for seq in 1..=ROWS {
        let id = Id::from_parts(5, seq);
        assert_eq!(
            data_client
                .conshash
                .get_server_id_for_slot(crate::slots::slot_of(&id) as u64),
            Some(owner),
            "the fixture expects one owner"
        );
    }
    (server, data_client, owner, dir)
}

fn compare(op: &str, field: &str, value: u64) -> Expr {
    Expr::List(vec![
        Expr::Symbol(hash_str(op), op.into()),
        Expr::Symbol(hash_str(field), field.into()),
        Expr::Value(OwnedValue::U64(value)),
    ])
}

/// The index-served query: candidates from the ranged index, read verified.
async fn indexed_query(client: &IndexedDataClient) -> Result<Vec<Id>, RPCError> {
    let mut cursor = client
        .query_ids_with_options(
            SchemaUid(SCHEMA),
            compare("=", PRICE, 7),
            QueryOrdering::Asc,
            None,
            None,
            None,
            None,
            Verify::Predicate,
        )
        .await?;
    let mut ids = vec![];
    while let Some(id) = cursor.next().await? {
        ids.push(id);
    }
    Ok(ids)
}

/// The resumable page of a filtered schema scan the index cannot serve.
async fn resumable_page(client: &IndexedDataClient) -> Result<ScanPage, RPCError> {
    Ok(client
        .query_ids_resumable(
            SchemaUid(SCHEMA),
            compare(">", NOTE, 0),
            1000,
            None,
            Verify::Predicate,
        )
        .await?
        .expect("an unindexed selection is a resumable scan"))
}

fn all_ids() -> Vec<Id> {
    (1..=ROWS).map(|seq| Id::from_parts(5, seq)).collect()
}

fn sorted(mut ids: Vec<Id>) -> Vec<Id> {
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_read_that_fails_twice_is_retried_and_returns_every_row() {
    let (server, client, owner, _dir) = fixture("read_fault_transient").await;
    read_faults::inject(owner, 2, 0);
    let ids = indexed_query(&client).await.unwrap();
    let fault = read_faults::clear(owner).unwrap();
    assert_eq!(sorted(ids), all_ids(), "a transient failure lost rows");
    assert!(
        fault.batch_sends >= 3,
        "the batch was not retried: {fault:?}"
    );
    assert_eq!(
        fault.single_sends, 0,
        "a retried batch needs no single reads"
    );

    read_faults::inject(owner, 2, 0);
    let page = resumable_page(&client).await.unwrap();
    read_faults::clear(owner).unwrap();
    assert_eq!(page.ids, all_ids(), "a transient failure lost page rows");
    assert_eq!(page.next_cursor, None);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_read_that_keeps_failing_falls_back_to_single_reads() {
    let (server, client, owner, _dir) = fixture("read_fault_single").await;
    read_faults::inject(owner, u32::MAX, 0);
    let ids = indexed_query(&client).await.unwrap();
    let fault = read_faults::clear(owner).unwrap();
    assert_eq!(sorted(ids), all_ids(), "the single-read fallback lost rows");
    assert!(fault.single_sends >= ROWS as u32, "{fault:?}");

    read_faults::inject(owner, u32::MAX, 0);
    let page = resumable_page(&client).await.unwrap();
    read_faults::clear(owner).unwrap();
    assert_eq!(
        page.ids,
        all_ids(),
        "the single-read fallback lost page rows"
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_that_cannot_be_read_is_a_typed_error_not_a_short_answer() {
    let (server, client, owner, _dir) = fixture("read_fault_permanent").await;
    read_faults::inject(owner, u32::MAX, u32::MAX);
    let query = indexed_query(&client).await;
    let page = resumable_page(&client).await;
    let fault = read_faults::clear(owner).unwrap();
    for (what, error) in [
        ("indexed query", query.map(|ids| ids.len()).unwrap_err()),
        (
            "resumable page",
            page.map(|page| page.ids.len()).unwrap_err(),
        ),
    ] {
        let failure = CellReadFailure::of(&error)
            .unwrap_or_else(|| panic!("{what}: not a cell read failure: {error:?}"));
        assert_eq!(failure.server_id, Some(owner), "{what}: {failure}");
        assert!(
            failure.reason.contains("injected read failure"),
            "{what}: {failure}"
        );
    }
    // Fail fast: one failed single read ends the read.
    assert!(fault.single_sends <= 2, "{fault:?}");

    // The same reads answer in full once the server recovers.
    assert_eq!(sorted(indexed_query(&client).await.unwrap()), all_ids());
    assert_eq!(resumable_page(&client).await.unwrap().ids, all_ids());
    server.shutdown().await;
}

/// A cell that fails its check is still dropped: that is not a read failure.
#[tokio::test(flavor = "multi_thread")]
async fn a_cell_that_fails_its_check_is_dropped_without_an_error() {
    let (server, client, _owner, _dir) = fixture("read_fault_check").await;
    let page = client
        .query_ids_resumable(
            SchemaUid(SCHEMA),
            compare(">", NOTE, ROWS / 2),
            1000,
            None,
            Verify::Predicate,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(page.ids, all_ids()[(ROWS / 2) as usize..].to_vec());
    let other_schema = client
        .read_cells_from_ids_checked(
            &all_ids(),
            &vec![],
            &Expr::nothing(),
            &Expr::nothing(),
            &vec![crate::index::verification::IndexCheck::Schema(SchemaUid(
                SCHEMA + 1,
            ))],
        )
        .await
        .unwrap();
    assert!(other_schema.is_empty());
    server.shutdown().await;
}
