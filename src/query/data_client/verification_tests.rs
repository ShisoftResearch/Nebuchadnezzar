use super::*;
use crate::{
    index::builder::IndexBuilder,
    ram::{
        cell::OwnedCell,
        schema::{Field, IndexType, Schema, SchemaVid},
        types::{Map, OwnedMap, Type, Value},
    },
    server::{NebServer, ServerOptions, Service},
};
use bifrost_hasher::hash_str;
use std::sync::Arc;
use tempfile::TempDir;

const SCHEMA: u32 = 4921;
const FIELD: &str = "price";

async fn fixture(
    name: &str,
    index: IndexType,
) -> (Arc<NebServer>, Arc<crate::client::AsyncClient>, TempDir) {
    let dir = TempDir::new().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
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
    let client = Arc::new(server.data_client(&vec![address]).await.unwrap());
    let schema = Schema::new_with_id(
        SCHEMA,
        name,
        None,
        Field::new_schema(vec![Field::new_indexed(FIELD, Type::U64, vec![index])]),
        false,
        true,
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();
    (server, client, dir)
}

fn cell(id: Id, price: u64) -> OwnedCell {
    let mut data = OwnedValue::Map(OwnedMap::new());
    data[FIELD] = OwnedValue::U64(price);
    OwnedCell::new_with_id(SchemaVid(SCHEMA), &id, data)
}

fn selection(price: u64) -> Expr {
    Expr::List(vec![
        Expr::Symbol(hash_str("="), "=".into()),
        Expr::Symbol(hash_str(FIELD), FIELD.into()),
        Expr::Value(OwnedValue::U64(price)),
    ])
}

fn range(price: u64) -> ValueRange {
    let feature = OwnedValue::U64(price).feature();
    ValueRange {
        start: ValueRangeTerm::Inclusive(feature),
        end: ValueRangeTerm::Inclusive(feature),
    }
}

async fn collect(mut cursor: IdCursor) -> Vec<Id> {
    let mut ids = vec![];
    while let Some(id) = cursor.next().await.unwrap() {
        ids.push(id);
    }
    ids
}

async fn publication_window(name: &str, index: IndexType, rows: bool) {
    let ranged = matches!(index, IndexType::Ranged);
    let (server, client, _dir) = fixture(name, index).await;
    // A full rejected block must precede the 2 valid hits.
    let stale: Vec<_> = (100..165).map(|seq| Id::from_parts(3, seq)).collect();
    let valid: Vec<_> = (200..202).map(|seq| Id::from_parts(3, seq)).collect();
    for id in stale.iter().chain(&valid) {
        client.write_cell(cell(*id, 0)).await.unwrap().unwrap();
    }
    let data_client = server.indexed_data_client();
    if ranged {
        let mut raw = data_client
            .index_clients
            .range_seek(
                range(0).to_key_range(SchemaUid(SCHEMA), hash_str(FIELD), Ordering::Forward),
                SCAN_BUFFER_SIZE,
                None,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            raw.next().await.unwrap(),
            Some(stale[0]),
            "fixture must put a stale hit first"
        );
    } else {
        let bucket = IndexedDataClient::hashed_index_id(
            SchemaUid(SCHEMA),
            hash_str(FIELD),
            &OwnedValue::U64(0),
        );
        let bucket_cell = client.read_cell(bucket).await.unwrap().unwrap();
        let raw = crate::index::hash::bucket_segment(&bucket_cell)
            .unwrap()
            .ids;
        assert_eq!(raw[0], stale[0], "fixture must put a stale hit first");
    }
    let (published, tasks) = IndexBuilder::collect_test_index_tasks(|| {
        for id in &stale {
            server.chunks().update_cell(&mut cell(*id, 1)).unwrap();
        }
    });
    let _ = published;
    assert!(!tasks.is_empty());
    assert_eq!(
        client.read_cell(stale[0]).await.unwrap().unwrap()[hash_str(FIELD)],
        OwnedValue::U64(1)
    );
    if ranged {
        for order in [
            crate::client::ScanOrder::Forward,
            crate::client::ScanOrder::Backward,
        ] {
            let feature = OwnedValue::U64(0).feature();
            let mut direct = client
                .ranged()
                .range_query(
                    SchemaUid(SCHEMA),
                    hash_str(FIELD),
                    Some(&feature),
                    Some(&feature),
                    order,
                    8,
                )
                .await
                .unwrap()
                .unwrap();
            assert!(direct.current_block().iter().all(|id| valid.contains(id)));
            let mut direct_ids = Vec::new();
            while let Some(id) = direct.next().await.unwrap() {
                direct_ids.push(id);
            }
            let mut expected = valid.clone();
            if matches!(order, crate::client::ScanOrder::Backward) {
                expected.reverse();
            }
            assert_eq!(
                direct_ids, expected,
                "public ranged cursor must verify every block"
            );
        }
    }
    let all = collect(
        data_client
            .query_ids(SchemaUid(SCHEMA), selection(0), QueryOrdering::Asc)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(all, valid, "stream and count must exclude stale hits");
    let limited = collect(
        data_client
            .query_ids_with_options(
                SchemaUid(SCHEMA),
                selection(0),
                QueryOrdering::Asc,
                None,
                None,
                Some(2),
                None,
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(limited, valid, "limit must refill past stale hits");
    let offset = collect(
        data_client
            .query_ids_with_options(
                SchemaUid(SCHEMA),
                selection(0),
                QueryOrdering::Asc,
                None,
                None,
                Some(1),
                Some(1),
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(offset, vec![valid[1]]);
    if rows {
        let mut projected = data_client
            .query_with_options(
                SchemaUid(SCHEMA),
                selection(0),
                QueryOrdering::Asc,
                None,
                None,
                Some(2),
                None,
                vec![ProjectionField {
                    field_id: hash_str(FIELD),
                    alias: None,
                }],
            )
            .await
            .unwrap();
        for id in &valid {
            let row = projected.next().await.unwrap().unwrap();
            assert_eq!(row.id, Some(*id));
            assert_eq!(row.columns[0].1, OwnedValue::U64(0));
        }
        assert!(projected.next().await.unwrap().is_none());
        let before = super::read::cells_read_count();
        let mut direct = data_client
            .range_index_scan(
                SchemaUid(SCHEMA),
                hash_str(FIELD),
                range(0),
                vec![hash_str(FIELD)],
                Expr::nothing(),
                Expr::nothing(),
                Ordering::Forward,
            )
            .await
            .unwrap();
        let mut seen = Vec::new();
        while let Some(cell) = direct.next().await.unwrap() {
            assert_eq!(cell[0usize], OwnedValue::U64(0));
            seen.push(cell.id());
        }
        assert_eq!(
            seen, valid,
            "direct cursor must refill past a rejected block"
        );
        assert_eq!(
            super::read::cells_read_count() - before,
            stale.len() + valid.len(),
            "verify and project with one read per hit"
        );
    } else if !ranged {
        let direct = data_client
            .hashed_query(SchemaUid(SCHEMA), hash_str(FIELD), &OwnedValue::U64(0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(direct, valid);
    }
    let mut count = data_client
        .aggregate(
            SchemaUid(SCHEMA),
            AggregateQuery {
                selection: selection(0),
                group_by_fields: vec![],
                aggregates: vec![AggregateSpec {
                    func: AggregateFunction::CountStar,
                    field_id: None,
                    alias: "count".into(),
                }],
                order_by: None,
                limit: None,
                offset: None,
            },
            vec![ProjectionItem::Aggregate {
                alias: "count".into(),
                output_name: None,
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        count.next().await.unwrap().unwrap().columns[0].1,
        OwnedValue::U64(2)
    );
    assert!(collect(
        data_client
            .query_ids(SchemaUid(SCHEMA), selection(1), QueryOrdering::Asc)
            .await
            .unwrap()
    )
    .await
    .is_empty());
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(
        collect(
            data_client
                .query_ids(SchemaUid(SCHEMA), selection(1), QueryOrdering::Asc)
                .await
                .unwrap()
        )
        .await,
        stale
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idxverify_ranged_publication_window_ids() {
    publication_window("idxverify_ranged_ids", IndexType::Ranged, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idxverify_ranged_publication_window_rows() {
    publication_window("idxverify_ranged_rows", IndexType::Ranged, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idxverify_hash_publication_window() {
    publication_window("idxverify_hash", IndexType::Hashed, false).await;
    verify_hash_schema_replacement().await;
}

async fn verify_hash_schema_replacement() {
    let (server, client, _dir) = fixture("idxverify_hash_schema", IndexType::Hashed).await;
    let other = Schema::new_with_id(
        SCHEMA + 1,
        "idxverify_other",
        None,
        Field::new_schema(vec![Field::new_indexed(
            FIELD,
            Type::U64,
            vec![IndexType::Hashed],
        )]),
        false,
        true,
    );
    client.new_schema_with_id(other).await.unwrap().unwrap();
    let stale = Id::from_parts(3, 100);
    let valid = Id::from_parts(3, 200);
    for id in [stale, valid] {
        client.write_cell(cell(id, 0)).await.unwrap().unwrap();
    }
    let mut replacement = cell(stale, 0);
    replacement.header.schema = SchemaVid(SCHEMA + 1);
    let (published, tasks) = IndexBuilder::collect_test_index_tasks(|| {
        server.chunks().remove_cell(&stale).unwrap();
        server.chunks().write_cell(&mut replacement)
    });
    published.unwrap();
    assert!(!tasks.is_empty());
    let data_client = server.indexed_data_client();
    assert_eq!(
        data_client
            .hashed_query(SchemaUid(SCHEMA), hash_str(FIELD), &OwnedValue::U64(0))
            .await
            .unwrap()
            .unwrap(),
        vec![valid]
    );
    assert_eq!(
        collect(
            data_client
                .query_ids_with_options(
                    SchemaUid(SCHEMA),
                    selection(0),
                    QueryOrdering::Asc,
                    None,
                    None,
                    Some(1),
                    None
                )
                .await
                .unwrap()
        )
        .await,
        vec![valid]
    );
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(
        data_client
            .hashed_query(SchemaUid(SCHEMA + 1), hash_str(FIELD), &OwnedValue::U64(0))
            .await
            .unwrap()
            .unwrap(),
        vec![stale]
    );
    server.shutdown().await;
}
