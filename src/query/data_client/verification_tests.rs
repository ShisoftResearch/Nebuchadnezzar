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
                    crate::index::verification::Verify::Predicate,
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
            .query_ids(
                SchemaUid(SCHEMA),
                selection(0),
                QueryOrdering::Asc,
                crate::index::verification::Verify::Predicate,
            )
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
                crate::index::verification::Verify::Predicate,
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
                crate::index::verification::Verify::Predicate,
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
            .hashed_query(
                SchemaUid(SCHEMA),
                hash_str(FIELD),
                &OwnedValue::U64(0),
                crate::index::verification::Verify::Predicate,
            )
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
            crate::index::verification::Verify::Predicate,
        )
        .await
        .unwrap();
    assert_eq!(
        count.next().await.unwrap().unwrap().columns[0].1,
        OwnedValue::U64(2)
    );
    assert!(collect(
        data_client
            .query_ids(
                SchemaUid(SCHEMA),
                selection(1),
                QueryOrdering::Asc,
                crate::index::verification::Verify::Predicate
            )
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
                .query_ids(
                    SchemaUid(SCHEMA),
                    selection(1),
                    QueryOrdering::Asc,
                    crate::index::verification::Verify::Predicate
                )
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
            .hashed_query(
                SchemaUid(SCHEMA),
                hash_str(FIELD),
                &OwnedValue::U64(0),
                crate::index::verification::Verify::Predicate
            )
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
                    None,
                    crate::index::verification::Verify::Predicate
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
            .hashed_query(
                SchemaUid(SCHEMA + 1),
                hash_str(FIELD),
                &OwnedValue::U64(0),
                crate::index::verification::Verify::Predicate
            )
            .await
            .unwrap()
            .unwrap(),
        vec![stale]
    );
    server.shutdown().await;
}

// Model the proposed owner counter with the strongest atomic order. The real
// verifier stays enabled so these tests preserve D9 while exposing raw hits.
struct CandidateInflightCounter(std::sync::atomic::AtomicUsize);

impl CandidateInflightCounter {
    fn new() -> Self {
        Self(std::sync::atomic::AtomicUsize::new(0))
    }

    fn begin(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn complete(&self) {
        let previous = self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        assert!(previous > 0);
    }

    fn count(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idxinflight_buffered_hit_outlives_successful_update() {
    let (server, client, _dir) = fixture("idxinflight_buffered", IndexType::Ranged).await;
    let id = Id::from_parts(3, 401);
    client.write_cell(cell(id, 0)).await.unwrap().unwrap();
    let data_client = server.indexed_data_client();
    let key_range = range(0).to_key_range(SchemaUid(SCHEMA), hash_str(FIELD), Ordering::Forward);
    let captured = data_client
        .index_clients
        .range_seek(key_range.clone(), 8, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(captured.current_block(), &vec![id]);

    let counter = CandidateInflightCounter::new();
    counter.begin();
    let (published, tasks) =
        IndexBuilder::collect_test_index_tasks(|| server.chunks().update_cell(&mut cell(id, 1)));
    published.unwrap();
    assert!(!tasks.is_empty());
    assert_eq!(counter.count(), 1);
    for task in tasks {
        task.await.unwrap();
    }
    counter.complete();
    assert_eq!(counter.count(), 0);
    assert_eq!(
        client.read_cell(id).await.unwrap().unwrap()[hash_str(FIELD)],
        OwnedValue::U64(1)
    );
    assert!(data_client
        .index_clients
        .range_seek(key_range.clone(), 8, None)
        .await
        .unwrap()
        .is_none());
    assert_eq!(captured.current_block(), &vec![id]);

    let checked = data_client
        .read_cells_from_ids_checked(
            captured.current_block(),
            &vec![],
            &Expr::nothing(),
            &Expr::nothing(),
            &vec![
                crate::index::verification::IndexCheck::Schema(SchemaUid(SCHEMA)),
                crate::index::verification::IndexCheck::Ranged(key_range),
            ],
        )
        .await
        .unwrap();
    println!(
        "IDXINFLIGHT_BUFFERED price=1 raw={:?} counter={} checked={:?}",
        captured.current_block(),
        counter.count(),
        checked.iter().map(|cell| cell.id()).collect::<Vec<_>>()
    );
    server.shutdown().await;
    assert!(
        checked.is_empty(),
        "a zero counter cannot validate a buffered hit"
    );
}

async fn reordered_successful_updates(index: IndexType) {
    let ranged = matches!(index, IndexType::Ranged);
    let name = if ranged {
        "idxinflight_reordered_ranged"
    } else {
        "idxinflight_reordered_hash"
    };
    let (server, client, _dir) = fixture(name, index).await;
    let id = Id::from_parts(3, 402);
    client.write_cell(cell(id, 0)).await.unwrap().unwrap();
    let counter = CandidateInflightCounter::new();

    counter.begin();
    let (first, first_tasks) =
        IndexBuilder::collect_test_index_tasks(|| server.chunks().update_cell(&mut cell(id, 1)));
    first.unwrap();
    assert!(!first_tasks.is_empty());
    counter.begin();
    let (second, second_tasks) =
        IndexBuilder::collect_test_index_tasks(|| server.chunks().update_cell(&mut cell(id, 2)));
    second.unwrap();
    assert!(!second_tasks.is_empty());
    assert_eq!(counter.count(), 2);

    for task in second_tasks {
        task.await.unwrap();
    }
    counter.complete();
    assert_eq!(counter.count(), 1, "the first update still owns its marker");
    for task in first_tasks {
        task.await.unwrap();
    }
    counter.complete();
    assert_eq!(counter.count(), 0);
    assert_eq!(
        client.read_cell(id).await.unwrap().unwrap()[hash_str(FIELD)],
        OwnedValue::U64(2)
    );

    let data_client = server.indexed_data_client();
    let raw = if ranged {
        let mut cursor = data_client
            .index_clients
            .range_seek(
                range(1).to_key_range(SchemaUid(SCHEMA), hash_str(FIELD), Ordering::Forward),
                8,
                None,
            )
            .await
            .unwrap()
            .unwrap();
        let mut ids = vec![];
        while let Some(id) = cursor.next().await.unwrap() {
            ids.push(id);
        }
        ids
    } else {
        let bucket = IndexedDataClient::hashed_index_id(
            SchemaUid(SCHEMA),
            hash_str(FIELD),
            &OwnedValue::U64(1),
        );
        data_client
            .index_clients
            .hashed_client
            .indexer
            .candidates(bucket)
            .await
            .unwrap()
            .unwrap()
    };
    assert_eq!(
        raw,
        vec![id],
        "successful reordered tasks left a stale entry"
    );
    let checked = collect(
        data_client
            .query_ids(
                SchemaUid(SCHEMA),
                selection(1),
                QueryOrdering::Asc,
                crate::index::verification::Verify::Predicate,
            )
            .await
            .unwrap(),
    )
    .await;
    let final_ids = collect(
        data_client
            .query_ids(
                SchemaUid(SCHEMA),
                selection(2),
                QueryOrdering::Asc,
                crate::index::verification::Verify::Predicate,
            )
            .await
            .unwrap(),
    )
    .await;
    println!(
        "IDXINFLIGHT_REORDERED index={name} price=2 raw={raw:?} counter={} checked={checked:?} final={final_ids:?}",
        counter.count()
    );
    server.shutdown().await;
    assert!(
        checked.is_empty(),
        "successful tasks and zero count do not prove a hit"
    );
    assert_eq!(final_ids, vec![id]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idxinflight_reordered_ranged_tasks_need_verification() {
    reordered_successful_updates(IndexType::Ranged).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idxinflight_reordered_hash_tasks_need_verification() {
    reordered_successful_updates(IndexType::Hashed).await;
}

async fn idcontract_count(client: &IndexedDataClient, price: u64, verify: Verify) -> u64 {
    let mut cursor = client
        .aggregate(
            SchemaUid(SCHEMA),
            AggregateQuery {
                selection: selection(price),
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
            verify,
        )
        .await
        .unwrap();
    let row = cursor.next().await.unwrap().unwrap();
    *row.columns[0].1.u64().unwrap()
}

async fn idcontract_reordered(index: IndexType) {
    let ranged = matches!(index, IndexType::Ranged);
    let name = if ranged {
        "idcontract_range"
    } else {
        "idcontract_hash"
    };
    let (server, client, _dir) = fixture(name, index).await;
    let stale = Id::from_parts(3, 801);
    let valid = Id::from_parts(3, 802);
    client.write_cell(cell(stale, 0)).await.unwrap().unwrap();
    client.write_cell(cell(valid, 1)).await.unwrap().unwrap();
    let (first, first_tasks) =
        IndexBuilder::collect_test_index_tasks(|| server.chunks().update_cell(&mut cell(stale, 1)));
    first.unwrap();
    let (second, second_tasks) =
        IndexBuilder::collect_test_index_tasks(|| server.chunks().update_cell(&mut cell(stale, 2)));
    second.unwrap();
    assert!(!first_tasks.is_empty() && !second_tasks.is_empty());
    for task in second_tasks.into_iter().chain(first_tasks) {
        task.await.unwrap();
    }
    let indexed = server.indexed_data_client();
    let before = super::read::cells_read_count();
    let ids = collect(
        indexed
            .query_ids(
                SchemaUid(SCHEMA),
                selection(1),
                QueryOrdering::Asc,
                Verify::Exists,
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        ids,
        vec![stale, valid],
        "existence permits the reordered stale entry"
    );
    assert_eq!(idcontract_count(&indexed, 1, Verify::Exists).await, 2);
    assert_eq!(
        super::read::cells_read_count(),
        before,
        "existence queries and counts must not read cells"
    );
    assert_eq!(
        collect(
            indexed
                .query_ids(
                    SchemaUid(SCHEMA),
                    selection(1),
                    QueryOrdering::Asc,
                    Verify::Predicate
                )
                .await
                .unwrap()
        )
        .await,
        vec![valid]
    );
    assert_eq!(idcontract_count(&indexed, 1, Verify::Predicate).await, 1);
    assert_eq!(
        idcontract_count(&indexed, 2, Verify::Exists).await,
        1,
        "count must include the true match"
    );
    let mut rows = indexed
        .query(
            SchemaUid(SCHEMA),
            selection(1),
            QueryOrdering::Asc,
            vec![ProjectionField {
                field_id: hash_str(FIELD),
                alias: None,
            }],
        )
        .await
        .unwrap();
    assert_eq!(rows.next().await.unwrap().unwrap().id, Some(valid));
    assert!(rows.next().await.unwrap().is_none());
    if ranged {
        let feature = OwnedValue::U64(1).feature();
        let mut cursor = client
            .ranged()
            .range_query(
                SchemaUid(SCHEMA),
                hash_str(FIELD),
                Some(&feature),
                Some(&feature),
                crate::client::ScanOrder::Forward,
                8,
                Verify::Exists,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cursor.current_block(), &vec![stale, valid]);
        assert_eq!(cursor.next_cell().await.unwrap().unwrap().id(), valid);
        assert!(cursor.next_cell().await.unwrap().is_none());
    } else {
        let mut ids = indexed
            .hashed_query(
                SchemaUid(SCHEMA),
                hash_str(FIELD),
                &OwnedValue::U64(1),
                Verify::Exists,
            )
            .await
            .unwrap()
            .unwrap();
        ids.sort();
        assert_eq!(ids, vec![stale, valid]);
    }
    // Hold delete tasks so both the current entry and the stale entry remain.
    let (deleted, tasks) =
        IndexBuilder::collect_test_index_tasks(|| server.chunks().remove_cell(&stale));
    deleted.unwrap();
    for verify in [Verify::Exists, Verify::Predicate] {
        assert_eq!(
            collect(
                indexed
                    .query_ids(SchemaUid(SCHEMA), selection(1), QueryOrdering::Asc, verify)
                    .await
                    .unwrap()
            )
            .await,
            vec![valid]
        );
        assert_eq!(idcontract_count(&indexed, 1, verify).await, 1);
        assert_eq!(idcontract_count(&indexed, 2, verify).await, 0);
    }
    for task in tasks {
        task.await.unwrap();
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idcontract_reordered_hash() {
    idcontract_reordered(IndexType::Hashed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idcontract_reordered_range() {
    idcontract_reordered(IndexType::Ranged).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idcontract_count_contains_true_matches() {
    assert_eq!(Verify::default(), Verify::Exists);
    let (server, client, _dir) = fixture("idcontract_count", IndexType::Ranged).await;
    client
        .write_cell(cell(Id::from_parts(3, 900), 1))
        .await
        .unwrap()
        .unwrap();
    let indexed = server.indexed_data_client();
    for verify in [Verify::Exists, Verify::Predicate] {
        assert_eq!(idcontract_count(&indexed, 1, verify).await, 1);
        assert_eq!(idcontract_count(&indexed, 2, verify).await, 0);
    }
    let before = super::read::cells_read_count();
    let result = indexed
        .query_ids_with_options(
            SchemaUid(SCHEMA),
            selection(1),
            QueryOrdering::Asc,
            Some(hash_str(FIELD)),
            None,
            None,
            None,
            Verify::Exists,
        )
        .await;
    assert!(
        matches!(result, Err(RPCError::IOError(error)) if error.kind() == std::io::ErrorKind::InvalidInput)
    );
    assert_eq!(super::read::cells_read_count(), before);
    server.shutdown().await;
}

#[tokio::test]
async fn idcontract_explicit_exists_refuses_value_dependent_shapes() {
    let (server, client, _dir) = fixture("idcontract_refusals", IndexType::Hashed).await;
    client
        .write_cell(cell(Id::from_parts(3, 990), 1))
        .await
        .unwrap()
        .unwrap();
    let indexed = server.indexed_data_client();
    for func in [
        AggregateFunction::CountField,
        AggregateFunction::Sum,
        AggregateFunction::Avg,
        AggregateFunction::Min,
        AggregateFunction::Max,
    ] {
        let query = AggregateQuery {
            selection: selection(1),
            group_by_fields: vec![],
            aggregates: vec![AggregateSpec {
                func,
                field_id: Some(hash_str(FIELD)),
                alias: "n".into(),
            }],
            order_by: None,
            limit: None,
            offset: None,
        };
        assert!(
            matches!(indexed.aggregate(SchemaUid(SCHEMA),query,vec![],Verify::Exists).await,
            Err(RPCError::IOError(error)) if error.kind()==std::io::ErrorKind::InvalidInput)
        );
    }
    let grouped = AggregateQuery {
        selection: selection(1),
        group_by_fields: vec![hash_str(FIELD)],
        aggregates: vec![AggregateSpec {
            func: AggregateFunction::CountStar,
            field_id: None,
            alias: "n".into(),
        }],
        order_by: None,
        limit: None,
        offset: None,
    };
    assert!(
        matches!(indexed.aggregate(SchemaUid(SCHEMA),grouped,vec![],Verify::Exists).await,
        Err(RPCError::IOError(error)) if error.kind()==std::io::ErrorKind::InvalidInput)
    );
    assert!(
        matches!(indexed.query_ids_resumable(SchemaUid(SCHEMA),selection(1),10,None,Verify::Exists).await,
        Err(RPCError::IOError(error)) if error.kind()==std::io::ErrorKind::InvalidInput)
    );
    assert!(
        matches!(indexed.query_ids_with_options(SchemaUid(SCHEMA),selection(1),QueryOrdering::Asc,None,Some(vec![hash_str(FIELD)]),None,None,Verify::Exists).await,
        Err(RPCError::IOError(error)) if error.kind()==std::io::ErrorKind::InvalidInput)
    );
    for raw in [
        "(= missing 1)",
        "(and (= price 1) (= missing 2))",
        "(text-match price \"one\")",
    ] {
        let expression = dovahkiin::integrated::lisp::parse_to_serde_expr(raw)
            .unwrap()
            .remove(0);
        let capability = indexed
            .id_query_capability(SchemaUid(SCHEMA), &expression, None, false, false, None)
            .await
            .unwrap();
        assert_eq!(capability.verify(), Verify::Predicate);
        assert!(
            matches!(indexed.query_ids(SchemaUid(SCHEMA),expression,QueryOrdering::Asc,Verify::Exists).await,
            Err(RPCError::IOError(error)) if error.kind()==std::io::ErrorKind::InvalidInput)
        );
    }
    server.shutdown().await;
}
