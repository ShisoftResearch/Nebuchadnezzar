//! Reusable timing probe for index queries. Run release builds on the timing host.
use bifrost_hasher::hash_str;
use dovahkiin::{
    expr::serde::Expr,
    types::{Id, Map, OwnedMap, OwnedValue, Type},
};
use neb::{
    client::ScanOrder,
    query::data_client::{
        AggregateFunction, AggregateQuery, AggregateSpec, IndexedDataClient, ProjectionItem,
        QueryOrdering, ValueRange, ValueRangeTerm,
    },
    ram::{
        cell::OwnedCell,
        schema::{Field, IndexType, Schema, SchemaUid, SchemaVid},
    },
    server::{NebServer, ServerOptions, Service},
};
use std::time::Instant;

const SCHEMA: u32 = 4931;
fn verify() -> neb::index::verification::Verify {
    match std::env::var("INDEX_LOOKUP_VERIFY").as_deref() {
        Ok("exists") => neb::index::verification::Verify::Exists,
        Ok("predicate") | Err(_) => neb::index::verification::Verify::Predicate,
        Ok(mode) => panic!("invalid INDEX_LOOKUP_VERIFY: {mode}"),
    }
}
fn eq(field: &str, value: u64) -> Expr {
    Expr::List(vec![
        Expr::Symbol(hash_str("="), "=".into()),
        Expr::Symbol(hash_str(field), field.into()),
        Expr::Value(OwnedValue::U64(value)),
    ])
}
async fn ids(client: &IndexedDataClient, field: &str, limit: Option<usize>) -> usize {
    let mut cursor = client
        .query_ids_with_options(
            SchemaUid(SCHEMA),
            eq(field, 0),
            QueryOrdering::Asc,
            None,
            None,
            limit,
            None,
            verify(),
        )
        .await
        .unwrap();
    let mut count = 0;
    while let Some(id) = cursor.next().await.unwrap() {
        std::hint::black_box(id);
        count += 1;
    }
    count
}
fn emit(kind: &str, rounds: usize, started: Instant) {
    println!(
        "LOOKUP kind={kind} ops={rounds} secs={:.9}",
        started.elapsed().as_secs_f64()
    );
}
#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    let count: usize = std::env::var("INDEX_LOOKUP_CELLS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4096);
    let rounds: usize = std::env::var("INDEX_LOOKUP_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    assert!(count >= 128 && count % 4 == 0);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let group = format!("index_lookup_{}", std::process::id());
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 256 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            raft_storage: None,
            index_enabled: true,
            services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &address,
        &group,
        async |_| {},
    )
    .await
    .unwrap();
    let client = server.data_client(&vec![address]).await.unwrap();
    let schema = Schema::new_with_id(
        SCHEMA,
        "lookup",
        None,
        Field::new_schema(vec![
            Field::new_indexed("price", Type::U64, vec![IndexType::Ranged]),
            Field::new_indexed("sku", Type::U64, vec![IndexType::Hashed]),
            Field::new_indexed("region", Type::U64, vec![IndexType::Hashed]),
        ]),
        false,
        true,
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();
    for offset in (0..count).step_by(128) {
        let cells = (offset..(offset + 128).min(count))
            .map(|i| {
                let mut data = OwnedValue::Map(OwnedMap::new());
                data["price"] = OwnedValue::U64((i % 4) as u64);
                data["sku"] = OwnedValue::U64(i as u64);
                data["region"] = OwnedValue::U64((i % 4) as u64);
                OwnedCell::new_with_id(SchemaVid(SCHEMA), &Id::from_parts(3, 100 + i as u64), data)
            })
            .collect();
        for result in client.upsert_all_cells(cells).await.unwrap() {
            result.unwrap();
        }
    }
    let indexed = server.indexed_data_client();
    for _ in 0..3 {
        assert_eq!(ids(&indexed, "price", None).await, count / 4);
        assert_eq!(ids(&indexed, "region", None).await, count / 4);
    }
    let began = Instant::now();
    let points = rounds * 128;
    for i in 0..points {
        let n = i % count;
        let found = indexed
            .hashed_query(
                SchemaUid(SCHEMA),
                hash_str("sku"),
                &OwnedValue::U64(n as u64),
                verify(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found, vec![Id::from_parts(3, 100 + n as u64)]);
        std::hint::black_box(found);
    }
    emit("hash_point", points, began);
    for (label, limit) in [("range_full", None), ("range_limit", Some(32))] {
        let feature = OwnedValue::U64(0).feature();
        let ranged = client.ranged();
        let began = Instant::now();
        for _ in 0..rounds {
            let mut cursor = ranged
                .range_query(
                    SchemaUid(SCHEMA),
                    hash_str("price"),
                    Some(&feature),
                    Some(&feature),
                    ScanOrder::Forward,
                    64,
                    verify(),
                )
                .await
                .unwrap()
                .unwrap();
            let mut hits = 0;
            while let Some(id) = cursor.next().await.unwrap() {
                std::hint::black_box(id);
                hits += 1;
                if limit == Some(hits) {
                    break;
                }
            }
            assert_eq!(hits, limit.unwrap_or(count / 4));
        }
        emit(label, rounds, began);
    }
    for (field, prefix) in [("price", "range"), ("region", "hash")] {
        for (suffix, limit) in [("ids", None), ("ids_limit", Some(32))] {
            let began = Instant::now();
            for _ in 0..rounds {
                assert_eq!(
                    ids(&indexed, field, limit).await,
                    limit.unwrap_or(count / 4)
                );
            }
            emit(&format!("{prefix}_{suffix}"), rounds, began);
        }
        let began = Instant::now();
        for _ in 0..rounds {
            let mut cursor = indexed
                .aggregate(
                    SchemaUid(SCHEMA),
                    AggregateQuery {
                        selection: eq(field, 0),
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
                    verify(),
                )
                .await
                .unwrap();
            assert_eq!(
                cursor.next().await.unwrap().unwrap().columns[0].1,
                OwnedValue::U64((count / 4) as u64)
            );
        }
        emit(&format!("{prefix}_count"), rounds, began);
    }
    let feature = OwnedValue::U64(0).feature();
    let began = Instant::now();
    for _ in 0..rounds {
        let mut cursor = indexed
            .range_index_scan(
                SchemaUid(SCHEMA),
                hash_str("price"),
                ValueRange {
                    start: ValueRangeTerm::Inclusive(feature),
                    end: ValueRangeTerm::Inclusive(feature),
                },
                vec![],
                Expr::nothing(),
                Expr::nothing(),
                neb::index::ranged::tree::btree::Ordering::Forward,
            )
            .await
            .unwrap();
        let mut hits = 0;
        while let Some(cell) = cursor.next().await.unwrap() {
            assert_eq!(cell[hash_str("price")], OwnedValue::U64(0));
            hits += 1;
        }
        assert_eq!(hits, count / 4);
    }
    emit("range_rows", rounds, began);
    server.shutdown().await;
}
