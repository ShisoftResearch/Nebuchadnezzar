//! What the recovery index reconciliation costs on a store big enough to
//! matter, and what the hash half of it adds.
//!
//! The full `--lib` suite answers "is it correct". It cannot answer "what does
//! it cost", because its stores hold hundreds of cells, not millions. This does,
//! and it prices the two walks separately so the added cost is a measurement
//! rather than an inference.
//!
//! ```text
//! usage: neb_hash_reconcile_cost <dir> <cells> [batch] [lanes]
//!        NEB_HRC_SCHEMA=ranged   to build the same store with no hash index
//!        NEB_HRC_REGIONS=16      to make the second hash field spill
//! ```
//!
//! Phase 1 loads the store and shuts it down cleanly, so phase 2 starts from a
//! state where the cells and both indexes already agree. That is the right shape
//! for a cost question: the reconcile walks the WHOLE store every start, whether
//! or not it finds anything, so a healthy store measures the walk and nothing
//! else. Phase 2 then reports three numbers:
//!
//! 1. `RECONCILE` -- what the startup pass cost, read from the statics the
//!    startup itself writes. This is the number an operator pays.
//! 2. `RANGED_VERIFY` and `HASH_VERIFY` -- the same two walks, timed apart,
//!    read-only, in the same process on the same store. Their difference is what
//!    the hash half adds.
//! 3. `SCHEMA=ranged` -- the same store with no hash-indexed field at all. The
//!    gap between its `RECONCILE` and the hash store's is the same answer from a
//!    different direction, and it is the one that does not depend on the timing
//!    of anything inside the process.
//!
//! A repair pass costs more than these numbers show, by one bucket write per
//! missing entry and one per stale entry. That is the crash window, and the
//! durability campaign measures it at 86 hash entries in 225 restarts, so it is
//! not where the cost lives.

use neb::client::AsyncClient;
use neb::index::builder::IndexBuilder;
use neb::index::scrub::{self, ScrubMode};
use neb::ram::cell::OwnedCell;
use neb::ram::schema::{Field, IndexType, Schema, SchemaVid};
use neb::ram::types::{Id, Map, OwnedMap, OwnedValue, Type};
use neb::server::{NebServer, ServerOptions, Service};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

const SCHEMA_ID: u32 = 9310;
const SKU_FIELD: &str = "sku";
const REGION_FIELD: &str = "region";
const PRICE_FIELD: &str = "price";
/// A second hash-indexed field, with `NEB_HRC_REGIONS` distinct values.
///
/// Its cardinality decides whether the buckets spill into chains, and that is
/// the whole difference between the two cost shapes this harness can measure:
/// at 100,000 values a bucket holds a dozen ids and one read answers for it,
/// while at 16 values a bucket holds tens of thousands and every membership
/// question walks a chain. The cardinality is a knob rather than a constant
/// because the load cost of a spilled bucket is not what is being measured, and
/// a fixed low value makes the load dominate the run.
fn regions() -> u64 {
    static REGION: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *REGION.get_or_init(|| {
        std::env::var("NEB_HRC_REGIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(100_000)
    })
}

fn schema(with_hash: bool) -> Schema {
    let mut fields = vec![Field::new_indexed(
        PRICE_FIELD,
        Type::U64,
        vec![IndexType::Ranged],
    )];
    if with_hash {
        fields.push(Field::new_indexed(
            SKU_FIELD,
            Type::U64,
            vec![IndexType::Hashed],
        ));
        fields.push(Field::new_indexed(
            REGION_FIELD,
            Type::String,
            vec![IndexType::Hashed],
        ));
    }
    Schema::new_with_id(
        SCHEMA_ID,
        "hrc_products",
        None,
        Field::new_schema(fields),
        false,
        false,
    )
}

/// Whether this run builds the hash-indexed shape or the ranged-only one. Read
/// once: the loader asks per cell, and a process-global lookup per million cells
/// is a million allocations spent on a question with one answer.
fn with_hash() -> bool {
    static HASH: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *HASH.get_or_init(|| std::env::var("NEB_HRC_SCHEMA").as_deref() != Ok("ranged"))
}

fn product_cell(i: u64) -> OwnedCell {
    let mut value = OwnedValue::Map(OwnedMap::new());
    value[PRICE_FIELD] = OwnedValue::U64(i % 1_000_003);
    if with_hash() {
        value[SKU_FIELD] = OwnedValue::U64(i);
        value[REGION_FIELD] = OwnedValue::String(format!("region_{}", i % regions()));
    }
    OwnedCell::new_with_id(SchemaVid(SCHEMA_ID), &Id::from_parts(3, i), value)
}

async fn start(
    dir: &Path,
    group: &str,
    addr: &str,
    enable_recovery: bool,
) -> (Arc<NebServer>, Arc<AsyncClient>) {
    for d in ["backup", "wal", "raft"] {
        std::fs::create_dir_all(dir.join(d)).expect("create storage dir");
    }
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: std::env::var("NEB_HRC_CHUNK_MB")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1024)
                * 1024
                * 1024,
            db_size: std::env::var("NEB_HRC_DB_GB")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(8)
                * 1024
                * 1024
                * 1024,
            tiered_config: None,
            backup_storage: Some(dir.join("backup").to_string_lossy().to_string()),
            wal_storage: Some(dir.join("wal").to_string_lossy().to_string()),
            raft_storage: Some(dir.join("raft").to_string_lossy().to_string()),
            index_enabled: true,
            services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
            enable_recovery,
            disable_storage_locks: true,
        },
        &addr.to_string(),
        &group.to_string(),
        async |_| {},
    )
    .await
    .expect("start server");
    let client = Arc::new(
        server
            .data_client(&vec![addr.to_string()])
            .await
            .expect("data client"),
    );
    let _ = client
        .new_schema_with_id(schema(with_hash()))
        .await
        .expect("schema registration");
    (server, client)
}

/// Load `cells` products in batches.
///
/// `upsert_all_cells` is the batch RPC the B-tree flusher already uses: one
/// round trip per batch, with the chunk write and the index maintenance still
/// per cell. Loading one cell per round trip would take longer than the thing
/// being measured.
/// Load one contiguous slice of the key space, in batches.
async fn load_lane(client: Arc<AsyncClient>, from: u64, to: u64, batch: u64) -> u64 {
    let began = Instant::now();
    let mut done = from;
    while done < to {
        let n = std::cmp::min(batch, to - done);
        let page: Vec<OwnedCell> = (done..done + n).map(product_cell).collect();
        let results = client
            .upsert_all_cells(page)
            .await
            .expect("batch upsert rpc");
        for result in results {
            result.expect("batch upsert stopped at a failed cell");
        }
        done += n;
        if done / 200_000 != (done - n) / 200_000 {
            println!(
                "  lane {from}: {done}/{to} in {:.0} s",
                began.elapsed().as_secs_f64()
            );
        }
    }
    done - from
}

/// Load `cells` products across several lanes.
///
/// Lanes are not a throughput trick, they are what makes the load measure the
/// shape the store actually has. A bucket is rewritten whole on every append, so
/// one lane writing a low-cardinality indexed value pays one full head rewrite
/// per cell -- the quadratic the indexer's coalescing latch exists to divide.
/// The latch only folds inserts that are IN FLIGHT together, so a single-lane
/// loader measures a workload no writer produces.
async fn load(client: &Arc<AsyncClient>, cells: u64, batch: usize, lanes: u64) {
    let began = Instant::now();
    let lanes = std::cmp::max(1, lanes);
    let per_lane = cells.div_ceil(lanes);
    let mut tasks = tokio::task::JoinSet::new();
    for lane in 0..lanes {
        let from = lane * per_lane;
        if from >= cells {
            break;
        }
        let to = std::cmp::min(from + per_lane, cells);
        let client = client.clone();
        tasks.spawn(load_lane(client, from, to, batch.max(1) as u64));
    }
    let mut loaded = 0u64;
    while let Some(out) = tasks.join_next().await {
        loaded += out.expect("a load lane panicked");
    }
    println!(
        "LOAD cells={loaded} lanes={lanes} secs={:.1} cells_per_sec={:.0}",
        began.elapsed().as_secs_f64(),
        loaded as f64 / began.elapsed().as_secs_f64().max(0.001),
    );
}

async fn run(dir: &Path, group: &str, cells: u64, batch: usize, lanes: u64, port: u16) {
    use std::sync::atomic::Ordering as AtomicOrdering;

    // ---- phase 1: build the store, and shut it down cleanly ------------------
    let addr = format!("127.0.0.1:{port}");
    let (server, client) = start(dir, group, &addr, false).await;
    load(&client, cells, batch, lanes).await;
    IndexBuilder::await_all_indices().await;
    let healthy = client.scrub_index(false).await.expect("healthy scrub");
    println!("HEALTHY {healthy}");
    assert!(
        healthy.is_clean(),
        "the store this harness builds is not clean before the restart: {healthy}"
    );
    server.shutdown().await;
    drop(client);
    drop(server);

    // ---- phase 2: restart, and price what the startup pass costs -------------
    let addr = format!("127.0.0.1:{}", port + 1);
    let began = Instant::now();
    let (server, _client) = start(dir, group, &addr, true).await;
    let startup = began.elapsed();
    println!(
        "RECONCILE ms={} scanned={} no_indices={} ranged_repaired={} ranged_pruned={} \
         hash_repaired={} hash_pruned={} whole_start_secs={:.2}",
        scrub::LAST_RECONCILE_MS.load(AtomicOrdering::Relaxed),
        scrub::LAST_RECONCILE_SCANNED.load(AtomicOrdering::Relaxed),
        scrub::LAST_RECONCILE_NOIDX.load(AtomicOrdering::Relaxed),
        scrub::LAST_RECONCILE_REPAIRED.load(AtomicOrdering::Relaxed),
        scrub::LAST_RECONCILE_PRUNED.load(AtomicOrdering::Relaxed),
        scrub::LAST_RECONCILE_HASH_REPAIRED.load(AtomicOrdering::Relaxed),
        scrub::LAST_RECONCILE_HASH_PRUNED.load(AtomicOrdering::Relaxed),
        startup.as_secs_f64(),
    );

    // The same two walks, timed apart. Read-only, so they change nothing and can
    // be run in either order against the store the startup just reconciled.
    let indexers = &server.indexer().unwrap().clients;
    let began = Instant::now();
    let ranged = scrub::scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
    let ranged_secs = began.elapsed().as_secs_f64();
    println!("RANGED_VERIFY secs={ranged_secs:.2} {ranged}");

    let began = Instant::now();
    let hashed = scrub::scrub_hash_index(server.chunks(), indexers, ScrubMode::Verify).await;
    let hash_secs = began.elapsed().as_secs_f64();
    println!("HASH_VERIFY secs={hash_secs:.2} {hashed}");

    println!(
        "SPLIT cells={} ranged_secs={:.2} hash_secs={:.2} hash_over_ranged={:.2} \
         hash_entries={} hash_ids={}",
        ranged.cells_scanned,
        ranged_secs,
        hash_secs,
        if ranged_secs > 0.0 {
            hash_secs / ranged_secs
        } else {
            0.0
        },
        hashed.hash_entries_derived,
        hashed.hash_index_ids_scanned,
    );
    assert!(
        ranged.is_clean(),
        "the restarted store's ranged index: {ranged}"
    );
    assert!(
        hashed.is_clean(),
        "the restarted store's hash index: {hashed}"
    );
    server.shutdown().await;
}

fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let Some(dir) = args.get(1) else {
        eprintln!("usage: neb_hash_reconcile_cost <dir> <cells> [batch] [lanes]");
        return std::process::ExitCode::from(64);
    };
    let cells: u64 = args
        .get(2)
        .map(|v| v.parse().expect("cells"))
        .unwrap_or(1_200_000);
    let batch: usize = args.get(3).map(|v| v.parse().unwrap()).unwrap_or(2000);
    let lanes: u64 = args.get(4).map(|v| v.parse().unwrap()).unwrap_or(16);
    let port: u16 = std::env::var("NEB_HRC_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(43100);
    let dir = Path::new(dir.as_str());
    std::fs::create_dir_all(dir).expect("create base dir");
    let group = format!("hrc_{}", std::process::id());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(run(dir, &group, cells, batch, lanes, port));
    println!(
        "DONE cells={cells} schema={} regions={} bucket_capacity={}",
        if with_hash() { "hash+ranged" } else { "ranged" },
        regions(),
        *neb::index::hash::BUCKET_CAPACITY,
    );
    std::process::ExitCode::SUCCESS
}
