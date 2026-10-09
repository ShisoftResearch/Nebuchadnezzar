// Cell write-path throughput, WAL on. General and reusable: it measures what a
// change to the write path costs, not any one feature.
//
//   cargo bench --bench write_path            (or run the built binary directly)
//
// One line per workload, machine-parseable:
//   WP <workload> ops=<n> secs=<s> ops_per_sec=<r>
// and a final `WP peak_rss_kb=<VmHWM>`.
//
// Environment: NEB_WP_DIR (scratch, default a temp dir), NEB_WP_SCALE (multiplier on
// every op count, default 1), NEB_WP_ONLY (comma list of workload names).
//
// The server is configured as production runs: recovery enabled, WAL, backup and
// raft storage set. Workloads use only public APIs present on older revisions, so
// the same file builds against a baseline for A/B comparison.

use std::sync::Arc;
use std::time::Instant;

use dovahkiin::types::*;
use neb::client::AsyncClient;
use neb::ram::cell::OwnedCell;
use neb::ram::schema::{Field, Schema, SchemaVid};
use neb::ram::types::{Id, Type};
use neb::server::{NebServer, ServerOptions, Service};

const SCHEMA_ID: u32 = 7401;
const VAL: &str = "val";
const PAD: &str = "pad";
const LOCALITIES: u64 = 64;

fn cell(seq: u64, version: u64) -> OwnedCell {
    let id = Id::from_parts(seq % LOCALITIES, seq);
    let mut value = OwnedValue::Map(OwnedMap::new());
    value[VAL] = OwnedValue::U64(version);
    value[PAD] = OwnedValue::String(format!("{:0>120}", seq));
    OwnedCell::new_with_id(SchemaVid(SCHEMA_ID), &id, value)
}

fn id_of(seq: u64) -> Id {
    Id::from_parts(seq % LOCALITIES, seq)
}

fn scale() -> u64 {
    std::env::var("NEB_WP_SCALE").ok().and_then(|v| v.parse().ok()).unwrap_or(1)
}

fn report(name: &str, ops: u64, started: Instant) {
    let secs = started.elapsed().as_secs_f64();
    println!("WP {name} ops={ops} secs={secs:.4} ops_per_sec={:.0}", ops as f64 / secs);
}

/// Runs `lanes` concurrent tasks, each over its own disjoint key range.
async fn lanes<F, Fut>(client: &Arc<AsyncClient>, base: u64, per_lane: u64, lanes: u64, f: F) -> Instant
where
    F: Fn(Arc<AsyncClient>, u64, u64) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let started = Instant::now();
    let mut tasks = Vec::new();
    for lane in 0..lanes {
        let (c, f) = (client.clone(), f.clone());
        tasks.push(tokio::spawn(async move { f(c, base + lane * per_lane, per_lane).await }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    started
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    let dir = std::env::var("NEB_WP_DIR").map(std::path::PathBuf::from).unwrap_or_else(|_| {
        std::env::temp_dir().join(format!("neb-write-path-{}", std::process::id()))
    });
    let _ = std::fs::remove_dir_all(&dir);
    let only: Option<Vec<String>> = std::env::var("NEB_WP_ONLY")
        .ok()
        .map(|v| v.split(',').map(str::to_string).collect());
    let wanted = |name: &str| only.as_ref().map_or(true, |o| o.iter().any(|n| n == name));

    let addr = neb::utils::test_port::unique_localhost_addr();
    let group = "write_path_bench".to_string();
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 256 * 1024 * 1024,
            db_size: 8 * 1024 * 1024 * 1024,
            tiered_config: None,
            backup_storage: Some(dir.join("backup").to_string_lossy().into_owned()),
            wal_storage: Some(dir.join("wal").to_string_lossy().into_owned()),
            raft_storage: Some(dir.join("raft").to_string_lossy().into_owned()),
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: true,
            disable_storage_locks: true,
        },
        &addr,
        &group,
        async |_| {},
    )
    .await
    .expect("start server");
    let client = Arc::new(server.data_client(&vec![addr.clone()]).await.expect("client"));
    let schema = Schema::new_with_id(
        SCHEMA_ID,
        "write_path",
        None,
        Field::new_schema(vec![
            Field::new_unindexed(VAL, Type::U64),
            Field::new_unindexed(PAD, Type::String),
        ]),
        false,
        false,
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();

    let s = scale();
    // Disjoint key ranges per workload so no workload sees another's cells.
    let mut next_base = 1_000u64; // Id 0 is invalid
    let mut range = |n: u64| {
        let b = next_base;
        next_base += n + 1_000;
        b
    };

    // ---- single-cell, one task --------------------------------------------------
    let n = 4_000 * s;
    let b = range(n);
    if wanted("insert_single") {
        let t = lanes(&client, b, n, 1, |c, base, n| async move {
            for i in 0..n {
                c.write_cell(cell(base + i, 0)).await.unwrap().unwrap();
            }
        })
        .await;
        report("insert_single", n, t);
    }
    if wanted("update_single") {
        let t = lanes(&client, b, n, 1, |c, base, n| async move {
            for i in 0..n {
                c.update_cell(cell(base + i, 1)).await.unwrap().unwrap();
            }
        })
        .await;
        report("update_single", n, t);
    }
    if wanted("delete_single") {
        let t = lanes(&client, b, n, 1, |c, base, n| async move {
            for i in 0..n {
                c.remove_cell(id_of(base + i)).await.unwrap().unwrap();
            }
        })
        .await;
        report("delete_single", n, t);
    }

    // ---- batched, one task -------------------------------------------------------
    let n = 100_000 * s;
    let b = range(n);
    if wanted("insert_batch256") || wanted("update_batch256") {
        let batch = 256u64;
        for (name, version) in [("insert_batch256", 0u64), ("update_batch256", 1)] {
            if !wanted(name) {
                continue;
            }
            let t = lanes(&client, b, n, 1, move |c, base, n| async move {
                let mut i = 0;
                while i < n {
                    let hi = (i + batch).min(n);
                    let cells: Vec<_> = (i..hi).map(|k| cell(base + k, version)).collect();
                    for r in c.upsert_all_cells(cells).await.unwrap() {
                        r.unwrap();
                    }
                    i = hi;
                }
            })
            .await;
            report(name, n, t);
        }
    }

    // ---- concurrent single-cell writers -----------------------------------------
    for threads in [16u64, 64] {
        let per = 2_000 * s;
        let b = range(per * threads);
        let name = format!("insert_c{threads}");
        if wanted(&name) {
            let t = lanes(&client, b, per, threads, |c, base, n| async move {
                for i in 0..n {
                    c.write_cell(cell(base + i, 0)).await.unwrap().unwrap();
                }
            })
            .await;
            report(&name, per * threads, t);
        }
        let name = format!("update_c{threads}");
        if wanted(&name) {
            let t = lanes(&client, b, per, threads, |c, base, n| async move {
                for i in 0..n {
                    c.update_cell(cell(base + i, 1)).await.unwrap().unwrap();
                }
            })
            .await;
            report(&name, per * threads, t);
        }
        let name = format!("delete_c{threads}");
        if wanted(&name) {
            let t = lanes(&client, b, per, threads, |c, base, n| async move {
                for i in 0..n {
                    c.remove_cell(id_of(base + i)).await.unwrap().unwrap();
                }
            })
            .await;
            report(&name, per * threads, t);
        }
    }

    // ---- transactions: 4 cells per commit ----------------------------------------
    for lanes_n in [1u64, 4] {
        let per = 500 * s;
        let b = range(per * lanes_n * 4);
        let name = format!("txn4_c{lanes_n}");
        if wanted(&name) {
            let t = lanes(&client, b, per * 4, lanes_n, |c, base, n| async move {
                for round in 0..n / 4 {
                    let cells: Vec<OwnedCell> = (0..4).map(|k| cell(base + round * 4 + k, 0)).collect();
                    c.transaction(move |txn| {
                        let cells = cells.clone();
                        async move {
                            for cell in cells {
                                txn.write(cell).await?;
                            }
                            Ok(())
                        }
                    })
                    .await
                    .unwrap();
                }
            })
            .await;
            report(&name, per * lanes_n, t);
        }
    }

    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM")).map(str::to_string))
        .and_then(|l| l.split_whitespace().nth(1).map(str::to_string))
        .unwrap_or_default();
    println!("WP peak_rss_kb={rss}");
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
