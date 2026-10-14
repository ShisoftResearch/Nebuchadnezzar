// Cell write-path throughput, WAL on. General and reusable: it measures what a
// change to the write path costs, not any one feature.
//
//   cargo bench --bench write_path            (or run the built binary directly)
//
// One line per workload, machine-parseable:
//   WP <workload> ops=<n> secs=<s> ops_per_sec=<r>
// and a final `WP peak_rss_kb=<VmHWM>`.
//
// Environment: NEB_WP_DURABLE (0/1), NEB_WP_OPS (cells per row),
// NEB_WP_CONCURRENCY, NEB_WP_BATCHES (comma-separated positive integers),
// NEB_WP_WORKERS (default: largest concurrency),
// NEB_WP_DIR (scratch, default a temp dir), NEB_WP_ONLY (comma list of workload names).
//
// The server is configured as production runs: recovery enabled, WAL, backup and
// raft storage set. Baseline builds use the same workload with configuration and
// counter access adapted to the baseline API.

use std::sync::Arc;
use std::time::Instant;
static DEFAULT_APPEND_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

use dovahkiin::types::*;
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

fn wal_files(server: &NebServer) -> u64 {
    server
        .chunks()
        .list
        .iter()
        .map(|chunk| {
            chunk
                .segs
                .iter_values()
                .filter(|seg| {
                    chunk
                        .file_manager
                        .wal_path(seg.chunk_id, seg.id, seg.seq_id)
                        .is_some_and(|p| std::path::Path::new(&p).exists())
                })
                .count() as u64
        })
        .sum()
}
fn namespace_syncs() -> u64 {
    neb::ram::durability::NAMESPACE_SYNCS.load(std::sync::atomic::Ordering::Relaxed)
}
fn numbers(name: &str, default: &[u64]) -> Vec<u64> {
    std::env::var(name)
        .map(|value| {
            value
                .split(',')
                .map(|n| n.parse().expect("numeric workload option"))
                .collect()
        })
        .unwrap_or_else(|_| default.to_vec())
}
fn metrics() -> [u64; 18] {
    use neb::ram::durability::*;
    use neb::ram::segs::*;
    use std::sync::atomic::Ordering::Relaxed;
    [
        GROUP_SYNCS.load(Relaxed),
        BACKGROUND_WAL_SYNCS.load(Relaxed),
        WAL_SYNCS.load(Relaxed),
        DRAIN_WAL_SYNCS.load(Relaxed),
        FORCED_WAL_SYNCS
            .load(Relaxed)
            .saturating_sub(BACKGROUND_WAL_SYNCS.load(Relaxed))
            .saturating_sub(DRAIN_WAL_SYNCS.load(Relaxed)),
        GROUP_BYTES.load(Relaxed),
        APPEND_NS.load(Relaxed) + DEFAULT_APPEND_NS.load(Relaxed),
        QUEUE_NS.load(Relaxed),
        SYNC_NS.load(Relaxed),
        RELEASE_NS.load(Relaxed),
        WAIT_CALLS.load(Relaxed),
        COMPLETED_WAITS.load(Relaxed),
        BARRIER_NS.load(Relaxed),
        BARRIER_SYNC_NS.load(Relaxed),
        BATCH_RECEIPTS_TAKEN.load(Relaxed),
        WAL_SYNC_SERVICE_NS.load(Relaxed),
        WAL_HEADER_SYNC_NS.load(Relaxed),
        WAL_BYTES.load(Relaxed),
    ]
}

fn syncs() -> u64 {
    use std::sync::atomic::Ordering::Relaxed;
    neb::ram::segs::WAL_SYNCS.load(Relaxed)
        + neb::ram::segs::FORCED_WAL_SYNCS.load(Relaxed)
        + neb::ram::durability::GROUP_SYNCS.load(Relaxed)
}

fn main() {
    let workers = numbers(
        "NEB_WP_WORKERS",
        &[numbers("NEB_WP_CONCURRENCY", &[1, 8, 64, 256])
            .into_iter()
            .max()
            .unwrap()],
    )[0] as usize;
    println!("WP runtime_workers={workers}");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}
async fn run() {
    let dir = std::env::var("NEB_WP_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
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
            db_size: 1024 * 1024 * 1024,
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
    let client = Arc::new(
        server
            .data_client(&vec![addr.clone()])
            .await
            .expect("client"),
    );
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

    let total: u64 = std::env::var("NEB_WP_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8192);
    let durable = std::env::var("NEB_WP_DURABLE").as_deref() == Ok("1");
    let mut next_base = 1000;
    for concurrency in numbers("NEB_WP_CONCURRENCY", &[1, 8, 64]) {
        for batch in numbers("NEB_WP_BATCHES", &[1, 64]) {
            let per_lane = total.div_ceil(concurrency * batch) * batch;
            let ops = per_lane * concurrency;
            let base = next_base;
            next_base += ops + 1000;
            for operation in ["insert", "update", "delete"] {
                let name = format!("{operation}_b{batch}_c{concurrency}");
                if !wanted(&name) {
                    continue;
                }
                server.chunks().sync_all();
                let before = syncs();
                let metrics_before = metrics();
                let headers_before = wal_files(&server);
                let namespace_before = namespace_syncs();
                let started = Instant::now();
                let mut tasks = Vec::new();
                let start_barrier = Arc::new(tokio::sync::Barrier::new(concurrency as usize));
                let active = Arc::new(std::sync::atomic::AtomicU64::new(0));
                let peak = Arc::new(std::sync::atomic::AtomicU64::new(0));
                for lane in 0..concurrency {
                    let client = client.clone();
                    let (start_barrier, active, peak) =
                        (start_barrier.clone(), active.clone(), peak.clone());
                    tasks.push(tokio::spawn(async move {
                        let mut latencies = Vec::with_capacity(per_lane as usize);
                        let base = base + lane * per_lane;
                        start_barrier.wait().await;
                        for offset in (0..per_lane).step_by(batch as usize) {
                            let at = Instant::now();
                            let pending =
                                active.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                            peak.fetch_max(pending, std::sync::atomic::Ordering::Relaxed);
                            if batch > 1 && operation != "delete" {
                                let version = u64::from(operation == "update");
                                let cells = (offset..offset + batch)
                                    .map(|i| cell(base + i, version))
                                    .collect();
                                let append_start = Instant::now();
                                let results = if durable {
                                    client.upsert_all_cells_durable(cells).await
                                } else {
                                    client.upsert_all_cells(cells).await
                                }
                                .unwrap();
                                if !durable {
                                    DEFAULT_APPEND_NS.fetch_add(
                                        append_start.elapsed().as_nanos() as u64,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                }
                                for result in results {
                                    result.unwrap();
                                }
                            } else {
                                // There is no delete-batch RPC. Client batches retain sequential order.
                                for i in offset..offset + batch {
                                    let append_start = Instant::now();
                                    match operation {
                                        "insert" => {
                                            (if durable {
                                                client.write_cell_durable(cell(base + i, 0)).await
                                            } else {
                                                client.write_cell(cell(base + i, 0)).await
                                            })
                                            .unwrap()
                                            .unwrap();
                                        }
                                        "update" => {
                                            (if durable {
                                                client.update_cell_durable(cell(base + i, 1)).await
                                            } else {
                                                client.update_cell(cell(base + i, 1)).await
                                            })
                                            .unwrap()
                                            .unwrap();
                                        }
                                        "delete" => {
                                            (if durable {
                                                client.remove_cell_durable(id_of(base + i)).await
                                            } else {
                                                client.remove_cell(id_of(base + i)).await
                                            })
                                            .unwrap()
                                            .unwrap();
                                        }
                                        _ => unreachable!(),
                                    }
                                    if !durable {
                                        DEFAULT_APPEND_NS.fetch_add(
                                            append_start.elapsed().as_nanos() as u64,
                                            std::sync::atomic::Ordering::Relaxed,
                                        );
                                    }
                                }
                            }
                            latencies.push(at.elapsed().as_nanos() as u64);
                            active.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        latencies
                    }));
                }
                let mut latencies = Vec::new();
                for task in tasks {
                    latencies.extend(task.await.unwrap());
                }
                let secs = started.elapsed().as_secs_f64();
                // Include the eventual drain in sync accounting, outside acknowledgement time.
                server.chunks().sync_all();
                let data_syncs = syncs() - before;
                let after = metrics();
                let delta: Vec<u64> = after
                    .iter()
                    .zip(metrics_before)
                    .map(|(a, b)| a - b)
                    .collect();
                let header_syncs = wal_files(&server) - headers_before;
                let namespace_syncs = namespace_syncs() - namespace_before;
                let count = data_syncs + header_syncs + namespace_syncs;
                latencies.sort_unstable();
                let p50 = latencies[latencies.len() / 2] as f64 / 1000.;
                let p99 =
                    latencies[(latencies.len() * 99 / 100).min(latencies.len() - 1)] as f64 / 1000.;
                println!("WP {name} ops={ops} secs={secs:.6} ops_per_sec={:.3} syncs={count} data_syncs={data_syncs} header_syncs={header_syncs} namespace_syncs={namespace_syncs} writes_per_sync={:.3} p50_us={p50:.3} p99_us={p99:.3} peak_requests={} foreground_syncs={} background_syncs={} inline_syncs={} drain_syncs={} other_syncs={} foreground_bytes={} append_ns={} queue_ns={} sync_ns={} release_ns={} file_waits={} completed_waits={} barrier_ns={} barrier_sync_ns={} batch_receipts={} writes_per_foreground_sync={:.3} other_service_ns={} header_service_ns={} wal_bytes={}",
                    ops as f64 / secs, ops as f64 / count.max(1) as f64, peak.load(std::sync::atomic::Ordering::Relaxed),
                    delta[0], delta[1], delta[2], delta[3], delta[4], delta[5], delta[6], delta[7], delta[8], delta[9], delta[10], delta[11], delta[12], delta[13], delta[14], ops as f64 / delta[0].max(1) as f64, delta[15], delta[16], delta[17]);
            }
        }
    }

    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM"))
                .map(str::to_string)
        })
        .and_then(|l| l.split_whitespace().nth(1).map(str::to_string))
        .unwrap_or_default();
    println!("WP peak_rss_kb={rss}");
    server.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
