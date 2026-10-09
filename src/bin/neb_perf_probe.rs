//! Write-path and recovery timings for the cell store, for before/after
//! comparisons of any change to the WAL, tombstones or recovery.
//!
//!   neb_perf_probe populate --base DIR [--cells 1000000]
//!   neb_perf_probe recover  --base DIR
//!   neb_perf_probe txn      --base DIR [--txns 20000] [--tasks 16,64] [--workers 4]
//!   neb_perf_probe mix      --base DIR [--txns 20000] [--abort-every 10]
//!   neb_perf_probe heads    --base DIR [--ops 200000] [--threads 1,16]
//!
//! `populate` starts a fresh durable server (WAL, backup and raft directories
//! under DIR), times single and transaction-batched insert / update / delete,
//! then shuts down gracefully. `recover` is a separate process (so its peak
//! RSS is recovery's own): it times server start on the populated directories
//! and samples the result. `txn` drives a participant's whole transaction
//! lifecycle (prepare, apply, commit or abort, end) through a `DataManager`:
//! sequential latency and concurrent throughput. `heads` claims and releases
//! write heads from many threads with no WAL: the head pool's own cost.
//! Output is `METRIC <name> <value>` lines.

use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::*;
use neb::ram::cell::OwnedCell;
use neb::ram::schema::{Field, Schema, SchemaVid};
use neb::ram::types::Type;
use neb::server::{NebServer, ServerOptions, Service};
use std::sync::Arc;
use std::time::Instant;

const SCHEMA: u32 = 9401;
const BATCH: u64 = 100;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn options(base: &str, recover: bool) -> ServerOptions {
    let dir = std::path::Path::new(base);
    for sub in ["backup", "wal", "raft"] {
        std::fs::create_dir_all(dir.join(sub)).expect("create dirs");
    }
    ServerOptions {
        chunk_size: 256 * 1024 * 1024,
        db_size: 8 * 1024 * 1024 * 1024,
        tiered_config: None,
        backup_storage: Some(dir.join("backup").to_string_lossy().into_owned()),
        wal_storage: Some(dir.join("wal").to_string_lossy().into_owned()),
        raft_storage: Some(dir.join("raft").to_string_lossy().into_owned()),
        index_enabled: false,
        services: vec![Service::Cell, Service::Transaction],
        enable_recovery: recover,
        disable_storage_locks: true,
    }
}

fn schema() -> Schema {
    Schema::new_with_id(
        SCHEMA,
        "perf_probe",
        None,
        Field::new_schema(vec![
            Field::new_unindexed("DATA", Type::U64),
            Field::new_unindexed("PAD", Type::String),
        ]),
        false,
        false,
    )
}

fn cell(index: u64, data: u64) -> OwnedCell {
    let mut value = OwnedValue::Map(OwnedMap::new());
    value["DATA"] = OwnedValue::U64(data);
    value["PAD"] = OwnedValue::String(format!("{index:0>64}"));
    OwnedCell::new_with_id(SchemaVid(SCHEMA), &id(index), value)
}

fn id(index: u64) -> Id {
    Id::allocated((index % 8) as u16, 5, index + 1)
}

fn peak_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

fn rate(name: &str, ops: u64, started: Instant) {
    let secs = started.elapsed().as_secs_f64();
    println!("METRIC {name}_ops_per_sec {:.0}", ops as f64 / secs);
}

async fn populate(base: &str, cells: u64) {
    let addr = format!("127.0.0.1:{}", 47900 + std::process::id() % 100);
    let server = NebServer::new_from_opts(&options(base, false), &addr, "perf_probe", async |_| {})
        .await
        .expect("server start");
    server.meta().schemas.register_internal_schema(schema());
    let client = Arc::new(server.data_client(&vec![addr]).await.expect("data client"));
    let chunks = server.chunks();

    // Single operations on the cell store directly: the engine's own cost.
    let started = Instant::now();
    for i in 0..cells {
        chunks.write_cell(&mut cell(i, i)).expect("insert");
    }
    rate("single_insert", cells, started);
    let started = Instant::now();
    for i in 0..cells / 2 {
        chunks.update_cell(&mut cell(i, i + 1)).expect("update");
    }
    rate("single_update", cells / 2, started);
    let started = Instant::now();
    for i in 0..cells / 4 {
        chunks.remove_cell(&id(i)).expect("delete");
    }
    rate("single_delete", cells / 4, started);

    // Batched: BATCH cells per transaction, on a disjoint id range, so the
    // bracketed write path and its recovery are exercised too.
    let batched = cells / 10;
    let base_index = cells;
    for (name, phase) in [
        ("batched_insert", 0u8),
        ("batched_update", 1),
        ("batched_delete", 2),
    ] {
        let started = Instant::now();
        let mut done = 0u64;
        let mut at = 0u64;
        while at < batched {
            let n = BATCH.min(batched - at);
            let first = base_index + at;
            client
                .transaction(move |txn| async move {
                    for i in first..first + n {
                        match phase {
                            0 => txn.write(cell(i, i)).await?,
                            1 => txn.update(cell(i, i + 1)).await?,
                            _ => txn.remove(id(i)).await?,
                        }
                    }
                    Ok(())
                })
                .await
                .expect("batched transaction");
            at += n;
            done += n;
            if phase == 2 && at >= batched / 2 {
                break; // leave half the batched range live
            }
        }
        rate(name, done, started);
    }

    let started = Instant::now();
    server.shutdown().await;
    println!(
        "METRIC shutdown_secs {:.3}",
        started.elapsed().as_secs_f64()
    );
    println!("METRIC populate_peak_rss_kb {}", peak_rss_kb());
}

async fn recover(base: &str, cells: u64) {
    let addr = format!("127.0.0.1:{}", 47900 + std::process::id() % 100);
    let started = Instant::now();
    let server = NebServer::new_from_opts(&options(base, true), &addr, "perf_probe", async |_| {})
        .await
        .expect("server start");
    println!(
        "METRIC recovery_secs {:.3}",
        started.elapsed().as_secs_f64()
    );
    println!("METRIC recover_peak_rss_kb {}", peak_rss_kb());
    server.meta().schemas.register_internal_schema(schema());
    // Sanity, not a benchmark: single ranges are deleted / live as populated.
    let live = |i: u64| server.chunks().read_cell(&id(i)).is_ok();
    let step = (cells / 1000).max(1);
    let wrong = (0..cells)
        .step_by(step as usize)
        .filter(|i| live(*i) != (*i >= cells / 4))
        .count();
    println!("METRIC sampled_wrong {wrong}");
    server.shutdown().await;
}

/// The participant's transaction lifecycle, driven directly through a
/// `DataManager`: prepare, apply, then the decision. One new cell per
/// transaction. Sequential runs report latency; concurrent runs throughput.
async fn txn_lifecycle(base: &str, txns: u64, task_counts: &[u64]) {
    use neb::server::transactions::data_site::{DataManager, Service as DataSite};
    use neb::server::transactions::{CellExpectation, CommitOp, PrepareIntent, PrepareOp};

    let addr = format!("127.0.0.1:{}", 47800 + std::process::id() % 100);
    let server = NebServer::new_from_opts(&options(base, false), &addr, "perf_probe", async |_| {})
        .await
        .expect("server start");
    server.meta().schemas.register_internal_schema(schema());
    let manager = DataManager::new(server.current_database(), server.hlc.clone());

    // One transaction; `abort` picks the decision. Returns its latency.
    async fn one(
        manager: &Arc<DataManager>,
        hlc: &bifrost::hlc::HlcSource,
        index: u64,
        abort: bool,
    ) -> std::time::Duration {
        let started = Instant::now();
        let tid = hlc.now();
        let op = PrepareOp {
            id: id(index),
            expectation: CellExpectation::Absent,
            intent: PrepareIntent::Write,
        };
        let prepared = DataSite::prepare(manager, 0, tid, tid, vec![op], vec![]).await;
        assert!(
            format!("{:?}", prepared.payload).contains("Success"),
            "prepare: {:?}",
            prepared.payload
        );
        let committed =
            DataSite::commit(manager, tid, tid, vec![CommitOp::Write(cell(index, index))]).await;
        assert!(
            format!("{:?}", committed.payload).contains("Success"),
            "apply: {:?}",
            committed.payload
        );
        if abort {
            let aborted = DataSite::abort(manager, tid, tid).await;
            assert!(
                format!("{:?}", aborted.payload).contains("Success"),
                "abort: {:?}",
                aborted.payload
            );
        }
        let ended = DataSite::end(manager, tid, tid).await;
        assert!(
            format!("{:?}", ended.payload).contains("Success"),
            "end: {:?}",
            ended.payload
        );
        started.elapsed()
    }

    let mut next = 50_000_000u64;
    for (name, abort) in [("txn_commit", false), ("txn_abort", true)] {
        // Sequential: latency.
        let mut latencies = Vec::with_capacity(txns as usize);
        let started = Instant::now();
        for i in 0..txns {
            latencies.push(one(&manager, &server.hlc, next + i, abort).await);
        }
        next += txns;
        rate(&format!("{name}_seq"), txns, started);
        latencies.sort_unstable();
        let pct =
            |p: f64| latencies[((latencies.len() as f64 * p) as usize).min(latencies.len() - 1)];
        println!("METRIC {name}_p50_us {}", pct(0.50).as_micros());
        println!("METRIC {name}_p99_us {}", pct(0.99).as_micros());

        // Concurrent: throughput.
        for &tasks in task_counts {
            let started = Instant::now();
            let per_task = txns / tasks;
            let mut handles = Vec::new();
            for t in 0..tasks {
                let manager = manager.clone();
                let hlc = server.hlc.clone();
                let first = next + t * per_task;
                handles.push(tokio::spawn(async move {
                    for i in 0..per_task {
                        one(&manager, &hlc, first + i, abort).await;
                    }
                }));
            }
            for handle in handles {
                handle.await.expect("task");
            }
            next += tasks * per_task;
            rate(&format!("{name}_conc{tasks}"), tasks * per_task, started);
        }
    }
    server.shutdown().await;
    println!("METRIC txn_peak_rss_kb {}", peak_rss_kb());
}

/// Head acquire/release under contention, with no WAL and no cells: the
/// claim, the per-entry ownership window and its release, nothing else.
fn heads(threads: u64, ops: u64) {
    let bytes = (threads * ops * 64) as usize;
    let size = (bytes + 64 * 1024 * 1024).next_power_of_two();
    let chunks = neb::ram::chunk::Chunks::new_dummy(1, size);
    let started = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            let chunks = &chunks;
            scope.spawn(move || {
                let chunk = &chunks.list[0];
                for _ in 0..ops {
                    drop(chunk.try_acquire(64, false).expect("claim"));
                }
            });
        }
    });
    rate(&format!("head_claim_t{threads}"), threads * ops, started);
}

/// Space under an abort-heavy load: `txns` one-cell transactions, every
/// `1/abort_every`-th aborted after apply. Reports the store's segments, used
/// and dead bytes right after the load, and again after a full cleaner pass,
/// so an abort that leaves space behind shows up as growth the cleaner must
/// (and does, or does not) take back.
async fn abort_mix(base: &str, txns: u64, abort_every: u64) {
    use neb::server::transactions::data_site::{DataManager, Service as DataSite};
    use neb::server::transactions::{CellExpectation, CommitOp, PrepareIntent, PrepareOp};

    let addr = format!("127.0.0.1:{}", 47700 + std::process::id() % 100);
    let server = NebServer::new_from_opts(&options(base, false), &addr, "perf_probe", async |_| {})
        .await
        .expect("server start");
    server.meta().schemas.register_internal_schema(schema());
    let manager = DataManager::new(server.current_database(), server.hlc.clone());
    let started = Instant::now();
    for i in 0..txns {
        let tid = server.hlc.now();
        let index = 70_000_000 + i;
        let op = PrepareOp {
            id: id(index),
            expectation: CellExpectation::Absent,
            intent: PrepareIntent::Write,
        };
        DataSite::prepare(&manager, 0, tid, tid, vec![op], vec![]).await;
        DataSite::commit(
            &manager,
            tid,
            tid,
            vec![CommitOp::Write(cell(index, index))],
        )
        .await;
        if abort_every > 0 && i % abort_every == 0 {
            DataSite::abort(&manager, tid, tid).await;
        }
        DataSite::end(&manager, tid, tid).await;
    }
    rate(&format!("mix_abort_every{abort_every}"), txns, started);
    let report = |stage: &str| {
        let (mut segments, mut used, mut dead) = (0u64, 0u64, 0u64);
        for chunk in server.chunks().list.iter() {
            for seg in chunk.segments() {
                segments += 1;
                used += seg.used_spaces() as u64;
                dead += seg.total_dead_space() as u64;
            }
        }
        println!("METRIC mix_{stage}_segments {segments}");
        println!("METRIC mix_{stage}_used_bytes {used}");
        println!("METRIC mix_{stage}_dead_bytes {dead}");
    };
    report("loaded");
    for chunk in server.chunks().list.iter() {
        neb::ram::cleaner::Cleaner::clean(chunk, true, true);
    }
    report("cleaned");
    server.shutdown().await;
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let base = arg(&args, "--base").expect("--base DIR");
    let cells: u64 = arg(&args, "--cells")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let workers: usize = arg(&args, "--workers")
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();
    match args.get(1).map(String::as_str) {
        Some("populate") => runtime.block_on(populate(&base, cells)),
        Some("recover") => runtime.block_on(recover(&base, cells)),
        Some("txn") => {
            let txns: u64 = arg(&args, "--txns")
                .and_then(|v| v.parse().ok())
                .unwrap_or(20_000);
            let tasks: Vec<u64> = arg(&args, "--tasks")
                .unwrap_or_else(|| "16,64".to_string())
                .split(',')
                .filter_map(|v| v.parse().ok())
                .collect();
            runtime.block_on(txn_lifecycle(&base, txns, &tasks))
        }
        Some("mix") => {
            let txns: u64 = arg(&args, "--txns")
                .and_then(|v| v.parse().ok())
                .unwrap_or(20_000);
            let every: u64 = arg(&args, "--abort-every")
                .and_then(|v| v.parse().ok())
                .unwrap_or(10);
            runtime.block_on(abort_mix(&base, txns, every))
        }
        Some("heads") => {
            let ops: u64 = arg(&args, "--ops")
                .and_then(|v| v.parse().ok())
                .unwrap_or(200_000);
            for threads in arg(&args, "--threads")
                .unwrap_or_else(|| "1,16".to_string())
                .split(',')
                .filter_map(|v| v.parse().ok())
            {
                heads(threads, ops);
            }
        }
        _ => eprintln!(
            "usage: neb_perf_probe <populate|recover|txn|mix|heads> --base DIR [--cells N] \
             [--txns N --tasks 16,64 --workers W] [--ops N --threads 1,16]"
        ),
    }
}
