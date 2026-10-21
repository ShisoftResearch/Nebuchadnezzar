//! Write-path and recovery timings for the cell store, for before/after
//! comparisons of any change to the WAL, tombstones or recovery.
//!
//!   neb_perf_probe populate --base DIR [--cells 1000000]
//!   neb_perf_probe churn    --base DIR [--cells 1000000] [--rounds 4]
//!   neb_perf_probe recover  --base DIR
//!   neb_perf_probe txn      --base DIR [--txns 20000] [--tasks 16,64] [--workers 4]
//!   neb_perf_probe mix      --base DIR [--txns 20000] [--abort-every 10]
//!   neb_perf_probe abortprof --base DIR [--txns 20000] [--tasks 1] [--decision abort|commit] [--op insert|update|delete]
//!   neb_perf_probe plain    --base DIR [--ops 20000] [--threads 1]
//!   neb_perf_probe heads    --base DIR [--ops 200000] [--threads 1,16]
//!
//! `populate` starts a fresh durable server (WAL, backup and raft directories
//! under DIR), times single and transaction-batched insert / update / delete,
//! then shuts down gracefully. `recover` is a separate process (so its peak
//! RSS is recovery's own): it times server start on the populated directories
//! and samples the result. Each process selects a PID-derived address, so `recover`
//! explicitly restores the sole voter and slot owners at its new address.
//! `txn` drives a participant's whole transaction
//! lifecycle (prepare, apply, commit or abort, end) through a `DataManager`:
//! sequential latency and concurrent throughput. `heads` claims and releases
//! write heads from many threads with no WAL: the head pool's own cost.
//! `plain` measures WAL append throughput and p50/p95/p99 latency for inserts
//! and updates. Run it with `--threads 1` and `--threads 16`.
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

fn batch_counters() -> [u64; 5] {
    use std::sync::atomic::Ordering::Relaxed;
    [
        neb::ram::durability::NAMESPACE_SYNCS.load(Relaxed),
        neb::ram::segs::FORCED_WAL_SYNCS.load(Relaxed),
        neb::ram::segs::ARCHIVE_REFUSALS.load(Relaxed),
        neb::ram::segs::SEAL_RETRIES.load(Relaxed),
        neb::client::TRANSACTION_RETRIES.load(Relaxed),
    ]
}

async fn populate(base: &str, cells: u64) {
    let addr = format!("127.0.0.1:{}", 47900 + std::process::id() % 100);
    let server = NebServer::new_from_opts(&options(base, false), &addr, "perf_probe", async |_| {})
        .await
        .expect("server start");
    server.cleaner().pause();
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
    let batched = std::env::var("NEB_PROBE_BATCHED_CELLS")
        .ok()
        .map(|value| value.parse::<u64>().expect("batched cell count"))
        .unwrap_or(cells / 10);
    let base_index = cells;
    for (name, phase) in [
        ("batched_insert", 0u8),
        ("batched_update", 1),
        ("batched_delete", 2),
    ] {
        let started = Instant::now();
        let counters_before = batch_counters();
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
        let counters_after = batch_counters();
        for (index, key) in [
            "namespace_syncs",
            "forced_wal_syncs",
            "archive_refusals",
            "seal_retries",
            "transaction_retries",
        ]
        .iter()
        .enumerate()
        {
            println!(
                "METRIC {name}_{key} {}",
                counters_after[index] - counters_before[index]
            );
        }
    }
    println!("METRIC pinned_segments {}", chunks.pinned_segments());
    println!("METRIC pinned_bytes {}", chunks.pinned_bytes());

    let bytes_before: usize = chunks
        .list
        .iter()
        .map(|chunk| chunk.total_space.load(std::sync::atomic::Ordering::Acquire))
        .sum();
    let started = Instant::now();
    for chunk in &chunks.list {
        neb::ram::cleaner::Cleaner::clean(chunk, true, true);
    }
    println!("METRIC cleaner_secs {:.6}", started.elapsed().as_secs_f64());
    let bytes_after: usize = chunks
        .list
        .iter()
        .map(|chunk| chunk.total_space.load(std::sync::atomic::Ordering::Acquire))
        .sum();
    println!(
        "METRIC cleaner_reclaimed_bytes {}",
        bytes_before.saturating_sub(bytes_after)
    );
    let started = Instant::now();
    server.shutdown().await;
    println!(
        "METRIC shutdown_secs {:.3}",
        started.elapsed().as_secs_f64()
    );
    println!("METRIC populate_peak_rss_kb {}", peak_rss_kb());
    {
        use neb::ram::segs::*;
        use std::sync::atomic::Ordering::Relaxed;
        println!(
            "METRIC seal_waiting_peak {}",
            SEAL_WAITING_PEAK.load(Relaxed)
        );
        println!("METRIC seal_inline {}", SEAL_INLINE.load(Relaxed));
        println!(
            "METRIC seal_wait_retries {}",
            SEAL_RECLAIM_WAIT_RETRIES.load(Relaxed)
        );
        println!("METRIC seal_retries {}", SEAL_RETRIES.load(Relaxed));
    }
}

/// Fragment live segments, then measure the combine pass separately from writes.
async fn churn(base: &str, cells: u64, rounds: u64) {
    use std::sync::atomic::Ordering::Relaxed;
    let mut opts = options(base, false);
    opts.chunk_size = 2 * 1024 * 1024 * 1024;
    opts.db_size = opts.chunk_size;
    let addr = format!("127.0.0.1:{}", 47900 + std::process::id() % 100);
    let server = NebServer::new_from_opts(&opts, &addr, "churn_probe", async |_| {})
        .await
        .expect("server start");
    server.cleaner().pause();
    server.meta().schemas.register_internal_schema(schema());
    let chunks = server.chunks();
    // The repeat distance exceeds LZ4's dictionary. Compression remains real work.
    let pads: Vec<String> = (0..1024u64)
        .map(|seed| {
            let mut state = seed + 1;
            (0..768)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (b'!' + (state % 90) as u8) as char
                })
                .collect()
        })
        .collect();
    let make_cell = |i, version| {
        let mut cell = cell(i, version);
        cell.data["PAD"] = OwnedValue::String(pads[i as usize % pads.len()].clone());
        cell
    };
    for i in 0..cells {
        chunks.write_cell(&mut make_cell(i, 0)).expect("seed");
    }
    let began = Instant::now();
    let mut cleaner_secs = 0.0;
    let mut write_secs = 0.0;
    let mut operations = 0;
    for round in 0..rounds {
        let started = Instant::now();
        if round != 0 {
            for i in (1..cells).step_by(4) {
                chunks
                    .write_cell(&mut make_cell(i, round))
                    .expect("reinsert");
                operations += 1;
            }
        }
        for i in (0..cells).step_by(2) {
            chunks
                .update_cell(&mut make_cell(i, round + 1))
                .expect("update");
            operations += 1;
        }
        for i in (1..cells).step_by(4) {
            chunks.remove_cell(&id(i)).expect("delete");
            operations += 1;
        }
        write_secs += started.elapsed().as_secs_f64();
        // Finish seal work before the measured combine pass.
        let deadline = Instant::now() + std::time::Duration::from_secs(60);
        while chunks.list.iter().any(|chunk| {
            chunk
                .segs
                .iter_values()
                .any(|segment| !chunk.is_active_head(segment.id) && !segment.is_sealed())
        }) {
            assert!(Instant::now() < deadline, "seal jobs did not finish");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let before = neb::ram::segs::ARCHIVE_COUNT.load(Relaxed);
        let started = Instant::now();
        for chunk in &chunks.list {
            neb::ram::cleaner::Cleaner::clean(chunk, true, true);
        }
        let elapsed = started.elapsed().as_secs_f64();
        cleaner_secs += elapsed;
        let archives = neb::ram::segs::ARCHIVE_COUNT.load(Relaxed) - before;
        println!("METRIC churn_round_{round}_cleaner_secs {elapsed:.6}");
        println!("METRIC churn_round_{round}_combine_archives {archives}");
        assert!(
            archives > 4,
            "the workload did not exercise parallel combine archives"
        );
    }
    println!("METRIC cleaner_secs {cleaner_secs:.6}");
    println!(
        "METRIC churn_write_ops_per_sec {:.3}",
        operations as f64 / write_secs
    );
    println!(
        "METRIC churn_ops_per_sec {:.3}",
        operations as f64 / began.elapsed().as_secs_f64()
    );
    println!(
        "METRIC churn_cleaner_fraction {:.6}",
        cleaner_secs / began.elapsed().as_secs_f64()
    );
    let wrong = (0..cells)
        .step_by((cells / 1000).max(1) as usize)
        .flat_map(|first| first..(first + 4).min(cells))
        .filter(|i| chunks.read_cell(&id(*i)).is_ok() != (*i % 4 != 1))
        .count();
    println!("METRIC sampled_wrong {wrong}");
    assert_eq!(wrong, 0);
    server.shutdown().await;
}

async fn recover(base: &str, cells: u64) {
    let addr = format!("127.0.0.1:{}", 47900 + std::process::id() % 100);
    let started = Instant::now();
    let server = NebServer::restore_from_opts(
        &options(base, true),
        &addr,
        "perf_probe",
        false,
        async |_| {},
    )
    .await
    .expect("server restore");
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

    /// The cell every prepare-time abort collides with.
    const CONFLICT_CELL: u64 = 49_999_999;

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
        let prepared =
            <DataManager as DataSite>::prepare(manager, 0, tid, tid, vec![op], vec![]).await;
        assert!(
            format!("{:?}", prepared.payload).contains("Success"),
            "prepare: {:?}",
            prepared.payload
        );
        let committed = <DataManager as DataSite>::commit(
            manager,
            tid,
            tid,
            vec![CommitOp::Write(cell(index, index))],
        )
        .await;
        assert!(
            format!("{:?}", committed.payload).contains("Success"),
            "apply: {:?}",
            committed.payload
        );
        if abort {
            let aborted = <DataManager as DataSite>::abort(manager, tid, tid).await;
            assert!(
                format!("{:?}", aborted.payload).contains("Success"),
                "abort: {:?}",
                aborted.payload
            );
        }
        let ended = <DataManager as DataSite>::end(manager, tid, tid, false).await;
        assert!(
            format!("{:?}", ended.payload).contains("Success"),
            "end: {:?}",
            ended.payload
        );
        started.elapsed()
    }

    // A transaction that expected the conflict cell to be absent: prepare
    // refuses it, nothing is applied, and the coordinator aborts it.
    async fn one_conflict(
        manager: &Arc<DataManager>,
        hlc: &bifrost::hlc::HlcSource,
    ) -> std::time::Duration {
        let started = Instant::now();
        let tid = hlc.now();
        let op = PrepareOp {
            id: id(CONFLICT_CELL),
            expectation: CellExpectation::Absent,
            intent: PrepareIntent::Write,
        };
        let prepared =
            <DataManager as DataSite>::prepare(manager, 0, tid, tid, vec![op], vec![]).await;
        assert!(
            format!("{:?}", prepared.payload).contains("NotRealizable"),
            "prepare must conflict: {:?}",
            prepared.payload
        );
        <DataManager as DataSite>::abort(manager, tid, tid).await;
        <DataManager as DataSite>::end(manager, tid, tid, false).await;
        started.elapsed()
    }

    one(&manager, &server.hlc, CONFLICT_CELL, false).await;
    let mut next = 50_000_000u64;
    for (name, kind) in [
        ("txn_commit", 0u8),
        ("txn_abort", 1),
        ("txn_prepare_abort", 2),
    ] {
        let run = move |manager: Arc<DataManager>,
                        hlc: Arc<bifrost::hlc::HlcSource>,
                        index: u64| async move {
            match kind {
                2 => one_conflict(&manager, &hlc).await,
                _ => one(&manager, &hlc, index, kind == 1).await,
            }
        };
        // Sequential: latency.
        let mut latencies = Vec::with_capacity(txns as usize);
        let started = Instant::now();
        for i in 0..txns {
            latencies.push(run(manager.clone(), server.hlc.clone(), next + i).await);
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
                        run(manager.clone(), hlc.clone(), first + i).await;
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
async fn plain_writes(base: &str, threads: u64, ops: u64) {
    let addr = format!("127.0.0.1:{}", 47400 + std::process::id() % 100);
    let server =
        NebServer::new_from_opts(&options(base, false), &addr, "plain_probe", async |_| {})
            .await
            .expect("server start");
    server.meta().schemas.register_internal_schema(schema());
    for update in [false, true] {
        let barrier = std::sync::Barrier::new(threads as usize);
        let started = Instant::now();
        let mut latencies = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|thread| {
                    let barrier = &barrier;
                    let server = &server;
                    scope.spawn(move || {
                        let mut latencies = Vec::with_capacity(ops as usize);
                        barrier.wait();
                        for i in 0..ops {
                            let index = thread * ops + i;
                            let mut value = cell(index, index + u64::from(update));
                            let start = Instant::now();
                            if update {
                                server.chunks().update_cell(&mut value)
                            } else {
                                server.chunks().write_cell(&mut value)
                            }
                            .expect("plain write");
                            latencies.push(start.elapsed().as_nanos() as u64);
                        }
                        latencies
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|handle| handle.join().expect("writer"))
                .collect::<Vec<_>>()
        });
        let phase = if update { "update" } else { "insert" };
        rate(&format!("plain_{phase}_t{threads}"), threads * ops, started);
        latencies.sort_unstable();
        for (name, percentile) in [("p50", 50usize), ("p95", 95), ("p99", 99)] {
            let index = (latencies.len() - 1) * percentile / 100;
            println!(
                "METRIC plain_{phase}_t{threads}_{name}_us {:.3}",
                latencies[index] as f64 / 1000.0
            );
        }
    }
    println!("METRIC plain_peak_rss_kb {}", peak_rss_kb());
    server.shutdown().await;
}

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

fn print_histogram(name: &str, sorted_ns: &[u64]) {
    print!("HIST {name} cumulative");
    for upper_ns in [
        100, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000, 500_000,
        1_000_000, 2_000_000, 5_000_000,
    ] {
        let count = sorted_ns.partition_point(|value| *value <= upper_ns);
        print!(" le_{:.1}us={count}", upper_ns as f64 / 1000.0);
    }
    println!(" total={}", sorted_ns.len());
}

/// Per-phase latency distribution and WAL cost of one transaction shape.
///
/// `txn` reports one number per run; a tail needs the distribution of each
/// phase (prepare, apply, abort, end), so this run keeps every latency and
/// prints quantiles. WAL records, bytes, forced syncs and lock waits are
/// divided by the transaction count: they show what one abort costs the log.
async fn abort_profile(base: &str, txns: u64, tasks: u64, abort: bool, operation: &str) {
    use neb::server::transactions::data_site::{DataManager, Service as DataSite};
    use neb::server::transactions::{
        AbortResult, CellExpectation, CommitOp, DMCommitResult, DMPrepareResult, EndResult,
        PrepareIntent, PrepareOp,
    };

    let addr = format!("127.0.0.1:{}", 47600 + std::process::id() % 100);
    let server = NebServer::new_from_opts(&options(base, false), &addr, "perf_probe", async |_| {})
        .await
        .expect("server start");
    server.meta().schemas.register_internal_schema(schema());
    let manager = DataManager::new(server.current_database(), server.hlc.clone());
    assert!(tasks > 0 && txns >= tasks);
    assert!(matches!(operation, "insert" | "update" | "delete"));
    assert!(
        abort || operation != "delete",
        "a delete profile requires abort"
    );
    if operation != "insert" {
        for t in 0..tasks {
            server
                .chunks()
                .write_cell(&mut cell(80_000_000 + t, 0))
                .expect("seed");
        }
        server.chunks().sync_all().expect("sync the store");
    }
    println!(
        "PROFILE operation={operation} decision={}",
        if abort { "abort" } else { "commit" }
    );
    let before = (
        neb::ram::segs::wal_counters(),
        neb::ram::segs::WAL_BYTES.load(std::sync::atomic::Ordering::Relaxed),
        neb::ram::segs::FORCED_WAL_SYNCS.load(std::sync::atomic::Ordering::Relaxed),
    );
    #[cfg(feature = "abort_profile")]
    drop(neb::ram::abort_profile::take());
    let per_task = txns / tasks;
    let started = Instant::now();
    let mut handles = Vec::new();
    for t in 0..tasks {
        let manager = manager.clone();
        let hlc = server.hlc.clone();
        let chunks = server.chunks().clone();
        let operation = operation.to_string();
        handles.push(tokio::spawn(async move {
            let mut phases: [Vec<u64>; 5] = Default::default();
            for i in 0..per_task {
                let index = if operation == "insert" {
                    80_000_000 + t * per_task + i
                } else {
                    80_000_000 + t
                };
                let tid = hlc.now();
                let op = PrepareOp {
                    id: id(index),
                    expectation: if operation == "insert" {
                        CellExpectation::Absent
                    } else {
                        CellExpectation::Present(
                            chunks.head_cell(&id(index)).expect("existing cell").version,
                        )
                    },
                    intent: PrepareIntent::Write,
                };
                let t0 = Instant::now();
                assert_eq!(
                    <DataManager as DataSite>::prepare(&manager, 0, tid, tid, vec![op], vec![])
                        .await
                        .payload,
                    DMPrepareResult::Success
                );
                let t1 = Instant::now();
                let applied = <DataManager as DataSite>::commit(
                    &manager,
                    tid,
                    tid,
                    vec![match operation.as_str() {
                        "update" => CommitOp::Update(cell(index, i)),
                        "delete" => CommitOp::Remove(id(index)),
                        _ => CommitOp::Write(cell(index, index)),
                    }],
                )
                .await
                .payload;
                assert_eq!(applied, DMCommitResult::Success);
                let t2 = Instant::now();
                if abort {
                    assert_eq!(
                        <DataManager as DataSite>::abort(&manager, tid, tid)
                            .await
                            .payload,
                        AbortResult::Success(None)
                    );
                }
                let t3 = Instant::now();
                assert_eq!(
                    <DataManager as DataSite>::end(&manager, tid, tid, false)
                        .await
                        .payload,
                    EndResult::Success
                );
                let t4 = Instant::now();
                let ns = |a: Instant, b: Instant| (b - a).as_nanos() as u64;
                phases[0].push(ns(t0, t1));
                phases[1].push(ns(t1, t2));
                phases[2].push(ns(t2, t3));
                phases[3].push(ns(t3, t4));
                phases[4].push(ns(t0, t4));
            }
            phases
        }));
    }
    let mut all: [Vec<u64>; 5] = Default::default();
    for handle in handles {
        for (merged, part) in all.iter_mut().zip(handle.await.expect("task")) {
            merged.extend(part);
        }
    }
    let total = tasks * per_task;
    rate(&format!("prof_t{tasks}"), total, started);
    let after = (
        neb::ram::segs::wal_counters(),
        neb::ram::segs::WAL_BYTES.load(std::sync::atomic::Ordering::Relaxed),
        neb::ram::segs::FORCED_WAL_SYNCS.load(std::sync::atomic::Ordering::Relaxed),
    );
    let delta = after.0.minus(&before.0);
    let per = |v: u64| v as f64 / total as f64;
    println!(
        "METRIC prof_t{tasks}_wal_records_per_txn {:.2}",
        per(delta.writes)
    );
    println!(
        "METRIC prof_t{tasks}_wal_bytes_per_txn {:.1}",
        per(after.1 - before.1)
    );
    println!(
        "METRIC prof_t{tasks}_forced_syncs_per_txn {:.2}",
        per(after.2 - before.2)
    );
    println!(
        "METRIC prof_t{tasks}_lock_wait_ns_per_txn {:.0}",
        per(delta.wait_nanos)
    );
    println!(
        "METRIC prof_t{tasks}_lock_held_ns_per_txn {:.0}",
        per(delta.held_nanos)
    );
    println!(
        "METRIC prof_t{tasks}_lock_contended_per_txn {:.3}",
        per(delta.contended)
    );
    for (name, values) in ["prepare", "apply", "abort", "end", "total"]
        .iter()
        .zip(all.iter_mut())
    {
        values.sort_unstable();
        let q = |p: f64| {
            values[((values.len() as f64 * p) as usize).min(values.len() - 1)] as f64 / 1000.0
        };
        println!(
            "METRIC prof_t{tasks}_{name}_us p50={:.1} p90={:.1} p99={:.1} p999={:.1} max={:.1}",
            q(0.5),
            q(0.9),
            q(0.99),
            q(0.999),
            values[values.len() - 1] as f64 / 1000.0
        );
        print_histogram(&format!("prof_t{tasks}_{name}_us"), values);
    }
    #[cfg(feature = "abort_profile")]
    for (phase, mut values) in neb::ram::abort_profile::take() {
        if values.is_empty() {
            continue;
        }
        values.sort_unstable();
        let q = |p: f64| {
            values[((values.len() as f64 * p) as usize).min(values.len() - 1)] as f64 / 1000.0
        };
        println!(
            "METRIC internal_{phase:?}_us n={} p50={:.3} p90={:.3} p99={:.3} p999={:.3} max={:.3}",
            values.len(),
            q(0.5),
            q(0.9),
            q(0.99),
            q(0.999),
            q(1.0)
        );
        print_histogram(&format!("internal_{phase:?}_us"), &values);
    }
    server.shutdown().await;
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
        <DataManager as DataSite>::prepare(&manager, 0, tid, tid, vec![op], vec![]).await;
        <DataManager as DataSite>::commit(
            &manager,
            tid,
            tid,
            vec![CommitOp::Write(cell(index, index))],
        )
        .await;
        if abort_every > 0 && i % abort_every == 0 {
            <DataManager as DataSite>::abort(&manager, tid, tid).await;
        }
        <DataManager as DataSite>::end(&manager, tid, tid, false).await;
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
        Some("churn") => {
            let rounds = arg(&args, "--rounds").map(|v| v.parse().expect("rounds")).unwrap_or(4);
            runtime.block_on(churn(&base, cells, rounds));
        }
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
        Some("abortprof") => {
            let txns: u64 = arg(&args, "--txns")
                .and_then(|v| v.parse().ok())
                .unwrap_or(20_000);
            let tasks: u64 = arg(&args, "--tasks")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1);
            let abort = arg(&args, "--decision").as_deref() != Some("commit");
            let operation = arg(&args, "--op").unwrap_or_else(|| "insert".to_string());
            runtime.block_on(abort_profile(&base, txns, tasks, abort, &operation))
        }
        Some("plain") => {
            let ops = arg(&args, "--ops")
                .and_then(|v| v.parse().ok())
                .unwrap_or(20_000);
            let threads = arg(&args, "--threads")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1);
            runtime.block_on(plain_writes(&base, threads, ops));
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
            "usage: neb_perf_probe <populate|recover|churn|txn|mix|plain|heads> --base DIR [--cells N] \
             [--txns N --tasks 16,64 --workers W] [--ops N --threads 1,16]"
        ),
    }
}
