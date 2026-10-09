//! Write-path and recovery timings for the cell store, for before/after
//! comparisons of any change to the WAL, tombstones or recovery.
//!
//!   neb_perf_probe populate --base DIR [--cells 1000000]
//!   neb_perf_probe recover  --base DIR
//!
//! `populate` starts a fresh durable server (WAL, backup and raft directories
//! under DIR), times single and transaction-batched insert / update / delete,
//! then shuts down gracefully. `recover` is a separate process (so its peak
//! RSS is recovery's own): it times server start on the populated directories
//! and samples the result. Output is `METRIC <name> <value>` lines.

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
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
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
    for (name, phase) in [("batched_insert", 0u8), ("batched_update", 1), ("batched_delete", 2)] {
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
    println!("METRIC shutdown_secs {:.3}", started.elapsed().as_secs_f64());
    println!("METRIC populate_peak_rss_kb {}", peak_rss_kb());
}

async fn recover(base: &str, cells: u64) {
    let addr = format!("127.0.0.1:{}", 47900 + std::process::id() % 100);
    let started = Instant::now();
    let server = NebServer::new_from_opts(&options(base, true), &addr, "perf_probe", async |_| {})
        .await
        .expect("server start");
    println!("METRIC recovery_secs {:.3}", started.elapsed().as_secs_f64());
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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let base = arg(&args, "--base").expect("--base DIR");
    let cells: u64 = arg(&args, "--cells").and_then(|v| v.parse().ok()).unwrap_or(1_000_000);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    match args.get(1).map(String::as_str) {
        Some("populate") => runtime.block_on(populate(&base, cells)),
        Some("recover") => runtime.block_on(recover(&base, cells)),
        _ => eprintln!("usage: neb_perf_probe <populate|recover> --base DIR [--cells N]"),
    }
}
