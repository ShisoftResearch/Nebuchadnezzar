//! Measure indexed writes with a durable store and concurrent RPC clients.
//!
//! `NEB_IWP_DIR` names a fresh scratch directory.
//! `NEB_IWP_COUNT` sets the operations per writer (default: 2000).
//! `NEB_IWP_ONLY` selects comma-separated rows, such as `update_1,delete_16`.
//! `compare_1,compare_16` uses successful CAS on distinct seeded cells.
//! `NEB_IWP_INDEXES` selects none, hashed, or mixed (ranged and hashed).
//! Output includes throughput, latency percentiles, and peak RSS.
use dovahkiin::types::*;
use futures::FutureExt;
use neb::index::ranged::tree::btree::storage::INDEX_PAGE_IMAGES_WRITTEN;
use neb::ram::{
    chunk::{ALLOCATION_EXHAUSTED, DEAD_BYTES_PRODUCED},
    cleaner::CLEANER_RECLAIMED_BYTES,
};
use neb::{
    client::AsyncClient,
    ram::{
        cell::OwnedCell,
        schema::{Field, IndexType, Schema, SchemaVid},
    },
    server::{NebServer, ServerOptions, Service},
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

fn counters() -> [u64; 4] {
    [
        INDEX_PAGE_IMAGES_WRITTEN.load(Ordering::Relaxed),
        DEAD_BYTES_PRODUCED.load(Ordering::Relaxed),
        CLEANER_RECLAIMED_BYTES.load(Ordering::Relaxed),
        ALLOCATION_EXHAUSTED.load(Ordering::Relaxed),
    ]
}
fn extended_counters() -> [u64; 8] {
    [
        neb::ram::cleaner::CAS_FULL_PASSES.load(Ordering::Relaxed),
        neb::ram::cleaner::CAS_RECLAIMED_BYTES.load(Ordering::Relaxed),
        neb::ram::segs::ARCHIVE_REFUSALS.load(Ordering::Relaxed),
        neb::ram::segs::SEAL_RETRIES.load(Ordering::Relaxed),
        neb::ram::durability::NAMESPACE_SYNCS.load(Ordering::Relaxed),
        neb::ram::segs::FORCED_WAL_SYNCS.load(Ordering::Relaxed),
        neb::client::TRANSACTION_RETRIES.load(Ordering::Relaxed),
        neb::ram::chunk::ROTATIONS.load(Ordering::Relaxed),
    ]
}

const SCHEMA: u32 = 7402;
const SEED_VERSION: u64 = 2;
fn cell(id: u64, value: u64) -> OwnedCell {
    let mut data = OwnedValue::Map(OwnedMap::new());
    data["value"] = OwnedValue::U64(value);
    OwnedCell::new_with_id(SchemaVid(SCHEMA), &Id::from_parts(3, id), data)
}
async fn phase(
    client: Arc<AsyncClient>,
    kind: &str,
    writers: u64,
    count: u64,
    hot: bool,
    measured: bool,
) {
    let before = counters();
    let extended_before = extended_counters();
    if measured {
        println!("IWP phase_begin {kind}_{writers}{} page_images={} dead_bytes={} cleaner_reclaimed_bytes={} store_full_refusals={}",
            if hot { "_hot" } else { "" }, before[0], before[1], before[2], before[3]);
    }
    let started = Instant::now();
    let cas_conflicts = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for lane in 0..writers {
        let client = client.clone();
        let kind = kind.to_owned();
        let cas_conflicts = cas_conflicts.clone();
        tasks.push(tokio::spawn(async move {
            let mut times = Vec::new();
            for seq in 0..count {
                let id = if hot {
                    64_000_000 + seq % 4
                } else {
                    writers * 1_000_000 + lane * count + seq
                };
                let start = Instant::now();
                match kind.as_str() {
                    "insert" => {
                        let seeded = client.write_cell(cell(id, 0)).await.unwrap().unwrap();
                        assert_eq!(seeded.version, SEED_VERSION);
                    }
                    "update" => {
                        client
                            .update_cell(cell(id, 1 + seq % 8))
                            .await
                            .unwrap()
                            .unwrap();
                    }
                    "compare" => {
                        let result = client
                            .compare_version_and_update_cell(
                                Id::from_parts(3, id),
                                SEED_VERSION,
                                cell(id, 1 + seq % 8),
                            )
                            .await
                            .unwrap();
                        if matches!(
                            &result,
                            Err(neb::ram::cell::WriteError::CellVersionMismatch)
                        ) {
                            cas_conflicts.fetch_add(1, Ordering::Relaxed);
                        }
                        result.unwrap();
                    }
                    "delete" => {
                        client
                            .remove_cell(Id::from_parts(3, id))
                            .await
                            .unwrap()
                            .unwrap();
                    }
                    _ => unreachable!(),
                }
                times.push(start.elapsed().as_nanos() as u64);
            }
            times
        }));
    }
    let mut times = Vec::new();
    for task in tasks {
        times.extend(task.await.unwrap());
    }
    let secs = started.elapsed().as_secs_f64();
    times.sort_unstable();
    if measured {
        let after = counters();
        let extended_after = extended_counters();
        let delta: Vec<_> = extended_after
            .iter()
            .zip(extended_before)
            .map(|(after, before)| after - before)
            .collect();
        println!("IWP_COUNTERS {kind}_{writers}{} cas_full_passes={} cas_reclaimed_bytes={} archive_refusals={} seal_retries={} namespace_syncs={} forced_wal_syncs={} transaction_retries={} rotations={}",
            if hot { "_hot" } else { "" }, delta[0], delta[1], delta[2], delta[3], delta[4], delta[5], delta[6], delta[7]);
        println!("IWP {kind}_{writers}{} ops={} secs={secs:.6} ops_per_sec={:.3} p50_us={:.3} p95_us={:.3} p99_us={:.3} page_images_per_sec={:.3} dead_bytes={} cleaner_reclaimed_bytes={} store_full_refusals={} cas_conflicts={}",
        if hot { "_hot" } else { "" }, times.len(), times.len() as f64 / secs,
        times[times.len()/2] as f64 / 1000.0, times[times.len()*95/100] as f64 / 1000.0,
        times[times.len()*99/100] as f64 / 1000.0,
        (after[0] - before[0]) as f64 / secs, after[1] - before[1], after[2] - before[2], after[3] - before[3], cas_conflicts.load(Ordering::Relaxed));
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    let limit = std::env::var("NEB_IWP_TIME_LIMIT_SECS")
        .map(|v| v.parse::<u64>().expect("invalid benchmark time limit"))
        .unwrap_or(300);
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watchdog_finished = finished.clone();
    std::thread::spawn(move || {
        let started = Instant::now();
        loop {
            std::thread::sleep(Duration::from_secs(1));
            if watchdog_finished.load(Ordering::Acquire) {
                break;
            }
            let elapsed = started.elapsed().as_secs();
            if elapsed % 5 == 0 || elapsed >= limit {
                let totals = counters();
                println!("IWP progress elapsed_secs={elapsed} page_images={} dead_bytes={} cleaner_reclaimed_bytes={} store_full_refusals={}",
                    totals[0], totals[1], totals[2], totals[3]);
            }
            if elapsed >= limit {
                eprintln!("IWP FAILURE watchdog_time_limit_secs={limit}");
                std::process::exit(124);
            }
        }
    });
    let outcome =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(limit), run()))
            .catch_unwind()
            .await;
    finished.store(true, Ordering::Release);
    let totals = counters();
    println!(
        "IWP total page_images={} dead_bytes={} cleaner_reclaimed_bytes={} store_full_refusals={}",
        totals[0], totals[1], totals[2], totals[3]
    );
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            eprintln!("IWP FAILURE time_limit_secs={limit}");
            std::process::exit(124);
        }
        Err(_) => {
            eprintln!("IWP FAILURE panic");
            std::process::exit(101);
        }
    }
}

async fn run() {
    let count: u64 = std::env::var("NEB_IWP_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let dir = std::path::PathBuf::from(std::env::var("NEB_IWP_DIR").expect("NEB_IWP_DIR"));
    std::fs::create_dir_all(&dir).unwrap();
    let mode = std::env::var("NEB_IWP_MODE").unwrap_or_else(|_| "writes".into());
    let startup_cells = std::env::var("NEB_IWP_STARTUP_COUNT")
        .map(|v| v.parse::<u64>().expect("invalid startup cell count"))
        .unwrap_or(1_200_000);
    let startup = mode != "writes";
    let addr = if startup {
        "127.0.0.1:39381".to_owned()
    } else {
        neb::utils::test_port::unique_localhost_addr()
    };
    let indexes = if startup {
        vec![IndexType::Ranged]
    } else {
        match std::env::var("NEB_IWP_INDEXES").as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("mixed") => {
                vec![IndexType::Ranged, IndexType::Hashed]
            }
            Ok("none") => vec![],
            Ok("hashed") => vec![IndexType::Hashed],
            other => panic!("invalid index mode: {other:?}"),
        }
    };
    let indexed = !indexes.is_empty();
    let began = Instant::now();
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: if startup {
                512 * 1024 * 1024
            } else {
                128 * 1024 * 1024
            },
            db_size: if startup {
                16 * 1024 * 1024 * 1024
            } else {
                512 * 1024 * 1024
            },
            tiered_config: None,
            backup_storage: Some(dir.join("backup").to_string_lossy().into()),
            wal_storage: Some(dir.join("wal").to_string_lossy().into()),
            raft_storage: Some(dir.join("raft").to_string_lossy().into()),
            index_enabled: indexed,
            services: vec![
                Service::Cell,
                Service::Query,
                Service::Transaction,
                Service::RangedIndexer,
            ],
            enable_recovery: true,
            disable_storage_locks: true,
        },
        &addr,
        &"index_write_path".to_string(),
        async |_| {},
    )
    .await
    .unwrap();
    if mode == "startup" {
        println!(
            "IWP startup cells={startup_cells} secs={:.6}",
            began.elapsed().as_secs_f64()
        );
        server.shutdown().await;
        return;
    }
    let client = Arc::new(server.data_client(&vec![addr]).await.unwrap());
    let schema = Schema::new_with_id(
        SCHEMA,
        "indexed_write_path",
        None,
        Field::new_schema(vec![Field::new_indexed("value", Type::U64, indexes)]),
        false,
        true,
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();
    if mode == "populate" {
        assert_eq!(startup_cells % 16, 0);
        phase(client, "insert", 16, startup_cells / 16, false, false).await;
        server.shutdown().await;
        println!("IWP populated cells={startup_cells}");
        return;
    }
    assert_eq!(mode, "writes", "unknown benchmark mode");
    let only = std::env::var("NEB_IWP_ONLY").ok();
    let wanted = |row: &str| {
        only.as_ref()
            .is_none_or(|list| list.split(',').any(|item| item == row))
    };
    if wanted("read_rotation") && only.is_some() {
        phase(client.clone(), "insert", 16, count, false, false).await;
        assert!(neb::index::ranged::tree::btree::storage::wait_until_updated().await);
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let read_finished = finished.clone();
        let reader = client.clone();
        let reads = tokio::spawn(async move {
            let mut samples = Vec::new();
            let mut sequence = 0;
            while !read_finished.load(Ordering::Acquire) {
                let started = Instant::now();
                reader
                    .read_cell(Id::from_parts(3, 16_000_000 + sequence % count))
                    .await
                    .unwrap()
                    .unwrap();
                samples.push(started.elapsed().as_nanos() as u64);
                sequence += 1;
                tokio::task::yield_now().await;
            }
            samples
        });
        let rotations = neb::ram::chunk::ROTATIONS.load(Ordering::Relaxed);
        phase(client.clone(), "update", 16, count, false, true).await;
        finished.store(true, Ordering::Release);
        let mut reads = reads.await.unwrap();
        reads.sort_unstable();
        assert!(!reads.is_empty());
        println!(
            "IWP read_rotation reads={} p99_us={:.3} rotations={}",
            reads.len(),
            reads[reads.len() * 99 / 100] as f64 / 1000.0,
            neb::ram::chunk::ROTATIONS.load(Ordering::Relaxed) - rotations
        );
    }
    for writers in [1, 16] {
        let insert = wanted(&format!("insert_{writers}"));
        let update = wanted(&format!("update_{writers}"));
        let delete = wanted(&format!("delete_{writers}"));
        let compare = only.is_some() && wanted(&format!("compare_{writers}"));
        assert!(
            !(compare && update),
            "select compare and update in separate runs"
        );
        if insert || update || delete || compare {
            phase(client.clone(), "insert", writers, count, false, insert).await;
            if !insert {
                assert!(neb::index::ranged::tree::btree::storage::wait_until_updated().await);
            }
        }
        if update {
            phase(client.clone(), "update", writers, count, false, true).await;
        }
        if compare {
            phase(client.clone(), "compare", writers, count, false, true).await;
        }
        if delete {
            phase(client.clone(), "delete", writers, count, false, true).await;
        }
    }
    if wanted("update_16_hot") {
        for id in 0..4 {
            client
                .write_cell(cell(64_000_000 + id, 0))
                .await
                .unwrap()
                .unwrap();
        }
        assert!(neb::index::ranged::tree::btree::storage::wait_until_updated().await);
        phase(client, "update", 16, count, true, true).await;
    }
    println!(
        "IWP retention pinned_segments={} pinned_bytes={}",
        server.chunks().pinned_segments(),
        server.chunks().pinned_bytes()
    );
    server.shutdown().await;
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        if let Some(line) = status.lines().find(|line| line.starts_with("VmHWM:")) {
            println!("IWP {line}");
        }
    }
}
