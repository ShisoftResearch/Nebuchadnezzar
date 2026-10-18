//! Fixed-work transaction probe with WAL, backup, and Raft storage enabled.
//! Usage: neb_lock_workload <scratch> <concurrency> <successful transactions>.
#[allow(dead_code)]
#[path = "../../benches/occ_support/mod.rs"]
mod occ_support;

use neb::server::{ServerOptions, Service};
use occ_support::{
    fixture::OccFixture,
    workloads::{run_fixed_success_rmw, BatchSpec},
};
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(
        args.len(),
        4,
        "expected scratch, concurrency, successful transactions"
    );
    let root = std::path::PathBuf::from(&args[1]);
    assert!(!root.exists(), "probe requires a fresh scratch directory");
    std::fs::create_dir_all(&root).unwrap();
    let concurrency = args[2].parse().unwrap();
    let successes = args[3].parse().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let fixture = Arc::new(
        OccFixture::single_with_options(
            address,
            "glneb_occ",
            ServerOptions {
                chunk_size: 256 * 1024 * 1024,
                db_size: 1024 * 1024 * 1024,
                tiered_config: None,
                backup_storage: Some(root.join("backup").to_string_lossy().into_owned()),
                wal_storage: Some(root.join("wal").to_string_lossy().into_owned()),
                raft_storage: Some(root.join("raft").to_string_lossy().into_owned()),
                services: vec![Service::Cell, Service::Transaction],
                index_enabled: false,
                enable_recovery: true,
                disable_storage_locks: false,
            },
        )
        .await,
    );
    let ids = Arc::new(fixture.ids_for_server(fixture.servers[0].server_id, 4096, 1_000_000));
    for id in ids.iter() {
        fixture.seed_counter(*id, 0).await;
    }
    let result = run_fixed_success_rmw(
        fixture.clone(),
        ids,
        BatchSpec {
            successes,
            concurrency,
            cells_per_txn: 1,
        },
    )
    .await;
    let summary = result.metrics.summary(result.elapsed);
    assert!(summary.invariants_passed, "{:?}", summary.unexpected);
    println!(
        "LOCK_WORKLOAD concurrency={concurrency} successes={successes} elapsed_ns={}",
        result.elapsed.as_nanos()
    );
    Arc::try_unwrap(fixture)
        .unwrap_or_else(|_| panic!("fixture still shared"))
        .shutdown()
        .await;
    #[cfg(feature = "lock_profile")]
    println!("LOCK_PROFILE {}", neb::utils::lock_profile::snapshot());
}
