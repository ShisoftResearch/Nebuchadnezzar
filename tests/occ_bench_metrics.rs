use std::{fs, sync::Arc, time::Duration};

use bifrost::hlc::HlcSource;
use serde_json::Value;

#[cfg(feature = "occ_phase_profile")]
use serde_json::json;

#[cfg(feature = "occ_phase_profile")]
use neb::server::transactions::phase_profile::{Phase, PhaseMeasurement, Snapshot, PHASE_COUNT};

#[path = "../benches/occ_support/fixture.rs"]
mod fixture;

#[path = "../benches/occ_support/metrics.rs"]
mod metrics;

#[path = "../benches/occ_support/workloads.rs"]
mod workloads;

#[cfg(feature = "occ_phase_profile")]
use metrics::PhaseSnapshotError;
use metrics::{BatchMetrics, RunReport, ScenarioSummary};
use workloads::{
    build_history_chain, hold_old_snapshot_across_newer_writes, run_expired_snapshot_read_batch,
    run_fixed_success_rmw, run_fresh_cleaner_reader_contention_batch, run_held_snapshot_read_batch,
    run_hlc_allocation_batch, run_non_transactional_conditional_update_batch,
    run_non_transactional_delete_recreate_batch, run_non_transactional_read_batch,
    run_non_transactional_remove_batch, run_non_transactional_upsert_batch,
    run_non_transactional_write_batch, run_projected_read_batch, run_read_only_current_batch,
    run_storage_bounded_non_transactional_update_batch, run_visible_history_read_batch,
    seed_history_counter, AttemptOutcome, AttemptTally, BatchSpec, ProjectionMode,
};

#[test]
fn occ_driver_routes_all_groups_through_flat_sampling_helper() {
    let driver = include_str!("../benches/occ_transactions.rs");
    let compact_driver: String = driver
        .chars()
        .filter(|char| !char.is_whitespace())
        .collect();

    assert_eq!(
        driver.matches(".benchmark_group(").count(),
        1,
        "all OCC groups must be constructed by one shared helper"
    );
    assert!(
        compact_driver.contains("fnocc_group"),
        "OCC driver must define a shared occ_group helper"
    );
    assert!(
        compact_driver.contains(".sampling_mode(SamplingMode::Flat)"),
        "OCC group helper must enforce flat Criterion sampling"
    );
    assert!(
        compact_driver.contains("constOCC_SAMPLE_SIZE:usize=10;"),
        "OCC driver must name the confirmed 10-sample window"
    );
    assert!(
        compact_driver.contains("constOCC_MEASUREMENT_SECONDS:u64=10;"),
        "OCC driver must name the confirmed 10-second measurement window"
    );
    assert_eq!(
        compact_driver
            .matches(".sample_size(OCC_SAMPLE_SIZE)")
            .count(),
        2,
        "standard and bounded cleaner sampling must use the confirmed sample count"
    );
    assert_eq!(
        compact_driver
            .matches(".measurement_time(Duration::from_secs(OCC_MEASUREMENT_SECONDS))")
            .count(),
        1,
        "OCC group helper must centralize the confirmed measurement time"
    );
    assert_eq!(
        compact_driver
            .matches(".throughput(Throughput::Elements(1))")
            .count(),
        1,
        "OCC group helper must centralize logical-operation throughput"
    );
    assert_eq!(
        compact_driver.matches("occ_group(").count(),
        5,
        "all OCC group construction sites must call the shared helper"
    );
    assert!(compact_driver.contains("constCLEANER_SAMPLE_BOUND_NANOS:u64=1;"));
    assert!(
        compact_driver.contains(".warm_up_time(Duration::from_nanos(CLEANER_SAMPLE_BOUND_NANOS))")
    );
    assert!(compact_driver
        .contains(".measurement_time(Duration::from_nanos(CLEANER_SAMPLE_BOUND_NANOS))"));
    assert!(compact_driver.contains(
        "elseif*scenario==\"mvcc/hlc_contention\"{configure_standard_sampling(&mutgroup);"
    ));
}

#[test]
fn bounded_cleaner_flat_sampling_requests_one_iteration_per_sample() {
    fn iterations_per_sample(measurement_ns: f64, samples: u64, mean_execution_ns: f64) -> u64 {
        ((measurement_ns / samples as f64 / mean_execution_ns).ceil() as u64).max(1)
    }

    assert_eq!(iterations_per_sample(1.0, 10, 1.0), 1);
    assert_eq!(iterations_per_sample(1.0, 10, 1_000_000_000.0), 1);
}

#[test]
fn mvcc_current_and_full_reads_use_distinct_workloads() {
    let driver = include_str!("../benches/occ_transactions.rs");
    let compact_driver: String = driver
        .chars()
        .filter(|char| !char.is_whitespace())
        .collect();

    assert!(compact_driver.contains("\"mvcc/read_only_current\"=>(run_read_only_current_batch("));
    assert!(compact_driver.contains("\"mvcc/full_read\"=>(run_projected_read_batch("));
}

#[test]
fn prepare_wait_responses_are_counted() {
    let workloads = include_str!("../benches/occ_support/workloads.rs");
    let arm_start = workloads
        .find("TMPrepareResult::DMPrepareError(DMPrepareResult::Wait)")
        .expect("finish_once must handle participant prepare waits explicitly");
    let arm_end = (arm_start + 700).min(workloads.len());

    assert!(
        workloads[arm_start..arm_end].contains(".with_wait()"),
        "the explicit participant prepare-wait arm must increment wait metrics"
    );
}

#[test]
fn nearest_rank_percentiles_are_deterministic() {
    let mut metrics = BatchMetrics::one_success(Duration::from_millis(1));
    for millis in 2..=100 {
        metrics.record_success(Duration::from_millis(millis), 1, 0);
    }

    let summary = metrics.summary(Duration::from_secs(1));

    assert_eq!(summary.p50_ns, 50_000_000);
    assert_eq!(summary.p95_ns, 95_000_000);
    assert_eq!(summary.p99_ns, 99_000_000);
}

#[test]
fn retries_and_failures_cannot_be_counted_as_throughput() {
    let mut metrics = BatchMetrics::default();
    metrics.record_retryable();
    metrics.record_retryable();

    let mut completed = BatchMetrics::default();
    completed.record_success(Duration::from_millis(4), 3, 2);
    completed.record_unexpected("rpc disconnected");

    metrics.merge(completed);

    let summary = metrics.summary(Duration::from_secs(2));

    assert_eq!(summary.committed, 1);
    assert_eq!(summary.attempts, 3);
    assert_eq!(summary.not_realizable, 2);
    assert_eq!(summary.logical_retries, 2);
    assert_eq!(summary.commits_per_second, 0.5);
    assert_eq!(summary.unexpected, vec![String::from("rpc disconnected")]);
    assert!(!summary.invariants_passed);
}

#[test]
fn checked_hlc_contention_allocates_every_requested_unique_timestamp() {
    let source = Arc::new(HlcSource::new(7));

    let batch = run_hlc_allocation_batch(source, 4_096, 16);
    let summary = batch.metrics.summary(batch.elapsed);

    assert_eq!(summary.committed, 4_096);
    assert_eq!(summary.attempts, 4_096);
    assert_eq!(summary.logical_retries, 0);
    assert_eq!(summary.waits, 0);
    assert!(batch.elapsed > Duration::ZERO);
    assert!(summary.unexpected.is_empty(), "{:?}", summary.unexpected);
    assert!(summary.invariants_passed);
}

#[cfg(feature = "occ_phase_profile")]
#[test]
fn occ_driver_brackets_every_workload_batch_with_phase_registry_calls() {
    let driver = include_str!("../benches/occ_transactions.rs");

    assert_eq!(
        driver.matches("reset_phase_profile();").count(),
        4,
        "OCC driver must reset the phase registry once per workload batch entry point"
    );
    assert_eq!(
        driver
            .matches("snapshot_phase_profile(&mut summary);")
            .count(),
        1,
        "OCC driver must snapshot the phase registry exactly once when publishing summaries"
    );
}

#[cfg(feature = "occ_phase_profile")]
#[test]
fn phase_snapshot_reports_per_invocation_and_per_commit_costs() {
    let mut phases = [PhaseMeasurement::default(); PHASE_COUNT];
    phases[Phase::PrepareBarrier as usize] = PhaseMeasurement {
        total_ns: 1_200,
        invocation_count: 3,
    };
    let snapshot = Snapshot {
        phases,
        active_guards: 0,
    };

    let mut metrics = BatchMetrics::default();
    metrics.record_success(Duration::from_nanos(900), 1, 0);
    metrics.record_success(Duration::from_nanos(900), 1, 0);

    let mut summary = metrics.summary(Duration::from_nanos(1_800));
    summary.attach_phase_snapshot(&snapshot).unwrap();

    let prepare_barrier = summary
        .phases
        .get("prepare_barrier")
        .expect("prepare_barrier summary");
    assert_eq!(prepare_barrier.total_ns, 1_200);
    assert_eq!(prepare_barrier.invocation_count, 3);
    assert_eq!(prepare_barrier.ns_per_invocation, 400.0);
    assert_eq!(prepare_barrier.ns_per_commit, 600.0);

    let summary_json = serde_json::to_value(&summary).expect("serialize summary");
    let phases_json = summary_json
        .get("phases")
        .and_then(Value::as_object)
        .expect("serialized phases");
    assert_eq!(
        phases_json
            .get("prepare_barrier")
            .and_then(Value::as_object)
            .and_then(|phase| phase.get("total_ns")),
        Some(&Value::from(1_200_u64))
    );
    assert!(phases_json.contains_key("participant_end"));
}

#[cfg(feature = "occ_phase_profile")]
#[test]
fn phase_snapshot_rejects_active_guards_without_publishing_partial_results() {
    let mut phases = [PhaseMeasurement::default(); PHASE_COUNT];
    phases[Phase::PrepareBarrier as usize] = PhaseMeasurement {
        total_ns: 1_200,
        invocation_count: 3,
    };
    let snapshot = Snapshot {
        phases,
        active_guards: 1,
    };

    let mut summary =
        BatchMetrics::one_success(Duration::from_nanos(900)).summary(Duration::from_nanos(900));
    let valid_snapshot = Snapshot {
        phases,
        active_guards: 0,
    };

    summary.attach_phase_snapshot(&valid_snapshot).unwrap();
    let before_rejected_attach = summary.phases.clone();

    assert!(!before_rejected_attach.is_empty());
    assert_eq!(
        summary.attach_phase_snapshot(&snapshot),
        Err(PhaseSnapshotError::ActiveGuards(1))
    );
    assert_eq!(summary.phases, before_rejected_attach);
}

#[cfg(feature = "occ_phase_profile")]
#[test]
fn phase_snapshot_uses_zero_ratios_for_zero_denominators() {
    let mut phases = [PhaseMeasurement::default(); PHASE_COUNT];
    phases[Phase::PrepareBarrier as usize] = PhaseMeasurement {
        total_ns: 1_200,
        invocation_count: 3,
    };
    phases[Phase::ParticipantEnd as usize] = PhaseMeasurement {
        total_ns: 777,
        invocation_count: 0,
    };
    let snapshot = Snapshot {
        phases,
        active_guards: 0,
    };

    let mut summary = BatchMetrics::default().summary(Duration::from_nanos(1));
    summary.attach_phase_snapshot(&snapshot).unwrap();

    let prepare_barrier = summary
        .phases
        .get("prepare_barrier")
        .expect("prepare_barrier summary");
    assert_eq!(prepare_barrier.ns_per_invocation, 400.0);
    assert_eq!(prepare_barrier.ns_per_commit, 0.0);

    let participant_end = summary
        .phases
        .get("participant_end")
        .expect("participant_end summary");
    assert_eq!(participant_end.total_ns, 777);
    assert_eq!(participant_end.invocation_count, 0);
    assert_eq!(participant_end.ns_per_invocation, 0.0);
    assert_eq!(participant_end.ns_per_commit, 0.0);
}

#[cfg(feature = "occ_phase_profile")]
#[test]
fn run_report_deserializes_legacy_scenario_json_without_phases_field() {
    let report_json = json!({
        "label": "occ-legacy",
        "revision": "deadbeef",
        "scenarios": {
            "repeatable-read": {
                "committed": 1,
                "attempts": 1,
                "not_realizable": 0,
                "logical_retries": 0,
                "commits_per_second": 1.0,
                "p50_ns": 100,
                "p95_ns": 100,
                "p99_ns": 100,
                "unexpected": [],
                "invariants_passed": true
            }
        }
    });

    let report: RunReport = serde_json::from_value(report_json).expect("deserialize legacy report");
    let scenario = report
        .scenarios
        .get("repeatable-read")
        .expect("repeatable-read scenario");
    assert!(scenario.phases.is_empty());
}

#[test]
fn retryable_attempts_do_not_advance_success_target() {
    let tally = AttemptTally::from_outcomes([
        AttemptOutcome::Retryable,
        AttemptOutcome::Committed,
        AttemptOutcome::Unexpected("bad state".to_string()),
    ]);
    assert_eq!(tally.attempts, 3);
    assert_eq!(tally.committed, 1);
    assert_eq!(tally.not_realizable, 1);
    assert_eq!(tally.unexpected, vec!["bad state"]);
}

#[test]
fn run_report_replaces_a_scenario_by_name() {
    let mut report = RunReport::new("occ-initial", "deadbeef");
    let first = ScenarioSummary {
        committed: 1,
        attempts: 1,
        not_realizable: 0,
        logical_retries: 0,
        waits: 0,
        commits_per_second: 0.5,
        p50_ns: 2_000_000,
        p95_ns: 2_000_000,
        p99_ns: 2_000_000,
        unexpected: Vec::new(),
        invariants_passed: true,
        retained_revisions: 0,
        retained_bytes: 0,
        segment_count: 0,
        #[cfg(feature = "occ_phase_profile")]
        phases: std::collections::BTreeMap::new(),
    };
    let second = ScenarioSummary {
        committed: 1,
        attempts: 1,
        not_realizable: 0,
        logical_retries: 0,
        waits: 0,
        commits_per_second: 1.0,
        p50_ns: 1_000_000,
        p95_ns: 1_000_000,
        p99_ns: 1_000_000,
        unexpected: Vec::new(),
        invariants_passed: true,
        retained_revisions: 0,
        retained_bytes: 0,
        segment_count: 0,
        #[cfg(feature = "occ_phase_profile")]
        phases: std::collections::BTreeMap::new(),
    };

    report.record("repeatable-read", first);
    report.record("repeatable-read", second.clone());

    assert_eq!(report.scenarios.len(), 1);
    assert_eq!(report.scenarios.get("repeatable-read"), Some(&second));

    let tempdir = tempfile::tempdir().expect("create tempdir");
    let path = tempdir.path().join("nested/report.json");
    report.write_json(&path).expect("write report");

    assert!(path.exists());
    assert!(!path.with_file_name("report.json.tmp").exists());

    let bytes = fs::read(&path).expect("read report");
    let persisted_json: Value = serde_json::from_slice(&bytes).expect("parse report as json");
    let scenario = persisted_json
        .get("scenarios")
        .and_then(|scenarios| scenarios.get("repeatable-read"))
        .and_then(Value::as_object)
        .expect("repeatable-read scenario object");
    assert_eq!(scenario.get("p50_ns"), Some(&Value::from(1_000_000_u64)));
    assert_eq!(scenario.get("p95_ns"), Some(&Value::from(1_000_000_u64)));
    assert_eq!(scenario.get("p99_ns"), Some(&Value::from(1_000_000_u64)));
    assert!(!scenario.contains_key("latency_p50_ns"));
    assert!(!scenario.contains_key("latency_p95_ns"));
    assert!(!scenario.contains_key("latency_p99_ns"));
    #[cfg(not(feature = "occ_phase_profile"))]
    assert!(!scenario.contains_key("phases"));

    let persisted: RunReport = serde_json::from_slice(&bytes).expect("parse typed report");
    assert_eq!(persisted.scenarios.len(), 1);
    assert_eq!(persisted.scenarios.get("repeatable-read"), Some(&second));
}

#[test]
fn benchmark_port_plan_allocates_non_overlapping_clusters() {
    let plan = fixture::PortPlan::new(39_400);
    assert_eq!(plan.single_server(), "127.0.0.1:39400");
    assert_eq!(plan.single(3), "127.0.0.1:39430");
    assert_eq!(
        plan.cluster(2),
        vec![
            "127.0.0.1:39420".to_string(),
            "127.0.0.1:39421".to_string(),
            "127.0.0.1:39422".to_string(),
        ]
    );
}

#[test]
fn benchmark_ids_probe_budget_scales_and_floors() {
    assert_eq!(fixture::ids_probe_budget(0), 10_000);
    assert_eq!(fixture::ids_probe_budget(1), 10_000);
    assert_eq!(fixture::ids_probe_budget(20), 20 * 1024);
}

#[tokio::test(flavor = "multi_thread")]
async fn fixed_success_hot_cell_batch_smoke_test() {
    let _ = env_logger::try_init();
    let fixture = Arc::new(fixture::OccFixture::single("127.0.0.1:54500", "occ_bench_smoke").await);
    let server_id = fixture.servers[0].server_id;
    let id = fixture.ids_for_server(server_id, 1, 54_500)[0];
    fixture.seed_counter(id, 0).await;

    let batch = run_fixed_success_rmw(
        fixture.clone(),
        Arc::new(vec![id]),
        BatchSpec {
            successes: 4,
            concurrency: 2,
            cells_per_txn: 1,
        },
    )
    .await;
    let summary = batch.metrics.summary(batch.elapsed);
    let final_score = fixture.score(id).await;

    let fixture = match Arc::try_unwrap(fixture) {
        Ok(fixture) => fixture,
        Err(_) => panic!("smoke test fixture should have no remaining shared owners"),
    };
    fixture.shutdown().await;

    assert_eq!(summary.committed, 4);
    assert!(summary.unexpected.is_empty(), "{:?}", summary.unexpected);
    assert!(summary.invariants_passed);
    assert_eq!(final_score, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn current_read_only_lifecycle_and_projected_full_read_both_preserve_state() {
    let fixture =
        Arc::new(fixture::OccFixture::single("127.0.0.1:54570", "occ_bench_distinct_reads").await);
    let server_id = fixture.servers[0].server_id;
    let id = fixture.ids_for_server(server_id, 1, 54_570)[0];
    fixture.seed_counter(id, 9).await;

    let current = run_read_only_current_batch(fixture.clone(), Arc::new(vec![id]), 2).await;
    let projected =
        run_projected_read_batch(fixture.clone(), Arc::new(vec![id]), 2, ProjectionMode::Full)
            .await;
    let current_summary = current.metrics.summary(current.elapsed);
    let projected_summary = projected.metrics.summary(projected.elapsed);
    let final_score = fixture.score(id).await;

    let fixture = Arc::try_unwrap(fixture)
        .unwrap_or_else(|_| panic!("distinct read test retained fixture owners"));
    fixture.shutdown().await;

    assert_eq!(current_summary.committed, 2);
    assert!(current_summary.unexpected.is_empty());
    assert_eq!(projected_summary.committed, 2);
    assert!(projected_summary.unexpected.is_empty());
    assert_eq!(final_score, 9);
}

#[tokio::test(flavor = "multi_thread")]
async fn clustered_seed_is_transactionally_visible_before_multi_participant_timing() {
    let fixture = fixture::OccFixture::cluster(
        fixture::PortPlan::new(54_600).cluster(0),
        "occ_bench_cluster_visibility",
    )
    .await;
    let ids = fixture
        .servers
        .iter()
        .enumerate()
        .flat_map(|(index, server)| {
            fixture.ids_for_server(server.server_id, 1, 54_600 + index as u64 * 10_000)
        })
        .collect::<Vec<_>>();
    let seed_revisions =
        futures::future::join_all(ids.iter().map(|id| fixture.seed_counter(*id, 0)))
            .await
            .into_iter()
            .map(|header| header.revision_ts)
            .collect::<Vec<_>>();
    let max_seed_revision = *seed_revisions
        .iter()
        .max()
        .expect("cluster visibility test seeded revisions");
    let first_coordinator_tid = fixture.observe_distributed_seed_revisions(seed_revisions);

    assert!(
        first_coordinator_tid > max_seed_revision,
        "first transaction coordinator TID must be newer than every direct seed revision"
    );
    fixture.assert_transactional_visibility(&ids).await;

    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deterministic_history_chain_reports_real_retained_state() {
    let fixture =
        Arc::new(fixture::OccFixture::single("127.0.0.1:54520", "occ_bench_history_chain").await);
    let server_id = fixture.servers[0].server_id;
    let id = fixture.ids_for_server(server_id, 1, 54_520)[0];
    seed_history_counter(&fixture, id, 0, 0).await;

    let chain = build_history_chain(fixture.clone(), id, 8).await;
    let telemetry = fixture.retention_telemetry(&chain.predecessors);
    let batch = run_visible_history_read_batch(fixture.clone(), chain.clone(), 3).await;
    let summary = batch.metrics.summary(batch.elapsed);

    let fixture = Arc::try_unwrap(fixture)
        .unwrap_or_else(|_| panic!("history chain test retained fixture owners"));
    fixture.shutdown().await;

    assert_eq!(chain.predecessors.len(), 8);
    assert_eq!(telemetry.retained_revisions, 8);
    assert!(telemetry.retained_bytes > 0);
    assert!(telemetry.segment_count > 0);
    assert_eq!(summary.committed, 3);
    assert!(summary.unexpected.is_empty(), "{:?}", summary.unexpected);
}

#[tokio::test(flavor = "multi_thread")]
async fn held_transaction_snapshot_reads_old_revision_after_newer_writes() {
    let fixture =
        Arc::new(fixture::OccFixture::single("127.0.0.1:54530", "occ_bench_old_snapshot").await);
    let server_id = fixture.servers[0].server_id;
    let id = fixture.ids_for_server(server_id, 1, 54_530)[0];
    seed_history_counter(&fixture, id, 0, 0).await;

    let held = hold_old_snapshot_across_newer_writes(fixture.clone(), id, 8).await;
    let batch = run_held_snapshot_read_batch(fixture.clone(), held.clone(), 3).await;
    let summary = batch.metrics.summary(batch.elapsed);
    held.abort(&fixture).await;

    let fixture = Arc::try_unwrap(fixture)
        .unwrap_or_else(|_| panic!("old snapshot test retained fixture owners"));
    fixture.shutdown().await;

    assert_eq!(held.history.predecessors.len(), 8);
    assert_eq!(summary.committed, 3);
    assert!(summary.unexpected.is_empty(), "{:?}", summary.unexpected);
}

#[tokio::test(flavor = "multi_thread")]
async fn expired_history_is_reported_after_retention_window() {
    let fixture = Arc::new(
        fixture::OccFixture::single_with_history_retention(
            "127.0.0.1:54540",
            "occ_bench_history_expiration",
            50,
        )
        .await,
    );
    let server_id = fixture.servers[0].server_id;
    let id = fixture.ids_for_server(server_id, 1, 54_540)[0];
    seed_history_counter(&fixture, id, 0, 0).await;

    let chain = build_history_chain(fixture.clone(), id, 8).await;
    fixture.wait_for_history_expiration(&chain).await;
    let batch = run_expired_snapshot_read_batch(fixture.clone(), chain, 3).await;
    let summary = batch.metrics.summary(batch.elapsed);

    let fixture = Arc::try_unwrap(fixture)
        .unwrap_or_else(|_| panic!("history expiration test retained fixture owners"));
    fixture.shutdown().await;

    assert_eq!(summary.committed, 3);
    assert!(summary.unexpected.is_empty(), "{:?}", summary.unexpected);
}

#[tokio::test(flavor = "multi_thread")]
async fn bounded_direct_update_maintenance_is_excluded_from_measured_elapsed() {
    let fixture = Arc::new(
        fixture::OccFixture::single_with_history_retention(
            "127.0.0.1:54550",
            "occ_bench_bounded_updates",
            1,
        )
        .await,
    );
    let server_id = fixture.servers[0].server_id;
    let id = fixture.ids_for_server(server_id, 1, 54_550)[0];
    fixture.seed_counter(id, 0).await;

    let wall_started = std::time::Instant::now();
    let batch = run_storage_bounded_non_transactional_update_batch(
        fixture.clone(),
        Arc::new(vec![id]),
        128,
    )
    .await;
    let wall_elapsed = wall_started.elapsed();
    let summary = batch.metrics.summary(batch.elapsed);

    let fixture = Arc::try_unwrap(fixture)
        .unwrap_or_else(|_| panic!("bounded update test retained fixture owners"));
    fixture.shutdown().await;

    assert_eq!(summary.committed, 128);
    assert!(summary.unexpected.is_empty(), "{:?}", summary.unexpected);
    assert!(
        wall_elapsed.saturating_sub(batch.elapsed) >= Duration::from_millis(2),
        "maintenance delay must stay outside returned measured elapsed: wall={wall_elapsed:?} measured={:?}",
        batch.elapsed
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_mutation_workloads_account_every_requested_operation() {
    const OPERATIONS: u64 = 3;

    let fixture = Arc::new(
        fixture::OccFixture::single_with_history_retention(
            "127.0.0.1:54555",
            "occ_bench_direct_mutations",
            1,
        )
        .await,
    );
    let server_id = fixture.servers[0].server_id;
    let ids = Arc::new(fixture.ids_for_server(server_id, OPERATIONS as usize, 54_555));
    let remove_ids = Arc::new(fixture.ids_for_server(server_id, OPERATIONS as usize, 55_555));
    let delete_recreate_ids =
        Arc::new(fixture.ids_for_server(server_id, OPERATIONS as usize, 56_555));

    let write = run_non_transactional_write_batch(fixture.clone(), ids.clone(), OPERATIONS).await;
    let read = run_non_transactional_read_batch(fixture.clone(), ids.clone(), OPERATIONS).await;
    let update = run_storage_bounded_non_transactional_update_batch(
        fixture.clone(),
        ids.clone(),
        OPERATIONS,
    )
    .await;
    let upsert = run_non_transactional_upsert_batch(fixture.clone(), ids.clone(), OPERATIONS).await;
    let conditional =
        run_non_transactional_conditional_update_batch(fixture.clone(), ids.clone(), OPERATIONS)
            .await;

    for id in remove_ids.iter() {
        fixture.seed_counter(*id, 0).await;
    }
    let remove =
        run_non_transactional_remove_batch(fixture.clone(), remove_ids.clone(), OPERATIONS).await;
    for id in remove_ids.iter() {
        fixture.seed_counter(*id, 0).await;
    }
    for id in delete_recreate_ids.iter() {
        fixture.seed_counter(*id, 0).await;
    }
    let delete_recreate = run_non_transactional_delete_recreate_batch(
        fixture.clone(),
        delete_recreate_ids,
        OPERATIONS,
    )
    .await;

    let fixture = Arc::try_unwrap(fixture)
        .unwrap_or_else(|_| panic!("direct mutation test retained fixture owners"));
    fixture.shutdown().await;

    for batch in [
        write,
        read,
        update,
        upsert,
        conditional,
        remove,
        delete_recreate,
    ] {
        let summary = batch.metrics.summary(batch.elapsed);
        assert_eq!(summary.attempts, OPERATIONS);
        assert_eq!(summary.committed, OPERATIONS);
        assert!(summary.unexpected.is_empty(), "{:?}", summary.unexpected);
        assert!(summary.invariants_passed);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cleaner_relocates_retained_history_while_reader_is_active() {
    let fixture = Arc::new(
        fixture::OccFixture::single_with_history_retention(
            "127.0.0.1:54560",
            "occ_bench_cleaner_contention",
            2_000,
        )
        .await,
    );
    fixture.servers[0].cleaner().pause();
    let server_id = fixture.servers[0].server_id;
    let all_ids = fixture
        .ids_for_server(server_id, 512, 54_560)
        .into_iter()
        .filter(|id| id.higher % 4 == 0)
        .take(48)
        .collect::<Vec<_>>();
    assert_eq!(all_ids.len(), 48);
    let target_ids = Arc::new(all_ids.iter().step_by(2).copied().collect::<Vec<_>>());
    let sacrificial_ids = Arc::new(
        all_ids
            .iter()
            .skip(1)
            .step_by(2)
            .copied()
            .collect::<Vec<_>>(),
    );
    assert_eq!(target_ids.len(), 24);
    assert_eq!(sacrificial_ids.len(), 24);
    for (target, sacrificial) in target_ids.iter().zip(sacrificial_ids.iter()) {
        for id in [target, sacrificial] {
            seed_history_counter(&fixture, *id, 0, 512 * 1024).await;
        }
    }

    let (batch, cleaner_history) =
        run_fresh_cleaner_reader_contention_batch(fixture.clone(), target_ids, sacrificial_ids, 2)
            .await;
    let summary = batch.metrics.summary(batch.elapsed);
    let telemetry = fixture.retention_telemetry(&cleaner_history.predecessors);

    let fixture = Arc::try_unwrap(fixture)
        .unwrap_or_else(|_| panic!("cleaner contention test retained fixture owners"));
    fixture.shutdown().await;

    assert_eq!(cleaner_history.relocations_observed(), 1);
    assert_eq!(telemetry.retained_revisions, 24);
    assert_eq!(summary.committed, 2);
    assert_eq!(summary.attempts, 2);
    assert!(summary.unexpected.is_empty(), "{:?}", summary.unexpected);
}

#[test]
#[should_panic(expected = "Port plan overflow")]
fn benchmark_port_plan_panics_on_slot_overflow() {
    let plan = fixture::PortPlan::new(u16::MAX - 5);
    let _ = plan.single(1);
}
