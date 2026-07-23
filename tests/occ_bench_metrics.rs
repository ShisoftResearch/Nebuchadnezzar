use std::{fs, sync::Arc, time::Duration};

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
use workloads::{run_fixed_success_rmw, AttemptOutcome, AttemptTally, BatchSpec};

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
        1,
        "OCC group helper must centralize the confirmed sample count"
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
        4,
        "all four OCC group construction sites must call the shared helper"
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
        commits_per_second: 0.5,
        p50_ns: 2_000_000,
        p95_ns: 2_000_000,
        p99_ns: 2_000_000,
        unexpected: Vec::new(),
        invariants_passed: true,
        #[cfg(feature = "occ_phase_profile")]
        phases: std::collections::BTreeMap::new(),
    };
    let second = ScenarioSummary {
        committed: 1,
        attempts: 1,
        not_realizable: 0,
        logical_retries: 0,
        commits_per_second: 1.0,
        p50_ns: 1_000_000,
        p95_ns: 1_000_000,
        p99_ns: 1_000_000,
        unexpected: Vec::new(),
        invariants_passed: true,
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

#[test]
#[should_panic(expected = "Port plan overflow")]
fn benchmark_port_plan_panics_on_slot_overflow() {
    let plan = fixture::PortPlan::new(u16::MAX - 5);
    let _ = plan.single(1);
}
