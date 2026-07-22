use std::{fs, time::Duration};

#[path = "../benches/occ_support/metrics.rs"]
mod metrics;

use metrics::{BatchMetrics, RunReport, ScenarioSummary};

#[test]
fn nearest_rank_percentiles_are_deterministic() {
    let mut metrics = BatchMetrics::one_success(Duration::from_millis(1));
    for millis in 2..=100 {
        metrics.record_success(Duration::from_millis(millis), 1, 0);
    }

    let summary = metrics.summary(Duration::from_secs(1));

    assert_eq!(summary.latency_p50_ns, 50_000_000);
    assert_eq!(summary.latency_p95_ns, 95_000_000);
    assert_eq!(summary.latency_p99_ns, 99_000_000);
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
    assert_eq!(summary.commits_per_second, 0.5);
    assert_eq!(summary.unexpected, vec![String::from("rpc disconnected")]);
    assert!(!summary.invariants_passed);
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
        latency_p50_ns: 2_000_000,
        latency_p95_ns: 2_000_000,
        latency_p99_ns: 2_000_000,
        unexpected: Vec::new(),
        invariants_passed: true,
    };
    let second = ScenarioSummary {
        committed: 1,
        attempts: 1,
        not_realizable: 0,
        logical_retries: 0,
        commits_per_second: 1.0,
        latency_p50_ns: 1_000_000,
        latency_p95_ns: 1_000_000,
        latency_p99_ns: 1_000_000,
        unexpected: Vec::new(),
        invariants_passed: true,
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

    let persisted: RunReport =
        serde_json::from_slice(&fs::read(&path).expect("read report")).expect("parse report");
    assert_eq!(persisted.scenarios.len(), 1);
    assert_eq!(persisted.scenarios.get("repeatable-read"), Some(&second));
}
