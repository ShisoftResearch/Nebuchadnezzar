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
        5,
        "all five OCC group construction sites must call the shared helper"
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
fn run_report_requires_phases_field() {
    let report_json = json!({
        "label": "occ-required-phases",
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

    assert!(serde_json::from_value::<RunReport>(report_json).is_err());
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
            window_stride: 1,
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
async fn cluster_fixture_places_one_id_on_every_member() {
    let _ = env_logger::try_init();
    let plan = fixture::PortPlan::new(54_510);
    let fixture = fixture::OccFixture::cluster(plan.cluster(0), "occ_bench_cluster_ids").await;
    for (index, server) in fixture.servers.iter().enumerate() {
        let start = 4_000_000 + index as u64 * 10_000;
        let ids = fixture
            .ids_placed_on_server(server.server_id, 1, start)
            .await;
        assert_eq!(ids.len(), 1);
        assert_eq!(
            fixture.client.locate_server_id(&ids[0]).unwrap(),
            server.server_id
        );
        // Every member must route the id to its owner, not only the members
        // that took part in the move.
        let slot = neb::slots::slot_of(&ids[0]) as u64;
        for member in &fixture.servers {
            assert_eq!(
                member.consh.get_server_id_for_slot(slot),
                Some(server.server_id)
            );
        }
        fixture.seed_counter(ids[0], index as u64).await;
        assert_eq!(fixture.score(ids[0]).await, index as u64);
    }
    fixture.shutdown().await;
}

#[test]
#[should_panic(expected = "Port plan overflow")]
fn benchmark_port_plan_panics_on_slot_overflow() {
    let plan = fixture::PortPlan::new(u16::MAX - 5);
    let _ = plan.single(1);
}

#[tokio::test(flavor = "multi_thread")]
async fn zz_experiment_stale_coordinator_after_migration() {
    let _ = env_logger::try_init();
    std::env::set_var("NEB_JOIN_FILL_DELAY_MS", "3600000");
    let plan = fixture::PortPlan::new(54_520);
    let fixture =
        Arc::new(fixture::OccFixture::cluster(plan.cluster(0), "occ_bench_stale_exp").await);
    let sids: Vec<u64> = fixture.servers.iter().map(|s| s.server_id).collect();
    let mut ids = Vec::new();
    for (index, sid) in sids.iter().enumerate() {
        ids.extend(
            fixture
                .ids_placed_on_server(*sid, 1, 4_000_000 + index as u64 * 10_000)
                .await,
        );
    }
    for id in &ids {
        fixture.seed_counter(*id, 0).await;
    }
    let ids = Arc::new(ids);
    let slots: Vec<u32> = ids.iter().map(neb::slots::slot_of).collect();
    let group = fixture.client.group_name().to_string();
    let db = fixture.client.database_name().to_string();
    let pos = |o: Option<u64>| o.and_then(|o| sids.iter().position(|x| *x == o));
    let other = Arc::new(
        neb::client::AsyncClient::new(
            &fixture.servers[1].rpc,
            &fixture.servers[1].membership,
            &fixture.addresses,
            &group,
        )
        .await
        .unwrap(),
    );
    let tables = |label: &str| {
        let mut v = format!("EXP {label}:");
        for (k, server) in fixture.servers.iter().enumerate() {
            v.push_str(&format!(
                " s{k}={:?}",
                slots
                    .iter()
                    .map(|s| pos(server.consh.slot_override(*s as u64)))
                    .collect::<Vec<_>>()
            ));
        }
        v.push_str(&format!(
            " fixture_client={:?}",
            slots
                .iter()
                .map(|s| pos(fixture.client.conshash.slot_override(*s as u64)))
                .collect::<Vec<_>>()
        ));
        v.push_str(&format!(
            " migrating_client={:?}",
            slots
                .iter()
                .map(|s| pos(other.conshash.slot_override(*s as u64)))
                .collect::<Vec<_>>()
        ));
        eprintln!("{v}");
    };
    let direct = |label: &'static str| {
        let fixture = fixture.clone();
        let ids = ids.clone();
        let sids = sids.clone();
        let group = group.clone();
        let db = db.clone();
        async move {
            let mut v = format!("EXP {label} direct id2 score per server:");
            for (k, sid) in sids.iter().enumerate() {
                let c = neb::client::client_by_server_id_for_database(
                    &fixture.client.conshash,
                    *sid,
                    &group,
                    &db,
                )
                .await
                .unwrap();
                let r = match c.read_cell(ids[2]).await {
                    Ok(Ok(cell)) => format!("{:?}", cell.data["score"].u64()),
                    Ok(Err(e)) => format!("{e:?}"),
                    Err(e) => format!("rpc {e:?}"),
                };
                v.push_str(&format!(" s{k}={r}"));
            }
            eprintln!("{v}");
        }
    };
    let batch = |n: u64| {
        let fixture = fixture.clone();
        let ids = ids.clone();
        async move {
            let b = run_fixed_success_rmw(
                fixture,
                ids,
                BatchSpec {
                    successes: n,
                    concurrency: 1,
                    cells_per_txn: 3,
                    window_stride: 3,
                },
            )
            .await;
            (b.metrics.committed, b.metrics.unexpected)
        }
    };
    tables("after placement");
    let (c0, u0) = batch(10).await;
    eprintln!("EXP batch0 committed={c0} unexpected={u0:?}");
    direct("before migration").await;

    let mplan = neb::migration::MigrationPlan::default();
    let h = neb::migration::migrate_slot(&other, slots[2], sids[2], sids[1], &mplan).await;
    eprintln!(
        "EXP migrate slot {} s2->s1: {:?}",
        slots[2],
        h.as_ref().map(|h| h.cells_transferred)
    );
    tables("after handover");
    let (c1, u1) = batch(10).await;
    eprintln!("EXP batch1 (after handover, before reclaim) committed={c1} unexpected={u1:?}");
    direct("after batch1").await;

    let r = neb::migration::reclaim_donor_copy(&other, slots[2], sids[2], sids[1], &mplan).await;
    eprintln!(
        "EXP reclaim: {:?}",
        r.as_ref().map(|r| (r.dropped, r.carried_over, r.retained))
    );
    tables("after reclaim");
    direct("after reclaim").await;
    let (c2, u2) = batch(10).await;
    eprintln!("EXP batch2 (after reclaim) committed={c2} unexpected={u2:?}");
    direct("after batch2").await;

    for server in &fixture.servers {
        server.refresh_slot_placement().await;
    }
    fixture.client.reload_slot_owners().await;
    tables("after explicit refresh");
    direct("after refresh").await;
    let mut finals = Vec::new();
    for id in ids.iter() {
        finals.push(fixture.score(*id).await);
    }
    eprintln!(
        "EXP total committed={} final scores={:?}",
        c0 + c1 + c2,
        finals
    );
    let (c3, u3) = batch(10).await;
    eprintln!("EXP batch3 (after refresh) committed={c3} unexpected={u3:?}");
    drop(other);
    let fixture = match Arc::try_unwrap(fixture) {
        Ok(f) => f,
        Err(_) => panic!("shared"),
    };
    fixture.shutdown().await;
}

type TxnClient = neb::server::transactions::manager::AsyncServiceClient;

/// The outcome of one read-modify-write of the `score` field.
#[derive(Debug)]
enum Rmw {
    /// The commit was acknowledged with this new score.
    Committed(u64),
    /// Nothing was written and the caller can try again.
    Retryable(String),
    /// Anything else. A correct cluster must not give this for a cell that
    /// exists.
    Failed(String),
}

/// One read-modify-write of `score` through the transaction manager of `txn`.
///
/// A refusal that names a slot owner counts as retryable, so that a fix that
/// refuses a stale route and lets the caller retry passes this check.
async fn rmw_once(txn: &TxnClient, id: neb::ram::types::Id) -> Rmw {
    use dovahkiin::types::Map as _;
    use neb::ram::cell::{ReadError, WriteError};
    use neb::ram::types::OwnedValue;
    use neb::server::transactions::{DMPrepareResult, EndResult, TMPrepareResult, TxnExecResult};

    let tid = match txn.begin().await {
        Ok(Ok(tid)) => tid,
        other => return Rmw::Failed(format!("begin: {other:?}")),
    };
    let outcome = async {
        let mut cell = match txn.read(tid.clone(), id).await {
            Ok(Ok(TxnExecResult::Accepted(cell))) => cell,
            Ok(Ok(TxnExecResult::Rejected | TxnExecResult::Wait)) => {
                return Rmw::Retryable("read rejected".into())
            }
            Ok(Ok(TxnExecResult::Error(ReadError::NotSlotOwner { owner, .. }))) => {
                return Rmw::Retryable(format!("read refused, owner {owner}"))
            }
            other => return Rmw::Failed(format!("read: {other:?}")),
        };
        let next = match cell.data["score"].u64() {
            Some(score) => score + 1,
            None => return Rmw::Failed(format!("no u64 score in {:?}", cell.data)),
        };
        let mut map = match &cell.data {
            OwnedValue::Map(map) => map.owned(),
            other => return Rmw::Failed(format!("not a map: {other:?}")),
        };
        *map.get_mut("score") = OwnedValue::U64(next);
        cell.data = OwnedValue::Map(map);
        match txn.update(tid.clone(), cell).await {
            Ok(Ok(TxnExecResult::Accepted(()))) => {}
            Ok(Ok(TxnExecResult::Rejected | TxnExecResult::Wait)) => {
                return Rmw::Retryable("update rejected".into())
            }
            Ok(Ok(TxnExecResult::Error(WriteError::NotSlotOwner { owner, .. }))) => {
                return Rmw::Retryable(format!("update refused, owner {owner}"))
            }
            other => return Rmw::Failed(format!("update: {other:?}")),
        }
        match txn.prepare(tid.clone()).await {
            Ok(Ok(TMPrepareResult::Success)) => {}
            Ok(Ok(TMPrepareResult::DMPrepareError(
                DMPrepareResult::NotRealizable | DMPrepareResult::Wait,
            ))) => return Rmw::Retryable("prepare not realizable".into()),
            other => return Rmw::Failed(format!("prepare: {other:?}")),
        }
        match txn.commit(tid.clone()).await {
            Ok(Ok(EndResult::Success)) => Rmw::Committed(next),
            other => Rmw::Failed(format!("commit: {other:?}")),
        }
    }
    .await;
    if !matches!(outcome, Rmw::Committed(_)) {
        // Best effort: a transaction that prepared nothing can already be gone.
        let _ = txn.abort(tid).await;
    }
    outcome
}

/// Retry `rmw_once` until it commits, fails or runs out of attempts.
async fn rmw_until_committed(
    txn: &TxnClient,
    id: neb::ram::types::Id,
    max_attempts: usize,
) -> Result<u64, String> {
    let mut last = String::new();
    for _ in 0..max_attempts {
        match rmw_once(txn, id).await {
            Rmw::Committed(score) => return Ok(score),
            Rmw::Retryable(reason) => {
                last = reason;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Rmw::Failed(reason) => return Err(reason),
        }
    }
    Err(format!(
        "no commit after {max_attempts} attempts, last: {last}"
    ))
}

/// The `score` of `id` as one member holds it, with no redirect.
async fn score_on(
    fixture: &fixture::OccFixture,
    server_id: u64,
    id: neb::ram::types::Id,
) -> Result<u64, String> {
    let member = neb::client::client_by_server_id_for_database(
        &fixture.client.conshash,
        server_id,
        fixture.client.group_name(),
        fixture.client.database_name(),
    )
    .await
    .map_err(|error| format!("connect: {error:?}"))?;
    match member.read_cell(id).await {
        Ok(Ok(cell)) => cell.data["score"]
            .u64()
            .copied()
            .ok_or_else(|| format!("no u64 score in {:?}", cell.data)),
        Ok(Err(error)) => Err(format!("{error:?}")),
        Err(error) => Err(format!("rpc {error:?}")),
    }
}

/// A 3-member cluster, and a client on member 1 for the migrations.
///
/// Member 0 hosts the coordinator of `fixture.txn`. A migration that the
/// member-1 client runs tells only the donor and the recipient, so member 0
/// keeps the old owner of the slot until something refreshes it.
async fn stale_coordinator_cluster(
    base: u16,
    group: &str,
) -> (
    Arc<fixture::OccFixture>,
    Vec<u64>,
    Arc<neb::client::AsyncClient>,
) {
    // No automatic join fill: it would move slots while the test measures.
    std::env::set_var("NEB_JOIN_FILL_DELAY_MS", "3600000");
    let plan = fixture::PortPlan::new(base);
    let fixture = Arc::new(fixture::OccFixture::cluster(plan.cluster(0), group).await);
    let sids: Vec<u64> = fixture.servers.iter().map(|s| s.server_id).collect();
    let migrator = Arc::new(
        neb::client::AsyncClient::new(
            &fixture.servers[1].rpc,
            &fixture.servers[1].membership,
            &fixture.addresses,
            fixture.client.group_name(),
        )
        .await
        .expect("client on member 1"),
    );
    (fixture, sids, migrator)
}

async fn shut_down(fixture: Arc<fixture::OccFixture>) {
    match Arc::try_unwrap(fixture) {
        Ok(fixture) => fixture.shutdown().await,
        Err(_) => panic!("fixture still shared at shutdown"),
    }
}

/// A slot moves from member 2 to member 1. The coordinator on member 0 took
/// no part in the move. A transaction through it must commit to the current
/// owner or be refused in a retryable way. It must not commit to the former
/// owner, and it must not fail with `CellDoesNotExisted` for a cell that
/// exists.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "open defect: a coordinator that took no part in a slot migration routes \
            transactions to the former owner (writes land on the donor, and fail with \
            CellDoesNotExisted after the reclaim)"]
async fn stale_coordinator_commits_to_the_current_slot_owner() {
    let _ = env_logger::try_init();
    let (fixture, sids, migrator) =
        stale_coordinator_cluster(54_600, "occ_stale_coordinator_owner").await;
    let (coordinator, recipient, donor) = (sids[0], sids[1], sids[2]);
    let id = fixture.ids_placed_on_server(donor, 1, 4_020_000).await[0];
    let slot = neb::slots::slot_of(&id);
    fixture.seed_counter(id, 0).await;

    let plan = neb::migration::MigrationPlan::default();
    neb::migration::migrate_slot(&migrator, slot, donor, recipient, &plan)
        .await
        .expect("handover");
    // The trigger, not the defect: the coordinator still names the donor.
    let coordinator_view = fixture.servers[0].consh.slot_override(slot as u64);
    assert_eq!(
        coordinator_view,
        Some(donor),
        "coordinator {coordinator} must be stale"
    );
    assert_eq!(
        fixture.servers[1].consh.slot_override(slot as u64),
        Some(recipient)
    );
    assert_eq!(
        fixture.servers[2].consh.slot_override(slot as u64),
        Some(recipient)
    );

    let mut violations = Vec::new();

    // Between handover and reclaim.
    match rmw_until_committed(&fixture.txn, id, 100).await {
        Ok(committed) => {
            let at_owner = score_on(&fixture, recipient, id).await;
            if at_owner != Ok(committed) {
                violations.push(format!(
                    "before reclaim: commit acknowledged score {committed}, but the current \
                     owner holds {at_owner:?} (donor holds {:?})",
                    score_on(&fixture, donor, id).await
                ));
            }
        }
        Err(reason) => violations.push(format!("before reclaim: {reason}")),
    }

    let reclaim = neb::migration::reclaim_donor_copy(&migrator, slot, donor, recipient, &plan)
        .await
        .expect("reclaim");
    eprintln!(
        "reclaim: dropped={} carried_over={} retained={}",
        reclaim.dropped, reclaim.carried_over, reclaim.retained
    );

    // After the reclaim the donor no longer holds the cell.
    let before = score_on(&fixture, recipient, id).await;
    match rmw_until_committed(&fixture.txn, id, 100).await {
        Ok(committed) => {
            let at_owner = score_on(&fixture, recipient, id).await;
            if at_owner != Ok(committed) || before.as_ref().ok().map(|b| b + 1) != Some(committed) {
                violations.push(format!(
                    "after reclaim: owner held {before:?}, commit acknowledged score \
                     {committed}, owner now holds {at_owner:?}"
                ));
            }
        }
        Err(reason) => violations.push(format!("after reclaim: {reason}")),
    }

    drop(migrator);
    shut_down(fixture).await;
    assert!(
        violations.is_empty(),
        "stale coordinator {coordinator} misrouted transactions:\n{}",
        violations.join("\n")
    );
}

/// Two coordinators write one cell between the handover and the reclaim:
/// member 1 knows the new owner, member 0 does not. After the reclaim and a
/// full refresh, the final score must equal the number of acknowledged
/// commits. Each round uses a new cell in a new slot.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "open defect: a stale and a current coordinator write two copies of a slot \
            after a migration, and the reclaim keeps one copy, so acknowledged commits \
            are lost"]
async fn stale_and_current_coordinators_lose_no_acknowledged_commit() {
    let _ = env_logger::try_init();
    let (fixture, sids, migrator) =
        stale_coordinator_cluster(54_620, "occ_stale_coordinator_lost").await;
    let (recipient, donor) = (sids[1], sids[2]);
    let current = neb::server::transactions::new_async_client_for_database(
        &fixture.addresses[1],
        fixture.client.group_name(),
        fixture.client.database_name(),
    )
    .await
    .expect("transaction client on member 1");
    let stale = fixture.txn.clone();
    let plan = neb::migration::MigrationPlan::default();
    let rounds: u64 = std::env::var("STALE_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20);

    let (mut total_acked, mut total_lost, mut lossy_rounds) = (0u64, 0u64, 0u64);
    let mut failures = Vec::new();
    for round in 0..rounds {
        let id = fixture
            .ids_placed_on_server(donor, 1, 4_100_000 + round)
            .await[0];
        let slot = neb::slots::slot_of(&id);
        fixture.seed_counter(id, 0).await;
        neb::migration::migrate_slot(&migrator, slot, donor, recipient, &plan)
            .await
            .expect("handover");
        assert_eq!(
            fixture.servers[0].consh.slot_override(slot as u64),
            Some(donor)
        );
        assert_eq!(
            fixture.servers[1].consh.slot_override(slot as u64),
            Some(recipient)
        );

        // Different counts per round, so that each copy is the newer one in
        // some rounds.
        let stale_target = [5u64, 10, 15][(round % 3) as usize];
        let current_target = 10u64;
        let worker = |txn: Arc<TxnClient>, target: u64| {
            tokio::spawn(async move {
                let (mut acked, mut errors) = (0u64, Vec::new());
                while acked < target {
                    match rmw_until_committed(&txn, id, 1_000).await {
                        Ok(_) => acked += 1,
                        Err(reason) => {
                            errors.push(reason);
                            break;
                        }
                    }
                }
                (acked, errors)
            })
        };
        let stale_task = worker(stale.clone(), stale_target);
        let current_task = worker(current.clone(), current_target);
        let (stale_acked, stale_errors) = stale_task.await.expect("stale worker");
        let (current_acked, current_errors) = current_task.await.expect("current worker");
        failures.extend(
            stale_errors
                .into_iter()
                .map(|e| format!("round {round} stale: {e}")),
        );
        failures.extend(
            current_errors
                .into_iter()
                .map(|e| format!("round {round} current: {e}")),
        );
        let donor_copy = score_on(&fixture, donor, id).await;
        let owner_copy = score_on(&fixture, recipient, id).await;

        let reclaim = neb::migration::reclaim_donor_copy(&migrator, slot, donor, recipient, &plan)
            .await
            .expect("reclaim");
        for server in &fixture.servers {
            assert!(server.refresh_slot_placement().await);
        }
        fixture.client.reload_slot_owners().await;
        let final_score = fixture.score(id).await;
        let acked = stale_acked + current_acked;
        let lost = acked.saturating_sub(final_score);
        total_acked += acked;
        total_lost += lost;
        if lost > 0 {
            lossy_rounds += 1;
        }
        eprintln!(
            "EXP round {round} slot {slot}: acked stale={stale_acked} current={current_acked} \
             before reclaim donor={donor_copy:?} owner={owner_copy:?} reclaim \
             dropped={} carried_over={} retained={} final={final_score} lost={lost}",
            reclaim.dropped, reclaim.carried_over, reclaim.retained
        );
    }
    eprintln!(
        "EXP summary: rounds={rounds} acked={total_acked} lost={total_lost} \
         lossy_rounds={lossy_rounds} failures={}",
        failures.len()
    );

    drop(migrator);
    drop(stale);
    drop(current);
    shut_down(fixture).await;
    assert!(failures.is_empty(), "transaction failures: {failures:#?}");
    assert_eq!(
        total_lost, 0,
        "{total_lost} of {total_acked} acknowledged commits lost in {lossy_rounds} of {rounds} rounds"
    );
}

/// One coordinator writes one cell while the cell's slot moves. The
/// coordinator is on the donor, so the migration tells it the new owner at
/// the handover: no member is stale here. After the reclaim and a full
/// refresh, the final score must equal the number of acknowledged commits.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "open defect: a slot handover does not stop the donor before the recipient \
            serves, so the recipient serves a stale copy, and the reclaim drops the \
            commits made on it"]
async fn writes_during_a_handover_lose_no_acknowledged_commit() {
    let _ = env_logger::try_init();
    let (fixture, sids, migrator) =
        stale_coordinator_cluster(54_640, "occ_handover_writes_lost").await;
    let (recipient, donor) = (sids[1], sids[2]);
    let on_donor = neb::server::transactions::new_async_client_for_database(
        &fixture.addresses[2],
        fixture.client.group_name(),
        fixture.client.database_name(),
    )
    .await
    .expect("transaction client on member 2");
    let plan = neb::migration::MigrationPlan::default();
    let rounds: u64 = std::env::var("STALE_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20);

    let (mut total_acked, mut total_lost, mut lossy_rounds) = (0u64, 0u64, 0u64);
    let mut failures = Vec::new();
    for round in 0..rounds {
        let id = fixture
            .ids_placed_on_server(donor, 1, 4_200_000 + round)
            .await[0];
        let slot = neb::slots::slot_of(&id);
        fixture.seed_counter(id, 0).await;
        let handed_over = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let txn = on_donor.clone();
            let handed_over = handed_over.clone();
            tokio::spawn(async move {
                let (mut acked, mut after, mut errors) = (0u64, 0u64, Vec::new());
                // Write until 10 commits are acknowledged after the handover.
                while after < 10 {
                    match rmw_until_committed(&txn, id, 1_000).await {
                        Ok(_) => {
                            acked += 1;
                            if handed_over.load(std::sync::atomic::Ordering::SeqCst) {
                                after += 1;
                            }
                        }
                        Err(reason) => {
                            errors.push(reason);
                            break;
                        }
                    }
                }
                (acked, errors)
            })
        };
        tokio::time::sleep(Duration::from_millis(2)).await;
        neb::migration::migrate_slot(&migrator, slot, donor, recipient, &plan)
            .await
            .expect("handover");
        handed_over.store(true, std::sync::atomic::Ordering::SeqCst);
        let (acked, errors) = writer.await.expect("writer");
        failures.extend(errors.into_iter().map(|e| format!("round {round}: {e}")));
        let donor_copy = score_on(&fixture, donor, id).await;
        let owner_copy = score_on(&fixture, recipient, id).await;
        let reclaim = neb::migration::reclaim_donor_copy(&migrator, slot, donor, recipient, &plan)
            .await
            .expect("reclaim");
        for server in &fixture.servers {
            assert!(server.refresh_slot_placement().await);
        }
        fixture.client.reload_slot_owners().await;
        let final_score = fixture.score(id).await;
        let lost = acked.saturating_sub(final_score);
        total_acked += acked;
        total_lost += lost;
        if lost > 0 {
            lossy_rounds += 1;
        }
        eprintln!(
            "EXP handover round {round} slot {slot}: acked={acked} before reclaim \
             donor={donor_copy:?} owner={owner_copy:?} reclaim dropped={} carried_over={} \
             retained={} final={final_score} lost={lost}",
            reclaim.dropped, reclaim.carried_over, reclaim.retained
        );
    }
    eprintln!(
        "EXP handover summary: rounds={rounds} acked={total_acked} lost={total_lost} \
         lossy_rounds={lossy_rounds} failures={}",
        failures.len()
    );

    drop(migrator);
    drop(on_donor);
    shut_down(fixture).await;
    assert!(failures.is_empty(), "transaction failures: {failures:#?}");
    assert_eq!(
        total_lost, 0,
        "{total_lost} of {total_acked} acknowledged commits lost in {lossy_rounds} of {rounds} rounds"
    );
}

/// The plain-write form of `writes_during_a_handover_lose_no_acknowledged_commit`.
/// One client writes one cell with plain read and update calls while the
/// cell's slot moves. The client runs the migration, so it learns the new
/// owner at the handover. The final score must equal the number of
/// acknowledged updates.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "open defect: a slot handover does not stop the donor before the recipient \
            serves, so the recipient serves a stale copy, and the reclaim drops the \
            updates made on it"]
async fn plain_writes_during_a_handover_lose_no_acknowledged_update() {
    use dovahkiin::types::Map as _;
    let _ = env_logger::try_init();
    let (fixture, sids, migrator) =
        stale_coordinator_cluster(54_660, "occ_handover_plain_lost").await;
    let (recipient, donor) = (sids[1], sids[2]);
    let plan = neb::migration::MigrationPlan::default();
    let rounds: u64 = std::env::var("STALE_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20);

    let (mut total_acked, mut total_lost, mut lossy_rounds) = (0u64, 0u64, 0u64);
    let mut failures = Vec::new();
    for round in 0..rounds {
        let id = fixture
            .ids_placed_on_server(donor, 1, 4_300_000 + round)
            .await[0];
        let slot = neb::slots::slot_of(&id);
        fixture.seed_counter(id, 0).await;
        migrator.reload_slot_owners().await;
        let handed_over = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let client = migrator.clone();
            let handed_over = handed_over.clone();
            tokio::spawn(async move {
                let (mut acked, mut after, mut errors) = (0u64, 0u64, Vec::new());
                while after < 10 {
                    let mut cell = match client.read_cell(id).await {
                        Ok(Ok(cell)) => cell,
                        other => {
                            errors.push(format!("read: {other:?}"));
                            break;
                        }
                    };
                    let next = cell.data["score"].u64().copied().unwrap() + 1;
                    let mut map = match &cell.data {
                        neb::ram::types::OwnedValue::Map(map) => map.owned(),
                        other => panic!("not a map: {other:?}"),
                    };
                    *map.get_mut("score") = neb::ram::types::OwnedValue::U64(next);
                    cell.data = neb::ram::types::OwnedValue::Map(map);
                    match client.update_cell(cell).await {
                        Ok(Ok(_)) => {
                            acked += 1;
                            if handed_over.load(std::sync::atomic::Ordering::SeqCst) {
                                after += 1;
                            }
                        }
                        other => {
                            errors.push(format!("update: {other:?}"));
                            break;
                        }
                    }
                }
                (acked, errors)
            })
        };
        tokio::time::sleep(Duration::from_millis(2)).await;
        neb::migration::migrate_slot(&migrator, slot, donor, recipient, &plan)
            .await
            .expect("handover");
        handed_over.store(true, std::sync::atomic::Ordering::SeqCst);
        let (acked, errors) = writer.await.expect("writer");
        failures.extend(errors.into_iter().map(|e| format!("round {round}: {e}")));
        let donor_copy = score_on(&fixture, donor, id).await;
        let owner_copy = score_on(&fixture, recipient, id).await;
        let reclaim = neb::migration::reclaim_donor_copy(&migrator, slot, donor, recipient, &plan)
            .await
            .expect("reclaim");
        for server in &fixture.servers {
            assert!(server.refresh_slot_placement().await);
        }
        fixture.client.reload_slot_owners().await;
        let final_score = fixture.score(id).await;
        let lost = acked.saturating_sub(final_score);
        total_acked += acked;
        total_lost += lost;
        if lost > 0 {
            lossy_rounds += 1;
        }
        eprintln!(
            "EXP plain round {round} slot {slot}: acked={acked} before reclaim \
             donor={donor_copy:?} owner={owner_copy:?} reclaim dropped={} carried_over={} \
             retained={} final={final_score} lost={lost}",
            reclaim.dropped, reclaim.carried_over, reclaim.retained
        );
    }
    eprintln!(
        "EXP plain summary: rounds={rounds} acked={total_acked} lost={total_lost} \
         lossy_rounds={lossy_rounds} failures={}",
        failures.len()
    );

    drop(migrator);
    shut_down(fixture).await;
    assert!(failures.is_empty(), "plain write failures: {failures:#?}");
    assert_eq!(
        total_lost, 0,
        "{total_lost} of {total_acked} acknowledged updates lost in {lossy_rounds} of {rounds} rounds"
    );
}
