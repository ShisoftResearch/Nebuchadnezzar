mod occ_support;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use bifrost::hlc::HlcSource;
use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId, Criterion,
    SamplingMode, Throughput,
};
use neb::ram::types::Id;
use tokio::runtime::Runtime;

use occ_support::{
    fixture::{counter_cell, OccFixture, PortPlan, RetainedRevision},
    metrics::RunReport,
    workloads::{
        build_history_chain, hold_old_snapshot_across_newer_writes, run_blind_remove_batch,
        run_blind_update_batch, run_expired_snapshot_read_batch, run_fixed_success_rmw,
        run_fresh_cleaner_reader_contention_batch, run_fresh_cleaner_relocation_batch,
        run_held_snapshot_read_batch, run_hlc_allocation_batch, run_non_transactional_read_batch,
        run_projected_read_batch, run_read_only_current_batch,
        run_storage_bounded_non_transactional_update_batch, run_visible_history_read_batch,
        BatchSpec, ProjectionMode, TimedBatch,
    },
};

const OCC_SAMPLE_SIZE: usize = 10;
const OCC_WARMUP_SECONDS: u64 = 3;
const OCC_MEASUREMENT_SECONDS: u64 = 10;
const CLEANER_SAMPLE_BOUND_NANOS: u64 = 1;

const REQUIRED_MVCC_SCENARIOS: &[&str] = &[
    "mvcc/non_transactional_read",
    "mvcc/non_transactional_update",
    "mvcc/read_only_current",
    "mvcc/rmw_one_cell",
    "mvcc/rmw_multi_cell",
    "mvcc/multi_participant",
    "mvcc/blind_update",
    "mvcc/blind_remove",
    "mvcc/full_read",
    "mvcc/selected_read",
    "mvcc/head_read",
    "mvcc/partial_read",
    "mvcc/history_depth_1",
    "mvcc/history_depth_8",
    "mvcc/history_depth_32",
    "mvcc/hot_cell_old_snapshot",
    "mvcc/history_expiration",
    "mvcc/cleaner_retained_revisions",
    "mvcc/cleaner_reader_contention",
    "mvcc/hlc_contention",
];

static RUN_REPORT: OnceLock<Mutex<RunReport>> = OnceLock::new();

fn report() -> &'static Mutex<RunReport> {
    RUN_REPORT.get_or_init(|| {
        Mutex::new(RunReport::new(
            std::env::var("NEB_OCC_BENCH_LABEL").unwrap_or_else(|_| "unlabelled".to_string()),
            std::env::var("NEB_OCC_BENCH_REVISION").unwrap_or_else(|_| "unknown".to_string()),
        ))
    })
}

fn report_path(label: &str) -> PathBuf {
    PathBuf::from("target/occ-bench").join(format!("{label}.json"))
}

#[cfg(feature = "occ_phase_profile")]
fn reset_phase_profile() {
    neb::server::transactions::phase_profile::reset()
        .unwrap_or_else(|active| panic!("reset OCC phase profile with active guards: {active:?}"));
}

#[cfg(not(feature = "occ_phase_profile"))]
fn reset_phase_profile() {}

#[cfg(feature = "occ_phase_profile")]
fn snapshot_phase_profile(summary: &mut occ_support::metrics::ScenarioSummary) {
    let snapshot = neb::server::transactions::phase_profile::snapshot();
    summary
        .attach_phase_snapshot(&snapshot)
        .unwrap_or_else(|error| match error {
            occ_support::metrics::PhaseSnapshotError::ActiveGuards(active_guards) => {
                panic!("snapshot OCC phase profile with active guards: {active_guards}")
            }
        });
}

#[cfg(not(feature = "occ_phase_profile"))]
fn snapshot_phase_profile(_: &mut occ_support::metrics::ScenarioSummary) {}

fn publish(scenario: &str, batch: TimedBatch) -> Duration {
    let elapsed = batch.elapsed;
    let mut summary = batch.metrics.summary(elapsed);
    snapshot_phase_profile(&mut summary);
    let mut report = report().lock().expect("lock OCC benchmark report");
    report.record(scenario, summary.clone());
    let path = report_path(&report.label);
    report
        .write_json(&path)
        .unwrap_or_else(|error| panic!("write OCC benchmark report {}: {error}", path.display()));
    drop(report);

    assert!(
        summary.invariants_passed,
        "OCC benchmark scenario {scenario} violated invariants: {:?}",
        summary.unexpected
    );
    elapsed
}

fn publish_mvcc(
    scenario: &str,
    batch: TimedBatch,
    fixture: &OccFixture,
    retained_revisions: &[RetainedRevision],
) -> Duration {
    let elapsed = batch.elapsed;
    let mut summary = batch.metrics.summary(elapsed);
    let telemetry = fixture.retention_telemetry(retained_revisions);
    summary.retained_revisions = telemetry.retained_revisions;
    summary.retained_bytes = telemetry.retained_bytes;
    summary.segment_count = telemetry.segment_count;
    snapshot_phase_profile(&mut summary);
    let mut report = report().lock().expect("lock MVCC benchmark report");
    report.record(scenario, summary.clone());
    let path = report_path(&report.label);
    report
        .write_json(&path)
        .unwrap_or_else(|error| panic!("write MVCC benchmark report {}: {error}", path.display()));
    drop(report);

    assert!(
        summary.invariants_passed,
        "MVCC benchmark scenario {scenario} violated invariants: {:?}",
        summary.unexpected
    );
    elapsed
}

fn seed(runtime: &Runtime, fixture: &OccFixture, ids: &[Id], payload_bytes: usize) {
    runtime.block_on(async {
        let mut revisions = Vec::with_capacity(ids.len());
        for id in ids {
            let header = fixture
                .client
                .write_cell(counter_cell(fixture.schema.id, *id, 0, payload_bytes))
                .await
                .expect("seed OCC benchmark counter RPC")
                .expect("seed OCC benchmark counter");
            revisions.push(header.revision_ts);
        }
        if fixture.servers.len() > 1 {
            fixture.observe_distributed_seed_revisions(revisions);
            fixture.assert_transactional_visibility(ids).await;
        }
    });
}

fn seed_cleaner_ids(
    runtime: &Runtime,
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
) -> (Arc<Vec<Id>>, Arc<Vec<Id>>) {
    assert_eq!(ids.len(), 48, "cleaner history requires 48 routed ids");
    let target_ids = Arc::new(ids.iter().step_by(2).copied().collect::<Vec<_>>());
    let sacrificial_ids = Arc::new(ids.iter().skip(1).step_by(2).copied().collect::<Vec<_>>());
    for (target, sacrificial) in target_ids.iter().zip(sacrificial_ids.iter()) {
        for id in [target, sacrificial] {
            runtime
                .block_on(fixture.client.write_cell(counter_cell(
                    fixture.schema.id,
                    *id,
                    0,
                    512 * 1024,
                )))
                .expect("seed cleaner benchmark counter RPC")
                .expect("seed cleaner benchmark counter");
        }
    }
    (target_ids, sacrificial_ids)
}

fn prepare_cleaner_fixture(
    runtime: &Runtime,
    plan: PortPlan,
    slot: u16,
    group: &str,
    id_start: u64,
) -> (Arc<OccFixture>, Arc<Vec<Id>>, Arc<Vec<Id>>) {
    let fixture = Arc::new(runtime.block_on(OccFixture::single_with_history_retention(
        plan.single(slot),
        group,
        2_000,
    )));
    fixture.servers[0].cleaner().pause();
    let server_id = fixture.servers[0].server_id;
    let ids = Arc::new(
        fixture
            .ids_for_server(server_id, 512, id_start)
            .into_iter()
            .filter(|id| id.higher % 4 == 0)
            .take(48)
            .collect::<Vec<_>>(),
    );
    assert_eq!(ids.len(), 48, "prepare cleaner benchmark ids");
    let (target_ids, sacrificial_ids) = seed_cleaner_ids(runtime, fixture.clone(), ids);
    (fixture, target_ids, sacrificial_ids)
}

fn shutdown_fixture(runtime: &Runtime, fixture: Arc<OccFixture>) {
    let fixture = Arc::try_unwrap(fixture)
        .unwrap_or_else(|_| panic!("OCC benchmark fixture retained shared owners during shutdown"));
    runtime.block_on(fixture.shutdown());
}

fn finish_fixture(runtime: Runtime, fixture: Arc<OccFixture>) {
    let fixture = Arc::try_unwrap(fixture)
        .unwrap_or_else(|_| panic!("OCC benchmark fixture retained shared owners during shutdown"));
    runtime.block_on(fixture.shutdown());
    runtime.shutdown_background();
}

fn occ_group<'a>(criterion: &'a mut Criterion, name: &str) -> BenchmarkGroup<'a, WallTime> {
    let mut group = criterion.benchmark_group(name);
    configure_standard_sampling(&mut group);
    group.throughput(Throughput::Elements(1));
    group
}

fn configure_standard_sampling(group: &mut BenchmarkGroup<'_, WallTime>) {
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(OCC_SAMPLE_SIZE);
    group.warm_up_time(Duration::from_secs(OCC_WARMUP_SECONDS));
    group.measurement_time(Duration::from_secs(OCC_MEASUREMENT_SECONDS));
}

fn configure_bounded_cleaner_sampling(group: &mut BenchmarkGroup<'_, WallTime>) {
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(OCC_SAMPLE_SIZE);
    group.warm_up_time(Duration::from_nanos(CLEANER_SAMPLE_BOUND_NANOS));
    group.measurement_time(Duration::from_nanos(CLEANER_SAMPLE_BOUND_NANOS));
}

fn register_rmw(
    criterion: &mut Criterion,
    runtime: &Runtime,
    group_name: &str,
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
    cases: &[(usize, usize)],
) {
    let mut group = occ_group(criterion, group_name);

    for &(concurrency, cells_per_txn) in cases {
        let fixture = fixture.clone();
        let ids = ids.clone();
        let scenario = format!("{group_name}/{concurrency}");
        group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &(concurrency, cells_per_txn),
            |bench, &(concurrency, cells_per_txn)| {
                let fixture = fixture.clone();
                let ids = ids.clone();
                let scenario = scenario.clone();
                bench.iter_custom(move |iterations| {
                    reset_phase_profile();
                    let batch = runtime.block_on(run_fixed_success_rmw(
                        fixture.clone(),
                        ids.clone(),
                        BatchSpec {
                            successes: iterations.max(1),
                            concurrency,
                            cells_per_txn,
                        },
                    ));
                    publish(&scenario, batch)
                });
            },
        );
    }

    group.finish();
}

fn run_single_server_rmw(
    criterion: &mut Criterion,
    group_name: &str,
    slot: u16,
    fixture_group: &str,
    id_start: u64,
    id_count: usize,
    payload_bytes: usize,
    cases: &[(usize, usize)],
) {
    let runtime = Runtime::new().expect("create OCC benchmark runtime");
    let plan = PortPlan::new(occ_support::base_port());
    let fixture = Arc::new(runtime.block_on(OccFixture::single(plan.single(slot), fixture_group)));
    let server_id = fixture.servers[0].server_id;
    let ids = Arc::new(fixture.ids_for_server(server_id, id_count, id_start));
    seed(&runtime, &fixture, ids.as_ref(), payload_bytes);
    register_rmw(criterion, &runtime, group_name, fixture.clone(), ids, cases);
    finish_fixture(runtime, fixture);
}

fn independent_rmw(criterion: &mut Criterion) {
    run_single_server_rmw(
        criterion,
        "occ/independent_rmw",
        0,
        "occ_independent_rmw",
        1_000_000,
        4_096,
        0,
        &[(1, 1), (8, 1), (32, 1)],
    );
}

fn hot_rmw(criterion: &mut Criterion) {
    run_single_server_rmw(
        criterion,
        "occ/hot_rmw",
        1,
        "occ_hot_rmw",
        2_000_000,
        1,
        0,
        &[(8, 1), (32, 1)],
    );
}

fn multi_cell(criterion: &mut Criterion) {
    run_single_server_rmw(
        criterion,
        "occ/multi_cell",
        2,
        "occ_multi_cell",
        3_000_000,
        4_096,
        0,
        &[(1, 8), (8, 8)],
    );
}

fn multi_participant(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("create OCC benchmark runtime");
    let plan = PortPlan::new(occ_support::base_port());
    let fixture = Arc::new(runtime.block_on(OccFixture::cluster(
        plan.cluster(3),
        "occ_multi_participant",
    )));
    let ids = Arc::new(
        fixture
            .servers
            .iter()
            .enumerate()
            .flat_map(|(index, server)| {
                fixture.ids_for_server(server.server_id, 1, 4_000_000 + index as u64 * 10_000)
            })
            .collect::<Vec<_>>(),
    );
    seed(&runtime, &fixture, ids.as_ref(), 0);
    register_rmw(
        criterion,
        &runtime,
        "occ/multi_participant",
        fixture.clone(),
        ids,
        &[(1, 3), (4, 3)],
    );
    finish_fixture(runtime, fixture);
}

fn projected_reads(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("create OCC benchmark runtime");
    let plan = PortPlan::new(occ_support::base_port());
    let fixture =
        Arc::new(runtime.block_on(OccFixture::single(plan.single(4), "occ_projected_reads")));
    let server_id = fixture.servers[0].server_id;
    let ids = Arc::new(fixture.ids_for_server(server_id, 128, 5_000_000));
    seed(&runtime, &fixture, ids.as_ref(), 64 * 1024);

    let runtime_ref = &runtime;
    let mut group = occ_group(criterion, "occ/projected_reads");
    for (name, mode) in [
        ("head", ProjectionMode::Head),
        ("selected", ProjectionMode::Selected),
        ("mixed", ProjectionMode::Mixed),
    ] {
        let fixture = fixture.clone();
        let ids = ids.clone();
        let scenario = format!("occ/projected_reads/{name}");
        group.bench_with_input(BenchmarkId::from_parameter(name), &mode, |bench, &mode| {
            let fixture = fixture.clone();
            let ids = ids.clone();
            let scenario = scenario.clone();
            bench.iter_custom(move |iterations| {
                reset_phase_profile();
                let batch = runtime_ref.block_on(run_projected_read_batch(
                    fixture.clone(),
                    ids.clone(),
                    iterations.max(1),
                    mode,
                ));
                publish(&scenario, batch)
            });
        });
    }
    group.finish();

    finish_fixture(runtime, fixture);
}

fn blind_mutations(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("create OCC benchmark runtime");
    let plan = PortPlan::new(occ_support::base_port());
    let fixture =
        Arc::new(runtime.block_on(OccFixture::single(plan.single(5), "occ_blind_mutations")));
    let server_id = fixture.servers[0].server_id;
    let update_ids = Arc::new(fixture.ids_for_server(server_id, 128, 6_000_000));
    let remove_ids = Arc::new(fixture.ids_for_server(server_id, 128, 7_000_000));
    seed(&runtime, &fixture, update_ids.as_ref(), 0);

    let runtime_ref = &runtime;
    {
        let mut group = occ_group(criterion, "occ/blind_update");
        let fixture = fixture.clone();
        let ids = update_ids.clone();
        group.bench_with_input(BenchmarkId::from_parameter(1), &(), |bench, _| {
            let fixture = fixture.clone();
            let ids = ids.clone();
            bench.iter_custom(move |iterations| {
                reset_phase_profile();
                let batch = runtime_ref.block_on(run_blind_update_batch(
                    fixture.clone(),
                    ids.clone(),
                    iterations.max(1),
                ));
                publish("occ/blind_update/1", batch)
            });
        });
        group.finish();
    }

    {
        let mut group = occ_group(criterion, "occ/blind_remove");
        let fixture = fixture.clone();
        let ids = remove_ids.clone();
        group.bench_with_input(BenchmarkId::from_parameter(1), &(), |bench, _| {
            let fixture = fixture.clone();
            let ids = ids.clone();
            bench.iter_custom(move |iterations| {
                reset_phase_profile();
                let batch = runtime_ref.block_on(run_blind_remove_batch(
                    fixture.clone(),
                    ids.clone(),
                    iterations.max(1),
                ));
                publish("occ/blind_remove/1", batch)
            });
        });
        group.finish();
    }

    drop(update_ids);
    drop(remove_ids);
    finish_fixture(runtime, fixture);
}

fn registered_mvcc_scenarios() -> &'static [&'static str] {
    REQUIRED_MVCC_SCENARIOS
}

fn mvcc_portfolio(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("create MVCC benchmark runtime");
    let plan = PortPlan::new(occ_support::base_port());
    let fixture = Arc::new(runtime.block_on(OccFixture::single(plan.single(10), "mvcc_portfolio")));
    let server_id = fixture.servers[0].server_id;
    let ids = Arc::new(fixture.ids_for_server(server_id, 256, 8_000_000));
    // Direct updates retain every revision for the normal five-minute window;
    // keep this dedicated portfolio's counter payload minimal.
    seed(&runtime, &fixture, ids.as_ref(), 0);
    let remove_ids = Arc::new(fixture.ids_for_server(server_id, 256, 8_300_000));

    let direct_fixture = Arc::new(runtime.block_on(OccFixture::single_with_history_retention(
        plan.single(14),
        "mvcc_direct_updates",
        1,
    )));
    let direct_server_id = direct_fixture.servers[0].server_id;
    let direct_ids = Arc::new(direct_fixture.ids_for_server(direct_server_id, 256, 8_400_000));
    seed(&runtime, &direct_fixture, direct_ids.as_ref(), 0);

    let retained_fixture = Arc::new(runtime.block_on(OccFixture::single_with_history_retention(
        plan.single(11),
        "mvcc_history_retention",
        50,
    )));
    let retained_server_id = retained_fixture.servers[0].server_id;
    let retained_ids = Arc::new(retained_fixture.ids_for_server(retained_server_id, 8, 8_500_000));
    seed(&runtime, &retained_fixture, retained_ids.as_ref(), 0);

    // These scenarios deliberately measure reads of a fixed history state.  Build
    // that state before Criterion starts its timer, and report the exact retained
    // revisions that the helpers established rather than an operation-count proxy.
    let history_depth_1 =
        Arc::new(runtime.block_on(build_history_chain(fixture.clone(), ids[0], 1)));
    let history_depth_8 =
        Arc::new(runtime.block_on(build_history_chain(fixture.clone(), ids[1], 8)));
    let history_depth_32 =
        Arc::new(runtime.block_on(build_history_chain(fixture.clone(), ids[2], 32)));
    let held_old_snapshot = Arc::new(runtime.block_on(hold_old_snapshot_across_newer_writes(
        fixture.clone(),
        ids[3],
        32,
    )));
    let expired_history = Arc::new(runtime.block_on(async {
        let chain = build_history_chain(retained_fixture.clone(), retained_ids[0], 8).await;
        retained_fixture.wait_for_history_expiration(&chain).await;
        chain
    }));

    let cluster_fixture = Arc::new(runtime.block_on(OccFixture::cluster(
        plan.cluster(12),
        "mvcc_multi_participant",
    )));
    let cluster_ids = Arc::new(
        cluster_fixture
            .servers
            .iter()
            .enumerate()
            .flat_map(|(index, server)| {
                cluster_fixture.ids_for_server(
                    server.server_id,
                    1,
                    8_700_000 + index as u64 * 10_000,
                )
            })
            .collect::<Vec<_>>(),
    );
    seed(&runtime, &cluster_fixture, cluster_ids.as_ref(), 0);
    let hlc_source = Arc::new(HlcSource::new(server_id));

    let runtime_ref = &runtime;
    let mut group = occ_group(criterion, "mvcc");
    for scenario in REQUIRED_MVCC_SCENARIOS {
        if matches!(
            *scenario,
            "mvcc/cleaner_retained_revisions" | "mvcc/cleaner_reader_contention"
        ) {
            configure_bounded_cleaner_sampling(&mut group);
        } else if *scenario == "mvcc/hlc_contention" {
            configure_standard_sampling(&mut group);
        }
        let fixture = fixture.clone();
        let ids = ids.clone();
        let remove_ids = remove_ids.clone();
        let direct_fixture = direct_fixture.clone();
        let direct_ids = direct_ids.clone();
        let retained_fixture = retained_fixture.clone();
        let cluster_fixture = cluster_fixture.clone();
        let cluster_ids = cluster_ids.clone();
        let history_depth_1 = history_depth_1.clone();
        let history_depth_8 = history_depth_8.clone();
        let history_depth_32 = history_depth_32.clone();
        let held_old_snapshot = held_old_snapshot.clone();
        let expired_history = expired_history.clone();
        let hlc_source = hlc_source.clone();
        let cleaner_setup = match *scenario {
            "mvcc/cleaner_retained_revisions" => Some(prepare_cleaner_fixture(
                &runtime,
                plan,
                15,
                "mvcc_cleaner_retained_revisions",
                8_900_000,
            )),
            "mvcc/cleaner_reader_contention" => Some(prepare_cleaner_fixture(
                &runtime,
                plan,
                16,
                "mvcc_cleaner_reader_contention",
                9_000_000,
            )),
            _ => None,
        };
        let cleaner_fixture = cleaner_setup
            .as_ref()
            .map(|(fixture, _, _)| fixture.clone());
        let cleaner_target_ids = cleaner_setup
            .as_ref()
            .map(|(_, target_ids, _)| target_ids.clone());
        let cleaner_sacrificial_ids = cleaner_setup
            .as_ref()
            .map(|(_, _, sacrificial_ids)| sacrificial_ids.clone());
        let cleaner_fixture_to_shutdown = cleaner_fixture.clone();
        let scenario = (*scenario).to_string();
        group.bench_function(
            BenchmarkId::from_parameter(scenario.trim_start_matches("mvcc/")),
            move |bench| {
                let fixture = fixture.clone();
                let ids = ids.clone();
                let remove_ids = remove_ids.clone();
                let direct_fixture = direct_fixture.clone();
                let direct_ids = direct_ids.clone();
                let retained_fixture = retained_fixture.clone();
                let cluster_fixture = cluster_fixture.clone();
                let cluster_ids = cluster_ids.clone();
                let history_depth_1 = history_depth_1.clone();
                let history_depth_8 = history_depth_8.clone();
                let history_depth_32 = history_depth_32.clone();
                let held_old_snapshot = held_old_snapshot.clone();
                let expired_history = expired_history.clone();
                let hlc_source = hlc_source.clone();
                let cleaner_fixture = cleaner_fixture.clone();
                let cleaner_target_ids = cleaner_target_ids.clone();
                let cleaner_sacrificial_ids = cleaner_sacrificial_ids.clone();
                let scenario = scenario.clone();
                bench.iter_custom(move |iterations| {
                    reset_phase_profile();
                    let operations = iterations;
                    let (batch, telemetry_fixture, retained_revisions) =
                        runtime_ref.block_on(async {
                            match scenario.as_str() {
                                "mvcc/non_transactional_read" => (
                                    run_non_transactional_read_batch(
                                        direct_fixture.clone(),
                                        direct_ids.clone(),
                                        operations,
                                    )
                                    .await,
                                    direct_fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/non_transactional_update" => (
                                    run_storage_bounded_non_transactional_update_batch(
                                        direct_fixture.clone(),
                                        direct_ids.clone(),
                                        operations,
                                    )
                                    .await,
                                    direct_fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/read_only_current" => (
                                    run_read_only_current_batch(
                                        fixture.clone(),
                                        ids.clone(),
                                        operations,
                                    )
                                    .await,
                                    fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/full_read" => (
                                    run_projected_read_batch(
                                        fixture.clone(),
                                        ids.clone(),
                                        operations,
                                        ProjectionMode::Full,
                                    )
                                    .await,
                                    fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/rmw_one_cell" => (
                                    run_fixed_success_rmw(
                                        fixture.clone(),
                                        ids.clone(),
                                        BatchSpec {
                                            successes: operations,
                                            concurrency: 1,
                                            cells_per_txn: 1,
                                        },
                                    )
                                    .await,
                                    fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/rmw_multi_cell" => (
                                    run_fixed_success_rmw(
                                        fixture.clone(),
                                        ids.clone(),
                                        BatchSpec {
                                            successes: operations,
                                            concurrency: 1,
                                            cells_per_txn: 8,
                                        },
                                    )
                                    .await,
                                    fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/multi_participant" => (
                                    run_fixed_success_rmw(
                                        cluster_fixture.clone(),
                                        cluster_ids.clone(),
                                        BatchSpec {
                                            successes: operations,
                                            concurrency: 1,
                                            cells_per_txn: 3,
                                        },
                                    )
                                    .await,
                                    cluster_fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/blind_update" => (
                                    run_blind_update_batch(
                                        fixture.clone(),
                                        ids.clone(),
                                        operations,
                                    )
                                    .await,
                                    fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/blind_remove" => (
                                    run_blind_remove_batch(
                                        fixture.clone(),
                                        remove_ids.clone(),
                                        operations,
                                    )
                                    .await,
                                    fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/selected_read" => (
                                    run_projected_read_batch(
                                        fixture.clone(),
                                        ids.clone(),
                                        operations,
                                        ProjectionMode::Selected,
                                    )
                                    .await,
                                    fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/head_read" => (
                                    run_projected_read_batch(
                                        fixture.clone(),
                                        ids.clone(),
                                        operations,
                                        ProjectionMode::Head,
                                    )
                                    .await,
                                    fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/partial_read" => (
                                    run_projected_read_batch(
                                        fixture.clone(),
                                        ids.clone(),
                                        operations,
                                        ProjectionMode::Mixed,
                                    )
                                    .await,
                                    fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/history_depth_1" => (
                                    run_visible_history_read_batch(
                                        fixture.clone(),
                                        history_depth_1.as_ref().clone(),
                                        operations,
                                    )
                                    .await,
                                    fixture.clone(),
                                    history_depth_1.predecessors.clone(),
                                ),
                                "mvcc/history_depth_8" => (
                                    run_visible_history_read_batch(
                                        fixture.clone(),
                                        history_depth_8.as_ref().clone(),
                                        operations,
                                    )
                                    .await,
                                    fixture.clone(),
                                    history_depth_8.predecessors.clone(),
                                ),
                                "mvcc/history_depth_32" => (
                                    run_visible_history_read_batch(
                                        fixture.clone(),
                                        history_depth_32.as_ref().clone(),
                                        operations,
                                    )
                                    .await,
                                    fixture.clone(),
                                    history_depth_32.predecessors.clone(),
                                ),
                                "mvcc/hot_cell_old_snapshot" => (
                                    run_held_snapshot_read_batch(
                                        fixture.clone(),
                                        held_old_snapshot.as_ref().clone(),
                                        operations,
                                    )
                                    .await,
                                    fixture.clone(),
                                    held_old_snapshot.history.predecessors.clone(),
                                ),
                                "mvcc/history_expiration" => (
                                    run_expired_snapshot_read_batch(
                                        retained_fixture.clone(),
                                        expired_history.as_ref().clone(),
                                        operations,
                                    )
                                    .await,
                                    retained_fixture.clone(),
                                    Vec::new(),
                                ),
                                "mvcc/cleaner_retained_revisions" => {
                                    let cleaner_fixture = cleaner_fixture
                                        .as_ref()
                                        .expect("cleaner retained fixture setup")
                                        .clone();
                                    let (batch, history) = run_fresh_cleaner_relocation_batch(
                                        cleaner_fixture.clone(),
                                        cleaner_target_ids
                                            .as_ref()
                                            .expect("cleaner retained target ids")
                                            .clone(),
                                        cleaner_sacrificial_ids
                                            .as_ref()
                                            .expect("cleaner retained sacrificial ids")
                                            .clone(),
                                        operations,
                                    )
                                    .await;
                                    (
                                        batch,
                                        cleaner_fixture,
                                        history.predecessors.as_ref().clone(),
                                    )
                                }
                                "mvcc/cleaner_reader_contention" => {
                                    let cleaner_fixture = cleaner_fixture
                                        .as_ref()
                                        .expect("cleaner reader fixture setup")
                                        .clone();
                                    let (batch, history) =
                                        run_fresh_cleaner_reader_contention_batch(
                                            cleaner_fixture.clone(),
                                            cleaner_target_ids
                                                .as_ref()
                                                .expect("cleaner reader target ids")
                                                .clone(),
                                            cleaner_sacrificial_ids
                                                .as_ref()
                                                .expect("cleaner reader sacrificial ids")
                                                .clone(),
                                            operations,
                                        )
                                        .await;
                                    (
                                        batch,
                                        cleaner_fixture,
                                        history.predecessors.as_ref().clone(),
                                    )
                                }
                                "mvcc/hlc_contention" => (
                                    run_hlc_allocation_batch(hlc_source.clone(), operations, 16),
                                    fixture.clone(),
                                    Vec::new(),
                                ),
                                other => panic!("unregistered MVCC benchmark scenario {other}"),
                            }
                        });
                    publish_mvcc(
                        &scenario,
                        batch,
                        telemetry_fixture.as_ref(),
                        retained_revisions.as_ref(),
                    )
                });
            },
        );
        drop(cleaner_setup);
        if let Some(cleaner_fixture) = cleaner_fixture_to_shutdown {
            shutdown_fixture(&runtime, cleaner_fixture);
        }
    }
    group.finish();

    runtime.block_on(held_old_snapshot.abort(fixture.as_ref()));

    finish_fixture(runtime, fixture);
    let runtime = Runtime::new().expect("create MVCC fixture shutdown runtime");
    finish_fixture(runtime, direct_fixture);
    let runtime = Runtime::new().expect("create MVCC retention fixture shutdown runtime");
    finish_fixture(runtime, retained_fixture);
    let runtime = Runtime::new().expect("create MVCC cluster shutdown runtime");
    finish_fixture(runtime, cluster_fixture);
}

criterion_group!(
    occ_transactions,
    independent_rmw,
    hot_rmw,
    multi_cell,
    multi_participant,
    projected_reads,
    blind_mutations,
    mvcc_portfolio
);
criterion_main!(occ_transactions);

#[cfg(test)]
mod mvcc_inventory_tests {
    use super::*;

    #[test]
    fn mvcc_workload_matrix_registers_every_required_scenario_once() {
        let registered = registered_mvcc_scenarios();
        assert_eq!(registered, REQUIRED_MVCC_SCENARIOS);
    }
}
