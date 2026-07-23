mod occ_support;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId, Criterion,
    SamplingMode, Throughput,
};
use neb::ram::types::Id;
use tokio::runtime::Runtime;

use occ_support::{
    fixture::{counter_cell, OccFixture, PortPlan},
    metrics::RunReport,
    workloads::{
        run_blind_remove_batch, run_blind_update_batch, run_fixed_success_rmw,
        run_projected_read_batch, BatchSpec, ProjectionMode, TimedBatch,
    },
};

const OCC_SAMPLE_SIZE: usize = 10;
const OCC_MEASUREMENT_SECONDS: u64 = 10;

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

fn seed(runtime: &Runtime, fixture: &OccFixture, ids: &[Id], payload_bytes: usize) {
    runtime.block_on(async {
        for id in ids {
            fixture
                .client
                .write_cell(counter_cell(fixture.schema.id, *id, 0, payload_bytes))
                .await
                .expect("seed OCC benchmark counter RPC")
                .expect("seed OCC benchmark counter");
        }
    });
}

fn finish_fixture(runtime: Runtime, fixture: Arc<OccFixture>) {
    let fixture = Arc::try_unwrap(fixture)
        .unwrap_or_else(|_| panic!("OCC benchmark fixture retained shared owners during shutdown"));
    runtime.block_on(fixture.shutdown());
    runtime.shutdown_background();
}

fn occ_group<'a>(criterion: &'a mut Criterion, name: &str) -> BenchmarkGroup<'a, WallTime> {
    let mut group = criterion.benchmark_group(name);
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(OCC_SAMPLE_SIZE);
    group.measurement_time(Duration::from_secs(OCC_MEASUREMENT_SECONDS));
    group.throughput(Throughput::Elements(1));
    group
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

criterion_group!(
    occ_transactions,
    independent_rmw,
    hot_rmw,
    multi_cell,
    multi_participant,
    projected_reads,
    blind_mutations
);
criterion_main!(occ_transactions);
