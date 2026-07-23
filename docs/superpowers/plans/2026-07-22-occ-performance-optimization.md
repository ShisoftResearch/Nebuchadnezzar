# OCC Transaction Performance Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a repeatable release-mode OCC benchmark, establish comparable baselines, and retain measured hot-path optimizations without weakening repeatable reads, version certification, lost-update prevention, or transaction cleanup guarantees.

**Architecture:** A Criterion benchmark drives transactions exclusively through the public client API against fixtures initialized outside timed regions. Support modules separate fixture lifecycle, workload semantics, and machine-readable outcome metrics. Production changes follow one hypothesis at a time and are retained only after the benchmark acceptance policy and all correctness gates pass.

**Tech Stack:** Rust 2021, Tokio, Criterion 0.5, Serde/serde_json, Nebuchadnezzar transaction RPC clients, Bifrost loopback RPC, Git worktrees.

---

## File Map

- Modify `Cargo.toml`: register the `occ_transactions` benchmark.
- Create `benches/occ_transactions.rs`: Criterion entry point and scenario registration.
- Create `benches/occ_support/mod.rs`: support-module exports and benchmark configuration.
- Create `benches/occ_support/metrics.rs`: outcome counters, latency summaries, JSON report writing, and stability calculations.
- Create `benches/occ_support/fixture.rs`: single-server and three-server fixtures, public schema installation, deterministic ownership selection, seeding, and shutdown.
- Create `benches/occ_support/workloads.rs`: transaction attempts, retry-until-success loops, scenario invariants, and workload batches.
- Create `tests/occ_bench_metrics.rs`: unit tests for benchmark metrics without starting servers.
- Modify `benches/README.md`: OCC benchmark commands, environment controls, comparison workflow, and acceptance thresholds.
- Modify `src/server/transactions/manager.rs`: prepare-payload reuse and projection/cache optimization candidates.
- Modify `src/server/transactions/data_site.rs`: linear prepare-operation validation candidate.

## Task 1: Implement Deterministic Benchmark Metrics

**Files:**
- Create: `benches/occ_support/metrics.rs`
- Create: `tests/occ_bench_metrics.rs`

- [ ] **Step 1: Write failing percentile and outcome tests**

Create `tests/occ_bench_metrics.rs`:

```rust
#[path = "../benches/occ_support/metrics.rs"]
mod metrics;

use metrics::{BatchMetrics, RunReport};
use std::time::Duration;

#[test]
fn nearest_rank_percentiles_are_deterministic() {
    let mut metrics = BatchMetrics::default();
    for millis in 1..=100 {
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
    metrics.record_success(Duration::from_millis(4), 3, 2);
    metrics.record_unexpected("rpc disconnected".to_string());

    let summary = metrics.summary(Duration::from_secs(2));
    assert_eq!(summary.committed, 1);
    assert_eq!(summary.attempts, 3);
    assert_eq!(summary.not_realizable, 2);
    assert_eq!(summary.commits_per_second, 0.5);
    assert_eq!(summary.unexpected, vec!["rpc disconnected"]);
    assert!(!summary.invariants_passed);
}

#[test]
fn run_report_replaces_a_scenario_by_name() {
    let mut report = RunReport::new("occ-initial", "deadbeef");
    let first = BatchMetrics::one_success(Duration::from_millis(2))
        .summary(Duration::from_secs(1));
    let second = BatchMetrics::one_success(Duration::from_millis(1))
        .summary(Duration::from_secs(1));
    report.record("independent/1", first);
    report.record("independent/1", second.clone());
    assert_eq!(report.scenarios.len(), 1);
    assert_eq!(report.scenarios["independent/1"], second);
}
```

- [ ] **Step 2: Run the metrics tests and verify RED**

Run:

```bash
cargo test --test occ_bench_metrics -- --nocapture
```

Expected: compilation fails because `benches/occ_support/metrics.rs` and its types do not exist.

- [ ] **Step 3: Implement metrics and atomic JSON output**

Create `benches/occ_support/metrics.rs` with these public types and methods:

```rust
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Debug, Default)]
pub struct BatchMetrics {
    latencies_ns: Vec<u64>,
    pub attempts: u64,
    pub committed: u64,
    pub not_realizable: u64,
    pub logical_retries: u64,
    pub unexpected: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ScenarioSummary {
    pub attempts: u64,
    pub committed: u64,
    pub not_realizable: u64,
    pub logical_retries: u64,
    pub commits_per_second: f64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub unexpected: Vec<String>,
    pub invariants_passed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunReport {
    pub label: String,
    pub revision: String,
    pub scenarios: BTreeMap<String, ScenarioSummary>,
}

impl BatchMetrics {
    pub fn one_success(latency: Duration) -> Self {
        let mut metrics = Self::default();
        metrics.record_success(latency, 1, 0);
        metrics
    }

    pub fn record_success(&mut self, latency: Duration, attempts: u64, retries: u64) {
        self.latencies_ns.push(latency.as_nanos() as u64);
        self.attempts += attempts;
        self.committed += 1;
        self.logical_retries += retries;
    }

    pub fn record_retryable(&mut self) {
        self.not_realizable += 1;
    }

    pub fn record_unexpected(&mut self, error: String) {
        self.unexpected.push(error);
    }

    pub fn merge(&mut self, mut other: Self) {
        self.latencies_ns.append(&mut other.latencies_ns);
        self.attempts += other.attempts;
        self.committed += other.committed;
        self.not_realizable += other.not_realizable;
        self.logical_retries += other.logical_retries;
        self.unexpected.append(&mut other.unexpected);
    }

    pub fn summary(mut self, elapsed: Duration) -> ScenarioSummary {
        self.latencies_ns.sort_unstable();
        ScenarioSummary {
            attempts: self.attempts,
            committed: self.committed,
            not_realizable: self.not_realizable,
            logical_retries: self.logical_retries,
            commits_per_second: self.committed as f64 / elapsed.as_secs_f64(),
            p50_ns: nearest_rank(&self.latencies_ns, 50),
            p95_ns: nearest_rank(&self.latencies_ns, 95),
            p99_ns: nearest_rank(&self.latencies_ns, 99),
            invariants_passed: self.unexpected.is_empty(),
            unexpected: self.unexpected,
        }
    }
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (percentile * sorted.len() + 99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

impl RunReport {
    pub fn new(label: impl Into<String>, revision: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            revision: revision.into(),
            scenarios: BTreeMap::new(),
        }
    }

    pub fn record(&mut self, name: impl Into<String>, summary: ScenarioSummary) {
        self.scenarios.insert(name.into(), summary);
    }

    pub fn write_json(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        fs::write(&temporary, bytes)?;
        fs::rename(temporary, path)
    }
}
```

- [ ] **Step 4: Run the metrics tests and verify GREEN**

Run:

```bash
cargo test --test occ_bench_metrics -- --nocapture
```

Expected: 3 passed, 0 failed.

- [ ] **Step 5: Commit the metrics layer**

```bash
git add benches/occ_support/metrics.rs tests/occ_bench_metrics.rs
git commit -m "bench(txn): add OCC outcome metrics"
```

## Task 2: Build Reusable Public-API Fixtures

**Files:**
- Create: `benches/occ_support/mod.rs`
- Create: `benches/occ_support/fixture.rs`
- Modify: `tests/occ_bench_metrics.rs`

- [ ] **Step 1: Add a failing deterministic port-plan test**

Extend `tests/occ_bench_metrics.rs`:

```rust
#[path = "../benches/occ_support/fixture.rs"]
mod fixture;

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
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test --test occ_bench_metrics benchmark_port_plan -- --exact
```

Expected: compilation fails because `PortPlan` does not exist.

- [ ] **Step 3: Create support exports and fixture types**

Create `benches/occ_support/mod.rs`:

```rust
pub mod fixture;
pub mod metrics;
pub mod workloads;

pub const DEFAULT_BASE_PORT: u16 = 39_400;

pub fn base_port() -> u16 {
    std::env::var("NEB_OCC_BENCH_BASE_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_BASE_PORT)
}
```

Create `benches/occ_support/fixture.rs` with this public surface and implementation rules:

```rust
use neb::client::AsyncClient;
use neb::ram::cell::OwnedCell;
use neb::ram::schema::{Field, Schema};
use neb::ram::segs::SEGMENT_SIZE;
use neb::ram::types::{Id, OwnedMap, OwnedValue};
use neb::server::transactions;
use neb::server::{NebServer, ServerOptions, Service};
use dovahkiin::types::Type;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub struct PortPlan {
    base: u16,
}

impl PortPlan {
    pub fn new(base: u16) -> Self {
        Self { base }
    }

    pub fn single_server(&self) -> String {
        self.single(0)
    }

    pub fn single(&self, slot: u16) -> String {
        format!("127.0.0.1:{}", self.base + slot * 10)
    }

    pub fn cluster(&self, slot: u16) -> Vec<String> {
        let first = self.base + slot * 10;
        (0..3)
            .map(|offset| format!("127.0.0.1:{}", first + offset))
            .collect()
    }
}

pub struct OccFixture {
    pub group: String,
    pub addresses: Vec<String>,
    pub servers: Vec<Arc<NebServer>>,
    pub client: Arc<AsyncClient>,
    pub txn: Arc<transactions::manager::AsyncServiceClient>,
    pub schema: Schema,
}

impl OccFixture {
    pub async fn single(address: String, group: &str) -> Self {
        let options = benchmark_server_options();
        let server = NebServer::new_from_opts(
            &options,
            &address,
            &group.to_string(),
            async |_| {},
        )
        .await
        .expect("start OCC benchmark server");
        Self::finish(group, vec![address], vec![server]).await
    }

    pub async fn cluster(addresses: Vec<String>, group: &str) -> Self {
        let options = benchmark_server_options();
        let mut servers = Vec::with_capacity(addresses.len());
        for address in &addresses {
            servers.push(
                NebServer::new_cluster_from_opts(
                    &options,
                    address,
                    &addresses,
                    group,
                    async |_| {},
                )
                .await
                .expect("start OCC benchmark cluster member"),
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        Self::finish(group, addresses, servers).await
    }

    async fn finish(
        group: &str,
        addresses: Vec<String>,
        servers: Vec<Arc<NebServer>>,
    ) -> Self {
        let client = Arc::new(
            AsyncClient::new(
                &servers[0].rpc,
                &servers[0].membership,
                &addresses,
                group,
            )
            .await
            .expect("create OCC benchmark client"),
        );
        let schema = Schema::new(
            "occ_benchmark",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("id", Type::I64),
                Field::new_unindexed("name", Type::String),
                Field::new_unindexed("score", Type::U64),
            ]),
            false,
            false,
        );
        let schema_id = client
            .new_schema(schema.clone())
            .await
            .expect("submit OCC benchmark schema")
            .expect("register OCC benchmark schema");
        let mut schema = schema;
        schema.id = schema_id;
        let txn = transactions::new_async_client_for_database(
            &addresses[0],
            group,
            group,
        )
        .await
        .expect("create scoped OCC benchmark transaction client");
        Self {
            group: group.to_string(),
            addresses,
            servers,
            client,
            txn,
            schema,
        }
    }

    pub async fn seed_counter(&self, id: Id, score: u64) {
        self.client
            .write_cell(counter_cell(self.schema.id, id, score, 0))
            .await
            .expect("send benchmark seed")
            .expect("write benchmark seed");
    }

    pub fn ids_for_server(&self, server_id: u64, count: usize, start: u64) -> Vec<Id> {
        let mut ids = Vec::with_capacity(count);
        let mut candidate = start;
        while ids.len() < count {
            let id = Id::new(candidate, candidate.rotate_left(17));
            if self.client.locate_server_id(&id).expect("locate benchmark ID") == server_id {
                ids.push(id);
            }
            candidate += 1;
        }
        ids
    }

    pub async fn shutdown(self) {
        for server in self.servers {
            server.shutdown().await;
        }
    }
}

pub fn counter_cell(schema: u32, id: Id, score: u64, payload_bytes: usize) -> OwnedCell {
    let mut map = OwnedMap::new();
    map.insert(&"id".to_string(), OwnedValue::I64(id.lower as i64));
    map.insert(&"score".to_string(), OwnedValue::U64(score));
    map.insert(
        &"name".to_string(),
        OwnedValue::String("x".repeat(payload_bytes.max(1))),
    );
    OwnedCell::new_with_id(schema, &id, OwnedValue::Map(map))
}

fn benchmark_server_options() -> ServerOptions {
    ServerOptions {
        chunk_size: SEGMENT_SIZE,
        db_size: SEGMENT_SIZE * 4,
        tiered_config: None,
        backup_storage: None,
        wal_storage: None,
        undo_log_storage: None,
        raft_storage: None,
        index_enabled: false,
        services: vec![Service::Cell, Service::Transaction],
        enable_recovery: false,
        disable_storage_locks: true,
    }
}
```

- [ ] **Step 4: Run the port-plan test and compile the fixture**

Run:

```bash
cargo test --test occ_bench_metrics benchmark_port_plan -- --exact
```

Expected: the port test passes. The fixture is compiled through the integration test's path import; the benchmark target is registered only after its entry point exists in Task 5.

- [ ] **Step 5: Commit the fixture layer**

```bash
git add benches/occ_support/mod.rs benches/occ_support/fixture.rs tests/occ_bench_metrics.rs
git commit -m "bench(txn): add OCC server fixtures"
```

## Task 3: Implement Fixed-Success Transaction Workloads

**Files:**
- Create: `benches/occ_support/workloads.rs`
- Modify: `benches/occ_support/fixture.rs`
- Modify: `tests/occ_bench_metrics.rs`

- [ ] **Step 1: Write failing outcome-classification tests**

Add to `tests/occ_bench_metrics.rs`:

```rust
#[path = "../benches/occ_support/workloads.rs"]
mod workloads;

use workloads::{AttemptOutcome, AttemptTally};

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
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test --test occ_bench_metrics retryable_attempts -- --exact
```

Expected: compilation fails because `AttemptOutcome` and `AttemptTally` do not exist.

- [ ] **Step 3: Implement outcome types and transaction attempts**

Create `benches/occ_support/workloads.rs` with:

```rust
use super::fixture::OccFixture;
use super::metrics::BatchMetrics;
use neb::ram::types::{Id, OwnedValue};
use neb::server::transactions::{
    AbortResult, DMPrepareResult, EndResult, TMPrepareResult, TxnExecResult,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptOutcome {
    Committed,
    Retryable,
    Unexpected(String),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AttemptTally {
    pub attempts: u64,
    pub committed: u64,
    pub not_realizable: u64,
    pub unexpected: Vec<String>,
}

impl AttemptTally {
    pub fn from_outcomes(outcomes: impl IntoIterator<Item = AttemptOutcome>) -> Self {
        let mut tally = Self::default();
        for outcome in outcomes {
            tally.attempts += 1;
            match outcome {
                AttemptOutcome::Committed => tally.committed += 1,
                AttemptOutcome::Retryable => tally.not_realizable += 1,
                AttemptOutcome::Unexpected(error) => tally.unexpected.push(error),
            }
        }
        tally
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BatchSpec {
    pub successes: u64,
    pub concurrency: usize,
    pub cells_per_txn: usize,
}

pub struct TimedBatch {
    pub metrics: BatchMetrics,
    pub elapsed: std::time::Duration,
}

async fn abort_started(
    fixture: &OccFixture,
    tid: neb::server::transactions::TxnId,
) -> Result<(), String> {
    match fixture.txn.abort(tid).await {
        Ok(Ok(AbortResult::Success(_))) => Ok(()),
        other => Err(format!("abort failed: {other:?}")),
    }
}

async fn retry_after_abort(
    fixture: &OccFixture,
    tid: neb::server::transactions::TxnId,
) -> AttemptOutcome {
    match abort_started(fixture, tid).await {
        Ok(()) => AttemptOutcome::Retryable,
        Err(error) => AttemptOutcome::Unexpected(error),
    }
}

async fn unexpected_after_abort(
    fixture: &OccFixture,
    tid: neb::server::transactions::TxnId,
    error: String,
) -> AttemptOutcome {
    match abort_started(fixture, tid).await {
        Ok(()) => AttemptOutcome::Unexpected(error),
        Err(abort) => AttemptOutcome::Unexpected(format!("{error}; {abort}")),
    }
}

pub async fn read_modify_write_once(fixture: &OccFixture, ids: &[Id]) -> AttemptOutcome {
    let tid = match fixture.txn.begin().await {
        Ok(Ok(tid)) => tid,
        other => return AttemptOutcome::Unexpected(format!("begin: {other:?}")),
    };
    for id in ids {
        let mut cell = match fixture.txn.read(tid.clone(), *id).await {
            Ok(Ok(TxnExecResult::Accepted(cell))) => cell,
            Ok(Ok(TxnExecResult::Rejected)) => {
                return retry_after_abort(fixture, tid).await;
            }
            other => {
                return unexpected_after_abort(
                    fixture,
                    tid,
                    format!("read {id:?}: {other:?}"),
                )
                .await;
            }
        };
        let score = *cell.data["score"].u64().expect("counter score");
        let mut data = cell.data.Map().expect("counter map").clone();
        data.insert(&"score".to_string(), OwnedValue::U64(score + 1));
        cell.data = OwnedValue::Map(data);
        match fixture.txn.update(tid.clone(), cell).await {
            Ok(Ok(TxnExecResult::Accepted(()))) => {}
            Ok(Ok(TxnExecResult::Rejected)) => {
                return retry_after_abort(fixture, tid).await;
            }
            other => {
                return unexpected_after_abort(
                    fixture,
                    tid,
                    format!("update {id:?}: {other:?}"),
                )
                .await;
            }
        }
    }
    match fixture.txn.prepare(tid.clone()).await {
        Ok(Ok(TMPrepareResult::Success)) => {}
        Ok(Ok(TMPrepareResult::DMPrepareError(
            DMPrepareResult::NotRealizable,
        ))) => return AttemptOutcome::Retryable,
        other => return AttemptOutcome::Unexpected(format!("prepare: {other:?}")),
    }
    match fixture.txn.commit(tid).await {
        Ok(Ok(EndResult::Success)) => AttemptOutcome::Committed,
        other => AttemptOutcome::Unexpected(format!("commit: {other:?}")),
    }
}

pub async fn run_fixed_success_rmw(
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
    spec: BatchSpec,
) -> TimedBatch {
    let score_before = fixture.sum_scores(&ids).await;
    let batch_started = Instant::now();
    let next = Arc::new(AtomicU64::new(0));
    let committed = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::with_capacity(spec.concurrency);
    for _ in 0..spec.concurrency {
        let fixture = fixture.clone();
        let ids = ids.clone();
        let next = next.clone();
        let committed = committed.clone();
        tasks.push(tokio::spawn(async move {
            let mut metrics = BatchMetrics::default();
            loop {
                let logical_index = next.fetch_add(1, Ordering::Relaxed);
                if logical_index >= spec.successes {
                    break;
                }
                let started = Instant::now();
                let mut attempts = 0;
                let mut retries = 0;
                loop {
                    attempts += 1;
                    let first = logical_index as usize % ids.len();
                    let selected: Vec<_> = (0..spec.cells_per_txn)
                        .map(|offset| ids[(first + offset) % ids.len()])
                        .collect();
                    match read_modify_write_once(&fixture, &selected).await {
                        AttemptOutcome::Committed => {
                            committed.fetch_add(1, Ordering::Relaxed);
                            metrics.record_success(started.elapsed(), attempts, retries);
                            break;
                        }
                        AttemptOutcome::Retryable => {
                            retries += 1;
                            metrics.record_retryable();
                        }
                        AttemptOutcome::Unexpected(error) => {
                            metrics.record_unexpected(error);
                            return metrics;
                        }
                    }
                }
            }
            metrics
        }));
    }
    let mut merged = BatchMetrics::default();
    for task in tasks {
        merged.merge(task.await.expect("join OCC benchmark worker"));
    }
    let elapsed = batch_started.elapsed();
    let committed = committed.load(Ordering::Relaxed);
    if committed != spec.successes {
        merged.record_unexpected(format!(
            "committed {committed} transactions; expected {}",
            spec.successes
        ));
    }
    let score_after = fixture.sum_scores(&ids).await;
    let expected_delta = spec.successes * spec.cells_per_txn as u64;
    if score_after.checked_sub(score_before) != Some(expected_delta) {
        merged.record_unexpected(format!(
            "score delta was {:?}; expected {expected_delta}",
            score_after.checked_sub(score_before)
        ));
    }
    TimedBatch { metrics: merged, elapsed }
}
```

Add these public verification helpers to `OccFixture`; both calls happen outside the
timed interval in `run_fixed_success_rmw`:

```rust
pub async fn score(&self, id: Id) -> u64 {
    let cell = self
        .client
        .read_cell(id)
        .await
        .expect("read benchmark verification cell")
        .expect("benchmark verification cell exists");
    *cell.data["score"].u64().expect("counter score")
}

pub async fn sum_scores(&self, ids: &[Id]) -> u64 {
    let mut total = 0;
    for id in ids {
        total += self.score(*id).await;
    }
    total
}
```

Remove unused imports after the first compile; do not add broad `allow` attributes.

- [ ] **Step 4: Run unit tests and check benchmark support**

Run:

```bash
cargo test --test occ_bench_metrics -- --nocapture
```

Expected: metrics and outcome tests pass. The benchmark target is not registered until its entry point is added in Task 5.

- [ ] **Step 5: Commit workload primitives**

```bash
git add benches/occ_support/workloads.rs benches/occ_support/fixture.rs tests/occ_bench_metrics.rs
git commit -m "bench(txn): add fixed-success OCC workloads"
```

## Task 4: Add Secondary Workloads and Invariants

**Files:**
- Modify: `benches/occ_support/workloads.rs`

- [ ] **Step 1: Add failing compile-contract tests for the secondary workloads**

Add this test module at the bottom of `workloads.rs` before the functions exist:

```rust
#[cfg(test)]
mod compile_contract {
    use super::*;

    #[test]
    fn secondary_workload_signatures_are_stable() {
        let _ = run_blind_update_batch;
        let _ = run_blind_remove_batch;
        let _ = run_projected_read_batch;
        let _ = ProjectionMode::Head;
        let _ = ProjectionMode::Selected;
        let _ = ProjectionMode::Mixed;
    }
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test --test occ_bench_metrics workloads::compile_contract::secondary_workload_signatures_are_stable -- --exact
```

Expected: compilation fails because the three functions and `ProjectionMode` do not exist.

- [ ] **Step 3: Implement shared completion and fixed-success helpers**

Add `ReadError` to the RAM imports, `Future` to the standard-library imports, and append:

```rust
#[derive(Clone, Copy, Debug)]
pub enum ProjectionMode {
    Head,
    Selected,
    Mixed,
}

async fn finish_once(
    fixture: &OccFixture,
    tid: neb::server::transactions::TxnId,
) -> AttemptOutcome {
    match fixture.txn.prepare(tid.clone()).await {
        Ok(Ok(TMPrepareResult::Success)) => {}
        Ok(Ok(TMPrepareResult::DMPrepareError(
            DMPrepareResult::NotRealizable,
        ))) => return AttemptOutcome::Retryable,
        other => return AttemptOutcome::Unexpected(format!("prepare: {other:?}")),
    }
    match fixture.txn.commit(tid).await {
        Ok(Ok(EndResult::Success)) => AttemptOutcome::Committed,
        other => AttemptOutcome::Unexpected(format!("commit: {other:?}")),
    }
}

async fn run_sequential_fixed_success<F, Fut>(
    successes: u64,
    mut attempt: F,
) -> TimedBatch
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = AttemptOutcome>,
{
    let started = Instant::now();
    let mut metrics = BatchMetrics::default();
    for logical_index in 0..successes {
        let transaction_started = Instant::now();
        let mut attempts = 0;
        let mut retries = 0;
        loop {
            attempts += 1;
            match attempt(logical_index).await {
                AttemptOutcome::Committed => {
                    metrics.record_success(transaction_started.elapsed(), attempts, retries);
                    break;
                }
                AttemptOutcome::Retryable => {
                    retries += 1;
                    metrics.record_retryable();
                }
                AttemptOutcome::Unexpected(error) => {
                    metrics.record_unexpected(error);
                    break;
                }
            }
        }
    }
    let elapsed = started.elapsed();
    if metrics.committed != successes {
        metrics.record_unexpected(format!(
            "committed {} transactions; expected {successes}",
            metrics.committed
        ));
    }
    TimedBatch { metrics, elapsed }
}
```

`TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)` is the only public
retry classification. Participant `Wait` responses are consumed inside the manager and
must never be counted by this harness.

- [ ] **Step 4: Implement blind-update and blind-remove batches**

Append these concrete attempt and batch functions:

```rust
async fn blind_update_once(fixture: &OccFixture, mut cell: neb::ram::cell::OwnedCell) -> AttemptOutcome {
    let score = *cell.data["score"].u64().expect("counter score");
    let mut data = cell.data.Map().expect("counter map").clone();
    data.insert(&"score".to_string(), OwnedValue::U64(score + 1));
    cell.data = OwnedValue::Map(data);
    let tid = match fixture.txn.begin().await {
        Ok(Ok(tid)) => tid,
        other => return AttemptOutcome::Unexpected(format!("begin: {other:?}")),
    };
    match fixture.txn.update(tid.clone(), cell).await {
        Ok(Ok(TxnExecResult::Accepted(()))) => finish_once(fixture, tid).await,
        Ok(Ok(TxnExecResult::Rejected)) => retry_after_abort(fixture, tid).await,
        other => {
            let error = format!("blind update: {other:?}");
            match abort_started(fixture, tid).await {
                Ok(()) => AttemptOutcome::Unexpected(error),
                Err(abort) => AttemptOutcome::Unexpected(format!("{error}; {abort}")),
            }
        }
    }
}

pub async fn run_blind_update_batch(
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
    operations: u64,
) -> TimedBatch {
    let score_before = fixture.sum_scores(&ids).await;
    let mut elapsed = std::time::Duration::ZERO;
    let mut metrics = BatchMetrics::default();
    'operations: for index in 0..operations {
        let id = ids[index as usize % ids.len()];
        let template = match fixture.client.read_cell(id).await {
            Ok(Ok(cell)) => cell,
            other => {
                metrics.record_unexpected(format!("blind-update setup read: {other:?}"));
                break;
            }
        };
        let logical_started = Instant::now();
        let mut attempts = 0;
        let mut retries = 0;
        loop {
            attempts += 1;
            match blind_update_once(&fixture, template.clone()).await {
                AttemptOutcome::Committed => {
                    let latency = logical_started.elapsed();
                    elapsed += latency;
                    metrics.record_success(latency, attempts, retries);
                    break;
                }
                AttemptOutcome::Retryable => {
                    retries += 1;
                    metrics.record_retryable();
                }
                AttemptOutcome::Unexpected(error) => {
                    metrics.record_unexpected(error);
                    break 'operations;
                }
            }
        }
    }
    let score_after = fixture.sum_scores(&ids).await;
    if score_after.checked_sub(score_before) != Some(operations) {
        metrics.record_unexpected(format!(
            "blind-update score delta was {:?}; expected {operations}",
            score_after.checked_sub(score_before)
        ));
    }
    TimedBatch { metrics, elapsed }
}

async fn blind_remove_once(fixture: &OccFixture, id: Id) -> AttemptOutcome {
    let tid = match fixture.txn.begin().await {
        Ok(Ok(tid)) => tid,
        other => return AttemptOutcome::Unexpected(format!("begin: {other:?}")),
    };
    match fixture.txn.remove(tid.clone(), id).await {
        Ok(Ok(TxnExecResult::Accepted(()))) => finish_once(fixture, tid).await,
        Ok(Ok(TxnExecResult::Rejected)) => retry_after_abort(fixture, tid).await,
        other => {
            let error = format!("blind remove: {other:?}");
            match abort_started(fixture, tid).await {
                Ok(()) => AttemptOutcome::Unexpected(error),
                Err(abort) => AttemptOutcome::Unexpected(format!("{error}; {abort}")),
            }
        }
    }
}

pub async fn run_blind_remove_batch(
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
    operations: u64,
) -> TimedBatch {
    let mut elapsed = std::time::Duration::ZERO;
    let mut metrics = BatchMetrics::default();
    'operations: for index in 0..operations {
        let id = ids[index as usize % ids.len()];
        fixture.seed_counter(id, index).await;
        let logical_started = Instant::now();
        let mut attempts = 0;
        let mut retries = 0;
        loop {
            attempts += 1;
            match blind_remove_once(&fixture, id).await {
                AttemptOutcome::Committed => {
                    let latency = logical_started.elapsed();
                    elapsed += latency;
                    metrics.record_success(latency, attempts, retries);
                    break;
                }
                AttemptOutcome::Retryable => {
                    retries += 1;
                    metrics.record_retryable();
                }
                AttemptOutcome::Unexpected(error) => {
                    metrics.record_unexpected(error);
                    break 'operations;
                }
            }
        }
        match fixture.client.read_cell(id).await {
            Ok(Err(ReadError::CellDoesNotExisted)) => {}
            other => metrics
                .record_unexpected(format!("removed cell {id:?} still readable: {other:?}")),
        }
    }
    TimedBatch { metrics, elapsed }
}
```

The public source read for each blind update and the re-seed/verification calls for each
blind remove happen outside the duration accumulated into `TimedBatch`. Only transaction
lifecycle time is reported. Reusing a bounded ID pool therefore cannot exhaust memory or
silently turn a blind update into a stale overwrite.

- [ ] **Step 5: Implement projected-read transactions**

Append:

```rust
async fn head_version(
    fixture: &OccFixture,
    tid: neb::server::transactions::TxnId,
    id: Id,
) -> Result<u64, AttemptOutcome> {
    match fixture.txn.head(tid, id).await {
        Ok(Ok(TxnExecResult::Accepted(header))) => Ok(header.version),
        Ok(Ok(TxnExecResult::Rejected)) => Err(AttemptOutcome::Retryable),
        other => Err(AttemptOutcome::Unexpected(format!("head: {other:?}"))),
    }
}

async fn selected_version(
    fixture: &OccFixture,
    tid: neb::server::transactions::TxnId,
    id: Id,
) -> Result<u64, AttemptOutcome> {
    match fixture
        .txn
        .read_selected(tid, id, vec![bifrost_hasher::hash_str("score")])
        .await
    {
        Ok(Ok(TxnExecResult::Accepted(cell))) => Ok(cell.header.version),
        Ok(Ok(TxnExecResult::Rejected)) => Err(AttemptOutcome::Retryable),
        other => Err(AttemptOutcome::Unexpected(format!("selected read: {other:?}"))),
    }
}

async fn full_version(
    fixture: &OccFixture,
    tid: neb::server::transactions::TxnId,
    id: Id,
) -> Result<u64, AttemptOutcome> {
    match fixture.txn.read(tid, id).await {
        Ok(Ok(TxnExecResult::Accepted(cell))) => Ok(cell.header.version),
        Ok(Ok(TxnExecResult::Rejected)) => Err(AttemptOutcome::Retryable),
        other => Err(AttemptOutcome::Unexpected(format!("full read: {other:?}"))),
    }
}

async fn projected_versions(
    fixture: &OccFixture,
    tid: &neb::server::transactions::TxnId,
    id: Id,
    mode: ProjectionMode,
) -> Result<Vec<u64>, AttemptOutcome> {
    Ok(match mode {
        ProjectionMode::Head => vec![
            head_version(fixture, tid.clone(), id).await?,
            head_version(fixture, tid.clone(), id).await?,
        ],
        ProjectionMode::Selected => vec![
            selected_version(fixture, tid.clone(), id).await?,
            selected_version(fixture, tid.clone(), id).await?,
        ],
        ProjectionMode::Mixed => vec![
            head_version(fixture, tid.clone(), id).await?,
            selected_version(fixture, tid.clone(), id).await?,
            full_version(fixture, tid.clone(), id).await?,
        ],
    })
}

async fn projected_read_once(
    fixture: &OccFixture,
    id: Id,
    mode: ProjectionMode,
) -> AttemptOutcome {
    let tid = match fixture.txn.begin().await {
        Ok(Ok(tid)) => tid,
        other => return AttemptOutcome::Unexpected(format!("begin: {other:?}")),
    };
    let versions = match projected_versions(fixture, &tid, id, mode).await {
        Ok(versions) => versions,
        Err(AttemptOutcome::Retryable) => return retry_after_abort(fixture, tid).await,
        Err(AttemptOutcome::Unexpected(error)) => {
            return match abort_started(fixture, tid).await {
                Ok(()) => AttemptOutcome::Unexpected(error),
                Err(abort) => AttemptOutcome::Unexpected(format!("{error}; {abort}")),
            };
        }
        Err(AttemptOutcome::Committed) => unreachable!("read helper cannot commit"),
    };
    if versions.windows(2).any(|pair| pair[0] != pair[1]) {
        let error = format!("snapshot versions differ: {versions:?}");
        return match abort_started(fixture, tid).await {
            Ok(()) => AttemptOutcome::Unexpected(error),
            Err(abort) => AttemptOutcome::Unexpected(format!("{error}; {abort}")),
        };
    }
    finish_once(fixture, tid).await
}

pub async fn run_projected_read_batch(
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
    operations: u64,
    mode: ProjectionMode,
) -> TimedBatch {
    run_sequential_fixed_success(operations, move |index| {
        let fixture = fixture.clone();
        let ids = ids.clone();
        async move {
            projected_read_once(&fixture, ids[index as usize % ids.len()], mode).await
        }
    })
    .await
}
```

- [ ] **Step 6: Compile the complete workload layer**

Run:

```bash
cargo test --test occ_bench_metrics -- --nocapture
```

Expected: all metric tests pass and the workload modules compile through their integration-test path imports.

- [ ] **Step 7: Commit secondary workloads**

```bash
git add benches/occ_support/workloads.rs
git commit -m "bench(txn): cover OCC workload portfolio"
```

## Task 5: Register the Criterion Driver and Documentation

**Files:**
- Modify: `Cargo.toml`
- Create: `benches/occ_transactions.rs`
- Modify: `benches/README.md`

- [ ] **Step 1: Register the benchmark and verify the missing-target failure**

Append to `Cargo.toml`:

```toml
[[bench]]
name = "occ_transactions"
harness = false
```

Run:

```bash
cargo check --bench occ_transactions
```

Expected: failure that `benches/occ_transactions.rs` does not exist.

- [ ] **Step 2: Create the Criterion entry point**

Create `benches/occ_transactions.rs`. The complete driver uses one fixture per benchmark
function, honors Criterion's requested iteration count, and publishes metrics only after
capturing the elapsed transaction time:

```rust
mod occ_support;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use occ_support::fixture::{counter_cell, OccFixture, PortPlan};
use occ_support::metrics::RunReport;
use occ_support::workloads::{
    run_blind_remove_batch, run_blind_update_batch, run_fixed_success_rmw,
    run_projected_read_batch, BatchSpec, ProjectionMode, TimedBatch,
};
use neb::ram::types::Id;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::runtime::Runtime;

static REPORT: OnceLock<Mutex<RunReport>> = OnceLock::new();

fn report() -> &'static Mutex<RunReport> {
    REPORT.get_or_init(|| {
        let label = std::env::var("NEB_OCC_BENCH_LABEL")
            .unwrap_or_else(|_| "unlabelled".to_string());
        let revision = std::env::var("NEB_OCC_BENCH_REVISION")
            .unwrap_or_else(|_| "unknown".to_string());
        Mutex::new(RunReport::new(label, revision))
    })
}

fn write_report() {
    let report = report().lock().unwrap();
    let path = PathBuf::from("target/occ-bench").join(format!("{}.json", report.label));
    report.write_json(&path).expect("write OCC benchmark report");
}

fn publish(scenario: String, batch: TimedBatch) -> Duration {
    let elapsed = batch.elapsed;
    let summary = batch.metrics.summary(elapsed);
    let passed = summary.invariants_passed;
    report().lock().unwrap().record(scenario.clone(), summary);
    write_report();
    assert!(passed, "OCC benchmark invariant failed: {scenario}");
    elapsed
}

fn seed(runtime: &Runtime, fixture: &OccFixture, ids: &[Id], payload_bytes: usize) {
    runtime.block_on(async {
        for id in ids {
            fixture
                .client
                .write_cell(counter_cell(fixture.schema.id, *id, 0, payload_bytes))
                .await
                .expect("send benchmark seed")
                .expect("write benchmark seed");
        }
    });
}

fn register_rmw(
    c: &mut Criterion,
    runtime: &Runtime,
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
    group_name: &str,
    report_prefix: &str,
    cases: &[(usize, usize)],
) {
    let mut group = c.benchmark_group(group_name);
    group.throughput(Throughput::Elements(1));
    for &(concurrency, cells_per_txn) in cases {
        let fixture_for_bench = fixture.clone();
        let ids_for_bench = ids.clone();
        let scenario = format!("{report_prefix}/{concurrency}");
        group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |bencher, &concurrency| {
                bencher.to_async(runtime).iter_custom(|iterations| {
                    let fixture = fixture_for_bench.clone();
                    let ids = ids_for_bench.clone();
                    let scenario = scenario.clone();
                    async move {
                        let batch = run_fixed_success_rmw(
                            fixture,
                            ids,
                            BatchSpec {
                                successes: iterations.max(1),
                                concurrency,
                                cells_per_txn,
                            },
                        )
                        .await;
                        publish(scenario, batch)
                    }
                });
            },
        );
    }
    group.finish();
}

fn finish_fixture(runtime: &Runtime, fixture: Arc<OccFixture>) {
    let fixture = Arc::try_unwrap(fixture).ok().expect("benchmark fixture references released");
    runtime.block_on(fixture.shutdown());
}

fn run_single_server_rmw(
    c: &mut Criterion,
    slot: u16,
    fixture_name: &str,
    group_name: &str,
    report_prefix: &str,
    id_count: usize,
    cells_per_txn: usize,
    concurrencies: &[usize],
) {
    let runtime = Runtime::new().expect("create benchmark runtime");
    let fixture = Arc::new(runtime.block_on(OccFixture::single(
        PortPlan::new(occ_support::base_port()).single(slot),
        fixture_name,
    )));
    let server_id = fixture.servers[0].server_id;
    let ids = Arc::new(fixture.ids_for_server(server_id, id_count, 1 + slot as u64 * 100_000));
    seed(&runtime, &fixture, &ids, 16);
    let cases: Vec<_> = concurrencies
        .iter()
        .map(|concurrency| (*concurrency, cells_per_txn))
        .collect();
    register_rmw(c, &runtime, fixture.clone(), ids, group_name, report_prefix, &cases);
    finish_fixture(&runtime, fixture);
}

fn bench_independent_rmw(c: &mut Criterion) {
    run_single_server_rmw(
        c,
        0,
        "occ_bench_independent",
        "occ/independent_rmw",
        "independent",
        4_096,
        1,
        &[1, 8, 32],
    );
}

fn bench_hot_rmw(c: &mut Criterion) {
    run_single_server_rmw(
        c,
        1,
        "occ_bench_hot",
        "occ/hot_rmw",
        "hot",
        1,
        1,
        &[8, 32],
    );
}

fn bench_multi_cell(c: &mut Criterion) {
    run_single_server_rmw(
        c,
        2,
        "occ_bench_multi_cell",
        "occ/multi_cell",
        "multi_cell",
        4_096,
        8,
        &[1, 8],
    );
}

fn bench_multi_participant(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create benchmark runtime");
    let plan = PortPlan::new(occ_support::base_port());
    let fixture = Arc::new(runtime.block_on(OccFixture::cluster(
        plan.cluster(3),
        "occ_bench_multi_participant",
    )));
    let ids = Arc::new(
        fixture
            .servers
            .iter()
            .enumerate()
            .map(|(index, server)| {
                fixture.ids_for_server(server.server_id, 1, 400_000 + index as u64 * 10_000)[0]
            })
            .collect(),
    );
    seed(&runtime, &fixture, &ids, 16);
    register_rmw(
        c,
        &runtime,
        fixture.clone(),
        ids,
        "occ/multi_participant",
        "multi_participant",
        &[(1, 3), (4, 3)],
    );
    finish_fixture(&runtime, fixture);
}

fn bench_projected_reads(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create benchmark runtime");
    let fixture = Arc::new(runtime.block_on(OccFixture::single(
        PortPlan::new(occ_support::base_port()).single(4),
        "occ_bench_projected",
    )));
    let ids = Arc::new(fixture.ids_for_server(fixture.servers[0].server_id, 128, 500_000));
    seed(&runtime, &fixture, &ids, 64 * 1024);
    let mut group = c.benchmark_group("occ/projected_reads");
    group.throughput(Throughput::Elements(1));
    for (name, mode) in [
        ("head", ProjectionMode::Head),
        ("selected", ProjectionMode::Selected),
        ("mixed", ProjectionMode::Mixed),
    ] {
        let fixture = fixture.clone();
        let ids = ids.clone();
        group.bench_function(name, |bencher| {
            bencher.to_async(&runtime).iter_custom(|iterations| {
                let fixture = fixture.clone();
                let ids = ids.clone();
                async move {
                    publish(
                        format!("projected/{name}"),
                        run_projected_read_batch(fixture, ids, iterations.max(1), mode).await,
                    )
                }
            });
        });
    }
    group.finish();
    finish_fixture(&runtime, fixture);
}

fn bench_blind_writes(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create benchmark runtime");
    let fixture = Arc::new(runtime.block_on(OccFixture::single(
        PortPlan::new(occ_support::base_port()).single(5),
        "occ_bench_blind",
    )));
    let update_ids = Arc::new(fixture.ids_for_server(fixture.servers[0].server_id, 128, 600_000));
    let remove_ids = Arc::new(fixture.ids_for_server(fixture.servers[0].server_id, 128, 700_000));
    seed(&runtime, &fixture, &update_ids, 16);

    let mut updates = c.benchmark_group("occ/blind_update");
    updates.throughput(Throughput::Elements(1));
    let update_fixture = fixture.clone();
    updates.bench_function("1", |bencher| {
        bencher.to_async(&runtime).iter_custom(|iterations| {
            let fixture = update_fixture.clone();
            let ids = update_ids.clone();
            async move {
                publish(
                    "blind_update/1".to_string(),
                    run_blind_update_batch(fixture, ids, iterations.max(1)).await,
                )
            }
        });
    });
    updates.finish();

    let mut removes = c.benchmark_group("occ/blind_remove");
    removes.throughput(Throughput::Elements(1));
    let remove_fixture = fixture.clone();
    removes.bench_function("1", |bencher| {
        bencher.to_async(&runtime).iter_custom(|iterations| {
            let fixture = remove_fixture.clone();
            let ids = remove_ids.clone();
            async move {
                publish(
                    "blind_remove/1".to_string(),
                    run_blind_remove_batch(fixture, ids, iterations.max(1)).await,
                )
            }
        });
    });
    removes.finish();
    drop(update_fixture);
    drop(remove_fixture);
    drop(update_ids);
    drop(remove_ids);
    finish_fixture(&runtime, fixture);
}

criterion_group!(
    occ_benches,
    bench_independent_rmw,
    bench_hot_rmw,
    bench_multi_cell,
    bench_multi_participant,
    bench_projected_reads,
    bench_blind_writes,
);
criterion_main!(occ_benches);
```

- [ ] **Step 3: Add release-build revision metadata**

Run benchmarks with the revision supplied explicitly because Cargo does not populate it:

```bash
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" \
NEB_OCC_BENCH_LABEL=smoke \
  cargo bench --bench occ_transactions -- --test
```

Expected: every scenario runs once, all invariant checks pass, and `target/occ-bench/smoke.json` contains every scenario with an empty `unexpected` list.

- [ ] **Step 4: Document exact commands and acceptance rules**

Append an “OCC transaction benchmarks” section to `benches/README.md` containing:

````markdown
## OCC transaction benchmarks

Run the release-mode smoke suite:

```bash
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" \
NEB_OCC_BENCH_LABEL=smoke \
  cargo bench --bench occ_transactions -- --test
```

Save a comparison baseline:

```bash
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" \
NEB_OCC_BENCH_LABEL=occ-initial \
  cargo bench --bench occ_transactions -- --save-baseline occ-initial
```

Use `NEB_OCC_BENCH_BASE_PORT` to move the loopback port range. JSON outcome and latency reports are written to `target/occ-bench`; Criterion reports are written to `target/criterion`.

A production optimization is retained only when its targeted stable scenario improves throughput or p95 latency by at least 5%, aggregate throughput does not decrease, no stable secondary throughput falls by more than 3%, no stable secondary p95 latency rises by more than 5%, unexpected outcomes remain zero, and all OCC correctness suites pass.
````

- [ ] **Step 5: Verify benchmark infrastructure**

Run:

```bash
cargo test --test occ_bench_metrics -- --nocapture
cargo check --bench occ_transactions
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" NEB_OCC_BENCH_LABEL=smoke cargo bench --bench occ_transactions -- --test
git diff --check
```

Expected: metrics tests pass, the release benchmark compiles, the smoke portfolio passes its invariants, and no whitespace errors are reported.

- [ ] **Step 6: Commit the benchmark driver**

```bash
git add Cargo.toml benches/occ_transactions.rs benches/README.md
git commit -m "bench(txn): add OCC performance portfolio"
```

## Task 6: Establish Develop and Initial OCC Baselines

**Files:**
- No tracked source changes.
- Generated: `target/occ-bench/develop.json`
- Generated: `target/occ-bench/occ-initial.json`
- Generated: Criterion baselines under `target/criterion`.

- [ ] **Step 1: Verify the benchmark commit is API-compatible with the merge base**

Use the existing isolated-worktree procedure to create a temporary worktree at `develop`, then cherry-pick only the benchmark commits from Tasks 1–5. Do not cherry-pick transaction implementation commits.

Run in that temporary worktree:

```bash
cargo check --bench occ_transactions
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" \
NEB_OCC_BENCH_LABEL=develop \
  cargo bench --bench occ_transactions -- --save-baseline develop
```

Expected: the harness compiles against the unchanged external transaction API, all
workload invariants pass, and `develop.json` is produced. This portfolio compares common
transaction costs; the dedicated correctness suites, rather than a performance timing,
remain the authority for the historical unchanged-read certification defect.

- [ ] **Step 2: Run the unoptimized OCC baseline**

Run in the feature worktree before any production optimization:

```bash
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" NEB_OCC_BENCH_LABEL=occ-initial cargo bench --bench occ_transactions -- --save-baseline occ-initial
```

Expected: all invariants pass and the report contains successful throughput, p50/p95/p99 latency, retry counts, and zero unexpected outcomes for every scenario.

- [ ] **Step 3: Check measurement stability**

For every scenario, calculate coefficient of variation from Criterion's per-sample
nanoseconds-per-iteration values:

```bash
find target/criterion/occ -path '*/new/sample.json' -print0 |
  while IFS= read -r -d '' sample; do
    jq -r --arg sample "$sample" '
      def mean: add / length;
      [.iters, .times] | transpose | map(.[1] / .[0]) as $values |
      ($values | mean) as $mean |
      (($values | map((. - $mean) * (. - $mean)) | add / (length - 1) | sqrt) / $mean) as $cv |
      "\($sample)\t\($cv)"
    ' "$sample"
  done
```

Expected: each printed CV is at most `0.05`. Rerun a scenario up to three times when
variation exceeds that value. Mark a still-noisy scenario inconclusive and exclude it
from retain/revert decisions.

Run filtered reruns as:

```bash
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" NEB_OCC_BENCH_LABEL=occ-initial-rerun cargo bench --bench occ_transactions -- '<scenario-filter>'
```

Expected: each scenario used for optimization decisions has coefficient of variation at most 5%.

## Task 7: Reuse Prepare Payloads Across Wait-Die Retries

**Files:**
- Modify: `src/server/transactions/manager.rs:1039-1070`

- [ ] **Step 1: Write a failing pure payload-construction test**

Add in `manager.rs` tests:

```rust
#[test]
fn prepare_ops_for_objects_preserves_sorted_expectations_and_intents() {
    let read_id = Id::new(0, 8101);
    let write_id = Id::new(0, 8102);
    let objects = BTreeMap::from([
        (
            read_id,
            DataObject {
                server: 1,
                cell: Some(counter_cell(1, read_id, 1)),
                expectation: CellExpectation::Present(7),
                changed: false,
                new: false,
            },
        ),
        (
            write_id,
            DataObject {
                server: 1,
                cell: Some(counter_cell(1, write_id, 2)),
                expectation: CellExpectation::Present(8),
                changed: true,
                new: false,
            },
        ),
    ]);
    assert_eq!(
        TransactionManager::prepare_ops_for_objects(&objects),
        vec![
            PrepareOp {
                id: read_id,
                expectation: CellExpectation::Present(7),
                intent: PrepareIntent::Read,
            },
            PrepareOp {
                id: write_id,
                expectation: CellExpectation::Present(8),
                intent: PrepareIntent::Write,
            },
        ]
    );
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test --lib server::transactions::manager::tests::prepare_ops_for_objects_preserves_sorted_expectations_and_intents -- --exact
```

Expected: compilation fails because `prepare_ops_for_objects` does not exist.

- [ ] **Step 3: Implement the helper and move construction outside the retry loop**

Add:

```rust
fn prepare_ops_for_objects(objs: &BTreeMap<Id, DataObject>) -> Vec<PrepareOp> {
    objs.iter()
        .map(|(id, data_obj)| PrepareOp {
            id: *id,
            expectation: data_obj.expectation.clone(),
            intent: if data_obj.changed {
                PrepareIntent::Write
            } else {
                PrepareIntent::Read
            },
        })
        .collect()
}
```

In `site_prepare`, construct once before `loop`:

```rust
let prepare_ops = Self::prepare_ops_for_objects(objs);
loop {
    // timeout and RPC logic remain unchanged
    let prepare_payload = data_site
        .prepare(
            coordinator_id,
            deps.clock.to_clock(),
            tid.clone(),
            prepare_ops.clone(),
        )
        .await;
    // existing response handling remains unchanged
}
```

Do not cache clocks, RPC responses, or ownership outcomes across retries.

- [ ] **Step 4: Verify correctness and targeted performance**

Run:

```bash
cargo test --lib server::transactions::manager::tests::prepare_ops_for_objects_preserves_sorted_expectations_and_intents -- --exact
cargo test --lib server::transactions::occ_tests -- --test-threads=1
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" NEB_OCC_BENCH_LABEL=prepare-payload cargo bench --bench occ_transactions -- 'hot_rmw'
```

Expected: tests pass. Retain the change only if stable hot-cell throughput or p95 improves by at least 5% and secondary smoke scenarios remain within policy. If it misses the threshold, reverse only this task's production/test patch with `apply_patch` and record the rejected hypothesis in the execution notes.

- [ ] **Step 5: Commit an accepted change**

```bash
git add src/server/transactions/manager.rs
git commit -m "perf(txn): reuse prepare payload across retries"
```

## Task 8: Replace Participant Re-Sorting with Linear Validation

**Files:**
- Modify: `src/server/transactions/data_site.rs:404-416`

- [ ] **Step 1: Write a failing unsorted-input test**

Add in `data_site.rs` tests:

```rust
#[test]
fn canonical_prepare_ops_rejects_unsorted_input() {
    let low = PrepareOp {
        id: Id::new(0, 8201),
        expectation: CellExpectation::Absent,
        intent: PrepareIntent::Write,
    };
    let high = PrepareOp {
        id: Id::new(0, 8202),
        expectation: CellExpectation::Absent,
        intent: PrepareIntent::Write,
    };
    assert_eq!(
        DataManager::canonical_prepare_ops(vec![high, low]),
        Err(DMPrepareResult::NotRealizable)
    );
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test --lib server::transactions::data_site::tests::canonical_prepare_ops_rejects_unsorted_input -- --exact
```

Expected: assertion fails because the current `BTreeMap` implementation silently sorts unsorted input.

- [ ] **Step 3: Implement allocation-free ordered validation**

Replace `canonical_prepare_ops` with:

```rust
fn canonical_prepare_ops(ops: Vec<PrepareOp>) -> Result<Vec<PrepareOp>, DMPrepareResult> {
    if ops.is_empty()
        || ops
            .windows(2)
            .any(|pair| pair[0].id >= pair[1].id)
    {
        return Err(DMPrepareResult::NotRealizable);
    }
    Ok(ops)
}
```

The strict comparison rejects both duplicates and unsorted input. The coordinator continues to generate sorted input from `BTreeMap` iteration.

- [ ] **Step 4: Verify malformed-input behavior, OCC correctness, and performance**

Run:

```bash
cargo test --lib server::transactions::data_site::tests::canonical_prepare_ops_rejects_unsorted_input -- --exact
cargo test --lib server::transactions::data_site::tests::prepare_rejects_duplicate_prepare_ops_without_publishing_owner -- --exact
cargo test --lib server::transactions::occ_tests -- --test-threads=1
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" NEB_OCC_BENCH_LABEL=linear-prepare cargo bench --bench occ_transactions -- 'multi_cell'
```

Expected: all tests pass. Retain only if stable multi-cell throughput or p95 improves by at least 5% and the portfolio remains within policy; otherwise reverse only this task's patch.

- [ ] **Step 5: Commit an accepted change**

```bash
git add src/server/transactions/data_site.rs
git commit -m "perf(txn): validate ordered prepare ops linearly"
```

## Task 9: Avoid Full-Cell Clones for Header and Selected Reads

**Files:**
- Modify: `src/server/transactions/manager.rs:395-460,785-931`
- Test: `src/server/transactions/occ_tests.rs`

- [ ] **Step 1: Add a failing test-only clone observation**

Add a `cfg(test)` counter beside the existing manager test hooks:

```rust
#[cfg(test)]
static FULL_SNAPSHOT_RETURN_CLONES: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn reset_full_snapshot_return_clones() {
    FULL_SNAPSHOT_RETURN_CLONES.store(0, Ordering::SeqCst);
}

#[cfg(test)]
pub(crate) fn full_snapshot_return_clones() -> u64 {
    FULL_SNAPSHOT_RETURN_CLONES.load(Ordering::SeqCst)
}
```

Increment it immediately before each full `OwnedCell` clone performed solely to feed `head` or `read_selected` through `read_cached_full_cell`.

Add to `occ_tests.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn projected_reads_do_not_clone_the_full_cached_snapshot() {
    let address = "127.0.0.1:5371";
    let group = "txn_occ_projection_clone";
    let server = start_occ_test_server(address, group).await;
    let runtime = server.current_database();
    let schema = install_occ_schema(&runtime);
    let id = Id::new(0, 90_201);
    let mut cell = counter_cell(schema.id, id, 7, &"x".repeat(64 * 1024));
    runtime.chunks().write_cell(&mut cell).unwrap();
    let txn = scoped_txn_client_for_database(address, group, group).await;
    let tid = txn.begin().await.unwrap().unwrap();

    transactions::manager::reset_full_snapshot_return_clones();
    let _ = txn.head(tid.clone(), id).await.unwrap().unwrap();
    let _ = txn
        .read_selected(tid.clone(), id, vec![bifrost_hasher::hash_str("score")])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(transactions::manager::full_snapshot_return_clones(), 0);

    abort_txn(&txn, tid).await;
    server.shutdown().await;
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test --lib server::transactions::occ_tests::projected_reads_do_not_clone_the_full_cached_snapshot -- --exact --test-threads=1
```

Expected: assertion fails with at least two full-snapshot return clones.

- [ ] **Step 3: Refactor cache filling to return readiness rather than an owned cell**

Replace `read_cached_full_cell` with:

```rust
async fn ensure_cached_cell<'a>(
    &self,
    tid: &TxnId,
    id: &Id,
    txn: &mut TxnGuard<'a>,
) -> Result<TxnExecResult<(), ReadError>, TMError> {
    txn.last_activity = get_time();
    if let Some(data_obj) = txn.data.get(id) {
        return Ok(if data_obj.cell.is_some() {
            TxnExecResult::Accepted(())
        } else {
            TxnExecResult::Error(ReadError::CellDoesNotExisted)
        });
    }
    let (server_id, server) = self
        .get_data_site_by_id(id)
        .await
        .map_err(|_| TMError::CannotLocateCellServer)?;
    self.cache_from_site(server_id, &server, tid, id, txn).await
}
```

Rename `read_from_site` to `cache_from_site`, change its accepted-cell branch to insert the owned RPC response without cloning, and return `TxnExecResult::Accepted(())`. Preserve Wait retry, missing-cell caching, read-your-writes, clock merging, and every error/state mapping.

After `ensure_cached_cell` returns `Accepted(())`:

- `read` clones the cached full cell exactly once because the RPC response owns its return value.
- `head` clones only `CellHeader` from the cached cell.
- `read_selected` calls `select_from_cell` directly on the cached cell reference.

Use one shared mapper for non-accepted variants so `Rejected`, `Wait`, `Error`, and `StateError` remain unchanged.

- [ ] **Step 4: Verify repeatable-read behavior and clone elimination**

Run:

```bash
cargo test --lib server::transactions::occ_tests::projected_reads_do_not_clone_the_full_cached_snapshot -- --exact --test-threads=1
cargo test --lib server::transactions::occ_tests::repeatable_full_read_uses_first_snapshot -- --exact --test-threads=1
cargo test --lib server::transactions::occ_tests::repeatable_selected_then_full_read_uses_first_snapshot -- --exact --test-threads=1
cargo test --lib server::transactions::occ_tests::repeatable_head_then_full_read_uses_first_snapshot -- --exact --test-threads=1
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" NEB_OCC_BENCH_LABEL=projection-cache cargo bench --bench occ_transactions -- 'projected_reads'
```

Expected: all tests pass and the test-only counter stays zero. Retain only if a stable projected-read scenario improves throughput or p95 by at least 5% with no portfolio regression; otherwise reverse the refactor and its test-only hook.

- [ ] **Step 5: Commit an accepted change**

```bash
git add src/server/transactions/manager.rs src/server/transactions/occ_tests.rs
git commit -m "perf(txn): project reads from cached snapshots"
```

## Task 10: Run the Full Retain-or-Revert Gate

**Files:**
- Modify only when formatting accepted changes.

- [ ] **Step 1: Run the complete benchmark portfolio**

```bash
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" NEB_OCC_BENCH_LABEL=occ-optimized cargo bench --bench occ_transactions -- --save-baseline occ-optimized
```

Expected: every scenario report has zero unexpected outcomes and passing invariants. Compare `occ-optimized` with `occ-initial`: aggregate geometric-mean throughput must not decrease, accepted targeted scenarios must improve at least 5%, secondary throughput regression must stay within 3%, and secondary p95 regression within 5%.

- [ ] **Step 2: Require a measured optimization before declaring the loop complete**

Review the retain/revert record from Tasks 7–9. At least one production candidate must
have met the acceptance policy and remained in the branch. If none did, do not claim the
goal is complete: use the benchmark portfolio to identify the next dominant stable cost,
write one new testable hypothesis as an additional task in this plan, and continue the
same one-change/measure/retain-or-revert loop.

Expected: the branch contains at least one accepted production optimization with a
stable targeted improvement of at least 5%, or execution remains explicitly in progress.

- [ ] **Step 3: Run all correctness suites from the final candidate**

```bash
cargo test --lib server::transactions -- --test-threads=1
cargo test --lib ram::tiered -- --test-threads=1
cargo test --lib index::full_text -- --test-threads=1
cargo check --lib
```

Expected: transaction tests have zero failures; tiered and full-text suites have zero failures; library check exits successfully.

- [ ] **Step 4: Run formatting and repository checks**

```bash
rustfmt --edition 2021 --check \
  src/server/transactions/manager.rs \
  src/server/transactions/data_site.rs \
  src/server/transactions/occ_tests.rs \
  benches/occ_transactions.rs \
  benches/occ_support/mod.rs \
  benches/occ_support/fixture.rs \
  benches/occ_support/workloads.rs \
  benches/occ_support/metrics.rs \
  tests/occ_bench_metrics.rs
git diff --check
git status --short
```

Expected: targeted formatting and diff checks pass, and the feature worktree is clean. `cargo fmt --all -- --check` may report only the four known trailing-whitespace errors in the linked Bifrost `src/membership/server.rs`; no new formatting error is acceptable.

- [ ] **Step 5: Audit guarantees against authoritative tests**

Confirm that the final transaction run included passing tests for repeatable full/selected/header reads, repeatable absence, standard and concurrent-clock lost updates, blind conflicts, multi-participant prepare failure, cancelled prepare cleanup, explicit abort races, partial abort retry, commit rejection after abort, ownership verification, and stale-abort cleanup retention.

Expected: every immutable contract item in the design has direct passing-test evidence; a benchmark result alone is not used as correctness evidence.

- [ ] **Step 6: Commit only remaining benchmark documentation changes**

If execution added exact reproduction notes to `benches/README.md`, verify their commands and commit them separately:

```bash
git add benches/README.md
git commit -m "docs: record OCC benchmark procedure"
```

Do not create an empty commit when the README was already complete in Task 5.

## Task 11: Borrow Wait-Die Owners Instead of Cloning Priorities

**Files:**
- Modify: `src/server/transactions/data_site.rs:3289-3390`

- [ ] **Step 1: Add a failing test-only owner-clone observation**

Add a `cfg(test)` atomic counter beside the existing participant test hooks. Increment
it immediately before the full `TxnPriority` clone used to inspect `CellMeta::owner` in
the prepare conflict path. Reset the counter before the conflicting second prepare in
`prepare_retry_exact_payload_does_not_blindly_succeed_with_foreign_owner`, then assert
that the conflict returns `NotRealizable`, leaves the foreign owner intact, and performs
zero owner snapshot clones.

- [ ] **Step 2: Run the focused test and verify RED**

```bash
cargo test --lib server::transactions::data_site::tests::prepare_retry_exact_payload_does_not_blindly_succeed_with_foreign_owner -- --exact
```

Expected: the existing wait-die and ownership assertions pass, but the new counter
assertion fails with one owner snapshot clone.

- [ ] **Step 3: Borrow the owner during the conflict decision**

Change only the participant prepare conflict check from cloning `meta.owner` to borrowing
it with `as_ref()`. Preserve the exact stale-lock reclamation, requester age comparison,
`Wait`, `NotRealizable`, logging, and lock-publication behavior. Do not change owner
representation, timestamps, certification, retry rules, or validation.

- [ ] **Step 4: Verify correctness and the targeted performance gate**

```bash
cargo test --lib server::transactions::data_site::tests::prepare_retry_exact_payload_does_not_blindly_succeed_with_foreign_owner -- --exact
cargo test --lib server::transactions::data_site::tests::concurrent_clock_wait_die_has_one_younger_requester -- --exact
cargo test --lib server::transactions::occ_tests -- --test-threads=1
```

On `192.168.10.17`, run stable exact `occ/hot_rmw/8` and `occ/hot_rmw/32`
measurements serially with `numactl --cpunodebind=0 --membind=0`. Use Criterion sample
mean/derived throughput as canonical, require CV at most 5%, and use the custom JSON p95
as the latest-batch diagnostic. Retain only if a stable target improves throughput or p95
by at least 5%; if it passes, run the full portfolio and enforce the existing aggregate,
secondary throughput, secondary p95, invariant, and unexpected-outcome policy. Otherwise
revert the counter, test assertion, and production change and record the rejected
hypothesis.

- [ ] **Step 5: Commit only an accepted change**

```bash
git add src/server/transactions/data_site.rs
git commit -m "perf(txn): borrow owners during wait-die checks"
```

## Task 12: Retain and Sync One Guard per Transaction Segment

**Files:**
- Modify: `src/server/transactions/data_site.rs:718-1020`
- Test: `src/server/transactions/data_site.rs`

- [ ] **Step 1: Add a failing same-segment guard and rollback test**

Add
`commit_retains_one_guard_per_segment_and_rolls_back_every_cell` in the
`data_site.rs` test module. On a fresh server, seed two small cells in the same
partition and assert from `address_of` plus `get_cell_segment_info` that their
old versions occupy the same `(chunk_id, segment_id)`. Prepare both as
`Present(version)` writes and commit two updates in one transaction.

Before abort, inspect the tracked committed transaction and assert:

```rust
assert_eq!(txn.history.len(), 2);
assert_eq!(txn.segment_guards.len(), 1);
assert_eq!(
    (
        txn.segment_guards[0].chunk_id(),
        txn.segment_guards[0].segment_id(),
    ),
    old_segment_key,
);
```

Record both committed versions, then abort the committed transaction. Verify
both cells were restored to their original values and that each rollback
version is greater than its committed version. End the transaction
successfully. This proves one segment reference protects every rollback entry
in that segment without regressing the monotonic versions required by OCC and
same-version ABA protection.

- [ ] **Step 2: Run the test and verify RED**

```bash
cargo test --lib server::transactions::data_site::tests::commit_retains_one_guard_per_segment_and_rolls_back_every_cell -- --exact
```

Expected: the commit and two-cell history assertions pass, but the guard-count
assertion fails because the current update path retains two guards for the same
segment.

- [ ] **Step 3: Retain only the first guard for each segment**

Add a small helper that accepts the newly acquired
`SegmentReferenceGuard`, compares both `chunk_id()` and `segment_id()` against
the guards already retained by the transaction, and pushes it only when that
segment is not already protected. A duplicate guard is dropped immediately.

Use the helper only after a successful `CommitOp::Update` or
`CommitOp::Remove`, replacing the two direct
`txn.segment_guards.push(guard)` calls. Do not change the `CommitOp::Write`
path, storage prevalidation, version-conditional mutation, undo/history
capture, owner validation, rollback, cleanup, or any distributed phase.

The existing post-mutation loop remains the durability barrier. Because the
retained guards are unique by `(chunk_id, segment_id)`, it invokes
`force_wal_sync` exactly once for each protected segment after all transaction
mutations have completed. A segment-level `sync_all` covers all WAL writes to
that segment; duplicate calls before any later transaction mutation add no
durability.

- [ ] **Step 4: Verify rollback, prevalidation, and commit behavior**

```bash
cargo test --lib server::transactions::data_site::tests::commit_retains_one_guard_per_segment_and_rolls_back_every_cell -- --exact
cargo test --lib server::transactions::data_site::tests::commit_prevalidates_current_storage_state_before_partial_write -- --exact
cargo test --lib server::transactions::data_site::tests::commit_rejects_change_after_certification -- --exact
git diff --check
```

Expected: every test passes. The new test must verify both restored cells, not
only the guard count.

- [ ] **Step 5: Run the targeted benchmark and retain-or-revert gate**

On `192.168.10.17`, run exact default-feature
`occ/multi_cell/8` measurements serially with
`numactl --cpunodebind=0 --membind=0` against the saved stable baseline. Use
Criterion sample mean/derived throughput as canonical, require CV at most 5%,
and use the custom JSON p95 as the latest-batch diagnostic.

Retain only if stable multi-cell throughput or p95 improves by at least 5%.
If it passes, run the complete stable portfolio and enforce the aggregate,
secondary throughput, secondary p95, invariant, unexpected-outcome, and
correctness policies. Otherwise preserve the audit patch, restore the accepted
base, and record the rejected hypothesis.

- [ ] **Step 6: Commit only an accepted change**

```bash
git add src/server/transactions/data_site.rs
git commit -m "perf(txn): deduplicate transaction segment guards"
```

## Task 13: Share Immutable Write Timestamps Across Committed Cells

**Files:**
- Modify: `src/server/transactions/data_site.rs:160-180,330-590,718-1020`
- Test: `src/server/transactions/data_site.rs`

- [ ] **Step 1: Add a failing shared-timestamp test**

Add
`multi_cell_commit_shares_one_write_timestamp_allocation` in the
`data_site.rs` test module. On a fresh server, seed two cells, prepare both with
`Present(version)` write intents, then commit two updates with an RPC clock
that is causally newer than the transaction ID. Read both `CellMeta` entries
and assert:

```rust
assert_eq!(write_a.as_ref(), &newer_commit_clock);
assert_eq!(write_b.as_ref(), &newer_commit_clock);
assert!(Arc::ptr_eq(&write_a, &write_b));
```

End the committed transaction successfully. The value assertions retain the
effective-timestamp selection and ordering contract; `Arc::ptr_eq` proves both
cells share one immutable allocation rather than only equal clock values.

- [ ] **Step 2: Run the test and verify RED**

```bash
cargo test --lib server::transactions::data_site::tests::multi_cell_commit_shares_one_write_timestamp_allocation -- --exact
```

Expected: the test does not compile against the current owned `TxnId`
`CellMeta::write` field because it cannot use `Arc::ptr_eq`.

- [ ] **Step 3: Share one immutable timestamp per participant commit**

Change only `CellMeta::write` from `TxnId` to `Arc<TxnId>` and initialize it
with `Arc::new(TxnId::new())`. Update cleanup and read-timestamp comparisons to
borrow `meta.write.as_ref()` while preserving the exact vector-clock
relations.

After all commit payload, owner, and storage prevalidation succeeds, create one
`Arc<TxnId>` from the effective commit timestamp. The `Write`, `Update`, and
`Remove` success branches assign `Arc::clone` of that same value to their cell
metadata. Thomas Write Rule comparisons and debug logging borrow the existing
clock; do not clone a clock solely for logging.

`CellMeta` is local, nonserialized participant metadata. Do not change
transaction IDs, RPC payloads, prepare expectations, owner priority, storage
versions, rollback history, clocks merged from responses, or any distributed
phase.

- [ ] **Step 4: Verify timestamp ordering and OCC behavior**

```bash
cargo test --lib server::transactions::data_site::tests::multi_cell_commit_shares_one_write_timestamp_allocation -- --exact
cargo test --lib server::transactions::data_site::tests::commit_rejects_change_after_certification -- --exact
cargo test --lib server::transactions::data_site::tests::concurrent_vector_clock_stale_update_rejected_after_committed_peer_changes_version -- --exact
cargo test --lib server::transactions::data_site::tests::concurrent_clock_wait_die_has_one_younger_requester -- --exact
git diff --check
```

Expected: all tests pass, including concurrent-clock ordering and Wait-Die
tie-breaking behavior.

- [ ] **Step 5: Run the targeted benchmark and retain-or-revert gate**

On `192.168.10.17`, run exact default-feature `occ/multi_cell/8` serially with
`numactl --cpunodebind=0 --membind=0` against the saved stable baseline.
Criterion sample mean/derived throughput is canonical, CV must be at most 5%,
and custom JSON p95 is the latest-batch diagnostic.

Retain only if stable throughput or p95 improves by at least 5%. If it passes,
run the complete stable portfolio and enforce the aggregate, secondary,
invariant, unexpected-outcome, and correctness gates. Otherwise preserve the
audit patch, restore the accepted base, and document the rejection.

- [ ] **Step 6: Commit only an accepted change**

```bash
git add src/server/transactions/data_site.rs
git commit -m "perf(txn): share committed write timestamps"
```

## Task 14: Compare Canonical Wait-Die Clocks Without Serialization

**Files:**
- Modify in Bifrost: `../bifrost/src/vector_clock/mod.rs`
- Modify: `src/server/transactions/mod.rs:49-70,240-330`

The linked Bifrost worktree has unrelated user changes, but
`src/vector_clock/mod.rs` is clean at the start of this task. Preserve every
other Bifrost path and scope any stash or commit to this file only.

- [ ] **Step 1: Add failing deterministic clock-order tests**

In Bifrost, add tests for a new
`VectorClock::deterministic_cmp(&self, other)` method:

- two distinct canonical concurrent clocks compare non-equal and reverse
  antisymmetrically;
- causally equal canonical clocks compare equal;
- semantically equal but noncanonical deserialized clocks fall back to a
  deterministic raw-map tie-break so distinct representations are still
  ordered antisymmetrically.

In Nebuchadnezzar, change the same-coordinator concurrent-priority test to
derive its expected order from `tid.deterministic_cmp(&other.tid)` rather than
serialized bytes. Keep the causal-order and coordinator-first tests unchanged.

- [ ] **Step 2: Run the tests and verify RED**

```bash
cargo test --manifest-path ../bifrost/Cargo.toml \
  vector_clock::test::deterministic_cmp_totally_orders_canonical_clocks -- --exact
cargo test --lib \
  server::transactions::occ_type_tests::txn_priority_totally_orders_same_coordinator_concurrent_clocks_without_serialization \
  -- --exact
```

Expected: compilation fails because `deterministic_cmp` does not exist.

- [ ] **Step 3: Add allocation-free canonical comparison with a fallback**

In `VectorClock`, add a private `map_is_canonical` check: every counter is
nonzero and component keys are strictly increasing. Change the existing
causal-relation implementation to compare stored map slices directly when both
clocks are canonical; retain the existing clone/canonicalize path whenever
either map is noncanonical. This preserves semantics for deserialized
unsorted, duplicate, or zero-valued maps.

Add `pub fn deterministic_cmp(&self, other: &Self) -> Ordering`. For canonical
clocks, compare their stored canonical `Vec<(S, u64)>` values lexicographically
without allocation. For a noncanonical input, compare canonicalized maps first
and then raw stored maps as a deterministic tie-break when the semantic clocks
are equal. The result must be antisymmetric and total for distinct stored
representations.

In `TxnPriority::compare_age`, preserve causal `Before` and `After` decisions
and the coordinator-ID comparison. Replace only the final two
`serde::serialize` calls with `self.tid.deterministic_cmp(&other.tid)`.

The concurrent tie-break may choose a different older transaction than the old
serialized-byte order, but it remains deterministic and total on every node.
Wait-Die requires a consistent total order, not the historical byte ordering.
Do not change prepare retries, ownership publication, transaction IDs, RPCs,
or any distributed phase.

- [ ] **Step 4: Verify clock and Wait-Die correctness**

```bash
cargo test --manifest-path ../bifrost/Cargo.toml vector_clock::test -- --test-threads=1
cargo test --lib server::transactions::occ_type_tests::txn_priority -- --test-threads=1
cargo test --lib server::transactions::data_site::tests::prepare_retry_exact_payload_does_not_blindly_succeed_with_foreign_owner -- --exact
cargo test --lib server::transactions::data_site::tests::concurrent_clock_wait_die_has_one_younger_requester -- --exact
git diff --check
git -C ../bifrost diff --check -- src/vector_clock/mod.rs
```

Expected: canonical, noncanonical, causal, total-order, and Wait-Die tests pass.
No Bifrost file other than `src/vector_clock/mod.rs` is added to this
candidate.

- [ ] **Step 5: Run the targeted benchmark and retain-or-revert gate**

Deploy only the two candidate files to the isolated sources on
`192.168.10.17`. Run exact default-feature `occ/hot_rmw/8` and
`occ/hot_rmw/32` serially with NUMA-node-0 pinning against the saved stable
baseline. Criterion sample mean/throughput is canonical, CV must be at most 5%,
and JSON p95 is the latest-batch diagnostic.

Retain only if one stable target improves throughput or p95 by at least 5%.
If it passes, run the complete stable portfolio and all correctness gates.
Otherwise preserve scoped audit patches in both repositories, restore both
remote files to the accepted base, and document the rejection.

- [ ] **Step 6: Commit only an accepted change**

Commit the Bifrost vector-clock change separately without staging its unrelated
dirty files, then commit the Nebuchadnezzar priority change:

```bash
git -C ../bifrost add src/vector_clock/mod.rs
git -C ../bifrost commit -m "perf(vector-clock): compare canonical clocks without allocation"
git add src/server/transactions/mod.rs
git commit -m "perf(txn): avoid serializing wait-die clocks"
```
