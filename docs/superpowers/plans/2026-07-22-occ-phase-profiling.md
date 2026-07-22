# OCC Phase Profiling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in, transaction-aware phase measurements that identify the dominant cost inside the existing distributed OCC protocol without changing any protocol step or adding overhead to default builds.

**Architecture:** A non-default `occ_phase_profile` feature exposes a fixed, allocation-free registry under `server::transactions`; feature-gated RAII guards measure coordinator and participant phases, including error and early-return paths. The OCC benchmark resets the registry around each workload batch, rejects non-quiescent snapshots, and writes phase totals, counts, per-invocation cost, and per-commit cost into its existing JSON report.

**Tech Stack:** Rust 2021, `std::time::Instant`, atomics, Cargo features, Serde JSON, Criterion, Tokio, existing OCC integration tests, Linux `numactl` on `192.168.10.17`.

---

## File map

- Create `src/server/transactions/phase_profile.rs`: fixed phase enum, registry, RAII guard, reset/snapshot API, and isolated registry unit tests.
- Modify `src/server/transactions/mod.rs`: export the profiler only when `occ_phase_profile` is enabled.
- Modify `Cargo.toml`: declare the empty, non-default `occ_phase_profile` feature.
- Modify `src/server/transactions/manager.rs`: add coordinator phase guards at existing read, grouping, participant lookup, prepare, commit, abort, and end boundaries.
- Modify `src/server/transactions/data_site.rs`: add participant phase guards at the existing prepare, commit, abort, and end service boundaries.
- Modify `benches/occ_support/metrics.rs`: convert a raw snapshot to serialized per-scenario phase summaries.
- Modify `benches/occ_transactions.rs`: reset before and snapshot after each timed workload batch.
- Modify `tests/occ_bench_metrics.rs`: verify phase arithmetic and JSON shape when the feature is enabled.
- Modify `benches/README.md`: document the profiling feature, output semantics, controlled portfolio, and invalid-snapshot rules.
- Create `scripts/check-occ-phase-profile-default.sh`: prove the default library artifact has no profiler symbols.

### Task 1: Feature-gated fixed phase registry

**Files:**
- Modify: `Cargo.toml`
- Create: `src/server/transactions/phase_profile.rs`
- Modify: `src/server/transactions/mod.rs`

- [ ] **Step 1: Declare the non-default feature and write registry tests first**

Add this feature beside the existing empty features in `Cargo.toml`:

```toml
occ_phase_profile = []
```

Create `src/server/transactions/phase_profile.rs` with the public types and the following private tests. The tests use a local registry, not the global registry, so feature-enabled transaction tests can run in parallel without corrupting their expectations.

```rust
use std::{
    array,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Instant,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum Phase {
    ReadSiteRpc,
    AffectedObjectGrouping,
    PrepareParticipantLookup,
    PrepareBarrier,
    CommitBarrier,
    AbortParticipantLookup,
    AbortCleanup,
    EndParticipantLookup,
    EndCleanup,
    ParticipantPrepare,
    ParticipantCommit,
    ParticipantAbort,
    ParticipantEnd,
}

pub const PHASE_COUNT: usize = 13;
pub const PHASES: [Phase; PHASE_COUNT] = [
    Phase::ReadSiteRpc,
    Phase::AffectedObjectGrouping,
    Phase::PrepareParticipantLookup,
    Phase::PrepareBarrier,
    Phase::CommitBarrier,
    Phase::AbortParticipantLookup,
    Phase::AbortCleanup,
    Phase::EndParticipantLookup,
    Phase::EndCleanup,
    Phase::ParticipantPrepare,
    Phase::ParticipantCommit,
    Phase::ParticipantAbort,
    Phase::ParticipantEnd,
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PhaseMeasurement {
    pub total_ns: u64,
    pub invocation_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub phases: [PhaseMeasurement; PHASE_COUNT],
    pub active_guards: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActiveGuards(pub usize);

struct Registry {
    total_ns: [AtomicU64; PHASE_COUNT],
    invocation_count: [AtomicU64; PHASE_COUNT],
    active_guards: AtomicUsize,
}

pub struct Guard {
    registry: &'static Registry,
    phase: Phase,
    started: Instant,
}

static REGISTRY: Registry = Registry::new();

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn reset_clears_totals_and_counts() {
        let registry = Box::leak(Box::new(Registry::new()));
        registry.record(Phase::PrepareBarrier, 17);
        registry.reset().unwrap();
        assert_eq!(registry.snapshot().phases, [PhaseMeasurement::default(); PHASE_COUNT]);
    }

    #[test]
    fn records_totals_counts_and_guard_drop() {
        let registry = Box::leak(Box::new(Registry::new()));
        registry.record(Phase::CommitBarrier, 10);
        registry.record(Phase::CommitBarrier, 20);
        {
            let guard = registry.guard(Phase::ParticipantCommit);
            std::thread::sleep(Duration::from_millis(1));
            drop(guard);
        }
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.phases[Phase::CommitBarrier as usize].total_ns, 30);
        assert_eq!(snapshot.phases[Phase::CommitBarrier as usize].invocation_count, 2);
        assert_eq!(
            snapshot.phases[Phase::ParticipantCommit as usize].invocation_count,
            1
        );
        assert!(snapshot.phases[Phase::ParticipantCommit as usize].total_ns > 0);
        assert_eq!(snapshot.active_guards, 0);
    }

    #[test]
    fn reset_rejects_an_active_guard() {
        let registry = Box::leak(Box::new(Registry::new()));
        let guard = registry.guard(Phase::AbortCleanup);
        assert_eq!(registry.reset(), Err(ActiveGuards(1)));
        drop(guard);
        assert_eq!(registry.reset(), Ok(()));
    }
}
```

Add this declaration to `src/server/transactions/mod.rs`:

```rust
#[cfg(feature = "occ_phase_profile")]
pub mod phase_profile;
```

- [ ] **Step 2: Run the tests to verify the registry is incomplete**

Run:

```bash
cargo test --features occ_phase_profile phase_profile::tests --lib
```

Expected: compilation fails because `Registry::new`, `record`, `reset`, `snapshot`, `guard`, `Phase::as_str`, and `Guard::drop` are not implemented.

- [ ] **Step 3: Implement the fixed registry and RAII guard**

Add these implementations above the test module in `phase_profile.rs`:

```rust
impl Phase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadSiteRpc => "read_site_rpc",
            Self::AffectedObjectGrouping => "affected_object_grouping",
            Self::PrepareParticipantLookup => "prepare_participant_lookup",
            Self::PrepareBarrier => "prepare_barrier",
            Self::CommitBarrier => "commit_barrier",
            Self::AbortParticipantLookup => "abort_participant_lookup",
            Self::AbortCleanup => "abort_cleanup",
            Self::EndParticipantLookup => "end_participant_lookup",
            Self::EndCleanup => "end_cleanup",
            Self::ParticipantPrepare => "participant_prepare",
            Self::ParticipantCommit => "participant_commit",
            Self::ParticipantAbort => "participant_abort",
            Self::ParticipantEnd => "participant_end",
        }
    }
}

impl Registry {
    const fn new() -> Self {
        Self {
            total_ns: [const { AtomicU64::new(0) }; PHASE_COUNT],
            invocation_count: [const { AtomicU64::new(0) }; PHASE_COUNT],
            active_guards: AtomicUsize::new(0),
        }
    }

    fn guard(&'static self, phase: Phase) -> Guard {
        self.active_guards.fetch_add(1, Ordering::AcqRel);
        Guard {
            registry: self,
            phase,
            started: Instant::now(),
        }
    }

    fn record(&self, phase: Phase, elapsed_ns: u64) {
        let index = phase as usize;
        self.total_ns[index].fetch_add(elapsed_ns, Ordering::Relaxed);
        self.invocation_count[index].fetch_add(1, Ordering::Relaxed);
    }

    fn reset(&self) -> Result<(), ActiveGuards> {
        let active = self.active_guards.load(Ordering::Acquire);
        if active != 0 {
            return Err(ActiveGuards(active));
        }
        for counter in &self.total_ns {
            counter.store(0, Ordering::Relaxed);
        }
        for counter in &self.invocation_count {
            counter.store(0, Ordering::Relaxed);
        }
        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            phases: array::from_fn(|index| PhaseMeasurement {
                total_ns: self.total_ns[index].load(Ordering::Relaxed),
                invocation_count: self.invocation_count[index].load(Ordering::Relaxed),
            }),
            active_guards: self.active_guards.load(Ordering::Acquire),
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let elapsed_ns = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.registry.record(self.phase, elapsed_ns);
        self.registry.active_guards.fetch_sub(1, Ordering::AcqRel);
    }
}

pub fn guard(phase: Phase) -> Guard {
    REGISTRY.guard(phase)
}

pub fn reset() -> Result<(), ActiveGuards> {
    REGISTRY.reset()
}

pub fn snapshot() -> Snapshot {
    REGISTRY.snapshot()
}
```

- [ ] **Step 4: Run and format the registry tests**

Run:

```bash
cargo fmt -- src/server/transactions/phase_profile.rs src/server/transactions/mod.rs
cargo test --features occ_phase_profile phase_profile::tests --lib
```

Expected: all three phase-profile registry tests pass.

- [ ] **Step 5: Commit the registry**

```bash
git add Cargo.toml src/server/transactions/mod.rs src/server/transactions/phase_profile.rs
git commit -m "feat(txn): add opt-in OCC phase registry"
```

### Task 2: Coordinator phase boundaries

**Files:**
- Modify: `src/server/transactions/manager.rs`

- [ ] **Step 1: Add a source-boundary regression test**

In `manager.rs`'s private `tests` module, add a feature-gated test that makes every required coordinator phase name explicit and verifies guard sites remain compile-time gated:

```rust
#[cfg(feature = "occ_phase_profile")]
#[test]
fn coordinator_profile_covers_every_existing_protocol_boundary() {
    let source = include_str!("manager.rs");
    for phase in [
        "Phase::ReadSiteRpc",
        "Phase::AffectedObjectGrouping",
        "Phase::PrepareParticipantLookup",
        "Phase::PrepareBarrier",
        "Phase::CommitBarrier",
        "Phase::AbortParticipantLookup",
        "Phase::AbortCleanup",
        "Phase::EndParticipantLookup",
        "Phase::EndCleanup",
    ] {
        assert!(source.contains(phase), "missing coordinator guard for {phase}");
    }
}
```

- [ ] **Step 2: Run the test to verify missing guards**

Run:

```bash
cargo test --features occ_phase_profile coordinator_profile_covers_every_existing_protocol_boundary --lib
```

Expected: FAIL with `missing coordinator guard for Phase::ReadSiteRpc`.

- [ ] **Step 3: Instrument uncached participant reads**

At the start of `read_from_site`, before `start_time`, add:

```rust
#[cfg(feature = "occ_phase_profile")]
let _phase_guard = super::phase_profile::guard(super::phase_profile::Phase::ReadSiteRpc);
```

This intentionally covers retries, response clock merging, and caching while leaving cache hits unmeasured.

- [ ] **Step 4: Instrument grouping, participant lookup, and barriers without combining them**

In `do_prepare`, replace only the body between `ensure_rw_state` and the existing result reduction with:

```rust
self.ensure_rw_state(&txn)?;
{
    #[cfg(feature = "occ_phase_profile")]
    let _grouping_guard =
        super::phase_profile::guard(super::phase_profile::Phase::AffectedObjectGrouping);
    self.generate_affected_objs(&mut txn);
}
let affect_objs = &txn.affected_objects;
let data_sites = {
    #[cfg(feature = "occ_phase_profile")]
    let _lookup_guard =
        super::phase_profile::guard(super::phase_profile::Phase::PrepareParticipantLookup);
    self.data_sites_for_objs(affect_objs).await?
};
let sites_prepare_result = {
    #[cfg(feature = "occ_phase_profile")]
    let _prepare_guard =
        super::phase_profile::guard(super::phase_profile::Phase::PrepareBarrier);
    self.sites_prepare(&tid, affect_objs, &data_sites).await?
};
if sites_prepare_result == DMPrepareResult::Success {
    let sites_commit_result = {
        #[cfg(feature = "occ_phase_profile")]
        let _commit_guard =
            super::phase_profile::guard(super::phase_profile::Phase::CommitBarrier);
        self.sites_commit(&tid, affect_objs, &data_sites).await?
    };
    match sites_commit_result {
        DMCommitResult::Success => TMPrepareResult::Success,
        _ => TMPrepareResult::DMCommitError(sites_commit_result),
    }
} else {
    TMPrepareResult::DMPrepareError(sites_prepare_result)
}
```

- [ ] **Step 5: Instrument abort lookup and cleanup separately**

In `Service::abort`, replace the current `result` expression with:

```rust
let data_sites = {
    #[cfg(feature = "occ_phase_profile")]
    let _lookup_guard =
        super::phase_profile::guard(super::phase_profile::Phase::AbortParticipantLookup);
    self.data_sites_for_objs(changed_objs).await
};
let result = match data_sites {
    Ok(data_sites) => {
        debug!("ABORT AFFECTED OBJS: {:?}", changed_objs);
        #[cfg(feature = "occ_phase_profile")]
        let _cleanup_guard =
            super::phase_profile::guard(super::phase_profile::Phase::AbortCleanup);
        self.sites_abort(&tid, changed_objs, &data_sites).await
    }
    Err(error) => Err(error),
};
```

- [ ] **Step 6: Instrument explicit commit cleanup lookup and end separately**

In `Service::commit`, replace the current `result` expression with:

```rust
let data_sites = {
    #[cfg(feature = "occ_phase_profile")]
    let _lookup_guard =
        super::phase_profile::guard(super::phase_profile::Phase::EndParticipantLookup);
    self.data_sites_for_objs(affected_objs).await
};
let result = match data_sites {
    Ok(data_sites) => {
        #[cfg(feature = "occ_phase_profile")]
        let _cleanup_guard =
            super::phase_profile::guard(super::phase_profile::Phase::EndCleanup);
        self.sites_end(&tid, affected_objs, &data_sites).await
    }
    Err(error) => Err(error),
};
```

- [ ] **Step 7: Run coordinator tests in both feature modes**

Run:

```bash
cargo fmt -- src/server/transactions/manager.rs
cargo test coordinator_profile_covers_every_existing_protocol_boundary --lib
cargo test --features occ_phase_profile coordinator_profile_covers_every_existing_protocol_boundary --lib
```

Expected: the default command selects zero tests; the feature command passes one test, and compilation confirms every guard is valid across `await` and early-return paths.

- [ ] **Step 8: Commit coordinator instrumentation**

```bash
git add src/server/transactions/manager.rs
git commit -m "feat(txn): profile OCC coordinator phases"
```

### Task 3: Participant phase boundaries

**Files:**
- Modify: `src/server/transactions/data_site.rs`

- [ ] **Step 1: Write the participant boundary regression test**

Add this feature-gated test to `data_site.rs`'s private test module:

```rust
#[cfg(feature = "occ_phase_profile")]
#[test]
fn participant_profile_covers_every_existing_protocol_boundary() {
    let source = include_str!("data_site.rs");
    for phase in [
        "Phase::ParticipantPrepare",
        "Phase::ParticipantCommit",
        "Phase::ParticipantAbort",
        "Phase::ParticipantEnd",
    ] {
        assert!(source.contains(phase), "missing participant guard for {phase}");
    }
}
```

- [ ] **Step 2: Run the test to verify missing guards**

Run:

```bash
cargo test --features occ_phase_profile participant_profile_covers_every_existing_protocol_boundary --lib
```

Expected: FAIL with `missing participant guard for Phase::ParticipantPrepare`.

- [ ] **Step 3: Add participant prepare, abort, and end guards**

Add the following as the first line inside `prepare`'s `async move` block:

```rust
#[cfg(feature = "occ_phase_profile")]
let _phase_guard =
    super::phase_profile::guard(super::phase_profile::Phase::ParticipantPrepare);
```

Add these guards at the beginning of the synchronous bodies of `abort` and `end`, before their first state mutation or early return:

```rust
#[cfg(feature = "occ_phase_profile")]
let _phase_guard =
    super::phase_profile::guard(super::phase_profile::Phase::ParticipantAbort);
```

```rust
#[cfg(feature = "occ_phase_profile")]
let _phase_guard = super::phase_profile::guard(super::phase_profile::Phase::ParticipantEnd);
```

- [ ] **Step 4: Add a commit guard that survives the indexed async path**

At the beginning of `commit`, before `update_clock`, add:

```rust
#[cfg(feature = "occ_phase_profile")]
let phase_guard =
    super::phase_profile::guard(super::phase_profile::Phase::ParticipantCommit);
```

Inside the indexed `async move` response block, add this first line so the guard is held until index completion:

```rust
#[cfg(feature = "occ_phase_profile")]
let _phase_guard = phase_guard;
```

Immediately before the non-indexed final `self.response_with(...)`, add:

```rust
#[cfg(feature = "occ_phase_profile")]
let _phase_guard = phase_guard;
```

The mutually exclusive branches move the feature-only guard exactly once; default builds contain neither binding.

- [ ] **Step 5: Run participant tests and the OCC correctness suite**

Run:

```bash
cargo fmt -- src/server/transactions/data_site.rs
cargo test --features occ_phase_profile participant_profile_covers_every_existing_protocol_boundary --lib
cargo test --features occ_phase_profile server::transactions::occ_tests --lib
```

Expected: participant boundary test passes and all OCC correctness tests pass.

- [ ] **Step 6: Commit participant instrumentation**

```bash
git add src/server/transactions/data_site.rs
git commit -m "feat(txn): profile OCC participant phases"
```

### Task 4: Phase report arithmetic and JSON

**Files:**
- Modify: `benches/occ_support/metrics.rs`
- Modify: `tests/occ_bench_metrics.rs`

- [ ] **Step 1: Write a failing synthetic snapshot report test**

Add this test to `tests/occ_bench_metrics.rs`:

```rust
#[cfg(feature = "occ_phase_profile")]
#[test]
fn phase_snapshot_reports_per_invocation_and_per_commit_costs() {
    use neb::server::transactions::phase_profile::{
        Phase, PhaseMeasurement, Snapshot, PHASE_COUNT,
    };

    let mut phases = [PhaseMeasurement::default(); PHASE_COUNT];
    phases[Phase::PrepareBarrier as usize] = PhaseMeasurement {
        total_ns: 1_200,
        invocation_count: 3,
    };
    let snapshot = Snapshot {
        phases,
        active_guards: 0,
    };
    let mut summary = BatchMetrics::default();
    summary.record_success(Duration::from_nanos(900), 1, 0);
    summary.record_success(Duration::from_nanos(900), 1, 0);
    let mut summary = summary.summary(Duration::from_nanos(1_800));
    summary.attach_phase_snapshot(&snapshot).unwrap();

    let prepare = summary
        .phases
        .get("prepare_barrier")
        .expect("prepare barrier report");
    assert_eq!(prepare.total_ns, 1_200);
    assert_eq!(prepare.invocation_count, 3);
    assert_eq!(prepare.ns_per_invocation, 400.0);
    assert_eq!(prepare.ns_per_commit, 600.0);

    let encoded = serde_json::to_value(&summary).unwrap();
    assert_eq!(encoded["phases"]["prepare_barrier"]["total_ns"], 1_200);
    assert!(encoded["phases"].get("participant_end").is_some());
}
```

- [ ] **Step 2: Run the test to verify report support is absent**

Run:

```bash
cargo test --features occ_phase_profile --test occ_bench_metrics phase_snapshot_reports_per_invocation_and_per_commit_costs
```

Expected: compilation fails because `ScenarioSummary::phases` and `attach_phase_snapshot` do not exist.

- [ ] **Step 3: Implement feature-only phase summaries**

In `benches/occ_support/metrics.rs`, add these imports and type:

```rust
#[cfg(feature = "occ_phase_profile")]
use neb::server::transactions::phase_profile::{Snapshot, PHASES};

#[cfg(feature = "occ_phase_profile")]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PhaseSummary {
    pub total_ns: u64,
    pub invocation_count: u64,
    pub ns_per_invocation: f64,
    pub ns_per_commit: f64,
}
```

Add this field to `ScenarioSummary` after `invariants_passed`:

```rust
#[cfg(feature = "occ_phase_profile")]
pub phases: BTreeMap<String, PhaseSummary>,
```

Initialize it in `BatchMetrics::summary`:

```rust
#[cfg(feature = "occ_phase_profile")]
phases: BTreeMap::new(),
```

Add this implementation:

```rust
#[cfg(feature = "occ_phase_profile")]
impl ScenarioSummary {
    pub fn attach_phase_snapshot(
        &mut self,
        snapshot: &Snapshot,
    ) -> Result<(), PhaseSnapshotError> {
        if snapshot.active_guards != 0 {
            return Err(PhaseSnapshotError::ActiveGuards(snapshot.active_guards));
        }
        self.phases = PHASES
            .iter()
            .map(|phase| {
                let measurement = snapshot.phases[*phase as usize];
                let ns_per_invocation = if measurement.invocation_count == 0 {
                    0.0
                } else {
                    measurement.total_ns as f64 / measurement.invocation_count as f64
                };
                let ns_per_commit = if self.committed == 0 {
                    0.0
                } else {
                    measurement.total_ns as f64 / self.committed as f64
                };
                (
                    phase.as_str().to_string(),
                    PhaseSummary {
                        total_ns: measurement.total_ns,
                        invocation_count: measurement.invocation_count,
                        ns_per_invocation,
                        ns_per_commit,
                    },
                )
            })
            .collect();
        Ok(())
    }
}

#[cfg(feature = "occ_phase_profile")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhaseSnapshotError {
    ActiveGuards(usize),
}
```

- [ ] **Step 4: Add empty feature-only phase maps to existing struct literals**

In the two `ScenarioSummary` literals in `run_report_replaces_a_scenario_by_name`, add:

```rust
#[cfg(feature = "occ_phase_profile")]
phases: std::collections::BTreeMap::new(),
```

- [ ] **Step 5: Run metrics tests in both modes**

Run:

```bash
cargo fmt -- benches/occ_support/metrics.rs tests/occ_bench_metrics.rs
cargo test --test occ_bench_metrics
cargo test --features occ_phase_profile --test occ_bench_metrics
```

Expected: all default metrics tests pass; the feature run additionally passes the phase snapshot arithmetic and JSON test.

- [ ] **Step 6: Commit report support**

```bash
git add benches/occ_support/metrics.rs tests/occ_bench_metrics.rs
git commit -m "bench(txn): report OCC phase timings"
```

### Task 5: Reset and capture each benchmark batch

**Files:**
- Modify: `benches/occ_transactions.rs`
- Modify: `tests/occ_bench_metrics.rs`

- [ ] **Step 1: Write a failing driver integration assertion**

Add this test to `tests/occ_bench_metrics.rs`:

```rust
#[cfg(feature = "occ_phase_profile")]
#[test]
fn occ_driver_brackets_every_workload_batch_with_phase_registry_calls() {
    let driver = include_str!("../benches/occ_transactions.rs");
    assert_eq!(driver.matches("reset_phase_profile();").count(), 4);
    assert_eq!(driver.matches("snapshot_phase_profile(&mut summary);").count(), 1);
}
```

The four reset call sites are shared RMW, projected reads, blind update, and blind remove; each passes through the single `publish` snapshot site.

- [ ] **Step 2: Run the test to verify the benchmark is not bracketed**

Run:

```bash
cargo test --features occ_phase_profile --test occ_bench_metrics occ_driver_brackets_every_workload_batch_with_phase_registry_calls
```

Expected: FAIL because both counts are zero.

- [ ] **Step 3: Add feature-only reset and snapshot helpers**

Add these functions near `report_path` in `benches/occ_transactions.rs`:

```rust
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
        .unwrap_or_else(|error| panic!("invalid OCC phase snapshot: {error:?}"));
}

#[cfg(not(feature = "occ_phase_profile"))]
fn snapshot_phase_profile(_summary: &mut occ_support::metrics::ScenarioSummary) {}
```

In `publish`, make `summary` mutable and snapshot before taking the report lock:

```rust
let mut summary = batch.metrics.summary(elapsed);
snapshot_phase_profile(&mut summary);
```

- [ ] **Step 4: Reset immediately before all four workload entry points**

Insert `reset_phase_profile();` as the first statement of each `iter_custom` closure, immediately before `runtime.block_on(...)`, in:

1. `register_rmw`;
2. `projected_reads`;
3. blind update;
4. blind remove.

The shared RMW call brackets all six selected portfolio scenarios without duplicating the protocol driver.

- [ ] **Step 5: Run driver and smoke tests**

Run:

```bash
cargo fmt -- benches/occ_transactions.rs tests/occ_bench_metrics.rs
cargo test --features occ_phase_profile --test occ_bench_metrics occ_driver_brackets_every_workload_batch_with_phase_registry_calls
NEB_OCC_BENCH_LABEL=phase-smoke NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" \
  cargo bench --features occ_phase_profile --bench occ_transactions -- occ/independent_rmw/1 --test
```

Expected: the driver assertion passes; `target/occ-bench/phase-smoke.json` has zero unexpected outcomes, `invariants_passed: true`, and all 13 phase keys.

- [ ] **Step 6: Commit benchmark integration**

```bash
git add benches/occ_transactions.rs tests/occ_bench_metrics.rs
git commit -m "bench(txn): capture OCC phase snapshots"
```

### Task 6: Prove default builds are uninstrumented and document operation

**Files:**
- Create: `scripts/check-occ-phase-profile-default.sh`
- Modify: `benches/README.md`

- [ ] **Step 1: Add the default-artifact check script**

Create executable `scripts/check-occ-phase-profile-default.sh` with:

```bash
#!/usr/bin/env bash
set -euo pipefail

cargo rustc --lib --release -- --emit=obj
artifact="$(find target/release/deps -maxdepth 1 -type f -name 'neb-*.o' -printf '%T@ %p\n' \
  | sort -nr | head -n 1 | cut -d' ' -f2-)"
if [[ -z "${artifact}" ]]; then
  echo "default OCC profile check: no object artifact found" >&2
  exit 1
fi
if nm -a "${artifact}" | grep -q 'phase_profile'; then
  echo "default OCC profile check: profiler symbol found in ${artifact}" >&2
  exit 1
fi
echo "default OCC profile check: no profiler symbols in ${artifact}"
```

Run:

```bash
chmod +x scripts/check-occ-phase-profile-default.sh
scripts/check-occ-phase-profile-default.sh
```

Expected: `default OCC profile check: no profiler symbols`.

- [ ] **Step 2: Document feature operation and report semantics**

Append this section to `benches/README.md`:

```markdown
### OCC phase profiling

`occ_phase_profile` is a non-default diagnostic feature. Default builds contain no
phase clocks or counter updates. A profiling build reports fixed coordinator and
participant phase totals under each scenario's `phases` object:

```bash
NEB_OCC_BENCH_LABEL=phase-profile \
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" \
  cargo bench --features occ_phase_profile --bench occ_transactions -- \
  'occ/independent_rmw/1$'
```

Each phase reports `total_ns`, `invocation_count`, `ns_per_invocation`, and
`ns_per_commit`. Coordinator barriers include their participant RPC work, so participant
times are nested diagnostics. Summed phase time can exceed elapsed wall time under
concurrency and must not be interpreted as a percentage. A snapshot with active guards
is invalid and makes the benchmark fail rather than publish partial data.

The controlled profiling portfolio is `occ/independent_rmw/1`, `occ/hot_rmw/8`,
`occ/hot_rmw/32`, `occ/multi_cell/8`, `occ/multi_participant/1`, and
`occ/multi_participant/4`, run serially on `192.168.10.17` with NUMA node 0 binding.
```

- [ ] **Step 3: Run documentation and default-build checks**

Run:

```bash
scripts/check-occ-phase-profile-default.sh
cargo test --test occ_bench_metrics
git diff --check
```

Expected: symbol check passes, default metrics tests pass without phase fields, and `git diff --check` prints nothing.

- [ ] **Step 4: Commit operational documentation**

```bash
git add scripts/check-occ-phase-profile-default.sh benches/README.md
git commit -m "docs: document OCC phase profiling"
```

### Task 7: Full correctness gate with the feature disabled and enabled

**Files:**
- Verify only; no production file changes expected.

- [ ] **Step 1: Run formatting and targeted default tests**

Run:

```bash
cargo fmt --check
cargo test --test occ_bench_metrics
cargo test server::transactions::occ_tests --lib
```

Expected: all commands pass; default reports have no `phases` field.

- [ ] **Step 2: Run targeted feature tests**

Run:

```bash
cargo test --features occ_phase_profile phase_profile::tests --lib
cargo test --features occ_phase_profile --test occ_bench_metrics
cargo test --features occ_phase_profile server::transactions::occ_tests --lib
```

Expected: registry, report, benchmark smoke, repeatable-read, certification, wait-die, and lost-update coverage all pass with profiling enabled.

- [ ] **Step 3: Run repository hygiene checks**

Run:

```bash
scripts/check-occ-phase-profile-default.sh
git diff --check
git status --short
```

Expected: no profiler symbols in the default object, no whitespace errors, and only intentional changes remain.

### Task 8: Controlled remote profile and hypothesis selection

**Files:**
- Create artifacts under: `target/occ-bench/` and `target/criterion/` on `192.168.10.17`; do not commit generated artifacts.

- [ ] **Step 1: Verify host policy, revision, exclusivity, and NUMA tools**

Run on `192.168.10.17`:

```bash
sysctl kernel.perf_event_paranoid
pgrep -af 'cargo bench|occ_transactions' || true
command -v numactl
git rev-parse HEAD
```

Expected: no overlapping benchmark process, `numactl` exists, and the revision matches the profiling commit. The source profiler does not require `perf_event`; lowering the sysctl is optional supplementary sampling only.

- [ ] **Step 2: Run the six exact scenarios serially**

For each filter below, use a unique label and the same revision:

```bash
revision="$(git rev-parse HEAD)"
for scenario in \
  'occ/independent_rmw/1$' \
  'occ/hot_rmw/8$' \
  'occ/hot_rmw/32$' \
  'occ/multi_cell/8$' \
  'occ/multi_participant/1$' \
  'occ/multi_participant/4$'
do
  label="phase-$(printf '%s' "${scenario}" | tr '/$' '--')"
  NEB_OCC_BENCH_LABEL="${label}" \
  NEB_OCC_BENCH_REVISION="${revision}" \
    numactl --cpunodebind=0 --membind=0 \
    cargo bench --features occ_phase_profile --bench occ_transactions -- "${scenario}"
done
```

Expected: every JSON report records the same revision, zero unexpected outcomes, passing invariants, all 13 phase keys, and no active-guard failure.

- [ ] **Step 3: Apply the stability and dominance rules**

For each scenario used to choose a hypothesis, calculate Criterion sample CV and rerun it up to three times if CV exceeds 5%. Quarantine any run with mismatched revision, overlap, active guards, or failed outcome invariants.

For `occ/independent_rmw/1`, compare non-overlapping coordinator `ns_per_commit` values and select a phase only if it is the largest and at least 20% of their sum. For concurrent paths, require that phase to remain the largest in a second stable workload or require a coordinator/participant barrier gap of at least 20% of the coordinator barrier. Do not sum participant time into coordinator time.

- [ ] **Step 4: Record the selected optimization hypothesis without changing protocol phases**

Append the stable measurements, rejected runs, dominance calculation, and one selected internal-cost hypothesis to `docs/superpowers/specs/2026-07-22-occ-performance-optimization-design.md`. The hypothesis may target allocation, cloning, serialization, lookup, locking, or fan-out overhead inside a phase; it must not remove, merge, or skip read observation, participant prepare/certification, prepare barrier, participant commit, or cleanup.

- [ ] **Step 5: Commit only the measured decision record**

```bash
git add docs/superpowers/specs/2026-07-22-occ-performance-optimization-design.md
git commit -m "docs: record OCC phase profile findings"
```

Do not commit `target/occ-bench`, `target/criterion`, `perf.data`, or host configuration.
