# OCC Phase Profiling Design

**Date:** 2026-07-22

## Purpose

Identify the dominant costs in Nebuchadnezzar's existing distributed OCC transaction
protocol before selecting another production optimization. Profiling must preserve the
explicit protocol sequence and must not merge, skip, or weaken read observation,
participant prepare/certification, the prepare barrier, participant commit, or cleanup.

This work is diagnostic only. It does not change transaction outcomes or qualify as a
retained performance optimization.

## Constraints

- Keep the current distributed protocol and all RPC phase boundaries unchanged.
- Preserve repeatable-read caching, read-version certification, wait-die ordering,
  participant ownership, commit validation, abort recovery, and lost-update prevention.
- Add zero code and timing overhead to default production builds.
- Run controlled measurements on `192.168.10.17` with NUMA node 0 binding.
- Use the existing Criterion and OCC outcome reports; phase timings are diagnostic and do
  not replace the established retain-or-revert policy.

## Considered Approaches

### 1. Compile-time phase instrumentation (selected)

Add a non-default `occ_phase_profile` Cargo feature. When enabled, small RAII timing
guards update per-phase atomic totals and invocation counts. The benchmark resets the
registry immediately before each timed workload batch and captures it immediately after
the batch.

This approach gives transaction-aware measurements, works with the in-process benchmark
fixtures, and compiles completely out of normal builds. Its cost is a clock read and an
atomic update per observed phase in profiling builds.

### 2. System sampling profiler

Linux `perf` would avoid source instrumentation, but the benchmark host currently has
`perf_event_paranoid=4`. More importantly, sampled async stacks would not reliably
separate coordinator barriers from participant work without additional annotations.
Changing host-wide kernel policy solely for this loop is unnecessary.

### 3. Per-transaction event tracing

Detailed event logs could reconstruct every timeline, but formatting, allocation, and I/O
would perturb the short transaction workloads. The output volume would also obscure the
aggregate decision. This is reserved for a later investigation of one specific phase.

## Architecture

Add a feature-gated profiling module under `server::transactions`. It owns a fixed set of
atomic `(total_nanoseconds, invocation_count)` counters plus an active-guard count, and
exposes three operations:

1. create an RAII guard for a named phase;
2. reset all counters before a benchmark batch;
3. take an immutable snapshot, including the active-guard count, after the batch.

Every guard records elapsed wall-clock time on drop, including early-return and error
paths. The registry performs no allocation after initialization. Code that creates guards
is itself guarded with `#[cfg(feature = "occ_phase_profile")]`, so default builds contain
neither clock reads nor atomic operations.

The benchmark support layer serializes the captured snapshot next to its existing outcome
metrics. Phase reports include totals, counts, nanoseconds per invocation, and
nanoseconds per committed operation. For concurrent workloads, summed phase time can
exceed wall time because transactions overlap; reports must not present it as a wall-time
percentage. Concurrency-1 workloads provide the additive latency view.

## Observed Phases

Coordinator measurements:

- `read_site_rpc`: fetching and caching an uncached snapshot from a participant;
- `affected_object_grouping`: building the participant/read-write certification sets;
- `prepare_participant_lookup`: resolving participants before prepare;
- `prepare_barrier`: all participant prepare RPCs, including wait-die retries;
- `commit_barrier`: all participant commit RPCs after prepare succeeds;
- `abort_participant_lookup` and `abort_cleanup`: rollback after rejected or failed
  attempts;
- `end_participant_lookup` and `end_cleanup`: explicit client commit cleanup.

Participant measurements:

- `participant_prepare`: certification, ownership checks, and lock publication;
- `participant_commit`: commit revalidation and storage mutation;
- `participant_abort`: rollback and ownership validation;
- `participant_end`: lock and transaction-state cleanup.

Coordinator barrier timings are inclusive of their participant RPC work. Participant
timings are nested diagnostics, not additional disjoint components. On a one-participant
workload, the difference between the coordinator barrier and participant timing estimates
RPC scheduling, serialization, clock handling, and response overhead.

## Benchmark Integration

Only the OCC benchmark uses the profiler API. Immediately before each `iter_custom` batch,
it resets the counters. After all operations in that batch settle, it takes one snapshot
and publishes it with the scenario report. Warmup data cannot leak into a measured batch,
and scenarios remain serial.

The profiling portfolio on `192.168.10.17` is:

- `occ/independent_rmw/1` for an additive uncontended path;
- `occ/hot_rmw/8` and `occ/hot_rmw/32` for wait-die conflicts and rejected attempts;
- `occ/multi_cell/8` for larger certification sets;
- `occ/multi_participant/1` and `occ/multi_participant/4` for fan-out barriers.

Commands use `numactl --cpunodebind=0 --membind=0` and exact scenario filters. Criterion
sample CV must be at most 5% for any scenario used to choose a hypothesis. A scenario may
be rerun up to three times under the existing stability rule.

## Validation

Unit tests verify reset, accumulation, guard-drop recording, snapshot arithmetic, and JSON
serialization. Benchmark tests verify that a fixed synthetic snapshot reports correct
per-invocation and per-commit values. The existing OCC correctness suite must pass with
the feature both disabled and enabled.

A default-build binary check confirms that profiling-only symbols and report fields are
absent without `occ_phase_profile`. Targeted formatting and `git diff --check` remain
required.

## Decision Rule

For the concurrency-1 additive path, a phase is dominant only when it is the largest
non-overlapping coordinator component and accounts for at least 20% of the summed
coordinator phase time per committed operation. For concurrent paths, the same phase must
either remain the largest coordinator component in a second stable workload or show a
coordinator/participant timing gap of at least 20% of the coordinator barrier. Only then
does the profile select a new hypothesis. The next production design must optimize work
inside the existing phase boundaries. It may reduce allocation, cloning, serialization,
lookup, locking, or fan-out overhead, but it may not remove or combine a distributed
transaction step.

Any resulting production candidate still requires the established gate: at least 5%
improvement on a stable target, nondecreasing aggregate throughput, no stable secondary
throughput regression beyond 3%, no stable secondary p95 regression beyond 5%, zero
unexpected outcomes, and all correctness suites passing.

## Failure Handling

Profiling snapshots are discarded if benchmark processes overlap, the feature build does
not match the recorded revision, a phase guard remains active when a snapshot is taken,
outcome invariants fail, or Criterion CV remains above 5% after three attempts. Invalid
artifacts are quarantined and never used to select an optimization.
