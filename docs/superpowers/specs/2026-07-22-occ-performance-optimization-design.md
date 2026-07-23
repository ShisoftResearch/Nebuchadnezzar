# OCC Transaction Performance Optimization Design

## Summary

Nebuchadnezzar will improve version-certified optimistic concurrency control (OCC) through a measured optimization loop. The loop begins with a release-mode end-to-end benchmark that reports successful transaction throughput, latency, and retry outcomes. Each implementation iteration targets one measured bottleneck, is compared with the unoptimized OCC baseline, and is retained only when it improves the intended workload without weakening the transaction contract or causing a material regression elsewhere.

Correctness is a hard gate rather than a performance trade-off. Repeatable cell snapshots, absence observations, read/write version certification, lost-update prevention, total Wait-Die priority, atomic commit prevalidation, rollback ownership, cancellation cleanup, and irreversible abort decisions remain mandatory.

## Context

The repeatable-read OCC implementation is committed at `52e2fa11`. Existing debug integration tests are useful correctness checks but poor performance measurements because server startup accounts for most of their runtime and contention tests do not count successful commits separately from rejected attempts.

Initial smoke measurements showed:

- Fifty sequential read-modify-write transactions: 1.10 seconds on OCC versus 1.09 seconds on `develop`.
- One hundred concurrent multi-cell attempts: 1.05 seconds on OCC versus 1.02 seconds on `develop`.
- Five hundred clients contending on one cell: 1.28 seconds median on OCC versus 1.05 seconds on `develop`.

These numbers demonstrate that there is work to measure, but they cannot establish transaction throughput. The hot-cell test reports completed tasks rather than successful commits, and every process includes about one second of fixture startup. Linux hardware performance counters are unavailable in the current environment because `perf_event_paranoid` is `4`, so the initial loop must work from release-mode wall-clock distributions and workload counters. Flamegraphs may be added when an environment permits sampling.

## Goals

- Measure OCC performance in units of successful transactions rather than client attempts.
- Measure latency distributions and certification retry rates separately.
- Cover low-contention, high-contention, blind-mutation, multi-cell, multi-participant, and projected-read behavior.
- Exclude server and schema startup from timed samples.
- Establish comparable `develop`, unoptimized OCC, and optimized OCC baselines.
- Optimize one demonstrated bottleneck per iteration.
- Preserve every guarantee in the repeatable-read OCC design.
- Leave a repeatable benchmark and comparison procedure in the repository.

## Non-Goals

- Relaxing repeatable reads, absence caching, or version certification.
- Replacing OCC with locking, MVCC, timestamp ordering, or last-writer-wins behavior.
- Optimizing non-transactional cell RPCs except where shared internal storage work is proven to dominate OCC.
- Treating fewer successful commits as a throughput improvement.
- Using debug-build integration-test duration as the acceptance metric.
- Reworking predicate or range isolation; phantom prevention remains outside the current transaction contract.
- Requiring privileged hardware counters for routine benchmark execution.

## Immutable Correctness Contract

An optimization is invalid if it changes any of these properties:

1. The first cell observation is the transaction's repeatable snapshot for later full, selected, or header reads.
2. An observed missing cell remains missing within that transaction.
3. Every read dependency that influences a write is certified at prepare.
4. Every insert certifies absence, and every update or remove certifies the expected present version.
5. Two transactions derived from the same cell version cannot both commit conflicting writes.
6. The guarantee holds for transactions coordinated by different servers with concurrent vector clocks.
7. All participant expectations are validated before any participant mutation begins.
8. Storage mutations remain conditional on the certified version or absence.
9. Certification ownership remains held through commit or rollback and is released only by its owner.
10. All prepare votes settle before failure cleanup starts.
11. A successful prepare whose response is not delivered is rolled back.
12. Once abort is accepted, commit remains illegal; partial abort failures remain retryable and are not removed by stale cleanup.
13. Read-only transactions continue to avoid distributed prepare.

Timestamp metadata may assist scheduling and cleanup, but it cannot replace cell version or absence validation. Concurrent vector clocks cannot be treated as a total version order.

## Workload Portfolio

Read-modify-write is the primary score because it exercises the guarantee that motivated the OCC restoration. The remaining workloads are mandatory non-regression gates.

### Independent read-modify-write

- One server and one participant.
- Prepopulate at least 4,096 small counter cells.
- Run concurrency levels 1, 8, and 32.
- Each logical operation reads one independently selected cell, updates it, prepares, and commits.
- Each worker continues until the sample reaches a fixed number of successful commits.
- Report successful commits per second, p50/p95/p99 successful-commit latency, attempts, and unexpected errors.

This is the primary optimization score and should expose coordinator, prepare, commit, and cleanup overhead without deliberate conflicts.

### Hot-cell read-modify-write

- One server and one counter cell.
- Run concurrency levels 8 and 32.
- Every rejected transaction retries the complete operation from a new transaction and fresh read.
- Each sample ends after a fixed number of successful increments, not a fixed number of attempts.
- Verify that the final counter delta equals the number of successful commits.
- Report throughput, latency, `NotRealizable` count, complete-transaction retries per success, and unexpected errors. Internal Wait-Die `Wait` responses are not reported because the unchanged public coordinator API consumes them before returning.

This prevents an implementation from appearing faster merely by rejecting more work.

### Multi-cell transaction

- One participant with eight independently selected cells per transaction.
- Run concurrency levels 1 and 8.
- Read and update all eight cells, then prepare and commit once.
- Verify every committed transaction changed all eight cells atomically.

This measures dependency collection, prepare payload construction, certification lookup, commit prevalidation, and conditional mutation scaling.

### Multi-participant transaction

- Three servers in one benchmark cluster.
- Select one cell owned by each participant per transaction.
- Run concurrency levels 1 and 4.
- Require all participants to commit before counting a success.
- Verify the final value of every participant cell.

This measures participant fan-out, vote settlement, and coordinator overhead without changing distributed semantics.

### Blind mutation

- Measure blind update and blind remove separately.
- The update workload replaces an existing cell without a prior transactional read.
- Each measured remove batch consumes a pool of distinct existing cells seeded before the timed region, so target recreation is not included in remove latency.
- Report operation-call latency separately from prepare/commit latency so the version-observation RPC is visible.
- Verify stale blind mutations still fail certification.

### Repeatable projected reads

- Use cells with 64 KiB payloads and a small selected field.
- Measure first `head`, first selected-field read, repeated `head`, repeated selected-field read, and a selected-read followed by a full read.
- Verify all operations within a transaction report the same header version and snapshot data.
- Report first-access and cached-access latency separately.

This exposes the cost of fetching and retaining a complete snapshot while preventing an optimization from returning inconsistent projections.

## Benchmark Architecture

The benchmark will use the unchanged public transaction API so the same harness can run against `develop`, the unoptimized OCC commit, and optimization candidates.

### Files

- `Cargo.toml` registers an `occ_transactions` benchmark with `harness = false`.
- `benches/occ_transactions.rs` defines the Criterion groups and scenario matrix.
- `benches/occ_support/mod.rs` exposes the benchmark support modules.
- `benches/occ_support/fixture.rs` owns server lifecycle, schema installation through the public client API, deterministic cell placement, and untimed seeding.
- `benches/occ_support/workloads.rs` implements transaction retry loops and scenario invariants through the public transaction API.
- `benches/occ_support/metrics.rs` records logical-operation latency, attempts, successful commits, outcome categories, and per-sample throughput; it emits a machine-readable JSON report.
- `benches/README.md` documents commands, environment controls, result interpretation, and comparison rules.

Fixture startup, membership convergence, schema creation, and cell seeding occur before Criterion's timed region. Fixture teardown occurs afterward. Each scenario uses a distinct configurable loopback port range. `NEB_OCC_BENCH_BASE_PORT` overrides the default range when another local process occupies it.

The harness uses Criterion's release build, warm-up, sampling, throughput, and saved-baseline comparison. Workloads that can abort perform a fixed number of successful logical operations inside each measured batch. They retain attempt and retry counters alongside Criterion timing so abort amplification remains visible. Logical-operation latency begins at the first `begin` attempt and ends at the successful commit, including full retries; individual attempt outcomes are reported separately.

`NEB_OCC_BENCH_LABEL` names the run. The metrics module writes `target/occ-bench/<label>.json`, containing scenario configuration, sample distributions, latency percentiles, counters, the Git revision, and whether every invariant passed. Criterion reports remain under `target/criterion`. Generated reports are not committed.

The benchmark commit must use only public APIs shared by `develop` and the OCC branch. It will be cherry-picked onto a temporary worktree at the `develop` baseline, allowing identical benchmark source to measure both implementations.

## Measurement Procedure

Use the following comparison sequence:

1. Build and run the benchmark-only commit on `develop`, saving the Criterion baseline as `develop`.
2. Run the same benchmark source at `52e2fa11`, saving the baseline as `occ-initial`.
3. Run each optimization candidate against `occ-initial` and the immediately preceding accepted candidate.
4. Repeat a noisy scenario until its coefficient of variation is at most 5%, up to three complete runs.
5. If it remains noisier than 5%, report it as inconclusive and do not use it to accept the change.

The normal command is:

```bash
NEB_OCC_BENCH_LABEL=<baseline-name> \
  cargo bench --bench occ_transactions -- --save-baseline <baseline-name>
```

Scenario filters may be supplied after the separator for an iteration's targeted measurement. The complete portfolio must run before an optimization commit is accepted.

## Acceptance Policy

Benchmark measurements are evidence only when fixture invariants pass and unexpected RPC/state errors are zero.

An optimization candidate is accepted when all of the following hold:

- Its targeted stable scenario improves successful-commit throughput by at least 5% or reduces p95 latency by at least 5%.
- The complete stable workload portfolio's geometric-mean throughput does not decrease.
- No stable secondary scenario loses more than 3% throughput or gains more than 5% p95 latency.
- Hot-cell final values equal successful commits, and retry/abort counts are reported rather than hidden.
- All correctness gates pass.

Changes below the threshold are reverted unless they are a prerequisite benchmark/instrumentation change with zero production-path effect. An optimization that improves timing by weakening a fixture invariant or increasing successful-result errors is rejected regardless of magnitude.

## Optimization Loop

Each iteration follows the same sequence:

1. Select the slowest or most expensive stable scenario.
2. Gather timing and workload counters for the unmodified candidate base.
3. Trace the request through coordinator, participant, storage validation, mutation, and cleanup.
4. State one falsifiable bottleneck hypothesis.
5. Add a regression test or benchmark assertion that would detect the unsafe or slow behavior being changed.
6. Run it before production changes and confirm the expected failure or baseline result.
7. Make one minimal production change.
8. Run the targeted correctness tests and targeted benchmark.
9. Run the complete portfolio and correctness gates.
10. Commit only an accepted change; otherwise revert that iteration.

Benchmark infrastructure and production optimizations are separate commits. Every accepted production commit includes its before/after benchmark evidence in the commit message body or an adjacent results note.

## Controlled OCC Phase Profile: 2026-07-23

The phase profiler at revision `38f84e702eab2c7b943361db3019db6a5b7a2232`
was run serially on `192.168.10.17`, pinned to NUMA node 0. The host was idle,
`numactl` was available, and `kernel.perf_event_paranoid` was `2`. Every accepted
or quarantined run recorded the expected revision, all 13 phase keys, passing
workload invariants, and zero unexpected outcomes.

The stable Criterion results were:

| Scenario | Mean throughput | CV | p95 logical latency | Complete retries |
| --- | ---: | ---: | ---: | ---: |
| `occ/independent_rmw/1` | 11,494 commits/s | 1.70% | 103.381 us | 0 |
| `occ/hot_rmw/8` | 9,311 commits/s | 3.25% | 464.885 us | 2,550 |
| `occ/hot_rmw/32` | 7,397 commits/s | 4.61% | 22.145 ms | 6,877 |
| `occ/multi_cell/8` | 4,260 commits/s | 3.91% | 7.387 ms | 1,465 |
| `occ/multi_participant/1` | 10,570 commits/s | 3.24% | 110.333 us | 0 |

The first `independent_rmw/1` run had 9.55% CV and was rejected before the
stable rerun. The first `hot_rmw/32` run had 5.16% CV and was likewise rejected.
All three `multi_participant/4` attempts remained correct but had 14.40%,
19.88%, and 19.09% CV, so that scenario is quarantined and does not influence
the optimization choice.

For stable `independent_rmw/1`, the non-overlapping coordinator phases summed to
54.132 us per commit. `commit_barrier` was the largest component at 21.629 us,
or 39.96% of that sum. Nested `participant_commit` time was 17.630 us, or 81.51%
of the barrier. Stable `multi_participant/1` confirmed the result:
`commit_barrier` was again the largest coordinator component at 28.132 us,
41.08% of the 68.488 us coordinator sum, and nested `participant_commit` was
23.537 us, or 83.67% of the barrier. These two stable workloads satisfy the
phase-dominance rule without relying on participant time as an additional
disjoint component.

A supplementary user-space cycles profile was collected after the phase
decision. It was not used as throughput evidence because sampling perturbed the
Criterion result and Criterion's post-sample analysis shared the profile. It
did confirm that participant commit executes `apply_commit_ops` together with
chunk read and lookup paths.

The selected first production hypothesis is storage-prevalidation reuse.
`apply_commit_ops` validates the current storage state of every commit operation
before mutation, then update and remove paths read the same cells again to
recover their address, old value, and segment coordinates before issuing a
version-conditional mutation. The candidate will build an owned mutation plan
while performing the all-operations prevalidation pass, retain any required
segment guards and rollback values in that plan, and consume it only after
every operation has validated.

This optimization does not remove a distributed phase. All participant
expectations must still pass before the first mutation, participant ownership
remains held, and `update_cell_by` or `remove_cell_by` must still condition the
actual mutation on the certified version. Any failure to create the complete
plan must return without mutation. The candidate is retained only if the
targeted stable benchmark improves by at least 5% and the complete correctness
and non-regression gates pass.

### Rejected storage-prevalidation-reuse attempt

The first implementation attempt was rejected during correctness review before
remote benchmarking. It captured rollback cells, segment guards, and undo-log
restore coordinates during the early all-operations validation pass. A
nontransactional remove and recreate can reuse the same stored version (a new
cell starts at version 1 and its first write stores version 2). The final
version predicate could therefore accept that replacement while abort recovery
restored the older prevalidation snapshot and coordinates.

Preserving rollback correctness requires capturing the overwritten cell,
location, segment guard, and undo record from the final storage mutation guard,
not from the earlier observation. That change would broaden the iteration into
a storage mutation API redesign, so the candidate was quarantined without a
performance claim. The next participant-commit iteration targets redundant
canonical-ID lookup structures, which does not move the point at which storage
or rollback state is observed.

### Rejected commit-lookup-structure attempt

The next candidate, patch digest `5258f0483495`, removed the commit and abort
`BTreeSet` union rebuild, retained the sorted certified IDs created by prepare,
and combined commit subset and payload validation into one linear pass.
Malformed, duplicate, and unsorted commit RPC payloads were rejected before
ownership or storage work. The coordinator remains compatible because it
generates participant operations from an ID-keyed `BTreeMap`.

The new canonical-order test failed before the production change and passed
afterward. Seven focused commit, storage-prevalidation, stale-version, and
rollback tests passed, and an independent static review found no loss of
certification, owner-lock, rollback, or distributed-phase guarantees.

Default-feature release benchmarks ran serially on `192.168.10.17`, pinned to
NUMA node 0. Every reported workload invariant passed and every `unexpected`
outcome list was empty. The stable comparisons were:

| Scenario | Base CV | Candidate CV | Throughput change | Base p95 | Candidate p95 | p95 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `occ/independent_rmw/1` | 2.00% | 2.30% | +4.42% | 107.567 us | 97.512 us | -9.35% |
| `occ/multi_cell/8` | 4.46% | 2.48% | +0.81% | 7.338 ms | 7.248 ms | -1.24% |
| `occ/hot_rmw/8` | 2.74% | 4.29% | -2.39% | 304.384 us | 349.777 us | +14.91% |
| `occ/hot_rmw/32` | 3.13% | 2.87% | -4.94% | 21.527 ms | 22.756 ms | +5.71% |

`occ/multi_participant/1` remained above the 5% CV limit in all three base
runs (8.57%, 6.44%, and 6.05%) and was excluded from the retain/revert
calculation. Across the four stable scenarios, geometric-mean throughput
decreased 0.59%. The candidate therefore failed both the portfolio geometric
mean gate and the secondary throughput/p95 limits. It was quarantined without
a production commit despite improving independent-transaction p95 latency.

### Rejected prepare-payload-reuse attempt

Patch digest `4efd95f3a160` moved construction of the sorted
`Vec<PrepareOp>` outside the coordinator's Wait-Die retry loop. Each retry still
cloned the by-value RPC payload, took a fresh clock snapshot, sent a fresh
prepare RPC, merged the response clock, and repeated backoff and participant
certification. A focused payload-order/expectation/intent test passed, and an
independent static review approved the protocol behavior.

The default-feature hot-cell comparison on `192.168.10.17` was stable and
correct:

| Scenario | Base CV | Candidate CV | Throughput change | Base p95 | Candidate p95 | p95 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `occ/hot_rmw/8` | 2.74% | 3.99% | -0.60% | 304.384 us | 235.416 us | -22.66% |
| `occ/hot_rmw/32` | 3.13% | 3.64% | -6.17% | 21.527 ms | 22.002 ms | +2.21% |

Both workload invariants passed and both `unexpected` outcome lists were
empty. Although hot-8 p95 cleared the improvement threshold, the stable
hot-32 throughput regression exceeded the 3% secondary limit. The candidate
was therefore quarantined without a production commit. Reconstructing
`PrepareOp` values is not treated as the dominant high-contention cost.

### Rejected linear-prepare-validation attempt

Patch digest `aac112078423` replaced participant prepare's first
`BTreeMap` canonicalization with a nonempty, sorted-unique linear validation
and retained the original RPC vector. The certification `BTreeMap`, ownership
checks, expectation validation, and all distributed phases remained unchanged.
An initial review found an idempotent-retry test that depended on the old
silent re-sort; its two payloads were made canonically ordered, the retry,
duplicate, and unsorted-input tests passed, and static re-review approved the
candidate.

The stable default-feature `occ/multi_cell/8` comparison on
`192.168.10.17` had 4.46% base CV and 3.38% candidate CV. Throughput changed
-1.64%, while p95 improved from 7.338 ms to 7.224 ms (-1.56%). Workload
invariants passed and the `unexpected` outcome list was empty. Because neither
target metric improved by 5%, the candidate was rejected before the full
portfolio and preserved only as an audit patch.

### Rejected projected-read snapshot-clone attempt

Patch digest `a95606d68097` changed the coordinator's first-read path to move the
participant's owned `OwnedCell` response directly into the transaction cache.
A full `read` cloned that cached snapshot once for its RPC return, while `head`
cloned only the header and `read_selected` projected directly from the borrowed
cached cell. The transaction cache still retained the complete first snapshot,
including missing observations, so later full reads remained repeatable.
Wait-Die retries, response-clock merging, error/state mappings, read-your-writes,
read expectations, participant prepare certification, and every distributed
phase were unchanged.

The clone-observation test first failed with two full-snapshot clones. After the
refactor it showed zero clones for one header plus one selected read, followed
by exactly one clone for a full read of the same cached version and value. The
focused full, selected-then-full, and header-then-full repeatable-read tests
passed. An independent static review initially rejected a vacuous test hook;
after the full-read control was added, re-review approved the production and
test behavior.

Default-feature release benchmarks ran serially on `192.168.10.17`, pinned to
NUMA node 0, against the saved `projection-cache-base`. Every run passed its
workload invariants and had an empty `unexpected` list:

| Run and scenario | Candidate CV | Throughput change | Base p95 | Candidate p95 | p95 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| run 1, `head` | 7.80% | +7.78% | 92.832 us | 82.098 us | -11.56% |
| run 1, `selected` | 14.83% | +2.80% | 92.276 us | 96.764 us | +4.86% |
| run 1, `mixed` | 17.74% | +43.15% | 93.548 us | 79.629 us | -14.88% |
| run 2, `head` | 2.62% | -3.73% | 92.832 us | 98.873 us | +6.51% |
| run 2, `selected` | 13.50% | +10.94% | 92.276 us | 77.823 us | -15.66% |
| run 2, `mixed` | 5.51% | +16.36% | 93.548 us | 81.710 us | -12.65% |
| run 3, `mixed` | 1.00% | +2.42% | 93.548 us | 91.424 us | -2.27% |

The saved base CVs were 3.32% for `head`, 4.19% for `selected`, and 3.35%
for `mixed`. The apparent large gains did not reproduce within the 5% CV
limit. The stable mixed confirmation improved neither throughput nor p95 by
5%, while the stable second header run crossed both regression limits.
The candidate was therefore rejected before the full portfolio and preserved
only as an audit patch.

### Rejected Wait-Die owner-borrow attempt

Patch digest `dd23dd277509` changed the participant prepare-conflict check from
cloning `CellMeta::owner` to borrowing it while the metadata lock remained
held. Stale-lock reclamation, requester age comparison, `Wait` and
`NotRealizable` outcomes, logging, owner publication, certification, and every
distributed phase remained unchanged.

Test-build instrumentation counted `TxnPriority::clone` directly. The focused
foreign-owner test failed with one clone before the change and passed with zero
afterward; the concurrent-clock Wait-Die test also passed. Release builds
retained the normal derived `Clone` implementation, so the counter added no
benchmark-path work. An independent static review approved the borrow lifetime,
test control, and protocol behavior.

The default-feature hot-cell comparisons on `192.168.10.17` were stable and
correct:

| Scenario | Base CV | Candidate CV | Throughput change | Base p95 | Candidate p95 | p95 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `occ/hot_rmw/8` | 2.74% | 2.08% | -1.79% | 304.384 us | 352.768 us | +15.90% |
| `occ/hot_rmw/32` | 3.13% | 3.50% | -1.18% | 21.527 ms | 19.777 ms | -8.13% |

Both workload invariants passed and both `unexpected` outcome lists were
empty. Although hot-32 p95 cleared the target improvement threshold, the
stable hot-8 p95 regression exceeded the 5% secondary limit, and throughput
did not improve. The candidate was therefore rejected before the full
portfolio and preserved only as an audit patch.

### Rejected transaction-segment-guard deduplication attempt

Patch digest `9b0075cd8208` retained only the first
`SegmentReferenceGuard` for each `(chunk_id, segment_id)` touched by successful
update and remove operations. Multiple rollback snapshots in the same segment
remained protected by that one segment-level reference, and the existing
post-mutation durability loop consequently called `force_wal_sync` once per
unique retained segment rather than once per mutated cell. Storage
prevalidation, version-conditional mutation, history and undo capture,
rollback, ownership checks, and every distributed phase were unchanged.

A focused two-cell test first failed with two retained guards for one old
segment. After the change it retained one guard, kept both history entries,
aborted the committed transaction, restored both original values, and advanced
both rollback versions beyond their committed versions. The existing storage
prevalidation and post-certification change tests passed. Independent static
review approved the segment lifetime, rollback, and durability behavior.

The default-feature `occ/multi_cell/8` comparison on `192.168.10.17` was
stable: base CV was 4.46% and candidate CV was 2.38%. Criterion throughput
changed -0.60%, while p95 changed from 7.338 ms to 7.350 ms (+0.16%).
The workload invariant passed and the `unexpected` outcome list was empty.
Because neither target metric improved by 5%, the candidate was rejected
before the full portfolio and preserved only as an audit patch.

### Rejected shared write-timestamp attempt

Patch digest `3671f1fc4468` changed participant-local `CellMeta::write` from an
owned vector clock to an immutable `Arc<TxnId>`. After commit prevalidation,
one allocation held the effective timestamp and successful multi-cell
mutations shared it. Cleanup, read ordering, and Thomas Write Rule comparisons
borrowed the same clock value. Effective timestamp selection, storage
versions, prepare expectations, rollback, ownership, RPC payloads, and every
distributed phase were unchanged.

A focused test first failed because two metadata entries exposed owned clocks.
After the change, a commit with an RPC clock causally newer than its transaction
ID stored that effective clock in both cells and `Arc::ptr_eq` confirmed one
shared allocation. Post-certification conflict, concurrent stale-update, and
concurrent-clock Wait-Die tests passed. Independent static review approved the
immutable representation and all comparison and success/error paths.

Three default-feature `occ/multi_cell/8` runs were collected on
`192.168.10.17`. All workload invariants passed and all `unexpected` lists
were empty:

| Run | Candidate CV | Throughput change | Base p95 | Candidate p95 | p95 change |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 15.00% | +3.16% | 7.338 ms | 7.386 ms | +0.66% |
| 2 | 41.06% | +33.83% | 7.338 ms | 5.196 ms | -29.19% |
| 3 | 2.93% | -2.62% | 7.338 ms | 7.433 ms | +1.29% |

The first two runs exceeded the 5% CV limit. The stable third run improved
neither target metric and regressed both. The candidate was therefore rejected
before the full portfolio and preserved only as an audit patch.

## Initial Hypotheses

The hypotheses are investigated in this order but reordered when benchmark evidence contradicts it.

### Prepare retry allocation

`site_prepare` rebuilds the same `Vec<PrepareOp>` inside every Wait-Die retry. Build the immutable payload once per participant prepare call and reuse it across retries. This must not reuse participant responses, ownership state, or clocks.

### Participant canonicalization

The coordinator already produces operations from a sorted `BTreeMap`, while the participant rebuilds another `BTreeMap` to canonicalize them. Validate strict ID ordering and uniqueness in one linear pass, then retain the ordered vector. Malformed, duplicate, or unsorted RPC input must still be rejected.

### Commit lookup structures

Commit constructs ordered sets, collects certified IDs, and binary-searches them for every mutation. Measure whether transaction state can retain one canonical ordered ID/index representation from prepare through commit without rebuilding it. The representation must cover all certified dependencies and ownership checks exactly once.

### Storage prevalidation reuse

Commit first validates every cell's current storage state and then reads update/remove targets again during mutation. Investigate a storage mutation plan that captures addresses, versions, and segment guards during the all-cells prevalidation phase and safely consumes those plans after every cell passes. No cell may be mutated until the full payload has passed validation, and every mutation remains conditional on its certified version.

### Blind-mutation observation

Blind update/remove currently performs a header RPC before prepare. First measure its share of latency. Potential fast paths may use a trustworthy version already present in the caller's cell header or co-locate observation without narrowing the conflict window. Deferring observation until prepare, accepting an unknown version, or skipping stale-blind-operation rejection is prohibited.

### Snapshot transfer and cloning

First `head` and selected reads fetch the full cell so a later full read can reproduce the same snapshot without MVCC. Measure serialization, transfer, and cloning costs for large cells. Safe changes may reduce duplicate clones or move an owned snapshot through the stack. Returning only a header or projection without retaining enough data for a later repeatable full read is prohibited.

## Correctness Gates

Every production iteration runs focused tests before benchmarking:

```bash
cargo test --lib server::transactions::occ_tests -- --test-threads=1
cargo test --lib server::transactions::manager::tests -- --test-threads=1
cargo test --lib server::transactions::data_site::tests -- --test-threads=1
```

Before accepting and committing a production optimization, run:

```bash
cargo test --lib server::transactions -- --test-threads=1
cargo test --lib ram::tiered -- --test-threads=1
cargo test --lib index::full_text -- --test-threads=1
cargo check --lib
```

The focused OCC suite must continue to cover repeatable full/selected/header reads, repeatable absence, standard and concurrent-clock lost updates, blind conflicts, multi-participant prepare failure, cancellation, explicit abort races, partial abort retry, commit rejection after abort, ownership checks, and stale-cleanup retention.

Targeted formatting and `git diff --check` must pass. The repository-wide formatting command may continue to report the known pre-existing trailing whitespace in the linked Bifrost checkout; an OCC change must not introduce additional formatting failures.

## Failure Handling

- Fixture setup failure invalidates the sample and stops that scenario.
- An unexpected RPC or transaction state result invalidates the run; it is never counted as an abort or success.
- A correctness invariant failure stops the benchmark immediately.
- Port conflicts identify the occupied address and instruct the operator to set `NEB_OCC_BENCH_BASE_PORT`.
- A candidate with inconclusive timing remains unaccepted until a stable run is obtained.
- A candidate that fails correctness is reverted before another hypothesis is attempted.

## Acceptance Criteria

- A committed release-mode OCC benchmark measures successful throughput, latency distribution, retries, aborts, and unexpected failures for the complete workload portfolio.
- Identical benchmark source establishes `develop` and `52e2fa11` baselines.
- At least one measured production bottleneck is optimized and satisfies the acceptance policy.
- No accepted change weakens any immutable correctness property.
- The complete transaction, tiered-storage, and full-text suites pass after the final accepted iteration.
- Benchmark results and reproduction commands are documented.
- The feature branch remains composed of reviewable benchmark and single-hypothesis optimization commits.
