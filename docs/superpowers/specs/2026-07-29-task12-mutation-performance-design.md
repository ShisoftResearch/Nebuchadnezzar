# Task 12 Mutation-Path Performance Design

## Status

Approved direction: continue a benchmark-driven optimization loop without
weakening point-cell MVCC, OCC read validation, lost-update prevention,
pending-revision invisibility, durability, or the distributed transaction
protocol.

Task 13 remains blocked until Task 12 passes its performance gate.

## Evidence

The final default-build comparison at product revision `5707f820` is
correctness-clean but has five reproducible mutation regressions:

| Scenario | Throughput | p99 |
|---|---:|---:|
| `non_transactional_update` | -61.92% | +187.00% |
| `rmw_multi_cell` | -35.14% | +60.28% |
| `blind_update` | -30.23% | +54.22% |
| `blind_remove` | -26.40% | +42.36% |
| `rmw_one_cell` | -24.20% | +30.80% |

All attempts committed, so the cost is successful-path execution overhead,
not OCC conflicts or retries. Direct and projected reads are close to the
baseline, which localizes the stable regression to mutation paths.

A matched-harness diagnostic `occ_phase_profile` run on `192.168.10.87` found
these candidate-minus-baseline costs:

| Phase | One-cell RMW | Eight-cell RMW |
|---|---:|---:|
| Participant commit | +6.51 us/commit | +45.01 us/commit |
| Participant end | +4.78 us/commit | +19.96 us/commit |
| Participant prepare | +3.69 us/commit | +10.16 us/commit |
| Read RPC | +0.78 us/commit | +0.88 us/commit |

Commit and end scale with the number of cells. Code comparison identifies the
new repeated work as:

- exact installed-output validation followed by another exact physical-output
  traversal and segment sync attempt, even when no durable storage exists;
- pre-promotion validation, per-node promotion validation, and post-promotion
  validation during participant end;
- revision-chain lookup, node allocation/publication, and one retirement
  enqueue/wakeup for every mutation;
- additional lifecycle/completion bookkeeping that is mostly per transaction.

The profile localizes the aggregate branch delta but is not causal acceptance
evidence: the baseline and candidate necessarily use their respective Bifrost
revisions. Optimization tranches therefore compare the candidate immediately
before and after one source change while keeping Bifrost, Dovahkiin, harness,
build settings, NUMA placement, dataset, and host fixed.

The direct-update path independently performs up to three history
chain/current lookups per successful update and schedules/wakes the retention
worker once per predecessor.

## Invariants

Every optimization must preserve all of the following:

1. A writing transaction validates every point read and exact absence
   observation.
2. A blind write can match any absence but cannot overwrite a present cell.
3. Only the exact expected predecessor may receive the next revision.
4. Revision timestamps are fresh and strictly greater than the predecessor.
5. Pending revisions remain invisible until participant end promotes them.
6. Participant commit success proves that every expected mutation installed
   the exact logical revision.
7. With durable storage configured, participant commit success also proves the
   exact installed physical output is WAL-durable.
8. Partial promotion failure restores every already-promoted node to pending
   before returning a retryable failure.
9. Historical bytes remain retained until their exact retention deadline and
   remain relocatable only through the existing history/segment protocol.
10. No prepare, participant-commit, participant-end, retire, or
    finalize-retirement protocol step is removed.

The documented non-transactional/transactional interoperability limitation is
unchanged. Pure non-transactional operations must now retain their direct
storage path and pay no history cost, as specified in
`2026-07-29-nontransaction-mvcc-cost-isolation-design.md`.

## Selected Optimization Tranches

### Tranche A: remove redundant transaction proof work

Participant commit will always retain `installed_revisions_agree`, which
proves that every expected write has the correct revision node and logical
head. `force_sync_installed_revisions` will run only when durable storage is
configured. In a RAM-only configuration there is no physical durability
promise to establish, so leasing every output segment, decoding it again,
locking segment file state, and issuing a no-op sync is unnecessary. The
durable path remains unchanged.

Participant end will retain one complete logical/physical pre-promotion proof
and the exact-current-node CAS performed by each promotion. A successful set
of exact-node promotions is sufficient; the second full post-promotion scan is
redundant while the transaction and all cell-owner guards remain held. The
rollback path remains unchanged and restores every partial promotion in
reverse order.

This tranche does not skip a distributed phase, a read validation, a storage
mutation, or a durable sync.

### Tranche B: isolate direct operations from history

The chain-reuse optimization was accepted for transaction-owned history, but
history-worker wake experiments did not pass the zero-tail-regression gate and
were reverted.

Direct point-cell operations must not enter history at all. The cell index
contains only raw present-cell addresses, and direct operations consult only
that index. Assigned-revision and snapshot operations retain the existing
history invariants without tagging the cell-index word. Direct delete removes
the index entry and leaves only a legacy durable tombstone whose cleaner
liveness is governed by its predecessor-segment sequence watermark.

### Deferred work

The following remain later candidates and are not part of the first
implementation:

- chain-level expiration-record coalescing;
- applying chain-reuse publication to remove/tombstone paths;
- batching completion-retirement RPCs;
- removing coordinator affected-object deep clones;
- pooling revision nodes;
- changing any wire protocol or transaction phase.

## Validation and Acceptance

Local verification and debugging run only on the local machine. Remote
`192.168.10.87` is used only for serialized benchmarks.

For each tranche:

1. Add focused regression tests before the implementation.
2. Run the focused tests, relevant existing race/durability tests, and
   `cargo check --lib` locally.
3. Obtain independent code review.
4. A candidate-only run may be used only as a smoke test. The keep/reject gate
   uses three serialized default-build runs of the exact pre-change candidate
   and three of the exact post-change candidate. Runs use the same Bifrost and
   Dovahkiin revisions, byte-identical harness, build settings, NUMA placement,
   ports, dataset, and idle remote host.
5. Require correctness-clean reports and throughput CV below 5% on both sides
   for every scenario in the tranche portfolio. Keep the optimization only if
   the targeted median throughput gain is greater than both 5% and the larger
   side's CV, targeted p99 does not regress by more than 5%, and no other
   scenario in the tranche portfolio regresses by more than 5% in median
   throughput or p99.

After accepted tranches, rebuild the exact comparison candidate and run fresh
default-build baseline/candidate acceptance evidence with the unchanged
13-scenario harness and strict comparator. Profile builds are never acceptance
evidence.

`perf` is currently unavailable on the benchmark host because
`kernel.perf_event_paranoid=4`; the existing feature-gated phase profiler is
used until hardware-counter access is restored.
