# Task 12 Mutation-Path Performance Implementation Plan

> Execute this plan with subagent-driven development. Every implementation
> task receives a fresh review before commit. Run tests locally and benchmarks
> only on `192.168.10.87`.

**Goal:** Recover the stable point-mutation performance regressions without
weakening MVCC/OCC guarantees or removing distributed transaction steps.

**Design:** First remove redundant proof work in RAM-only participant commit
and successful participant end. Measure that tranche. Then optimize revision
chain lookup reuse and retention-worker notification as separately measurable
changes.

**Primary files:** `src/server/transactions/data_site.rs`,
`src/ram/chunk.rs`, `src/ram/history.rs`, focused tests in the same modules,
and ignored benchmark reports under `target/occ-bench`.

---

## Task 1: Review and freeze the optimization contract

1. Review the design against the current product source and the two diagnostic
   JSON reports.
2. Reject any proposal that skips read validation, exact-predecessor checks,
   participant phases, durable WAL sync, pending visibility, or promotion
   rollback.
3. Record review findings and amend the design before source changes.

## Task 2: RAM-only participant commit fast path

1. Add a failing nondurable participant test that records the installed
   segment's force-sync count, commits one update, and expects no force-sync
   attempt while commit/end still succeed.
2. Retain an assertion that the transaction's exact installed revision agrees
   before commit success.
3. Run the new test and confirm RED.
4. Change `apply_commit_ops` so `installed_revisions_agree` always runs, but
   `force_sync_installed_revisions` runs only when
   `durable_storage_configured()` is true.
5. Keep the durable-output readiness gate true after the required proof for
   the configured mode.
6. Run the new test plus existing exact-output sync failure, directory-sync
   failure, commit retry, lost-update, and blind-absence tests.

## Task 3: Eliminate successful end's redundant post-promotion scan

1. Add a test-only agreement/promotion observation or a focused behavioral
   regression proving:
   - successful multi-cell end promotes every exact installed node;
   - injected partial promotion failure restores all earlier nodes to pending;
   - retry then succeeds.
2. Confirm the relevant test is RED if it directly counts duplicate proof
   work; otherwise document why the existing partial-promotion regression is
   the controlling test.
3. Keep the complete pre-promotion agreement check and exact-node promotion
   CAS.
4. On a fully successful promotion loop, do not repeat
   `installed_revisions_agree`.
5. On any promotion failure, restore promoted nodes in reverse order and
   return `CannotEnd` exactly as before.
6. Run focused participant-end, snapshot-visibility, response-loss retry, and
   corruption tests.

## Task 4: Verify, review, and benchmark transaction proof tranche

1. Run scoped rustfmt, focused tests serially, `cargo check --lib`, and inspect
   the exact diff.
2. Obtain independent correctness review.
3. Commit the reviewed transaction-proof change separately.
4. Build disposable exact pre-change and post-change candidates with identical
   Bifrost/Dovahkiin revisions, byte-identical harness files, build settings,
   NUMA placement, ports, and dataset.
5. On an idle `192.168.10.87`, run three serialized default-build reports per
   side for:
   `rmw_one_cell`, `rmw_multi_cell`, `blind_update`, and `blind_remove`.
6. Require correctness-clean reports and throughput CV below 5% on both sides.
   Retain the commit only if targeted median throughput gains exceed both 5%
   and the larger side's CV, targeted p99 does not regress over 5%, and no
   other tranche scenario loses over 5% median throughput or p99. Otherwise
   revert only this tranche and preserve the evidence.

## Task 5: Reuse revision chain/predecessor in direct updates

1. Add focused tests for:
   - current update installs exactly one newer revision;
   - stale/concurrent expected predecessors cannot both install;
   - tombstoned and absent cells retain existing update behavior;
   - snapshot readers still resolve the predecessor;
   - revision timestamps remain strictly increasing.
2. Add test-only counters or a microbenchmark assertion proving that the
   successful direct update performs one chain-map resolution, rather than
   repeated speculative/current lookups.
3. Refactor the guarded update path to return and carry
   `(RevisionChain, predecessor)` through revision allocation, exact
   publication, and retirement.
4. Preserve the exact predecessor pointer comparison immediately before list
   publication.
5. Keep transaction-assigned revision paths and direct allocated-revision
   paths distinct where doing so avoids discarded `InstalledRevision` clones.
   Do not change remove/tombstone publication in this tranche.
6. Run cell, history, cleaner, recovery, and OCC lost-update tests.

## Task 6: Coalesce retention-worker notifications

1. Add deterministic worker tests for:
   - multiple retirements while a notification is pending cause one unpark;
   - retirement racing the worker's clear/park boundary cannot lose a wake;
   - shutdown joins promptly with a pending backlog;
   - every node retains its authoritative original retirement deadline, no
     expiration record is dropped, and no node expires early;
   - a blocked scheduler record may still move to `now + 1` so the existing
     suffix-pruning fairness rule cannot starve.
2. Replace producer-side locking of the join-handle mutex with an immutable
   thread handle.
3. Add an atomic pending-notification latch and implement the worker clear/
   process/park order that preserves an unpark token across races.
4. Do not coalesce or discard expiration records in this task.
5. Run all history and cleaner concurrency tests, including loom/Miri-style
   checks if available.

## Task 7: Verify, review, and benchmark history tranches independently

1. Verify and review chain reuse; commit it separately.
2. Benchmark `non_transactional_update` remotely with three serialized exact
   parent runs and three exact chain-reuse runs.
3. Verify and review notification coalescing; commit it separately.
4. Benchmark `non_transactional_update` again with three exact chain-reuse
   runs and three exact notification-coalescing runs.
5. Keep only correctness-clean changes for which both sides have throughput CV
   below 5%, target median throughput improves by more than both 5% and the
   larger side's CV, and target p99 does not regress over 5%.
   `non_transactional_update` is the complete performance portfolio for each
   individual history tranche. Transactional mutation cases receive
   correctness-only smoke coverage at this point; no performance threshold is
   inferred from one-off smoke data. Their performance is judged by the final
   repeated Task 12 acceptance portfolio.
6. Every measured pair must keep Bifrost, Dovahkiin, harness, build settings,
   NUMA placement, ports, dataset, and remote host fixed.

## Task 8: Final Task 12 acceptance

1. Rebuild the comparison candidate from all accepted optimization commits and
   the audited compatibility harness.
2. Run candidate full correctness coverage.
3. Run fresh serialized default-build comparison reports with exact
   provenance and no process overlap.
4. Apply the unchanged strict comparator to three baseline and three candidate
   reports.
5. Task 12 passes only if every correctness predicate passes, throughput CV is
   below 5% on both sides, and no comparable scenario exceeds the 5%
   throughput or p99 regression threshold.
6. Start Task 13 only after this gate passes.
