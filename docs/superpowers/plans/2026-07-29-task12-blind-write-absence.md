# Task 12 Blind-Write Absence Implementation Plan

> **Execution:** Use `superpowers:subagent-driven-development`,
> `superpowers:test-driven-development`, and
> `superpowers:verification-before-completion`.

**Goal:** Restore safe blind create-after-delete behavior without adding a read
RPC and without weakening exact validation of absence reads.

**Scope:** Native MVCC product files only:

- `src/server/transactions/mod.rs`
- `src/server/transactions/manager.rs`
- `src/server/transactions/data_site.rs`
- `src/server/transactions/occ_tests.rs`

Never edit, stage, or format the eleven user-owned ranged-index files. Do not
change Bifrost, Dovahkiin, the compatible benchmark harness, distributed
transaction phases, retry rules, or comparator thresholds.

## Task 1: Write the behavioral RED tests

- Add a public transaction test on unused port `5501` that creates and removes
  a cell, begins a fresh transaction, blind-writes the deleted ID, and expects
  prepare plus commit to succeed.
- Add a public transaction test on unused port `5502` in which T1 reads a
  never-created ID as absent, T2 transactionally creates it, T3
  transactionally removes it, T1 writes the ID, and T1 prepare must return
  `DMPrepareError(NotRealizable)`.
- Add participant/public regressions proving `UnobservedAbsent` with a read
  intent is rejected, a blind write rejects a currently present cell, and two
  blind writers cannot both prepare and commit.
- Run only the two new tests with `--test-threads=1`. The recreate test must
  fail before product code changes; the exact-read test should already pass.
  Run the three hardening regressions after introducing the enum variant.
- Record the RED output in the task report.

## Task 2: Implement the minimum product change

- Add `CellExpectation::UnobservedAbsent`.
- Use it only for the first uncached `write`.
- Add one shared expectation-matching rule at the participant:
  `UnobservedAbsent` with `PrepareIntent::Write` matches current `Absent(_)`;
  `UnobservedAbsent` with any other intent is rejected; all observed
  expectations compare exactly.
- Use that rule in both prepare certification and commit storage-state
  prevalidation.
- Extend absent-write classification, commit-payload validation, and commit-HLC
  validation for the new variant.
- Audit every `CellExpectation` match. Do not mechanically convert observed
  `Absent(None)` test fixtures or read paths.

## Task 3: Verify and review locally

Run one heavy command at a time:

```bash
cargo test server::transactions::occ_tests::transactional_blind_write_recreates_tombstoned_cell -- --exact --test-threads=1
cargo test server::transactions::occ_tests::observed_never_absence_rejects_create_delete_aba -- --exact --test-threads=1
cargo test server::transactions::occ_tests::blind_write_rejects_present_cell -- --exact --test-threads=1
cargo test server::transactions::occ_tests::competing_blind_writers_cannot_both_prepare -- --exact --test-threads=1
cargo test server::transactions::data_site::tests::unobserved_absence_requires_write_intent -- --exact --test-threads=1
cargo test server::transactions::data_site::tests::prepare_certifies_exact_tombstone_never_absence_and_full_id -- --exact --test-threads=1
cargo test server::transactions::occ_tests::lost_update_prepare_rejects_stale_retry_and_fresh_retry_succeeds -- --exact --test-threads=1
```

Then run scoped formatting, `git diff --check`, inspect the four-file diff, and
request an independent correctness review. Fix every Critical or Important
finding and rerun affected tests. Commit only the four product/test files with
subject:

```text
fix(mvcc): distinguish blind and observed absence
```

## Task 4: Rebuild the disposable comparison candidate

- Preserve the failed remote `mvcc-1` JSON and log in a new failure-evidence
  directory before replacing labels.
- Construct a clean disposable candidate whose product ancestry contains the
  native fix and whose compatibility-harness commits remain byte-identical to
  baseline.
- Prove exact native product patch identity, five-file harness byte identity,
  clean worktree state, and unchanged Bifrost/Dovahkiin revisions. Record the
  exact native fix SHA and show that the disposable candidate carries the
  identical product diff.
- Run the local 13-scenario benchmark test mode and strict JSON predicate.
- Transfer by Git bundle; do not push.

## Task 5: Reopen the remote correctness/performance gate

- On `192.168.10.87`, run only the candidate `^mvcc/blind_remove$` benchmark
  first. Validate positive commits, attempts at least commits, passing
  invariants, and empty `unexpected`.
- If clean, run `mvcc-1`, `mvcc-2`, and `mvcc-3` serially with the exact
  13-scenario filter. Do not rerun the three already accepted baseline reports.
- Stop on any failed invariant or unexpected outcome.
- Run the comparator only after all three new candidate reports pass the strict
  predicate and provenance checks.
- Record median/CV, throughput and p99 deltas, comparator exit status, and the
  accepted/rejected regression decision before continuing to Task 13.
