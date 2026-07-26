# Task 8 Report: Certified Reads and Shared Pending Commits

## Implementation

- Preserved every point observation as a prepare dependency whenever a
  transaction contains any write. Unchanged objects use `PrepareIntent::Read`;
  read-only transactions still complete locally without participant state.
- Participant prepare now acquires owners in canonical full-`Id` order before
  validating exact `Present(ts)`, tombstone `Absent(Some(ts))`, or never-present
  `Absent(None)` expectations. A pending head blocks validation, and a failed
  validation releases every owner acquired by that attempt.
- The coordinator allocates one HLC only after all prepare responses and clocks
  have settled, passes that complete HLC unchanged to every participant, and
  reports clock exhaustion explicitly. Commit payloads remain changed writes
  only.
- Participant commit rejects non-increasing assigned revisions, installs each
  mutation with `RevisionWrite::pending(commit_hlc.ts)`, retains owners, and
  records the exact shared HLC and `InstalledRevision` handles. Same-HLC commit
  retries are idempotent; conflicting-HLC retries fail.
- Participant end verifies and promotes installed revisions before atomically
  releasing any local owner. Failed release validation retains all owners and
  participant state for retry.
- Added coverage for write skew, selected and partial read certificates, exact
  tombstone/never-present/full-Id expectations, shared distributed timestamps,
  equal snapshot boundaries, pending visibility, and commit/end retry.

## TDD Evidence

RED:

```text
timeout 90s cargo test --lib server::transactions::occ_tests::all_participants_install_the_same_commit_timestamp -- --nocapture --test-threads=1
```

Failed as expected because the two participants installed different local
timestamps:

```text
assertion `left == right` failed
  left: 116988579699097602
 right: 116988579699097603
```

Focused GREEN checks:

- `all_participants_install_the_same_commit_timestamp`: 1 passed.
- `full_read_validation_prevents_point_write_skew`: 1 passed.
- `equal_commit_timestamp_is_invisible_to_snapshot`: 1 passed.
- Exact tombstone/never-present/full-Id certification: 1 passed.
- Partial point-observation certification: 1 passed.
- Pending install, same-HLC retry, conflicting-HLC rejection, `Wait`, and end
  promotion/retry: 1 passed.
- Distributed delayed-participant visibility: 1 passed.
- Existing lost-update prevention: 1 passed.
- Existing commit recheck: 1 passed.

Serial suite GREEN checks:

- `timeout 600s cargo test --lib server::transactions::occ_tests -- --test-threads=1`:
  33 passed, 0 failed.
- `timeout 180s cargo test --lib server::transactions::tests -- --test-threads=1`:
  5 passed, 0 failed.
- `timeout 600s cargo test --lib server::transactions::data_site -- --test-threads=1`:
  37 passed, 0 failed.
- `timeout 600s cargo test --lib server::transactions::manager -- --test-threads=1`:
  15 passed, 0 failed.

The first data-site suite run exposed one obsolete fixture assumption: it tried
to prepare a second transaction against a pending revision and expected
success. Task 8 correctly returned `NotRealizable`; the fixture now directly
simulates stale-owner replacement for its abort-validation purpose. Its focused
rerun passed, followed by the clean 37-test serial suite above.

Additional GREEN checks:

- `cargo test --lib --features occ_phase_profile server::transactions::phase_profile -- --test-threads=1`:
  5 passed, 0 failed.
- Coordinator and participant phase-boundary structural checks: 1 passed each.
- `cargo check --lib`: passed.
- Touched-file `rustfmt --check`: passed.
- `git diff --check`: passed.

## Protocol Audits

- Unchanged reads reach prepare for every writing transaction:
  `generate_affected_objs` retains the complete point-observation map and
  `site_prepare` emits `PrepareIntent::Read` for unchanged entries.
- Commit payloads contain writes only: `sites_commit` filters on
  `data_obj.changed`; participant validation rejects `CommitOp::Read` and
  `CommitOp::None`.
- One coordinator HLC is distributed unchanged: one `try_now()` occurs after
  the prepare barrier and the same `commit_hlc` is captured by every commit RPC.
- No `effective_ts` or Thomas-write skipping remains.
- No Task 7 pin state/read pinning was reintroduced, and no startup-barrier,
  index, range, or non-transactional isolation behavior was changed.
- Existing prepare/commit/abort/end phase boundaries remain present and their
  feature-gated profile tests pass.

## Files

- `src/server/transactions/mod.rs`
- `src/server/transactions/manager.rs`
- `src/server/transactions/data_site.rs`
- `src/server/transactions/occ_tests.rs`

## Deferred Boundaries

- Task 9 compensating revisions for failed/aborted pending installs remain
  intentionally deferred.
- Task 10 stale-owner resolution remains intentionally deferred.
- Index/range MVCC and non-transactional-operation isolation remain out of
  scope.

Recurring service-lookup messages during server shutdown are pre-existing test
fixture teardown logs; all counted suites exited successfully.
