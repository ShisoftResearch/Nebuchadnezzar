# Task 9 Report: Revision-Based Abort Compensation

## Implementation

- Replaced the undo record's single revision with
  `installed_revision_ts` and `prior_revision_ts`. `None` is encoded as zero,
  persisted revisions remain nonzero, and the decoder recognizes only the new
  fixed field order and length.
- Transactional insert, update, and remove now write, flush, and `sync_data`
  their undo entry before the storage mutation. Update/remove records include
  the exact prior immutable chunk/sequence/offset. An undo write failure
  returns `CannotEnd` before that mutation.
- Added `Chunks::compensate`, which aborts the exact installed pending node and
  installs a newer committed tombstone for an insert or newer restored content
  for an update/delete. A head other than the exact installed node is a
  rollback failure and cannot be overwritten.
- Participant abort retains transaction state, cell owners, and immutable
  segment guards when any compensation fails. Each successful compensation's
  exact revision is recorded in transaction-private state, allowing retry to
  resume without duplicating completed compensation while still rejecting an
  unrelated later winner.
- Startup undo compares the recovered logical head with the logged installed
  revision, invalidates only that exact recovered committed node, verifies the
  prior immutable hash and revision, and installs the same newer compensation.
  A timestamp mismatch is skipped, making completed recovery idempotent and
  preserving later successful revisions.
- Recovery continues attempting independent undo records and returns an
  aggregate error if any compensation fails.
- Added `cfg(test)` hooks for the durable-before-mutation boundary, one-shot
  undo write failure, and allocation failure coverage.

## TDD Evidence

Initial RED evidence:

- `aborted_update_restores_content_with_newer_revision` restored current bytes
  but left the failed pending node visible as `Wait` in snapshot history.
- `undo_decoder_rejects_legacy_single_revision_layout` accepted the old
  single-revision bytes.
- The requested two-revision serialization test initially failed to compile
  because the undo entry lacked both new fields.
- `partial_compensation_retry_resumes_aborted_node_without_duplicates`
  returned `Success(Some(RollbackFailure))` and discarded retry state instead
  of returning `CheckFailed(CannotEnd)`.
- `abort_rejects_a_later_successful_revision_without_overwriting_it` returned
  `Success(None)` instead of `CheckFailed(CannotEnd)` because any newer
  committed head was incorrectly inferred to be an earlier compensation.

Focused GREEN coverage after implementation:

- Aborted insert/update/delete each install the required newer compensation
  and retain the prior snapshot semantics.
- The new undo byte layout round-trips both revisions and rejects the legacy
  record layout.
- The pause immediately before mutation observes unchanged storage and a
  recoverable synced undo entry.
- An injected undo write failure prevents storage mutation.
- Partial compensation failure retains both owners; retry completes the
  remaining compensation without changing the first compensation revision.
- An unrelated later committed revision makes abort fail without changing that
  revision.
- Recovery insert/update/delete compensation is idempotent, and recovery does
  not overwrite a later successful update.

## Serial Verification

All server suites were bounded and run strictly serially:

- `timeout 900s cargo test --lib server::transactions::undo_log -- --test-threads=1`:
  30 passed, 0 failed, 680 filtered, 29.56s on the final source.
- `timeout 1200s cargo test --lib server::transactions::occ_tests -- --test-threads=1`:
  41 passed, 0 failed, 669 filtered, 481.18s.
- `timeout 900s cargo test --lib ram::recovery -- --test-threads=1`:
  37 passed, 0 failed, 2 ignored, 671 filtered, 3.49s.
- `timeout 1200s cargo test --lib server::transactions::data_site -- --test-threads=1`:
  47 passed, 0 failed, 663 filtered, 449.98s.
- `timeout 1200s cargo test --lib server::transactions::manager -- --test-threads=1`:
  15 passed, 0 failed, 695 filtered, 110.14s.
- `timeout 300s cargo test --lib server::transactions::tests -- --test-threads=1`:
  5 passed, 0 failed, 705 filtered, 5.04s.
- `timeout 600s cargo check --lib`: passed with existing warnings.
- Touched-file `rustfmt --check`: passed.
- `git diff --check`: passed.

The full-worktree `cargo fmt --all -- --check` remains blocked by pre-existing
formatting debt outside Task 9, including trailing whitespace in the sibling
`bifrost/src/membership/server.rs` worktree and existing B-tree formatting
differences. The four touched Rust files pass the scoped formatting gate.

## Protocol Audits

- The undo durability hook executes only after `write`, `flush`, and
  `sync_data`, and immediately before the corresponding storage mutation.
- Live compensation identifies the failed revision by the exact
  `InstalledRevision` node, not timestamp ordering. Retry recognizes only the
  exact transaction-private compensation revision.
- Recovery-only invalidation accepts only an exact installed timestamp and a
  recovered committed present/deleted state whose current cell-index mirror is
  consistent.
- Every restored update/delete verifies both the stored full cell identity and
  the exact prior revision at its stable immutable address.
- Recovery observes later-revision mismatches as already compensated or won by
  a later mutation; live abort reports such a mismatch as a rollback failure.
- No stale-owner resolution, index/range MVCC, or non-transactional isolation
  behavior from Task 10 was introduced.

## Files

- `src/ram/chunk.rs`
- `src/server/transactions/data_site.rs`
- `src/server/transactions/occ_tests.rs`
- `src/server/transactions/undo_log.rs`
- `.superpowers/sdd/task-9-report.md`

Recurring service-lookup messages during server shutdown are pre-existing test
fixture teardown logs; every counted suite exited successfully.
