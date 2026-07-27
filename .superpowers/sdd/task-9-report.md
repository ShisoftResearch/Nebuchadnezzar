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

## Review Fixes Round 1

### Implementation

- Added checked exact-output WAL synchronization for `InstalledRevision`
  handles. The helper verifies the full cell/tombstone identity, revision-chain
  node identity, current location, live state, and containing segment, and
  deduplicates revisions sharing a segment.
- Reordered local commit completion to synchronize exact installed output,
  durably record the commit decision, promote pending nodes, then release
  owners and wipe transaction state. A marker failure retains all retry state.
  A durable commit decision is transaction-private and prevents a later abort
  if promotion must be retried.
- Replaced timestamp-only compensation retry state with the exact
  `InstalledRevision` handle. Live and recovery compensation both synchronize
  that exact output before success. Abort completion now records its durable
  marker only after every exact compensation is durable, and releases owners
  only after the marker succeeds.
- Recovery restore-address validation now compares the complete `Id` from the
  stored header, including partition and hash, as well as the exact prior
  revision and existing stable location context.
- Assigned the two-revision undo layout its own record type (`4`); legacy type
  `1` records have no compatibility decoder and cannot consume bytes from an
  adjacent record.
- Recovery observes the maximum installed revision timestamp from all
  incomplete durable undo entries before deciding whether individual storage
  mutations need compensation.
- Durable transactional configurations now require undo storage, undo logger
  initialization failure is fatal, and a participant-side guard rejects a
  durable mutation if undo is unavailable before any cell change.
- Coordinator abort now waits for every participant to resolve successfully
  before sending `end` anywhere. RPC errors, `CannotEnd`, and rollback
  failures retain every participant owner for retry.

### TDD Evidence

Focused RED observations, one finding at a time:

- The two-participant abort regression observed participant A's owner as
  `None` after B failed, instead of retaining it for the global retry barrier.
- The adjacent-record legacy undo bytes decoded as a current record and
  produced a bogus `cell_offset`.
- A prior restore source with the same hash/revision but another partition was
  accepted and invalidated the failed installed head.
- Exact insert/update/delete output synchronization coverage initially did not
  compile because no checked installed-output sync/count interface existed.
- An injected commit-marker failure returned `Success`, promoted the revision,
  and released retry state.
- An injected compensation-output sync failure returned success instead of
  retaining the exact compensation handle and owner.
- Recovery left the fresh revision clock below an undo-only installed
  timestamp.
- A durable transactional server without undo storage initialized
  successfully.

Focused GREEN observations:

- Exact insert, update, and tombstone segments synchronize once per unique
  segment; installed-output sync failure precedes marker emission and retains
  the pending node, owner, undo entry, and transaction.
- Commit-marker failure re-synchronizes the exact output, returns
  `CannotEnd`, and retains state. Retry succeeds. A durable marker followed by
  promotion failure suppresses undo, rejects abort as `AlreadyCommitted`, and
  remains retryable.
- Compensation sync failure retains the exact handle without duplicate
  output. Abort-marker failure retains the owner and recoverable undo state;
  the successful retry suppresses undo only after exact compensation sync.
- Recovery compensation synchronizes its exact output, full-partition
  collision input is rejected before invalidation, legacy adjacent undo is
  rejected, and a fresh clock advances beyond every undo-only installed
  timestamp.
- Durable configuration/logger failures are fatal or rejected before
  mutation, while non-durable in-memory fixtures remain usable.
- The two-participant abort retry leaves both failed values invisible, retains
  both owners while either participant is unresolved, and releases both only
  after retry without duplicate compensation.

### Serial Verification

All bounded server suites ran strictly serially on the final source:

- `timeout 900s cargo test --lib server::transactions::undo_log -- --test-threads=1`:
  33 passed, 0 failed, 686 filtered, 29.56s.
- `timeout 900s cargo test --lib server::transactions::occ_tests -- --test-threads=1`:
  41 passed, 0 failed, 678 filtered, 481.45s.
- `timeout 900s cargo test --lib ram::recovery -- --test-threads=1`:
  37 passed, 0 failed, 2 ignored, 680 filtered, 3.43s.
- `timeout 900s cargo test --lib server::transactions::data_site -- --test-threads=1`:
  50 passed, 0 failed, 669 filtered, 483.34s.
- `timeout 900s cargo test --lib server::transactions::manager -- --test-threads=1`:
  15 passed, 0 failed, 704 filtered, 110.14s.
- `timeout 900s cargo test --lib server::transactions::tests -- --test-threads=1`:
  5 passed, 0 failed, 714 filtered, 5.05s.
- `timeout 900s cargo test --lib server::tests::durable_ -- --test-threads=1`:
  2 passed, 0 failed, 717 filtered, 2.01s.
- `timeout 900s cargo check --lib`: passed with existing warnings.
- Scoped `rustfmt --edition 2021 --check` for all ten touched Rust files:
  passed.
- `git diff --check`: passed.

### Files

- `src/ram/cell.rs`
- `src/ram/chunk.rs`
- `src/ram/segs.rs`
- `src/ram/tests/cell.rs`
- `src/server/mod.rs`
- `src/server/tests.rs`
- `src/server/transactions/data_site.rs`
- `src/server/transactions/manager.rs`
- `src/server/transactions/occ_tests.rs`
- `src/server/transactions/undo_log.rs`
- `.superpowers/sdd/task-9-report.md`

Task 10 stale-owner resolution, index/range MVCC, and unrelated
non-transactional isolation behavior remain explicitly deferred.
