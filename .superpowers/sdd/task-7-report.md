# Task 7 Report: Snapshot Transaction Point Reads

## Scope

Implemented Task 7 from
`docs/superpowers/plans/2026-07-26-point-cell-mvcc.md` on base
`03a92c9a69339edeb5be94cfcda83f4f88031b1f`.

The change replaces transaction-lifetime point-read pins with fixed-snapshot
observations while preserving the Task 8 and Task 9 boundaries.

## Implementation

- Added revision-aware logical observations:
  - `CellExpectation::Present(u64)`
  - `CellExpectation::Absent(Option<u64>)`
  - `ObservedPoint<T>`
- Changed participant full, selected, head, and partial point-read RPCs to:
  - check the current owner first and return `Wait`;
  - advance the participant HLC from the incoming coordinator clock;
  - stamp read metadata with the fixed `TxnId`;
  - resolve visibility exclusively at `TxnId.ts`;
  - return exact present, tombstone, or never-existed expectations;
  - propagate `Wait`, `SnapshotTooOld`, and other read errors.
- Replaced coordinator pin state with transaction-private owned logical cache:
  full cells, headers, and exact-field projections. Cross-shape observations
  must agree on `CellExpectation` before a new result is cached.
- Preserved coordinator-buffered read-your-writes behavior and made read-only
  point transactions clear locally without creating participant transaction
  state.
- Removed point-read pins, participant pin sets, coordinator pinned-server
  tracking, pin-release RPC/queues/flusher, and transaction point-read
  by-address paths.
- Centralized public client point-read result mapping so
  `ReadError::SnapshotTooOld` remains
  `TxnError::ReadError(ReadError::SnapshotTooOld)`.

Modified production/test files:

- `src/server/transactions/mod.rs`
- `src/server/transactions/data_site.rs`
- `src/server/transactions/manager.rs`
- `src/server/transactions/occ_tests.rs`
- `src/client/transaction.rs`

`src/server/transactions/tests.rs` required no source change because its
existing `workspace_wr` integration test already covers buffered
write/read/update/read/remove/read behavior; that module was rerun explicitly.

## TDD Evidence

RED:

- `transaction_reads_revision_older_than_current_head` initially failed because
  the old transaction read returned current revision
  `116988373693956098` at snapshot boundary `116988373693956097`.
- The exact tombstone test initially could not express or inspect the required
  observation: the coordinator still used unit `CellExpectation::Absent` and
  had no expectation test accessor.
- The public-client error regression initially failed with `E0599` because
  `Transaction::map_point_read` did not exist.

GREEN:

- The fixed-snapshot, exact tombstone, never-existed absence, mixed-shape,
  `SnapshotTooOld`, buffered-update, owner-first `Wait`, and participant
  statelessness regressions all pass.
- The public-client `SnapshotTooOld` mapping regression passes.

## Verification

Authoritative bounded module gates:

| Command | Result |
| --- | --- |
| `cargo test --lib server::transactions::data_site -- --nocapture --test-threads=1` | 34 passed, 0 failed |
| `cargo test --lib server::transactions::manager -- --nocapture --test-threads=1` | 15 passed, 0 failed |
| `cargo test --lib server::transactions::occ_tests -- --nocapture --test-threads=1` | 28 passed, 0 failed |
| `cargo test --lib server::transactions::tests -- --nocapture --test-threads=1` | 5 passed, 0 failed |

Fresh post-format gates:

- `cargo check --lib`: exit 0 (repository warnings remain).
- `rustfmt --edition 2021 --check` on all touched Rust files: exit 0.
- `git diff --check`: exit 0.
- `cargo test --lib client::transaction::tests::point_read_mapping_preserves_snapshot_too_old -- --test-threads=1`:
  1 passed, 0 failed.
- `cargo test --lib server::transactions::occ_tests::transaction_reads_revision_older_than_current_head -- --test-threads=1`:
  1 passed, 0 failed.

The apparent long-running module tests were diagnosed rather than treated as a
hang. Individual server fixtures spend about 11 seconds in shutdown; isolated
tests terminate normally. The module runs completed within their explicit
bounds. Repeated RPC service-lookup messages occur during passing fixture
teardown and predate this protocol change.

## Static Audits and Boundaries

The touched transaction/client production paths have zero matches for:

- `PinnedRead`, `PinnedReadSet`, `PinnedReadCache`
- `pinned_servers`, `pinned_reads`, `release_read_pins`
- `read_cell_at`, `read_selected_at`, `head_at`
- `cell_location_and_revision`, `read_partial_raw_at`

Residual raw locations in `transactions/undo_log.rs` are storage/undo-log
internals. Residual `rollback_guards` in `data_site.rs` protect commit rollback
material and were deliberately renamed to distinguish them from removed
point-read guards. Their eventual replacement belongs to Task 9. Storage
by-address history helpers remain outside transaction read paths as required.

Task 8 certification/shared pending-commit work and Task 9 undo/rollback
removal were not implemented in this task.

## Review

Requirements-focused self-review and an independent read-only review found no
Critical or Important issues. The independent reviewer assessed the change as
ready to merge and noted two Minor coverage opportunities:

- retry the owner-first test after owner release with the same old `TxnId`;
- extend read-your-writes coverage across every exposed shape and buffered
  insert/remove combinations.

These are coverage extensions, not observed implementation defects. Existing
tests already exercise owner-first `Wait`, fixed historical reads, all
participant read shapes, coordinator full/selected/head consistency, buffered
update overlay, and integration-level buffered insert/update/remove reads.

Planned commit subject:
`feat(mvcc): resolve transaction point reads by snapshot`.
