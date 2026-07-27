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

## Review Fixes Round 2

### Root-Cause Trace

#### Critical 1: durable transactions could run without an output WAL

The transaction durability predicate treated backup/recovery storage as
durable output storage. `Segment::force_wal_sync` then returned success when a
segment had no WAL handle. Consequently, a backup + undo configuration could
install a revision only in memory, persist a transaction marker, and suppress
the undo record even though no output WAL existed.

The correction separates "some durable storage is configured" from "a writable
output WAL is configured." Durable transaction startup now requires both WAL
and undo storage; direct participants check both before mutation. Required
exact-output synchronization fails if the exact segment has no WAL, including
when backup storage exists, while genuinely volatile fixtures retain the
permissive path.

#### Critical 2: cleaner relocation could invalidate an output-durability proof

Exact-output synchronization previously validated an address, retained only
its `(chunk_id, segment_id)`, and synchronized later without a segment lease.
The combine cleaner could move that revision and reclaim the source between
validation and synchronization. In WAL-only mode it copied memory to the
destination, skipped backup archiving, and could delete the source WAL before
the destination was durably represented.

Exact sync now acquires fallible, short-lived `SegmentReferenceGuard`s,
revalidates full identity, chain node, location, registry entry, and segment
while each lease is held, and retains those guards through `fsync`. Contention
therefore returns a retryable error. Cleaner relocation writes and strictly
synchronizes the destination WAL before publication or source reclamation.
Required WAL sync can reopen an existing configured WAL after rollover closed
the live writer, but cannot create or silently substitute a missing WAL.
There are no transaction-lifetime segment pins.

#### Critical 3: a chosen distributed outcome could be forgotten

The coordinator unconditionally removed live state after `sites_end`, even
when a participant returned `CannotEnd` or its response was lost. It neither
remembered completed participants nor retained enough durable data to replay
unresolved participants after coordinator restart. Participant startup also
presumed abort when it found incomplete undo, and the old commit marker type
could not distinguish coordinator decision evidence from local participant
completion.

Commit and abort choices now become irrevocable before the first decision
write. Distinct durable coordinator Commit and Abort records persist the
canonical participant set; participant completion retains its separate record
type. Live retries target only unresolved participants, and a caller retry
after live coordinator state is gone reconstructs the outcome and participant
set from durable evidence. Participant startup resolves locally when possible,
otherwise asks `tid.node`; Commit preserves the installed output and writes a
participant completion marker, Abort durably compensates before its marker,
and Unknown/InProgress/RPC failure halts recovery conservatively.

Participant Commit synchronizes its exact output before returning `Success`.
Participant Abort synchronizes the exact compensation before returning
success. The resulting durability proof is cached through `end`, avoiding a
second `fsync`, while failures retain retry state. Reopened undo writers start
after the maximum existing log sequence so recovery completion markers sort
after all prior undo records.

### Focused TDD Evidence

Each GREEN line is one focused test run with one test thread. The initial
behavioral REDs were:

- `recoverable_transaction_configuration_requires_output_wal_storage`: RED
  accepted backup/recovery + undo without WAL; GREEN 1 passed, 0 failed,
  0 ignored, 736 filtered, 1.00s.
- `backup_only_direct_participant_rejects_mutation_without_wal_before_any_change`:
  RED allowed the participant mutation instead of returning `CannotEnd`;
  GREEN 1 passed, 0 failed, 0 ignored, 736 filtered, 11.15s. The post-format
  verification run was also green with 739 filtered in 11.15s.
- `backup_only_chunks_reject_exact_transaction_output_sync_without_wal`: RED
  exact sync returned `Ok(())` for a backup-only segment; GREEN 1 passed,
  0 failed, 0 ignored, 736 filtered, 0.00s.
- `required_wal_sync_rejects_a_segment_without_a_real_wal` and
  `required_wal_sync_accepts_an_existing_wal_after_its_live_handle_was_closed`:
  RED exposed the no-WAL success path and inability to prove a closed live
  writer's existing WAL; each GREEN run passed its single test, with the
  closed-handle run finishing in 0.00s.
- `exact_output_sync_fails_retryably_while_cleaner_has_exclusive_source`: RED
  synchronization succeeded from an unleased stale location; GREEN 1 passed,
  0 failed, 0 ignored, 736 filtered, 0.00s.
- `exact_output_sync_short_lease_blocks_cleaner_relocation_only_until_sync_finishes`:
  RED relocation could cross validation/sync with no lease; GREEN 1 passed,
  0 failed, 0 ignored, 736 filtered, 0.00s.
- `wal_only_relocation_persists_destination_before_source_cleanup_and_recovery`:
  RED reopening after source cleanup could not recover the relocated output;
  GREEN 1 passed, 0 failed, 0 ignored, 736 filtered, 0.01s.
- `marked_committed_insert_update_delete_survive_relocation_and_recovery`: RED
  the marked insert/update/delete set was not fully reconstructible after
  WAL-only relocation; GREEN 1 passed, 0 failed, 0 ignored, 736 filtered,
  0.01s.
- `marked_aborted_compensation_survives_relocation_and_recovery`: RED the
  durable abort marker could outlive its relocated compensation; GREEN 1
  passed, 0 failed, 0 ignored, 736 filtered, 0.01s.
- `participant_commit_syncs_exact_output_before_success_without_emitting_decision`:
  RED participant Commit returned success without an exact-output sync;
  GREEN 1 passed, 0 failed, 0 ignored, 736 filtered, 11.13s.
- `end_retry_after_durable_commit_completion_is_idempotently_successful`: RED a
  lost `end` response followed by retry returned `TransactionNotFound`; GREEN
  1 passed, 0 failed, 0 ignored, 736 filtered, 11.13s.
- `partial_commit_end_failure_retains_coordinator_state_until_retry_completes`:
  RED cleaned coordinator state after A completed and B returned `CannotEnd`;
  GREEN 1 passed, 0 failed, 0 ignored, 736 filtered, 12.26s.
- `coordinator_decision_marker_failure_keeps_commit_irrevocable_and_retryable`:
  RED lacked a durable, participant-distinct commit choice and allowed the
  caller path to lose retryability; GREEN 1 passed, 0 failed, 0 ignored,
  736 filtered, 11.15s.
- `coordinator_abort_marker_failure_defers_compensation_until_retry_is_durable`:
  RED could begin participant abort before the distributed Abort decision was
  durable; GREEN 1 passed, 0 failed, 0 ignored, 737 filtered, 11.15s.
- `restarted_unended_participant_resolves_durable_commit_from_remote_coordinator`:
  RED participant B presumed Abort at startup despite the coordinator's
  chosen Commit; GREEN 1 passed, 0 failed, 0 ignored, 736 filtered, 28.55s.
- The local-decision restart regression was RED because incomplete undo was
  compensated instead of resolved from its durable Commit; GREEN 1 passed,
  0 failed, 0 ignored, 736 filtered, 8.11s.
- `coordinator_commit_decision_is_durable_without_suppressing_participant_undo`,
  `committed_recovery_resolution_preserves_installed_output_instead_of_compensating`,
  and
  `unknown_recovery_resolution_is_conservative_and_leaves_pending_bytes_and_undo_intact`
  respectively exposed conflated decision/completion evidence, presumed-abort
  compensation despite Commit, and destructive handling of Unknown. Each
  GREEN run passed 1 test, failed 0, ignored 0, filtered 736, and finished in
  0.00s.
- Recovery completion regressions for compensated insert, update, and delete
  were RED because successful startup compensation emitted no durable
  participant Abort completion. Their GREEN runs each passed 1 test, failed 0,
  ignored 0, filtered 737, in 7.10s, 7.12s, and 7.12s respectively.

The formal-review follow-up added four further RED checks:

- The compensation retry sync-count assertion was RED at 4 calls versus the
  expected 3 because `end` synchronized the already durable abort compensation
  again. `compensation_sync_failure_retries_the_exact_handle_without_duplicate_output`
  was GREEN with 1 passed, 0 failed, 0 ignored, 738 filtered, 11.13s test time
  and 21.21s wall time.
- `reopened_logger_orders_recovery_completion_after_existing_log_sequences`
  was RED because a reopened writer reused sequence zero, so the completion
  marker sorted before an older undo entry. GREEN: 1 passed, 0 failed,
  0 ignored, 738 filtered, 0.00s test time and 6.20s wall time.
- A deliberate retention-removal mutation made
  `trimming_retains_distributed_decisions_and_participant_completion_evidence`
  RED (`None` instead of the persisted `Some(Commit)` record): 0 passed,
  1 failed, 0 ignored, 739 filtered, 0.00s test time and 4.70s wall time.
  Restored retention was GREEN: 1 passed, 0 failed, 0 ignored, 739 filtered,
  0.00s test time and 4.60s wall time.
- Forgetting coordinator live state made the caller retry RED with
  `TransactionNotFound`. Durable participant-set replay made the Commit
  variant GREEN with 1 passed, 0 failed, 0 ignored, 739 filtered, 11.15s test
  time and 19.51s wall time. The symmetric Abort replay was GREEN with the same
  counts and 11.15s test time, 15.81s wall time.

### Serial Verification

All server suites ran strictly serially. Each completed before its configured
generous bound; no timeout was reached.

- `cargo test --lib server::transactions::undo_log -- --test-threads=1`:
  38 passed, 0 failed, 0 ignored, 702 filtered; 29.56s test time, 29.72s wall.
- `cargo test --lib server::transactions::occ_tests -- --test-threads=1`:
  45 passed, 0 failed, 0 ignored, 695 filtered; 549.36s test time, 549.49s wall.
- `cargo test --lib ram::recovery -- --test-threads=1`: 37 passed, 0 failed,
  2 ignored, 701 filtered; 3.40s test time, 3.60s wall.
- `cargo test --lib ram::cleaner -- --test-threads=1`: 21 passed, 0 failed,
  0 ignored, 719 filtered; 0.35s test time, 0.50s wall.
- `cargo test --lib server::transactions::data_site -- --test-threads=1`:
  53 passed, 0 failed, 0 ignored, 687 filtered; 516.66s test time, 516.82s wall.
- `cargo test --lib server::transactions::manager -- --test-threads=1`:
  15 passed, 0 failed, 0 ignored, 725 filtered; 110.15s test time, 110.27s wall.
- `cargo test --lib server::transactions::tests -- --test-threads=1`: 5
  passed, 0 failed, 0 ignored, 735 filtered; 5.04s test time, 5.20s wall.
- `cargo test --lib server::tests::durable_transaction_configuration_requires_undo_storage -- --test-threads=1`:
  1 passed, 0 failed, 0 ignored, 739 filtered; 1.00s.
- `cargo test --lib server::tests::recoverable_transaction_configuration_requires_output_wal_storage -- --test-threads=1`:
  1 passed, 0 failed, 0 ignored, 739 filtered; 1.01s.
- `cargo test --lib server::tests::durable_undo_initialization_failure_is_fatal -- --test-threads=1`:
  1 passed, 0 failed, 0 ignored, 739 filtered; 1.01s.
- `cargo check --lib`: exit 0 in 0.83s with 58 existing warnings and no
  errors; timeout not reached.
- `rustfmt --edition 2021 src/ram/chunk.rs src/ram/cleaner/combine.rs src/ram/cleaner/tests.rs src/ram/segs.rs src/server/mod.rs src/server/tests.rs src/server/transactions/data_site.rs src/server/transactions/manager.rs src/server/transactions/mod.rs src/server/transactions/occ_tests.rs src/server/transactions/undo_log.rs`:
  exit 0 in 0.15s; timeout not reached.
- `git diff --check`: exit 0 in 0.00s; timeout not reached.

### Files

- `src/ram/chunk.rs`
- `src/ram/cleaner/combine.rs`
- `src/ram/cleaner/tests.rs`
- `src/ram/segs.rs`
- `src/server/mod.rs`
- `src/server/tests.rs`
- `src/server/transactions/data_site.rs`
- `src/server/transactions/manager.rs`
- `src/server/transactions/mod.rs`
- `src/server/transactions/occ_tests.rs`
- `src/server/transactions/undo_log.rs`
- `.superpowers/sdd/task-9-report.md`

Coordinator decisions and participant completion evidence are conservatively
retained by trimming so delayed retries and restart resolution cannot lose
their safety proof. A bounded, acknowledged retirement/compaction protocol is
deferred to Task 10; until then these records can grow the undo-log footprint.
Task 10 stale-owner resolution, index/range MVCC, and unrelated
non-transactional isolation behavior also remain deferred.

## Review Fixes Round 3

### Root-Cause Trace

#### Recovery files were individually synced but their directory entries were not

WAL and undo-log creation used normal filesystem opens after recursive directory
creation. Backup replacement used a direct final filename, and WAL/backup
deletion did not synchronize the containing directory. A successful file
`fsync` therefore did not prove that a newly created filename, rename, or
unlink would survive a crash. Storage locking also pre-created scoped storage
roots without publishing each new directory entry, hiding the same gap from
lower-level open paths.

`ram::durable_fs` now centralizes the publication protocol. It creates nested
directories one component at a time and synchronizes the parent after every
new entry; opens existing files without a directory sync; synchronizes the
parent after new files, renames, and removals; handles bare relative paths via
`.`; and makes failed new-directory publication retryable by removing the
uncertain entry before returning the failure. A missing-file unlink retry
re-synchronizes the parent because an earlier unlink may have succeeded before
its directory sync failed.

The file manager, segment lifecycle, undo logger, and storage lock now use that
protocol. Backup publication writes a `.nbackup.staging` file that discovery
ignores, synchronizes its complete padded contents, then renames it to the final
backup name and synchronizes the directory. Cleaner archive publication follows
the same staged path. Source WAL removal happens only after the destination WAL
or staged backup is durable, and every deletion failure propagates before the
source is removed from the registry or freed from memory.

#### Fallible durability work happened in destructors or was discarded

`PendingEntry::drop` previously attempted the WAL write and unwrapped its
result. A directory-publication failure could therefore panic during a normal
mutation and provided no structured way to stop history publication. Cleaner
combine and segment disposal also discarded or collapsed errors, allowing
in-memory reclamation to outrun durable source cleanup. Failed unpublished
cleaner destinations consumed allocator capacity across retries.

All mutation paths now call fallible `PendingEntry::finish` before installing a
revision in history. A failed WAL publication accounts the reserved bytes as a
dead orphan while leaving tombstone counters unchanged. Drop only releases the
allocation reference. Segment removal and cleaner combine return durability
errors; the background boolean interface logs the error and reports no
progress. An unpublished-destination guard durably discards a failed cleaner
output and returns its allocator slot. On source unlink failure, the source
stays registered and readable; a subsequent combine retries the missing-parent
sync and completes cleanup.

#### Undo depended on a reclaimable segment location and transaction lifetime pin

The prior undo layout stored a chunk/sequence/offset locator and transactions
kept rollback segment guards alive for their entire lifetime. That made undo
recovery depend on a source segment that cleaner wanted to reclaim, and the
lifetime guards prevented reclamation while transactions remained in doubt.

Fresh undo format tag 7 owns the complete prior `OwnedCell` payload for update
and remove records. Insert undo has no prior payload. The payload uses bincode
v2 serde encoding so all value types round-trip without JSON's loss of IEEE
NaN/infinity bit patterns. Decoding validates the fresh tag, operation/payload
shape, complete transaction and cell IDs, revision ordering, exact record
length, and a 16 MiB payload bound; legacy raw-location layouts are rejected.
Recovery compensates from the owned payload after exact installed-head
validation and synchronizes the compensation before the abort marker.

`Transaction::rollback_guards`, the segment-guard acquisition helper, and every
transaction-lifetime rollback pin were removed. Data-site update/remove clones
the prior cell while the ordinary current-cell guard is scoped to the mutation,
and both the durable undo record and in-memory `CellHistory` own that clone.
The cleaner can consequently relocate and reclaim the prior source while a
transaction is in doubt, and live abort still restores the prior value at a
newer revision.

#### Failed undo rotation could age the active writer into the trim window

Rotation incremented the log sequence before a newly published writer was
available. Repeated directory-sync failures could make the still-active file
look old enough for trimming. Rotation is now serialized through the writer
mutex and does not advance `log_seq` or replace the old writer until the new
file is open and durably published. Decision writes propagate rotation errors,
so neither participant nor coordinator can report success without a durable
record.

The variable-length trim scanner was also corrected to advance by each decoded
record size. Distributed decisions and participant completion evidence remain
conservatively retained.

### Focused TDD Evidence

All focused GREEN runs used one test thread. The principal RED-to-GREEN checks
were:

- `failed_new_directory_publication_can_be_retried_safely`: RED left an
  uncertain new directory entry after its parent sync failed; GREEN proved the
  entry is removed and a retry succeeds.
- `bare_relative_directory_is_durably_created`: RED mishandled an empty lexical
  parent; GREEN normalizes it to `.` and records the parent sync.
- `lock_startup_durably_publishes_new_storage_directory`: RED observed zero
  parent syncs for a new scoped storage root; GREEN observed one.
- WAL publication tests proved a new filename is synchronized once, an existing
  WAL remains on the fast path without another directory sync, and directory
  failure reaches the caller.
- `backup_publication_syncs_staging_contents_before_final_rename`: RED had no
  staged rename; GREEN orders staging creation, complete file sync, final
  rename, and directory sync.
- `failed_tombstone_wal_publication_accounts_orphan_without_tombstone_drift`:
  RED left the reserved bytes live; GREEN accounts the orphan once without
  incrementing logical tombstone state.
- `cleaner_repeated_destination_publication_failures_reuse_unpublished_capacity`:
  RED exhausted the chunk after six injected failures; GREEN reuses each
  unpublished allocation and permits the final retry.
- `cleaner_source_directory_sync_failure_returns_error_and_retains_sources`:
  GREEN proves failure propagation, readable registered sources, and successful
  retry cleanup.
- Undo creation/rotation tests prove one directory publication per new file,
  existing-file fast paths, creation failure propagation, coordinator decision
  failure propagation, and repeated failed rotations retaining the active file.
- `undo_owned_prior_round_trips_by_operation_without_a_raw_location` and
  `restart_compensation_uses_owned_payload_after_source_wal_is_removed` prove
  the fresh owned-payload format and source-independent restart recovery.
- `undo_owned_prior_payload_preserves_ieee_float_bits`: RED JSON decoded
  non-finite floats as invalid/null; GREEN bincode preserves their exact bits.
- `trimming_follows_owned_prior_payload_to_participant_completion`: RED the
  variable-size parser lost the following completion; GREEN retains the
  completed outcome.
- Participant and manager WAL/undo directory failure tests prove `CannotEnd`,
  no success marker, no panic, no cell publication, and no durable coordinator
  choice on publication failure.
- `in_doubt_update_does_not_pin_prior_segment_and_abort_uses_owned_prior`
  proves an exclusive cleaner lease and real combine/reclamation can complete
  while the transaction is in doubt, after which abort restores from owned
  history at a newer revision.
- The required full cleaner gate first exposed a deterministic test-helper RED:
  the helper requested revision 100 at snapshot boundary 100, while MVCC
  visibility is strictly before the snapshot and correctly returned
  `Absent(None)`. It now reads the predecessor at installed timestamp 200 and
  asserts that the returned revision is exactly 100. Both marked recovery
  tests then passed together, 2 passed, 0 failed, 759 filtered, in 0.01s.

A final focused aggregate ran 22 exact durability, owned-undo, rotation,
cleaner, recovery, participant, and manager regressions strictly serially:
22 passed, 0 failed, in 35s total.

### Serial Verification

Every required suite completed before the next began, with
`--test-threads=1` and an explicit timeout. No timeout was reached.

- `cargo test --lib server::transactions::undo_log -- --test-threads=1`:
  46 passed, 0 failed, 0 ignored, 715 filtered; 29.56s test time, 29.72s wall.
- `cargo test --lib server::transactions::occ_tests -- --test-threads=1`:
  45 passed, 0 failed, 0 ignored, 716 filtered; 549.58s test time, 549.75s wall.
- `cargo test --lib ram::recovery -- --test-threads=1`: 37 passed, 0 failed,
  2 ignored, 722 filtered; 3.37s test time, 3.50s wall.
- `cargo test --lib ram::cleaner -- --test-threads=1`: 24 passed, 0 failed,
  0 ignored, 737 filtered; 0.35s test time, 0.50s wall.
- `cargo test --lib server::transactions::data_site -- --test-threads=1`:
  55 passed, 0 failed, 0 ignored, 706 filtered; 538.91s test time, 539.05s wall.
- `cargo test --lib server::transactions::manager -- --test-threads=1`:
  16 passed, 0 failed, 0 ignored, 745 filtered; 121.27s test time, 121.37s wall.
- `cargo test --lib server::transactions::tests -- --test-threads=1`: 5
  passed, 0 failed, 0 ignored, 756 filtered; 5.04s test time, 5.20s wall.
- `cargo check --lib`: exit 0; 19.25s cargo time, 19.31s wall, 60 existing
  warnings and no errors.
- `git diff --check`: exit 0.
- Static transaction audit found no `rollback_guards`,
  `acquire_segment_guard`, or transaction use of `SegmentReferenceGuard::new`;
  the fresh undo layout contains no prior chunk/sequence/offset/address fields.

### Files

- `Cargo.toml`
- `src/ram/durable_fs.rs`
- `src/ram/file_manager.rs`
- `src/ram/segs.rs`
- `src/ram/chunk.rs`
- `src/ram/cleaner/combine.rs`
- `src/ram/cleaner/mod.rs`
- `src/ram/cleaner/tests.rs`
- `src/ram/mod.rs`
- `src/ram/tests/blob_schema.rs`
- `src/ram/tests/cell.rs`
- `src/server/storage_lock.rs`
- `src/server/transactions/data_site.rs`
- `src/server/transactions/manager.rs`
- `src/server/transactions/occ_tests.rs`
- `src/server/transactions/undo_log.rs`
- `.superpowers/sdd/task-9-report.md`

Coordinator decisions and participant completion evidence are still retained
conservatively until an acknowledged, bounded retirement/compaction protocol
exists. That storage-growth concern remains deferred to Task 10, together with
stale-owner resolution, index/range MVCC, and unrelated non-transactional
isolation work.
