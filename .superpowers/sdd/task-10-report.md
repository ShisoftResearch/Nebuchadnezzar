# Task 10 Report: Safe Stale-Owner Resolution

## Implementation

### Explicit stale-owner resolution

- Production participant lock aging is exactly 30,000 milliseconds. The
  test-only constructor accepts a shorter timeout and rejects zero or negative
  values.
- Encountering an aged foreign owner no longer changes cell metadata. Prepare
  queues a deduplicated asynchronous resolution job and returns the existing
  Wait-Die result.
- Resolution jobs attempt immediately, then retry with exponential backoff
  from 10 milliseconds to a one-second cap. A weak participant reference
  prevents the resolver itself from adding a strong ownership cycle.
- Coordinator lookup handles local, remote, unavailable, and unknown member
  IDs without panicking. Only an explicit coordinator decision can release an
  owner:
  - `Commit(commit_hlc)` requires the matching coordinator, participant state
    `Committed`, exact full-HLC equality, and agreement for every installed
    revision before idempotent `end`.
  - `Abort` runs idempotent participant abort and then idempotent `end`.
  - `InProgress`, `Unknown`, RPC failure, timestamp mismatch, compensation
    failure, and end failure retain the owner and retry.
- Active read-only committed coordinators resolve to their transaction HLC
  even though they have no participant-installed commit timestamp.
- Manager stale cleanup snapshots candidate IDs, then takes the exact
  transaction guard and rechecks state, activity, and commit dispatch before
  removal. Cell metadata cleanup holds the metadata guard through an
  identity-checked map removal, and prepare retries if a prefetched metadata
  Arc is no longer current.
- Transaction map publication and sorted-watermark registration share one
  lifecycle mutex. The conservative sorted watermark is installed before the
  map entry is exposed; removal rechecks the exact transaction Arc under that
  same mutex. This prevents both replacement loss and ghost IDs that could pin
  CellMeta cleanup.

### Bounded completed decisions and safe retirement

- Completed decisions use an exact logical retention interval of 300,000
  milliseconds after live coordinator cleanup and a strict
  `now < expires_at` boundary. Incomplete retirement does not extend
  coordinator decision visibility.
- A coordinator removes live transaction state and publishes its retained
  decision atomically with respect to resolution, allocating the retention
  deadline at that cleanup boundary. It then writes and synchronizes the
  self-contained completion record; the older durable irrevocable decision
  remains the restart source until that succeeds. Active state is consulted
  first, then the completed cache, then durable status.
- Participant evidence is retired in a background two-step protocol:
  participants durably prepare retirement; the coordinator durably records
  all acknowledgements; participants then durably finalize retirement.
  Prepare/finalize requests are idempotent and are never placed on the user
  response path.
- Lost responses retain participant evidence and are retried. Startup scans
  incomplete durable completions and resumes retirement jobs.
- Unresolved decisions have no completion deadline. Incomplete coordinator
  completion metadata remains durable after its resolution-visibility
  deadline solely so retirement can continue; resolution is `Unknown` at and
  after equality. Unfinalized participant retirement proof remains
  indefinite. A participant's strict 300,000-millisecond expiry starts only
  when it accepts finalize.
- An expired completion remains a masking state, so restart or compaction
  cannot fall back to an older durable decision for the same transaction.
- Participants keep an in-memory final-outcome cache for idempotent
  abort/end and duplicate-prepare handling. Durable startup rebuilds it once
  from canonical undo state; normal lookups use a `HashMap` with targeted lazy
  expiry and a background sweep, never a per-request log scan or whole-cache
  scan.
- Final completion evidence is published while the exact transaction and
  owner guards remain held, before map removal. It is cached only after owner
  clearing succeeds, so a durable marker left by failed revision promotion
  cannot erase a legitimate live retry transaction. Delayed duplicate
  prepares fail closed with the exact final state and may remove only a
  placeholder they themselves created.

### Crash-safe global undo-log compaction

- Added durable coordinator-completion, participant-retirement, and
  compaction-snapshot records while preserving the existing undo record
  layout.
- Compaction folds every log generation into one canonical state, retains all
  unresolved evidence, expires only fully acknowledged evidence, and publishes
  a higher-sequence snapshot barrier before removing source files.
- The staging snapshot is flushed and synchronized before rename. Its already
  synchronized file handle is carried across publication and adopted as the
  active writer, eliminating a post-publication reopen/fallback window.
- If rename succeeds but parent-directory synchronization fails, the
  published higher-sequence snapshot is still adopted and later appends,
  rotation, and compaction remain fail-closed until publication sync
  succeeds.
- A published snapshot barrier masks any superseded lower file that survives
  a crash or unlink failure. Repeated compaction therefore remains bounded
  instead of re-accumulating completed records.
- Generation read/write synchronization and the active-writer mutex prevent
  compaction from unlinking an enumerated scanner input, prevent scanners from
  observing partial appends, and serialize append/rotation with snapshot
  adoption.
- Canonical recovery deduplicates byte-identical undo records, and per-TID
  replay locks serialize concurrent restart completion so duplicate replay
  cannot duplicate or erase retirement progress.

## TDD and Failure Evidence

- The clean base already preserved aged ownership but never initiated
  resolution. The first resolver tests therefore observed an owner that stayed
  pending indefinitely even after a known coordinator decision.
- Constructor, durable completion, retirement, and compaction tests were added
  before their APIs and record formats; they initially failed to compile
  against the missing behavior.
- A real unavailable-coordinator test initially panicked while translating an
  unknown member ID. The coordinator lookup now returns a retryable error; the
  owner remains installed across repeated attempts.
- Fault-injection tests exercised unknown decisions, RPC response loss,
  commit-HLC mismatch, abort-marker failure, snapshot-sync failure,
  post-rename directory-sync failure, source-file survival, paused scanners,
  and append/rotation during adoption.
- Independent review found and drove deterministic regressions for manager
  stale-cleanup TOCTOU, CellMeta cleanup/orphaning, double
  post-rename-directory-sync failure, concurrent replay, lost volatile end
  responses, and duplicate undo growth. Each regression failed before its
  corresponding fix and passed afterward.
- A second review found delayed read-only duplicate prepare resurrection.
  Durable and volatile regressions now prove completed transactions cannot
  republish owners. The failed-promotion regression proves a marker written
  before promotion failure does not replace the exact live Arc, pending
  revisions, or owners, and that a later end retry succeeds.
- Cache-path review found whole-map pruning on ordinary lookups and insertion.
  The cache was changed to targeted `HashMap` expiry plus bounded background
  maintenance; a regression proves one TID lookup does not scan unrelated
  expired entries.
- Registry review found the gap between transaction-map publication and
  sorted-ID insertion. A deterministic pause test failed when end crossed the
  partial boundary and passed after serialized, identity-checked lifecycle
  publication/removal.
- The first final data-site gate passed 68 of 69 tests. The failure showed that
  CellMeta prefetch occurred before rejecting a non-identical retry. A
  fail-fast completion/live-request preflight was added before prefetch while
  retaining the authoritative TOCTOU checks in acquisition. The exact test,
  delayed-meta race, and duplicate-completion tests passed, independent review
  returned READY again, and the complete data-site rerun passed 69 of 69.
- The pre-Round-1 implementation audit returned **READY** for that earlier
  text. The later full-range review produced the findings and superseding
  evidence recorded below.

## Serial Verification

The heavy server suites were run strictly serially. Each process exited before
the next began and completed within its gate bound:

- `cargo test --lib server::transactions::undo_log::tests -- --test-threads=1`:
  60 passed, 0 failed, 746 filtered, 29.71s.
- `cargo test --lib server::transactions::manager::tests -- --test-threads=1`:
  27 passed, 0 failed, 779 filtered, 210.33s.
- `cargo test --lib server::transactions::data_site::tests -- --test-threads=1`:
  69 passed, 0 failed, 737 filtered, 683.78s.
- `cargo test --lib server::transactions::occ_tests -- --test-threads=1`:
  45 passed, 0 failed, 761 filtered, 549.61s.
- `cargo test --lib ram::recovery::tests -- --test-threads=1`:
  41 passed, 0 failed, 2 ignored, 763 filtered, 3.83s.
- `cargo test --lib server::transactions::tests -- --test-threads=1`:
  5 passed, 0 failed, 801 filtered, 5.04s.
- `cargo test --lib server::tests::durable_ -- --test-threads=1`:
  2 passed, 0 failed, 804 filtered, 2.01s.
- `cargo test --lib --features occ_phase_profile server::transactions::phase_profile::tests -- --test-threads=1`:
  5 passed, 0 failed, 808 filtered.

The exact final text also passed these focused regressions:

- Registry publication/end race: 1 passed.
- Durable and volatile delayed duplicate prepare: 2 passed.
- Failed promotion with delayed duplicate prepare and exact retry: 1 passed.
- Prefetched CellMeta eviction/identity retry: 1 passed.
- Non-identical prepare fail-fast before CellMeta creation: 1 passed.
- Durable participant-cache reopen/strict expiry: 1 passed.
- Volatile exact-outcome retry and targeted cache expiry: 1 passed.

Final static gates:

- `cargo check --lib`: passed with the repository's existing warnings.
- `cargo fmt --check` for all three touched Rust files: passed.
- `git diff --check`: passed.

## Protocol and Scope Audits

- The only owner-clearing production paths remain rollback of ownership
  acquired by the same failed prepare and the validated participant `end`
  barrier. Age and network outcomes never clear ownership.
- Completed-decision expiry uses strict comparison; unresolved durable records
  remain indefinite, and incomplete retirement metadata remains restart-safe
  without extending externally visible completed decisions.
- The point snapshot visibility rule remains strict `<`; the 32-byte cell
  header and revision-chain layout are untouched.
- No transaction path stores raw cell addresses, segment pins, or mutable cell
  references. Installed output continues to use logical IDs and revision
  handles.
- Read-only/stateless fast paths, distributed prepare/commit/end phase
  ordering, exact-output durability, and compensation behavior remain covered
  by the full data-site, manager, OCC, undo-log, recovery, and transaction
  suites.
- Shared B-tree worktree changes are unrelated and are excluded from the Task
  10 commit.

## Files

- `src/server/transactions/data_site.rs`
- `src/server/transactions/manager.rs`
- `src/server/transactions/undo_log.rs`
- `.superpowers/sdd/task-10-report.md`

Recurring service-lookup messages during server shutdown are existing fixture
teardown logs; every counted suite exited successfully.

## Review Fixes Round 1

This section supersedes the base report wherever lifecycle ordering differs.
In particular, coordinator completion is no longer synchronized before live
state removal. The completed-decision mutex is held across identity-checked
live removal and cache publication, and the exact 300,000ms deadline is
allocated at that cleanup boundary. Completion persistence follows. If it
fails, the in-process cache remains visible and the older durable `Decided`
record remains the crash-replay source; replay idempotently reconstructs the
completion.

### Resolution and retirement design

- Participant ownership now uses exact `HashMap` indices and timestamps only
  genuine new acquisitions. An identical prepare retry preserves the original
  age.
- A fixed set of 64 lifecycle shards serializes retirement prepare/finalize,
  expired-proof re-finalization, authoritative prepare completion checks,
  transaction publication, and owner acquisition for the same TID. This
  bounds lock storage independently of historical transaction count.
- Participant expiry is derived only from the participant's local clock when
  finalize is accepted. No coordinator absolute deadline crosses the RPC
  boundary. Durable and volatile evidence remains unexpired until retirement
  finalize, and idempotent retries do not extend an accepted deadline.
- `PrepareDispatchState` and its RAII guard keep a coordinator transaction
  in-flight through distributed prepare and partial-failure abort cleanup.
  Stale cleanup revalidates and skips it; cancellation transfers abort cleanup
  to the background lifecycle and cannot strand an in-flight bit.
- Completion and replay hot paths use targeted `HashMap` lookups. The undo
  logger constructs one canonical per-TID state index at startup and updates
  it after durable append, replay, and compaction. A bounded central channel
  of capacity 256, bounded batches, rotating discovery queues, and an in-flight
  `HashSet` replace per-TID retry tasks and full scans. Whole-map cache and
  weak-lock pruning occurs only in periodic maintenance.
- Compaction records pending covered-file cleanup, attempts every safe
  deletion, and retries cleanup before publishing another snapshot. Startup
  reuses the latest authoritative generation while cleanup is pending, so
  repeated restarts cannot add empty generations. Abandoned `.compacting`
  files are durably removed.
- Restart recovery uses optional coordinator membership lookup. Missing
  membership returns a fail-closed recoverability error, retains undo
  evidence, and never infers Abort.

### Finding-by-finding RED/GREEN evidence

1. **Important 1 — duplicate prepare age.** RED:
   `identical_prepare_retry_preserves_original_owner_age` observed the owner
   timestamp change on each sub-timeout duplicate. GREEN: four identical
   retries leave the original timestamp unchanged; after the original timeout
   a foreign prepare queues resolution of the coordinator's durable Abort and
   then acquires the released owner.
2. **Important 2 — post-cleanup coordinator retention.** RED:
   `coordinator_retention_starts_only_after_live_cleanup_boundary` expired a
   decision whose persistence/removal pause exceeded the old preallocated
   window. GREEN: the result is visible through cleanup plus 299,999ms and is
   `Unknown` at exactly 300,000ms.
   `resolve_never_observes_a_gap_between_live_cleanup_and_completion_publication`
   separately failed with `Unknown` in the live-to-cache window before the
   mutex handoff was made atomic.
   `completion_persistence_failure_after_cleanup_preserves_replay_source`
   proves the old durable decision survives a failed completion write.
3. **Important 3 — participant-local TTL.** RED:
   `participant_finalize_ttl_starts_after_delayed_local_acceptance_under_clock_skew`
   showed coordinator time and send delay changing participant expiry. GREEN:
   a 120,000ms delivery delay and 10,000,000ms clock skew do not alter the
   participant's exact local acceptance interval; evidence is present at
   299,999ms and expired at 300,000ms.
   `participant_completion_retention_uses_the_exact_logical_deadline` also
   covers the strict equality boundary.
4. **Important 4 — serialized participant retirement.** RED:
   `participant_retirement_rmw_is_serialized_and_monotonic_per_tid` let an
   older paused retire overwrite a newer finalized record. GREEN: the older
   operation cannot cross finalize, and durable/in-memory state remains
   finalized with its original local expiry.
   `expired_proof_refinalize_is_atomic_with_same_tid_prepare` closes the
   related re-finalize/prepare resurrection race.
5. **Important 5 — in-flight distributed prepare.** RED: the paused
   partial-success path could be removed as stale before its later participant
   failed, leaving the first owner unresolved.
   `prepare_failure_racing_with_slow_success_settles_before_cleanup` and
   `dropped_prepare_dispatch_guard_hands_off_abort_without_leaking_inflight_state`
   now prove `InProgress` during dispatch, durable explicit Abort afterward,
   complete participant cleanup, and no permanent in-flight state after drop.
6. **Important 6 — volatile lost end response.** RED:
   `volatile_end_response_loss_waits_for_retirement_before_starting_ttl`
   advanced 300,001ms after a lost end response and found the old fixed-TTL
   evidence gone. GREEN: pending volatile evidence has no expiry, the end
   retry and retirement handshake complete, delayed duplicate prepare remains
   rejected, and only participant finalize starts the local 300-second TTL.
7. **Important 7 — bounded O(1) hot paths.** RED: targeted resolution and
   retirement lookups incremented full-log scan counts; retry held the writer
   path; normal cache lookup pruned the full map; retry scheduling was
   unbounded. GREEN:
   `targeted_resolution_and_retirement_reads_never_rescan_log_files`,
   `manager_resolve_unknown_retries_use_only_targeted_canonical_lookups`
   (256 `Unknown` retries),
   `paused_retirement_retry_does_not_hold_canonical_index_against_append`,
   `retirement_discovery_is_bounded_round_robin_and_releases_index_before_append`,
   `replay_lock_lookup_is_targeted_and_dead_entry_pruning_is_maintenance_only`,
   and
   `maintenance_worker_eventually_prunes_idle_expired_cache_entries`
   prove zero request-path scans, bounded/fair discovery, nonblocking append,
   and eventual maintenance pruning.
8. **Important 8 — unlink-failure boundedness.** RED:
   `persistent_unlink_failure_allows_only_one_pending_snapshot_generation`
   grew the log footprint from two files to three across a repeated restart.
   GREEN: persistent failures across repeated compaction/restart attempts keep
   one authoritative pending generation and bounded bytes; state remains
   correct and cleanup converges after fault removal.
9. **Important 9 — missing membership.** RED:
   `missing_recovery_coordinator_membership_is_fail_closed_and_retryable`
   reached the panicking membership conversion during startup. GREEN: startup
   returns the expected recovery error without panic or inferred Abort,
   preserves the exact undo files, and converges after membership restoration.
10. **Minor 1 — transient startup discovery.** RED:
    `retirement_discovery_worker_retries_after_transient_index_error` showed
    the one-shot scan being abandoned. GREEN: periodic bounded discovery
    retries and queues the incomplete TID.
11. **Minor 2 — canonical chronology.** RED:
    `targeted_participant_state_follows_later_undo_and_completion_chronology`
    returned obsolete retirement state. GREEN: later undo/completion records
    supersede older retirement consistently in the targeted index.
12. **Minor 3 — barrier validation.** RED:
    `startup_rejects_invalid_or_out_of_order_snapshot_barriers` accepted a
    sequence-100 snapshot covering sequence 0. GREEN: barriers require
    monotonic chronology and exact `covered_through_seq + 1 == snapshot_seq`;
    malformed and out-of-order input fails closed.
13. **Minor 4 — staging cleanup.** RED:
    `startup_durably_removes_abandoned_compacting_snapshots` accumulated
    ignored staging files. GREEN: startup removes every abandoned
    `.compacting` file and synchronizes the directory before proceeding.

The clean focused aggregate covering all 13 findings and the audit-discovered
publication/re-finalization/restart races passed 26 of 26 tests. One
deterministic dispatch test originally used three Tokio workers; the new
50ms maintenance worker legitimately occupied the third blocked worker, so the
fixture was corrected to four workers and the exact test then passed in
11.23s.

### Round 1 serial verification

Heavy suites ran one process at a time with no overlap:

- `cargo test --lib server::transactions::undo_log -- --test-threads=1`:
  65 passed, 0 failed, 762 filtered, 29.85s. The first broad run exposed two
  stale test assumptions after canonical startup indexing: 63 passed and 2
  failed. The tests were corrected to assert startup-time validation and
  indexed recovery after directory removal; both exact tests and the full
  rerun passed.
- `cargo test --lib server::transactions::manager -- --test-threads=1`:
  36 passed, 0 failed, 791 filtered, 310.84s.
- `cargo test --lib server::transactions::data_site -- --test-threads=1`:
  74 passed, 0 failed, 753 filtered, 740.50s.
- `cargo test --lib server::transactions::occ_tests -- --test-threads=1`:
  46 passed, 0 failed, 781 filtered, 555.98s.
- `cargo test --lib ram::recovery -- --test-threads=1`:
  41 passed, 0 failed, 2 ignored, 784 filtered, 4.49s.
- `cargo test --lib server::transactions::tests -- --test-threads=1`:
  5 passed, 0 failed, 822 filtered, 5.07s.
- `cargo test --lib server::tests::durable_ -- --test-threads=1`:
  2 passed, 0 failed, 825 filtered, 2.01s.
- `cargo test --lib --features occ_phase_profile server::transactions::phase_profile::tests -- --test-threads=1`:
  5 passed, 0 failed, 829 filtered, 0.00s after a 22.89s feature build.
- `cargo test --lib ram::cell::tests::revision_header_is_exactly_32_bytes -- --test-threads=1`:
  1 passed, 0 failed, 826 filtered, 0.00s.
- `cargo check --lib`: passed in 4.44s with existing repository warnings.
- Scoped `rustfmt --check` over the five touched Rust files and
  `git diff --check`: passed.

The first fresh rustfmt check found one line-wrap-only difference in the
updated undo-log recovery test; scoped rustfmt normalized it and both final
static checks passed.

### Audit and deferrals

The independent integrated implementation audit found no remaining Critical
or Important implementation defect. It initially identified evidence-only
gaps for combined delay/skew and manager-level zero-scan/maintenance behavior;
the focused tests listed under Important 3 and Important 7 close those gaps.
This is not a claim of formal approval: the required full-range rereview
follows this commit.

Index/range isolation remains outside the point-cell MVCC contract, and the
unrelated B-tree/range-service worktree edits remain untouched and excluded.
The strict point visibility rule, 32-byte cell header, revision-chain layout,
and logical-ID-only transaction state are unchanged.
