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
- A coordinator writes and synchronizes its self-contained completion record
  before removing live transaction state. Active state is consulted first,
  then the completed cache, then durable status.
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
- Final independent review verdict: **READY**, with no Critical or Important
  findings.

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
