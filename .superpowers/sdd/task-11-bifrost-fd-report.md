# Task 11 Bifrost RPC Shortcut / FD Lifecycle Implementer Report

Date: 2026-07-27

## Result

Implementation is complete and committed in the isolated Bifrost worktree.

- Worktree:
  `/home/shisoft/Dropbox/Code/OSS Projects/Nebuchadnezzar/.worktrees/bifrost`
- Branch: `feature/checked-hlc`
- Base: `61239a7` (`fix(proc-macro): use syn 2 full AST`)
- Commit: `5ee96ea` (`fix(rpc): release shortcut registrations on shutdown`)
- Scope: 6 files, 745 insertions, 168 deletions
- Worktree after commit: clean
- Push status: not pushed
- Correctness/debugging location: local machine only
- Remote benchmark host: not used; this task did not require benchmarking

The leak was a real ownership cycle, not merely delayed cleanup:

1. the global TCP shortcut table strongly owned the request callback;
2. the callback strongly owned the RPC server;
3. the RPC server strongly owned its services;
4. generated service shortcut tables also strongly owned services;
5. a `RaftService` owned a dedicated 12-worker Tokio runtime, whose reactor
   descriptors remained open while the leaked service remained reachable.

Explicit shutdown previously broadcast a signal and slept briefly. It neither
awaited the listener nor removed these global ownership roots.

## Requirements mapping

### Identity-safe TCP shortcut lifecycle

Implemented in `src/tcp/shortcut.rs` and `src/tcp/server.rs`.

- Each registration receives an identity token backed by a distinct
  `Arc<()>`.
- `register_server` returns an RAII `ShortcutRegistration`.
- The TCP server owns that registration guard.
- Dropping or explicitly taking the guard removes the callback only when the
  table still contains the same identity.
- A stale server therefore cannot remove a same-address replacement.
- Callback lookup clones the callback while holding the short synchronous read
  lock and releases the lock before awaiting user code.
- A failed socket bind cannot replace the live shortcut callback because bind
  now precedes shortcut publication.

An Arc identity token was selected instead of an incrementing integer so a
stale live owner cannot collide with a wrapped generation.

### Generated service shortcut lifecycle

Implemented in `src/rpc/proto.rs` and `src/rpc/mod.rs`.

- Generated `RPC_SVRS` entries now contain
  `(ShortcutToken, Weak<dyn Service>)`, not a strong service `Arc`.
- Registration returns the token to the RPC server.
- Unregistration compare-removes only the exact registration identity.
- `get_local` upgrades the weak reference and opportunistically removes a
  dead entry, again only if its identity still matches.
- The RPC server owns the token for every registered service and unregisters
  it during service removal or server shutdown.
- Shutdown clears the server's strong service map after shortcut
  unregistration, breaking the server/service ownership cycle.

### Awaitable RPC/TCP shutdown

Implemented in `src/tcp/server.rs` and `src/rpc/mod.rs`.

The explicit RPC shutdown order is:

1. mark the server as shutting down so new service registrations are rejected;
2. take the owned TCP server;
3. identity-unregister its TCP shortcut and signal shutdown;
4. take and await the listener task without holding a synchronous mutex;
5. let the TCP listener drain tracked connection-handler tasks;
6. evict only a client-pool entry belonging to the stopped TCP generation;
7. identity-unregister generated service shortcuts;
8. clear the server-owned services.

The TCP server also has an atomic shutdown-requested state. This prevents a
shutdown that happens before the listener begins polling from being lost.
`Drop` provides a synchronous best-effort fallback, while explicit async
shutdown provides the full await-and-drain guarantee.

### Preserve established live-listener semantics

The request callback intentionally retains a strong `Arc<rpc::Server>`.
Existing behavior permits `listen_and_resume` to keep a live server operating
after the caller drops its local `Arc`. Replacing this with a weak callback
caused real distributed test failures. The strong root is now safe because
explicit shutdown identity-unregisters the callback, awaits the listener, and
then releases services.

### Generation-safe client-pool cleanup

Implemented in `src/tcp/client.rs` and `src/rpc/mod.rs`.

- A shortcut client records the exact TCP registration identity observed when
  it is constructed.
- Server shutdown evicts the cached client only if that client belongs to the
  stopped registration.
- Eviction uses Lightning's per-key map lock, re-reads the current value under
  that lock, and removes through the guard.
- This closes both stale-server/replacement races and the read/remove TOCTOU
  race.
- Shortcut clients remain socketless; no distributed RPC step was removed.

### Release `RaftService` runtime resources

Implemented in `src/raft/mod.rs`.

- `RaftService` exposes a cloneable `tokio::runtime::Handle` for task spawning.
- The actual `Runtime` is privately owned in
  `StdMutex<Option<Runtime>>`.
- Graceful shutdown first stops managed Raft tasks, then takes and destroys the
  runtime outside an outer Tokio worker through `spawn_blocking`.
- If shutdown is invoked from the owned runtime itself, it uses
  `shutdown_background` to avoid self-join deadlock.
- `Drop` also uses `shutdown_background` as a safe async-context fallback.

This eliminates the persistent epoll/eventfd descriptors previously retained
by each leaked Raft runtime.

### Locking and performance constraints

- No synchronous mutex or read/write lock is held across an `.await`.
- Shortcut table operations use short `parking_lot::RwLock` critical sections.
- Callback dispatch clones under the read lock and invokes outside it.
- Service lifecycle serialization is synchronous and contains no await.
- Client-pool eviction locks one key only during identity recheck/removal.
- The live shortcut request path retains the same direct callback behavior;
  generation work occurs at construction, registration, and shutdown.
- No distributed transaction or RPC protocol step was removed.

## Exact RED to GREEN evidence

### 1. Explicit shutdown releases roots and shortcuts

Test:
`rpc::test::simple_service::shutdown_releases_server_service_and_shortcuts`

- RED: after shutdown, the TCP shortcut remained registered; 1.10s.
- GREEN: TCP shortcut absent, generated service shortcut absent, matching
  client-pool entry evicted, and both weak service/server references dead after
  external owners are dropped; 1.01s.

### 2. Stale TCP registration cannot remove a replacement

Test:
`tcp::shortcut::tests::stale_registration_drop_does_not_remove_replacement`

- RED: registration returned `()`, so lifecycle ownership and identity-safe
  removal could not be expressed; the final callback remained.
- GREEN: dropping the stale guard preserves the replacement, and dropping the
  replacement removes it; 0.00s.

### 3. Stale generated-service registration cannot remove a replacement

Test:
`rpc::test::simple_service::stale_server_shutdown_preserves_replacement_service_shortcut`

- GREEN: an old server shutdown preserves the newer service registration;
  0.00s in the focused generation test.

### 4. Shutdown waits for the listener and permits immediate same-port rebind

Test:
`rpc::test::simple_service::shutdown_awaits_listener_and_allows_immediate_same_port_rebind`

- GREEN: immediate rebind succeeds, and a later stale shutdown preserves the
  replacement TCP shortcut, generated service, and cached client; 2.01s.

### 5. Repeated Raft lifecycle keeps descriptors bounded

Test:
`rpc::test::simple_service::repeated_raft_service_lifecycle_keeps_fds_bounded`

- GREEN in an isolated child process across 6 create/register/shutdown/drop
  cycles.
- Linux `/proc/self/fd` counts: baseline 10, peak 14, final 10.
- The `Weak<RaftService>` was dead after every lifecycle.

### 6. Shutdown-before-listen is not lost

Test:
`tcp::server::tests::shutdown_requested_before_listen_is_not_lost`

- RED: timed out after 250ms.
- GREEN: listener exits, shortcut is absent, and the port is bindable; 0.00s.

### 7. Runtime destruction is async-context safe

Tests:

- `shutdown_allows_raft_service_drop_inside_async_context`
- `raft_runtime_drop_fallback_is_async_context_safe`
- `raft_shutdown_called_from_owned_runtime_does_not_deadlock`

Evidence:

- RED: dropping the owned Tokio runtime in async context panicked with
  `Cannot drop a runtime in a context where blocking is not allowed`; 0.00s.
- GREEN: ordinary graceful shutdown/drop succeeds; 0.00s.
- GREEN: Drop fallback succeeds in async context; 0.00s.
- GREEN: shutdown invoked from the owned runtime completes within the
  two-second timeout; 0.00s.

### 8. Stale server shutdown preserves replacement pool generation

Covered by the same-port/replacement lifecycle test.

- RED: shutdown of the old server removed the replacement cached client;
  `Arc::ptr_eq` was false; 2.00s.
- GREEN: generation-aware eviction preserves it; 2.01s.

### 9. Client-pool eviction closes read/remove TOCTOU

Test:
`rpc::test::simple_service::pool_eviction_rechecks_identity_before_removing`

The deterministic test-only hook replaces the cached matching generation
between the optimistic read and removal.

- RED: cache was unexpectedly `None` at `src/rpc/mod.rs:645`; 2.01s.
- GREEN: the per-key lock/recheck/remove preserves the intervening generation;
  2.01s.

### 10. Strong live-listener ownership regression and correction

An intermediate weak request callback broke the established
`listen_and_resume` behavior.

- First full run at that intermediate state:
  165 passed, 4 failed, 281.31s.
- Focused membership primary reproduction:
  1/1 passed, 23.02s.
- Focused callback dummy reproduction:
  1/1 passed, 5.01s.
- Focused callback type-2 reproduction:
  1/1 passed, 3.01s.
- Exact `multi_server_command` reproduction:
  RED 0/1, `CannotConstructClient`, 37.02s.
- After restoring intentional strong listener ownership and relying on
  explicit shutdown to break the root:
  GREEN 1/1, 18.03s.

## Focused final verification

All commands were run locally in the Bifrost worktree.

### RPC suite

Command:

```text
cargo test rpc:: -- --test-threads=1
```

Result: 14/14 passed in 16.05s.

### TCP suite

Command:

```text
cargo test tcp:: -- --test-threads=1
```

Result: 9/9 passed in 1.41s.

### Distributed multi-server regression

The exact `multi_server_command` test passed 1/1 in 18.03s.

### Isolated lifecycle descriptor regression

The isolated child passed with baseline 10, peak 14, and final 10 open file
descriptors.

## Final full verification

This was rerun fresh after the last production synchronization change:

```text
RUSTFLAGS='-Awarnings' cargo test --all-targets -- --test-threads=1
```

Exit status: 0.

- Library tests: 170 passed, 0 failed; 272.36s.
- Graceful-shutdown integration tests:
  6 passed, 0 failed, 1 pre-existing ignored; 18.74s.
- Single-node recovery integration tests: 2 passed, 0 failed; 41.07s.
- Example targets: 0 tests.
- Aggregate: 178 passed, 0 failed, 1 ignored.

The ignored `test_raft_service_shutdown_stops_tasks` was also invoked
explicitly and exited successfully, but followed its existing
`RaftService::start == false` skip path. The new focused runtime/lifecycle
regressions exercise the behavior fixed here directly.

An earlier complete full run, before the final client-pool TOCTOU hardening,
also passed its then-current set: library 169/169, graceful shutdown 6 passed
and 1 ignored, recovery 2/2, aggregate 177 passed and 1 ignored.

## Final static verification

### All-target compilation

```text
RUSTFLAGS='-Awarnings' cargo check --all-targets
```

Passed. The initial final run completed in 1.78s; the final cached recheck
before commit completed in 0.02s.

### Scoped formatting

```text
rustfmt --edition 2021 --check \
  src/raft/mod.rs \
  src/rpc/mod.rs \
  src/rpc/proto.rs \
  src/tcp/client.rs \
  src/tcp/server.rs \
  src/tcp/shortcut.rs
```

Passed.

Global `cargo fmt --all` is blocked by pre-existing trailing whitespace in
untouched `src/membership/server.rs`; no unrelated formatting was included.

### Diff hygiene

```text
git diff --check
git diff --cached --check
```

Both passed.

## Public API changes

### `RPCService`

`RPCService::register_shortcut_service` now returns a `ShortcutToken`, and the
trait includes `unregister_shortcut_service`.

- Macro-generated implementations rebuild automatically.
- Manually implemented `RPCService` implementations must adopt the two method
  signatures.
- `ShortcutToken` is public but documentation-hidden because it supports
  generated lifecycle plumbing rather than the normal RPC API.

### `RaftService::rt`

The public `RaftService::rt` field changed from
`tokio::runtime::Runtime` to `tokio::runtime::Handle`.

- All repository uses call `.spawn` and compile unchanged.
- Downstream code using methods available only on `Runtime` must adjust.
- Runtime ownership is deliberately private so it can be taken and shut down
  exactly once.

## Committed scope

Only these files are in commit `5ee96ea`:

1. `src/raft/mod.rs`
2. `src/rpc/mod.rs`
3. `src/rpc/proto.rs`
4. `src/tcp/client.rs`
5. `src/tcp/server.rs`
6. `src/tcp/shortcut.rs`

No Nebuchadnezzar MVCC files, unrelated Bifrost changes, benchmark artifacts,
or generated output were staged.

## Self-review concerns and residual boundaries

1. **Strong live callback ownership is intentional.**
   A listening server remains alive even if the caller drops its local
   `Arc`, preserving established behavior. Consequently, callers that want to
   release a live listener deterministically must invoke explicit shutdown.
   The old leak was fixed at that explicit lifecycle boundary.

2. **`Drop` cannot provide the full async guarantee.**
   It unregisters/signals synchronously and releases owned services, but it
   cannot await listener or connection-handler completion. The full guarantee
   belongs to `Server::shutdown().await`.

3. **Graceful handler draining can wait for user dispatch.**
   Accepted connections observe shutdown between messages. A callback already
   executing is allowed to finish, and explicit shutdown waits for tracked
   handlers. A permanently stuck service callback can therefore delay
   shutdown; aborting in-flight application work was not part of the approved
   semantics.

4. **Raft graceful runtime destruction assumes a Tokio execution context.**
   The public shutdown path already uses Tokio synchronization and task
   machinery. The final runtime drop uses `tokio::task::spawn_blocking` when
   called outside the owned runtime, and `shutdown_background` when called
   from the owned runtime. Polling the entire shutdown future from a
   non-Tokio executor was not part of the requested or verified contract.

5. **The runtime field type is intentionally breaking.**
   This prevents external ownership of the runtime from defeating exact
   lifecycle cleanup, but downstream Runtime-specific field users need a
   migration to `Handle`.

6. **The ignored legacy shutdown test remains structurally skipped.**
   It was not converted as part of this scope. Direct focused regressions now
   cover async-context destruction, owned-runtime shutdown, weak-reference
   release, and bounded descriptors.

7. **No benchmark was run.**
   The live shortcut request path remains direct and lock hold time was
   shortened around callback invocation. This task was correctness and
   resource-lifecycle work; the remote host is reserved for benchmarks and
   was intentionally not used.

No unresolved correctness blocker was found in the final self-review.

## Review Round 1 Fixes

Date: 2026-07-27

Independent review of `5ee96ea` found one Critical and three Important
lifecycle gaps. All four were handled in a separate strict RED-to-GREEN
round. No commit was amended or reset.

### TCP callback destruction outside global locks

Root cause:

- `BTreeMap::insert` dropped the replaced callback while
  `TCP_CALLBACKS.write()` was still held.
- A replaced callback can own the final strong RPC server reference.
- `Server::drop` then drops its stale `ShortcutRegistration`, which re-enters
  the same non-reentrant write lock.
- Identity-safe compare/remove was correct, but destruction timing still
  deadlocked.

Fix:

- Both replaced and explicitly removed `CallbackRegistration` values are
  moved out of the table while locked and dropped only after releasing the
  `parking_lot::RwLock`.

Deterministic test:
`standalone_replacement_drops_old_callback_outside_shortcut_lock`.

- The reproduction runs in an isolated child because the expected failure
  permanently blocks the child thread in the non-reentrant lock.
- RED: child exceeded the six-second deadline; parent failed with
  `standalone replacement deadlocked`; exit 101, 0/1, 6.01s.
- GREEN: replacement gets a distinct registration, preserves it, and releases
  the old last-external-`Arc` server; child and parent pass 1/1 in 2.01s.

### Listener publication serialized with shutdown

Root cause:

- RPC listener ownership and the spawned join handle were published through
  separate mutexes.
- Shutdown could take the TCP owner after publication but before join-handle
  publication, signal it, observe no handle, and return before the listener
  had even started.
- Direct `Server::listen` never published any completion object.
- Already-stopped servers could start again; STANDALONE could publish a
  shortcut after shutdown and leave it resident.

Fix:

- A single mutex-protected `ListenerLifecycle` now contains closing state and
  the one active TCP owner.
- Active listeners have allocation identity and a Tokio watch completion
  value.
- Direct and resumed listen paths publish the complete active state before
  running or spawning TCP work.
- Shutdown marks closing under the same lifecycle mutex, snapshots and
  signals that exact TCP owner, waits for its completion value without holding
  the mutex, and identity-retires the active state.
- Both listen entry points reject an already-closing server.
- TCP listen checks pre-requested shutdown before binding or registration.
- A completed STANDALONE listener deliberately leaves its registration owner
  in the active RPC state until explicit shutdown/replacement; the request
  callback remains the live server owner, preserving established semantics.
- The resumed task uses a weak server reference for completion bookkeeping,
  so it does not become an additional permanent ownership root.

Deterministic tests use a barrier immediately after full active-listener
publication and before TCP work:

- `shutdown_waits_for_concurrent_direct_listener_publication`
- `shutdown_waits_for_concurrent_resumed_listener_publication`

RED:

- Both failed with `shutdown returned before ... listener completed`;
  0/2, 1.01s, exit 101.

GREEN:

- Both pass; shutdown remains pending until the barrier releases and the
  listener reports completion; 2/2, 1.21s.

Already-stopped STANDALONE tests:

- `already_shutdown_server_rejects_direct_standalone_listen`
- `already_shutdown_server_rejects_resumed_standalone_listen`

RED:

- Direct listen returned success rather than rejection.
- Resumed listen left the STANDALONE shortcut registered.
- 0/2, 1.00s, exit 101.

GREEN:

- Both reject/return without publishing a shortcut; 2/2, 0.00s.

The last-external-`Arc` replacement regression was rerun after the ownership
correction and remained GREEN, child and parent 1/1 in 2.01s.

### Atomic pool construction, insertion, and conditional eviction

Root cause:

- Client connection happened while the cache key was absent, then insertion
  occurred later with no shared serialization against eviction or competing
  insertion.
- Shutdown could observe an absent cache and return; the old construction
  could then overwrite a replacement.
- Tokenless socket clients were treated as owned by every local registration
  through `unwrap_or(true)`, allowing stale local shutdown to remove a socket
  replacement.

Fix:

- Each key can have one `ClientConstruction` reservation recording the exact
  observed optional TCP registration identity.
- Reservation creation, final identity validation plus insertion,
  cancellation, and conditional ready-entry eviction serialize through one
  short construction-state mutex.
- Connection remains outside every synchronous lock.
- Concurrent getters subscribe to a durable Tokio watch completion value and
  cannot miss completion/cancellation.
- Eviction cancels only an in-flight construction whose expected local token
  matches the stopped registration.
- A late owner must still own its reservation, its constructed client must
  match the expected optional token, and the registration must still be
  current before insertion.
- A tokenless client never matches an old local registration and is never
  removed by that registration's eviction.
- The previous Lightning per-entry ready-map identity recheck remains in use
  inside the construction serialization boundary.

Deterministic late-insert/competing-replacement test:
`late_old_pool_insertion_does_not_overwrite_competing_replacement`.

- The old construction is held immediately before insertion.
- Old shutdown sees no ready entry.
- A replacement constructs and inserts.
- The old construction is then released.
- RED: old construction overwrote replacement; 0/1, 2.01s, exit 101.
- GREEN: cancellation/identity revalidation returns the replacement and
  preserves it; 1/1, 2.01s.

Real tokenless replacement test:
`old_local_eviction_preserves_tokenless_socket_replacement`.

- A real local RPC generation is stopped.
- A raw TCP listener is rebound to the same address.
- `RPCClient::new_async` constructs a real socket-backed, tokenless client.
- Delayed eviction for the old local token is invoked.
- RED: tokenless replacement was evicted; 0/1, 1.00s, exit 101.
- GREEN: tokenless replacement remains cached; 1/1, 1.00s.

Existing pool regressions after the fix:

- identity recheck/read-remove TOCTOU: 1/1, 2.01s;
- same-port replacement generation: 1/1, 2.01s.

### Deterministic external and eventual self-runtime teardown

The new isolated external contract test is
`external_raft_shutdown_releases_driver_fds_before_returning`.

RED:

- baseline 10, peak 14, immediately after external `shutdown().await` 14;
- the child failed because driver descriptors remained open while the service
  `Arc` was still alive; child and parent exit 101.

Root cause:

- The private runtime owner had been synchronously destroyed, but the public
  `tokio::runtime::Handle` field retained the runtime's reactor/driver
  allocation.

Fix:

- `RaftService::rt` is now a `RaftRuntimeHandle` wrapper that preserves the
  repository's `.spawn(...)` API.
- Its underlying Tokio handle is takeable exactly once.
- External shutdown closes the service handle before dropping/joining the
  runtime on `spawn_blocking`.
- Drop also closes the handle before background runtime teardown.
- The self-runtime branch takes ownership out of the service, closes the
  service handle, and uses `shutdown_background`; it never attempts to join
  the runtime from one of its own tasks.

GREEN external evidence:

- baseline 10, peak 14, immediately after shutdown 10;
- the service `Arc` deliberately remains alive during the assertion;
- child and parent pass 1/1, 0.00s.

The isolated self-runtime test is
`owned_runtime_shutdown_eventually_releases_owner_and_driver_fds`.

- The calling task runs on the owned runtime.
- The test first requires that task to exit within two seconds.
- Only afterward does it poll for weak-service death and bounded FDs.
- Before the handle correction, this already confirmed the architectural
  exception: eventual baseline 10, peak 14, final 11.
- After the handle correction: baseline 10, peak 14, final 10; child and
  parent pass 1/1, 0.00s.
- The pre-existing owned-runtime no-deadlock test remains 1/1 GREEN.

This preserves the normative distinction:

- external graceful shutdown deterministically releases driver resources
  before returning;
- self-runtime shutdown transfers teardown ownership and returns without
  deadlock, with cleanup becoming observable after the calling task exits.

## Review Round 1 Focused Verification

All commands ran locally in the Bifrost worktree.

```text
RUSTFLAGS='-Awarnings' cargo test rpc:: -- --test-threads=1
```

- 23 passed, 0 failed; 22.29s.

```text
RUSTFLAGS='-Awarnings' cargo test tcp:: -- --test-threads=1
```

- 9 passed, 0 failed; 1.41s.

```text
RUSTFLAGS='-Awarnings' cargo test \
  raft::test::state_machine::multi_server_command \
  -- --exact --nocapture --test-threads=1
```

- 1 passed, 0 failed; 18.03s.

The isolated repeated six-cycle Raft lifecycle remains GREEN:
baseline 10, peak 14, final 10.

## Review Round 1 Final Full Verification

Fresh after the last production change:

```text
RUSTFLAGS='-Awarnings' cargo test --all-targets -- --test-threads=1
```

Exit status: 0.

- Library: 179 passed, 0 failed, 0 ignored; 281.59s.
- Graceful-shutdown integration:
  6 passed, 0 failed, 1 pre-existing ignored; 18.76s.
- Single-node recovery: 2 passed, 0 failed; 41.11s.
- Example target: 0 tests.
- Aggregate: 187 passed, 0 failed, 1 ignored.

Final static commands:

```text
RUSTFLAGS='-Awarnings' cargo check --all-targets
rustfmt --edition 2021 --check \
  src/raft/mod.rs \
  src/rpc/mod.rs \
  src/tcp/server.rs \
  src/tcp/shortcut.rs
git diff --check
```

- All-target check passed in 0.74s on the final formatted tree.
- Scoped rustfmt check passed.
- Diff check passed.

## Review Round 1 Scope and API Update

The fix round changes only:

1. `src/raft/mod.rs`
2. `src/rpc/mod.rs`
3. `src/tcp/server.rs`
4. `src/tcp/shortcut.rs`

No Nebuchadnezzar files, generated files, dependencies, or unrelated Bifrost
files changed.

The earlier report's statement that `RaftService::rt` is a raw
`tokio::runtime::Handle` is superseded. It is now a public
`RaftRuntimeHandle` with the repository-used `.spawn(...)` operation.
Downstream users of other raw Tokio `Handle` methods must migrate; backward
compatibility was explicitly not required.

## Review Round 1 Self-Review

1. The listener lifecycle mutex is never held across `.await`.
2. Callback values removed or replaced in `TCP_CALLBACKS` are always dropped
   after releasing the global table lock.
3. Client connection occurs outside the construction mutex. Only reservation,
   final validation/insertion, cancellation, and eviction use the short
   critical section.
4. Pool waiters use a watch value rather than edge-only notification, avoiding
   a completion lost-wakeup window.
5. Fast ready-client reads can race with shutdown and return an existing
   client to an already-started caller; shutdown still atomically removes the
   cached generation. The task did not require revoking client `Arc`s already
   handed to callers.
6. A STANDALONE TCP `listen` continues to return after registration. The RPC
   listener state deliberately retains that TCP owner until explicit
   shutdown/replacement so the live callback semantics are unchanged.
7. Explicit shutdown waits for listener/handler completion. The synchronous
   Drop fallback still cannot await; this remains an inherent Rust Drop
   boundary rather than a weakened explicit-shutdown guarantee.
8. External Raft shutdown is deterministic while the service remains alive.
   Self-runtime shutdown cannot join itself and is therefore deliberately
   eventual after the caller task exits.
9. `RaftRuntimeHandle::spawn` panics after shutdown instead of returning a
   silently cancelled task. Spawning work after service shutdown was never a
   supported lifecycle operation.
10. No unresolved Critical or Important correctness issue was found in the
    final implementation review.

## Review Round 2

Review input:
`.superpowers/sdd/task-11-bifrost-review-2.md`, base `61239a7`, reviewed head
`2cb59a0`.

The round resolved all four Important findings and the exact-FD Minor without
changing dependencies or touching Nebuchadnezzar.

### Cancellation-safe client construction

Root cause:

- construction ownership was represented only by a map entry;
- ordinary success/error paths removed the entry, but dropping or aborting the
  owner future bypassed cleanup;
- the retained watch sender could leave later getters waiting on a reservation
  that no future still owned.

Fix:

- `ClientConstructionOwner` is an armed RAII guard over the exact
  pool/key/construction identity;
- cancellation identity-removes only its own reservation;
- completion uses `watch::Sender::send_replace(true)`, so the terminal value is
  durable even if no receiver is subscribed at the instant of cancellation;
- ordinary failure explicitly cancels the guard and successful finalization
  disarms it only after the reservation has been resolved.

Deterministic regression:
`aborted_pool_owner_releases_reservation_and_wakes_waiter`.

- RED: 0/1, exit 101, 1.00s; the aborted owner stranded the construction
  reservation.
- GREEN: 1/1, exit 0, 0.00s.

### Serialized STANDALONE retirement

Root cause:

- listener completion consulted a separate shutdown atomic and could remove
  STANDALONE `active` state before explicit shutdown acquired the lifecycle
  mutex;
- shutdown could then snapshot no listener, return, and leave the finishing
  `ListenerRun` holding its shortcut registration.

Fix:

- successful STANDALONE completion always retains the active lifecycle tuple;
- explicit shutdown is the serialized owner that snapshots, stops, waits for,
  and retires that tuple.

Deterministic regression:
`standalone_shutdown_owns_retirement_before_returning`.

- RED: 0/1, exit 101, 0.00s; shutdown returned while the finishing run retained
  the registration.
- GREEN: 1/1, exit 0, 0.00s.

### Atomic TCP closing and registration publication

Root cause:

- TCP listen checked the shutdown atomic before binding, then published its
  registration later;
- shutdown could snapshot no registration between those operations;
- the listener could then publish a transient generation, let a client cache
  it, and retire the shortcut without giving RPC shutdown a token to evict.

Fix:

- one `RegistrationLifecycle` mutex now owns both `closing` and the optional
  registration;
- registration publication and the closing transition serialize under that
  mutex;
- publication cannot occur after closing wins;
- shortcut registration is synchronous because it contains no await, allowing
  publication and state installation to form one critical section;
- replaced registrations are still dropped after releasing lifecycle/global
  callback locks.

Deterministic regression:
`shutdown_evicts_generation_published_after_tcp_precheck`.

- RED: 0/1, exit 101, 0.00s; a transient stopped generation remained cached.
- GREEN: 1/1, exit 0, 0.00s.

### Shared cancellation-safe Raft runtime teardown

Root causes:

- the first shutdown caller took `rt_owner`; concurrent callers then observed
  `None` and returned while runtime destruction was still in progress;
- cancelling the initiating external future could abandon the only await of
  teardown;
- a self-runtime caller could not synchronously join its own runtime;
- `RaftRuntimeHandle::spawn` cloned a raw Tokio handle and released the wrapper
  mutex before spawning, so close could win while an untracked clone escaped.

Fix:

- `RuntimeLifecycle` owns the runtime and one immutable shared
  `RuntimeTeardown`;
- the first caller closes the public service handle and transfers runtime
  ownership to a named OS teardown thread synchronously, before any await or
  cancellation point;
- all external callers subscribe to the same durable watch completion and
  return only after runtime destruction;
- a caller executing on the owned runtime initiates teardown but does not wait
  on itself; teardown completes after its calling task exits;
- `RuntimeTeardownJob::Drop` uses `shutdown_background` and publishes completion
  if thread creation fails or the job unwinds before ordinary destruction;
- completion uses `send_replace(true)`, remaining visible to later subscribers;
- `RaftRuntimeHandle::spawn` holds the wrapper handle mutex through the
  synchronous Tokio spawn call, so no raw handle clone escapes close.

Deterministic regressions:

- `runtime_spawn_does_not_escape_after_close_wins`
  - RED: 0/1, exit 101, 0.00s; spawn used a raw handle after close won.
  - GREEN: 1/1, exit 0, 0.00s.
- `cancelled_external_teardown_is_awaited_by_next_external_shutdown`
  - RED: isolated child and parent failed, 0/1, exit 101, 0.01s; the second
    external shutdown returned before prior teardown completed.
  - GREEN: isolated child and parent passed, 1/1, exit 0, 0.11s.
- `external_shutdown_waits_for_self_runtime_teardown`
  - RED: isolated child and parent failed, 0/1, exit 101, 0.02s; external
    shutdown returned before the self-runtime task exited and teardown
    completed.
  - GREEN: isolated child and parent passed, 1/1, exit 0, 0.11s.

The pre-existing owned-runtime no-deadlock regression remains GREEN, 1/1.

### Exact FD restoration

All isolated lifecycle checks now require equality with the starting FD count,
not `baseline + 2`.

Final observed evidence:

- repeated six-cycle lifecycle: baseline 10, peak 14, final 10;
- external graceful shutdown while the service `Arc` remains alive:
  baseline 10, peak 14, after-return 10;
- self-runtime shutdown after caller exit: baseline 10, peak 14, final 10;
- concurrent external/external and self/external tests also restored the exact
  baseline before the waiting external caller returned.

## Review Round 2 Focused Verification

All verification ran locally in the isolated Bifrost worktree.

```text
RUSTFLAGS='-Awarnings' cargo test rpc:: -- --test-threads=1
```

- 28 passed, 0 failed; 22.51s.

```text
RUSTFLAGS='-Awarnings' cargo test tcp:: -- --test-threads=1
```

- 9 passed, 0 failed; 1.41s.

```text
RUSTFLAGS='-Awarnings' cargo test \
  raft::test::state_machine::multi_server_command \
  -- --exact --nocapture --test-threads=1
```

- 1 passed, 0 failed; 18.03s.

## Review Round 2 Final Full Verification

Fresh after the last production change:

```text
RUSTFLAGS='-Awarnings' cargo test --all-targets -- --test-threads=1
```

Exit status: 0.

- Library: 185 passed, 0 failed, 0 ignored; 293.82s.
- Graceful-shutdown integration:
  6 passed, 0 failed, 1 pre-existing ignored; 18.75s.
- Single-node recovery: 2 passed, 0 failed; 41.07s.
- Example target: 0 tests.
- Aggregate: 193 passed, 0 failed, 1 ignored.

Final static commands:

```text
RUSTFLAGS='-Awarnings' cargo check --all-targets
rustfmt --edition 2021 --check \
  src/raft/mod.rs \
  src/rpc/mod.rs \
  src/tcp/server.rs \
  src/tcp/shortcut.rs
git diff --check
```

- All-target check passed in 0.53s on the final formatted tree.
- Scoped rustfmt check passed.
- Diff check passed.

## Review Round 2 Scope and Self-Review

The Bifrost commit changes only:

1. `src/raft/mod.rs`
2. `src/rpc/mod.rs`
3. `src/tcp/server.rs`
4. `src/tcp/shortcut.rs`

Final invariants checked:

1. No synchronous lifecycle mutex is held across an await.
2. Client connection remains outside the construction mutex.
3. RAII cancellation removes only the exact reservation it owns, and its
   durable watch value cannot lose completion when no receiver exists.
4. Explicit shutdown exclusively retires retained STANDALONE state.
5. TCP closing and publication form one critical section; no stopped
   post-closing generation can be published or cached.
6. Replaced registrations/callbacks are dropped outside shared callback and
   lifecycle locks.
7. Runtime ownership transfers before the initiating future can be cancelled.
8. Every external shutdown caller waits for the same exact teardown
   completion; self-runtime shutdown never waits on itself.
9. Runtime completion is published after ordinary destruction, with a
   cancellation/thread-failure fallback.
10. Runtime spawn and close serialize on the same handle mutex.
11. Exact isolated FD baselines are restored.
12. No unresolved Critical or Important correctness issue was found.

## Subscription Callback Lifetime Follow-Up

Base: `e47928e`.

The accepted lifecycle teardown exposed a separate process-global ownership
defect in `raft::client::CALLBACK`.

### Root cause

`CALLBACK` stored `Option<Arc<SubscriptionService>>`. RPC shutdown correctly
removed the callback service from the retiring server's service map, but the
global strong `Arc` kept the service, old server address, session, and
subscription map alive indefinitely. `prepare_subscription` checked only
`Option::is_none`, so a later server skipped registration. `can_callback` and
`get_callback` likewise treated mere slot presence as a live callback.

### Deterministic lifecycle regression

`prepare_subscription_rebinds_after_prior_server_shutdown`:

1. prepares a callback on server A;
2. explicitly shuts down and drops A without retaining a callback `Arc`;
3. verifies both `get_callback` and `can_callback` report no live callback;
4. prepares server B;
5. verifies B performed registration and the live callback address is B.

RED at `e47928e`:

- exit 101, 0 passed, 1 failed, 0.00s;
- failure: `server A callback remained globally live after RPC shutdown`;
- the same run also observed B skipping preparation and retaining A's address
  before reaching the first assertion.

GREEN:

- exit 0, 1 passed, 0 failed, 0.00s.

`repeated_prepare_preserves_same_live_server_callback` is the companion
characterization. It confirms repeated preparation against one still-live
server returns `None`, preserves exact `Arc` identity, and retains the existing
subscription map. It passed before and after the ownership fix.

### Minimal weak ownership protocol

- `CALLBACK` now stores `Option<Weak<SubscriptionService>>`.
- The RPC server service map remains the durable owner of a usable callback.
- A private async preparation mutex preserves single initialization without
  holding the callback `RwLock` across service registration.
- `live_callback` upgrades under a read lock.
- A dead observation is cleared only after acquiring the write lock and
  confirming pointer identity with `Weak::ptr_eq`.
- `prepare_subscription`, `can_callback`, and `get_callback` all use the same
  live-upgrade semantics.

### Concurrent stale-clear audit

Self-review found an additional interleaving: a reader could observe dead A,
then B could publish before the reader acquired the stale-clear write lock.
An identity check protected B from deletion, but the first implementation
still returned absent once.

`dead_observation_rechecks_concurrent_live_replacement` deterministically holds
the reader after dead-A observation, publishes B, then releases the reader.

RED:

- exit 101, 0 passed, 1 failed, 0.00s;
- failure: `dead A observation hid concurrently published live callback B`.

Fix:

- if the slot changed while the reader upgraded locks, the reader upgrades and
  returns the current weak value;
- it clears that replacement only if the replacement is also dead.

GREEN:

- exit 0, 1 passed, 0 failed, 0.00s.

No retry loop or per-client callback redesign was added.

## Subscription Callback Focused Verification

All commands ran locally.

```text
RUSTFLAGS='-Awarnings' cargo test raft::client:: -- --test-threads=1
```

- 3 passed, 0 failed; 0.01s.

```text
RUSTFLAGS='-Awarnings' cargo test \
  raft::state_machine::callback:: -- --test-threads=1
```

- 10 passed, 0 failed; 8.01s.

```text
RUSTFLAGS='-Awarnings' cargo test rpc:: -- --test-threads=1
```

- 28 passed, 0 failed; 22.51s.

## Subscription Callback Final Verification

Fresh after the concurrent replacement correction:

```text
RUSTFLAGS='-Awarnings' cargo test --all-targets -- --test-threads=1
```

Exit status: 0.

- Library: 188 passed, 0 failed, 0 ignored; 292.89s.
- Graceful-shutdown integration:
  6 passed, 0 failed, 1 pre-existing ignored; 18.75s.
- Single-node recovery: 2 passed, 0 failed; 41.09s.
- Example target: 0 tests.
- Aggregate: 196 passed, 0 failed, 1 ignored.

Final static verification:

```text
RUSTFLAGS='-Awarnings' cargo check --all-targets
rustfmt --edition 2021 --check src/raft/client.rs
git diff --check
```

- All-target check passed in 1.90s.
- Scoped rustfmt and diff checks passed.

## Subscription Callback Scope and Concerns

The Bifrost commit changes only `src/raft/client.rs`.

Self-review:

1. No callback `RwLock` guard crosses an await.
2. The preparation mutex crosses initialization intentionally to serialize the
   one process-global callback choice; no path acquires the mutex in reverse
   order.
3. Dead clearing is allocation-identity safe and rechecks a concurrently
   replaced slot.
4. `can_callback` and `get_callback` never report a dead weak entry as live.
5. Same-live repeated preparation preserves callback identity and subscriptions.
6. The design deliberately remains one live process-global callback. A
   transient strong `Arc` already handed to an in-flight caller can delay
   rebinding until that operation drops it; the required shutdown regression
   explicitly retains no such caller ownership.
7. No in-repository code accesses `CALLBACK` directly outside
   `src/raft/client.rs`; changing its public value from strong to weak ownership
   has no in-repository migration surface.
