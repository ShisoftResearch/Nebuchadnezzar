# Bifrost lifecycle review 1

Base: `61239a7`
Head: `5ee96ea`
Verdict: Needs fixes

## Spec Compliance

Partial. The patch implements identity-tagged TCP/service registrations, weak
generated-service shortcuts, handler draining, service clearing, and explicit
Raft runtime ownership. It does not yet meet the replacement,
shutdown/listen-race, atomic client-generation, and owned-runtime teardown
requirements.

## Strengths

- TCP and generated-service tokens use distinct `Arc<()>` allocation identity
  and `Arc::ptr_eq`, avoiding numeric-generation ABA
  (`src/tcp/shortcut.rs:22-32`, `src/rpc/mod.rs:45-61`).
- Generated shortcuts store `Weak<dyn Service>` and compare-remove by token
  (`src/rpc/proto.rs:126-135`, `src/rpc/proto.rs:168-188`). Service
  registration/removal/shutdown use a consistent lock order, with no lock
  held across `.await` (`src/rpc/mod.rs:250-324`).
- TCP callback lookup clones under the read lock and invokes after releasing
  it (`src/tcp/shortcut.rs:76-87`). The listener binds before publishing,
  tracks handlers, and drains them before returning
  (`src/tcp/server.rs:47-57`, `src/tcp/server.rs:62-145`).
- The normal externally invoked Raft shutdown path takes the runtime owner and
  awaits destruction on `spawn_blocking`; `Drop` avoids Tokio's
  async-context runtime-drop panic (`src/raft/mod.rs:545-555`,
  `src/raft/mod.rs:2080-2097`).

## Critical

### Same-ID TCP replacement can deadlock

`register_server` replaces and drops the old strong callback while holding
`TCP_CALLBACKS.write()` (`src/tcp/shortcut.rs:65-72`). That callback strongly
owns the RPC server (`src/rpc/mod.rs:126-159`). If it is the last server owner,
`Server::drop` calls `shutdown_owned`, drops the stale registration, and
`ShortcutRegistration::drop` re-enters the same write lock
(`src/rpc/mod.rs:331-344`, `src/tcp/server.rs:154-165`,
`src/tcp/shortcut.rs:46-55`). Drop replaced/removed callbacks only after
releasing the global lock.

## Important

### RPC shutdown is not serialized with listener publication

`listen_and_resume` separately stores the TCP server, spawns, and stores the
join handle (`src/rpc/mod.rs:194-217`), while shutdown separately takes the
two fields (`src/rpc/mod.rs:222-247`). Shutdown can miss the handle and return
before listener/handler completion. Direct `Server::listen` never records
completion (`src/rpc/mod.rs:181-192`). Neither listen entry rejects an already
shutting-down server. TCP pre-shutdown standalone listen still registers and
does not execute listener-branch cleanup (`src/tcp/server.rs:47-59`,
`src/tcp/server.rs:134-145`).

### Client-pool generation handling is incomplete

Socket clients have no token (`src/tcp/client.rs:46-63`), but eviction treats
`None` as owned by every local generation (`src/rpc/mod.rs:413-443`), so an
old shutdown can evict a replacement socket client. `get_by_id` connects and
later inserts without the per-key lock (`src/rpc/mod.rs:399-409`); a late
client can be inserted after eviction or overwrite a replacement. Existing
tests cover replacement before eviction lock acquisition but not
absent/connect/insert races.

### Owned-runtime shutdown cannot synchronously await itself

The self-runtime branch uses `shutdown_background`
(`src/raft/mod.rs:2090-2097`), so the return itself does not prove immediate
driver-FD release. The implementable contract must be explicit:

- externally invoked graceful shutdown deterministically releases the runtime
  and driver FDs before returning;
- shutdown invoked by a task running on the owned runtime transfers teardown
  out of the service, returns without deadlock, and completes teardown after
  that calling task exits. It cannot wait for its own runtime to join without
  deadlocking.

Add eventual post-task FD/weak-owner evidence for the self-runtime path. Do
not weaken the deterministic external-shutdown path.

## Required regression coverage

- Last-external-`Arc` standalone replacement completes under timeout and
  preserves the replacement.
- Barrier-controlled concurrent RPC listen/shutdown, direct-listen shutdown,
  and already-shutdown-before-listen behavior.
- Tokenless replacement-client eviction and late/competing pool insertion.
- Immediate FD accounting for external graceful Raft shutdown and eventual FD
  accounting after an owned-runtime shutdown caller exits.
