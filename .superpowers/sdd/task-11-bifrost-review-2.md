# Bifrost lifecycle review 2

Base: `61239a7`
Head: `2cb59a0`
Verdict: Needs fixes

## Resolved

- Callback replacement/removal drops strong callbacks outside
  `TCP_CALLBACKS` locks.
- Listener ownership is published as one identity/TCP-owner/completion tuple.
- Tokenless clients no longer match arbitrary local generations, and ordinary
  late/competing pool insertion is serialized and revalidated.
- Single-owner external Raft shutdown closes the service handle and awaits
  runtime destruction; owned-runtime shutdown avoids self-deadlock.

## Important

### Client construction cancellation strands reservations

The owner inserts a construction before awaiting connection
(`src/rpc/mod.rs:563-603`), but cleanup occurs only on ordinary
success/error/timeout or server eviction (`src/rpc/mod.rs:632-699`,
`src/rpc/mod.rs:711-744`). Aborting/dropping the owner future leaves the
reservation and its retained watch sender in the map forever; later callers
wait forever (`src/rpc/mod.rs:571-596`). Add RAII cancellation cleanup that
identity-removes the reservation and durably wakes all waiters.

### STANDALONE completion can outrun shutdown snapshot

Direct/resumed completion decide retention from a separate atomic
(`src/rpc/mod.rs:309-316`, `src/rpc/mod.rs:337-344`), while shutdown stores it
before locking lifecycle state (`src/rpc/mod.rs:355-365`). Completion can
observe shutdown and remove `active` before shutdown snapshots it. Shutdown
then neither signals nor waits, and the standalone registration can remain
owned by the finishing `ListenerRun` until after shutdown returns. Make
explicit shutdown the serialized owner of standalone retirement; do not
self-retire standalone state from the unsynchronized atomic.

### TCP shutdown and registration publication are not atomic

TCP listen checks `shutdown_requested`, later registers/stores the guard
(`src/tcp/server.rs:47-61`), while `shutdown_owned` can run between and return
no token (`src/tcp/server.rs:165-178`). Listen can transiently publish/cache a
generation and then remove it (`src/tcp/server.rs:139-155`); RPC shutdown
evicts only the earlier missing token (`src/rpc/mod.rs:366-397`). Serialize
closing with registration publication under one state mutex, or return the
completed generation for post-completion eviction. No stopped transient
generation may survive in the client pool.

### Concurrent Raft teardown has no shared completion

The first caller takes `rt_owner`; later callers see `None`, close the handle,
and return while teardown may still be running (`src/raft/mod.rs:2123-2143`).
An external graceful call must wait for a teardown already started externally
or on the owned runtime. The self-runtime caller must still return without
self-deadlock, with teardown completing after its task exits.

`RaftRuntimeHandle::spawn` clones the raw handle and unlocks before spawning
(`src/raft/mod.rs:556-568`), so close can race with an untracked clone. Invoke
spawn while holding the wrapper lock so no handle clone escapes the close
critical section. Add a shared durable teardown-completion protocol that is
cancellation-safe after runtime ownership transfers.

## Minor

FD regressions currently allow `baseline + 2`; require exact return to the
starting FD count in isolated tests (`src/rpc/mod.rs:1315`,
`src/rpc/mod.rs:1366`).

## Required deterministic regressions

- Abort a client-construction owner while connection is held; a later getter
  must take ownership/complete rather than wait forever.
- Barrier interleaving where standalone completion reaches retirement while
  shutdown starts; shutdown must not return before registration retirement.
- TCP barrier between pre-check and registration publication plus a cached
  transient-token client; shutdown must evict it before returning.
- Concurrent external/external and self/external Raft shutdown; every external
  caller returns only after exact FD baseline restoration.
- Spawn/close barrier proving no task/handle escapes after close wins.
