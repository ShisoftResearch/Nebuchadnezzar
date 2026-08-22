# Task 11 dependency: Bifrost RPC/Raft lifecycle fix

## Problem

Nebuchadnezzar's combined local transaction suite exhausts its normal
`RLIMIT_NOFILE=1024` because stopped Bifrost RPC servers and registered Raft
services remain strongly reachable. Repeated server lifecycles retain Tokio
runtime epoll/eventfd descriptors.

## Requirements

- Explicit RPC shutdown must release the TCP shortcut registration, wait for
  the listener and accepted handlers to finish, unregister generated service
  shortcuts, clear owned services, and release the server/service ownership
  cycle.
- A running listener must continue to own its RPC server even if the caller
  drops its external `Arc`; explicit shutdown is the operation that breaks
  this ownership.
- TCP and generated-service unregister operations must be allocation-identity
  safe: shutdown/drop of an old instance must never remove a same-address or
  same-service replacement.
- Client-pool eviction must be generation-safe and atomic with respect to
  replacement; an old shutdown must not evict a replacement's cached client.
- `RaftService` must release its owned Tokio runtime without panicking when
  shutdown/drop occurs in async context, without deadlocking when shutdown is
  invoked from the owned runtime, and without retaining runtime driver file
  descriptors after ordinary graceful shutdown.
- Do not raise the file-descriptor limit. Correctness verification runs
  locally. No backward compatibility is required, but all in-repository uses
  must compile and pass.
- Keep the dependency fix in Bifrost and commit it separately as
  `fix(rpc): release shortcut registrations on shutdown`.

## Expected evidence

- Regression tests demonstrate RED then GREEN for the leaked registrations,
  pre-listen shutdown, async runtime drop, stale generation shutdown, and
  atomic pool eviction.
- Repeated real `RaftService` lifecycles return to their starting FD count.
- Bifrost focused RPC/TCP/Raft tests, full local serial all-target tests,
  `cargo check --all-targets`, scoped rustfmt, and diff checks pass.
