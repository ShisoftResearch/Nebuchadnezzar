# Ranged Write-Back Lifecycle Design

**Date:** 2026-07-22

## Purpose

Make ranged B-tree write-back progress independent of the lifetime and execution order of
individual Nebuchadnezzar servers and Tokio runtimes in the same process. The repair must
eliminate the clean-baseline failure in which ranged migration remains active until clients
exhaust 300 retries, and it must prevent one server shutdown from stopping workers or
discarding queued writes owned by another live server.

This repair is a prerequisite for OCC phase profiling. It does not change ranged index
routing, split boundaries, migration ordering, persistence ordering, or the OCC protocol.

## Root Cause

The B-tree write-back queue, counter, progress watermark, and worker-control flags are
process-global. However, `start_external_nodes_write_back` launches the workers with
`tokio::spawn`, binding them to the first caller's runtime, and discards their join handles.
When that runtime exits, the workers disappear while the process-global `WB_STARTED` flag
remains true. Later servers therefore refuse to start replacement workers.

The migration retain path waits for the process-global write-back watermark before clearing
the source tree's migration marker. If the workers have disappeared, the watermark never
advances. Client operations continue receiving `OpResult::Migrating` until the retry ceiling
is reached. A serial run of the two ranged pressure tests reproduced the complementary hang:
both tests pass in fresh processes and in a concurrent run, while the second test in a
single-process serial run timed out after 600 seconds.

Individual server shutdown is unsafe for the same reason. `NebServer::shutdown` currently
calls a global reset that stops all workers, resets the shared counters, and drains the shared
queue without checking whether another server is still active.

## Considered Approaches

### 1. Process-owned write-back runtime (selected)

Create the write-back runtime once per process and retain it in a `OnceLock`. Its worker tasks
live on that dedicated runtime instead of whichever application or test runtime happens to
initialize the first ranged service. Individual servers wait for the queue watermark during
shutdown but never stop the process worker pool or reset shared progress.

This matches the existing process-global queue and counters, fixes both observed lifecycle
failures at their source, and requires no routing or storage-format change. The runtime and
workers intentionally live until process exit.

### 2. Runtime-bound workers with liveness restart

Track active workers, clear `WB_STARTED` when their futures are dropped, and restart workers
from a later `wait_until_updated` call. This reduces the symptom but leaves an interval with
no workers, complicates generation races, and still requires solving destructive per-server
reset semantics. It is rejected in favor of stable process ownership.

### 3. Test-only serialization and cleanup

Serialize the pressure tests and explicitly shut each server down. This can hide the immediate
failure but does not protect multiple live servers in production; in fact, the existing global
shutdown reset can corrupt the other live server. It is useful only as supplementary fixture
hygiene and is not the repair.

## Architecture

`src/index/ranged/tree/btree/storage.rs` remains the sole owner of write-back scheduling.
Replace the caller-runtime flags with a process-level manager initialized through
`std::sync::OnceLock`:

1. `start_external_nodes_write_back` calls `get_or_init`.
2. Initialization builds a dedicated Tokio multi-thread runtime with the existing bounded
   worker count and descriptive thread names.
3. The manager spawns the existing queue-consumer loops onto that runtime and retains the
   runtime for the life of the process.
4. Subsequent ranged services reuse the initialized manager and do not create more workers.

The existing `CHANGED_NODES`, `CHANGE_COUNTER`, `CHANGE_PROGRESS`, and ordered-completion set
remain process-global. Queue entries already carry the client needed to persist their node, so
the workers do not depend on the server that initialized the manager.

`wait_until_updated` continues to snapshot the newest queued operation and waits until the
contiguous completion watermark reaches it. It checks whether the process manager has been
initialized, rather than consulting a mutable server-owned started flag.

## Lifecycle

The process manager has no per-server stop operation. Dropping a `NebServer` or its Tokio
runtime cannot drop the write-back runtime. On process exit, the operating system terminates
the process-owned worker threads along with the rest of the process.

`NebServer::shutdown` retains the write-back barrier before it shuts down RPC or storage. It
must not call `reset_write_back_state`. Because the shared counters are monotonic, a later
server in the same process can enqueue more operations and use the same completion watermark.
No queued entry is drained without being processed.

Counter overflow is not changed by this repair. At one billion queued changes per second, a
64-bit/64-bit-platform `usize` counter lasts centuries; altering counter representation is out
of scope.

## Error Handling

Worker initialization failure is fatal and reports a specific message because ranged storage
cannot make durable progress without the manager. A deletion RPC failure must be logged and
must not panic the worker task. The operation is still marked complete under the current
best-effort deletion contract, matching the existing behavior after the former `unwrap`
succeeded; changing persistence retry policy is outside this repair.

Worker loops must not contain a normal per-server stop flag. This removes the state in which
the manager claims to be started while every worker has exited.

## Testing

The implementation follows TDD using the already observed red reproduction and a focused
worker-lifetime regression test.

- A storage-level test starts write-back from one Tokio runtime, drops that runtime, then
  submits a test probe and requires the process manager to consume it. The pre-fix design
  cannot satisfy this because its tasks die with the caller runtime.
- The two existing pressure tests run serially in one test process under a bounded timeout;
  the pre-fix run timed out after 600 seconds and the repaired run must pass.
- Each pressure test must continue to pass alone and under default test concurrency.
- The complete `cargo test` baseline must pass without the earlier migration retry exhaustion.
- `cargo fmt --check` and `git diff --check` remain required.

Test-only probe support is compiled only under `cfg(test)` and does not alter production queue
entries or worker behavior.

## Safety and Compatibility

The repair does not change the order assigned to queued changes or the contiguous completion
watermark. It does not clear a migration marker earlier, redirect an operation during a split,
or bypass persistence. It only ensures that the existing write-back work continues to run
independently of unrelated runtime teardown.

There is no configuration, wire-protocol, storage-format, or public API migration. Default
production behavior gains a small fixed process-level Tokio runtime only when ranged B-tree
write-back is first initialized; worker count remains bounded by the existing 2-to-8 rule.

## Non-Goals

- Per-database write-back queues or fairness policies.
- Retrying failed deletion RPCs.
- Changing migration retry counts or backoff.
- Changing split, retain, or placement semantics.
- Refactoring the B-tree persistence format.
- Optimizing OCC transactions.
