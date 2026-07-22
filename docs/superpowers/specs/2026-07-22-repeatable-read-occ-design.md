# Repeatable-Read OCC Design

## Summary

Nebuchadnezzar will restore version-certified optimistic concurrency control (OCC) for transactions. Reads remain lock-free during transaction execution. At prepare, each participant acquires the existing short-lived per-cell certification locks and validates the versions or absence conditions observed by the transaction. A stale transaction is rejected with `NotRealizable` before its writes are applied.

This restores the intent of the original 2017 read-version checking without restoring its commit-time deadlock. It also removes lost updates between transactions coordinated by different servers, where concurrent vector clocks cannot be ordered with `<` or `>`.

## Historical Context

The repository still contains most of the original OCC data model:

- `DataObject.version` records a version returned by a transactional read.
- `CommitOp::Read(Id, u64)` represents a read observation.
- The transaction manager can construct read operations.

The working certification path was removed in stages:

1. On 2017-05-27, commit `cb4e2f3b` added read-version validation.
2. On 2017-05-27, commit `a0c04e3d` retained read locations while validating them.
3. On 2017-05-28, commit `abbfe385` commented out that validation after a deadlock.
4. On 2017-07-18, commit `583208e8` removed the commented implementation.
5. On 2025-05-02, commit `fa894d2b` filtered unchanged reads out of `generate_affected_objs`, so read observations no longer reach participants.

The current timestamp checks are not a substitute for version certification. Transaction IDs are vector clocks, and causally concurrent vector clocks have no `<` or `>` ordering. Two coordinators can therefore accept stale read-modify-write transactions and lose an update.

## Goals

- Return the same observed cell state for repeated reads within one transaction.
- Treat observed absence as repeatable state within one transaction.
- Prevent two transactional read-modify-write operations from both committing from the same cell version.
- Detect conflicts using cell versions rather than total ordering assumptions about vector clocks.
- Preserve optimistic execution: normal reads do not acquire locks retained until transaction end.
- Preserve the existing transaction API and use `NotRealizable` for retryable certification conflicts.
- Keep prepare overhead linear in the number of transaction dependencies and limited to header/version checks.

## Non-Goals

- Predicate or range-lock support and phantom prevention.
- MVCC or historical version retention.
- Making non-transactional RPC operations participate in transaction isolation.
- Replacing the current two-phase commit, undo-log, timeout, or Wait-Die mechanisms.
- Providing external linearizability.

## Isolation Contract

The resulting contract is repeatable cell reads with OCC certification:

- The first read of a cell records its complete value and version in the transaction-local cache.
- Later full, selected-field, and header reads of that cell derive their result from the cached observation.
- A missing-cell result is cached as an expected-absence observation.
- A read-write transaction certifies every cell whose value or absence influenced it.
- A cell version or existence change before certification causes the transaction to abort.
- Certification locks are retained from successful prepare through commit or abort.

This prevents non-repeatable cell reads and lost updates. It does not prevent phantoms because queries do not yet record predicate or range dependencies.

Read-only transactions do not require distributed prepare or commit. Their local cached observations are already repeatable, and they have no writes whose correctness depends on certification.

## Transaction Observations

Replace implicit `version: Option<u64>` interpretation with an explicit serializable expectation:

```rust
pub enum CellExpectation {
    Present(u64),
    Absent,
}

pub enum PrepareIntent {
    Read,
    Write,
}

pub struct PrepareOp {
    pub id: Id,
    pub expectation: CellExpectation,
    pub intent: PrepareIntent,
}
```

The transaction manager maintains one data object per observed or changed cell:

- `Present(version)` for an existing cell read or selected for update/removal.
- `Absent` for a missing cell observation or a new-cell insertion.
- `Read` when the transaction only observed the cell.
- `Write` when the transaction will insert, update, or remove the cell.

The explicit representation avoids treating `None` as both “not read” and “expected missing.”

## Read Path

### Full reads

The existing full-cell cache remains authoritative. The first accepted read stores the complete `OwnedCell` and `Present(header.version)`. Subsequent reads return the cached cell without another data-site read.

### Selected and header reads

`read_selected` and `head` must use the same full-cell snapshot as `read`. On the first access they fetch and cache the full cell, then project fields or return its header locally. This trades a larger first response for a single coherent per-cell observation and avoids incompatible partial caches.

### Missing cells

`CellDoesNotExisted` is stored as `cell: None` with `CellExpectation::Absent` and `changed: false`. Repeated reads return the cached missing result even if another transaction inserts the cell later.

### Read-your-writes

The existing transaction-local changed value remains authoritative. Update-then-read, remove-then-read, and write-then-read continue to return the pending local state.

## Write Path

### Read-modify-write

When an existing cached observation is updated or removed, its original `Present(version)` expectation is retained. The desired value changes, but the expected base version never changes.

### Blind update and remove

If `update` or `remove` is called without a prior transactional read, the transaction manager obtains the current cell header and records `Present(version)` before adding the write intent. This preserves the existing blind-operation API while giving it compare-and-swap semantics from the operation's execution point.

If the target does not exist, the existing write error is returned and no write intent is added.

### Insert

`write` records `Absent`. Prepare fails if a cell with the same ID exists. If an earlier transactional read already observed the cell as absent, the same expectation is reused.

## Prepare and Certification

The data-site `prepare` RPC receives sorted `PrepareOp` values instead of bare cell IDs.

For each operation, the participant:

1. Resolves the per-cell metadata mutex.
2. Applies the existing owner conflict and Wait-Die policy.
3. Acquires the certification ownership marker.
4. Reads only the current cell header or confirms absence.
5. Compares the current state with `CellExpectation`.

All ownership markers for the participant are acquired before certification succeeds. On any mismatch, prepare returns `NotRealizable` and releases ownership acquired by that prepare attempt. The transaction manager aborts every participant, including participants that already voted success.

The participant stores the certified expectations in its local transaction state. They are not trusted solely from the later commit payload.

Timestamp read/write metadata may remain for cleanup, scheduling, and compatibility, but it is not the authority for version correctness. Concurrent vector-clock timestamps must not cause a stale version to pass certification.

### Total Wait-Die priority

Wait-Die requires a total transaction priority, but transaction IDs are vector clocks and concurrent vector clocks have no natural order. Certification therefore derives a separate `TxnPriority` from the transaction ID and coordinator server ID:

1. Causally ordered vector clocks retain their causal order.
2. Concurrent vector clocks are ordered by coordinator server ID.
3. Transaction IDs from the same coordinator are causally ordered; equality identifies the same transaction.

The per-cell ownership record stores both transaction ID and coordinator server ID. Every participant uses the same comparator, so exactly one of two conflicting transactions is older. A younger requester aborts and an older requester may wait. Wait edges can only point from older to younger priorities and therefore cannot form a cycle, including across participants.

This total priority is used only for contention scheduling. Cell-version expectations remain the correctness authority.

## Commit and Direct-Write Boundary

Commit applies operations while certification ownership remains held.

Transactional update and remove operations use the certified expected version in the storage-layer conditional mutation. This is a defensive check against mutation paths that do not participate in the transaction metadata locks. Insert already relies on the storage layer's “must not exist” behavior.

If a defensive commit-time comparison fails, the participant returns `CellChanged`; the transaction manager invokes the existing abort/undo flow for all participants. This check is a safety net, not the primary certification point.

Plain cell RPC writes currently bypass transaction certification ownership. They remain outside the transaction isolation contract. Applications requiring no-lost-update behavior must use transactions or the existing explicit compare-version RPC. Making plain writes coordinate with prepared transactions is a separate API and availability decision.

## Distributed Transactions

Each participant certifies its local dependency set. The transaction manager sends prepares concurrently and proceeds to commit only after every participant votes success. A failed vote aborts all participants.

The manager must await every prepare RPC it launched before starting abort. It records which participants voted success and aborts the complete participant set after all responses arrive. This prevents a slow prepare from acquiring ownership after an early failing response has already triggered cleanup.

This design does not compare transaction vector clocks to decide whether an observed value is current. Consequently, transactions begun on independent coordinators are protected even when their vector clocks are concurrent.

## Conflict and Error Behavior

- Version mismatch: `DMPrepareResult::NotRealizable`.
- Expected-present cell missing: `DMPrepareResult::NotRealizable`.
- Expected-absent cell present: `DMPrepareResult::NotRealizable`.
- Certification owner conflict: existing Wait-Die `Wait` or `NotRealizable` behavior.
- Concurrent-vector-clock owner conflict: causal order followed by coordinator-ID tie-breaking determines the Wait-Die result.
- Storage changed after prepare through a non-participating path: `DMCommitResult::CellChanged` followed by abort/undo.
- Missing target during blind update/remove discovery: existing `WriteError`.

No new public retry category is required. Clients already treat `NotRealizable` as a signal to begin a fresh transaction and retry the complete operation.

## Performance

- Normal execution reads remain lock-free.
- Repeat reads are served from the local transaction cache.
- Certification performs one header lookup and comparison per dependency.
- Prepare messages add a compact expectation and intent to each existing cell ID.
- Locks are held only during the existing prepare-to-end window.
- Read-only transactions avoid distributed certification.
- Memory grows by one compact expectation per observed cell until prepare/abort/timeout.

The principal behavior change is that read dependencies are retained for read-write transactions instead of discarded. Large read-write transactions will therefore send and certify their actual dependency set, which is required for correctness.

## Testing Strategy

Tests must be written before implementation and must first fail for the expected reason.

### Repeatable reads

- A full cell read returns the original value after another transaction updates and commits.
- A selected-field read followed by another selected or full read uses the original cell snapshot.
- A header read followed by a full read reports the same version.
- A missing-cell read remains missing after another transaction inserts that ID.
- A transaction reads its own pending write, update, and removal.

### Lost-update prevention

- Two transactions read counter `0`, both calculate `1`, and only one can certify and commit.
- The losing transaction returns `NotRealizable`; retrying from a fresh read produces final counter `2`.
- The same test uses transaction managers on different servers with concurrent vector clocks.
- A blind update racing another update certifies only one base version.
- Update and remove preserve the version observed before the write call.
- Insert fails certification when another transaction creates the ID first.

### Distributed failure behavior

- A two-participant transaction with one stale dependency applies no durable writes after prepare failure.
- A participant that prepared successfully releases its ownership when another participant rejects.
- A defensive commit-time version mismatch triggers abort/undo on every participant.

### Regression and performance checks

- Existing transaction cleanup, Wait-Die, undo-log, tiered-memory, and index coordination tests remain green.
- A read-only transaction performs no prepare RPC.
- Prepare performs header reads rather than cloning cell payloads.

## Compatibility

The external begin/read/write/update/remove/prepare/commit API remains unchanged. Internal transaction and data-site RPC payloads change together and do not require compatibility with mixed binary versions unless rolling upgrades are introduced separately.

Existing callers that use blind update/remove continue to work, with the stronger behavior that a concurrent change makes prepare fail rather than being overwritten.

## Acceptance Criteria

- Every transactional access path has stable per-cell rereads.
- Read-modify-write conflicts are decided by observed cell versions.
- No two transactions can successfully commit updates derived from the same base version of one cell.
- The guarantee holds for concurrent vector clocks from different coordinators.
- Certification happens before write application and under prepare ownership.
- Read-path locking behavior is unchanged.
- The focused OCC tests and the existing transaction suite pass.
