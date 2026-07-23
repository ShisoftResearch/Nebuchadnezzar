# OCC Repeatable-Read Snapshot Pinning (Size-Gated)

## Summary

Repeatable reads in the current OCC implementation are bought by cloning the
entire cell into the coordinator's private transaction memory on first access,
even when the caller only needs a header or a projected field. For large cells
this whole-cell fetch, transfer, and clone dominates read latency and is the
principal cause of the `projected_reads` regression (~12x slower than
`develop`).

This design removes that clone for large cells without changing any isolation
guarantee. A transactional read of a large cell records the version's storage
**location plus a held `SegmentReferenceGuard`** instead of copying its bytes.
The storage engine is copy-on-write, so the read version's bytes are immutable;
the guard prevents the cleaner and tiered eviction from reclaiming them.
Repeatability is then provided by immutability-plus-pinning rather than by
copying. The whole-cell transfer is deferred until (and only if) an actual full
read occurs, and is served from the pinned version.

This is the read-side half of MVCC with none of the hard parts: no version
index, no visibility rule, no garbage-collection redesign. Each transaction
needs only the single version it read at first access, pinned in place. Full
MVCC (a later increment) generalizes "one pinned version per cell" into "a
pinned version chain plus a visibility rule," reusing the pin and the
participant read-set lifecycle built here.

## Context

The repeatable-read OCC implementation is committed on
`feature/repeatable-read-occ`. Today's read path:

- The coordinator's `read_from_site` fetches the full cell from the participant
  and stores the owned cell in the transaction's `txn.data` cache.
- Later `head`, `read_selected`, and full `read` calls are served from that
  cached owned cell, which guarantees repeatable full/selected/header reads and
  repeatable absence.
- The cached version also feeds read certification: the observed version is
  recorded as `CellExpectation::Present(version)` and certified at prepare.

The engine that makes a cheaper path possible:

- Updates are copy-on-write: `Chunks::update_cell_by` writes a new cell at a new
  location, atomically flips the `cell_index` pointer, and marks the old entry
  dead. Old-version bytes are never mutated in place; they remain valid until the
  cleaner reclaims the dead space.
- Removes write a tombstone and mark the old entry dead.
- `SegmentReferenceGuard` pins a segment so the cleaner cannot reclaim it and
  tiered storage cannot evict it. The commit/abort paths already use it to keep
  rollback cells alive.

Because old versions are immutable until reclaimed, and reclamation is
preventable with an existing primitive, a transaction can hold a stable
reference to the exact version it read without copying the bytes.

## Goals

- Eliminate the whole-cell fetch and clone that provides repeatable reads, for
  cells above a configurable size threshold.
- Preserve every immutable correctness property of the repeatable-read OCC
  contract, unchanged.
- Improve `projected_reads` (`head`, `selected`, `mixed`) throughput and p95
  latency without regressing any other portfolio scenario.
- Keep small-cell and non-transactional behavior byte-for-byte identical to
  today.
- Leave the pin and participant read-set machinery reusable by a later MVCC
  increment.

## Non-Goals

- Building a version index, a snapshot visibility rule, or multi-version garbage
  collection. Those are later MVCC increments.
- Changing the prepare, commit, or abort protocols, or the certification logic.
  (A new lightweight read-only pin release is added; it is not a prepare and
  does not alter those protocols. See the read-only pin lifecycle.)
- Optimizing small-cell reads, where the clone is already cheap.
- Changing the on-mmap cell format or cell header.
- Changing non-transactional cell RPCs.

## Immutable Correctness Contract (unchanged)

This increment must not change any property of the repeatable-read OCC contract.
In particular:

1. The transaction's first observation of a cell remains its repeatable snapshot
   for later full, selected, and header reads.
2. An observed missing cell remains missing within the transaction.
3. Every read dependency that influences a write is certified at prepare.
4. Inserts certify absence; updates and removes certify the expected present
   version.
5. Two transactions derived from the same cell version cannot both commit
   conflicting writes.
6. The guarantee holds across coordinators with concurrent vector clocks.
7. Participant expectations are validated before any participant mutation.
8. Storage mutations remain conditional on the certified version or absence.
9. Certification ownership is held through commit or rollback and released only
   by its owner.
10. Prepare votes settle before failure cleanup.
11. A successful prepare whose response is not delivered is rolled back.
12. Once abort is accepted, commit stays illegal; partial abort failures stay
    retryable.
13. Read-only transactions continue to avoid distributed prepare. (A pin release
    is not a prepare; transactions that pin nothing contact no participant at
    completion, exactly as today.)

## Design

### Size gate

A tunable threshold (`NEB_TXN_READ_PIN_BYTES`, default a small multiple of the
cell-header size on the order of a few kilobytes) decides the read path per
cell:

- Serialized cell size at or below the threshold: the current
  clone-into-`txn.data` path is used, unchanged.
- Serialized cell size above the threshold: the pin-and-defer path is used.

Because sub-threshold cells take the exact current path, small-cell scenarios,
blind mutation, hot-cell, multi-cell, and multi-participant workloads composed
of small counter cells are provably unaffected. Only large-cell reads change.

### Participant-side pinned read-set

Each participant maintains, per transaction, a pinned read-set:

```
txn -> { cell_id -> { location, version, guard: SegmentReferenceGuard } }
```

On a large-cell read for a transaction, the participant:

1. Resolves the current version of the cell (as today).
2. Acquires a `SegmentReferenceGuard` on the segment holding that version.
3. Records `{location, version, guard}` in the transaction's pinned read-set.
4. Returns only what the caller asked for (header for `head`, projected fields
   for `read_selected`, full bytes for a full `read`).

Subsequent reads of the same cell by the same transaction are served from the
pinned `location`, not from `cell_index`, so a concurrent update that advances
the current version does not change what the transaction observes. The pinned
bytes are immutable (copy-on-write) and cannot be reclaimed (guard held), so all
later reads of that cell within the transaction are repeatable and consistent.

### Coordinator-side deferred fetch and small-result cache

For a pinned large cell the coordinator's `txn.data` entry holds
`{version, header, cached projections, pin reference}` instead of the full owned
cell. Reads are served as follows:

- `head`: served from the cached header after the first access; no full transfer
  ever occurs for a header-only usage.
- `read_selected`: served from a cached projection when present; otherwise one
  RPC projects it from the pinned version and caches the result.
- Full `read`: one RPC fetches the full bytes from the pinned version, done once,
  only when a full read is actually requested.

A transaction that only ever performs `head` and `selected` reads on a large
cell never transfers or clones the whole cell. Read-your-writes is unchanged: a
buffered write shadows the pinned read exactly as it shadows a cached cell
today.

### Certification (unchanged)

The coordinator records the observed version as `CellExpectation::Present`
exactly as today and certifies it at prepare. The pin is orthogonal to
certification; it provides repeatability of the read value, not conflict
detection. Write-skew prevention and gates 3 through 5 are untouched.

### Read-only pin lifecycle

Pins are memory-retention references, not correctness locks. Losing a pin
release is fail-safe: it only delays reclamation, never affects correctness or
isolation.

- A read-write transaction already contacts each participant at commit or abort;
  its pins are dropped there, alongside the existing segment-guard cleanup.
- A read-only transaction that pinned large cells sends a lightweight release to
  the participants holding its pins on completion. The participant's existing
  stale-transaction cleanup is the backstop if that release is lost.
- A read-only transaction that pinned nothing (all reads sub-threshold) keeps
  today's zero-participant-contact completion, preserving property 13.

### Concurrency and correctness

- **Concurrent transactional overwrite.** An update writes a new version
  elsewhere and marks the old dead; the pinned version's bytes are unchanged and
  unreclaimable. The reader keeps seeing its version.
- **Concurrent non-transactional overwrite.** Same: a non-transactional
  `update_cell_by` is copy-on-write and marks the old dead; the guard prevents
  the cleaner from reclaiming the pinned version.
- **Remove.** A remove tombstones and marks the old dead; the pinned version
  survives, so repeatable reads and repeatable absence both hold.
- **Cleaner and tiered eviction.** Both honor `SegmentReferenceGuard`, so
  neither reclaims nor evicts a pinned version.
- **Certification races** are unchanged, because certification still runs
  against the current stored version at prepare.

## Testing and Acceptance

Correctness gates (must pass before acceptance), run as in the OCC optimization
loop:

```
cargo test --lib server::transactions -- --test-threads=1
cargo test --lib ram::tiered -- --test-threads=1
cargo test --lib index::full_text -- --test-threads=1
cargo check --lib
```

New focused tests:

- A large-cell transaction observes the same header, selected fields, and full
  value across repeated reads while a concurrent transaction updates the cell to
  a new version.
- The same, with a concurrent non-transactional overwrite plus a forced cleaner
  pass, proving the pinned version is neither mutated nor reclaimed.
- Repeatable absence: a pinned large cell removed by a concurrent transaction
  still reads as its pre-remove snapshot within the pinning transaction.
- A header-only large-cell transaction performs no full-cell transfer
  (instrumented assertion on bytes transferred or cells cloned).
- Certification still aborts a read-write transaction whose pinned cell advanced
  since it was read.
- Read-only pins are released on completion, and stale-cleanup reclaims a pin
  whose release was dropped.

Benchmark acceptance (same policy as the OCC optimization loop, on the dedicated
host with NUMA binding):

- `projected_reads` (`head`, `selected`, `mixed`) improves throughput or p95 by
  at least 5% at CV at most 5%.
- No stable secondary scenario loses more than 3% throughput or gains more than
  5% p95. Small-cell scenarios are expected to be unchanged because they take
  the current path.
- All workload invariants pass and all unexpected-outcome lists are empty.

## Roadmap: path to full MVCC

This increment is the first slice of a snapshot-isolation-with-MVCC-reads
direction whose target is snapshot reads plus kept read certification (so every
current gate, including write-skew prevention, is preserved while reads get
cheaper). Later increments, each its own spec:

- **Version index.** Generalize the single pinned version into a side index
  `cell_id -> ordered [{version, commit_ts, location}]`, populated at commit,
  with no cell-format change and pay-per-use cost (zero on cells never accessed
  transactionally).
- **Snapshot visibility.** Assign each transaction a snapshot `S` (begin clock)
  and resolve the highest version with `commit_ts <= S` under the existing
  total order (causal `Before` with `deterministic_cmp` tie-break), giving a
  consistent, repeatable, cross-server cut.
- **Snapshot-gated GC.** Retain versions until the oldest active snapshot passes
  them; reclaim below the watermark, integrated with the cleaner.

## Risks and Mitigations

- **Pinning blocks the cleaner** for a transaction's lifetime on the pinned
  segments. Mitigated by the size gate (only large cells pin) and by read-only
  release plus stale-cleanup. If long transactions over large cells prove to
  pin too much, the threshold is tunable upward.
- **Participant read routing** must serve a pinned cell's later reads from the
  pin, not from `cell_index`. This is the one participant-path change; it is
  covered by the repeatable-read-under-concurrent-update tests.
- **Engine under active repair.** This increment touches the transaction read
  path and the pin lifecycle, not the cleaner internals or the cell format, so
  it stays clear of the storage-engine areas currently being repaired.
