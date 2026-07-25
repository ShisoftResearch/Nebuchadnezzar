# Point-Cell MVCC with Revision-Timestamp OCC

## Summary

Add volatile multi-version history for point cells while retaining
Nebuchadnezzar's optimistic concurrency control (OCC) guarantees.

Each logical cell revision receives a storage-assigned `revision_ts: u64`.
Transactional mutations use one shared transaction commit timestamp;
non-transactional mutations use the data server's local Hybrid Logical Clock
(HLC). The revision timestamp becomes the sole identity and ordering token for
a cell revision:

- snapshot visibility,
- repeatable point reads,
- OCC read and write certification,
- recovery ordering,
- tombstone ordering,
- cleaner recency, and
- conditional cell mutation.

The existing `version: u64` field is removed rather than narrowed. There is no
version wrap, version/timestamp pair, or version-based recovery path.

Each `Chunk` owns a history index alongside its existing cell index. A history
index entry is a newest-to-oldest version chain built with Lightning's
`LinkedRingBufferList`. Historical values remain in the append-only segments
where they were written. The cleaner treats retained revisions as live and can
relocate them by atomically updating their chain nodes. Reads hold a short-lived
shared segment lease only while materializing bytes; transactions do not retain
raw addresses or transaction-lifetime segment pins.

History is volatile and time-bounded. The initial default retention is five
minutes and is tunable. Recovery restores only the latest durable cell or
tombstone, advances the local HLC beyond the maximum recovered timestamp, and
starts with no historical snapshot coverage.

This increment covers point cells only. Index, range, and predicate snapshot
semantics remain outside the isolation guarantee.

## Goals

- Give every transaction a stable point-cell snapshot at its transaction ID's
  HLC timestamp.
- Keep automatic read-set certification for every writing transaction, so
  point-read write skew and lost updates remain prevented.
- Make read-only transactions use snapshot history without distributed
  prepare.
- Replace transaction-private pinned addresses with chunk-owned version
  chains.
- Preserve full, selected, header, partial, conditional-update, insert,
  update, remove, undo, and rollback functionality.
- Make recovery and cleaner ordering independent of a wrapping counter.
- Keep the 32-byte cell header and 32-byte tombstone payload sizes.
- Keep current-head reads off the history-index path when the head is directly
  visible.
- Establish benchmark gates before accepting cleaner and reclamation overhead.

## Non-Goals

- MVCC visibility for indexes, ranges, predicates, or phantom prevention.
- Transactional isolation or safety between transactional and
  non-transactional operations.
- Persistent historical version chains across restart.
- Backward compatibility for cells, tombstones, WAL, undo logs, RPCs, public
  header fields, or client binaries.
- A memory-cap retention policy in the initial implementation.
- Replacing two-phase commit or removing a distributed transaction phase.
- External linearizability beyond the existing transaction contract.

## Isolation Contract

The result is point-cell snapshot reads plus full read validation for writing
transactions:

1. `TxnId.ts` is the transaction's fixed snapshot boundary.
2. A revision is visible when `revision_ts < TxnId.ts`.
3. Equality is treated conservatively as concurrent and therefore invisible.
4. A transaction's buffered writes overlay its snapshot.
5. Repeated full, selected, header, and partial reads resolve to the same
   visible revision while that revision remains retained.
6. Read-only transactions do not prepare or certify.
7. A writing transaction automatically certifies every point-cell read,
   including visible absence.
8. Any certified point that changed after the transaction's snapshot causes
   prepare to reject the transaction.
9. Successful prepare ownership remains held through commit, compensation, or
   abort and is released only after distributed resolution.
10. Index and range behavior remains functional but is outside this MVCC
    isolation contract.

The strict `<` rule intentionally uses only the packed `Hlc.ts`, not the HLC
`node`. Distinct concurrent coordinators may issue equal `ts` values for
different cells; treating the complete equality bucket as invisible yields a
safe snapshot cut. Successive accepted mutations of the same cell must have
strictly increasing timestamps, enforced by prepare clock merging and
per-cell mutation serialization.

## Revision Timestamp

### Source

Bifrost's `Hlc` is:

```rust
pub struct Hlc {
    pub ts: u64,   // 48-bit physical milliseconds | 16-bit logical counter
    pub node: u64,
}
```

Only `Hlc.ts` is stored in a cell. The node is needed to make transaction IDs
globally unique and totally ordered for scheduling; it is not needed for
per-cell revision identity under the strict-equality-bucket visibility rule.

### Transactional mutations

The coordinator:

1. begins the transaction with a fresh `TxnId`,
2. sends prepare to every participant,
3. observes every prepare response clock,
4. allocates one fresh commit HLC greater than all prepare response clocks, and
5. sends that same `commit_hlc.ts` to every participant.

Every cell and tombstone installed by the transaction receives that same
`revision_ts`.

### Non-transactional mutations

Every non-transactional cell mutation receives a fresh timestamp from the data
server's local HLC. This timestamp is internal storage ordering for recovery
and cleaner temperature; it does not make non-transactional operations aware
of transactional snapshots and does not create a mixed-mode isolation
guarantee.

The storage runtime must have access to the server HLC or an injected revision
allocator. An unstamped caller-created cell has `revision_ts == 0`; only the
storage layer assigns a persisted nonzero revision.

### High-frequency behavior

The 16-bit HLC logical component naturally carries into the physical portion
after 65,536 logical events in one physical millisecond. Ordering and
uniqueness continue; the logical wall component may move ahead of physical
time. At normal physical progression, the nominal capacity is 65.536 million
HLC allocations per second per source.

Required safeguards:

- HLC increment uses checked arithmetic and must never wrap silently.
- Exhaustion refuses further writes rather than reusing a timestamp.
- Retention uses monotonic elapsed time, not HLC numeric distance, so logical
  clock drift cannot prematurely expire history.
- Recovery advances the HLC beyond all recovered timestamps before writes are
  accepted.
- The shared HLC atomic is included in performance benchmarks.

## Storage Layout

### Cell header

The header remains exactly 32 bytes:

```text
offset  size  field
0       8     revision_ts: u64
8       4     flags: u32
12      4     schema: u32
16      8     partition: u64
24      8     hash: u64
```

`flags` is reserved for format and storage-state metadata and is initially
zero unless an implementation task assigns explicit bits.

`CellHeader.version` and `CellHeader.timestamp` are removed. The old cached
cell wall clock no longer participates in cell writes. Cleaner temperature is
the cell's revision timestamp.

### Tombstone

The tombstone payload remains exactly 32 bytes:

```text
offset  size  field
0       8     segment_seq_id: u64
8       8     revision_ts: u64
16      8     partition: u64
24      8     hash: u64
```

Cells and tombstones share one revision ordering domain. Recovery chooses the
greater timestamp regardless of entry kind.

### Compatibility

There is no backward compatibility:

- existing cell and tombstone entry type numbers may be reused,
- no legacy layout decoder or migration is added,
- existing database directories must be recreated,
- existing WAL and undo logs are invalid,
- RPCs and client binaries change together, and
- public `version` fields and aliases are removed directly.

## Chunk-Owned History Index

Each `Chunk` owns a history index parallel to its cell index and keyed by the
same cell identity. The cell index remains the authoritative optimized pointer
for the current present cell. The history index owns logical revision order and
historical liveness.

Conceptually:

```text
HistoryIndex[CellId]
    -> revision 300: Present(current location)
    -> revision 240: Present(segment location B)
    -> revision 180: Deleted(tombstone location)
    -> revision 120: Present(segment location A)
```

The chain includes the logical current revision. A present current revision
mirrors the cell index location; a deleted current revision represents the
current tombstone. This makes visible absence and delete/recreate history
explicit.

The top-level history index uses the same lock-free/sharded map family as the
cell index. Each value owns one Lightning revision list:

```rust
struct VersionChain {
    revisions: LinkedRingBufferList<Option<Arc<RevisionNode>>, B>,
    truncated_before_ts: AtomicU64,
}

struct RevisionNode {
    revision_ts: u64,
    state_and_location: AtomicUsize,
    retire_deadline: AtomicU64,
}
```

`B` is a small fixed ring-buffer capacity selected by microbenchmark; 32 is the
initial candidate. `Option<Arc<RevisionNode>>` satisfies Lightning's
`Clone + Default` item requirements. The stable `Arc` lets cleaner and GC work
on node metadata independently of list structural changes.

`state_and_location` atomically encodes both the address and one of:

- pending present,
- pending deleted,
- committed present,
- committed deleted,
- aborted, or
- expired.

A transactional physical install is pending until distributed resolution. It
is retained but cannot satisfy a snapshot read. Commit resolution promotes it
to committed before releasing ownership. Abort resolution marks it aborted and
installs any required compensation before releasing ownership. Cleaner treats
pending nodes as live and relocatable; GC cannot expire them.

The list order is newest to oldest:

- new revisions use `push_front`,
- snapshot lookup iterates from the front,
- retention pruning uses `pop_back`, and
- ring-buffer nodes amortize allocation across several revisions.

Lightning's list is mostly lock-free: normal item operations are atomic and
rare ring-buffer structural changes use small spin locks. This is preferred
over a per-cell `RwLock<VecDeque<_>>`. A strictly lock-free custom Crossbeam
singly linked list remains an alternative only if benchmarking shows the
Lightning structure is material overhead.

## Snapshot Read Resolution

### Current-head fast path

A point read first examines the current cell header through the existing cell
index path:

- if the current present head has `revision_ts < snapshot_ts`, return it
  without traversing the history list;
- if the head is too new, or current state is deleted, consult the chain.

This keeps normal current-head reads free from new history traversal.

### Historical lookup

Traverse newest to oldest and select the first node with:

```text
node.revision_ts < snapshot_ts
```

- `Present(location)` materializes that immutable cell revision.
- `Deleted(location)` returns visible absence.
- no match with no truncation means the cell did not yet exist and returns
  absence;
- no match beyond a recorded truncation boundary returns `SnapshotTooOld`.

Lookup is linear in the number of revisions newer than the snapshot. The
common current-head case is O(1). The hot-cell/old-snapshot worst case is a
required benchmark; sparse timestamp checkpoints may be added later without
changing semantics.

### Repeat reads

Transaction-private state records the observed `revision_ts` and cached result,
never a raw storage address or segment guard. A later full, selected, header,
or partial read either uses a cached owned result or resolves the same fixed
snapshot again. Because accepted revisions for a cell are strictly ordered and
immutable, the same snapshot selects the same revision until retention expires.

An exact result already cached as owned transaction data remains usable. If a
later read shape requires rematerialization after the required history expires,
it returns `SnapshotTooOld`; it must not silently return a different revision.

### Read-your-writes

Coordinator-buffered inserts, updates, and removes overlay participant snapshot
reads. A write followed by any read shape returns the pending transaction-local
state.

## OCC Expectations and Write-Skew Prevention

The version-based expectation becomes:

```rust
pub enum CellExpectation {
    Present(u64),         // observed revision_ts
    Absent(Option<u64>),  // visible delete revision, or never existed
}
```

- `Present(ts)` certifies that the point's current logical revision is still
  `ts`.
- `Absent(Some(delete_ts))` certifies the exact visible tombstone.
- `Absent(None)` certifies that no revision has ever become visible for the
  point.

The timestamp on absence prevents insert-delete ABA: a cell that was created
and deleted after a transaction observed never-existing absence is not
indistinguishable from the original absence.

Every point read made by a writing transaction becomes a prepare dependency,
whether or not the cell is also written. Prepare acquires the existing
canonical per-cell ownership set, then validates every expectation before any
mutation. This preserves full read validation, prevents lost updates, and
prevents point-read write skew.

A transaction that reads a historical revision because the current head is
newer than its snapshot will fail certification if it later attempts to
commit a write. This is intentional: it may read its snapshot, but it cannot
serialize as a writer after a certified dependency changed.

Read-only transactions use the same snapshot resolution and do not prepare.

## Conditional and Direct Cell Operations

Removing the version field does not remove functionality:

- compare-version-and-update becomes compare-revision-and-update,
- compare-version-and-set-field becomes compare-revision-and-set-field,
- update and remove defensive checks compare `revision_ts`,
- blind transactional update/remove first observes the current revision,
- insert certifies visible absence, and
- mismatch errors become `CellRevisionMismatch`.

Non-transactional operations remain available. They assign local revision
timestamps but do not coordinate isolation with transactions. The contract
explicitly provides no isolation or safety guarantee across transactional and
non-transactional modes.

## Physical Version Lifetime

### No transaction-private retention pins

The old `PinnedReadSet { location, version, SegmentReferenceGuard }` is
replaced. Transaction-private state does not own history and does not retain a
segment for the transaction lifetime.

The chunk-owned history chain is the long-lived liveness authority. Segment
references are only physical memory leases while bytes are actively decoded or
serialized.

### Historical materialization

A historical read:

1. resolves the desired `RevisionNode`,
2. loads its current physical location,
3. attempts a shared segment reference,
4. rechecks that the node is live and still points to that location,
5. retries if cleaner relocation won the race,
6. materializes the cell, and
7. releases the segment reference.

Current-head reads without tiered memory continue to rely on the existing cell
index lock. Under tiered memory, the existing shared segment protection
remains.

### Cleaner exclusivity

Some lifetime barrier is required before dereferencing a raw segment address.
The chosen barrier is the existing shared/exclusive segment reference state,
not a lock in every revision node.

The combine cleaner must change from "check `no_references`, then take an
ordinary reference" to a true exclusive segment acquisition before relocating
or freeing a source segment:

- if a reader acquired a shared reference first, cleaning skips/retries;
- if cleaner acquired exclusivity first, a reader fails acquisition and
  retries after relocation.

This is required for historical entries because they do not have the current
cell-index lock that currently closes the cleaner race.

## Cleaner Integration

The current combine cleaner deduplicates by cell hash and keeps only the
greatest numeric version. Under MVCC it must:

- treat the current cell-index target as live,
- treat every unexpired history node as live,
- identify retained entries by `(CellId, revision_ts)`,
- preserve current and historical tombstones needed by chains,
- choose duplicate physical entries by `revision_ts`,
- sort cleaner layout by revision recency and size,
- copy retained historical entries during compaction,
- CAS-update the corresponding `RevisionNode.state_and_location`, and
- update the cell index as well when relocating the current present head.

Cleaner versus GC:

1. cleaner copies a retained entry while holding source-segment exclusivity,
2. cleaner CASes the node from source location to destination location,
3. if GC already marked the node expired, cleaner's CAS fails and the
   destination copy is marked dead,
4. if cleaner wins, GC later expires the destination location, and
5. the source segment is freed only after all retained entries have either
   moved or expired.

Cleaner temperature ordering uses `revision_ts`, which is the logical
last-committed-write order. The broken cached `u32` cell wall clock is no longer
used for cell temperature.

## Retention and Garbage Collection

### Initial policy

History retention is time-based for the initial implementation:

- default: five minutes,
- configurable,
- measured with monotonic elapsed time,
- no initial memory cap, and
- memory use is therefore proportional to revisions created during the
  configured window.

A future timestamp-horizon policy may replace the expiration policy without
changing the chain representation.

### Per-chunk expiration

When a revision is superseded:

1. its monotonic retirement deadline is recorded,
2. an expiration record is pushed into a per-chunk lock-free queue, and
3. the new revision is published at the front of the cell's chain.

One background GC worker per chunk drains expired records and removes the
contiguous expired suffix with Lightning `pop_back`. GC never holds a history
list structural operation while acquiring cell or segment locks.

When pruning makes an older snapshot unresolvable, the chain advances
`truncated_before_ts` to the oldest remaining revision timestamp. Because
visibility uses strict `<`, a traversal that finds no visible node and has
`snapshot_ts <= truncated_before_ts` returns `SnapshotTooOld`. If traversal
finds a visible retained node, pruning only the oldest suffix cannot affect
that result. A directly visible current head does not fail merely because
unrelated history was pruned.

The logical current node, including a current tombstone, never expires merely
because its retention interval elapsed. Only superseded nodes are eligible for
expiration. This preserves revision-aware absence and prevents
`Absent(Some(delete_ts))` from degrading into `Absent(None)`.

### Restart

Historical chains are not recovered:

- recovery selects only the latest durable cell or tombstone,
- recovered current states receive one-node current chains as needed,
- old physical entries are classified as dead rather than linked,
- the participant establishes a recovery snapshot floor, and
- snapshots predating that floor return `SnapshotTooOld`.

This deliberately trades post-restart historical availability for startup and
steady-state performance.

## Distributed Commit Visibility

The existing ownership lifecycle is retained as the visibility barrier:

```text
prepare acquires owner
    -> first reads return Wait
commit installs pending revisions but retains owner
    -> first reads still return Wait
all participants acknowledge installation
coordinator sends end
    -> pending revisions become committed
    -> owners release and new revisions become readable
```

Rules:

- Incoming RPC clocks advance the local HLC but never change the fixed snapshot
  boundary. Existing `effective_ts = max(clock, tid)` behavior must not be used
  for MVCC visibility.
- A transaction that already selected an immutable historical revision may
  continue reading that revision while another transaction owns the cell.
- A first read that encounters an owner waits. Serving provably older snapshots
  during prepare/commit is a later optimization.
- The ownership check precedes the current-head fast path. A reader must not
  bypass an unresolved pending head through the cell index.
- The coordinator sends `end` only after every participant installed the shared
  commit timestamp.
- Lock timeout must not blindly clear an owner in `Prepared`, `Committing`, or
  `Committed`. It must invoke transaction resolution; age alone cannot make a
  partially installed distributed commit visible.
- A participant inserts the pending revision at the chain front and updates the
  current cell-index mirror as one owner-protected state transition. It
  acknowledges installation only after both structures agree.
- Commit resolution promotes the pending node before owner release. Abort
  resolution marks it aborted, installs any required compensating current
  revision, and only then releases ownership.

The failure test pauses one participant after another participant installs its
new revision. Readers must wait or see their already-selected old revision;
they must never observe a mixed transaction.

## Recovery and Compensation

### Recovery ordering

Recovery scans cells and tombstones and keeps the greatest `revision_ts` per
cell ID. Cleaner relocation may leave two physical copies of the same logical
revision after a crash. Equal `(CellId, revision_ts)` copies are therefore
valid only when their entry kind and logical contents are identical; recovery
may choose either copy deterministically. Equal timestamps with conflicting
cell/tombstone state or contents are corruption. Distinct logical revisions of
one cell must still have strictly increasing timestamps.

After scanning:

1. determine the maximum recovered revision timestamp,
2. make the server HLC observe that maximum,
3. use durable transaction/undo state to classify unresolved transactional
   installs before constructing visible current chains,
4. apply unresolved transaction recovery,
5. ensure any compensating writes receive still-greater timestamps, and
6. accept new traffic only after the clock and state are ready.

### Undo durability

Undo or intent information required to reverse a mutation must become durable
before the associated cell mutation can become durable. The existing
functionality is retained, but undo records use revision timestamps rather
than versions.

Rollback validates that the current revision still belongs to the transaction
being rolled back. It must not overwrite a later successful revision.

### Compensating revisions

If a transaction aborts before applying storage mutations, no compensation is
needed.

If a failed/partially applied transaction installed a physical revision, old
content is restored as a new compensating revision with a timestamp greater
than the failed revision. The old timestamp is not reused:

```text
100: value A       original committed revision
200: value B       failed transaction; never visible
300: value A       compensating current revision
```

Recovery therefore chooses `300: A`, not the failed value at `200`. At runtime
the failed revision may temporarily occupy a pending chain node while its owner
is held, but it never becomes a committed visible node. Abort marks that node
aborted before publishing compensation and releasing ownership.
Snapshots whose timestamps fall between the failed revision and compensation
resolve to the original historical revision. Compensation is idempotent by
expected revision comparison.

An aborted insert compensates with a newer tombstone. An aborted delete
compensates with a newer restored cell.

## Public and Internal API Changes

There is no compatibility layer.

- `CellHeader.version` is removed.
- `CellHeader.timestamp` becomes `revision_ts: u64`.
- Synthetic query metadata changes from
  `__header.ts: U32(timestamp)` to `__header.ts: U64(revision_ts)`.
- Synthetic `__header.ver` is removed.
- `CellVersionMismatch` becomes `CellRevisionMismatch`.
- `CellExpectation` carries revision timestamps and versioned absence.
- `PinnedRead`/`PinnedReadSet` raw locations and transaction-lifetime guards
  are removed.
- Full, selected, header, and partial read RPCs resolve the transaction snapshot
  or an explicitly observed revision rather than a private pinned address.
- The read-pin release RPC and read-only pin cleanup are removed if no remaining
  consumer needs them.
- Undo log entries and commit payloads carry revision timestamps.
- Test helpers and assertions compare revision identity or timestamp order;
  no test asserts numeric version increments.

New primary error:

```rust
SnapshotTooOld
```

It is returned when a requested point snapshot requires history below a
retention or recovery truncation boundary. It is never converted into current
data or ordinary absence.

## Index and Range Boundary

Index, range, predicate, and phantom semantics are explicitly excluded from
this increment. Their APIs remain functional with their current behavior, but
the point-cell MVCC isolation and safety guarantee does not apply to results
derived from them.

No index entry, range cursor, or predicate observation is added to the version
chain or writing transaction read set in this implementation.

## Performance Design

### Hot-path constraints

- A visible current-head point read does not traverse the history index.
- Transaction-private state stores a timestamp, not a segment pin.
- History list operations use Lightning's mostly lock-free ring-buffer list.
- Expiration enqueue is lock-free.
- Segment read leases and cleaner exclusivity are atomic reference-state
  operations, not mutexes.
- Cleaner and GC work remains outside normal read execution.
- The same 32-byte cell and tombstone sizes avoid per-entry storage expansion.

### Benchmark environment

Primary host: `192.168.10.87`.

Local runs are acceptable for focused tests and benchmarks that do not need
substantial RAM. Accept-grade distributed/storage results use identical
baseline and candidate builds, datasets, NUMA binding, and host conditions.

### Workloads

Measure develop baseline versus candidate for:

- non-transactional point read and update,
- read-only transactional current-head reads,
- one-cell and multi-cell read-modify-write,
- multi-participant distributed commit,
- blind update and remove,
- full, selected, header, and partial point reads,
- historical reads at several chain depths,
- one extremely hot cell with an old snapshot,
- history insertion and expiration,
- cleaner throughput with retained revisions,
- cleaner/reader relocation contention,
- tiered-memory current and historical reads, and
- HLC allocation contention.

Collect throughput, p50/p95/p99 latency, CPU, allocation rate, segment count,
retained bytes, and coefficient of variation.

Microbenchmarks compare:

- Lightning revision-list push/iterate/pop behavior,
- candidate ring-buffer capacities,
- shared segment reference acquire/release,
- exclusive cleaner acquisition under readers, and
- current-head fast path with MVCC enabled but unused.

### Acceptance

- Every correctness invariant and unexpected-outcome list must be clean.
- A reproducible regression greater than 5% on a non-historical hot path
  requires investigation and an alternate design or explicit approval.
- Historical lookup and cleaner costs are compared against
  correctness-equivalent alternatives, not against a system that discards
  history.
- If segment-reference synchronization is material overhead, evaluate an
  epoch/hazard alternative without weakening reclamation safety.
- No distributed transaction phase may be removed as an optimization.

## Testing Strategy

Tests are written before implementation behavior and must fail for the expected
reason.

### Header and revision assignment

- Cell and tombstone layouts remain 32 bytes.
- Caller-created cells are unstamped.
- Transaction commit stamps all participant cells with the same timestamp.
- Non-transactional mutations receive increasing local timestamps.
- Successive accepted mutations of one cell have strictly increasing
  timestamps.
- HLC is advanced beyond recovery maximum and never silently wraps.

### Snapshot visibility

- Current head before snapshot is visible.
- Current head equal to snapshot is invisible.
- Current head after snapshot resolves to the newest older revision.
- Repeated full, selected, header, and partial reads select one revision.
- Buffered insert/update/remove overlays the snapshot.
- Visible tombstone produces repeatable absence.
- Insert-delete ABA changes the absence expectation.
- Pruned required history returns `SnapshotTooOld`.
- Restart-floor snapshots return `SnapshotTooOld`.

### OCC safety

- Two transactions reading one counter cannot both commit derived updates.
- Full read validation prevents point-read write skew.
- A writer that read a historical revision fails if current state advanced.
- Blind update/remove compare the observed revision.
- Insert certifies exact absence.
- Read-only snapshots do not prepare.

### Version-chain concurrency

- Concurrent push-front, snapshot iteration, pop-back expiration, and cleaner
  relocation preserve chain order and liveness.
- Cleaner CAS and GC expiration resolve without resurrecting an expired node.
- A shared reader blocks/skips exclusive cleanup until materialization ends.
- A reader losing the cleaner race retries at the relocated address.
- No transaction-private raw address remains after the read.

### Recovery and rollback

- Recovery chooses the greater cell/tombstone timestamp.
- Equal-timestamp cleaner copies with identical logical contents are accepted.
- Equal per-cell timestamps with conflicting logical contents are rejected as
  corruption.
- Old physical versions are not reconstructed as history.
- Undo is durable before its mutation.
- Partial update rollback writes newer restored content.
- Partial insert rollback writes a newer tombstone.
- Partial delete rollback writes a newer restored cell.
- Repeated compensation is idempotent.

### Distributed visibility

- Delay one participant after another installs commit: new readers wait.
- A previously selected old revision remains readable during the delay.
- `end` releases owners only after all commit acknowledgements.
- Lock timeout cannot expose a partial prepared/committed transaction.
- Incoming RPC clock changes do not change a transaction's snapshot.

### Regression suites

At minimum:

```text
cargo check --lib
cargo test --lib server::transactions -- --test-threads=1
cargo test --lib ram:: -- --test-threads=1
cargo test --lib ram::tiered -- --test-threads=1
cargo test --lib index::full_text -- --test-threads=1
cargo test --lib index::ranged -- --test-threads=1
```

The exact command list may be split in the implementation plan to keep
feedback fast, but final acceptance covers transaction, recovery, cleaner,
tiered-memory, and existing index regression suites.

## Risks and Mitigations

### Linear historical traversal

A singly ordered history scan is linear in the revisions newer than the
snapshot. The current-head fast path avoids it normally. Benchmark the hot-cell
old-snapshot case; add sparse checkpoints only with evidence.

### Time retention does not bound memory

Five minutes of a very hot cell can retain many revisions. Measure retained
bytes and GC throughput. A timestamp horizon or memory cap is a later policy
change if required.

### Cleaner protocol expansion

The cleaner must preserve more than the current hash winner and update both
current and historical pointers. Use exclusive segment acquisition, focused
race tests, and benchmark-driven acceptance before landing.

### Mostly lock-free Lightning list

Lightning's `LinkedRingBufferList` uses small spin locks for rare structural
changes. Measure contention and candidate buffer sizes. Replace it only if
evidence shows material cost.

### Clock drift and remote skew

HLC ordering survives regression and logical carry, but an extreme future
remote clock can advance a node significantly. Checked arithmetic, monitoring,
and future-skew policy belong in the HLC implementation. Retention remains
monotonic-time based.

### Restart snapshot availability

Volatile history means an old transaction cannot assume its snapshot survives a
participant restart. The explicit `SnapshotTooOld` error is the contract;
returning a newer revision is forbidden.

## Acceptance Criteria

- `version` is absent from the cell header and all correctness decisions.
- Every persisted mutation has a nonzero `revision_ts`.
- Transactional participant writes share one commit timestamp.
- Current and historical point reads obey strict snapshot visibility.
- Every point read made by a writing transaction is certified.
- Lost updates and point-read write skew remain prevented.
- Absence is revision-aware and detects insert-delete ABA.
- Pending transactional revisions cannot satisfy snapshot reads and are
  resolved before ownership release.
- History is chunk-owned, mostly lock-free, cleaner-relocatable, and
  time-bounded.
- Transactions retain no raw historical addresses or lifetime segment pins.
- Recovery, tombstones, conditional mutations, undo, and compensation use
  revision timestamps.
- Distributed writes become visible only after all participants install them.
- Old storage, WAL, undo, RPC, and public version formats are intentionally
  unsupported.
- Point-cell functionality remains available with revision-based APIs.
- Index/range scope is explicit and unchanged.
- Correctness gates pass and benchmark regressions satisfy the acceptance
  policy on `192.168.10.87`; local is acceptable for low-RAM workloads.
