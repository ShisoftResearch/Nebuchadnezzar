# Non-Transactional / MVCC Cost Isolation

## Status

Approved architectural correction for point-cell MVCC.

Pure non-transactional point-cell workloads must retain the direct storage
path. They must not allocate revision-chain nodes, resolve the history map,
enqueue retention records, or wake history workers. MVCC costs belong to
transactional operations.

The caller chooses transactional or non-transactional APIs. Those APIs do not
observe or coordinate with one another. Mixing them for the same logical cell
has no isolation, safety, lost-update, repeatable-read, snapshot, or recovery
guarantee.

## Motivation

The first MVCC implementation installed every direct mutation into a revision
chain. A non-transactional update therefore paid for:

- history-map resolution and chain creation,
- `RevisionNode` allocation and lock-free list publication,
- predecessor retention and expiration enqueue,
- history-worker notification, and
- later expiration and dead-space accounting.

That produced a roughly 61% throughput regression in the matched
`non_transactional_update` workload even though the workload never used a
transactional snapshot. This violates the API boundary: a caller that chooses
the direct API must not subsidize an unused transaction guarantee.

## State Boundaries

The cell index has one representation:

- a raw aligned address means a present current cell, and
- no entry means absence.

The cell index contains no MVCC ownership tag and never points to a tombstone.
Every direct operation determines presence, absence, and its mutation target
from this index alone.

Transactions separately own logical revision order and retained physical
versions through the chunk history index. A transaction-owned present current
revision mirrors the same raw address in the cell index. A transaction-owned
deleted current revision exists only in history. Direct operations neither ask
whether a matching history node exists nor change behavior because of it.

Direct tombstones are legacy durable storage records. They are not indexed,
are not revision-chain nodes, and are not consulted by direct operations.

## Direct Mutation Path

A direct insert:

1. locks or reserves the cell-index key,
2. assigns a local HLC revision timestamp,
3. writes and durably finishes the physical entry,
4. publishes the raw cell address,
5. updates secondary indexes and statistics, and
6. creates no history state.

A direct update:

1. locks the current raw cell-index address,
2. validates its identity and reads its revision timestamp,
3. assigns a strictly greater local HLC revision timestamp,
4. writes and durably finishes the replacement,
5. publishes the raw replacement address,
6. updates secondary indexes and statistics, and
7. dead-accounts the replaced physical entry exactly once.

A direct delete:

1. locks the current raw cell-index address,
2. preflights all fallible index and storage work,
3. writes and durably finishes a timestamped tombstone,
4. removes secondary indexes and removes the cell-index entry,
5. dead-accounts the deleted cell exactly once, and
6. creates no history state.

All direct entry points use these paths, including update-by, remove-by,
upsert, compare-revision mutations, and `CellGuard` mutations. They never
resolve history, create a chain, allocate a revision node, enqueue retention,
or wake the history worker.

## Transactional Conversion

The first transactional access to a raw present head pays the conversion cost
while holding the existing cell-index guard:

1. decode and validate the current cell,
2. create a singleton committed predecessor node if no matching chain exists,
3. publish or validate the revision chain, and
4. continue snapshot resolution or assigned-revision mutation.

The raw cell-index address does not change during conversion. Direct absence
has no corresponding history revision because transactions do not observe
direct tombstones.

All assigned-revision writes publish a raw present cell-index address.
Transactional deletion removes the present mirror only after the tombstone
revision is installed in the chain, as before.

For transaction-only use, the transactional MVCC/OCC invariants are unchanged:
snapshot reads resolve retained revisions, writing transactions validate every
point read, pending revisions remain invisible, exact predecessors are
certified, and distributed commit phases are preserved.

## Recovery and Cleaner

Recovery still selects the physical entry with the greatest `revision_ts`.
It publishes a recovered present cell as a raw cell-index address. A winning
tombstone leaves the cell index absent. Recovery advances the HLC beyond every
recovered timestamp and reconstructs no historical chain.

Cleaner liveness deliberately combines two independent mechanisms:

- a cell is live if its address is the current cell-index target or an
  unexpired MVCC history node;
- an MVCC tombstone is live while its revision node is live; and
- a legacy direct tombstone is live while any resident segment has
  `seq_id <= tombstone.segment_seq_id`, where `segment_seq_id` records the
  predecessor cell's segment sequence.

The sequence watermark strengthens the old immediate-predecessor rule. Every
older physical version of the deleted direct cell must have been written in a
segment no newer than its immediate predecessor. The cleaner therefore keeps
the tombstone until all possible older versions have disappeared, preventing
restart resurrection without adding any direct-operation lookup.

During relocation:

- a current direct cell updates only the raw cell-index address;
- an MVCC historical cell or tombstone updates only its revision node;
- an MVCC current-present cell updates its revision node and raw cell-index
  mirror as one reconciled operation; and
- a legacy direct tombstone is copied according to the sequence watermark and
  requires no pointer publication.

Source-segment exclusivity and revision-node relocation checks remain unchanged
for MVCC history, so cleaner compaction preserves transaction snapshots and
pending/committed visibility.

## Contract for Mixed APIs

Transactional and direct operations do not observe or certify one another.
For the same cell, mixing the APIs may produce transaction aborts,
`SnapshotTooOld`, ordinary absence, or last-published direct state. It must not
be presented as snapshot isolation, serializability, repeatable read, or
lost-update prevention.

## Acceptance

Correctness tests must prove:

- every pure direct mutation entry point creates zero chains, zero revision
  nodes, zero expiration records, and zero history-worker wakes;
- direct revision timestamps remain strictly increasing;
- direct update and delete dead-account each replaced cell exactly once;
- direct delete removes its cell-index entry and creates no indexed or
  history-owned tombstone;
- direct delete survives multi-segment cleaner execution and restart without
  resurrection after its immediate predecessor segment is removed first;
- the first transactional access converts a direct present head and retains it
  as the correct predecessor without changing the raw cell-index word;
- assigned-revision, pending visibility, promotion, abort, compensation,
  MVCC cell/tombstone cleaner relocation, and recovery tests remain clean.

On `192.168.10.87`, matched develop/non-MVCC-OCC and candidate populations must
show no reproducible throughput or p99 regression for pure non-transactional
point reads, writes, updates, upserts, conditional updates, removes, and
delete/recreate workloads. Both populations must have coefficient of variation
below 5%. A candidate that regresses a pure non-transactional workload is
rejected; transactional MVCC performance cannot be purchased with direct-path
overhead.
