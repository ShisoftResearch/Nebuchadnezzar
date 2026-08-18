# Linearizable Slot Placement Refresh Design

## Problem

The ignored `a_transaction_reads_a_migrated_cell` test appears to show a
transaction permanently rejected with `ReadTooLate` after migration. The actual
failure is different:

1. `migrate_slot` commits the new owner and pushes that authoritative fact into
   the initiating client, donor, and recipient rings.
2. The test immediately calls `NebServer::refresh_slot_placement` on both
   members.
3. Refresh reads `Slots::all_slots` through a Raft query. A query may run on a
   replica that has committed but not yet applied the migration, so it can return
   the previous owner.
4. Refresh replaces the authoritative local table with that stale snapshot.
5. A transaction coordinator routes the migrated cell to the donor after its
   copy has been reclaimed and receives `CellDoesNotExisted`.
6. The test converts `None` into `NotRealizable(ReadTooLate)`, causing the client
   to retry up to 1,000 times and hiding the missing-cell result.

This was captured in a real failure: the participant emitted no `ReadTooLate`
warning, while the client retried the same missing cell. Skipping only the
explicit refresh made four concurrent reproductions pass.

## Goals

- A successfully loaded placement snapshot must include every placement command
  ordered before it.
- Refresh must never replace a newer authoritative owner with a lagging replica's
  state.
- One explicit server refresh should perform one consensus operation regardless
  of the number of hosted databases.
- A missing migrated cell in the regression test must fail immediately and be
  reported accurately.
- Normal cell reads, writes, transactions, and per-slot migrations must not add
  consensus traffic.

## Non-goals

- This change does not add periodic placement polling.
- It does not introduce per-slot epochs or change the serialized `SlotState`
  format.
- It does not replace Bifrost Raft with a ReadIndex implementation.
- It does not address the separate Morpheus migrated id-list failure.

## Chosen approach

Add a non-mutating Raft command, `all_slots_consistent`, to Bifrost's slot state
machine. It returns the same snapshot as the existing `all_slots` query, but the
command is ordered through the Raft log and applied by the leader. Its input log
entry contains only the group id; the full slot table is returned in the command
response rather than stored in the log.

Neb's placement loaders will use the command whenever they install a complete
routing table. The existing `all_slots` query remains available for diagnostic
or explicitly stale-tolerant reads.

This is preferred over per-slot generations because it fixes the consistency
boundary without changing stored placement data or every ownership RPC. It is
preferred over broadcasting to all members because broadcast alone cannot make
an offline or rejoining member's later pull safe.

## Components and data flow

### Bifrost slot state machine

The slot state-machine service gains:

```text
cmd all_slots_consistent(group) -> optional slot map
```

Its implementation clones the same group map as `all_slots` and performs no
mutation. Command ordering provides the guarantee: after the command completes,
its returned map reflects all preceding committed placement commands.

### Neb placement loading

`slots::load_table` will call `all_slots_consistent`; `load_owner_vec` continues
to flatten that map into the 32,768-entry owner vector. Existing server and
client initialization therefore also receive a current snapshot rather than a
possibly stale one.

`AsyncClient::reload_slot_owners` remains an explicit operation. There is no
timer and no placement command on the request path. `migrate_slot` continues to
use the cheaper single-slot `note_slot_owner` push after its own commit.

### Server refresh fan-out

`NebServer::refresh_slot_placement` will load one consistent owner vector. It
will install that vector into the server ring and clone it into each hosted
database client's ring. It will not call `AsyncClient::reload_slot_owners` for
each database, which currently performs one independent table read per runtime.

The resulting cost is exactly one small Raft command per explicit server
refresh. Initialization still loads once per independently constructed ring.

## Error handling

- If the consistent command fails, the caller keeps its existing placement
  table and returns/reports failure exactly as it does today.
- An error must never clear the table and fall back to jump hashing.
- `Ok(None)` continues to mean the group has never adopted a slot table; this is
  distinct from a transport or consensus error.
- The migration transaction test will use `expect` for a cell that must exist.
  It will no longer translate absence into a retryable timestamp conflict.

## Tests

Development follows red-green-refactor:

1. Correct and unignore `a_transaction_reads_a_migrated_cell` while the old
   query implementation is still present. Run concurrent copies until the stale
   refresh produces the expected immediate missing-cell failure, establishing
   the red regression.
2. Add Bifrost state-machine coverage showing `all_slots_consistent` returns the
   current stable and migrating states without changing them.
3. Implement the consistent loader and single-snapshot server fan-out.
4. Run the migrated-cell transaction test repeatedly and concurrently; its
   unmigrated control must also pass.
5. Run the Bifrost slot-state tests, Neb migration tests, the Neb library suite,
   and `cargo check --all-targets`.

The formerly ignored test keeps its post-migration refresh. That is intentional:
it proves the operation that previously regressed routing is now safe.

## Compatibility and rollout

Neb uses Bifrost as a path dependency, so the two repositories must be updated
together. The new generated state-machine method requires upgraded Bifrost slot
services before upgraded Neb processes invoke it; this work assumes a coordinated
upgrade rather than mixed-version rolling compatibility.

No placement snapshots or cell data change format. Rollback consists of reverting
the new command call and restoring query-based loading, though doing so restores
the stale-refresh correctness risk.
