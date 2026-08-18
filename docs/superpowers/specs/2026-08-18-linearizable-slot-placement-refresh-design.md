# Linearizable Slot Placement Refresh Design

## Problem

The formerly ignored `a_transaction_reads_a_migrated_cell` test appeared to
show a transaction permanently rejected with `ReadTooLate` after migration.
The actual failure is different:

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
  state, including when a migration commits after the refresh command applies
  but before its caller installs the returned vector.
- One explicit server refresh should perform one consensus operation regardless
  of the number of hosted databases.
- A missing migrated cell in the regression test must fail immediately and be
  reported accurately.
- Normal cell reads, writes, transactions, and per-slot migrations must not add
  consensus traffic.

## Non-goals

- This change does not add periodic placement polling.
- It does not add epochs to the serialized `SlotState` format. Applied Raft log
  indices exist only in the in-memory routing cache and notification messages.
- It does not replace Bifrost Raft with a ReadIndex implementation.
- It does not address the separate deterministic Morpheus ranged-index/sidecar
  migration failure.

## Chosen approach

Add a non-mutating Raft command, `all_slots_consistent`, to Bifrost's slot state
machine. It returns the same snapshot as the existing `all_slots` query, but the
command is ordered through the Raft log and applied by the leader. The Raft
client returns both the command result and that command's applied log index. Its
input log entry contains only the group id; the full slot table is returned in
the command response rather than stored in the log.

Neb's placement loaders use the command whenever they install a complete routing
table. Each local cache stores an applied index per slot and refuses an update
older than the knowledge it already has. This second rule is necessary because
linearizing the read does not make installation atomic with the command: a
migration may commit and push owner N+1 after the read command applies at N but
before its caller installs the N snapshot. The existing `all_slots` query
remains available for diagnostic or explicitly stale-tolerant reads.

Applied log indices are preferable to a second stored placement epoch because
Raft already assigns the ordering needed by both snapshots and migration
commands. They do change ownership notifications and refusals so every cache
update can be compared, but do not change replicated placement data. Broadcast
alone is insufficient because it cannot make an offline or rejoining member's
later pull safe.

## Components and data flow

### Bifrost slot state machine

The slot state-machine service gains:

```text
cmd all_slots_consistent(group) -> optional slot map, plus applied log index
```

Its implementation clones the same group map as `all_slots` and performs no
mutation. Command ordering provides the guarantee: after the command completes,
its returned map reflects all preceding committed placement commands.

Bifrost's existing `ClientCmdResponse::Success.last_log_id` is the id allocated
to this command. The generic state-machine client exposes an indexed command
execution method while its existing `execute` API discards the index and remains
source-compatible.

### Versioned local cache

The slot override cache stores owners, a Raft log index per slot, and the newest
complete-snapshot index. Installation obeys these rules:

1. Reject a complete snapshot below the cache's snapshot watermark.
2. For each slot, install the snapshot's owner only when that slot's cached
   index is not greater than the snapshot index.
3. Apply a direct owner notification only when its index is not below either
   the complete-snapshot watermark or that slot's cached index.

The per-slot rule matters. A single global watermark would either let an old
snapshot overwrite a newer push or force the entire snapshot to be discarded,
preventing unrelated older slots from catching up.

### Neb placement loading

`slots::load_table` calls `all_slots_consistent`; `load_owner_vec` continues to
flatten that map into the 32,768-entry owner vector and returns the applied log
index alongside it. Existing server and client initialization therefore also
receive a current, versioned snapshot rather than a possibly stale one.

`AsyncClient::reload_slot_owners` remains an explicit operation. There is no
timer and no placement command on the request path. `migrate_slot` uses the
cheaper single-slot `note_slot_owner` push after its own commit and carries the
completion command's applied index. Bulk completion carries one index for the
whole batch. A `NotSlotOwner` refusal likewise includes the cached owner's index
so redirect learning cannot roll placement backward. After merging a refusal,
the retry resolves its target from the monotonic cache rather than blindly using
the reply's owner; a late refusal therefore cannot misroute the in-flight retry.

### Server refresh fan-out

`NebServer::refresh_slot_placement` loads one consistent owner vector and index.
It installs that snapshot into the server ring and clones it into each hosted
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
- The migration transaction test uses `expect` for a cell that must exist. It no
  longer translates absence into a retryable timestamp conflict.

## Tests

Development follows red-green-refactor:

1. Correct `a_transaction_reads_a_migrated_cell` while the old query
   implementation is still present. Run concurrent copies until the stale
   refresh produces the expected immediate missing-cell failure, establishing
   the red regression, and land that correction separately.
2. Add Bifrost state-machine coverage showing `all_slots_consistent` returns the
   current stable and migrating states without changing them.
3. Add cache tests for an older snapshot, a snapshot racing a newer per-slot
   push, unrelated-slot catch-up, and an older push. Add an integration test in
   which a stale refusal arrives after the client has learned a newer owner and
   prove the retry uses that newer cached target.
4. Implement the indexed loader, notifications, redirects, and single-snapshot
   server fan-out.
5. Run the migrated-cell transaction test repeatedly and concurrently; its
   unmigrated control must also pass.
6. Run the Bifrost slot-state tests, Neb migration tests, the Neb library suite,
   and `cargo check --all-targets`.

The formerly ignored test keeps its post-migration refresh. That is intentional:
it proves the operation that previously regressed routing is now safe.

## Compatibility and rollout

Neb uses Bifrost as a path dependency, so the two repositories must be updated
together. The new generated state-machine method requires upgraded Bifrost slot
services before upgraded Neb processes invoke it; this work assumes a coordinated
upgrade rather than mixed-version rolling compatibility.

No placement snapshots or cell data change format. The owner-notification RPCs
and `WriteError::NotSlotOwner` wire shape gain an applied index, so Bifrost and
Neb members must move together; mixed-version operation is unsupported.
Rollback consists of reverting the new command call and restoring query-based
loading, though doing so restores the stale-refresh correctness risk.

## Safety boundary

This design fixes stale placement refresh only. It does **not** fix the separate,
deterministic ranged-index/sidecar migration failure (currently 0/12), which is
the blocker for running migration against a real store. A green transaction
placement regression must not be interpreted as evidence that real-store
migration is safe.
