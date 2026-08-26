# Schema UID/VID Evolution Design (Neb)

**Upstream design:** `../Morpheus/agents_docs/TODO_SCHEMA_UID_VID_EVOLUTION_DESIGN.md`

That document is the cross-project statement of intent. This one is the Neb
half, corrected against the code that has to carry it. Where the two differ,
this document is normative for Neb and the differences are called out
explicitly under "Corrections to the upstream design".

**Compatibility stance (decided 2026-08-25):** this campaign does **not**
preserve backward compatibility. Existing stores are not readable afterwards
and must be rebuilt. See "The format break" for exactly what stops working and
what that buys.

## Problem

One `u32` carries two unrelated responsibilities today:

- it names a logical schema family (the thing a graph edge, a query, or an
  index namespace means when it says "person")
- it names one exact physical field layout (the thing `read_by_schema` needs
  to decode a run of bytes)

`Schema { id: u32, name: String, .. }` (`src/ram/schema/mod.rs:34`) is stored in
a Raft state machine keyed by that id, with a second map from name to the same
id (`src/ram/schema/sm.rs:22`). `SchemasMap::new_schema` refuses a name that
already exists, and there is no update or rename command at all.

Three consequences:

1. **A schema cannot be renamed.** The name is a primary key with cells
   hanging off it.
2. **A schema cannot change shape.** Every existing cell decodes through the
   record found at its header's id (`src/ram/cell.rs:533`, `cell.rs:987`).
   Mutating that record in place reinterprets every already-written byte under
   a layout that did not produce it.
3. **Durable references cannot outlive a layout change.** Morpheus vertex id
   lists, every index namespace, and the statistics table all key by this id,
   so any new layout would orphan them.

## Goals

- A schema's logical identity survives both rename and shape change.
- Cells written under an older layout stay readable, byte-for-byte untouched,
  for as long as they exist.
- New writes always land in the newest layout, whatever id the caller passed.
- Stale cells drain lazily. No stop-the-world migration job is required for
  correctness.
- Query and index paths stay generation-agnostic: a scan over a schema does
  not fan out across its generations.
- **The compiler, not a test, proves that every site that touches a schema id
  was classified as logical or physical.** With 811 such sites, review alone
  is not a credible audit.

## Non-goals

- No arbitrary user-supplied transform code in this campaign.
- No aggressive deletion of superseded schema records. They are retained
  indefinitely; reclaiming them is a later, separately designed operation.
- No change to the Morpheus graph layer here. That is tracked upstream as its
  Phase 2 and depends only on this document's `uid` being available.
- No migration path for existing stores. Rebuild them.

## What the code already gets right

The read half is already the shape the design wants. `CellHeader.schema`
(`src/ram/cell.rs:35`) is consulted on every single decode --
`chunk.meta.schemas.get(&header.schema)` in `cell.rs:533` for a full read and
`cell.rs:987` for a projected one. Cells are already decoded by the exact
record their header names, and no read path rewrites bytes. Nothing in the
read path needs to change to support multiple live generations; it only needs
the map it consults to hold more than one record per logical schema.

## The identity model

Two distinct types, one allocator.

**`SchemaVid`** -- physical generation. Immutable. Names one exact field
layout. Stored in the cell header. This is what `Schema.id` means today.

**`SchemaUid`** -- logical family. Immutable. Names the schema a durable
reference means.

```rust
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug,
         Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaUid(pub u32);

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug,
         Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaVid(pub u32);
```

`Schema.id` is **renamed to `Schema.vid: SchemaVid`**, and `Schema.uid:
SchemaUid` is added. This is the single most valuable consequence of dropping
backward compatibility. Under a compatible design the two would have been
numerically interchangeable, `Schema.id` would have stayed a bare `u32`, and
every one of the 811 sites that touches a schema id would have been classified
by hand and verified by golden bytes -- a check that passes for the wrong
reason the moment someone adds an 812th site. As distinct types, the classification
is enforced at every site forever, and adding a new site forces a decision
rather than inheriting a default.

Both are `#[serde(transparent)]`, so they serialize as bare `u32`, and both are
`Eq`, which is all Lightning's `PtrHashMap` needs -- its `SwiftKeyProbe` bound
is satisfied by the blanket `impl<K: Eq>` (`Lightning/src/map/ptr_map.rs:2210`),
so they drop into the existing local-cache maps unchanged.

### Both `u32`, not a `u64` uid

The upstream design specifies a `u64` uid. Neb keeps both at `u32`:

- `EntryKey` spends 4 bytes of a fixed-width key on the schema prefix
  (`src/index/entry.rs:27-30`). A `u64` uid grows every ranged index key by 4
  bytes, against a codebase with a standing memory-per-cell campaign.
- `CellHeader.schema` is 4 bytes in a header that is deliberately small.
- 4 billion schema families is not a constraint anyone will meet.

### One allocator, for uniqueness rather than compatibility

Both are drawn from the same `next_id` Raft counter. A new schema draws one
number `n` and gets `uid = SchemaUid(n)`, `vid = SchemaVid(n)`. Evolving it
draws `m` and adds a record with `uid = SchemaUid(n)`, `vid = SchemaVid(m)`.

The reason is no longer compatibility -- it is that **a number is never both a
live uid and an unrelated schema's vid**. If the two were drawn from separate
counters, uid 7 and vid 7 would name different schemas, and a confusion that
escaped into durable bytes would silently resolve to the wrong record. Sharing
the counter makes such a confusion a lookup miss instead. The types prevent it
in code; the shared counter prevents it in data, where there is no compiler.

There is one benign coincidence: for a generation-0 schema, `uid` and `vid`
hold the same number, and a durable reference to `SchemaUid(n)` used as a vid
would find the generation-0 record. That record is stale after the first
evolution, so it redirects -- which is exactly the intended semantics.

Internal schemas keep their hash-derived fixed ids (`RANGED_TREE_SCHEMA_ID`,
`PAGE_SCHEMA_ID`, the Morpheus sidecar range at
`src/query/statistics/mod.rs:55`), registered per-node outside Raft. They take
`uid` and `vid` from the same hash and never evolve.

### Version status

```rust
pub enum SchemaVersionStatus {
    Current,
    Stale { superseded_by: SchemaVid },
}
```

`Schema` gains `uid`, `generation: u32`, and `status`. All are **required**
fields -- no `#[serde(default)]`, no normalization on ingress. A record that
does not carry them is not a record this build understands, and should be
rejected rather than quietly repaired.

### No redirect table

The upstream design calls for a `stale_vid -> current_vid` table that "must be
total for every stale schema version". Neb does not need one. Resolution is
`vid -> uid -> current_vid`, using the record's own `uid` and the handle's
`current_vid`. That is total by construction and, unlike a chain of
`superseded_by` hops, stays O(1) after a schema has been evolved twice. The
`superseded_by` field is retained for diagnostics and for reasoning about
generation order, not as the resolution mechanism.

### Lookup tables

In `SchemasMap` (Raft SM, `src/ram/schema/sm.rs:22`):

- `schema_map: HashMap<SchemaVid, Schema>`
- `name_map: HashMap<String, SchemaUid>`
- `handles: HashMap<SchemaUid, SchemaHandle>` -- new. Holds
  `{ uid, current_name, current_vid, generation }`.

In `LocalSchemasMap` (per-node cache, `src/ram/schema/mod.rs:599`) the same
three, as `LFHashMap`s.

The local cache's existing name-collision guard (`src/ram/schema/mod.rs:812`)
refuses to rebind a name that maps to a different id. Under the new model the
name binds to the *uid*, and a uid never changes for a given name, so the guard
stays correct without modification -- evolution rebinds
`handles[uid].current_vid`, which the guard does not police.

## The format break

Making `uid`, `generation`, and `status` required changes the `Schema` record
format. Bifrost serializes the SM snapshot with CBOR in release and JSON in
debug; both will fail to deserialize a record missing required fields.

**This walks every pre-existing store into a live latent bug.**
`SchemasSM::recover` (`src/ram/schema/sm.rs:141-150`) matches the deserialize
result and, on `None`, logs at `trace!` and returns -- leaving the state
machine with an **empty schema map**. The database then comes up looking like
one that has no schemas rather than one this build cannot read: every cell
fails `SchemaDoesNotExisted`, and in debug builds `select_from_chunk_raw`
(`src/ram/cell.rs:1004`) panics on the way. That is the same shape as the
recovery bug that silently wiped the ranged index -- an unreadable input
treated as an empty one.

This path was unreachable while the record format only ever grew defaulted
fields. Deliberately breaking the format makes it the *expected* path for every
old store, so it must be fixed as part of the break, not after it:

- a failed snapshot deserialize is an `error!`, not a `trace!`
- `recover` refuses to complete rather than installing an empty map, so the
  database fails to load loudly instead of loading wrong
- the message names the likely cause -- a store written by a build from before
  the uid/vid split -- and says to rebuild

What specifically stops working:

- **Schema SM snapshots.** Records lack the required fields.
- **Every existing store, transitively.** With no schema records, no cell can
  be decoded, whatever its bytes say.
- **Recorded TB-scale stores** must be re-imported. The Wikidata-class runs are
  hours of work; that cost is the price of this decision and is accepted.

What does *not* change format:

- `CellHeader` layout. `schema` stays a 4-byte field at the same offset; it is
  hand-encoded with `write_u32` (`src/ram/cell.rs:282`), so a
  `#[serde(transparent)]` newtype changes the Rust type and not one byte.
- `EntryKey` layout. Its 4-byte schema prefix keeps its width and position; its
  *meaning* changes from vid to uid, which is invisible in a rebuilt store.
- The entry, segment, and WAL formats are untouched.

## Write resolution, and the single chokepoint

Every write must land in the current generation regardless of which id the
caller had in hand. There is exactly one place to enforce that.

`OwnedCell::plan_write` (`src/ram/cell.rs:201`) already resolves
`self.header.schema` against the local cache and already carries the resolved
schema into `WritePlan` (`WritePlan::new(.., schema, ..)`). Two changes make
redirection universal:

1. `plan_write` resolves through a new
   `LocalSchemasCache::resolve_for_write(vid) -> Option<SchemaRef>`, which
   returns the record if it is `Current` and otherwise returns
   `handles[record.uid].current_vid`'s record. Return `None` if either lookup
   misses, so `plan_write` keeps returning `WriteError::SchemaDoesNotExisted`.
2. `write_to_addr` (`cell.rs:282`) writes `write_plan.schema.vid` into the
   header instead of `header.schema`, and the `CellHeader` handed back to the
   caller carries the resolved vid.

Everything upstream -- `write_cell`, `update_cell`, `upsert_cell`, the
transaction data site, migration's cell replay -- goes through `plan_write`,
so none of them need to know evolution exists.

**Consequence the upstream design does not state:** an ordinary update to a
stale cell is itself a migration. `update_cell_by` decodes under the old vid,
mutates the `OwnedValue`, and re-encodes -- and after this change it re-encodes
under the *new* vid. The transform is therefore required on the write path,
not only in the cleaner.

## Which evolutions need a transform, and which do not

`plan_write_field` (`src/ram/io/writer.rs:59`) rejects exactly one shape:
a non-nullable field with no value (`writer.rs:191`,
`WriteError::DataMismatchSchema`). Missing map keys decode to `Null`, and the
encoder reads only the fields the target schema declares. That gives a free
tier:

**Identity-transform evolutions -- work today, with no transform engine:**

- add a nullable field (encodes as null in the new layout)
- drop a field (the new layout simply does not read it)
- index-only changes: adding or removing a `Ranged`/`Hashed`/`Vector`/
  `Fulltext`/`Embedding`/`Statistics` index on an existing field
- `is_scannable` and `blobs` flag changes

*Caveat:* on an `is_dynamic` schema, a dropped field is not dropped. It falls
through to `plan_write_dynamic_fields` and is re-encoded in the dynamic region.
Dropping a field from a dynamic schema must therefore be classified as
transform-requiring, not identity.

**Transform-requiring evolutions -- deferred to a later increment:**

- add a non-nullable field with a default (the default must be injected into
  the decoded map before encoding)
- rename a field (its `name_id` changes, so the value must be re-keyed)
- numeric widening (`U32` -> `U64` is a `DataMismatchSchema` as written)

A proposed evolution is classified at admission time. Anything outside the
identity tier is refused until the transform mechanism exists.

## Index namespaces key by uid, not vid

This is the largest correction to the upstream design, which does not say
which of the two indexes should use.

Every index subsystem currently namespaces by schema id:

- `EntryKey::from_props` writes it as the leading 4 bytes of the durable
  ranged-index key (`src/index/entry.rs:27-30`); scans use
  `EntryKey::for_schema(id)` as their range prefix
  (`src/index/ranged/tree/tree.rs:1342`, `src/query/data_client/read.rs:56`)
- hash buckets are `Id::from_obj(&(schema, field, feat))`
  (`src/index/hash/mod.rs:426`)
- full-text, vector, and embedding indexes take `schema_id` for namespace
  isolation
- `Statistics::schemas` is keyed by it (`src/query/statistics/mod.rs:45`)

All of these are logical namespaces and must key by **uid**. If they keyed by
vid, then migrating one cell would have to delete and reinsert every index
entry it owns, and every scan would have to fan out across all generations of
its schema and merge -- which is exactly the query-layer fanout the upstream
design lists as a non-goal. Keyed by uid, migrating a cell touches its bytes
and nothing else, and `EntryKey::for_schema(uid)` keeps covering the whole
family.

Cell identity keys by uid for the same reason. `OwnedCell::default_id` derives
a keyed cell's `Id` from `Id::from_obj(&(schema_id, key_value))`
(`src/ram/cell.rs:171-191`). The upstream design lists "does logical cell
identity embed the physical generation?" as an open question; the answer is
yes, it does, and it must become the uid or a cell's identity would change
under it on the first evolution.

Index *maintenance* also keys by uid: `ensure_indices` and
`probe_cell_indices` (`src/index/builder.rs:601`) take the schema the cell was
written under, and must emit keys under `schema.uid`. Note that two
generations of a schema may declare different index sets. The rule is that
index membership follows the *current* generation, so a cell migrated from a
generation that indexed a field into one that does not must have that field's
entries removed as part of the migration.

`post_schema_add` / `post_schema_delete` (`src/ram/schema/mod.rs:872`, `:945`)
create and destroy vector and embedding indexes by schema id. These become uid
too, and evolution must diff the two generations' index sets rather than
blindly creating everything again.

Because `SchemaUid` and `SchemaVid` are distinct types, this switchover is not
a judgement call repeated 811 times -- it is a compile error at every site
until someone states which one they meant. That is the verification, and it
does not decay.

## Cleaner-driven lazy migration

The upstream design describes this as piggybacking on an existing rewrite
boundary. In this codebase it is closer to a new path than a hook.

`CombinedCleaner` (`src/ram/cleaner/combine.rs:284-460`) is a pure byte
relocation. It plans destination segment layout from the *existing* entry
sizes (`plan_segment_layout`, `:219`), `libc::memcpy`s each entry verbatim
(`:327`), and then swaps `chunk.cell_index` from the old address to the new one
under a per-cell lock (`:411`). It never decodes a cell, and it never changes
an entry's size.

Migration breaks all three assumptions: the re-encoded entry has a different
size, so the pre-computed layout is invalid; it needs the cell decoded and
re-encoded, which the combine threads have no path to today; and the rewritten
cell is a new version, which means a version bump, index-entry maintenance,
and a statistics refresh -- none of which the relocation path performs.

The design therefore separates migration from relocation:

- during candidate collection, an entry whose header names a stale vid is
  marked as *needing migration* rather than *relocatable*
- migrating entries are decoded, re-encoded under the current vid, and sized
  **before** `plan_segment_layout` runs, so the planner sees the post-migration
  size
- a cell that fails to migrate (transform refuses, schema record missing) is
  relocated verbatim and left stale. Migration is opportunistic; failing it
  must never lose or corrupt a cell.
- the index-swap step gains the index-entry and statistics maintenance that a
  version-bumping write performs

This is the largest single piece of work in the campaign and is deliberately
sequenced last, after evolution is already correct without it. Until it lands,
stale cells drain only through ordinary updates -- which is correct, just
slower.

## Retention

A superseded schema record must outlive every cell that names it. There is no
cheap oracle for that, and the upstream design's non-goals already rule out
aggressive reclamation.

For this campaign: stale records are never deleted automatically. `del_schema`
by name deletes the handle and **all** generations under its uid, which is
correct because deleting a schema is already understood to abandon its cells.
Reclaiming an individual superseded generation needs a cluster-wide proof that
no cell references it; the existing `src/index/scrub.rs` cluster scan is the
natural place to build that later, and it is out of scope here.

## Corrections to the upstream design

1. **Cell identity does embed the physical id.** Listed upstream as an open
   question. It does, via `encode_cell_key`, and it must become the uid.
2. **Index namespaces key by uid.** Upstream is silent; keying by vid would
   force per-generation query fanout, contradicting its own stated goal.
3. **No redirect table.** `vid -> uid -> current_vid` is total and chain-free;
   a separate table would need maintaining and could go partial.
4. **The transform is needed on the write path too.** An ordinary update to a
   stale cell re-encodes it under the current generation. Upstream places the
   transform only in the cleaner.
5. **The cleaner is not a hook.** It is a byte relocator with a size-committed
   layout plan; migration is a new path through it.
6. **`uid` is `u32`, not `u64`,** and both are newtypes rather than bare
   integers. `u64` would widen every `EntryKey` for no reachable benefit; the
   newtypes are what make an 811-site reclassification auditable at all.

## Sequencing

1. **Identity types.** Introduce `SchemaUid`/`SchemaVid`, rename `Schema.id`
   to `vid`, add `uid`/`generation`/`status` as required fields, and build the
   handle maps in the SM and the local cache. Every site that touches a schema
   id is reclassified here, because nothing compiles until it is.
2. **Fail loudly on an unreadable snapshot.** The format break makes
   `SchemasSM::recover`'s silent-empty path the expected path for old stores.
3. **Rename.** A `rename_schema` Raft command rebinding the handle's name and
   the name map. Touches no cell and no index -- the cheapest end-to-end proof
   that the split works.
4. **Write resolution.** `resolve_for_write` plus the `write_to_addr` header
   source change.
5. **Evolution, identity tier.** An `evolve_schema` command that allocates a
   new vid under the same uid, marks the old one stale, and refuses anything
   outside the identity-transform tier. Evolution now works end-to-end, with
   stale cells draining through ordinary updates.
6. **Cleaner migration.** Decode/transform/encode inside combine, with the
   size-aware layout plan and full index maintenance.
7. **Transform engine.** Defaults, field renames, numeric widening.

Morpheus's Phase 2 -- moving durable graph references from physical id to uid
-- depends only on step 1 publishing a `uid`, and can proceed in parallel from
that point.
