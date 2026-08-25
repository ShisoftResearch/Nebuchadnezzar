# Schema UID/VID Evolution Implementation Plan (Neb)

> **For agentic workers:** implement task-by-task, one commit per task (Task 3
> is explicitly several). This campaign **breaks the store format on purpose**
> -- see Task 1. Do not add a compatibility shim, a defaulted field, or a
> normalization step to make an old snapshot load. Making an old store fail
> loudly is Task 2 and is part of the deliverable.

**Goal:** Give a Neb schema a logical identity (`uid`) that survives rename and
shape change, separate from the physical generation (`vid`) that a cell header
names, so schemas can be renamed and evolved without rewriting existing cells.

**Architecture:** `Schema.id` is renamed to `Schema.vid: SchemaVid` and a
`Schema.uid: SchemaUid` is added, as distinct newtypes over `u32` drawn from the
same `next_id` Raft counter. The Raft SM and the per-node cache each gain a
uid-keyed handle map holding the current name and current vid. Writes resolve
through one chokepoint in `OwnedCell::plan_write`; reads keep decoding by the
exact vid in the header, as they already do. Every index namespace, statistics
entry, and cell-id derivation is reclassified to uid -- enforced by the type
checker, not by review. Evolution then allocates a new vid under an existing
uid, and the cleaner later migrates stale cells opportunistically.

**Tech Stack:** Rust, Bifrost Raft state machines and callbacks, Lightning
`PtrHashMap` for the local cache, Dovahkiin `OwnedValue`/`Type`.

**Normative design:** `docs/superpowers/specs/2026-08-25-schema-uid-vid-design.md`

**Upstream design:** `../Morpheus/agents_docs/TODO_SCHEMA_UID_VID_EVOLUTION_DESIGN.md`

**Verification note:** Neb's own `cargo test --lib` runs locally at
`--test-threads=8`, which is the protocol its recorded flake set was measured
under. (The 2956/1/121-in-630s figure and the "run it on .239" rule belong to
the *Morpheus* suite, whose 302 Neb servers and ~9100 threads thrash a 29 GB
box -- they are not Neb's baseline and must not be cited as one.) Neb has no
single pass/fail baseline number; dispose of a failure by the verify-alone
protocol against the known load flakes: the `migration::cluster_tests` family
(~2-3/11 rounds on clean develop, including the ranged-scan-vanish signature),
`occ_tests::shape_gated_reads_defer_full_cell_fetch`, and
`mem_shim::tests::buckets_split_by_size_class`. All pass in isolation.

Never `stash`/`checkout` in the user's working tree -- use a worktree. Build
with `CARGO_TARGET_DIR` on real disk; the scratchpad is tmpfs, and so is any
log written there.

---

## Task 1: Identity types, and the mechanical rename

The point of this task is to make the compiler enumerate every site that
touches a schema id. It is a large diff and a trivial one: **every existing
site becomes `vid`**. No site is reclassified to `uid` here -- that is Task 3,
where each change is small enough to argue about.

**Files:**

- Modify: `src/ram/schema/mod.rs` (`Schema`, constructors, `LocalSchemasMap`, `LocalSchemasCache`)
- Modify: `src/ram/schema/sm.rs` (`SchemasMap`, `SchemasSM`, state machine block)
- Modify: every file the compiler names -- expect ~55 files, ~811 sites

- [x] Add `SchemaUid(pub u32)` and `SchemaVid(pub u32)` in `src/ram/schema/mod.rs`,
      each `#[serde(transparent)]` and deriving
      `Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize`.
      Give each a `pub const fn get(self) -> u32` for the hand-rolled encoders
      that need the raw value.
- [x] Add `SchemaVersionStatus { Current, Stale { superseded_by: SchemaVid } }`.
- [x] Rename `Schema.id` to `Schema.vid: SchemaVid`. Add `uid: SchemaUid`,
      `generation: u32`, `status: SchemaVersionStatus`. **All required** -- no
      `#[serde(default)]`, no normalization on ingress.
- [x] `Schema::new` and `Schema::new_with_id` take one number and set
      `uid = SchemaUid(n)`, `vid = SchemaVid(n)`, `generation = 0`,
      `status = Current`.
- [x] `CellHeader.schema` becomes `SchemaVid`. It is hand-encoded with
      `write_u32` (`src/ram/cell.rs:282`) and hand-decoded in
      `minimal_header_from_chunk_raw` (`cell.rs:965`); use `.get()` / the
      constructor there. **Assert no byte moved**: the header layout test must
      still see `schema` as 4 bytes at its current offset.
- [x] `EntryKey::from_props` takes `SchemaVid` for now (Task 3 flips it to
      `SchemaUid`), still writing 4 bytes.
- [x] Work outward from `src/ram/schema` until it compiles. At each site the
      mechanical answer is `vid`; if a site looks like it obviously wants `uid`,
      **leave it as `vid` and note the file and line in the commit message** so
      Task 3 has a worklist rather than a rediscovery.
- [x] Confirm the Lightning maps accept the newtypes -- `PtrHashMap`'s
      `SwiftKeyProbe` bound is satisfied by the blanket `impl<K: Eq>`
      (`Lightning/src/map/ptr_map.rs:2210`). Do not add an impl in Lightning;
      if one seems necessary, stop and re-read the bound.
- [x] Fix the fallout in `src/ram/tests`, `src/server/tests.rs`,
      `src/client/tests.rs`, and the `#[cfg(test)]` block in
      `src/ram/schema/mod.rs`.
- [x] `cargo fmt`, build, full suite locally at `--test-threads=8`.
- [x] Commit: `refactor(schema): separate a schema's generation from its family in the type system`.
      Include the Task 3 worklist in the message body.

## Task 2: An unreadable snapshot must fail loudly

Task 1 broke the record format, which makes this latent path the expected one
for every pre-existing store.

**Files:**

- Modify: `src/ram/schema/sm.rs` (`SchemasSM::recover`)

- [x] `SchemasSM::recover` currently matches the deserialize result and, on
      `None`, logs at `trace!` and returns -- installing an **empty schema map**
      (`sm.rs:141-150`). A database then comes up looking schema-less rather
      than unreadable: every cell fails `SchemaDoesNotExisted`, and
      `select_from_chunk_raw` (`cell.rs:1004`) panics in debug builds.
- [x] Log at `error!`, and refuse to complete recovery rather than installing an
      empty map. The database must fail to load, not load wrong.
- [x] Name the likely cause in the message -- a store written before the
      uid/vid split -- and say to rebuild.
- [x] Check whether `recover` returning without loading is distinguishable to
      its caller at all. If `StateMachineCtl::recover` cannot signal failure,
      say so in the commit message and panic with the diagnostic instead; a
      loud crash beats a silently empty database.
- [x] Test: hand-build a snapshot from the pre-split `Schema` shape, feed it to
      `recover`, and assert the SM does not come up with an empty map.
- [x] Commit: `fix(schema): an unreadable snapshot fails the load instead of emptying it`.

## Task 3: Reclassify the logical namespaces to uid

Every commit here is small, semantic, and independently arguable. The type
checker turns each signature change into a complete list of its call sites, so
none can be half-done.

**Files (one commit per bullet):**

- [x] **Cell identity.** `OwnedCell::default_id` / `encode_cell_key`
      (`src/ram/cell.rs:171-191`) derive a keyed cell's `Id` from the schema id;
      that must be the uid, or a cell's identity changes under it on the first
      evolution. `Id::from_obj(&(uid.get(), key_value))`.
      Commit: `refactor(cell): derive a keyed cell id from the schema family`.
- [x] **Statistics.** `Statistics::schemas` (`src/query/statistics/mod.rs:45`)
      and `refresh_statistics_for_schema` plus its ~9 callers in
      `src/ram/chunk.rs`. `schema_tracks_statistics`'s hard-coded ids
      (`statistics/mod.rs:55-65`) are internal schemas -- uid and vid are the
      same hash there, but state which one the function takes.
      Commit: `refactor(statistics): key per-schema statistics by family`.
- [x] **Hash index.** `get_hash_id`, `get_null_hash_id`,
      `get_hash_id_from_value` (`src/index/hash/mod.rs:426-437`) and callers.
      Commit: `refactor(index): key hash buckets by schema family`.
- [x] **Ranged index.** `EntryKey::from_props` and `for_schema` take
      `SchemaUid` (`src/index/entry.rs:27`, `:41`); callers in
      `src/client/ranged.rs`, `src/query/data_client/read.rs`,
      `src/index/ranged/tree/tree.rs:1342`. Add a doc comment on `EntryKey`
      saying its 4-byte prefix is a **uid**, so a prefix scan covers the whole
      family. The key's width and position do not change.
      Commit: `refactor(index): a ranged key prefix names a schema family`.
- [x] **Vector, embedding, full-text.** Their `schema_id` namespace parameters
      (`src/index/vector/mod.rs`, `src/index/embedding/mod.rs`,
      `src/index/full_text/*`).
      Commit: `refactor(index): namespace vector, embedding and full-text by family`.
- [x] **Index maintenance.** `ensure_indices`, `probe_cell_indices`,
      `remove_indices` (`src/index/builder.rs:601+`) emit keys under
      `schema.uid`.
      Commit: `refactor(index): emit index entries under the schema family`.
- [x] **Post-schema hooks.** `post_schema_add` / `post_schema_delete`
      (`src/ram/schema/mod.rs:872`, `:945`) create and destroy vector and
      embedding indexes by uid.
      Commit: `refactor(schema): create and destroy indexes by family`.
- [x] Leave the decode sites alone -- `cell.rs:533`, `cell.rs:987`,
      `plan_write` (`cell.rs:201`). Those take the vid, deliberately. If one of
      them stops compiling during this task, something upstream was
      misclassified; fix that rather than widening the site.
- [x] Full suite locally at `--test-threads=8` after the last commit.

## Progress note (2026-08-25)

Tasks 1-3 are committed on `feat/schema-uid-vid`. Two things went differently
from the plan and are worth carrying forward:

- **Task 3 landed in five commits, not seven.** Hash buckets and the ranged key
  prefix are reached through the *same* query-layer functions, so typing one
  and wrapping the other would have meant wrapping in one commit and unwrapping
  in the next. The query data client and `CostFunction` were therefore typed
  once, in the hash commit, which made the ranged commit small. Vector,
  embedding, full-text, the index metas and the post-schema hooks were likewise
  one commit, because `IndexComps` carries the id from `probe_cell_indices`
  straight into every one of them.
- **`schema_tracks_statistics` deliberately still takes a generation.** It runs
  once per live cell during a gather, and everything it rejects is an internal
  schema that never evolves. Resolving a record just to reject a b-tree page
  would put a map lookup on the hottest path in the gather.

The compiler-propagation premise held: typing a boundary pushed the type back
to the schema record on its own, and interior plumbing that never reaches a
boundary correctly stayed `u32`. Two RPC-facing surfaces keep a bare `u32` and
wrap at the boundary, documented in place -- ids arriving over the wire are
always logical, so there is nothing to resolve.

## Task 4: Handle maps in the state machine and the local cache

**Files:**

- Modify: `src/ram/schema/sm.rs` (`SchemasMap`, `SchemasSM`)
- Modify: `src/ram/schema/mod.rs` (`LocalSchemasMap`, `LocalSchemasCache`)

- [ ] Add `SchemaHandle { uid, current_name, current_vid, generation }`.
- [ ] `SchemasMap` gains `handles: HashMap<SchemaUid, SchemaHandle>`, populated
      by `new_schema` and `load_from_list`, removed by `del_schema`.
- [ ] `SchemasMap::name_map` maps `String -> SchemaUid`.
- [ ] `SchemasMap::del_schema(name)` resolves name -> uid, then removes the
      handle, the name binding, and **every** record in `schema_map` whose
      `uid` matches -- not just the current vid.
- [ ] `LocalSchemasMap` gains `handles: LFHashMap<SchemaUid, SchemaVid>`.
      Add `uid_of_name` and `current_vid_of_uid`.
- [ ] Confirm the local cache's name-collision guard
      (`src/ram/schema/mod.rs:812`) is still correct now that the name binds to
      a uid. Add a test that re-delivering the same schema through the
      `on_schema_added` subscription is an idempotent upsert, as the
      subscribe-then-read comment above `new_for_database` requires.
- [ ] Tests: two schemas get distinct uids; `del_schema` on a uid with two
      generations removes both records (build the second by hand --
      `evolve_schema` does not exist yet).
- [ ] `cargo fmt`, build, focused tests.
- [ ] Commit: `feat(schema): index schema records by family as well as by generation`.

## Task 5: Rename

**Files:**

- Modify: `src/ram/schema/sm.rs` (state machine block, `SchemasMap`)
- Modify: `src/ram/schema/mod.rs` (subscription wiring)
- Modify: `src/client/mod.rs`

- [ ] Add `def cmd rename_schema(old_name: String, new_name: String) -> Result<(), RenameSchemaError>`
      and `def sub on_schema_renamed() -> (SchemaUid, String)`.
- [ ] `RenameSchemaError`: `SchemaDoesNotExist`, `NameExists(String)`,
      `NotifyError(NotifyError)`.
- [ ] SM behaviour: resolve old name -> uid; refuse if the new name is bound;
      rebind `name_map`; update `handles[uid].current_name`. Touch no record in
      `schema_map`, no cell, no index. Suppress the callback during recovery,
      matching `new_schema`/`del_schema`.
- [ ] Subscribe the local cache to `on_schema_renamed` **inside the existing
      subscribe-before-read block** in `new_for_database`, so it is ordered
      with the others.
- [ ] Add `NebClient::rename_schema(old, new)`.
- [ ] Tests: rename then look up by the new name; the old name resolves to
      nothing; the vid is unchanged and its record reports the new name; a
      rename onto an occupied name is refused; cells written before the rename
      still read back afterwards.
- [ ] `cargo fmt`, build, focused tests.
- [ ] Commit: `feat(schema): rename a schema without touching a single cell`.

## Task 6: Write resolution chokepoint

**Files:**

- Modify: `src/ram/schema/mod.rs` (`LocalSchemasCache`)
- Modify: `src/ram/cell.rs` (`plan_write`, `write_to_addr`)

- [ ] Add `LocalSchemasCache::resolve_for_write(&self, vid: SchemaVid) -> Option<SchemaRef>`:
      look up the record; if `status == Current` return it; otherwise look up
      `handles[record.uid]` and return that vid's record. `None` if either
      lookup misses, so `plan_write` keeps returning
      `WriteError::SchemaDoesNotExisted`.
- [ ] `OwnedCell::plan_write` resolves through `resolve_for_write` instead of
      `chunk.meta.schemas.get`.
- [ ] `OwnedCell::write_to_addr` writes `write_plan.schema.vid` into the header
      rather than `self.header.schema`, and the returned `CellHeader` reports
      the resolved vid.
- [ ] Test: construct a stale record by hand (a `Current` schema plus a second
      record marked `Stale { superseded_by }` under the same uid), write a cell
      naming the stale vid, assert the persisted header names the current vid.
- [ ] Test: a write naming `SchemaVid(uid.get())` -- the generation-0 vid --
      lands in the current generation once generation 0 is stale.
- [ ] Full suite locally at `--test-threads=8`.
- [ ] Commit: `feat(schema): every write resolves to the current generation`.

## Task 7: Evolution, identity-transform tier

**Files:**

- Modify: `src/ram/schema/sm.rs` (state machine block, `SchemasMap`)
- Modify: `src/ram/schema/mod.rs` (classification, `post_schema_add` diffing)
- Modify: `src/client/mod.rs`

- [ ] Add `Schema::classify_evolution(from: &Schema, to: &Schema) -> EvolutionKind`
      returning `Identity`, `NeedsTransform(reason)`, or `Illegal(reason)`.
      `Identity` covers: added nullable fields, dropped fields **on a
      non-dynamic schema**, index-set changes, and `is_scannable`/`blobs`
      changes. Dropping a field from an `is_dynamic` schema is
      `NeedsTransform`, because `plan_write_dynamic_fields` would re-encode it
      into the dynamic region rather than drop it. Added non-nullable fields,
      renamed fields, and type changes are `NeedsTransform`. A changed
      `key_field` is `Illegal` -- it would change the cell ids of every future
      write and orphan every existing one.
- [ ] Add `def cmd evolve_schema(name: String, schema: Schema) -> Result<SchemaVid, EvolveSchemaError>`
      and `def sub on_schema_evolved() -> Schema`.
- [ ] SM behaviour: resolve name -> uid -> current record; classify; refuse
      anything but `Identity` with `EvolveSchemaError::TransformRequired(reason)`;
      allocate a new vid from the same `next_id` counter; insert the new record
      with `uid` carried over, `generation + 1`, `status: Current`; set the old
      record to `Stale { superseded_by: new_vid }`; update the handle.
- [ ] Local cache handles `on_schema_evolved` by inserting the new record and
      updating both the old record's status and the handle. Subscribe inside
      the existing subscribe-before-read block.
- [ ] `post_schema_add` must **diff** the two generations' index sets rather
      than recreating every index: create what the new generation adds, destroy
      what it drops, leave the rest alone.
- [ ] Add `NebClient::evolve_schema(name, schema)`.
- [ ] Tests, each writing cells under generation 0 first:
      - add a nullable field; old cells still read (missing field is null), new
        writes land in generation 1, a full scan returns both
      - drop a field on a non-dynamic schema; old cells still read
      - drop a field on a dynamic schema; refused as `TransformRequired`
      - add a non-nullable field; refused as `TransformRequired`
      - change `key_field`; refused as `Illegal`
      - evolve twice, then write naming the original vid; it lands in
        generation 2 in one hop, proving `vid -> uid -> current_vid` does not
        chain
      - update a generation-0 cell after evolving; it returns as a
        generation-1 cell with the same `Id` and a bumped version
      - a ranged scan by schema returns cells of both generations from one
        `EntryKey::for_schema(uid)` prefix, with no fanout
- [ ] Full suite locally at `--test-threads=8`.
- [ ] Commit: `feat(schema): evolve a schema into a new generation`.

## Task 8: Cleaner-driven lazy migration

Sequenced last on purpose: evolution is already correct without it, because
stale cells drain through ordinary updates. This task only makes them drain
without being touched.

**Files:**

- Modify: `src/ram/cleaner/combine.rs` (`collect_and_deduplicate_entries`,
  `plan_segment_layout`, `execute_combine_phases`)
- Modify: `src/ram/cleaner/tests.rs`

- [ ] During `collect_and_deduplicate_entries`, read each live entry's header
      and mark entries whose vid is stale as needing migration.
- [ ] Decode, transform, and re-encode migrating cells into an owned buffer
      **before** `plan_segment_layout`, so `DummyEntry::size` is the
      post-migration size. The planner's size arithmetic is what keeps the
      destination copy inside its segment (`combine.rs:311`, the COMBINE
      OVERRUN probe); it must never see a stale size.
- [ ] In `execute_combine_phases`, a migrating entry is copied from its owned
      buffer instead of `libc::memcpy` from the source address.
- [ ] The index-swap step gains what a version-bumping write does: bump the
      cell version, maintain index entries for any index the two generations
      disagree on, and refresh statistics for the uid. Reuse the existing
      write-path helpers rather than reimplementing them -- and note that the
      counter-only-write plus sweeper rule applies here too: do **not** do an
      O(cells) statistics refresh inline on a cleaner thread.
- [ ] A cell that cannot be migrated -- transform refuses, schema record
      missing -- is relocated verbatim and left stale. Log it; never drop it.
- [ ] Tests: a segment of stale cells combines to current cells with the same
      ids and readable values; a mixed segment migrates only the stale ones; a
      cell whose schema record is missing survives unchanged; the
      destination-overrun probe never fires (assert the error log is empty --
      the probe is a `break`, not a panic).
- [ ] Run the crash-churn fuzzer over a store with a mid-flight evolution.
- [ ] Full suite locally at `--test-threads=8`.
- [ ] Commit: `feat(cleaner): migrate stale cells while combining`.

## Task 9: Transform engine

Replan before starting -- the shape of `SchemaTransform` should be decided
against whatever the first real evolution need turns out to be, not guessed
here.

**Files:**

- Modify: `src/ram/schema/mod.rs`
- Modify: `src/ram/cleaner/combine.rs`

- [ ] Represent a transform as an ordered list of mechanical ops: inject a
      default for an added non-nullable field, re-key a renamed field, widen a
      numeric type.
- [ ] Apply it between decode and encode on both the write path and the
      cleaner path -- the same function in both, or they will diverge.
- [ ] Extend `classify_evolution` so an evolution a transform can express is
      admitted with that transform attached, instead of refused.
- [ ] Store the transform on the new record so a cell migrating across several
      generations at once applies each hop's ops in order.

---

## Consequences of the format break

- **Every existing store must be rebuilt.** Including the TB-scale and
  Wikidata-class stores, which are hours of import each. This cost was
  accepted when backward compatibility was dropped; it is listed here so it is
  not rediscovered mid-campaign.
- **Do not reach for a shim.** If an old snapshot needs to load, that is a new
  decision to take with the user, not a defaulted field to add quietly.

## Out of scope for this plan

- **Morpheus Phase 2.** Moving durable graph references (vertex id lists, graph
  schema references, edge metadata) from the physical id to the uid. It depends
  only on Task 1 publishing a `uid` and can proceed in parallel from there.
- **Reclaiming superseded generations.** Needs a cluster-wide proof that no
  cell names a given vid; `src/index/scrub.rs` is the natural home. Until then
  stale records are retained indefinitely, which costs metadata only.
- **A forced migration sweep.** The upstream design lists it as optional and
  not required for a first implementation.
