# Non-Transactional / MVCC Cost Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore a raw, history-free non-transactional point-cell path while preserving transactional MVCC/OCC, cleaner relocation, crash recovery, and undo compensation.

**Architecture:** `cell_index` stores only raw addresses of present cells. Transactions lazily create chunk-owned revision chains; direct tombstones remain unindexed physical recovery records whose cleaner lifetime is controlled by the predecessor-segment sequence watermark. Recovery publishes only present winners, while startup undo lazily reconstructs only the exact recovered revision that requires compensation.

**Tech Stack:** Rust 2021, Lightning lock-free maps/lists, append-only RAM segments and WAL, HLC revision timestamps, Criterion benchmark harness.

## Global Constraints

- The approved design is `docs/superpowers/specs/2026-07-29-nontransaction-mvcc-cost-isolation-design.md` at commit `59942938`.
- No cell-index ownership tags, indexed tombstones, or `direct_deleted_heads` counter may remain.
- Pure direct operations consult only `cell_index`; they do not resolve history, allocate `RevisionNode`, schedule expiration, or wake the history worker.
- A direct delete durably finishes its tombstone before removing the cell-index entry.
- Transactions retain every MVCC/OCC guarantee and every distributed commit phase.
- Mixing direct and transactional APIs on the same logical cell has no isolation, safety, lost-update, snapshot, or recovery guarantee.
- Recovery rebuilds no general revision history. Startup undo may lazily seed one exact recovered revision before installing a newer compensation.
- No backward compatibility is required for storage, WAL, undo, RPC, or public formats.
- Run tests and debugging locally. Use `192.168.10.87` only for benchmarks.
- Run one heavy command at a time.
- Do not push.
- Do not modify or stage the eleven user-owned ranged-index files under `src/index/ranged/tree/`.
- Reject any reproducible pure non-transactional throughput or p99 regression; both benchmark populations must have throughput CV below 5%.

## File Map

- `src/ram/chunk.rs` — raw cell-index representation, direct mutation paths, transactional conversion, recovery-only revision lookup.
- `src/ram/history.rs` — test-only counters proving direct paths allocate and schedule no history work.
- `src/ram/tests/cell.rs` — direct-path and direct-to-transaction conversion tests.
- `src/ram/cleaner/combine.rs` — dual liveness and relocation publication.
- `src/ram/cleaner/tests.rs` — watermark, MVCC relocation, pending, abort, and restart tests.
- `src/ram/recovery.rs` — greatest-`revision_ts` winner selection and raw-present publication.
- `src/server/transactions/undo_log.rs` — startup compensation and idempotence tests.
- `src/ram/tiered/cell_locking.rs` — raw current-cell locking; tombstones remain outside `cell_index`.
- `Cargo.toml` — benchmark-only `mvcc_revision_api` adapter feature.
- `benches/occ_support/workloads.rs` — pure direct acceptance workloads.
- `benches/occ_transactions.rs` — benchmark scenario registration.
- `tests/occ_bench_metrics.rs` — workload accounting tests.

---

### Task 1: Restore the raw direct point-cell path

**Files:**

- Modify: `src/ram/history.rs`
- Modify: `src/ram/chunk.rs`
- Modify: `src/ram/tests/cell.rs`

**Interfaces:**

- Produces: `CellGuard::get_ptr() -> usize` returning the exact raw index word.
- Produces: `CellGuard::set_ptr(&mut self, ptr: usize)`.
- Produces: direct `write_cell`, `update_cell`, `update_cell_by`, `upsert_cell`, guarded update/upsert, and compare-revision mutations that never enter `HistoryIndex`.
- Preserves: assigned-revision entry points for Task 2.

- [ ] **Step 1: Add test-only history activity counters**

In `src/ram/history.rs`, count node construction, expiration scheduling, and
worker wakes only in test builds:

```rust
#[cfg(test)]
static REVISION_NODE_ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

impl RevisionNode {
    pub fn new(
        revision_ts: u64,
        state: RevisionState,
        location: usize,
        entry_size: u32,
    ) -> Self {
        #[cfg(test)]
        REVISION_NODE_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        assert_eq!(
            location & STATE_MASK,
            0,
            "revision entry addresses must be 8-byte aligned"
        );
        Self {
            revision_ts,
            state_and_location: AtomicUsize::new(location | state as usize),
            entry_size,
            retire_deadline_ms: AtomicU64::new(0),
        }
    }
}

#[cfg(test)]
pub(crate) fn take_revision_node_allocations_for_test() -> usize {
    REVISION_NODE_ALLOCATIONS.swap(0, Ordering::AcqRel)
}
```

Add `expiration_schedules` and `worker_wakes` as `#[cfg(test)] AtomicUsize`
fields on `HistoryIndex`. Increment them in `schedule_expiration` and
`wake_worker`, and expose:

```rust
#[cfg(test)]
pub(crate) fn take_direct_path_activity_for_test(&self) -> (usize, usize) {
    (
        self.expiration_schedules.swap(0, Ordering::AcqRel),
        self.worker_wakes.swap(0, Ordering::AcqRel),
    )
}
```

- [ ] **Step 2: Extend the direct-path test and make it fail**

In `src/ram/tests/cell.rs`, extend
`every_direct_update_entry_point_bypasses_history` to exercise update-by,
occupied and empty upsert, `CellGuard::update_cell`,
`CellGuard::upsert_cell`, `compare_revision_and_update_cell`, and
`compare_revision_and_set_field`.

Before the operations, drain the test counters:

```rust
crate::ram::history::take_revision_node_allocations_for_test();
history.take_chain_map_resolutions_for_test();
history.take_direct_path_activity_for_test();
```

After the operations, require:

```rust
assert_eq!(history.take_chain_map_resolutions_for_test(), 0);
assert_eq!(
    crate::ram::history::take_revision_node_allocations_for_test(),
    0
);
assert_eq!(history.take_direct_path_activity_for_test(), (0, 0));
for id in [
    update_by_id,
    guarded_id,
    upsert_id,
    guarded_upsert_id,
    empty_upsert_id,
    conditional_update_id,
    conditional_field_id,
] {
    assert_eq!(history.revision_count_for_test(&id), 0);
}
```

Also capture every published word directly:

```rust
let old_location = chunks.address_of(&id);
let old_segment = chunk.locate_segment(old_location).unwrap();
let (old_entry, ()) =
    crate::ram::entry::Entry::decode_from(old_location, |_, _| ());
let old_entry_size = old_entry.content_length
    + crate::ram::entry::ENTRY_HEAD_SIZE as u32;
let dead_before = old_segment.dead_space.load(Ordering::Acquire);

chunks.update_cell(&mut replacement).unwrap();

assert_eq!(
    old_segment.dead_space.load(Ordering::Acquire) - dead_before,
    old_entry_size
);
let word = chunk
    .cell_index
    .get_from_mutex(&(id.lower as usize))
    .expect("present direct head");
assert_eq!(word, chunks.address_of(&id));
assert_eq!(word & 0b111, 0);
```

- [ ] **Step 3: Run the direct test to verify RED**

Run:

```bash
cargo test --lib ram::tests::cell::every_direct_update_entry_point_bypasses_history -- --exact --nocapture
```

Expected: failure because transaction ownership tags or history-backed direct
updates are still observable.

- [ ] **Step 4: Remove every cell-index tag and deleted-head counter**

In `src/ram/chunk.rs`, delete:

```rust
CELL_INDEX_TRANSACTIONAL_TAG
CELL_INDEX_DIRECT_DELETED_TAG
CELL_INDEX_TAG_MASK
cell_index_address
cell_index_is_transactional
cell_index_is_direct_deleted
transactional_cell_index_word
direct_deleted_cell_index_word
Chunk::direct_deleted_heads
```

Make `location_for_read`, `location_for_write`, `address_of`,
`CellGuard::from_guard`, and tiered segment lookup use the raw word directly.
Restore one setter:

```rust
fn set_ptr(&mut self, ptr: usize) {
    debug_assert_eq!(ptr & 0b111, 0);
    **self.guard.as_mut().expect("cell guard") = ptr;
}

pub fn get_ptr(&self) -> usize {
    **self.guard.as_ref().expect("cell guard")
}

fn is_logically_absent(&self) -> bool {
    self.is_unassigned()
}
```

Remove `is_transactional`, `is_direct_deleted`,
`set_transactional_ptr`, `set_direct_ptr`,
`set_direct_deleted_ptr`, and `remove_direct_deleted_index_entry`.

- [ ] **Step 5: Make every direct mutation unconditionally use direct helpers**

Reduce `write_direct_cell_with_guard` to an empty-slot insert. It assigns
`next_revision_ts(0)`, finishes the physical entry, then calls `set_ptr`.
It does not inspect a tombstone:

```rust
if !guard.is_unassigned() {
    return Err(WriteError::CellAlreadyExisted);
}
let revision_ts = self.next_revision_ts(0)?;
let write_plan = cell.plan_write(self)?;
let pending_entry = write_plan.allocate(self, true)?;
let write_result =
    self.write_cell_to_chunk(cell, &write_plan, &pending_entry, revision_ts)?;
pending_entry.finish()?;
guard.set_ptr(write_result.addr);
```

Make `update_cell_with_guard` call `update_direct_cell_with_guard` without
checking history ownership. Keep its existing order: preflight schema/index
materialization, allocate and finish the replacement, publish the raw pointer,
then dead-account the replaced entry exactly once.

Make `write_cell`, `update_cell`, `upsert_cell`, and `update_cell_by` depend
only on raw present/absent state. Direct `remove_cell` and `remove_cell_by`
move to the tombstone path in Task 3. Assigned-revision methods remain
separate and are handled in Task 2.

- [ ] **Step 6: Run direct insertion and update tests**

Run:

```bash
cargo test --lib ram::tests::cell::direct_write_and_update_do_not_create_or_resolve_history -- --exact
cargo test --lib ram::tests::cell::direct_update_revision_timestamps_are_strictly_increasing -- --exact
cargo test --lib ram::tests::cell::every_direct_update_entry_point_bypasses_history -- --exact
```

Expected: all pass, raw words are aligned addresses, and every history
activity counter remains zero.

- [ ] **Step 7: Commit the raw direct path**

```bash
git add src/ram/history.rs src/ram/chunk.rs src/ram/tests/cell.rs
git commit -m "perf(ram): restore raw nontransaction cell path"
```

Before committing, verify `git diff --cached --name-only` lists only those
three files.

---

### Task 2: Lazily convert direct heads for transactional MVCC

**Files:**

- Modify: `src/ram/chunk.rs`
- Modify: `src/ram/tests/cell.rs`

**Interfaces:**

- Consumes: raw `CellGuard::get_ptr` and `CellGuard::set_ptr` from Task 1.
- Produces: `ensure_present_predecessor_with_chain(Id, &CellHeader, usize)`.
- Preserves: `InstalledRevision`, `RevisionWrite`, exact predecessor
  certification, pending visibility, promotion, abort, and retention.

- [ ] **Step 1: Add a raw-word-stability conversion test**

Extend `assigned_update_converts_a_direct_head_and_retains_its_predecessor`:

```rust
let raw_before = chunk
    .cell_index
    .get_from_mutex(&(id.lower as usize))
    .unwrap();

let SnapshotRead::Present(snapshot) =
    chunks.read_cell_snapshot(&id, direct.header.revision_ts + 1).unwrap()
else {
    panic!("transaction snapshot must convert the direct head");
};
assert_eq!(snapshot.header.revision_ts, direct.header.revision_ts);

let raw_after_snapshot = chunk
    .cell_index
    .get_from_mutex(&(id.lower as usize))
    .unwrap();
assert_eq!(raw_after_snapshot, raw_before);
assert_eq!(chunk.history.revision_count_for_test(&id), 1);
```

After the assigned update, assert the index word is exactly the new physical
address and the chain contains the direct predecessor plus the assigned head.

- [ ] **Step 2: Run the conversion test to verify RED**

Run:

```bash
cargo test --lib ram::tests::cell::assigned_update_converts_a_direct_head_and_retains_its_predecessor -- --exact --nocapture
```

Expected: failure until snapshot conversion and assigned writes stop changing
the cell-index representation.

- [ ] **Step 3: Keep assigned writes raw while retaining their chains**

In `write_cell_with_guard_at_revision` and
`update_cell_with_guard_at_revision`, replace transaction-tag publication
with:

```rust
guard.set_ptr(write_result.addr);
```

Continue to install the `RevisionNode` before exposing the raw present mirror.
For assigned delete, install the tombstone node before removing the raw index
entry. Keep exact predecessor pointer comparison and `history.retire`
unchanged.

- [ ] **Step 4: Convert direct present heads without changing the index**

In `read_snapshot_at`, read the raw current cell and call
`ensure_present_predecessor_with_chain` when no matching chain exists:

```rust
let location = guard.get_ptr();
let header = guard.head_cell()?;
if Id::from_header(&header) == *id {
    let current = match self
        .ensure_present_predecessor_with_chain(*id, &header, location)
    {
        Ok((_, current)) => Some(current),
        Err(_) => self.history.current(id),
    };
    if header.revision_ts < snapshot_ts
        && current.as_ref().is_some_and(|current| {
            current.revision_ts == header.revision_ts
                && current.load()
                    == (RevisionState::CommittedPresent, location)
        })
    {
        return materialize(location, current.unwrap().entry_size)
            .map(SnapshotRead::Present);
    }
}
```

If an existing chain does not match the raw head, fall through to
`history.resolve`; this is the unsupported mixed-mode case and must not add
coordination to direct operations.

Remove all direct-tombstone-to-history conversion. Raw absence resolves only
from existing transaction history or returns `SnapshotRead::Absent(None)`.

- [ ] **Step 5: Run transactional point-cell tests**

Run:

```bash
cargo test --lib ram::tests::cell::assigned_update_converts_a_direct_head_and_retains_its_predecessor -- --exact
cargo test --lib ram::tests::cell::snapshot_reads_old_address_after_current_update -- --exact
cargo test --lib ram::tests::cell::delete_and_recreate_preserve_revision_aware_absence -- --exact
cargo test --lib ram::tests::cell::pending_revision_waits_below_boundary_then_promotes_without_moving -- --exact
cargo test --lib ram::tests::cell::abort_pending_update_skips_it_without_changing_identity -- --exact
cargo test --lib ram::tests::cell::pending_delete_promotes_to_revision_aware_absence -- --exact
```

Expected: all pass with raw current pointers and unchanged MVCC visibility.

- [ ] **Step 6: Commit lazy transaction conversion**

```bash
git add src/ram/chunk.rs src/ram/tests/cell.rs
git commit -m "feat(mvcc): lazily seed history from raw cells"
```

---

### Task 3: Make direct tombstones unindexed and cleaner-safe

**Files:**

- Modify: `src/ram/chunk.rs`
- Modify: `src/ram/cleaner/combine.rs`
- Modify: `src/ram/cleaner/tests.rs`
- Modify: `src/ram/tiered/cell_locking.rs`
- Modify: `src/ram/tests/cell.rs`

**Interfaces:**

- Consumes: raw present-only index from Task 1.
- Produces: cleaner liveness
  `current_index_target || history_live || direct_tombstone_live`.
- Produces: direct tombstone watermark
  `oldest_resident_seq <= tombstone.segment_seq_id`.
- Preserves: `HistoryIndex::relocate` and current-history/raw-mirror
  reconciliation.

- [ ] **Step 1: Make direct-delete tests require an absent index**

In `direct_remove_paths_bypass_history_and_keep_cells_absent`, assert:

```rust
let old_location = chunks.address_of(&direct_id);
let old_segment = chunk.locate_segment(old_location).unwrap();
let (old_entry, ()) =
    crate::ram::entry::Entry::decode_from(old_location, |_, _| ());
let old_entry_size = old_entry.content_length
    + crate::ram::entry::ENTRY_HEAD_SIZE as u32;
let dead_before = old_segment.dead_space.load(Ordering::Acquire);

chunks.remove_cell(&direct_id).unwrap();

assert!(
    chunk
        .cell_index
        .get_from_mutex(&(direct_id.lower as usize))
        .is_none()
);
assert_eq!(chunk.history.revision_count_for_test(&direct_id), 0);
assert_eq!(chunks.count(), 0);
assert_eq!(chunks.clear_cell_index(), 0);
assert_eq!(
    old_segment.dead_space.load(Ordering::Acquire) - dead_before,
    old_entry_size
);
```

Drain and assert the activity counters from Task 1 around both `remove_cell`
and `remove_cell_by`.

Rename `legacy_guard_upsert_of_empty_slot_publishes_history` to
`direct_guard_upsert_of_empty_slot_bypasses_history` and require zero
revisions.

Rewrite `indexed_remove_with_missing_schema_is_side_effect_free` to enter the
direct remove path. Capture the original raw index word, segment append
header, tombstone count, dead-space count, and secondary-index removal count;
after `SchemaDoesNotExisted`, require every captured value to be unchanged.

- [ ] **Step 2: Add the multi-segment resurrection regression**

In `src/ram/cleaner/tests.rs`, add
`direct_tombstone_survives_after_predecessor_segment_is_cleaned_first`.
The setup must:

1. write direct revision A;
2. force a new segment and write direct revision B;
3. force a new segment and directly delete B, recording B's segment sequence
   in the tombstone;
4. clean B's segment together with a filler segment;
5. clean the tombstone segment while A's older segment is still resident;
6. drop and recover the WAL-backed chunks; and
7. assert the cell remains absent and has no history chain.

The controlling assertions are:

```rust
assert!(chunk.segs.contains_seq_id(oldest_segment.seq_id));
assert!(!chunk.segs.contains_seq_id(predecessor_segment.seq_id));
assert!(chunks.read_cell(&id).is_err());

drop(chunks);
let recovered = recover_retained_revision_chunks_with_wal(
    8,
    wal_path,
    raft_path,
    schema,
);
assert!(recovered.read_cell(&id).is_err());
assert_eq!(
    recovered.list[0].history.revision_count_for_test(&id),
    0
);
```

Add `direct_tombstone_expires_after_every_older_segment_is_removed`. Remove
all resident segments with `seq_id <= tombstone.segment_seq_id`, run another
combine over the relocated tombstone and a filler segment, and assert no
physical tombstone for the full `Id` remains:

```rust
let tombstones = chunk
    .segments()
    .into_iter()
    .flat_map(|segment| {
        segment
            .entry_iter()
            .filter_map(|entry| {
                if entry.entry_header.entry_type != EntryType::TOMBSTONE {
                    return None;
                }
                let tombstone =
                    Tombstone::read_from_entry_content_addr(entry.body_pos);
                (Id::new(tombstone.partition, tombstone.hash) == id)
                    .then_some(())
            })
            .collect::<Vec<_>>()
    })
    .count();
assert_eq!(tombstones, 0);
```

- [ ] **Step 3: Run direct delete and cleaner tests to verify RED**

Run:

```bash
cargo test --lib ram::tests::cell::direct_remove_paths_bypass_history_and_keep_cells_absent -- --exact
cargo test --lib ram::cleaner::tests::direct_tombstone_survives_after_predecessor_segment_is_cleaned_first -- --exact --nocapture
cargo test --lib ram::cleaner::tests::direct_tombstone_expires_after_every_older_segment_is_removed -- --exact --nocapture
```

Expected: the first fails while the tombstone is indexed; the second fails if
the cleaner uses only exact predecessor presence.

- [ ] **Step 4: Publish direct delete in failure-atomic order**

Keep all schema/index materialization and HLC allocation before the tombstone
write. Then:

```rust
let pending_entry = self.try_acquire(TOMBSTONE_ENTRY_SIZE as u32, true)?;
let tombstone_segment = pending_entry.seg.clone();
Tombstone::put(
    pending_entry.addr,
    old_segment.seq_id,
    revision_ts,
    id.higher,
    id.lower,
);
pending_entry.finish()?;
tombstone_segment.tombstones.fetch_add(1, Ordering::Relaxed);
tombstone_segment.note_dead_bytes_change();
```

After the successful finish, remove secondary indexes, then call:

```rust
guard.remove_index_entry();
self.refresh_statistics_for_schema(old_header.schema);
self.mark_dead_entry_with_size(old_location, old_entry_size, &old_segment);
```

Do not publish the tombstone address anywhere. `cell_count` becomes exactly
`cell_index.len()`, and `clear_cell_index` has no deleted-head bookkeeping.

- [ ] **Step 5: Implement dual cleaner liveness**

At the start of `collect_and_deduplicate_entries`, compute once:

```rust
let oldest_resident_seq = chunk
    .segments()
    .into_iter()
    .map(|segment| segment.seq_id)
    .min();
```

Add `direct_tombstone_live: bool` to `DummyEntry` and `Relocation`.
For each physical entry, compute tombstone watermark liveness while decoding
the entry so the physical tombstone remains available:

```rust
let (key, kind, direct_tombstone_live) = match entry.entry_header.entry_type {
    EntryType::CELL => {
        let header =
            cell::cell_header_from_entry_content_addr(entry.body_pos);
        (
            RevisionKey {
                id: Id::from_header(&header),
                revision_ts: header.revision_ts,
            },
            RevisionKind::Cell,
            false,
        )
    }
    EntryType::TOMBSTONE => {
        let tombstone =
            Tombstone::read_from_entry_content_addr(entry.body_pos);
        (
            RevisionKey {
                id: Id::new(tombstone.partition, tombstone.hash),
                revision_ts: tombstone.revision_ts,
            },
            RevisionKind::Tombstone,
            oldest_resident_seq
                .is_some_and(|oldest| oldest <= tombstone.segment_seq_id),
        )
    }
    EntryType::UNDECIDED => continue,
};
let history_live =
    chunk.history.is_live_at(key.id, key.revision_ts, entry.entry_pos);
let current_index_target = kind == RevisionKind::Cell
    && chunk.cell_index.get_from_mutex(&(key.id.lower as usize))
        == Some(entry.entry_pos);

if !history_live && !current_index_target && !direct_tombstone_live {
    continue;
}
```

On `RelocateResult::LostRace`:

- a current-only cell performs `compare_exchange_current_only_address`;
- a watermark-live tombstone keeps the copied destination and publishes no
  pointer;
- any other lost destination is dead-accounted once.

Delete `compare_exchange_current_only_tombstone_address`.

Apply the same liveness rule in `Chunk::live_entries`: current raw cell,
history node, or watermark-live tombstone.

- [ ] **Step 6: Preserve MVCC relocation reconciliation**

Do not change these cases:

```rust
RelocateResult::HistoricalMoved
RelocateResult::CurrentPresentMoved
```

For a current MVCC present cell, move the history location and reconcile the
raw cell-index mirror. If mirror publication is inconsistent, roll the
history location back before allowing source reclamation. Transaction
tombstones move only through their history nodes.

In `src/ram/tiered/cell_locking.rs`, collect and lock hashes only for physical
cells. Remove tombstone hash collection and all address decoding; index words
are raw current-cell addresses.

- [ ] **Step 7: Run cleaner correctness coverage**

Run:

```bash
cargo test --lib ram::cleaner::tests::direct_tombstone_survives_after_predecessor_segment_is_cleaned_first -- --exact --nocapture
cargo test --lib ram::cleaner::tests::direct_tombstone_expires_after_every_older_segment_is_removed -- --exact --nocapture
cargo test --lib ram::cleaner::tests::combine_relocates_a_current_only_cell_without_a_history_node -- --exact
cargo test --lib ram::cleaner::tests::combine_relocates_current_and_historical_revisions_for_one_id -- --exact
cargo test --lib ram::cleaner::tests::combine_preserves_current_and_historical_tombstones_for_colliding_full_ids -- --exact
cargo test --lib ram::cleaner::tests::relocation_lost_to_expiration_marks_the_destination_dead_once -- --exact
cargo test --lib ram::cleaner::tests::marked_committed_insert_update_delete_survive_relocation_and_recovery -- --exact
cargo test --lib ram::cleaner::tests::marked_aborted_compensation_survives_relocation_and_recovery -- --exact
```

Expected: direct tombstones prevent resurrection, while current, historical,
pending, committed, aborted, and compensated MVCC revisions remain readable.

- [ ] **Step 8: Commit cleaner-safe direct tombstones**

```bash
git add \
  src/ram/chunk.rs \
  src/ram/cleaner/combine.rs \
  src/ram/cleaner/tests.rs \
  src/ram/tiered/cell_locking.rs \
  src/ram/tests/cell.rs
git commit -m "fix(cleaner): retain direct tombstones by sequence watermark"
```

---

### Task 4: Recover raw winners and preserve startup undo

**Files:**

- Modify: `src/ram/recovery.rs`
- Modify: `src/ram/chunk.rs`
- Modify: `src/server/transactions/undo_log.rs`

**Interfaces:**

- Consumes: `RecoveryCandidates` greatest-`revision_ts` selection.
- Produces: `publish_recovered_present(Id, usize) -> io::Result<()>`.
- Produces: recovery-only exact revision seeding used by
  `compensate_recovered`.
- Preserves: newer compensating revisions, exact installed timestamp checks,
  durability ordering, and idempotence.

- [ ] **Step 1: Make recovery tests require no deleted index or history**

In `recovery_orders_present_and_deleted_revisions_in_both_scan_directions`,
replace the indexed-tombstone assertions with:

```rust
assert!(recovered.chunks.read_cell(&id).is_err());
assert!(
    recovered.chunks.list[0]
        .cell_index
        .get_from_mutex(&(id.lower as usize))
        .is_none()
);
assert_eq!(
    recovered.chunks.list[0].history.revision_count_for_test(&id),
    0
);
```

Keep `current_revision_ts(&id) == Some(200)` as a recovery/undo diagnostic,
but assert that calling it does not create a history chain.

Add restart coverage for
`recovery_compensates_delete_with_newer_content_idempotently`: write an
assigned delete plus durable undo entry, restart, run undo recovery, and
require the owned prior cell to reappear at a timestamp greater than the
failed delete.

- [ ] **Step 2: Run recovery and undo tests to verify RED**

Run:

```bash
cargo test --lib ram::recovery::tests::recovery_orders_present_and_deleted_revisions_in_both_scan_directions -- --exact
cargo test --lib server::transactions::undo_log::tests::recovery_compensates_delete_with_newer_content_idempotently -- --exact --nocapture
```

Expected: recovery still indexes the tombstone, or delete compensation cannot
find the recovered physical tombstone after the index is removed.

- [ ] **Step 3: Publish only recovered present winners**

Replace `publish_recovered_current` with:

```rust
pub(crate) fn publish_recovered_present(
    &self,
    id: Id,
    address: usize,
) -> io::Result<()> {
    let mut current = self.cell_index.lock_or_insert(id.lower as usize, 0);
    if *current != 0 && *current != address {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("conflicting recovered current head for {id:?}"),
        ));
    }
    *current = address;
    Ok(())
}
```

In `rebuild_recovered_current`, mark every non-selected physical entry dead.
For selected entries:

```rust
if candidate.kind == RecoveredKind::Present {
    chunk.publish_recovered_present(id, candidate.entry_addr)?;
}
```

A selected tombstone remains a live physical record but leaves `cell_index`
absent and creates no chain.

- [ ] **Step 4: Add recovery-only tombstone lookup**

In `Chunk`, add a private full-ID lookup that scans resident physical
tombstones and deterministically selects the greatest
`(revision_ts, segment.seq_id, entry_address)`:

```rust
fn latest_physical_tombstone(
    &self,
    id: &Id,
) -> Option<(usize, Tombstone)> {
    self.segments()
        .into_iter()
        .flat_map(|segment| {
            let segment_seq_id = segment.seq_id;
            segment.entry_iter().filter_map(move |entry| {
                if entry.entry_header.entry_type != EntryType::TOMBSTONE {
                    return None;
                }
                let tombstone =
                    Tombstone::read_from_entry_content_addr(entry.body_pos);
                (Id::new(tombstone.partition, tombstone.hash) == *id)
                    .then_some((entry.entry_pos, tombstone, segment_seq_id))
            })
        })
        .max_by_key(|(address, tombstone, seq_id)| {
            (tombstone.revision_ts, *seq_id, *address)
        })
        .map(|(address, tombstone, _)| (address, tombstone))
}
```

This helper is used only by recovery diagnostics and startup undo; no direct
operation calls it.

- [ ] **Step 5: Lazily seed only the revision being compensated**

Change `invalidate_recovered_revision` so it accepts a recovered raw present
or an absent raw index with an exact physical tombstone:

```rust
let node = if let Ok(mut guard) = CellGuard::for_read(id.lower, self) {
    let location = guard.get_ptr();
    let header = guard.head_cell().map_err(WriteError::ReadError)?;
    if Id::from_header(&header) != *id
        || header.revision_ts != installed_revision_ts
    {
        return Err(WriteError::CellRevisionMismatch);
    }
    self.ensure_present_predecessor(*id, &header, location)?
} else {
    let (location, tombstone) = self
        .latest_physical_tombstone(id)
        .filter(|(_, tombstone)| {
            tombstone.revision_ts == installed_revision_ts
        })
        .ok_or(WriteError::CellRevisionMismatch)?;
    let node = Arc::new(RevisionNode::new(
        installed_revision_ts,
        RevisionState::CommittedDeleted,
        location,
        TOMBSTONE_ENTRY_SIZE as u32,
    ));
    self.history
        .install(*id, node.clone(), None)
        .map_err(|_| WriteError::CellRevisionMismatch)?;
    node
};
```

Validate the raw mirror/absence and atomically transition that exact node to
`Aborted`, then install the existing newer compensation. A repeated recovery
sees the compensation timestamp instead of `installed_revision_ts` and
performs no duplicate write.

Make `current_revision_ts` check, in order: history current, raw current cell,
then `latest_physical_tombstone`. It must not seed a chain.

- [ ] **Step 6: Run recovery and undo compensation coverage**

Run:

```bash
cargo test --lib ram::recovery::tests::recovery_selects_largest_revision_across_cell_and_tombstone -- --exact
cargo test --lib ram::recovery::tests::recovery_orders_present_and_deleted_revisions_in_both_scan_directions -- --exact
cargo test --lib ram::recovery::tests::recovered_present_and_deleted_snapshots_respect_the_common_floor -- --exact
cargo test --lib server::transactions::undo_log::tests::recovery_compensates_insert_with_newer_tombstone_idempotently -- --exact
cargo test --lib server::transactions::undo_log::tests::recovery_compensates_update_with_newer_content_idempotently -- --exact
cargo test --lib server::transactions::undo_log::tests::restart_compensation_uses_owned_payload_after_source_wal_is_removed -- --exact
cargo test --lib server::transactions::undo_log::tests::recovery_compensates_delete_with_newer_content_idempotently -- --exact
```

Expected: recovery creates no general history, while incomplete inserts,
updates, and deletes receive exactly one newer durable compensation.

- [ ] **Step 7: Commit raw recovery and lazy undo seeding**

```bash
git add src/ram/recovery.rs src/ram/chunk.rs src/server/transactions/undo_log.rs
git commit -m "fix(recovery): lazily seed revisions for undo"
```

---

### Task 5: Run the complete local correctness gate

**Files:**

- Verify: `src/ram/chunk.rs`
- Verify: `src/ram/history.rs`
- Verify: `src/ram/cleaner/combine.rs`
- Verify: `src/ram/cleaner/tests.rs`
- Verify: `src/ram/recovery.rs`
- Verify: `src/ram/tiered/cell_locking.rs`
- Verify: `src/server/transactions/undo_log.rs`

**Interfaces:**

- Consumes all Tasks 1–4.
- Produces a correctness-clean candidate before any remote benchmark.

- [ ] **Step 1: Format only MVCC-owned files**

Run:

```bash
rustfmt --edition 2021 \
  src/ram/chunk.rs \
  src/ram/history.rs \
  src/ram/cleaner/combine.rs \
  src/ram/cleaner/tests.rs \
  src/ram/recovery.rs \
  src/ram/tiered/cell_locking.rs \
  src/ram/tests/cell.rs \
  src/server/transactions/undo_log.rs
```

Expected: the user-owned ranged-index files remain byte-for-byte untouched.

- [ ] **Step 2: Run storage suites serially**

Run each command separately:

```bash
cargo test --lib ram::tests::cell -- --test-threads=1
cargo test --lib ram::history -- --test-threads=1
cargo test --lib ram::cleaner::tests -- --test-threads=1
cargo test --lib ram::recovery::tests -- --test-threads=1
```

Expected: every non-ignored test passes.

- [ ] **Step 3: Run transaction and undo suites serially**

Run each command separately:

```bash
cargo test --lib server::transactions::undo_log -- --test-threads=1
cargo test --lib server::transactions::data_site -- --test-threads=1
cargo test --lib server::transactions::occ_tests -- --test-threads=1
```

Expected: read validation, lost-update rejection, pending visibility,
distributed commit, compensation, retry, and recovery tests pass.

- [ ] **Step 4: Run compilation and structural checks**

Run:

```bash
cargo check --lib
git diff --check
rg -n 'CELL_INDEX_.*TAG|direct_deleted_heads|direct_deleted_cell_index_word|compare_exchange_current_only_tombstone_address' src/ram
```

Expected: compilation succeeds, `git diff --check` is silent, and the final
search has no matches.

- [ ] **Step 5: Audit cleaner invariants from the final diff**

Confirm in the diff:

- raw current cells relocate through `cell_index`;
- MVCC historical cells and tombstones relocate through revision nodes;
- MVCC current-present relocation reconciles history and the raw mirror;
- direct tombstones copy only while the sequence watermark is live;
- a lost relocation destination is dead-accounted exactly once;
- unresolved history/raw reconciliation retains source segments.

Record the exact passing commands and counts in the task handoff.

---

### Task 6: Extend the pure direct benchmark portfolio

**Files:**

- Modify: `Cargo.toml`
- Modify: `benches/occ_support/workloads.rs`
- Modify: `benches/occ_transactions.rs`
- Modify: `tests/occ_bench_metrics.rs`

**Interfaces:**

- Produces scenarios:
  `mvcc/non_transactional_write`,
  `mvcc/non_transactional_read`,
  `mvcc/non_transactional_update`,
  `mvcc/non_transactional_upsert`,
  `mvcc/non_transactional_conditional_update`,
  `mvcc/non_transactional_remove`, and
  `mvcc/non_transactional_delete_recreate`.
- Preserves byte-identical harness logic between the non-MVCC OCC baseline
  and candidate.
- Produces a benchmark-only `mvcc_revision_api` Cargo feature: disabled on
  the OCC baseline to call `compare_version_*`, enabled on MVCC to call
  `compare_revision_*`. It adds no product compatibility API.

- [ ] **Step 1: Add workload accounting tests**

For each direct workload, assert:

```rust
assert_eq!(batch.attempts, operations);
assert_eq!(batch.committed, operations);
assert!(batch.unexpected.is_empty());
assert!(batch.invariants_passed);
```

For remove, seed before the timer and recreate after the timer. For write,
allocate fresh IDs before the timer. For delete/recreate, include both
operations in the measured batch. Keep cleaner maintenance and fixture reset
outside measured elapsed time.

- [ ] **Step 2: Run metric tests to verify RED**

Run:

```bash
cargo test --test occ_bench_metrics -- --test-threads=1
```

Expected: new workload functions or scenario accounting are missing.

- [ ] **Step 3: Implement direct workloads using public APIs**

Follow the existing `BatchResult` and
`run_storage_bounded_non_transactional_update_batch` patterns. Each workload
must call only the direct public API being measured and must return all
operation failures in `unexpected`.

Add this empty adapter feature to the shared `Cargo.toml`:

```toml
mvcc_revision_api = []
```

Keep both adapter branches in the byte-identical benchmark source:

```rust
#[cfg(feature = "mvcc_revision_api")]
async fn compare_and_update(
    fixture: &OccFixture,
    id: &Id,
    token: u64,
    cell: &mut OwnedCell,
) -> Result<CellHeader, WriteError> {
    fixture.servers[0]
        .chunks()
        .compare_revision_and_update_cell(id, token, cell)
}

#[cfg(not(feature = "mvcc_revision_api"))]
async fn compare_and_update(
    fixture: &OccFixture,
    id: &Id,
    token: u64,
    cell: &mut OwnedCell,
) -> Result<CellHeader, WriteError> {
    fixture.servers[0]
        .chunks()
        .compare_version_and_update_cell(id, token, cell)
}
```

Likewise, obtain `token` from `header.revision_ts` in the enabled branch and
`header.version` in the disabled branch. The timed loop and all accounting
remain common code.

Register the seven scenario names in `SCENARIOS` and dispatch them from the
existing direct fixture. Do not add transaction begin/prepare/commit/end calls
to these scenarios.

- [ ] **Step 4: Run benchmark harness tests**

Run:

```bash
cargo test --test occ_bench_metrics -- --test-threads=1
```

Expected: accounting, bounded storage, and invariant tests pass.

- [ ] **Step 5: Commit the benchmark harness separately**

```bash
git add \
  Cargo.toml \
  benches/occ_support/workloads.rs \
  benches/occ_transactions.rs \
  tests/occ_bench_metrics.rs
git commit -m "bench(mvcc): cover pure direct mutations"
```

This commit is applied byte-for-byte to both disposable comparison trees.

---

### Task 7: Run accept-grade benchmarks on `192.168.10.87`

**Files:**

- Generate only under: `target/occ-bench/`
- Do not commit generated JSON, logs, Criterion output, bundles, or `perf.data`.

**Interfaces:**

- Consumes: correctness-clean candidate and shared benchmark commit.
- Produces: three baseline and three candidate reports with exact provenance.

- [ ] **Step 1: Build matched disposable trees**

Create a non-MVCC OCC baseline tree and a candidate tree. Apply the benchmark
commit from Task 6 identically. Record:

```bash
git rev-parse HEAD
git -C ../bifrost rev-parse HEAD
sha256sum \
  benches/occ_transactions.rs \
  benches/occ_support/workloads.rs \
  tests/occ_bench_metrics.rs
```

Expected: the three harness hashes match between trees. Product and Bifrost
revisions are recorded explicitly.

- [ ] **Step 2: Transfer without pushing**

Use git bundles or `rsync` to transfer the two exact trees to
`192.168.10.87`. Do not change source on the benchmark host. Confirm no
benchmark process is already running before each run.

- [ ] **Step 3: Run three serialized baseline portfolios**

From the remote baseline tree, run three separate commands with labels
`direct-base-1`, `direct-base-2`, and `direct-base-3`:

```bash
numactl --cpunodebind=0 --membind=0 env \
  CARGO_TARGET_DIR='/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/direct-build' \
  NEB_OCC_BENCH_BASE_PORT=39400 \
  NEB_OCC_BENCH_LABEL='direct-base-1' \
  NEB_OCC_BENCH_REVISION="product-$(git rev-parse HEAD)+bifrost-$(git -C ../bifrost rev-parse HEAD)" \
  cargo bench --bench occ_transactions -- \
  '^mvcc/non_transactional_(write|read|update|upsert|conditional_update|remove|delete_recreate)$'
```

Expected: each run exits 0 and writes seven clean scenarios.

- [ ] **Step 4: Run three serialized candidate portfolios**

Repeat the exact command from the remote candidate tree with labels
`direct-mvcc-1`, `direct-mvcc-2`, and `direct-mvcc-3`, changing only exact
candidate provenance and adding `--features mvcc_revision_api` before
`--bench`. This feature selects only the benchmark adapter; the timed workload
and product implementations remain unchanged.

Expected: each run exits 0 and writes seven clean scenarios.

- [ ] **Step 5: Validate and compare reports**

For all six reports require seven scenarios, positive commits, passed
invariants, and an empty `unexpected` list. Run:

```bash
scripts/compare-mvcc-benchmarks.sh \
  target/occ-bench/direct-base-1.json \
  target/occ-bench/direct-base-2.json \
  target/occ-bench/direct-base-3.json \
  -- \
  target/occ-bench/direct-mvcc-1.json \
  target/occ-bench/direct-mvcc-2.json \
  target/occ-bench/direct-mvcc-3.json
```

The comparator validates inventory, correctness, and CV. Apply the stricter
design gate afterward: if any direct scenario shows a repeatable throughput
decrease or p99 increase, rerun the matched pair to distinguish noise; reject
the implementation if the regression reproduces. Both populations require
throughput CV below 5%.

- [ ] **Step 6: Run the transactional smoke portfolio**

Using the candidate only, run:

```bash
cargo bench --bench occ_transactions -- \
  '^mvcc/(read_only_current|rmw_one_cell|rmw_multi_cell|multi_participant|blind_update|blind_remove|full_read|selected_read|head_read|partial_read|hlc_contention)$'
```

Expected: every invariant passes, `unexpected` is empty, and no distributed
phase has been removed. Transactional optimizations are retained only when
their separately repeated improvement exceeds 1% with CV below 5% and no p99
regression.

- [ ] **Step 7: Record the final decision**

Record exact revisions, host, NUMA placement, commands, report paths,
throughput medians, p99 medians, CVs, and the keep/reject decision. Leave all
generated evidence uncommitted and do not push.
