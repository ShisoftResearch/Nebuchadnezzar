# B+ Tree Correctness Audit — July 2026

Scope: `src/index/ranged/tree/btree/` (~5.5k lines) — the concurrent B-link
tree behind the ranged index. Method: full code read, targeted probe tests,
TLA+ model checking of the two core protocols, fixes, and benchmarks.

## Architecture recap

- **B-link tree** (Lehman & Yao style): every node carries a `right` sibling
  pointer and a `right_bound`; readers and writers that land on a node whose
  range moved right simply chase the sibling chain. This is what makes
  lock-free descent sound.
- **Latching**: each `Node` has one word `cc` = MSB latch flag + version
  counter. Writers spin-CAS the flag (`write_node`); `NodeWriteGuard::drop`
  releases and bumps the version *unconditionally* (even for no-op writes).
- **Optimistic reads** (`read_node`): snapshot version → run closure over the
  raw node data → re-read version → **re-run the closure** if it changed.
  Consequence: closures must be side-effect free and idempotent.
- **Root splits** are serialized by a latch on the dummy `root_versioning`
  node.
- `NodeCellRef` is a hand-rolled atomic refcount pointer; sibling cycles are
  broken by an eager cascade in `Drop` plus `clear_by_node`.

## Findings and fixes

### F1 (critical): scans skip / reorder keys under concurrent writes
`RTCursor::next_raw_candidate` mutated `self.index`, `self.current`, and
`self.page` **inside** the `read_node` closure. When a writer bumped the page
version mid-read, the closure re-ran with the already-advanced index, so the
scan skipped a key (or yielded out of order). Because releasing a write latch
always bumps the version, even a no-op write (re-inserting an existing key)
could trigger it.

*Demonstrated* by `audit_test::concurrent_scan_does_not_skip_keys` (failed
with out-of-order yields before the fix) and by TLC on
`docs/tla/SeqlockCursor.tla` (`Buggy = TRUE` violates `NoSkip` in a 9-state
trace).

*Fix*: `cursor.rs` rewritten. The cursor now snapshots the reachable part of
a page (keys copied out) inside a single validated read whose closure only
constructs and returns data, then iterates locally and follows the sibling
chain page by page. Side effect: scans got ~2.8× faster (one validated read
per page instead of two per item).

### F2 (high): root-split fix-up path is triple-broken
`check_root_modification` (now replaced) handled the race "my top-level node
split, but another thread grew the tree while I waited for latches":

1. **Double insert**: the function *always* returned `None`, so its
   "handled" outcome was discarded and `insert()` stacked a new root anyway —
   after the pivot had already been inserted into the current root level. The
   new right node ended up with two parents.
2. **Wrong detection**: staleness was detected by comparing *first keys* of
   the current root and the split node. A right-of-root leaf shares its first
   key with the root's first pivot, so the fix-up was silently skipped and a
   spurious root level stacked on top (unbounded height growth under
   contention). Pointer identity is the correct test.
3. **Latent deadlocks**: the fix-up walked the root level while still holding
   the split node's latch (`write_targeted` can walk right into that very
   node) and inserted while holding `root_versioning` (a full target node
   re-latches `root_versioning` inside `InNode::insert`). It also discarded a
   secondary split of the fix-up target.

B-link right pointers mask all of this from *searches* (which is why it
survived), but the structure degrades permanently.

*Demonstrated* by TLC on `docs/tla/BLinkInsert.tla`: `Buggy=TRUE, Stale=TRUE`
violates `SingleParent` (double insert); `Buggy=TRUE, Stale=FALSE` violates
`HeightOK` (spurious root). The fixed model passes `SearchAll`, `Ordered`,
`SingleParent`, `HeightOK` with no deadlock.

*Fix*: `insert::apply_top_level_split` — installs a new root only when the
split node *is* the current root (pointer identity), serialized by
`root_versioning`; otherwise releases both latches first, then walks the
current root level and inserts the pivot, looping if that insert itself
splits.

### F3 (medium): bulk merge duplicates keys through the split path
`merge_into_tree_node`'s full-page branch called `split_insert` without
checking whether the merge key already existed in the page, producing a
duplicate key (the in-place branch dedups via `merge_sort`). Demonstrated by
`audit_test::merge_existing_key_into_full_page_no_duplicate` (tree ordering
verification failed before the fix). Also fixed the `len` accounting drift:
duplicates dropped by `merge_sort` (which now returns the count) and skipped
by the split path are subtracted, since `merge_with_keys_` adds the full
batch size.

### F4 (medium): backward seeks fabricate or mis-position
- Backward seek on an **empty tree** returned a phantom all-zero key read
  from an uninitialized slot (`audit_test::backward_seek_on_empty_tree_is_empty`).
- Backward seek for a key **smaller than every stored key** returned the
  smallest key instead of an empty cursor
  (`audit_test::backward_seek_before_min_is_empty`).

Both fixed by the cursor rewrite: position normalization happens against the
snapshot (`largest key <= seek key`, falling through to the previous page,
which is `Nil` in these cases). Mid-tree gap seeks were verified correct
before and after (`backward_seek_gap_key_returns_predecessor`).

### F5 (medium, UB): `Slice::as_slice_immute` manufactured `&mut` from `&`
The trait default did `self as *const Self as *mut Self` and called
`as_slice(&mut self)` — undefined behavior under Rust's aliasing rules, used
pervasively on hot paths. Fixed: the `impl_slice_ops!` macro now implements
`as_slice_immute` directly (`&self -> &[T]`), and the UB default was removed
from the trait.

### F6 (high): write-back worker fleet dies with its runtime and never respawns
Found while chasing a "deadlocked" `migration_stress_insert_only` run: the
process was fully parked with `CHANGE_PROGRESS` frozen exactly at the value
where the previous test ended. `start_external_nodes_write_back` spawned its
workers once per process (`WB_STARTED` latch) onto whichever tokio runtime
called it first; when that runtime is dropped (test teardown, in-process
restart) the workers die silently while the latch stays set, so every later
server instance runs with zero write-back workers and `wait_until_updated`
polls forever.

*Fixes* (`storage.rs`): live-worker accounting via RAII guards with
respawn-on-zero; drop-safe completion recording (a popped change id is
recorded even if persisting panics or the task is cancelled — one lost id
used to stall the progress chain permanently); worker panics caught and
logged; `remove_cell().unwrap()` replaced with logged errors;
`wait_until_updated` bails out with a warning when no worker is alive.
`ExtNode::to_cell` also no longer panics on `Empty` tombstone neighbors
(that panic previously killed a worker task).

**Process note — a deadlock introduced and caught during this audit.** The
first version of the `to_cell` neighbor fix resolved the *left* neighbor
with a waiting `read_node` while holding the node's own write latch. That
leftward wait closes a cycle with the tree's global left-before-right latch
order (splits and `write_targeted` hold a page while acquiring its right
sibling) and deadlocked the very next stress run — 8 threads spinning in
`write_node`, one persist worker holding a page while read-spinning on its
left neighbor. The original code's `read_unchecked` on `prev` existed
precisely to avoid this. The corrected version waits only rightward;
`docs/tla/PersistLatch.tla` now models this protocol — TLC finds the
deadlock with `WaitLeft = TRUE` and proves the fixed variant deadlock-free.
Lesson applied: latch-order changes get a model before they get committed.

### Follow-up batch (post-audit hardening)

- **Tombstone reclamation** (`remove_contains`): compaction now removes
  tombstoned keys from a page AND drops their tombstones while the page's
  write latch is held (write-back persist and bulk-merge paths; merges
  compact before deciding to split). Cursors filter tombstoned keys inside
  their validated snapshot closures, so reclamation cannot make a scan yield
  a deleted key. Protocol model-checked in `docs/tla/DeletionReclaim.tla`;
  TLC rejected the first design (grace period + recheck-undo loses inserts)
  before it was implemented. Fixes the unbounded DeletionSet growth.
- **Bulk-merge root install** is now serialized with insert-driven root
  splits: upper levels are built against an observed root from fresh nodes
  only, then installed under `root_versioning` after a pointer-identity
  check, rebuilding on conflict (merger process added to
  `docs/tla/BLinkInsert.tla`; all configs re-verified).
- **Layout hardening**: `Node`/`NodeData` are `#[repr(C)]` so the
  cross-instantiation casts in `NodeCellRef::deref` rely on defined layout.
- **Dead rebalance code removed**: `merge_children`, `relocate_children`,
  `rebalance_candidate`, `NodeData::remove` and their helpers (their
  parent-before-child latch order would deadlock against the live paths).

### Epoch-based reclamation (closes the reader use-after-free)

The seqlock reader UAF window is closed: node destruction is deferred
through crossbeam-epoch (`NodeCellRef::drop` queues the destruction
cascade; `read_node` pins the epoch for the whole optimistic read), and
pointers read out of unlatched node data are cloned with
`try_clone_speculative` (increment-only-if-nonzero) so a condemned node can
never be resurrected or double-freed; failed clones retry the optimistic
read. `key_at_right_node` peeks at the sibling through a borrowed reference
without touching its refcount. Protocol modeled in
`docs/tla/SeqlockReclaim.tla`: TLC shows the pre-fix code violates
`NoUseAfterFree`, pin-without-guarded-clone violates `NoResurrection`, and
the shipped variant passes all invariants. Cursor snapshots now also filter
tombstones once per page with an emptiness fast path, which removed the
legacy per-yield hash lookup (scans roughly doubled again). Exclusive-path
walks (level merge pruning, clear, verification, reconstruction) still use
plain clones by design and remain non-concurrent-safe as documented.

### Soak test finding (capacity, pre-existing)

`soak_migration_stress_30m_64_threads` (ignored by default) OOMs on a 62 GB
machine in both the pre-audit and current builds: 64 unthrottled insert
workers accumulate the in-memory tree plus a page-cell version in the
32 GB log-structured chunk for every drained write-back entry, faster than
debug-mode cleaning reclaims. Measured RSS growth is linear:
**~97 MB/s pre-audit vs ~52 MB/s with the audit fixes** (tombstone
compaction shrinks persisted pages), so the audit work roughly halved the
burn rate but the 30-minute run still needs a larger machine. Implemented follow-up: write-back entries are now coalesced per page — a
node-level dirty flag enqueues only on the false-to-true transition, and
persist clears the flag under the page write latch before snapshotting
(model: `docs/tla/DirtyCoalesce.tla`, including the mark-after-latch-release
call sites; verified no lost updates and a bounded per-page queue). This
bounds queue growth and page-cell version churn, but does not make the
30-minute soak fit a 62 GB machine: an smaps breakdown shows the dominant
residual growth is general heap (dozens of 64 MB glibc arenas — service/RPC
allocation churn and allocator retention, ~75+ MB/s at speed, present
pre-audit as well) plus ~20 MB/s of chunk-page touching from cell appends.
A full fix is server-side: allocator strategy (jemalloc/MADV_FREE), debug
cleaner throughput, or bounding ingest; out of scope for the B-tree.

## Known remaining risks (documented, not fixed)

- **Seqlock reads are formally data races.** `read_node` closures read node
  data that a latch holder may be mutating; validation discards the result,
  but under the Rust memory model the read itself is UB (works in practice on
  current hardware; `search_unwindable`'s `catch_unwind` is a band-aid for
  torn reads). A rigorous fix means routing reads through atomics or seqlock
  primitives.
- **`NodeCellRef::deref` casts between `Node<KS, PS>` instantiations** (and
  the all-`None` default node) assuming identical enum layout across generic
  instantiations — not guaranteed by repr(Rust), true in practice.
- **Bulk merge vs. insert-driven root split.** `merge_with_keys_` builds new
  root levels without holding `root_versioning`; a concurrent insert-driven
  root split can interleave. `apply_top_level_split` now degrades gracefully
  (stacks a root over a same-level leaf root; B-link keeps searches correct),
  but the merge path itself still assumes a stable root
  (`debug_assert` only).
- **Cursor on a concurrently-emptied page chain**: an `Empty` tombstone whose
  `right` still points into a cleared region relies on prune order to
  terminate; no live path violates it today.
- **Dead code**: `merge_children`, `relocate_children`, `rebalance_candidate`
  (and `NodeData::remove`) have no callers — remnants of a physical-delete
  design (deletes now go through the `DeletionSet` tombstones). Their latch
  order (parent→child) would deadlock against the live bottom-up paths if
  ever revived.

## Model checking

`docs/tla/` contains both specs and configs; checked with TLC (tla2tools
v1.8.0, Java 21):

| Spec | Config | Result |
|---|---|---|
| SeqlockCursor | Buggy=TRUE | `NoSkip` **violated** (9-state trace = F1) |
| SeqlockCursor | Buggy=FALSE | pass (110 states) |
| BLinkInsert | Buggy=FALSE, organic 4 procs | pass: SearchAll, Ordered, SingleParent, HeightOK, no deadlock (1021 states; fix-up path reachability confirmed) |
| BLinkInsert | Buggy=FALSE, seeded stale | pass (139 states) |
| BLinkInsert | Buggy=TRUE, seeded stale | `SingleParent` **violated** (= F2.1) |
| BLinkInsert | Buggy=TRUE, organic | `HeightOK` **violated** (= F2.2) |
| PersistLatch | WaitLeft=TRUE | **Deadlock reached** (the reverted to_cell variant) |
| PersistLatch | WaitLeft=FALSE | pass, deadlock-free (6448 states) |
| BLinkInsert | Fixed + 1 merger | pass: merger install serialized, no deadlock |
| DeletionReclaim | grace+undo draft | `QuiescentCorrect` **violated** (loses inserts; design rejected) |
| DeletionReclaim | latched compaction | pass with 2 concurrent users (398 states) |
| SeqlockReclaim | NoPin (pre-fix code) | `NoUseAfterFree` **violated** |
| SeqlockReclaim | Pin, plain clone | `NoResurrection` **violated** (resurrected clone) |
| SeqlockReclaim | Pin + try_clone (shipped) | pass |

Run: `java -cp tla2tools.jar tlc2.TLC -config <cfg> <spec>.tla`

## Performance

Optimizations (beyond the cursor rewrite):
- `NodeData::first_key`/`last_key` no longer materialize every key of an
  internal node into a `Vec` to read one key.
- `Slice::insert_at`/`remove_at` use `rotate_right`/`rotate_left`
  (memmove-style) instead of element-by-element swap loops.
- `InternalKeys` search compares the shared prefix once, then binary-searches
  suffixes only (`InternalKeys::search`).
- Seek snapshots copy only the directional tail of the page the cursor can
  still visit.

Benchmarks (`bench_test.rs`, 1M keys, 128-key pages, release, single machine,
3 runs each; run with
`cargo test --release --lib ...bench_test -- --ignored --nocapture`):

| Bench | pre-audit (HEAD) | after fixes + opts | delta |
|---|---|---|---|
| insert sequential | 2.22–2.29M ops/s | 2.39–2.44M ops/s | ~+8% |
| insert random | 0.90–1.12M ops/s | 1.13–1.31M ops/s | ~+19% |
| insert parallel (rayon) | 1.27–2.36M ops/s | 2.32–2.59M ops/s | noisy; ≥ parity |
| point seek | 718–874K ops/s | 703–761K ops/s | ~parity* |
| full scan | 13.3–13.7M ops/s | 36.5–38.5M ops/s | **~2.8×** |

\* the correctness fix requires snapshotting the seek page; the directional
trim claws the cost back to parity.

## Test inventory added

`src/index/ranged/tree/btree/audit_test.rs`:
- `backward_seek_on_empty_tree_is_empty` (F4, failed pre-fix)
- `forward_seek_on_empty_tree_is_empty` (control)
- `backward_seek_before_min_is_empty` (F4, failed pre-fix)
- `backward_seek_gap_key_returns_predecessor` (regression guard)
- `merge_existing_key_into_full_page_no_duplicate` (F3, failed pre-fix)
- `concurrent_scan_does_not_skip_keys` (F1, failed pre-fix; also checks scan
  ordering under write churn)

`src/index/ranged/tree/btree/bench_test.rs`: 5 ignored benchmark tests.
