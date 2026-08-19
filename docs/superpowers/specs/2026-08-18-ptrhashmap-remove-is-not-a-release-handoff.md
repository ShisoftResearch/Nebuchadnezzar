# Handoff: `PtrHashMap::remove` is not a release

**Status:** Nebuchadnezzar's exposure is fixed. Lightning is unchanged, deliberately.
This document exists so the next person meets the sharp edge with a map instead of a
mystery.

**Date:** 2026-08-18
**Repos:** Lightning (`src/map/ptr_map.rs`), Nebuchadnezzar (consumer)

---

## 1. The behaviour, in one sentence

`PtrHashMap::remove` returns a **clone** of the value and leaves the original in the
retired node, where it is destroyed only when that node is **reused** or the **map is
dropped** — so removing an entry does not release what its value owns.

Measured directly (`neb::server::tests::ptr_hash_map_remove_does_not_release_the_value`,
and `lightning::map::ptr_map::value_release_semantics`):

| step | `Arc::strong_count` |
|---|---|
| `insert` | 2 |
| `remove` | **3** — ours, the retired node's original, and the returned clone |
| drop what `remove` returned | 2 |
| **900 further insert/remove cycles** | **2** — nothing reclaimed |
| drop the map | 1 |

The library says so itself, at `src/map/ptr_map.rs:1465`:

```rust
// Clone the value to return it
// The original stays in the node and will be dropped when:
// 1. The node is reused (make_ref_val drops it)
// 2. The map is dropped (Drop impl cleans up freed nodes)
let val = (*ptr).clone();
self.retire_node(node_ptr, epoch, &guard);
```

## 2. This is not a bug. Read this before "fixing" it.

Dropping the value at removal time would be a **use-after-free**: a concurrent reader may
still be dereferencing that node. The QSBR scheme (`src/map/ptr_map.rs:138`) exists to make
the eventual drop safe — a node retired at epoch R is not reused until every active reader
has announced a newer epoch.

What QSBR guarantees is that the drop is **safe**. It does not guarantee that it is
**prompt**, because the drop is coupled to node *reuse*. For a map that outlives its
removals — the normal case — that means never.

`remove_rt_ref` is not an escape hatch either: its `PtrRef` drop calls `retire_owned`,
which buffers the node for free in exactly the same way.

## 3. Where it bit us

Two places in Nebuchadnezzar, both fixed consumer-side in `a6f84c8a`:

- **`NebServer.database_runtimes: PtrHashMap<String, Arc<DatabaseRuntime>>`** lives as long
  as the server, so **every database ever unloaded kept its entire memory store alive** —
  mmap'd chunks included — for the life of the process. Reachable in production through
  Morpheus's `unload_runtime` and `drop_database`, clusterwide. Traced through the unload,
  the refcount went 2 → 3 (the removal's clone) → 2 (services released) → **1, and stayed**.
- **`LOCAL_TREE_SERVICES: PtrHashMap<String, Arc<TreeService>>`** was worse: nothing ever
  removed from it at all, so every database's tree service and every B+tree page its trees
  had loaded stayed resident forever.

Both now use `parking_lot::RwLock<HashMap<..>>`, which drops on removal like anything else.

**The cost of that change was nothing**, because neither map is on a hot path. Measured on
.239, 6 quiet rounds each: write 44387 → 43912 cells/s (−1.1%), reshard 41.9 → 43.9 MB/s
(+4.8%) — opposite directions, both inside noise. The live call sites run **twice per
reshard**, not per cell.

## 4. What is still true for everyone else

Any `PtrHashMap` whose values own real resources has this shape. In Nebuchadnezzar the
remaining users were audited:

| field | value | verdict |
|---|---|---|
| `TieredMemoryManager.chunk_states` | `Arc<ChunkTierState>` | **left alone** — two atomics and a mutex; and it *is* on eviction paths, so a lock there is not free |
| `NebServer.registered_schema_services` | `HashSet<String>` | fine, strings |

**The rule to carry:** *do not store a value that owns real resources in a `PtrHashMap` and
expect `remove` to release it.* If the map outlives its removals, it does not.

## 5. If someone wants a library-side answer

Options, roughly in increasing risk. **All of them are subject to
`lightning-no-regression-rule`: Lightning is the PPoPP subject, so any change to library
code needs correctness AND performance A/B before it lands.** Test-only additions are fine.

1. **Documentation and a type-level signal.** Cheapest and possibly sufficient. Have
   `remove` return something whose name says the deferral out loud, so the contract is
   visible at the call site rather than in a comment 90 lines away. No behaviour change, no
   perf risk.
2. **Drop the value at the quiescent point rather than at reuse.** The machinery is already
   there — `NEEDS_DROP` gates a whole QSBR path, and `grace_passed` computes exactly the
   boundary. The change is to run the value's drop when a retired node clears its grace
   period, instead of waiting for something to allocate it again. This is the *correct*
   general fix. It touches the reclamation path, which is the hottest, most delicate code
   in the map.
3. **Move the value out on removal instead of cloning it.** Tempting and **wrong as
   stated**: a concurrent reader mid-clone would then read moved-out memory. It would need
   a reader barrier first, which is what the current design deliberately avoids.

Whoever picks this up: option 1 costs nothing and removes most of the trap. Option 2 is the
real fix and should be treated as a performance-sensitive change to the paper's subject, not
a bug fix.

## 6. Reproducing it in thirty seconds

```
cd Nebuchadnezzar
cargo test --lib ptr_hash_map_remove_does_not_release_the_value -- --nocapture
```

The consumer-side test pins the single fact Neb depends on. The fuller version — including
that 900 insert/remove cycles reclaim nothing and only dropping the map does — is
`value_release_semantics` in Lightning.

## 7. Two process notes worth keeping

- A first version of the Lightning test was committed and then reverted, on the grounds
  that Lightning was not what was wrong. The corrected rule from the owner: **tests in
  Lightning are fine; the library code is what must stay stable.** A test that pins a
  dependency's *documented, correct* behaviour is reasonable in either repo — but the fix
  belongs with the consumer that misused it.
- `lightning-ppopp27/` is the paper. Never stage it. Stage Lightning changes by explicit
  path, never `git add -A`.
