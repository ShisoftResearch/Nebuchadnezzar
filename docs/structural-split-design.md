# Structural tree split — design

## Problem

Migration currently splits an oversized tree by **copying keys**: scan the
upper half and `merge_keys` them into a brand-new tree, then `retain` the
lower half in the source. That is O(n) per split, and a halving cascade over
a billion-key tree is O(n·log(n/capacity)) of single-threaded key copying —
the multi-minute migration tail seen in the soak. Parallelizing the balancer
did not help (the early cascade is inherently serial) and introduced an
epoch-consistency regression, so it was reverted.

## Key insight

A ranged tree's **durable state is its leaf (external) chain**; internal
nodes are never persisted — they are rebuilt in memory from the leaf chain on
load (`reconstruct.rs`, `TreeConstructor`). So a tree *is* its leaf chain plus
a rebuilt spine.

Therefore a split at pivot P does not need to move keys at all:

- The source keeps the leaves whose keys are `< P`.
- The leaves whose keys are `>= P` become a **new tree** — re-parented under a
  freshly built spine (`TreeConstructor`), sharing the exact same leaf node
  objects.

Cost drops from O(n) key copies to O(n / node_size) leaf re-parenting plus
O(spine) construction — ~128× cheaper at a 128-key node — which makes each
migration milliseconds, so the serial cascade is no longer a problem and
there is nothing to parallelize.

## The split procedure (balancer, source frozen)

Migration already freezes the source: while `prop.migration.is_some()`,
`apply_in_ranged_tree` returns `OpResult::Migrating` and rejects writes, so
**no concurrent writer touches the source during the split**. Only optimistic
readers (seeks/cursors that began before the freeze) may still traverse it;
new reads retry against fresh placement.

1. **Locate the boundary leaf.** Descend to the leaf that would contain P
   (`mut_search`). Walk right if needed so the boundary leaf is the first leaf
   whose `right_bound > P` — i.e. the leaf straddling or just left of P.
2. **Clean-cut vs. split-leaf.**
   - If P equals the boundary leaf's first key, the cut is clean: the boundary
     leaf and everything right of it move.
   - Otherwise split the boundary leaf under its write latch: keys `< P` stay
     (left leaf, truncated `len`, `right_bound = P`), keys `>= P` go to a new
     right leaf inserted into the chain. This is the existing `split_insert`
     shape without an inserted key.
3. **Sever the chain.** Under write latches: `left_last.next = Nil`,
   `right_first.prev = Nil`. The two leaf chains are now independent.
4. **Build the new spine.** Feed the right-half leaves to a `TreeConstructor`
   in order → `(new_root, new_height)`. Construct the new tree with
   `BPlusTree::from_root(new_root, right_first.id, new_len, new_height, …)`.
5. **Truncate the source spine.** Drop the source's internal pointers that
   covered the moved leaves and fix its right-most `right_bound` to P — the
   internal-node half of the existing `retain` logic (`split::retain`), which
   already handles emptying the spine down the right edge.
6. **Publish.** As today: create the target's metadata cell, `mark_migration`,
   SM `split`, `ensure_split_target_loaded`, bump the source epoch, clear the
   marker. The leaf cells are already persisted (they were never moved), so
   only the new tree's head cell and the source's changed spine-edge leaves
   need write-back.

`len` bookkeeping: the source loses the moved leaves' key counts; the new tree
gets them. Both are counted while walking the right leaves in step 4.

## Concurrency argument (what the model must check)

During the split the source has **no writers**, only optimistic readers. The
structural mutations (split boundary leaf, sever chain, truncate spine) all
happen under node **write latches**, which bump node versions — so any
optimistic reader that observed a pre-mutation pointer fails its version
re-check and retries (the seqlock protocol, `SeqlockCursor.tla`). Severed
leaves and replaced spine nodes are retired through **epoch reclamation**
(`SeqlockReclaim.tla`), so a reader pinned before the split keeps valid
memory.

The two new properties a structural split introduces, beyond the existing
seqlock/epoch guarantees:

- **P1 — partition completeness.** Every key present before the split is,
  after it, in exactly one of {source, new tree}, on the correct side of P,
  reachable from that tree's root. No key lost or duplicated.
- **P2 — reader consistency under chain severing.** A cursor walking the leaf
  `next`/`prev` chain concurrently with step 3 must not (a) follow a dangling
  pointer, or (b) cross from the source's leaves into the new tree's leaves
  (which would let one scan observe keys from both trees, i.e. an inconsistent
  boundary). Because the source is frozen and post-split reads for the moved
  range retry via `Migrating`, the surviving concern is an in-flight cursor
  that snapshotted a leaf whose `next` is being rewritten to `Nil`.

`docs/tla/StructuralSplit.tla` models P1 and P2 with a small tree and a
concurrent chain-walking reader; `SeqlockCursor.tla` / `SeqlockReclaim.tla`
already cover the version-retry and reclamation substrate this builds on.

## Why this also fixes the v9 regression

The epoch-mismatch failure came from migrations being slow enough that the
SM-split→epoch-bump window stayed open for minutes, exhausting client
retries. A structural split completes in milliseconds, so that window is tiny
again — the same reason the serial copy-migration at depth 2 passed. No
concurrency is added to the balancer.

## Rollout

1. Model P1/P2 in TLA+, check.
2. Implement `BPlusTree::split_off(pivot) -> (BPlusTree, moved_len)` behind the
   model, with a randomized differential test: split a tree at many pivots,
   assert the two resulting trees partition the keys exactly and both verify
   in order.
3. Swap the balancer's scan+merge_keys+retain block for `split_off` + wrap the
   moved root in the new `DistTree`. Keep all placement/SM coordination
   unchanged (serial migration).
4. Soak on Genoa.
