# Spine-structural split — design (exploration)

## Goal

Make a tree split truly O(height) = O(log n), removing the last
single-threaded segment of the migration tail. The current structural split
(`split_off`) re-parents the leaves `>= pivot` but rebuilds a spine over all
of them — `capture_moved_chain` walks every moved leaf, so it is
O(moved leaves) ≈ O(n / node_size). For a billion-key first split that is ~4M
leaf touches on one thread.

## Idea: cut the spine instead of rebuilding it

Only the nodes on the **root-to-pivot path** need to change — one per level.
Descend from the root to the boundary leaf. At each node on the path, split it
into a *left part* (children whose keys are `< pivot`, kept in the source) and
a *right part* (children `>= pivot`, moved). Every subtree entirely to the
right of the path moves **by pointer**, untouched. The new tree's spine is
assembled from the right parts, nested by the recursion: the root's right part
is the new root; its first child is the next level's right part; and so on
down to the boundary leaf's right part over the moved leaves.

Work per level: one node split + O(node_size) pointer moves. Total
O(height · node_size) = O(log n). No leaf is walked.

The reader-safety story is unchanged from `split_off`: the source is frozen
(migration marker), no subtree is freed (moved subtrees are shared by the new
spine), and the path-node mutations bump versions under write latches — so
`SeqlockCursor.tla` / `SeqlockReclaim.tla` / `StructuralSplit.tla` (freeze
necessity) already cover it. Only the seam needs fixing: sever the leaf chain
at the boundary and the internal `right`/`right_bound` links on the path.

## The blocker: key counts

`BPlusTree::len` (used by `count()` → the oversized check and stats) must be
apportioned between the two trees after a split. The current `split_off` gets
`moved_len` for free because it already walks every moved leaf. A spine split
does **not** walk them, so it has no cheap exact count of the moved subtrees —
and walking to count would reintroduce the O(leaves) cost we are trying to
remove.

Options:

1. **Store subtree key counts in `InNode`.** Add a `count` (sum of keys in the
   subtree) to each internal node, maintained O(height) per insert and updated
   on split/merge. Then a spine split sums the moved children's `count`s in
   O(node_size) per level — exact and O(log n). This is the clean solution but
   touches the insert hot path and every structural op that moves pointers.
2. **Approximate `len`.** `mid_key` is a near-median, so `moved ≈ len/2`.
   Cheap and O(1), but the error compounds over a halving cascade and could
   make a tree mis-judge `oversized` (never splitting → unbounded growth, or
   splitting spuriously). Unacceptable without a bounded-drift reconciler.
3. **Deferred exact count.** Split with an estimate, then reconcile the two
   `len`s in the background (a bounded walk amortized over the checkpoint
   interval). Adds a background O(n) pass.

Option 1 is the right long-term answer; it is also the most invasive. Given
the current soak already passes with depth-2 + leaf-rebuild split, the spine
split only pays off at depth-3+ (few, large shards), so this is an
exploration gated on whether the subtree-count change is worth it.

## Plan

1. Prototype the spine-cut recursion (`split_off_spine`) counting via an
   O(leaves) walk *for validation only*, and differential-test that it
   produces the same exact partition as `split_off` — this proves the
   spine-cut structure is correct independent of the count question.
2. If correct, add `InNode::count` (option 1) and switch the count to
   O(node_size)-per-level, making the whole thing O(log n).
3. Re-point the balancer at `split_off_spine`, re-enable depth-3, soak.

Step 1 is cheap and de-risks the hard part (the recursion). Steps 2–3 are the
investment, taken only if step 1 validates.
