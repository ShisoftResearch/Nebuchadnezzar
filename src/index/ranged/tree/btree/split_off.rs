// Structural tree split: partition a tree at `pivot` into the source (keys
// < pivot, kept) and a new tree (keys >= pivot), sharing the same leaf nodes
// instead of copying keys. See docs/structural-split-design.md and
// docs/tla/StructuralSplit.tla. The caller must hold the source frozen (no
// concurrent writers) — the migration marker guarantees this.
use super::leaf_keys::LeafKeys;
use super::node::NodeData;
use super::reconstruct::TreeConstructor;
use super::*;
use std::fmt::Debug;

pub struct SplitOff {
    pub new_root: NodeCellRef,
    pub new_height: usize,
    pub moved_len: usize,
    pub new_head_id: Id,
}

/// Structurally split `tree` at `pivot`. Returns None when nothing moves
/// (pivot is greater than every key). Keys `< pivot` remain in `tree`; keys
/// `>= pivot` are re-parented into the returned root, which shares `tree`'s
/// existing leaf nodes.
pub fn split_off<KS, PS>(tree: &BPlusTree<KS, PS>, pivot: &EntryKey) -> Option<SplitOff>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut ctx = SplitCtx {
        constructor: TreeConstructor::new(),
        moved_len: 0,
        head_id: None,
    };
    let root_kept = split_off_by_node::<KS, PS>(tree, &tree.get_root(), pivot, &mut ctx);
    if ctx.moved_len == 0 {
        return None;
    }
    if !root_kept {
        // Every key moved; the source root still points at a now-moved node.
        // Replace it with a fresh empty leaf so the source is a valid empty
        // tree (its head id is unchanged).
        let empty = NodeCellRef::new(Node::<KS, PS>::new_external(
            tree.head_page_id,
            max_entry_key(),
        ));
        *tree.root.write() = empty;
        tree.height.store(0, std::sync::atomic::Ordering::Release);
    } else {
        // Spine truncation may have collapsed the root to a single-child
        // Empty bypass whose `left` points at that child. The tree's root
        // has no left sibling, so clear it (matches an empty/leaf root) —
        // is_tree_in_order requires the leftmost node's left to be Nil.
        let root_ref = tree.get_root();
        let mut root_guard = write_node::<KS, PS>(&root_ref);
        if root_guard.is_empty_node() {
            if let Some(left) = root_guard.left_ref_mut() {
                *left = NodeCellRef::default();
            }
        }
    }
    let new_root = ctx.constructor.root();
    let new_height = ctx.constructor.levels();
    tree.len.fetch_sub(ctx.moved_len, std::sync::atomic::Ordering::Release);
    Some(SplitOff {
        new_root,
        new_height,
        moved_len: ctx.moved_len,
        new_head_id: ctx.head_id.expect("moved leaves imply a head id"),
    })
}

struct SplitCtx<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    constructor: TreeConstructor<KS, PS>,
    moved_len: usize,
    head_id: Option<Id>,
}

// Walk the moved leaf chain starting at `head`, fixing the head's prev to Nil
// and feeding every leaf to the constructor in order. Counts moved keys. Only
// the head's pointers change, so only it is marked for write-back.
fn capture_moved_chain<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    head: NodeCellRef,
    ctx: &mut SplitCtx<KS, PS>,
) where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut cur = head;
    let mut first = true;
    while !cur.is_default() {
        let (next, len, first_key, is_ext) = {
            let mut guard = write_node::<KS, PS>(&cur);
            match &mut *guard {
                &mut NodeData::External(ref mut n) => {
                    if first {
                        // Only the head's prev pointed back into the source;
                        // the internal chain links stay intact for the new tree.
                        n.prev = NodeCellRef::default();
                    }
                    (n.next.clone(), n.len, n.keys.key_at(0), true)
                }
                // The moved chain is a leaf chain; nothing else should appear.
                _ => {
                    debug_assert!(false, "moved chain reached a non-external node");
                    (NodeCellRef::default(), 0, min_entry_key(), false)
                }
            }
        };
        if !is_ext {
            break;
        }
        if first {
            // The head's prev pointer changed; persist it.
            external::make_changed(&cur, tree);
        }
        if len > 0 {
            if first {
                ctx.head_id = Some(read_unchecked::<KS, PS>(&cur).ext_id());
            }
            ctx.constructor.push_extnode(&cur, first_key);
            ctx.moved_len += len;
            first = false;
        }
        cur = next;
    }
}

// Returns whether this subtree keeps anything in the source.
fn split_off_by_node<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node_ref: &NodeCellRef,
    pivot: &EntryKey,
    ctx: &mut SplitCtx<KS, PS>,
) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    match &*read_unchecked::<KS, PS>(node_ref) {
        &NodeData::External(_) => {
            let mut node = write_node::<KS, PS>(node_ref);
            let n = node.extnode_mut(tree);
            let key_index = n.search(pivot);
            if key_index >= n.len {
                // Every key in this leaf is < pivot: it stays. Everything
                // right of it moves; sever and capture.
                let moved_first = mem::take(&mut n.next);
                drop(node);
                // The severed forward link must reach disk: the moved chain
                // now belongs to the sibling tree, which will rewrite and
                // delete these pages on its own schedule. An unpersisted cut
                // leaves this tree's on-disk chain dangling into the
                // sibling's deleted pages after a restart (TB16: MissingPage
                // on the genesis tree, sidecar metadata unreachable).
                external::make_changed(node_ref, tree);
                capture_moved_chain::<KS, PS>(tree, moved_first, ctx);
                return true;
            }
            if key_index == 0 {
                // The whole leaf moves. Detach it as the new chain head; the
                // parent drops its pointer as the recursion unwinds.
                let prev_ref = n.prev.clone();
                let captured_next = mem::take(&mut n.next);
                n.prev = NodeCellRef::default();
                n.next = captured_next;
                let this_ref = node.node_ref().clone();
                drop(node);
                // Sever the source predecessor's forward link into the moved
                // chain, if this leaf was not the source head.
                if !prev_ref.is_default() {
                    {
                        let mut prev = write_node::<KS, PS>(&prev_ref);
                        if let Some(nx) = prev.right_ref_mut() {
                            *nx = NodeCellRef::default();
                        }
                    }
                    external::make_changed(&prev_ref, tree);
                }
                capture_moved_chain::<KS, PS>(tree, this_ref, ctx);
                return false;
            }
            // The leaf straddles pivot: keys[0..key_index] stay, keys[key_index..]
            // become a fresh leaf that heads the moved chain.
            let moved_keys = n.keys.to_vec(key_index..n.len);
            let captured_next = mem::take(&mut n.next);
            let right_bound = mem::replace(&mut n.right_bound, pivot.clone());
            n.len = key_index;
            let new_id = BPlusTree::<KS, PS>::new_page_id_near(&n.id);
            let mut moved_leaf = ExtNode::<KS, PS>::new(new_id, right_bound);
            moved_leaf.keys = LeafKeys::from_keys(&moved_keys, KS::slice_len());
            moved_leaf.len = moved_keys.len();
            moved_leaf.next = captured_next.clone();
            moved_leaf.prev = NodeCellRef::default();
            drop(node);
            // The truncated boundary leaf changed on every axis that matters
            // on disk: len shrank, right_bound moved to the pivot, and next
            // was severed. Persist it, or a reload resurrects the moved keys
            // AND follows the stale link into the sibling's pages.
            external::make_changed(node_ref, tree);
            let moved_ref = NodeCellRef::new(Node::with_external(moved_leaf));
            // The leaf that followed the boundary now sits behind the freshly
            // created head; repoint its back-link into the new tree.
            if !captured_next.is_default() {
                {
                    let mut nxt = write_node::<KS, PS>(&captured_next);
                    if let Some(prev) = nxt.left_ref_mut() {
                        *prev = moved_ref.clone();
                    }
                }
                external::make_changed(&captured_next, tree);
            }
            capture_moved_chain::<KS, PS>(tree, moved_ref, ctx);
            true
        }
        &NodeData::Internal(ref n) => {
            let index = n.search(pivot);
            let child_kept = split_off_by_node::<KS, PS>(
                tree,
                &n.ptrs.as_slice_immute()[index],
                pivot,
                ctx,
            );
            if index >= n.len && child_kept {
                // pivot is past this node's separators and the covering child
                // kept everything — nothing to truncate here.
                return true;
            }
            // Truncate the source spine: drop the pointers to moved children
            // and any right sibling subtrees. Identical to retain's internal
            // handling; the moved leaves survive because the new spine
            // (constructor) already references them.
            let mut node = write_node::<KS, PS>(node_ref);
            let mut right_node_ref = {
                let innode = node.innode_mut();
                let original_len = innode.len;
                let kept_children = if child_kept { index + 1 } else { index };
                for ptr in innode.ptrs.as_slice()[kept_children..=original_len].iter_mut() {
                    *ptr = NodeCellRef::default();
                }
                let right_node_ref = mem::take(&mut innode.right);
                if kept_children == 0 {
                    *node = NodeData::Empty(Box::new(Default::default()));
                } else if kept_children == 1 {
                    let child = innode.ptrs.as_slice_immute()[0].clone();
                    *node = NodeData::Empty(Box::new(node::EmptyNode {
                        left: Some(child.clone()),
                        right: child,
                    }));
                } else {
                    innode.len = kept_children - 1;
                    innode.right_bound = pivot.clone();
                }
                right_node_ref
            };
            drop(node);
            while !right_node_ref.is_default() {
                let mut rn = write_node::<KS, PS>(&right_node_ref);
                right_node_ref = mem::take(rn.right_ref_mut().unwrap());
                *rn = NodeData::Empty(Box::new(Default::default()));
            }
            child_kept || index > 0
        }
        &NodeData::Empty(ref n) => split_off_by_node::<KS, PS>(tree, &n.right, pivot, ctx),
        &NodeData::None => false,
    }
}

// ---------------------------------------------------------------------------
// Spine-structural split prototype (docs/spine-split-design.md).
//
// Cuts the root-to-pivot path instead of rebuilding a spine over the moved
// leaves: each node on the path splits into a left part (kept) and a right
// part (moved); whole subtrees right of the path move by pointer. O(height)
// node touches. `moved_len` is still counted by walking the moved leaf chain
// here — that O(leaves) walk exists only to validate the structure against
// split_off; the real version needs subtree counts in InNode.
// ---------------------------------------------------------------------------

struct NodeSplitResult {
    kept: bool,
    right: Option<NodeCellRef>,
    right_first_leaf: Option<NodeCellRef>,
}

fn build_internal<KS, PS>(
    ptrs: Vec<NodeCellRef>,
    keys: Vec<EntryKey>,
    right_bound: EntryKey,
) -> NodeCellRef
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    debug_assert_eq!(keys.len() + 1, ptrs.len());
    let mut innode = InNode::<KS, PS>::new(keys.len(), right_bound);
    innode.keys = super::internal::InternalKeys::from_keys(&keys);
    for (i, p) in ptrs.into_iter().enumerate() {
        innode.ptrs.as_slice()[i] = p;
    }
    NodeCellRef::new(Node::with_internal(innode))
}

fn split_node_spine<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node_ref: &NodeCellRef,
    pivot: &EntryKey,
) -> NodeSplitResult
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    match &*read_unchecked::<KS, PS>(node_ref) {
        &NodeData::External(_) => {
            let mut node = write_node::<KS, PS>(node_ref);
            let n = node.extnode_mut(tree);
            let key_index = n.search(pivot);
            if key_index >= n.len {
                // Entirely < pivot; stays. Its right sibling leaves move, so
                // sever this leaf's forward link.
                n.next = NodeCellRef::default();
                drop(node);
                external::make_changed(node_ref, tree);
                NodeSplitResult { kept: true, right: None, right_first_leaf: None }
            } else if key_index == 0 {
                // Whole leaf moves; it heads the moved chain.
                let prev_ref = n.prev.clone();
                n.prev = NodeCellRef::default();
                let this_ref = node.node_ref().clone();
                drop(node);
                if !prev_ref.is_default() {
                    {
                        let mut prev = write_node::<KS, PS>(&prev_ref);
                        if let Some(nx) = prev.right_ref_mut() {
                            *nx = NodeCellRef::default();
                        }
                    }
                    external::make_changed(&prev_ref, tree);
                }
                external::make_changed(&this_ref, tree);
                NodeSplitResult {
                    kept: false,
                    right: Some(this_ref.clone()),
                    right_first_leaf: Some(this_ref),
                }
            } else {
                // Straddle: left keeps [0..key_index], right = new leaf.
                let moved_keys = n.keys.to_vec(key_index..n.len);
                let captured_next = mem::take(&mut n.next);
                let right_bound = mem::replace(&mut n.right_bound, pivot.clone());
                n.len = key_index;
                let new_id = BPlusTree::<KS, PS>::new_page_id_near(&n.id);
                let mut moved_leaf = ExtNode::<KS, PS>::new(new_id, right_bound);
                moved_leaf.keys = LeafKeys::from_keys(&moved_keys, KS::slice_len());
                moved_leaf.len = moved_keys.len();
                moved_leaf.next = captured_next.clone();
                moved_leaf.prev = NodeCellRef::default();
                drop(node);
                let moved_ref = NodeCellRef::new(Node::with_external(moved_leaf));
                if !captured_next.is_default() {
                    {
                        let mut nxt = write_node::<KS, PS>(&captured_next);
                        if let Some(prev) = nxt.left_ref_mut() {
                            *prev = moved_ref.clone();
                        }
                    }
                    external::make_changed(&captured_next, tree);
                }
                external::make_changed(node_ref, tree);
                external::make_changed(&moved_ref, tree);
                NodeSplitResult {
                    kept: true,
                    right: Some(moved_ref.clone()),
                    right_first_leaf: Some(moved_ref),
                }
            }
        }
        &NodeData::Internal(_) => {
            // Snapshot the node's pointers and keys under the read.
            let (index, ptrs, keys, orig_right_bound) = {
                let g = read_unchecked::<KS, PS>(node_ref);
                let n = g.innode();
                let index = n.search(pivot);
                let ptrs = n.ptrs.as_slice_immute()[..n.len + 1].to_vec();
                let keys = n.keys.to_vec(n.len);
                (index, ptrs, keys, n.right_bound.clone())
            };
            let child_res = split_node_spine::<KS, PS>(tree, &ptrs[index], pivot);

            // Moved side: [child_res.right?] ++ ptrs[index+1..].
            let mut right_ptrs: Vec<NodeCellRef> = Vec::new();
            let mut right_keys: Vec<EntryKey> = Vec::new();
            if let Some(cr) = child_res.right.clone() {
                right_ptrs.push(cr);
            }
            for i in (index + 1)..ptrs.len() {
                if !right_ptrs.is_empty() {
                    // separator preceding ptrs[i] is keys[i-1]
                    right_keys.push(keys[i - 1].clone());
                }
                right_ptrs.push(ptrs[i].clone());
            }
            let right = if right_ptrs.is_empty() {
                None
            } else if right_ptrs.len() == 1 {
                Some(right_ptrs.pop().unwrap())
            } else {
                Some(build_internal::<KS, PS>(right_ptrs, right_keys, orig_right_bound))
            };

            // Kept side: ptrs[0..index] ++ (child kept ? ptrs[index] : none).
            let mut kept_ptrs: Vec<NodeCellRef> = ptrs[..index].to_vec();
            let kept_keys: Vec<EntryKey>;
            if child_res.kept {
                kept_ptrs.push(ptrs[index].clone());
                // index+1 ptrs, separators keys[0..index]
                kept_keys = keys[..index].to_vec();
            } else {
                // index ptrs, separators keys[0..index-1]
                kept_keys = if index >= 1 { keys[..index - 1].to_vec() } else { vec![] };
            }

            let mut node = write_node::<KS, PS>(node_ref);
            let kept = if kept_ptrs.is_empty() {
                *node = NodeData::Empty(Box::new(Default::default()));
                false
            } else if kept_ptrs.len() == 1 {
                let child = kept_ptrs[0].clone();
                *node = NodeData::Empty(Box::new(node::EmptyNode {
                    left: Some(child.clone()),
                    right: child,
                }));
                true
            } else {
                let innode = node.innode_mut();
                innode.keys = super::internal::InternalKeys::from_keys(&kept_keys);
                for (i, p) in kept_ptrs.iter().enumerate() {
                    innode.ptrs.as_slice()[i] = p.clone();
                }
                for i in kept_ptrs.len()..(KS::slice_len() + 1) {
                    innode.ptrs.as_slice()[i] = NodeCellRef::default();
                }
                innode.len = kept_ptrs.len() - 1;
                innode.right = NodeCellRef::default();
                innode.right_bound = pivot.clone();
                true
            };
            drop(node);
            NodeSplitResult { kept, right, right_first_leaf: child_res.right_first_leaf }
        }
        &NodeData::Empty(ref n) => split_node_spine::<KS, PS>(tree, &n.right, pivot),
        &NodeData::None => NodeSplitResult { kept: true, right: None, right_first_leaf: None },
    }
}

// Returns (moved_len, height, leftmost_leaf) for the moved subtree.
fn count_and_height<KS, PS>(root: &NodeCellRef) -> (usize, usize, NodeCellRef)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // height: descend leftmost
    let mut height = 0;
    let mut cur = root.clone();
    loop {
        let (descend, is_internal) = match &*read_unchecked::<KS, PS>(&cur) {
            &NodeData::Internal(ref n) => (Some(n.ptrs.as_slice_immute()[0].clone()), true),
            &NodeData::Empty(ref n) => (Some(n.right.clone()), false),
            _ => (None, false),
        };
        match descend {
            Some(c) => {
                if is_internal {
                    height += 1;
                }
                cur = c;
            }
            None => break,
        }
    }
    let first_leaf = cur.clone();
    // count: walk the leaf chain from the leftmost leaf
    let mut len = 0;
    let mut leaf = cur;
    while !leaf.is_default() {
        let (n_len, next) = match &*read_unchecked::<KS, PS>(&leaf) {
            &NodeData::External(ref n) => (n.len, n.next.clone()),
            &NodeData::Empty(ref n) => (0, n.right.clone()),
            _ => break,
        };
        len += n_len;
        leaf = next;
    }
    (len, height, first_leaf)
}

// Read-only simulation of the spine split's height arithmetic. Returns the
// subtree's full height plus the heights of the moved (right) and kept (left)
// parts this node would produce, or None if either part would end up with
// children of differing heights (an unbalanced tree). The spine cut is only
// balanced when the moved/kept part at every level combines subtrees of equal
// height; a mid-leaf pivot whose boundary leaf is the last child of a middle
// subtree collapses one side a level short, which this catches. The migration
// always splits at a root separator (whole subtrees move) and passes.
struct SplitSim {
    full: usize,
    moved_h: Option<usize>,
    kept_h: Option<usize>,
    kept: bool,
}

fn simulate_spine_split<KS, PS>(node_ref: &NodeCellRef, pivot: &EntryKey) -> Option<SplitSim>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    match &*read_unchecked::<KS, PS>(node_ref) {
        &NodeData::External(ref n) => {
            let ki = n.search(pivot);
            Some(if ki >= n.len {
                SplitSim { full: 0, moved_h: None, kept_h: Some(0), kept: true }
            } else if ki == 0 {
                SplitSim { full: 0, moved_h: Some(0), kept_h: None, kept: false }
            } else {
                SplitSim { full: 0, moved_h: Some(0), kept_h: Some(0), kept: true }
            })
        }
        &NodeData::Internal(ref n) => {
            let index = n.search(pivot);
            let child = simulate_spine_split::<KS, PS>(&n.ptrs.as_slice_immute()[index], pivot)?;
            let och = child.full; // children are all this height in a balanced source
            let full = och + 1;
            let num_sibs = n.len - index; // ptrs[index+1 ..= len]

            let num_moved = (child.moved_h.is_some() as usize) + num_sibs;
            let moved_h = if num_moved == 0 {
                None
            } else if num_moved == 1 {
                if child.moved_h.is_some() { child.moved_h } else { Some(och) }
            } else {
                if let Some(c) = child.moved_h {
                    if c != och {
                        return None; // moved part would mix heights
                    }
                }
                Some(och + 1)
            };

            let num_kept = index + (child.kept as usize);
            let kept_h = if num_kept == 0 {
                None
            } else if num_kept == 1 {
                if child.kept { child.kept_h } else { Some(och) }
            } else {
                if child.kept {
                    if child.kept_h != Some(och) {
                        return None; // kept part would mix heights
                    }
                }
                Some(och + 1)
            };

            Some(SplitSim { full, moved_h, kept_h, kept: num_kept > 0 })
        }
        &NodeData::Empty(ref n) => simulate_spine_split::<KS, PS>(&n.right, pivot),
        &NodeData::None => {
            Some(SplitSim { full: 0, moved_h: None, kept_h: None, kept: false })
        }
    }
}

/// Structurally split `tree` at `pivot` in O(height) node touches by cutting the
/// root-to-pivot spine. Same contract as `split_off`. For pivots that would land
/// mid-leaf in a way that unbalances the tree (never produced by the balancer,
/// which splits at a subtree boundary), it falls back to the leaf-rebuild
/// `split_off`, which is always balanced.
pub fn split_off_spine<KS, PS>(tree: &BPlusTree<KS, PS>, pivot: &EntryKey) -> Option<SplitOff>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // Read-only: if the spine cut would unbalance either side, use the proven
    // leaf-rebuild split instead. The balancer's separator pivots always pass.
    if simulate_spine_split::<KS, PS>(&tree.get_root(), pivot).is_none() {
        return split_off::<KS, PS>(tree, pivot);
    }
    let res = split_node_spine::<KS, PS>(tree, &tree.get_root(), pivot);
    let Some(new_root) = res.right else {
        return None;
    };
    let (moved_len, new_height, head) = count_and_height::<KS, PS>(&new_root);
    // The moved chain's head prev pointed back into the source; sever it.
    if !head.is_default() {
        {
            let mut hg = write_node::<KS, PS>(&head);
            if let Some(prev) = hg.left_ref_mut() {
                *prev = NodeCellRef::default();
            }
        }
        external::make_changed(&head, tree);
    }
    let new_head_id = read_unchecked::<KS, PS>(&head).ext_id();
    if moved_len == 0 {
        return None;
    }
    if !res.kept {
        let empty = NodeCellRef::new(Node::<KS, PS>::new_external(
            tree.head_page_id,
            max_entry_key(),
        ));
        *tree.root.write() = empty;
        tree.height.store(0, std::sync::atomic::Ordering::Release);
    } else {
        let root_ref = tree.get_root();
        let mut root_guard = write_node::<KS, PS>(&root_ref);
        if root_guard.is_empty_node() {
            if let Some(left) = root_guard.left_ref_mut() {
                *left = NodeCellRef::default();
            }
        }
    }
    tree.len.fetch_sub(moved_len, std::sync::atomic::Ordering::Release);
    Some(SplitOff { new_root, new_height, moved_len, new_head_id })
}
