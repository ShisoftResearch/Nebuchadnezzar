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
// and feeding every leaf to the constructor in order. Counts moved keys.
fn capture_moved_chain<KS, PS>(head: NodeCellRef, ctx: &mut SplitCtx<KS, PS>)
where
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
                    let next = n.next.clone();
                    (next, n.len, n.keys.key_at(0), true)
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
                capture_moved_chain::<KS, PS>(moved_first, ctx);
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
                    let mut prev = write_node::<KS, PS>(&prev_ref);
                    if let Some(nx) = prev.right_ref_mut() {
                        *nx = NodeCellRef::default();
                    }
                }
                capture_moved_chain::<KS, PS>(this_ref, ctx);
                return false;
            }
            // The leaf straddles pivot: keys[0..key_index] stay, keys[key_index..]
            // become a fresh leaf that heads the moved chain.
            let moved_keys = n.keys.to_vec(key_index..n.len);
            let captured_next = mem::take(&mut n.next);
            let right_bound = mem::replace(&mut n.right_bound, pivot.clone());
            n.len = key_index;
            let new_id = BPlusTree::<KS, PS>::new_page_id();
            let mut moved_leaf = ExtNode::<KS, PS>::new(new_id, right_bound);
            moved_leaf.keys = LeafKeys::from_keys(&moved_keys, KS::slice_len());
            moved_leaf.len = moved_keys.len();
            moved_leaf.next = captured_next.clone();
            moved_leaf.prev = NodeCellRef::default();
            drop(node);
            let moved_ref = NodeCellRef::new(Node::with_external(moved_leaf));
            // The leaf that followed the boundary now sits behind the freshly
            // created head; repoint its back-link into the new tree.
            if !captured_next.is_default() {
                let mut nxt = write_node::<KS, PS>(&captured_next);
                if let Some(prev) = nxt.left_ref_mut() {
                    *prev = moved_ref.clone();
                }
            }
            capture_moved_chain::<KS, PS>(moved_ref, ctx);
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
