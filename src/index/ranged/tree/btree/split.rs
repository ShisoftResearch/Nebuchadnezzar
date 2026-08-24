use super::node::NodeData;
use super::*;
use std::fmt::Debug;

// Assuming the tree is almost full, worst scenario it is half full
// Pick the mid point in each of the levels, this will give us an approximate half key of the tree
pub fn last_node_prev_digest<KS, PS>(
    node_ref: &NodeCellRef,
) -> Option<(usize, NodeCellRef, EntryKey)>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // Use read unchecked for there should be no writer for disk trees
    match &*read_unchecked::<KS, PS>(node_ref) {
        &NodeData::External(ref n) => Some((n.len, n.prev.clone(), n.keys.key_at(n.len / 2))),
        &NodeData::Internal(ref n) => {
            debug!("Collecting pivot in internal {:?}", node_ref);
            last_node_prev_digest::<KS, PS>(&n.ptrs.as_slice_immute()[..n.len].last().unwrap())
        }
        &NodeData::Empty(ref n) => {
            debug!("Collecting pivot in empty {:?}", node_ref);
            last_node_prev_digest::<KS, PS>(n.left.as_ref().unwrap())
        }
        &NodeData::None => None,
    }
}

// Retain the keys in the left hand side of the mid key
// Best case scenario we can cut the tree in half
// Worst scenario, the will be some node have no key but one ptr
//  In this scenario, we can split its left hand side node and rebalance the keys
//  This can potentially produce a quarter filled page, which makes it still valid as a node
pub fn retain<KS, PS>(tree: &BPlusTree<KS, PS>, mid_key: &EntryKey)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // The root is the head of its level by definition, and the walk carries
    // that down the leftmost path.
    let _ = retain_by_node::<KS, PS>(tree, &tree.get_root(), mid_key, 0, true);
}

/// Retain the keys left of `mid_key` in the subtree at `node_ref`.
///
/// `is_level_head` says this node is the LEFTMOST of its level, which is what
/// makes it undestroyable. Everything else the cut empties becomes a bypass
/// `Empty` node -- a node that forwards a traversal to its right sibling --
/// and that is correct for a node in the middle of a level. The head has no
/// right sibling once the cut takes the rest of the level, so a bypass there
/// forwards NOWHERE: `mut_search` follows the default ref into `NodeData::None`
/// and panics, and `write_targeted` hands the unwritable node to a caller that
/// immediately asks `is_ext()`. A pivot below every key in the tree hits
/// exactly this, and turned a retained-to-nothing tree into one that could
/// never be read or written again.
fn retain_by_node<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node_ref: &NodeCellRef,
    mid_key: &EntryKey,
    level: usize,
    is_level_head: bool,
) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // Assert the thread have exclusive access to the node
    match &*read_unchecked::<KS, PS>(node_ref) {
        &NodeData::External(_) => {
            // Assert the key exists in the node for it is immutable
            debug!(
                "Retaining keys at {:?}, from node {:?}, external level {}",
                mid_key, node_ref, level
            );
            let mut node = write_node::<KS, PS>(node_ref);
            debug!("Retain key lock obtained for {:?}", node_ref);
            let n = node.extnode_mut(tree);
            let key_index = n.search(mid_key);
            if key_index >= n.len {
                // Pivot is beyond reach, nothing to do
                return true;
            }
            let selected_key = &n.keys.key_at(key_index);
            let origin_node_len = n.len;
            let prev_node_ref = n.prev.clone();
            let node_id = n.id;
            debug_assert!(
                selected_key >= mid_key,
                "Selected {:?}, mid {:?}",
                selected_key,
                mid_key
            );
            let mut right_node_ref = mem::take(&mut n.next);
            let mut num_removed_keys = origin_node_len - key_index;
            // Emptied rather than destroyed when this is the level head: an
            // external node with no keys is exactly what a fresh tree's root
            // is, so the tree stays readable and writable with nothing in it.
            if key_index == 0 && !is_level_head {
                *node = NodeData::Empty(Box::new(Default::default()));
            } else {
                n.len = key_index; // All others will be ignored
            }
            drop(node);
            if key_index == 0 && !is_level_head {
                // Unlink BEFORE enqueueing the delete: extnode_mut marks the
                // predecessor dirty, and the write-back hub fences deletions
                // behind modifications enqueued before them, so the severed
                // next pointer reaches disk no later than the page removal.
                // The reverse order left a crash window where the on-disk
                // chain pointed at deleted pages.
                if !prev_node_ref.is_default() {
                    let mut prev_node = write_node::<KS, PS>(&prev_node_ref);
                    prev_node.extnode_mut(tree).next = Default::default();
                }
                make_deleted::<KS, PS>(&node_id, tree);
            }
            while !right_node_ref.is_default() {
                trace!("Obtaining right node lock for {:?}", right_node_ref);
                let mut node = write_node::<KS, PS>(&right_node_ref);
                trace!("Right node lock obrained for {:?}", right_node_ref);
                let node_id = node.ext_id();
                right_node_ref = mem::take(node.right_ref_mut().unwrap());
                num_removed_keys += node.len();
                *node = NodeData::Empty(Box::new(Default::default()));
                make_deleted::<KS, PS>(&node_id, tree);
            }
            tree.len.fetch_sub(num_removed_keys, Release);
            info!("LSM tree retention removed {} keys", num_removed_keys);
            key_index > 0
        }
        &NodeData::Internal(ref n) => {
            let index = n.search(mid_key);
            let child_kept = retain_by_node::<KS, PS>(
                tree,
                &n.ptrs.as_slice_immute()[index],
                mid_key,
                level + 1,
                is_level_head && index == 0,
            );
            if index >= n.len && child_kept {
                return true;
            }
            debug!("Retaining keys at internal level {}", level);
            let mut node = write_node::<KS, PS>(node_ref);
            let expected_index = Some(
                node.keys()
                    .binary_search(mid_key)
                    .map(|i| i + 1)
                    .unwrap_or_else(|i| i),
            );
            let mut right_node_ref = {
                let innode = node.innode_mut();
                let original_len = innode.len;
                let mut kept_children = if child_kept { index + 1 } else { index };
                // The leftmost child survived as the level below's head, even
                // though it kept no keys. Keep the pointer to it: a head with
                // no child to descend into is a dead end.
                if kept_children == 0 && is_level_head {
                    kept_children = 1;
                }
                for ptr in innode.ptrs.as_slice()[kept_children..=original_len].iter_mut() {
                    *ptr = Default::default();
                }
                let right_node_ref = mem::take(&mut innode.right);
                if kept_children == 1 && is_level_head {
                    // Stays a REAL internal node, with no separator keys and
                    // one child. The bypass `Empty` used below is neither
                    // External nor Internal, and the root is asked which one
                    // it is on every path that grows the tree --
                    // `apply_top_level_split` calls `is_ext()` on it directly,
                    // which is how a retained-to-one-child root panicked.
                    innode.len = 0;
                } else if kept_children == 0 {
                    *node = NodeData::Empty(Box::new(Default::default()));
                } else if kept_children == 1 {
                    // This internal node now has a single live child and no separator
                    // keys. Represent it as a bypass Empty node, matching merge behavior.
                    let child = innode.ptrs.as_slice_immute()[0].clone();
                    *node = NodeData::Empty(Box::new(EmptyNode {
                        left: Some(child.clone()),
                        right: child,
                    }));
                } else {
                    debug_assert_eq!(Some(index), expected_index);
                    innode.len = kept_children - 1;
                }
                right_node_ref
            };
            drop(node);
            while !right_node_ref.is_default() {
                let mut node = write_node::<KS, PS>(&right_node_ref);
                right_node_ref = mem::take(node.right_ref_mut().unwrap());
                *node = NodeData::Empty(Box::new(Default::default()));
            }
            child_kept || index > 0
        }
        &NodeData::Empty(ref n) => {
            retain_by_node::<KS, PS>(tree, &n.right, mid_key, level, is_level_head)
        }
        &NodeData::None => unreachable!(),
    }
}
