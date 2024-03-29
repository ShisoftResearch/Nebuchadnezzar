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
        &NodeData::External(ref n) => {
            let keys = n.keys.as_slice_immute();
            Some((n.len, n.prev.clone(), keys[n.len / 2].clone()))
        }
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
    retain_by_node::<KS, PS>(tree, &tree.get_root(), mid_key, 0);
}

fn retain_by_node<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node_ref: &NodeCellRef,
    mid_key: &EntryKey,
    level: usize,
) where
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
                return;
            }
            let selected_key = &n.keys.as_slice_immute()[key_index];
            let origin_node_len = n.len;
            debug_assert!(
                selected_key >= mid_key,
                "Selected {:?}, mid {:?}",
                selected_key,
                mid_key
            );
            n.len = key_index; // All others will be ignored
            debug_assert_ne!(
                n.len, 0,
                "No keys left in page, selected {:?}, mid {:?}, left ref {:?}",
                selected_key, mid_key, &n.prev
            ); // Assert no empty node after cut
               // Cut out the right half of the node in this tree
            let mut right_node_ref = mem::take(&mut n.next);
            let mut num_removed_keys = origin_node_len - key_index;
            drop(node);
            while !right_node_ref.is_default() {
                trace!("Obtaining right node lock for {:?}", right_node_ref);
                let mut node = write_node::<KS, PS>(&right_node_ref);
                trace!("Right node lock obrained for {:?}", right_node_ref);
                let node_id = node.ext_id();
                right_node_ref = mem::take(node.right_ref_mut().unwrap());
                num_removed_keys += node.len();
                *node = NodeData::Empty(Box::new(Default::default()));
                make_deleted::<KS, PS>(&node_id);
            }
            tree.len.fetch_sub(num_removed_keys, Release);
            info!("LSM tree retention removed {} keys", num_removed_keys);
        }
        &NodeData::Internal(ref n) => {
            let index = n.search(mid_key);
            retain_by_node::<KS, PS>(tree, &n.ptrs.as_slice_immute()[index], mid_key, level + 1);
            if index >= n.len {
                return;
            }
            assert_ne!(index, 0, "This case is not possible and not handled");
            debug!("Retaining keys at internal level {}", level);
            let mut node = write_node::<KS, PS>(node_ref);
            debug_assert_eq!(
                // Ensure two searches have the same result
                index,
                node.keys()
                    .binary_search(mid_key)
                    .map(|i| i + 1)
                    .unwrap_or_else(|i| i)
            );
            debug_assert_ne!(index, 0);
            let innode = node.innode_mut();
            for ptr in innode.ptrs.as_slice()[index + 1..=innode.len].iter_mut() {
                *ptr = Default::default();
            }
            innode.len = index;
            let mut right_node_ref = mem::take(&mut innode.right);
            drop(node);
            while !right_node_ref.is_default() {
                let mut node = write_node::<KS, PS>(&right_node_ref);
                right_node_ref = mem::take(node.right_ref_mut().unwrap());
                *node = NodeData::Empty(Box::new(Default::default()));
            }
        }
        &NodeData::Empty(ref n) => {
            retain_by_node::<KS, PS>(tree, &n.right, mid_key, level);
        }
        &NodeData::None => unreachable!(),
    }
}
