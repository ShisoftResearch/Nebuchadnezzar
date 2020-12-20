use super::node::read_unchecked;
use super::node::write_node;
use super::node::NodeWriteGuard;
use super::search::MutSearchResult;
use super::LevelTree;
use super::NodeCellRef;
use super::*;
use super::BPlusTree;
use itertools::Itertools;
use std::fmt::Debug;

pub const LEVEL_PAGE_DIFF_MULTIPLIER: usize = 4;
pub const LEVEL_TREE_DEPTH: u32 = 2;

pub const LEVEL_M: usize = 16; // Smaller can be faster but more fragmented
pub const LEVEL_1: usize = LEVEL_M * LEVEL_PAGE_DIFF_MULTIPLIER;
pub const LEVEL_2: usize = LEVEL_1 * LEVEL_PAGE_DIFF_MULTIPLIER;

pub const NUM_LEVELS: usize = 3;

fn merge_prune<KS, PS>(
    level: usize,
    node: &NodeCellRef,
    src_tree: &BPlusTree<KS, PS>,
    dest_tree: &dyn LevelTree,
    boundary: &EntryKey
) -> usize
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let search = mut_first::<KS, PS>(node);
    match search {
        MutSearchResult::External => {
            let (mut nodes, mut new_first) = select_nodes_in_boundary::<KS, PS>(node, boundary);
            // Collect keys to merge
            let all_keys = nodes.iter().map(|n| n.keys()).flatten().cloned().collect_vec();
            let mut deleted_keys = Vec::with_capacity(all_keys.len());
            let mut merging_keys = Vec::with_capacity(all_keys.len());
            all_keys.iter().for_each(|k| {
                if src_tree.deleted.contains(k) {
                    deleted_keys.push(k);
                } else {
                    merging_keys.push(k.clone());
                }
            });
            let num_keys_merging = merging_keys.len();
            // Merging with destination tree
            dest_tree.merge_with_keys(box merging_keys);
            // Update delete set in source
            for dk in deleted_keys { src_tree.deleted.remove(dk); }
            let first_node_id = nodes[0].extnode().id;
            // Cut tree in external level
            clear_nodes(&mut nodes);
            // Reset first node. We don't want to update the tree head id so we will reuse the id
            let ext_node = new_first.extnode_mut(src_tree);
            ext_node.id = first_node_id;
            ext_node.prev = Default::default();
            return num_keys_merging;
        }
        MutSearchResult::Internal(sub_node) => {
            let num_keys_merged = merge_prune(level + 1, &sub_node, src_tree, dest_tree, boundary);
            if level > 0 {
                let (mut nodes, _) = select_nodes_in_boundary::<KS, PS>(node, boundary);
                clear_nodes(&mut nodes);
            } else {
                let mut node = write_node::<KS, PS>(node);
                debug_assert!(node.right_ref().unwrap().is_default()); // Ensure the root is still the root, not splitted
                // Find the boundary in the root and remove all keys and ptrs to there
                let bound_id = node.keys().binary_search(boundary).unwrap();
                let mut new_keys = KS::init();
                let mut new_ptrs = PS::init();
                let mut num_keys = 0;
                for (i, k) in node.keys()[bound_id + 1..].iter().enumerate() {
                    new_keys.as_slice()[i] = k.clone();
                    num_keys += 1;
                }
                for (i, p) in node.ptrs()[bound_id + 1..].iter().enumerate() {
                    new_ptrs.as_slice()[i] = p.clone();
                }
                let root_innode = node.innode_mut();
                root_innode.keys = new_keys;
                root_innode.ptrs = new_ptrs;
                root_innode.len = num_keys;
            }
            return num_keys_merged;
        }
    }
}

fn clear_nodes<KS, PS>(nodes: &mut Vec<NodeWriteGuard<KS, PS>>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    for node in nodes.iter_mut() {
        node.make_empty_node(false);
    }
}

fn select_nodes_in_boundary<KS, PS>(first_node: &NodeCellRef, right_boundary: &EntryKey) -> (Vec<NodeWriteGuard<KS, PS>>, NodeWriteGuard<KS, PS>)
 where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    trace!("Acquiring first node");
    let first_node = write_node(first_node);
    let mut next_node = write_node(first_node.right_ref().unwrap());
    let mut collected = vec![first_node];
    loop {
        debug_assert!(next_node.first_key() < right_boundary);
        debug_assert!(next_node.right_bound() <= right_boundary);
        let next_right = write_node(next_node.right_ref().unwrap());
        collected.push(next_node);
        next_node = next_right;
        if next_node.first_key() >= right_boundary {
            return (collected, next_node)
        }
    }
}

fn select_boundary_from_root<KS, PS>(root: &NodeCellRef) -> EntryKey 
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    read_node(root, |root_node: &NodeReadHandler<KS, PS>| {
        debug_assert!(root_node.is_internal()); // It should be but not mandatory
        let node_keys = root_node.keys();
        let node_len = node_keys.len();
        debug_assert!(node_len > 2); // Must have enough keys in the root
        // Pick half of the keys in the root
        // Genreally, higher level sub tree in LSM tree will select more keys to merged 
        // into next level
        let mid_idx = node_len / 2;
        // Return the mid key as the boundary for selection (cut)
        node_keys[mid_idx].clone()
    })
} 

pub async fn level_merge<KS, PS>(
    level: usize,
    src_tree: &BPlusTree<KS, PS>,
    dest_tree: &dyn LevelTree,
) -> usize
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    debug!("Merging LSM tree level {}", level);
    debug_assert!(verification::tree_has_no_empty_node(&src_tree));
    debug_assert!(verification::is_tree_in_order(&src_tree, level));
    let root = src_tree.get_root();
    let key_boundary = select_boundary_from_root::<KS, PS>(&root);
    let num_keys = merge_prune(0, &src_tree.get_root(), src_tree, dest_tree, &key_boundary);
    debug_assert!(verification::tree_has_no_empty_node(&src_tree));
    debug_assert!(verification::is_tree_in_order(&src_tree, level));
    debug!("Merge and pruned level {}, waiting for storage", level);
    storage::wait_until_updated().await;
    debug!("MERGE LEVEL {} COMPLETED", level);
    return num_keys;
}
