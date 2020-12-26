use super::node::write_node;
use super::node::NodeWriteGuard;
use super::search::MutSearchResult;
use super::BPlusTree;
use super::LevelTree;
use super::NodeCellRef;
use super::*;
use itertools::Itertools;
use std::fmt::Debug;

pub const LEVEL_PAGE_DIFF_MULTIPLIER: usize = 4;
pub const LEVEL_TREE_DEPTH: u32 = 2;

pub const LEVEL_M: usize = 16; // Smaller can be faster but more fragmented
pub const LEVEL_1: usize = LEVEL_M * LEVEL_PAGE_DIFF_MULTIPLIER;
pub const LEVEL_2: usize = LEVEL_1 * LEVEL_PAGE_DIFF_MULTIPLIER;

pub const NUM_LEVELS: usize = 3;


// NEVER MAKE THIS ASYNC
fn merge_prune<KS, PS>(
    level: usize,
    node: &NodeCellRef,
    src_tree: &BPlusTree<KS, PS>,
    dest_tree: &dyn LevelTree,
    boundary: &EntryKey,
) -> usize
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let search = mut_first::<KS, PS>(node);
    match search {
        MutSearchResult::External => {
            let (mut nodes, mut new_first) = select_nodes_in_boundary::<KS, PS>(node, boundary);
            debug_assert!(
                !nodes.is_empty(),
                "Cannot find any keys in range in external with boundary {:?}",
                boundary
            );
            // Collect keys to merge
            let all_keys = nodes
                .iter()
                .map(|n| n.keys())
                .flatten()
                .cloned()
                .collect_vec();
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
            debug!("Collected {} keys, merging to destination tree", num_keys_merging);
            // Merging with destination tree
            dest_tree.merge_with_keys(box merging_keys);
            debug!("Merged into destination tree, cutting external {} nodes", nodes.len());
            // Update delete set in source
            for dk in deleted_keys {
                src_tree.deleted.remove(dk);
            }
            let first_node_id = nodes[0].extnode().id;
            // Cut tree in external level
            clear_nodes(nodes, new_first.node_ref());
            // Reset first node. We don't want to update the tree head id so we will reuse the id
            let ext_node = new_first.extnode_mut(src_tree);
            ext_node.id = first_node_id;
            ext_node.prev = Default::default();
            debug!("Merge prune completed in external level");
            return num_keys_merging;
        }
        MutSearchResult::Internal(sub_node) => {
            let num_keys_merged = merge_prune(level + 1, &sub_node, src_tree, dest_tree, boundary);
            let (nodes, right_node) = select_nodes_in_boundary::<KS, PS>(node, boundary);
            if !nodes.is_empty() {
                clear_nodes(nodes, right_node.node_ref());
            } else {
                // The boundary does nott covered the first node in this level
                // Need to partially remove the node keys and ptrs
                let mut node = right_node;
                // Find the boundary in the node and remove all keys and ptrs to there
                match node.keys().binary_search(boundary) {
                    Ok(bound_id) => {
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
                    Err(id) => {
                        assert_eq!(id, 0);
                    }
                }
            }
            return num_keys_merged;
        }
    }
}

fn clear_nodes<KS, PS>(nodes: Vec<NodeWriteGuard<KS, PS>>, right_ref: &NodeCellRef)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    for mut node in nodes.into_iter() {
        node.make_empty_node(false);
        *node.right_ref_mut().unwrap() = right_ref.clone();
        node.left_ref_mut().map(|r| *r = right_ref.clone());
    }
}

fn select_nodes_in_boundary<KS, PS>(
    first_node: &NodeCellRef,
    right_boundary: &EntryKey,
) -> (Vec<NodeWriteGuard<KS, PS>>, NodeWriteGuard<KS, PS>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let first_node = write_node::<KS, PS>(first_node);
    if first_node.right_bound() > right_boundary {
        return (vec![], first_node);
    }
    trace!("Acquiring first node");
    let mut next_node = write_node(first_node.right_ref().unwrap());
    let mut collected = vec![first_node];
    loop {
        debug_assert!(next_node.first_key() < right_boundary);
        debug_assert!(next_node.right_bound() <= right_boundary);
        let next_right = write_node(next_node.right_ref().unwrap());
        collected.push(next_node);
        next_node = next_right;
        if next_node.first_key() >= right_boundary {
            return (collected, next_node);
        }
    }
}

fn select_boundary<KS, PS>(node: &NodeCellRef) -> EntryKey
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    read_node(node, |node: &NodeReadHandler<KS, PS>| {
        let node_keys = node.keys();
        let node_len = node_keys.len();
        if node.len() < 2 {
            return select_boundary::<KS, PS>(&node.ptrs()[0]);
        }
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
    let key_boundary = select_boundary::<KS, PS>(&root);
    debug!(
        "Level merge level {} with boundary {:?}",
        level, key_boundary
    );
    let num_keys = merge_prune(0, &src_tree.get_root(), src_tree, dest_tree, &key_boundary);
    debug_assert!(verification::tree_has_no_empty_node(&src_tree));
    debug_assert!(verification::is_tree_in_order(&src_tree, level));
    debug!("Merge and pruned level {}, waiting for storage", level);
    storage::wait_until_updated().await;
    debug!("MERGE LEVEL {} COMPLETED", level);
    return num_keys;
}
