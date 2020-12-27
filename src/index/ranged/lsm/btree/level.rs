use super::node::write_node;
use super::node::NodeWriteGuard;
use super::search::MutSearchResult;
use super::BPlusTree;
use super::LevelTree;
use super::NodeCellRef;
use super::*;
use itertools::Itertools;
use std::fmt::Debug;
use std::sync::atomic::Ordering;

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
            let (nodes, mut new_first) = select_nodes_in_boundary::<KS, PS>(node, boundary, level);
            let new_first_ref = new_first.node_ref().clone();
            let head_id = nodes[0].ext_id();
            new_first.extnode_mut(src_tree).id = head_id;
            new_first.left_ref_mut().map(|lr| *lr = Default::default());
            drop(new_first);
            debug_assert!(
                !nodes.is_empty(),
                "Cannot find any keys in range in external with boundary {:?}",
                boundary
            );
            let mut num_keys_merged = 0;
            debug!("Selected {} pages to merge", nodes.len());            
            for mut node in nodes {
                let node_id = node.ext_id();
                let node_len = node.len();
                let deleted_keys = node
                    .keys()
                    .iter()
                    .filter(|k| src_tree.deleted.contains(k))
                    .cloned()
                    .collect_vec();
                let merging_keys = node
                    .extnode_mut_no_persist()
                    .keys
                    .as_slice()[..node_len]
                    .iter_mut()
                    .filter(|k| !src_tree.deleted.contains(k))
                    .map(|k| mem::take(k))
                    .collect_vec();
                let num_merging_keys = merging_keys.len();
                num_keys_merged += num_merging_keys;
                debug!("Collected {} keys, merging to destination tree", num_merging_keys);
                dest_tree.merge_with_keys(box merging_keys);
                debug!("Merge completed, cleanup");
                // Update delete set in source
                for dk in deleted_keys {
                    src_tree.deleted.remove(&dk);
                }
                clear_node(node, &new_first_ref);
                if node_id != head_id {
                    external::make_deleted::<KS, PS>(&node_id);
                }
                src_tree.len.fetch_sub(num_merging_keys, Ordering::Relaxed);
            }
            debug!("Merge prune completed in external level");
            return num_keys_merged;
        }
        MutSearchResult::Internal(sub_node) => {
            let num_keys_merged = merge_prune(level + 1, &sub_node, src_tree, dest_tree, boundary);
            let (nodes, right_node) = select_nodes_in_boundary::<KS, PS>(node, boundary, level);
            if !nodes.is_empty() {
                for node in nodes {
                    clear_node(node, right_node.node_ref());
                }
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

fn clear_node<KS, PS>(mut node: NodeWriteGuard<KS, PS>, right_ref: &NodeCellRef)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    node.make_empty_node(false);
    *node.right_ref_mut().unwrap() = right_ref.clone();
    node.left_ref_mut().map(|r| *r = right_ref.clone());
}

fn select_nodes_in_boundary<KS, PS>(
    first_node: &NodeCellRef,
    right_boundary: &EntryKey,
    level: usize,
) -> (Vec<NodeWriteGuard<KS, PS>>, NodeWriteGuard<KS, PS>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let first_node = write_node::<KS, PS>(first_node);
    if first_node.right_bound() > right_boundary {
        return (vec![], first_node);
    }
    let mut next_node = write_node(first_node.right_ref().unwrap());
    let mut collected = vec![first_node];
    loop {
        let first_key = next_node.first_key();
        let node_right = next_node.right_bound();
        debug_assert!(first_key < right_boundary, "First key {:?} out of bound {:?}, collected {}", first_key, right_boundary, collected.len());
        debug_assert!(node_right <= right_boundary, "Node right {:?} out of bound {:?}, collected {}", node_right, right_boundary, collected.len());
        let next_right = write_node(next_node.right_ref().unwrap());
        collected.push(next_node);
        next_node = next_right;
        if next_node.first_key() >= right_boundary {
            trace!("Collected {} nodes at level {}", collected.len(), level);
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
