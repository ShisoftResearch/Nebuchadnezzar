use super::dump::dump_tree;
use super::node::read_unchecked;
use super::node::write_node;
use super::node::NodeWriteGuard;
use super::prune::*;
use super::search::MutSearchResult;
use super::LevelTree;
use super::NodeCellRef;
use super::*;
use super::{external, BPlusTree};
use itertools::Itertools;
use std::fmt::Debug;
use std::sync::atomic::Ordering::Relaxed;

pub const LEVEL_PAGE_DIFF_MULTIPLIER: usize = 4;
pub const LEVEL_TREE_DEPTH: u32 = 2;

pub const LEVEL_M: usize = 16; // Smaller can be faster but more fragmented
pub const LEVEL_1: usize = LEVEL_M * LEVEL_PAGE_DIFF_MULTIPLIER;
pub const LEVEL_2: usize = LEVEL_1 * LEVEL_PAGE_DIFF_MULTIPLIER;

pub const NUM_LEVELS: usize = 3;

// Select left most leaf nodes and acquire their write guard
fn merge_prune<KS, PS>(
    level: usize,
    node: &NodeCellRef,
    src_tree: &BPlusTree<KS, PS>,
    dest_tree: &dyn LevelTree,
) -> (AlteredNodes, usize, Vec<NodeWriteGuard<KS, PS>>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let search = mut_first::<KS, PS>(node);
    match search {
        MutSearchResult::External => {
            debug!("Processing external level {}", level);
            let left_most_leaf_guards = select_ext_nodes(node);
            let num_guards = left_most_leaf_guards.len();
            let (altered, num_keys) =
                merge_remove_empty_ext_nodes(left_most_leaf_guards, src_tree, dest_tree);
            debug!("Merged {} keys, {} pages", num_keys, num_guards);
            (altered, num_keys, vec![])
        }
        MutSearchResult::Internal(sub_node) => {
            let (lower_altered, num_keys, _lower_guards) =
                merge_prune(level + 1, &sub_node, src_tree, dest_tree);
            debug!("Processing internal level {}, node {:?}", level, node);
            let (altered, guards) = prune(&node, lower_altered, level);
            (altered, num_keys, guards)
        }
    }
}

fn merge_remove_empty_ext_nodes<KS, PS>(
    mut left_most_leaf_guards: Vec<NodeWriteGuard<KS, PS>>,
    src_tree: &BPlusTree<KS, PS>,
    dest_tree: &dyn LevelTree,
) -> (AlteredNodes, usize)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let num_keys_moved;
    let left_most_id = left_most_leaf_guards.first().unwrap().ext_id();
    let prune_bound = left_most_leaf_guards.last().unwrap().right_bound().clone();

    debug!("Merge selected {} pages", left_most_leaf_guards.len());
    debug!(
        "Have {:?} pages after selection",
        num_pages(&left_most_leaf_guards[0])
    );
    if cfg!(debug_assertions) {
        if left_most_id != src_tree.head_page_id {
            dump_tree(src_tree, "level_lsm_merge_failure_dump.json");
        }
    }
    debug_assert_eq!(
        left_most_id,
        src_tree.head_page_id,
        "{}",
        left_most_leaf_guards.first().unwrap().type_name()
    );

    // merge to dest_tree
    {
        let deleted_keys = &src_tree.deleted;
        let mut merged_deleted_keys = vec![];
        let keys: Vec<EntryKey> = left_most_leaf_guards
            .iter()
            .map(|g| &g.keys()[..g.len()])
            .flatten()
            .filter(|&k| {
                if deleted_keys.contains(k) {
                    merged_deleted_keys.push(k.clone());
                    false
                } else {
                    true
                }
            })
            .cloned()
            .collect_vec();
        num_keys_moved = keys.len();
        debug!(
            "Merging {} keys, have {}",
            num_keys_moved,
            src_tree.len.load(Relaxed)
        );
        dest_tree.merge_with_keys(box keys);
        for rk in &merged_deleted_keys {
            deleted_keys.remove(rk);
        }
    }

    // adjust leaf left, right references
    let mut removed_nodes = AlteredNodes {
        removed: vec![],
        key_modified: vec![],
    };
    {
        let right_right_most = left_most_leaf_guards
            .last()
            .unwrap()
            .right_ref()
            .unwrap()
            .clone();

        let left_left_most = left_most_leaf_guards
            .first()
            .unwrap()
            .left_ref()
            .unwrap()
            .clone();

        debug_assert!(read_unchecked::<KS, PS>(&left_left_most).is_none());
        debug_assert!(!read_unchecked::<KS, PS>(&right_right_most).is_none());
        debug_assert!(read_unchecked::<KS, PS>(&right_right_most).is_ext());
        debug_assert!(!right_right_most.ptr_eq(left_most_leaf_guards.last().unwrap().node_ref()));

        for g in &mut left_most_leaf_guards {
            external::make_deleted::<KS, PS>(&g.ext_id());
            removed_nodes
                .removed
                .push((g.right_bound().clone(), g.node_ref().clone()));
            g.make_empty_node(false);
            g.left_ref_mut().map(|lr| *lr = NodeCellRef::default());
            g.right_ref_mut().map(|rr| *rr = right_right_most.clone());
        }
        debug!(
            "Have {:?} pages after removal",
            num_pages(&left_most_leaf_guards[0])
        );
        trace!("Acquiring new first node");
        let mut new_first_node = write_node::<KS, PS>(&right_right_most);
        let mut new_first_node_ext = new_first_node.extnode_mut(src_tree);
        debug!(
            "Right most original id is {:?}, now is {:?}",
            new_first_node_ext.id, src_tree.head_page_id
        );
        trace!(
            "New first node right is {:?}",
            read_unchecked::<KS, PS>(&new_first_node_ext.next).ext_id()
        );

        new_first_node_ext.id = src_tree.head_page_id;
        new_first_node_ext.prev = NodeCellRef::default();
        debug_assert!(&new_first_node_ext.right_bound > &prune_bound);
        src_tree.len.fetch_sub(num_keys_moved, Relaxed);
        (removed_nodes, num_keys_moved)
    }
}

fn select_ext_nodes<KS, PS>(first_node: &NodeCellRef) -> Vec<NodeWriteGuard<KS, PS>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    trace!("Acquiring first node");
    let first_node = write_node(first_node);
    assert!(
        read_unchecked::<KS, PS>(first_node.left_ref().unwrap()).is_none(),
        "Left most is not none, have {}",
        read_unchecked::<KS, PS>(first_node.left_ref().unwrap()).type_name()
    );
    let mut collected = vec![first_node];
    let target_guards = if KS::slice_len() > LEVEL_M {
        KS::slice_len()
    } else {
        KS::slice_len().pow(LEVEL_TREE_DEPTH - 1) >> 1 // merge half of the pages from memory
    }; // pages to collect
    while collected.len() < target_guards {
        trace!("Acquiring select collection node");
        let right = write_node(collected.last().unwrap().right_ref().unwrap());
        if right.is_none() {
            // Early break for reach the end of the linked list
            // Should not be possible, will warn
            warn!(
                "Searching node to move and reach the end, maybe this is not the right parameter"
            );
            break;
        } else {
            debug_assert!(!right.is_empty(), "found empty node on selection!!!");
            debug_assert!(!read_unchecked::<KS, PS>(right.right_ref().unwrap()).is_none());
            debug_assert!(!read_unchecked::<KS, PS>(right.right_ref().unwrap()).is_empty_node());
            collected.push(right);
        }
    }
    return collected;
}

fn num_pages<KS, PS>(head_page: &NodeWriteGuard<KS, PS>) -> (usize, usize)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut num = 0;
    let mut non_empty = 0;
    let mut node_ref = head_page.node_ref().clone();
    loop {
        let node = read_unchecked::<KS, PS>(&node_ref);
        if node.is_none() {
            break;
        }
        if !node.is_empty() {
            non_empty += 1;
        }
        if let Some(node) = node.right_ref() {
            node_ref = node.clone()
        } else {
            break;
        }
        num += 1;
    }
    (num, non_empty)
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
    let (_, num_keys, _) = merge_prune(0, &src_tree.get_root(), src_tree, dest_tree);
    debug_assert!(verification::tree_has_no_empty_node(&src_tree));
    debug_assert!(verification::is_tree_in_order(&src_tree, level));
    debug!("Merge and pruned level {}, waiting for storage", level);
    storage::wait_until_updated().await;
    debug!("MERGE LEVEL {} COMPLETED", level);
    return num_keys;
}
