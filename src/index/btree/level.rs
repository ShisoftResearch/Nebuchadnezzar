use crate::index::btree::dump::dump_tree;
use crate::index::btree::node::read_unchecked;
use crate::index::btree::node::write_node;
use crate::index::btree::node::NodeWriteGuard;
use crate::index::btree::prune::*;
use crate::index::btree::search::mut_search;
use crate::index::btree::search::MutSearchResult;
use crate::index::btree::NodeCellRef;
use crate::index::btree::{external, BPlusTree};
use crate::index::btree::{LevelTree, MIN_ENTRY_KEY};
use crate::index::trees::EntryKey;
use crate::index::trees::Slice;
use itertools::Itertools;
use std::cmp::min;
use std::fmt::Debug;
use std::sync::atomic::Ordering::Relaxed;

pub const LEVEL_PAGE_DIFF_MULTIPLIER: usize = 10;

pub const LEVEL_M: usize = 24;
pub const LEVEL_1: usize = LEVEL_M * LEVEL_PAGE_DIFF_MULTIPLIER;
pub const LEVEL_2: usize = LEVEL_1 * LEVEL_PAGE_DIFF_MULTIPLIER;
pub const LEVEL_3: usize = LEVEL_2 * LEVEL_PAGE_DIFF_MULTIPLIER;
pub const LEVEL_4: usize = LEVEL_3 * LEVEL_PAGE_DIFF_MULTIPLIER;

pub const NUM_LEVELS: usize = 5;

// Select left most leaf nodes and acquire their write guard
fn select<KS, PS>(node: &NodeCellRef) -> Vec<NodeWriteGuard<KS, PS>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let search = mut_search::<KS, PS>(node, &*MIN_ENTRY_KEY);
    match search {
        MutSearchResult::External => {
            debug!("Acquiring first node");
            let first_node = write_node(node);
            assert!(read_unchecked::<KS, PS>(first_node.left_ref().unwrap()).is_none());
            let mut collected_keys = first_node.len();
            let mut collected = vec![first_node];
            let target_keys = min(KS::slice_len() * LEVEL_PAGE_DIFF_MULTIPLIER, LEVEL_3);
            let target_guards = LEVEL_2;
            while collected_keys < target_keys && collected.len() < target_guards {
                debug!("Acquiring select collection node");
                let right = write_node(collected.last().unwrap().right_ref().unwrap());
                if right.is_none() {
                    // Early break for reach the end of the linked list
                    // Should not be possible, will warn
                    warn!("Searching node to move and reach the end, maybe this is not the right parameter");
                    break;
                } else {
                    debug_assert!(!right.is_empty(), "found empty node on selection!!!");
                    debug_assert!(!read_unchecked::<KS, PS>(right.right_ref().unwrap()).is_none());
                    debug_assert!(
                        !read_unchecked::<KS, PS>(right.right_ref().unwrap()).is_empty_node()
                    );
                    collected_keys += right.len();
                    collected.push(right);
                }
            }
            return collected;
        }
        MutSearchResult::Internal(node) => select::<KS, PS>(&node),
    }
}

pub fn level_merge<KS, PS>(src_tree: &BPlusTree<KS, PS>, dest_tree: &dyn LevelTree) -> usize
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut left_most_leaf_guards = select::<KS, PS>(&src_tree.get_root());
    let merge_page_len = left_most_leaf_guards.len();
    let num_keys_moved;
    let left_most_id = left_most_leaf_guards.first().unwrap().ext_id();
    let prune_bound = left_most_leaf_guards.last().unwrap().right_bound().clone();

    debug!("Merge selected {} pages", left_most_leaf_guards.len());
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
        debug!("Merge selected keys {:?}", keys.len());
        dest_tree.merge_with_keys(box keys);
        for rk in &merged_deleted_keys {
            deleted_keys.remove(rk);
        }
    }

    // adjust leaf left, right references
    let mut removed_nodes = AlteredNodes {
        removed: vec![],
        added: vec![],
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
            external::make_deleted(&g.ext_id());
            removed_nodes
                .removed
                .push((g.right_bound().clone(), g.node_ref().clone()));
            g.make_empty_node(false);
            g.right_ref_mut().map(|rr| *rr = right_right_most.clone());
            g.left_ref_mut().map(|lr| *lr = left_left_most.clone());
        }

        debug!("Acquiring new first node");
        let mut new_first_node = write_node::<KS, PS>(&right_right_most);
        let mut new_first_node_ext = new_first_node.extnode_mut(src_tree);
        debug!(
            "Right most original id is {:?}, now is {:?}",
            new_first_node_ext.id, src_tree.head_page_id
        );
        debug!(
            "New first node right is {:?}",
            read_unchecked::<KS, PS>(&new_first_node_ext.next).ext_id()
        );

        new_first_node_ext.id = src_tree.head_page_id;
        new_first_node_ext.prev = left_left_most;

        debug_assert!(&new_first_node_ext.right_bound > &prune_bound);
    }

    // cleanup upper level references
    prune::<KS, PS>(&src_tree.get_root(), box removed_nodes, 0);

    src_tree.len.fetch_sub(num_keys_moved, Relaxed);

    debug!("Merge completed");

    merge_page_len
}
