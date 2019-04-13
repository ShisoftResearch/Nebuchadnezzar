use index::btree::dump::dump_tree;
use index::btree::external::ExtNode;
use index::btree::internal::InNode;
use index::btree::node::is_node_locked;
use index::btree::node::read_node;
use index::btree::node::read_unchecked;
use index::btree::node::write_node;
use index::btree::node::write_non_empty;
use index::btree::node::write_targeted;
use index::btree::node::EmptyNode;
use index::btree::node::Node;
use index::btree::node::NodeData;
use index::btree::node::NodeWriteGuard;
use index::btree::search::mut_search;
use index::btree::search::MutSearchResult;
use index::btree::LevelTree;
use index::btree::NodeCellRef;
use index::btree::{external, BPlusTree};
use index::lsmtree::tree::{LEVEL_2, LEVEL_3, LEVEL_PAGE_DIFF_MULTIPLIER};
use index::EntryKey;
use index::Slice;
use itertools::Itertools;
use smallvec::SmallVec;
use std::cmp::min;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::mem;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

enum Selection<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    Selected(Vec<NodeWriteGuard<KS, PS>>),
    Innode(NodeCellRef),
}

fn select<KS, PS>(node: &NodeCellRef) -> Vec<NodeWriteGuard<KS, PS>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let search = mut_search::<KS, PS>(node, &smallvec!());
    match search {
        MutSearchResult::External => {
            let first_node = write_node(node);
            let mut collected_keys = first_node.len();
            let mut collected = vec![first_node];
            let target_keys = min(KS::slice_len() * LEVEL_PAGE_DIFF_MULTIPLIER, LEVEL_3);
            let target_guards = LEVEL_2;
            while collected_keys < target_keys && collected.len() < target_guards {
                let right = write_node(
                    collected
                        .last_mut()
                        .unwrap()
                        .right_ref_mut_no_empty()
                        .unwrap(),
                );
                if right.is_none() {
                    break;
                } else {
                    collected_keys += right.len();
                    collected.push(right);
                }
            }
            return collected;
        }
        MutSearchResult::Internal(node) => select::<KS, PS>(&node),
    }
}

fn prune_selected<'a, KS, PS>(node: &NodeCellRef, bound: &EntryKey) -> Vec<NodeCellRef>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let first_search = mut_search::<KS, PS>(node, &smallvec!());
    match first_search {
        MutSearchResult::Internal(sub_node_ref) => {
            let sub_node = read_unchecked::<KS, PS>(&sub_node_ref);
            if !sub_node.is_empty_node() && !sub_node.is_ext() {
                prune_selected::<KS, PS>(&sub_node_ref, bound);
            }
        }
        MutSearchResult::External => unreachable!(),
    };
    let mut prev_node_guard: Option<NodeWriteGuard<KS, PS>> = None;
    let mut page = write_node::<KS, PS>(&node);
    let mut next_live_ptrs = None;
    let mut emptying_nodes = vec![];
    let mut empty_nodes = vec![];
    let select_live = |page: &NodeWriteGuard<KS, PS>| {
        page.innode().ptrs.as_slice_immute()[..page.len() + 1]
            .iter()
            .enumerate()
            .filter_map(|(i, sub_level)| {
                if !read_unchecked::<KS, PS>(sub_level).is_empty_node() {
                    Some((i, sub_level.clone()))
                } else {
                    None
                }
            })
            .collect()
    };
    let make_empty = |page: &mut NodeWriteGuard<KS, PS>, right: &NodeCellRef| {
        **page = NodeData::Empty(box EmptyNode {
            left: None,
            right: right.clone(),
        });
    };
    let mut next_non_empty = |page: &NodeWriteGuard<KS, PS>, empty_nodes: &mut Vec<_>| {
        let mut next = page.right_ref().cloned();
        while let Some(next_ref) = next {
            let guard = write_node::<KS, PS>(&next_ref);
            next = guard.right_ref().cloned();
            let live_ptrs: HashMap<_, _> = select_live(&guard);
            if live_ptrs.is_empty() {
                empty_nodes.push(guard);
            } else {
                return (guard, live_ptrs);
            }
        };
        unreachable!()
    };
    loop {
        let page_len = page.len();
        let page_right_ref = page.right_ref().unwrap().clone();
        let page_right_bound = page.right_bound().clone();
        let next;
        if !page.is_empty_node() {
            let mut live_ptrs: HashMap<_, _> = next_live_ptrs.unwrap_or_else(|| select_live(&page));
            // debug_assert_ne!(live_ptrs.len(), 1);
            if live_ptrs.len() == 0 {
                // all sub nodes are empty
                // will set current node empty either
                next = next_non_empty(&page, &mut empty_nodes);
                empty_nodes.push(page);
            } else {
                let emptying = {
                    let mut innode = page.innode_mut();
                    let mut new_keys = KS::init();
                    let mut new_ptrs = PS::init();
                    {
                        let keys: Vec<&mut _> = innode.keys.as_slice()[..page_len]
                            .iter_mut()
                            .enumerate()
                            .filter(|(i, _)| live_ptrs.contains_key(&i))
                            .map(|(_, k)| k)
                            .collect();
                        debug!("Prune filtered page have keys {:?}", &keys);
                        debug_assert_eq!(live_ptrs.len(), keys.len() + 1);
                        innode.len = keys.len();
                        let new_keys = new_keys.as_slice();
                        let new_ptrs = new_ptrs.as_slice();
                        for (i, key) in keys.into_iter().enumerate() {
                            debug_assert!(key > &mut smallvec!(0));
                            mem::swap(&mut new_keys[i], key);
                        }
                        for (i, (_, ptr)) in live_ptrs.into_iter().enumerate() {
                            debug_assert!(!ptr.is_default());
                            mem::replace(&mut new_ptrs[i], ptr);
                        }
                    }
                    innode.keys = new_keys;
                    innode.ptrs = new_ptrs;
                    innode.len == 0
                };
                if emptying {
                    // current innode have one ptr and no keys
                    // need to merge with the right page
                    // if the right page is full, partial of the right page will be moved to the current page
                    // merging right page will also been cleaned

                }
                next = next_non_empty(&page, &mut empty_nodes);
                prev_node_guard = Some(page);
            }
            let (next_page, ptrs) = next;
            page = next_page;
            next_live_ptrs = Some(ptrs);
            if &page_right_bound >= bound {
                break;
            }
        } else {
            unreachable!();
        }
    }
    for empty in &mut empty_nodes {
        // make nodes empty and set its next ptr to last non empty node
        make_empty(empty, page.node_ref());
    }
    emptying_nodes
}

pub fn level_merge<KS, PS>(src_tree: &BPlusTree<KS, PS>, dest_tree: &LevelTree) -> usize
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut left_most_leaf_guards = select::<KS, PS>(&src_tree.get_root());
    let merge_page_len = left_most_leaf_guards.len();
    let mut num_keys_removed = 0;
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
        let mut deleted_keys = src_tree.deleted.write();
        let mut merged_deleted_keys = vec![];
        let keys: Vec<EntryKey> = left_most_leaf_guards
            .iter()
            .filter(|&g| !g.is_empty_node())
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
        num_keys_removed = keys.len();
        debug!("Merge selected keys are {:?}", &keys);
        dest_tree.merge_with_keys(box keys);
        for rk in &merged_deleted_keys {
            deleted_keys.remove(rk);
        }
    }

    // adjust leaf left, right references
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
        debug_assert!(read_unchecked::<KS, PS>(&right_right_most).is_ext());
        debug_assert!(!Arc::ptr_eq(
            &right_right_most.inner,
            &left_most_leaf_guards.last().unwrap().node_ref().inner
        ));

        for mut g in &mut left_most_leaf_guards {
            external::make_deleted(&g.ext_id());
            **g = NodeData::Empty(box EmptyNode {
                left: Some(left_left_most.clone()),
                right: right_right_most.clone(),
            });
        }

        let mut new_first_node = write_node::<KS, PS>(&right_right_most);
        let mut new_first_node_ext = new_first_node.extnode_mut();
        new_first_node_ext.id = src_tree.head_page_id;
        new_first_node_ext.prev = left_left_most;

        debug_assert!(&new_first_node_ext.right_bound > &prune_bound);

        ExtNode::<KS, PS>::make_changed(&right_right_most, src_tree);
    }

    // cleanup upper level references
    prune_selected::<KS, PS>(&src_tree.get_root(), &prune_bound);

    src_tree.len.fetch_sub(num_keys_removed, Relaxed);

    merge_page_len
}
