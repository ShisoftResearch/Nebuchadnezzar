use core::borrow::{Borrow, BorrowMut};
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
use index::btree::node::NodeData::Empty;
use index::btree::node::NodeWriteGuard;
use index::btree::search::mut_search;
use index::btree::search::MutSearchResult;
use index::btree::LevelTree;
use index::btree::NodeCellRef;
use index::btree::{external, BPlusTree};
use index::lsmtree::tree::{LEVEL_2, LEVEL_3, LEVEL_PAGE_DIFF_MULTIPLIER};
use index::EntryKey;
use index::Slice;
use itertools::free::all;
use itertools::Itertools;
use serde_json::to_vec;
use smallvec::SmallVec;
use std::cell::RefCell;
use std::cmp::min;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
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

struct KeyAltered {
    new_key: EntryKey,
    ptr: NodeCellRef,
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

fn prune_selected<'a, KS, PS>(node: &NodeCellRef, bound: &EntryKey, level: usize) -> Vec<KeyAltered>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut level_page_altered = vec![];
    let first_search = mut_search::<KS, PS>(node, &smallvec!());
    let altered_keys = match first_search {
        MutSearchResult::Internal(sub_node_ref) => {
            let sub_node = read_unchecked::<KS, PS>(&sub_node_ref);
            if !sub_node.is_empty_node() && !sub_node.is_ext() {
                prune_selected::<KS, PS>(&sub_node_ref, bound, level + 1)
            } else {
                vec![]
            }
        }
        MutSearchResult::External => unreachable!(),
    };
    let mut all_pages = vec![write_node::<KS, PS>(node)];
    // collect all pages in bound and in this level
    loop {
        let (right_ref, right_bound) = {
            let last_page = all_pages.last().unwrap().borrow();
            if last_page.is_none() {
                break;
            }
            (
                last_page.right_ref().unwrap().clone(),
                last_page.right_bound().clone(),
            )
        };
        let right = write_node::<KS, PS>(&right_ref);
        if right.is_none() {
            break;
        }
        all_pages.push(right);
        if &right_bound > bound {
            break;
        }
    }
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
            .collect_vec()
    };
    let page_lives = all_pages.iter().map(|p| select_live(p)).collect_vec();
    all_pages
        .iter_mut()
        .zip(page_lives)
        .for_each(|(mut page, live_ptrs)| {
            if live_ptrs.len() == 0 {
                **page = NodeData::Empty(box EmptyNode {
                    left: None,
                    right: Default::default(),
                })
            } else {
                let mut new_keys = KS::init();
                let mut new_ptrs = PS::init();
                let ptr_len = live_ptrs.len();
                for (i, &(oi, _)) in live_ptrs.iter().take(live_ptrs.len() - 1).enumerate() {
                    new_keys.as_slice()[i] = page.keys()[oi].clone();
                }
                for (i, (_, ptr)) in live_ptrs.into_iter().enumerate() {
                    new_ptrs.as_slice()[i] = ptr;
                }
                let innode = page.innode_mut();
                innode.len = ptr_len - 1;
                innode.keys = new_keys;
                innode.ptrs = new_ptrs;
            }
        });

    let non_empty_right_node = |i: usize, pages: &Vec<NodeWriteGuard<KS, PS>>| {
        let mut i = i + 1;
        loop {
            if i == pages.len() {
                return pages[i - 1].borrow().right_ref().unwrap().clone();
            } else if i < pages.len() {
                let p = pages[i].borrow();
                if !p.is_empty_node() {
                    return p.node_ref().clone();
                } else {
                    i += 1;
                }
            } else {
                unreachable!()
            }
        }
    };

    let update_and_mark_altered_keys =
        |page: &mut NodeWriteGuard<KS, PS>, altered: &mut Vec<KeyAltered>| {
            let mut innode = page.innode_mut();
            let marked_ptrs = innode
                .ptrs
                .as_slice_immute()
                .iter()
                .enumerate()
                .filter_map(|(i, p)| {
                    altered_keys
                        .iter()
                        .find(|ak| ak.ptr.ptr_eq(p))
                        .map(|ak| (i, ak.new_key.clone()))
                })
                .collect_vec();
            for (i, new_key) in marked_ptrs {
                if i == 0 {
                    // cannot update the key in current page
                    // will postpone to upper level
                    altered.push(KeyAltered {
                        new_key: new_key,
                        ptr: innode.ptrs.as_slice_immute()[i].clone(),
                    });
                } else {
                    innode.keys.as_slice()[i - 1] = new_key;
                }
            }
        };

    let update_right_nodes = |all_pages: &mut Vec<NodeWriteGuard<KS, PS>>| {
        let right_ptrs = all_pages
            .iter()
            .enumerate()
            .map(|(i, _)| non_empty_right_node(i, &all_pages))
            .collect_vec();
        all_pages
            .iter_mut()
            .zip(right_ptrs.into_iter())
            .for_each(|(p, r)| *p.right_ref_mut().unwrap() = r);
    };

    update_right_nodes(&mut all_pages);

    all_pages = all_pages
        .into_iter()
        .filter(|p| !p.borrow().is_empty_node())
        .map(|mut p| {
            update_and_mark_altered_keys(&mut p, &mut level_page_altered);
            p
        })
        .collect_vec();

    let mut index = 0;
    while index < all_pages.len() {
        if all_pages[index].len() == 0 {
            // current page have one ptr and no keys
            // need to merge with the right page
            // if the right page is full, partial of the right page will be moved to the current page
            // merging right page will also been cleaned
            let mut next_from_ptr = if index + 1 >= all_pages.len() {
                Some(write_node::<KS, PS>(all_pages[index].right_ref().unwrap()))
            } else {
                None
            };
            let (keys, ptrs, right_bound, right_ref) = {
                let mut next = next_from_ptr
                    .as_mut()
                    .unwrap_or_else(|| &mut all_pages[index + 1]);
                if next.len() < KS::slice_len() - 1 {
                    // Merge next node with current node
                    let tuple = {
                        let next_innode = next.innode();
                        let len = next_innode.len;
                        (
                            next_innode.keys.as_slice_immute()[..len].to_vec(),
                            next_innode.ptrs.as_slice_immute()[..len + 1].to_vec(),
                            next_innode.right_bound.clone(),
                            next_innode.right.clone(),
                        )
                    };
                    **next = NodeData::Empty(box EmptyNode {
                        left: None,
                        right: NodeCellRef::default(),
                    });
                    tuple
                } else {
                    // Rebalance next node with current node
                    // Next node left bound need to be updated in upper level
                    let right_ref = next.node_ref().clone();
                    let next_innode = next.innode_mut();
                    let next_len = next_innode.len;
                    let next_mid = next_len / 2;
                    let right_left_bound = next_innode.keys.as_slice_immute()[next_mid].clone();
                    let tuple = (
                        next_innode.keys.as_slice_immute()[..next_mid].to_vec(),
                        next_innode.ptrs.as_slice_immute()[..next_mid + 1].to_vec(),
                        right_left_bound.clone(),
                        right_ref.clone(),
                    );
                    let mut keys = KS::init();
                    let mut ptrs = PS::init();

                    for (i, key) in next_innode.keys.as_slice_immute()[next_mid + 1..next_len]
                        .iter()
                        .enumerate()
                    {
                        keys.as_slice()[i] = key.clone();
                    }
                    for (i, ptr) in next_innode.ptrs.as_slice_immute()[next_mid + 1..next_len + 1]
                        .iter()
                        .enumerate()
                    {
                        ptrs.as_slice()[i] = ptr.clone();
                    }
                    next_innode.keys = keys;
                    next_innode.ptrs = ptrs;
                    next_innode.len = next_len - next_mid - 1;

                    level_page_altered.push(KeyAltered {
                        new_key: right_left_bound,
                        ptr: right_ref,
                    });
                    tuple
                }
            };
            let page_innode = all_pages[index].innode_mut();
            page_innode.keys.as_slice()[0] = page_innode.right_bound.clone();
            page_innode.right_bound = right_bound;
            page_innode.right = right_ref;
            page_innode.len = 1 + keys.len();
            for (i, key) in keys.into_iter().enumerate() {
                page_innode.keys.as_slice()[i + 1] = key;
            }
            for (i, ptr) in ptrs.into_iter().enumerate() {
                page_innode.ptrs.as_slice()[i + 1] = ptr;
            }
            index += 1;
        }
        index += 1;
    }

    update_right_nodes(&mut all_pages);

    level_page_altered
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
        debug!("Merge selected keys {:?}", keys.len());
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
        debug_assert!(!right_right_most.ptr_eq(left_most_leaf_guards.last().unwrap().node_ref()));

        for mut g in &mut left_most_leaf_guards {
            external::make_deleted(&g.ext_id());
            debug!("make deleted {:?}", g.ext_id());
            **g = NodeData::Empty(box EmptyNode {
                left: Some(left_left_most.clone()),
                right: right_right_most.clone(),
            });
        }

        let mut new_first_node = write_node::<KS, PS>(&right_right_most);
        let mut new_first_node_ext = new_first_node.extnode_mut();
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

        ExtNode::<KS, PS>::make_changed(&right_right_most, src_tree);
    }

    // cleanup upper level references
    prune_selected::<KS, PS>(&src_tree.get_root(), &prune_bound, 0);

    src_tree.len.fetch_sub(num_keys_removed, Relaxed);

    debug!("Merge completed");

    merge_page_len
}
