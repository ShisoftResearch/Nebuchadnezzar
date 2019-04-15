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
use itertools::free::all;
use itertools::Itertools;
use smallvec::SmallVec;
use std::cmp::min;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::cell::RefCell;
use core::borrow::{Borrow, BorrowMut};
use index::btree::node::NodeData::Empty;

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

    let right_ptrs = all_pages
        .iter()
        .enumerate()
        .map(|(i, _)| non_empty_right_node(i, &all_pages))
        .collect_vec();
    all_pages
        .iter_mut()
        .zip(right_ptrs.into_iter())
        .for_each(|(p, r)| *p.right_ref_mut().unwrap() = r);

    all_pages = all_pages
        .into_iter()
        .filter(|p| !p.borrow().is_empty_node())
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
                let mut next = next_from_ptr.as_mut().unwrap_or_else(|| &mut all_pages[index + 1]);
                if next.len() < KS::slice_len() - 1 {
                    let tuple = {
                        let next_innode = next.innode();
                        let len = next_innode.len;
                        (
                            next_innode.keys.as_slice_immute()[..len].to_vec(),
                            next_innode.ptrs.as_slice_immute()[..len + 1].to_vec(),
                            next_innode.right_bound.clone(),
                            next_innode.right.clone()
                        )
                    };
                    **next = NodeData::Empty(box EmptyNode {
                        left: None,
                        right: NodeCellRef::default()
                    });
                    tuple
                } else {
                    unreachable!()
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

    vec![]

    //    debug!("Have {} altered keys", altered_keys.len());
    //    let is_altered = |ptr: &NodeCellRef| altered_keys.iter().find(|a| a.ptr.ptr_eq(ptr));

    //    let mut next_non_empty = |page: &NodeWriteGuard<KS, PS>, empty_nodes: &mut Vec<_>| {
    //        let mut next = page.right_ref().cloned();
    //        while let Some(next_ref) = next {
    //            let guard = write_node::<KS, PS>(&next_ref);
    //            if guard.is_none() {
    //                return None;
    //            }
    //            next = guard.right_ref().cloned();
    //            let live_ptrs: HashMap<_, _> = select_live(&guard);
    //            if live_ptrs.is_empty() {
    //                empty_nodes.push(guard);
    //            } else {
    //                return Some((guard, live_ptrs));
    //            }
    //        }
    //        unreachable!()
    //    };
    //    let preserve_live = |innode: &mut InNode<KS, PS>,
    //                         node_ref: &NodeCellRef,
    //                         live_ptrs: HashMap<usize, NodeCellRef>,
    //                         altered_list: &mut Vec<KeyAltered>| {
    //        debug!("preserving live ptrs - {}", level);
    //        let mut new_keys = KS::init();
    //        let mut new_ptrs = PS::init();
    //        {
    //            let keys: Vec<&mut _> = innode.keys.as_slice()[..innode.len]
    //                .iter_mut()
    //                .enumerate()
    //                .filter(|(i, _)| live_ptrs.contains_key(&i))
    //                .map(|(_, k)| k)
    //                .collect();
    //            debug!("Prune filtered page have keys {}, was {}, node right bound: {:?}, prune right bound {:?} - {}", keys.len(), innode.len, innode.right_bound, bound, level);
    //            debug_assert_eq!(live_ptrs.len(), keys.len() + 1);
    //            innode.len = keys.len();
    //            let new_keys = new_keys.as_slice();
    //            let new_ptrs = new_ptrs.as_slice();
    //            for (i, key) in keys.into_iter().enumerate() {
    //                debug_assert!(key > &mut smallvec!(0));
    //                new_keys[i] = key.clone();
    //            }
    //            for (i, (_, ptr)) in live_ptrs.into_iter().enumerate() {
    //                debug_assert!(!ptr.is_default());
    //                if let Some(altered) = is_altered(&ptr) {
    //                    debug!("Node with first key {:?} was altered to {:?}",  new_keys[i], altered.new_key);
    //                    new_keys[i] = altered.new_key.clone();
    //                    if i == 0 {
    //                        altered_list.push(KeyAltered {
    //                            new_key: altered.new_key.clone(),
    //                            ptr: node_ref.clone(),
    //                        })
    //                    }
    //                }
    //                new_ptrs[i] = ptr;
    //            }
    //        }
    //        innode.keys = new_keys;
    //        innode.ptrs = new_ptrs;
    //        innode.len == 0
    //    };
    //    let mut prev_node_guard: Option<NodeWriteGuard<KS, PS>> = None;
    //    let mut page = write_node::<KS, PS>(&node);
    //    let mut live_ptrs: HashMap<_, _> = select_live(&page);
    //    let mut altered_list = vec![];
    //    loop {
    //        let page_right_ref = page.right_ref().unwrap().clone();
    //        if !page.is_empty_node() {
    //            let is_empty = live_ptrs.is_empty();
    //            if !is_empty {
    //                let emptying = {
    //                    let page_ref = page.node_ref().clone();
    //                    let mut innode = page.innode_mut();
    //                    preserve_live(&mut innode, &page_ref, live_ptrs, &mut altered_list)
    //                };
    //                if emptying {
    //                    // current innode have one ptr and no keys
    //                    // need to merge with the right page
    //                    // if the right page is full, partial of the right page will be moved to the current page
    //                    // merging right page will also been cleaned
    //                    debug!("node is emptying - {}", level);
    //                    if let Some((mut next_guard, next_ptrs)) =
    //                        next_non_empty(&page, &mut empty_nodes)
    //                    {
    //                        let mid = next_guard.len() / 2;
    //                        let next_ref = next_guard.node_ref().clone();
    //                        let page_ref = page.node_ref().clone();
    //                        let mut page_innode = page.innode_mut();
    //                        let next_node_emptying = preserve_live(
    //                            &mut next_guard.innode_mut(),
    //                            &next_ref,
    //                            next_ptrs,
    //                            &mut altered_list,
    //                        );
    //                        page_innode.keys.as_slice()[0] = page_innode.right_bound.clone();
    //                        if next_guard.len() >= KS::slice_len() {
    //                            // will overflow if merged, have to rebalance right node contents with current node
    //                            debug!("Merge emptying page in rebalance style");
    //                            let mut next_innode = next_guard.innode_mut();
    //                            let next_search_key = next_innode.keys.as_slice()[0].clone();
    //                            // move next node keys and ptrs to current node
    //                            for i in 0..mid {
    //                                page_innode.keys.as_slice()[i + 1] = next_innode.keys.as_slice()[i].clone();
    //                                next_innode.keys.as_slice()[i] = Default::default();
    //
    //                                page_innode.ptrs.as_slice()[i + 1] = next_innode.ptrs.as_slice()[i].clone();
    //                                next_innode.ptrs.as_slice()[i] = Default::default();
    //                            }
    //                            for i in mid..next_innode.len {
    //                                let target_pos = i - mid;
    //                                next_innode.keys.as_slice()[target_pos] = Default::default();
    //                                next_innode.ptrs.as_slice()[target_pos] = Default::default();
    //                                next_innode.keys.as_slice().swap(i, target_pos);
    //                                next_innode.ptrs.as_slice().swap(i, target_pos);
    //                            }
    //                            next_innode.len -= mid;
    //                            page_innode.len += mid;
    //                            page_innode.right_bound = page_innode.keys.as_slice()[mid].clone();
    //                            page_innode.len = 1 + mid;
    //                            altered_list.push(KeyAltered {
    //                                new_key: page_innode.right_bound.clone(),
    //                                ptr: next_ref,
    //                            });
    //                        } else {
    //                            // Can move right page contents to current node and empty next node
    //                            debug!("Merge emptying page in merge style");
    //                            {
    //                                let mut next_innode = next_guard.innode_mut();
    //                                let next_node_len = next_innode.len;
    //                                for i in 0..next_innode.len {
    //                                    page_innode.keys.as_slice()[i + 1] = next_innode.keys.as_slice()[i].clone();
    //                                    next_innode.keys.as_slice()[i] = Default::default();
    //
    //                                    page_innode.ptrs.as_slice()[i + 1] = next_innode.ptrs.as_slice()[i].clone();
    //                                    next_innode.ptrs.as_slice()[i] = Default::default();
    //                                }
    //                                page_innode.ptrs.as_slice()[next_node_len + 1] = next_innode.ptrs.as_slice()[next_node_len].clone();
    //                                next_innode.ptrs.as_slice()[next_node_len] = Default::default();
    //                                page_innode.len = 1 + next_node_len;
    //                                page_innode.right = next_innode.right.clone();
    //                                page_innode.right_bound = next_innode.right_bound.clone();
    //                            }
    //                            *next_guard = NodeData::Empty(box EmptyNode {
    //                                left: None,
    //                                right: page_ref,
    //                            });
    //                        }
    //                    } else {
    //                        break;
    //                    }
    //                }
    //            }
    //            if let Some((next_page, next_live_ptrs)) = next_non_empty(&page, &mut empty_nodes) {
    //                prev_node_guard = Some(page);
    //                page = next_page;
    //                live_ptrs = next_live_ptrs;
    //            } else {
    //                debug!("Cannot find any right node to continue prune - {}", level);
    //                break;
    //            }
    //            if page.right_bound() >= bound {
    //                debug!("Out of bound to continue prune - {}", level);
    //                break;
    //            }
    //        } else {
    //            unreachable!();
    //        }
    //    }
    //    // Reverse the list so only latest pushed will take effect
    //    altered_list.reverse();
    //    altered_list
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

    if cfg!(debug_assertions) {
        dump_tree(src_tree, "last_merged_tree_dump.json");
    }
    debug!("Merge completed");

    merge_page_len
}
