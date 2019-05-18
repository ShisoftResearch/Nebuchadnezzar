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
use index::btree::NodeCellRef;
use index::btree::{external, BPlusTree};
use index::btree::{LevelTree, NodeReadHandler};
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
use std::iter::Peekable;
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

struct NodeAltered {
    key: Option<EntryKey>,
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
            assert!(read_unchecked::<KS, PS>(first_node.left_ref().unwrap()).is_none());
            let mut collected_keys = first_node.len();
            let mut collected = vec![first_node];
            let target_keys = min(KS::slice_len() * LEVEL_PAGE_DIFF_MULTIPLIER, LEVEL_3);
            let target_guards = LEVEL_2;
            while collected_keys < target_keys && collected.len() < target_guards {
                let right = write_node(collected.last_mut().unwrap().right_ref().unwrap());
                if right.is_none() {
                    break;
                } else {
                    debug_assert!(!right.is_empty(), "found empty node on selection!!!");
                    collected_keys += right.len();
                    collected.push(right);
                }
            }
            return collected;
        }
        MutSearchResult::Internal(node) => select::<KS, PS>(&node),
    }
}

fn prune_removed<'a, KS, PS>(
    node: &NodeCellRef,
    removed: Box<Vec<NodeAltered>>,
    bound: &EntryKey,
    level: usize,
) -> Box<Vec<NodeAltered>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut level_page_altered = vec![];
    let sub_node_ref = read_node(node, |n: &NodeReadHandler<KS, PS>| {
        n.innode().ptrs.as_slice_immute()[0].clone()
    });
    let altered_keys = {
        let sub_node = read_unchecked::<KS, PS>(&sub_node_ref);
        // first meet empty should be the removed external node
        if sub_node.is_empty_node() || sub_node.is_ext() {
            removed
        } else {
            prune_removed::<KS, PS>(&sub_node_ref, removed, bound, level + 1)
        }
    };
    let mut all_pages = vec![write_node::<KS, PS>(node)];
    // collect all pages in bound and in this level
    loop {
        let (right_ref, right_bound) = {
            let last_page = all_pages.last().unwrap().borrow();
            debug_assert!(!last_page.is_none());
            debug_assert!(
                is_node_serial(last_page.innode()),
                "node not serial on fetching pages {:?} - {}",
                last_page.keys(),
                level
            );
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
    debug!("Prune selected level {}, {} pages", level, all_pages.len());
    let select_live = |page: &NodeWriteGuard<KS, PS>, removed: &mut Peekable<_>| {
        // removed is a sequential external nodes that have been removed and have been set to empty
        // nodes are ordered so we can iterate them while scanning the reference in upper levels.
        debug_assert!(
            is_node_serial(page.innode()),
            "node not serial before live selection - {}",
            level
        );
        page.innode().ptrs.as_slice_immute()[..page.len() + 1]
            .iter()
            .enumerate()
            .filter_map(|(i, sub_level)| {
                let mut found_removed = false;
                let current_removed: Option<&&NodeAltered> = removed.peek();
                debug_assert_ne!(
                    current_removed
                        .clone()
                        .map(|cr| read_unchecked::<KS, PS>(&cr.ptr).is_empty()),
                    Some(false)
                );
                if let Some(&rm) = current_removed {
                    if sub_level.ptr_eq(&rm.ptr) {
                        found_removed = true;
                    }
                }
                if !found_removed {
                    Some((i, sub_level.clone()))
                } else {
                    removed.next();
                    None
                }
            })
            .collect_vec()
    };
    let page_lives_ptrs = {
        let mut removed = altered_keys.iter().filter(|na| na.key.is_none()).peekable();
        let living_ptrs = all_pages
            .iter()
            .map(|p| select_live(p, &mut removed))
            .collect_vec();
        debug_assert!(
            removed.next().is_none(),
            "remaining removed {}, total {}, level {}",
            removed.count() + 1,
            altered_keys.len(),
            level
        );
        living_ptrs
    };

    // make all the necessary changes in current level pages according to is living children
    all_pages = all_pages
        .into_iter()
        .zip(page_lives_ptrs)
        .filter_map(|(mut page, live_ptrs)| {
            if live_ptrs.len() == 0 {
                // check if all the children ptr in this page have been removed
                // if yes mark it and upper level will handel it
                level_page_altered.push(NodeAltered {
                    key: None,
                    ptr: page.node_ref().clone(),
                });
                // set length zero without do anything else
                // this will ease read hazard
                page.make_empty_node(false);
                debug!("Found empty node");
                None
            } else {
                // extract all live child ptrs and construct a new page from them
                let mut new_keys = KS::init();
                let mut new_ptrs = PS::init();
                let ptr_len = live_ptrs.len();
                for (i, &(oi, _)) in live_ptrs.iter().skip(1).enumerate() {
                    new_keys.as_slice()[i] = page.keys()[oi - 1].clone();
                }
                for (i, (_, ptr)) in live_ptrs.into_iter().enumerate() {
                    new_ptrs.as_slice()[i] = ptr;
                }
                {
                    let mut innode = page.innode_mut();
                    debug_assert!(
                        is_node_serial(innode),
                        "node not serial before update - {}",
                        level
                    );
                    innode.len = ptr_len - 1;
                    innode.keys = new_keys;
                    innode.ptrs = new_ptrs;
                    debug_assert!(
                        is_node_serial(innode),
                        "node not serial after update - {}",
                        level
                    );
                    debug!("Found non-empty node, new ptr length {}, node len {}", ptr_len, innode.len);
                }
                Some(page)
            }
        })
        .collect_vec();

    let update_and_mark_altered_keys =
        |page: &mut NodeWriteGuard<KS, PS>,
         current_altered: &mut Peekable<_>,
         next_level_altered: &mut Vec<NodeAltered>| {
            // update all nodes marked changed, not removed
            let page_ref = page.node_ref().clone();
            let mut innode = page.innode_mut();
            let innde_len = innode.len;

            debug_assert!(
                is_node_serial(innode),
                "node not serial before update altered - {}",
                level
            );
            // search for all children nodes in this page to find the altered pointers
            let marked_ptrs = innode.ptrs.as_slice_immute()[..innde_len + 1]
                .iter()
                .enumerate()
                .filter_map(|(i, p)| {
                    let mut found_key = None;
                    if let Some(&ak) = current_altered.peek() {
                        let ak: &NodeAltered = ak;
                        if ak.ptr.ptr_eq(p) {
                            found_key = Some((i, ak.key.clone().unwrap()));
                        }
                    }
                    if found_key.is_some() {
                        current_altered.next();
                    }
                    found_key
                })
                .collect_vec();

            // alter keys corresponding to the ptr, which is ptr id - 1; 0 will postpone to upper level
            debug!("We have {} altered pointers", marked_ptrs.len());
            for (i, new_key) in marked_ptrs {
                // update key for children ptr, note that node all key can be updated in this level
                if i == 0 {
                    // cannot update the key in current level
                    // will postpone to upper level
                    next_level_altered.push(NodeAltered {
                        key: Some(new_key),
                        ptr: page_ref.clone(),
                    });
                } else {
                    // can be updated, set the new key
                    innode.keys.as_slice()[i - 1] = new_key;
                }
            }
            debug_assert!(
                is_node_serial(innode),
                "node not serial after update altered - {}",
                level
            );
        };

    // alter keys
    {
        let mut current_altered = altered_keys.iter().filter(|ak| ak.key.is_some()).peekable();
        all_pages.iter_mut().for_each(|p| update_and_mark_altered_keys(p, &mut current_altered, &mut level_page_altered));
        debug_assert!(current_altered.next().is_none());
    }

    // dealing with corner cases
    // here, a page may have one ptr and no keys, then the remaining ptr need to be merge with right page
    debug!("Checking corner cases");
    let mut index = 0;
    while index < all_pages.len() {
        if all_pages[index].len() == 0 {
            // current page have one ptr and no keys
            // need to merge with the right page
            // if the right page is full, partial of the right page will be moved to the current page
            // merging right page will also been cleaned
            debug!("Dealing with emptying node {}", index);
            let mut next_from_ptr = if index + 1 >= all_pages.len() {
                debug!("Trying to fetch node guard for last node right");
                Some(write_node::<KS, PS>(all_pages[index].right_ref().unwrap()))
            } else {
                None
            };
            // extract keys, ptrs from right that will merge to left
            // new right key bound and right ref  from right (if right will be removed) also defines here
            let (keys, ptrs, right_bound, right_ref, merging) = {
                // get next page from right reference or next in the vec
                let mut next = next_from_ptr
                    .as_mut()
                    .unwrap_or_else(|| &mut all_pages[index + 1]);
                let merging_node = next.len() < KS::slice_len() - 1;
                debug_assert!(
                    is_node_serial(next.innode()),
                    "node 2 not serial before rebalance - {}",
                    level
                );

                if merging_node {
                    // Merge next node with current node
                    debug!("Merging node...");
                    let tuple = {
                        let next_innode = next.innode();
                        let len = next_innode.len;
                        (
                            next_innode.keys.as_slice_immute()[..len].to_vec(),
                            next_innode.ptrs.as_slice_immute()[..len + 1].to_vec(),
                            next_innode.right_bound.clone(),
                            next_innode.right.clone(),
                            merging_node,
                        )
                    };
                    level_page_altered.push(NodeAltered {
                        key: None,
                        ptr: next.node_ref().clone(),
                    });
                    next.make_empty_node(false);
                    tuple
                } else {
                    // Rebalance next node with current node
                    // Next node left bound need to be updated in upper level
                    debug!("Rebalancing node...");
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
                        merging_node,
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

                    debug_assert!(
                        is_node_serial(next_innode),
                        "node 2 not serial after rebalance - {}",
                        level
                    );

                    level_page_altered.push(NodeAltered {
                        key: Some(right_left_bound),
                        ptr: right_ref,
                    });
                    tuple
                }
            };
            debug_assert!(!right_ref.is_default());
            let mut page_innode = all_pages[index].innode_mut();
            debug_assert!(
                is_node_serial(page_innode),
                "node 1 not serial before rebalance - {}",
                level
            );
            page_innode.keys.as_slice()[0] = page_innode.right_bound.clone();
            page_innode.right_bound = right_bound;
            page_innode.right = right_ref;
            page_innode.len = 1 + keys.len();
            // again, keys and pres here are vec that will merge into the current emptying node
            for (i, key) in keys.into_iter().enumerate() {
                page_innode.keys.as_slice()[i + 1] = key;
            }
            for (i, ptr) in ptrs.into_iter().enumerate() {
                page_innode.ptrs.as_slice()[i + 1] = ptr;
            }
            debug_assert!(
                is_node_serial(page_innode),
                "node 1 not serial after rebalance - {} - {} - {:?}",
                level,
                merging,
                page_innode.keys
            );
            index += 1;
        }
        index += 1;
    }

    let update_right_nodes = |all_pages: &mut Vec<NodeWriteGuard<KS, PS>>| {
        let right_ptrs = all_pages
            .iter()
            .filter(|p| !p.is_empty_node())
            .enumerate()
            .map(|(i, p)| {
                if i == all_pages.len() - 1 {
                    p.right_ref().unwrap().clone()
                } else {
                    all_pages[i + 1].node_ref().clone()
                }
            })
            .collect_vec();
        all_pages
            .iter_mut()
            .zip(right_ptrs.into_iter())
            .for_each(|(p, r)| {
                debug_assert!(!r.is_default());
                *p.right_ref_mut().unwrap() = r;
            });
    };
    update_right_nodes(&mut all_pages);

    box level_page_altered
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
    let mut removed_nodes = box vec![];
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
            g.make_empty_node(false);
            g.right_ref_mut().map(|rr| *rr = right_right_most.clone());
            g.left_ref_mut().map(|lr| *lr = left_left_most.clone());
            removed_nodes.push(NodeAltered {
                key: None,
                ptr: g.node_ref().clone(),
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
    prune_removed::<KS, PS>(&src_tree.get_root(), removed_nodes, &prune_bound, 0);

    src_tree.len.fetch_sub(num_keys_removed, Relaxed);

    debug!("Merge completed");

    merge_page_len
}

pub fn is_node_serial<KS, PS>(node: &InNode<KS, PS>) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // check keys
    for i in 1..node.len {
        if node.keys.as_slice_immute()[i - 1] >= node.keys.as_slice_immute()[i] {
            error!("serial check failed for key ordering");
            return false;
        }
    }
    true
}
