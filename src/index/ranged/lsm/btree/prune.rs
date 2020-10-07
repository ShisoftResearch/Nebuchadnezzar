use super::node::*;
use super::verification::{is_node_list_serial, is_node_serial};
use super::NodeCellRef;
use super::MIN_ENTRY_KEY;
use super::*;
use itertools::Itertools;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::iter::Iterator;

type AlterPair = (EntryKey, NodeCellRef);

#[derive(Debug)]
pub struct AlteredNodes {
    pub removed: Vec<AlterPair>,
    pub key_modified: Vec<AlterPair>,
}

impl AlteredNodes {
    fn summary(&self) -> String {
        format!(
            "Removed {}, modified {}",
            self.removed.len(),
            self.key_modified.len()
        )
    }
    fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.key_modified.is_empty()
    }
}

pub fn prune<'a, KS, PS>(
    node: &NodeCellRef,
    altered: AlteredNodes,
    level: usize,
) -> (AlteredNodes, Vec<NodeWriteGuard<KS, PS>>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut level_page_altered = AlteredNodes {
        removed: vec![],
        key_modified: vec![],
    };

    if altered.is_empty() {
        debug!("Nothing to do at level {}, skipped", level);
        return (level_page_altered, vec![]);
    }
    debug!("Start prune at level {}", level);
    // Follwing procedures will scan the pages from left to right in key order, we need to sort it to
    // make sure the keys we are probing are also sorted
    // altered.removed.sort_by(|a, b| a.0.cmp(&b.0));
    // altered.key_modified.sort_by(|a, b| a.0.cmp(&b.0));
    // altered.added.sort_by(|a, b| a.0.cmp(&b.0));

    let mut all_pages = probe_key_range(node, &altered, level);
    debug!(
        "Prune had selected {} pages at level {}",
        all_pages.len(),
        level
    );
    if all_pages.is_empty() {
        trace!("No node to prune at this level - {}", level);
        return (level_page_altered, vec![]);
    }

    if cfg!(debug_assertions) && !is_node_list_serial(&all_pages) {
        error!("Node list not serial after selection. Dumping");
        for (i, page) in all_pages.iter().enumerate() {
            println!(
                "{}\t{}: {:?} - [{:?} keys] -> {:?}",
                i,
                page.type_name(),
                page.node_ref().address(),
                page.keys().len(),
                page.right_bound()
            );
        }
        panic!();
    }

    if !altered.removed.is_empty() {
        // Locating refrences in the pages to be removed
        let page_children_to_be_retained = ref_to_be_retained(&mut all_pages, &altered, level);
        trace!(
            "Prune retains {:?} pages {}, items {}, empty {}",
            page_children_to_be_retained,
            page_children_to_be_retained.len(),
            page_children_to_be_retained
                .iter()
                .map(|p| p.len())
                .sum::<usize>(),
            page_children_to_be_retained
                .iter()
                .filter(|p| p.is_empty())
                .count()
        );
        if cfg!(debug_assertions) {
            for (pid, page) in page_children_to_be_retained.iter().enumerate() {
                for entry in page {
                    let node = read_unchecked::<KS, PS>(&entry.1);
                    debug_assert!(
                        !node.is_empty_node(),
                        "{} - {:?}, {}",
                        pid,
                        entry,
                        node.type_name()
                    );
                }
            }
        }
        // make all the necessary changes in current level pages according to is living children
        all_pages = filter_retained(
            all_pages,
            page_children_to_be_retained,
            &mut level_page_altered,
            level,
        );
        debug!("Sub altred {}", altered.summary());
        debug!("Cur altered {}", level_page_altered.summary());
    }

    debug!(
        "Prune had selected {} living pages at level {}",
        all_pages.len(),
        level
    );

    // alter keys
    if altered.key_modified.len() > 0 {
        update_and_mark_altered_keys(&mut all_pages, &altered, &mut level_page_altered);
    }

    debug!("Sub altred {}", altered.summary());
    debug!("Cur altered {}", level_page_altered.summary());

    if level != 0 {
        all_pages = update_right_nodes(all_pages);

        debug!(
            "Prune had updated right nodes for {} living pages at level {}",
            all_pages.len(),
            level
        );
        debug_assert!(
            is_node_list_serial(&all_pages),
            "node not serial before checking corner cases"
        );

        if merge_single_ref_pages(&mut all_pages, &mut level_page_altered) {
            all_pages = update_right_nodes(all_pages);
        }

        debug!(
            "Prune had merged all single ref pages, now have {} living pages at level {}",
            all_pages.len(),
            level
        );

        debug_assert!(
            is_node_list_serial(&all_pages),
            "node not serial after checked corner cases"
        );
    }

    debug!("Sub altred {}", altered.summary());
    debug!("Cur altered {}", level_page_altered.summary());
    debug!("Prune completed at level {}", level);
    (level_page_altered, all_pages)
}

fn removed_iter<'a>(altered_keys: &AlteredNodes) -> impl Iterator<Item = &AlterPair> {
    altered_keys.removed.iter()
}
fn altered_iter<'a>(altered_keys: &AlteredNodes) -> impl Iterator<Item = &AlterPair> {
    altered_keys.key_modified.iter()
}

fn probe_key_range<KS, PS>(
    node: &NodeCellRef,
    altered: &AlteredNodes,
    level: usize,
) -> Vec<NodeWriteGuard<KS, PS>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    trace!("Acquiring first prune node");
    let mut all_pages = vec![write_node::<KS, PS>(node)];
    // collect all pages in bound and in this level
    let max_key = {
        let removed = removed_iter(&altered)
            .map(|(e, _)| e)
            .last()
            .unwrap_or(&*MIN_ENTRY_KEY);
        let alted = altered_iter(&altered)
            .map(|(e, _)| e)
            .last()
            .unwrap_or(&*MIN_ENTRY_KEY);
        std::cmp::max(removed, alted)
    };
    debug!("Max key to prune to is {:?}", max_key);
    // This process will probe pages by all alter node types in this level to select the right pages
    // which contains those entries to work with
    loop {
        let (next, ends) = {
            let last_page = all_pages.last().unwrap().borrow();
            if last_page.is_ref_none() {
                break;
            }
            debug_assert!(!last_page.is_empty_node(), "at {:?}", last_page.node_ref());
            let last_innode = last_page.innode();
            debug_assert!(
                is_node_serial(last_page),
                "node not serial on fetching pages {:?} - {:?} | {}",
                last_page.keys(),
                last_page.innode().ptrs.as_slice_immute(),
                level
            );
            let next_node_ref = &last_innode.right;
            if cfg!(debug_assertions) {
                let next_node = read_unchecked::<KS, PS>(next_node_ref);
                // right refercing nodes are out of order, need to investigate
                debug_assert!(!next_node.node_ref().ptr_eq(last_page.node_ref()));
                for p in &all_pages {
                    if p.node_ref().ptr_eq(next_node.node_ref()) {
                        panic!("Duplicated unordered node from right referecing");
                    }
                    if !next_node.is_none() {
                        assert!(p.right_bound() < next_node.right_bound());
                    }
                }
            }
            debug!("Obtain node lock for {:?}...", next_node_ref);
            let next_node = write_node::<KS, PS>(next_node_ref);
            debug!("Obtained node lock for {:?}", next_node_ref);
            let ends = &last_innode.right_bound > max_key;
            debug!("Collecting node {:?}", next_node.node_ref());
            (next_node, ends)
        };
        // all_pages contains all of the entry keys we need to work for remove, add and modify
        all_pages.push(next);
        if ends {
            break;
        }
    }
    debug!(
        "Prune selected at level {}, {} pages",
        level,
        all_pages.len()
    );
    return all_pages;
}

fn ref_to_be_retained<'a, KS, PS>(
    all_pages: &mut Vec<NodeWriteGuard<KS, PS>>,
    altered: &AlteredNodes,
    level: usize,
) -> Vec<Vec<(usize, NodeCellRef)>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let removed = altered
        .removed
        .iter()
        .map(|(_, r)| r.address())
        .collect::<HashSet<_>>();
    debug!(
        "Remove set is {:?}, size {} at level {}",
        removed,
        altered.removed.len(),
        level
    );
    debug_assert!(all_pages.first().unwrap().first_key() <= &altered.removed.first().unwrap().0);
    let mut remove_count = 0;
    let matching_refs = all_pages
        .iter()
        .map(|page| {
            // removed is a sequential external nodes that have been removed and have been set to empty
            // nodes are ordered so we can iterate them while scanning the reference in upper levels.
            debug_assert!(
                is_node_serial(page),
                "node not serial before live selection - {}",
                level
            );
            if page.is_ref_none() {
                return vec![];
            }
            page.innode().ptrs.as_slice_immute()[..page.len() + 1]
                .iter()
                .enumerate()
                .filter_map(|(i, sub_level)| {
                    if removed.contains(&sub_level.address()) {
                        remove_count += 1;
                        return None;
                    }
                    Some((i, sub_level.clone()))
                })
                .collect_vec()
        })
        .collect_vec();
    debug!(
        "Remove count is {}, should be {} at level {}",
        remove_count,
        removed.len(),
        level
    );
    if cfg!(debug_assertions) && remove_count != removed.len() {
        warn!("Remove set and actual removed numner does not match");
    }
    matching_refs
}

fn filter_retained<KS, PS>(
    all_pages: Vec<NodeWriteGuard<KS, PS>>,
    retained: Vec<Vec<(usize, NodeCellRef)>>,
    level_page_altered: &mut AlteredNodes,
    level: usize,
) -> Vec<NodeWriteGuard<KS, PS>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    return all_pages
        .into_iter()
        .zip(retained)
        .filter_map(|(mut page, retained_refs)| {
            let is_not_none = !page.is_ref_none();
            if is_not_none && retained_refs.len() == 0 {
                // check if all the children ptr in this page have been removed
                // if yes mark it and upper level will handel it
                level_page_altered
                    .removed
                    .push((page.right_bound().clone(), page.node_ref().clone()));
                page.make_empty_node(false);
                None
            } else {
                if is_not_none {
                    // extract all live child ptrs and construct a new page from them
                    let mut new_keys = KS::init();
                    let mut new_ptrs = PS::init();
                    let ptr_len = retained_refs.len();
                    // Copy retained keys and refs to target page
                    for (i, &(oi, _)) in retained_refs.iter().skip(1).enumerate() {
                        new_keys.as_slice()[i] = page.keys()[oi - 1].clone();
                    }
                    for (i, (_, ptr)) in retained_refs.into_iter().enumerate() {
                        new_ptrs.as_slice()[i] = ptr;
                    }
                    {
                        debug_assert!(
                            is_node_serial(&page),
                            "node not serial before update - {}",
                            level
                        );
                        let mut innode = page.innode_mut();
                        innode.len = ptr_len - 1;
                        innode.keys = new_keys;
                        innode.ptrs = new_ptrs;
                        trace!(
                            "Found non-empty node, new ptr length {}, node len {}",
                            ptr_len,
                            innode.len
                        );
                    }
                    debug_assert!(
                        is_node_serial(&page),
                        "node not serial after update - {}",
                        level
                    );
                }
                Some(page)
            }
        })
        .collect_vec();
}

fn update_and_mark_altered_keys<'a, KS, PS>(
    pages: &mut Vec<NodeWriteGuard<KS, PS>>,
    to_be_altered: &AlteredNodes,
    next_level_altered: &mut AlteredNodes,
) where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut modified_set = HashMap::new();
    for modified_items in &to_be_altered.key_modified {
        modified_set.insert(modified_items.1.address(), modified_items.0.clone());
    }
    let mut modified = 0;
    debug!("Doing key modification, {} items", modified_set.len());
    for page in pages {
        for (i, ptr) in page.innode().ptrs.as_slice_immute().iter().enumerate() {
            if let Some(modified_key) = modified_set.get_mut(&ptr.address()) {
                let modify_to = mem::take(modified_key);
                let alter_index = i + 1;
                debug!("Alter key at {}  with {:?}", alter_index, modify_to);
                if alter_index < page.len() {
                    debug!("Modify key to {:?}", modify_to);
                    page.innode_mut().keys.as_slice()[alter_index] = modify_to;
                } else {
                    debug!("Modify key on boundary, postpone for {:?}", modify_to);
                    // This page have changed its boundary, should postpone modification to upper level
                    *page.right_bound_mut() = modify_to.clone();
                    next_level_altered
                        .key_modified
                        .push((modify_to, page.node_ref().clone()));
                }
                modified += 1;
                break;
            }
        }
    }
    debug!(
        "Done key modification, {}, expect {}",
        modified,
        modified_set.len()
    );
}

fn update_right_nodes<KS, PS>(all_pages: Vec<NodeWriteGuard<KS, PS>>) -> Vec<NodeWriteGuard<KS, PS>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // This procedure will also remove empty nodes
    if all_pages.is_empty() {
        trace!("No nodes available to update right node");
        return all_pages;
    }
    let mut non_emptys = all_pages
        .into_iter()
        .filter(|p| p.is_ref_none() || !p.is_empty_node())
        .collect_vec();
    let right_refs = non_emptys
        .iter()
        .enumerate()
        .map(|(i, p)| {
            if p.is_ref_none() {
                return NodeCellRef::default();
            }
            let right_ref = if i == non_emptys.len() - 1 {
                non_emptys[i].right_ref().unwrap().clone()
            } else {
                non_emptys[i + 1].node_ref().clone()
            };
            debug_assert!(!p.is_none());
            if cfg!(debug_assertions) {
                let right = read_unchecked::<KS, PS>(&right_ref);
                if !right.is_none() {
                    let left = p.right_bound();
                    let right = right.right_bound();
                    assert!(left < right, "failed on checking left right page right bound, expecting {:?} less than {:?}", left, right);
                }
            }
            right_ref
        })
        .collect_vec();
    non_emptys
        .iter_mut()
        .zip(right_refs.into_iter())
        .for_each(|(p, r)| {
            if !p.is_ref_none() {
                debug_assert!(!p.node_ref().ptr_eq(&r));
                *p.right_ref_mut().unwrap() = r
            }
        });
    return non_emptys;
}

// Return true if the case is handled
fn merge_single_ref_pages<KS, PS>(
    all_pages: &mut Vec<NodeWriteGuard<KS, PS>>,
    level_page_altered: &mut AlteredNodes,
) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // dealing with corner cases
    // here, a page may have one ptr and no keys, then the remaining ptr need to be merge with right page
    let num_pages = all_pages.len();
    debug!("Checking single ptr node, {} pages", num_pages);
    let mut index = 0;
    let mut corner_case_handled = false;
    while index < num_pages - 1 {
        if all_pages[index].is_ref_none() || all_pages[index + 1].is_ref_none() {
            break;
        }
        let left_node_len = all_pages[index].len();
        let right_node_len = all_pages[index + 1].len();
        if left_node_len == 0 || right_node_len == 0 {
            debug!("Found single ref node, prep to merge");
            corner_case_handled = true;
            // left or right node (or both) have only one ptr and length 0
            // We are not going to use any complicated approach, will take it slow
            // but easy to understand and debug
            // Here, we use one vec for keys and one for ptrs
            let mut keys = vec![];
            let mut ptrs = vec![];
            // Ensure node serial
            debug_assert!(verification::is_node_serial(&all_pages[index]));
            debug_assert!(verification::is_node_serial(&all_pages[index + 1]));
            debug_assert!(all_pages[index].right_bound() <= all_pages[index + 1].right_bound());
            // Collect all left node keys
            for key in all_pages[index].keys() {
                keys.push(key.clone());
            }
            // Collect left node right bound key
            keys.push(all_pages[index].right_bound().clone());
            // Collect right node keys
            for key in all_pages[index + 1].keys() {
                keys.push(key.clone());
            }
            // Ensure all keys are serial after keys are collected
            if cfg!(debug_assertions) {
                debug_assert!(verification::are_keys_serial(keys.as_slice()));
            }
            // Collect all left node ptrs
            for ptr in all_pages[index].ptrs() {
                ptrs.push(ptr.clone());
            }
            // Collect all right node ptrs
            for ptr in all_pages[index + 1].ptrs() {
                ptrs.push(ptr.clone());
            }
            let num_keys = keys.len();
            // Ensure key and value numbers matches
            debug_assert_eq!(num_keys + 1, ptrs.len());
            // Here we have two strategy to deal with single node problem
            // When both left and right node have no keys and one ptr, we should
            // combine two nodes to right and remove the left node;
            // this can be hazardous for node search in only one direction
            // When only one of the node has more than one keys and two ptrs, we can split the
            // combined vec in the half and distribute them
            let left_keys = if num_keys < 3 { num_keys } else { num_keys / 2 };
            if num_keys >= KS::slice_len() {
                debug!(
                    "Merge single node by rebalancing with right, (l:{}, r{}) of {} at {}",
                    left_node_len,
                    right_node_len,
                    KS::slice_len(),
                    index
                );
                // Move and setup left node
                for i in 0..left_keys {
                    all_pages[index].innode_mut().keys.as_slice()[i] = mem::take(&mut keys[i]);
                    all_pages[index].innode_mut().ptrs.as_slice()[i] = mem::take(&mut ptrs[i]);
                }
                all_pages[index].innode_mut().ptrs.as_slice()[left_keys] =
                    mem::take(&mut ptrs[left_keys]);
                all_pages[index].innode_mut().len = left_keys;
                // Update the right boundary of the left node
                let left_node_right_bound = mem::take(&mut keys[left_keys]);
                all_pages[index].innode_mut().right_bound = left_node_right_bound.clone();
                // The right boundary of left node has been changed, put the left node to alter list
                level_page_altered
                    .key_modified
                    .push((left_node_right_bound, all_pages[index].node_ref().clone()));
                // Move and setup right node
                let right_keys_offset = left_keys + 1;
                for i in right_keys_offset..num_keys {
                    let right_index = i - right_keys_offset;
                    all_pages[index + 1].innode_mut().keys.as_slice()[right_index] =
                        mem::take(&mut keys[i]);
                    all_pages[index + 1].innode_mut().ptrs.as_slice()[right_index] =
                        mem::take(&mut ptrs[i]);
                }
                all_pages[index + 1].innode_mut().ptrs.as_slice()[num_keys] =
                    mem::take(&mut ptrs[num_keys]);
                all_pages[index + 1].innode_mut().len = num_keys - right_keys_offset;
            } else {
                debug!(
                    "Merge single node by eliminating left node with {} keys",
                    num_keys
                );
                let first_key = keys[0].clone();
                for i in 0..num_keys {
                    all_pages[index + 1].innode_mut().keys.as_slice()[i] = mem::take(&mut keys[i]);
                    all_pages[index + 1].innode_mut().ptrs.as_slice()[i] = mem::take(&mut ptrs[i]);
                }
                all_pages[index + 1].innode_mut().ptrs.as_slice()[num_keys] =
                    mem::take(&mut ptrs[num_keys]);
                all_pages[index + 1].innode_mut().len = num_keys;
                // make left node empty
                all_pages[index].make_empty_node(false);
                level_page_altered
                    .removed
                    .push((first_key, all_pages[index].node_ref().clone()));
            }
        }
        index += 1;
    }
    debug!("Single ptr processed: {}", corner_case_handled);
    return corner_case_handled;
}
