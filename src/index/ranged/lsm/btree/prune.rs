use super::internal::InNode;
use super::node::read_node;
use super::node::Node;
use super::node::NodeData;
use super::node::*;
use super::verification::{is_node_list_serial, is_node_serial};
use super::*;
use super::{min_entry_key, NodeCellRef};
use super::{NodeReadHandler, MIN_ENTRY_KEY};
use itertools::Itertools;
use std::borrow::Borrow;
use std::fmt::Debug;
use std::iter::{Iterator, Peekable};
use std::collections::HashSet;

type AlterPair = (EntryKey, NodeCellRef);

#[derive(Debug)]
pub struct AlteredNodes {
    pub removed: Vec<AlterPair>,
    pub added: Vec<AlterPair>,
    pub key_modified: Vec<AlterPair>,
}

impl AlteredNodes {
    fn summary(&self) -> String {
        format!("Removed {}, added {}, modified {}", self.removed.len(), self.added.len(), self.key_modified.len())
    }
    fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.added.is_empty() && self.key_modified.is_empty()
    }
}

pub fn prune<'a, KS, PS>(
    node: &NodeCellRef,
    altered: Box<AlteredNodes>,
    level: usize,
) -> (Box<AlteredNodes>, Box<Vec<NodeWriteGuard<KS, PS>>>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut level_page_altered = AlteredNodes {
        removed: vec![],
        added: vec![],
        key_modified: vec![],
    };
    let sub_node_ref = read_node(node, |n: &NodeReadHandler<KS, PS>| {
        n.innode().ptrs.as_slice_immute()[0].clone()
    });
    let (mut altered, _sub_level_locks) = {
        let sub_node = unchecked_read_non_empty_node(read_unchecked::<KS, PS>(&sub_node_ref));
        // first meet empty should be the removed external node
        if sub_node.is_ext() {
            debug!("Probe ended at level {}, found child node {}", level, sub_node.type_name());
            (altered, box vec![])
        } else {
            prune::<KS, PS>(&sub_node_ref, altered, level + 1)
        }
    };
    if altered.is_empty() {
        debug!("Nothing to do at level {}, skipped", level);
        return (box level_page_altered, box vec![]);
    }
    debug!("Start prune at level {}", level);
    // Follwing procedures will scan the pages from left to right in key order, we need to sort it to
    // make sure the keys we are probing are also sorted
    altered.removed.sort_by(|a, b| a.0.cmp(&b.0));
    altered.key_modified.sort_by(|a, b| a.0.cmp(&b.0));
    altered.added.sort_by(|a, b| a.0.cmp(&b.0));

    let mut all_pages = probe_key_range(node, &altered, level);
    debug!(
        "Prune had selected {} pages at level {}",
        all_pages.len(),
        level
    );
    if all_pages.is_empty() {
        trace!("No node to prune at this level - {}", level);
        return (box level_page_altered, box vec![]);
    }
    debug_assert!(
        is_node_list_serial(&all_pages),
        "node list not serial after selection"
    );

    // insert new nodes
    insert_new_and_mark_altered_keys(&mut all_pages, &altered, &mut level_page_altered);
    debug!("Sub altred {}", altered.summary());
    debug!("Cur altered {}", level_page_altered.summary());
    if !altered.removed.is_empty() {
        // Locating refrences in the pages to be removed
        let page_children_to_be_retained = ref_to_be_retained(&mut all_pages, &altered, level);
        debug!(
            "Prune retains pages {}, empty {}",
            page_children_to_be_retained.len(),
            page_children_to_be_retained.iter().filter(|p| p.is_empty()).count()
        );
        if cfg!(debug_assertions) {
            for (pid, page) in page_children_to_be_retained.iter().enumerate() {
                for entry in page {
                    let node = read_unchecked::<KS, PS>(&entry.1);
                    debug_assert!(!node.is_empty_node(), "{} - {:?}, {}", pid, entry, node.type_name());
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
    {
        let mut current_altered = peek_altered_iter(&altered);
        all_pages.iter_mut().for_each(|p| {
            update_and_mark_altered_keys(p, &mut current_altered, &mut level_page_altered, level)
        });
        debug_assert!(
            current_altered.next().is_none(),
            "there are {} pages remain unaltered",
            current_altered.count() + 1
        );
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
        if all_pages.len() > 2 {
            debug_assert!(
                is_node_list_serial(&all_pages),
                "node not serial before checking corner cases"
            );
        
            if merge_single_ref_pages(&mut all_pages, &mut level_page_altered, level) {
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
    }

    debug!("Sub altred {}", altered.summary());
    debug!("Cur altered {}", level_page_altered.summary());
    debug!("Prune completed at level {}", level);
    (box level_page_altered, box all_pages)
}

fn removed_iter<'a>(altered_keys: &Box<AlteredNodes>) -> impl Iterator<Item = &AlterPair> {
    altered_keys.removed.iter()
}
fn altered_iter<'a>(altered_keys: &Box<AlteredNodes>) -> impl Iterator<Item = &AlterPair> {
    altered_keys.key_modified.iter()
}
fn added_iter<'a>(altered_keys: &Box<AlteredNodes>) -> impl Iterator<Item = &AlterPair> {
    altered_keys.added.iter()
}

fn peek_removed_iter<'a>(
    altered_keys: &Box<AlteredNodes>,
) -> Peekable<impl Iterator<Item = &AlterPair>> {
    removed_iter(altered_keys).peekable()
}
fn peek_altered_iter<'a>(
    altered_keys: &Box<AlteredNodes>,
) -> Peekable<impl Iterator<Item = &AlterPair>> {
    altered_iter(altered_keys).peekable()
}
fn peek_added_iter<'a>(
    altered_keys: &Box<AlteredNodes>,
) -> Peekable<impl Iterator<Item = &AlterPair>> {
    added_iter(altered_keys).peekable()
}

fn probe_key_range<KS, PS>(
    node: &NodeCellRef,
    altered: &Box<AlteredNodes>,
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
        let removed = removed_iter(&altered).map(|(e, _)| e).last().unwrap_or(&*MIN_ENTRY_KEY);
        let alted = altered_iter(&altered).map(|(e, _)| e).last().unwrap_or(&*MIN_ENTRY_KEY);
        let added = added_iter(&altered).map(|(e, _)| e).last().unwrap_or(&*MIN_ENTRY_KEY);
        [removed, alted, added].iter().max().unwrap().clone()
    };
    // This process will probe pages by all alter node types in this level to select the right pages
    // which contains those entries to work with
    loop {
        let (next, ends) = {
            let last_page = all_pages.last().unwrap().borrow();
            if last_page.is_none() {
                break;
            }
            debug_assert!(!last_page.is_empty_node());
            let last_innode = last_page.innode();
            debug_assert!(
                is_node_serial(last_page),
                "node not serial on fetching pages {:?} - {}",
                last_page.keys(),
                level
            );
            (
                write_node::<KS, PS>(&last_innode.right),
                &last_innode.right_bound > max_key
            )
        };
        // all_pages contains all of the entry keys we need to work for remove, add and modify
        all_pages.push(next);
        if ends {
            break;
        }
    }
    debug!("Prune selected at level {}, {} pages", level, all_pages.len());
    return all_pages;
}

fn insert_new_and_mark_altered_keys<KS, PS>(
    all_pages: &mut Vec<NodeWriteGuard<KS, PS>>,
    altered: &Box<AlteredNodes>,
    next_level_altered: &mut AlteredNodes,
) where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // write lock and insert all nodes marked new to current all pages
    // Note: Entries in all pages and altered nodes are sorted
    let mut new_nodes = peek_added_iter(altered);
    let mut pages = all_pages.iter_mut();
    let mut current_page = pages.next().unwrap();
    while let Some(&(new_key, new_node)) = new_nodes.peek() {
        if new_key < current_page.right_bound() {
            // First occurance of the page that have larger right bound than new node should be the
            // page to insert into
            let innode = (*current_page).innode_mut();
            let new_key = new_key.clone();
            let new_node_ref = new_node.clone();
            let pos = innode.search(&new_key);
            trace!("inserting new at {} with key {:?}", pos, new_key);
            if innode.len >= KS::slice_len() {
                let (split_ref, split_key) = innode.split_insert(new_key, new_node_ref, pos, true);
                next_level_altered.added.push((split_key, split_ref));
            } else {
                innode.insert_in_place(new_key, new_node_ref, pos, true);
            }
            new_nodes.next();
        } else {
            if let Some(next_page) = pages.next() {
                current_page = next_page;
            } else {
                break;
            }
        }
    }
}

fn ref_to_be_retained<'a, KS, PS>(
    all_pages: &mut Vec<NodeWriteGuard<KS, PS>>,
    altered: &Box<AlteredNodes>,
    level: usize,
) -> Vec<Vec<(usize, NodeCellRef)>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let removed = altered.removed.iter().map(|(_, r)| r.address()).collect::<HashSet<_>>();
    debug!("Remove set is {:?}, size {} at level {}", removed, altered.removed.len(), level);
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
            if page.is_none() {
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
    debug!("Remove count is {}, should be {} at level {}", remove_count, removed.len(), level);
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
            let is_not_none = !page.is_none();
            if is_not_none && retained_refs.len() == 0 {
                // check if all the children ptr in this page have been removed
                // if yes mark it and upper level will handel it
                level_page_altered
                    .removed
                    .push((page.right_bound().clone(), page.node_ref().clone()));
                // set length zero without do anything else
                // this will ease read hazard
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
    page: &mut NodeWriteGuard<KS, PS>,
    current_altered: &mut Peekable<impl Iterator<Item = &'a AlterPair>>,
    next_level_altered: &mut AlteredNodes,
    level: usize,
) where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    if !page.is_none() {
        debug_assert!(
            is_node_serial(page),
            "node not serial before update altered - {}",
            level
        );
        // update all nodes marked changed, not removed
        let page_ref = page.node_ref().clone();
        let innode = page.innode_mut();
        let innde_len = innode.len;

        // search for all children nodes in this page to find the altered pointers
        let marked_ptrs = innode.ptrs.as_slice_immute()[..innde_len + 1]
            .iter()
            .enumerate()
            .filter_map(|(i, child_ref)| {
                let mut found_key = None;
                if let Some(t) = current_altered.peek() {
                    let t: &&(EntryKey, NodeCellRef) = t;
                    let (key, ptr) = t;
                    if child_ref.ptr_eq(ptr) {
                        found_key = Some((i, key.clone()));
                    }
                }
                if found_key.is_some() {
                    current_altered.next();
                }
                found_key
            })
            .collect_vec();

        // alter keys corresponding to the ptr, which is ptr id - 1; 0 will postpone to upper level
        trace!("We have {} altered pointers", marked_ptrs.len());
        for (i, new_key) in marked_ptrs {
            // update key for children ptr, note that not all key can be updated in this level
            if i == 0 {
                // cannot update the key in current level
                // will postpone to upper level
                trace!("postpone key update to upper level {:?}", new_key);
                next_level_altered
                    .key_modified
                    .push((new_key, page_ref.clone()));
            } else {
                // can be updated, set the new key
                trace!("perform key update {:?}", new_key);
                debug_assert!(&new_key > &*MIN_ENTRY_KEY, "new key is empty at {}", i);
                innode.keys.as_slice()[i - 1] = new_key;
            }
        }
        debug_assert!(
            is_node_serial(page),
            "node not serial after update altered - {}, keys {:?}",
            level,
            page.keys()
        );
    }
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
        .filter(|p| !p.is_empty_node())
        .collect_vec();
    let right_refs = non_emptys
        .iter()
        .enumerate()
        .map(|(i, p)| {
            if p.is_none() {
                return NodeCellRef::default();
            }
            let right_ref = if i == non_emptys.len() - 1 {
                NodeCellRef::default()
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
            if !p.is_none() {
                *p.right_ref_mut().unwrap() = r
            }
        });
    return non_emptys;
}

// Return true if the case is handled
fn merge_single_ref_pages<KS, PS>(
    all_pages: &mut Vec<NodeWriteGuard<KS, PS>>,
    level_page_altered: &mut AlteredNodes,
    level: usize,
) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // dealing with corner cases
    // here, a page may have one ptr and no keys, then the remaining ptr need to be merge with right page
    trace!("Checking corner cases");
    let mut index = 0;
    let mut corner_case_handled = false;
    let mut current_left_bound = min_entry_key();
    while index < all_pages.len() {
        if index < all_pages.len() - 1 {
            if all_pages[index].is_none() || all_pages[index + 1].is_none() {
                continue;
            }
            let right_node_len = all_pages[index + 1].len();
            if right_node_len == 0 {
                // right node length is 0, should merge with current node
                let keys_cap = KS::slice_len();
                let current_node_len = all_pages[index].len();
                if current_node_len < keys_cap {
                    // current node is not full, can fit the only reference on the right
                    let (right_node_ptr, right_node_bound) = {
                        let right_node = all_pages[index + 1].innode();
                        (
                            right_node.ptrs.as_slice_immute()[0].clone(),
                            right_node.right_bound.clone(),
                        )
                    };
                    {
                        let mut current_node = all_pages[index].innode_mut();
                        current_node.keys.as_slice()[keys_cap - 1] =
                            current_node.right_bound.clone();
                        current_node.right_bound = right_node_bound;
                        current_node.ptrs.as_slice()[keys_cap] = right_node_ptr;
                        current_node.len += 1;
                        debug_assert_eq!(current_node.len, keys_cap);
                    }
                    all_pages[index + 1].make_empty_node(false);
                } else {
                    // current node is full, will need to move part of currrent node to right node
                    let (current_bound, current_ptr) = {
                        let current_node = all_pages[index].innode_mut();
                        debug_assert_eq!(current_node.len, keys_cap);
                        mem::swap(
                            &mut current_node.keys.as_slice()[keys_cap - 1],
                            &mut current_node.right_bound,
                        );
                        let current_right_bound = mem::replace(
                            &mut current_node.keys.as_slice()[keys_cap - 1],
                            EntryKey::new(),
                        );
                        let current_right_ptr = mem::replace(
                            &mut current_node.ptrs.as_slice()[keys_cap],
                            NodeCellRef::new_none::<KS, PS>(),
                        );
                        current_node.len -= 1;
                        (current_right_bound, current_right_ptr)
                    };
                    {
                        let right_node = all_pages[index + 1].innode_mut();
                        right_node.keys.as_slice()[0] = current_bound;
                        {
                            let right_node_ptrs = right_node.ptrs.as_slice();
                            right_node_ptrs[1] = right_node_ptrs[0].clone();
                            right_node_ptrs[0] = current_ptr;
                        }
                        right_node.len += 1;
                        debug_assert_eq!(right_node.len, 1);
                    }
                }
                index += 2;
                continue;
            }
        }
        let current_right_bound = all_pages[index].right_bound().clone();
        if all_pages[index].len() == 0 {
            // current page have one ptr and no keys
            // need to merge to the right page
            // if the right page is full, partial of the right page will be moved to the third page
            // the emptying node will always been cleaned
            // It is not legit to move keys and ptrs from right to left, I have tried and there are errors
            debug!(
                "Dealing with single ptr node {}, has {} pages in level {}",
                index,
                all_pages.len(),
                level
            );
            corner_case_handled = true;
            // extract keys, ptrs from right that will merge to left
            // new right key bound and right ref  from right (if right will be removed) also defines here
            let has_new = {
                let (remaining_key, remaining_ptr) = {
                    let current_innode: &InNode<KS, PS> = all_pages[index].innode();
                    let new_first_key = current_innode.right_bound.clone();
                    let new_first_ptr = current_innode.ptrs.as_slice_immute()[0].clone();
                    debug_assert!(
                        read_unchecked::<KS, PS>(&new_first_ptr).last_key() < &new_first_key
                    );
                    trace!("Using new first key as remaining key {:?}", new_first_key);
                    (new_first_key, new_first_ptr)
                };
                let mut new_next_keys = KS::init();
                let mut new_next_ptrs = PS::init();
                let mut has_new = None;

                let next = &mut all_pages[index + 1];
                debug_assert!(
                    is_node_serial(next),
                    "node not serial before next updated - {}",
                    level
                );

                {
                    let mut next_innode = next.innode_mut();

                    let keys_slice = new_next_keys.as_slice();
                    let ptrs_slice = new_next_ptrs.as_slice();
                    debug_assert!(current_right_bound < next_innode.right_bound);
                    debug_assert!(current_right_bound <= next_innode.keys.as_slice_immute()[0],
                                  "Bad boundary current right bound {:?} should less than or eq next first element {:?}",
                                  current_right_bound, next_innode.keys.as_slice_immute()[0]);
                    keys_slice[0] = remaining_key;
                    ptrs_slice[0] = remaining_ptr;
                    let next_len = next_innode.len;
                    if next_len >= KS::slice_len() {
                        // full node, need to be split and relocated
                        trace!("Full node, will split");
                        let mid = next_len / 2;
                        // Moving keys
                        for (i, k) in next_innode.keys.as_slice_immute()[..mid].iter().enumerate() {
                            keys_slice[i + 1] = k.clone();
                        }
                        // Moving ptrs
                        for (i, p) in next_innode.ptrs.as_slice_immute()[..=mid]
                            .iter()
                            .enumerate()
                        {
                            ptrs_slice[i + 1] = p.clone();
                        }

                        // Work on the second pard of the split node
                        let mut third_node_keys = KS::init();
                        let mut third_node_ptrs = PS::init();
                        // Moving keys
                        // Note that the key at mid will be assigned as boundary
                        for (i, k) in next_innode.keys.as_slice_immute()[mid + 1..next_len]
                            .iter()
                            .enumerate()
                        {
                            third_node_keys.as_slice()[i] = k.clone();
                        }
                        // Moving ptrs
                        for (i, p) in next_innode.ptrs.as_slice_immute()[mid + 1..next_len + 1]
                            .iter()
                            .enumerate()
                        {
                            third_node_ptrs.as_slice()[i] = p.clone();
                        }
                        let next_right_bound = next_innode.keys.as_slice_immute()[mid].clone();
                        let third_len = next_len - mid - 1;
                        // Use the mid key as boundary
                        let third_right_bound = next_right_bound.clone();
                        let third_right_ptr = next_innode.right.clone();
                        let third_innode = InNode {
                            keys: third_node_keys,
                            ptrs: third_node_ptrs,
                            len: third_len,
                            right: third_right_ptr,
                            right_bound: third_right_bound,
                        };
                        let third_node_ref =
                            NodeCellRef::new(Node::new(NodeData::Internal(box third_innode)));
                        next_innode.right = third_node_ref.clone();
                        next_innode.right_bound = next_right_bound.clone();
                        next_innode.keys = new_next_keys;
                        next_innode.ptrs = new_next_ptrs;
                        next_innode.len = mid + 1;
                        // insert the third page
                        level_page_altered
                            .added
                            .push((next_right_bound.clone(), third_node_ref.clone()));

                        // return the locked third node to be inserted into the all_pages

                        trace!("Acquiring for third node");
                        let third_node = write_node::<KS, PS>(&third_node_ref);
                        debug_assert!(
                            is_node_serial(&third_node),
                            "node not serial for third node - {}",
                            level
                        );
                        debug_assert!(
                            third_node.first_key()
                                > read_unchecked::<KS, PS>(
                                    &third_node.innode().ptrs.as_slice_immute()[0]
                                )
                                .last_key()
                        );
                        has_new = Some(third_node)
                    } else {
                        // not full node, can be relocated
                        trace!("Not full node, wil relocated");
                        for (i, k) in next_innode.keys.as_slice_immute()[..next_len]
                            .iter()
                            .enumerate()
                        {
                            keys_slice[i + 1] = k.clone();
                        }
                        for (i, p) in next_innode.ptrs.as_slice_immute()[..=next_len]
                            .iter()
                            .enumerate()
                        {
                            ptrs_slice[i + 1] = p.clone();
                        }
                        next_innode.keys = new_next_keys;
                        next_innode.ptrs = new_next_ptrs;
                        next_innode.len = next_len + 1;
                    }
                }

                debug_assert!(
                    is_node_serial(next),
                    "node not serial after next updated - {} - {:?}",
                    level,
                    &next.keys()
                );
                debug_assert!(&current_right_bound > &*MIN_ENTRY_KEY);
                debug_assert!(
                    next.first_key()
                        > read_unchecked::<KS, PS>(&next.innode().ptrs.as_slice_immute()[0])
                            .last_key()
                );

                if &current_left_bound != &*MIN_ENTRY_KEY {
                    // modify next node key
                    level_page_altered
                        .key_modified
                        .push((current_left_bound.clone(), next.node_ref().clone()));
                } else {
                    trace!("Skipped modify key for left bound is min key");
                }

                // make current node empty
                level_page_altered.removed.push((
                    current_right_bound.clone(),
                    all_pages[index].node_ref().clone(),
                ));
                all_pages[index].make_empty_node(false);
                has_new
            };
            index += if let Some(new_page) = has_new {
                all_pages.insert(index + 1, new_page);
                // because there have been a new inserted page, it have to been skipped with next node
                2
            } else {
                1
            };
        }
        current_left_bound = current_right_bound;
        index += 1;
    }
    return corner_case_handled;
}
