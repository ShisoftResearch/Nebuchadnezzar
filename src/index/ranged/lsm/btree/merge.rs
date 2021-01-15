use super::external;
use super::node::*;
use super::search::mut_search;
use super::search::MutSearchResult;
use super::*;
use itertools::Itertools;
use std::fmt::Debug;

pub fn merge_into_internal<KS, PS>(
    node: &NodeCellRef,
    lower_level_new_pages: BTreeMap<EntryKey, NodeCellRef>,
    new_pages: &mut BTreeMap<EntryKey, NodeCellRef>,
) where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let node_guard = write_node::<KS, PS>(node);
    merge_into_internal_guard(node_guard, lower_level_new_pages, new_pages)
}

pub fn merge_into_internal_guard<KS, PS>(
    mut node_guard: NodeWriteGuard<KS, PS>,
    lower_level_new_pages: BTreeMap<EntryKey, NodeCellRef>,
    new_pages: &mut BTreeMap<EntryKey, NodeCellRef>,
) where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    for (pivot, node) in lower_level_new_pages.into_iter() {
        let mut target_guard = write_targeted(node_guard, &pivot);
        {
            debug_assert!(!target_guard.is_none());
            let innode = target_guard.innode_mut();
            let pos = innode.search(&pivot);
            if innode.len >= KS::slice_len() {
                // TODO: check boundary
                // full node, going to split
                let (node_ref, key) = innode.split_insert(pivot, node, pos, true);
                new_pages.insert(key, node_ref);
            } else {
                innode.insert_in_place(pivot, node, pos, true);
            }
        }
        node_guard = target_guard;
    }
}

pub fn new_internal_node<KS, PS>(
    left_most: &NodeCellRef,
    new_pages: &mut BTreeMap<EntryKey, NodeCellRef>,
) -> NodeCellRef
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut new_keys = KS::init();
    let mut new_ptrs = PS::init();
    let first_key = new_pages.keys().next().unwrap().clone();
    let first_ptr = new_pages.remove(&first_key).unwrap();
    new_ptrs.as_slice()[0] = left_most.clone();
    new_ptrs.as_slice()[1] = first_ptr;
    new_keys.as_slice()[0] = first_key;
    let mut new_innode = InNode::<KS, PS>::new(1, EntryKey::max());
    new_innode.keys = new_keys;
    new_innode.ptrs = new_ptrs;
    NodeCellRef::new(Node::with_internal(new_innode))
}

fn debug_check_serialized(keys: &Vec<EntryKey>) {
    if cfg!(debug_assertions) && keys.len() > 0 {
        for i in 0..keys.len() - 1 {
            let left = &keys[i];
            let right = &keys[i + 1];
            assert!(left < right, "at {}, {:?} >= {:?}", i, left, right);
        }
    }
}

pub fn merge_into_tree_node<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node: &NodeCellRef,
    _parent: &NodeCellRef,
    keys: Vec<EntryKey>,
    level: usize,
) -> BTreeMap<EntryKey, NodeCellRef>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    debug_check_serialized(&keys);
    let search = mut_search::<KS, PS>(node, &keys[0]);
    let new_pages = match search {
        MutSearchResult::External => {
            // merge keys into internal pages
            // this is a oneshot action.
            // after the merge, it will return all new inserted new pages to upper level
            debug!(
                "Merge into external at level {} with {} keys",
                level,
                keys.len()
            );
            let keys_len = keys.len();
            let mut merging_pos = 0;
            let mut current_guard = write_node::<KS, PS>(&node);
            let mut new_pages = BTreeMap::new();
            debug!("Start external merging by pages");
            // merge by pages
            while merging_pos < keys_len {
                let start_key = &keys[merging_pos];
                trace!(
                    "Locking on target starts from {:?} for {:?}",
                    current_guard.node_ref(),
                    start_key
                );
                current_guard = write_targeted(current_guard, start_key);
                let remain_slots = KS::slice_len() - current_guard.len();
                trace!(
                    "Locked on targed with {:?}, remaining slots {}",
                    current_guard.node_ref(),
                    remain_slots
                );
                if remain_slots > 0 {
                    let ext_node = current_guard.extnode_mut(tree);
                    ext_node.remove_contains(&*tree.deletion);
                    let selection = keys[merging_pos..keys_len]
                        .iter()
                        .filter(|&k| k < &ext_node.right_bound)
                        .take(remain_slots)
                        .collect_vec();
                    trace!("Merge by merge sort with {:?}", ext_node.id);
                    ext_node.merge_sort(selection.as_slice());
                    merging_pos += selection.len();
                } else if remain_slots == 0 {
                    let insert_pos = current_guard.search(&start_key);
                    let target_node_ref = current_guard.node_ref().clone();
                    let right_node_ref = current_guard.right_ref().unwrap();
                    trace!(
                        "Merge with node split at {:?}, locking on right node {:?}",
                        target_node_ref,
                        right_node_ref
                    );
                    #[cfg(debug_assertions)]
                    if is_node_locked::<KS, PS>(right_node_ref) {
                        unsafe {
                            trace!(
                                "Right node {:?} is LOCKED!!! current id {:?} lock thread id {:?}",
                                &right_node_ref,
                                std::thread::current().id(),
                                right_node_ref.get_backtrace()
                            );
                        }
                    }
                    let mut right_guard = write_node::<KS, PS>(right_node_ref);
                    trace!("Split insert into {:?}", target_node_ref);
                    let (new_node, pivot) = current_guard.extnode_mut(tree).split_insert(
                        start_key.clone(),
                        insert_pos,
                        &target_node_ref,
                        &mut right_guard,
                        tree,
                    );
                    drop(right_guard);
                    merging_pos += 1;
                    external::make_changed(&new_node, tree);
                    new_pages.insert(pivot, new_node);
                }
                trace!("Key {:?} merged", start_key);
            }
            debug!("External merge completed at level {}", level);
            if cfg!(debug_assertions) {
                let page_keys = new_pages.iter().map(|t| t.0.clone()).collect_vec();
                if !verification::are_keys_serial(page_keys.as_slice()) {
                    error!("External produced page keys not serial {:?}", page_keys);
                }
            }
            new_pages
        }
        MutSearchResult::Internal(sub_node) => {
            let lower_level_new_pages =
                merge_into_tree_node(tree, &sub_node, node, keys, level + 1);
            let mut new_pages = BTreeMap::new();
            if lower_level_new_pages.len() > 0 {
                merge_into_internal::<KS, PS>(node, lower_level_new_pages, &mut new_pages);
            }
            if cfg!(debug_assertions) {
                let page_keys = new_pages.iter().map(|t| t.0.clone()).collect_vec();
                if !verification::are_keys_serial(page_keys.as_slice()) {
                    error!("Internal produced page keys not serial {:?}", page_keys);
                }
            }
            new_pages
        }
    };
    if level == 0 && new_pages.len() > 0 {
        // it is impossible to have a node been changed during merge for merges are performed in serial
        debug_assert_eq!(
            read_unchecked::<KS, PS>(&tree.get_root()).first_key(),
            read_unchecked::<KS, PS>(&node).first_key()
        )
    }
    return new_pages;
}
