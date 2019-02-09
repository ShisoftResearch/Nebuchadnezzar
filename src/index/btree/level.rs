use index::btree::internal::InNode;
use index::btree::node::is_node_locked;
use index::btree::node::read_node;
use index::btree::node::read_unchecked;
use index::btree::node::write_node;
use index::btree::node::write_targeted;
use index::btree::node::write_unchecked;
use index::btree::node::EmptyNode;
use index::btree::node::NodeData;
use index::btree::node::NodeWriteGuard;
use index::btree::search::mut_search;
use index::btree::search::MutSearchResult;
use index::btree::BPlusTree;
use index::btree::LevelTree;
use index::btree::NodeCellRef;
use index::lsmtree::LEVEL_PAGE_DIFF_MULTIPLIER;
use index::EntryKey;
use index::Slice;
use itertools::Itertools;
use smallvec::SmallVec;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::mem;
use index::btree::node::write_non_empty;

enum Selection<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    Selected(Vec<NodeWriteGuard<KS, PS>>),
    Innode(NodeCellRef),
}

enum PruningSearch {
    DeepestInnode,
    Innode(NodeCellRef),
}

struct NodeRemoval<'a> {
    empty_pages: Vec<&'a EntryKey>,
    index_changed: Vec<(EntryKey, EntryKey)>,
    split: Vec<(NodeCellRef, EntryKey)>
}

impl <'a> NodeRemoval <'a> {
    fn new() -> Self {
        Self {
            empty_pages: vec![],
            index_changed: vec![],
            split: vec![]
        }
    }
}

fn select<KS, PS>(node: &NodeCellRef) -> Vec<NodeWriteGuard<KS, PS>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let search = mut_search::<KS, PS>(node, &smallvec!());
    match search {
        MutSearchResult::External => {
            let mut collected = vec![write_non_empty(write_node(node))];
            while collected.len() < LEVEL_PAGE_DIFF_MULTIPLIER {
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
                    collected.push(right);
                }
            }
            return collected;
        }
        MutSearchResult::Internal(node) => select::<KS, PS>(&node),
    }
}

fn merge_innode_remnant<'a, KS, PS>(current_node: &mut NodeWriteGuard<KS, PS>, prev_key: &'a EntryKey, removal: &mut NodeRemoval)
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
{
    let curr_right_ref = current_node.right_ref_mut_no_empty().unwrap().clone();
    let curr_innode = current_node.innode_mut();
    let curr_right_bound = &curr_innode.right_bound;
    let curr_last_child = mem::replace(&mut curr_innode.ptrs.as_slice()[0], NodeCellRef::default());
    debug_assert_eq!(curr_innode.len, 0);
    if curr_last_child.is_default() {
        return;
    }
    let mut next_node = write_targeted::<KS, PS>(write_node(&curr_right_ref), curr_right_bound);
    let pos = next_node.search(curr_right_bound);
    if pos == 0 {
        removal.index_changed.push((next_node.keys().first().unwrap().clone(), prev_key.clone()));
    }
    let mut next_innode = next_node.innode_mut();
    let overflow = if next_innode.len == KS::slice_len() {
        Some(next_innode.split_insert(curr_right_bound.clone(), curr_last_child, pos, false))
    } else {
        next_innode.insert_in_place(curr_right_bound.clone(), curr_last_child, pos, false);
        None
    };
    if let Some(tuple) = overflow {
        removal.split.push(tuple);
    }
}

fn apply_removal<'a, KS, PS>(
    cursor_guard: &mut NodeWriteGuard<KS, PS>,
    poses: &mut BTreeSet<usize>,
    node_removal: &mut NodeRemoval<'a>,
    prev_key: &Option<&'a EntryKey>,
    remove_children_right_nodes: bool,
) where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    if poses.is_empty() {
        return;
    }
    debug!(
        "Applying removal, guard key {:?}, poses {:?}",
        cursor_guard.first_key(),
        poses
    );
    {
        let mut innode = cursor_guard.innode_mut();
        let mut new_keys = KS::init();
        let mut new_ptrs = PS::init();
        {
            if remove_children_right_nodes {
                // A node will be reclaimed if and only if it have zero references
                // If there is any sequential empty nodes, they have to be unlinked each other
                // The upper level will do the rest to unlink it from the tree
                innode.ptrs.as_slice()[..innode.len + 1]
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| poses.contains(i))
                    .for_each(|(_, r)| {
                        write_node::<KS, PS>(r).right_ref_mut_no_empty();
                    });
            }
            let mut keys: Vec<&mut _> = innode.keys.as_slice()[..innode.len]
                .iter_mut()
                .enumerate()
                .filter(|(i, _)| !poses.contains(i))
                .map(|(_, k)| k)
                .collect();
            let mut ptrs: Vec<&mut _> = innode.ptrs.as_slice()[..innode.len + 1]
                .iter_mut()
                .enumerate()
                .filter(|(i, _)| !poses.contains(i))
                .map(|(_, p)| p)
                .collect();
            innode.len = keys.len();
            debug!("Prune filtered page have keys {:?}", &keys);
            for (i, key) in keys.into_iter().enumerate() {
                debug_assert!(key > &mut smallvec!(0));
                mem::swap(&mut new_keys.as_slice()[i], key);
            }
            for (i, ptr) in ptrs.into_iter().enumerate() {
                debug_assert!(!ptr.is_default());
                mem::swap(&mut new_ptrs.as_slice()[i], ptr);
            }
        }
        innode.keys = new_keys;
        innode.ptrs = new_ptrs;
        innode.debug_check_integrity();

        debug!(
            "Pruned page have keys {:?}",
            &innode.keys.as_slice_immute()[..innode.len]
        );
    }

    if cursor_guard.is_empty() {
        if let &Some(k) = prev_key {
            node_removal.empty_pages.push(k);
            if !cursor_guard.is_ext() {
                merge_innode_remnant(cursor_guard, k, node_removal);
            }
        }
        debug!("Pruned page is empty: {:?}", prev_key);
        cursor_guard.make_empty_node();
    }
    poses.clear();
}

fn prune_selected<'a, KS, PS>(
    node: &NodeCellRef,
    mut removal: Box<NodeRemoval<'a>>,
    level: usize,
) -> Box<NodeRemoval<'a>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let first_search = mut_search::<KS, PS>(node, removal.empty_pages.first().unwrap());
    let pruning = match first_search {
        MutSearchResult::Internal(sub_node) => {
            if read_unchecked::<KS, PS>(&sub_node).is_ext() {
                PruningSearch::DeepestInnode
            } else {
                PruningSearch::Innode(sub_node)
            }
        }
        MutSearchResult::External => unreachable!(),
    };
    let mut deepest = false;
    match pruning {
        PruningSearch::DeepestInnode => {
            debug!(
                "Removing in deepest nodes keys {:?}, level {}",
                &removal.empty_pages, level
            );
            deepest = true;
        }
        PruningSearch::Innode(sub_node) => {
            removal = prune_selected::<KS, PS>(&sub_node, removal, level + 1);
        }
    }
    // empty page references that will dealt with by upper level
    let mut upper_removal = NodeRemoval::new();
    if !removal.empty_pages.is_empty() {
        debug!("Pruning page containing keys {:?}, level {}", &removal.empty_pages, level);
        // start delete
        let mut cursor_guard = write_node::<KS, PS>(node);
        let mut guard_removing_poses = BTreeSet::new();
        let mut prev_key = None;
        debug!(
            "Prune deepest node starting key: {:?}, level {}",
            cursor_guard.first_key(),
            level
        );
        for (i, &key_to_del) in removal.empty_pages.iter().enumerate() {
            if key_to_del >= cursor_guard.right_bound() {
                debug!(
                    "Applying removal for overflow current page ({}/{}) key: {:?} >= bound: {:?}. guard keys: {:?}, level {}",
                    guard_removing_poses.len(),
                    cursor_guard.len() + 1,
                    key_to_del,
                    cursor_guard.right_bound(),
                    &cursor_guard.keys()[..cursor_guard.len()],
                    level
                );
                apply_removal(
                    &mut cursor_guard,
                    &mut guard_removing_poses,
                    &mut upper_removal,
                    &prev_key,
                    !deepest,
                );
                cursor_guard = write_targeted(cursor_guard, key_to_del);
                debug_assert!(!cursor_guard.is_empty_node());
                debug!(
                    "Applied removal for overflow current page ({}), level {}",
                    cursor_guard.len(),
                    level
                );
            }
            let pos = cursor_guard.search(key_to_del);
            debug!(
                "Key to delete have position {}, key: {:?}, level {}",
                pos, key_to_del, level
            );
            guard_removing_poses.insert(pos);
            prev_key = Some(key_to_del)
        }
        if !guard_removing_poses.is_empty() {
            debug!(
                "Applying removal for last keys {:?}, level {}",
                &guard_removing_poses, level
            );
            apply_removal(
                &mut cursor_guard,
                &mut guard_removing_poses,
                &mut upper_removal,
                &prev_key,
                !deepest,
            );
        }
    }
    if !removal.split.is_empty() {
        let mut cursor_guard = write_node::<KS, PS>(node);
        for (node, pivot) in &removal.split {
            cursor_guard = write_targeted(cursor_guard, &pivot);
            let pos = cursor_guard.search(&pivot);
            let innode = cursor_guard.innode_mut();
            let pivot = pivot.clone();
            let node = node.clone();
            if innode.len == KS::slice_len() {
                upper_removal.split.push(innode.split_insert(pivot, node, pos, true));
            } else {
                innode.insert_in_place(pivot, node, pos, true)
            }
        }
    }
    if !removal.index_changed.is_empty() {
        let mut cursor_guard = write_node::<KS, PS>(node);
        for (search_key, change) in &removal.index_changed {
            cursor_guard = write_targeted(cursor_guard, search_key);
            let pos = cursor_guard.search(search_key);
            let current_key = &mut cursor_guard.innode_mut().keys.as_slice()[pos];
            *current_key = change.clone();
        }
    }
    debug!("Have empty nodes {:?}, level {:?}", &upper_removal.empty_pages, level);
    box upper_removal
}

pub fn level_merge<KSA, PSA>(src_tree: &BPlusTree<KSA, PSA>, dest_tree: &LevelTree) -> usize
where
    KSA: Slice<EntryKey> + Debug + 'static,
    PSA: Slice<NodeCellRef> + 'static,
{
    let left_most_leaf_guards = select::<KSA, PSA>(&src_tree.get_root());
    let merge_page_len = left_most_leaf_guards.len();
    debug!("Merge selected {} pages", left_most_leaf_guards.len());

    // merge to dest_tree
    {
        let mut deleted_keys = src_tree.deleted.write();
        let keys: Vec<EntryKey> = left_most_leaf_guards
            .iter()
            .map(|g| &g.keys()[..g.len()])
            .flatten()
            .filter(|&k| !deleted_keys.remove(k))
            .cloned()
            .collect_vec();
        debug!("Merge selected keys are {:?}", &keys);
        dest_tree.merge_with_keys(box keys);
    }

    // cleanup upper level references
    {
        let page_keys = left_most_leaf_guards
            .iter()
            .filter(|g| !g.is_empty())
            .map(|g| g.first_key())
            .collect_vec();
        let mut removal = NodeRemoval::new();
        removal.empty_pages = page_keys;
        prune_selected::<KSA, PSA>(&src_tree.get_root(), box removal, 0);
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

        for mut g in left_most_leaf_guards {
            *g = NodeData::Empty(box EmptyNode {
                left: Some(left_left_most.clone()),
                right: right_right_most.clone(),
            })
        }
    }

    merge_page_len
}
