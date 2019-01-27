use index::btree::internal::InNode;
use index::btree::node::read_node;
use index::btree::node::read_unchecked;
use index::btree::node::write_node;
use index::btree::node::write_targeted_extnode;
use index::btree::node::EmptyNode;
use index::btree::node::NodeData;
use index::btree::node::NodeWriteGuard;
use index::btree::search::mut_search;
use index::btree::search::MutSearchResult;
use index::btree::BPlusTree;
use index::btree::NodeCellRef;
use index::lsmtree::LEVEL_PAGE_DIFF_MULTIPLIER;
use index::EntryKey;
use index::Slice;
use itertools::Itertools;
use smallvec::SmallVec;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::mem;

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

fn select<KS, PS>(node: &NodeCellRef) -> Vec<NodeWriteGuard<KS, PS>>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let search = mut_search::<KS, PS>(node, &smallvec!());
    match search {
        MutSearchResult::External => {
            let mut collected = vec![write_node(node)];
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

fn apply_removal<'a, KS, PS>(
    cursor_guard: &mut NodeWriteGuard<KS, PS>,
    poses: &mut BTreeSet<usize>,
    key_to_del: &'a EntryKey,
    empty_pages: &mut Vec<&'a EntryKey>,
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
                mem::swap(&mut new_keys.as_slice()[i], key);
            }
            for (i, ptr) in ptrs.into_iter().enumerate() {
                mem::swap(&mut new_ptrs.as_slice()[i], ptr);
            }
        }
        innode.keys = new_keys;
        innode.ptrs = new_ptrs;

        debug!(
            "Pruned page have keys {:?}",
            &innode.keys.as_slice_immute()[..innode.len]
        );
    }

    if cursor_guard.is_empty() {
        debug!("Pruned page is empty: {:?}", key_to_del);
        cursor_guard.make_empty_node();
        empty_pages.push(key_to_del);
    }
    poses.clear();
}

fn upper_bound<KS, PS>(node: &NodeWriteGuard<KS, PS>) -> EntryKey
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
{
    let right_most_child = &node.innode().ptrs.as_slice_immute()[node.len()];
    let right_node = read_unchecked::<KS, PS>(right_most_child);
    if right_node.is_empty() {
        &*node
    } else {
        &*right_node
    }.keys().last().unwrap().clone()
}

fn lower_bound<KS, PS>(node: &NodeWriteGuard<KS, PS>) -> EntryKey
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
{
    let left_most_child = &node.innode().ptrs.as_slice_immute()[0];
    let left_node = read_unchecked::<KS, PS>(left_most_child);
    if left_node.is_empty() {
        &*node
    } else {
        &*left_node
    }.keys().first().unwrap().clone()
}

fn shift_to_right_innode<KS, PS>(
    mut search_node: NodeWriteGuard<KS, PS>,
    key: &EntryKey,
    node_upper_bound: &mut EntryKey,
) -> NodeWriteGuard<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    if search_node.is_empty() || (search_node.len() > 0 && &*node_upper_bound < key) {
        debug!("Shifting to right node for {:?}", key);
        let right_ref = search_node.right_ref_mut_no_empty().unwrap();
        let right_node = write_node(right_ref);
        let right_upper_bound = upper_bound(&right_node);
        if !right_node.is_none() && (right_node.len() > 0 && &lower_bound(&right_node) <= key) {
            *node_upper_bound = right_upper_bound;
            return shift_to_right_innode(right_node, key, node_upper_bound);
        }
    }
    search_node
}

fn prune_selected<'a, KS, PS>(node: &NodeCellRef, mut keys: Vec<&'a EntryKey>) -> Vec<&'a EntryKey>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let key_len = keys.len();
    let first_search = mut_search::<KS, PS>(node, keys.first().unwrap());
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
    match pruning {
        PruningSearch::DeepestInnode => {
            debug!("Removing in deepest nodes keys {:?}", &keys);
        }
        PruningSearch::Innode(sub_node) => {
            keys = prune_selected::<KS, PS>(&sub_node, keys);
        }
    }
    // empty page references that will dealt with by upper level
    let mut empty_pages = vec![];
    if !keys.is_empty() {
        debug!("Pruning page containing keys {:?}", &keys);
        // start delete
        let mut cursor_guard = write_node::<KS, PS>(node);
        let mut guard_removing_poses = BTreeSet::new();
        let mut cursor_upper_bound = upper_bound(&cursor_guard);
        debug!(
            "Prune deepest node starting key: {:?}",
            cursor_guard.first_key()
        );
        for (i, key_to_del) in keys.into_iter().enumerate() {
            if key_to_del > &cursor_upper_bound {
                debug!(
                    "Applying removal for overflow current page ({}/{}) {:?} < {:?}. {:?}",
                    guard_removing_poses.len(),
                    cursor_guard.len() + 1,
                    cursor_upper_bound,
                    key_to_del,
                    &cursor_guard.keys()[..cursor_guard.len()]
                );
                apply_removal(
                    &mut cursor_guard,
                    &mut guard_removing_poses,
                    key_to_del,
                    &mut empty_pages,
                );
                debug!(
                    "Applied removal for overflow current page ({})",
                    cursor_guard.len()
                );
                cursor_guard =
                    shift_to_right_innode(cursor_guard, key_to_del, &mut cursor_upper_bound);
                debug_assert!(!cursor_guard.is_empty_node());
            }
            let pos = cursor_guard.search(key_to_del);
            debug!("Key to delete have position {}, key: {:?}", pos, key_to_del);
            guard_removing_poses.insert(pos);
            if i == key_len - 1 {
                debug!("Applying removal for last key");
                apply_removal(
                    &mut cursor_guard,
                    &mut guard_removing_poses,
                    key_to_del,
                    &mut empty_pages,
                );
            }
        }
    }
    empty_pages
}

pub fn level_merge<KSA, PSA, KSB, PSB>(
    src_tree: &BPlusTree<KSA, PSA>,
    dest_tree: &BPlusTree<KSB, PSB>,
) -> usize
where
    KSA: Slice<EntryKey> + Debug + 'static,
    PSA: Slice<NodeCellRef> + 'static,
    KSB: Slice<EntryKey> + Debug + 'static,
    PSB: Slice<NodeCellRef> + 'static,
{
    let left_most_leaf_guards = select::<KSA, PSA>(&src_tree.get_root());
    let merge_page_len = left_most_leaf_guards.len();
    debug!("Merge selected {} pages", left_most_leaf_guards.len());

    // merge to dest_tree
    {
        let keys: Vec<EntryKey> = left_most_leaf_guards
            .iter()
            .map(|g| &g.keys()[..g.len()])
            .flatten()
            .cloned()
            .collect_vec();
        debug!("Merge selected keys are {:?}", &keys);
        dest_tree.merge_with_keys(keys);
    }

    // cleanup upper level references
    {
        let page_keys = left_most_leaf_guards
            .iter()
            .filter(|g| !g.is_empty())
            .map(|g| g.first_key())
            .collect_vec();
        prune_selected::<KSA, PSA>(&src_tree.get_root(), page_keys);
    }

    // adjust leaf left, right references
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
    merge_page_len
}
