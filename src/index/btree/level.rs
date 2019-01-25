use index::btree::internal::InNode;
use index::btree::node::read_node;
use index::btree::node::read_unchecked;
use index::btree::node::write_key_page;
use index::btree::node::write_node;
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
    let selection = read_node(node, |node_handler| {
        debug_assert!(!node_handler.is_ext());
        let innode: &InNode<KS, PS> = node_handler.innode();
        let first_node = innode.ptrs.as_slice_immute().first().unwrap();
        if read_unchecked::<KS, PS>(first_node).is_ext() {
            Selection::Selected(
                innode
                    .ptrs
                    .as_slice_immute()
                    .iter()
                    .take(LEVEL_PAGE_DIFF_MULTIPLIER)
                    .map(|r| write_node::<KS, PS>(r))
                    .collect(),
            )
        } else {
            Selection::Innode(first_node.clone())
        }
    });
    match selection {
        Selection::Selected(res) => res,
        Selection::Innode(node) => select::<KS, PS>(&node),
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
    {
        let innode = cursor_guard.innode_mut();
        let mut new_keys = KS::init();
        let mut new_ptrs = PS::init();
        {
            let keys: Vec<_> = innode
                .keys
                .as_slice()
                .into_iter()
                .enumerate()
                .filter(|(i, _)| !poses.contains(i))
                .map(|(_, k)| k)
                .collect();
            let ptrs: Vec<_> = innode
                .ptrs
                .as_slice()
                .into_iter()
                .enumerate()
                .filter(|(i, _)| !poses.contains(i))
                .map(|(_, p)| p)
                .collect();
            innode.len = keys.len();
            for (i, key) in keys.into_iter().enumerate() {
                mem::swap(&mut new_keys.as_slice()[i], key);
            }
            for (i, ptr) in ptrs.into_iter().enumerate() {
                mem::swap(&mut new_ptrs.as_slice()[i], ptr);
            }
        }
        innode.keys = new_keys;
        innode.ptrs = new_ptrs;
    }

    if cursor_guard.is_empty() {
        cursor_guard.make_empty_node();
        empty_pages.push(key_to_del);
    }
    *poses = BTreeSet::new();
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
    // empty page references that will dealt with by upper level
    let mut empty_pages = vec![];
    match pruning {
        PruningSearch::DeepestInnode => {}
        PruningSearch::Innode(sub_node) => {
            keys = prune_selected::<KS, PS>(&sub_node, keys);
        }
    }
    // start delete
    let mut cursor_guard = write_node::<KS, PS>(node);
    let mut guard_removing_poses = BTreeSet::new();
    for (i, key_to_del) in keys.into_iter().enumerate() {
        if cursor_guard.last_key() < key_to_del {
            apply_removal(
                &mut cursor_guard,
                &mut guard_removing_poses,
                key_to_del,
                &mut empty_pages,
            );
            cursor_guard = write_key_page(cursor_guard, key_to_del);
        }
        let pos = cursor_guard.search(key_to_del);
        guard_removing_poses.insert(pos);
        if i == key_len - 1 {
            apply_removal(
                &mut cursor_guard,
                &mut guard_removing_poses,
                key_to_del,
                &mut empty_pages,
            );
        }
    }
    empty_pages
}

pub fn level_merge<KSA, PSA, KSB, PSB>(
    src_tree: &BPlusTree<KSA, PSA>,
    dest_tree: &BPlusTree<KSB, PSB>,
) where
    KSA: Slice<EntryKey> + Debug + 'static,
    PSA: Slice<NodeCellRef> + 'static,
    KSB: Slice<EntryKey> + Debug + 'static,
    PSB: Slice<NodeCellRef> + 'static,
{
    let left_most_leaf_guards = select::<KSA, PSA>(&src_tree.get_root());

    // merge to dest_tree
    {
        let keys: Vec<EntryKey> = left_most_leaf_guards
            .iter()
            .map(|g| g.keys())
            .flatten()
            .cloned()
            .collect_vec();
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
}
