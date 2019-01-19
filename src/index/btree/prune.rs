use index::btree::NodeCellRef;
use index::EntryKey;
use smallvec::SmallVec;
use index::btree::search::mut_search;
use index::btree::node::read_node;
use index::Slice;
use index::btree::internal::InNode;
use index::btree::node::read_unchecked;
use std::fmt::Debug;
use index::btree::node::write_node;
use index::btree::BPlusTree;
use index::lsmtree::LEVEL_PAGE_DIFF_MULTIPLIER;
use index::btree::node::NodeWriteGuard;
use itertools::Itertools;
use index::btree::search::MutSearchResult;
use index::btree::node::write_key_page;

enum Selection<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    Selected(Vec<(EntryKey, NodeWriteGuard<KS, PS>)>),
    Innode(NodeCellRef)
}

enum Pruning {
    DeepestInnode,
    Innode(NodeCellRef),
}

fn select<KS, PS>(node: &NodeCellRef) -> Vec<(EntryKey, NodeWriteGuard<KS, PS>)>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    let selection = read_node(node, |node_handler| {
        debug_assert!(!node_handler.is_ext());
        let innode: &InNode<KS, PS> = node_handler.innode();
        let first_node = innode.ptrs.as_slice_immute().first().unwrap();
        if read_unchecked::<KS, PS>(first_node).is_ext() {
            let mut keys = innode.keys.as_slice_immute().iter();
            keys.next(); // skip the first
            Selection::Selected(innode.ptrs
                .as_slice_immute()
                .iter()
                .zip(keys)
                .take(LEVEL_PAGE_DIFF_MULTIPLIER)
                .map(|(r, k)| (k.clone(), write_node::<KS, PS>(r)))
                .collect())
        } else {
            Selection::Innode(first_node.clone())
        }
    });
    match selection {
        Selection::Selected(res) => res,
        Selection::Innode(node) => select::<KS, PS>(&node)
    }
}

fn prune_selected<KS, PS>(node: &NodeCellRef, keys: Vec<&EntryKey>)
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    let first_search = mut_search::<KS, PS>(node, keys.first().unwrap());
    let pruning = match first_search {
        MutSearchResult::Internal(sub_node) => {
            if read_unchecked::<KS, PS>(&sub_node).is_ext() {
                Pruning::DeepestInnode
            } else {
                Pruning::Innode(sub_node)
            }
        },
        MutSearchResult::External => unreachable!()
    };
    match pruning {
        Pruning::DeepestInnode => {
            // start delete
            // empty page references that will dealt with later
            let mut empty_pages = vec![];
            {
                let mut cursor_guard = write_node::<KS, PS>(node);
                for keys_to_del in keys {
                    cursor_guard = write_key_page(cursor_guard, keys_to_del);
                    let pos = cursor_guard.search(keys_to_del);
                    cursor_guard.remove(pos);
                    if cursor_guard.is_empty() {
                        empty_pages.push(cursor_guard.node_ref().clone());
                    }
                }
            }
            for empty_node in empty_pages {

            }
        },
        Pruning::Innode(sub_node) => {

        }
    }
}

pub fn level_merge<KSA, PSA, KSB, PSB>(src_tree: &BPlusTree<KSA, PSA>, dest_tree: &BPlusTree<KSB, PSB>)
    where KSA: Slice<EntryKey> + Debug + 'static,
          PSA: Slice<NodeCellRef> + 'static,
          KSB: Slice<EntryKey> + Debug + 'static,
          PSB: Slice<NodeCellRef> + 'static
{
    let left_most_leaf_guards = select::<KSA, PSA>(&src_tree.get_root());
    let keys: Vec<EntryKey> = left_most_leaf_guards.iter()
        .map(|(_, g)| g.keys())
        .flatten()
        .cloned()
        .collect_vec();
    // merge to dest_tree
    dest_tree.merge_page(keys);

    // cleanup
    let page_keys = left_most_leaf_guards.iter()
        .map(|(k, _)| k)
        .collect_vec();

    unimplemented!()
}