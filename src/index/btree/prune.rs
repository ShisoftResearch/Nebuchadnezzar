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

enum Selection {
    Selected(Vec<EntryKey>),
    Innode(NodeCellRef)
}

pub fn select<KS, PS>(node: &NodeCellRef) -> Vec<EntryKey>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    let selection = read_node(node, |node_handler| {
        debug_assert!(!node_handler.is_ext());
        let innode: &InNode<KS, PS> = node_handler.innode();
        let first_node = innode.ptrs.as_slice_immute().first().unwrap();
        let first_key = innode.keys.as_slice_immute().first().unwrap();
        if read_unchecked::<KS, PS>(first_node).is_ext() {
            Selection::Selected(innode.ptrs
                .as_slice_immute()
                .iter()
                .take(LEVEL_PAGE_DIFF_MULTIPLIER)
                .map(|r| write_node::<KS, PS>(r))
                .map(|g| g.keys().to_vec())
                .flatten()
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