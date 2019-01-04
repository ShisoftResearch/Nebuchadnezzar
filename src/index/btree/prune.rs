use index::btree::NodeCellRef;
use index::EntryKey;
use smallvec::SmallVec;
use index::btree::search::mut_search;
use index::btree::node::read_node;
use index::Slice;
use index::btree::internal::InNode;
use index::btree::node::read_unchecked;
use std::fmt::Debug;

enum PruneSearch {
    SubBottomLevel,
    Innode(NodeCellRef)
}

pub fn select_right_most_nodes<KS, PS>(node: &NodeCellRef)
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    let search = read_node(node, |node_handler| {
        debug_assert!(!node_handler.is_ext());
        let innode: &InNode<KS, PS> = node_handler.innode();
        let last_node = innode.ptrs.as_slice_immute().last().unwrap();
        if read_unchecked::<KS, PS>(last_node).is_ext() {
            return PruneSearch::SubBottomLevel
        } else {
            return PruneSearch::Innode(last_node.clone())
        }
    });
    unimplemented!()
}