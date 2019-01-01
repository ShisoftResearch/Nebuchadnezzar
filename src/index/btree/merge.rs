use index::EntryKey;
use index::btree::BPlusTree;
use index::btree::NodeCellRef;
use index::btree::node::read_node;
use index::btree::node::NodeReadHandler;
use std::fmt::Debug;
use index::Slice;

enum MergeSearch {
    External,
    Internal(NodeCellRef),
    RightNode(NodeCellRef)
}

pub fn merge<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    keys: Vec<EntryKey>
)
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{

}

pub fn merge_into_tree_node<KS, PS>(
    tree: &BPlusTree<KS, PS>,
    node: &NodeCellRef,
    keys: Vec<EntryKey>
)
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
//    let mut search = read_node(node, |node_handler: &NodeReadHandler<KS, PS>| {
//        match &**node_handler {
//            &Node
//        }
//    });

}