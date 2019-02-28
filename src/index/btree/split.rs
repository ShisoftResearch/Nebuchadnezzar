use index::btree::NodeCellRef;
use index::EntryKey;
use index::btree::node::read_node;
use index::Slice;
use index::btree::node::NodeReadHandler;
use std::fmt::Debug;
use index::btree::node::NodeData;

pub fn mid_key<KS, PS>(node_ref: &NodeCellRef) -> Option<EntryKey>
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
{
    debug!("Searching for mid key for split");
    read_node(node_ref, |node_handler: &NodeReadHandler<KS, PS>| {
        let node = &**node_handler;
        match node {
            &NodeData::External(ref n) => {
                Some(n.keys.as_slice_immute()[n.len / 2].clone())
            }
            &NodeData::Internal(ref n) => {
                mid_key::<KS, PS>(&n.ptrs.as_slice_immute()[n.len / 2])
            }
            &NodeData::Empty(ref n) => {
                mid_key::<KS, PS>(&n.right)
            }
            &NodeData::None => None
        }
    })
}