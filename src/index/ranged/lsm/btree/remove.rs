use super::external;
use super::node::write_node;
use super::node::NodeData;
use super::*;
use std::fmt::Debug;

// scatter the node and its references to ensure garbage collection
pub fn scatter_nodes<KS, PS>(node_ref: &NodeCellRef) -> usize
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut footprint = 0;
    if node_ref.is_default() {
        return footprint;
    }
    let mut node = write_node::<KS, PS>(node_ref);
    loop {
        match &*node {
            &NodeData::None => return footprint,
            &NodeData::External(ref node) => {
                external::make_deleted::<KS, PS>(&node.id);
            }
            _ => {}
        }
        let right = node.right_ref().unwrap().clone();
        footprint += node.len();
        *node = NodeData::None;
        node = write_node::<KS, PS>(&right);
    }
}
