use super::node::read_node;
use super::node::write_node;
use super::node::write_targeted;
use super::node::NodeData;
use super::node::NodeReadHandler;
use super::remove::scatter_nodes;
use super::search::mut_search;
use super::search::MutSearchResult;
use super::*;
use std::fmt::Debug;

pub fn mid_key<KS, PS>(node_ref: &NodeCellRef) -> Option<EntryKey>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    debug!("Searching for mid key for split");
    enum R {
        Result(Option<EntryKey>),
        SubSearch(NodeCellRef),
        None,
    }
    let r = read_node(node_ref, |node_handler: &NodeReadHandler<KS, PS>| {
        let node = &**node_handler;
        match node {
            &NodeData::External(ref n) => {
                R::Result(Some(n.keys.as_slice_immute()[n.len / 2].clone()))
            }
            &NodeData::Internal(ref n) => R::SubSearch(n.ptrs.as_slice_immute()[n.len / 2].clone()),
            &NodeData::Empty(ref n) => R::SubSearch(n.right.clone()),
            &NodeData::None => R::None,
        }
    });
    match r {
        R::Result(r) => r,
        R::SubSearch(r) => mid_key::<KS, PS>(&r),
        R::None => None,
    }
}
