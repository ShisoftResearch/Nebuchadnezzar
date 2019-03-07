use index::btree::node::read_node;
use index::btree::node::NodeData;
use index::btree::node::NodeReadHandler;
use index::btree::NodeCellRef;
use index::EntryKey;
use index::Slice;
use std::fmt::Debug;
use index::btree::search::mut_search;
use index::btree::search::MutSearchResult;

pub fn mid_key<KS, PS>(node_ref: &NodeCellRef) -> Option<EntryKey>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    debug!("Searching for mid key for split");
    read_node(node_ref, |node_handler: &NodeReadHandler<KS, PS>| {
        let node = &**node_handler;
        match node {
            &NodeData::External(ref n) => Some(n.keys.as_slice_immute()[n.len / 2].clone()),
            &NodeData::Internal(ref n) => mid_key::<KS, PS>(&n.ptrs.as_slice_immute()[n.len / 2]),
            &NodeData::Empty(ref n) => mid_key::<KS, PS>(&n.right),
            &NodeData::None => None,
        }
    })
}

pub fn remove_to_right<KS, PS>(node_ref: &NodeCellRef, start_key: &EntryKey)
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
{
    let search = mut_search::<KS, PS>(node_ref, start_key);
//    match search {
//        MutSearchResult::External => {
//
//        }
//    }
}