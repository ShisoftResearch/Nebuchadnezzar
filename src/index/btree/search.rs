use index::btree::node::NodeData;
use std::marker::PhantomData;
use index::btree::cursor::RTCursor;
use index::btree::node::NodeReadHandler;
use index::btree::NodeCellRef;
use index::EntryKey;
use index::Ordering;
use index::btree::node::read_node;
use index::Slice;
use std::fmt::Debug;

pub fn search_node<KS, PS>(node_ref: &NodeCellRef, key: &EntryKey, ordering: Ordering) -> RTCursor<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    debug!("searching for {:?}", key);
    read_node(node_ref, |node_handler: &NodeReadHandler<KS, PS>| {
        let node = &**node_handler;
        if let Some(right_node) = node.key_at_right_node(key) {
            debug!("Search found a node at the right side");
            return search_node(right_node, key, ordering);
        }
        let pos = node.search(key);
        match node {
            &NodeData::External(ref n) => {
                debug!(
                    "search in external for {:?}, len {}, content: {:?}",
                    key, n.len, n.keys
                );
                RTCursor::new(pos, node_ref, ordering)
            },
            &NodeData::Internal(ref n) => {
                debug!("search in internal node for {:?}, len {}", key, n.len);
                let next_node_ref = &n.ptrs.as_slice_immute()[pos];
                search_node(next_node_ref, key, ordering)
            },
            &NodeData::Empty(ref n) => {
                search_node(&n.right, key, ordering)
            },
            &NodeData::None => {
                RTCursor{
                    index: 0,
                    ordering,
                    page: None,
                    marker: PhantomData
                }
            }
        }
    })
}

pub enum MutSearchResult {
    External,
    Internal(NodeCellRef),
}

pub fn mut_search<KS, PS>(node_ref: &NodeCellRef, key: &EntryKey) -> MutSearchResult
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    read_node(node_ref, |node: &NodeReadHandler<KS, PS>| {
        match &**node {
            &NodeData::Internal(ref n) => {
                let pos = n.search(key);
                let sub_node = n.ptrs.as_slice_immute()[pos].clone();
                MutSearchResult::Internal(sub_node)
            }
            &NodeData::External(_) => MutSearchResult::External,
            &NodeData::Empty(ref n) => mut_search::<KS, PS>(&n.right, key),
            &NodeData::None => unreachable!()
        }
    })
}