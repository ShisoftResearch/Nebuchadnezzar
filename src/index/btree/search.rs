use index::btree::cursor::RTCursor;
use index::btree::node::read_node;
use index::btree::node::NodeData;
use index::btree::node::NodeReadHandler;
use index::btree::DeletionSet;
use index::btree::NodeCellRef;
use index::EntryKey;
use index::Ordering;
use index::Slice;
use std::fmt::Debug;
use std::marker::PhantomData;

pub fn search_node<KS, PS>(
    node_ref: &NodeCellRef,
    key: &EntryKey,
    ordering: Ordering,
    deleted: &DeletionSet,
) -> RTCursor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    debug!("searching for {:?}", key);
    read_node(node_ref, |node_handler: &NodeReadHandler<KS, PS>| {
        let node = &**node_handler;
        let gen_empty_cursor = || RTCursor {
            index: 0,
            ordering,
            page: None,
            marker: PhantomData,
            deleted: deleted.clone(),
            current: None,
        };
        if let Some(right_node) = node.key_at_right_node(key) {
            debug!("Search found a node at the right side");
            return search_node(right_node, key, ordering, deleted);
        }
        debug!("search node have keys {:?}", node.keys());
        let mut pos = node.search(key);
        match node {
            &NodeData::External(ref n) => {
                debug!(
                    "search in external for {:?}, len {}, ordering {:?}, content: {:?}",
                    key,
                    n.len,
                    ordering,
                    &n.keys.as_slice_immute()[..n.len]
                );
                if ordering == Ordering::Backward {
                    debug!("found cursor pos {} for backwards, will be corrected", pos);
                    if pos > 0 {
                        pos -= 1;
                    }
                    debug!("cursor pos have been corrected to {}", pos);
                }
                RTCursor::new(pos, node_ref, ordering, deleted)
            }
            &NodeData::Internal(ref n) => {
                debug!(
                    "search in internal node for {:?}, len {}, pos {}",
                    key, n.len, pos
                );
                let next_node_ref = &n.ptrs.as_slice_immute()[pos];
                debug_assert!(pos <= n.len);
                debug_assert!(!next_node_ref.is_default());
                search_node(next_node_ref, key, ordering, deleted)
            }
            &NodeData::Empty(ref n) => search_node(&n.right, key, ordering, deleted),
            &NodeData::None => gen_empty_cursor(),
        }
    })
}

pub enum MutSearchResult {
    External,
    Internal(NodeCellRef),
}

pub fn mut_search<KS, PS>(node_ref: &NodeCellRef, key: &EntryKey) -> MutSearchResult
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    read_node(node_ref, |node: &NodeReadHandler<KS, PS>| match &**node {
        &NodeData::Internal(ref n) => {
            let pos = n.search(key);
            let sub_node = n.ptrs.as_slice_immute()[pos].clone();
            MutSearchResult::Internal(sub_node)
        }
        &NodeData::External(_) => MutSearchResult::External,
        &NodeData::Empty(ref n) => mut_search::<KS, PS>(&n.right, key),
        &NodeData::None => unreachable!(),
    })
}
