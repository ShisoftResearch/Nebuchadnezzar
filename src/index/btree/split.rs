use index::btree::node::read_node;
use index::btree::node::NodeData;
use index::btree::node::NodeReadHandler;
use index::btree::search::mut_search;
use index::btree::search::MutSearchResult;
use index::btree::NodeCellRef;
use index::EntryKey;
use index::Slice;
use std::fmt::Debug;
use index::btree::node::write_node;
use index::btree::node::write_targeted;
use index::btree::remove::scatter_node;

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
    match mut_search::<KS, PS>(node_ref, start_key) {
        MutSearchResult::Internal(ref next_level_node) => {
            {
                remove_to_right::<KS, PS>(next_level_node, start_key);
                let mut pivot_node = write_targeted::<KS, PS>(write_node(node_ref), start_key);
                if pivot_node.is_none() {
                    // terminate when at and of nodes of this level
                    return;
                }
                debug_assert!(!pivot_node.is_empty_node());
                debug_assert!(!pivot_node.is_ext());
                let mut innode = pivot_node.innode_mut();
                let right_pos = innode.keys.as_slice_immute().binary_search(start_key).unwrap_or_else(|x| x);
                debug_assert!(right_pos < innode.len);
                for i in right_pos..innode.len {
                    scatter_node::<KS, PS>(&innode.ptrs.as_slice()[i + 1]);
                }
                scatter_node::<KS, PS>(&innode.right);
                innode.right = NodeCellRef::new_none::<KS, PS>();
                innode.len = right_pos;
            }
        }
        MutSearchResult::External => {
            let mut pivot_node = write_targeted::<KS, PS>(write_node(node_ref), start_key);
            if pivot_node.is_none() {
                // terminate when at and of nodes of this level
                return;
            }
            debug_assert!(!pivot_node.is_empty_node());
            debug_assert!(pivot_node.is_ext());
            let mut extnode = pivot_node.extnode_mut();
            extnode.len = extnode.keys.as_slice_immute().binary_search(start_key).unwrap_or_else(|x| x);
            scatter_node::<KS, PS>(&extnode.next);
        }
    }
}
