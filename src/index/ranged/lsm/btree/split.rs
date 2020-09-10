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

pub fn remove_to_right<KS, PS>(
    node_ref: &NodeCellRef,
    start_key: &EntryKey,
    tree: &BPlusTree<KS, PS>,
) -> usize
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut removed_nodes = 0;
    match mut_search::<KS, PS>(node_ref, start_key) {
        MutSearchResult::Internal(ref next_level_node) => {
            {
                removed_nodes = remove_to_right::<KS, PS>(next_level_node, start_key, tree);
                let mut pivot_node = write_targeted::<KS, PS>(write_node(node_ref), start_key);
                if pivot_node.is_none() {
                    // terminate when at and of nodes of this level
                    return removed_nodes;
                }
                debug_assert!(!pivot_node.is_empty_node());
                debug_assert!(!pivot_node.is_ext());
                let mut innode = pivot_node.innode_mut();
                let right_pos = innode.keys.as_slice_immute()[..innode.len]
                    .binary_search(start_key)
                    .unwrap_or_else(|x| x);
                debug_assert!(
                    right_pos <= innode.len,
                    "pos {} >= len {}",
                    right_pos,
                    innode.len
                );
                for i in right_pos..innode.len {
                    innode.ptrs.as_slice()[i + 1] = NodeCellRef::new_none::<KS, PS>();
                }
                scatter_nodes::<KS, PS>(&innode.right);
                innode.right = NodeCellRef::new_none::<KS, PS>();
                innode.len = right_pos;
            }
        }
        MutSearchResult::External => {
            let mut pivot_node = write_targeted::<KS, PS>(write_node(node_ref), start_key);
            if pivot_node.is_none() {
                // terminate when at and of nodes of this level
                return removed_nodes;
            }
            debug_assert!(!pivot_node.is_empty_node());
            debug_assert!(pivot_node.is_ext());
            let mut extnode = pivot_node.extnode_mut(tree);
            let len = extnode.len;
            let new_len = extnode.keys.as_slice_immute()[..extnode.len]
                .binary_search(start_key)
                .unwrap_or_else(|x| x);
            extnode.len = new_len;
            let node_key_removed = len - new_len;
            removed_nodes = scatter_nodes::<KS, PS>(&extnode.next) + node_key_removed;
        }
    }
    removed_nodes
}
