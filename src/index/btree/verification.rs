use index::btree::internal::InNode;
use index::btree::{NodeCellRef, NodeWriteGuard, write_node, BPlusTree, NodeData};
use index::{EntryKey, Slice};
use std::fmt::Debug;

pub fn is_node_serial<KS, PS>(node: &NodeWriteGuard<KS, PS>) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // check keys
    for i in 0..node.len() {
        if node.keys()[i] <= smallvec!() {
            error!("EMPTY KEY DETECTED !!!");
            return false;
        }
    }
    for i in 1..node.len() {
        if node.keys()[i - 1] >= node.keys()[i] {
            error!("serial check failed for key ordering");
            return false;
        }
    }
    true
}

pub fn is_node_list_serial<KS, PS>(nodes: &Vec<NodeWriteGuard<KS, PS>>) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    if nodes.len() == 0 {
        return true;
    }
    for (i, n) in nodes.iter().enumerate() {
        if !is_node_serial(n) {
            error!("node at {} not serial", i);
            return false;
        }
    }

    // check right ref
    for i in 0..nodes.len() - 1 {
        let first_right_bound = nodes[i].right_bound();
        let second_first_node = nodes[i + 1].first_key();
        let second_right_bound = nodes[i + 1].right_bound();
        if first_right_bound >= second_right_bound {
            error!("right bound at {} larger than right right bound", i);
            return false;
        }
        if first_right_bound > second_first_node {
            error!("right bound at {} larger than right first node", i);
            return false;
        }
    }
    return true;
}

pub fn is_node_level_serial<KS, PS>(mut node: NodeWriteGuard<KS, PS>) -> bool
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
{
    loop {
        let right_ref = node.right_ref().unwrap();
        let right_bound = node.right_bound();
        if !is_node_serial(&node) {
            error!("Node not serial");
            return false;
        }
        if node.first_key() > right_bound {
            error!("Node have right key smaller than the first");
            return false;
        }
        if node.last_key() > right_bound {
            error!("Node have right key smaller than the last");
            return false;
        }
        let next = write_node(right_ref);
        if next.is_none() {
            debug!("Node level check reached non node");
            return true;
        }
        if next.first_key() < right_bound {
            error!("next first key smaller than right bound");
            return false;
        }
        if next.right_bound() < right_bound {
            error!("next right bound key smaller than right bound");
            return false
        }
        node = next;
    }
}

pub fn is_tree_in_order<KS, PS>(tree: &BPlusTree<KS, PS>) -> bool
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
{
    return ensure_level_in_order::<KS, PS>(&tree.get_root());
}

fn ensure_level_in_order<KS, PS>(node: &NodeCellRef) -> bool
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
{
    let first_node = write_node::<KS, PS>(&node);
    let sub_ref = match &*first_node {
        &NodeData::Internal(ref n) => Some(n.ptrs.as_slice_immute()[0].clone()),
        _ => None
    };
    if !is_node_level_serial(first_node) {
        return false;
    }
    if let Some(sub_level) = sub_ref {
        return ensure_level_in_order::<KS, PS>(&sub_level);
    }
    return true;
}