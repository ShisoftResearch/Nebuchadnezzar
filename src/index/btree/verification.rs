use index::{Slice, EntryKey};
use index::btree::{NodeCellRef, NodeWriteGuard};
use std::fmt::Debug;
use index::btree::internal::InNode;

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
            return false
        }
        if first_right_bound > second_first_node {
            error!("right bound at {} larger than right first node", i);
            return false
        }
    }
    return true;
}