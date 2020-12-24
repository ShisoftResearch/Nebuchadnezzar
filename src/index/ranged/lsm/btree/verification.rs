use super::*;
use std::fmt::Debug;

pub fn are_keys_serial(keys: &[EntryKey]) -> bool {
    for i in 1..keys.len() {
        if keys[i - 1] >= keys[i] {
            error!(
                "serial check failed for key ordering: {:?}",
                std::backtrace::Backtrace::capture()
            );
            return false;
        }
    }
    return true;
}

pub fn is_node_serial<KS, PS>(node: &NodeWriteGuard<KS, PS>) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // check keys
    if node.is_ref_none() {
        return true;
    }
    if !are_keys_serial(node.keys()) {
        return false;
    }
    for i in 0..node.len() {
        if &node.keys()[i] <= &*MIN_ENTRY_KEY {
            error!("EMPTY KEY DETECTED at {} !!! {:?}", i, node.keys());
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
        if nodes[i + 1].is_ref_none() {
            break;
        }
        let first_right_bound = nodes[i].right_bound();
        let second_first_node = nodes[i + 1].first_key();
        let second_right_bound = nodes[i + 1].right_bound();
        if first_right_bound >= second_right_bound {
            error!(
                "right bound at {} larger than right right bound, first_right_bound {:?}, second_right_bound {:?}", 
                i, first_right_bound, second_right_bound
            );
            return false;
        }
        if first_right_bound > second_first_node {
            error!(
                "right bound at {} larger than right first node, first_right_bound {:?}, second_first_node {:?}", 
                i, first_right_bound, second_first_node
            );
            return false;
        }
    }
    return true;
}

pub fn is_node_level_serial<KS, PS>(
    mut node: NodeWriteGuard<KS, PS>,
    lsm_level: usize,
    tree_level: usize,
) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    loop {
        let right_ref = node.right_ref().unwrap();
        let right_bound = if !node.is_empty_node() {
            let right_bound = node.right_bound();
            assert!(
                right_bound > &*MIN_ENTRY_KEY,
                "node right bound > smallest possible key"
            );
            if !is_node_serial(&node) {
                error!("Node not serial - {} - {}", lsm_level, tree_level);
                return false;
            }
            if node.first_key() > right_bound {
                error!(
                    "Node have right key smaller than the first - {} - {}",
                    lsm_level, tree_level
                );
                return false;
            }
            if node.last_key() > right_bound {
                error!(
                    "Node have right key smaller than the last - {} - {}",
                    lsm_level, tree_level
                );
                return false;
            }
            right_bound
        } else {
            &*MIN_ENTRY_KEY
        };
        let next = write_node(right_ref);
        if next.is_ref_none() {
            debug!(
                "Node level check reached non node - {} - {}",
                lsm_level, tree_level
            );
            return true;
        }
        if !next.is_empty_node() && !node.is_empty_node() {
            assert!(right_bound > &*MIN_ENTRY_KEY, "unreachable");
            if next.first_key() < right_bound {
                error!("next first key smaller than right bound - {} - {}, type {}. Left keys {:?}, right keys {:?}, right bound {:?}, next right bound {:?}",
                       lsm_level, tree_level, node.type_name(), node.keys(), next.keys(), node.right_bound(), next.right_bound());
                return false;
            }
            if next.right_bound() < right_bound {
                error!(
                    "next right bound key smaller than right bound - {} - {}",
                    lsm_level, tree_level
                );
                return false;
            }
        }
        node = next;
    }
}

pub fn is_tree_in_order<KS, PS>(tree: &BPlusTree<KS, PS>, level: usize) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    debug!("Checking tree {} in order...", level);
    return ensure_level_in_order::<KS, PS>(&tree.get_root(), level, 0);
}

fn ensure_level_in_order<KS, PS>(node: &NodeCellRef, lsm_level: usize, tree_level: usize) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let first_node = write_node::<KS, PS>(&node);
    first_node.left_ref().map(|lr| {
        debug_assert!(
            lr.is_default(),
            "Left node of first node is not default, {:?}, left node type {:?}, ref {:?}",
            first_node.ext_id(),
            read_unchecked::<KS, PS>(lr).type_name(),
            lr
        )
    });
    let sub_ref = match &*first_node {
        &NodeData::Internal(ref n) => Some(n.ptrs.as_slice_immute()[0].clone()),
        _ => None,
    };
    if !is_node_level_serial(first_node, lsm_level, tree_level) {
        return false;
    }
    if let Some(sub_level) = sub_ref {
        return ensure_level_in_order::<KS, PS>(&sub_level, lsm_level, tree_level + 1);
    }
    return true;
}

pub fn tree_has_no_empty_node<KS, PS>(tree: &BPlusTree<KS, PS>) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    debug!("Asserting tree has no empty node");
    let res = ensure_level_no_empty::<KS, PS>(&tree.get_root());
    debug!("Asserting tree has no empty node...Success");
    res
}

fn ensure_level_no_empty<KS, PS>(node: &NodeCellRef) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let first_node = read_unchecked::<KS, PS>(&node);
    let sub_ref = match &*first_node {
        &NodeData::Internal(ref n) => Some(n.ptrs.as_slice_immute()[0].clone()),
        &NodeData::External(_) => {
            let mut node = first_node;
            let mut counter = -1;
            loop {
                counter += 1;
                if node.is_none() {
                    break;
                }
                if node.is_empty() {
                    panic!("Found empty at external page {}", counter);
                }
                if node.is_ext() {
                    let next = read_unchecked::<KS, PS>(node.right_ref().unwrap());
                    // debug!("Tracking down to address {}", next.node_ref().address());
                    assert!(
                        !next.node_ref().ptr_eq(node.node_ref()),
                        "Next node is it self {:?}",
                        node.ext_id()
                    );
                    node = next;
                    continue;
                }
                unreachable!();
            }
            None
        }
        _ => panic!("Expecting external, got {}", first_node.type_name()),
    };
    if let Some(sub_level) = sub_ref {
        return ensure_level_no_empty::<KS, PS>(&sub_level);
    }
    true
}
