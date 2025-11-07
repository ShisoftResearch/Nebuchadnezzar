use super::*;

pub fn clear_by_node<KS, PS>(node_ref: &NodeCellRef)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // First, take ownership of child pointers if this is an internal node
    let child_refs: Vec<NodeCellRef> = match &*read_unchecked::<KS, PS>(node_ref) {
        &NodeData::Internal(ref node) => {
            // Collect all non-default child pointers
            node.ptrs
                .as_slice_immute()
                .iter()
                .filter(|r| !r.is_default())
                .cloned()
                .collect()
        }
        &NodeData::External(_) => Vec::new(),
        &NodeData::Empty(_) | &NodeData::None => return, // Already cleared, skip
    };

    // Recursively clear all children first
    for child_ref in child_refs {
        clear_by_node::<KS, PS>(&child_ref);
    }

    // Now clear this node and its right siblings
    let mut node = write_node::<KS, PS>(&node_ref);
    let mut next_ref = mem::take(node.right_ref_mut().unwrap());
    *node = NodeData::Empty(Box::new(Default::default()));
    while !next_ref.is_default() {
        let mut node = write_node::<KS, PS>(&next_ref);
        next_ref = mem::take(node.right_ref_mut().unwrap());
        *node = NodeData::Empty(Box::new(Default::default()));
    }
}
