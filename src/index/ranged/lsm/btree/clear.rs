use super::*;

pub fn clear_by_node<KS, PS>(node_ref: &NodeCellRef) 
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    match &*read_unchecked::<KS, PS>(node_ref) {
        &NodeData::Internal(ref node) => {
            clear_by_node::<KS, PS>(&node.ptrs.as_slice_immute()[0]);
        },
        &NodeData::External(_) => {},
        &NodeData::Empty(_) | &NodeData::None => unreachable!(),
    }
    let mut node = write_node::<KS, PS>(&node_ref);
    let mut next_ref = mem::take(node.right_ref_mut().unwrap());
    *node = NodeData::Empty(box Default::default());
    while !next_ref.is_default() {
        let mut node = write_node::<KS, PS>(&next_ref);
        next_ref = mem::take(node.right_ref_mut().unwrap());
        *node = NodeData::Empty(box Default::default());
    }
}