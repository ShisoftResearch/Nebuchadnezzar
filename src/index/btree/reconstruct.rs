use index::{EntryKey, Slice};
use index::btree::{NodeCellRef, BPlusTree, max_entry_key};
use index::btree::external::*;
use ram::cell::Cell;
use dovahkiin::types::*;
use std::fmt::Debug;
use index::btree::external::ExtNode;
use std::marker::PhantomData;
use index::btree::node::{write_node, Node};

pub fn reconstruct_from_head_page<KS, PS>(head_page_cell: Cell) -> BPlusTree<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    // let rev_levels = vec![];
    let mut prev_ref = NodeCellRef::new_none::<KS, PS>();
    let mut cell = head_page_cell;
    let mut at_end = false;
    while !at_end {
        let page = ExtNode::<KS, PS>::from_cell(&cell);
        let next_id = page.next_id;
        let prev_id = page.prev_id;
        let mut node = page.node;
        let first_key = node.keys.as_slice_immute()[0].clone();
        at_end = next_id.is_unit_id();
        node.prev = prev_ref.clone();
        if at_end {
            node.next = NodeCellRef::new_none::<KS, PS>();
        }
        let node_ref = NodeCellRef::new(Node::with_external(box node));
        let mut prev_lock = write_node::<KS, PS>(&prev_ref);
        if !prev_lock.is_none() {
            *prev_lock.right_bound_mut() = first_key;
            *prev_lock.right_ref_mut().unwrap() = node_ref.clone();
        } else {
            assert_eq!(prev_id, Id::unit_id());
        }
        prev_ref = node_ref;
    }
    unimplemented!();
}