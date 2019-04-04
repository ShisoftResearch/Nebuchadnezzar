use index::{EntryKey, Slice};
use index::btree::{NodeCellRef, BPlusTree, max_entry_key};
use index::btree::external::*;
use ram::cell::Cell;
use dovahkiin::types::*;
use std::fmt::Debug;
use index::btree::external::ExtNode;
use std::marker::PhantomData;

pub fn reconstruct_from_head_page<KS, PS>(head_page_cell: Cell) -> BPlusTree<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    let mut prev_node = NodeCellRef::new_none::<KS, PS>();
    unimplemented!();
}