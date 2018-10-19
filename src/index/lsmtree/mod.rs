use std::collections::BTreeMap;
use index::EntryKey;
use index::{Slice, Sortable};
use std::fmt::Debug;

// LevelTree items cannot been added or removed individually
// Items must been merged from higher level in bulk
// Deletion will be performed when merging to or from higher level tree
// Because tree update will not to be performed in parallel. Unlike memtable, a single r/w lock
// should be sufficient. Thus concurrency control will be simple and efficient.
pub struct LevelTree<S>
    where S: Slice<EntryKey> + Sortable<EntryKey>
{
    index: BTreeMap<EntryKey, SSIndex<S>>
}

struct SSIndex<S>
    where S: Slice<EntryKey> + Sortable<EntryKey>
{
    slice: S
}

pub trait Sortable<T>: Sized + Slice<T>
    where T: Default + Debug + Ord
{
    fn merge_with(&mut self,  x: &mut [T]) -> Self {
        let self_slice = self.as_slice();
        let mut slice_1 = 0;
        let mut slice_2 = 0;
        debug_assert!(x.len() <= self_slice.len());
        unimplemented!()
    }
}