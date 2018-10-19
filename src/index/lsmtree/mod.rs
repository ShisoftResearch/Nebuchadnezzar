use std::collections::BTreeMap;
use index::EntryKey;
use index::Slice;

// LevelTree items cannot been added or removed individually
// Items must been merged from higher level in bulk
// Deletion will be performed when merging to or from higher level tree
// Because tree updata will not to be performed in parallel. Unlike memtable, a single r/w lock
// should be sufficient. Thus concurrency control will be simple and efficient.
pub struct LevelTree<S> where S: Slice<EntryKey> {
    index: BTreeMap<EntryKey, SSIndex<S>>
}

struct SSIndex<S> where S: Slice<EntryKey> {
    slice: S
}