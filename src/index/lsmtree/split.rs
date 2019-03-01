use index::lsmtree::tree::LSMTree;
use index::EntryKey;
use rayon::prelude::*;
use itertools::Itertools;
use dovahkiin::types::custom_types::id::Id;

pub struct SplitStatus {
    start: EntryKey,
    target: Id
}

pub fn mid_key(tree: &LSMTree) -> EntryKey {
    // TODO: more accurate mid key take account of all tree levels
    // Current implementation only take the mid key from the tree with the most number of keys
    tree.trees
        .iter()
        .map(|tree| (tree.mid_key(), tree.count()))
        .filter_map(|(mid, count)| mid.map(|mid| (mid, count)))
        .max_by_key(|(mid, count)| *count)
        .map(|(mid, _)| mid)
        .unwrap()
}

pub fn check_and_split(tree: &LSMTree) -> bool {
    if tree.is_full() && tree.split.lock().is_none() {
        // need to initiate a split
        let tree_key_range = tree.range.lock().clone();
        let mid_key = mid_key(tree);
        let new_tree_range = (mid_key, tree_key_range.0.clone());

    }
    unimplemented!()
}