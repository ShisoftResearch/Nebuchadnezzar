use index::lsmtree::tree::LSMTree;
use index::EntryKey;
use rayon::iter::IntoParallelRefIterator;

pub fn mid_key<KS, PS>(tree: &LSMTree) -> Option<EntryKey> {
    let (mid_keys, weights) : (Vec<_>, Vec<_>) = tree.trees
        .iter()
        .map(|tree| (tree.mid_key(), tree.count()))
        .unzip();
    unimplemented!()
}