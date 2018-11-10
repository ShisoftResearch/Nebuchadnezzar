use super::btree::BPlusTree;
use super::sstable::*;
use super::*;
use client::AsyncClient;
use parking_lot::RwLock;
use std::sync::Arc;
use std::{mem, ptr};

const LEVEL_PAGES_MULTIPLIER: usize = 1000;
const LEVEL_DIFF_MULTIPLIER: usize = 10;
const LEVEL_M: usize = super::btree::NUM_KEYS;
const LEVEL_1: usize = LEVEL_M * LEVEL_DIFF_MULTIPLIER;
const LEVEL_2: usize = LEVEL_1 * LEVEL_DIFF_MULTIPLIER;
const LEVEL_3: usize = LEVEL_2 * LEVEL_DIFF_MULTIPLIER;
const LEVEL_4: usize = LEVEL_3 * LEVEL_DIFF_MULTIPLIER;
// TODO: debug assert the last one will not overflow MAX_SEGMENT_SIZE

type LevelTrees = Vec<Box<Tree>>;

macro_rules! with_levels {
    ($($sym:ident, $level:ident;)*) => {
        $(
            type $sym = [EntryKey; $level];
            impl_sspage_slice!($sym, EntryKey, $level);
        )*

        fn init_lsm_level_trees(neb_client: &Arc<AsyncClient>) -> (LevelTrees, Vec<usize>) {
            let mut trees = LevelTrees::new();
            let mut sizes = Vec::new();
            $(
                trees.push(box LevelTree::<$sym>::new(neb_client));
                sizes.push($level);
            )*
            return (trees, sizes);
        }
    };
}

with_levels!{
    L1, LEVEL_1;
    L2, LEVEL_2;
    L3, LEVEL_3;
    L4, LEVEL_4;
}

pub struct LSMTree {
    level_m: BPlusTree,
    trees: LevelTrees,
    // use Vec here for convenience
    sizes: Vec<usize>
}

impl LSMTree {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        let (trees, sizes) = init_lsm_level_trees(neb_client);
        LSMTree {
            level_m: BPlusTree::new(neb_client),
            trees,
            sizes
        }
    }
}
