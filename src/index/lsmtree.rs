use super::btree::BPlusTree;
use super::sstable::*;
use super::*;
use client::AsyncClient;
use parking_lot::RwLock;
use std::sync::Arc;
use std::{mem, ptr};

const LEVEL_M: usize = super::btree::NUM_KEYS;
const LEVEL_1: usize = LEVEL_M * 10;
const LEVEL_2: usize = LEVEL_1 * 10;
const LEVEL_3: usize = LEVEL_2 * 10;
const LEVEL_4: usize = LEVEL_3 * 10;
// TODO: debug assert the last one will not overflow MAX_SEGMENT_SIZE

macro_rules! with_levels {
    ($($sym:ident, $level:ident;)*) => {
        $(
            type $sym = [EntryKey; $level];
            impl_sspage_slice!($sym, EntryKey, $level);
        )*

        pub struct LSMTree {
            level_m: BPlusTree,
            trees: Vec<Box<Tree>> // use Vec here for convenience
        }

        impl LSMTree {
            pub fn new(neb_client: &Arc<AsyncClient>) {
                let mtree = BPlusTree::new(neb_client);
                let mut trees: Vec<Box<Tree>> = vec![];
                $(
                    trees.push(box LevelTree::<$sym>::new(neb_client));
                )*
            }
        }
    };
}

with_levels!{
    L1, LEVEL_1;
    L2, LEVEL_2;
    L3, LEVEL_3;
    L4, LEVEL_4;
}
