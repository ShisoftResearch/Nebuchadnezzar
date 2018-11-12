use super::btree::{BPlusTree, RTCursor as BPlusTreeCursor};
use super::sstable::*;
use super::*;
use client::AsyncClient;
use itertools::Itertools;
use parking_lot::RwLock;
use std::sync::Arc;
use std::{mem, ptr};
use index::btree::Ordering;

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
    sizes: Vec<usize>,
}

impl LSMTree {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        let (trees, sizes) = init_lsm_level_trees(neb_client);
        LSMTree {
            level_m: BPlusTree::new(neb_client),
            trees,
            sizes,
        }
    }

    pub fn insert(&self, mut key: EntryKey, id: &Id) -> Result<(), ()> {
        key_with_id(&mut key, id);
        self.level_m.insert(&key).map_err(|e| ())
    }

    pub fn remove(&self, mut key: EntryKey, id: &Id) -> Result<bool, ()> {
        key_with_id(&mut key, id);
        let m_deleted = self.level_m.remove(&key).map_err(|e| ())?;
        let levels_deleted = self
            .trees
            .iter()
            .map(|tree| tree.mark_deleted(&key))
            .collect_vec() // collect here to prevent short circuit
            .into_iter()
            .any(|d| d);
        Ok(m_deleted || levels_deleted)
    }

    pub fn seek(&self, mut key: EntryKey, ordering: Ordering) -> LSMTreeCursor {
        match ordering {
            Ordering::Forward => key_with_id(&mut key, &Id::unit_id()),
            Ordering::Backward => key_with_id(&mut key, &Id::new(::std::u64::MAX, ::std::u64::MAX))
        };
        let mut cursors = vec![self.level_m.seek(&key, ordering).unwrap()];
        for tree in &self.trees {
            cursors.push(tree.seek(&key, ordering));
        }
        return LSMTreeCursor::new(cursors);
    }
}

pub struct LSMTreeCursor {
    level_cursors: Vec<Box<Cursor>>,
}

impl LSMTreeCursor {
    fn new(cursors: Vec<Box<Cursor>>) -> Self {
        LSMTreeCursor {
            level_cursors: cursors,
        }
    }
}

impl Cursor for LSMTreeCursor {
    fn next(&mut self) -> bool {
        let min_tree = self
            .level_cursors
            .iter()
            .enumerate()
            .map(|(i, cursor)| (i, cursor.current()))
            .filter_map(|(i, current)| current.map(|current_val| (i, current_val)))
            .min_by_key(|(i, val)| *val)
            .map(|(id, _)| id);
        if let Some(id) = min_tree {
            let min_has_next = self.level_cursors[id].next();
            if !min_has_next {
                return self.level_cursors.iter().any(|level| level.current().is_some());
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    fn current(&self) -> Option<&EntryKey> {
        self.level_cursors.iter().filter_map(|c| c.current()).min()
    }
}
