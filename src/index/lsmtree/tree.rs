use client::AsyncClient;
use index::btree::LevelTree;
use index::btree::NodeCellRef;
use index::btree::{BPlusTree, RTCursor as BPlusTreeCursor};
use index::key_with_id;
use index::lsmtree::cursor::LSMTreeCursor;
use index::lsmtree::placement::sm::InSplitStatus;
use index::lsmtree::placement::sm::Placement;
use index::lsmtree::split::check_and_split;
use index::lsmtree::split::SplitStatus;
use index::Cursor;
use index::EntryKey;
use index::Ordering;
use index::*;
use itertools::Itertools;
use parking_lot::Mutex;
use parking_lot::RwLock;
use ram::segs::MAX_SEGMENT_SIZE;
use ram::types::Id;
use rayon::iter::IntoParallelRefIterator;
use std::collections::BTreeSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::{mem, ptr};
use std::fmt::Debug;

pub const LEVEL_PAGE_DIFF_MULTIPLIER: usize = 10;

const LEVEL_M: usize = 24;
const LEVEL_1: usize = LEVEL_M * LEVEL_PAGE_DIFF_MULTIPLIER;
const LEVEL_2: usize = LEVEL_1 * LEVEL_PAGE_DIFF_MULTIPLIER;
const LEVEL_3: usize = LEVEL_2 * LEVEL_PAGE_DIFF_MULTIPLIER;
const LEVEL_4: usize = LEVEL_3 * LEVEL_PAGE_DIFF_MULTIPLIER;

pub type LevelTrees = Vec<Box<LevelTree>>;
pub type Ptr = NodeCellRef;
pub type Key = EntryKey;
pub type TreeLevels = (LevelTrees, Vec<usize>);

pub type KeyRange = (EntryKey, EntryKey);

#[derive(Serialize, Deserialize, Debug)]
pub enum LSMTreeResult<T> {
    Ok(T),
    EpochMismatch(u64, u64),
}

impl<T> LSMTreeResult<T> {
    pub fn unwrap(self) -> T where T: Debug {
        if let LSMTreeResult::Ok(v) = self {
            v
        } else {
            panic!("Cannot unwrap LSMTreeResult, {:?}", self);
        }
    }
}

with_levels! {
    lm, LEVEL_M;
    l1, LEVEL_1;
    l2, LEVEL_2;
    l3, LEVEL_3;
    // l4, LEVEL_4; // See https://github.com/rust-lang/rust/issues/58164
}

pub struct LSMTree {
    pub trees: LevelTrees,
    pub split: Mutex<Option<SplitStatus>>,
    pub range: Mutex<KeyRange>,
    pub epoch: AtomicU64,
    // use Vec here for convenience
    max_sizes: Vec<usize>,
    lsm_tree_max_size: usize,
    pub id: Id,
}

unsafe impl Send for LSMTree {}
unsafe impl Sync for LSMTree {}

impl LSMTree {
    pub fn new(range: KeyRange, id: Id) -> Self {
        Self::new_with_levels(init_lsm_level_trees(), range, id)
    }

    pub fn new_with_levels(levels: TreeLevels, range: KeyRange, id: Id) -> Self {
        debug!("Initializing LSM-tree...");
        let (trees, max_sizes) = levels;
        let lsm_tree_max_size = max_sizes.iter().sum();
        let split = Mutex::new(None);
        let range = Mutex::new(range);
        let epoch = AtomicU64::new(0);
        debug!(
            "Initialized LSM-tree. max size: {}, id {:?}",
            lsm_tree_max_size, id
        );
        LSMTree {
            trees,
            max_sizes,
            lsm_tree_max_size,
            split,
            range,
            epoch,
            id,
        }
    }

    pub fn insert(&self, mut key: EntryKey) -> bool {
        self.trees[0].insert_into(&key)
    }

    pub fn remove(&self, mut key: EntryKey, epoch: u64) -> bool {
        self.trees
            .iter()
            .map(|tree| tree.mark_key_deleted(&key))
            .collect_vec() // collect here to prevent short circuit
            .into_iter()
            .any(|d| d)
    }

    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> LSMTreeCursor {
        let mut cursors: Vec<Box<Cursor>> = vec![];
        for tree in &self.trees {
            cursors.push(tree.seek_for(key, ordering));
        }
        LSMTreeCursor::new(cursors, ordering)
    }

    pub fn check_and_merge(&self) {
        for i in 0..self.trees.len() - 1 {
            debug!("Checking tree merge {}", i);
            let lower = &*self.trees[i];
            let upper = &*self.trees[i + 1];
            if lower.count() > self.max_sizes[i] {
                lower.merge_to(upper);
            }
        }
    }

    pub fn start_sentinel(this: &Arc<Self>) {
        let this = this.clone();
        thread::Builder::new()
            .name("LSM-Tree Sentinel".to_string())
            .spawn(move || loop {
                this.check_and_merge();
                thread::sleep(Duration::from_millis(500));
            });
    }

    pub fn level_sizes(&self) -> Vec<usize> {
        self.trees.iter().map(|t| t.count()).collect()
    }

    pub fn count(&self) -> usize {
        self.trees.iter().map(|t| t.count()).sum()
    }

    pub fn len(&self) -> usize {
        self.trees.iter().map(|tree| tree.count()).sum::<usize>()
    }

    pub fn is_full(&self) -> bool {
        self.len() > self.lsm_tree_max_size
    }

    pub fn full_size(&self) -> usize {
        self.lsm_tree_max_size
    }

    pub fn last_level_size(&self) -> usize {
        *self.max_sizes.last().unwrap()
    }

    pub fn remove_following_tombstones(&self, start: &EntryKey) {
        self.trees
            .iter()
            .for_each(|tree| tree.remove_following_tombstones(start))
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Relaxed)
    }

    pub fn merge(&self, keys: Box<Vec<EntryKey>>) {
        // merge to highest level
        self.trees.last().unwrap().merge_with_keys(keys)
    }

    pub fn remove_to_right(&self, start_key: &EntryKey) -> usize {
        self.trees
            .iter()
            .map(|tree| tree.remove_to_right(start_key))
            .sum()
    }

    pub fn count_to_right(&self, start_key: &EntryKey) -> usize {
        let mut cursor = self.seek(start_key, Ordering::Forward);
        let mut count = 1;
        while cursor.next() {
            count += 1;
        }
        count
    }

    pub fn set_epoch(&self, epoch: u64) {
        self.epoch.store(epoch, Relaxed);
    }

    pub fn bump_epoch(&self) -> u64 {
        self.epoch.fetch_add(1, Relaxed) + 1
    }

    pub fn to_placement(&self) -> Placement {
        let range = self.range.lock();
        Placement {
            starts: range.0.iter().cloned().collect(),
            ends: range.1.iter().cloned().collect(),
            in_split: self.split.lock().as_ref().map(|stat| InSplitStatus {
                dest: stat.target,
                pivot: stat.pivot.iter().cloned().collect(),
            }),
            epoch: self.epoch(),
            id: self.id,
        }
    }
}
