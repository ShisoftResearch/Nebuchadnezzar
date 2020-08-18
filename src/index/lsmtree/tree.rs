use crate::client::AsyncClient;
use crate::index::btree::NodeCellRef;
use crate::index::btree::LevelTree;
use crate::index::btree::BPlusTree;
use crate::index::trees::*;
use crate::index::lsmtree::cursor::LSMTreeCursor;
use crate::index::lsmtree::placement::sm::InSplitStatus;
use crate::index::lsmtree::placement::sm::Placement;
use crate::index::lsmtree::split::SplitStatus;
use itertools::Itertools;
use parking_lot::Mutex;
use crate::ram::segs::MAX_SEGMENT_SIZE;
use crate::ram::types::Id;
use std::fmt::Debug;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::{mem, ptr};


pub type LevelTrees = Vec<Box<dyn LevelTree>>;
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
    pub fn unwrap(self) -> T
    where
        T: Debug,
    {
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

    pub fn new_with_level_ids(
        range: KeyRange,
        id: Id,
        tree_ids: Vec<Id>,
        neb: &AsyncClient,
    ) -> Self {
        let mut tree = Self::new(range, id);
        debug_assert_eq!(tree.trees.len(), tree_ids.len());
        tree.trees.iter_mut().zip(tree_ids).for_each(|(tree, id)| {
            tree.from_tree_id(&id, neb);
        });
        tree
    }

    pub fn insert(&self, key: EntryKey) -> bool {
        self.trees[0].insert_into(&key)
    }

    pub fn remove(&self, key: EntryKey, _epoch: u64) -> bool {
        self.trees
            .iter()
            .map(|tree| tree.mark_key_deleted(&key))
            .collect_vec() // collect here to prevent short circuit
            .into_iter()
            .any(|d| d)
    }

    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> LSMTreeCursor {
        let mut cursors: Vec<Box<dyn Cursor>> = vec![];
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

    pub fn oversized(&self) -> bool {
        (0..self.trees.len() - 1).any(|i| self.trees[i].count() > self.max_sizes[i])
    }

    pub fn start_sentinel(this: &Arc<Self>) {
        let this = this.clone();
        thread::Builder::new()
            .name("LSM-Tree Sentinel".to_string())
            .spawn(move || loop {
                this.check_and_merge();
                thread::sleep(Duration::from_millis(500));
            }).unwrap();
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

    pub fn ensure_trees_in_order(&self) {
        for (i, t) in self.trees.iter().enumerate() {
            assert!(t.verify(i), "tree not in order {}", i);
        }
    }
}
