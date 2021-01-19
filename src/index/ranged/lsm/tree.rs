// A machine-local concurrent LSM tree based on B-tree implementation

use super::btree::level::*;
use super::btree::*;
use crate::client::AsyncClient;
use crate::ram::cell::Cell;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use crossbeam_epoch::*;
use lightning::map::HashSet as LFHashSet;
use std::collections::HashSet as StdHashSet;
use std::mem;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::Arc;

pub const LSM_TREE_SCHEMA_NAME: &'static str = "NEB_LSM_TREE";
pub const LSM_TREE_LEVELS_NAME: &'static str = "levels";
pub const LSM_TREE_MIGRATION_NAME: &'static str = "migration";
pub const LAST_LEVEL_MULT_FACTOR: usize = 2;
pub const INITIAL_TREE_EPOCH: u64 = 0;

type LevelTrees = [Box<dyn LevelTree>; NUM_LEVELS];
type LevelCusors = [Box<dyn Cursor>; NUM_LEVELS + 2]; // 2 for mem and trans mem
pub type DeletionSet = LFHashSet<EntryKey>;

lazy_static! {
    pub static ref LSM_TREE_SCHEMA_ID: u32 = key_hash(LSM_TREE_SCHEMA_NAME) as u32;
    pub static ref LSM_TREE_LEVELS_HASH: u64 = key_hash(LSM_TREE_LEVELS_NAME);
    pub static ref LSM_TREE_MIGRATION_HASH: u64 = key_hash(LSM_TREE_MIGRATION_NAME);
    pub static ref LSM_TREE_SCHEMA: Schema = lsm_treee_schema();
}

pub struct LSMTree {
    pub mem_tree: Atomic<Box<dyn LevelTree>>,
    pub trans_mem_tree: Atomic<Box<dyn LevelTree>>,
    pub disk_trees: LevelTrees,
    pub deletion: Arc<DeletionSet>,
}

impl LSMTree {
    pub async fn create(neb_client: &Arc<AsyncClient>, id: &Id) -> Self {
        let deletion_ref = Arc::new(LFHashSet::with_capacity(16));
        let tree_m = LevelMTree::new(&deletion_ref);
        let tree_0 = Level0Tree::new(&deletion_ref);
        let tree_1 = Level1Tree::new(&deletion_ref);
        tree_0.persist_root(neb_client).await;
        tree_1.persist_root(neb_client).await;
        let level_ids = vec![tree_0.head_id(), tree_1.head_id()];
        let lsm_tree_cell = lsm_tree_cell(&level_ids, id, None);
        neb_client.write_cell(lsm_tree_cell).await.unwrap().unwrap();
        Self {
            mem_tree: Atomic::new(box tree_m),
            trans_mem_tree: Atomic::null(),
            disk_trees: [box tree_0, box tree_1],
            deletion: deletion_ref,
        }
    }

    pub async fn recover(neb_client: &Arc<AsyncClient>, lsm_tree_id: &Id) -> Self {
        info!("Recovering LSM tree {:?}", lsm_tree_id);
        let deletion_ref = Arc::new(LFHashSet::with_capacity(16));
        let cell = neb_client.read_cell(*lsm_tree_id).await.unwrap().unwrap();
        let trees = &cell.data[*LSM_TREE_LEVELS_HASH]
            .prim_array()
            .unwrap()
            .Id()
            .unwrap();
        let tree_0_id = &trees[0usize];
        let tree_1_id = &trees[1usize];
        info!("Record shows trees {:?}", trees);
        debug!("Recovering level 0 tree {:?}", tree_0_id);
        let tree_0 = Level0Tree::from_head_id(tree_0_id, neb_client, &deletion_ref, 0).await;
        debug!("Recovering level 1 tree {:?}", tree_1_id);
        let tree_1 = Level1Tree::from_head_id(tree_1_id, neb_client, &deletion_ref, 1).await;
        Self {
            mem_tree: Atomic::new(box LevelMTree::new(&deletion_ref)),
            trans_mem_tree: Atomic::null(),
            disk_trees: [box tree_0, box tree_1],
            deletion: deletion_ref,
        }
    }

    pub fn insert(&self, entry: &EntryKey) -> bool {
        let guard = crossbeam_epoch::pin();
        let mem_tree_ptr = self.mem_tree.load(Acquire, &guard);
        let mem_tree = unsafe { mem_tree_ptr.as_ref().unwrap() };
        mem_tree.insert_into(entry)
    }

    pub fn delete(&self, entry: &EntryKey) -> bool {
        if let Some(k) = self.seek(entry, Ordering::Forward).current() {
            if k == entry {
                self.deletion.insert(entry);
                return true;
            }
        }
        return false;
    }

    pub fn seek(&self, entry: &EntryKey, ordering: Ordering) -> LSMTreeCursor {
        LSMTreeCursor::new(entry, self, ordering)
    }

    pub async fn merge_levels(&self) -> bool {
        let mut merged = false;
        let mut deleted = StdHashSet::new();
        {
            let guard = crossbeam_epoch::pin();
            let mem_tree_ptr = self.mem_tree.load(Acquire, &guard);
            let mem_tree = unsafe { mem_tree_ptr.as_ref().unwrap() };
            if mem_tree.oversized() {
                info!("Memory tree oversized, moving everything to disk tree");
                // Memory tree oversized
                // Will construct a new tree and swap the old one to trans_mem_tree for query and move all
                // keys in old tree to disk tree level 0
                let new_mem_tree: Box<dyn LevelTree> = box LevelMTree::new(&self.deletion);
                let new_mem_tree_ptr = Owned::new(new_mem_tree).into_shared(&guard);
                self.trans_mem_tree.store(mem_tree_ptr, Release);
                self.mem_tree.store(new_mem_tree_ptr, Release);
                info!("Starting moving memory tree...");
                mem_tree.merge_all_to(0, &*self.disk_trees[0], &mut deleted, false);
                info!("Memory tree merge completed, reset trans tree and destory old tree");
                self.trans_mem_tree.store(Shared::null(), Release);
                unsafe {
                    guard.defer_destroy(mem_tree_ptr);
                }
                merged = true;
                info!("Memory tree merging completed");
            } else {
                trace!("Memory tree not oversized");
            }
        }
        storage::wait_until_updated().await;
        for i in 0..self.disk_trees.len() - 1 {
            let level = i + 1;
            if self.disk_trees[i].oversized() {
                info!(
                    "Level {}, {:?} tree oversized, merging",
                    level,
                    self.disk_trees[i].head_id()
                );
                self.disk_trees[i].merge_to(level, &*self.disk_trees[i + 1], &mut deleted, true);
                storage::wait_until_updated().await;
                info!("Level {} merge completed", level);
                merged = true;
            } else {
                trace!("Level {} tree not oversized", level);
            }
        }
        if !deleted.is_empty() {
            debug!(
                "LSM merges collected {} deleted keys, remove them from deletion set",
                deleted.len()
            );
            for dk in deleted {
                self.deletion.remove(&dk);
            }
        }
        merged
    }

    pub fn retain(&self, pivot: &EntryKey) {
        info!("Retaining tree keys to {:?} for LSM tree split", pivot);
        let guard = crossbeam_epoch::pin();
        let mem_tree_ptr = self.mem_tree.load(Acquire, &guard);
        let mem_tree = unsafe { mem_tree_ptr.as_ref().unwrap() };
        let new_mem_tree: Box<dyn LevelTree> = box LevelMTree::new(&self.deletion);
        let new_mem_tree_ptr = Owned::new(new_mem_tree).into_shared(&guard);
        self.trans_mem_tree.store(mem_tree_ptr, Release);
        self.mem_tree.store(new_mem_tree_ptr, Release);
        info!("Tree retaining by merging all retained keys to disk tree");
        let mut working_trees = vec![mem_tree];
        let mut retained_keys = vec![];
        let mut deleted_keys = vec![];
        let last_tree_index = self.disk_trees.len() - 1;
        let last_tree = &self.disk_trees[last_tree_index];
        for i in 0..last_tree_index {
            working_trees.push(&self.disk_trees[i]);
        }
        for tree in working_trees {
            let mut cursor = tree.seek_for(&*MIN_ENTRY_KEY, Ordering::Forward);
            while let Some(key) = cursor.next() {
                if &key >= pivot {
                    break;
                }
                if !self.deletion.contains(&key) {
                    retained_keys.push(key);
                } else {
                    deleted_keys.push(key);
                }
            }
            // Cleanup tree
            tree.clear_tree();
        }
        let num_keys_retained = retained_keys.len();
        // Merge memory tree to last tree
        last_tree.merge_with_keys(retained_keys);
        info!("Tree retained {} keys", num_keys_retained);
        self.trans_mem_tree.store(Shared::null(), Release);
        unsafe {
            guard.defer_destroy(mem_tree_ptr);
        }
        // Use retain by key only for the last disk tree
        info!("Retaining last level disk tree keys");
        self.disk_trees[last_tree_index].retain_by_key(pivot);
        info!("Updating deletion set with {} keys", deleted_keys.len());
        for dk in deleted_keys {
            self.deletion.remove(&dk);
        }
        for dk in self.deletion.items() {
            if &dk >= pivot {
                self.deletion.remove(&dk);
            }
        }
        info!("Retain process completed");
    }

    pub fn oversized(&self) -> bool {
        self.last_level_tree().count() > self.ideal_capacity()
    }

    pub fn pivot_key(&self) -> Option<EntryKey> {
        let mut accum_keys = 0;
        let mut trees = self
            .disk_trees
            .iter()
            .filter_map(|tree| {
                tree.last_node_digest(&tree.root())
                    .map(|tuple| (tree, tuple, true))
            })
            .collect::<Vec<_>>();
        if trees.is_empty() {
            return None;
        }
        let scale = self.ideal_capacity() / 16;
        loop {
            trace!("Probing trees for pivot {:?}", trees.iter().map(|t| (&t.1, &t.2)).collect::<Vec<_>>());
            if let Some((tree, (node_len, prev_node, mid_key), avil)) = trees
                .iter_mut()
                .filter(|(_, _, avil)| *avil)
                .max_by(|(_, (_, _, k1), _), (_, (_, _, k2), _)| k1.cmp(k2))
            {
                accum_keys += *node_len;
                if accum_keys > scale {
                    debug!("Selected {} keys for pivot at {:?}", accum_keys, mid_key);
                    return Some(mid_key.clone());
                }
                if let Some((prev_len, prev_prev_node, prev_mid)) = tree.last_node_digest(prev_node)
                {
                    *node_len = prev_len;
                    *prev_node = prev_prev_node;
                    *mid_key = prev_mid;
                } else {
                    *avil = false;
                }
            } else {
                return None;
            }
        }
    }

    pub async fn mark_migration(&self, id: &Id, migration: Option<Id>, client: &Arc<AsyncClient>) {
        let lsm_tree_cell = lsm_tree_cell(
            &self.disk_trees.iter().map(|tree| tree.head_id()).collect(),
            id,
            migration,
        );
        client.update_cell(lsm_tree_cell).await.unwrap().unwrap();
    }

    pub fn merge_keys(&self, keys: Vec<EntryKey>) {
        self.last_level_tree().merge_with_keys(keys);
    }

    pub fn ideal_capacity(&self) -> usize {
        self.last_level_tree().ideal_capacity() * LAST_LEVEL_MULT_FACTOR
    }

    pub fn count(&self) -> usize {
        // Only count keys in disk trees
        self.disk_trees.iter().map(|t| t.count()).sum()
    }

    fn last_level_tree(&self) -> &Box<dyn LevelTree> {
        self.disk_trees.last().unwrap()
    }
}

pub struct LSMTreeCursor {
    pub current: Option<(usize, EntryKey)>,
    cursors: LevelCusors,
    ordering: Ordering,
    deletion: Arc<DeletionSet>,
}

impl LSMTreeCursor {
    fn new(key: &EntryKey, lsm_tree: &LSMTree, ordering: Ordering) -> Self {
        let guard = crossbeam_epoch::pin();
        let disk_trees = &lsm_tree.disk_trees;
        let mem_tree_ptr = lsm_tree.mem_tree.load(Acquire, &guard);
        let trans_mem_tree_ptr = lsm_tree.trans_mem_tree.load(Acquire, &guard);
        let mem_tree = unsafe { mem_tree_ptr.as_ref().unwrap() };
        let mut cursors = [
            mem_tree.seek_for(key, ordering),
            disk_trees[0].seek_for(key, ordering),
            disk_trees[1].seek_for(key, ordering),
            box DummyCursor, // for trans mem
        ];
        if !trans_mem_tree_ptr.is_null() && trans_mem_tree_ptr != mem_tree_ptr {
            let trans_mem_tree = unsafe { trans_mem_tree_ptr.as_ref().unwrap() };
            *cursors.last_mut().unwrap() = trans_mem_tree.seek_for(key, ordering);
        }
        let current = Self::leading_tree_key(&cursors, ordering);
        let deletion = lsm_tree.deletion.clone();
        Self {
            cursors,
            current,
            ordering,
            deletion,
        }
    }

    fn leading_tree_key(cursors: &LevelCusors, ordering: Ordering) -> Option<(usize, EntryKey)> {
        match ordering {
            Ordering::Forward => cursors
                .iter()
                .enumerate()
                .filter_map(|(i, c)| c.current().map(|c| (i, c)))
                .min_by(|(_, x), (_, y)| x.cmp(y))
                .map(|(i, k)| (i, k.clone())),
            Ordering::Backward => cursors
                .iter()
                .enumerate()
                .filter_map(|(i, c)| c.current().map(|c| (i, c)))
                .max_by(|(_, x), (_, y)| x.cmp(y))
                .map(|(i, k)| (i, k.clone())),
        }
    }
}

impl Cursor for LSMTreeCursor {
    fn current(&self) -> Option<&EntryKey> {
        self.current.as_ref().map(|(_, k)| k)
    }
    fn next(&mut self) -> Option<EntryKey> {
        loop {
            if let Some((tree_id, _)) = self.current {
                self.cursors[tree_id].next();
                let mut other_entry = Self::leading_tree_key(&self.cursors, self.ordering);
                mem::swap(&mut other_entry, &mut self.current);
                let res = other_entry.map(|(_, key)| key);
                if let Some(k) = &res {
                    if self.deletion.contains(k) {
                        // Skip keys in deletion set
                        continue;
                    }
                }
                return res;
            } else {
                return None;
            }
        }
    }
}

fn lsm_treee_schema() -> Schema {
    Schema {
        id: *LSM_TREE_SCHEMA_ID,
        name: String::from(LSM_TREE_SCHEMA_NAME),
        key_field: None,
        str_key_field: None,
        is_dynamic: false,
        fields: Field::new(
            "*",
            0,
            false,
            false,
            Some(vec![
                Field::new(
                    LSM_TREE_LEVELS_NAME,
                    type_id_of(Type::Id),
                    false,
                    true,
                    None,
                    vec![],
                ),
                Field::new(
                    LSM_TREE_MIGRATION_NAME,
                    type_id_of(Type::Id),
                    true,
                    false,
                    None,
                    vec![],
                ),
            ]),
            vec![],
        ),
    }
}

fn lsm_tree_cell(level_ids: &Vec<Id>, id: &Id, migration: Option<Id>) -> Cell {
    let mut cell_map = Map::new();
    cell_map.insert_key_id(
        *LSM_TREE_LEVELS_HASH,
        Value::Array(level_ids.iter().map(|id| id.value()).collect()),
    );
    cell_map.insert_key_id(
        *LSM_TREE_MIGRATION_HASH,
        migration.map(|id| Value::Id(id)).unwrap_or(Value::Null),
    );
    Cell::new_with_id(*LSM_TREE_SCHEMA_ID, id, Value::Map(cell_map))
}

impl_btree_level!(LEVEL_M);
type LevelMTreeKeySlice = [EntryKey; LEVEL_M];
type LevelMTreePtrSlice = [NodeCellRef; LEVEL_M + 1];
type LevelMTree = BPlusTree<LevelMTreeKeySlice, LevelMTreePtrSlice>;

impl_btree_level!(LEVEL_0);
type Level0TreeKeySlice = [EntryKey; LEVEL_0];
type Level0TreePtrSlice = [NodeCellRef; LEVEL_0 + 1];
type Level0Tree = BPlusTree<Level0TreeKeySlice, Level0TreePtrSlice>;

impl_btree_level!(LEVEL_1);
type Level1TreeKeySlice = [EntryKey; LEVEL_1];
type Level1TreePtrSlice = [NodeCellRef; LEVEL_1 + 1];
type Level1Tree = BPlusTree<Level1TreeKeySlice, Level1TreePtrSlice>;

unsafe impl Send for LSMTree {}
unsafe impl Sync for LSMTree {}

struct DummyCursor;

impl Cursor for DummyCursor {
    fn next(&mut self) -> Option<EntryKey> {
        None
    }
    fn current(&self) -> Option<&EntryKey> {
        None
    }
}
