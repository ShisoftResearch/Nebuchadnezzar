// A machine-local concurrent LSM tree based on B-tree implementation

use super::btree::level::*;
use super::btree::*;
use crate::client::AsyncClient;
use crate::ram::cell::Cell;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use lightning::map::HashSet;
use std::sync::atomic::Ordering::{Acquire, Release};
use crossbeam_epoch::*;
use std::mem;
use std::sync::Arc;

pub const LSM_TREE_SCHEMA_NAME: &'static str = "NEB_LSM_TREE";
pub const LSM_TREE_LEVELS_NAME: &'static str = "levels";
pub const LSM_TREE_MIGRATION_NAME: &'static str = "migration";
pub const LAST_LEVEL_MULT_FACTOR: usize = 16;

type LevelTrees = [Box<dyn LevelTree>; NUM_LEVELS];
type LevelCusors = Vec<Box<dyn Cursor>>;
pub type DeletionSet = HashSet<EntryKey>;

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
    pub deletion: Arc<DeletionSet>
}

impl LSMTree {
    pub async fn create(neb_client: &Arc<AsyncClient>, id: &Id) -> Self {
        let deletion_ref = Arc::new(HashSet::with_capacity(16));
        let tree_m = LevelMTree::new(&deletion_ref);
        let tree_0 = Level0Tree::new(&deletion_ref);
        let tree_1 = Level1Tree::new(&deletion_ref);
        let tree_2 = Level2Tree::new(&deletion_ref);
        tree_0.persist_root(neb_client).await;
        tree_1.persist_root(neb_client).await;
        tree_2.persist_root(neb_client).await;
        let level_ids = vec![tree_m.head_id(), tree_1.head_id(), tree_2.head_id()];
        let lsm_tree_cell = lsm_tree_cell(&level_ids, id, None);
        neb_client.write_cell(lsm_tree_cell).await.unwrap().unwrap();
        Self {
            mem_tree: Atomic::new(box tree_m),
            trans_mem_tree: Atomic::null(),
            disk_trees: [box tree_0, box tree_1, box tree_2],
            deletion: deletion_ref
        }
    }

    pub async fn recover(neb_client: &Arc<AsyncClient>, lsm_tree_id: &Id) -> Self {
        let deletion_ref = Arc::new(HashSet::with_capacity(16));
        let cell = neb_client.read_cell(*lsm_tree_id).await.unwrap().unwrap();
        let trees = &cell.data[*LSM_TREE_LEVELS_HASH];
        let trees_0_val = &trees[0usize];
        let trees_1_val = &trees[1usize];
        let trees_2_val = &trees[2usize];

        let tree_0 = Level0Tree::from_head_id(trees_0_val.Id().unwrap(), neb_client, &deletion_ref).await;
        let tree_1 = Level1Tree::from_head_id(trees_1_val.Id().unwrap(), neb_client, &deletion_ref).await;
        let tree_2 = Level2Tree::from_head_id(trees_2_val.Id().unwrap(), neb_client, &deletion_ref).await;

        Self {
            mem_tree: Atomic::new(box LevelMTree::new(&deletion_ref)),
            trans_mem_tree: Atomic::null(),
            disk_trees: [box tree_0, box tree_1, box tree_2],
            deletion: deletion_ref
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
                mem_tree.merge_all_to(0, &*self.disk_trees[0], false);
                self.trans_mem_tree.store(Shared::null(), Acquire);
                unsafe {
                    guard.defer_destroy(mem_tree_ptr);
                }
                info!("Memory tree merging completed");
            } else {
                trace!("Memory tree not oversized");
            }
        }
        for i in 0..self.disk_trees.len() - 1 {
            let level = i + 1;
            if self.disk_trees[i].oversized() {
                info!("Level {} tree oversized, merging", level);
                self.disk_trees[i].merge_to(level, &*self.disk_trees[i + 1], true).await;
                info!("Level {} merge completed", level);
                merged = true;
            } else {
                trace!("Level {} tree not oversized", level);
            }
        }
        merged
    }

    pub fn oversized(&self) -> bool {
        self.last_level_tree().count() > self.ideal_capacity()
    }

    pub fn mid_key(&self) -> Option<EntryKey> {
        self.last_level_tree().mid_key()
    }

    pub async fn mark_migration(&self, id: &Id, migration: Option<Id>, client: &Arc<AsyncClient>) {
        let lsm_tree_cell = lsm_tree_cell(
            &self.disk_trees.iter().map(|tree| tree.head_id()).collect(),
            id,
            migration,
        );
        client.update_cell(lsm_tree_cell).await.unwrap().unwrap();
    }

    pub fn merge_keys(&self, keys: Box<Vec<EntryKey>>) {
        self.last_level_tree().merge_with_keys(keys);
    }

    pub fn ideal_capacity(&self) -> usize {
        self.last_level_tree().ideal_capacity() * LAST_LEVEL_MULT_FACTOR
    }

    fn last_level_tree(&self) -> &Box<dyn LevelTree> {
        self.disk_trees.last().unwrap()
    }
}

pub struct LSMTreeCursor {
    cursors: LevelCusors,
    current: Option<(usize, EntryKey)>,
    ordering: Ordering,
    deletion: Arc<DeletionSet>
}

impl LSMTreeCursor {
    fn new(key: &EntryKey, lsm_tree: &LSMTree, ordering: Ordering) -> Self {
        let guard = crossbeam_epoch::pin();
        let disk_trees = &lsm_tree.disk_trees;
        let mem_tree_ptr = lsm_tree.mem_tree.load(Acquire, &guard);
        let trans_mem_tree_ptr = lsm_tree.trans_mem_tree.load(Acquire, &guard);
        let mem_tree = unsafe { mem_tree_ptr.as_ref().unwrap() };
        let mut cursors = vec![
            mem_tree.seek_for(key, ordering),
            disk_trees[0].seek_for(key, ordering),
            disk_trees[1].seek_for(key, ordering),
            disk_trees[2].seek_for(key, ordering),
        ];
        if !trans_mem_tree_ptr.is_null() && trans_mem_tree_ptr != mem_tree_ptr {
            let trans_mem_tree = unsafe { trans_mem_tree_ptr.as_ref().unwrap() };
            cursors.push(trans_mem_tree.seek_for(key, ordering));
        }
        let current = Self::leading_tree_key(&cursors, ordering);
        let deletion = lsm_tree.deletion.clone();
        Self {
            cursors,
            current,
            ordering,
            deletion
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

impl_btree_level!(LEVEL_2);
type Level2TreeKeySlice = [EntryKey; LEVEL_2];
type Level2TreePtrSlice = [NodeCellRef; LEVEL_2 + 1];
type Level2Tree = BPlusTree<Level2TreeKeySlice, Level2TreePtrSlice>;

unsafe impl Send for LSMTree {}
unsafe impl Sync for LSMTree {}