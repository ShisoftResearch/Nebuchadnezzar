// A machine-local concurrent LSM tree based on B-tree implementation

use super::btree::level::*;
use super::btree::*;
use crate::client::AsyncClient;
use crate::ram::cell::Cell;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use std::mem;
use std::sync::Arc;

pub const LSM_TREE_SCHEMA_NAME: &'static str = "NEB_LSM_TREE";
pub const LSM_TREE_LEVELS_NAME: &'static str = "levels";
pub const LSM_TREE_MIGRATION_NAME: &'static str = "migration";
pub const LAST_LEVEL_MULT_FACTOR: usize = 16;

type LevelTrees = [Box<dyn LevelTree>; NUM_LEVELS];
type LevelCusors = [Box<dyn Cursor>; NUM_LEVELS];

lazy_static! {
    pub static ref LSM_TREE_SCHEMA_ID: u32 = key_hash(LSM_TREE_SCHEMA_NAME) as u32;
    pub static ref LSM_TREE_LEVELS_HASH: u64 = key_hash(LSM_TREE_LEVELS_NAME);
    pub static ref LSM_TREE_MIGRATION_HASH: u64 = key_hash(LSM_TREE_MIGRATION_NAME);
    pub static ref LSM_TREE_SCHEMA: Schema = lsm_treee_schema();
}

pub struct LSMTree {
    pub trees: LevelTrees,
}

impl LSMTree {
    pub async fn create(neb_client: &Arc<AsyncClient>, id: &Id) -> Self {
        let tree_m = LevelMTree::new();
        let tree_1 = Level1Tree::new();
        let tree_2 = Level2Tree::new();
        tree_m.persist_root(neb_client).await;
        tree_1.persist_root(neb_client).await;
        tree_2.persist_root(neb_client).await;
        let level_ids = vec![tree_m.head_id(), tree_1.head_id(), tree_2.head_id()];
        let lsm_tree_cell = lsm_tree_cell(&level_ids, id, None);
        neb_client.write_cell(lsm_tree_cell).await.unwrap().unwrap();
        Self {
            trees: [box tree_m, box tree_1, box tree_2],
        }
    }

    pub async fn recover(neb_client: &Arc<AsyncClient>, lsm_tree_id: &Id) -> Self {
        let cell = neb_client.read_cell(*lsm_tree_id).await.unwrap().unwrap();
        let trees = &cell.data[*LSM_TREE_LEVELS_HASH];
        let trees_m_val = &trees[0usize];
        let trees_1_val = &trees[1usize];
        let trees_2_val = &trees[2usize];

        let tree_m = LevelMTree::from_head_id(trees_m_val.Id().unwrap(), neb_client).await;
        let tree_1 = Level1Tree::from_head_id(trees_1_val.Id().unwrap(), neb_client).await;
        let tree_2 = Level2Tree::from_head_id(trees_2_val.Id().unwrap(), neb_client).await;

        Self {
            trees: [box tree_m, box tree_1, box tree_2],
        }
    }

    pub fn insert(&self, entry: &EntryKey) -> bool {
        self.trees[0].insert_into(entry)
    }

    pub fn delete(&self, entry: &EntryKey) -> bool {
        for tree in &self.trees {
            if tree.mark_key_deleted(entry) {
                return true;
            }
        }
        false
    }

    pub fn seek(&self, entry: &EntryKey, ordering: Ordering) -> LSMTreeCursor {
        LSMTreeCursor::new(entry, &self.trees, ordering)
    }

    pub async fn merge_levels(&self) -> bool {
        let mut merged = false;
        for i in 0..self.trees.len() - 1 {
            if self.trees[i].oversized() {
                info!("Level {} tree oversized, merging", i);
                self.trees[i].merge_to(i, &*self.trees[i + 1]).await;
                info!("Level {} merge completed", i);
                merged = true;
            } else {
               trace!("Level {} tree not oversized", i);
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
            &self.trees.iter().map(|tree| tree.head_id()).collect(),
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
        self.trees.last().unwrap()
    }
}

pub struct LSMTreeCursor {
    cursors: LevelCusors,
    current: Option<(usize, EntryKey)>,
    ordering: Ordering,
}

impl LSMTreeCursor {
    fn new(key: &EntryKey, trees: &LevelTrees, ordering: Ordering) -> Self {
        let cursors = [
            trees[0].seek_for(key, ordering),
            trees[1].seek_for(key, ordering),
            trees[2].seek_for(key, ordering),
        ];
        let current = Self::leading_tree_key(&cursors, ordering);
        Self {
            cursors,
            current,
            ordering,
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
        if let Some((tree_id, _)) = self.current {
            self.cursors[tree_id].next();
            let mut other_entry = Self::leading_tree_key(&self.cursors, self.ordering);
            mem::swap(&mut other_entry, &mut self.current);
            other_entry.map(|(_, key)| key)
        } else {
            return None;
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

impl_btree_level!(LEVEL_1);
type Level1TreeKeySlice = [EntryKey; LEVEL_1];
type Level1TreePtrSlice = [NodeCellRef; LEVEL_1 + 1];
type Level1Tree = BPlusTree<Level1TreeKeySlice, Level1TreePtrSlice>;

impl_btree_level!(LEVEL_2);
type Level2TreeKeySlice = [EntryKey; LEVEL_2];
type Level2TreePtrSlice = [NodeCellRef; LEVEL_2 + 1];
type Level2Tree = BPlusTree<Level2TreeKeySlice, Level2TreePtrSlice>;
