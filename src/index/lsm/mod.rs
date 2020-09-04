// A machine-local concurrent LSM tree based on B-tree implementation

use crate::client::AsyncClient;
use crate::index::btree::level::*;
use crate::index::btree::*;
use crate::index::trees::*;
use crate::ram::cell::Cell;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use crate::server;
use std::sync::Arc;
use std::{mem, ptr};

pub const LSM_TREE_SCHEMA_NAME: &'static str = "NEB_LSM_TREE";
pub const LSM_TREE_LEVELS_NAME: &'static str = "levels";
type LevelTrees = [Box<dyn LevelTree>; NUM_LEVELS];
type LevelCusors = [Box<dyn Cursor>; NUM_LEVELS];

lazy_static! {
    pub static ref LSM_TREE_SCHEMA_ID: u32 = key_hash(LSM_TREE_SCHEMA_NAME) as u32;
    pub static ref LSM_TREE_LEVELS_HASH: u64 = key_hash(LSM_TREE_LEVELS_NAME);
    pub static ref LSM_TREE_SCHEMA: Schema = lsm_treee_schema();
}

pub struct LSMTree {
    trees: LevelTrees,
}

impl LSMTree {
    pub async fn create(neb_client: &Arc<AsyncClient>) -> Self {
        let tree_m = LevelMTree::new();
        let tree_1 = Level1Tree::new();
        let tree_2 = Level2Tree::new();
        let tree_3 = Level3Tree::new();
        let tree_4 = Level4Tree::new();
        tree_m.persist_root(neb_client).await;
        tree_1.persist_root(neb_client).await;
        tree_2.persist_root(neb_client).await;
        tree_3.persist_root(neb_client).await;
        tree_4.persist_root(neb_client).await;
        let level_ids = vec![
            tree_m.head_id(),
            tree_1.head_id(),
            tree_2.head_id(),
            tree_3.head_id(),
            tree_4.head_id(),
        ];
        let mut cell_map = Map::new();
        cell_map.insert_key_id(
            *LSM_TREE_LEVELS_HASH,
            Value::Array(level_ids.iter().map(|id| id.value()).collect()),
        );
        let lsm_tree_cell = Cell::new(&*LSM_TREE_SCHEMA, Value::Map(cell_map)).unwrap();
        neb_client.write_cell(lsm_tree_cell).await.unwrap().unwrap();
        Self {
            trees: [box tree_m, box tree_1, box tree_2, box tree_3, box tree_4],
        }
    }

    pub async fn recover(neb_client: &Arc<AsyncClient>, lsm_tree_id: Id) -> Self {
        let cell = neb_client.read_cell(lsm_tree_id).await.unwrap().unwrap();
        let trees = &cell.data[*LSM_TREE_LEVELS_HASH];
        let trees_m_val = &trees[0usize];
        let trees_1_val = &trees[1usize];
        let trees_2_val = &trees[2usize];
        let trees_3_val = &trees[3usize];
        let trees_4_val = &trees[4usize];

        let tree_m = LevelMTree::from_head_id(trees_m_val.Id().unwrap(), neb_client).await;
        let tree_1 = Level1Tree::from_head_id(trees_1_val.Id().unwrap(), neb_client).await;
        let tree_2 = Level2Tree::from_head_id(trees_2_val.Id().unwrap(), neb_client).await;
        let tree_3 = Level3Tree::from_head_id(trees_3_val.Id().unwrap(), neb_client).await;
        let tree_4 = Level4Tree::from_head_id(trees_4_val.Id().unwrap(), neb_client).await;

        Self {
            trees: [box tree_m, box tree_1, box tree_2, box tree_3, box tree_4],
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

    pub fn merge_levels(&self) {
        for i in 0..self.trees.len() - 1 {
            if self.trees[i].oversized() {
                self.trees[i].merge_to(&*self.trees[i + 1]);
            }
        }
    }

    pub fn oversized(&self) -> bool {
        self.trees[self.trees.len() - 1].oversized()
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
            trees[3].seek_for(key, ordering),
            trees[4].seek_for(key, ordering),
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
    fn next(&mut self) -> bool {
        if let Some((tree_id, _)) = self.current {
            self.cursors[tree_id].next();
            let next_leading = Self::leading_tree_key(&self.cursors, self.ordering);
            let has_next = next_leading.is_some();
            self.current = next_leading;
            has_next
        } else {
            return false;
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
            Some(vec![Field::new(
                LSM_TREE_LEVELS_NAME,
                type_id_of(Type::Id),
                false,
                true,
                None,
                vec![],
            )]),
            vec![],
        ),
    }
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

impl_btree_level!(LEVEL_3);
type Level3TreeKeySlice = [EntryKey; LEVEL_3];
type Level3TreePtrSlice = [NodeCellRef; LEVEL_3 + 1];
type Level3Tree = BPlusTree<Level3TreeKeySlice, Level3TreePtrSlice>;

impl_btree_level!(LEVEL_4);
type Level4TreeKeySlice = [EntryKey; LEVEL_4];
type Level4TreePtrSlice = [NodeCellRef; LEVEL_4 + 1];
type Level4Tree = BPlusTree<Level4TreeKeySlice, Level4TreePtrSlice>;
