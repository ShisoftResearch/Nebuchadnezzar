use crate::index::btree::level::*;
use crate::index::btree::*; 
use crate::index::trees::*;
use crate::ram::types::*;
use crate::ram::schema::{Field, Schema};
use crate::server;
use crate::client::AsyncClient;
use crate::ram::cell::Cell;
use std::{mem, ptr};
use std::sync::Arc;
use futures::stream::futures_unordered::FuturesUnordered;

pub const LSM_TREE_SCHEMA_NAME: &'static str = "NEB_LSM_TREE";
pub const LSM_TREE_LEVELS_NAME: &'static str = "levels";

lazy_static! {
    pub static ref LSM_TREE_SCHEMA_ID: u32 = key_hash(LSM_TREE_SCHEMA_NAME) as u32;
    pub static ref LSM_TREE_LEVELS_HASH: u64 = key_hash(LSM_TREE_LEVELS_NAME); 
    pub static ref LSM_TREE_SCHEMA: Schema = lsm_treee_schema();
}

pub struct LSMTree {
    trees: [Box<dyn LevelTree>; NUM_LEVELS]
}

impl LSMTree {
    pub async fn create(neb: &Arc<server::NebServer>, neb_client: &Arc<AsyncClient>) -> Self {
        let trees: [Box<dyn LevelTree>; NUM_LEVELS];
        let tree_m = LevelMTree::new();
        let tree_1 = Level1Tree::new();
        let tree_2 = Level2Tree::new();
        let tree_3 = Level3Tree::new();
        let tree_4 = Level4Tree::new();
        tree_m.persist_root(neb).await;
        tree_1.persist_root(neb).await;
        tree_2.persist_root(neb).await;
        tree_3.persist_root(neb).await;
        tree_4.persist_root(neb).await;
        let level_ids = vec![
            tree_m.head_id(), 
            tree_1.head_id(), 
            tree_2.head_id(), 
            tree_3.head_id(), 
            tree_4.head_id()
        ];
        let mut cell_map = Map::new();
        cell_map.insert_key_id(*LSM_TREE_LEVELS_HASH, Value::Array(level_ids.iter().map(|id| id.value()).collect()));
        let lsm_tree_cell = Cell::new(&*LSM_TREE_SCHEMA, Value::Map(cell_map)).unwrap();
        neb_client.write_cell(lsm_tree_cell).await.unwrap().unwrap();
        trees = [box tree_m, box tree_1, box tree_2, box tree_3, box tree_4];
        Self {
            trees
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
            ]),
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