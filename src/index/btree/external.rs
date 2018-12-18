use bifrost::utils::async_locks::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use bifrost::utils::fut_exec::wait;
use client::AsyncClient;
use core::borrow::BorrowMut;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::custom_types::map::Map;
use dovahkiin::types::type_id_of;
use dovahkiin::types::value::ToValue;
use futures::Future;
use index::btree::*;
use itertools::Itertools;
use owning_ref::{OwningHandle, OwningRef, RcRef};
use ram::cell::Cell;
use ram::schema::{Field, Schema};
use ram::types::*;
use std::cell::Ref;
use std::cell::RefCell;
use std::cell::RefMut;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use utils::lru_cache::LRUCache;

pub type ExtNodeCacheMap = Mutex<LRUCache<Id, Arc<RwLock<ExtNode>>>>;
pub type ExtNodeCachedMut = RwLockWriteGuard<ExtNode>;
pub type ExtNodeCachedImmute = RwLockReadGuard<ExtNode>;

const PAGE_SCHEMA: &'static str = "NEB_BTREE_PAGE";
const KEYS_FIELD: &'static str = "keys";
const NEXT_FIELD: &'static str = "next";
const PREV_FIELD: &'static str = "prev";

lazy_static! {
    static ref KEYS_KEY_HASH: u64 = key_hash(KEYS_FIELD);
    static ref NEXT_PAGE_KEY_HASH: u64 = key_hash(NEXT_FIELD);
    static ref PREV_PAGE_KEY_HASH: u64 = key_hash(PREV_FIELD);
    static ref PAGE_SCHEMA_ID: u32 = key_hash(PAGE_SCHEMA) as u32;
}

pub struct ExtNode {
    pub id: Id,
    pub keys: EntryKeySlice,
    pub next: NodeCellRef,
    pub prev: NodeCellRef,
    pub len: usize,
    pub dirty: bool,
}

pub struct ExtNodeSplit {
    pub node_2: ExtNode,
    pub keys_1_len: usize,
}

impl ExtNode {
    pub fn new(id: Id) -> ExtNode {
        ExtNode {
            id,
            keys: EntryKeySlice::init(),
            next: Node::none_ref(),
            prev: Node::none_ref(),
            len: 0,
            dirty: false,
        }
    }

    pub fn to_cell(&self) -> Cell {
        let mut value = Value::Map(Map::new());
        let prev_id = read_node(&self.prev, |node| node.extnode().id);
        let next_id = read_node(&self.next, |node| node.extnode().id);

        value[*NEXT_PAGE_KEY_HASH] = Value::Id(prev_id);
        value[*PREV_PAGE_KEY_HASH] = Value::Id(next_id);
        value[*KEYS_KEY_HASH] = self.keys[..self.len]
            .iter()
            .map(|key| SmallBytes::from_vec(key.as_slice().to_vec()))
            .collect_vec()
            .value();
        Cell::new_with_id(*PAGE_SCHEMA_ID, &self.id, value)
    }
    pub fn search(&self, key: &EntryKey) -> usize {
        self.keys[..self.len]
            .binary_search(key)
            .unwrap_or_else(|i| i)
    }
    pub fn remove_at(&mut self, pos: usize) {
        let mut cached_len = self.len;
        debug!("Removing from external pos {}, len {}, key {:?}",
               pos, cached_len, self.keys[pos]);
        self.keys.remove_at(pos, &mut cached_len);
        self.len = cached_len;
    }
    pub fn insert(
        &mut self,
        key: &EntryKey,
        tree: &BPlusTree,
        self_ref: &NodeCellRef,
        parent: &NodeCellRef,
    ) -> Option<NodeSplit> {
        let self_len = self.len;
        let key = key.clone();
        let pos = self.search(&key);
        debug_assert!(self_len <= NUM_KEYS);
        debug_assert!(pos <= self_len);
        if self_len == NUM_KEYS {
            // need to split
            debug!("insert to external with split, key {:?}, pos {}", key, pos);
            let self_next = &mut *write_node(&self.next);
            let parent_latch = write_node(parent);
            // cached.dump();
            let pivot = self_len / 2;
            let new_page_id = tree.new_page_id();
            let mut keys_1 = &mut self.keys;
            let mut keys_2 = keys_1.split_at_pivot(pivot, self_len);
            let mut keys_1_len = pivot;
            let mut keys_2_len = self_len - pivot;
            // modify next node point previous to new node
            insert_into_split(
                key,
                keys_1,
                &mut keys_2,
                &mut keys_1_len,
                &mut keys_2_len,
                pos,
            );
            let pivot_key = keys_2[0].clone();
            let extnode_2 = ExtNode {
                id: new_page_id,
                keys: keys_2,
                next: self.next.clone(),
                prev: self_ref.clone(),
                len: keys_2_len,
                dirty: true,
            };
            debug_assert!(pivot_key > smallvec!(0));
            debug_assert!(
                &pivot_key > &keys_1[keys_1_len - 1],
                "{:?} / {:?} @ {}",
                pivot_key,
                &keys_1[keys_1_len - 1],
                pos
            );
            debug_assert!(extnode_2.prev.read_unchecked().is_ext());
            self.len = keys_1_len;
            debug!(
                "Split to left len {}, right len {}, right prev id: {:?}",
                self.len,
                extnode_2.len,
                extnode_2.prev.read_unchecked().ext_id()
            );
            let node_2 = Arc::new(Node::external(extnode_2));
            {
                if !self_next.is_none() {
                    self_next.extnode_mut().prev = node_2.clone();
                }
            }
            self.next = node_2.clone();

            return Some(NodeSplit {
                new_right_node: node_2,
                pivot: pivot_key,
                parent_latch,
                left_node_latch: NodeWriteGuard::default(),
            });
        } else {
            debug!("insert to external without split at {}, key {:?}", pos, key);
            let mut new_cached_len = self_len;
            self.keys.insert_at(key, pos, &mut new_cached_len);
            self.len = new_cached_len;
            return None;
        }
    }
    pub fn merge_with(&mut self, right: &mut Self) {
        debug!(
            "Merge external node, left len {}:{:?}, right len {}:{:?}",
            self.len, self.keys, right.len, right.keys
        );
        let self_len = self.len;
        let new_len = self.len + right.len;
        debug_assert!(new_len <= self.keys.len());
        for i in self.len..new_len {
            self.keys[i] = mem::replace(&mut right.keys[i - self_len], Default::default());
        }
        self.len = new_len;
    }
    pub fn dump(&self) {
        debug!("Dumping {:?}, keys {}", self.id, self.len);
        for i in 0..NUM_KEYS {
            debug!("{}\t- {:?}", i, self.keys[i]);
        }
    }
    pub fn is_dirty(&self) -> bool {
        unimplemented!()
    }
}

pub fn page_schema() -> Schema {
    Schema {
        id: *PAGE_SCHEMA_ID,
        name: String::from(PAGE_SCHEMA),
        key_field: None,
        str_key_field: None,
        is_dynamic: false,
        fields: Field::new(
            "*",
            0,
            false,
            false,
            Some(vec![
                Field::new(NEXT_FIELD, type_id_of(Type::Id), false, false, None),
                Field::new(PREV_FIELD, type_id_of(Type::Id), false, false, None),
                Field::new(KEYS_FIELD, type_id_of(Type::SmallBytes), false, true, None),
            ]),
        ),
    }
}

pub struct NodeCache {
    nodes: RefCell<HashMap<Id, NodeCellRef>>,
    storage: Arc<AsyncClient>,
}

impl NodeCache {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        NodeCache {
            nodes: RefCell::new(HashMap::new()),
            storage: neb_client.clone(),
        }
    }

    pub fn get(&self, id: &Id) -> NodeCellRef {
        if id.is_unit_id() {
            return Arc::new(Node::none());
        }
        let mut nodes = self.nodes.borrow_mut();
        nodes
            .entry(*id)
            .or_insert_with(|| {
                let cell = self.storage.read_cell(*id).wait().unwrap().unwrap();
                Arc::new(Node::external(self.extnode_from_cell(cell)))
            })
            .clone()
    }

    fn extnode_from_cell(&self, cell: Cell) -> ExtNode {
        let cell_id = cell.id();
        let cell_version = cell.header.version;
        let next = cell.data[*NEXT_PAGE_KEY_HASH].Id().unwrap();
        let prev = cell.data[*PREV_PAGE_KEY_HASH].Id().unwrap();
        let keys = &cell.data[*KEYS_KEY_HASH];
        let keys_len = keys.len().unwrap();
        let keys_array = if let Value::PrimArray(PrimitiveArray::SmallBytes(ref array)) = keys {
            array
        } else {
            panic!()
        };
        let mut key_slice = EntryKeySlice::init();
        let mut key_count = 0;
        for (i, key_val) in keys_array.iter().enumerate() {
            key_slice[i] = EntryKey::from(key_val.as_slice());
            key_count += 1;
        }
        ExtNode {
            id: cell_id,
            keys: key_slice,
            next: self.get(next),
            prev: self.get(prev),
            len: key_count,
            dirty: false,
        }
    }
}
