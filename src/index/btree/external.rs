use utils::lru_cache::LRUCache;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::custom_types::map::Map;
use std::cell::RefCell;
use ram::cell::Cell;
use std::cell::Ref;
use std::cell::RefMut;
use index::btree::*;
use parking_lot::{RwLock, RwLockWriteGuard, RwLockReadGuard};

pub type ExtNodeCacheMap = RwLock<LRUCache<Id, RwLock<ExtNodeCached>>>;
pub type ExtNodeCachedMut<'a> = RwLockWriteGuard<'a, ExtNodeCached>;
pub type ExtNodeCachedImmute<'a> = RwLockReadGuard<'a, ExtNodeCached>;

lazy_static! {
    static ref KEYS_KEY_HASH : u64 = key_hash("keys");
    static ref NEXT_PAGE_KEY_HASH : u64 = key_hash("next");
    static ref PREV_PAGE_KEY_HASH : u64 = key_hash("prev");
    static ref PAGE_SCHEMA_ID: u32 = key_hash("BTREE_SCHEMA_ID") as u32;
}

pub struct ExtNodeCached {
    pub id: Id,
    pub keys: EntryKeySlice,
    pub next: Id,
    pub prev: Id,
    pub len: usize,
    pub version: u64,
    pub removed: bool,
}

pub struct ExtNodeSplit {
    pub node_2: ExtNodeCached,
    pub keys_1_len: usize
}

impl ExtNodeCached {
    pub fn new(id: &Id) -> ExtNodeCached {
        ExtNodeCached {
            id: *id,
            keys: EntryKeySlice::init(),
            next: Id::unit_id(),
            prev: Id::unit_id(),
            len: 0,
            version: 0,
            removed: false
        }
    }
    pub fn from_cell(cell: Cell) -> Self {
        let keys = &cell.data[*KEYS_KEY_HASH];
        let keys_len = keys.len().unwrap();
        let mut key_slice = EntryKeySlice::init();
        let mut key_count = 0;
        let next = cell.data[*NEXT_PAGE_KEY_HASH].Id().unwrap();
        let prev = cell.data[*PREV_PAGE_KEY_HASH].Id().unwrap();
        for (i, key_val) in keys.iter_value().unwrap().enumerate() {
            let key = if let Value::PrimArray(PrimitiveArray::U8(ref array)) = key_val {
                EntryKey::from_slice(array.as_slice())
            } else { panic!("invalid entry") };
            key_slice[i] = key;
            key_count += 1;
        }
        ExtNodeCached {
            id: cell.id(),
            keys: key_slice,
            next: *next,
            prev: *prev,
            len: key_count,
            version: cell.header.version,
            removed: false
        }
    }
    pub fn to_cell(&self) -> Cell {
        let mut value = Value::Map(Map::new());
        value[*NEXT_PAGE_KEY_HASH] = Value::Id(self.next);
        value[*KEYS_KEY_HASH] = self
            .keys[..self.len]
            .iter()
            .map(|key| {
                key.as_slice().to_vec().value()
            })
            .collect_vec()
            .value();
        Cell::new_with_id(*PAGE_SCHEMA_ID, &self.id, value)
    }
    pub fn remove(&mut self, pos: usize, tree: &BPlusTree) {
        let cached_len = self.len;
        self.keys.remove_at(pos, cached_len);
        self.len -= 1;
    }
    pub fn insert(&mut self, key: EntryKey, pos: usize, tree: &BPlusTree) -> Option<(Node, Option<EntryKey>)> {
        let mut cached = self;
        let cached_len = cached.len;
        if cached_len + 1 >= NUM_KEYS {
            // need to split
            let pivot = cached_len / 2;
            let split = {
                let cached_next = *&cached.next;
                let cached_id = *&cached.id;
                let new_page_id = tree.new_page_id();
                cached.next = new_page_id;
                let mut keys_1 = &mut cached.keys;
                let mut keys_2 = keys_1.split_at_pivot(pivot, cached_len);
                let mut keys_1_len = pivot;
                let mut keys_2_len = cached_len - pivot;
                insert_into_split(
                    key,
                    keys_1, &mut keys_2,
                    &mut keys_1_len, &mut keys_2_len,
                    pos, pivot);
                ExtNodeSplit {
                    keys_1_len,
                    node_2: ExtNodeCached {
                        id: new_page_id,
                        keys: keys_2,
                        next: cached_next,
                        prev: cached_id,
                        len: keys_2_len,
                        version: 0,
                        removed: false
                    }
                }
            };
            cached.next = split.node_2.id;
            cached.len = split.keys_1_len;
            return Some((Node::External(box ExtNode::from_cached(split.node_2)), None));

        } else {
            cached.keys.insert_at(key, pos, cached_len);
            return None;
        }
    }
    pub fn update(&self) {

    }
}

pub fn rearrange_empty_extnode(mut sub_level_pointer: RefMut<Node>, tree: &BPlusTree) {
    let node = sub_level_pointer.ext_node();
    let cached = node.get_cached(tree);
    let prev = cached.prev;
    let next = cached.next;
    if !prev.is_unit_id() {
        let mut prev_node = tree.get_ext_node_cached(&prev);
        prev_node.next = next;
    }
    if !next.is_unit_id() {
        let mut next_node = tree.get_ext_node_cached(&next);
        next_node.prev = prev;
    }
}