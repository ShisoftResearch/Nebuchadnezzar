use utils::lru_cache::LRUCache;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::custom_types::map::Map;
use std::cell::RefCell;
use ram::cell::Cell;
use std::cell::Ref;
use std::cell::RefMut;
use index::btree::*;
use bifrost::utils::async_locks::{RwLock, RwLockWriteGuard, RwLockReadGuard, Mutex};
use std::ops::Deref;
use std::ops::DerefMut;
use core::borrow::BorrowMut;
use dovahkiin::types::value::ToValue;
use itertools::Itertools;
use std::collections::BTreeMap;
use std::mem;
use std::rc::Rc;
use owning_ref::{RcRef, OwningHandle, OwningRef};

pub type ExtNodeCacheMap = Mutex<LRUCache<Id, RwLock<ExtNode>>>;
pub type ExtNodeCachedMut = RwLockWriteGuard<ExtNode>;
pub type ExtNodeCachedImmute = RwLockReadGuard<ExtNode>;

lazy_static! {
    static ref KEYS_KEY_HASH : u64 = key_hash("keys");
    static ref NEXT_PAGE_KEY_HASH : u64 = key_hash("next");
    static ref PREV_PAGE_KEY_HASH : u64 = key_hash("prev");
    static ref PAGE_SCHEMA_ID: u32 = key_hash("BTREE_SCHEMA_ID") as u32;
}

#[derive(Clone)]
pub struct ExtNode {
    pub id: Id,
    pub keys: EntryKeySlice,
    pub next: Id,
    pub prev: Id,
    pub len: usize,
    pub version: u64,
    pub removed: bool,
}

pub struct ExtNodeSplit {
    pub node_2: ExtNode,
    pub keys_1_len: usize
}

impl ExtNode {
    pub fn new(id: &Id) -> ExtNode {
        ExtNode {
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
        ExtNode {
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
    pub fn remove(&mut self, pos: usize) {
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
                    node_2: ExtNode {
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
            return Some((Node::External(cached.id), None));

        } else {
            cached.keys.insert_at(key, pos, cached_len);
            return None;
        }
    }
    pub fn merge_with(&mut self, right: &mut Self) {
        let self_len = self.len;
        let new_len = self.len + right.len;
        assert!(new_len <= self.keys.len());
        for i in self.len .. new_len {
            self.keys[i] = mem::replace(&mut right.keys[i - self_len], Default::default());
        }
    }
}

pub fn rearrange_empty_extnode(node: &ExtNode, bz: &mut CacheBufferZone) -> Id {
    let prev = node.prev;
    let next = node.next;
    if !prev.is_unit_id() {
        let mut prev_node = bz.get_for_mut(&prev);
        prev_node.next = next;
    }
    if !next.is_unit_id() {
        let mut next_node = bz.get_for_mut(&next);
        next_node.prev = prev;
    }
    return node.id;
}

#[derive(Clone)]
enum CacheGuardHolder {
    Read(ExtNodeCachedImmute),
    Write(ExtNodeCachedMut)
}

type NodeRcRefCell = Rc<RefCell<ExtNode>>;

pub type RcNodeRef<'a> = OwningHandle<RcRef<RefCell<ExtNode>>, Ref<'a, ExtNode>>;
pub type RcNodeRefMut<'a> = OwningHandle<RcRef<RefCell<ExtNode>>, RefMut<'a, ExtNode>>;


pub struct CacheBufferZone<'a> {
    tree: &'a BPlusTree,
    guards: RefCell<BTreeMap<Id, CacheGuardHolder>>,
    data: RefCell<BTreeMap<Id, Option<NodeRcRefCell>>>
}


impl <'a> CacheBufferZone <'a> {
    pub fn new(tree: &BPlusTree) -> CacheBufferZone {
        CacheBufferZone {
            tree,
            guards: RefCell::new(BTreeMap::new()),
            data: RefCell::new(BTreeMap::new())
        }
    }

    pub fn get(&self, id: &Id) -> RcNodeRef {
        {
            let data = self.data.borrow();
            match data.get(id) {
                Some(Some(ref data)) => {
                    let cell = data.clone();
                    let cell_ref = RcRef::new(cell);
                    let handle = OwningHandle::new(cell_ref);
                    return handle
                },
                Some(None) => panic!(),
                _ => {}
            }
        }
        {
            let guard = self.tree.get_ext_node_cached(id);
            let mut data = self.data.borrow_mut();
            data.insert(*id, Some(Rc::new(RefCell::new((&*guard).clone()))));
            let holder = CacheGuardHolder::Read(guard);
            let mut guards = self.guards.borrow_mut();
            guards.insert(*id, holder);
        }
        return self.get(id);
    }

    pub fn get_for_mut(&self, id: &Id) -> RcNodeRefMut {
        {
            let data = self.data.borrow();
            match data.get(id) {
                Some(Some(ref data)) => {
                    let guards = self.guards.borrow();
                    if let CacheGuardHolder::Read(_) = guards.get(id).unwrap() {
                        panic!("Mutating readonly buffered cache");
                    }
                    let cell = data.clone();
                    let cell_ref = RcRef::new(cell);
                    let handle = OwningHandle::new_mut(cell_ref);
                    return handle
                },
                Some(None) => panic!(),
                _ => {}
            }
        }
        {
            let guard = self.tree.get_mut_ext_node_cached(id);
            let mut data = self.data.borrow_mut();
            data.borrow_mut().insert(*id, Some(Rc::new(RefCell::new((&*guard).clone()))));
            let holder = CacheGuardHolder::Write(guard);
            let mut guards = self.guards.borrow_mut();
            guards.insert(*id, holder);
        }
        self.get_for_mut(id)
    }

    pub fn update(&self, id: &Id, node: ExtNode) {
        let mut data = self.data.borrow_mut();
        data.insert(*id, Some(Rc::new(RefCell::new(node))));
    }
    pub fn delete(&self, id: &Id) {
        let mut data = self.data.borrow_mut();
        data.insert(*id, None);
    }

    pub fn flush(self) {
        let mut guards = self.guards.into_inner();
        for (id, data) in self.data.into_inner() {
            if let Some(mut node_rc) = data {
                let mut holder = guards.get_mut(&id).unwrap();
                if let &mut CacheGuardHolder::Write(ref mut guard) = holder {
                    let mut ext_node = Rc::get_mut(&mut node_rc).unwrap();
                    **guard = mem::replace(ext_node.get_mut(), ExtNode::new(&Id::unit_id()))
                } else { panic!() }
            } else {
                unimplemented!();
            }
        }
    }
}
