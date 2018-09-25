use utils::lru_cache::LRUCache;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::custom_types::map::Map;
use std::cell::RefCell;
use ram::cell::Cell;
use ram::types::*;
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
use ram::schema::{Schema, Field};
use dovahkiin::types::type_id_of;
use std::sync::Arc;
use client::AsyncClient;
use futures::Future;

pub type ExtNodeCacheMap = Mutex<LRUCache<Id, RwLock<ExtNode>>>;
pub type ExtNodeCachedMut = RwLockWriteGuard<ExtNode>;
pub type ExtNodeCachedImmute = RwLockReadGuard<ExtNode>;

const PAGE_SCHEMA: &'static str = "NEB_BTREE_PAGE";
const KEYS_FIELD: &'static str = "keys";
const NEXT_FIELD: &'static str = "next";
const PREV_FIELD: &'static str = "prev";

lazy_static! {
    static ref KEYS_KEY_HASH : u64 = key_hash(KEYS_FIELD);
    static ref NEXT_PAGE_KEY_HASH : u64 = key_hash(NEXT_FIELD);
    static ref PREV_PAGE_KEY_HASH : u64 = key_hash(PREV_FIELD);
    static ref PAGE_SCHEMA_ID: u32 = key_hash(PAGE_SCHEMA) as u32;
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
    pub fn insert(
        &mut self,
        key: EntryKey,
        pos: usize,
        tree: &BPlusTree,
        bz: &CacheBufferZone)
        -> Option<(Node, Option<EntryKey>)>
    {
        let mut cached = self;
        let cached_len = cached.len;
        if cached_len >= NUM_KEYS {
            // need to split
            debug!("insert to external with split, key {:?}, pos {}", &key, pos);
            cached.dump();
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
            let node_2_id = split.node_2.id;
            let node_2 = Node::External(box node_2_id);
            debug!("Split to left len {}, right len {}", cached.len, split.node_2.len);
            cached.dump();
            split.node_2.dump();
            bz.new_node(split.node_2);
            return Some((node_2, None));

        } else {
            debug!("insert to external without split at {}, key {:?}", pos, key);
            let middle_insertion = pos != cached_len;
            let mut new_cached_len = cached_len;
            cached.keys.insert_at(key, pos, &mut new_cached_len);
            cached.len = new_cached_len;
            if middle_insertion {
                cached.dump();
            }
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
    pub fn dump(&self) {
        debug!("Dumping {:?}, keys {}", self.id, self.len);
        for i in 0..NUM_KEYS {
            println!("{}\t- {:?}", i, self.keys[i]);
        }
    }
}

pub fn rearrange_empty_extnode(node: &ExtNode, bz: &CacheBufferZone) -> Id {
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
pub enum CacheGuardHolder {
    Read(ExtNodeCachedImmute),
    Write(ExtNodeCachedMut)
}

type NodeRcRefCell = Rc<RefCell<ExtNode>>;

pub type RcNodeRefData<'a> = OwningHandle<RcRef<RefCell<ExtNode>>, Ref<'a, ExtNode>>;
pub type RcNodeRefMut<'a> = OwningHandle<RcRef<RefCell<ExtNode>>, RefMut<'a, ExtNode>>;


pub enum RcNodeRef<'a> {
    Data(RcNodeRefData<'a>),
    Guard(CacheGuardHolder)
}

pub struct CacheBufferZone {
    mapper: Arc<ExtNodeCacheMap>,
    storage: Arc<AsyncClient>,
    guards: RefCell<BTreeMap<Id, CacheGuardHolder>>,
    data: RefCell<BTreeMap<Id, Option<NodeRcRefCell>>>
}

impl Deref for CacheGuardHolder {
    type Target = ExtNode;

    fn deref(&self) -> &'_ <Self as Deref>::Target {
        match self {
            &CacheGuardHolder::Read(ref g) => &*g,
            &CacheGuardHolder::Write(ref g) => &*g
        }
    }
}

impl <'a> Deref for RcNodeRef<'a> {
    type Target = ExtNode;

    fn deref(&self) -> &'_ <Self as Deref>::Target {
        match self {
            &RcNodeRef::Guard(ref g) => &*g,
            &RcNodeRef::Data(ref g) => &*g
        }
    }
}

impl CacheBufferZone {
    pub fn new(mapper: &Arc<ExtNodeCacheMap>, storage: &Arc<AsyncClient>) -> CacheBufferZone {
        CacheBufferZone {
            mapper: mapper.clone() ,
            storage: storage.clone(),
            guards: RefCell::new(BTreeMap::new()),
            data: RefCell::new(BTreeMap::new())
        }
    }

    pub fn get(&self, id: &Id) -> RcNodeRef {
        debug!("Getting cached fo readonly {:?}", id);
        {
            // read from data (from write)
            let data = self.data.borrow();
            match data.get(id) {
                Some(Some(ref data)) => {
                    let cell = data.clone();
                    let cell_ref = RcRef::new(cell);
                    let handle = OwningHandle::new(cell_ref);
                    return RcNodeRef::Data(handle);
                },
                Some(None) => panic!(), // deleted
                _ => {}
            }
        }
        {
            // read from guards
            let guard = self.guards.borrow();
            if let Some(holder) = guard.get(id) {
                return RcNodeRef::Guard(holder.clone());
            }
        }
        {
            debug!("Buffer zone inserting for readonly");
            let guard = self.get_ext_node_cached(id);
            let holder = CacheGuardHolder::Read(guard);
            let mut guards = self.guards.borrow_mut();
            guards.insert(*id, holder);
            debug!("Buffer zone now have data {}", guards.len());
        }
        return self.get(id);
    }

    pub fn get_for_mut(&self, id: &Id) -> RcNodeRefMut {
        debug!("Getting cached for mutation {:?}", id);
        {
            let data = self.data.borrow();
            match data.get(id) {
                Some(Some(ref extnode)) => {
                    let guards = self.guards.borrow();
                    if let CacheGuardHolder::Read(_) = guards.get(id).unwrap() {
                        panic!("Mutating readonly buffered cache");
                    }
                    let cell = extnode.clone();
                    let cell_ref = RcRef::new(cell);
                    let handle = OwningHandle::new_mut(cell_ref);
                    debug!("Got cached, len {}, cache data {}, cache guards {}", handle.len, data.len(), guards.len());
                    return handle
                },
                Some(None) => panic!(),
                _ => {}
            }
        }
        {
            debug!("Buffer zone inserting for mutation {:?}", id);
            let guard = self.get_mut_ext_node_cached(id);
            let mut data = self.data.borrow_mut();
            data.insert(*id, Some(Rc::new(RefCell::new((&*guard).clone()))));
            let holder = CacheGuardHolder::Write(guard);
            let mut guards = self.guards.borrow_mut();
            guards.insert(*id, holder);
            debug!("Buffer zone now have data {}, guards {}", data.len(), guards.len());
        }
        self.get_for_mut(id)
    }

    pub fn delete(&self, id: &Id) {
        let mut data = self.data.borrow_mut();
        data.insert(*id, None);
    }

    pub fn new_node(&self, node: ExtNode) {
        let mut mapper = self.mapper.lock();
        if mapper.get(&node.id).is_some() {
            panic!()
        } else {
            let node_id = node.id;
            let lock = RwLock::new(ExtNode::new(&node_id));
            let guard = CacheGuardHolder::Write(lock.write());
            let mut guards = self.guards.borrow_mut();
            let mut data = self.data.borrow_mut();
            debug!("new node in buffered cache: {:?}, was guards: {}, data: {}", node_id, guards.len(), data.len());
            mapper.insert(node_id, lock);
            guards.insert(node_id, guard);
            data.insert(node_id, Some(Rc::new(RefCell::new(node))));
        }
    }

    pub fn flush(&self) {
        let mut guards = self.guards.borrow_mut();
        let data = self.data.borrow();
        debug!("Flushing cache buffer, guards: {:?}, data: {:?}", guards.keys(), data.keys());
        for ((id, data),  (gid, mut holder)) in data.iter().zip(guards.iter_mut()) {
            assert_eq!(id, gid);
            if let Some(ref node_rc) = data {
                debug!("Flushing updating node: {:?}", id);
                if let &mut CacheGuardHolder::Write(ref mut guard) = holder {
                    let mut ext_node = (*(*node_rc).borrow()).clone();
                    debug!("flushing {:?} to cache with {} keys", ext_node.id, ext_node.len);
                    **guard = ext_node
                }
            } else {
                debug!("Flushing deleting node: {:?}", id);
                self.mapper.lock().remove(id);
                self.storage.remove_cell(*id).wait().unwrap();
            }
        }
    }

    pub fn get_guard(&self, id: &Id) -> Option<CacheGuardHolder> {
        if id.is_unit_id() {
            return None;
        }
        let mut guards = self.guards.borrow_mut();
        {
            let res = guards.get(id).map(|rg| rg.clone());
            if res.is_some() { return res; }
        }
        let guard = self.get_ext_node_cached(id);
        let holder = CacheGuardHolder::Read(guard);
        guards.insert(*id, holder.clone());
        return Some(holder)
    }

    pub fn get_mut_ext_node_cached(&self, id: &Id) -> ExtNodeCachedMut {
        let mut map = self.mapper.lock();
        return map.get_or_fetch(id).unwrap().write();
    }
    pub fn get_ext_node_cached(&self, id: &Id) -> ExtNodeCachedImmute {
        let mut map = self.mapper.lock();
        return map.get_or_fetch(id).unwrap().read();
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
            "*", 0, false, false,
            Some(vec![
                Field::new(NEXT_FIELD, type_id_of(Type::Id), false, false, None),
                Field::new(PREV_FIELD, type_id_of(Type::Id), false, false, None),
                Field::new(KEYS_FIELD, type_id_of(Type::U8), false, true, None)
            ])
        )
    }
}
