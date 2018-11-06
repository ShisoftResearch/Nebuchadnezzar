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
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;
use std::rc::Rc;
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

#[derive(Clone)]
pub struct ExtNode {
    pub id: Id,
    pub keys: EntryKeySlice,
    pub next: Id,
    pub prev: Id,
    pub len: usize,
    pub version: u64,
    pub dirty: bool,
}

pub struct ExtNodeSplit {
    pub node_2: ExtNode,
    pub keys_1_len: usize,
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
            dirty: false,
        }
    }
    pub fn from_cell(cell: Cell) -> Self {
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
            next: *next,
            prev: *prev,
            len: key_count,
            version: cell_version,
            dirty: false,
        }
    }
    pub fn to_cell(&self) -> Cell {
        let mut value = Value::Map(Map::new());
        value[*NEXT_PAGE_KEY_HASH] = Value::Id(self.next);
        value[*PREV_PAGE_KEY_HASH] = Value::Id(self.prev);
        value[*KEYS_KEY_HASH] = self.keys[..self.len]
            .iter()
            .map(|key| SmallBytes::from_vec(key.as_slice().to_vec()))
            .collect_vec()
            .value();
        Cell::new_with_id(*PAGE_SCHEMA_ID, &self.id, value)
    }
    pub fn remove_at(&mut self, pos: usize) {
        let mut cached_len = self.len;
        debug!("Removing from external pos {}, len {}", pos, cached_len);
        self.keys.remove_at(pos, &mut cached_len);
        self.len = cached_len;
    }
    pub fn insert(
        &mut self,
        key: EntryKey,
        pos: usize,
        tree: &BPlusTree,
        bz: &CacheBufferZone,
    ) -> Option<(Node, Option<EntryKey>)> {
        let mut cached = self;
        let cached_len = cached.len;
        debug_assert!(cached_len <= NUM_KEYS);
        if cached_len == NUM_KEYS {
            // need to split
            debug!("insert to external with split, key {:?}, pos {}", &key, pos);
            // cached.dump();
            let pivot = cached_len / 2;
            let cached_next = *&cached.next;
            let cached_id = *&cached.id;
            let new_page_id = tree.new_page_id();
            let mut keys_1 = &mut cached.keys;
            let mut keys_2 = keys_1.split_at_pivot(pivot, cached_len);
            let mut keys_1_len = pivot;
            let mut keys_2_len = cached_len - pivot;
            // modify next node point previous to new node
            if !cached_next.is_unit_id() {
                let mut prev_node = bz.get_for_mut(&cached_next);
                prev_node.prev = new_page_id;
            }
            insert_into_split(
                key,
                keys_1,
                &mut keys_2,
                &mut keys_1_len,
                &mut keys_2_len,
                pos,
                pivot,
            );
            let extnode_2 = ExtNode {
                id: new_page_id,
                keys: keys_2,
                next: cached_next,
                prev: cached_id,
                len: keys_2_len,
                version: 0,
                dirty: true,
            };
            debug!(
                "new node have next {:?} prev {:?}, current id {:?}",
                extnode_2.next, extnode_2.prev, cached.id
            );
            cached.next = new_page_id;
            cached.len = keys_1_len;
            let node_2 = Node::External(box new_page_id);
            debug!(
                "Split to left len {}, right len {}",
                cached.len, extnode_2.len
            );
            bz.new_node(extnode_2);
            return Some((node_2, None));
        } else {
            debug!("insert to external without split at {}, key {:?}", pos, key);
            let mut new_cached_len = cached_len;
            cached.keys.insert_at(key, pos, &mut new_cached_len);
            cached.len = new_cached_len;
            return None;
        }
    }
    pub fn merge_with(&mut self, right: &mut Self) {
        debug!(
            "Merge external node, left len {}, right len {}",
            self.len, right.len
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
    pub fn remove_node(&self, bz: &CacheBufferZone) {
        let id = &self.id;
        let prev = &self.prev;
        let next = &self.next;
        debug_assert_ne!(id, &Id::unit_id());
        if !prev.is_unit_id() {
            let mut prev_node = bz.get_for_mut(&prev);
            prev_node.next = *next;
        }
        if !next.is_unit_id() {
            let mut next_node = bz.get_for_mut(&next);
            next_node.prev = *prev;
        }
        debug!("Removing node {:?}, len {}", id, self.len);
        bz.delete(id);
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
    Write(ExtNodeCachedMut),
}

type NodeRcRefCell = Rc<RefCell<ExtNode>>;

pub type RcNodeRefData<'a> = OwningHandle<RcRef<RefCell<ExtNode>>, Ref<'a, ExtNode>>;
pub type RcNodeRefMut<'a> = OwningHandle<RcRef<RefCell<ExtNode>>, RefMut<'a, ExtNode>>;

enum CachedData {
    Some(NodeRcRefCell),
    None,
    Deleted,
}

pub enum RcNodeRef<'a> {
    Data(RcNodeRefData<'a>),
    Guard(CacheGuardHolder),
}

pub struct CacheBufferZone {
    mapper: Arc<ExtNodeCacheMap>,
    storage: Arc<AsyncClient>,
    data: RefCell<HashMap<Id, (CacheGuardHolder, CachedData)>>,
}

impl Deref for CacheGuardHolder {
    type Target = ExtNode;

    fn deref(&self) -> &'_ <Self as Deref>::Target {
        match self {
            &CacheGuardHolder::Read(ref g) => &*g,
            &CacheGuardHolder::Write(ref g) => &*g,
        }
    }
}

impl<'a> Deref for RcNodeRef<'a> {
    type Target = ExtNode;

    fn deref(&self) -> &'_ <Self as Deref>::Target {
        match self {
            &RcNodeRef::Guard(ref g) => &*g,
            &RcNodeRef::Data(ref g) => &*g,
        }
    }
}

impl CacheBufferZone {
    pub fn new(mapper: &Arc<ExtNodeCacheMap>, storage: &Arc<AsyncClient>) -> CacheBufferZone {
        CacheBufferZone {
            mapper: mapper.clone(),
            storage: storage.clone(),
            data: RefCell::new(HashMap::new()),
        }
    }

    pub fn get(&self, id: &Id) -> RcNodeRef {
        debug!("Getting cached fo readonly {:?}", id);
        {
            // read from data (from write)
            let data = self.data.borrow();
            match data.get(id) {
                Some((guard, CachedData::Some(data))) => {
                    let cell = data.clone();
                    let cell_ref = RcRef::new(cell);
                    let handle = OwningHandle::new(cell_ref);
                    return RcNodeRef::Data(handle);
                }
                Some((guard, CachedData::None)) => {
                    // read from guards
                    return RcNodeRef::Guard(guard.clone());
                }
                _ => {}
            }
        }
        {
            debug!("Buffer zone inserting for readonly");
            let guard = self.get_ext_node_cached(id);
            let holder = CacheGuardHolder::Read(guard);
            let mut data = self.data.borrow_mut();
            // data placeholder for aliened flush. None will be ignored in both get and get mut
            data.insert(*id, (holder, CachedData::None));
        }
        return self.get(id);
    }

    pub fn get_for_mut(&self, id: &Id) -> RcNodeRefMut {
        debug!("Getting cached for mutation {:?}", id);
        {
            let data = self.data.borrow();
            match data.get(id) {
                Some((CacheGuardHolder::Write(_), CachedData::Some(ref extnode))) => {
                    let cell = extnode.clone();
                    let cell_ref = RcRef::new(cell);
                    let handle = OwningHandle::new_mut(cell_ref);
                    return handle;
                }
                Some((CacheGuardHolder::Read(_), _)) => panic!("mutating readonly buffered cache"),
                Some((CacheGuardHolder::Write(_), _)) => panic!("item not available"),
                _ => {}
            }
        }
        {
            debug!("Buffer zone inserting for mutation {:?}", id);
            let mut guard = self.get_mut_ext_node_cached(id);
            guard.dirty = true;
            let mut data_map = self.data.borrow_mut();
            let data = RefCell::new((&*guard).clone());
            let holder = CacheGuardHolder::Write(guard);
            data_map.insert(*id, (holder, CachedData::Some(Rc::new(data))));
        }
        self.get_for_mut(id)
    }

    pub fn delete(&self, id: &Id) {
        let mut data_map = self.data.borrow_mut();
        debug!(
            "remove {:?} from data map with keys {:?}",
            id,
            data_map.keys()
        );
        debug_assert!(data_map.contains_key(id), "cannot found {:?}", id);
        match data_map.get_mut(id) {
            Some((CacheGuardHolder::Write(_), cache)) => *cache = CachedData::Deleted,
            Some((CacheGuardHolder::Read(_), _)) => panic!("data readonly {:?}", id),
            None => panic!("data not loaded {:?}", id),
        }
    }

    pub fn new_node(&self, node: ExtNode) {
        let mut mapper = self.mapper.lock();
        if mapper.get(&node.id).is_some() {
            panic!()
        } else {
            let node_id = node.id;
            // a placeholder guard
            let lock = RwLock::new(ExtNode::new(&node_id));
            let guard = CacheGuardHolder::Write(lock.write());
            let mut data = self.data.borrow_mut();
            mapper.insert(node_id, Arc::new(lock));
            data.insert(
                node_id,
                (guard, CachedData::Some(Rc::new(RefCell::new(node)))),
            );
        }
    }

    pub fn flush(&self) {
        let mut data_map = self.data.borrow_mut();
        for ((id, (holder, data))) in data_map.iter_mut() {
            if let &mut CacheGuardHolder::Write(ref mut guard) = holder {
                debug!("Flushing updating node: {:?}", id);
                match data {
                    CachedData::Some(ref node_rc) => {
                        let mut ext_node = (*(*node_rc).borrow()).clone();
                        debug!(
                            "flushing {:?} to cache with {} keys",
                            ext_node.id, ext_node.len
                        );
                        **guard = ext_node
                    }
                    CachedData::Deleted => {
                        debug!("Flushing deleting node: {:?}", id);
                        self.mapper.lock().remove(id);
                        wait(self.storage.remove_cell(*id)).unwrap();
                    }
                    _ => panic!(),
                }
            }
        }
    }

    pub fn get_guard(&self, id: &Id) -> Option<CacheGuardHolder> {
        if id.is_unit_id() {
            return None;
        }
        let mut data_map = self.data.borrow_mut();
        {
            let res = data_map.get(id).map(|(holder, _)| holder.clone());
            if res.is_some() {
                return res;
            }
        }
        let guard = self.get_ext_node_cached(id);
        let holder = CacheGuardHolder::Read(guard);
        data_map.insert(*id, (holder.clone(), CachedData::None)); // act as a read operation
        return Some(holder);
    }

    fn get_ext_node_lock(&self, id: &Id) -> Arc<RwLock<ExtNode>> {
        self.mapper.lock().get_or_fetch(id).unwrap().clone()
    }

    pub fn get_mut_ext_node_cached(&self, id: &Id) -> ExtNodeCachedMut {
        self.get_ext_node_lock(id).write()
    }
    pub fn get_ext_node_cached(&self, id: &Id) -> ExtNodeCachedImmute {
        self.get_ext_node_lock(id).read()
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
