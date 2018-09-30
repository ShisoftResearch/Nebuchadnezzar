use smallvec::SmallVec;
use dovahkiin::types::custom_types::id::Id;
use utils::lru_cache::LRUCache;
use std::io::Cursor;
use std::rc::Rc;
use ram::types::RandValue;
use client::AsyncClient;
use std::cmp::{max, min};
use std::ptr;
use std::mem;
use std::sync::Arc;
use itertools::{Itertools, chain};
use ram::cell::Cell;
use futures::Future;
use dovahkiin::types;
use dovahkiin::types::{Value, Map, PrimitiveArray, ToValue, key_hash};
use index::btree::external::*;
use index::btree::internal::*;
use bifrost::utils::async_locks::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use hermes::stm::{TxnManager, TxnValRef, Txn, TxnErr};
use bifrost::utils::async_locks::Mutex;
use std::cell::RefMut;
use std::cell::Ref;
use std::fmt::Debug;
use std::ops::Range;
use std::fmt::Formatter;
use std::fmt::Error;

mod internal;
mod external;

const ID_SIZE: usize = 16;
const NUM_KEYS: usize = 32;
const NUM_PTRS: usize = NUM_KEYS + 1;
const CACHE_SIZE: usize = 2048;

type EntryKey = SmallVec<[u8; 32]>;
type EntryKeySlice = [EntryKey; NUM_KEYS];
type NodePointerSlice = [TxnValRef; NUM_PTRS];

#[derive(Clone)]
enum Node {
    External(Box<Id>),
    Internal(Box<InNode>),
    None
}

// This is the runtime cursor on iteration
// It hold a copy of the containing page next page lock guard
// These lock guards are preventing the node and their neighbourhoods been changed externally
// Ordering are specified that can also change lock pattern
pub struct RTCursor {
    index: usize,
    ordering: Ordering,
    page: CacheGuardHolder,
    next_page: Option<CacheGuardHolder>,
    bz: Rc<CacheBufferZone>
}

pub struct BPlusTree {
    root: TxnValRef,
    num_nodes: usize,
    height: usize,
    ext_node_cache: Arc<ExtNodeCacheMap>,
    stm: TxnManager,
    storage: Arc<AsyncClient>
}

#[derive(Debug, Clone, Copy)]
pub enum Ordering {
    Forward, Backward
}

pub struct TreeTxn<'a> {
    tree: &'a BPlusTree,
    bz: Rc<CacheBufferZone>,
    txn: &'a mut Txn
}

macro_rules! make_array {
    ($n: expr, $constructor:expr) => {
        unsafe {
            let mut items: [_; $n] = mem::uninitialized();
            for place in items.iter_mut() {
                ptr::write(place, $constructor);
            }
            items
        }
    };
}

impl BPlusTree {
    pub fn new(neb_client: &Arc<AsyncClient>) -> BPlusTree {
        let neb_client_1 = neb_client.clone();
        let neb_client_2 = neb_client.clone();
        let mut tree = BPlusTree {
            root: Default::default(),
            num_nodes: 0,
            height: 0,
            stm: TxnManager::new(),
            storage: neb_client.clone(),
            ext_node_cache:
            Arc::new(Mutex::new(
                LRUCache::new(
                    CACHE_SIZE,
                    move |id|{
                        debug!("Reading page to cache {:?}", id);
                        neb_client_1.read_cell(*id).wait().unwrap().map(|cell| {
                            RwLock::new(ExtNode::from_cell(cell))
                        }).ok()
                    },
                    move |id, value| {
                        debug!("Flush page to cache {:?}", &id);
                        let cache = value.read();
                        let cell = cache.to_cell();
                        neb_client_2.upsert_cell(cell).wait().unwrap();
                    })))
        };
        {
            let actual_tree_root = tree.new_ext_cached_node();
            let root_ref = tree.stm.with_value(actual_tree_root);
            tree.root = root_ref;
        }
        return tree;
    }
    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> Result<RTCursor, TxnErr> {
        debug!("searching for {:?}", key);
        self.transaction(|txn| txn.seek(key, ordering))
    }

    pub fn insert(&self, key: &EntryKey, id: &Id) -> Result<(), TxnErr> {
        debug!("inserting {:?}", &key);
        self.transaction(|txn| txn.insert(key, id))
    }

    fn remove(&self, key: &EntryKey, id: &Id) -> Result<bool, TxnErr> {
        debug!("removing {:?}", &key);
        self.transaction(|txn| txn.remove(key, id))
    }

    pub fn transaction<R, F>(&self, func: F) -> Result<R, TxnErr>
        where F: Fn(&mut TreeTxn) -> Result<R, TxnErr>
    {
        self.stm.transaction(|txn| {
            let mut t_txn = TreeTxn::new(self, txn);
            let res = func(&mut t_txn)?;
            Ok((res, t_txn.bz))
        }).map(|(res, mut bz)| {
            bz.flush();
            res
        })
    }
    fn new_page_id(&self) -> Id {
        // TODO: achieve locality
        Id::rand()
    }
    fn new_ext_cached_node(&self) -> Node {
        let id = self.new_page_id();
        let node = ExtNode::new(&id);
        self.ext_node_cache.lock().insert(id, RwLock::new(node));
        debug!("New page id is: {:?}", id);
        return Node::External(box id);
    }
}

impl Node {
    fn search(&self, key: &EntryKey, bz: &CacheBufferZone) -> usize {
        let len = self.len(bz);
        if self.is_ext() {
            self.extnode(bz).keys[..len].binary_search(key).unwrap_or_else(|i| i)
        } else {
            self.innode().keys[..len].binary_search(key).unwrap_or_else(|i| i)
        }
    }
    fn warm_cache_for_write_intention(&self, bz: &CacheBufferZone) {
        if self.is_ext() { self.extnode_mut(bz); }
    }
    fn insert(
        &mut self,
        key: EntryKey,
        ptr: Option<TxnValRef>,
        pos: usize,
        tree: &BPlusTree,
        bz: &CacheBufferZone) -> Option<(Node, Option<EntryKey>)>
    {
        if let &mut Node::External(ref id) = self {
            debug!("inserting to external node at: {:?}, key {:?}", pos, key);
            self.extnode_mut(bz).insert(key, pos, tree, bz)
        } else {
            debug!("inserting to internal node at: {:?}, key {:?}", pos, key);
            self.innode_mut().insert(key, ptr, pos)
        }
    }
    fn remove(&mut self, pos: usize, bz: &CacheBufferZone) {
        if let &mut Node::External(ref id) = self {
            self.extnode_mut(bz).remove(pos)
        } else {
            self.innode_mut().remove(pos)
        }
    }
    fn is_ext(&self) -> bool {
        match self {
            &Node::External(_) => true,
            &Node::Internal(_) => false,
            &Node::None => panic!()
        }
    }
    fn first_key(&self, bz: &CacheBufferZone) -> EntryKey {
        if self.is_ext() {
            self.extnode(bz).keys[0].to_owned()
        } else {
            self.innode().keys[0].to_owned()
        }
    }
    fn len(&self, bz: &CacheBufferZone) -> usize {
        if self.is_ext() {
            self.extnode(bz).len
        } else {
            self.innode().len
        }
    }
    fn is_half_full(&self, bz: &CacheBufferZone) -> bool {
        self.len(bz) >= NUM_KEYS / 2
    }
    fn cannot_merge(&self, bz: &CacheBufferZone) -> bool {
        self.len(bz) >= NUM_KEYS/ 2 - 1
    }
    fn extnode_mut<'a>(&self, bz: &'a CacheBufferZone) -> RcNodeRefMut<'a> {
        match self {
            &Node::External(ref id) => bz.get_for_mut(id),
            _ => unreachable!()
        }
    }
    fn innode_mut(&mut self) -> &mut InNode {
        match self {
            &mut Node::Internal(ref mut n) => n,
            _ => unreachable!()
        }
    }
    fn extnode<'a>(&self, bz: &'a CacheBufferZone) -> RcNodeRef<'a> {
        match self {
            &Node::External(ref id) => bz.get(id),
            _ => unreachable!()
        }
    }
    fn ext_id(&self) -> Id { if let &Node::External(ref id) = self { **id } else { panic!() } }
    fn innode(&self) -> &InNode {
        match self {
            &Node::Internal(ref n) => n,
            _ => unreachable!()
        }
    }
}

impl <'a> TreeTxn<'a> {
    fn new(tree: &'a BPlusTree, txn: &'a mut Txn) -> TreeTxn<'a> {
        TreeTxn {
            bz: Rc::new(CacheBufferZone::new(&tree.ext_node_cache, &tree.storage)), tree, txn
        }
    }

    pub fn seek(&mut self, key: &EntryKey, ordering: Ordering) -> Result<RTCursor, TxnErr> {
//        let mut key = key.clone();
//        key_with_id(&mut key, &Id::unit_id());
        let root = self.tree.root;
        self.search(root, &key, ordering)
    }

    fn search(
        &mut self,
        node: TxnValRef,
        key: &EntryKey,
        ordering: Ordering) -> Result<RTCursor, TxnErr>
    {
        debug!("searching for {:?} at {:?}", key, node);
        let node = self.txn.read::<Node>(node)?.unwrap();
        let pos = node.search(key, &mut self.bz);
        if node.is_ext() {
            debug!("search in external for {:?}", key);
            Ok(RTCursor::new(pos, &node.ext_id(), &self.bz, ordering))
        } else if let Node::Internal(ref n) = *node {
            let next_node_ref = n.pointers[pos];
            self.search(next_node_ref, key, ordering)
        } else {
            unreachable!()
        }
    }
    fn insert(&mut self, key: &EntryKey, id: &Id) -> Result<(), TxnErr> {
        let mut key = key.clone();
        key_with_id(&mut key, id);
        let root_ref = self.tree.root;
        if let Some((new_node, pivotKey)) = self.insert_to_node(root_ref, key.clone())? {
            debug!("split root with pivot key {:?}", pivotKey);
            let first_key = new_node.first_key(&mut self.bz);
            let new_node_ref = self.txn.new_value(new_node);
            let pivot = pivotKey.unwrap_or_else(|| first_key);
            let mut new_in_root = InNode {
                keys: make_array!(NUM_KEYS, Default::default()),
                pointers: make_array!(NUM_PTRS, Default::default()),
                len: 1
            };
            let old_root = self.txn.read_owned::<Node>(self.tree.root)?.unwrap();
            let old_root_ref = self.txn.new_value(old_root);
            new_in_root.keys[0] = pivot;
            new_in_root.pointers[0] = old_root_ref;
            new_in_root.pointers[1] = new_node_ref;
            let new_root = Node::Internal(box new_in_root);
            self.txn.update(self.tree.root, new_root);
        }
        Ok(())
    }
    fn insert_to_node(
        &mut self,
        node: TxnValRef,
        key: EntryKey
    ) -> Result<Option<(Node, Option<EntryKey>)>, TxnErr> {
        let ref_node = self.txn.read::<Node>(node)?.unwrap();
        ref_node.warm_cache_for_write_intention(&mut self.bz);
        let pos = ref_node.search(&key, &mut self.bz);
        debug!("insert to node: {:?}, len {}, pos: {}, external: {}",
               node, ref_node.len(&mut self.bz), pos, ref_node.is_ext());
        let split_node = match &*ref_node {
            &Node::External(ref id) => {
                debug!("insert into external at {}, key {:?}, id {:?}", pos, key, id);
                let mut node = self.bz.get_for_mut(id);
                return Ok(node.insert(key, pos, self.tree, &*self.bz));
            },
            &Node::Internal(ref n) => {
                let next_node_ref = n.pointers[pos];
                debug!("insert into internal at {}, has keys {}, ref {:?}", pos, n.len, next_node_ref);
                self.insert_to_node(next_node_ref, key)?
            },
            &Node::None => unreachable!()
        };
        match split_node {
            Some((new_node, pivot_key)) => {
                debug!("Sub level node split, shall insert new node to current level, pivot {:?}, current keys: ", pivot_key);
                assert!(!(!new_node.is_ext() && pivot_key.is_none()));
                let pivot = pivot_key.unwrap_or_else(|| {
                    let first_key = new_node.first_key(&mut self.bz);
                    assert_ne!(first_key.len(), 0);
                    first_key
                });
                let new_node_ref = self.txn.new_value(new_node);
                debug!("New node in transaction {:?}, pivot {:?}", new_node_ref, pivot);
                let mut acq_node = self.txn.read_owned::<Node>(node)?.unwrap();
                assert!(!acq_node.is_ext());
                let pivot_pos = acq_node.search(&pivot, &mut self.bz);
                let result = acq_node.insert(
                    pivot,
                    Some(new_node_ref),
                    pivot_pos,
                    self.tree, &mut self.bz);
                self.txn.update(node, acq_node);
                if result.is_some() { debug!("Sub level split caused current level split"); }
                return Ok(result);
            },
            None => return Ok(None)
        }
    }

    fn remove(&mut self, key: &EntryKey, id: &Id) -> Result<bool, TxnErr> {
        let mut key = key.clone();
        key_with_id(&mut key, id);
        let root = self.tree.root;
        let removed = self.remove_from_node(root, &key)?.is_some();
        let root_node = self.txn.read::<Node>(root)?.unwrap();
        if removed && !root_node.is_ext() && root_node.len(&mut self.bz) == 0 {
            self.txn.update(root, self.tree.new_ext_cached_node());
        }
        Ok(removed)
    }

    fn remove_from_node(
        &mut self,
        node: TxnValRef,
        key: &EntryKey
    ) -> Result<Option<()>, TxnErr> {
        let node_ref = self.txn.read::<Node>(node)?.unwrap();
        node_ref.warm_cache_for_write_intention(&mut self.bz);
        let key_pos = node_ref.search(key, &mut self.bz);
        if let Node::Internal(n) = &*node_ref {
            let pointer_pos = key_pos + 1;
            let result = self.remove_from_node(n.pointers[pointer_pos], key)?;
            if result.is_none() { return Ok(result) }
            let sub_node = n.pointers[pointer_pos];
            let sub_node_ref = self.txn.read::<Node>(sub_node)?.unwrap();
            let mut node_owned = self.txn.read_owned::<Node>(node)?.unwrap();
            {
                let mut n = node_owned.innode_mut();
                if sub_node_ref.len(&mut self.bz) == 0 {
                    // need to remove empty child node
                    if sub_node_ref.is_ext() {
                        // empty external node should be removed and rearrange 'next' and 'prev' pointer for neighbourhoods
                        let (prev, next, nid) = {
                            let n = sub_node_ref.extnode(&mut self.bz);
                            (n.prev, n.next, n.id)
                        };
                        if !prev.is_unit_id() {
                            let mut prev_node = self.bz.get_for_mut(&prev);
                            prev_node.next = next;
                        }
                        if !next.is_unit_id() {
                            let mut next_node = self.bz.get_for_mut(&next);
                            next_node.prev = prev;
                        }
                        n.remove(pointer_pos);
                        self.bz.delete(&nid)
                    } else {
                        // empty internal nodes should be replaced with it's only remaining child pointer
                        // there must be at least one child pointer exists
                        n.pointers[pointer_pos] = sub_node_ref.innode().pointers[0];
                    }
                    self.txn.delete(sub_node);
                } else if !sub_node_ref.is_half_full(&mut self.bz) {
                    // need to rebalance
                    let cand_key_pos = n.rebalance_candidate(key_pos, &mut self.txn, &mut self.bz)?;
                    let cand_ptr_pos = cand_key_pos + 1;
                    let left_ptr_pos = min(pointer_pos, cand_ptr_pos);
                    let right_ptr_pos = max(pointer_pos, cand_ptr_pos);
                    if sub_node_ref.cannot_merge(&mut self.bz) {
                        // relocate
                        n.relocate_children(left_ptr_pos, right_ptr_pos, &mut self.txn, &mut self.bz)?;
                    } else {
                        // merge
                        n.merge_children(left_ptr_pos, right_ptr_pos, &mut self.txn, &mut self.bz);
                        n.remove(right_ptr_pos - 1);
                    }
                }
            }
            self.txn.update(node, node_owned);
            return Ok(result);
        } else if let &Node::External(ref id) = &*node_ref {
            let mut cached_node = self.bz.get_for_mut(id);
            if &cached_node.keys[key_pos] == key {
                cached_node.remove(key_pos);
                return Ok(Some(()));
            } else {
                return Ok(None);
            }
        } else { unreachable!() }
    }
}

impl RTCursor {
    fn new(
        pos: usize,
        id: &Id,
        bz: &Rc<CacheBufferZone>,
        ordering: Ordering,
    ) -> RTCursor {
        let node = bz.get(id);
        let page = bz.get_guard(id).unwrap();
        let key = &node.keys[pos];
        let next = page.next;
        let prev = page.prev;
        debug!("Cursor page len: {}, id {:?}", &node.len, node.id);
        debug!("Key at pos {} is {:?}, next: {:?}, prev: {:?}", pos, &key, next, prev);
        RTCursor {
            index: pos,
            ordering,
            page,
            bz: bz.clone(),
            next_page: bz.get_guard(match ordering {
                Ordering::Forward => &next,
                Ordering::Backward  => &prev
            })
        }
    }
    pub fn next(&mut self) -> bool {
        debug!("Next id with index: {}, length: {}", self.index + 1, self.page.len);
        match self.ordering {
            Ordering::Forward => {
                if self.index + 1 >= self.page.len {
                    // next page
                    if let Some(next_page) = self.next_page.clone() {
                        debug!("Shifting page forward");
                        self.next_page = self.bz.get_guard(&next_page.next);
                        self.index = 0;
                        self.page = next_page;
                    } else {
                        debug!("iterated to end");
                        return false;
                    }
                } else {
                    self.index += 1;
                    debug!("Advancing cursor to index {}", self.index);
                }
            },
            Ordering::Backward => {
                if self.index <= 0 {
                    // next page
                    if let Some(next_page) = self.next_page.clone() {
                        debug!("Shifting page backward");
                        self.next_page = self.bz.get_guard(&next_page.prev);
                        self.index = next_page.len - 1;
                        self.page = next_page;
                    } else {
                        debug!("iterated to end");
                        return false
                    }
                } else {
                    self.index -= 1;
                    debug!("Advancing cursor to index {}", self.index);
                }
            }
        }
        return true;
    }

    pub fn current(&self) -> Id {
        id_from_key(&self.page.keys[self.index])
    }

    pub fn has_next(&self) -> bool {
        match self.ordering {
            Ordering::Forward => self.index < self.page.len - 1 || self.next_page.is_some(),
            Ordering::Backward => self.index > 0 || self.next_page.is_some()
        }
    }
}

fn id_from_key(key: &EntryKey) -> Id {
    debug!("Decoding key to id {:?}", key);
    let mut id_cursor = Cursor::new(&key[key.len() - ID_SIZE ..]);
    return Id::from_binary(&mut id_cursor).unwrap(); // read id from tailing 128 bits
}

fn key_prefixed(prefix: &EntryKey, x: &EntryKey) -> bool {
    return prefix.as_slice() == &x[.. x.len() - ID_SIZE];
}

fn insert_into_split<T, S>(
    item: T,
    x: &mut S, y: &mut S,
    xlen: &mut usize, ylen: &mut usize,
    pos: usize, pivot: usize
)
    where S: Slice<T>, T: Default + Debug
{
    debug!("insert into split left len {}, right len {}, pos {}, pivot {}", xlen, ylen, pos, pivot);
    if pos < pivot {
        x.insert_at(item, pos, xlen);
    } else {
        y.insert_at(item, pos - pivot, ylen);
    }
}

fn key_with_id(key: &mut EntryKey, id: &Id) {
    let id_bytes = id.to_binary();
    key.extend_from_slice(&id_bytes);
}

trait Slice<T> : Sized where T: Default + Debug {
    fn as_slice(&mut self) -> &mut [T];
    fn init() -> Self;
    fn item_default() -> T {
        T::default()
    }
    fn split_at_pivot(&mut self, pivot: usize, len: usize) -> Self {
        let mut right_slice = Self::init();
        {
            let mut slice1: &mut[T] = self.as_slice();
            let mut slice2: &mut[T] = right_slice.as_slice();
            for i in pivot .. len { // leave pivot to the right slice
                let right_pos = i - pivot;
                let item = mem::replace(&mut slice1[i], T::default());
                debug!("Moving from left pos {} to right {}, item {:?} for split", i, right_pos, &item);
                slice2[right_pos] = item;
            }
        }
        return right_slice;
    }
    fn insert_at(&mut self, item: T, pos: usize, len: &mut usize) {
        assert!(pos <= *len, "pos {:?} larger or equals to len {:?}, item: {:?}", pos, len, item);
        debug!("insert into slice, pos: {}, len {}, item {:?}", pos, len, item);
        let mut slice = self.as_slice();
        if *len > 0 {
            slice[*len] = T::default();
            for i in (pos ..= *len - 1).rev() {
                debug!("swap {} with {} for insertion", i, i + 1);
                slice.swap(i, i + 1);
            }
        }
        debug!("setting item {:?} at {} for insertion", item, pos);
        *len += 1;
        slice[pos] = item;
    }
    fn remove_at(&mut self, pos: usize, len: usize) {
        if pos >= len - 1 { return; }
        let slice  = self.as_slice();
        for i in pos .. len - 1 {
            slice[i] = mem::replace(&mut slice[i + 1], T::default());
        }
    }
}
macro_rules! impl_slice_ops {
    ($t: ty, $et: ty, $n: expr) => {
        impl Slice<$et> for $t {
            fn as_slice(&mut self) -> &mut [$et] { self }
            fn init() -> Self { make_array!($n, Self::item_default()) }
        }
    };
}

impl_slice_ops!(EntryKeySlice, EntryKey, NUM_KEYS);
impl_slice_ops!(NodePointerSlice, TxnValRef, NUM_PTRS);

impl Default for Node {
    fn default() -> Self {
        Node::None
    }
}


#[cfg(test)]
mod test {
    use std::mem::size_of;
    use super::{Node, BPlusTree, Ordering};
    use server::ServerOptions;
    use server;
    use std::sync::Arc;
    use client;
    use server::NebServer;
    use dovahkiin::types::custom_types::id::Id;
    use ram::types::RandValue;
    use index::btree::NUM_KEYS;
    use futures::future::Future;
    use std::io::Cursor;
    use byteorder::BigEndian;
    use smallvec::SmallVec;
    use byteorder::WriteBytesExt;
    use hermes::stm::TxnValRef;
    use index::btree::external::CacheBufferZone;
    use std::fs::File;
    use std::io::Write;
    use index::btree::id_from_key;

    extern crate env_logger;
    extern crate serde_json;

    #[derive(Serialize, Deserialize)]
    struct DebugNode {
        keys: Vec<String>,
        nodes: Vec<DebugNode>,
        id: Option<String>,
        next: Option<String>,
        prev: Option<String>,
        is_external: bool
    }

    fn dump_tree(tree: &BPlusTree) {
        let mut bz = CacheBufferZone::new(&tree.ext_node_cache, &tree.storage);
        let debug_root = cascading_dump_node(tree, tree.root, &mut bz);
        let json = serde_json::to_string_pretty(&debug_root).unwrap();
        let mut file = File::create("tree_dump.json").unwrap();
        file.write_all(json.as_bytes());
    }

    fn cascading_dump_node(tree: &BPlusTree, node: TxnValRef, bz: &mut CacheBufferZone) -> DebugNode {
        let node = tree.stm.transaction(|txn| {
           txn.read_owned::<Node>(node).map(|opt| opt.unwrap())
        }).unwrap();
        match node {
            Node::External(id) => {
                let node = bz.get(&*id);
                let keys = node.keys
                    .iter()
                    .take(node.len)
                    .map(|key| {
                        let id = id_from_key(key);
                        format!("{}\t{:?}", id.lower, key)
                    })
                    .collect();
                return DebugNode {
                    keys,
                    nodes: vec![],
                    id: Some(format!("{:?}", node.id)),
                    next: Some(format!("{:?}", node.next)),
                    prev: Some(format!("{:?}", node.prev)),
                    is_external: true
                }
            },
            Node::Internal(innode) => {
                let len = innode.len;
                let nodes = innode.pointers
                    .iter()
                    .take(len + 1)
                    .map(|node_ref| {cascading_dump_node(tree, *node_ref, bz)})
                    .collect();
                let keys = innode.keys
                    .iter()
                    .take(len )
                    .map(|key| format!("{:?}", key))
                    .collect();
                return DebugNode {
                    keys,
                    nodes,
                    id: None,
                    next: None,
                    prev: None,
                    is_external: false
                }
            },
            Node::None => panic!()
        }
    }

    #[test]
    fn node_size() {
        // expecting the node size to be an on-heap pointer plus node type tag, aligned.
        assert_eq!(size_of::<Node>(), size_of::<usize>() * 2);
    }

    #[test]
    fn init() {
        env_logger::init();
        let server_group = "index_init";
        let server_addr = String::from("127.0.0.1:5100");
        let server = NebServer::new_from_opts(&ServerOptions {
            chunk_count: 1,
            memory_size: 16 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None
        }, &server_addr, &server_group);
        let client = Arc::new(client::AsyncClient::new(
            &server.rpc, &vec!(server_addr),
            server_group).unwrap());
        client.new_schema_with_id(super::external::page_schema());
        let tree = BPlusTree::new(&client);
        let id = Id::unit_id();
        let key = smallvec![1, 2, 3, 4, 5, 6];
        info!("test insertion");
        tree.insert(&key, &id).unwrap();
        let mut cursor =  tree.seek(&key, Ordering::Forward).unwrap();
        assert_eq!(cursor.current(), id);
    }

    #[test]
    fn lots_of_insertions() {
        env_logger::init();
        let server_group = "index_insertions";
        let server_addr = String::from("127.0.0.1:5101");
        let server = NebServer::new_from_opts(&ServerOptions {
            chunk_count: 1,
            memory_size: 512 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None
        }, &server_addr, &server_group);
        let client = Arc::new(client::AsyncClient::new(
            &server.rpc, &vec!(server_addr),
            server_group).unwrap());
        client.new_schema_with_id(super::external::page_schema()).wait();
        let tree = BPlusTree::new(&client);
        info!("test insertion");
        let num = 100_000;
        for i in 0..num {
            let id = Id::new(0, i);
            let mut key_slice = [0u8; 8];
            {
                let mut cursor = Cursor::new(&mut key_slice[..]);
                cursor.write_u64::<BigEndian>(i);
            };
            let key = SmallVec::from_slice(&key_slice);
            debug!("insert id: {}", i);
            tree.insert(&key, &id).unwrap();
        }
        tree.transaction(|txn| {
            let root = txn.txn.read::<Node>(tree.root)?.unwrap();
            let root_innode = root.innode();
            debug!("root have {} keys", root_innode.keys.len());
            for i in 0..root_innode.len {
                print!("{:?} | ", root_innode.keys[i]);
            }
            println!();
            // debug!("{:?}", root_innode.keys);
            Ok(())
        }).unwrap();
        // sequence check
        dump_tree(&tree);
        debug!("Scanning for sequence verification");
        let mut cursor = tree.seek(&smallvec!(0), Ordering::Forward).unwrap();
        for i in 0..num {
            let expected = Id::new(0, i);
            debug!("Expecting id {:?}", expected);
            let id = cursor.current();
            println!("{:?}", id);
            if i + 1 < num {
                assert!(cursor.next());
            }
        }
    }
}