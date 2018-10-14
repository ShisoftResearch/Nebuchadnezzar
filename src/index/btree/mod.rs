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
use std::io::Write;
use std;

mod internal;
mod external;

const ID_SIZE: usize = 16;
const NUM_KEYS: usize = 4;
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

impl Default for Ordering {
    fn default() -> Self {
        Ordering::Forward
    }
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
                            Arc::new(RwLock::new(ExtNode::from_cell(cell)))
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
        self.ext_node_cache.lock().insert(id, Arc::new(RwLock::new(node)));
        debug!("New page id is: {:?}", id);
        return Node::External(box id);
    }
}

impl Node {
    fn search(&self, key: &EntryKey, bz: &CacheBufferZone) -> usize {
        let len = self.len(bz);
        if self.is_ext() {
            self.extnode(bz).keys[..len]
                .binary_search(key)
                .unwrap_or_else(|i| i)
        } else {
            self.innode().keys[..len]
                .binary_search(key)
                .map(|i| i + 1)
                .unwrap_or_else(|i| i)
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
        match ordering {
            Ordering::Forward => self.search(root, &key, ordering),
            Ordering::Backward => {
                // fill highest bits to the end of the search key as the last possible id for backward search
                let mut key = key.clone();
                let max_id = Id::new(std::u64::MAX, std::u64::MAX);
                key_with_id(&mut key, &max_id);
                debug!("seek backwards with precise key {:?}", &key);
                self.search(root, &key, ordering)
                    .map(|mut cursor| {
                        debug!("found cursor pos {} for backwards, len {}, will be corrected",
                               cursor.index, cursor.page.len);
                        let cursor_index = cursor.index;
                        if cursor_index > 0 { cursor.index -= 1; }
                        debug!("cursor pos have been corrected to {}, len {}, item {:?}",
                               cursor.index, cursor.page.len, cursor.page.keys[cursor.index]);
                        return cursor;
                    })
            }
        }
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
            debug!("search in external for {:?}, items {:?}", key, node.extnode(&self.bz).keys);
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
                debug!("Sub level node split, shall insert new node to current level, pivot {:?}", pivot_key);
                debug_assert!(!(!new_node.is_ext() && pivot_key.is_none()));
                let pivot = pivot_key.unwrap_or_else(|| {
                    let first_key = new_node.first_key(&mut self.bz);
                    debug_assert_ne!(first_key.len(), 0);
                    first_key
                });
                let new_node_ref = self.txn.new_value(new_node);
                debug!("New node in transaction {:?}, pivot {:?}", new_node_ref, pivot);
                let mut acq_node = self.txn.read_owned::<Node>(node)?.unwrap();
                let result = {
                    let pivot_pos = acq_node.search(&pivot, &mut self.bz);
                    let mut current_innode = acq_node.innode_mut();
                    current_innode.insert(
                        pivot,
                        Some(new_node_ref),
                        pivot_pos)
                };
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
        let removed = self.remove_from_node(root, &key)?;
        let root_node = self.txn.read::<Node>(root)?.unwrap();
        if removed.item_found && removed.removed && !root_node.is_ext() && root_node.len(&mut self.bz) == 0 {
            self.txn.update(root, self.tree.new_ext_cached_node());
        }
        Ok(removed.item_found)
    }

    fn remove_from_node(
        &mut self,
        node: TxnValRef,
        key: &EntryKey
    ) -> Result<RemoveStatus, TxnErr> {
        let node_ref = self.txn.read::<Node>(node)?.unwrap();
        node_ref.warm_cache_for_write_intention(&mut self.bz);
        let pos = node_ref.search(key, &mut self.bz);;
        if let Node::Internal(n) = &*node_ref {
            let mut status = self.remove_from_node(n.pointers[pos], key)?;
            if !status.removed { return Ok(status) }
            let sub_node_ref = n.pointers[pos];
            let sub_node = self.txn.read::<Node>(sub_node_ref)?.unwrap();
            let mut current_node_owned = self.txn.read_owned::<Node>(node)?.unwrap();
            {
                let mut current_innode = current_node_owned.innode_mut();
                if sub_node.len(&mut self.bz) == 0 {
                    // need to remove empty child node
                    if sub_node.is_ext() {
                        // empty external node should be removed and rearrange 'next' and 'prev' pointer for neighbourhoods
                        let (prev, next, nid) = {
                            // use ext mut for loading node into cache for delete
                            let n = sub_node.extnode_mut(&mut self.bz);
                            (n.prev, n.next, n.id)
                        };
                        debug_assert_ne!(nid, Id::unit_id());
                        if !prev.is_unit_id() {
                            let mut prev_node = self.bz.get_for_mut(&prev);
                            prev_node.next = next;
                        }
                        if !next.is_unit_id() {
                            let mut next_node = self.bz.get_for_mut(&next);
                            next_node.prev = prev;
                        }
                        debug!("removing node {:?}", nid);
                        self.bz.delete(&nid);
                        current_innode.remove(pos);
                    } else {
                        // empty internal nodes should be replaced with it's only remaining child pointer
                        // there must be at least one child pointer exists
                        current_innode.pointers[pos] = sub_node.innode().pointers[0];
                    }
                    self.txn.delete(sub_node_ref);
                } else if !sub_node.is_half_full(&mut self.bz) && current_innode.len > 1 {
                    // need to rebalance
                    // pick up a subnode for rebalance, it can be at the left or the right of the node that is not half full
                    let cand_ptr_pos = current_innode.rebalance_candidate(pos, &mut self.txn, &mut self.bz)?;;
                    let left_ptr_pos = min(pos, cand_ptr_pos);
                    let right_ptr_pos = max(pos, cand_ptr_pos);
                    if sub_node.cannot_merge(&mut self.bz) {
                        // relocate
                        debug!("relocating {} to {}", left_ptr_pos, right_ptr_pos);
                        current_innode.relocate_children(left_ptr_pos, right_ptr_pos, &mut self.txn, &mut self.bz)?;
                    } else {
                        // merge
                        debug!("relocating {} with {}", left_ptr_pos, right_ptr_pos);
                        current_innode.merge_children(left_ptr_pos, right_ptr_pos, &mut self.txn, &mut self.bz);
                        current_innode.remove(right_ptr_pos - 1);
                    }
                }
            }
            self.txn.update(node, current_node_owned);
            return Ok(status);
        } else if let &Node::External(ref id) = &*node_ref {
            let mut cached_node = self.bz.get_for_mut(id);
            if pos >= cached_node.len {
                debug!("Removing pos overflows external node, pos {}, len {}, expecting key {:?}, keys {:?}",
                       pos, cached_node.len, key, &cached_node.keys);
                return Ok(RemoveStatus{ item_found: false, removed: false })
            }
            if &cached_node.keys[pos] == key {
                cached_node.remove(pos);
                return Ok(RemoveStatus{ item_found: true, removed: true });
            } else {
                debug!("Search check failed for remove at pos {}, expecting {:?}, actual {:?}, keys {:?}",
                       pos, key, &cached_node.keys[pos], &cached_node.keys);
                return Ok(RemoveStatus{ item_found: false, removed: false });
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
        let next = page.next;
        let prev = page.prev;
        debug!("Cursor page len: {}, id {:?}", &node.len, node.id);
        if pos < node.len {
            debug!("Key at pos {} is {:?}, next: {:?}, prev: {:?}", pos, &node.keys[pos], next, prev);
        } else {
            debug!("Cursor creation without valid pos")
        }
        let mut cursor = RTCursor {
            index: pos,
            ordering,
            page,
            bz: bz.clone(),
            next_page: bz.get_guard(match ordering {
                Ordering::Forward => &next,
                Ordering::Backward  => &prev
            })
        };

        match ordering {
            Ordering::Forward => {
                if pos >= node.len {
                    cursor.next();
                }
            },
            Ordering::Backward => {}
        }

        return cursor;
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
                if self.index == 0 {
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

    pub fn current(&self) -> Option<Id> {
        if self.index >= 0 && self.index < self.page.len {
            Some(id_from_key(&self.page.keys[self.index]))
        } else {
            None
        }
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
        debug!("insert into left part, pos: {}", pos);
        x.insert_at(item, pos, xlen);
    } else {
        let right_pos = pos - pivot;
        debug!("insert into right part, pos: {}", right_pos);
        y.insert_at(item, right_pos, ylen);
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
        debug_assert!(pos <= *len, "pos {:?} larger or equals to len {:?}, item: {:?}", pos, len, item);
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
    fn remove_at(&mut self, pos: usize, len: &mut usize) {
        if pos > *len - 1 { return; }
        let slice  = self.as_slice();
        for i in pos .. *len - 1 {
            slice.swap(pos, pos + 1);
        }
        *len -= 1;
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

struct RemoveStatus {
    item_found: bool,
    removed: bool
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

    fn dump_tree(tree: &BPlusTree, f: &str) {
        let mut bz = CacheBufferZone::new(&tree.ext_node_cache, &tree.storage);
        let debug_root = cascading_dump_node(tree, tree.root, &mut bz);
        let json = serde_json::to_string_pretty(&debug_root).unwrap();
        let mut file = File::create(f).unwrap();
        file.write_all(json.as_bytes());
    }

    fn cascading_dump_node(tree: &BPlusTree, node: TxnValRef, bz: &mut CacheBufferZone) -> DebugNode {
        let node = tree.stm.transaction(|txn| {
           txn.read_owned::<Node>(node)
               .map(|opt| opt.unwrap_or(Node::None))
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
            Node::None => DebugNode {
                keys: vec![String::from("<NOT FOUND>")],
                nodes: vec![],
                id: None,
                next: None,
                prev: None,
                is_external: false
            }
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
        assert_eq!(cursor.current().unwrap(), id);
    }

    fn u64_to_slice(n: u64) -> [u8; 8] {
        let mut key_slice = [0u8; 8];
        {
            let mut cursor = Cursor::new(&mut key_slice[..]);
            cursor.write_u64::<BigEndian>(n);
        };
        key_slice
    }

    #[test]
    fn crd() {
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
        let num = 10_000;
        for i in (0..num).rev() {
            let id = Id::new(0, i);
            let key_slice = u64_to_slice(i);
            let key = SmallVec::from_slice(&key_slice);
            debug!("insert id: {}", i);
            tree.insert(&key, &id).unwrap();
        }
        tree.transaction(|txn| {
            let root = txn.txn.read::<Node>(tree.root)?.unwrap();
            let root_innode = root.innode();
            debug!("root have {} keys", root_innode.keys.len());
            for i in 0..root_innode.len {
                debug!("{:?} | ", root_innode.keys[i]);
            }
            // debug!("{:?}", root_innode.keys);
            Ok(())
        }).unwrap();
        // sequence check
        dump_tree(&tree, "tree_dump.json");
        debug!("Scanning for sequence dump");
        let mut cursor = tree.seek(&smallvec!(0), Ordering::Forward).unwrap();
        for i in 0..num {
            let id  = cursor.current().unwrap();
            let unmatched = i != id.lower;
            let check_msg = if unmatched { "=-=-=-=-=-=-=-= NO =-=-=-=-=-=-=" } else { "YES" };
            debug!("Index {} have id {:?} check: {}", i, id, check_msg);
            if unmatched {
                debug!("Expecting index {} encoded {:?}", i, Id::new(0, i).to_binary());
            }
            if i + 1 < num {
                assert!(cursor.next(), i);
            } else {
                assert!(!cursor.next(), i);
            }
        }
        debug!("Forward scanning for sequence verification");
        let mut cursor = tree.seek(&smallvec!(0), Ordering::Forward).unwrap();
        for i in 0..num {
            let expected = Id::new(0, i);
            debug!("Expecting id {:?}", expected);
            let id = cursor.current().unwrap();
            assert_eq!(id, expected);
            if i + 1 < num {
                assert!(cursor.next());
            }
        }

        debug!("Backward scanning for sequence verification");
        let backward_start_key_slice = u64_to_slice(num - 1);
        let mut cursor = tree.seek(&SmallVec::from_slice(&backward_start_key_slice), Ordering::Backward).unwrap();
        for i in (0..num).rev() {
            let expected = Id::new(0, i);
            debug!("Expecting id {:?}", expected);
            let id = cursor.current().unwrap();
            assert_eq!(id, expected, "{}", i);
            if i > 0 {
                assert!(cursor.next());
            }
        }

        // point search
        for i in 0..num {
            let id = Id::new(0, i);
            let key_slice = u64_to_slice(i);
            let key = SmallVec::from_slice(&key_slice);
            assert_eq!(tree.seek(&key, Ordering::default()).unwrap().current(), Some(id), "{}", i);
        }

        // deletion
        let deletion_volume = num / 2;
        for i in (deletion_volume..num).rev() {
            let id = Id::new(0, i);
            let key_slice = u64_to_slice(i);
            let key = SmallVec::from_slice(&key_slice);
            debug!("delete: {}", i);
            let remove_succeed = tree.remove(&key, &id).unwrap();
            if !remove_succeed {
                dump_tree(&tree, "removing_dump.json");
            }
            assert!(remove_succeed, "{}", i);
        }
    }
}