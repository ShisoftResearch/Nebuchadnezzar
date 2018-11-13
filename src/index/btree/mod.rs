use bifrost::utils::async_locks::Mutex;
use bifrost::utils::async_locks::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use bifrost::utils::fut_exec::wait;
use bifrost_hasher::hash_bytes;
use byteorder::{LittleEndian, WriteBytesExt};
use client::AsyncClient;
use dovahkiin::types;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::{key_hash, Map, PrimitiveArray, ToValue, Value};
use futures::Future;
use hermes::stm::{Txn, TxnErr, TxnManager, TxnValRef};
use index::btree::external::*;
use index::btree::internal::*;
use index::EntryKey;
use index::Slice;
use itertools::{chain, Itertools};
use ram::cell::Cell;
use ram::types::RandValue;
use smallvec::SmallVec;
use std;
use std::cell::Ref;
use std::cell::RefMut;
use std::cmp::{max, min};
use std::fmt::Debug;
use std::fmt::Error;
use std::fmt::Formatter;
use std::io::Write;
use std::mem;
use std::ops::Range;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use utils::lru_cache::LRUCache;
use index::{Cursor as IndexCursor};

mod external;
mod internal;

pub const NUM_KEYS: usize = 24;
const NUM_PTRS: usize = NUM_KEYS + 1;
const CACHE_SIZE: usize = 2048;

type EntryKeySlice = [EntryKey; NUM_KEYS];
type NodePointerSlice = [TxnValRef; NUM_PTRS];

// B-Tree is the memtable for the LSM-Tree
// Items can be added and removed in real-time
// It is not supposed to hold a lot of items when it is actually feasible
// There will be a limit for maximum items in ths data structure, when the limit exceeds, higher ordering
// items with number of one page will be merged to next level
pub struct BPlusTree {
    root: TxnValRef,
    ext_node_cache: Arc<ExtNodeCacheMap>,
    stm: TxnManager,
    storage: Arc<AsyncClient>,
    len: Arc<AtomicUsize>,
}

#[derive(Clone)]
enum Node {
    External(Box<Id>),
    Internal(Box<InNode>),
    None,
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
    bz: Rc<CacheBufferZone>,
    ended: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum Ordering {
    Forward,
    Backward,
}

impl Default for Ordering {
    fn default() -> Self {
        Ordering::Forward
    }
}

pub struct TreeTxn<'a> {
    tree: &'a BPlusTree,
    bz: Rc<CacheBufferZone>,
    len: Arc<AtomicUsize>,
    txn: &'a mut Txn,
}

impl BPlusTree {
    pub fn new(neb_client: &Arc<AsyncClient>) -> BPlusTree {
        let neb_client_1 = neb_client.clone();
        let neb_client_2 = neb_client.clone();
        let mut tree = BPlusTree {
            root: Default::default(),
            stm: TxnManager::new(),
            storage: neb_client.clone(),
            len: Arc::new(AtomicUsize::new(0)),
            ext_node_cache: Arc::new(Mutex::new(LRUCache::new(
                CACHE_SIZE,
                move |id| {
                    debug!("Reading page to cache {:?}", id);
                    neb_client_1
                        .read_cell(*id)
                        .wait()
                        .unwrap()
                        .map(|cell| Arc::new(RwLock::new(ExtNode::from_cell(cell))))
                        .ok()
                },
                move |id, value| {
                    debug!("Flush page to cache {:?}", &id);
                    Self::flush_item(&neb_client_2, &value);
                },
            ))),
        };
        {
            let actual_tree_root = tree.new_ext_cached_node();
            let root_ref = tree.stm.with_value(actual_tree_root);
            tree.root = root_ref;
        }
        return tree;
    }
    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> Result<Box<IndexCursor>, TxnErr> {
        debug!("searching for {:?}", key);
        self.transaction(|txn| txn.seek(key, ordering))
            .map(|c| c.boxed())
    }

    pub fn insert(&self, key: &EntryKey) -> Result<(), TxnErr> {
        debug!("inserting {:?}", &key);
        self.transaction(|txn| txn.insert(key))
    }

    pub fn remove(&self, key: &EntryKey) -> Result<bool, TxnErr> {
        debug!("removing {:?}", &key);
        self.transaction(|txn| txn.remove(key))
    }

    pub fn transaction<R, F>(&self, func: F) -> Result<R, TxnErr>
    where
        F: Fn(&mut TreeTxn) -> Result<R, TxnErr>,
    {
        self.stm
            .transaction(|txn| {
                let mut t_txn = TreeTxn::new(self, txn);
                let res = func(&mut t_txn)?;
                Ok((res, t_txn.bz))
            })
            .map(|(res, mut bz)| {
                bz.flush();
                res
            })
    }

    pub fn flush_all(&self) {
        for (_, value) in self.ext_node_cache.lock().iter() {
            Self::flush_item(&self.storage, value)
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Relaxed)
    }

    fn flush_item(client: &Arc<AsyncClient>, value: &Arc<RwLock<ExtNode>>) {
        let cache = value.read();
        if cache.dirty {
            let cell = cache.to_cell();
            client.upsert_cell(cell).wait().unwrap();
        }
    }

    fn new_page_id(&self) -> Id {
        // TODO: achieve locality
        Id::rand()
    }
    fn new_ext_cached_node(&self) -> Node {
        let id = self.new_page_id();
        let mut node = ExtNode::new(&id);
        node.dirty = true;
        self.ext_node_cache
            .lock()
            .insert(id, Arc::new(RwLock::new(node)));
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
        if self.is_ext() {
            self.extnode_mut(bz);
        }
    }
    fn insert(
        &mut self,
        key: EntryKey,
        ptr: Option<TxnValRef>,
        pos: usize,
        tree: &BPlusTree,
        bz: &CacheBufferZone,
    ) -> Option<(Node, Option<EntryKey>)> {
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
            self.extnode_mut(bz).remove_at(pos)
        } else {
            self.innode_mut().remove_at(pos)
        }
    }
    fn is_ext(&self) -> bool {
        match self {
            &Node::External(_) => true,
            &Node::Internal(_) => false,
            &Node::None => panic!(),
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
        self.len(bz) > NUM_KEYS / 2
    }
    fn extnode_mut<'a>(&self, bz: &'a CacheBufferZone) -> RcNodeRefMut<'a> {
        match self {
            &Node::External(ref id) => bz.get_for_mut(id),
            _ => unreachable!(),
        }
    }
    fn innode_mut(&mut self) -> &mut InNode {
        match self {
            &mut Node::Internal(ref mut n) => n,
            _ => unreachable!(),
        }
    }
    fn extnode<'a>(&self, bz: &'a CacheBufferZone) -> RcNodeRef<'a> {
        match self {
            &Node::External(ref id) => bz.get(id),
            _ => unreachable!(),
        }
    }
    fn ext_id(&self) -> Id {
        if let &Node::External(ref id) = self {
            **id
        } else {
            panic!()
        }
    }
    fn innode(&self) -> &InNode {
        match self {
            &Node::Internal(ref n) => n,
            _ => unreachable!(),
        }
    }
}

impl<'a> TreeTxn<'a> {
    fn new(tree: &'a BPlusTree, txn: &'a mut Txn) -> TreeTxn<'a> {
        TreeTxn {
            bz: Rc::new(CacheBufferZone::new(&tree.ext_node_cache, &tree.storage)),
            len: tree.len.clone(),
            tree,
            txn,
        }
    }

    pub fn seek(&mut self, key: &EntryKey, ordering: Ordering) -> Result<RTCursor, TxnErr> {
        let root = self.tree.root;
        match ordering {
            Ordering::Forward => self.search(root, key, ordering),
            Ordering::Backward => {
                // fill highest bits to the end of the search key as the last possible id for backward search
                debug!("seek backwards with precise key {:?}", key);
                self.search(root, key, ordering).map(|mut cursor| {
                    debug!(
                        "found cursor pos {} for backwards, len {}, will be corrected",
                        cursor.index, cursor.page.len
                    );
                    let cursor_index = cursor.index;
                    if cursor_index > 0 {
                        cursor.index -= 1;
                    }
                    debug!(
                        "cursor pos have been corrected to {}, len {}, item {:?}",
                        cursor.index, cursor.page.len, cursor.page.keys[cursor.index]
                    );
                    return cursor;
                })
            }
        }
    }

    fn search(
        &mut self,
        node: TxnValRef,
        key: &EntryKey,
        ordering: Ordering,
    ) -> Result<RTCursor, TxnErr> {
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
    pub fn insert(&mut self, key: &EntryKey) -> Result<(), TxnErr> {
        let root_ref = self.tree.root;
        if let Some((new_node, pivotKey)) = self.insert_to_node(root_ref, key.clone())? {
            debug!("split root with pivot key {:?}", pivotKey);
            let first_key = new_node.first_key(&mut self.bz);
            let new_node_ref = self.txn.new_value(new_node);
            let pivot = pivotKey.unwrap_or_else(|| first_key);
            let mut new_in_root = InNode {
                keys: make_array!(NUM_KEYS, Default::default()),
                pointers: make_array!(NUM_PTRS, Default::default()),
                len: 1,
            };
            let old_root = self.txn.read_owned::<Node>(self.tree.root)?.unwrap();
            let old_root_ref = self.txn.new_value(old_root);
            new_in_root.keys[0] = pivot;
            new_in_root.pointers[0] = old_root_ref;
            new_in_root.pointers[1] = new_node_ref;
            let new_root = Node::Internal(box new_in_root);
            self.txn.update(self.tree.root, new_root);
        }
        let len_counter = self.len.clone();
        self.txn.defer(move || {
            len_counter.fetch_add(1, Relaxed);
        });
        Ok(())
    }
    fn insert_to_node(
        &mut self,
        node: TxnValRef,
        key: EntryKey,
    ) -> Result<Option<(Node, Option<EntryKey>)>, TxnErr> {
        let ref_node = self.txn.read::<Node>(node)?.unwrap();
        ref_node.warm_cache_for_write_intention(&mut self.bz);
        let pos = ref_node.search(&key, &mut self.bz);
        debug!(
            "insert to node: {:?}, len {}, pos: {}, external: {}",
            node,
            ref_node.len(&mut self.bz),
            pos,
            ref_node.is_ext()
        );
        let split_node = match &*ref_node {
            &Node::External(ref id) => {
                debug!(
                    "insert into external at {}, key {:?}, id {:?}",
                    pos, key, id
                );
                let mut node = self.bz.get_for_mut(id);
                if node.removed.load(Relaxed) {
                    return Err(TxnErr::NotRealizable)
                }
                return Ok(node.insert(key, pos, self.tree, &*self.bz));
            }
            &Node::Internal(ref n) => {
                let next_node_ref = n.pointers[pos];
                debug!(
                    "insert into internal at {}, has keys {}, ref {:?}",
                    pos, n.len, next_node_ref
                );
                self.insert_to_node(next_node_ref, key)?
            }
            &Node::None => unreachable!(),
        };
        match split_node {
            Some((new_node, pivot_key)) => {
                debug!(
                    "Sub level node split, shall insert new node to current level, pivot {:?}",
                    pivot_key
                );
                debug_assert!(!(!new_node.is_ext() && pivot_key.is_none()));
                let pivot = pivot_key.unwrap_or_else(|| {
                    let first_key = new_node.first_key(&mut self.bz);
                    debug_assert_ne!(first_key.len(), 0);
                    first_key
                });
                let new_node_ref = self.txn.new_value(new_node);
                debug!(
                    "New node in transaction {:?}, pivot {:?}",
                    new_node_ref, pivot
                );
                let mut acq_node = self.txn.read_owned::<Node>(node)?.unwrap();
                let result = {
                    let pivot_pos = acq_node.search(&pivot, &mut self.bz);
                    let mut current_innode = acq_node.innode_mut();
                    current_innode.insert(pivot, Some(new_node_ref), pivot_pos)
                };
                self.txn.update(node, acq_node);
                if result.is_some() {
                    debug!("Sub level split caused current level split");
                }
                return Ok(result);
            }
            None => return Ok(None),
        }
    }

    pub fn remove(&mut self, key: &EntryKey) -> Result<bool, TxnErr> {
        let root = self.tree.root;
        let removed = self.remove_from_node(root, key)?;
        let root_node = self.txn.read::<Node>(root)?.unwrap();
        if removed.item_found
            && removed.removed
            && !root_node.is_ext()
            && root_node.len(&mut self.bz) == 0
        {
            // When root is external and have no keys but one pointer will take the only sub level
            // pointer node as the new root node.
            // It is not possible to change the root reference so the sub level node will be cloned
            // and removed. Its clone will be used as the new root node.
            let new_root_ref = root_node.innode().pointers[0];
            let new_root_cloned = self.txn.read_owned::<Node>(new_root_ref)?.unwrap();
            self.txn.update(root, new_root_cloned);
            self.txn.delete(new_root_ref);
        }
        if removed.item_found {
            let len_counter = self.len.clone();
            self.txn.defer(move || {
                len_counter.fetch_sub(1, Relaxed);
            });
        }
        Ok(removed.item_found)
    }

    fn remove_from_node(
        &mut self,
        node: TxnValRef,
        key: &EntryKey,
    ) -> Result<RemoveStatus, TxnErr> {
        debug!("Removing {:?} from node {:?}", key, node);
        let node_ref = self.txn.read::<Node>(node)?.unwrap();
        node_ref.warm_cache_for_write_intention(&mut self.bz);
        let pos = node_ref.search(key, &mut self.bz);;
        if let Node::Internal(n) = &*node_ref {
            let mut status = self.remove_from_node(n.pointers[pos], key)?;
            if !status.removed {
                return Ok(status);
            }
            let sub_node_ref = n.pointers[pos];
            let sub_node = self.txn.read::<Node>(sub_node_ref)?.unwrap();
            let mut current_node_owned = self.txn.read_owned::<Node>(node)?.unwrap();
            {
                let mut current_innode = current_node_owned.innode_mut();
                if sub_node.len(&mut self.bz) == 0 {
                    // need to remove empty child node
                    if sub_node.is_ext() {
                        debug!("Removing empty node");
                        current_innode.remove_at(pos);
                        self.txn.delete(sub_node_ref);
                        sub_node.extnode_mut(&self.bz).remove_node(&self.bz);
                        status.removed = true;
                    } else {
                        // empty internal nodes should be replaced with it's only remaining child pointer
                        // there must be at least one child pointer exists
                        let sub_innode = sub_node.innode();
                        let sub_sub_node_ref = sub_innode.pointers[0];
                        debug_assert!(sub_innode.len == 0);
                        debug_assert!(self.txn.read::<Node>(sub_sub_node_ref).unwrap().is_some());
                        current_innode.pointers[pos] = sub_sub_node_ref;
                        status.removed = false;
                    }
                } else if !sub_node.is_half_full(&mut self.bz) && current_innode.len > 1 {
                    // need to rebalance
                    // pick up a subnode for rebalance, it can be at the left or the right of the node that is not half full
                    let cand_ptr_pos =
                        current_innode.rebalance_candidate(pos, &mut self.txn, &mut self.bz)?;;
                    let left_ptr_pos = min(pos, cand_ptr_pos);
                    let right_ptr_pos = max(pos, cand_ptr_pos);
                    let cand_node = self.txn.read::<Node>(n.pointers[cand_ptr_pos])?.unwrap();
                    debug_assert_eq!(cand_node.is_ext(), sub_node.is_ext());
                    if cand_node.is_ext() {
                        cand_node.extnode_mut(&mut self.bz);
                        sub_node.extnode_mut(&mut self.bz);
                    }
                    if sub_node.cannot_merge(&mut self.bz) || cand_node.cannot_merge(&mut self.bz) {
                        // relocate
                        debug!("Relocating {} to {}", left_ptr_pos, right_ptr_pos);
                        current_innode.relocate_children(
                            left_ptr_pos,
                            right_ptr_pos,
                            &mut self.txn,
                            &mut self.bz,
                        )?;
                        status.removed = false;
                    } else {
                        // merge
                        debug!("Merge {} with {}", left_ptr_pos, right_ptr_pos);
                        current_innode.merge_children(
                            left_ptr_pos,
                            right_ptr_pos,
                            &mut self.txn,
                            &mut self.bz,
                        );
                        status.removed = true;
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
                return Ok(RemoveStatus {
                    item_found: false,
                    removed: false,
                });
            }
            if &cached_node.keys[pos] == key {
                cached_node.remove_at(pos);
                return Ok(RemoveStatus {
                    item_found: true,
                    removed: true,
                });
            } else {
                debug!("Search check failed for remove at pos {}, expecting {:?}, actual {:?}, keys {:?}",
                       pos, key, &cached_node.keys[pos], &cached_node.keys);
                return Ok(RemoveStatus {
                    item_found: false,
                    removed: false,
                });
            }
        } else {
            unreachable!()
        }
    }
}

impl RTCursor {
    fn new(pos: usize, id: &Id, bz: &Rc<CacheBufferZone>, ordering: Ordering) -> RTCursor {
        let node = bz.get(id);
        let page = bz.get_guard(id).unwrap();
        let next = page.next;
        let prev = page.prev;
        debug!("Cursor page len: {}, id {:?}", &node.len, node.id);
        if pos < node.len {
            debug!(
                "Key at pos {} is {:?}, next: {:?}, prev: {:?}",
                pos, &node.keys[pos], next, prev
            );
        } else {
            debug!("Cursor creation without valid pos")
        }
        let mut cursor = RTCursor {
            index: pos,
            ordering,
            page,
            bz: bz.clone(),
            ended: false,
            next_page: bz.get_guard(match ordering {
                Ordering::Forward => &next,
                Ordering::Backward => &prev,
            }),
        };

        match ordering {
            Ordering::Forward => {
                if pos >= node.len {
                    cursor.next();
                }
            }
            Ordering::Backward => {}
        }

        return cursor;
    }
    fn boxed(self) -> Box<IndexCursor> {
        box self
    }
}

impl IndexCursor for RTCursor {
    fn next(&mut self) -> bool {
        debug!(
            "Next id with index: {}, length: {}",
            self.index + 1,
            self.page.len
        );
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
                        self.ended = true;
                        return false;
                    }
                } else {
                    self.index += 1;
                    debug!("Advancing cursor to index {}", self.index);
                }
            }
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
                        self.ended = true;
                        return false;
                    }
                } else {
                    self.index -= 1;
                    debug!("Advancing cursor to index {}", self.index);
                }
            }
        }
        return true;
    }

    fn current(&self) -> Option<&EntryKey> {
        if !self.ended && self.index >= 0 && self.index < self.page.len {
            Some(&self.page.keys[self.index])
        } else {
            None
        }
    }
}

fn insert_into_split<T, S>(
    item: T,
    x: &mut S,
    y: &mut S,
    xlen: &mut usize,
    ylen: &mut usize,
    pos: usize,
    pivot: usize,
) where
    S: Slice<Item = T> + BTreeSlice<T>,
    T: Default + Debug,
{
    debug!(
        "insert into split left len {}, right len {}, pos {}, pivot {}",
        xlen, ylen, pos, pivot
    );
    if pos < pivot {
        debug!("insert into left part, pos: {}", pos);
        x.insert_at(item, pos, xlen);
    } else {
        let right_pos = pos - pivot;
        debug!("insert into right part, pos: {}", right_pos);
        y.insert_at(item, right_pos, ylen);
    }
}

macro_rules! impl_btree_slice {
    ($t: ty, $et: ty, $n: expr) => {
        impl_slice_ops!($t, $et, $n);
        impl BTreeSlice<$et> for $t {}
    };
}

impl_btree_slice!(EntryKeySlice, EntryKey, NUM_KEYS);
impl_btree_slice!(NodePointerSlice, TxnValRef, NUM_PTRS);

struct RemoveStatus {
    item_found: bool,
    removed: bool,
}

impl Default for Node {
    fn default() -> Self {
        Node::None
    }
}

trait BTreeSlice<T>: Sized + Slice<Item = T>
where
    T: Default + Debug,
{
    fn split_at_pivot(&mut self, pivot: usize, len: usize) -> Self {
        let mut right_slice = Self::init();
        {
            let mut slice1: &mut [T] = self.as_slice();
            let mut slice2: &mut [T] = right_slice.as_slice();
            for i in pivot..len {
                // leave pivot to the right slice
                let right_pos = i - pivot;
                let item = mem::replace(&mut slice1[i], T::default());
                slice2[right_pos] = item;
            }
        }
        return right_slice;
    }
    fn insert_at(&mut self, item: T, pos: usize, len: &mut usize) {
        debug_assert!(
            pos <= *len,
            "pos {:?} larger or equals to len {:?}, item: {:?}",
            pos,
            len,
            item
        );
        debug!(
            "insert into slice, pos: {}, len {}, item {:?}",
            pos, len, item
        );
        let mut slice = self.as_slice();
        if *len > 0 {
            slice[*len] = T::default();
            for i in (pos..=*len - 1).rev() {
                slice.swap(i, i + 1);
            }
        }
        debug!("setting item {:?} at {} for insertion", item, pos);
        *len += 1;
        slice[pos] = item;
    }
    fn remove_at(&mut self, pos: usize, len: &mut usize) {
        debug!("remove at {} len {}", pos, len);
        debug_assert!(pos < *len, "remove overflow, pos {}, len {}", pos, len);
        if pos < *len - 1 {
            let slice = self.as_slice();
            for i in pos..*len - 1 {
                slice.swap(i, i + 1);
            }
        }
        *len -= 1;
    }
}

#[cfg(test)]
mod test {
    use super::{BPlusTree, Node, Ordering};
    use bifrost::utils::fut_exec::wait;
    use byteorder::BigEndian;
    use byteorder::WriteBytesExt;
    use client;
    use dovahkiin::types::custom_types::id::Id;
    use futures::future::Future;
    use hermes::stm::TxnValRef;
    use index::btree::external::CacheBufferZone;
    use index::btree::NUM_KEYS;
    use index::{id_from_key, key_with_id};
    use ram::types::RandValue;
    use rand::distributions::Uniform;
    use rand::prelude::*;
    use server;
    use server::NebServer;
    use server::ServerOptions;
    use smallvec::SmallVec;
    use std::env;
    use std::fs::File;
    use std::io::Cursor;
    use std::io::Write;
    use std::mem::size_of;
    use std::sync::Arc;

    extern crate env_logger;
    extern crate serde_json;

    #[derive(Serialize, Deserialize)]
    struct DebugNode {
        keys: Vec<String>,
        nodes: Vec<DebugNode>,
        id: Option<String>,
        next: Option<String>,
        prev: Option<String>,
        len: usize,
        is_external: bool,
    }

    fn dump_tree(tree: &BPlusTree, f: &str) {
        debug!("dumping {}", f);
        let mut bz = CacheBufferZone::new(&tree.ext_node_cache, &tree.storage);
        let debug_root = cascading_dump_node(tree, tree.root, &mut bz);
        let json = serde_json::to_string_pretty(&debug_root).unwrap();
        let mut file = File::create(f).unwrap();
        file.write_all(json.as_bytes());
    }

    fn cascading_dump_node(
        tree: &BPlusTree,
        node: TxnValRef,
        bz: &mut CacheBufferZone,
    ) -> DebugNode {
        let node = tree
            .stm
            .transaction(|txn| {
                txn.read_owned::<Node>(node)
                    .map(|opt| opt.unwrap_or(Node::None))
            })
            .unwrap();
        match node {
            Node::External(id) => {
                let node = bz.get(&*id);
                let keys = node
                    .keys
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
                    len: node.len,
                    is_external: true,
                };
            }
            Node::Internal(innode) => {
                let len = innode.len;
                let nodes = innode
                    .pointers
                    .iter()
                    .take(len + 1)
                    .map(|node_ref| cascading_dump_node(tree, *node_ref, bz))
                    .collect();
                let keys = innode
                    .keys
                    .iter()
                    .take(len)
                    .map(|key| format!("{:?}", key))
                    .collect();
                return DebugNode {
                    keys,
                    nodes,
                    id: None,
                    next: None,
                    prev: None,
                    len,
                    is_external: false,
                };
            }
            Node::None => DebugNode {
                keys: vec![String::from("<NOT FOUND>")],
                nodes: vec![],
                id: None,
                next: None,
                prev: None,
                len: 0,
                is_external: false,
            },
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
        let server_group = "bree_index_init";
        let server_addr = String::from("127.0.0.1:5100");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 16 * 1024 * 1024,
                backup_storage: None,
                wal_storage: None,
            },
            &server_addr,
            &server_group,
        );
        let client = Arc::new(
            client::AsyncClient::new(&server.rpc, &vec![server_addr], server_group).unwrap(),
        );
        client.new_schema_with_id(super::external::page_schema());
        let tree = BPlusTree::new(&client);
        let id = Id::unit_id();
        let key = smallvec![1, 2, 3, 4, 5, 6];
        info!("test insertion");
        let mut entry_key = key.clone();
        key_with_id(&mut entry_key, &id);
        tree.insert(&entry_key).unwrap();
        let mut cursor = tree.seek(&key, Ordering::Forward).unwrap();
        assert_eq!(id_from_key(cursor.current().unwrap()), id);
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
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 1024 * 1024 * 1024,
                backup_storage: None,
                wal_storage: None,
            },
            &server_addr,
            &server_group,
        );
        let client = Arc::new(
            client::AsyncClient::new(&server.rpc, &vec![server_addr], server_group).unwrap(),
        );
        client
            .new_schema_with_id(super::external::page_schema())
            .wait()
            .unwrap();
        let tree = BPlusTree::new(&client);
        let num = env::var("BTREE_TEST_ITEMS")
            .unwrap_or("1000".to_string())
            .parse::<u64>()
            .unwrap();
        {
            info!("test insertion");
            for i in (0..num).rev() {
                let id = Id::new(0, i);
                let key_slice = u64_to_slice(i);
                let key = SmallVec::from_slice(&key_slice);
                debug!("insert id: {}", i);
                let mut entry_key = key.clone();
                key_with_id(&mut entry_key, &id);
                tree.insert(&entry_key).unwrap();
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
            })
            .unwrap();
            assert_eq!(tree.len(), num as usize);
            dump_tree(&tree, "tree_dump.json");
        }

        {
            debug!("Scanning for sequence");
            let mut cursor = tree.seek(&smallvec!(0), Ordering::Forward).unwrap();
            for i in 0..num {
                let id = id_from_key(cursor.current().unwrap());
                let unmatched = i != id.lower;
                let check_msg = if unmatched {
                    "=-=-=-=-=-=-=-= NO =-=-=-=-=-=-="
                } else {
                    "YES"
                };
                debug!("Index {} have id {:?} check: {}", i, id, check_msg);
                if unmatched {
                    debug!(
                        "Expecting index {} encoded {:?}",
                        i,
                        Id::new(0, i).to_binary()
                    );
                }
                assert_eq!(cursor.next(), i + 1 < num);
            }
            debug!("Forward scanning for sequence verification");
            let mut cursor = tree.seek(&smallvec!(0), Ordering::Forward).unwrap();
            for i in 0..num {
                let expected = Id::new(0, i);
                debug!("Expecting id {:?}", expected);
                let id = id_from_key(cursor.current().unwrap());
                assert_eq!(id, expected);
                assert_eq!(cursor.next(), i + 1 < num);
            }
            assert!(cursor.current().is_none());
        }

        {
            debug!("Backward scanning for sequence verification");
            let backward_start_key_slice = u64_to_slice(num - 1);
            let mut entry_key = SmallVec::from_slice(&backward_start_key_slice);
            // search backward required max possible id
            key_with_id(&mut entry_key, &Id::new(::std::u64::MAX, ::std::u64::MAX));
            let mut cursor = tree.seek(&entry_key, Ordering::Backward).unwrap();
            for i in (0..num).rev() {
                let expected = Id::new(0, i);
                debug!("Expecting id {:?}", expected);
                let id = id_from_key(cursor.current().unwrap());
                assert_eq!(id, expected, "{}", i);
                assert_eq!(cursor.next(), i > 0);
            }
            assert!(cursor.current().is_none());
        }

        {
            debug!("point search");
            for i in 0..num {
                let id = Id::new(0, i);
                let key_slice = u64_to_slice(i);
                let key = SmallVec::from_slice(&key_slice);
                assert_eq!(
                    id_from_key(
                        tree.seek(&key, Ordering::default())
                            .unwrap()
                            .current()
                            .unwrap()
                    ),
                    id,
                    "{}",
                    i
                );
            }
        }

        {
            debug!("Testing deletion");
            let deletion_volume = num / 2;
            for i in 0..deletion_volume {
                debug!("delete: {}", i);
                let id = Id::new(0, i);
                let key_slice = u64_to_slice(i);
                let key = SmallVec::from_slice(&key_slice);
                let mut entry_key = key.clone();
                key_with_id(&mut entry_key, &id);
                let remove_succeed = tree.remove(&entry_key).unwrap();
                if !remove_succeed {
                    dump_tree(&tree, &format!("removing_{}_dump.json", i));
                }
                assert!(remove_succeed, "{}", i);
            }

            assert_eq!(tree.len(), (num - deletion_volume) as usize);
            dump_tree(&tree, "remove_completed_dump.json");

            debug!("check for removed items");
            for i in 0..deletion_volume {
                let key_slice = u64_to_slice(i);
                let key = SmallVec::from_slice(&key_slice);
                assert_eq!(
                    id_from_key(
                        tree.seek(&key, Ordering::default())
                            .unwrap()
                            .current()
                            .unwrap()
                    ),
                    Id::new(0, deletion_volume), // seek should reach deletion_volume
                    "{}",
                    i
                );
            }

            debug!("check for remaining items");
            for i in deletion_volume..num {
                let id = Id::new(0, i);
                let key_slice = u64_to_slice(i);
                let key = SmallVec::from_slice(&key_slice);
                assert_eq!(
                    id_from_key(
                        tree.seek(&key, Ordering::default())
                            .unwrap()
                            .current()
                            .unwrap()
                    ),
                    id,
                    "{}",
                    i
                );
            }

            tree.flush_all();

            debug!("remove remaining items, with extensive point search");
            // die-rolling
            let mut rng = thread_rng();
            let die_range = Uniform::new_inclusive(1, 6);
            let mut roll_die = rng.sample_iter(&die_range);
            for i in (deletion_volume..num).rev() {
                {
                    debug!("delete and sampling: {}", i);
                    let id = Id::new(0, i);
                    let key_slice = u64_to_slice(i);
                    let key = SmallVec::from_slice(&key_slice);
                    let mut entry_key = key.clone();
                    key_with_id(&mut entry_key, &id);
                    let remove_succeed = tree.remove(&entry_key).unwrap();
                    if !remove_succeed {
                        dump_tree(&tree, &format!("removing_{}_remaining_dump.json", i));
                    }
                    assert!(remove_succeed, "{}", i);
                }
                if roll_die.next().unwrap() != 6 {
                    continue;
                }
                debug!("sampling for remaining integrity for {}", i);
                for j in deletion_volume..i {
                    if roll_die.next().unwrap() != 6 {
                        continue;
                    }
                    let id = Id::new(0, j);
                    let key_slice = u64_to_slice(j);
                    let key = SmallVec::from_slice(&key_slice);
                    assert_eq!(
                        id_from_key(
                            tree.seek(&key, Ordering::default())
                                .unwrap()
                                .current()
                                .unwrap()
                        ),
                        id,
                        "{} / {}",
                        i,
                        j
                    );
                }
            }
            dump_tree(&tree, "remove_remains_dump.json");

            debug!("check for removed items");
            for i in 0..num {
                let key_slice = u64_to_slice(i);
                let key = SmallVec::from_slice(&key_slice);
                assert_eq!(
                    tree.seek(&key, Ordering::default()).unwrap().current(),
                    None, // should always be 'None' for empty tree
                    "{}",
                    i
                );
            }

            tree.flush_all();
            assert_eq!(tree.len(), 0);
            assert_eq!(client.count().wait().unwrap(), 1);
        }
    }
}
