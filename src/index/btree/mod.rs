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
use index::MergeableTree;
use index::MergingPage;
use index::MergingTreeGuard;
use index::Slice;
use index::{Cursor as IndexCursor, Ordering};
use itertools::{chain, Itertools};
use ram::cell::Cell;
use ram::types::RandValue;
use smallvec::SmallVec;
use std;
use std::cell::Ref;
use std::cell::RefCell;
use std::cell::RefMut;
use std::cell::UnsafeCell;
use std::cmp::{max, min};
use std::fmt::Debug;
use std::fmt::Error;
use std::fmt::Formatter;
use std::io::Write;
use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Range;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed, Ordering::SeqCst};
use std::sync::Arc;
use utils::lru_cache::LRUCache;

mod external;
mod internal;

pub const NUM_KEYS: usize = 24;
const NUM_PTRS: usize = NUM_KEYS + 1;
const CACHE_SIZE: usize = 2048;

pub type NodeCellRef = Arc<Node>;
type EntryKeySlice = [EntryKey; NUM_KEYS];
type NodePtrCellSlice = [NodeCellRef; NUM_PTRS];

// B-Tree is the memtable for the LSM-Tree
// Items can be added and removed in real-time
// It is not supposed to hold a lot of items when it is actually feasible
// There will be a limit for maximum items in ths data structure, when the limit exceeds, higher ordering
// items with number of one page will be merged to next level
pub struct BPlusTree {
    pub root: NodeCellRef,
    storage: Arc<AsyncClient>,
    len: Arc<AtomicUsize>,
}

unsafe impl Sync for BPlusTree {}
unsafe impl Send for BPlusTree {}

// This is the runtime cursor on iteration
// It hold a copy of the containing page next page lock guard
// These lock guards are preventing the node and their neighbourhoods been changed externally
// Ordering are specified that can also change lock pattern
pub struct RTCursor {
    index: usize,
    ordering: Ordering,
    page: Option<NodeCellRef>,
}

impl Default for Ordering {
    fn default() -> Self {
        Ordering::Forward
    }
}

enum InsertSearchResult {
    External,
    Internal(NodeCellRef),
    RightNode(NodeCellRef),
}

enum InsertToNodeResult {
    NoSplit,
    Split(NodeWriteGuard, NodeCellRef, Option<EntryKey>),
    SplitParentChanged,
}

enum RemoveSearchResult {
    External,
    RightNode(NodeCellRef),
    Internal(NodeCellRef, SubNodeStatus),
}

enum SubNodeStatus {
    ExtNodeEmpty,
    InNodeEmpty,
    Relocate(usize, usize),
    Merge(usize, usize),
    Ok,
}

pub struct NodeSplit {
    new_right_node: NodeCellRef,
    left_node_latch: NodeWriteGuard,
    pivot: Option<EntryKey>,
    parent_latch: Option<NodeWriteGuard>,
}

pub enum NodeSplitResult {
    Split(NodeSplit),
    Retry,
}

impl BPlusTree {
    pub fn new(neb_client: &Arc<AsyncClient>) -> BPlusTree {
        let neb_client_1 = neb_client.clone();
        let neb_client_2 = neb_client.clone();
        let mut tree = BPlusTree {
            root: Arc::new(Node::none()),
            storage: neb_client.clone(),
            len: Arc::new(AtomicUsize::new(0)),
        };
        let root_id = tree.new_page_id();
        tree.root = Arc::new(Node::new_external(root_id));
        return tree;
    }

    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> RTCursor {
        let mut cursor = unsafe { self.search(&self.root, key, ordering) };
        match ordering {
            Ordering::Forward => {}
            Ordering::Backward => {
                // fill highest bits to the end of the search key as the last possible id for backward search
                debug!(
                    "found cursor pos {} for backwards, will be corrected",
                    cursor.index
                );
                let cursor_index = cursor.index;
                if cursor_index > 0 {
                    cursor.index -= 1;
                }
                debug!("cursor pos have been corrected to {}", cursor.index);
            }
        }
        cursor
    }

    fn search(&self, node_ref: &NodeCellRef, key: &EntryKey, ordering: Ordering) -> RTCursor {
        debug!("searching for {:?}", key);
        return node_ref.read(|node_handler| {
            let node = &**node_handler;
            if let Some(right_node) = node.key_at_right_node(key) {
                debug!("Search found a node at the right side");
                return self.search(right_node, key, ordering);
            }
            let pos = node.search(key);
            if node.is_ext() {
                let extnode = node.extnode();
                debug!(
                    "search in external for {:?}, len {}, content: {:?}",
                    key, extnode.len, extnode.keys
                );
                RTCursor::new(pos, node_ref, ordering)
            } else if let &NodeData::Internal(ref n) = node {
                debug!("search in internal node for {:?}, len {}", key, n.len);
                let next_node_ref = &n.ptrs[pos];
                self.search(next_node_ref, key, ordering)
            } else {
                unreachable!()
            }
        });
    }

    pub fn insert(&self, key: &EntryKey) {
        match self.insert_to_node(&self.root, None, None, &key) {
            Some(NodeSplitResult::Split(mut split)) => {
                debug!("split root with pivot key {:?}", split.pivot);
                let new_node = split.new_right_node;
                let first_key = new_node.read_unchecked().first_key();
                let pivot = split.pivot.unwrap_or_else(|| first_key);
                let mut new_in_root = InNode {
                    keys: make_array!(NUM_KEYS, Default::default()),
                    ptrs: make_array!(NUM_PTRS, Default::default()),
                    right: Arc::new(Node::none()),
                    len: 1,
                };
                // TODO: review swap root
                unsafe {
                    debug!("obtain latch for root split");
                    let old_root = &mut *split.left_node_latch;
                    new_in_root.keys[0] = pivot;
                    new_in_root.ptrs[1] = new_node;
                    debug!("obtain latch for root new splited root");
                    let first_ptr = &mut *new_in_root.ptrs[0].write();
                    let new_root = NodeData::Internal(box new_in_root);
                    *first_ptr = new_root;
                    mem::swap(old_root, first_ptr);
                }
            }
            Some(_) => unreachable!(),
            None => {}
        }
        self.len.fetch_add(1, Relaxed);
    }

    fn insert_to_node(
        &self,
        node_ref: &NodeCellRef,
        parent: Option<&NodeCellRef>,
        parent_version: Option<usize>,
        key: &EntryKey,
    ) -> Option<NodeSplitResult> {
        loop {
            let mut version = 0;
            let mut search = node_ref.read(|node_handler| {
                debug!(
                    "insert to node, len {}, external: {}",
                    node_handler.len(),
                    node_handler.is_ext()
                );
                version = node_handler.version;
                if let Some(right_node) = node_handler.key_at_right_node(key) {
                    debug!("Moving to right node for insertion");
                    return InsertSearchResult::RightNode(right_node.clone());
                }
                match &**node_handler {
                    &NodeData::External(ref node) => InsertSearchResult::External,
                    &NodeData::Internal(ref node) => {
                        let pos = node_handler.search(&key);
                        let next_node_ref = &node.ptrs[pos];
                        InsertSearchResult::Internal(next_node_ref.clone())
                    }
                    &NodeData::None => unreachable!(),
                }
            });
            match search {
                InsertSearchResult::RightNode(node) => {
                    return self.insert_to_node(&node, parent, parent_version, key);
                }
                InsertSearchResult::External => {
                    // latch nodes from left to right
                    debug!("Obtain latch for external node");
                    let node_guard = node_ref.write();
                    if node_ref.version() != version {
                        continue;
                    }
                    let mut searched_guard = write_key_page(node_guard, key);
                    let pos = searched_guard.search(key);
                    debug_assert!(
                        searched_guard.is_ext(),
                        "{:?}",
                        searched_guard.innode().keys
                    );
                    let mut split_result = searched_guard.extnode_mut().insert(
                        key,
                        pos,
                        self,
                        node_ref,
                        parent,
                        parent_version,
                    );
                    if let &mut Some(NodeSplitResult::Split(ref mut split)) = &mut split_result {
                        split.left_node_latch = searched_guard;
                    }
                    return split_result;
                }
                InsertSearchResult::Internal(node) => {
                    let split_res = self.insert_to_node(&node, Some(node_ref), Some(version), key);
                    match split_res {
                        Some(NodeSplitResult::Retry) => continue,
                        None => return None,
                        Some(NodeSplitResult::Split(split)) => {
                            debug!(
                                "Sub level node split, shall insert new node to current level, pivot {:?}",
                                split.pivot
                            );
                            let pivot = {
                                let new_node = split.new_right_node.read_unchecked();
                                debug_assert!(!(!new_node.is_ext() && split.pivot.is_none()));
                                split.pivot.unwrap_or_else(|| {
                                    let first_key = new_node.first_key();
                                    debug_assert_ne!(first_key.len(), 0);
                                    first_key
                                })
                            };
                            debug!("New pivot {:?}", pivot);
                            debug!("obtain latch for internal node split");
                            let mut target_guard = write_key_page(split.parent_latch.unwrap(), key);
                            let pivot_pos = target_guard.search(&pivot);
                            debug!(
                                "will insert into current node at {}, node len {}",
                                pivot_pos,
                                target_guard.len()
                            );
                            let mut split_result = target_guard.innode_mut().insert(
                                pivot,
                                split.new_right_node,
                                parent,
                                parent_version,
                                pivot_pos,
                            );
                            if let &mut Some(NodeSplitResult::Split(ref mut split)) = &mut split_result {
                                split.left_node_latch = target_guard;
                            }
                            return split_result;
                        }
                    }
                }
            };
            break;
        }
        unreachable!()
    }

    pub fn remove(&self, key: &EntryKey) -> bool {
        let root = &self.root;
        let removed = self.remove_from_node(root, key);
        let root_node = &mut *root.write();
        if removed.item_found && removed.removed && !root_node.is_ext() && root_node.len() == 0 {
            // When root is external and have no keys but one pointer will take the only sub level
            // pointer node as the new root node.
            let new_root = &mut *root_node.innode().ptrs[0].write();
            // TODO: check memory leak
            mem::swap(root_node, new_root);
        }
        if removed.item_found {
            self.len.fetch_sub(1, Relaxed);
        }
        removed.item_found
    }

    fn remove_from_node(&self, node_ref: &NodeCellRef, key: &EntryKey) -> RemoveStatus {
        debug!("Removing {:?} from node", key);
        loop {
            let mut pos = 0;
            let mut ver = 0;
            let (mut remove_stat, search) = node_ref.read(|node| {
                if let Some(right_node) = node.key_at_right_node(key) {
                    return (
                        RemoveStatus {
                            item_found: false,
                            removed: false,
                        },
                        RemoveSearchResult::RightNode(right_node.clone()),
                    );
                }
                ver = node.version;
                pos = node.search(key);
                if let &NodeData::Internal(ref n) = &**node {
                    let sub_node = n.ptrs[pos].clone();
                    let mut sub_node_remove = self.remove_from_node(&sub_node, key);
                    let sub_node_stat = sub_node.read(|sub_node_handler| {
                        if sub_node_handler.len() == 0 {
                            if sub_node_handler.is_ext() {
                                SubNodeStatus::ExtNodeEmpty
                            } else {
                                SubNodeStatus::InNodeEmpty
                            }
                        } else if !sub_node_handler.is_half_full() && n.len > 1 {
                            // need to rebalance
                            // pick up a subnode for rebalance, it can be at the left or the right of the node that is not half full
                            let cand_ptr_pos = n.rebalance_candidate(pos);
                            let left_ptr_pos = min(pos, cand_ptr_pos);
                            let right_ptr_pos = max(pos, cand_ptr_pos);
                            let cand_node = &mut *n.ptrs[cand_ptr_pos].write();
                            debug_assert_eq!(cand_node.is_ext(), sub_node_handler.is_ext());
                            if sub_node_handler.cannot_merge() || cand_node.cannot_merge() {
                                SubNodeStatus::Relocate(left_ptr_pos, right_ptr_pos)
                            } else {
                                SubNodeStatus::Merge(left_ptr_pos, right_ptr_pos)
                            }
                        } else {
                            SubNodeStatus::Ok
                        }
                    });
                    (
                        sub_node_remove,
                        RemoveSearchResult::Internal(sub_node, sub_node_stat),
                    )
                } else if let &NodeData::External(ref node) = &**node {
                    if pos >= node.len || &node.keys[pos] != key {
                        (
                            RemoveStatus {
                                item_found: false,
                                removed: false,
                            },
                            RemoveSearchResult::External,
                        )
                    } else {
                        (
                            RemoveStatus {
                                item_found: true,
                                removed: true,
                            },
                            RemoveSearchResult::External,
                        )
                    }
                } else {
                    unreachable!()
                }
            });
            match search {
                RemoveSearchResult::RightNode(node) => return self.remove_from_node(&node, key),
                RemoveSearchResult::Internal(sub_node, sub_node_stat) => {
                    if !remove_stat.removed {
                        return remove_stat;
                    }
                    let mut node_guard = node_ref.write();
                    let mut n = node_guard.innode_mut();
                    if node_ref.version() != ver {
                        continue;
                    }
                    match sub_node_stat {
                        SubNodeStatus::Ok => {}
                        SubNodeStatus::Relocate(left_ptr_pos, right_ptr_pos) => {
                            n.relocate_children(left_ptr_pos, right_ptr_pos);
                            remove_stat.removed = false;
                        }
                        SubNodeStatus::Merge(left_ptr_pos, right_ptr_pos) => {
                            n.merge_children(left_ptr_pos, right_ptr_pos);
                            remove_stat.removed = true;
                        }
                        SubNodeStatus::ExtNodeEmpty => {
                            n.remove_at(pos);
                            sub_node.write().extnode_mut().remove_node();
                            remove_stat.removed = true;
                        }
                        SubNodeStatus::InNodeEmpty => {
                            // empty internal nodes should be replaced with it's only remaining child pointer
                            // there must be at least one child pointer exists
                            let sub_node_guard = sub_node.write();
                            let sub_innode = sub_node_guard.innode();
                            let sub_sub_node_ref = sub_innode.ptrs[0].clone();
                            debug_assert!(sub_innode.len == 0);
                            n.ptrs[pos] = sub_sub_node_ref;
                            remove_stat.removed = false;
                        }
                    }
                    return remove_stat;
                }
                RemoveSearchResult::External => {
                    let mut node_guard = node_ref.write();
                    let mut node = node_guard.extnode_mut();
                    if node_ref.version() != ver {
                        continue;
                    }
                    if pos >= node.len {
                        debug!(
                            "Removing pos overflows external node, pos {}, len {}, expecting key {:?}",
                            pos, node.len, key
                        );
                        return RemoveStatus {
                            item_found: false,
                            removed: false,
                        };
                    }
                    if &node.keys[pos] == key {
                        node.remove_at(pos);
                        return RemoveStatus {
                            item_found: true,
                            removed: true,
                        };
                    } else {
                        debug!(
                            "Search check failed for remove at pos {}, expecting {:?}, actual {:?}",
                            pos, key, &node.keys[pos]
                        );
                        return RemoveStatus {
                            item_found: false,
                            removed: false,
                        };
                    }
                }
            }
        }
    }

    pub fn flush_all(&self) {
        // unimplemented!()
    }

    pub fn len(&self) -> usize {
        self.len.load(Relaxed)
    }

    fn flush_item(client: &Arc<AsyncClient>, value: &NodeCellRef) {
        let cell = value.read(|node| {
            let extnode = node.extnode();
            if extnode.is_dirty() {
                Some(extnode.to_cell())
            } else {
                None
            }
        });
        if let Some(cell) = cell {
            client.upsert_cell(cell).wait().unwrap();
        }
    }

    fn new_page_id(&self) -> Id {
        // TODO: achieve locality
        Id::rand()
    }
}

pub enum NodeData {
    External(Box<ExtNode>),
    Internal(Box<InNode>),
    None,
}

impl NodeData {
    pub fn is_none(&self) -> bool {
        match self {
            &NodeData::None => true,
            _ => false,
        }
    }
    fn search(&self, key: &EntryKey) -> usize {
        let len = self.len();
        if self.is_ext() {
            self.extnode().keys[..len]
                .binary_search(key)
                .unwrap_or_else(|i| i)
        } else {
            self.innode().keys[..len]
                .binary_search(key)
                .map(|i| i + 1)
                .unwrap_or_else(|i| i)
        }
    }

    fn remove(&mut self, pos: usize) {
        match self {
            &mut NodeData::External(ref mut node) => node.remove_at(pos),
            &mut NodeData::Internal(ref mut node) => node.remove_at(pos),
            &mut NodeData::None => unreachable!(),
        }
    }
    fn is_ext(&self) -> bool {
        match self {
            &NodeData::External(_) => true,
            &NodeData::Internal(_) => false,
            &NodeData::None => panic!(),
        }
    }
    fn first_key(&self) -> EntryKey {
        if self.is_ext() {
            self.extnode().keys[0].to_owned()
        } else {
            self.innode().keys[0].to_owned()
        }
    }
    fn len(&self) -> usize {
        if self.is_ext() {
            self.extnode().len
        } else {
            self.innode().len
        }
    }
    fn is_half_full(&self) -> bool {
        self.len() >= NUM_KEYS / 2
    }
    fn cannot_merge(&self) -> bool {
        self.len() > NUM_KEYS / 2
    }
    fn extnode_mut(&mut self) -> &mut ExtNode {
        match self {
            &mut NodeData::External(ref mut node) => node,
            _ => unreachable!(self.type_name()),
        }
    }
    fn innode_mut(&mut self) -> &mut InNode {
        match self {
            &mut NodeData::Internal(ref mut n) => n,
            _ => unreachable!(self.type_name()),
        }
    }
    pub fn extnode(&self) -> &ExtNode {
        match self {
            &NodeData::External(ref node) => node,
            _ => unreachable!(self.type_name()),
        }
    }
    pub fn ext_id(&self) -> Id {
        match self {
            &NodeData::External(ref node) => node.id,
            &NodeData::None => Id::unit_id(),
            _ => unreachable!(self.type_name()),
        }
    }
    pub fn innode(&self) -> &InNode {
        match self {
            &NodeData::Internal(ref n) => n,
            _ => unreachable!(self.type_name()),
        }
    }
    pub fn type_name(&self) -> &'static str {
        match self {
            &NodeData::Internal(_) => "internal",
            &NodeData::External(_) => "external",
            &NodeData::None => "none",
        }
    }
    pub fn key_at_right_node(&self, key: &EntryKey) -> Option<&NodeCellRef> {
        if self.len() > 0 {
            match self {
                &NodeData::Internal(ref n) => {
                    if &n.keys[n.len - 1] < key {
                        let right_node = n.right.read_unchecked();
                        if !right_node.is_none() {
                            let right_innode = right_node.innode();
                            if right_innode.keys.len() > 0 && &right_innode.keys[0] <= key {
                                return Some(&n.right);
                            }
                        }
                    }
                }
                &NodeData::External(ref n) => {
                    if &n.keys[n.len - 1] < key {
                        let right_node = n.next.read_unchecked();
                        if !right_node.is_none() {
                            let right_extnode = right_node.extnode();
                            if right_extnode.keys.len() > 0 && &right_extnode.keys[0] <= key {
                                return Some(&n.next);
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
        return None;
    }
}

pub fn write_key_page(search_page: NodeWriteGuard, key: &EntryKey) -> NodeWriteGuard {
    if search_page.len() > 0 {
        match &*search_page {
            &NodeData::Internal(ref n) => {
                if &n.keys[n.len - 1] < key {
                    let right_ref = &n.right;
                    debug!("Obtain latch for internal write key page");
                    let right_node = right_ref.write();
                    if !right_node.is_none()
                        && right_node.len() > 0
                        && &right_node.innode().keys[0] <= key
                    {
                        debug_assert!(!right_node.is_ext());
                        return write_key_page(right_node, key);
                    }
                }
            }
            &NodeData::External(ref n) => {
                if &n.keys[n.len - 1] < key {
                    let right_ref = &n.next;
                    debug!("Obtain latch for external write key page");
                    let right_node = right_ref.write();
                    if !right_node.is_none()
                        && right_node.len() > 0
                        && &right_node.extnode().keys[0] <= key
                    {
                        debug_assert!(right_node.is_ext());
                        return write_key_page(right_node, key);
                    }
                }
            }
            _ => unreachable!(),
        }
        return search_page;
    } else {
        return search_page;
    }
}

const LATCH_FLAG: usize = !(!0 >> 1);

pub struct Node {
    data: UnsafeCell<NodeData>,
    cc: AtomicUsize,
}

impl Node {
    fn new(data: NodeData) -> Self {
        Node {
            data: UnsafeCell::new(data),
            cc: AtomicUsize::new(0),
        }
    }

    fn internal(innode: InNode) -> Self {
        Self::new(NodeData::Internal(box innode))
    }

    fn external(extnode: ExtNode) -> Self {
        Self::new(NodeData::External(box extnode))
    }

    fn none() -> Self {
        Self::new(NodeData::None)
    }
    pub fn none_ref() -> NodeCellRef {
        Arc::new(Node::none())
    }
    pub fn new_external(id: Id) -> Self {
        Self::external(ExtNode::new(id))
    }

    pub fn write(&self) -> NodeWriteGuard {
        let cc = &self.cc;
        loop {
            let cc_num = cc.load(Relaxed);
            let expected = cc_num & (!LATCH_FLAG);
            if cc.compare_and_swap(expected, cc_num | LATCH_FLAG, Relaxed) == expected {
                return NodeWriteGuard {
                    data: self.data.get(),
                    cc: &self.cc as *const AtomicUsize,
                    version: node_version(cc_num),
                };
            }
            debug!("acquire latch failed, retry {:b}", cc_num);
        }
    }

    pub fn read<'a, F: FnMut(&NodeReadHandler) -> R + 'a, R: 'a>(&'a self, mut func: F) -> R {
        let mut handler = NodeReadHandler {
            ptr: self.data.get(),
            version: 0,
        };
        let cc = &self.cc;
        loop {
            let cc_num = cc.load(Relaxed);
            if cc_num & LATCH_FLAG == LATCH_FLAG {
                debug!("read have a latch, retry {:b}", cc_num);
                continue;
            }
            handler.version = cc_num & (!LATCH_FLAG);
            let res = func(&handler);
            let new_cc_num = cc.load(Relaxed);
            if new_cc_num == cc_num {
                return res;
            }
            debug!("read version changed, retry {:b}", cc_num);
        }
    }

    pub fn read_unchecked(&self) -> &NodeData {
        unsafe { &*self.data.get() }
    }

    pub fn version(&self) -> usize {
        node_version(self.cc.load(Relaxed))
    }
}

pub fn node_version(cc_num: usize) -> usize {
    cc_num & (!LATCH_FLAG)
}

pub struct NodeWriteGuard {
    data: *mut NodeData,
    cc: *const AtomicUsize,
    version: usize,
}

impl Deref for NodeWriteGuard {
    type Target = NodeData;

    fn deref(&self) -> &<Self as Deref>::Target {
        debug_assert_ne!(self.data as usize, 0);
        unsafe { &*self.data }
    }
}

impl DerefMut for NodeWriteGuard {
    fn deref_mut(&mut self) -> &mut <Self as Deref>::Target {
        debug_assert_ne!(self.data as usize, 0);
        unsafe { &mut *self.data }
    }
}

impl Drop for NodeWriteGuard {
    fn drop(&mut self) {
        // cope with null pointer
        if self.cc as usize != 0 {
            let cc = unsafe { &*self.cc };
            let cc_num = cc.load(Relaxed);
            cc.store((cc_num & (!LATCH_FLAG)) + 1, Relaxed);
        }
    }
}

impl Default for NodeWriteGuard {
    fn default() -> Self {
        NodeWriteGuard {
            data: 0 as *mut NodeData,
            cc: 0 as *const AtomicUsize,
            version: 0
        }
    }
}

pub struct NodeReadHandler {
    ptr: *const NodeData,
    version: usize,
}

impl Deref for NodeReadHandler {
    type Target = NodeData;

    fn deref(&self) -> &<Self as Deref>::Target {
        unsafe { &*self.ptr }
    }
}

impl RTCursor {
    fn new(pos: usize, page: &NodeCellRef, ordering: Ordering) -> RTCursor {
        let mut cursor = RTCursor {
            index: pos,
            ordering,
            page: Some(page.clone()),
        };
        match ordering {
            Ordering::Forward => {
                let len = page.read(|node| node.len());
                if pos >= len {
                    cursor.next();
                }
            }
            Ordering::Backward => {}
        }
        cursor
    }
    fn boxed(self) -> Box<IndexCursor> {
        box self
    }
}

impl IndexCursor for RTCursor {
    fn next(&mut self) -> bool {
        if self.page.is_some() {
            let current_page = self.page.clone().unwrap();
            current_page.read(|page| {
                let ext_page = page.extnode();
                debug!(
                    "Next id with index: {}, length: {}",
                    self.index + 1,
                    ext_page.len
                );
                match self.ordering {
                    Ordering::Forward => {
                        if self.index + 1 >= page.len() {
                            // next page
                            return ext_page.next.read(|next_page| {
                                if !next_page.is_none() {
                                    debug!(
                                        "Shifting page forward, next page len: {}",
                                        next_page.len()
                                    );
                                    self.index = 0;
                                    self.page = Some(ext_page.next.clone());
                                    return true;
                                } else {
                                    debug!("iterated to end");
                                    self.page = None;
                                    return false;
                                }
                            });
                        } else {
                            self.index += 1;
                            debug!("Advancing cursor to index {}", self.index);
                        }
                    }
                    Ordering::Backward => {
                        if self.index == 0 {
                            // next page
                            return ext_page.prev.read(|prev_page| {
                                if !prev_page.is_none() {
                                    debug!("Shifting page backward");
                                    self.page = Some(ext_page.prev.clone());
                                    self.index = prev_page.len() - 1;
                                    return true;
                                } else {
                                    debug!("iterated to end");
                                    self.page = None;
                                    return false;
                                }
                            });
                        } else {
                            self.index -= 1;
                            debug!("Advancing cursor to index {}", self.index);
                        }
                    }
                }
                true
            })
        } else {
            false
        }
    }

    fn current(&self) -> Option<&EntryKey> {
        if let &Some(ref page_ref) = &self.page {
            page_ref.read(|handler| {
                let page = unsafe { &*handler.ptr };
                if self.index >= page.len() {
                    panic!("cursor position overflowed {}/{}", self.index, page.len())
                }
                Some(&page.extnode().keys[self.index])
            })
        } else {
            return None;
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
) where
    S: Slice<Item = T> + BTreeSlice<T>,
    T: Default,
{
    debug!(
        "insert into split left len {}, right len {}, pos {}",
        xlen, ylen, pos
    );
    if pos < *xlen {
        debug!("insert into left part, pos: {}", pos);
        x.insert_at(item, pos, xlen);
    } else {
        let right_pos = pos - *xlen;
        debug!("insert into right part, pos: {}", right_pos);
        y.insert_at(item, right_pos, ylen);
    }
}

pub struct BPlusTreeMergingPage {
    page: RwLockReadGuard<ExtNode>,
    mapper: Arc<ExtNodeCacheMap>,
    pages: Rc<RefCell<Vec<Id>>>,
}

impl MergingPage for BPlusTreeMergingPage {
    fn next(&self) -> Box<MergingPage> {
        unimplemented!()
    }

    fn keys(&self) -> &[EntryKey] {
        &self.page.keys[..self.page.len]
    }
}

impl MergeableTree for BPlusTree {
    fn prepare_level_merge(&self) -> Box<MergingTreeGuard> {
        unimplemented!()
    }

    fn elements(&self) -> usize {
        self.len()
    }
}

pub struct BPlusTreeMergingTreeGuard {
    cache: Arc<ExtNodeCacheMap>,
    txn: RefCell<Txn>,
    last_node: Arc<Node>,
    last_node_ref: TxnValRef,
    storage: Arc<AsyncClient>,
    pages: Rc<RefCell<Vec<Id>>>,
}

impl MergingTreeGuard for BPlusTreeMergingTreeGuard {
    fn remove_pages(&self, pages: &[&[EntryKey]]) {
        unimplemented!()
    }

    fn last_page(&self) -> Box<MergingPage> {
        unimplemented!()
    }
}

impl Drop for BPlusTreeMergingTreeGuard {
    fn drop(&mut self) {
        self.txn.borrow_mut().commit();
    }
}

macro_rules! impl_btree_slice {
    ($t: ty, $et: ty, $n: expr) => {
        impl_slice_ops!($t, $et, $n);
        impl BTreeSlice<$et> for $t {}
    };
}

impl_btree_slice!(EntryKeySlice, EntryKey, NUM_KEYS);
impl_btree_slice!(NodePtrCellSlice, NodeCellRef, NUM_PTRS);

struct RemoveStatus {
    item_found: bool,
    removed: bool,
}

impl Default for Node {
    fn default() -> Self {
        Node::none()
    }
}

trait BTreeSlice<T>: Sized + Slice<Item = T>
where
    T: Default,
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
        debug_assert!(pos <= *len, "pos {} larger or equals to len {}", pos, len);
        debug!("insert into slice, pos: {}, len {}", pos, len);
        let mut slice = self.as_slice();
        if *len > 0 {
            slice[*len] = T::default();
            for i in (pos..=*len - 1).rev() {
                slice.swap(i, i + 1);
            }
        }
        debug!("setting item at {} for insertion", pos);
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
pub mod test {
    use super::{BPlusTree, Node, Ordering};
    use bifrost::utils::fut_exec::wait;
    use byteorder::BigEndian;
    use byteorder::WriteBytesExt;
    use client;
    use dovahkiin::types::custom_types::id::Id;
    use futures::future::Future;
    use hermes::stm::TxnValRef;
    use index::btree::NodeCellRef;
    use index::btree::NodeData;
    use index::btree::NUM_KEYS;
    use index::Cursor;
    use index::{id_from_key, key_with_id};
    use ram::types::RandValue;
    use rand::distributions::Uniform;
    use rand::prelude::*;
    use rayon::prelude::*;
    use server;
    use server::NebServer;
    use server::ServerOptions;
    use smallvec::SmallVec;
    use std::env;
    use std::fs::File;
    use std::io::Cursor as StdCursor;
    use std::io::Write;
    use std::mem::size_of;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use itertools::Itertools;

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

    pub fn dump_tree(tree: &BPlusTree, f: &str) {
        debug!("dumping {}", f);
        let debug_root = cascading_dump_node(&tree.root);
        let json = serde_json::to_string_pretty(&debug_root).unwrap();
        let mut file = File::create(f).unwrap();
        file.write_all(json.as_bytes());
    }

    fn cascading_dump_node(node: &NodeCellRef) -> DebugNode {
        unsafe {
            match &*node.write() {
                &NodeData::External(ref node) => {
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
                        next: Some(format!("{:?}", node.next.write().ext_id())),
                        prev: Some(format!("{:?}", node.prev.write().ext_id())),
                        len: node.len,
                        is_external: true,
                    };
                }
                &NodeData::Internal(ref innode) => {
                    let len = innode.len;
                    let nodes = innode
                        .ptrs
                        .iter()
                        .take(len + 1)
                        .map(|node_ref| cascading_dump_node(node_ref))
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
                &NodeData::None => {
                    return DebugNode {
                        keys: vec![String::from("<NOT FOUND>")],
                        nodes: vec![],
                        id: None,
                        next: None,
                        prev: None,
                        len: 0,
                        is_external: false,
                    }
                }
            }
        }
    }

    #[test]
    fn node_size() {
        // expecting the node size to be an on-heap pointer plus node type tag, aligned, and one for concurrency control.
        assert_eq!(size_of::<Node>(), size_of::<usize>() * 3);
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
        tree.insert(&entry_key);
        let mut cursor = tree.seek(&key, Ordering::Forward);
        assert_eq!(id_from_key(cursor.current().unwrap()), id);
    }

    fn u64_to_slice(n: u64) -> [u8; 8] {
        let mut key_slice = [0u8; 8];
        {
            let mut cursor = StdCursor::new(&mut key_slice[..]);
            cursor.write_u64::<BigEndian>(n);
        };
        key_slice
    }

    #[test]
    fn crd() {
        use index::Cursor;
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
            let mut nums = (0..num).collect_vec();
            let mut slice = nums.as_mut_slice();
            thread_rng().shuffle(slice);
            for i in slice {
                let id = Id::new(0, *i);
                let key_slice = u64_to_slice(*i);
                let key = SmallVec::from_slice(&key_slice);
                debug!("insert id: {}", i);
                let mut entry_key = key.clone();
                key_with_id(&mut entry_key, &id);
                tree.insert(&entry_key);
            }
            assert_eq!(tree.len(), num as usize);
            dump_tree(&tree, "tree_dump.json");
        }

        {
            debug!("Scanning for sequence");
            let mut cursor = tree.seek(&smallvec!(0), Ordering::Forward);
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
            let mut cursor = tree.seek(&smallvec!(0), Ordering::Forward);
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
            let mut cursor = tree.seek(&entry_key, Ordering::Backward);
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
                    id_from_key(tree.seek(&key, Ordering::default()).current().unwrap()),
                    id,
                    "{}",
                    i
                );
            }
        }

        {
            debug!("Testing deletion");
            let deletion_volume = num / 2;
            let mut deletions = (0..deletion_volume).collect_vec();
            thread_rng().shuffle(deletions.as_mut_slice());
            for i in deletions {
                debug!("delete: {}", i);
                let id = Id::new(0, i);
                let key_slice = u64_to_slice(i);
                let key = SmallVec::from_slice(&key_slice);
                let mut entry_key = key.clone();
                key_with_id(&mut entry_key, &id);
                let remove_succeed = tree.remove(&entry_key);
                if !remove_succeed {
                    dump_tree(&tree, &format!("removing_{}_dump.json", i));
                }
                assert!(remove_succeed, "remove at {}", i);
            }

            assert_eq!(tree.len(), (num - deletion_volume) as usize);
            dump_tree(&tree, "remove_completed_dump.json");

            debug!("check for removed items");
            for i in 0..deletion_volume {
                let key_slice = u64_to_slice(i);
                let key = SmallVec::from_slice(&key_slice);
                assert_eq!(
                    id_from_key(tree.seek(&key, Ordering::default()).current().unwrap()),
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
                    id_from_key(tree.seek(&key, Ordering::default()).current().unwrap()),
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
                    let remove_succeed = tree.remove(&entry_key);
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
                        id_from_key(tree.seek(&key, Ordering::default()).current().unwrap()),
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
                    tree.seek(&key, Ordering::default()).current(),
                    None, // should always be 'None' for empty tree
                    "{}",
                    i
                );
            }

            tree.flush_all();
            assert_eq!(tree.len(), 0);
            // assert_eq!(client.count().wait().unwrap(), 1);
        }
    }

    #[test]
    pub fn alternative_insertion_pattern() {
        use index::Cursor;
        env_logger::init();
        let server_group = "b+ tree alternative insertion pattern";
        let server_addr = String::from("127.0.0.1:5400");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 16 * 1024 * 1024 * 1024,
                backup_storage: None,
                wal_storage: None,
            },
            &server_addr,
            &server_group,
        );
        let client = Arc::new(
            client::AsyncClient::new(&server.rpc, &vec![server_addr], server_group).unwrap(),
        );
        client.new_schema_with_id(super::page_schema()).wait();
        let tree = BPlusTree::new(&client);
        let num = env::var("BTREE_TEST_ITEMS")
            // this value cannot do anything useful to the test
            // must arrange a long-term test to cover every levels
            .unwrap_or("1000".to_string())
            .parse::<u64>()
            .unwrap();

        for i in 0..num {
            let id = Id::new(0, i);
            let key_slice = u64_to_slice(i);
            let mut key = SmallVec::from_slice(&key_slice);
            key_with_id(&mut key, &id);
            debug!("insert {:?}", key);
            tree.insert(&key);
        }

        let mut rng = thread_rng();
        let die_range = Uniform::new_inclusive(1, 6);
        let mut roll_die = rng.sample_iter(&die_range);
        for i in 0..num {
            let id = Id::new(0, i);
            let key_slice = u64_to_slice(i);
            let mut key = SmallVec::from_slice(&key_slice);
            key_with_id(&mut key, &id);
            debug!("checking {:?}", &key);
            if roll_die.next().unwrap() != 6 {
                continue;
            }
            let mut cursor = tree.seek(&key, Ordering::Forward);
            for j in i..num {
                let id = Id::new(0, j);
                let key_slice = u64_to_slice(j);
                let mut key = SmallVec::from_slice(&key_slice);
                key_with_id(&mut key, &id);
                assert_eq!(cursor.current(), Some(&key));
                assert_eq!(cursor.next(), j != num - 1);
            }
        }
    }

    #[test]
    fn parallel() {
        env_logger::init();
        let server_group = "sstable_index_init";
        let server_addr = String::from("127.0.0.1:5600");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 4 * 1024 * 1024 * 1024,
                backup_storage: None,
                wal_storage: None,
            },
            &server_addr,
            &server_group,
        );
        let client = Arc::new(
            client::AsyncClient::new(&server.rpc, &vec![server_addr], server_group).unwrap(),
        );
        client.new_schema_with_id(super::page_schema()).wait();
        let tree = Arc::new(BPlusTree::new(&client));
        let num = env::var("BTREE_TEST_ITEMS")
            // this value cannot do anything useful to the test
            // must arrange a long-term test to cover every levels
            .unwrap_or("1000".to_string())
            .parse::<u64>()
            .unwrap();

        let tree_clone = tree.clone();
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(10));
            let tree_len = tree_clone.len();
            debug!(
                "LSM-Tree now have {}/{} elements, total {:.2}%",
                tree_len,
                num,
                tree_len as f32 / num as f32 * 100.0
            );
        });

        let mut nums = (0..num).collect::<Vec<_>>();
        thread_rng().shuffle(nums.as_mut_slice());
        nums.par_iter().for_each(|i| {
            let i = *i;
            let id = Id::new(0, i);
            let key_slice = u64_to_slice(i);
            let mut key = SmallVec::from_slice(&key_slice);
            key_with_id(&mut key, &id);
            tree.insert(&key);
        });

        dump_tree(&*tree, "btree_parallel_insertion_dump.json");
        debug!("Start validation on single thread");
        (0..num).collect::<Vec<_>>().iter().for_each(|i| {
            let mut rng = rand::rngs::OsRng::new().unwrap();
            let die_range = Uniform::new_inclusive(1, 6);
            let mut roll_die = rng.sample_iter(&die_range);
            let i = *i;
            let id = Id::new(0, i);
            let key_slice = u64_to_slice(i);
            let mut key = SmallVec::from_slice(&key_slice);
            debug!("checking: {}", i);
            let mut cursor = tree.seek(&key, Ordering::Forward);
            key_with_id(&mut key, &id);
            assert_eq!(cursor.current(), Some(&key), "{}", i);
            if roll_die.next().unwrap() == 6 {
                for j in i..num {
                    let id = Id::new(0, j);
                    let key_slice = u64_to_slice(j);
                    let mut key = SmallVec::from_slice(&key_slice);
                    key_with_id(&mut key, &id);
                    assert_eq!(cursor.current(), Some(&key), "{}/{}", i, j);
                    assert_eq!(cursor.next(), j != num - 1, "{}/{}", i, j);
                }
            }
        });
    }
}
