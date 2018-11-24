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
use std::ops::Range;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use utils::lru_cache::LRUCache;

mod external;
mod internal;

pub const NUM_KEYS: usize = 24;
const NUM_PTRS: usize = NUM_KEYS + 1;
const CACHE_SIZE: usize = 2048;

pub type NodeCellRef = Arc<UnsafeCell<Node>>;
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

pub enum Node {
    External(Box<ExtNode>),
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
    page: Option<NodeCellRef>,
}

impl Default for Ordering {
    fn default() -> Self {
        Ordering::Forward
    }
}

impl BPlusTree {
    pub fn new(neb_client: &Arc<AsyncClient>) -> BPlusTree {
        let neb_client_1 = neb_client.clone();
        let neb_client_2 = neb_client.clone();
        let mut tree = BPlusTree {
            root: Arc::new(UnsafeCell::new(Node::None)),
            storage: neb_client.clone(),
            len: Arc::new(AtomicUsize::new(0)),
        };
        let root_id = tree.new_page_id();
        tree.root = Arc::new(UnsafeCell::new(Node::new_external(root_id)));
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

    unsafe fn search(&self, node_ref: &NodeCellRef, key: &EntryKey, ordering: Ordering) -> RTCursor {
        debug!("searching for {:?}", key);
        let node = &*node_ref.get();
        let pos = node.search(key);
        if node.is_ext() {
            let extnode = node.extnode();
            debug!("search in external for {:?}, len {}", key, extnode.len);
            RTCursor::new(pos, node_ref, ordering)
        } else if let &Node::Internal(ref n) = node {
            let next_node_ref = &n.ptrs[pos];
            self.search(next_node_ref, key, ordering)
        } else {
            unreachable!()
        }
    }

    pub fn insert(&self, key: &EntryKey) {
        if let Some((new_node, pivotKey)) = unsafe { self.insert_to_node(&self.root, key.clone()) }
        {
            debug!("split root with pivot key {:?}", pivotKey);
            let first_key = unsafe { (&*new_node.get()).first_key() };
            let pivot = pivotKey.unwrap_or_else(|| first_key);
            let mut new_in_root = InNode {
                keys: make_array!(NUM_KEYS, Default::default()),
                ptrs: make_array!(NUM_PTRS, Default::default()),
                right_ptr: Arc::new(UnsafeCell::new(Node::None)),
                cc: AtomicUsize::new(0),
                len: 1,
            };
            // TODO: review swap root
            unsafe {
                let old_root = self.root.get();
                new_in_root.keys[0] = pivot;
                new_in_root.ptrs[0].get().swap(old_root);
                new_in_root.ptrs[1] = new_node;
                let new_root = Node::Internal(box new_in_root);
                self.root.get().replace(new_root);
            }
        }
        self.len.fetch_add(1, Relaxed);
    }

    unsafe fn insert_to_node(
        &self,
        node_ref: &NodeCellRef,
        key: EntryKey,
    ) -> Option<(NodeCellRef, Option<EntryKey>)> {
        let node = &mut *node_ref.get();
        let pos = node.search(&key);
        debug!(
            "insert to node, len {}, pos: {}, external: {}",
            node.len(),
            pos,
            node.is_ext()
        );
        let split_node = match node {
            &mut Node::External(ref mut node) => {
                debug!(
                    "insert into external at {}, key {:?}, id {:?}",
                    pos, key, node.id
                );
                return node.insert(key, pos, self, node_ref);
            }
            &mut Node::Internal(ref mut n) => {
                let next_node_ref = &n.ptrs[pos];
                debug!("insert into internal at {}, has keys {}", pos, n.len);
                self.insert_to_node(next_node_ref, key)
            }
            &mut Node::None => unreachable!(),
        };
        match split_node {
            Some((new_node_ref, pivot_key)) => {
                debug!(
                    "Sub level node split, shall insert new node to current level, pivot {:?}",
                    pivot_key
                );
                let new_node = &*new_node_ref.get();
                debug_assert!(!(!new_node.is_ext() && pivot_key.is_none()));
                let pivot = pivot_key.unwrap_or_else(|| {
                    let first_key = new_node.first_key();
                    debug_assert_ne!(first_key.len(), 0);
                    first_key
                });
                debug!("New pivot {:?}", pivot);
                let result = {
                    let pivot_pos = node.search(&pivot);
                    debug!(
                        "will insert into current node at {}, node len {}",
                        pivot_pos,
                        node.len()
                    );
                    let mut current_innode = node.innode_mut();
                    current_innode.insert(pivot, Some(new_node_ref), pivot_pos)
                };
                if result.is_some() {
                    debug!("Sub level split caused current level split");
                }
                return result;
            }
            None => None,
        }
    }

    pub fn remove(&self, key: &EntryKey) -> bool {
        let root = &self.root;
        let removed = unsafe { self.remove_from_node(root, key) };
        let root_node = unsafe { &mut *root.get() };
        if removed.item_found && removed.removed && !root_node.is_ext() && root_node.len() == 0 {
            // When root is external and have no keys but one pointer will take the only sub level
            // pointer node as the new root node.
            let new_root = unsafe { &mut *root_node.innode().ptrs[0].get() };
            // TODO: check memory leak
            mem::swap(root_node, new_root);
        }
        if removed.item_found {
            self.len.fetch_sub(1, Relaxed);
        }
        removed.item_found
    }

    unsafe fn remove_from_node(&self, node: &NodeCellRef, key: &EntryKey) -> RemoveStatus {
        debug!("Removing {:?} from node", key);
        let mut node = &mut *node.get();
        let pos = node.search(key);
        if let Node::Internal(ref mut n) = node {
            let mut status = self.remove_from_node(&n.ptrs[pos], key);
            if !status.removed {
                return status;
            }
            let sub_node = &mut *n.ptrs[pos].get();
            {
                if sub_node.len() == 0 {
                    // need to remove empty child node
                    if sub_node.is_ext() {
                        debug!("Removing empty node");
                        n.remove_at(pos);
                        sub_node.extnode_mut().remove_node();
                        status.removed = true;
                    } else {
                        // empty internal nodes should be replaced with it's only remaining child pointer
                        // there must be at least one child pointer exists
                        let sub_innode = sub_node.innode();
                        let sub_sub_node_ref = sub_innode.ptrs[0].clone();
                        debug_assert!(sub_innode.len == 0);
                        n.ptrs[pos] = sub_sub_node_ref;
                        status.removed = false;
                    }
                } else if !sub_node.is_half_full() && n.len > 1 {
                    // need to rebalance
                    // pick up a subnode for rebalance, it can be at the left or the right of the node that is not half full
                    let cand_ptr_pos = n.rebalance_candidate(pos);
                    let left_ptr_pos = min(pos, cand_ptr_pos);
                    let right_ptr_pos = max(pos, cand_ptr_pos);
                    let cand_node = &mut *n.ptrs[cand_ptr_pos].get();
                    debug_assert_eq!(cand_node.is_ext(), sub_node.is_ext());
                    if sub_node.cannot_merge() || cand_node.cannot_merge() {
                        // relocate
                        debug!("Relocating {} to {}", left_ptr_pos, right_ptr_pos);
                        n.relocate_children(left_ptr_pos, right_ptr_pos);
                        status.removed = false;
                    } else {
                        // merge
                        debug!("Merge {} with {}", left_ptr_pos, right_ptr_pos);
                        n.merge_children(left_ptr_pos, right_ptr_pos);
                        status.removed = true;
                    }
                }
            }
            return status;
        } else if let &mut Node::External(ref mut node) = node {
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
        } else {
            unreachable!()
        }
    }

    pub fn flush_all(&self) {
        unimplemented!()
    }

    pub fn len(&self) -> usize {
        self.len.load(Relaxed)
    }

    unsafe fn flush_item(client: &Arc<AsyncClient>, value: &NodeCellRef) {
        let value = &*value.get();
        let extnode = value.extnode();
        if extnode.is_dirty() {
            let cell = value.extnode().to_cell();
            client.upsert_cell(cell).wait().unwrap();
        }
    }

    fn new_page_id(&self) -> Id {
        // TODO: achieve locality
        Id::rand()
    }
}

impl Node {
    pub fn none_ref() -> NodeCellRef {
        Arc::new(UnsafeCell::new(Node::None))
    }
    pub fn new_external(id: Id) -> Node {
        Node::External(box ExtNode::new(id))
    }
    pub fn is_none(&self) -> bool {
        match self {
            &Node::None => true,
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
            &mut Node::External(ref mut node) => node.remove_at(pos),
            &mut Node::Internal(ref mut node) => node.remove_at(pos),
            &mut Node::None => unreachable!(),
        }
    }
    fn is_ext(&self) -> bool {
        match self {
            &Node::External(_) => true,
            &Node::Internal(_) => false,
            &Node::None => panic!(),
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
            &mut Node::External(ref mut node) => node,
            _ => unreachable!(),
        }
    }
    fn innode_mut(&mut self) -> &mut InNode {
        match self {
            &mut Node::Internal(ref mut n) => n,
            _ => unreachable!(),
        }
    }
    pub fn extnode(&self) -> &ExtNode {
        match self {
            &Node::External(ref node) => node,
            _ => unreachable!(),
        }
    }
    fn ext_id(&self) -> Id {
        match self {
            &Node::External(ref node) => node.id,
            &Node::None => Id::unit_id(),
            _ => unreachable!(),
        }
    }
    pub fn innode(&self) -> &InNode {
        match self {
            &Node::Internal(ref n) => n,
            _ => unreachable!(),
        }
    }
}

impl RTCursor {
    fn new(pos: usize, page: &NodeCellRef, ordering: Ordering) -> RTCursor {
        RTCursor {
            index: pos,
            ordering,
            page: Some(page.clone())
        }
    }
    fn boxed(self) -> Box<IndexCursor> {
        box self
    }
}

impl IndexCursor for RTCursor {
    fn next(&mut self) -> bool {
        if self.page.is_some() {
            let current_page = self.page.clone().unwrap();
            let page = unsafe { &*current_page.get() };
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
                        if unsafe { &*ext_page.next.get() }.is_none() {
                            debug!("Shifting page forward");
                            self.index = 0;
                            self.page = Some(ext_page.next.clone());
                        } else {
                            debug!("iterated to end");
                            self.page = None;
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
                        let prev_page = unsafe { &*ext_page.prev.get() };
                        if !prev_page.is_none() {
                            debug!("Shifting page backward");
                            self.page = Some(ext_page.prev.clone());
                            self.index = prev_page.len() - 1;
                        } else {
                            debug!("iterated to end");
                            self.page = None;
                            return false;
                        }
                    } else {
                        self.index -= 1;
                        debug!("Advancing cursor to index {}", self.index);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    fn current(&self) -> Option<&EntryKey> {
        if let &Some(ref page_ref) = &self.page {
            let page = unsafe { &*page_ref.get() };
            Some(&page.extnode().keys[self.index])
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
        Node::None
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
mod test {
    use super::{BPlusTree, Node, Ordering};
    use bifrost::utils::fut_exec::wait;
    use byteorder::BigEndian;
    use byteorder::WriteBytesExt;
    use client;
    use dovahkiin::types::custom_types::id::Id;
    use futures::future::Future;
    use hermes::stm::TxnValRef;
    use index::btree::NodeCellRef;
    use index::btree::NUM_KEYS;
    use index::Cursor;
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
    use std::io::Cursor as StdCursor;
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
        let debug_root = cascading_dump_node(&tree.root);
        let json = serde_json::to_string_pretty(&debug_root).unwrap();
        let mut file = File::create(f).unwrap();
        file.write_all(json.as_bytes());
    }

    fn cascading_dump_node(node: &NodeCellRef) -> DebugNode {
        unsafe {
            match &*node.get() {
                &Node::External(ref node) => {
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
                        next: Some(format!("{:?}", (&*node.next.get()).extnode().id)),
                        prev: Some(format!("{:?}", (&*node.prev.get()).extnode().id)),
                        len: node.len,
                        is_external: true,
                    };
                }
                &Node::Internal(ref innode) => {
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
                &Node::None => return DebugNode {
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
            for i in (0..num).rev() {
                let id = Id::new(0, i);
                let key_slice = u64_to_slice(i);
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
            for i in 0..deletion_volume {
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
                assert!(remove_succeed, "{}", i);
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
            assert_eq!(client.count().wait().unwrap(), 1);
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
}
