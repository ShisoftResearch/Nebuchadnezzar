use bifrost::utils::fut_exec::wait;
use bifrost_hasher::hash_bytes;
use byteorder::{LittleEndian, WriteBytesExt};
use client::AsyncClient;
use dovahkiin::types;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::{key_hash, Map, PrimitiveArray, ToValue, Value};
use futures::Future;
use hermes::stm::{Txn, TxnErr, TxnManager, TxnValRef};
pub use index::btree::cursor::*;
use index::btree::external::*;
use index::btree::internal::*;
pub use index::btree::merge::*;
pub use index::btree::node::*;
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
use parking_lot::RwLock;

mod cursor;
mod external;
mod internal;
mod merge;
mod node;

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
    root: RwLock<NodeCellRef>,
    root_versioning: NodeCellRef,
    storage: Arc<AsyncClient>,
    len: Arc<AtomicUsize>,
}

unsafe impl Sync for BPlusTree {}
unsafe impl Send for BPlusTree {}

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
            root: RwLock::new(Arc::new(Node::none())),
            root_versioning: NodeCellRef::new(Node::none()),
            storage: neb_client.clone(),
            len: Arc::new(AtomicUsize::new(0)),
        };
        let root_id = tree.new_page_id();
        *tree.root.write() = Arc::new(Node::new_external(root_id));
        return tree;
    }

    pub fn get_root(&self) -> NodeCellRef {
        self.root.read().clone()
    }

    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> RTCursor {
        let mut cursor = self.search(&self.get_root(), key, ordering);
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
            match node {
                &NodeData::External(ref n) => {
                    debug!(
                        "search in external for {:?}, len {}, content: {:?}",
                        key, n.len, n.keys
                    );
                    RTCursor::new(pos, node_ref, ordering)
                },
                &NodeData::Internal(ref n) => {
                    debug!("search in internal node for {:?}, len {}", key, n.len);
                    let next_node_ref = &n.ptrs[pos];
                    self.search(next_node_ref, key, ordering)
                },
                &NodeData::Empty(ref n) => {
                    self.search(&n.right, key, ordering)
                },
                &NodeData::None => {
                    RTCursor{
                        index: 0,
                        ordering,
                        page: None
                    }
                }
            }
        });
    }

    pub fn insert(&self, key: &EntryKey) {
        match self.insert_to_node(&self.get_root(), &self.root_versioning, &key, 0) {
            Some(split) => {
                debug!("split root with pivot key {:?}", split.pivot);
                let new_node = split.new_right_node;
                let pivot = split.pivot;
                let mut new_in_root = InNode {
                    keys: make_array!(NUM_KEYS, Default::default()),
                    ptrs: make_array!(NUM_PTRS, Default::default()),
                    right: Arc::new(Node::none()),
                    len: 1,
                };
                let mut old_root = self.get_root().clone();
                // check latched root and current root are the same node
                debug_assert_eq!(
                    old_root.read_unchecked().first_key(),
                    split.left_node_latch.first_key(),
                    "root verification failed, left node right node type: {}",
                    split.left_node_latch.innode().right.read_unchecked().type_name()
                );
                new_in_root.keys[0] = pivot;
                new_in_root.ptrs[0] = old_root;
                new_in_root.ptrs[1] = new_node;
                *self.root.write() = NodeCellRef::new(Node::new(NodeData::Internal(box new_in_root)));
            }
            None => {}
        }
        self.len.fetch_add(1, Relaxed);
    }
    fn insert_to_node(
        &self,
        node_ref: &NodeCellRef,
        parent: &NodeCellRef,
        key: &EntryKey,
        level: usize,
    ) -> Option<NodeSplit> {
        let mut search = node_ref.read(|node_handler| {
            debug!(
                "insert to node, len {}, external: {}",
                node_handler.len(),
                node_handler.is_ext()
            );
            if let Some(right_node) = node_handler.key_at_right_node(key) {
                debug!("Moving to right node for insertion");
                return InsertSearchResult::RightNode(right_node.clone());
            }
            match &**node_handler {
                &NodeData::External(ref node) => InsertSearchResult::External,
                &NodeData::Internal(ref node) => {
                    let pos = node.search(key);
                    let sub_node_ref = &node.ptrs[pos];
                    InsertSearchResult::Internal(sub_node_ref.clone())
                }
                &NodeData::Empty(ref node) => InsertSearchResult::RightNode(node.right.clone()),
                &NodeData::None => unreachable!(),
            }
        });
        let modification = match search {
            InsertSearchResult::RightNode(node) => {
                self.insert_to_node(&node, parent, key, level)
            }
            InsertSearchResult::External => {
                // latch nodes from left to right
                debug!("Obtain latch for external node");
                let node_guard = node_ref.write();
                let (mut searched_guard, _) = write_key_page(node_guard, node_ref, key);
                debug_assert!(
                    searched_guard.is_ext(),
                    "{:?}",
                    searched_guard.innode().keys
                );
                let mut split_result = searched_guard.extnode_mut().insert(
                    key,
                    self,
                    node_ref,
                    parent
                );
                if let &mut Some(ref mut split) = &mut split_result {
                    split.left_node_latch = searched_guard;
                }
                split_result
            }
            InsertSearchResult::Internal(sub_node) => {
                let split_res =
                    self.insert_to_node(&sub_node, node_ref, key, level + 1);
                match split_res {
                    None => None,
                    Some(split) => {
                        debug!(
                            "Sub level node split, shall insert new node to current level, pivot {:?}",
                            split.pivot
                        );
                        let pivot = split.pivot;
                        debug!("New pivot {:?}", pivot);
                        debug!("obtain latch for internal node split");
                        let mut self_guard = split.parent_latch;
                        let (mut target_guard, _) = write_key_page(self_guard, node_ref, &pivot);
                        debug_assert!(
                            split.new_right_node.read_unchecked().first_key() >= &pivot
                        );
                        let mut split_result = target_guard.innode_mut().insert(
                            pivot,
                            split.new_right_node,
                            parent
                        );
                        debug_assert!(
                            target_guard.first_key()
                                > target_guard.innode().ptrs[0].read_unchecked().first_key()
                        );
                        if let &mut Some(ref mut split) = &mut split_result {
                            split.left_node_latch = target_guard;
                        }
                        split_result
                    }
                }
            }
        };
        if level == 0 {
            if let &Some(ref split) = &modification {
                let current_root = self.get_root().clone();
                if  current_root.read_unchecked().first_key() != split.left_node_latch.first_key() &&
                    split.left_node_latch.has_vaild_right_node() {
                    // at this point, root split occurred when waiting for the latch
                    // the new right node should be inserted to any right node of the old root
                    // hopefully that node won't split again
                    let current_root_guard = current_root.write();
                    // at this point, the root may have split again, we need to search for the exact one
                    let (mut root_level_target, target_ref) = write_key_page(current_root_guard, &current_root, &split.pivot);
                    // assert this even in production
                    assert!(!root_level_target.is_ext());
                    return root_level_target.innode_mut().insert(split.pivot.clone(), split.new_right_node.clone(), parent);
                }
            }
        }
        modification
    }

    pub fn remove(&self, key: &EntryKey) -> bool {
        let mut root = self.get_root();
        let result = self.remove_from_node(&mut root, &mut key.clone(), &self.root_versioning, 0);
        if let Some(rebalance) = result.rebalancing {
            let root_node = rebalance.parent;
            if self.root.read().read_unchecked().innode().keys[0] == root_node.innode().keys[0] {
                // Make sure root node does not changed during the process. If it did changed, ignore it
                // When root is external and have no keys but one pointer will take the only sub level
                // pointer node as the new root node.
                let new_root = root_node.innode().ptrs[0].clone();
                *self.root.write() = new_root;
            }
        }
        if result.removed {
            self.len.fetch_sub(1, Relaxed);
        }
        result.removed
    }

    fn with_innode_removing<F>(
        &self,
        key: &EntryKey,
        mut rebalancing: RebalancingNodes,
        parent: &NodeCellRef,
        parent_parent: &NodeCellRef,
        removed: bool,
        level: usize,
        func: F) -> RemoveResult
        where F: Fn(&mut RebalancingNodes)
    {
        let parent_half_full = rebalancing.parent.is_half_full();
        let mut parent_right_guard = rebalancing.parent.innode_mut().right.write();
        let mut parent_remove_result = RemoveResult {
            rebalancing: None,
            removed,
            empty: false
        };

        let parent_parent_guard;
        let parent_parent_ref;
        let parent_parent_remove_pos;
        let parent_right_right_guard;
        if level == 0 {
            parent_parent_guard = parent_parent.write();
            parent_parent_ref = parent_parent.clone();
            parent_parent_remove_pos = 0;
            parent_right_right_guard = None;
        } else {
            let (pp_guard, pp_ref) = write_key_page(parent_parent.write(), parent_parent, key);
            parent_parent_guard = pp_guard;
            parent_parent_ref = pp_ref;
            parent_parent_remove_pos = parent_parent_guard.search(key);
            let parent_right_half_full = parent_right_guard.is_half_full();
            parent_right_right_guard = if !parent_half_full || !parent_right_half_full {
                // indicates whether the upper parent level need to relocated
                parent_right_guard.right_ref().map(|r| r.write())
            } else { None };
        }

        func(&mut rebalancing);
        if rebalancing.parent.innode().len == 0 {
            parent_remove_result.empty = true;
        }
        parent_remove_result.rebalancing = parent_right_right_guard.map(|parent_right_right_guard| {
            RebalancingNodes {
                left_guard: rebalancing.parent,
                left_ref: parent.clone(),
                right_right_guard: parent_right_right_guard,
                right_guard: parent_right_guard,
                parent: parent_parent_guard,
                parent_pos: parent_parent_remove_pos,
            }
        });
        parent_remove_result
    }

    fn remove_from_node(
        &self,
        node_ref: &mut NodeCellRef,
        key: &mut EntryKey,
        parent: &NodeCellRef,
        level: usize) -> RemoveResult
    {
        debug!("Removing {:?} from node, level {}", key, level);
        let mut search = node_ref.read(|node| {
            if let Some(right_node) = node.key_at_right_node(key) {
                return RemoveSearchResult::RightNode(right_node.clone());
            }
            match &**node {
                &NodeData::Internal(ref n) => {
                    let pos = n.search(key);
                    let sub_node = n.ptrs[pos].clone();
                    RemoveSearchResult::Internal(sub_node)
                }
                &NodeData::External(_) => RemoveSearchResult::External,
                &NodeData::Empty(ref n) => RemoveSearchResult::RightNode(n.right.clone()),
                &NodeData::None => unreachable!()
            }
        });
        match search {
            RemoveSearchResult::RightNode(mut node) => return self.remove_from_node(&mut node, key, parent, level),
            RemoveSearchResult::Internal(mut sub_node) => {
                let mut node_remove_res = self.remove_from_node(&mut sub_node, key, node_ref, level + 1);
                let removed = node_remove_res.removed;
                if let Some(mut rebalancing) = node_remove_res.rebalancing {
                    if node_remove_res.empty {
                        debug!("Remove {:?} sub level is empty, level {}", key, level);
                        Some(self.with_innode_removing(
                            key,
                            rebalancing,
                            &sub_node,
                            parent,
                            removed,
                            level,
                            |rebalancing| {
                                // Remove the empty node that have a right node with the same parent
                                // Because we cannot lock from left to right, we have to move the content
                                // of the right node to the left and remove the right node instead so the
                                // left right pointers can be modified
                                let mut left_node = &mut*rebalancing.left_guard;
                                let mut right_node = &mut*rebalancing.right_guard;
                                // swap the content of left and right, then delete right
                                // this procedure will prevent locking left node for changing right reference
                                let left_left_ref = left_node.left_ref_mut().map(|r| r.clone());
                                let right_right_ref = right_node.right_ref_mut().map(|r| r.clone());
                                let left_ref = rebalancing.left_ref.clone();
                                right_node.right_ref_mut().map(|mut r| *r = left_ref.clone());
                                // swap the empty node with the right node. In this case left node holds
                                // content of the right node but pointers need to be corrected.
                                mem::swap(left_node, right_node);
                                left_node.left_ref_mut().map(|r| *r = left_left_ref.unwrap());
                                left_node.right_ref_mut().map(|r| *r = right_right_ref.unwrap());
                                rebalancing.right_right_guard.left_ref_mut().map(|r| *r = left_ref);
                                // remove the left ptr
                                rebalancing.parent.remove(rebalancing.parent_pos);
                                // point the right ptr to the replaced sub node
                                rebalancing.parent.innode_mut().ptrs[rebalancing.parent_pos + 1] = sub_node.clone();
                            }))
                    } else if rebalancing.right_guard.is_empty() {
                        debug!("Remove {:?} sub level right node is empty, level {}", key, level);
                        Some(self.with_innode_removing(
                            key,
                            rebalancing,
                            &sub_node,
                            parent,
                            removed,
                            level,
                            |rebalancing| {
                                // There is a right empty node that can be deleted directly without hassle
                                let mut node_to_remove = &mut rebalancing.right_guard;
                                let owned_left_ref = rebalancing.left_ref.clone();
                                rebalancing.left_guard.right_ref_mut().map(|r| *r = node_to_remove.right_ref_mut().unwrap().clone());
                                rebalancing.right_right_guard.left_ref_mut().map(|r| *r = owned_left_ref);
                                rebalancing.parent.remove(rebalancing.parent_pos + 1);
                            }))
                    } else if rebalancing.left_guard.cannot_merge() || rebalancing.right_guard.cannot_merge() {
                        if rebalancing.left_guard.len() as f32 <= NUM_KEYS as f32 / 1.5 || rebalancing.right_guard.len() as f32 <= NUM_KEYS as f32 / 1.5 {
                            // Relocate the nodes with the same parent for balance.
                            debug!("Remove {:?} sub level need relocation, level {}", key, level);
                            let mut left_node = &mut*rebalancing.left_guard;
                            let mut right_node = &mut*rebalancing.right_guard;
                            let left_pos = rebalancing.parent_pos;
                            let right_pos = left_pos + 1;
                            rebalancing.parent.innode_mut().relocate_children(left_pos, right_pos, left_node, right_node);
                        }
                        None
                    } else {
                        // Nodes with the same parent can merge
                        debug!("Remove {:?} sub level need to be merged, level {}", key, level);
                        Some(self.with_innode_removing(
                            key,
                            rebalancing,
                            &sub_node,
                            parent,
                            removed,
                            level,
                            |rebalancing| {
                                let mut left_node = &mut *rebalancing.left_guard;
                                let mut right_node = &mut *rebalancing.right_guard;
                                let mut right_node_next = &mut *rebalancing.right_right_guard;
                                let left_pos = rebalancing.parent_pos;
                                let right_pos = left_pos + 1;
                                rebalancing.parent.innode_mut().merge_children(left_pos, right_pos, left_node, right_node, right_node_next);
                            }))
                    }
                } else {
                    None
                }.unwrap_or_else(|| {
                    RemoveResult {
                        rebalancing: None,
                        empty: false,
                        removed
                    }
                })
            }
            RemoveSearchResult::External => {
                let node_guard = node_ref.write();
                let (mut target_guard, target_ref) = write_key_page(node_guard, node_ref, key);
                let mut remove_result = RemoveResult {
                    rebalancing: None,
                    removed: false,
                    empty: false
                };
                {
                    let is_left_half_full = target_guard.is_half_full();
                    let mut node = target_guard.extnode_mut();
                    let pos = node.search(key);
                    let right_guard = node.next.write();
                    let is_right_half_full = right_guard.is_half_full();
                    if !is_left_half_full || !is_right_half_full {
                        let (parent_guard, _) = write_key_page(parent.write(), parent, key);
                        let parent_pos = parent_guard.search(key);
                        // Check if the right node is innode and its parent is the same as the left one
                        // because we have to lock from left to right, there is no way to lock backwards
                        // if the empty non half-full node is at the right most of its parent
                        // So left over imbalanced such nodes will be expected and they can be eliminate
                        // by their left node remove operations.
                        if !right_guard.is_none() && parent_pos < parent_guard.len() - 1 {
                            debug_assert!(right_guard.is_ext());
                            let right_right_guard = right_guard.extnode().next.write();
                            let rebalacing = RebalancingNodes {
                                left_guard: NodeWriteGuard::default(),
                                left_ref: target_ref.clone(),
                                parent: parent_guard,
                                parent_pos,
                                right_right_guard,
                                right_guard
                            };
                            remove_result.rebalancing = Some(rebalacing);
                        }
                    }
                    if pos >= node.len {
                        debug!(
                            "Removing pos overflows external node, pos {}, len {}, expecting key {:?}",
                            pos, node.len, key
                        );
                        remove_result.removed = false;
                    }
                    if &node.keys[pos] == key {
                        node.remove_at(pos);
                        remove_result.removed = true;
                    } else {
                        debug!(
                            "Search check failed for remove at pos {}, expecting {:?}, actual {:?}",
                            pos, key, &node.keys[pos]
                        );
                        remove_result.removed = false;;
                    }
                    remove_result.empty = node.len == 0;
                }
                if let &mut Some(ref mut rebalancing) = &mut remove_result.rebalancing {
                    rebalancing.left_guard = target_guard;
                }
                remove_result
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

macro_rules! impl_btree_slice {
    ($t: ty, $et: ty, $n: expr) => {
        impl_slice_ops!($t, $et, $n);
        impl BTreeSlice<$et> for $t {}
    };
}

impl_btree_slice!(EntryKeySlice, EntryKey, NUM_KEYS);
impl_btree_slice!(NodePtrCellSlice, NodeCellRef, NUM_PTRS);

pub trait BTreeSlice<T>: Sized + Slice<Item = T>
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
                mem::swap(&mut slice1[i], &mut slice2[right_pos]);
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
    use super::*;
    use bifrost::utils::fut_exec::wait;
    use byteorder::BigEndian;
    use byteorder::WriteBytesExt;
    use client;
    use dovahkiin::types::custom_types::id::Id;
    use futures::future::Future;
    use hermes::stm::TxnValRef;
    use index::btree::node::*;
    use index::btree::NodeCellRef;
    use index::btree::NodeData;
    use index::btree::NUM_KEYS;
    use index::Cursor;
    use index::EntryKey;
    use index::{id_from_key, key_with_id};
    use itertools::Itertools;
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
        let debug_root = cascading_dump_node(&tree.get_root());
        let json = serde_json::to_string_pretty(&debug_root).unwrap();
        let mut file = File::create(f).unwrap();
        file.write_all(json.as_bytes());
    }

    fn cascading_dump_node(node: &NodeCellRef) -> DebugNode {
        unsafe {
            match &*node.read_unchecked() {
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
                        next: Some(format!("{:?}", node.next.read_unchecked().ext_id())),
                        prev: Some(format!("{:?}", node.prev.read_unchecked().ext_id())),
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
                &NodeData::Empty(ref n) => {
                    return DebugNode {
                        keys: vec![String::from("<EMPTY>")],
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

    fn check_ordering(tree: &BPlusTree, key: &EntryKey) {
        let mut cursor = tree.seek(&smallvec!(0), Ordering::Forward);
        let mut last_key = cursor.current().unwrap().clone();
        while cursor.next() {
            let current = cursor.current().unwrap();
            if &last_key > current {
                dump_tree(tree, "error_insert_dump.json");
                panic!(
                    "error on ordering check {:?} > {:?}, key {:?}",
                    last_key, current, key
                );
            }
            last_key = current.clone();
        }
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
        ::std::fs::remove_dir_all("dumps");
        ::std::fs::create_dir_all("dumps");
        let num = env::var("BTREE_TEST_ITEMS")
            .unwrap_or("1000".to_string())
            .parse::<u64>()
            .unwrap();
        {
            info!("test insertion");
            let mut nums = (0..num).collect_vec();
            thread_rng().shuffle(nums.as_mut_slice());
            let json = serde_json::to_string(&nums).unwrap();
            let mut file = File::create("nums_dump.json").unwrap();
            file.write_all(json.as_bytes());
            let mut i = 0;
            for n in nums {
                let id = Id::new(0, n);
                let key_slice = u64_to_slice(n);
                let key = SmallVec::from_slice(&key_slice);
                debug!("{}. insert id: {}", i, n);
                let mut entry_key = key.clone();
                key_with_id(&mut entry_key, &id);
                tree.insert(&entry_key);
                check_ordering(&tree, &entry_key);
                i += 1;
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
                // dump_tree(&tree, &format!("removing_{}_dump.json", i));
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
        let server_group = "b_plus_index_init";
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
                "B+ Tree now have {}/{} elements, total {:.2}%",
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
        debug!("Start validation");

        let mut rng = rand::rngs::OsRng::new().unwrap();
        let die_range = Uniform::new_inclusive(1, 6);
        let roll_die = RwLock::new(rng.sample_iter(&die_range));
        (0..num).collect::<Vec<_>>().par_iter().for_each(|i| {
            let i = *i;
            let id = Id::new(0, i);
            let key_slice = u64_to_slice(i);
            let mut key = SmallVec::from_slice(&key_slice);
            debug!("checking: {}", i);
            let mut cursor = tree.seek(&key, Ordering::Forward);
            key_with_id(&mut key, &id);
            assert_eq!(cursor.current(), Some(&key), "{}", i);
            if roll_die.write().next().unwrap() == 6 {
                debug!("Scanning {}", num);
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

    #[test]
    fn node_lock() {
        env_logger::init();
        let server_group = "node_lock_test";
        let server_addr = String::from("127.0.0.1:5610");
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
        let client =
            Arc::new(AsyncClient::new(&server.rpc, &vec![server_addr], server_group).unwrap());
        let tree = BPlusTree::new(&client);
        let node = Arc::new(Node::new(NodeData::External(box ExtNode::new(Id::new(
            1, 2,
        )))));
        let num = 100000;
        let mut nums = (0..num).collect_vec();
        let dummy_node = NodeCellRef::new(Node::none());
        thread_rng().shuffle(nums.as_mut_slice());
        nums.par_iter().for_each(|num| {
            let key_slice = u64_to_slice(*num);
            let mut key = SmallVec::from_slice(&key_slice);
            let mut guard = node.write();
            let mut ext_node = guard.extnode_mut();
            ext_node.insert(&key, &tree, &node, &dummy_node);
        });
        let read = node.read_unchecked();
        let extnode = read.extnode();
        for i in 0..read.len() - 1 {
            assert!(extnode.keys[i] < extnode.keys[i + 1]);
        }
        assert_eq!(node.version(), num as usize);
    }
}
