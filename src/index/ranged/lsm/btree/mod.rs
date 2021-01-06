use crate::client::AsyncClient;
use crate::index::ranged::lsm::tree::DeletionSet;
pub use crate::index::ranged::trees::*;
use crate::ram::types::RandValue;
pub use cell_ref::NodeCellRef;
pub use cursor::*;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::{key_hash, PrimitiveArray, Value};
pub use external::page_schema;
use external::*;
use futures::future::BoxFuture;
use futures::FutureExt;
use insert::*;
use internal::*;
use itertools::Itertools;
use level::LEVEL_TREE_DEPTH;
use merge::*;
pub use node::*;
use parking_lot::RwLock;
use search::*;
use std::any::Any;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

pub mod cell_ref;
mod cursor;
mod dump;
mod external;
mod insert;
mod internal;
pub mod level;
mod merge;
mod node;
// mod prune;
mod reconstruct;
mod search;
mod split;
pub mod storage;
pub mod verification;
#[macro_use]
pub mod marco;

// Items can be added in real-time
// It is not supposed to hold a lot of items when it is actually feasible
// There will be a limit for maximum items in ths data structure, when the limit exceeds, higher ordering
// items with number of one page will be merged to next level
pub struct BPlusTree<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    root: RwLock<NodeCellRef>,
    root_versioning: NodeCellRef,
    head_page_id: Id,
    len: AtomicUsize,
    pub deletion: Arc<DeletionSet>,
    marker: PhantomData<(KS, PS)>,
}

unsafe impl<KS, PS> Sync for BPlusTree<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
}

unsafe impl<KS, PS> Send for BPlusTree<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
}

impl Default for Ordering {
    fn default() -> Self {
        Ordering::Forward
    }
}

impl<KS, PS> BPlusTree<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn new(deletion: &Arc<DeletionSet>) -> BPlusTree<KS, PS> {
        trace!("Creating B+ Tree, with capacity {}", KS::slice_len());
        let mut tree = BPlusTree {
            root: RwLock::new(NodeCellRef::new(Node::<KS, PS>::new(NodeData::None))),
            root_versioning: NodeCellRef::new(Node::<KS, PS>::new(NodeData::None)),
            head_page_id: Id::unit_id(),
            len: AtomicUsize::new(0),
            marker: PhantomData,
            deletion: deletion.clone(),
        };
        let root_id = Self::new_page_id();
        let max_key = max_entry_key();
        trace!("New External L1");
        let root_inner = Node::<KS, PS>::new_external(root_id, max_key);
        trace!("B+ Tree created");
        *tree.root.write() = NodeCellRef::new(root_inner);
        tree.head_page_id = root_id;
        return tree;
    }

    pub async fn persist_root(&self, neb: &Arc<crate::client::AsyncClient>) {
        let root = self.get_root();
        root.persist(&self.deletion, &neb).await
    }

    pub async fn from_head_id(
        head_id: &Id,
        neb: &AsyncClient,
        deletion: &Arc<DeletionSet>,
    ) -> Self {
        reconstruct::reconstruct_from_head_id(*head_id, neb, deletion).await
    }

    pub fn from_root(
        root: NodeCellRef,
        head_id: Id,
        len: usize,
        deletion: &Arc<DeletionSet>,
    ) -> Self {
        BPlusTree {
            root: RwLock::new(root),
            root_versioning: NodeCellRef::default(),
            head_page_id: head_id,
            len: AtomicUsize::new(len),
            marker: PhantomData,
            deletion: deletion.clone(),
        }
    }

    pub fn get_root(&self) -> NodeCellRef {
        self.root.read().clone()
    }

    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> RTCursor<KS, PS> {
        search_node(&self.get_root(), key, ordering)
    }

    pub fn insert(&self, key: &EntryKey) -> bool {
        match insert_to_tree_node(&self, &self.get_root(), &self.root_versioning, &key, 0) {
            Some(Some(split)) => {
                trace!("split root with pivot key {:?}", split.pivot);
                let new_node = split.new_right_node;
                let pivot = split.pivot;
                let mut new_in_root: Box<InNode<KS, PS>> = InNode::new(1, max_entry_key());
                let old_root = self.get_root().clone();
                new_in_root.keys.as_slice()[0] = pivot;
                new_in_root.ptrs.as_slice()[0] = old_root;
                new_in_root.ptrs.as_slice()[1] = new_node;
                *self.root.write() = NodeCellRef::new(Node::new(NodeData::Internal(new_in_root)));
            }
            Some(None) => {}
            None => return false,
        }
        self.len.fetch_add(1, Relaxed);
        return true;
    }

    pub fn merge_with_keys_(&self, keys: Box<Vec<EntryKey>>) {
        let keys_len = keys.len();
        if keys.len() == 0 {
            warn!("Merge attempt with no keys");
            return;
        }
        let root = self.get_root();
        debug!("Performing merging in sub levels with {} keys", keys.len());
        let root_new_pages = merge_into_tree_node(self, &root, &self.root_versioning, keys, 0);
        debug!(
            "Sub level merge completed, have {} new pages for root",
            root_new_pages.len()
        );
        if root_new_pages.len() > 0 {
            if cfg!(debug_assertions) {
                let root_serial = verification::is_node_serial(&write_node::<KS, PS>(&self.get_root()));
                if !root_serial { 
                    error!("root serial verification failed before merge root split");
                    unreachable!();
                }
                let page_keys = root_new_pages
                    .iter()
                    .map(|t| t.0.clone())
                    .collect_vec();
                let page_keys_serial = verification::are_keys_serial(page_keys.as_slice());
                if !page_keys_serial {
                    error!("Page first keys are not serial before merge root split {:?}", page_keys);
                    unreachable!();
                }
            }
            debug_assert!(
                root.ptr_eq(&self.get_root()),
                "Merge target tree should always have a persistent root unless merge split"
            );
            let root_guard = write_node::<KS, PS>(&root);
            debug_assert!(*root_guard.node_ref() == self.get_root());
            info!(
                "Radical merge split for root (may need to split more than once), {} to {}, num keys {}", 
                root_new_pages.len() + 1,
                KS::slice_len(),
                keys_len
            );
            let mut new_pages = root_new_pages;
            let mut left_most_page = root;
            if new_pages.is_empty() {
                // The root is still intact and does not need to be splitted
                // Nothing need to do here
            } else {
                // In the current root level we have more than one root, should generate new levels
                loop {
                    // Need generate and rearrange new pages
                    // First, generate a innode with one key and two pointers
                    // The first pointer of the innode is original node (can be the root)
                    let new_node_ref = new_internal_node::<KS, PS>(&left_most_page, &mut new_pages);
                    let mut this_level_new_pages = vec![];
                    if !new_pages.is_empty() {
                        merge_into_internal::<KS, PS>(
                            &new_node_ref,
                            new_pages,
                            &mut this_level_new_pages,
                        );
                    }
                    if this_level_new_pages.is_empty() {
                        // All sub pages merged into the new page, should set the page as root and break
                        *self.root.write() = new_node_ref;
                        break;
                    } else {
                        // Have new pages to generate a new level
                        new_pages = box this_level_new_pages;
                        left_most_page = new_node_ref;
                    }
                }
            }
            debug_assert!(
                verification::is_node_serial(&write_node::<KS, PS>(&self.get_root())),
                "verification failed after merge root split"
            );
        }
        self.len.fetch_add(keys_len, Relaxed);
    }

    pub fn flush_all(&self) {
        // unimplemented!()
    }

    pub fn len(&self) -> usize {
        self.len.load(Relaxed)
    }

    fn new_page_id() -> Id {
        // TODO: achieve locality
        Id::rand()
    }
}

pub trait LevelTree: Sync + Send {
    fn size(&self) -> usize;
    fn count(&self) -> usize;
    fn merge_to<'a>(
        &'a self,
        level: usize,
        target: &'a dyn LevelTree,
        prune: bool,
    ) -> usize;
    fn merge_all_to<'a>(
        &'a self,
        level: usize,
        target: &'a dyn LevelTree,
        prune: bool,
    ) -> usize;
    fn merge_with_keys(&self, keys: Box<Vec<EntryKey>>);
    fn insert_into(&self, key: &EntryKey) -> bool;
    fn seek_for(&self, key: &EntryKey, ordering: Ordering) -> Box<dyn Cursor>;
    fn dump(&self, f: &str);
    fn mid_key(&self) -> Option<EntryKey>;
    fn head_id(&self) -> Id;
    fn verify(&self, level: usize) -> bool;
    fn ideal_capacity(&self) -> usize {
        ideal_capacity_from_node_size(self.size())
    }
    fn oversized(&self) -> bool {
        self.count() > self.ideal_capacity()
    }
}

pub fn ideal_capacity_from_node_size(size: usize) -> usize {
    size.pow(LEVEL_TREE_DEPTH)
}

impl<KS, PS> LevelTree for BPlusTree<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    fn size(&self) -> usize {
        KS::slice_len()
    }

    fn count(&self) -> usize {
        self.len()
    }

    fn merge_to<'a>(
        &'a self,
        level: usize,
        target: &'a dyn LevelTree,
        prune: bool,
    ) -> usize {
        level::level_merge(level, self, target, prune)
    }

    fn merge_all_to<'a>(
        &'a self,
        level: usize,
        target: &'a dyn LevelTree,
        prune: bool,
    ) -> usize {
        level::merge_with_boundary(level, self, target, &*MAX_ENTRY_KEY, prune)
    }

    fn merge_with_keys(&self, keys: Box<Vec<EntryKey>>) {
        self.merge_with_keys_(keys)
    }

    fn insert_into(&self, key: &EntryKey) -> bool {
        self.insert(key)
    }

    fn seek_for(&self, key: &EntryKey, ordering: Ordering) -> Box<dyn Cursor> {
        box self.seek(key, ordering)
    }

    fn dump(&self, f: &str) {
        dump::dump_tree(self, f);
    }

    fn mid_key(&self) -> Option<EntryKey> {
        split::mid_key::<KS, PS>(&self.get_root())
    }

    fn head_id(&self) -> Id {
        self.head_page_id
    }

    fn verify(&self, level: usize) -> bool {
        verification::is_tree_in_order(self, level)
    }
}

pub struct DummyLevelTree;

impl LevelTree for DummyLevelTree {
    fn size(&self) -> usize {
        unreachable!()
    }

    fn count(&self) -> usize {
        unreachable!()
    }

    fn merge_to<'a>(
        &'a self,
        _level: usize,
        _target: &'a dyn LevelTree,
        _prune: bool,
    ) -> usize {
        unreachable!()
    }

    fn merge_all_to<'a>(
        &'a self,
        _level: usize,
        _target: &'a dyn LevelTree,
        _prune: bool,
    ) -> usize {
        unreachable!()
    }

    fn merge_with_keys(&self, _keys: Box<Vec<EntryKey>>) {
        unreachable!()
    }

    fn insert_into(&self, _key: &EntryKey) -> bool {
        unreachable!()
    }

    fn seek_for(&self, _key: &EntryKey, _ordering: Ordering) -> Box<dyn Cursor> {
        unreachable!()
    }

    fn dump(&self, _f: &str) {
        unreachable!()
    }

    fn mid_key(&self) -> Option<EntryKey> {
        unreachable!()
    }

    fn head_id(&self) -> Id {
        unreachable!()
    }

    fn verify(&self, _level: usize) -> bool {
        unreachable!()
    }
}

impl_slice_ops!([EntryKey; 0], EntryKey, 0);
impl_slice_ops!([NodeCellRef; 0], NodeCellRef, 0);

#[cfg(test)]
pub mod test;
