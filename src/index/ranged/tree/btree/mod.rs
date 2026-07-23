use crate::client::AsyncClient;
use crate::index::ranged::tree::tree::DeletionSet;
pub use crate::index::ranged::trees::*;
use crate::ram::types::RandValue;
pub use cell_ref::NodeCellRef;
pub use cursor::*;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::key_hash;
use external::*;
pub use external::{page_schema, PAGE_SCHEMA_ID};
use futures::future::BoxFuture;
use insert::*;
use internal::*;
use itertools::Itertools;

use merge::*;
pub use node::*;
use parking_lot::RwLock;
use search::*;
use std::any::Any;
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering::*};
use std::sync::Arc;

pub mod cell_ref;
mod clear;
mod cursor;
mod dump;
mod external;
mod insert;
mod internal;
pub mod leaf_keys;
pub mod level;
mod merge;
mod node;
mod reconstruct;
mod search;
mod split;
pub mod split_off;
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
    height: AtomicUsize,
    pub deletion: Arc<DeletionSet>,
    writeback_client: Option<Arc<AsyncClient>>,
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
            height: AtomicUsize::new(0),
            marker: PhantomData,
            deletion: deletion.clone(),
            writeback_client: None,
        };
        let root_id = Self::new_page_id();
        let max_key = max_entry_key();
        debug!("Created B-tree with {:?}", root_id);
        let root_inner = Node::<KS, PS>::new_external(root_id, max_key);
        trace!("B+ Tree created");
        *tree.root.write() = NodeCellRef::new(root_inner);
        tree.head_page_id = root_id;
        return tree;
    }

    pub fn new_with_client(
        deletion: &Arc<DeletionSet>,
        client: &Arc<AsyncClient>,
    ) -> BPlusTree<KS, PS> {
        let mut tree = Self::new(deletion);
        tree.writeback_client = Some(client.clone());
        tree
    }

    // Non-atomic
    pub fn clear(&self) {
        let new_node = NodeCellRef::new(Node::<KS, PS>::new_external(
            self.head_page_id,
            max_entry_key(),
        ));
        let old_node = mem::replace(&mut *self.root.write(), new_node);
        self.len.store(0, Release);
        self.height.store(0, Release);
        clear::clear_by_node::<KS, PS>(&old_node);
    }

    pub async fn persist_root(&self, neb: &Arc<crate::client::AsyncClient>) {
        let root = self.get_root();
        root.persist(&self.deletion, &neb).await
    }

    pub async fn from_head_id(
        head_id: &Id,
        neb: &AsyncClient,
        deletion: &Arc<DeletionSet>,
        level: usize,
    ) -> Result<Self, reconstruct::ReconstructError> {
        reconstruct::reconstruct_from_head_id(*head_id, neb, deletion, level).await
    }

    pub fn from_root(
        root: NodeCellRef,
        head_id: Id,
        len: usize,
        height: usize,
        deletion: &Arc<DeletionSet>,
    ) -> Self {
        BPlusTree {
            root: RwLock::new(root),
            root_versioning: NodeCellRef::default(),
            head_page_id: head_id,
            len: AtomicUsize::new(len),
            height: AtomicUsize::new(height),
            marker: PhantomData,
            deletion: deletion.clone(),
            writeback_client: None,
        }
    }

    pub fn set_writeback_client(&mut self, client: &Arc<AsyncClient>) {
        self.writeback_client = Some(client.clone());
    }

    pub fn writeback_client(&self) -> Option<Arc<AsyncClient>> {
        self.writeback_client.clone()
    }

    pub fn get_root(&self) -> NodeCellRef {
        self.root.read().clone()
    }

    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> RTCursor<KS, PS> {
        search_node(&self.get_root(), key, ordering, &self.deletion, true)
    }

    /// Approximate median key, found by descending the tree (O(height))
    /// instead of walking a cursor count/2 steps (O(n)). Returns None only
    /// for an empty tree. Used to pick a balanced migration split point.
    pub fn mid_key(&self) -> Option<EntryKey> {
        level::select_merge_boundary::<KS, PS>(&self.get_root())
    }

    pub(crate) fn seek_raw(&self, key: &EntryKey, ordering: Ordering) -> RTCursor<KS, PS> {
        search_node(&self.get_root(), key, ordering, &self.deletion, false)
    }

    pub(crate) fn mark_changed(&self, node: &NodeCellRef) {
        external::make_changed(node, self);
    }

    pub(crate) fn increment_visible_len(&self) {
        self.len.fetch_add(1, Relaxed);
    }

    pub(crate) fn decrement_visible_len(&self) {
        self.len.fetch_sub(1, Relaxed);
    }

    pub fn insert(&self, key: &EntryKey) -> bool {
        match insert_to_tree_node(&self, &self.get_root(), &self.root_versioning, &key, 0) {
            Some(Some(split)) => apply_top_level_split(self, split),
            Some(None) => {}
            None => return false,
        }
        self.len.fetch_add(1, Relaxed);
        return true;
    }

    pub fn merge_with_keys_(&self, keys: Vec<EntryKey>) {
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
                let root_serial =
                    verification::is_node_serial(&write_node::<KS, PS>(&self.get_root()));
                if !root_serial {
                    error!("root serial verification failed before merge root split");
                    unreachable!();
                }
                let page_keys = root_new_pages.iter().map(|t| t.0.clone()).collect_vec();
                let page_keys_serial = verification::are_keys_serial(page_keys.as_slice());
                if !page_keys_serial {
                    error!(
                        "Page keys are not serial before merge root split {:?}",
                        page_keys
                    );
                    unreachable!();
                }
            }
            info!(
                "Radical merge split for root (may need to split more than once), {} to {}, num keys {}",
                root_new_pages.len() + 1,
                KS::slice_len(),
                keys_len
            );
            // The merged pages are already linked into the sibling chains, so
            // they are searchable through B-link pointers regardless of what
            // happens to the root. Build the new upper levels against an
            // observed root using only fresh, unshared nodes, then install
            // under the root-versioning latch if the root has not moved.
            // Installation races with insert-driven root splits
            // (apply_top_level_split), which take the same latch. Modeled in
            // docs/tla/BLinkInsert.tla (the merger process).
            loop {
                let observed_root = self.get_root();
                let mut new_pages = root_new_pages.clone();
                let mut left_most_page = observed_root.clone();
                let mut new_levels = 0usize;
                let new_top = loop {
                    // Generate an innode; its first pointer is the previous
                    // level's left-most node (initially the observed root).
                    let new_node_ref = new_internal_node::<KS, PS>(&left_most_page, &mut new_pages);
                    let mut this_level_new_pages = BTreeMap::new();
                    if !new_pages.is_empty() {
                        merge_into_internal::<KS, PS>(
                            &new_node_ref,
                            new_pages,
                            &mut this_level_new_pages,
                        );
                    }
                    new_levels += 1;
                    if this_level_new_pages.is_empty() {
                        break new_node_ref;
                    }
                    new_pages = this_level_new_pages;
                    left_most_page = new_node_ref;
                };
                // No other latch is held here, so holding root_versioning
                // cannot participate in a latch cycle.
                let _root_ver_guard = write_node::<KS, PS>(&self.root_versioning);
                if observed_root.ptr_eq(&self.get_root()) {
                    *self.root.write() = new_top;
                    for _ in 0..new_levels {
                        self.height.fetch_add(1, AcqRel);
                    }
                    break;
                }
                // An insert installed a new root while the scaffolding was
                // being built; discard it and rebuild against the new root.
                warn!("Bulk merge lost a root install race; rebuilding upper levels");
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

    pub fn _height(&self) -> usize {
        self.height.load(Relaxed)
    }

    fn new_page_id() -> Id {
        // TODO: achieve locality
        Id::rand()
    }
}

impl<KS, PS> Drop for BPlusTree<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    fn drop(&mut self) {
        clear::clear_by_node::<KS, PS>(&*self.root.read());
    }
}

pub trait LevelTree: Sync + Send {
    fn size(&self) -> usize;
    fn count(&self) -> usize;
    fn height(&self) -> usize;
    fn merge_to<'a>(
        &'a self,
        level: usize,
        target: &'a dyn LevelTree,
        deleted: &mut HashSet<EntryKey>,
        prune: bool,
    ) -> usize;
    fn merge_all_to<'a>(
        &'a self,
        level: usize,
        target: &'a dyn LevelTree,
        deleted: &mut HashSet<EntryKey>,
        prune: bool,
    ) -> usize;
    fn merge_with_keys(&self, keys: Vec<EntryKey>);
    fn retain_by_key(&self, key: &EntryKey);
    fn insert_into(&self, key: &EntryKey) -> bool;
    fn seek_for(&self, key: &EntryKey, ordering: Ordering) -> Box<dyn Cursor>;
    fn dump(&self, f: &str);
    fn head_id(&self) -> Id;
    fn verify(&self, level: usize) -> bool;
    fn ideal_capacity(&self) -> usize {
        ideal_capacity_from_node_size(self.size())
    }
    fn oversized(&self) -> bool {
        self.count() > self.ideal_capacity()
    }
    fn root(&self) -> NodeCellRef;
    fn clear_tree(&self);
    fn last_node_digest(&self, node: &NodeCellRef) -> Option<(usize, NodeCellRef, EntryKey)>;
}

// How many B+ tree levels a tree may fill before it is considered oversized
// and migrates. A fixed depth of 2 (size^2 ~= 16K keys for a 128-fanout node)
// split large datasets into tens of thousands of tiny trees, so migration
// churn dominated. Depth 3 (~2M keys) is still a shallow, fast tree but needs
// ~120x fewer migrations for a billion keys. Default is read once from
// NEB_TREE_DEPTH (clamped to 2..=4); set_tree_depth overrides it (tests that
// must trigger migration at small scale pin it to 2).
static TREE_DEPTH: AtomicU32 = AtomicU32::new(0);

fn resolve_tree_depth() -> u32 {
    let cached = TREE_DEPTH.load(Relaxed);
    if cached != 0 {
        return cached;
    }
    // Default depth 3 (~2M keys): with the spine-structural split each
    // migration is ~O(log n) regardless of tree size and the marker window is
    // decoupled from the global write-back drain, so large shards no longer
    // stall. Fewer, larger shards mean ~120x fewer migrations for a billion
    // keys. Tunable via NEB_TREE_DEPTH (clamped 2..=4).
    let d = std::env::var("NEB_TREE_DEPTH")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .map(|d| d.clamp(2, 4))
        .unwrap_or(3);
    TREE_DEPTH.store(d, Relaxed);
    d
}

/// Override the migration depth threshold (clamped 2..=4). Used by tests to
/// force migration at a small key count, and available for runtime tuning.
pub fn set_tree_depth(depth: u32) {
    TREE_DEPTH.store(depth.clamp(2, 4), Relaxed);
}

pub fn ideal_capacity_from_node_size(size: usize) -> usize {
    size.pow(resolve_tree_depth())
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

    fn height(&self) -> usize {
        self._height()
    }

    fn merge_to<'a>(
        &'a self,
        level: usize,
        target: &'a dyn LevelTree,
        deleted: &mut HashSet<EntryKey>,
        prune: bool,
    ) -> usize {
        level::level_merge(level, self, target, deleted, prune)
    }

    fn merge_all_to<'a>(
        &'a self,
        level: usize,
        target: &'a dyn LevelTree,
        deleted: &mut HashSet<EntryKey>,
        prune: bool,
    ) -> usize {
        level::merge_with_boundary(level, self, target, &*MAX_ENTRY_KEY, deleted, prune)
    }

    fn merge_with_keys(&self, keys: Vec<EntryKey>) {
        self.merge_with_keys_(keys)
    }

    fn insert_into(&self, key: &EntryKey) -> bool {
        self.insert(key)
    }

    fn seek_for(&self, key: &EntryKey, ordering: Ordering) -> Box<dyn Cursor> {
        Box::new(self.seek(key, ordering))
    }

    fn dump(&self, f: &str) {
        dump::dump_tree(self, f);
    }

    fn head_id(&self) -> Id {
        self.head_page_id
    }

    fn verify(&self, level: usize) -> bool {
        verification::is_tree_in_order(self, level)
    }
    fn root(&self) -> NodeCellRef {
        self.get_root()
    }
    fn retain_by_key(&self, key: &EntryKey) {
        split::retain(self, key);
    }
    fn clear_tree(&self) {
        self.clear()
    }
    fn last_node_digest(&self, node: &NodeCellRef) -> Option<(usize, NodeCellRef, EntryKey)> {
        split::last_node_prev_digest::<KS, PS>(node)
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
        _deleted: &mut HashSet<EntryKey>,
        _prune: bool,
    ) -> usize {
        unreachable!()
    }

    fn merge_all_to<'a>(
        &'a self,
        _level: usize,
        _target: &'a dyn LevelTree,
        _deleted: &mut HashSet<EntryKey>,
        _prune: bool,
    ) -> usize {
        unreachable!()
    }

    fn merge_with_keys(&self, _keys: Vec<EntryKey>) {
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

    fn head_id(&self) -> Id {
        unreachable!()
    }

    fn verify(&self, _level: usize) -> bool {
        unreachable!()
    }

    fn root(&self) -> NodeCellRef {
        unreachable!()
    }
    fn retain_by_key(&self, _key: &EntryKey) {
        unreachable!()
    }
    fn clear_tree(&self) {
        unreachable!()
    }
    fn last_node_digest(&self, _node: &NodeCellRef) -> Option<(usize, NodeCellRef, EntryKey)> {
        unreachable!()
    }

    fn height(&self) -> usize {
        unreachable!()
    }
}

impl_slice_ops!([EntryKey; 0], EntryKey, 0);
impl_slice_ops!([NodeCellRef; 0], NodeCellRef, 0);

#[cfg(test)]
pub mod audit_test;
#[cfg(test)]
pub mod bench_test;
#[cfg(test)]
pub mod test;
