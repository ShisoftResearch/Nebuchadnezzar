use crate::client::AsyncClient;
pub use crate::index::ranged::trees::*;
use crate::ram::types::RandValue;
pub use cursor::*;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::{key_hash, PrimitiveArray, Value};
pub use external::page_schema;
use external::*;
use futures::future::BoxFuture;
use insert::*;
use internal::*;
use itertools::Itertools;
use level::LEVEL_TREE_DEPTH;
use lightning::map::HashSet;
use merge::merge_into_tree_node;
pub use node::*;
use parking_lot::RwLock;
use search::*;
use split::remove_to_right;
use std::any::Any;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed, Ordering::SeqCst};
use std::sync::Arc;

mod cursor;
mod dump;
mod external;
mod insert;
mod internal;
pub mod level;
mod merge;
mod node;
mod prune;
mod reconstruct;
mod remove;
mod search;
mod split;
pub mod storage;
pub mod verification;
#[macro_use]
pub mod marco;

const DEL_SET_CAP: usize = 16;

pub type DeletionSetInneer = HashSet<EntryKey>;
pub type DeletionSet = Arc<DeletionSetInneer>;

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
    deleted: DeletionSet,
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
    pub fn new() -> BPlusTree<KS, PS> {
        debug!("Creating B+ Tree, with capacity {}", KS::slice_len());
        let mut tree = BPlusTree {
            root: RwLock::new(NodeCellRef::new(Node::<KS, PS>::with_none())),
            root_versioning: NodeCellRef::new(Node::<KS, PS>::with_none()),
            head_page_id: Id::unit_id(),
            len: AtomicUsize::new(0),
            deleted: Arc::new(DeletionSetInneer::with_capacity(DEL_SET_CAP)),
            marker: PhantomData,
        };
        let root_id = Self::new_page_id();
        let max_key = max_entry_key();
        debug!("New External L1");
        let root_inner = Node::<KS, PS>::new_external(root_id, max_key);
        debug!("B+ Tree created");
        *tree.root.write() = NodeCellRef::new(root_inner);
        tree.head_page_id = root_id;
        return tree;
    }

    pub async fn persist_root(&self, neb: &Arc<crate::client::AsyncClient>) {
        let root = self.get_root();
        root.persist(&self.deleted, &neb).await
    }

    pub async fn from_head_id(head_id: &Id, neb: &AsyncClient) -> Self {
        reconstruct::reconstruct_from_head_id(*head_id, neb).await
    }

    pub fn from_root(root: NodeCellRef, head_id: Id, len: usize) -> Self {
        BPlusTree {
            root: RwLock::new(root),
            root_versioning: NodeCellRef::new(Node::<KS, PS>::with_none()),
            head_page_id: head_id,
            len: AtomicUsize::new(len),
            deleted: Arc::new(DeletionSetInneer::with_capacity(DEL_SET_CAP)),
            marker: PhantomData,
        }
    }

    pub fn get_root(&self) -> NodeCellRef {
        self.root.read().clone()
    }

    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> RTCursor<KS, PS> {
        search_node(&self.get_root(), key, ordering, &self.deleted)
    }

    pub fn insert(&self, key: &EntryKey) -> bool {
        match insert_to_tree_node(&self, &self.get_root(), &self.root_versioning, &key, 0) {
            Some(Some(split)) => {
                debug!("split root with pivot key {:?}", split.pivot);
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
        self.deleted.remove(key);
        return true;
    }

    pub fn merge_with_keys_(&self, keys: Box<Vec<EntryKey>>) {
        let keys_len = keys.len();
        let root = self.get_root();
        let root_new_pages = merge_into_tree_node(self, &root, &self.root_versioning, keys, 0);
        if root_new_pages.len() > 0 {
            debug!("Merge have a root node split");
            debug_assert!(
                verification::is_node_serial(&write_node::<KS, PS>(&self.get_root())),
                "verification failed before merge root split"
            );
            debug_assert!(
                verification::are_keys_serial(
                    root_new_pages
                        .iter()
                        .map(|t| t.0.clone())
                        .collect_vec()
                        .as_slice()
                ),
                "verification failed before merge root split"
            );
            debug_assert!(
                root.ptr_eq(&self.get_root()),
                "Merge target tree should always have a persistent root unless merge split"
            );
            let _root_guard = write_node::<KS, PS>(&root);
            let new_root_len = root_new_pages.len();
            debug_assert!(
                new_root_len + 1 < KS::slice_len(),
                "Radical merge split, cannot handle this for now (need to split more than once)"
            );
            let mut new_in_root: Box<InNode<KS, PS>> = InNode::new(new_root_len, max_entry_key());
            new_in_root.ptrs.as_slice()[0] = root.clone();
            for (i, (key, node)) in root_new_pages.into_iter().enumerate() {
                new_in_root.keys.as_slice()[i] = key;
                new_in_root.ptrs.as_slice()[i + 1] = node;
            }
            // new_in_root.debug_check_integrity();
            *self.root.write() = NodeCellRef::new(Node::new(NodeData::Internal(new_in_root)));
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
    fn merge_to(&self, upper_level: &dyn LevelTree) -> usize;
    fn merge_with_keys(&self, keys: Box<Vec<EntryKey>>);
    fn insert_into(&self, key: &EntryKey) -> bool;
    fn seek_for(&self, key: &EntryKey, ordering: Ordering) -> Box<dyn Cursor>;
    fn mark_key_deleted(&self, key: &EntryKey) -> bool;
    fn dump(&self, f: &str);
    fn mid_key(&self) -> Option<EntryKey>;
    fn remove_to_right(&self, start_key: &EntryKey) -> usize;
    fn head_id(&self) -> Id;
    fn verify(&self, level: usize) -> bool;
    fn ideal_capacity(&self) -> usize {
        self.size().pow(LEVEL_TREE_DEPTH)
    }
    fn oversized(&self) -> bool {
        self.count() > self.ideal_capacity()
    }
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

    fn merge_to(&self, upper_level: &dyn LevelTree) -> usize {
        level::level_merge(self, upper_level)
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

    fn mark_key_deleted(&self, key: &EntryKey) -> bool {
        if let Some(seek_key) = self.seek(key, Ordering::Forward).current() {
            if seek_key == key {
                return self.deleted.insert(key);
            }
        }
        false
    }

    fn dump(&self, f: &str) {
        dump::dump_tree(self, f);
    }

    fn mid_key(&self) -> Option<EntryKey> {
        split::mid_key::<KS, PS>(&self.get_root())
    }

    fn remove_to_right(&self, start_key: &EntryKey) -> usize {
        let removed = remove_to_right::<KS, PS>(&self.get_root(), start_key, self);
        self.len.fetch_sub(removed, Relaxed);
        removed
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

    fn merge_to(&self, _upper_level: &dyn LevelTree) -> usize {
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

    fn mark_key_deleted(&self, _key: &EntryKey) -> bool {
        unreachable!()
    }

    fn dump(&self, _f: &str) {
        unreachable!()
    }

    fn mid_key(&self) -> Option<EntryKey> {
        unreachable!()
    }

    fn remove_to_right(&self, _start_key: &EntryKey) -> usize {
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

pub struct NodeCellRef {
    inner: Arc<dyn AnyNode>,
}

unsafe impl Send for NodeCellRef {}
unsafe impl Sync for NodeCellRef {}

impl NodeCellRef {
    pub fn new<KS, PS>(node: Node<KS, PS>) -> Self
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
    {
        NodeCellRef {
            inner: Arc::new(node),
        }
    }

    pub fn new_none<KS, PS>() -> Self
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
    {
        Node::<KS, PS>::none_ref()
    }

    #[inline]
    fn deref<KS, PS>(&self) -> &Node<KS, PS>
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
    {
        // The only unmatched scenario is the NodeCellRef was constructed by default function
        // Because the size of different type of NodeData are the same, we can still cast them safely
        // for NodeData have a fixed size for all the time
        debug_assert!(
            self.inner.is_type::<Node<KS, PS>>(),
            "Node ref type unmatched, is default: {}",
            self.is_default()
        );
        unsafe { &*(self.inner.deref() as *const dyn AnyNode as *const Node<KS, PS>) }
    }

    pub fn is_default(&self) -> bool {
        self.inner
            .is_type::<Node<DefaultKeySliceType, DefaultPtrSliceType>>()
    }

    pub fn to_string<KS, PS>(&self) -> String
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
    {
        if self.is_default() {
            String::from("<<DEFAULT>>")
        } else {
            let node = read_unchecked::<KS, PS>(self);
            if node.is_none() {
                String::from("<NONE>")
            } else if node.is_empty_node() {
                String::from("<EMPTY>")
            } else {
                format!("{:?}", node.first_key())
            }
        }
    }

    pub fn persist(
        &self,
        deletion: &DeletionSet,
        neb: &Arc<crate::client::AsyncClient>,
    ) -> BoxFuture<()> {
        self.inner.persist(self, deletion, neb)
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Clone for NodeCellRef {
    fn clone(&self) -> Self {
        NodeCellRef {
            inner: self.inner.clone(),
        }
    }
}

type DefaultKeySliceType = [EntryKey; 0];
type DefaultPtrSliceType = [NodeCellRef; 0];
type DefaultNodeDataType = NodeData<DefaultKeySliceType, DefaultPtrSliceType>;

impl Default for NodeCellRef {
    fn default() -> Self {
        let data: DefaultNodeDataType = NodeData::None;
        Self::new(Node::new(data))
    }
}

#[cfg(test)]
pub mod test;