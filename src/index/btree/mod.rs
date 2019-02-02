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
use index::btree::insert::*;
use index::btree::internal::*;
use index::btree::merge::merge_into_tree_node;
pub use index::btree::node::*;
use index::btree::remove::*;
use index::btree::search::*;
use index::EntryKey;
use index::MergeableTree;
use index::MergingPage;
use index::MergingTreeGuard;
use index::Slice;
use index::KEY_SIZE;
use index::{Cursor as IndexCursor, Ordering};
use itertools::{chain, Itertools};
use parking_lot::RwLock;
use ram::cell::Cell;
use ram::types::RandValue;
use smallvec::SmallVec;
use std;
use std::any::Any;
use std::cell::Ref;
use std::cell::RefCell;
use std::cell::RefMut;
use std::cell::UnsafeCell;
use std::cmp::{max, min};
use std::fmt::Debug;
use std::fmt::Error;
use std::fmt::Formatter;
use std::io::Write;
use std::iter;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Range;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed, Ordering::SeqCst};
use std::sync::Arc;
use utils::lru_cache::LRUCache;

mod cursor;
mod external;
mod insert;
mod internal;
mod level;
mod merge;
mod node;
mod remove;
mod search;

const CACHE_SIZE: usize = 2048;

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
    storage: Arc<AsyncClient>,
    len: Arc<AtomicUsize>,
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
    pub fn new(neb_client: &Arc<AsyncClient>) -> BPlusTree<KS, PS> {
        let neb_client_1 = neb_client.clone();
        let neb_client_2 = neb_client.clone();
        let mut tree = BPlusTree {
            root: RwLock::new(NodeCellRef::new(Node::<KS, PS>::none())),
            root_versioning: NodeCellRef::new(Node::<KS, PS>::none()),
            storage: neb_client.clone(),
            len: Arc::new(AtomicUsize::new(0)),
            marker: PhantomData,
        };
        let root_id = tree.new_page_id();
        *tree.root.write() =
            NodeCellRef::new(Node::<KS, PS>::new_external(root_id, max_entry_key()));
        return tree;
    }

    pub fn get_root(&self) -> NodeCellRef {
        self.root.read().clone()
    }

    pub fn seek(&self, key: &EntryKey, ordering: Ordering) -> RTCursor<KS, PS> {
        let mut cursor = search_node(&self.get_root(), key, ordering);
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

    pub fn insert(&self, key: &EntryKey) {
        match insert_to_tree_node(&self, &self.get_root(), &self.root_versioning, &key, 0) {
            Some(split) => {
                debug!("split root with pivot key {:?}", split.pivot);
                let new_node = split.new_right_node;
                let pivot = split.pivot;
                let mut new_in_root: InNode<KS, PS> = InNode::new(1, max_entry_key());
                let mut old_root = self.get_root().clone();
                // check latched root and current root are the same node
                debug_assert_eq!(
                    read_unchecked::<KS, PS>(&old_root).first_key(),
                    split.left_node_latch.first_key(),
                    "root verification failed, left node right node type: {}",
                    read_unchecked::<KS, PS>(&split.left_node_latch.innode().right).type_name()
                );
                new_in_root.keys.as_slice()[0] = pivot;
                new_in_root.ptrs.as_slice()[0] = old_root;
                new_in_root.ptrs.as_slice()[1] = new_node;
                *self.root.write() =
                    NodeCellRef::new(Node::new(NodeData::Internal(box new_in_root)));
            }
            None => {}
        }
        self.len.fetch_add(1, Relaxed);
    }

    pub fn remove(&self, key: &EntryKey) -> bool {
        let mut root = self.get_root();
        let result = remove_from_node(self, &mut root, &mut key.clone(), &self.root_versioning, 0);
        if let Some(rebalance) = result.rebalancing {
            let root_node = rebalance.parent;
            if read_unchecked::<KS, PS>(&(*self.root.read()))
                .innode()
                .keys
                .as_slice_immute()[0]
                == root_node.innode().keys.as_slice_immute()[0]
            {
                // Make sure root node does not changed during the process. If it did changed, ignore it
                // When root is external and have no keys but one pointer will take the only sub level
                // pointer node as the new root node.
                let new_root = root_node.innode().ptrs.as_slice_immute()[0].clone();
                *self.root.write() = new_root;
            }
        }
        if result.removed {
            self.len.fetch_sub(1, Relaxed);
        }
        result.removed
    }

    pub fn merge_with_keys_(&self, keys: Vec<EntryKey>) {
        let keys_len = keys.len();
        let root = self.get_root();
        let root_new_pages = merge_into_tree_node(self, &root, &self.root_versioning, keys, 0);
        if root_new_pages.len() > 0 {
            let root_guard = write_node::<KS, PS>(&root);
            let new_root_len = root_new_pages.len() + 1;
            debug_assert!(new_root_len < KS::slice_len());
            let mut new_in_root: InNode<KS, PS> = InNode::new(new_root_len, max_entry_key());
            new_in_root.ptrs.as_slice()[0] = root.clone();
            for (i, (key, node)) in root_new_pages.into_iter().enumerate() {
                new_in_root.keys.as_slice()[i] = key;
                new_in_root.ptrs.as_slice()[i + 1] = node;
            }
            *self.root.write() = NodeCellRef::new(Node::new(NodeData::Internal(box new_in_root)));
        }
        self.len.fetch_add(keys_len, Relaxed);
    }

    pub fn flush_all(&self) {
        // unimplemented!()
    }

    pub fn len(&self) -> usize {
        self.len.load(Relaxed)
    }

    fn flush_item(client: &Arc<AsyncClient>, value: &NodeCellRef) {
        let cell = read_node(value, |node: &NodeReadHandler<KS, PS>| {
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

pub trait LevelTree {
    fn size(&self) -> usize;
    fn count(&self) -> usize;
    fn merge_to(&self, upper_level: &LevelTree) -> usize;
    fn merge_with_keys(&self, keys: Vec<EntryKey>);
}

impl <KS, PS> LevelTree for BPlusTree<KS, PS> where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    fn size(&self) -> usize {
        KS::slice_len()
    }

    fn count(&self) -> usize {
        self.len()
    }

    fn merge_to(&self, upper_level: &LevelTree) -> usize {
        level::level_merge(self, upper_level)
    }

    fn merge_with_keys(&self, keys: Vec<SmallVec<[u8; 32]>>) {
        self.merge_with_keys_(keys)
    }
}

impl_slice_ops!([EntryKey; 0], EntryKey, 0);
impl_slice_ops!([NodeCellRef; 0], NodeCellRef, 0);

macro_rules! impl_btree_level {
    ($items: expr) => {
        impl_slice_ops!([EntryKey; $items], EntryKey, $items);
        impl_slice_ops!([NodeCellRef; $items + 1], NodeCellRef, $items + 1);
    };
}

pub struct NodeCellRef {
    inner: Arc<Any>,
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
            self.inner.is::<Node<KS, PS>>(),
            "Node ref type unmatched, is default: {}",
            self.is_default()
        );
        unsafe { &*(self.inner.deref() as *const dyn Any as *const Node<KS, PS>) }
    }

    pub fn is_default(&self) -> bool {
        self.inner
            .is::<Node<DefaultKeySliceType, DefaultPtrSliceType>>()
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
}

impl Clone for NodeCellRef {
    fn clone(&self) -> Self {
        NodeCellRef {
            inner: self.inner.clone(),
        }
    }
}

lazy_static! {
    pub static ref MAX_ENTRY_KEY: EntryKey = max_entry_key();
    pub static ref MIN_ENTRY_KEY: EntryKey = smallvec!(0);
}

fn max_entry_key() -> EntryKey {
    EntryKey::from(iter::repeat(255u8).take(KEY_SIZE).collect_vec())
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
