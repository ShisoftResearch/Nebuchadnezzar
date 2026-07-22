use super::leaf_keys::LeafKeys;
use super::Slice;
use super::*;
use std::any::Any;
use std::marker::PhantomData;
use std::{mem, panic};

// Prefix-compressed pivot storage for internal nodes, sharing LeafKeys'
// reader-safe buffer: optimistic readers reach the suffixes through one
// atomic pointer to a self-describing allocation, and replaced buffers are
// epoch-retired instead of freed under a live stale view (the plain-Vec
// predecessor freed its allocation immediately on rebuild).
pub struct InternalKeys {
    inner: LeafKeys,
}

impl std::fmt::Debug for InternalKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl InternalKeys {
    pub fn from_keys(keys: &[EntryKey]) -> Self {
        InternalKeys {
            inner: LeafKeys::from_keys(keys, keys.len().max(1)),
        }
    }

    // Atomically replace the pivots of a published node (swap + epoch
    // retire); used by in-place rebuilds under the node's write latch.
    pub fn set(&self, keys: &[EntryKey]) {
        self.inner.set(keys);
    }

    pub fn key_at(&self, index: usize) -> EntryKey {
        self.inner.key_at(index)
    }

    #[inline]
    pub fn cmp_at(&self, index: usize, key: &EntryKey) -> std::cmp::Ordering {
        self.inner.cmp_at(index, key)
    }

    pub fn to_vec(&self, len: usize) -> Vec<EntryKey> {
        self.inner.to_vec(0..len)
    }

    // Routing search over `len` keys: returns the child position for `key`,
    // with keys equal to a pivot routing right.
    pub fn search(&self, len: usize, key: &EntryKey) -> usize {
        let pos = self.inner.search(len, key);
        if pos < len && self.inner.cmp_at(pos, key) == std::cmp::Ordering::Equal {
            pos + 1
        } else {
            pos
        }
    }
}

pub struct InNode<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub keys: InternalKeys,
    pub ptrs: PS,
    pub len: usize,
    pub right: NodeCellRef,
    pub right_bound: EntryKey,
    _marker: PhantomData<KS>,
}

impl<KS, PS> InNode<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    #[inline]
    fn node_capacity() -> usize {
        debug_assert_eq!(PS::SLICE_LEN, KS::SLICE_LEN + 1);
        KS::SLICE_LEN
    }

    pub fn new(len: usize, right_bound: EntryKey) -> Box<Self> {
        Box::new(InNode::<KS, PS> {
            keys: InternalKeys::from_keys(&[]),
            ptrs: PS::init(),
            right: NodeCellRef::default(),
            right_bound,
            len,
            _marker: PhantomData,
        })
    }

    pub fn search(&self, key: &EntryKey) -> usize {
        self.keys.search(self.len, key)
    }
    pub fn search_unwindable(
        &self,
        key: &EntryKey,
    ) -> Result<usize, Box<dyn Any + Send + 'static>> {
        panic::catch_unwind(panic::AssertUnwindSafe(|| self.search(key)))
    }
    pub fn split_insert(
        &mut self,
        key: EntryKey,
        new_node: NodeCellRef,
        pos: usize,
        padding_ptr_pos: bool,
    ) -> (NodeCellRef, EntryKey) {
        let appending = pos == self.len;
        let mut keys = self.keys.to_vec(self.len);
        keys.insert(pos, key);

        let ptr_insert_pos = pos + if padding_ptr_pos { 1 } else { 0 };
        let mut ptrs = self.ptrs.as_slice_immute()[..self.len + 1].to_vec();
        ptrs.insert(ptr_insert_pos, new_node);

        // Append-aware split, mirroring the external nodes: rightmost
        // inserts leave the left node nearly full and give the right node a
        // single key (internal nodes cannot be keyless).
        let pivot = if appending {
            keys.len() - 2
        } else {
            keys.len() / 2
        };
        let pivot_key = keys[pivot].clone();

        let left_keys = keys[..pivot].to_vec();
        let right_keys = keys[pivot + 1..].to_vec();
        let left_ptrs = ptrs[..pivot + 1].to_vec();
        let right_ptrs = ptrs[pivot + 1..].to_vec();

        self.keys.set(left_keys.as_slice());
        self.len = left_keys.len();
        for (i, ptr) in left_ptrs.iter().enumerate() {
            self.ptrs.as_slice()[i] = ptr.clone();
        }
        for i in left_ptrs.len()..(Self::node_capacity() + 1) {
            self.ptrs.as_slice()[i] = NodeCellRef::default();
        }

        let mut ptrs_2 = PS::init();
        for (i, ptr) in right_ptrs.iter().enumerate() {
            ptrs_2.as_slice()[i] = ptr.clone();
        }

        let right_bound = mem::replace(&mut self.right_bound, pivot_key.clone());
        let node_2 = Box::new(InNode::<KS, PS> {
            len: right_keys.len(),
            keys: InternalKeys::from_keys(right_keys.as_slice()),
            ptrs: ptrs_2,
            right: self.right.clone(),
            right_bound,
            _marker: PhantomData,
        });
        let node_2_first = node_2.keys.key_at(0);
        debug_assert!(self.right_bound < node_2.right_bound);
        debug_assert!(self.right_bound <= node_2_first);
        let node_2_ref = NodeCellRef::new(Node::with_internal(node_2));
        self.right = node_2_ref.clone();
        self.debug_check_integrity();
        (node_2_ref, pivot_key)
    }

    pub fn insert_in_place(
        &mut self,
        key: EntryKey,
        new_node: NodeCellRef,
        pos: usize,
        padding_ptr_pos: bool,
    ) {
        debug_assert!(self.len < Self::node_capacity());
        let mut keys = self.keys.to_vec(self.len);
        keys.insert(pos, key);
        let mut new_node_len = self.len;
        let mut new_node_ptrs = self.len + 1;
        let ptr_padding = if padding_ptr_pos { 1 } else { 0 };
        new_node_len += 1;
        self.keys.set(keys.as_slice());
        self.ptrs
            .insert_at(new_node, pos + ptr_padding, &mut new_node_ptrs);
        self.len = new_node_len;
        self.debug_check_integrity();
    }

    pub fn insert(
        &mut self,
        key: EntryKey,
        new_node: NodeCellRef,
        parent: &NodeCellRef,
    ) -> Option<NodeSplit<KS, PS>> {
        let node_len = self.len;
        let _ptr_len = self.len + 1;
        let pos = self.search(&key);
        trace!("Insert into internal node at {}, key: {:?}", pos, key);
        debug_assert!(node_len <= Self::node_capacity());
        if node_len == Self::node_capacity() {
            let parent_guard = write_node(parent);
            let (node_2, pivot_key) = self.split_insert(key, new_node, pos, true);
            return Some(NodeSplit {
                new_right_node: node_2,
                left_node_latch: NodeWriteGuard::default(),
                pivot: pivot_key,
                parent_latch: parent_guard,
            });
        } else {
            self.insert_in_place(key, new_node, pos, true);
            return None;
        }
    }
    pub fn debug_check_integrity(&self) {
        if cfg!(debug_assertions) {
            if self.len == 0 {
                // will not check empty node
                return;
            }
            let keys = self.keys.to_vec(self.len);
            for (i, key) in keys.iter().enumerate() {
                debug_assert!(
                    key > &*MIN_ENTRY_KEY,
                    "{} keys {}/{} {:?}",
                    Self::node_capacity(),
                    i,
                    self.len,
                    keys
                );
            }
        }
    }
}
