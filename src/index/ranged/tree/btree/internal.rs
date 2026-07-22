use super::Slice;
use super::*;
use crate::index::KEY_SIZE;
use std::any::Any;
use std::marker::PhantomData;
use std::{mem, panic};

#[derive(Clone, Debug)]
pub struct InternalKeys {
    blob: InternalKeysBlob,
}

#[derive(Clone, Debug)]
struct InternalKeysBlob {
    shared_prefix: [u8; KEY_SIZE],
    shared_prefix_len: u8,
    suffixes: Vec<u8>,
}

impl InternalKeys {
    fn empty() -> Self {
        Self {
            blob: InternalKeysBlob {
                shared_prefix: [0; KEY_SIZE],
                shared_prefix_len: 0,
                suffixes: Vec::new(),
            },
        }
    }

    pub fn from_keys(keys: &[EntryKey]) -> Self {
        if keys.is_empty() {
            return Self::empty();
        }

        let first = keys[0].as_slice();
        let mut prefix_len = KEY_SIZE;
        for key in &keys[1..] {
            let bytes = key.as_slice();
            let mut i = 0;
            while i < prefix_len && bytes[i] == first[i] {
                i += 1;
            }
            prefix_len = i;
            if prefix_len == 0 {
                break;
            }
        }

        let mut shared_prefix = [0; KEY_SIZE];
        shared_prefix[..prefix_len].copy_from_slice(&first[..prefix_len]);
        let mut suffixes = Vec::with_capacity(keys.len() * (KEY_SIZE - prefix_len));
        for key in keys {
            let tail = &key.as_slice()[prefix_len..];
            suffixes.extend_from_slice(tail);
        }

        Self {
            blob: InternalKeysBlob {
                shared_prefix,
                shared_prefix_len: prefix_len as u8,
                suffixes,
            },
        }
    }

    pub fn key_at(&self, index: usize) -> EntryKey {
        let prefix_len = self.blob.shared_prefix_len as usize;
        let suffix_len = KEY_SIZE - prefix_len;
        let start = index * suffix_len;
        let end = start + suffix_len;
        debug_assert!(end <= self.blob.suffixes.len());
        let mut key = EntryKey::new();
        key.as_mut_slice()[..prefix_len].copy_from_slice(&self.blob.shared_prefix[..prefix_len]);
        key.as_mut_slice()[prefix_len..].copy_from_slice(&self.blob.suffixes[start..end]);
        key
    }

    #[inline]
    pub fn cmp_at(&self, index: usize, key: &EntryKey) -> std::cmp::Ordering {
        let prefix_len = self.blob.shared_prefix_len as usize;
        let key_bytes = key.as_slice();

        let prefix_cmp = self.blob.shared_prefix[..prefix_len].cmp(&key_bytes[..prefix_len]);
        if prefix_cmp != std::cmp::Ordering::Equal {
            return prefix_cmp;
        }

        let suffix_len = KEY_SIZE - prefix_len;
        let start = index * suffix_len;
        let end = start + suffix_len;
        debug_assert!(end <= self.blob.suffixes.len());
        self.blob.suffixes[start..end].cmp(&key_bytes[prefix_len..])
    }

    pub fn to_vec(&self, len: usize) -> Vec<EntryKey> {
        (0..len).map(|i| self.key_at(i)).collect()
    }

    // Routing search over `len` keys: returns the child position for `key`,
    // with keys equal to a pivot routing right. The shared prefix is compared
    // once up front so the binary search only touches the stored suffixes.
    pub fn search(&self, len: usize, key: &EntryKey) -> usize {
        let prefix_len = self.blob.shared_prefix_len as usize;
        let key_bytes = key.as_slice();
        match self.blob.shared_prefix[..prefix_len].cmp(&key_bytes[..prefix_len]) {
            std::cmp::Ordering::Greater => return 0, // every stored key > key
            std::cmp::Ordering::Less => return len,  // every stored key < key
            std::cmp::Ordering::Equal => {}
        }
        let suffix_len = KEY_SIZE - prefix_len;
        let key_suffix = &key_bytes[prefix_len..];
        let mut left = 0;
        let mut right = len;
        while left < right {
            let mid = left + (right - left) / 2;
            let start = mid * suffix_len;
            match self.blob.suffixes[start..start + suffix_len].cmp(key_suffix) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => return mid + 1,
            }
        }
        left
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
            keys: InternalKeys::empty(),
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
        let mut keys = self.keys.to_vec(self.len);
        keys.insert(pos, key);

        let ptr_insert_pos = pos + if padding_ptr_pos { 1 } else { 0 };
        let mut ptrs = self.ptrs.as_slice_immute()[..self.len + 1].to_vec();
        ptrs.insert(ptr_insert_pos, new_node);

        let pivot = keys.len() / 2;
        let pivot_key = keys[pivot].clone();

        let left_keys = keys[..pivot].to_vec();
        let right_keys = keys[pivot + 1..].to_vec();
        let left_ptrs = ptrs[..pivot + 1].to_vec();
        let right_ptrs = ptrs[pivot + 1..].to_vec();

        self.keys = InternalKeys::from_keys(left_keys.as_slice());
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
        self.keys = InternalKeys::from_keys(keys.as_slice());
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
