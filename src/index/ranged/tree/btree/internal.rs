use super::node::EmptyNode;
use super::Slice;
use super::*;
use crate::index::KEY_SIZE;
use itertools::free::chain;
use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{mem, panic};

#[derive(Clone, Debug)]
pub struct InternalKeys {
    blob: Arc<InternalKeysBlob>,
}

#[derive(Clone, Debug)]
struct InternalKeysBlob {
    shared_prefix: [u8; KEY_SIZE],
    shared_prefix_len: u8,
    offsets: Vec<u16>,
    suffixes: Vec<u8>,
}

impl InternalKeys {
    fn empty() -> Self {
        Self {
            blob: Arc::new(InternalKeysBlob {
                shared_prefix: [0; KEY_SIZE],
                shared_prefix_len: 0,
                offsets: vec![0],
                suffixes: Vec::new(),
            }),
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
        let mut offsets = Vec::with_capacity(keys.len() + 1);
        let mut suffixes = Vec::with_capacity(keys.len() * (KEY_SIZE - prefix_len));
        offsets.push(0);
        for key in keys {
            let tail = &key.as_slice()[prefix_len..];
            suffixes.extend_from_slice(tail);
            offsets.push(suffixes.len() as u16);
        }

        Self {
            blob: Arc::new(InternalKeysBlob {
                shared_prefix,
                shared_prefix_len: prefix_len as u8,
                offsets,
                suffixes,
            }),
        }
    }

    pub fn key_at(&self, index: usize) -> EntryKey {
        let blob = self.blob.clone();
        let prefix_len = blob.shared_prefix_len as usize;
        debug_assert!(index + 1 < blob.offsets.len());
        let start = blob.offsets[index] as usize;
        let end = blob.offsets[index + 1] as usize;
        debug_assert!(end <= blob.suffixes.len());
        let mut key = EntryKey::new();
        key.as_mut_slice()[..prefix_len].copy_from_slice(&blob.shared_prefix[..prefix_len]);
        key.as_mut_slice()[prefix_len..].copy_from_slice(&blob.suffixes[start..end]);
        key
    }

    pub fn to_vec(&self, len: usize) -> Vec<EntryKey> {
        (0..len).map(|i| self.key_at(i)).collect()
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

    pub fn key_pos_from_ptr_pos(&self, ptr_pos: usize) -> usize {
        if ptr_pos == 0 {
            0
        } else {
            ptr_pos - 1
        }
    }
    pub fn search(&self, key: &EntryKey) -> usize {
        let mut left = 0;
        let mut right = self.len;
        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.keys.key_at(mid);
            if mid_key < *key {
                left = mid + 1;
            } else if mid_key > *key {
                right = mid;
            } else {
                return mid + 1;
            }
        }
        left
    }
    pub fn search_unwindable(
        &self,
        key: &EntryKey,
    ) -> Result<usize, Box<dyn Any + Send + 'static>> {
        panic::catch_unwind(panic::AssertUnwindSafe(|| self.search(key)))
    }
    pub fn remove_at(&mut self, ptr_pos: usize) {
        {
            let key_pos = self.key_pos_from_ptr_pos(ptr_pos);
            let n_key_len = &mut self.len;
            let mut n_ptr_len = *n_key_len + 1;
            let mut keys = self.keys.to_vec(*n_key_len);
            trace!(
                "Removing from internal node pos {}, len {}, key {:?}",
                key_pos,
                n_key_len,
                &keys[key_pos]
            );
            keys.remove(key_pos);
            self.keys = InternalKeys::from_keys(keys.as_slice());
            *n_key_len -= 1;
            self.ptrs.remove_at(ptr_pos, &mut n_ptr_len);
        }
        self.debug_check_integrity();
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
        for i in left_ptrs.len()..(KS::slice_len() + 1) {
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
        debug_assert!(self.len < KS::slice_len());
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
        debug_assert!(node_len <= KS::slice_len());
        if node_len == KS::slice_len() {
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
    pub fn rebalance_candidate(&self, pointer_pos: usize) -> usize {
        debug_assert!(pointer_pos <= self.len);
        trace!(
            "Searching for rebalance candidate, pos {}, len {}",
            pointer_pos,
            self.len
        );
        if pointer_pos == 0 {
            1
        } else if pointer_pos + 1 >= self.len {
            // the last one, pick left
            pointer_pos - 1
        } else {
            // pick the right one
            // we should pick the one  with least pointers, but it cost for the check is too high
            pointer_pos + 1
        }
    }
    pub fn merge_children(
        &mut self,
        left_ptr_pos: usize,
        right_ptr_pos: usize,
        left_node: &mut NodeWriteGuard<KS, PS>,
        right_node: &mut NodeWriteGuard<KS, PS>,
        right_node_next: &mut NodeWriteGuard<KS, PS>,
        tree: &BPlusTree<KS, PS>,
    ) {
        let left_node_ref = self.ptrs.as_slice()[left_ptr_pos].clone();
        let left_len = left_node.len();
        let right_len = right_node.len();
        let right_key_pos = self.key_pos_from_ptr_pos(right_ptr_pos);
        let merged_len;
        trace!("Merge children, left len {}, right len {}, left_ptr_pos {}, right_ptr_pos {}, right_key_pos {}",
                left_len, right_len, left_ptr_pos, right_ptr_pos, right_key_pos);
        debug_assert_eq!(left_node.is_ext(), right_node.is_ext());
        if !left_node.is_ext() {
            {
                let left_innode = left_node.innode_mut();
                let mut right_innode = right_node.innode_mut();
                let right_key = self.keys.key_at(right_key_pos);
                left_innode.merge_with(&mut right_innode, right_key);
                left_innode.right = right_innode.right.clone();
                merged_len = left_innode.len;
            }
        } else {
            let mut right_extnode = right_node.extnode_mut(tree);
            let left_extnode = left_node.extnode_mut(tree);
            {
                left_extnode.merge_with(&mut right_extnode);
                merged_len = left_extnode.len;
            }
            if !right_node_next.is_none() {
                right_node_next.extnode_mut(tree).prev = left_node_ref.clone()
            }
            left_extnode.next = right_extnode.next.clone();
        }
        **right_node = NodeData::Empty(Box::new(EmptyNode {
            left: Some(left_node_ref.clone()),
            right: left_node_ref.clone(),
        }));
        trace!(
            "Removing merged node at {}, left {}, right {}, merged {}",
            right_ptr_pos,
            left_len,
            right_len,
            merged_len
        );
        self.remove_at(right_ptr_pos);
        trace!("Merged parent level keys: {:?}", self.keys);
        trace!("Merged level keys {:?}", left_node.keys());
        self.debug_check_integrity();
    }
    pub fn merge_with(&mut self, right: &mut Self, right_key: EntryKey) {
        trace!(
            "Merge internal node, left len {}, right len {}, right_key {:?}",
            self.len,
            right.len,
            right_key
        );
        let self_len = self.len;
        let right_len = right.len;
        let new_len = self_len + right_len + 1;
        debug_assert!(new_len <= KS::slice_len());

        let mut merged_keys = self.keys.to_vec(self_len);
        merged_keys.push(right_key);
        merged_keys.extend(right.keys.to_vec(right_len));
        self.keys = InternalKeys::from_keys(merged_keys.as_slice());

        let mut merged_ptrs = self.ptrs.as_slice_immute()[..self_len + 1].to_vec();
        merged_ptrs.extend_from_slice(&right.ptrs.as_slice_immute()[..right_len + 1]);
        for (i, ptr) in merged_ptrs.into_iter().enumerate() {
            self.ptrs.as_slice()[i] = ptr;
        }

        self.len = new_len;
        self.debug_check_integrity();
    }
    pub fn relocate_children(
        &mut self,
        left_ptr_pos: usize,
        right_ptr_pos: usize,
        left_node: &mut NodeWriteGuard<KS, PS>,
        right_node: &mut NodeWriteGuard<KS, PS>,
        tree: &BPlusTree<KS, PS>,
    ) {
        debug_assert_ne!(left_ptr_pos, right_ptr_pos);
        let mut new_right_node_key = Default::default();
        let half_full_pos = (left_node.len() + right_node.len()) / 2;
        debug_assert_eq!(left_node.is_ext(), right_node.is_ext());
        if !left_node.is_ext() {
            // relocate internal sub nodes

            {
                let left_innode = left_node.innode_mut();
                let right_innode = right_node.innode_mut();

                trace!(
                    "Before relocation internal children. left {}:{:?} right {}:{:?}",
                    left_innode.len,
                    left_innode.keys,
                    right_innode.len,
                    right_innode.keys
                );

                let mut new_left_ptrs = PS::init();
                let mut new_right_ptrs = PS::init();

                debug_assert!(self.len >= right_ptr_pos);
                debug_assert!(
                    !read_unchecked::<KS, PS>(&self.ptrs.as_slice()[right_ptr_pos]).is_none()
                );
                let pivot_key_pos = right_ptr_pos - 1;
                let pivot_key = self.keys.key_at(pivot_key_pos);
                debug_assert!(pivot_key > min_entry_key(),
                              "Current pivot key {:?} at {} is empty, left ptr {}, right ptr {}, now keys are {:?}",
                              pivot_key, pivot_key_pos, left_ptr_pos, right_ptr_pos, self.keys.to_vec(self.len));

                let mut all_keys = left_innode.keys.to_vec(left_innode.len);
                all_keys.push(pivot_key);
                all_keys.extend(right_innode.keys.to_vec(right_innode.len));

                let new_left_keys = all_keys[..half_full_pos].to_vec();
                new_right_node_key = all_keys[half_full_pos].clone();
                let new_right_keys = all_keys[half_full_pos + 1..].to_vec();

                let mut all_ptrs =
                    left_innode.ptrs.as_slice_immute()[..left_innode.len + 1].to_vec();
                all_ptrs.extend_from_slice(
                    &right_innode.ptrs.as_slice_immute()[..right_innode.len + 1],
                );

                for (i, ptr) in all_ptrs.into_iter().enumerate() {
                    if i < half_full_pos + 1 {
                        new_left_ptrs.as_slice()[i] = ptr;
                    } else {
                        new_right_ptrs.as_slice()[i - half_full_pos - 1] = ptr;
                    }
                }

                left_innode.keys = InternalKeys::from_keys(new_left_keys.as_slice());
                left_innode.ptrs = new_left_ptrs;
                left_innode.len = new_left_keys.len();

                right_innode.keys = InternalKeys::from_keys(new_right_keys.as_slice());
                right_innode.ptrs = new_right_ptrs;
                right_innode.len = new_right_keys.len();

                trace!(
                    "Relocated internal children. left {}:{:?} right {}:{:?}",
                    left_innode.len,
                    left_innode.keys.to_vec(left_innode.len),
                    right_innode.len,
                    right_innode.keys.to_vec(right_innode.len)
                );
            }
        } else if left_node.is_ext() {
            // relocate external sub nodes

            let left_extnode = left_node.extnode_mut(tree);
            let right_extnode = right_node.extnode_mut(tree);

            trace!(
                "Before relocation external children. left {}:{:?} right {}:{:?}",
                left_extnode.len,
                left_extnode.keys,
                right_extnode.len,
                right_extnode.keys
            );

            let mut new_left_keys = KS::init();
            let mut new_right_keys = KS::init();

            let left_len = left_extnode.len;
            let right_len = right_extnode.len;
            let mut new_left_keys_len = 0;
            let mut new_right_keys_len = 0;
            for (i, key) in chain(
                left_extnode.keys.as_slice()[..left_len].iter_mut(),
                right_extnode.keys.as_slice()[..right_len].iter_mut(),
            )
            .enumerate()
            {
                if i < half_full_pos {
                    mem::swap(key, &mut new_left_keys.as_slice()[i]);
                    new_left_keys_len += 1;
                } else {
                    mem::swap(key, &mut new_right_keys.as_slice()[i - half_full_pos]);
                    new_right_keys_len += 1;
                }
            }

            new_right_node_key = new_right_keys.as_slice()[0].clone();

            left_extnode.keys = new_left_keys;
            left_extnode.len = new_left_keys_len;

            right_extnode.keys = new_right_keys;
            right_extnode.len = new_right_keys_len;

            trace!(
                "Relocated external children. left {}:{:?} right {}:{:?}",
                left_extnode.len,
                left_extnode.keys,
                right_extnode.len,
                right_extnode.keys
            );
        }

        let right_key_pos = right_ptr_pos - 1;
        trace!(
            "Setting key at pos {} to new key {:?}",
            right_key_pos,
            new_right_node_key
        );
        debug_assert!(new_right_node_key > min_entry_key());
        let mut parent_keys = self.keys.to_vec(self.len);
        parent_keys[right_key_pos] = new_right_node_key;
        self.keys = InternalKeys::from_keys(parent_keys.as_slice());
        self.debug_check_integrity();
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
                    KS::slice_len(),
                    i,
                    self.len,
                    keys
                );
            }
        }
    }
}
