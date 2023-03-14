use super::node::EmptyNode;
use super::Slice;
use super::*;
use itertools::free::chain;
use std::any::Any;
use std::{mem, panic};

pub struct InNode<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub keys: KS,
    pub ptrs: PS,
    pub len: usize,
    pub right: NodeCellRef,
    pub right_bound: EntryKey,
}

pub struct InNodeKeysSplit<KS>
where
    KS: Slice<EntryKey> + Debug + 'static,
{
    pub keys_2: KS,
    pub keys_1_len: usize,
    pub keys_2_len: usize,
    pub pivot_key: EntryKey,
}

pub struct InNodePtrSplit<PS>
where
    PS: Slice<NodeCellRef> + 'static,
{
    pub ptrs_2: PS,
}

impl<KS, PS> InNode<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn new(len: usize, right_bound: EntryKey) -> Box<Self> {
        Box::new(InNode {
            keys: KS::init(),
            ptrs: PS::init(),
            right: NodeCellRef::default(),
            right_bound,
            len,
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
        self.keys.as_slice_immute()[..self.len]
            .binary_search(key)
            .map(|i| i + 1)
            .unwrap_or_else(|i| i)
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
            trace!(
                "Removing from internal node pos {}, len {}, key {:?}",
                key_pos,
                n_key_len,
                &self.keys.as_slice()[key_pos]
            );
            self.keys.remove_at(key_pos, n_key_len);
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
        let node_len = self.len;
        let ptr_len = self.len + 1;
        let pivot = node_len / 2; // pivot key will be removed
        let pivot_key;
        trace!("Going to split at pivot {}", pivot);
        let keys_split = {
            trace!("insert into keys");
            if pivot == pos {
                trace!("special key treatment when pivot == pos");
                let keys_1 = &mut self.keys;
                let keys_2 = keys_1.split_at_pivot(pivot, node_len);
                let keys_1_len = pivot;
                let keys_2_len = node_len - pivot;
                pivot_key = key;
                InNodeKeysSplit {
                    keys_2,
                    keys_1_len,
                    keys_2_len,
                    pivot_key: pivot_key.clone(),
                }
            } else {
                let keys_1 = &mut self.keys;
                let mut keys_2 = keys_1.split_at_pivot(pivot + 1, node_len);
                let mut keys_1_len = pivot; // will not count the pivot
                let mut keys_2_len = node_len - pivot - 1;
                let mut key_pos = pos;
                if pos > pivot {
                    // pivot moved, need to compensate
                    key_pos -= 1;
                }
                trace!(
                    "keys 1 len: {}, keys 2 len: {}, pos {}",
                    keys_1_len,
                    keys_2_len,
                    key_pos
                );
                debug_assert_ne!(pivot, pos);
                pivot_key = keys_1.as_slice()[pivot].to_owned();
                insert_into_split(
                    key,
                    keys_1,
                    &mut keys_2,
                    &mut keys_1_len,
                    &mut keys_2_len,
                    key_pos,
                );
                InNodeKeysSplit {
                    keys_2,
                    keys_1_len,
                    keys_2_len,
                    pivot_key: pivot_key.clone(),
                }
            }
        };
        let ptr_padding = if padding_ptr_pos { 1 } else { 0 };
        let ptr_split = {
            if pivot == pos {
                trace!("special ptr treatment when pivot == pos");
                let ptrs_1 = &mut self.ptrs;
                let mut ptrs_2 = ptrs_1.split_at_pivot(pivot + 1, ptr_len);
                let ptrs_1_len = pivot + 1;
                let mut ptrs_2_len = ptr_len - pivot - 1;
                ptrs_2.insert_at(new_node, 0, &mut ptrs_2_len);
                debug_assert_eq!(ptrs_1_len, keys_split.keys_1_len + 1);
                debug_assert_eq!(ptrs_2_len, keys_split.keys_2_len + 1);
                InNodePtrSplit { ptrs_2 }
            } else {
                trace!("insert into ptrs");
                let ptrs_1 = &mut self.ptrs;
                let mut ptrs_2 = ptrs_1.split_at_pivot(pivot + 1, ptr_len);
                let mut ptrs_1_len = pivot + 1;
                let mut ptrs_2_len = ptr_len - pivot - 1;
                let ptr_pos = pos + ptr_padding;
                insert_into_split(
                    new_node,
                    ptrs_1,
                    &mut ptrs_2,
                    &mut ptrs_1_len,
                    &mut ptrs_2_len,
                    ptr_pos,
                );
                debug_assert_eq!(ptrs_1_len, keys_split.keys_1_len + 1);
                debug_assert_eq!(ptrs_2_len, keys_split.keys_2_len + 1);
                InNodePtrSplit { ptrs_2 }
            }
        };
        let right_bound = mem::replace(&mut self.right_bound, pivot_key);
        let node_2 = Box::new(InNode {
            len: keys_split.keys_2_len,
            keys: keys_split.keys_2,
            ptrs: ptr_split.ptrs_2,
            right: self.right.clone(),
            right_bound,
        });
        debug_assert!(self.right_bound < node_2.right_bound);
        debug_assert!(self.right_bound <= node_2.keys.as_slice_immute()[0]);
        let node_2_ref = NodeCellRef::new(Node::with_internal(node_2));
        self.len = keys_split.keys_1_len;
        self.right = node_2_ref.clone();
        self.debug_check_integrity();
        (node_2_ref, keys_split.pivot_key)
    }

    pub fn insert_in_place(
        &mut self,
        key: EntryKey,
        new_node: NodeCellRef,
        pos: usize,
        padding_ptr_pos: bool,
    ) {
        debug_assert!(self.len < KS::slice_len());
        let node_len = self.len;
        let mut new_node_len = node_len;
        let mut new_node_ptrs = node_len + 1;
        let ptr_padding = if padding_ptr_pos { 1 } else { 0 };
        self.keys.insert_at(key, pos, &mut new_node_len);
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
                let right_key = self.keys.as_slice()[right_key_pos].clone();
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
        let new_len = self_len + right.len + 1;
        debug_assert!(new_len <= KS::slice_len());
        // moving keys
        self.keys.as_slice()[self_len] = right_key;
        for i in self_len + 1..new_len {
            mem::swap(
                &mut self.keys.as_slice()[i],
                &mut right.keys.as_slice()[i - self_len - 1],
            );
        }
        for i in self_len + 1..new_len + 1 {
            mem::swap(
                &mut self.ptrs.as_slice()[i],
                &mut right.ptrs.as_slice()[i - self_len - 1],
            );
        }
        self.len += right.len + 1;
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

                let mut new_left_keys = KS::init();
                let mut new_left_ptrs = PS::init();

                let mut new_right_keys = KS::init();
                let mut new_right_ptrs = PS::init();

                let mut new_left_keys_len = 0;
                let mut new_right_keys_len = 0;
                debug_assert!(self.len >= right_ptr_pos);
                debug_assert!(
                    !read_unchecked::<KS, PS>(&self.ptrs.as_slice()[right_ptr_pos]).is_none()
                );
                let pivot_key_pos = right_ptr_pos - 1;
                let pivot_key = self.keys.as_slice()[pivot_key_pos].to_owned();
                debug_assert!(pivot_key > min_entry_key(),
                              "Current pivot key {:?} at {} is empty, left ptr {}, right ptr {}, now keys are {:?}",
                              pivot_key, pivot_key_pos, left_ptr_pos, right_ptr_pos, self.keys);
                for (i, key) in chain(
                    chain(
                        left_innode.keys.as_slice()[..left_innode.len].iter_mut(),
                        [pivot_key].iter_mut(),
                    ),
                    right_innode.keys.as_slice()[..right_innode.len].iter_mut(),
                )
                .enumerate()
                {
                    if i < half_full_pos {
                        mem::swap(key, &mut new_left_keys.as_slice()[i]);
                        new_left_keys_len += 1;
                    } else if i == half_full_pos {
                        mem::swap(key, &mut new_right_node_key);
                    } else {
                        let nk_index = i - half_full_pos - 1;
                        mem::swap(key, &mut new_right_keys.as_slice()[nk_index]);
                        new_right_keys_len += 1;
                    }
                }

                for (i, ptr) in chain(
                    left_innode.ptrs.as_slice()[..left_innode.len + 1].iter_mut(),
                    right_innode.ptrs.as_slice()[..right_innode.len + 1].iter_mut(),
                )
                .enumerate()
                {
                    if i < half_full_pos + 1 {
                        mem::swap(ptr, &mut new_left_ptrs.as_slice()[i]);
                    } else {
                        mem::swap(ptr, &mut new_right_ptrs.as_slice()[i - half_full_pos - 1]);
                    }
                }

                left_innode.keys = new_left_keys;
                left_innode.ptrs = new_left_ptrs;
                left_innode.len = new_left_keys_len;

                right_innode.keys = new_right_keys;
                right_innode.ptrs = new_right_ptrs;
                right_innode.len = new_right_keys_len;

                trace!(
                    "Relocated internal children. left {}:{:?} right {}:{:?}",
                    left_innode.len,
                    left_innode.keys,
                    right_innode.len,
                    right_innode.keys
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
        self.keys.as_slice()[right_key_pos] = new_right_node_key;
        self.debug_check_integrity();
    }

    pub fn debug_check_integrity(&self) {
        if cfg!(debug_assertions) {
            if self.len == 0 {
                // will not check empty node
                return;
            }
            for (i, key) in self.keys.as_slice_immute()[..self.len].iter().enumerate() {
                debug_assert!(
                    key > &*MIN_ENTRY_KEY,
                    "{} keys {}/{} {:?}",
                    KS::slice_len(),
                    i,
                    self.len,
                    &self.keys.as_slice_immute()[..self.len]
                );
            }
        }
    }
}
