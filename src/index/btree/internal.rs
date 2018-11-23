use hermes::stm::Txn;
use hermes::stm::TxnErr;
use hermes::stm::TxnValRef;
use index::btree::external::CacheBufferZone;
use index::btree::Slice;
use index::btree::*;
use itertools::free::chain;
use std::mem;
use std::sync::atomic::AtomicPtr;
use std::sync::Arc;
use std::cell::UnsafeCell;

#[derive(Clone)]
pub struct InNode {
    pub keys: EntryKeySlice,
    pub ptrs: NodePtrCellSlice,
    pub right_ptr: NodeCellRef,
    pub cc: AtomicUsize,
    pub len: usize,
}

pub struct InNodeKeysSplit {
    pub keys_2: EntryKeySlice,
    pub keys_1_len: usize,
    pub keys_2_len: usize,
    pub pivot_key: EntryKey,
}

pub struct InNodePtrSplit {
    pub ptrs_2: NodePtrCellSlice,
}

impl InNode {
    pub fn key_pos_from_ptr_pos(&self, ptr_pos: usize) -> usize {
        if ptr_pos == 0 {
            0
        } else {
            ptr_pos - 1
        }
    }
    pub fn remove_at(&mut self, ptr_pos: usize) {
        let mut n_key_len = self.len;
        let mut n_ptr_len = n_key_len + 1;
        let key_pos = self.key_pos_from_ptr_pos(ptr_pos);
        debug!(
            "Removing from internal node pos {}, len {}",
            key_pos, n_key_len
        );
        self.keys.remove_at(key_pos, &mut n_key_len);
        self.ptrs.remove_at(ptr_pos, &mut n_ptr_len);
        self.len = n_key_len;
    }
    pub fn insert(
        &mut self,
        key: EntryKey,
        ptr: Option<NodeCellRef>,
        pos: usize,
    ) -> Option<(NodeCellRef, Option<EntryKey>)> {
        debug!("Insert into internal node at {}, key: {:?}", pos, key);
        let node_len = self.len;
        let ptr_len = self.len + 1;
        debug_assert!(node_len <= NUM_KEYS);
        if node_len == NUM_KEYS {
            let pivot = node_len / 2; // pivot key will be removed
            debug!("Going to split at pivot {}", pivot);
            let keys_split = {
                debug!("insert into keys");
                let mut keys_1 = &mut self.keys;
                let mut keys_2 = keys_1.split_at_pivot(pivot + 1, node_len);
                let mut keys_1_len = pivot; // will not count the pivot
                let mut keys_2_len = node_len - pivot - 1;
                let mut key_pos = pos;
                if pos > pivot {
                    // pivot moved, need to compensate
                    key_pos -= 1;
                }
                debug!(
                    "keys 1 len: {}, keys 2 len: {}, pos {}",
                    keys_1_len, keys_2_len, key_pos
                );
                let pivot_key = keys_1[pivot].to_owned();
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
                    pivot_key,
                }
            };
            let ptr_split = {
                debug!("insert into ptrs");
                let mut ptrs_1 = &mut self.ptrs;
                let mut ptrs_2 = ptrs_1.split_at_pivot(pivot + 1, ptr_len);
                let mut ptrs_1_len = pivot + 1;
                let mut ptrs_2_len = ptr_len - pivot - 1;
                let mut ptr_pos = pos + 1;
                insert_into_split(
                    ptr.unwrap(),
                    ptrs_1,
                    &mut ptrs_2,
                    &mut ptrs_1_len,
                    &mut ptrs_2_len,
                    ptr_pos,
                );
                debug_assert_eq!(ptrs_1_len, keys_split.keys_1_len + 1);
                debug_assert_eq!(ptrs_2_len, keys_split.keys_2_len + 1);
                InNodePtrSplit { ptrs_2 }
            };
            let node_2 = InNode {
                len: keys_split.keys_2_len,
                keys: keys_split.keys_2,
                ptrs: ptr_split.ptrs_2,
                right_ptr: self.right_ptr.clone(),
                cc: AtomicUsize::new(0)
            };
            let node_2_ref = Arc::new(UnsafeCell::new(Node::Internal(box node_2)));
            self.len = keys_split.keys_1_len;
            self.right_ptr = node_2_ref.clone();
            return Some((node_2_ref, Some(keys_split.pivot_key)));
        } else {
            let mut new_node_len = node_len;
            let mut new_node_pointers = node_len + 1;
            self.keys.insert_at(key, pos, &mut new_node_len);
            self.ptrs.insert_at(ptr.unwrap(), pos + 1, &mut new_node_pointers);
            self.len = new_node_len;
            return None;
        }
    }
    pub fn rebalance_candidate(
        &self,
        pointer_pos: usize,
    ) -> Result<usize, TxnErr> {
        debug_assert!(pointer_pos <= self.len);
        debug!(
            "Searching for rebalance candidate, pos {}, len {}",
            pointer_pos, self.len
        );
        if pointer_pos == 0 {
            Ok(1)
        } else if pointer_pos + 1 >= self.len {
            // the last one, pick left
            Ok(pointer_pos - 1)
        } else {
            // pick the right one
            // we should pick the one  with least pointers, but it cost for the check is too high
            Ok(pointer_pos + 1)
        }
    }
    pub fn merge_children(
        &mut self,
        left_ptr_pos: usize,
        right_ptr_pos: usize,
    ) -> Result<(), TxnErr> {
        let mut left_node = self.ptrs[left_ptr_pos].get();
        let mut right_node = self.ptrs[right_ptr_pos].get();
        debug_assert_ne!(left_node, right_node);
        let left_len = left_node.len();
        let right_len = right_node.len();
        let mut merged_len = 0;
        debug_assert_eq!(left_node.is_ext(), right_node.is_ext());
        if !left_node.is_ext() {
            {
                let right_key_pos = self.key_pos_from_ptr_pos(right_ptr_pos);
                let mut left_innode = left_node.innode_mut();
                let mut right_innode = right_node.innode_mut();
                let right_key = self.keys[right_key_pos].clone();
                left_innode.merge_with(&mut right_innode, right_key);
                merged_len = left_innode.len;
            }
            txn.update(left_ref, left_node);
        } else {
            let mut right_extnode = right_node.extnode_mut();
            {
                let mut left_extnode = left_node.extnode_mut();
                left_extnode.merge_with(&mut right_extnode);
                merged_len = left_extnode.len;
            }
            right_extnode.remove_node();
        }
        self.remove_at(right_ptr_pos);
        debug!(
            "Removing merged node, left {}, right {}, merged {}",
            left_len, right_len, merged_len
        );
        txn.delete(right_ref);
        Ok(())
    }
    pub fn merge_with(&mut self, right: &mut Self, right_key: EntryKey) {
        debug!(
            "Merge internal node, left len {}, right len {}, right_key {:?}",
            self.len, right.len, right_key
        );
        let mut self_len = self.len;
        let new_len = self_len + right.len + 1;
        debug_assert!(new_len <= self.keys.len());
        // moving keys
        self.keys[self_len] = right_key;
        for i in self_len + 1..new_len {
            mem::swap(&mut self.keys[i], &mut right.keys[i - self_len - 1]);
        }
        for i in self_len + 1..new_len + 2 {
            mem::swap(&mut self.ptrs[i], &mut right.ptrs[i - self_len - 1]);
        }
        self.len += right.len + 1;
    }
    pub fn relocate_children(
        &mut self,
        left_ptr_pos: usize,
        right_ptr_pos: usize,
    ) -> Result<(), TxnErr> {
        debug_assert_ne!(left_ptr_pos, right_ptr_pos);
        let mut left_node = self.ptrs[left_ptr_pos].get();
        let mut right_node = self.ptrs[right_ptr_pos].get();
        let mut new_right_node_key = Default::default();
        let half_full_pos = (left_node.len() + right_node.len()) / 2;
        debug_assert_eq!(left_node.is_ext(), right_node.is_ext());
        if !left_node.is_ext() {
            // relocate internal sub nodes

            {
                let mut left_innode = left_node.innode_mut();
                let mut right_innode = right_node.innode_mut();

                debug!(
                    "Before relocation internal children. left {}:{:?} right {}:{:?}",
                    left_innode.len, left_innode.keys, right_innode.len, right_innode.keys
                );

                let mut new_left_keys = EntryKeySlice::init();
                let mut new_left_ptrs = NodePtrCellSlice::init();

                let mut new_right_keys = EntryKeySlice::init();
                let mut new_right_ptrs = NodePtrCellSlice::init();

                let pivot_key = self.keys[right_ptr_pos - 1].to_owned();
                let mut new_left_keys_len = 0;
                let mut new_right_keys_len = 0;
                for (i, key) in chain(
                    chain(
                        left_innode.keys[..left_innode.len].iter_mut(),
                        [pivot_key].iter_mut(),
                    ),
                    right_innode.keys[..right_innode.len].iter_mut(),
                )
                .enumerate()
                {
                    let key_owned = mem::replace(key, Default::default());
                    if i < half_full_pos {
                        new_left_keys[i] = key_owned;
                        new_left_keys_len += 1;
                    } else if i == half_full_pos {
                        new_right_node_key = key_owned
                    } else {
                        let nk_index = i - half_full_pos - 1;
                        new_right_keys[nk_index] = key_owned;
                        new_right_keys_len += 1;
                    }
                }

                for (i, ptr) in chain(
                    left_innode.ptrs[..left_innode.len + 1].iter_mut(),
                    right_innode.ptrs[..right_innode.len + 1].iter_mut(),
                )
                .enumerate()
                {
                    let ptr_owned = mem::replace(ptr, Default::default());
                    if i < half_full_pos + 1 {
                        new_right_ptrs[i] = ptr_owned;
                    } else {
                        new_left_ptrs[i - half_full_pos - 1] = ptr_owned;
                    }
                }

                left_innode.keys = new_left_keys;
                left_innode.ptrs = new_left_ptrs;
                left_innode.len = new_left_keys_len;

                right_innode.keys = new_right_keys;
                right_innode.ptrs = new_right_ptrs;
                right_innode.len = new_right_keys_len;

                debug!(
                    "Relocated internal children. left {}:{:?} right {}:{:?}",
                    left_innode.len, left_innode.keys, right_innode.len, right_innode.keys
                );
            }
        } else if left_node.is_ext() {
            // relocate external sub nodes

            let mut left_extnode = left_node.extnode_mut();
            let mut right_extnode = right_node.extnode_mut();

            debug!(
                "Before relocation external children. left {}:{:?} right {}:{:?}",
                left_extnode.len, left_extnode.keys, right_extnode.len, right_extnode.keys
            );

            let mut new_left_keys = EntryKeySlice::init();
            let mut new_right_keys = EntryKeySlice::init();

            let left_len = left_extnode.len;
            let right_len = right_extnode.len;
            let mut new_left_keys_len = 0;
            let mut new_right_keys_len = 0;
            for (i, key) in chain(
                left_extnode.keys[..left_len].iter_mut(),
                right_extnode.keys[..right_len].iter_mut(),
            )
            .enumerate()
            {
                let key_owned = mem::replace(key, Default::default());
                if i < half_full_pos {
                    new_left_keys[i] = key_owned;
                    new_left_keys_len += 1;
                } else {
                    if i == half_full_pos {
                        new_right_node_key = key_owned.clone()
                    }
                    new_right_keys[i - half_full_pos] = key_owned;
                    new_right_keys_len += 1;
                }
            }

            left_extnode.keys = new_left_keys;
            left_extnode.len = new_left_keys_len;

            right_extnode.keys = new_right_keys;
            right_extnode.len = new_right_keys_len;

            debug!(
                "Relocated external children. left {}:{:?} right {}:{:?}",
                left_extnode.len, left_extnode.keys, right_extnode.len, right_extnode.keys
            );
        }

        let right_key_pos = right_ptr_pos - 1;
        debug!(
            "Setting key at pos {} to new key {:?}",
            right_key_pos, new_right_node_key
        );
        self.keys[right_key_pos] = new_right_node_key;

        Ok(())
    }
}
