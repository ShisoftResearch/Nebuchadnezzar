use hermes::stm::Txn;
use hermes::stm::TxnErr;
use hermes::stm::TxnValRef;
use index::btree::Slice;
use index::btree::*;
use itertools::free::chain;
use std::cell::UnsafeCell;
use std::mem;
use std::sync::atomic::AtomicPtr;
use std::sync::Arc;

pub struct InNode {
    pub keys: EntryKeySlice,
    pub ptrs: NodePtrCellSlice,
    pub len: usize,
    pub right: NodeCellRef,
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
    pub fn search(&self, key: &EntryKey) -> usize {
        self.keys[..self.len]
            .binary_search(key)
            .map(|i| i + 1)
            .unwrap_or_else(|i| i)
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
        new_node: NodeCellRef,
        parent: &NodeCellRef,
    ) -> Option<NodeSplit> {
        let node_len = self.len;
        let ptr_len = self.len + 1;
        let pos = self.search(&key);
        debug!("Insert into internal node at {}, key: {:?}", pos, key);
        debug_assert!(node_len <= NUM_KEYS);
        if node_len == NUM_KEYS {
            let pivot = node_len / 2; // pivot key will be removed
            debug!("Going to split at pivot {}", pivot);
            let parent_guard = parent.write();
            let keys_split = {
                debug!("insert into keys");
                if pivot == pos {
                    debug!("special key treatment when pivot == pos");
                    let mut keys_1 = &mut self.keys;
                    let mut keys_2 = keys_1.split_at_pivot(pivot, node_len);
                    let mut keys_1_len = pivot;
                    let mut keys_2_len = node_len - pivot;
                    let pivot_key = key;
                    InNodeKeysSplit {
                        keys_2,
                        keys_1_len,
                        keys_2_len,
                        pivot_key,
                    }
                } else {
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
                    debug_assert_ne!(pivot, pos);
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
                }
            };
            let ptr_split = {
                if pivot == pos {
                    debug!("special ptr treatment when pivot == pos");
                    let mut ptrs_1 = &mut self.ptrs;
                    let mut ptrs_2 = ptrs_1.split_at_pivot(pivot + 1, ptr_len);
                    let mut ptrs_1_len = pivot + 1;
                    let mut ptrs_2_len = ptr_len - pivot - 1;
                    ptrs_2.insert_at(new_node, 0, &mut ptrs_2_len);
                    debug_assert_eq!(ptrs_1_len, keys_split.keys_1_len + 1);
                    debug_assert_eq!(ptrs_2_len, keys_split.keys_2_len + 1);
                    InNodePtrSplit { ptrs_2 }
                } else {
                    debug!("insert into ptrs");
                    let mut ptrs_1 = &mut self.ptrs;
                    let mut ptrs_2 = ptrs_1.split_at_pivot(pivot + 1, ptr_len);
                    let mut ptrs_1_len = pivot + 1;
                    let mut ptrs_2_len = ptr_len - pivot - 1;
                    let mut ptr_pos = pos + 1;
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
            let node_2 = InNode {
                len: keys_split.keys_2_len,
                keys: keys_split.keys_2,
                ptrs: ptr_split.ptrs_2,
                right: self.right.clone(),
            };
            let node_2_ref = Arc::new(Node::internal(node_2));
            self.len = keys_split.keys_1_len;
            self.right = node_2_ref.clone();
            return Some(NodeSplit {
                new_right_node: node_2_ref,
                left_node_latch: NodeWriteGuard::default(),
                pivot: keys_split.pivot_key,
                parent_latch: parent_guard,
            });
        } else {
            let mut new_node_len = node_len;
            let mut new_node_pointers = node_len + 1;
            self.keys.insert_at(key, pos, &mut new_node_len);
            self.ptrs
                .insert_at(new_node, pos + 1, &mut new_node_pointers);
            self.len = new_node_len;
            return None;
        }
    }
    pub fn rebalance_candidate(&self, pointer_pos: usize) -> usize {
        debug_assert!(pointer_pos <= self.len);
        debug!(
            "Searching for rebalance candidate, pos {}, len {}",
            pointer_pos, self.len
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
        left_node: &mut NodeData,
        right_node: &mut NodeData,
        right_node_next: &mut NodeData,
    ) -> Result<(), TxnErr> {
        let left_node_ref = self.ptrs[left_ptr_pos].clone();
        let left_len = left_node.len();
        let right_len = right_node.len();
        let right_key_pos = self.key_pos_from_ptr_pos(right_ptr_pos);
        let mut merged_len = 0;
        debug!("Merge children, left len {}, right len {}, left_ptr_pos {}, right_ptr_pos {}, right_key_pos {}",
                left_len, right_len, left_ptr_pos, right_ptr_pos, right_key_pos);
        debug_assert_eq!(left_node.is_ext(), right_node.is_ext());
        if !left_node.is_ext() {
            {
                let mut left_innode = left_node.innode_mut();
                let mut right_innode = right_node.innode_mut();
                let right_key = self.keys[right_key_pos].clone();
                left_innode.merge_with(&mut right_innode, right_key);
                left_innode.right = right_innode.right.clone();
                merged_len = left_innode.len;
            }
        } else {
            let mut right_extnode = right_node.extnode_mut();
            let mut left_extnode = left_node.extnode_mut();
            {
                left_extnode.merge_with(&mut right_extnode);
                merged_len = left_extnode.len;
            }
            if !right_node_next.is_none() {
                right_node_next.extnode_mut().prev = left_node_ref
            }
            left_extnode.next = right_extnode.next.clone();
        }
        self.remove_at(right_ptr_pos);
        debug!(
            "Removing merged node at {}, left {}, right {}, merged {}",
            right_ptr_pos,left_len, right_len, merged_len
        );
        debug!("Merged parent level keys: {:?}", self.keys);
        debug!("Merged level keys {:?}", left_node.keys());
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
        for i in self_len + 1..new_len + 1 {
            mem::swap(&mut self.ptrs[i], &mut right.ptrs[i - self_len - 1]);
        }
        self.len += right.len + 1;
    }
    pub fn relocate_children(
        &mut self,
        left_ptr_pos: usize,
        right_ptr_pos: usize,
        left_node: &mut NodeData,
        right_node: &mut NodeData,
    ) {
        debug_assert_ne!(left_ptr_pos, right_ptr_pos);
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
                debug_assert!(pivot_key > smallvec!(0));
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
                        new_left_ptrs[i] = ptr_owned;
                    } else {
                        new_right_ptrs[i - half_full_pos - 1] = ptr_owned;
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
                    new_right_keys[i - half_full_pos] = key_owned;
                    new_right_keys_len += 1;
                }
            }

            new_right_node_key = new_right_keys[0].clone();

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
        debug_assert!(new_right_node_key > smallvec!(0));
        self.keys[right_key_pos] = new_right_node_key;
    }
}
