use index::btree::*;
use std::mem;
use itertools::free::chain;
use index::btree::Slice;
use hermes::stm::TxnValRef;
use hermes::stm::Txn;
use hermes::stm::TxnErr;
use index::btree::external::CacheBufferZone;

#[derive(Clone)]
pub struct InNode {
    pub keys: EntryKeySlice,
    pub pointers: NodePointerSlice,
    pub len: usize
}

pub struct InNodeKeysSplit {
    pub keys_2: EntryKeySlice,
    pub keys_1_len: usize,
    pub keys_2_len: usize,
    pub pivot_key: EntryKey
}

pub struct InNodePtrSplit {
    pub ptrs_2: NodePointerSlice
}

impl InNode {
    pub fn remove(&mut self, pos: usize) {
        let mut n_key_len = self.len;
        let mut n_ptr_len = n_key_len + 1;
        self.keys.remove_at(pos, &mut n_key_len);
        self.pointers.remove_at(pos + 1, &mut n_ptr_len);
        self.len = n_key_len;
    }
    pub fn insert(&mut self, key: EntryKey, ptr: Option<TxnValRef>, pos: usize)
        -> Option<(Node, Option<EntryKey>)>
    {
        debug!("Insert into internal node at {}, key: {:?}", pos, key);
        let node_len = self.len;
        let ptr_len = self.len + 1;
        debug_assert!(node_len <= NUM_KEYS);
        if node_len == NUM_KEYS {
            let pivot = node_len / 2; // pivot key will be removed
            let keys_split = {
                let mut keys_1 = &mut self.keys;
                let mut keys_2 = keys_1.split_at_pivot(pivot + 1, node_len);
                let mut keys_1_len = pivot; // will not count the pivot
                let mut keys_2_len = node_len - pivot - 1;
                let pivot_key = keys_1[pivot].to_owned();
                insert_into_split(
                    key,
                    keys_1, &mut keys_2,
                    &mut keys_1_len, &mut keys_2_len,
                    pos, pivot - 1);
                InNodeKeysSplit {
                    keys_2, keys_1_len, keys_2_len, pivot_key
                }
            };
            let ptr_split = {
                let mut ptrs_1 = &mut self.pointers;
                let mut ptrs_2 = ptrs_1.split_at_pivot(pivot + 1, ptr_len);
                let mut ptrs_1_len = pivot + 1;
                let mut ptrs_2_len = ptr_len - pivot - 1;
                insert_into_split(
                    ptr.unwrap(),
                    ptrs_1, &mut ptrs_2,
                    &mut ptrs_1_len, &mut ptrs_2_len,
                    pos + 1, pivot);
                debug_assert_eq!(ptrs_1_len, keys_split.keys_1_len + 1);
                debug_assert_eq!(ptrs_2_len, keys_split.keys_2_len + 1);
                InNodePtrSplit { ptrs_2 }
            };
            let node_2 = InNode {
                len: keys_split.keys_2_len,
                keys: keys_split.keys_2,
                pointers: ptr_split.ptrs_2
            };
            self.len = keys_split.keys_1_len;
            return Some((Node::Internal(box node_2), Some(keys_split.pivot_key)));
        } else {
            let mut new_node_len = node_len;
            let mut new_node_pointers = node_len + 1;
            self.keys.insert_at(key, pos, &mut new_node_len);
            self.pointers.insert_at(ptr.unwrap(), pos + 1, &mut new_node_pointers);
            self.len = new_node_len;
            return None;
        }
    }
    pub fn rebalance_candidate(
        &self,
        pointer_pos: usize,
        txn: &mut Txn,
        bz: &CacheBufferZone
    ) -> Result<usize, TxnErr> {
        debug_assert!(pointer_pos <= self.len);
        debug!("Searching for rebalance candidate, pos {}, len {}", pointer_pos, self.len);
        if pointer_pos == 0 {
            Ok(1)
        } else if pointer_pos + 1 >= self.len {
            // the last one, pick left
            Ok(pointer_pos - 1)
        } else {
            // pick the one with least pointers
            let left_pos = pointer_pos - 1;
            let right_pos = pointer_pos + 1;
            let left_node = txn.read::<Node>(self.pointers[left_pos])?.unwrap();
            let right_node = txn.read::<Node>(self.pointers[right_pos])?.unwrap();
            if left_node.len(bz) <= right_node.len(bz) {
                Ok(left_pos)
            } else {
                Ok(right_pos)
            }
        }
    }
    pub fn merge_children(
        &mut self,
        left_ptr_pos: usize,
        right_ptr_pos: usize,
        txn: &mut Txn,
        bz: &CacheBufferZone
    ) -> Result<(), TxnErr> {
        let left_ref = self.pointers[left_ptr_pos];
        let right_ref = self.pointers[right_ptr_pos];
        let mut left_node = txn.read_owned::<Node>(left_ref)?.unwrap();
        let mut right_node = txn.read_owned::<Node>(right_ref)?.unwrap();
        let right_key_pos = right_ptr_pos - 1;
        debug_assert_eq!(left_node.is_ext(), right_node.is_ext());
        if !left_node.is_ext() {
            {
                let mut left_innode = left_node.innode_mut();
                let mut right_innode = right_node.innode_mut();
                let right_key = self.keys[right_key_pos].clone();
                left_innode.merge_with(&mut right_innode, right_key);
            }
            txn.update(left_ref, left_node);
        } else {
            let mut right_extnode = right_node.extnode_mut(bz);
            let mut left_extnode = left_node.extnode_mut(bz);
            left_extnode.merge_with(&mut right_extnode);
            bz.delete(&right_extnode.id);
        }
        self.remove(right_key_pos);
        txn.delete(right_ref);
        Ok(())
    }
    pub fn merge_with(&mut self, right: &mut Self, right_key: EntryKey) {
        let mut self_len = self.len;
        let new_len = self_len + right.len + 1;
        debug_assert!(new_len <= self.keys.len());
        // moving keys
        self.keys[self_len] = right_key;
        // TODO: avoid repeatedly default construction
        for i in self_len + 1 .. new_len {
            self.keys[i] = mem::replace(&mut right.keys[i - self_len - 1], Default::default());
        }
        for i in self_len .. new_len + 1 {
            self.pointers[i] = mem::replace(&mut right.pointers[i - self_len - 1], Default::default());
        }
        self.len += right.len + 1;
    }
    pub fn relocate_children(
        &mut self,
        left_ptr_pos: usize,
        right_ptr_pos: usize,
        txn: &mut Txn,
        bz: &CacheBufferZone
    ) -> Result<(), TxnErr> {
        debug_assert_ne!(left_ptr_pos, right_ptr_pos);
        let left_ref = self.pointers[left_ptr_pos];
        let right_ref = self.pointers[right_ptr_pos];
        let mut left_node = txn.read_owned::<Node>(left_ref)?.unwrap();
        let mut right_node = txn.read_owned::<Node>(right_ref)?.unwrap();
        let mut new_right_node_key = Default::default();
        let half_full_pos = NUM_KEYS / 2 + 1;
        debug_assert_eq!(left_node.is_ext(), right_node.is_ext());
        if !left_node.is_ext() {
            // relocate internal sub nodes

            {
                let mut left_innode = left_node.innode_mut();
                let mut right_innode = right_node.innode_mut();

                debug!("Before relocation internal children. left {}:{:?} right {}:{:?}",
                       left_innode.len, left_innode.keys,
                       right_innode.len, right_innode.keys);

                let mut new_left_keys = EntryKeySlice::init();
                let mut new_left_ptrs = NodePointerSlice::init();

                let mut new_right_keys = EntryKeySlice::init();
                let mut new_right_ptrs = NodePointerSlice::init();

                let pivot_key = self.keys[right_ptr_pos - 1].to_owned();
                let mut new_left_keys_len = 0;
                let mut new_right_keys_len = 0;
                for (i, key) in chain(
                    chain(left_innode.keys[..left_innode.len].iter_mut(),[pivot_key].iter_mut()),
                    right_innode.keys[..right_innode.len].iter_mut()
                ).enumerate() {
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
                    left_innode.pointers[..left_innode.len + 1].iter_mut(),
                    right_innode.pointers[..right_innode.len + 1].iter_mut()
                ).enumerate() {
                    let ptr_owned = mem::replace(ptr, Default::default());
                    if i < half_full_pos {
                        new_right_ptrs[i] = ptr_owned;
                    } else {
                        new_left_ptrs[i - half_full_pos] = ptr_owned;
                    }
                }

                left_innode.keys = new_left_keys;
                left_innode.pointers = new_left_ptrs;
                left_innode.len = new_left_keys_len;

                right_innode.keys = new_right_keys;
                right_innode.pointers = new_right_ptrs;
                right_innode.len = new_right_keys_len;

                debug!("Relocated internal children. left {}:{:?} right {}:{:?}",
                       left_innode.len, left_innode.keys,
                       right_innode.len, right_innode.keys);
            }

            txn.update(left_ref, left_node);
            txn.update(right_ref, right_node);

        } else if left_node.is_ext() {
            // relocate external sub nodes

            let mut left_extnode = left_node.extnode_mut(bz);
            let mut right_extnode = right_node.extnode_mut(bz);

            debug!("Before relocation external children. left {}:{:?} right {}:{:?}",
                   left_extnode.len, left_extnode.keys,
                   right_extnode.len, right_extnode.keys);

            let mut new_left_keys = EntryKeySlice::init();
            let mut new_right_keys = EntryKeySlice::init();

            let left_len = left_extnode.len;
            let right_len = right_extnode.len;
            let mut new_left_keys_len = 0;
            let mut new_right_keys_len = 0;
            for (i, key) in chain(
                left_extnode.keys[..left_len].iter_mut(),
                right_extnode.keys[..right_len].iter_mut()
            ).enumerate() {
                let key_owned = mem::replace(key, Default::default());
                if i < half_full_pos {
                    new_left_keys[i] = key_owned;
                    new_left_keys_len += 1;
                } else {
                    if i == half_full_pos {
                        new_right_node_key = key_owned.clone()
                    }
                    let nk_index = i - half_full_pos - 1;
                    new_right_keys[nk_index] = key_owned;
                    new_right_keys_len += 1;
                }
            }

            left_extnode.keys = new_left_keys;
            left_extnode.len = new_left_keys_len;

            right_extnode.keys = new_right_keys;
            right_extnode.len = new_right_keys_len;

            debug!("Relocated external children. left {}:{:?} right {}:{:?}",
                   left_extnode.len, left_extnode.keys,
                   right_extnode.len, right_extnode.keys);
        }

        self.keys[right_ptr_pos - 1] = new_right_node_key;

        Ok(())
    }
}