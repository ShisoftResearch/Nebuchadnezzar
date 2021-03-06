use super::*;
use crate::index::ranged::lsm::tree::DeletionSet;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use crate::{index::ranged::lsm::btree::level::LEVEL_M, ram::cell::OwnedCell};
use crossbeam::queue::SegQueue;
use dovahkiin::types::custom_types::id::Id;
use itertools::Itertools;
use std::marker::PhantomData;
use std::{mem, panic};

pub const PAGE_SCHEMA: &'static str = "NEB_BTREE_PAGE";
pub const KEYS_FIELD: &'static str = "keys";
pub const NEXT_FIELD: &'static str = "next";
pub const PREV_FIELD: &'static str = "prev";

pub enum ChangingNode {
    Deleted(Id),
    Modified(NodeModified),
}

pub struct NodeModified {
    pub node: NodeCellRef,
    pub deletion: Arc<DeletionSet>,
}

lazy_static! {
    pub static ref KEYS_KEY_HASH: u64 = key_hash(KEYS_FIELD);
    pub static ref NEXT_PAGE_KEY_HASH: u64 = key_hash(NEXT_FIELD);
    pub static ref PREV_PAGE_KEY_HASH: u64 = key_hash(PREV_FIELD);
    pub static ref PAGE_SCHEMA_ID: u32 = key_hash(PAGE_SCHEMA) as u32;
    pub static ref CHANGED_NODES: SegQueue<(usize, ChangingNode)> = SegQueue::new();
    pub static ref CHANGE_COUNTER: AtomicUsize = AtomicUsize::new(0);
}

pub struct ExtNode<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub id: Id,
    pub keys: KS,
    pub next: NodeCellRef,
    pub prev: NodeCellRef,
    pub len: usize,
    pub right_bound: EntryKey,
    pub mark: PhantomData<PS>,
}

impl<KS, PS> ExtNode<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn new(id: Id, right_bound: EntryKey) -> Box<Self> {
        box ExtNode {
            id,
            keys: KS::init(),
            next: Node::<KS, PS>::none_ref(),
            prev: Node::<KS, PS>::none_ref(),
            len: 0,
            right_bound,
            mark: PhantomData,
        }
    }

    pub fn from_cell(cell: &OwnedCell) -> Box<IncubatingExtNode<KS, PS>> {
        let cell_id = cell.id();
        let _cell_version = cell.header.version;
        let next = cell.data[*NEXT_PAGE_KEY_HASH].id().unwrap();
        let prev = cell.data[*PREV_PAGE_KEY_HASH].id().unwrap();
        let keys = &cell.data[*KEYS_KEY_HASH];
        let keys_array = if let OwnedValue::PrimArray(OwnedPrimArray::SmallBytes(ref array)) = keys
        {
            array
        } else {
            panic!()
        };
        let mut key_slice = KS::init();
        let mut key_count = 0;
        for (i, key_val) in keys_array.iter().enumerate() {
            key_slice.as_slice()[i] = EntryKey::from_slice(key_val.as_slice());
            key_count += 1;
        }
        let ext_node = ExtNode {
            id: cell_id,
            keys: key_slice,
            next: NodeCellRef::default(), // UNDETERMINED
            prev: NodeCellRef::default(), // UNDETERMINED
            len: key_count,
            right_bound: max_entry_key(), // UNDETERMINED
            mark: PhantomData,
        };
        box IncubatingExtNode {
            node: ext_node,
            prev_id: *prev,
            next_id: *next,
        }
    }

    pub fn to_cell(&self, deleted: &DeletionSet) -> OwnedCell {
        let mut value = OwnedValue::Map(OwnedMap::new());
        let prev_id = {
            let node = read_unchecked::<KS, PS>(&self.prev);
            if node.is_none() {
                Id::unit_id()
            } else {
                node.extnode().id
            }
        };
        let next_id = read_node(&self.next, |node: &NodeReadHandler<KS, PS>| {
            if node.is_none() {
                Id::unit_id()
            } else {
                node.extnode().id
            }
        });

        value[*NEXT_PAGE_KEY_HASH] = OwnedValue::Id(next_id);
        value[*PREV_PAGE_KEY_HASH] = OwnedValue::Id(prev_id);
        value[*KEYS_KEY_HASH] = self.keys.as_slice_immute()[..self.len]
            .iter()
            .filter(|&key| !deleted.contains(key))
            .map(|key| SmallBytes::from_vec(key.as_slice().to_vec()))
            .collect_vec()
            .value();
        OwnedCell::new_with_id(*PAGE_SCHEMA_ID, &self.id, value)
    }

    pub fn search(&self, key: &EntryKey) -> usize {
        self.keys.as_slice_immute()[..self.len]
            .binary_search(key)
            .unwrap_or_else(|i| i)
    }
    pub fn search_unwindable(
        &self,
        key: &EntryKey,
    ) -> Result<usize, Box<dyn std::any::Any + Send + 'static>> {
        panic::catch_unwind(panic::AssertUnwindSafe(|| self.search(key)))
    }
    pub fn remove_at(&mut self, pos: usize) {
        let cached_len = &mut self.len;
        trace!(
            "Removing from external pos {}, len {}, key {:?}",
            pos,
            cached_len,
            self.keys.as_slice()[pos]
        );
        self.keys.remove_at(pos, cached_len);
    }

    pub fn split_insert(
        &mut self,
        key: EntryKey,
        pos: usize,
        self_ref: &NodeCellRef,
        self_next: &mut NodeWriteGuard<KS, PS>,
        tree: &BPlusTree<KS, PS>,
    ) -> (NodeCellRef, EntryKey) {
        // cached.dump();
        let keys_1 = &mut self.keys;
        let pivot = self.len / 2;
        let new_page_id = BPlusTree::<KS, PS>::new_page_id();
        let mut keys_2 = keys_1.split_at_pivot(pivot, self.len);
        let mut keys_1_len = pivot;
        let mut keys_2_len = self.len - pivot;
        // modify next node point previous to new node
        insert_into_split(
            key,
            keys_1,
            &mut keys_2,
            &mut keys_1_len,
            &mut keys_2_len,
            pos,
        );
        let pivot_key = keys_2.as_slice_immute()[0].clone();
        let extnode_2: Box<ExtNode<KS, PS>> = box ExtNode {
            id: new_page_id,
            keys: keys_2,
            next: self.next.clone(),
            prev: self_ref.clone(),
            len: keys_2_len,
            right_bound: self.right_bound.clone(),
            mark: PhantomData,
        };
        debug_assert!(
            pivot_key < self.right_bound,
            "pivot {:?}, right bound {:?}",
            pivot_key,
            self.right_bound
        );
        debug_assert!(pivot_key > Default::default());
        debug_assert!(
            &pivot_key > &keys_1.as_slice()[keys_1_len - 1],
            "{:?} / {:?} @ {}",
            pivot_key,
            &keys_1.as_slice()[keys_1_len - 1],
            pos
        );
        self.len = keys_1_len;
        self.right_bound = pivot_key.clone();
        trace!(
            "Split to left len {}, right len {}, right prev id: {:?}",
            self.len,
            extnode_2.len,
            read_unchecked::<KS, PS>(&extnode_2.prev).ext_id()
        );
        debug_assert_eq!(self.right_bound, pivot_key);
        debug_assert_eq!(&self.right_bound, &extnode_2.keys.as_slice_immute()[0]);
        debug_assert!(
            self.right_bound < extnode_2.right_bound,
            "node right bound >= next right bound {:?} - {:?}",
            self.right_bound,
            extnode_2.right_bound
        );
        debug_assert!(
            self.right_bound <= extnode_2.keys.as_slice_immute()[0],
            "node right bound < next first key, {:?} - {:?}",
            self.right_bound,
            &extnode_2.keys.as_slice_immute()[..extnode_2.len]
        );
        let node_2 = NodeCellRef::new(Node::with_external(extnode_2));
        if !self_next.is_ref_none() {
            let mut self_next_node = self_next.extnode_mut(tree);
            debug_assert!(
                self_next_node.prev.ptr_eq(&self_ref),
                "node next node's prev node is not self {:?}",
                self_next_node.keys.as_slice_immute()[0]
            );
            self_next_node.prev = node_2.clone();
        }
        self.next = node_2.clone();
        make_changed(&node_2, tree);
        (node_2, pivot_key)
    }

    pub fn insert(
        &mut self,
        key: &EntryKey,
        tree: &BPlusTree<KS, PS>,
        self_ref: &NodeCellRef,
        parent: &NodeCellRef,
    ) -> Option<Option<NodeSplit<KS, PS>>> {
        let key = key.clone();
        let pos = self.search(&key);
        debug_assert!(self.len <= KS::slice_len());
        debug_assert!(pos <= self.len);
        if self.len > pos && self.keys.as_slice_immute()[pos] == key {
            trace!("inserting existing key");
            return None;
        }
        Some(if self.len == KS::slice_len() {
            // need to split
            trace!("insert to external with split, key {:?}, pos {}", key, pos);
            let mut self_next: NodeWriteGuard<KS, PS> = write_node(&self.next);
            let parent_latch: NodeWriteGuard<KS, PS> = write_node(parent);
            let (node_2, pivot_key) = self.split_insert(key, pos, self_ref, &mut self_next, tree);
            Some(NodeSplit {
                new_right_node: node_2,
                pivot: pivot_key,
                parent_latch,
                left_node_latch: NodeWriteGuard::default(),
            })
        } else {
            trace!("insert to external without split at {}, key {:?}", pos, key);
            self.keys.insert_at(key, pos, &mut self.len);
            None
        })
    }

    pub fn merge_with(&mut self, right: &mut Self) {
        trace!(
            "Merge external node, left len {}:{:?}, right len {}:{:?}",
            self.len,
            self.keys,
            right.len,
            right.keys
        );
        let self_len = self.len;
        let new_len = self.len + right.len;
        debug_assert!(new_len <= KS::slice_len());
        for i in self.len..new_len {
            mem::swap(
                &mut self.keys.as_slice()[i],
                &mut right.keys.as_slice()[i - self_len],
            );
        }
        self.len = new_len;
    }

    pub fn remove_contains(&mut self, set: &DeletionSet) {
        let remaining_keys = {
            let remaining = self.keys.as_slice()[..self.len]
                .iter()
                .filter(|&k| !set.contains(k))
                .collect_vec();
            if remaining.len() == self.len {
                return;
            }
            remaining.into_iter().cloned().collect_vec()
        };
        let self_key_slice = self.keys.as_slice();
        self.len = remaining_keys.len();
        for (i, k_ref) in remaining_keys.into_iter().enumerate() {
            self_key_slice[i] = k_ref;
        }
    }

    pub fn merge_sort(&mut self, right: &[&EntryKey]) {
        trace!("Merge sort have right nodes {:?}", right);
        let self_len_before_merge = self.len;
        debug_assert!(self_len_before_merge + right.len() <= KS::slice_len());
        let mut pos = 0;
        let mut left_pos = 0;
        let mut right_pos = 0;
        let mut left_keys = mem::replace(&mut self.keys, KS::init());
        let left = left_keys.as_slice();
        while left_pos < self.len && right_pos < right.len() {
            let left_key = &left[left_pos];
            let right_key = right[right_pos];
            if left_key <= right_key {
                self.keys.as_slice()[pos] = left_key.clone();
                left_pos += 1;
            } else if left_key > right_key {
                self.keys.as_slice()[pos] = right_key.clone();
                right_pos += 1;
            }
            if left_key == right_key {
                // when duplication detected, skip the duplicated one
                right_pos += 1;
            }
            if pos == 0 || self.keys.as_slice_immute()[pos - 1] != self.keys.as_slice_immute()[pos]
            {
                // if no duplicate assigned, step further
                pos += 1;
            }
        }
        for key in &left[left_pos..self.len] {
            if pos == 0 || &self.keys.as_slice_immute()[pos - 1] != key {
                self.keys.as_slice()[pos] = key.clone();
                pos += 1;
            }
            left_pos += 1;
        }
        for key in &right[right_pos..] {
            if pos == 0 || &self.keys.as_slice_immute()[pos - 1] != *key {
                self.keys.as_slice()[pos] = (*key).clone();
                pos += 1;
            }
            right_pos += 1;
        }
        trace!(
            "Merge sorted have keys {:?}",
            &self.keys.as_slice_immute()[..pos]
        );
        self.len = pos;
        debug_assert_eq!(self_len_before_merge, left_pos);
        debug_assert_eq!(right.len(), right_pos);
        debug_assert_eq!(self.len, self_len_before_merge + right.len());
        trace!(
            "Merge sorted page have keys: {:?}",
            &self.keys.as_slice_immute()[..self.len]
        )
    }

    pub fn dump(&self) {
        trace!("Dumping {:?}, keys {}", self.id, self.len);
        for i in 0..KS::slice_len() {
            trace!("{}\t- {:?}", i, self.keys.as_slice_immute()[i]);
        }
    }
}

pub fn make_changed<KS, PS>(node: &NodeCellRef, tree: &BPlusTree<KS, PS>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // Only accept lower higher level trees
    if KS::slice_len() > LEVEL_M {
        CHANGED_NODES.push((
            CHANGE_COUNTER.fetch_add(1, Relaxed),
            ChangingNode::Modified(NodeModified {
                node: node.clone(),
                deletion: tree.deletion.clone(),
            }),
        ));
    }
}

pub fn make_deleted<KS, PS>(id: &Id)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // Only accept lower higher level trees
    if KS::slice_len() > LEVEL_M {
        CHANGED_NODES.push((
            CHANGE_COUNTER.fetch_add(1, Relaxed),
            ChangingNode::Deleted(*id),
        ));
    }
}

pub fn page_schema() -> Schema {
    Schema {
        id: *PAGE_SCHEMA_ID,
        name: String::from(PAGE_SCHEMA),
        key_field: None,
        str_key_field: None,
        is_dynamic: false,
        fields: Field::new(
            "*",
            Type::Map,
            false,
            false,
            Some(vec![
                Field::new(NEXT_FIELD, Type::Id, false, false, None, vec![]),
                Field::new(PREV_FIELD, Type::Id, false, false, None, vec![]),
                Field::new(
                    KEYS_FIELD,
                    // The small bytes is for keys
                    Type::SmallBytes,
                    false,
                    true,
                    None,
                    vec![],
                ),
            ]),
            vec![],
        ),
    }
}

pub struct IncubatingExtNode<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub node: ExtNode<KS, PS>,
    pub prev_id: Id,
    pub next_id: Id,
}
