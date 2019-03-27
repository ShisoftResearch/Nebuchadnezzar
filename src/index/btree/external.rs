use bifrost::utils::async_locks::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use bifrost::utils::fut_exec::wait;
use client::AsyncClient;
use core::borrow::BorrowMut;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::custom_types::map::Map;
use dovahkiin::types::type_id_of;
use dovahkiin::types::value::ToValue;
use futures::Future;
use index::btree::*;
use index::EntryKey;
use index::Slice;
use itertools::Itertools;
use owning_ref::{OwningHandle, OwningRef, RcRef};
use ram::cell::Cell;
use ram::schema::{Field, Schema};
use ram::types::*;
use std::cell::Ref;
use std::cell::RefCell;
use std::cell::RefMut;
use std::cell::UnsafeCell;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use utils::lru_cache::LRUCache;

const PAGE_SCHEMA: &'static str = "NEB_BTREE_PAGE";
const KEYS_FIELD: &'static str = "keys";
const NEXT_FIELD: &'static str = "next";
const PREV_FIELD: &'static str = "prev";

lazy_static! {
    static ref KEYS_KEY_HASH: u64 = key_hash(KEYS_FIELD);
    static ref NEXT_PAGE_KEY_HASH: u64 = key_hash(NEXT_FIELD);
    static ref PREV_PAGE_KEY_HASH: u64 = key_hash(PREV_FIELD);
    static ref PAGE_SCHEMA_ID: u32 = key_hash(PAGE_SCHEMA) as u32;
}

thread_local! {
    static CHANGED_NODES: RefCell<Vec<NodeCellRef>> = RefCell::new(vec![]);
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
    pub dirty: bool,
    pub right_bound: EntryKey,
    mark: PhantomData<PS>,
}

pub struct ExtNodeSplit<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub node_2: ExtNode<KS, PS>,
    pub keys_1_len: usize,
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
            dirty: false,
            right_bound,
            mark: PhantomData,
        }
    }

    pub fn to_cell(&self) -> Cell {
        let mut value = Value::Map(Map::new());
        let prev_id = read_node(&self.prev, |node: &NodeReadHandler<KS, PS>| {
            node.extnode().id
        });
        let next_id = read_node(&self.next, |node: &NodeReadHandler<KS, PS>| {
            node.extnode().id
        });

        value[*NEXT_PAGE_KEY_HASH] = Value::Id(prev_id);
        value[*PREV_PAGE_KEY_HASH] = Value::Id(next_id);
        value[*KEYS_KEY_HASH] = self.keys.as_slice_immute()[..self.len]
            .iter()
            .map(|key| SmallBytes::from_vec(key.as_slice().to_vec()))
            .collect_vec()
            .value();
        Cell::new_with_id(*PAGE_SCHEMA_ID, &self.id, value)
    }
    pub fn search(&self, key: &EntryKey) -> usize {
        self.keys.as_slice_immute()[..self.len]
            .binary_search(key)
            .unwrap_or_else(|i| i)
    }
    pub fn remove_at(&mut self, pos: usize) {
        let cached_len = &mut self.len;
        debug!(
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
        let pivot = self.len / 2;
        let new_page_id = tree.new_page_id();
        let keys_1 = &mut self.keys;
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
        let pivot_key = keys_2.as_slice()[0].clone();
        let extnode_2: Box<ExtNode<KS, PS>> = box ExtNode {
            id: new_page_id,
            keys: keys_2,
            next: self.next.clone(),
            prev: self_ref.clone(),
            len: keys_2_len,
            dirty: true,
            right_bound: mem::replace(&mut self.right_bound, pivot_key.clone()),
            mark: PhantomData,
        };
        debug_assert!(pivot_key > smallvec!(0));
        debug_assert!(
            &pivot_key > &keys_1.as_slice()[keys_1_len - 1],
            "{:?} / {:?} @ {}",
            pivot_key,
            &keys_1.as_slice()[keys_1_len - 1],
            pos
        );
        self.len = keys_1_len;
        debug!(
            "Split to left len {}, right len {}, right prev id: {:?}",
            self.len,
            extnode_2.len,
            read_unchecked::<KS, PS>(&extnode_2.prev).ext_id()
        );
        let node_2 = NodeCellRef::new(Node::with_external(extnode_2));
        if !self_next.is_none() {
            let mut self_next_node = self_next.extnode_mut();
            debug_assert!(Arc::ptr_eq(&self_next_node.prev.inner, &self_ref.inner), "{:?}", self_next_node.keys.as_slice_immute()[0]);
            self_next_node.prev = node_2.clone();
        }
        self.next = node_2.clone();

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
            return None;
        }
        Some(if self.len == KS::slice_len() {
            // need to split
            debug!("insert to external with split, key {:?}, pos {}", key, pos);
            let mut self_next: NodeWriteGuard<KS, PS> = write_non_empty(write_node(&self.next));
            let parent_latch: NodeWriteGuard<KS, PS> = write_node(parent);
            let (node_2, pivot_key) = self.split_insert(key, pos, self_ref, &mut self_next, tree);
            Some(NodeSplit {
                new_right_node: node_2,
                pivot: pivot_key,
                parent_latch,
                left_node_latch: NodeWriteGuard::default(),
            })
        } else {
            debug!("insert to external without split at {}, key {:?}", pos, key);
            self.keys.insert_at(key, pos, &mut self.len);
            None
        })
    }

    pub fn merge_with(&mut self, right: &mut Self) {
        debug!(
            "Merge external node, left len {}:{:?}, right len {}:{:?}",
            self.len, self.keys, right.len, right.keys
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

    pub fn remove_contains(&mut self, set: &mut BTreeSet<EntryKey>) {
        let remaining_keys = {
            let remaining = self.keys.as_slice()[..self.len]
                .iter()
                .filter(|&k| !set.remove(k))
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
        debug!("Merge sort have right nodes {:?}", right);
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
        debug!(
            "Merge sorted have keys {:?}",
            &self.keys.as_slice_immute()[..pos]
        );
        self.len = pos;
        self.dirty = true;
        debug_assert_eq!(self_len_before_merge, left_pos);
        debug_assert_eq!(right.len(), right_pos);
        debug_assert_eq!(self.len, self_len_before_merge + right.len());
        debug!(
            "Merge sorted page have keys: {:?}",
            &self.keys.as_slice_immute()[..self.len]
        )
    }

    pub fn dump(&self) {
        debug!("Dumping {:?}, keys {}", self.id, self.len);
        for i in 0..KS::slice_len() {
            debug!("{}\t- {:?}", i, self.keys.as_slice_immute()[i]);
        }
    }
    pub fn is_dirty(&self) -> bool {
        unimplemented!()
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
            0,
            false,
            false,
            Some(vec![
                Field::new(NEXT_FIELD, type_id_of(Type::Id), false, false, None),
                Field::new(PREV_FIELD, type_id_of(Type::Id), false, false, None),
                Field::new(KEYS_FIELD, type_id_of(Type::SmallBytes), false, true, None),
            ]),
        ),
    }
}

pub struct NodeCache<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    nodes: RefCell<HashMap<Id, NodeCellRef>>,
    storage: Arc<AsyncClient>,
    marker: PhantomData<(KS, PS)>,
}

impl<KS, PS> NodeCache<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        NodeCache {
            nodes: RefCell::new(HashMap::new()),
            storage: neb_client.clone(),
            marker: PhantomData,
        }
    }

    pub fn get(&self, id: &Id) -> NodeCellRef {
        if id.is_unit_id() {
            return NodeCellRef::new(Node::<KS, PS>::with_none());
        }
        let mut nodes = self.nodes.borrow_mut();
        nodes
            .entry(*id)
            .or_insert_with(|| {
                let cell = self.storage.read_cell(*id).wait().unwrap().unwrap();
                NodeCellRef::new(Node::with_external(self.extnode_from_cell(cell)))
            })
            .clone()
    }

    fn extnode_from_cell(&self, cell: Cell) -> Box<ExtNode<KS, PS>> {
        let cell_id = cell.id();
        let _cell_version = cell.header.version;
        let next = cell.data[*NEXT_PAGE_KEY_HASH].Id().unwrap();
        let prev = cell.data[*PREV_PAGE_KEY_HASH].Id().unwrap();
        let keys = &cell.data[*KEYS_KEY_HASH];
        let _keys_len = keys.len().unwrap();
        let keys_array = if let Value::PrimArray(PrimitiveArray::SmallBytes(ref array)) = keys {
            array
        } else {
            panic!()
        };
        let mut key_slice = KS::init();
        let mut key_count = 0;
        for (i, key_val) in keys_array.iter().enumerate() {
            key_slice.as_slice()[i] = EntryKey::from(key_val.as_slice());
            key_count += 1;
        }
        box ExtNode {
            id: cell_id,
            keys: key_slice,
            next: self.get(next),
            prev: self.get(prev),
            len: key_count,
            dirty: false,
            right_bound: max_entry_key(), // TODO: assign a real one on reconstructing
            mark: PhantomData,
        }
    }
}

pub fn make_changed(node: &NodeCellRef) {
    let node = node.clone();
    CHANGED_NODES.with(|changes| {
       changes.borrow_mut().push(node);
    });
}

pub fn flush_changed() -> Vec<NodeCellRef> {
    CHANGED_NODES.with(|changes| {
        mem::replace(&mut*changes.borrow_mut(), vec![])
    })
}
