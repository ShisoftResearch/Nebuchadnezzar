use super::leaf_keys::LeafKeys;
use super::reconstruct::ReconstructError;
use super::*;
use crate::client::AsyncClient;
use crate::index::ranged::tree::tree::DeletionSet;
use crate::ram::cell::OwnedCell;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use dovahkiin::types::custom_types::id::Id;
use itertools::Itertools;
use std::marker::PhantomData;
use std::panic;

pub const PAGE_SCHEMA: &'static str = "NEB_BTREE_PAGE";
pub const KEYS_FIELD: &'static str = "keys";
pub const NEXT_FIELD: &'static str = "next";
pub const PREV_FIELD: &'static str = "prev";

pub enum ChangingNode {
    DeletedWithClient(Id, Arc<AsyncClient>),
    Modified(NodeModified),
}

enum NeighborStep {
    Found(Id),
    Follow(NodeCellRef),
    Retry,
}

pub struct NodeModified {
    pub node: NodeCellRef,
    pub deletion: Arc<DeletionSet>,
    pub client: Arc<AsyncClient>,
}

lazy_static! {
    pub static ref KEYS_KEY_HASH: u64 = key_hash(KEYS_FIELD);
    pub static ref NEXT_PAGE_KEY_HASH: u64 = key_hash(NEXT_FIELD);
    pub static ref PREV_PAGE_KEY_HASH: u64 = key_hash(PREV_FIELD);
    pub static ref PAGE_SCHEMA_ID: u32 = key_hash(PAGE_SCHEMA) as u32;
}

fn invalid_page_format(cell: &OwnedCell, reason: impl Into<String>) -> ReconstructError {
    ReconstructError::InvalidPageFormat {
        page_id: cell.id(),
        schema_id: cell.header.schema,
        reason: reason.into(),
    }
}

pub struct ExtNode<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub id: Id,
    // Prefix-compressed page keys; KS only provides the page capacity.
    pub keys: LeafKeys,
    pub next: NodeCellRef,
    pub prev: NodeCellRef,
    pub len: usize,
    pub right_bound: EntryKey,
    pub mark: PhantomData<(KS, PS)>,
}

impl<KS, PS> ExtNode<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn new(id: Id, right_bound: EntryKey) -> Box<Self> {
        Box::new(ExtNode {
            id,
            keys: LeafKeys::new(KS::slice_len()),
            next: Node::<KS, PS>::none_ref(),
            prev: Node::<KS, PS>::none_ref(),
            len: 0,
            right_bound,
            mark: PhantomData,
        })
    }

    pub fn from_cell(cell: &OwnedCell) -> Result<Box<IncubatingExtNode<KS, PS>>, ReconstructError> {
        let cell_id = cell.id();
        let _cell_version = cell.header.version;
        if cell.header.schema != *PAGE_SCHEMA_ID {
            return Err(invalid_page_format(
                cell,
                format!(
                    "expected page schema {}, got {}",
                    *PAGE_SCHEMA_ID, cell.header.schema
                ),
            ));
        }
        let next = cell.data[*NEXT_PAGE_KEY_HASH]
            .id()
            .copied()
            .ok_or_else(|| {
                invalid_page_format(cell, format!("missing or invalid `{}` field", NEXT_FIELD))
            })?;
        let prev = cell.data[*PREV_PAGE_KEY_HASH]
            .id()
            .copied()
            .ok_or_else(|| {
                invalid_page_format(cell, format!("missing or invalid `{}` field", PREV_FIELD))
            })?;
        let keys = &cell.data[*KEYS_KEY_HASH];
        let keys_array = if let OwnedValue::PrimArray(OwnedPrimArray::SmallBytes(ref array)) = keys
        {
            array
        } else {
            return Err(invalid_page_format(
                cell,
                format!("missing or invalid `{}` field", KEYS_FIELD),
            ));
        };
        if keys_array.len() > KS::slice_len() {
            return Err(invalid_page_format(
                cell,
                format!(
                    "page stores {} keys but capacity is {}",
                    keys_array.len(),
                    KS::slice_len()
                ),
            ));
        };
        let cell_keys = keys_array
            .iter()
            .map(|key_val| EntryKey::from_slice(key_val.as_slice()))
            .collect_vec();
        let key_count = cell_keys.len();
        let ext_node = ExtNode {
            id: cell_id,
            keys: LeafKeys::from_keys(&cell_keys, KS::slice_len()),
            next: NodeCellRef::default(), // UNDETERMINED
            prev: NodeCellRef::default(), // UNDETERMINED
            len: key_count,
            right_bound: max_entry_key(), // UNDETERMINED
            mark: PhantomData,
        };
        Ok(Box::new(IncubatingExtNode {
            node: ext_node,
            prev_id: prev,
            next_id: next,
        }))
    }

    // Resolve the id of the nearest live external neighbor, skipping Empty
    // tombstones left behind by merges and migrations (calling extnode() on
    // one of those panics, which used to kill write-back workers).
    //
    // The caller (persist/to_cell) holds this node's write latch. Waiting on
    // a neighbor is only safe rightward — that matches the global latch order
    // (splits and write_targeted hold a node while acquiring its right
    // sibling). The backward walk must use an unvalidated read: a leftward
    // wait here deadlocks against a rightward waiter holding the left
    // neighbor.
    fn live_neighbor_id(start: &NodeCellRef, forward: bool) -> Id {
        let mut node_ref = start.clone();
        let mut retries = 0;
        loop {
            if node_ref.is_default() {
                return Id::unit_id();
            }
            let step = if forward {
                read_node(&node_ref, |node: &NodeReadHandler<KS, PS>| {
                    Self::neighbor_step(&**node, forward)
                })
            } else {
                // Unvalidated walk (must not wait leftward); pin so a
                // condemned tombstone's memory stays readable.
                let _epoch = crossbeam_epoch::pin();
                Self::neighbor_step(&*read_unchecked::<KS, PS>(&node_ref), forward)
            };
            match step {
                NeighborStep::Found(id) => return id,
                NeighborStep::Follow(next) => node_ref = next,
                NeighborStep::Retry => {
                    retries += 1;
                    if retries > 16 {
                        // The persisted link is advisory; reconstruction
                        // re-derives chains. Give up rather than spin.
                        return Id::unit_id();
                    }
                }
            }
        }
    }

    fn neighbor_step(node: &NodeData<KS, PS>, forward: bool) -> NeighborStep {
        match node {
            &NodeData::External(ref n) => NeighborStep::Found(n.id),
            &NodeData::Empty(ref e) => {
                let next = if forward {
                    e.right.try_clone_speculative()
                } else {
                    match e.left.as_ref() {
                        Some(l) => l.try_clone_speculative(),
                        None => Some(NodeCellRef::default()),
                    }
                };
                match next {
                    Some(next) => NeighborStep::Follow(next),
                    None => NeighborStep::Retry,
                }
            }
            &NodeData::None => NeighborStep::Found(Id::unit_id()),
            &NodeData::Internal(_) => {
                warn!("external sibling chain reached an internal node");
                NeighborStep::Found(Id::unit_id())
            }
        }
    }

    pub fn to_cell(&self, deleted: &DeletionSet) -> OwnedCell {
        let mut value = OwnedValue::Map(OwnedMap::new());
        let prev_id = Self::live_neighbor_id(&self.prev, false);
        let next_id = Self::live_neighbor_id(&self.next, true);

        value[*NEXT_PAGE_KEY_HASH] = OwnedValue::Id(next_id);
        value[*PREV_PAGE_KEY_HASH] = OwnedValue::Id(prev_id);
        value[*KEYS_KEY_HASH] = self
            .keys
            .to_vec(0..self.len)
            .into_iter()
            .filter(|key| !deleted.contains(key))
            .map(|key| SmallBytes::from_vec(key.as_slice().to_vec()))
            .collect_vec()
            .value();
        OwnedCell::new_with_id(*PAGE_SCHEMA_ID, &self.id, value)
    }

    pub fn search(&self, key: &EntryKey) -> usize {
        self.keys.search(self.len, key)
    }
    pub fn search_unwindable(
        &self,
        key: &EntryKey,
    ) -> Result<usize, Box<dyn std::any::Any + Send + 'static>> {
        panic::catch_unwind(panic::AssertUnwindSafe(|| self.search(key)))
    }
    pub fn split_insert(
        &mut self,
        key: EntryKey,
        pos: usize,
        self_ref: &NodeCellRef,
        self_next: &mut NodeWriteGuard<KS, PS>,
        tree: &BPlusTree<KS, PS>,
    ) -> (NodeCellRef, EntryKey) {
        // Append-aware split: a key landing past the last slot keeps the
        // left page completely full and opens a fresh right page (sequential
        // writers otherwise leave every page half empty and split twice as
        // often). Mid-page inserts split at the middle as usual.
        let pivot = if pos == self.len {
            self.len
        } else {
            self.len / 2
        };
        let new_page_id = BPlusTree::<KS, PS>::new_page_id();
        let mut keys_2 = self.keys.split_off(pivot, self.len);
        let mut keys_1_len = pivot;
        let mut keys_2_len = self.len - pivot;
        // insert the new key into whichever side owns its position
        if pos < keys_1_len {
            self.keys.insert_at(key, pos, &mut keys_1_len);
        } else {
            keys_2.insert_at(key, pos - keys_1_len, &mut keys_2_len);
        }
        let pivot_key = keys_2.key_at(0);
        let extnode_2: Box<ExtNode<KS, PS>> = Box::new(ExtNode {
            id: new_page_id,
            keys: keys_2,
            next: self.next.clone(),
            prev: self_ref.clone(),
            len: keys_2_len,
            right_bound: self.right_bound.clone(),
            mark: PhantomData,
        });
        debug_assert!(
            pivot_key < self.right_bound,
            "pivot {:?}, right bound {:?}",
            pivot_key,
            self.right_bound
        );
        debug_assert!(pivot_key > Default::default());
        debug_assert!(
            pivot_key > self.keys.key_at(keys_1_len - 1),
            "{:?} / {:?} @ {}",
            pivot_key,
            self.keys.key_at(keys_1_len - 1),
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
        debug_assert_eq!(self.right_bound, extnode_2.keys.key_at(0));
        debug_assert!(
            self.right_bound < extnode_2.right_bound,
            "node right bound >= next right bound {:?} - {:?}",
            self.right_bound,
            extnode_2.right_bound
        );
        debug_assert!(
            self.right_bound <= extnode_2.keys.key_at(0),
            "node right bound < next first key, {:?} - {:?}",
            self.right_bound,
            extnode_2.keys.to_vec(0..extnode_2.len)
        );
        let node_2 = NodeCellRef::new(Node::with_external(extnode_2));
        // Dirty-mark the new page BEFORE any link to it is installed: the
        // write-back flusher pulls dirty forward siblings into the flush
        // stream ahead of their referrers, and that pull keys off this flag.
        // A link installed first would leave a window where the left page
        // serializes naming a page the flusher does not know is pending.
        make_changed(&node_2, tree);
        if !self_next.is_ref_none() {
            let self_next_node = self_next.extnode_mut(tree);
            debug_assert!(
                self_next_node.prev.ptr_eq(&self_ref),
                "node next node's prev node is not self {:?}",
                self_next_node.keys.key_at(0)
            );
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
        if self.len > pos && self.keys.cmp_at(pos, &key) == std::cmp::Ordering::Equal {
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

    // Compacts tombstoned keys: removes them from the page AND drops their
    // tombstones from the deletion set. The caller must hold this page's
    // write latch — the set drop happens before the latch releases, so any
    // validated snapshot orders entirely before (key present + tombstone
    // present) or after (neither) this compaction. Cursors additionally
    // filter tombstoned keys at snapshot time to stay race-free. Protocol
    // model-checked in docs/tla/DeletionReclaim.tla.
    pub fn remove_contains(&mut self, set: &DeletionSet) {
        let all = self.keys.to_vec(0..self.len);
        let (removed, remaining): (Vec<EntryKey>, Vec<EntryKey>) =
            all.into_iter().partition(|k| set.contains(k));
        if removed.is_empty() {
            return;
        }
        self.keys.set_from(&remaining, &mut self.len);
        for key in &removed {
            set.remove(key);
        }
    }

    // Merges `right` into this node's keys, dropping duplicates.
    // Returns the number of duplicate keys that were dropped.
    pub fn merge_sort(&mut self, right: &[&EntryKey]) -> usize {
        trace!("Merge sort have right nodes {:?}", right);
        let self_len_before_merge = self.len;
        debug_assert!(self_len_before_merge + right.len() <= KS::slice_len());
        let left = self.keys.to_vec(0..self.len);
        let mut merged: Vec<EntryKey> = Vec::with_capacity(self.len + right.len());
        let mut left_pos = 0;
        let mut right_pos = 0;
        while left_pos < left.len() && right_pos < right.len() {
            let left_key = &left[left_pos];
            let right_key = right[right_pos];
            let take = if left_key <= right_key {
                left_pos += 1;
                if left_key == right_key {
                    // duplicate across the two runs: consume both, keep one
                    right_pos += 1;
                }
                left_key
            } else {
                right_pos += 1;
                right_key
            };
            if merged.last() != Some(take) {
                merged.push(take.clone());
            }
        }
        for key in &left[left_pos..] {
            if merged.last() != Some(key) {
                merged.push(key.clone());
            }
            left_pos += 1;
        }
        for key in &right[right_pos..] {
            if merged.last() != Some(*key) {
                merged.push((*key).clone());
            }
            right_pos += 1;
        }
        self.keys.set_from(&merged, &mut self.len);
        debug_assert_eq!(self_len_before_merge, left_pos);
        debug_assert_eq!(right.len(), right_pos);

        // Note: self.len can be less than (left + right) when there are duplicates
        // This is expected behavior - duplicates are intentionally removed
        debug_assert!(self.len <= self_len_before_merge + right.len());

        // Log a warning if we detect many duplicates, as this could indicate an issue
        let num_duplicates = (self_len_before_merge + right.len()) - self.len;
        if num_duplicates > 0 {
            trace!(
                "Merge removed {} duplicate keys (left={}, right={}, result={})",
                num_duplicates,
                self_len_before_merge,
                right.len(),
                self.len
            );
            // If ALL keys are duplicates, this is highly suspicious and worth investigating
            if num_duplicates == self_len_before_merge + right.len() {
                warn!(
                    "Merge resulted in NO unique keys! left={}, right={}, all {} keys were duplicates. This may indicate repeated merge of same data.",
                    self_len_before_merge,
                    right.len(),
                    num_duplicates
                );
            }
        }

        num_duplicates
    }

    pub fn dump(&self) {
        trace!("Dumping {:?}, keys {}", self.id, self.len);
        for i in 0..self.len {
            trace!("{}\t- {:?}", i, self.keys.key_at(i));
        }
    }
}

pub fn make_changed<KS, PS>(node: &NodeCellRef, tree: &BPlusTree<KS, PS>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // Queue the changed node on this server's write-back hub. Coalesced:
    // only the false->true transition of the node's dirty flag enqueues;
    // repeated touches of an already-queued page are free. Persist clears
    // the flag under the page latch before snapshotting, so later touches
    // re-queue (docs/tla/DirtyCoalesce.tla).
    let Some(client) = tree.writeback_client() else {
        return;
    };
    if node.is_default() {
        return;
    }
    if node
        .deref::<KS, PS>()
        .dirty
        .swap(true, std::sync::atomic::Ordering::AcqRel)
    {
        return;
    }
    let modified = ChangingNode::Modified(NodeModified {
        node: node.clone(),
        deletion: tree.deletion.clone(),
        client: client.clone(),
    });
    super::storage::hub_for(&client).push(modified);
}

pub fn make_deleted<KS, PS>(id: &Id, tree: &BPlusTree<KS, PS>)
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    // Queue the deleted node on this server's write-back hub
    let Some(client) = tree.writeback_client() else {
        return;
    };
    super::storage::hub_for(&client).push(ChangingNode::DeletedWithClient(*id, client));
}

pub fn page_schema() -> Schema {
    Schema::new_with_id(
        *PAGE_SCHEMA_ID,
        &String::from(PAGE_SCHEMA),
        None,
        Field::new_schema(vec![
            Field::new_unindexed(NEXT_FIELD, Type::Id),
            Field::new_unindexed(PREV_FIELD, Type::Id),
            Field::new_unindexed_array(KEYS_FIELD, Type::SmallBytes),
        ]),
        false,
        false,
    )
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
