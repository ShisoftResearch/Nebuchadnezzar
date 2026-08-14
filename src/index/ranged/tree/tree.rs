// Single B+ tree for range indexing (simplified from LSM-tree)

use super::btree::level::*;
use super::btree::*;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use crate::{client::AsyncClient, ram::cell::OwnedCell};
use lightning::map::HashSet as LFHashSet;
use std::mem;
use std::sync::Arc;

// DeletionSet hides deleted keys immediately and lets page writeback compact them.
pub type DeletionSet = LFHashSet<EntryKey>;

pub const RANGED_TREE_SCHEMA_NAME: &'static str = "NEB_RANGED_TREE";
pub const RANGED_TREE_HEAD_NAME: &'static str = "head";
pub const RANGED_TREE_MIGRATION_NAME: &'static str = "migration";
pub const INITIAL_TREE_EPOCH: u64 = 0;
lazy_static! {
    pub static ref RANGED_TREE_SCHEMA_ID: u32 = key_hash(RANGED_TREE_SCHEMA_NAME) as u32;
    pub static ref RANGED_TREE_HEAD_HASH: u64 = key_hash(RANGED_TREE_HEAD_NAME);
    pub static ref RANGED_TREE_MIGRATION_HASH: u64 = key_hash(RANGED_TREE_MIGRATION_NAME);
    pub static ref RANGED_TREE_SCHEMA: Schema = ranged_tree_schema();
}

// Single disk tree type - 512 keys per node
type DiskTreeKeySlice = [EntryKey; BTREE_NODE_SIZE];
type DiskTreePtrSlice = [NodeCellRef; BTREE_NODE_SIZE + 1];
type DiskTree = BPlusTree<DiskTreeKeySlice, DiskTreePtrSlice>;

/// Single B+ tree for range indexing
///
/// This is a simplified design that directly inserts/queries from a single
/// persistent B+ tree, eliminating the complexity of LSM-tree leveling and merging.
pub struct RangedTree {
    pub tree: DiskTree,
}

/// Why a tree could not be loaded from storage.
///
/// Every variant leaves persistent state exactly as it was. A tree that
/// cannot be read is not the same thing as a tree that is empty, and the
/// difference is only recoverable while the metadata still points at the
/// original chain.
#[derive(Debug)]
pub enum TreeRecoverError {
    /// The metadata cell itself could not be read.
    RootCellUnreadable { tree_id: Id, reason: String },
    /// The metadata cell exists but carries no head pointer.
    RootHeadMissing { tree_id: Id },
    /// The head is known but its page chain could not be read.
    PagesUnreadable {
        tree_id: Id,
        head_id: Id,
        reason: String,
    },
}

impl std::fmt::Display for TreeRecoverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TreeRecoverError::RootCellUnreadable { tree_id, reason } => write!(
                f,
                "ranged tree {:?}: metadata cell unreadable: {}",
                tree_id, reason
            ),
            TreeRecoverError::RootHeadMissing { tree_id } => {
                write!(f, "ranged tree {:?}: metadata cell has no head pointer", tree_id)
            }
            TreeRecoverError::PagesUnreadable {
                tree_id,
                head_id,
                reason,
            } => write!(
                f,
                "ranged tree {:?}: page chain from head {:?} unreadable: {}",
                tree_id, head_id, reason
            ),
        }
    }
}

impl RangedTree {
    /// Create a new ranged tree
    pub async fn create(neb_client: &Arc<AsyncClient>, id: &Id) -> Self {
        let deletion_set = Arc::new(lightning::map::HashSet::with_capacity(0));
        let tree = DiskTree::new_with_client(&deletion_set, neb_client);
        tree.persist_root(neb_client).await;

        let tree_cell = ranged_tree_cell(&tree.head_id(), id, None);
        match neb_client.write_cell(tree_cell).await {
            Ok(Ok(_)) => {
                info!("Created new ranged tree {:?}", id);
                Self { tree }
            }
            Ok(Err(e)) => {
                use crate::ram::cell::WriteError;
                match e {
                    WriteError::CellAlreadyExisted => {
                        info!("Ranged tree already exists for {:?}, recovering", id);
                        match Self::recover(neb_client, id).await {
                            Ok(tree) => tree,
                            Err(error) => {
                                // The tree exists on disk but cannot be read
                                // right now. Returning a fresh in-memory tree
                                // is safe only because this path does not
                                // persist it: the metadata cell still points
                                // at the real chain, so a later load recovers
                                // it once the store can answer.
                                error!(
                                    "Ranged tree {:?} exists but could not be loaded: {}. \
                                     Serving an unpersisted empty tree; the stored chain is \
                                     untouched.",
                                    id, error
                                );
                                let deletion_set =
                                    Arc::new(lightning::map::HashSet::with_capacity(0));
                                Self {
                                    tree: DiskTree::new(&deletion_set),
                                }
                            }
                        }
                    }
                    _ => panic!("Failed to create ranged tree cell: {:?}", e),
                }
            }
            Err(e) => panic!("RPC error creating ranged tree cell: {:?}", e),
        }
    }

    /// Load a ranged tree from persistent storage.
    ///
    /// **Never writes.** A failure here used to replace the tree: a fresh
    /// empty `DiskTree` was persisted and the metadata cell updated to point
    /// at it, which discarded the only reference to the real page chain. That
    /// turned every read failure -- including one caused by a store that had
    /// not finished recovering -- into permanent index loss. On TB14 that cost
    /// 31 of 40 trees, because recovery had aborted and the store was empty
    /// when the trees were loaded.
    ///
    /// So the caller gets an error and the persisted metadata is left alone.
    /// A tree that cannot be read is left absent rather than installed empty,
    /// and the next operation that touches its range retries the load -- which
    /// is what makes a transient failure survivable.
    pub async fn recover(
        neb_client: &Arc<AsyncClient>,
        tree_id: &Id,
    ) -> Result<Self, TreeRecoverError> {
        Self::recover_bounded(neb_client, tree_id, None).await
    }

    /// Like [`Self::recover`], but with the tree's placement upper bound.
    /// A bounded tree may carry a stale chain link into pages a split-off
    /// moved to a sibling; the bound lets reconstruction stop at the
    /// boundary (or truncate a dangling link beyond it) instead of refusing
    /// to load a tree whose own range is fully readable.
    pub async fn recover_bounded(
        neb_client: &Arc<AsyncClient>,
        tree_id: &Id,
        upper_bound: Option<&EntryKey>,
    ) -> Result<Self, TreeRecoverError> {
        info!("[TREE LOAD] Starting load for tree {:?}", tree_id);

        let deletion_set = Arc::new(lightning::map::HashSet::with_capacity(0));

        let cell = match neb_client.read_cell(*tree_id).await {
            Ok(Ok(cell)) => {
                info!("[TREE LOAD] Successfully read tree root cell {:?}", tree_id);
                cell
            }
            Ok(Err(e)) => {
                error!(
                    "[TREE LOAD] Cannot read tree root cell {:?}: {:?}. Leaving the tree \
                     unloaded; its metadata is untouched and the load will be retried.",
                    tree_id, e
                );
                return Err(TreeRecoverError::RootCellUnreadable {
                    tree_id: *tree_id,
                    reason: format!("{:?}", e),
                });
            }
            Err(e) => {
                error!(
                    "[TREE LOAD] RPC error reading tree root cell {:?}: {:?}",
                    tree_id, e
                );
                return Err(TreeRecoverError::RootCellUnreadable {
                    tree_id: *tree_id,
                    reason: format!("rpc: {:?}", e),
                });
            }
        };

        let head_id = match cell.data[*RANGED_TREE_HEAD_HASH].id() {
            Some(id) => *id,
            None => {
                error!(
                    "[TREE LOAD] Tree root cell {:?} carries no head pointer. Leaving the \
                     tree unloaded rather than replacing it.",
                    tree_id
                );
                return Err(TreeRecoverError::RootHeadMissing { tree_id: *tree_id });
            }
        };
        info!("[TREE LOAD] Loading B-tree from head {:?}", head_id);

        let tree = match DiskTree::from_head_id(&head_id, neb_client, &deletion_set, 0, upper_bound)
            .await
        {
            Ok(mut tree) => {
                tree.set_writeback_client(neb_client);
                tree
            }
            Err(e) => {
                error!(
                    "[TREE LOAD] Cannot reconstruct B-tree for {:?} from head {:?}: {:?}. \
                     The metadata still points at this head, so the chain is not lost -- \
                     check whether the store finished recovering.",
                    tree_id, head_id, e
                );
                return Err(TreeRecoverError::PagesUnreadable {
                    tree_id: *tree_id,
                    head_id,
                    reason: format!("{:?}", e),
                });
            }
        };
        info!("[TREE LOAD] B-tree loaded with {} keys", tree.count());

        Ok(Self { tree })
    }

    /// Insert an entry into the tree
    pub fn insert(&self, entry: &EntryKey) -> bool {
        debug!("Inserting entry: {:?}", entry);
        if self.tree.deletion.remove(entry) {
            let cursor = self.tree.seek_raw(entry, Ordering::Forward);
            if cursor.current() == Some(entry) {
                if let Some(page) = cursor.page.as_ref() {
                    self.tree.mark_changed(page);
                }
                self.tree.increment_visible_len();
                return true;
            }
        }
        self.tree.insert(entry)
    }

    /// Delete an entry from the tree
    pub fn delete(&self, entry: &EntryKey) -> bool {
        let cursor = self.tree.seek_raw(entry, Ordering::Forward);
        if cursor.current() != Some(entry) {
            return false;
        }

        if !self.tree.deletion.insert(entry.clone()) {
            return false;
        }

        if let Some(page) = cursor.page.as_ref() {
            self.tree.mark_changed(page);
        }

        self.tree.decrement_visible_len();
        true
    }

    /// Seek to a position in the tree
    pub fn seek(
        &self,
        entry: &EntryKey,
        ordering: Ordering,
    ) -> RTCursor<DiskTreeKeySlice, DiskTreePtrSlice> {
        self.tree.seek(entry, ordering)
    }

    /// Check if tree is oversized and needs splitting
    pub fn oversized(&self) -> bool {
        self.tree.count() > self.ideal_capacity()
    }

    pub fn should_split(&self) -> bool {
        self.oversized()
    }

    /// Get pivot key for tree splitting.
    ///
    /// Descends the tree picking the root's middle separator (O(height)),
    /// which is a balanced approximate median for a B+ tree. The previous
    /// implementation walked a cursor count/2 steps from the start (O(n)),
    /// which made a single migration of a very large tree take minutes and
    /// starved the balancer at billion-key scale.
    pub fn pivot_key(&self) -> Option<EntryKey> {
        if self.count() < 2 {
            return None;
        }
        self.tree.mid_key()
    }

    /// Retain only keys less than pivot (for tree splitting)
    pub fn retain(&self, pivot: &EntryKey) {
        info!("Retaining tree keys up to {:?}", pivot);
        self.tree.retain_by_key(pivot);
        info!("Retain completed");
    }

    /// Mark tree as migrating
    pub async fn mark_migration(
        &self,
        id: &Id,
        migration: Option<Id>,
        client: &Arc<AsyncClient>,
    ) -> Result<(), String> {
        use crate::ram::cell::WriteError;

        let tree_cell = ranged_tree_cell(&self.tree.head_id(), id, migration);
        match client.update_cell(tree_cell.clone()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(WriteError::CellDoesNotExisted)) => {
                warn!(
                    "Ranged tree metadata cell {:?} missing during checkpoint/update; recreating",
                    id
                );
                match client.upsert_cell(tree_cell).await {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(e)) => Err(format!(
                        "Failed to recreate missing tree cell after update miss: {:?}",
                        e
                    )),
                    Err(e) => Err(format!(
                        "RPC error recreating missing tree cell after update miss: {:?}",
                        e
                    )),
                }
            }
            Ok(Err(e)) => Err(format!("Failed to write tree cell: {:?}", e)),
            Err(e) => Err(format!("RPC error updating tree cell: {:?}", e)),
        }
    }

    /// Merge keys from another source (for tree splitting/migration)
    pub fn merge_keys(&self, keys: Vec<EntryKey>) {
        self.tree.merge_with_keys(keys);
    }

    /// Structurally split off keys `>= pivot` into a new tree that shares this
    /// tree's leaf nodes — O(leaves) instead of copying every key. The caller
    /// must hold this tree frozen (migration marker) so no writer races the
    /// split. Returns the new tree and how many keys moved, or None if nothing
    /// moves. The new tree shares this tree's deletion set: the key ranges are
    /// disjoint, so a tombstone only affects the one tree that holds its key.
    pub fn split_off(&self, pivot: &EntryKey, client: &Arc<AsyncClient>) -> Option<(RangedTree, usize)> {
        let so = super::btree::split_off::split_off_spine(&self.tree, pivot)?;
        let mut new_tree = DiskTree::from_root(
            so.new_root,
            so.new_head_id,
            so.moved_len,
            so.new_height,
            &self.tree.deletion,
        );
        new_tree.set_writeback_client(client);
        Some((RangedTree { tree: new_tree }, so.moved_len))
    }

    /// Get ideal capacity for this tree
    pub fn ideal_capacity(&self) -> usize {
        self.tree.ideal_capacity() * 2
    }

    /// Get count of keys in tree
    pub fn count(&self) -> usize {
        self.tree.count()
    }

    /// Get the tree's head ID for persistence
    pub fn head_id(&self) -> Id {
        self.tree.head_id()
    }

    // Legacy methods for compatibility - these are no-ops in the simplified design

    /// No-op: Single tree doesn't need level merging
    pub async fn merge_levels(&self) -> bool {
        // No levels to merge - storage is updated automatically
        storage::wait_until_updated().await;
        false
    }

    /// No-op: Single tree doesn't need forced merging
    pub async fn force_merge_levels(&self) -> bool {
        storage::wait_until_updated().await;
        false
    }

    /// No-op: No separate memory tree
    pub fn mem_tree_count(&self) -> usize {
        0
    }
}

/// Read a tree's metadata cell: (head id, migration marker). None when the
/// cell is missing or unreadable.
pub async fn read_tree_metadata(
    client: &Arc<AsyncClient>,
    tree_id: &Id,
) -> Option<(Id, Option<Id>)> {
    let cell = client.read_cell(*tree_id).await.ok()?.ok()?;
    let head = *cell.data[*RANGED_TREE_HEAD_HASH].id()?;
    let migration = cell.data[*RANGED_TREE_MIGRATION_HASH].id().copied();
    Some((head, migration))
}

/// Clear a tree's durable migration marker without touching its head
/// pointer. Used by split reconciliation at load time, before the tree
/// itself is reconstructed.
pub async fn clear_migration_marker(client: &Arc<AsyncClient>, tree_id: &Id) {
    let Some((head, _)) = read_tree_metadata(client, tree_id).await else {
        return;
    };
    let cell = ranged_tree_cell(&head, tree_id, None);
    if let Err(e) = client.upsert_cell(cell).await {
        warn!(
            "Failed to clear migration marker on tree {:?}: {:?}",
            tree_id, e
        );
    }
}

/// Walk a persisted page chain from `head`, returning the page ids in
/// order. Stops at the first unreadable page (the ids read so far are
/// returned) — reconciliation uses the shape of the walk, not its
/// completeness.
pub async fn walk_chain_page_ids(client: &Arc<AsyncClient>, head: Id) -> Vec<Id> {
    use super::btree::NEXT_PAGE_KEY_HASH;
    let mut ids = Vec::new();
    let mut current = head;
    let mut seen = std::collections::HashSet::new();
    while !current.is_unit_id() && seen.insert(current) {
        let Ok(Ok(cell)) = client.read_cell(current).await else {
            break;
        };
        ids.push(current);
        let Some(next) = cell.data[*NEXT_PAGE_KEY_HASH].id().copied() else {
            break;
        };
        current = next;
    }
    ids
}

/// Point `page`'s persisted next pointer at `next`: the cell-level relink
/// used when rolling back an uncommitted split (the severed chain is
/// rejoined to the orphaned target's head).
pub async fn relink_page_next(client: &Arc<AsyncClient>, page: Id, next: Id) -> Result<(), String> {
    use super::btree::NEXT_PAGE_KEY_HASH;
    let mut cell = client
        .read_cell(page)
        .await
        .map_err(|e| format!("rpc reading page {:?}: {:?}", page, e))?
        .map_err(|e| format!("reading page {:?}: {:?}", page, e))?;
    if let OwnedValue::Map(ref mut map) = cell.data {
        map.insert_key_id(*NEXT_PAGE_KEY_HASH, OwnedValue::Id(next));
    } else {
        return Err(format!("page {:?} does not hold a map body", page));
    }
    match client.upsert_cell(cell).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!("relinking page {:?}: {:?}", page, e)),
        Err(e) => Err(format!("rpc relinking page {:?}: {:?}", page, e)),
    }
}

/// Schema for ranged tree persistence
fn ranged_tree_schema() -> Schema {
    Schema::new_with_id(
        *RANGED_TREE_SCHEMA_ID,
        &String::from(RANGED_TREE_SCHEMA_NAME),
        None,
        Field::new_schema(vec![
            Field::new_unindexed(RANGED_TREE_HEAD_NAME, Type::Id),
            Field::new_unindexed_nullable(RANGED_TREE_MIGRATION_NAME, Type::Id),
        ]),
        false,
        false,
    )
}

/// Create a cell for storing tree metadata
fn ranged_tree_cell(head_id: &Id, id: &Id, migration: Option<Id>) -> OwnedCell {
    let mut cell_map = OwnedMap::new();
    cell_map.insert_key_id(*RANGED_TREE_HEAD_HASH, OwnedValue::Id(*head_id));
    cell_map.insert_key_id(
        *RANGED_TREE_MIGRATION_HASH,
        migration
            .map(|id| OwnedValue::Id(id))
            .unwrap_or(OwnedValue::Null),
    );
    OwnedCell::new_with_id(*RANGED_TREE_SCHEMA_ID, id, OwnedValue::Map(cell_map))
}

// Implement slice operations for the B+ tree node size
impl_btree_level!(BTREE_NODE_SIZE);

unsafe impl Send for RangedTree {}
unsafe impl Sync for RangedTree {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{Feature, FEATURE_SIZE};
    use crate::ram::types::Id;
    use byteorder::{BigEndian, WriteBytesExt};
    use lightning::map::HashSet as LFHashSet;
    use std::sync::Arc;

    fn make_tree() -> RangedTree {
        let ds = Arc::new(LFHashSet::<EntryKey>::with_capacity(0));
        RangedTree {
            tree: DiskTree::new(&ds),
        }
    }

    fn make_feature(n: u64) -> Feature {
        let mut feature: Feature = [0u8; FEATURE_SIZE];
        let mut c = std::io::Cursor::new(&mut feature[..]);
        c.write_u64::<BigEndian>(n).unwrap();
        feature
    }

    fn feature_from_key(key: &EntryKey) -> u64 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&key.as_slice()[8..16]);
        u64::from_be_bytes(bytes)
    }

    fn make_key(n: u64) -> EntryKey {
        let feature = make_feature(n);
        EntryKey::from_props(&Id::from_parts(1, n), &feature, 100, 1)
    }

    fn make_scan_key(schema_id: u32, id: Id) -> EntryKey {
        EntryKey::for_scannable(&id, schema_id)
    }

    fn make_field_key(schema_id: u32, field: u64, n: u64, id: Id) -> EntryKey {
        let feature = make_feature(n);
        EntryKey::from_props(&id, &feature, field, schema_id)
    }

    // ---- pivot_key correctness tests ----------------------------------------

    #[test]
    fn pivot_key_empty_tree_is_none() {
        assert!(make_tree().pivot_key().is_none());
    }

    #[test]
    fn pivot_key_single_key_is_none() {
        let tree = make_tree();
        tree.insert(&make_key(42));
        assert!(tree.pivot_key().is_none());
    }

    #[test]
    fn pivot_key_two_keys_returns_some() {
        let tree = make_tree();
        tree.insert(&make_key(1));
        tree.insert(&make_key(2));
        // Any tree with >=2 keys must produce a pivot; used to always return None (the bug).
        assert!(tree.pivot_key().is_some());
    }

    #[test]
    fn pivot_key_lies_within_key_range() {
        let tree = make_tree();
        let n = 100u64;
        for i in 0..n {
            tree.insert(&make_key(i));
        }
        let pivot = tree
            .pivot_key()
            .expect("pivot_key must return Some for 100 keys");
        assert!(
            pivot >= make_key(0) && pivot <= make_key(n - 1),
            "pivot {:?} must be within the inserted key range [0, {}]",
            pivot,
            n - 1
        );
    }

    #[test]
    fn pivot_key_splits_tree_roughly_in_half() {
        let tree = make_tree();
        let n = 200u64;
        for i in 0..n {
            tree.insert(&make_key(i));
        }

        let pivot = tree
            .pivot_key()
            .expect("pivot_key must return Some for 200 keys");

        // Count keys strictly less than pivot (left side after split).
        let mut cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
        let mut left = 0usize;
        if let Some(k) = cursor.current() {
            if k < &pivot {
                left += 1;
            }
        }
        loop {
            match cursor.next() {
                Some(k) => {
                    if k < pivot {
                        left += 1;
                    }
                }
                None => break,
            }
        }

        let total = n as usize;
        let min_acceptable = total / 4;
        let max_acceptable = 3 * total / 4;
        assert!(
            left >= min_acceptable && left <= max_acceptable,
            "Pivot should produce a balanced split: {} keys left of pivot ({}%), expected 25–75%",
            left,
            left * 100 / total
        );
    }

    #[test]
    fn pivot_key_balanced_on_multi_level_tree() {
        // Enough keys to force several internal levels; the descent-based
        // median must still split within 25-75%.
        let tree = make_tree();
        let n = 50_000u64;
        for i in 0..n {
            tree.insert(&make_key(i));
        }
        let pivot = tree.pivot_key().expect("pivot for 50k keys");
        let mut cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
        let mut left = 0usize;
        if let Some(k) = cursor.current() {
            if k < &pivot {
                left += 1;
            }
        }
        while let Some(k) = cursor.next() {
            if k < pivot {
                left += 1;
            }
        }
        let total = n as usize;
        assert!(
            left >= total / 4 && left <= 3 * total / 4,
            "multi-level pivot unbalanced: {} of {} left ({}%)",
            left,
            total,
            left * 100 / total
        );
    }

    #[test]
    fn retain_handles_leftmost_internal_subtree() {
        let tree = make_tree();
        let n = 1_000u64;
        for i in 0..n {
            tree.insert(&make_key(i));
        }

        // A very small pivot forces retain() down the leftmost internal path,
        // which used to panic with "This case is not possible and not handled".
        let pivot = make_key(1);
        tree.retain(&pivot);

        let mut cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
        let mut retained = Vec::new();
        while let Some(key) = cursor.next() {
            retained.push(key);
        }

        assert_eq!(retained, vec![make_key(0)]);
        assert_eq!(tree.count(), 1);
    }

    #[test]
    fn retain_handles_pivot_equal_to_first_key_in_leaf() {
        let tree = make_tree();
        let n = 1_000u64;
        for i in 0..n {
            tree.insert(&make_key(i));
        }

        // With BTREE_NODE_SIZE=128, key 128 should be the first key in the second
        // leaf for monotonically inserted data. Retaining below this pivot used to
        // panic because that pivot leaf kept zero keys.
        let pivot = make_key(128);
        tree.retain(&pivot);

        let mut cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
        let mut retained = Vec::new();
        while let Some(key) = cursor.next() {
            retained.push(key);
        }

        let expected: Vec<_> = (0..128u64).map(make_key).collect();
        assert_eq!(retained, expected);
        assert_eq!(tree.count(), 128);
    }

    #[test]
    fn seek_iteration_preserves_all_sequential_keys() {
        let tree = make_tree();
        let n = 1_024u64;
        for i in 0..n {
            tree.insert(&make_key(i));
        }

        let mut cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
        let mut seen = Vec::new();

        if let Some(key) = cursor.current() {
            seen.push(key.clone());
        }
        while let Some(key) = cursor.next() {
            if seen.last() != Some(&key) {
                seen.push(key);
            }
        }

        let expected: Vec<_> = (0..n).map(make_key).collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn delete_hides_key_and_insert_restores_it() {
        let tree = make_tree();
        let key_1 = make_key(1);
        let key_2 = make_key(2);
        let key_3 = make_key(3);

        assert!(tree.insert(&key_1));
        assert!(tree.insert(&key_2));
        assert!(tree.insert(&key_3));
        assert_eq!(tree.count(), 3);

        assert!(tree.delete(&key_2));
        assert!(!tree.delete(&key_2));
        assert_eq!(tree.count(), 2);

        let mut cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
        let mut visible = Vec::new();
        if let Some(key) = cursor.current() {
            visible.push(key.clone());
        }
        while let Some(key) = cursor.next() {
            if visible.last() != Some(&key) {
                visible.push(key);
            }
        }
        assert_eq!(visible, vec![key_1.clone(), key_3.clone()]);

        assert!(tree.insert(&key_2));
        assert_eq!(tree.count(), 3);

        let mut cursor = tree.seek(&*MIN_ENTRY_KEY, Ordering::Forward);
        let mut restored = Vec::new();
        if let Some(key) = cursor.current() {
            restored.push(key.clone());
        }
        while let Some(key) = cursor.next() {
            if restored.last() != Some(&key) {
                restored.push(key);
            }
        }
        assert_eq!(restored, vec![key_1, key_2, key_3]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delete_survives_recovery() {
        use crate::client;
        use crate::index::ranged::tree::btree::{page_schema, storage, Ordering};
        use crate::server::{NebServer, ServerOptions, Service};

        let _ = env_logger::try_init();

        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "ranged_tree_delete_recovery";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                server_group,
            )
            .await
            .unwrap(),
        );

        client
            .new_schema_with_id(page_schema())
            .await
            .unwrap()
            .unwrap();
        client
            .new_schema_with_id(RANGED_TREE_SCHEMA.clone())
            .await
            .unwrap()
            .unwrap();

        storage::start_external_nodes_write_back(&client);

        let tree_id = Id::from_parts(901, 901);
        let schema_id = 1;
        let field = 777;
        let tree = RangedTree::create(&client, &tree_id).await;

        for value in 10..=12 {
            assert!(tree.insert(&make_field_key(schema_id, field, value, Id::from_parts(5, value))));
        }
        assert_eq!(tree.count(), 3);

        let deleted = make_field_key(schema_id, field, 11, Id::from_parts(5, 11));
        assert!(tree.delete(&deleted));
        assert_eq!(tree.count(), 2);

        let start_key = EntryKey::for_schema_field_feature(schema_id, field, &make_feature(10));
        let end_key = EntryKey::from_props(
            &Id::from_parts(u64::MAX, u64::MAX),
            &make_feature(12),
            field,
            schema_id,
        );

        let collect_visible = |tree: &RangedTree| {
            let mut cursor = tree.seek(&start_key, Ordering::Forward);
            let mut visible = Vec::new();
            while let Some(key) = cursor.next() {
                if key.prefix_gt(&end_key) {
                    break;
                }
                visible.push(feature_from_key(&key));
            }
            visible
        };

        assert_eq!(collect_visible(&tree), vec![10, 12]);

        storage::wait_until_updated().await;
        drop(tree);

        let recovered = RangedTree::recover(&client, &tree_id).await
            .expect("the tree should load back from storage");
        assert_eq!(recovered.count(), 2);
        assert_eq!(collect_visible(&recovered), vec![10, 12]);

        server.shutdown().await;
    }

    /// A tree whose pages cannot be read must keep pointing at them.
    ///
    /// Recovery used to answer an unreadable page chain by persisting a fresh
    /// empty tree and updating the metadata cell to point at it -- discarding
    /// the only reference to the real chain. Any read failure therefore became
    /// permanent loss, including one caused by a store that had simply not
    /// finished recovering yet. On TB14 that turned an aborted recovery into
    /// 31 destroyed indexes.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unreadable_tree_keeps_its_head_pointer() {
        use crate::client;
        use crate::index::ranged::tree::btree::{page_schema, storage};
        use crate::server::{NebServer, ServerOptions, Service};

        let _ = env_logger::try_init();

        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "ranged_tree_unreadable_head";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                server_group,
            )
            .await
            .unwrap(),
        );
        client
            .new_schema_with_id(page_schema())
            .await
            .unwrap()
            .unwrap();
        client
            .new_schema_with_id(RANGED_TREE_SCHEMA.clone())
            .await
            .unwrap()
            .unwrap();
        storage::start_external_nodes_write_back(&client);

        let tree_id = Id::from_parts(902, 902);
        let tree = RangedTree::create(&client, &tree_id).await;
        for value in 20..=22 {
            assert!(tree.insert(&make_field_key(1, 778, value, Id::from_parts(6, value))));
        }
        storage::wait_until_updated().await;

        let head_before = client
            .read_cell(tree_id)
            .await
            .unwrap()
            .unwrap()
            .data[*RANGED_TREE_HEAD_HASH]
            .id()
            .copied()
            .expect("the tree metadata should carry a head pointer");
        drop(tree);

        // Make the head page unreadable, exactly as an unrecovered store does.
        client.remove_cell(head_before).await.unwrap().unwrap();

        let outcome = RangedTree::recover(&client, &tree_id).await;
        assert!(
            outcome.is_err(),
            "a tree whose pages cannot be read must not load as an empty tree"
        );

        let head_after = client
            .read_cell(tree_id)
            .await
            .unwrap()
            .unwrap()
            .data[*RANGED_TREE_HEAD_HASH]
            .id()
            .copied()
            .expect("the metadata cell must still carry a head pointer");
        assert_eq!(
            head_after, head_before,
            "the failed load rewrote the head pointer, discarding the original chain"
        );

        server.shutdown().await;
    }

    /// A chain that references an unreadable page refuses to load — bounded
    /// or not — while a bounded load stops cleanly at a READABLE page that
    /// starts beyond the tree's upper bound.
    ///
    /// The first half guards against the TB16 lesson in the opposite
    /// direction from the original fix: an earlier revision truncated
    /// bounded trees at a missing page, which silently hid 3M live keys
    /// behind a mid-chain hole. Missing means refuse. The second half is the
    /// sound part of boundary awareness: pages a split moved to a sibling
    /// are readable and provably foreign (their first key sits at or beyond
    /// the bound), so the walk excludes them instead of double-serving.
    #[tokio::test(flavor = "multi_thread")]
    async fn bounded_tree_refuses_missing_page_but_stops_at_boundary() {
        use crate::client;
        use crate::index::ranged::tree::btree::{page_schema, storage, NEXT_PAGE_KEY_HASH};
        use crate::server::{NebServer, ServerOptions, Service};

        let _ = env_logger::try_init();

        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "ranged_tree_bounded_truncate";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                server_group,
            )
            .await
            .unwrap(),
        );
        client
            .new_schema_with_id(page_schema())
            .await
            .unwrap()
            .unwrap();
        client
            .new_schema_with_id(RANGED_TREE_SCHEMA.clone())
            .await
            .unwrap()
            .unwrap();
        storage::start_external_nodes_write_back(&client);

        let tree_id = Id::from_parts(903, 903);
        let schema_id = 1;
        let field = 779;
        let tree = RangedTree::create(&client, &tree_id).await;
        // Three pages at BTREE_NODE_SIZE=128 for monotone inserts.
        let n = 300u64;
        for value in 0..n {
            assert!(tree.insert(&make_field_key(schema_id, field, value, Id::from_parts(7, value))));
        }
        storage::wait_until_updated().await;
        drop(tree);

        // Walk the persisted chain to find the page ids.
        let head_id = client
            .read_cell(tree_id)
            .await
            .unwrap()
            .unwrap()
            .data[*RANGED_TREE_HEAD_HASH]
            .id()
            .copied()
            .expect("the tree metadata should carry a head pointer");
        let mut page_ids = vec![head_id];
        loop {
            let cell = client
                .read_cell(*page_ids.last().unwrap())
                .await
                .unwrap()
                .unwrap();
            let next = cell.data[*NEXT_PAGE_KEY_HASH]
                .id()
                .copied()
                .expect("pages must carry a next pointer");
            if next.is_unit_id() {
                break;
            }
            page_ids.push(next);
        }
        assert!(
            page_ids.len() >= 3,
            "expected at least 3 pages for {} keys, got {}",
            n,
            page_ids.len()
        );

        // Half 2 setup runs FIRST while every page is readable: a bounded
        // load whose upper bound equals the last page's first key must stop
        // before that page — it is provably foreign — and serve the rest.
        let keys_before_last_page = 128 * (page_ids.len() as u64 - 1);
        let upper = make_field_key(
            schema_id,
            field,
            keys_before_last_page,
            Id::from_parts(7, keys_before_last_page),
        );
        let bounded = RangedTree::recover_bounded(&client, &tree_id, Some(&upper))
            .await
            .expect("a bounded tree must stop at a readable foreign page and load");
        assert_eq!(
            bounded.count() as u64,
            keys_before_last_page,
            "the bounded tree must serve exactly the keys below its boundary"
        );
        drop(bounded);

        // Half 1: delete the LAST page's cell. The chain now references an
        // unreadable page, and BOTH load modes must refuse — truncating here
        // would hide the hole.
        let deleted_page = *page_ids.last().unwrap();
        client.remove_cell(deleted_page).await.unwrap().unwrap();

        let outcome = RangedTree::recover(&client, &tree_id).await;
        assert!(
            outcome.is_err(),
            "an unbounded tree with an unreadable page must refuse to load"
        );
        let outcome = RangedTree::recover_bounded(&client, &tree_id, Some(&upper)).await;
        assert!(
            outcome.is_err(),
            "a bounded tree with an unreadable page inside its range must refuse to load"
        );

        server.shutdown().await;
    }

    /// The split-rollback mechanics: a chain severed by an uncommitted
    /// split (durable seam cut, orphaned target head) loads short; after
    /// `relink_page_next` rejoins the severed tail, a fresh load serves
    /// every key again. This is what `reconcile_split_marker` performs when
    /// the placement flip never happened.
    #[tokio::test(flavor = "multi_thread")]
    async fn severed_chain_relinks_and_recovers_fully() {
        use crate::client;
        use crate::index::ranged::tree::btree::{page_schema, storage, NEXT_PAGE_KEY_HASH};
        use crate::server::{NebServer, ServerOptions, Service};

        let _ = env_logger::try_init();

        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "ranged_tree_split_relink";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                disable_storage_locks: true,
                enable_recovery: false,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();
        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                server_group,
            )
            .await
            .unwrap(),
        );
        client
            .new_schema_with_id(page_schema())
            .await
            .unwrap()
            .unwrap();
        client
            .new_schema_with_id(RANGED_TREE_SCHEMA.clone())
            .await
            .unwrap()
            .unwrap();
        storage::start_external_nodes_write_back(&client);

        let tree_id = Id::from_parts(904, 904);
        let schema_id = 1;
        let field = 780;
        let tree = RangedTree::create(&client, &tree_id).await;
        let n = 300u64;
        for value in 0..n {
            assert!(tree.insert(&make_field_key(schema_id, field, value, Id::from_parts(8, value))));
        }
        storage::wait_until_updated().await;
        drop(tree);

        let head_id = client
            .read_cell(tree_id)
            .await
            .unwrap()
            .unwrap()
            .data[*RANGED_TREE_HEAD_HASH]
            .id()
            .copied()
            .unwrap();
        let chain = walk_chain_page_ids(&client, head_id).await;
        assert!(chain.len() >= 3);
        let severed_at = chain[chain.len() - 2];
        let orphan_head = *chain.last().unwrap();

        // Durable seam cut with no committed placement: the tail dangles.
        relink_page_next(&client, severed_at, Id::unit_id())
            .await
            .expect("severing must be expressible as a relink to unit");

        let short = RangedTree::recover(&client, &tree_id)
            .await
            .expect("a cleanly severed chain still loads");
        assert!(
            (short.count() as u64) < n,
            "the severed tree must load short, got {}",
            short.count()
        );
        drop(short);

        // Rollback: rejoin the severed tail, reload, and every key returns.
        relink_page_next(&client, severed_at, orphan_head)
            .await
            .expect("the rollback relink must succeed");
        let whole = RangedTree::recover(&client, &tree_id)
            .await
            .expect("the rejoined chain must load");
        assert_eq!(
            whole.count() as u64,
            n,
            "the rolled-back tree must serve every key"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mark_migration_recreates_missing_tree_cell() {
        use crate::client;
        use crate::index::ranged::tree::btree::{page_schema, storage};
        use crate::server::{NebServer, ServerOptions, Service};

        let _ = env_logger::try_init();

        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "ranged_tree_mark_migration_repair";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                server_group,
            )
            .await
            .unwrap(),
        );

        client
            .new_schema_with_id(page_schema())
            .await
            .unwrap()
            .unwrap();
        client
            .new_schema_with_id(RANGED_TREE_SCHEMA.clone())
            .await
            .unwrap()
            .unwrap();

        storage::start_external_nodes_write_back(&client);

        let tree_id = Id::from_parts(902, 902);
        let tree = RangedTree::create(&client, &tree_id).await;
        let head_id = tree.head_id();

        client.remove_cell(tree_id).await.unwrap().unwrap();
        tree.mark_migration(&tree_id, None, &client)
            .await
            .expect("mark_migration should recreate a missing tree metadata cell");

        let restored = client.read_cell(tree_id).await.unwrap().unwrap();
        assert_eq!(restored.data[*RANGED_TREE_HEAD_HASH].id(), Some(&head_id));

        server.shutdown().await;
    }

    #[test]
    fn scan_prefix_iteration_preserves_all_scannable_keys_with_mixed_entries() {
        let tree = make_tree();
        let schema_id = 123u32;
        let field_id = 100u64;
        let n = 1_024u64;

        for i in 0..n {
            let id = Id::from_parts(1, i);
            assert!(tree.insert(&make_scan_key(schema_id, id)));
            assert!(tree.insert(&make_field_key(schema_id, field_id, i, id)));
        }

        let prefix = EntryKey::for_schema(schema_id).as_slice()[..16].to_vec();
        let mut cursor = tree.seek(&EntryKey::for_schema(schema_id), Ordering::Forward);
        let mut seen = Vec::new();

        if let Some(key) = cursor.current() {
            if key.as_slice()[..16] == prefix {
                seen.push(key.id());
            }
        }
        while let Some(key) = cursor.next() {
            if key.as_slice()[..16] != prefix {
                break;
            }
            if seen.last() != Some(&key.id()) {
                seen.push(key.id());
            }
        }

        let expected: Vec<_> = (0..n).map(|i| Id::from_parts(1, i)).collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn scan_prefix_iteration_preserves_all_scannable_keys_across_two_schemas() {
        let tree = make_tree();
        let schema_1 = 123u32;
        let schema_2 = 234u32;
        let field_id = 100u64;
        let n = 1_024u64;

        for i in 0..n {
            let id = Id::from_parts(1, i);
            assert!(tree.insert(&make_scan_key(schema_1, id)));
            assert!(tree.insert(&make_field_key(schema_1, field_id, i, id)));
        }

        for i in 0..n {
            let id = Id::from_parts(2, i);
            assert!(tree.insert(&make_scan_key(schema_2, id)));
            assert!(tree.insert(&make_field_key(schema_2, field_id, i, id)));
        }

        for (schema_id, higher) in [(schema_1, 1u64), (schema_2, 2u64)] {
            let prefix = EntryKey::for_schema(schema_id).as_slice()[..16].to_vec();
            let mut cursor = tree.seek(&EntryKey::for_schema(schema_id), Ordering::Forward);
            let mut seen = Vec::new();

            if let Some(key) = cursor.current() {
                if key.as_slice()[..16] == prefix {
                    seen.push(key.id());
                }
            }
            while let Some(key) = cursor.next() {
                if key.as_slice()[..16] != prefix {
                    break;
                }
                if seen.last() != Some(&key.id()) {
                    seen.push(key.id());
                }
            }

            let expected: Vec<_> = (0..n).map(|i| Id::from_parts(higher, i)).collect();
            assert_eq!(seen, expected, "schema {}", schema_id);
        }
    }

    // ---- oversized detection ------------------------------------------------

    #[test]
    fn oversized_false_for_empty_tree() {
        assert!(!make_tree().oversized());
    }

    #[test]
    fn oversized_false_below_ideal_capacity() {
        let tree = make_tree();
        // ideal_capacity = BTREE_NODE_SIZE^2 * 2, far more than 10 keys.
        for i in 0..10u64 {
            tree.insert(&make_key(i));
        }
        assert!(!tree.oversized());
    }

    // ---- regression: old code always returned None --------------------------

    /// Verify that the old scale guard (node_len > ideal_capacity/16) would have
    /// rejected every possible node_len, confirming the bug was real.
    #[test]
    fn old_scale_guard_was_always_false() {
        // BTREE_NODE_SIZE is the maximum number of keys in a single leaf node.
        // ideal_capacity() = BTREE_NODE_SIZE^2 * 2, so scale = BTREE_NODE_SIZE^2 / 8.
        // A single leaf holds at most BTREE_NODE_SIZE keys, which is always < scale.
        let max_leaf_keys = BTREE_NODE_SIZE;
        let ideal_cap = BTREE_NODE_SIZE * BTREE_NODE_SIZE * 2; // mirrors RangedTree::ideal_capacity
        let old_scale = ideal_cap / 16;
        assert!(
            max_leaf_keys < old_scale,
            "old scale guard (node_len > {}) can never be true for a leaf with at most {} keys — confirms the bug",
            old_scale,
            max_leaf_keys
        );
    }
}
