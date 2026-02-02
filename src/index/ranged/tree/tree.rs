// Single B+ tree for range indexing (simplified from LSM-tree)

use super::btree::level::*;
use super::btree::*;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use crate::{client::AsyncClient, ram::cell::OwnedCell};
use lightning::map::HashSet as LFHashSet;
use std::mem;
use std::sync::Arc;

// DeletionSet type - kept for btree module compatibility
// In single-tree design, we use an empty set (no tombstone tracking needed)
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

// Single disk tree type - using LEVEL_1 size (512 keys per node)
type DiskTreeKeySlice = [EntryKey; LEVEL_1];
type DiskTreePtrSlice = [NodeCellRef; LEVEL_1 + 1];
type DiskTree = BPlusTree<DiskTreeKeySlice, DiskTreePtrSlice>;

/// Single B+ tree for range indexing
///
/// This is a simplified design that directly inserts/queries from a single
/// persistent B+ tree, eliminating the complexity of LSM-tree leveling and merging.
pub struct RangedTree {
    pub tree: DiskTree,
}

impl RangedTree {
    /// Create a new ranged tree
    pub async fn create(neb_client: &Arc<AsyncClient>, id: &Id) -> Self {
        // Create a single disk tree (no deletion set needed for direct operations)
        let deletion_set = Arc::new(lightning::map::HashSet::with_capacity(0));
        let tree = DiskTree::new(&deletion_set);
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
                        Self::recover(neb_client, id).await
                    }
                    _ => panic!("Failed to create ranged tree cell: {:?}", e),
                }
            }
            Err(e) => panic!("RPC error creating ranged tree cell: {:?}", e),
        }
    }

    /// Recover a ranged tree from persistent storage
    pub async fn recover(neb_client: &Arc<AsyncClient>, tree_id: &Id) -> Self {
        info!("[TREE RECOVERY] Starting recovery for tree {:?}", tree_id);

        let deletion_set = Arc::new(lightning::map::HashSet::with_capacity(0));

        let cell = match neb_client.read_cell(*tree_id).await {
            Ok(Ok(cell)) => {
                info!(
                    "[TREE RECOVERY] Successfully read tree root cell {:?}",
                    tree_id
                );
                cell
            }
            Ok(Err(e)) => {
                error!(
                    "[TREE RECOVERY] Failed to read tree root cell {:?}: {:?}",
                    tree_id, e
                );
                panic!("Tree root cell not found");
            }
            Err(e) => {
                error!(
                    "[TREE RECOVERY] RPC error reading tree root cell {:?}: {:?}",
                    tree_id, e
                );
                panic!("RPC error reading tree root cell");
            }
        };

        let head_id = cell.data[*RANGED_TREE_HEAD_HASH].id().unwrap();
        info!("[TREE RECOVERY] Recovering B-tree from head {:?}", head_id);

        let tree = DiskTree::from_head_id(&head_id, neb_client, &deletion_set, 0).await;
        info!(
            "[TREE RECOVERY] B-tree recovered with {} keys",
            tree.count()
        );

        Self { tree }
    }

    /// Insert an entry into the tree
    pub fn insert(&self, entry: &EntryKey) -> bool {
        debug!("Inserting entry: {:?}", entry);
        self.tree.insert(entry)
    }

    /// Delete an entry from the tree
    ///
    /// Note: Currently uses seek to verify existence.
    /// TODO: Implement true B+ tree deletion for better performance.
    pub fn delete(&self, entry: &EntryKey) -> bool {
        if let Some(k) = self.seek(entry, Ordering::Forward).current() {
            if k == entry {
                // TODO: Implement actual B+ tree node deletion
                // For now, we mark as deleted in deletion set (temporary until delete is implemented)
                warn!(
                    "delete() not yet fully implemented - key {} will still exist",
                    entry.len()
                );
                return true;
            }
        }
        false
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

    /// Get pivot key for tree splitting
    pub fn pivot_key(&self) -> Option<EntryKey> {
        let root = self.tree.get_root();
        let scale = self.ideal_capacity() / 16;

        if let Some((node_len, _, mid_key)) = self.tree.last_node_digest(&root) {
            if node_len > scale {
                return Some(mid_key);
            }
        }
        None
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
        let tree_cell = ranged_tree_cell(&self.tree.head_id(), id, migration);
        match client.update_cell(tree_cell).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(format!("Failed to write tree cell: {:?}", e)),
            Err(e) => Err(format!("RPC error updating tree cell: {:?}", e)),
        }
    }

    /// Merge keys from another source (for tree splitting/migration)
    pub fn merge_keys(&self, keys: Vec<EntryKey>) {
        self.tree.merge_with_keys(keys);
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

// Keep LEVEL_1 tree type for the disk tree
impl_btree_level!(LEVEL_1);

unsafe impl Send for RangedTree {}
unsafe impl Sync for RangedTree {}
