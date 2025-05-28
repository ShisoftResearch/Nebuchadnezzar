use super::hash::get_hash_id;
// Import required dependencies
use super::{EntryKey, Feature, IndexerClients};
use crate::client::transaction::TxnError;
use crate::client::AsyncClient;
use crate::dovahkiin::types::Value;
use crate::ram::cell::{OwnedCell, SharedCell};
use crate::ram::types::Id;
use crate::ram::{
    cell::Cell,
    schema::{IndexType, Schema},
};
use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient, rpc::RPCError};
use futures::FutureExt;
use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
};
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::{cell::RefCell, sync::Arc};
use tokio::task::{JoinError, JoinHandle};

// Constant representing an unset/empty feature
const UNSETTLED: Feature = [0u8; 8];

// Define index rules
// Index can be applied on scala value and scala arrays for both Ranged and Hashed
// Only scala can be use vectorization index
// String for ranged will take first 64-bit, hash will hash the string
// Index on nested fields are allowed

// Metadata struct for ranged indices
#[derive(Hash)]
pub struct RangedIndexMeta {
    key: EntryKey,
}

// Metadata struct for hashed indices
#[derive(Hash)]
pub struct HashedIndexMeta {
    hash_id: Id,
    cell_id: Id,
}

// Enum containing all possible index metadata types
#[derive(Hash)]
pub enum IndexMeta {
    Ranged(RangedIndexMeta),
    Hashed(HashedIndexMeta),
}

// Enum for different types of index components
pub enum IndexComps {
    Ranged(Feature),
    Hashed(Feature),
}

#[derive(Debug)]
pub enum IndexError {
    TxnError(TxnError),
    RPCError(RPCError),
}

// Struct holding a collection of index metadata
pub struct IndexRes {
    meta: Vec<IndexMeta>,
}

impl IndexRes {
    // Convert index metadata into hash-metadata pairs
    fn to_meta_hash_pairs(self) -> Vec<(u64, IndexMeta)> {
        self.meta
            .into_iter()
            .map(|meta| {
                let mut hasher = DefaultHasher::default();
                meta.hash(&mut hasher);
                (hasher.finish(), meta)
            })
            .collect()
    }
}

impl IndexMeta {
    // Insert an index into the indexer clients
    async fn insert(&self, indexers: &IndexerClients) -> Result<(), IndexError> {
        match self {
            &IndexMeta::Ranged(ref meta) => {
                indexers.ranged_client.insert(&meta.key).await
                    .map_err(|e| IndexError::RPCError(e))?;
            }
            &IndexMeta::Hashed(ref meta) => {
                indexers.hashed_client.insert(&meta.hash_id, &meta.cell_id).await
                    .map_err(|e| IndexError::TxnError(e))?;
            }
        }
        Ok(())
    }

    // Remove an index from the indexer clients
    async fn remove(&self, indexers: &IndexerClients) -> Result<(), IndexError> {
        match self {
            &IndexMeta::Ranged(ref meta) => {
                indexers.ranged_client.delete(&meta.key).await
                    .map_err(|e| IndexError::RPCError(e))?;
            }
            &IndexMeta::Hashed(ref meta) => {
                let _ = indexers.hashed_client.indexer.remove_index(&meta.cell_id, &meta.hash_id).await
                    .map_err(|e| IndexError::TxnError(e))?;
            }
        }
        Ok(())
    }
}

// Thread local storage for pending index tasks
thread_local! {
    pub static PENDING_INDEX_TASKS: RefCell<Vec<JoinHandle<Result<(), IndexError>>>> = RefCell::new(Vec::new());
}

// Main struct for building and managing indices
pub struct IndexBuilder {
    clients: Arc<IndexerClients>,
}

impl IndexBuilder {
    // Create a new IndexBuilder instance
    pub async fn new(neb_client: &Arc<AsyncClient>, conshash: &Arc<ConsistentHashing>, raft_client: &Arc<RaftClient>) -> Self {
        let _ = IndexerClients::init_index_schema(neb_client).await;
        Self {
            clients: Arc::new(IndexerClients::new(neb_client, conshash, raft_client)),
        }
    }

    // Ensure indices are properly set for a cell
    pub fn ensure_indices(
        &self,
        cell: &OwnedCell,
        schema: &Schema,
        old_indices: Option<Vec<IndexRes>>,
    ) {
        let indexers = self.clients.to_owned();
        // Handle scannable indices if needed
        if schema.is_scannable {
            self.ensure_scannable(cell, &indexers);
        }
        // Get new indices for the cell
        let new_indices = probe_cell_indices(cell, schema);
        if !new_indices.is_empty() {
            let task = tokio::spawn(async move {
                Self::ensure_indices_(new_indices, old_indices, indexers).await
            });
            PENDING_INDEX_TASKS.with(|task_list| {
                task_list.borrow_mut().push(task);
            });
        }
    }

    // Ensure scannable indices are set
    fn ensure_scannable(&self, cell: &OwnedCell, indexers: &Arc<IndexerClients>) {
        let key = EntryKey::for_scannable(&cell.id(), cell.header.schema);
        let indexers = indexers.to_owned();
        let task = tokio::spawn(async move {
            indexers.ranged_client.insert(&key).await
                .map_err(|e| IndexError::RPCError(e))?;
            Ok(())
        });
        PENDING_INDEX_TASKS.with(|task_list| {
            task_list.borrow_mut().push(task);
        });
    }

    // Remove indices for a cell
    pub fn remove_indices(&self, cell: &SharedCell, schema: &Schema) {
        let indexers = self.clients.to_owned();
        if schema.is_scannable {
            self.remove_scannable(cell, &indexers);
        }
        let indices = probe_cell_indices(cell, schema);
        let task = tokio::spawn(async move { Self::remove_indices_(indices, indexers).await });
        PENDING_INDEX_TASKS.with(|task_list| {
            task_list.borrow_mut().push(task);
        });
    }

    // Remove scannable indices
    fn remove_scannable(&self, cell: &SharedCell, indexers: &Arc<IndexerClients>) {
        let key = EntryKey::for_scannable(&cell.id(), cell.header.schema);
        let indexers = indexers.to_owned();
        let task = tokio::spawn(async move {
            indexers.ranged_client.delete(&key).await
                .map_err(|e| IndexError::RPCError(e))?;
            Ok(())
        });
        PENDING_INDEX_TASKS.with(|task_list| {
            task_list.borrow_mut().push(task);
        });
    }

    // Wait for all pending index tasks to complete
    pub fn await_indices<'a>() -> BoxFuture<'a, Vec<Result<Result<(), IndexError>, JoinError>>> {
        PENDING_INDEX_TASKS
            .with(|task_list| {
                let tasks = std::mem::take(&mut *task_list.borrow_mut());
                tasks.into_iter().collect::<FuturesUnordered<_>>()
            })
            .collect::<Vec<_>>()
            .boxed()
    }

    // Helper function to remove indices
    async fn remove_indices_(
        indices: Vec<IndexRes>,
        indexers: Arc<IndexerClients>,
    ) -> Result<(), IndexError> {
        for index in indices.into_iter().flat_map(|res| res.meta) {
            index.remove(&indexers).await?
        }
        Ok(())
    }

    // Helper function to ensure indices are properly set
    async fn ensure_indices_(
        new_indices: Vec<IndexRes>,
        old_indices: Option<Vec<IndexRes>>,
        indexers: Arc<IndexerClients>,
    ) -> Result<(), IndexError> {
        // Convert indices to hash maps for efficient comparison
        let mut index_of_old_index = old_indices
            .unwrap_or_default()
            .into_iter()
            .flat_map(|res| res.to_meta_hash_pairs())
            .collect::<HashMap<_, _>>();
        let mut index_of_new_index = new_indices
            .into_iter()
            .flat_map(|res| res.to_meta_hash_pairs())
            .collect::<HashMap<_, _>>();

        // Remove unchanged indices
        for index in index_of_old_index.keys().cloned().collect::<Vec<_>>() {
            if index_of_new_index.contains_key(&index) {
                index_of_new_index.remove(&index);
                index_of_old_index.remove(&index);
            }
        }

        // Insert new indices and remove old ones
        for new_index in index_of_new_index.values() {
            new_index.insert(&*indexers).await?;
        }
        for old_index in index_of_old_index.values() {
            old_index.remove(&*indexers).await?;
        }
        Ok(())
    }
}

// Function to probe and generate indices for a cell based on its schema
pub fn probe_cell_indices<C: Cell>(cell: &C, schema: &Schema) -> Vec<IndexRes> {
    let mut res = vec![];
    schema.index_fields.iter().for_each(|(field_id, indices)| {
        if let Some(id_path) = schema.id_index.get(field_id) {
            let value = cell.data().get_in_by_ids(id_path);
            let mut components = vec![];

            // Handle array data
            if let Some(array_data_size) = value.prim_array_data_size() {
                for index in indices {
                    match index {
                        // Generate ranged indices for array elements
                        &IndexType::Ranged => components.append(
                            &mut value
                                .features()
                                .into_iter()
                                .map(|vec| IndexComps::Ranged(vec))
                                .collect(),
                        ),
                        // Generate hashed indices for array elements
                        &IndexType::Hashed => components.append(
                            &mut value
                                .hashes()
                                .into_iter()
                                .map(|vec| IndexComps::Hashed(vec))
                                .collect(),
                        ),
                        &IndexType::Statistics => {}
                    }
                }
            } else {
                // Handle scalar data
                for index in indices {
                    match index {
                        &IndexType::Ranged => components.push(IndexComps::Ranged(value.feature())),
                        &IndexType::Hashed => components.push(IndexComps::Hashed(value.hash())),
                        &IndexType::Statistics => {}
                    }
                }
            }

            // Generate index metadata for each component
            let mut metas = vec![];
            let cell_id = cell.id();
            for comp in components {
                match comp {
                    IndexComps::Hashed(feat) => {
                        if feat == UNSETTLED {
                            continue;
                        }
                        let hash_id = get_hash_id(schema.id, *field_id, feat);
                        metas.push(IndexMeta::Hashed(HashedIndexMeta { hash_id, cell_id }));
                    }
                    IndexComps::Ranged(feat) => {
                        if feat == UNSETTLED {
                            continue;
                        }
                        let key = EntryKey::from_props(&cell_id, &feat, *field_id, schema.id);
                        metas.push(IndexMeta::Ranged(RangedIndexMeta { key }));
                    }
                }
            }
            res.push(IndexRes { meta: metas });
        }
    });
    res
}
