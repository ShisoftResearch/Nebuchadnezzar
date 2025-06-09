use super::hash::get_hash_id;
// Import required dependencies
use super::{EntryKey, Feature, IndexerClients};
use crate::client::transaction::TxnError;
use crate::client::AsyncClient;
use crate::dovahkiin::types::Value;
use crate::index::vector::MetricEncoding;
use crate::ram::cell::{OwnedCell, SharedCell};
use crate::ram::types::Id;
use crate::ram::{
    cell::Cell,
    schema::{IndexType, Schema},
};
use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient, rpc::RPCError};
use dovahkiin::types::OwnedValue;
use futures::FutureExt;
use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
};
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::future::Future;
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
#[derive(Hash, Debug)]
pub struct RangedIndexMeta {
    key: EntryKey,
}

// Metadata struct for hashed indices
#[derive(Hash, Debug)]
pub struct HashedIndexMeta {
    hash_id: Id,
    cell_id: Id,
}

#[derive(Hash, Debug)]
pub struct VectorIndexMeta {
    cell_id: Id,
    schema_id: u32,
    field_id: u64,
    metric_encoding: MetricEncoding,
}

// Enum containing all possible index metadata types
#[derive(Hash, Debug)]
pub enum IndexMeta {
    Ranged(RangedIndexMeta),
    Hashed(HashedIndexMeta),
    Vector(VectorIndexMeta),
}

// Enum for different types of index components
pub enum IndexComps {
    Ranged(Feature),
    Hashed(Feature),
    Vector(Id, u32, u64, MetricEncoding)
}

#[derive(Debug)]
pub enum IndexError {
    TxnError(TxnError),
    RPCError(RPCError),
}

// Struct holding a collection of index metadata
#[derive(Debug)]
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
            &IndexMeta::Vector(ref meta) => {
                indexers.vector_client.insert(&meta.cell_id, meta.schema_id, meta.field_id, meta.metric_encoding).await?;
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
            &IndexMeta::Vector(ref meta) => {
                indexers.vector_client.remove(&meta.cell_id, meta.schema_id, meta.field_id, meta.metric_encoding).await?;
            }
        }
        Ok(())
    }
}

// Thread local storage for pending index tasks
thread_local! {
    pub static PENDING_INDEX_TASKS: RefCell<Vec<JoinHandle<Result<(), IndexError>>>> = RefCell::new(Vec::new());
}

fn new_index_task(task: impl Future<Output = Result<(), IndexError>> + Send + 'static)  {
    let tokio_task = tokio::spawn(task);
    PENDING_INDEX_TASKS.with(|task_list| {
        task_list.borrow_mut().push(tokio_task);
    });
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
            debug!("New indices: {:?}", new_indices);
            new_index_task(async move {
                let res = Self::ensure_indices_(new_indices, old_indices, indexers).await;
                debug!("Ensure indices result: {:?}", res);
                res
            });
        }
    }

    // Ensure scannable indices are set
    fn ensure_scannable(&self, cell: &OwnedCell, indexers: &Arc<IndexerClients>) {
        let key = EntryKey::for_scannable(&cell.id(), cell.header.schema);
        let indexers = indexers.to_owned();
        new_index_task(async move {
            indexers.ranged_client.insert(&key).await
                .map_err(|e| IndexError::RPCError(e))?;
            Ok(())
        });
    }

    // Remove indices for a cell
    pub fn remove_indices(&self, cell: &SharedCell, schema: &Schema) {
        let indexers = self.clients.to_owned();
        if schema.is_scannable {
            self.remove_scannable(cell, &indexers);
        }
        let indices = probe_cell_indices(cell, schema);
        new_index_task(async move { Self::remove_indices_(indices, indexers).await });
    }

    // Remove scannable indices
    fn remove_scannable(&self, cell: &SharedCell, indexers: &Arc<IndexerClients>) {
        let key = EntryKey::for_scannable(&cell.id(), cell.header.schema);
        let indexers = indexers.to_owned();
        new_index_task(async move {
            indexers.ranged_client.delete(&key).await
                .map_err(|e| IndexError::RPCError(e))?;
            Ok(())
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
        debug!("Inserting new indices: {:?}", index_of_new_index);
        for new_index in index_of_new_index.values() {
            debug!("Inserting new index: {:?}", new_index);
            new_index.insert(&*indexers).await?;
        }
        debug!("Removing old indices: {:?}", index_of_old_index);
        for old_index in index_of_old_index.values() {
            debug!("Removing old index: {:?}", old_index);
            old_index.remove(&*indexers).await?;
        }
        debug!("Indices updated: {:?}", index_of_new_index);
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
            if value.is_prime_array() { // Index each element of the array
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
                        // For vector, only provide its property
                        &IndexType::Vector(metric_encoding) => components
                            .push(IndexComps::Vector(cell.id(), schema.id, *field_id, metric_encoding)),
                        &IndexType::Statistics => {}
                    }
                }
            } else {
                // Handle scalar data
                for index in indices {
                    match index {
                        &IndexType::Ranged => components.push(IndexComps::Ranged(value.feature())),
                            &IndexType::Hashed => components.push(IndexComps::Hashed(value.hash())),
                            &IndexType::Vector(_metric_encoding) => {}
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
                    IndexComps::Vector(cell_id, schema_id, field_id, metric_encoding) => {
                        metas.push(IndexMeta::Vector(VectorIndexMeta { cell_id, schema_id, field_id, metric_encoding }));
                    }
                }
            }
            res.push(IndexRes { meta: metas });
        }
    });
    res
}
