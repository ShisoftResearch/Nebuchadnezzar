use super::hash::get_hash_id;
// Import required dependencies
use super::{EntryKey, Feature, IndexerClients};
use crate::client::AsyncClient;
use crate::dovahkiin::types::Value;
use crate::index::embedding::EmbeddingModel;
use crate::index::full_text::{
    build_index_meta as build_inverted_index_meta, FullTextIndexMeta, ToOwnedValue,
};
use crate::index::vector::MetricEncoding;
use crate::ram::cell::{OwnedCell, SharedCell, WriteError};
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
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use tokio::task::{JoinError, JoinHandle};
use lazy_static::lazy_static;

use crate::ram::types::OwnedPrimArray;

// Constant representing an unset/empty feature
const UNSETTLED: Feature = [0u8; 8];

/// Build embedding index metadata from a cell value.
/// Extracts text content from String or String array values.
fn build_embedding_index_meta(
    cell_id: Id,
    schema_id: u32,
    field_id: u64,
    model: EmbeddingModel,
    value: crate::ram::types::OwnedValue,
) -> Option<EmbeddingIndexMeta> {
    let text = match value {
        crate::ram::types::OwnedValue::String(s) => {
            if s.is_empty() {
                return None;
            }
            s
        }
        crate::ram::types::OwnedValue::PrimArray(OwnedPrimArray::String(items)) => {
            // Concatenate all strings with space separator for embedding
            let joined = items.join(" ");
            if joined.is_empty() {
                return None;
            }
            joined
        }
        crate::ram::types::OwnedValue::Null => return None,
        _ => return None, // Only text types are supported for embedding
    };

    Some(EmbeddingIndexMeta {
        cell_id,
        schema_id,
        field_id,
        model,
        text,
    })
}

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

/// Metadata for embedding index operations.
/// Contains the text content to be embedded by the external indexer.
#[derive(Debug)]
pub struct EmbeddingIndexMeta {
    pub cell_id: Id,
    pub schema_id: u32,
    pub field_id: u64,
    pub model: EmbeddingModel,
    pub text: String,
}

// Implement Hash manually for EmbeddingIndexMeta to ensure consistent hashing
impl Hash for EmbeddingIndexMeta {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cell_id.hash(state);
        self.schema_id.hash(state);
        self.field_id.hash(state);
        self.model.hash(state);
        self.text.hash(state);
    }
}

// Enum containing all possible index metadata types
#[derive(Hash, Debug)]
pub enum IndexMeta {
    Ranged(RangedIndexMeta),
    Hashed(HashedIndexMeta),
    Vector(VectorIndexMeta),
    FullText(FullTextIndexMeta),
    Embedding(EmbeddingIndexMeta),
}

// Enum for different types of index components
pub enum IndexComps {
    Ranged(Feature),
    Hashed(Feature),
    Vector(Id, u32, u64, MetricEncoding),
}

#[derive(Debug)]
pub enum IndexError {
    WriteError(WriteError),
    RPCError(RPCError),
    Other(String),
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
                indexers
                    .ranged_client
                    .insert(&meta.key)
                    .await
                    .map_err(|e| IndexError::RPCError(e))?;
            }
            &IndexMeta::Hashed(ref meta) => {
                indexers
                    .hashed_client
                    .insert(&meta.hash_id, &meta.cell_id)
                    .await
                    .map_err(|e| IndexError::WriteError(e))?;
            }
            &IndexMeta::Vector(ref meta) => {
                indexers
                    .vector_client
                    .insert(
                        &meta.cell_id,
                        meta.schema_id,
                        meta.field_id,
                        meta.metric_encoding,
                    )
                    .await?;
            }
            &IndexMeta::FullText(ref meta) => {
                if let Some(indexer) = indexers.fulltext_indexer() {
                    // Write posting lists to Chunk (synchronous)
                    indexer.add_document(meta)?;
                    // Update stats cache (lock-free, sync)
                    indexer.update_stats_for_add(meta);
                } else {
                    return Err(IndexError::Other(
                        "Inverted indexer not available".to_string(),
                    ));
                }
            }
            &IndexMeta::Embedding(ref meta) => {
                indexers
                    .embedding_client
                    .insert(
                        &meta.cell_id,
                        meta.schema_id,
                        meta.field_id,
                        &meta.model,
                        &meta.text,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    // Remove an index from the indexer clients
    async fn remove(&self, indexers: &IndexerClients) -> Result<(), IndexError> {
        match self {
            &IndexMeta::Ranged(ref meta) => {
                indexers
                    .ranged_client
                    .delete(&meta.key)
                    .await
                    .map_err(|e| IndexError::RPCError(e))?;
            }
            &IndexMeta::Hashed(ref meta) => {
                indexers
                    .hashed_client
                    .indexer
                    .remove_index(&meta.cell_id, &meta.hash_id)
                    .await
                    .map_err(|e| IndexError::WriteError(e))?;
            }
            &IndexMeta::Vector(ref meta) => {
                indexers
                    .vector_client
                    .remove(&meta.cell_id, meta.schema_id, meta.field_id)
                    .await?;
            }
            &IndexMeta::FullText(ref meta) => {
                if let Some(indexer) = indexers.fulltext_indexer() {
                    indexer.remove_document(meta)?;
                } else {
                    return Err(IndexError::Other(
                        "Inverted indexer not available".to_string(),
                    ));
                }
            }
            &IndexMeta::Embedding(ref meta) => {
                indexers
                    .embedding_client
                    .remove(&meta.cell_id, meta.schema_id, meta.field_id)
                    .await?;
            }
        }
        Ok(())
    }
}

// Global storage for pending index tasks
// Using std::sync::Mutex for immediate synchronous access
lazy_static! {
    static ref PENDING_INDEX_TASKS: Arc<Mutex<Vec<JoinHandle<Result<(), IndexError>>>>> =
        Arc::new(Mutex::new(Vec::new()));
}

fn new_index_task(task: impl Future<Output = Result<(), IndexError>> + Send + 'static) {
    let handle = tokio::spawn(task);
    PENDING_INDEX_TASKS.lock().unwrap().push(handle);
}

// Main struct for building and managing indices
pub struct IndexBuilder {
    pub clients: Arc<IndexerClients>,
}

impl IndexBuilder {
    // Create a new IndexBuilder instance
    pub async fn new(
        neb_client: &Arc<AsyncClient>,
        conshash: &Arc<ConsistentHashing>,
        raft_client: &Arc<RaftClient>,
        server_id: u64,
    ) -> Self {
        let _ = IndexerClients::init_index_schema(neb_client).await;
        Self {
            clients: Arc::new(IndexerClients::new(
                neb_client,
                conshash,
                raft_client,
                server_id,
            )),
        }
    }

    /// Initialize the inverted indexer with chunks (called after chunks creation)
    pub fn initialize_inverted_indexer(&self, chunks: &Arc<crate::ram::chunk::Chunks>) {
        self.clients.initialize_inverted_indexer(chunks);
    }

    // Ensure indices are properly set for a cell
    pub fn ensure_indices(
        &self,
        cell: &OwnedCell,
        schema: &Schema,
        old_indices: Option<Vec<IndexRes>>,
    ) {
        log::debug!("ensure_indices: cell_id={:?}, schema_id={}, schema_name={}, is_scannable={}",
            cell.id(), schema.id, schema.name, schema.is_scannable);
        let indexers = self.clients.clone();
        // Handle scannable indices if needed
        if schema.is_scannable {
            log::debug!("Schema {} is scannable, calling ensure_scannable for cell {:?}", schema.id, cell.id());
            self.ensure_scannable(cell, &indexers);
        } else {
            log::debug!("Schema {} is NOT scannable for cell {:?}", schema.id, cell.id());
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
        let cell_id = cell.id();
        let schema_id = cell.header.schema;
        let indexers = indexers.clone();
        new_index_task(async move {
            log::debug!("ensure_scannable: Inserting key for cell_id={:?}, schema_id={}", cell_id, schema_id);
            indexers
                .ranged_client
                .insert(&key)
                .await
                .map(|_| ())
                .map_err(|e| {
                    log::error!("ensure_scannable: Failed to insert key: {:?}", e);
                    IndexError::RPCError(e)
                })
        });
    }

    // Remove indices for a cell
    pub fn remove_indices(&self, cell: &SharedCell, schema: &Schema) {
        let indexers = self.clients.clone();
        if schema.is_scannable {
            self.remove_scannable(cell, &indexers);
        }
        let indices = probe_cell_indices(cell, schema);
        new_index_task(async move { Self::remove_indices_(indices, indexers).await });
    }

    // Remove scannable indices
    fn remove_scannable(&self, cell: &SharedCell, indexers: &Arc<IndexerClients>) {
        let key = EntryKey::for_scannable(&cell.id(), cell.header.schema);
        let indexers = indexers.clone();
        new_index_task(async move {
            indexers
                .ranged_client
                .delete(&key)
                .await
                .map_err(|e| IndexError::RPCError(e))?;
            Ok(())
        });
    }

    // Wait for all pending index tasks to complete (legacy method for per-RPC call)
    // This is called by with_indices_ensured() after each RPC to ensure indices are updated
    pub fn await_indices<'a>() -> BoxFuture<'a, Vec<Result<Result<(), IndexError>, JoinError>>> {
        async move {
            let tasks: Vec<JoinHandle<Result<(), IndexError>>> = {
                let mut guard = PENDING_INDEX_TASKS.lock().unwrap();
                std::mem::take(&mut *guard)
            };
            tasks
                .into_iter()
                .collect::<FuturesUnordered<JoinHandle<Result<(), IndexError>>>>()
                .collect::<Vec<Result<Result<(), IndexError>, JoinError>>>()
                .await
        }
        .boxed()
    }

    // Wait for ALL pending index tasks globally (for shutdown)
    // This ensures all index tasks from all threads are completed before shutdown
    pub async fn await_all_indices() -> Vec<Result<Result<(), IndexError>, JoinError>> {
        let tasks: Vec<JoinHandle<Result<(), IndexError>>> = {
            let mut guard = PENDING_INDEX_TASKS.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        
        let count = tasks.len();
        if count > 0 {
            log::info!("Waiting for {} pending index tasks to complete before shutdown", count);
        }

        let results: Vec<Result<Result<(), IndexError>, JoinError>> = futures::future::join_all(tasks).await;

        let success_count = results.iter().filter(|r| matches!(r, Ok(Ok(_)))).count();
        let error_count = results.len() - success_count;
        if error_count > 0 {
            log::warn!("Index tasks completed: {} succeeded, {} failed", success_count, error_count);
        } else if count > 0 {
            log::info!("All {} index tasks completed successfully", success_count);
        }
        
        results
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
        let new_values = index_of_new_index.into_values().collect::<Vec<_>>();
        let old_values = index_of_old_index.into_values().collect::<Vec<_>>();

        let (inverted_new, regular_new): (Vec<_>, Vec<_>) = new_values
            .into_iter()
            .partition(|meta| matches!(meta, IndexMeta::FullText(_)));
        let (inverted_old, regular_old): (Vec<_>, Vec<_>) = old_values
            .into_iter()
            .partition(|meta| matches!(meta, IndexMeta::FullText(_)));

        for new_index in regular_new {
            debug!("Inserting new index: {:?}", new_index);
            new_index.insert(&*indexers).await?;
        }
        debug!("Removing old indices: {:?}", regular_old);
        for old_index in regular_old {
            debug!("Removing old index: {:?}", old_index);
            old_index.remove(&*indexers).await?;
        }
        debug!("Removing old inverted indices: {:?}", inverted_old);
        for old_index in inverted_old {
            debug!("Removing inverted index: {:?}", old_index);
            old_index.remove(&*indexers).await?;
        }
        debug!("Inserting new inverted indices: {:?}", inverted_new);
        for new_index in inverted_new {
            debug!("Inserting inverted index: {:?}", new_index);
            new_index.insert(&*indexers).await?;
        }
        Ok(())
    }
}

// Function to probe and generate indices for a cell based on its schema
pub fn probe_cell_indices<C>(cell: &C, schema: &Schema) -> Vec<IndexRes>
where
    C: Cell,
    <C::Value as Value>::Out: ToOwnedValue,
{
    let mut res = vec![];
    schema.index_fields.iter().for_each(|(field_id, indices)| {
        if let Some(id_path) = schema.id_index.get(field_id) {
            let value = cell.data().get_in_by_ids(id_path);
            let mut components = vec![];
            let mut metas = vec![];

            // Handle array data
            if value.is_prime_array() {
                // Index each element of the array
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
                        &IndexType::Vector(metric_encoding) => components.push(IndexComps::Vector(
                            cell.id(),
                            schema.id,
                            *field_id,
                            metric_encoding,
                        )),
                        &IndexType::Fulltext => {
                            let owned_value = value.to_owned_value();
                            if let Some(meta) = build_inverted_index_meta(
                                cell.id(),
                                cell.header().version,
                                schema.id,
                                *field_id,
                                owned_value,
                            ) {
                                metas.push(IndexMeta::FullText(meta));
                            }
                        }
                        &IndexType::Embedding(ref model) => {
                            let owned_value = value.to_owned_value();
                            if let Some(meta) = build_embedding_index_meta(
                                cell.id(),
                                schema.id,
                                *field_id,
                                model.clone(),
                                owned_value,
                            ) {
                                metas.push(IndexMeta::Embedding(meta));
                            }
                        }
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
                        &IndexType::Fulltext => {
                            let owned_value = value.to_owned_value();
                            if let Some(meta) = build_inverted_index_meta(
                                cell.id(),
                                cell.header().version,
                                schema.id,
                                *field_id,
                                owned_value,
                            ) {
                                metas.push(IndexMeta::FullText(meta));
                            }
                        }
                        &IndexType::Embedding(ref model) => {
                            let owned_value = value.to_owned_value();
                            if let Some(meta) = build_embedding_index_meta(
                                cell.id(),
                                schema.id,
                                *field_id,
                                model.clone(),
                                owned_value,
                            ) {
                                metas.push(IndexMeta::Embedding(meta));
                            }
                        }
                        &IndexType::Statistics => {}
                    }
                }
            }

            // Generate index metadata for each component
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
                        metas.push(IndexMeta::Vector(VectorIndexMeta {
                            cell_id,
                            schema_id,
                            field_id,
                            metric_encoding,
                        }));
                    }
                }
            }
            res.push(IndexRes { meta: metas });
        }
    });
    res
}
