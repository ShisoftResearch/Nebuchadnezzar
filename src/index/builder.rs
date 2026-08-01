use super::hash::{get_hash_id, get_null_hash_id};
// Import required dependencies
use super::{EntryKey, Feature, IndexerClients};
use crate::client::AsyncClient;
use crate::dovahkiin::types::Value;
use crate::index::embedding::{EmbeddingIndexConfig, EmbeddingModel};
use crate::index::full_text::{
    build_index_meta as build_inverted_index_meta, FullTextIndexMeta, ToOwnedValue,
};
use crate::index::vector::VectorIndexConfig;
use crate::ram::cell::{OwnedCell, SharedCell, WriteError};
use crate::ram::types::{hash_indexable_owned_value, Id, OwnedValue};
use crate::ram::{
    cell::Cell,
    schema::{CompoundIndex, IndexType, Schema},
};
use bifrost::{conshash::ConsistentHashing, raft::client::AsRaftPlaneClient, rpc::RPCError};
use futures::FutureExt;
use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
};
use lazy_static::lazy_static;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::task::{JoinError, JoinHandle};

use crate::ram::types::OwnedPrimArray;

const COMPOUND_MISSING_PLACEHOLDER: &str = "";

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

fn extract_embedding_text(value: &crate::ram::types::OwnedValue) -> Option<String> {
    match value {
        crate::ram::types::OwnedValue::String(s) => {
            if s.is_empty() {
                None
            } else {
                Some(s.clone())
            }
        }
        crate::ram::types::OwnedValue::PrimArray(OwnedPrimArray::String(items)) => {
            let joined = items.join(" ");
            if joined.is_empty() {
                None
            } else {
                Some(joined)
            }
        }
        crate::ram::types::OwnedValue::Null => None,
        _ => None,
    }
}

fn build_compound_embedding_text<C>(
    cell: &C,
    schema: &Schema,
    compound: &CompoundIndex,
) -> Option<String>
where
    C: Cell,
    <C::Value as Value>::Out: ToOwnedValue,
{
    let delimiter = ". ";
    let mut parts = Vec::with_capacity(compound.field_ids.len());
    for field_id in &compound.field_ids {
        if let Some(id_path) = schema.id_index.get(field_id) {
            let value = cell.data().get_in_by_ids(id_path);
            if let Some(text) = extract_embedding_text(&value.to_owned_value()) {
                parts.push(text);
            } else {
                parts.push(COMPOUND_MISSING_PLACEHOLDER.to_string());
            }
        } else {
            parts.push(COMPOUND_MISSING_PLACEHOLDER.to_string());
        }
    }
    if parts.iter().all(|part| part.is_empty()) {
        return None;
    }
    Some(parts.join(delimiter))
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
pub struct NullIndexMeta {
    hash_id: Id,
    cell_id: Id,
}

#[derive(Hash, Debug)]
pub struct VectorIndexMeta {
    cell_id: Id,
    schema_id: u32,
    field_id: u64,
    config: VectorIndexConfig,
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
    Null(NullIndexMeta),
    Vector(VectorIndexMeta),
    FullText(FullTextIndexMeta),
    Embedding(EmbeddingIndexMeta),
}

// Enum for different types of index components
pub enum IndexComps {
    Ranged(Feature),
    Hashed(Feature),
    Null,
    Vector(Id, u32, u64, VectorIndexConfig),
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
            &IndexMeta::Null(ref meta) => {
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
                        meta.config.metric,
                        meta.config,
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
            &IndexMeta::Null(ref meta) => {
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
// Using Mutex for synchronous access across async boundaries
lazy_static! {
    static ref PENDING_INDEX_TASKS: Arc<Mutex<Vec<JoinHandle<Result<(), IndexError>>>>> =
        Arc::new(Mutex::new(Vec::new()));
}

std::thread_local! {
    static REQUEST_INDEX_TASKS: RefCell<Vec<Vec<JoinHandle<Result<(), IndexError>>>>> =
        RefCell::new(Vec::new());
}

fn new_index_task(task: impl Future<Output = Result<(), IndexError>> + Send + 'static) {
    let mut handle = Some(tokio::spawn(task));
    let stored_locally = REQUEST_INDEX_TASKS.with(|scopes| {
        let mut scopes = scopes.borrow_mut();
        if let Some(tasks) = scopes.last_mut() {
            tasks.push(handle.take().expect("index task handle should exist"));
            true
        } else {
            false
        }
    });
    if !stored_locally {
        PENDING_INDEX_TASKS
            .lock()
            .push(handle.expect("index task handle should exist"));
    }
}

// Main struct for building and managing indices
pub struct IndexBuilder {
    pub clients: Arc<IndexerClients>,
}

impl IndexBuilder {
    // Create a new IndexBuilder instance
    pub async fn new<C>(
        neb_client: &Arc<AsyncClient>,
        conshash: &Arc<ConsistentHashing>,
        raft_client: &Arc<C>,
        server_id: u64,
    ) -> Self
    where
        C: AsRaftPlaneClient + 'static,
    {
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

    pub async fn graceful_shutdown(&self) -> Result<(), IndexError> {
        if let Some(indexer) = self.clients.fulltext_indexer() {
            indexer.graceful_shutdown().await?;
        }

        Ok(())
    }

    // Ensure indices are properly set for a cell
    pub fn ensure_indices(
        &self,
        cell: &OwnedCell,
        schema: &Schema,
        old_indices: Option<Vec<IndexRes>>,
    ) {
        log::debug!(
            "ensure_indices: cell_id={:?}, schema_id={}, schema_name={}, is_scannable={}",
            cell.id(),
            schema.id,
            schema.name,
            schema.is_scannable
        );
        let indexers = self.clients.clone();
        let scannable_key = if schema.is_scannable {
            log::debug!(
                "Schema {} is scannable, scheduling scannable insert for cell {:?}",
                schema.id,
                cell.id()
            );
            Some(EntryKey::for_scannable(&cell.id(), cell.header.schema))
        } else {
            log::debug!(
                "Schema {} is NOT scannable for cell {:?}",
                schema.id,
                cell.id()
            );
            None
        };
        // Get new indices for the cell
        let new_indices = probe_cell_indices(cell, schema);
        if scannable_key.is_some() || !new_indices.is_empty() {
            debug!("New indices: {:?}", new_indices);
            let cell_id = cell.id();
            let schema_id = cell.header.schema;
            new_index_task(async move {
                if let Some(key) = scannable_key {
                    Self::ensure_scannable_insert(indexers.clone(), key, cell_id, schema_id)
                        .await?;
                }
                let res = Self::ensure_indices_(new_indices, old_indices, indexers).await;
                debug!("Ensure indices result: {:?}", res);
                res
            });
        }
    }

    async fn ensure_scannable_insert(
        indexers: Arc<IndexerClients>,
        key: EntryKey,
        cell_id: Id,
        schema_id: u32,
    ) -> Result<(), IndexError> {
        log::debug!(
            "ensure_scannable: Inserting key for cell_id={:?}, schema_id={}",
            cell_id,
            schema_id
        );
        let pattern = Some(key.as_slice()[..16].to_vec());
        // A failed insert permanently loses the index entry, so transient
        // RPC failures (e.g. tree service still electing a leader during
        // cluster bring-up) must be retried, not dropped.
        let mut inserted = false;
        let mut insert_result = None;
        for attempt in 0..30 {
            match indexers.ranged_client.insert(&key).await {
                Ok(value) => {
                    inserted = value;
                    insert_result = Some(Ok(()));
                    break;
                }
                Err(e) => {
                    log::warn!(
                        "ensure_scannable: insert failed for cell_id={:?} (attempt {}): {:?}",
                        cell_id,
                        attempt + 1,
                        e
                    );
                    insert_result = Some(Err(IndexError::RPCError(e)));
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        }
        if let Some(Err(e)) = insert_result {
            log::error!("ensure_scannable: Failed to insert key: {:?}", e);
            return Err(e);
        }

        for attempt in 0..32 {
            let range = crate::index::ranged::tree::service::Range::new_inclusive_opened(
                key.clone(),
                crate::index::ranged::tree::btree::Ordering::Forward,
            );
            match crate::index::ranged::client::RangedIndexerClient::seek(
                &indexers.ranged_client,
                range,
                1,
                pattern.clone(),
            )
            .await
            {
                Ok(Some(cursor)) if cursor.current_block().first() == Some(&cell_id) => {
                    return Ok(());
                }
                Ok(Some(cursor)) => {
                    log::warn!(
                        "ensure_scannable: inserted key not yet visible for cell_id={:?}, schema_id={}, attempt={}, inserted={}, first_seen={:?}",
                        cell_id,
                        schema_id,
                        attempt + 1,
                        inserted,
                        cursor.current_block().first()
                    );
                }
                Ok(None) => {
                    log::warn!(
                        "ensure_scannable: inserted key not yet queryable for cell_id={:?}, schema_id={}, attempt={}, inserted={}",
                        cell_id,
                        schema_id,
                        attempt + 1,
                        inserted
                    );
                }
                Err(e) => {
                    log::warn!(
                        "ensure_scannable: visibility check failed for cell_id={:?}, schema_id={}, attempt={}, inserted={}: {:?}",
                        cell_id,
                        schema_id,
                        attempt + 1,
                        inserted,
                        e
                    );
                }
            }

            let delay_ms = 10 + ((attempt / 4) as u64 * 10).min(70);
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        Err(IndexError::Other(format!(
            "ensure_scannable insert never became visible for cell_id={:?}, schema_id={}, inserted={}",
            cell_id, schema_id, inserted
        )))
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
                let mut guard = PENDING_INDEX_TASKS.lock();
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

    // Scope index tasks spawned by a single request so callers can wait only on
    // the work that their own write generated instead of draining the global backlog.
    pub fn with_request_index_scope<'a, R, F>(
        op: F,
    ) -> BoxFuture<'a, (R, Vec<Result<Result<(), IndexError>, JoinError>>)>
    where
        R: Send + 'a,
        F: FnOnce() -> R + 'a,
    {
        let (res, tasks) = REQUEST_INDEX_TASKS.with(|scopes| {
            scopes.borrow_mut().push(Vec::new());
            let res = op();
            let tasks = scopes
                .borrow_mut()
                .pop()
                .expect("request index scope stack should not be empty");
            (res, tasks)
        });

        async move {
            let results = tasks
                .into_iter()
                .collect::<FuturesUnordered<JoinHandle<Result<(), IndexError>>>>()
                .collect::<Vec<Result<Result<(), IndexError>, JoinError>>>()
                .await;
            (res, results)
        }
        .boxed()
    }

    // Wait for ALL pending index tasks globally (for shutdown)
    // This ensures all index tasks from all threads are completed before shutdown
    pub async fn await_all_indices() -> Vec<Result<Result<(), IndexError>, JoinError>> {
        let tasks: Vec<JoinHandle<Result<(), IndexError>>> = {
            let mut guard = PENDING_INDEX_TASKS.lock();
            std::mem::take(&mut *guard)
        };

        let count = tasks.len();
        if count > 0 {
            log::info!(
                "Waiting for {} pending index tasks to complete before shutdown",
                count
            );
        }

        let results: Vec<Result<Result<(), IndexError>, JoinError>> =
            futures::future::join_all(tasks).await;

        let success_count = results.iter().filter(|r| matches!(r, Ok(Ok(_)))).count();
        let error_count = results.len() - success_count;
        if error_count > 0 {
            log::warn!(
                "Index tasks completed: {} succeeded, {} failed",
                success_count,
                error_count
            );
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
            let owned_value = value.to_owned_value();
            let mut components = vec![];
            let mut metas = vec![];

            // Handle array data
            if matches!(owned_value, OwnedValue::Array(_) | OwnedValue::PrimArray(_)) {
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
                        &IndexType::Null => {}
                        // For vector, only provide its property
                        &IndexType::Vector(config) => components.push(IndexComps::Vector(
                            cell.id(),
                            schema.id,
                            *field_id,
                            config,
                        )),
                        &IndexType::Fulltext => {
                            if let Some(meta) = build_inverted_index_meta(
                                cell.id(),
                                cell.header().version,
                                schema.id,
                                *field_id,
                                owned_value.clone(),
                            ) {
                                metas.push(IndexMeta::FullText(meta));
                            }
                        }
                        &IndexType::Embedding(ref config) => {
                            if let Some(meta) = build_embedding_index_meta(
                                cell.id(),
                                schema.id,
                                *field_id,
                                config.model.clone(),
                                owned_value.clone(),
                            ) {
                                metas.push(IndexMeta::Embedding(meta));
                            }
                        }
                        &IndexType::Statistics => {}
                    }
                }
            } else {
                // Handle scalar data
                let null_scalar = matches!(
                    owned_value,
                    crate::ram::types::OwnedValue::Null | crate::ram::types::OwnedValue::NA
                );
                for index in indices {
                    match index {
                        &IndexType::Ranged => {
                            if !null_scalar {
                                components.push(IndexComps::Ranged(value.feature()))
                            }
                        }
                        &IndexType::Hashed => {
                            if let Some(feature) = hash_indexable_owned_value(&owned_value) {
                                components.push(IndexComps::Hashed(feature))
                            }
                        }
                        &IndexType::Null => {
                            if null_scalar {
                                components.push(IndexComps::Null)
                            }
                        }
                        &IndexType::Vector(_config) => {}
                        &IndexType::Fulltext => {
                            if let Some(meta) = build_inverted_index_meta(
                                cell.id(),
                                cell.header().version,
                                schema.id,
                                *field_id,
                                owned_value.clone(),
                            ) {
                                metas.push(IndexMeta::FullText(meta));
                            }
                        }
                        &IndexType::Embedding(ref config) => {
                            if let Some(meta) = build_embedding_index_meta(
                                cell.id(),
                                schema.id,
                                *field_id,
                                config.model.clone(),
                                owned_value.clone(),
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
                        let hash_id = get_hash_id(schema.id, *field_id, feat);
                        metas.push(IndexMeta::Hashed(HashedIndexMeta { hash_id, cell_id }));
                    }
                    IndexComps::Null => {
                        let hash_id = get_null_hash_id(schema.id, *field_id);
                        metas.push(IndexMeta::Null(NullIndexMeta { hash_id, cell_id }));
                    }
                    IndexComps::Ranged(feat) => {
                        let key = EntryKey::from_props(&cell_id, &feat, *field_id, schema.id);
                        metas.push(IndexMeta::Ranged(RangedIndexMeta { key }));
                    }
                    IndexComps::Vector(cell_id, schema_id, field_id, config) => {
                        metas.push(IndexMeta::Vector(VectorIndexMeta {
                            cell_id,
                            schema_id,
                            field_id,
                            config,
                        }));
                    }
                }
            }
            res.push(IndexRes { meta: metas });
        }
    });

    schema
        .compound_index_fields
        .iter()
        .for_each(|(compound_id, compound)| {
            let mut metas = vec![];
            for index in &compound.indices {
                match index {
                    IndexType::Embedding(config) => {
                        if let Some(text) = build_compound_embedding_text(cell, schema, compound) {
                            metas.push(IndexMeta::Embedding(EmbeddingIndexMeta {
                                cell_id: cell.id(),
                                schema_id: schema.id,
                                field_id: *compound_id,
                                model: config.model.clone(),
                                text,
                            }));
                        }
                    }
                    _ => {}
                }
            }
            if !metas.is_empty() {
                res.push(IndexRes { meta: metas });
            }
        });
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::schema::{Field, IndexType, Schema};
    use crate::ram::types::{Id, Map, OwnedMap, OwnedPrimArray, OwnedValue};
    use bifrost_hasher::hash_str;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::time::{Duration, Instant};

    fn collect_embedding_metas(indices: Vec<IndexRes>) -> Vec<EmbeddingIndexMeta> {
        let mut embedding_metas = Vec::new();
        for res in indices {
            for meta in res.meta {
                if let IndexMeta::Embedding(meta) = meta {
                    embedding_metas.push(meta);
                }
            }
        }
        embedding_metas
    }

    #[test]
    fn compound_embedding_concat_comma() {
        let fields = Field::new_schema(vec![
            Field::new_unindexed("title", dovahkiin::types::Type::String),
            Field::new_unindexed("body", dovahkiin::types::Type::String),
        ]);
        let mut schema = Schema::new("compound_embedding", None, fields, false, false);
        schema.add_compound_index(
            "title_body",
            vec!["title".to_string(), "body".to_string()],
            vec![IndexType::Embedding(EmbeddingIndexConfig::for_model(
                EmbeddingModel::from("test-model"),
            ))],
        );

        let mut data = OwnedMap::new();
        data.insert("title", OwnedValue::String("hello".to_string()));
        data.insert("body", OwnedValue::String("world".to_string()));
        let cell = crate::ram::cell::OwnedCell::new_with_id(
            schema.id,
            &Id::from_parts(1, 1),
            OwnedValue::Map(data),
        );

        let embedding_metas = collect_embedding_metas(probe_cell_indices(&cell, &schema));

        assert_eq!(embedding_metas.len(), 1);
        let meta = &embedding_metas[0];
        assert_eq!(meta.field_id, hash_str("title_body"));
        assert_eq!(meta.text, "hello. world");
        assert_eq!(meta.model, EmbeddingModel::from("test-model"));
    }

    #[test]
    fn compound_embedding_concat_with_array() {
        let fields = Field::new_schema(vec![
            Field::new_unindexed_array("title", dovahkiin::types::Type::String),
            Field::new_unindexed("body", dovahkiin::types::Type::String),
        ]);
        let mut schema = Schema::new("compound_embedding_array", None, fields, false, false);
        schema.add_compound_index(
            "title_body",
            vec!["title".to_string(), "body".to_string()],
            vec![IndexType::Embedding(EmbeddingIndexConfig::for_model(
                EmbeddingModel::from("test-model"),
            ))],
        );

        let mut data = OwnedMap::new();
        data.insert(
            "title",
            OwnedValue::PrimArray(OwnedPrimArray::String(vec![
                "hello".to_string(),
                "there".to_string(),
            ])),
        );
        data.insert("body", OwnedValue::String("world".to_string()));
        let cell = crate::ram::cell::OwnedCell::new_with_id(
            schema.id,
            &Id::from_parts(1, 2),
            OwnedValue::Map(data),
        );

        let embedding_metas = collect_embedding_metas(probe_cell_indices(&cell, &schema));

        assert_eq!(embedding_metas.len(), 1);
        let meta = &embedding_metas[0];
        assert_eq!(meta.field_id, hash_str("title_body"));
        assert_eq!(meta.text, "hello there. world");
        assert_eq!(meta.model, EmbeddingModel::from("test-model"));
    }

    #[tokio::test]
    async fn request_index_scope_does_not_wait_for_unrelated_global_tasks() {
        let _ = IndexBuilder::await_indices().await;

        new_index_task(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            Ok(())
        });

        let started = Instant::now();
        let (value, local_results) = IndexBuilder::with_request_index_scope(|| {
            new_index_task(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            });
            42usize
        })
        .await;

        assert_eq!(value, 42);
        assert_eq!(
            local_results.len(),
            1,
            "request scope should await only the local index task"
        );
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "request scope should not block on unrelated global index work"
        );

        let global_results =
            tokio::time::timeout(Duration::from_secs(1), IndexBuilder::await_indices())
                .await
                .expect("global index backlog should drain");
        assert_eq!(
            global_results.len(),
            1,
            "the unrelated global task should remain pending until explicitly drained"
        );
    }

    #[tokio::test]
    async fn global_backlog_can_be_drained_before_task_completion() {
        let _ = IndexBuilder::await_indices().await;

        let completed = Arc::new(AtomicBool::new(false));
        let completed_flag = completed.clone();
        new_index_task(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            completed_flag.store(true, Ordering::SeqCst);
            Ok(())
        });

        let drained_tasks = {
            let mut guard = PENDING_INDEX_TASKS.lock();
            std::mem::take(&mut *guard)
        };
        assert_eq!(
            drained_tasks.len(),
            1,
            "the global queue should expose the in-flight task handle"
        );

        let later_waiter_results = IndexBuilder::await_indices().await;
        assert!(
            later_waiter_results.is_empty(),
            "a later waiter sees an empty backlog once another waiter drained the handles"
        );
        assert!(
            !completed.load(Ordering::SeqCst),
            "draining task handles is not the same as the index task finishing"
        );

        let drained_results = futures::future::join_all(drained_tasks).await;
        assert_eq!(drained_results.len(), 1);
        assert!(
            completed.load(Ordering::SeqCst),
            "the original drained task should still complete successfully"
        );
    }
}
