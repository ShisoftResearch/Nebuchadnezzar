// Nebuchadnezzar does not provide an embedding indexer itself, it requires an external implementation.
// This module provides a trait and a client that can be used to integrate an embedding indexer.
//
// Similar to the vector index, the default implementation is expected to be Morpheus,
// which will set the embedding core to handle text embedding and semantic search.
//
// Unlike full-text search (BM25) which uses lexical matching, embedding search uses
// dense vector representations of text for semantic similarity search.
//
// The embedding model is configurable via a string identifier. Morpheus decides which
// models it supports and how to interpret the model name. This allows flexibility for
// different embedding models (e.g., "text-embedding-ada-002", "all-MiniLM-L6-v2", etc.)

use std::sync::Arc;
use std::sync::OnceLock;

use dovahkiin::types::Id;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::index::builder::IndexError;

pub const NO_EMBEDDING_CORE_ERROR: &str =
    "Embedding indexer core is not set. Should call `set_embedding_index_core` to set it.";

/// Embedding model identifier.
///
/// This is an opaque string that Morpheus interprets. Nebuchadnezzar does not
/// validate or understand model names - it simply passes them to the embedding
/// implementation.
///
/// # Examples
/// Common model names (defined by Morpheus):
/// - `"default"` - Use the default model configured in Morpheus
/// - `"text-embedding-ada-002"` - OpenAI's embedding model
/// - `"all-MiniLM-L6-v2"` - Sentence Transformers model
/// - Custom model names as supported by your Morpheus configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EmbeddingModel(pub String);

impl EmbeddingModel {
    /// Create a new embedding model identifier.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Use the default model configured in the embedding implementation.
    pub fn default_model() -> Self {
        Self("default".to_string())
    }

    /// Get the model name.
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl Default for EmbeddingModel {
    fn default() -> Self {
        Self::default_model()
    }
}

impl From<&str> for EmbeddingModel {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for EmbeddingModel {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Information about an available embedding model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingModelInfo {
    /// Model identifier (used in EmbeddingModel)
    pub name: String,
    /// Human-readable description
    pub description: String,
    /// Vector dimension produced by this model
    pub dimensions: usize,
    /// Maximum input tokens/characters (if applicable)
    pub max_input_length: Option<usize>,
}

/// Result of an embedding similarity search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingHit {
    /// The document/cell ID
    pub id: Id,
    /// Similarity score (higher is more similar, typically 0.0 to 1.0 for cosine similarity)
    pub score: f32,
}

/// Core trait for embedding index implementations.
///
/// Implementations should handle:
/// - Text embedding generation (via external model)
/// - Vector storage and indexing (e.g., HNSW, IVF)
/// - Approximate nearest neighbor search
/// - Model management and selection
///
/// The `text` parameter in insert operations is the raw text content to be embedded.
/// The implementation is responsible for:
/// 1. Generating embeddings from text using the specified model
/// 2. Storing the embeddings with appropriate indexing structures
/// 3. Performing similarity search when queried
pub trait EmbeddingIndexerCore: Send + Sync {
    /// List available embedding models.
    ///
    /// Returns information about all models supported by this implementation.
    /// Users can use these model names when creating embedding indices.
    fn list_models(&self) -> BoxFuture<'_, Result<Vec<EmbeddingModelInfo>, IndexError>>;

    /// Insert a text document into the embedding index.
    ///
    /// # Arguments
    /// * `cell_id` - The ID of the cell/document
    /// * `schema_id` - Schema ID for namespace isolation
    /// * `field_id` - Field ID being indexed
    /// * `model` - The embedding model to use
    /// * `text` - The text content to embed and index
    fn insert(
        &self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
        model: &EmbeddingModel,
        text: &str,
    ) -> BoxFuture<'_, Result<(), IndexError>>;

    /// Remove a document from the embedding index.
    ///
    /// # Arguments
    /// * `cell_id` - The ID of the cell/document to remove
    /// * `schema_id` - Schema ID for namespace isolation
    /// * `field_id` - Field ID being indexed
    fn remove(
        &self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
    ) -> BoxFuture<'_, Result<(), IndexError>>;

    /// Search for similar documents using a text query.
    ///
    /// The implementation should:
    /// 1. Embed the query text using the model configured for this index
    /// 2. Perform approximate nearest neighbor search
    /// 3. Return the top-k most similar documents
    ///
    /// # Arguments
    /// * `schema_id` - Schema ID for namespace isolation
    /// * `field_id` - Field ID to search within
    /// * `query` - The query text to embed and search
    /// * `limit` - Maximum number of results to return
    fn search(
        &self,
        schema_id: u32,
        field_id: u64,
        query: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<EmbeddingHit>, IndexError>>;

    /// Create a new embedding index for a schema/field combination.
    ///
    /// Called when a new schema with embedding index is created.
    ///
    /// # Arguments
    /// * `schema_id` - Schema ID for namespace isolation
    /// * `field_id` - Field ID to index
    /// * `model` - The embedding model to use for this index
    fn new_index(
        &self,
        schema_id: u32,
        field_id: u64,
        model: &EmbeddingModel,
    ) -> BoxFuture<'_, Result<(), IndexError>>;

    /// Delete an embedding index for a schema/field combination.
    ///
    /// Called when a schema with embedding index is deleted.
    fn delete_index(&self, schema_id: u32, field_id: u64) -> BoxFuture<'_, Result<(), IndexError>>;
}

/// Client for embedding index operations.
///
/// This client wraps an `EmbeddingIndexerCore` implementation and provides
/// a convenient interface for embedding index operations.
///
/// # Example
/// ```ignore
/// // In Morpheus or another implementation:
/// let client = EmbeddingIndexClient::new();
/// client.set_embedding_index_core(MyEmbeddingCore::new());
///
/// // List available models
/// let models = client.list_models().await?;
/// for model in models {
///     println!("{}: {} ({}d)", model.name, model.description, model.dimensions);
/// }
///
/// // Then use through IndexerClients for insert/remove during transactions
/// // Or directly for search operations
/// ```
pub struct EmbeddingIndexClient {
    embedding_core: OnceLock<Arc<dyn EmbeddingIndexerCore>>,
}

impl EmbeddingIndexClient {
    pub fn new() -> Self {
        Self {
            embedding_core: OnceLock::new(),
        }
    }

    /// Set the embedding index core implementation.
    ///
    /// This should be called once during initialization, typically by Morpheus.
    /// Returns true if the core was set successfully, false if already set.
    pub fn set_embedding_index_core<C: EmbeddingIndexerCore + 'static>(&self, core: C) -> bool {
        let res = self.embedding_core.set(Arc::new(core));
        return res.is_ok();
    }

    /// Get the embedding index core.
    ///
    /// # Panics
    /// Panics if the core has not been set via `set_embedding_index_core`.
    pub fn get_embedding_index_core(&self) -> &Arc<dyn EmbeddingIndexerCore> {
        self.embedding_core.get().expect(NO_EMBEDDING_CORE_ERROR)
    }

    /// Check if the embedding index core has been set.
    pub fn is_embedding_index_core_set(&self) -> bool {
        self.embedding_core.get().is_some()
    }

    /// List available embedding models.
    ///
    /// Returns information about all models supported by the embedding implementation.
    pub fn list_models(&self) -> BoxFuture<'_, Result<Vec<EmbeddingModelInfo>, IndexError>> {
        self.get_embedding_index_core().list_models()
    }

    /// Insert a text document into the embedding index.
    pub fn insert<'a>(
        &'a self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
        model: &'a EmbeddingModel,
        text: &'a str,
    ) -> BoxFuture<'a, Result<(), IndexError>> {
        self.get_embedding_index_core()
            .insert(cell_id, schema_id, field_id, model, text)
    }

    /// Remove a document from the embedding index.
    pub fn remove<'a>(
        &'a self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
    ) -> BoxFuture<'a, Result<(), IndexError>> {
        self.get_embedding_index_core()
            .remove(cell_id, schema_id, field_id)
    }

    /// Search for similar documents.
    pub fn search<'a>(
        &'a self,
        schema_id: u32,
        field_id: u64,
        query: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<EmbeddingHit>, IndexError>> {
        self.get_embedding_index_core()
            .search(schema_id, field_id, query, limit)
    }

    /// Create a new embedding index with the specified model.
    pub fn new_index<'a>(
        &'a self,
        schema_id: u32,
        field_id: u64,
        model: &'a EmbeddingModel,
    ) -> BoxFuture<'a, Result<(), IndexError>> {
        self.get_embedding_index_core()
            .new_index(schema_id, field_id, model)
    }

    /// Delete an embedding index.
    pub fn delete_index(
        &self,
        schema_id: u32,
        field_id: u64,
    ) -> BoxFuture<'_, Result<(), IndexError>> {
        self.get_embedding_index_core()
            .delete_index(schema_id, field_id)
    }
}

impl Default for EmbeddingIndexClient {
    fn default() -> Self {
        Self::new()
    }
}

