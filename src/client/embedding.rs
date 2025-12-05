//! Embedding search client for semantic similarity queries
//!
//! This module provides `EmbeddingClient` for performing semantic search
//! using text embeddings. The actual embedding generation and search is
//! delegated to an external implementation (e.g., Morpheus).

use std::sync::Arc;

use bifrost::rpc::RPCError;

use crate::index::embedding::{EmbeddingHit, EmbeddingIndexClient, EmbeddingModelInfo};

/// Client for semantic embedding search operations
///
/// Provides text-based semantic similarity search. Unlike full-text search
/// which uses lexical matching (BM25), embedding search uses dense vector
/// representations for semantic understanding.
///
/// # Example
/// ```ignore
/// // Get embedding client from AsyncClient
/// let emb = client.embedding();
///
/// // List available models
/// let models = emb.list_models().await?;
/// for model in models {
///     println!("{}: {} ({}d)", model.name, model.description, model.dimensions);
/// }
///
/// // Search by schema and field IDs
/// let hits = emb.search(schema_id, field_id, "machine learning concepts", 10).await?;
///
/// // Process results
/// for hit in hits {
///     println!("Doc {:?} similarity: {:.3}", hit.id, hit.score);
///     let doc = client.read_cell(hit.id).await?;
/// }
/// ```
pub struct EmbeddingClient {
    embedding_client: Arc<EmbeddingIndexClient>,
}

impl EmbeddingClient {
    /// Create a new embedding client
    pub fn new(embedding_client: Arc<EmbeddingIndexClient>) -> Self {
        Self { embedding_client }
    }

    /// List available embedding models
    ///
    /// Returns information about all models supported by the embedding implementation.
    /// Use these model names when creating schemas with embedding indices.
    pub async fn list_models(&self) -> Result<Vec<EmbeddingModelInfo>, RPCError> {
        match self.embedding_client.list_models().await {
            Ok(models) => Ok(models),
            Err(e) => Err(RPCError::IOError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("List models error: {:?}", e),
            ))),
        }
    }

    /// Search for semantically similar documents
    ///
    /// Performs a semantic similarity search using text embeddings.
    /// The query text is embedded and compared against indexed documents.
    ///
    /// # Arguments
    /// * `schema_id` - Schema ID containing the indexed field
    /// * `field_id` - Field ID to search (must have Embedding index)
    /// * `query` - Search query text (will be embedded)
    /// * `limit` - Maximum results to return
    ///
    /// # Returns
    /// Vector of `EmbeddingHit` with document IDs and similarity scores
    pub async fn search(
        &self,
        schema_id: u32,
        field_id: u64,
        query: &str,
        limit: usize,
    ) -> Result<Vec<EmbeddingHit>, RPCError> {
        match self
            .embedding_client
            .search(schema_id, field_id, query, limit)
            .await
        {
            Ok(hits) => Ok(hits),
            Err(e) => Err(RPCError::IOError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Embedding search error: {:?}", e),
            ))),
        }
    }

    /// Search using field name (convenience method)
    ///
    /// Converts field name to field ID using hash.
    pub async fn search_field(
        &self,
        schema_id: u32,
        field_name: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<EmbeddingHit>, RPCError> {
        let field_id = bifrost_hasher::hash_str(field_name) as u64;
        self.search(schema_id, field_id, query, limit).await
    }

    /// Check if the embedding index core is available
    ///
    /// Returns true if an embedding implementation (e.g., Morpheus) has been
    /// registered with the system.
    pub fn is_available(&self) -> bool {
        self.embedding_client.is_embedding_index_core_set()
    }
}

/// Search result with document ID and similarity score
///
/// Re-exported from `crate::index::embedding::EmbeddingHit` for convenience.
pub use crate::index::embedding::EmbeddingHit as SemanticHit;

/// Information about an available embedding model
///
/// Re-exported from `crate::index::embedding::EmbeddingModelInfo` for convenience.
pub use crate::index::embedding::EmbeddingModelInfo as ModelInfo;

