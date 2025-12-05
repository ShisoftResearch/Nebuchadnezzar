//! Vector search client for similarity queries
//!
//! This module provides `VectorClient` for performing vector similarity search.
//! The actual vector storage and search is delegated to an external implementation
//! (e.g., Morpheus).

use std::sync::Arc;

use bifrost::rpc::RPCError;

use crate::index::vector::{VectorHit, VectorIndexClient};

/// Client for vector similarity search operations
///
/// Provides vector-based similarity search using approximate nearest neighbor
/// algorithms. The query is a raw vector, and results are ranked by distance
/// or similarity depending on the metric used.
///
/// # Example
/// ```ignore
/// // Get vector client from the index client
/// let vec_client = VectorClient::new(vector_index_client);
///
/// // Search by schema and field IDs with a query vector
/// let query = vec![0.1, 0.2, 0.3, 0.4]; // Your query vector
/// let hits = vec_client.search(schema_id, field_id, &query, 10).await?;
///
/// // Process results
/// for hit in hits {
///     println!("Doc {:?} distance/similarity: {:.3}", hit.id, hit.score);
///     let doc = client.read_cell(hit.id).await?;
/// }
/// ```
pub struct VectorClient {
    vector_client: Arc<VectorIndexClient>,
}

impl VectorClient {
    /// Create a new vector client
    pub fn new(vector_client: Arc<VectorIndexClient>) -> Self {
        Self { vector_client }
    }

    /// Search for similar vectors
    ///
    /// Performs approximate nearest neighbor search using the configured
    /// distance metric (L2, Cosine, Manhattan, or Chebyshev).
    ///
    /// # Arguments
    /// * `schema_id` - Schema ID containing the indexed field
    /// * `field_id` - Field ID to search (must have Vector index)
    /// * `query_vector` - The query vector to find neighbors for
    /// * `limit` - Maximum results to return
    ///
    /// # Returns
    /// Vector of `VectorHit` with document IDs and distance/similarity scores.
    /// Score interpretation depends on the metric:
    /// - L2/Manhattan/Chebyshev: lower is more similar (distance)
    /// - Cosine: higher is more similar (similarity)
    pub async fn search(
        &self,
        schema_id: u32,
        field_id: u64,
        query_vector: &[f32],
        limit: usize,
    ) -> Result<Vec<VectorHit>, RPCError> {
        match self
            .vector_client
            .search(schema_id, field_id, query_vector, limit)
            .await
        {
            Ok(hits) => Ok(hits),
            Err(e) => Err(RPCError::IOError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Vector search error: {:?}", e),
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
        query_vector: &[f32],
        limit: usize,
    ) -> Result<Vec<VectorHit>, RPCError> {
        let field_id = bifrost_hasher::hash_str(field_name) as u64;
        self.search(schema_id, field_id, query_vector, limit).await
    }

    /// Check if the vector index core is available
    ///
    /// Returns true if a vector implementation (e.g., Morpheus) has been
    /// registered with the system.
    pub fn is_available(&self) -> bool {
        self.vector_client.is_vector_index_core_set()
    }
}

/// Search result with document ID and distance/similarity score
///
/// Re-exported from `crate::index::vector::VectorHit` for convenience.
pub use crate::index::vector::VectorHit as SimilarityHit;

