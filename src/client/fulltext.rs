//! Full-text search client for distributed BM25 queries
//!
//! This module provides `FulltextClient` for performing distributed full-text
//! search across the Nebuchadnezzar cluster.

use std::io;
use std::sync::Arc;

use bifrost::conshash::ConsistentHashing;
use bifrost::rpc::{ClientPool, RPCError};

use crate::index::full_text::coordinator::DistributedInvertedIndexCoordinator;
use crate::index::full_text::BM25Hit;

/// Client for distributed full-text search operations
///
/// Provides BM25-ranked full-text search across all nodes in the cluster.
///
/// # Example
/// ```ignore
/// // Get full-text client from AsyncClient
/// let ft = client.full_text();
///
/// // Search by schema and field IDs
/// let hits = ft.search(schema_id, field_id, "rust programming", 10).await?;
///
/// // Search with global re-ranking for better accuracy
/// let hits = ft.search_rerank(schema_id, field_id, "rust programming", 10).await?;
///
/// // Process results
/// for hit in hits {
///     println!("Doc {:?} score: {:.3}", hit.id, hit.score);
///     let doc = client.read_cell(hit.id).await?;
/// }
/// ```
pub struct FullTextClient {
    conshash: Arc<ConsistentHashing>,
    client_pool: Arc<ClientPool>,
}

impl FullTextClient {
    /// Create a new full-text client
    pub fn new(conshash: Arc<ConsistentHashing>) -> Self {
        Self {
            conshash,
            client_pool: Arc::new(ClientPool::new()),
        }
    }

    /// Create the coordinator for search operations
    fn coordinator(&self) -> DistributedInvertedIndexCoordinator {
        DistributedInvertedIndexCoordinator::new(self.conshash.clone(), self.client_pool.clone())
    }

    /// Search for documents matching the query
    ///
    /// Performs a distributed BM25 search across all nodes, returning
    /// the top `limit` results sorted by relevance score.
    ///
    /// # Arguments
    /// * `schema_id` - Schema ID containing the indexed field
    /// * `field_id` - Field ID to search (must have Fulltext index)
    /// * `query` - Search query (will be tokenized)
    /// * `limit` - Maximum results to return
    ///
    /// # Returns
    /// Vector of `BM25Hit` with document IDs and scores
    pub async fn search(
        &self,
        schema_id: u32,
        field_id: u64,
        query: &str,
        limit: usize,
    ) -> Result<Vec<BM25Hit>, RPCError> {
        let coordinator = self.coordinator();

        match coordinator
            .distributed_search(schema_id, field_id, query, limit, false)
            .await
        {
            Ok(Ok(hits)) => Ok(hits),
            Ok(Err(read_err)) => Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::Other,
                format!("Search read error: {:?}", read_err),
            ))),
            Err(rpc_err) => Err(rpc_err),
        }
    }

    /// Search with global re-ranking for better accuracy
    ///
    /// Same as `search()` but re-computes BM25 scores using global statistics
    /// gathered from all nodes. This provides more accurate ranking when
    /// documents are unevenly distributed across shards.
    ///
    /// More expensive than `search()` due to additional RPC calls for statistics.
    pub async fn search_rerank(
        &self,
        schema_id: u32,
        field_id: u64,
        query: &str,
        limit: usize,
    ) -> Result<Vec<BM25Hit>, RPCError> {
        let coordinator = self.coordinator();

        match coordinator
            .distributed_search(schema_id, field_id, query, limit, true)
            .await
        {
            Ok(Ok(hits)) => Ok(hits),
            Ok(Err(read_err)) => Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::Other,
                format!("Search read error: {:?}", read_err),
            ))),
            Err(rpc_err) => Err(rpc_err),
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
    ) -> Result<Vec<BM25Hit>, RPCError> {
        let field_id = bifrost_hasher::hash_str(field_name) as u64;
        self.search(schema_id, field_id, query, limit).await
    }
}

/// Search result with document ID and relevance score
///
/// Re-exported from `crate::index::full_text::BM25Hit` for convenience.
pub use crate::index::full_text::BM25Hit as SearchHit;
