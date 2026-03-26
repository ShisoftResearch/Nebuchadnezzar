/// RPC service for distributed inverted index queries
///
/// Each server exposes an RPC endpoint that allows coordinators to:
/// 1. Query the local partition (owned documents only)
/// 2. Aggregate results from multiple nodes
/// 3. Return partial results for distributed BM25 scoring
use bifrost::rpc::*;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use futures::future::BoxFuture;
use futures::prelude::*;
use serde::{Deserialize, Serialize};

use super::{shard::InvertedIndexer, BM25Hit};
use crate::ram::types::Id;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(NEB_INVERTED_INDEX_RPC_SERVICE) as u64;

pub fn generate_scoped_service_id(group_name: &str, database_name: &str) -> u64 {
    if group_name == database_name || group_name.is_empty() || database_name.is_empty() {
        DEFAULT_SERVICE_ID
    } else {
        hash_str(&format!(
            "NEB_INVERTED_INDEX_RPC_SERVICE-{}-{}",
            group_name, database_name
        ))
    }
}

/// Error type for inverted index RPC operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InvertedIndexError {
    SearchError(String),
    Other(String),
}

/// Request to search a specific field for a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvertedSearchRequest {
    pub schema_id: u32,
    pub field_id: u64,
    pub query: String,
    pub limit: usize,
    pub phrase_boost: bool,
}

/// Response containing search results from this partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvertedSearchResponse {
    pub hits: Vec<BM25Hit>,
    pub local_doc_count: u64,    // Documents in this partition
    pub local_total_length: u64, // Total length in this partition
}

/// Request for field statistics (for distributed BM25)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldStatsRequest {
    pub schema_id: u32,
    pub field_id: u64,
}

/// Response containing field statistics from this partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldStatsResponse {
    pub doc_count: u64,
    pub total_length: u64,
}

/// Request for posting list of a specific term (for distributed queries)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermPostingsRequest {
    pub schema_id: u32,
    pub field_id: u64,
    pub term_hash: u64,
}

/// Response containing postings for a specific term
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermPostingsResponse {
    pub postings: Vec<(Id, u32, u32)>, // (doc_id, term_freq, doc_length)
}

/// RPC service for inverted index operations
pub struct InvertedIndexRPCService {
    indexer: Arc<InvertedIndexer>,
}

impl InvertedIndexRPCService {
    pub fn new(indexer: Arc<InvertedIndexer>) -> Arc<Self> {
        Arc::new(Self { indexer })
    }
}

// Bifrost RPC service definition
service! {
    rpc search_local(req: InvertedSearchRequest) -> Result<InvertedSearchResponse, InvertedIndexError>;
    rpc get_field_stats(req: FieldStatsRequest) -> Result<FieldStatsResponse, InvertedIndexError>;
    rpc get_term_postings(req: TermPostingsRequest) -> Result<TermPostingsResponse, InvertedIndexError>;
}

service_with_id!(InvertedIndexRPCService, DEFAULT_SERVICE_ID);

impl Service for InvertedIndexRPCService {
    fn search_local(
        &self,
        req: InvertedSearchRequest,
    ) -> BoxFuture<'_, Result<InvertedSearchResponse, InvertedIndexError>> {
        async move {
            let hits = self
                .indexer
                .bm25_search(
                    req.schema_id,
                    req.field_id,
                    &req.query,
                    req.limit,
                    req.phrase_boost,
                )
                .await
                .map_err(|e| InvertedIndexError::SearchError(format!("{:?}", e)))?;

            let stats = self.indexer.get_field_stats(req.schema_id, req.field_id);

            Ok(InvertedSearchResponse {
                hits,
                local_doc_count: stats.doc_count,
                local_total_length: stats.total_length,
            })
        }
        .boxed()
    }

    fn get_field_stats(
        &self,
        req: FieldStatsRequest,
    ) -> BoxFuture<'_, Result<FieldStatsResponse, InvertedIndexError>> {
        async move {
            let stats = self.indexer.get_field_stats(req.schema_id, req.field_id);

            Ok(FieldStatsResponse {
                doc_count: stats.doc_count,
                total_length: stats.total_length,
            })
        }
        .boxed()
    }

    fn get_term_postings(
        &self,
        req: TermPostingsRequest,
    ) -> BoxFuture<'_, Result<TermPostingsResponse, InvertedIndexError>> {
        async move {
            let postings =
                self.indexer
                    .get_term_postings(req.schema_id, req.field_id, req.term_hash);

            Ok(TermPostingsResponse { postings })
        }
        .boxed()
    }
}

dispatch_rpc_service_functions!(InvertedIndexRPCService);
