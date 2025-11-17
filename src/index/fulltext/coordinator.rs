/// Coordinator for distributed inverted index queries
///
/// Integrates with the existing IndexerClients API to provide:
/// 1. Distributed BM25 search across all partitions
/// 2. Aggregation of results from multiple nodes
/// 3. Global statistics computation
/// 4. Transparent routing to appropriate nodes

use std::collections::HashMap;

use bifrost::conshash::ConsistentHashing;
use bifrost::rpc::{ClientPool, RPCError, RPCClient};
use futures::future::join_all;
use log::error;

use crate::ram::cell::ReadError;
use crate::ram::types::Id;
use crate::server::NebServer;
use std::sync::Arc;

use super::{BM25Hit, bm25_score, compute_idf, tokenize_query};
use super::rpc::{
    InvertedSearchRequest, InvertedSearchResponse,
    FieldStatsRequest, FieldStatsResponse,
    TermPostingsRequest, TermPostingsResponse,
    AsyncServiceClient, DEFAULT_SERVICE_ID, InvertedIndexError,
};

/// Distributed inverted index coordinator
///
/// This coordinator integrates with the existing IndexerClients architecture
/// and provides distributed search capabilities across all nodes in the cluster.
pub struct DistributedInvertedIndexCoordinator {
    conshash: Arc<ConsistentHashing>,
    client_pool: Arc<ClientPool>,
}

impl DistributedInvertedIndexCoordinator {
    pub fn new(conshash: Arc<ConsistentHashing>, client_pool: Arc<ClientPool>) -> Self {
        Self {
            conshash,
            client_pool,
        }
    }

    /// Get all active server IDs from the consistent hash ring
    async fn get_all_server_ids(&self) -> Vec<u64> {
        // Get all active members from the membership
        match self.conshash.membership().all_members(true).await {
            Ok((members, _)) => {
                members.into_iter().map(|m| m.id).collect()
            }
            Err(e) => {
                error!("Failed to get all members: {:?}", e);
                vec![]
            }
        }
    }

    /// Get RPC client for a specific server
    async fn get_client(&self, server_id: u64) -> Result<Arc<AsyncServiceClient>, RPCError> {
        // Get the server address from consistent hashing
        let server_addr = self.conshash.to_server_name(server_id);
        
        // Get or create RPC client from the client pool
        let rpc_client = self.client_pool
            .get_by_id(server_id, |_| server_addr)
            .await
            .map_err(|e| RPCError::IOError(e))?;
        
        // Create the async service client using helper function
        let service_client = client_by_rpc_client(&rpc_client);
        
        Ok(service_client)
    }

    /// Search across all partitions and aggregate results
    ///
    /// This provides a unified BM25 search interface that:
    /// 1. Queries all nodes in parallel
    /// 2. Collects local results and statistics
    /// 3. Optionally recomputes global BM25 scores (if rerank = true)
    /// 4. Returns top K results globally
    pub async fn distributed_search(
        &self,
        schema_id: u32,
        field_id: u64,
        query: &str,
        limit: usize,
        rerank: bool, // Whether to recompute BM25 with global stats
    ) -> Result<Result<Vec<BM25Hit>, ReadError>, RPCError> {
        if query.trim().is_empty() || limit == 0 {
            return Ok(Ok(vec![]));
        }

        let server_ids = self.get_all_server_ids().await;
        if server_ids.is_empty() {
            return Ok(Ok(vec![]));
        }

        // Step 1: Query all nodes in parallel
        let mut tasks = vec![];
        for server_id in &server_ids {
            let client = self.get_client(*server_id).await?;
            let req = InvertedSearchRequest {
                schema_id,
                field_id,
                query: query.to_string(),
                limit, // Each node returns top K
            };
            tasks.push(async move {
                client.search_local(req).await
            });
        }

        let responses = join_all(tasks).await;

        // Step 2: Collect results and statistics
        let mut global_doc_count = 0u64;
        let mut global_total_length = 0u64;
        let mut all_hits: Vec<BM25Hit> = vec![];
        let _term_doc_frequencies: HashMap<u64, u64> = HashMap::new();

        for response in responses {
            match response {
                Ok(Ok(resp)) => {
                    global_doc_count += resp.local_doc_count;
                    global_total_length += resp.local_total_length;
                    all_hits.extend(resp.hits);
                }
                Ok(Err(e)) => {
                    error!("RPC error from node: {:?}", e);
                    // Continue with partial results
                }
                Err(e) => {
                    error!("Failed to query node: {:?}", e);
                    // Continue with partial results
                }
            }
        }

        if global_doc_count == 0 {
            return Ok(Ok(vec![]));
        }

        // Step 3: Merge and rerank if requested
        let final_hits = if rerank {
            // Recompute BM25 scores with global statistics
            self.rerank_with_global_stats(
                schema_id,
                field_id,
                query,
                all_hits,
                global_doc_count,
                global_total_length,
                limit,
            ).await?
        } else {
            // Simple merge: sum scores for duplicate documents
            self.merge_hits(all_hits, limit)
        };

        Ok(Ok(final_hits))
    }

    /// Merge hits from multiple nodes (simple score aggregation)
    fn merge_hits(&self, hits: Vec<BM25Hit>, limit: usize) -> Vec<BM25Hit> {
        let mut score_map: HashMap<Id, f32> = HashMap::new();
        for hit in hits {
            *score_map.entry(hit.id).or_insert(0.0) += hit.score;
        }

        let mut final_hits: Vec<BM25Hit> = score_map
            .into_iter()
            .map(|(id, score)| BM25Hit { id, score })
            .collect();

        final_hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        if final_hits.len() > limit {
            final_hits.truncate(limit);
        }

        final_hits
    }

    /// Rerank results using global statistics
    ///
    /// This queries each node for term-level document frequencies
    /// and recomputes BM25 scores with global IDF values.
    async fn rerank_with_global_stats(
        &self,
        schema_id: u32,
        field_id: u64,
        query: &str,
        hits: Vec<BM25Hit>,
        global_doc_count: u64,
        global_total_length: u64,
        limit: usize,
    ) -> Result<Vec<BM25Hit>, RPCError> {
        // Extract unique document IDs
        let mut doc_ids: HashMap<Id, ()> = HashMap::new();
        for hit in &hits {
            doc_ids.insert(hit.id, ());
        }

        // Get query terms
        let query_terms = tokenize_query(query);
        if query_terms.is_empty() {
            return Ok(vec![]);
        }

        // Compute global average document length
        let avg_doc_len = if global_doc_count > 0 {
            global_total_length as f32 / global_doc_count as f32
        } else {
            1.0
        };

        // For each term, collect postings from all nodes
        let mut term_postings: HashMap<u64, Vec<(Id, u32, u32)>> = HashMap::new();
        let server_ids = self.get_all_server_ids().await;

        for term_hash in &query_terms {
            let mut all_postings = vec![];
            
            for server_id in &server_ids {
                let client = self.get_client(*server_id).await?;
                let req = TermPostingsRequest {
                    schema_id,
                    field_id,
                    term_hash: *term_hash,
                };
                
                match client.get_term_postings(req).await {
                    Ok(Ok(resp)) => {
                        all_postings.extend(resp.postings);
                    }
                    Ok(Err(e)) => {
                        error!("RPC error getting term postings: {:?}", e);
                        // Continue with partial results
                    }
                    Err(e) => {
                        error!("Failed to get term postings: {:?}", e);
                        // Continue with partial results
                    }
                }
            }
            
            term_postings.insert(*term_hash, all_postings);
        }

        // Recompute BM25 scores with global statistics
        let mut new_scores: HashMap<Id, f32> = HashMap::new();
        
        for term_hash in query_terms {
            if let Some(postings) = term_postings.get(&term_hash) {
                // Global document frequency for this term
                let df = postings.len() as u64;
                if df == 0 {
                    continue;
                }
                
                // Compute global IDF
                let idf = compute_idf(global_doc_count, df);
                if idf <= 0.0 {
                    continue;
                }
                
                // Update scores for each document
                for (doc_id, tf, doc_len) in postings {
                    // Only score documents that were in the original result set
                    if !doc_ids.contains_key(doc_id) {
                        continue;
                    }
                    
                    let score = bm25_score(*tf as f32, *doc_len as f32, avg_doc_len, idf);
                    if score > 0.0 {
                        *new_scores.entry(*doc_id).or_insert(0.0) += score;
                    }
                }
            }
        }

        // Sort and return top K
        let mut final_hits: Vec<BM25Hit> = new_scores
            .into_iter()
            .map(|(id, score)| BM25Hit { id, score })
            .collect();
        
        final_hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        if final_hits.len() > limit {
            final_hits.truncate(limit);
        }

        Ok(final_hits)
    }

    /// Get global field statistics across all partitions
    pub async fn get_global_stats(
        &self,
        schema_id: u32,
        field_id: u64,
    ) -> Result<FieldStatsResponse, RPCError> {
        let server_ids = self.get_all_server_ids().await;
        let mut tasks = vec![];
        
        for server_id in &server_ids {
            let client = self.get_client(*server_id).await?;
            let req = FieldStatsRequest { schema_id, field_id };
            tasks.push(async move {
                client.get_field_stats(req).await
            });
        }

        let responses = join_all(tasks).await;

        let mut global_doc_count = 0u64;
        let mut global_total_length = 0u64;

        for response in responses {
            match response {
                Ok(Ok(resp)) => {
                    global_doc_count += resp.doc_count;
                    global_total_length += resp.total_length;
                }
                Ok(Err(e)) => {
                    error!("RPC error getting field stats: {:?}", e);
                    // Continue with partial results
                }
                Err(e) => {
                    error!("Failed to get field stats: {:?}", e);
                    // Continue with partial results
                }
            }
        }

        Ok(FieldStatsResponse {
            doc_count: global_doc_count,
            total_length: global_total_length,
        })
    }
}

/// Helper function to create AsyncServiceClient from RPCClient
/// Similar to the pattern used in ranged indexer  
/// Note: The macro generates AsyncServiceClient but may not generate ::new
/// We'll use ServiceClientWithId directly - it returns Arc already
fn client_by_rpc_client(rpc: &Arc<RPCClient>) -> Arc<AsyncServiceClient> {
    use bifrost::rpc::ServiceClientWithId;
    // ServiceClientWithId::new returns Arc<ServiceClientWithId<InvertedIndexRPCService>>
    // AsyncServiceClient should be a type alias for ServiceClientWithId<InvertedIndexRPCService>
    ServiceClientWithId::new(rpc)
}

/// Helper function to create a coordinator from server components
pub async fn create_coordinator_from_server(
    conshash: Arc<ConsistentHashing>,
    client_pool: Arc<ClientPool>,
) -> DistributedInvertedIndexCoordinator {
    DistributedInvertedIndexCoordinator::new(conshash, client_pool)
}

/// Builder for creating a coordinator instance
pub struct CoordinatorBuilder {
    conshash: Option<Arc<ConsistentHashing>>,
    client_pool: Option<Arc<ClientPool>>,
}

impl CoordinatorBuilder {
    pub fn new() -> Self {
        Self {
            conshash: None,
            client_pool: None,
        }
    }

    pub fn with_conshash(mut self, conshash: Arc<ConsistentHashing>) -> Self {
        self.conshash = Some(conshash);
        self
    }

    pub fn with_client_pool(mut self, client_pool: Arc<ClientPool>) -> Self {
        self.client_pool = Some(client_pool);
        self
    }

    pub fn from_server(server: &Arc<NebServer>) -> Self {
        Self {
            conshash: Some(server.consh.clone()),
            client_pool: Some(server.member_pool.clone()),
        }
    }

    pub fn build(self) -> Result<DistributedInvertedIndexCoordinator, String> {
        let conshash = self.conshash
            .ok_or_else(|| "ConsistentHashing not set".to_string())?;
        let client_pool = self.client_pool
            .ok_or_else(|| "ClientPool not set".to_string())?;
        
        Ok(DistributedInvertedIndexCoordinator::new(conshash, client_pool))
    }
}

impl Default for CoordinatorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

