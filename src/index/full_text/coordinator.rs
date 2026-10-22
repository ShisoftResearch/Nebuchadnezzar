/// Coordinator for distributed inverted index queries
///
/// Integrates with the existing IndexerClients API to provide:
/// 1. Distributed BM25 search across all partitions
/// 2. Aggregation of results from multiple nodes
/// 3. Global statistics computation
/// 4. Transparent routing to appropriate nodes
use std::collections::HashMap;

use bifrost::conshash::ConsistentHashing;
use bifrost::rpc::{ClientPool, RPCClient, RPCError, ServiceClient};
use futures::future::join_all;

use crate::ram::cell::ReadError;
use crate::ram::types::Id;
use std::sync::Arc;

use super::rpc::{
    generate_scoped_service_id, AsyncServiceClient, FieldStatsRequest, FieldStatsResponse,
    InvertedIndexError, InvertedSearchRequest, InvertedSearchResponse, TermPostingsRequest,
    TermPostingsResponse,
};
use super::{bm25_score, compute_idf, tokenize_query, BM25Hit};
use crate::ram::schema::SchemaUid;

/// Distributed inverted index coordinator
///
/// This coordinator integrates with the existing IndexerClients architecture
/// and provides distributed search capabilities across all nodes in the cluster.
pub struct DistributedInvertedIndexCoordinator {
    conshash: Arc<ConsistentHashing>,
    client_pool: Arc<ClientPool>,
    group_name: String,
    database_name: String,
    deadline: std::time::Duration,
}

/// The time one coordinator call (a search, with its rerank, or a statistics
/// read) may take over all shards. A shard that has not answered by then
/// fails the call with `FullTextShardFailure`. TCP calls have their own send
/// and response timeouts, but a call to the local shard does not go through
/// TCP, so without this bound a stalled local shard stalls the search.
pub const SEARCH_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

impl DistributedInvertedIndexCoordinator {
    pub fn new(
        conshash: Arc<ConsistentHashing>,
        client_pool: Arc<ClientPool>,
        group_name: impl Into<String>,
        database_name: impl Into<String>,
    ) -> Self {
        Self {
            conshash,
            client_pool,
            group_name: group_name.into(),
            database_name: database_name.into(),
            deadline: SEARCH_DEADLINE,
        }
    }

    /// Replace `SEARCH_DEADLINE` for this coordinator.
    pub fn with_deadline(mut self, deadline: std::time::Duration) -> Self {
        self.deadline = deadline;
        self
    }

    /// One shard call, bounded by the call's `deadline`. Every failure,
    /// including no answer in time, is a `FullTextShardFailure` for the shard.
    async fn ask_shard<T, F, Fut>(
        &self,
        deadline: tokio::time::Instant,
        server_id: u64,
        call: F,
    ) -> Result<T, RPCError>
    where
        F: FnOnce(Arc<AsyncServiceClient>) -> Fut,
        Fut: std::future::Future<Output = Result<Result<T, InvertedIndexError>, RPCError>>,
    {
        let answer = tokio::time::timeout_at(deadline, async {
            let client = self.get_client(server_id).await?;
            call(client).await
        })
        .await;
        match answer {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(error))) => Err(shard_failure(Some(server_id), format!("{error:?}"))),
            Ok(Err(error)) => Err(shard_failure(Some(server_id), format!("{error:?}"))),
            Err(_) => Err(shard_failure(
                Some(server_id),
                format!(
                    "no answer within the search deadline of {} ms",
                    self.deadline.as_millis()
                ),
            )),
        }
    }

    /// Get all active server IDs from the consistent hash ring
    async fn get_all_server_ids(&self) -> Result<Vec<u64>, RPCError> {
        let (members, _) = self
            .conshash
            .membership()
            .all_members(true)
            .await
            .map_err(|error| shard_failure(None, format!("membership query failed: {error:?}")))?;
        Ok(members.into_iter().map(|m| m.id).collect())
    }

    /// Get RPC client for a specific server
    async fn get_client(&self, server_id: u64) -> Result<Arc<AsyncServiceClient>, RPCError> {
        // Get the server address from consistent hashing
        // An unknown member is routine here -- placement can name one that has
        // left -- so it is an error the caller handles, not a panic.
        let server_addr = self.conshash.try_server_name(server_id).ok_or_else(|| {
            RPCError::IOError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no address known for server id {server_id}"),
            ))
        })?;

        // Get or create RPC client from the client pool
        let rpc_client = self
            .client_pool
            .get_by_id(server_id, |_| Some(server_addr))
            .await
            .map_err(|e| RPCError::IOError(e))?;

        // Create the async service client using helper function
        let service_client =
            client_by_rpc_client(&rpc_client, &self.group_name, &self.database_name);

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
        schema_id: SchemaUid,
        field_id: u64,
        query: &str,
        limit: usize,
        rerank: bool, // Whether to recompute BM25 with global stats (distributed re-ranking)
        phrase_boost: bool, // Whether to apply local phrase boosting
    ) -> Result<Result<Vec<BM25Hit>, ReadError>, RPCError> {
        if query.trim().is_empty() || limit == 0 {
            return Ok(Ok(vec![]));
        }

        let deadline = tokio::time::Instant::now() + self.deadline;
        let server_ids = self.get_all_server_ids().await?;
        if server_ids.is_empty() {
            return Ok(Ok(vec![]));
        }

        // Step 1: Query all nodes in parallel, each bounded by the deadline.
        let responses = join_all(server_ids.iter().map(|server_id| {
            let req = InvertedSearchRequest {
                schema_id,
                field_id,
                query: query.to_string(),
                limit,        // Each node returns top K
                phrase_boost, // Apply local phrase boosting if requested
            };
            self.ask_shard(deadline, *server_id, move |client| async move {
                client.search_local(req).await
            })
        }))
        .await;

        // Step 2: Collect results and statistics
        let mut global_doc_count = 0u64;
        let mut global_total_length = 0u64;
        let mut all_hits: Vec<BM25Hit> = vec![];
        let _term_doc_frequencies: HashMap<u64, u64> = HashMap::new();

        // A shard that did not answer would leave its documents out of the
        // ranking: that is an error, not a shorter result.
        for response in responses {
            let resp = response?;
            global_doc_count += resp.local_doc_count;
            global_total_length += resp.local_total_length;
            all_hits.extend(resp.hits);
        }

        if global_doc_count == 0 {
            return Ok(Ok(vec![]));
        }

        // Step 3: Merge and rerank if requested
        let final_hits = if rerank {
            // Recompute BM25 scores with global statistics
            self.rerank_with_global_stats(
                deadline,
                schema_id,
                field_id,
                query,
                all_hits,
                global_doc_count,
                global_total_length,
                limit,
            )
            .await?
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
        deadline: tokio::time::Instant,
        schema_id: SchemaUid,
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
        let server_ids = self.get_all_server_ids().await?;

        for term_hash in &query_terms {
            let mut all_postings = vec![];

            for server_id in &server_ids {
                let req = TermPostingsRequest {
                    schema_id,
                    field_id,
                    term_hash: *term_hash,
                };
                let resp = self
                    .ask_shard(deadline, *server_id, move |client| async move {
                        client.get_term_postings(req).await
                    })
                    .await?;
                all_postings.extend(resp.postings);
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
        schema_id: SchemaUid,
        field_id: u64,
    ) -> Result<FieldStatsResponse, RPCError> {
        let deadline = tokio::time::Instant::now() + self.deadline;
        let server_ids = self.get_all_server_ids().await?;
        let responses = join_all(server_ids.iter().map(|server_id| {
            let req = FieldStatsRequest {
                schema_id,
                field_id,
            };
            self.ask_shard(deadline, *server_id, move |client| async move {
                client.get_field_stats(req).await
            })
        }))
        .await;

        let mut global_doc_count = 0u64;
        let mut global_total_length = 0u64;
        for response in responses {
            let resp = response?;
            global_doc_count += resp.doc_count;
            global_total_length += resp.total_length;
        }

        Ok(FieldStatsResponse {
            doc_count: global_doc_count,
            total_length: global_total_length,
        })
    }
}

/// A full-text shard that did not answer. Carried in `RPCError::IOError`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FullTextShardFailure {
    pub server_id: Option<u64>,
    pub reason: String,
}

impl FullTextShardFailure {
    /// The shard failure inside an error from the coordinator, if it is one.
    pub fn of(error: &RPCError) -> Option<&FullTextShardFailure> {
        match error {
            RPCError::IOError(error) => error.get_ref()?.downcast_ref(),
            _ => None,
        }
    }
}

impl std::fmt::Display for FullTextShardFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.server_id {
            Some(server) => write!(
                f,
                "full-text shard on server {server} failed: {}",
                self.reason
            ),
            None => write!(f, "full-text search failed: {}", self.reason),
        }
    }
}

impl std::error::Error for FullTextShardFailure {}

fn shard_failure(server_id: Option<u64>, reason: String) -> RPCError {
    RPCError::IOError(std::io::Error::other(FullTextShardFailure {
        server_id,
        reason,
    }))
}

/// Helper function to create AsyncServiceClient from RPCClient
/// Similar to the pattern used in ranged indexer  
/// Note: The macro generates AsyncServiceClient but may not generate ::new
/// We'll use ServiceClientWithId directly - it returns Arc already
fn client_by_rpc_client(
    rpc: &Arc<RPCClient>,
    group_name: &str,
    database_name: &str,
) -> Arc<AsyncServiceClient> {
    AsyncServiceClient::new_with_service_id(
        generate_scoped_service_id(group_name, database_name),
        rpc,
    )
}

/// Helper function to create a coordinator from server components
pub async fn create_coordinator_from_server(
    conshash: Arc<ConsistentHashing>,
    client_pool: Arc<ClientPool>,
    group_name: impl Into<String>,
    database_name: impl Into<String>,
) -> DistributedInvertedIndexCoordinator {
    DistributedInvertedIndexCoordinator::new(conshash, client_pool, group_name, database_name)
}

/// Builder for creating a coordinator instance
pub struct CoordinatorBuilder {
    conshash: Option<Arc<ConsistentHashing>>,
    client_pool: Option<Arc<ClientPool>>,
    group_name: Option<String>,
    database_name: Option<String>,
}

impl CoordinatorBuilder {
    pub fn new() -> Self {
        Self {
            conshash: None,
            client_pool: None,
            group_name: None,
            database_name: None,
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

    pub fn with_database_scope(
        mut self,
        group_name: impl Into<String>,
        database_name: impl Into<String>,
    ) -> Self {
        self.group_name = Some(group_name.into());
        self.database_name = Some(database_name.into());
        self
    }

    pub fn from_parts(conshash: Arc<ConsistentHashing>, client_pool: Arc<ClientPool>) -> Self {
        Self {
            conshash: Some(conshash),
            client_pool: Some(client_pool),
            group_name: None,
            database_name: None,
        }
    }

    pub fn build(self) -> Result<DistributedInvertedIndexCoordinator, String> {
        let conshash = self
            .conshash
            .ok_or_else(|| "ConsistentHashing not set".to_string())?;
        let client_pool = self
            .client_pool
            .ok_or_else(|| "ClientPool not set".to_string())?;

        Ok(DistributedInvertedIndexCoordinator::new(
            conshash,
            client_pool,
            self.group_name.unwrap_or_default(),
            self.database_name.unwrap_or_default(),
        ))
    }
}

impl Default for CoordinatorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::cell::OwnedCell;
    use crate::ram::schema::SchemaVid;
    use crate::ram::types::{Map, OwnedMap, OwnedValue};
    use bifrost_hasher::hash_str;
    use log::info;
    use std::time::Duration;

    /// Test coordinator logic for aggregating results from multiple shards
    ///
    /// NOTE: True multi-node cluster testing requires separate processes with shared
    /// Raft state, which is complex in unit tests. This test verifies the coordinator's
    /// aggregation logic works correctly by:
    /// 1. Using two independent servers (simulating shards)
    /// 2. Each server indexes its own documents
    /// 3. We manually aggregate using both coordinators to verify the logic
    ///
    /// For full integration testing of distributed search across a real cluster,
    /// use integration tests with multiple processes.
    #[tokio::test]
    async fn test_coordinator_aggregation_logic() {
        let _ = env_logger::try_init();

        info!("Starting coordinator aggregation logic test");

        // Create two independent servers (simulating separate shards)
        // NOTE: In a real cluster, these would share Raft state and see each other
        info!("Creating shard 1...");
        let shard1 = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![crate::server::Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &crate::utils::test_port::unique_localhost_addr(),
            "shard1_group",
            async |_| {},
        )
        .await
        .unwrap();

        info!("Creating shard 2...");
        let shard2 = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![crate::server::Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &crate::utils::test_port::unique_localhost_addr(),
            "shard2_group",
            async |_| {},
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Create schema
        let schema_id = 502u32;
        let content_field = "content";
        let content_field_id = hash_str(content_field);

        let fields =
            crate::ram::schema::Field::new_schema(vec![crate::ram::schema::Field::new_indexed(
                content_field,
                dovahkiin::types::Type::String,
                vec![crate::ram::schema::IndexType::Fulltext],
            )]);

        let schema = crate::ram::schema::Schema::new_with_id(
            schema_id,
            "aggregation_test_schema",
            None,
            fields,
            false,
            false,
        );

        shard1.meta().schemas.debug_only_new_schema(schema.clone());
        shard2.meta().schemas.debug_only_new_schema(schema.clone());

        // Find documents owned by each shard
        let mut shard1_docs = Vec::new();
        let mut shard2_docs = Vec::new();
        for i in 0..1000 {
            let test_id = Id::from_parts(i, i);
            if shard1
                .consh
                .get_server_id(test_id.locality() as u64)
                .map(|sid| sid == shard1.server_id)
                .unwrap_or(false)
                && shard1_docs.len() < 2
            {
                shard1_docs.push(test_id);
            }
            if shard2
                .consh
                .get_server_id(test_id.locality() as u64)
                .map(|sid| sid == shard2.server_id)
                .unwrap_or(false)
                && shard2_docs.len() < 2
            {
                shard2_docs.push(test_id);
            }
            if shard1_docs.len() >= 2 && shard2_docs.len() >= 2 {
                break;
            }
        }

        assert!(shard1_docs.len() >= 2, "Need docs for shard1");
        assert!(shard2_docs.len() >= 2, "Need docs for shard2");

        // Index documents on shard1: "rust programming", "database systems"
        let mut cell1_data = OwnedMap::new();
        cell1_data.insert(
            content_field,
            OwnedValue::String("rust programming language guide".to_string()),
        );
        let mut cell1 = OwnedCell::new_with_id(
            SchemaVid(schema_id),
            &shard1_docs[0],
            OwnedValue::Map(cell1_data),
        );

        let mut cell2_data = OwnedMap::new();
        cell2_data.insert(
            content_field,
            OwnedValue::String("database storage systems".to_string()),
        );
        let mut cell2 = OwnedCell::new_with_id(
            SchemaVid(schema_id),
            &shard1_docs[1],
            OwnedValue::Map(cell2_data),
        );

        shard1.chunks().write_cell(&mut cell1).unwrap();
        shard1.chunks().write_cell(&mut cell2).unwrap();
        if let Some(ib) = shard1.indexer() {
            ib.ensure_indices(&cell1, &schema, None);
            ib.ensure_indices(&cell2, &schema, None);
        }

        // Index documents on shard2: "rust async tokio", "search architecture"
        let mut cell3_data = OwnedMap::new();
        cell3_data.insert(
            content_field,
            OwnedValue::String("rust async programming tokio".to_string()),
        );
        let mut cell3 = OwnedCell::new_with_id(
            SchemaVid(schema_id),
            &shard2_docs[0],
            OwnedValue::Map(cell3_data),
        );

        let mut cell4_data = OwnedMap::new();
        cell4_data.insert(
            content_field,
            OwnedValue::String("search engine architecture".to_string()),
        );
        let mut cell4 = OwnedCell::new_with_id(
            SchemaVid(schema_id),
            &shard2_docs[1],
            OwnedValue::Map(cell4_data),
        );

        shard2.chunks().write_cell(&mut cell3).unwrap();
        shard2.chunks().write_cell(&mut cell4).unwrap();
        if let Some(ib) = shard2.indexer() {
            ib.ensure_indices(&cell3, &schema, None);
            ib.ensure_indices(&cell4, &schema, None);
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Test 1: Coordinator on shard1 finds its local documents
        info!("Testing shard1 coordinator...");
        let coord1 = DistributedInvertedIndexCoordinator::new(
            shard1.consh.clone(),
            shard1.member_pool.clone(),
            shard1.database_runtime().group_name(),
            shard1.database_name(),
        );

        let stats1 = coord1
            .get_global_stats(SchemaUid(schema_id), content_field_id)
            .await
            .unwrap();
        info!("Shard1 stats: doc_count={}", stats1.doc_count);
        assert_eq!(stats1.doc_count, 2, "Shard1 should have 2 documents");

        let hits1 = coord1
            .distributed_search(
                SchemaUid(schema_id),
                content_field_id,
                "rust",
                10,
                false,
                false,
            )
            .await
            .unwrap()
            .unwrap();
        info!("Shard1 'rust' hits: {}", hits1.len());
        assert_eq!(hits1.len(), 1, "Shard1 should find 1 'rust' document");
        assert_eq!(hits1[0].id, shard1_docs[0]);

        // Test 2: Coordinator on shard2 finds its local documents
        info!("Testing shard2 coordinator...");
        let coord2 = DistributedInvertedIndexCoordinator::new(
            shard2.consh.clone(),
            shard2.member_pool.clone(),
            shard2.database_runtime().group_name(),
            shard2.database_name(),
        );

        let stats2 = coord2
            .get_global_stats(SchemaUid(schema_id), content_field_id)
            .await
            .unwrap();
        info!("Shard2 stats: doc_count={}", stats2.doc_count);
        assert_eq!(stats2.doc_count, 2, "Shard2 should have 2 documents");

        let hits2 = coord2
            .distributed_search(
                SchemaUid(schema_id),
                content_field_id,
                "rust",
                10,
                false,
                false,
            )
            .await
            .unwrap()
            .unwrap();
        info!("Shard2 'rust' hits: {}", hits2.len());
        assert_eq!(hits2.len(), 1, "Shard2 should find 1 'rust' document");
        assert_eq!(hits2[0].id, shard2_docs[0]);

        // Test 3: Verify merge_hits logic works correctly
        // Create hits with explicitly different IDs to test aggregation
        let test_hit1 = BM25Hit {
            id: Id::from_parts(1000, 1),
            score: 1.5,
        };
        let test_hit2 = BM25Hit {
            id: Id::from_parts(1000, 2),
            score: 2.0,
        };
        let test_hit3 = BM25Hit {
            id: Id::from_parts(1000, 1), // Duplicate of hit1
            score: 0.5,
        };
        let combined_hits = vec![test_hit1, test_hit2, test_hit3];
        let merged = coord1.merge_hits(combined_hits, 10);
        assert_eq!(
            merged.len(),
            2,
            "Merged results should have 2 unique documents"
        );
        // Score for id (1000,1) should be 1.5 + 0.5 = 2.0
        let hit_1_1 = merged
            .iter()
            .find(|h| h.id == Id::from_parts(1000, 1))
            .unwrap();
        assert!(
            (hit_1_1.score - 2.0).abs() < 0.001,
            "Scores should be aggregated"
        );

        // Test 4: Verify stats aggregation logic
        let global_doc_count = stats1.doc_count + stats2.doc_count;
        let global_total_length = stats1.total_length + stats2.total_length;
        assert_eq!(global_doc_count, 4, "Combined should have 4 documents");
        info!(
            "Combined stats: doc_count={}, total_length={}",
            global_doc_count, global_total_length
        );

        // Test 5: Each shard finds its unique content
        let db_hits = coord1
            .distributed_search(
                SchemaUid(schema_id),
                content_field_id,
                "database",
                10,
                false,
                false,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(db_hits.len(), 1, "Should find 'database' on shard1");
        assert_eq!(db_hits[0].id, shard1_docs[1]);

        let search_hits = coord2
            .distributed_search(
                SchemaUid(schema_id),
                content_field_id,
                "search engine",
                10,
                false,
                false,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            search_hits.len(),
            1,
            "Should find 'search engine' on shard2"
        );
        assert_eq!(search_hits[0].id, shard2_docs[1]);

        info!("Coordinator aggregation logic test passed!");
    }

    /// Test that coordinator handles single-server clusters correctly
    #[tokio::test]
    async fn test_distributed_search_single_shard() {
        let _ = env_logger::try_init();

        info!("Starting single-shard distributed search test");

        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![crate::server::Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &crate::utils::test_port::unique_localhost_addr(),
            "single_shard_test",
            async |_| {},
        )
        .await
        .unwrap();

        let schema_id = 501u32;
        let content_field = "content";
        let content_field_id = hash_str(content_field);

        let fields =
            crate::ram::schema::Field::new_schema(vec![crate::ram::schema::Field::new_indexed(
                content_field,
                dovahkiin::types::Type::String,
                vec![crate::ram::schema::IndexType::Fulltext],
            )]);

        let schema = crate::ram::schema::Schema::new_with_id(
            schema_id,
            "single_shard_test_schema",
            None,
            fields,
            false,
            false,
        );

        server.meta().schemas.debug_only_new_schema(schema.clone());

        // Find owned document
        let mut doc_id = None;
        for i in 0..1000 {
            let test_id = Id::from_parts(i, i);
            if server
                .consh
                .get_server_id(test_id.locality() as u64)
                .map(|sid| sid == server.server_id)
                .unwrap_or(false)
            {
                doc_id = Some(test_id);
                break;
            }
        }
        let doc_id = doc_id.expect("Should find owned document");

        // Create and index document
        let mut cell_data = OwnedMap::new();
        cell_data.insert(
            content_field,
            OwnedValue::String("hello world from single shard".to_string()),
        );
        let mut cell =
            OwnedCell::new_with_id(SchemaVid(schema_id), &doc_id, OwnedValue::Map(cell_data));

        server.chunks().write_cell(&mut cell).unwrap();
        if let Some(index_builder) = server.indexer() {
            index_builder.ensure_indices(&cell, &schema, None);
        }

        tokio::time::sleep(Duration::from_millis(300)).await;

        // Create coordinator
        let coordinator = DistributedInvertedIndexCoordinator::new(
            server.consh.clone(),
            server.member_pool.clone(),
            server.database_runtime().group_name(),
            server.database_name(),
        );

        // Search
        let hits_result = coordinator
            .distributed_search(
                SchemaUid(schema_id),
                content_field_id,
                "hello world",
                10,
                false,
                false,
            )
            .await
            .unwrap();
        let hits = hits_result.unwrap();

        assert!(!hits.is_empty(), "Should find document");
        assert_eq!(hits[0].id, doc_id);

        info!("Single-shard distributed search test passed!");
    }

    /// A shard that does not answer fails the search at the deadline, with a
    /// typed error that names it. The local shard is called without TCP, so
    /// no transport timeout would end the wait.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_slow_shard_fails_the_search_at_the_deadline() {
        let _ = env_logger::try_init();
        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![crate::server::Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &crate::utils::test_port::unique_localhost_addr(),
            "slow_shard_test",
            async |_| {},
        )
        .await
        .unwrap();
        let schema_id = 502u32;
        let content_field = "content";
        let content_field_id = hash_str(content_field);
        let schema = crate::ram::schema::Schema::new_with_id(
            schema_id,
            "slow_shard_test_schema",
            None,
            crate::ram::schema::Field::new_schema(vec![crate::ram::schema::Field::new_indexed(
                content_field,
                dovahkiin::types::Type::String,
                vec![crate::ram::schema::IndexType::Fulltext],
            )]),
            false,
            false,
        );
        server.meta().schemas.debug_only_new_schema(schema.clone());
        // From 1: `Id::from_parts(0, 0)` is the unit id.
        let doc_id = (1..1000)
            .map(|i| Id::from_parts(i, i))
            .find(|id| server.consh.get_server_id(id.locality() as u64) == Some(server.server_id))
            .expect("an owned document id");
        let mut cell_data = OwnedMap::new();
        cell_data.insert(content_field, OwnedValue::String("hello slow shard".into()));
        let mut cell =
            OwnedCell::new_with_id(SchemaVid(schema_id), &doc_id, OwnedValue::Map(cell_data));
        server.chunks().write_cell(&mut cell).unwrap();
        server
            .indexer()
            .unwrap()
            .ensure_indices(&cell, &schema, None);
        let _ = server.await_index_tasks().await;

        let coordinator = DistributedInvertedIndexCoordinator::new(
            server.consh.clone(),
            server.member_pool.clone(),
            server.database_runtime().group_name(),
            server.database_name(),
        )
        .with_deadline(Duration::from_millis(300));
        let search = || {
            coordinator.distributed_search(
                SchemaUid(schema_id),
                content_field_id,
                "hello",
                10,
                false,
                false,
            )
        };
        assert_eq!(search().await.unwrap().unwrap()[0].id, doc_id);

        super::super::rpc::slow_shards::delay(server.server_id, 3_000);
        let began = std::time::Instant::now();
        let slow = search().await;
        let waited = began.elapsed();
        super::super::rpc::slow_shards::clear(server.server_id);
        let error = slow.expect_err("a shard that does not answer must fail the search");
        let failure = FullTextShardFailure::of(&error).expect("a typed shard failure");
        assert_eq!(failure.server_id, Some(server.server_id), "{failure}");
        assert!(failure.reason.contains("deadline"), "{failure}");
        assert!(
            waited < Duration::from_millis(2_000),
            "the search waited {waited:?} for a 300 ms deadline"
        );
        server.shutdown().await;
    }

    /// Test empty query handling
    #[tokio::test]
    async fn test_distributed_search_empty_query() {
        let _ = env_logger::try_init();

        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![crate::server::Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &crate::utils::test_port::unique_localhost_addr(),
            "empty_query_test",
            async |_| {},
        )
        .await
        .unwrap();

        let coordinator = DistributedInvertedIndexCoordinator::new(
            server.consh.clone(),
            server.member_pool.clone(),
            server.database_runtime().group_name(),
            server.database_name(),
        );

        // Empty query should return empty results
        let hits_result = coordinator
            .distributed_search(SchemaUid(100), 1, "", 10, false, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert!(hits.is_empty(), "Empty query should return empty results");

        // Whitespace-only query should return empty results
        let hits_result = coordinator
            .distributed_search(SchemaUid(100), 1, "   ", 10, false, false)
            .await
            .unwrap();
        let hits = hits_result.unwrap();
        assert!(
            hits.is_empty(),
            "Whitespace query should return empty results"
        );

        info!("Empty query test passed!");
    }
}
