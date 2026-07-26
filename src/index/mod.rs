#[macro_use]
mod macros;
#[macro_use]
pub mod builder;
pub mod embedding;
pub mod entry;
pub mod full_text;
pub mod hash;
pub mod ranged;
pub mod vector;

pub const FEATURE_SIZE: usize = 8;
pub const KEY_SIZE: usize = ID_SIZE + FEATURE_SIZE + 8; // 8 is the estimate length of: schema id u32 (4) + field id u32(4, reduced from u64)
pub const SCHEMA_SCAN_PATT_SIZE: u8 = (FEATURE_SIZE + 8) as u8;
pub const MAX_KEY_SIZE: usize = KEY_SIZE * 2;

use std::sync::Arc;
use std::sync::OnceLock;

use bifrost::rpc::RPCError;
use bifrost::{conshash::ConsistentHashing, raft::client::AsRaftPlaneClient};
use dovahkiin::types::{Id, OwnedValue};
pub use entry::EntryKey;
pub use entry::ID_SIZE;
use futures::Future;
use hash::{hash_index_schema, HashedIndexClient};

use crate::client::AsyncClient;
use crate::index::embedding::EmbeddingIndexClient;
use crate::index::full_text::shard::InvertedIndexer;
use crate::index::full_text::{
    inverted_doc_schema, inverted_index_schema, inverted_stats_schema, BM25Hit,
};
use crate::index::vector::VectorIndexClient;
use crate::query::statistics::SchemaStatistics;
use crate::ram::cell::ReadError;
use crate::ram::chunk::Chunks;
use std::time::Duration;

use self::ranged::client::cursor::ClientCursor;
use self::ranged::client::RangedIndexerClient;
use self::ranged::tree::service::Range;

pub type Feature = [u8; FEATURE_SIZE];

pub struct IndexerClients {
    pub ranged_client: Arc<RangedIndexerClient>,
    pub hashed_client: Arc<HashedIndexClient>,
    pub vector_client: Arc<VectorIndexClient>,
    pub embedding_client: Arc<EmbeddingIndexClient>,
    fulltext_indexer: OnceLock<Arc<InvertedIndexer>>,
    // Store initialization parameters for lazy initialization
    server_id: u64,
    pub(crate) neb_client: Arc<AsyncClient>,
    conshash: Arc<ConsistentHashing>,
}

impl IndexerClients {
    /// Create IndexerClients without inverted indexer (lazy initialization)
    pub fn new<C>(
        neb_client: &Arc<AsyncClient>,
        conshash: &Arc<ConsistentHashing>,
        raft_client: &Arc<C>,
        server_id: u64,
    ) -> Self
    where
        C: AsRaftPlaneClient + 'static,
    {
        IndexerClients {
            ranged_client: Arc::new(RangedIndexerClient::new_for_database(
                conshash,
                raft_client,
                neb_client.group_name(),
                neb_client.database_name(),
            )),
            hashed_client: Arc::new(HashedIndexClient::new(neb_client)),
            vector_client: Arc::new(VectorIndexClient::new()),
            embedding_client: Arc::new(EmbeddingIndexClient::new()),
            fulltext_indexer: OnceLock::new(),
            server_id,
            neb_client: neb_client.clone(),
            conshash: conshash.clone(),
        }
    }

    /// Attach the inverted indexer to chunks without starting background work.
    pub fn initialize_inverted_indexer(&self, chunks: &Arc<Chunks>) {
        let inverted_indexer = Arc::new(InvertedIndexer::new(
            self.server_id,
            chunks.clone(),
            self.neb_client.clone(),
            Duration::from_secs(30), // Flush every 30 seconds
        ));

        // OnceLock ensures this can only be set once
        let _ = self.fulltext_indexer.set(inverted_indexer);
    }

    /// Start inverted-index background work after storage is ready for traffic.
    pub fn start_inverted_indexer_background_flush(&self) {
        if let Some(indexer) = self.fulltext_indexer.get() {
            indexer.start_background_flush();
        }
    }

    /// Get the inverted indexer if initialized
    pub fn fulltext_indexer(&self) -> Option<&Arc<InvertedIndexer>> {
        self.fulltext_indexer.get()
    }

    pub fn overall_schema_statistics(&self, schema_id: u32) -> Option<Arc<SchemaStatistics>> {
        self.fulltext_indexer()
            .and_then(|indexer| indexer.try_overall_schema_statistics(schema_id))
    }

    /// Create IndexerClients without hybrid inverted indexer (for query-only clients)
    pub fn new_query_only<C>(
        neb_client: &Arc<AsyncClient>,
        conshash: &Arc<ConsistentHashing>,
        raft_client: &Arc<C>,
        server_id: u64,
    ) -> Self
    where
        C: AsRaftPlaneClient + 'static,
    {
        IndexerClients {
            ranged_client: Arc::new(RangedIndexerClient::new_for_database(
                conshash,
                raft_client,
                neb_client.group_name(),
                neb_client.database_name(),
            )),
            hashed_client: Arc::new(HashedIndexClient::new(neb_client)),
            vector_client: Arc::new(VectorIndexClient::new()),
            embedding_client: Arc::new(EmbeddingIndexClient::new()),
            fulltext_indexer: OnceLock::new(), // Empty, won't be initialized
            server_id,
            neb_client: neb_client.clone(),
            conshash: conshash.clone(),
        }
    }
    pub async fn init_index_schema(neb_client: &Arc<AsyncClient>) {
        let hash_index_schema = hash_index_schema();
        let _ = neb_client.new_schema_with_id(hash_index_schema).await;
        let _ = neb_client.new_schema_with_id(inverted_index_schema()).await;
        let _ = neb_client.new_schema_with_id(inverted_stats_schema()).await;
        let _ = neb_client.new_schema_with_id(inverted_doc_schema()).await;
    }
    pub fn range_seek<'a>(
        &'a self,
        range: Range,
        buffer_size: u16,
        pattern: Option<u8>,
    ) -> impl Future<Output = Result<Option<ClientCursor>, RPCError>> + 'a {
        let key = range.key();
        let pattern = pattern.map(|n| {
            let n = n as usize;
            key.as_slice()[..n].to_vec()
        });
        RangedIndexerClient::seek(&self.ranged_client, range, buffer_size, pattern)
    }

    pub async fn hashed_query(
        &self,
        index_id: Id,
        field_id: u64,
        value: &OwnedValue,
    ) -> Result<Result<Vec<Id>, ReadError>, RPCError> {
        HashedIndexClient::query(&self.hashed_client, index_id, field_id, value).await
    }

    pub async fn bm25_search(
        &self,
        schema_id: u32,
        field_id: u64,
        query: &str,
        limit: usize,
        phrase_boost: bool,
    ) -> Result<Result<Vec<BM25Hit>, ReadError>, RPCError> {
        match self.fulltext_indexer() {
            Some(indexer) => match indexer
                .bm25_search(schema_id, field_id, query, limit, phrase_boost)
                .await
            {
                Ok(hits) => Ok(Ok(hits)),
                Err(e) => Ok(Err(ReadError::ExecError(format!("Index error: {:?}", e)))),
            },
            None => Ok(Err(ReadError::ExecError(
                "Inverted indexer not available".to_string(),
            ))),
        }
    }
}
