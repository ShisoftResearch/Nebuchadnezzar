#[macro_use]
mod macros;
#[macro_use]
pub mod builder;
pub mod entry;
pub mod hash;
pub mod ranged;
pub mod vector;

pub const FEATURE_SIZE: usize = 8;
pub const KEY_SIZE: usize = ID_SIZE + FEATURE_SIZE + 8; // 8 is the estimate length of: schema id u32 (4) + field id u32(4, reduced from u64)
pub const SCHEMA_SCAN_PATT_SIZE: u8 = (FEATURE_SIZE + 8) as u8;
pub const MAX_KEY_SIZE: usize = KEY_SIZE * 2;

use std::sync::Arc;

use bifrost::rpc::RPCError;
use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient};
use dovahkiin::types::{Id, OwnedValue};
pub use entry::EntryKey;
pub use entry::ID_SIZE;
use futures::Future;
use hash::{hash_index_schema, HashedIndexClient};

use crate::client::AsyncClient;
use crate::index::vector::{VectorIndexClient, VectorIndexerCore};
use crate::ram::cell::ReadError;

use self::ranged::client::cursor::ClientCursor;
use self::ranged::client::RangedIndexerClient;
use self::ranged::lsm::service::Range;

pub type Feature = [u8; FEATURE_SIZE];

pub struct IndexerClients {
    pub ranged_client: Arc<RangedIndexerClient>,
    pub hashed_client: Arc<HashedIndexClient>,
    pub vector_client: Arc<VectorIndexClient>,
}

impl IndexerClients {
    pub fn new(
        neb_client: &Arc<AsyncClient>,
        conshash: &Arc<ConsistentHashing>,
        raft_client: &Arc<RaftClient>,
    ) -> Self {
        IndexerClients {
            ranged_client: Arc::new(RangedIndexerClient::new(conshash, raft_client)),
            hashed_client: Arc::new(HashedIndexClient::new(neb_client)),
            vector_client: Arc::new(VectorIndexClient::new()),
        }
    }
    pub async fn init_index_schema(neb_client: &Arc<AsyncClient>) {
        let hash_index_schema = hash_index_schema();
        let _ = neb_client.new_schema_with_id(hash_index_schema).await;
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
}
