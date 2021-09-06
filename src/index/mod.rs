#[macro_use]
mod macros;
#[macro_use]
pub mod builder;
pub mod entry;
pub mod hash;
pub mod ranged;

pub const FEATURE_SIZE: usize = 8;
pub const KEY_SIZE: usize = ID_SIZE + FEATURE_SIZE + 8; // 8 is the estimate length of: schema id u32 (4) + field id u32(4, reduced from u64)
pub const SCHEMA_SCAN_PATT_SIZE: u8 = (FEATURE_SIZE + 8) as u8;
pub const MAX_KEY_SIZE: usize = KEY_SIZE * 2;

use std::sync::Arc;

use bifrost::rpc::RPCError;
use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient};
pub use entry::EntryKey;
pub use entry::ID_SIZE;
use futures::Future;
use smallvec::SmallVec;

use self::ranged::client::cursor::ClientCursor;
use self::ranged::client::RangedQueryClient;
use self::ranged::lsm::btree::Ordering;

pub type Feature = [u8; FEATURE_SIZE];

pub struct IndexerClients {
    ranged_client: Arc<RangedQueryClient>,
}

impl IndexerClients {
    pub fn new(conshash: &Arc<ConsistentHashing>, raft_client: &Arc<RaftClient>) -> Self {
        IndexerClients {
            ranged_client: Arc::new(RangedQueryClient::new(conshash, raft_client)),
        }
    }
    pub fn range_seek<'a>(
        &'a self,
        key: &'a EntryKey,
        ordering: Ordering,
        buffer_size: u16,
        pattern: Option<u8>,
    ) -> impl Future<Output = Result<Option<ClientCursor>, RPCError>> + 'a {
        let pattern = pattern.map(|n| {
            let remain = KEY_SIZE - (n as usize);
            key.as_slice()[remain..].to_vec()
        });
        RangedQueryClient::seek(&self.ranged_client, key, ordering, buffer_size, pattern)
    }
}
