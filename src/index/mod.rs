#[macro_use]
mod macros;
#[macro_use]
pub mod builder;
pub mod entry;
pub mod hash;
pub mod ranged;

pub const FEATURE_SIZE: usize = 8;
pub const KEY_SIZE: usize = ID_SIZE + FEATURE_SIZE + 8; // 8 is the estimate length of: schema id u32 (4) + field id u32(4, reduced from u64)
pub const MAX_KEY_SIZE: usize = KEY_SIZE * 2;

use std::sync::Arc;

use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient};
pub use entry::EntryKey;
pub use entry::ID_SIZE;

use self::ranged::client::RangedQueryClient;
pub type Feature = [u8; FEATURE_SIZE];

struct IndexerClients {
    ranged_client: RangedQueryClient,
}

impl IndexerClients {
    pub fn new(
      conshash: &Arc<ConsistentHashing>,
      raft_client: &Arc<RaftClient>
    ) -> Self {
        IndexerClients {
            ranged_client: RangedQueryClient::new(conshash, raft_client),
        }
    }
}
