use bifrost::conshash::ConsistentHashing;
use lightning::aarc::Arc;

use super::Partitioner;

type CHash = Arc<ConsistentHashing>;

pub struct HashPartitioner {
    conshash: CHash,
}

impl Partitioner for HashPartitioner {
    fn partition(&self, key: u64) -> Option<u64> {
        self.conshash.get_server_id(key)
    }
}

pub fn init(params: CHash) -> HashPartitioner {
    let chash = params;
    HashPartitioner { conshash: chash }
}
