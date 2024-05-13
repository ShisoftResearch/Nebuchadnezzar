use bifrost::conshash::ConsistentHashing;
use dovahkiin::types::Id;
use lightning::aarc::Arc;

use super::Partitioner;

type CHash = Arc<ConsistentHashing>;

pub struct HashPartitioner {
    conshash: CHash
}

impl Partitioner<u64, u64, &CHash> for HashPartitioner {
    fn init(params: &CHash) -> Self {
        let chash = params.clone();
        Self {
            conshash: chash
        }
    }

    fn partition(&self, key: &u64) -> Option<u64> {
        self.conshash.get_server_id(*key)
    }
}

impl Partitioner<Id, u64, &CHash> for HashPartitioner {
    fn init(params: &CHash) -> Self {
        let chash = params.clone();
        Self {
            conshash: chash
        }
    }

    fn partition(&self, key: &Id) -> Option<u64> {
        self.conshash.get_server_id(key.higher)
    }
}