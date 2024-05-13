use std::sync::Arc;

use bifrost::conshash::ConsistentHashing;

use crate::index::Feature;

use super::Partitioner;

type CHash = Arc<ConsistentHashing>;

pub struct RangePartitionParams {
    start: Feature,
    ends: Feature,
    num_parts: u64,
    chash: CHash
}

pub struct RangePartitioner {
    start: u64,
    part_size: u64,
    num_parts: u64,
    conshash: CHash
}

impl Partitioner<Feature, u64, RangePartitionParams> for RangePartitioner {
    fn init(params: RangePartitionParams) -> Self {
        let start_num = u64::from_le_bytes(params.start);
        let ends_num = u64::from_le_bytes(params.ends);
        let range = ends_num - start_num;
        let part_size = range / params.num_parts;
        Self {
            part_size,
            start: start_num,
            conshash: params.chash,
            num_parts: params.num_parts
        }
    }

    fn partition(&self, key: &Feature) -> Option<u64> {
        let num_servers = self.conshash.server_count();
        let key_num = u64::from_le_bytes(key.clone());
        if key_num < self.start {
            return None;
        }
        let key_offset = key_num - self.start;
        let part_id = key_offset / self.part_size;
        if part_id >= self.num_parts {
            return None;
        }
        return Some(part_id)
    }
}