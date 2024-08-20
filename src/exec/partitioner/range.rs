use crate::index::Feature;

use super::Partitioner;

pub struct RangePartitionParams {
    start: Feature,
    ends: Feature,
    num_parts: u64,
}

pub struct RangePartitioner {
    start: u64,
    part_size: u64,
    num_parts: u64,
}

impl Partitioner for RangePartitioner {
    fn partition(&self, key: u64) -> Option<u64> {
        let key_num = key;
        if key_num < self.start {
            return None;
        }
        let key_offset = key_num - self.start;
        let part_id = key_offset / self.part_size;
        if part_id >= self.num_parts {
            return None;
        }
        return Some(part_id);
    }
}

pub fn init(params: RangePartitionParams) -> RangePartitioner {
    let start_num = u64::from_le_bytes(params.start);
    let ends_num = u64::from_le_bytes(params.ends);
    let range = ends_num - start_num;
    let part_size = range / params.num_parts;
    RangePartitioner {
        part_size,
        start: start_num,
        num_parts: params.num_parts,
    }
}
