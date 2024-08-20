pub mod hash;
pub mod range;

pub trait Partitioner {
    fn partition(&self, key: u64) -> Option<u64>;
}
