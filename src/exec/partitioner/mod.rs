pub mod hash;
pub mod range;

pub trait Partitioner<K, P, I> {
    fn init(params: I) -> Self;
    fn partition(&self, key: &K) -> Option<P>;
}