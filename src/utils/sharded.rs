//! Fixed hash partitions. Callers hold at most one partition guard at a time.

use std::hash::{DefaultHasher, Hash, Hasher};

pub const SHARD_COUNT: usize = 64;

pub struct Sharded<T> {
    shards: [T; SHARD_COUNT],
}

impl<T: Default> Default for Sharded<T> {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| T::default()),
        }
    }
}

impl<T> Sharded<T> {
    pub fn shard<K: Hash + ?Sized>(&self, key: &K) -> &T {
        let mut hash = DefaultHasher::new();
        key.hash(&mut hash);
        &self.shards[hash.finish() as usize % SHARD_COUNT]
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.shards.iter()
    }
}
