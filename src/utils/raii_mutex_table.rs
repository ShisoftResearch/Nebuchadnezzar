use parking_lot::Mutex;
use std::cmp::Eq;
use std::collections::HashSet;
use std::hash::Hash;

pub struct RAIIMutexTable<K> {
    map: Mutex<HashSet<K>>,
}

pub struct RAIIMutexGuard<'a, K>
where
    K: Eq + Clone + Hash,
{
    parent: &'a RAIIMutexTable<K>,
    key: K,
}

impl<K> RAIIMutexTable<K>
where
    K: Eq + Clone + Hash,
{
    pub fn new() -> RAIIMutexTable<K> {
        RAIIMutexTable {
            map: Mutex::new(HashSet::new()),
        }
    }

    pub fn lock(&self, k: K) -> RAIIMutexGuard<K> {
        loop {
            let mut map_guard = self.map.lock();
            if !map_guard.contains(&k) {
                map_guard.insert(k.clone());
                return RAIIMutexGuard {
                    parent: self,
                    key: k,
                };
            }
        }
    }

    pub fn unlock(&self, key: &K) {
        let mut map_guard = self.map.lock();
        debug_assert!(map_guard.contains(key));
        map_guard.remove(key);
    }
}

impl<'a, K> Drop for RAIIMutexGuard<'a, K>
where
    K: Eq + Clone + Hash,
{
    fn drop(&mut self) {
        self.parent.unlock(&self.key)
    }
}
