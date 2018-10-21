use parking_lot::Mutex;
use std::collections::HashSet;
use std::cmp::Eq;
use std::hash::Hash;

pub struct RIIAMutexTable<K> {
    map: Mutex<HashSet<K>>
}

pub struct RIIAMutexGuard<'a, K>  where K: Eq + Clone + Hash {
    parent: &'a RIIAMutexTable<K>,
    key: K
}

impl <K> RIIAMutexTable<K> where K: Eq + Clone + Hash {
    pub fn new() -> RIIAMutexTable<K> {
        RIIAMutexTable {
            map: Mutex::new(HashSet::new())
        }
    }

    pub fn lock(&self, k: K) -> RIIAMutexGuard<K> {
        loop {
            let mut map_guard = self.map.lock();
            if !map_guard.contains(&k) {
                map_guard.insert(k.clone());
                return RIIAMutexGuard { parent: self, key: k }
            }
        }
    }

    pub fn unlock(&self, key: &K) {
        let mut map_guard = self.map.lock();
        debug_assert!(map_guard.contains(key));
        map_guard.remove(key);
    }
}

impl <'a, K> Drop for RIIAMutexGuard<'a, K> where K: Eq + Clone + Hash {
    fn drop(&mut self) {
        self.parent.unlock(&self.key)
    }
}