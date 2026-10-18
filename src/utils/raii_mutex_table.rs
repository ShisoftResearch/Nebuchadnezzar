use crate::utils::sharded::Sharded;
use parking_lot::Mutex;
use std::cmp::Eq;
use std::collections::HashSet;
use std::hash::Hash;

pub struct RAIIMutexTable<K> {
    map: Sharded<Mutex<HashSet<K>>>,
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
            map: Sharded::default(),
        }
    }

    pub fn lock(&self, k: K) -> RAIIMutexGuard<'_, K> {
        loop {
            let mut map_guard = self.map.shard(&k).lock();
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
        let mut map_guard = self.map.shard(key).lock();
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unrelated_mutex_keys_progress_and_owned_keys_stay_exclusive() {
        let table = std::sync::Arc::new(RAIIMutexTable::new());
        let first = 1u64;
        let second = (2..1000).find(|key| !std::ptr::eq(table.map.shard(&first), table.map.shard(key))).unwrap();
        let held = table.map.shard(&first).lock();
        let worker = table.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let guard = worker.lock(second);
            assert!(worker.map.shard(&second).lock().contains(&second));
            drop(guard);
            assert!(!worker.map.shard(&second).lock().contains(&second));
            tx.send(()).unwrap();
        });
        let result = rx.recv_timeout(std::time::Duration::from_secs(2));
        drop(held);
        thread.join().unwrap();
        result.unwrap();
    }
}
