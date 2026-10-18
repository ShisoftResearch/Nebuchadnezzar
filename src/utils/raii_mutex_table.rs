use lightning::map::{Map, PtrHashMap};
use std::cmp::Eq;
use std::hash::Hash;

pub struct RAIIMutexTable<K: Eq + Clone + Hash> {
    map: PtrHashMap<K, bool>,
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
            map: PtrHashMap::with_capacity(128),
        }
    }

    pub fn lock(&self, k: K) -> RAIIMutexGuard<'_, K> {
        loop {
            if !self.map.contains_key(&k) && self.map.try_insert(k.clone(), true).is_none() {
                return RAIIMutexGuard { parent: self, key: k };
            }
            std::hint::spin_loop();
        }
    }

    pub fn unlock(&self, key: &K) {
        debug_assert!(self.map.contains_key(key));
        self.map.remove(key);
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
        let second = 2u64;
        let held = table.lock(first);
        let worker = table.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let guard = worker.lock(second);
            assert!(worker.map.contains_key(&second));
            drop(guard);
            assert!(!worker.map.contains_key(&second));
            tx.send(()).unwrap();
        });
        let result = rx.recv_timeout(std::time::Duration::from_secs(2));
        drop(held);
        thread.join().unwrap();
        result.unwrap();
    }
}
