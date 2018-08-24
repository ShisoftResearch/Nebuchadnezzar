use linked_hash_map::*;
use std::hash::Hash;

pub struct LRUCache<K, V> where K: Clone + Eq + Hash {
    capacity: usize,
    map: LinkedHashMap<K, V>,
    fetch_fn: Box<Fn(&K) -> Option<V>>,
    evict_fn: Box<Fn(K, V)>
}

impl <K, V> LRUCache<K, V> where K: Clone + Eq + Hash {
    pub fn new<FF, EF>(capacity: usize, fetch_fn: FF, evict_fn: EF) -> LRUCache<K, V>
        where FF: Fn(&K) -> Option<V> + 'static,
              EF: Fn(K, V) + 'static
    {
        LRUCache {
            capacity,
            fetch_fn: Box::new(fetch_fn),
            evict_fn: Box::new(evict_fn),
            map: LinkedHashMap::with_capacity(capacity),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.map.insert(key, value);
        self.pop_overflows();
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.map.get_mut(key)
    }

    pub fn update(&mut self, key: &K) -> Option<&mut V> {
        self.remove(key);
        self.get_or_fetch(key)
    }

    pub fn get_or_fetch(&mut self, key: &K) -> Option<&mut V>
    {
        // TODO: one search only
        if self.map.contains_key(key) {
            return self.map.get_refresh(key);
        }
        if let Some(v) = (self.fetch_fn)(key) {
            self.insert(key.clone(), v);
            return self.map.get_mut(key);
        }
        return None;
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.map.remove(key)
    }

    fn pop_overflows(&mut self) {
        while self.map.len() >= self.capacity {
            if let Some((key, value)) = self.map.pop_front() {
                (self.evict_fn)(key, value)
            }
        }
    }
}