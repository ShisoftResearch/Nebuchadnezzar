use linked_hash_map::*;
use std::hash::Hash;

pub struct LRUCache<K, V> where K: Clone + Eq + Hash {
    capacity: usize,
    map: LinkedHashMap<K, V>,
    fetch_fn: Box<Fn(&K) -> Option<V>>
}

impl <K, V> LRUCache<K, V> where K: Clone + Eq + Hash {
    pub fn new<F>(capacity: usize, fetch_fn: F) -> LRUCache<K, V>
        where F: Fn(&K) -> Option<V> + 'static
    {
        LRUCache {
            capacity,
            fetch_fn: Box::new(fetch_fn),
            map: LinkedHashMap::with_capacity(capacity),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.pop_overflows();
        self.map.insert(key, value);
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
        while self.map.len() >= self.capacity && self.map.pop_front().is_some() {}
    }
}