use std::collections::HashMap;
use std::hash::Hash;

pub trait ReduceCollector<K, V> {
    fn reduce_with(&mut self, _key: K, _value: V);
}

pub struct LocalReduceCollector<K, V> {
    mapping: HashMap<K, Vec<V>>,
}

impl <K: Hash + Eq, V> LocalReduceCollector<K, V> {
    pub fn new() -> Self {
        Self {
            mapping: HashMap::new(),
        }
    }
}

impl <K: Hash + Eq, V> ReduceCollector<K, V> for LocalReduceCollector<K, V> {
    fn reduce_with(&mut self, key: K, value: V) {
        self.mapping.entry(key).or_insert_with(|| vec![]).push(value);
    } 
}

pub trait Reducer<K, V, O, C: ReduceCollector<K, V>> {
    fn reduce(&self, _data: &[V], _collector: &mut C);
    fn map(&self, key: K, value: V) -> Vec<O>;
}