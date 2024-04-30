use std::collections::HashMap;
use std::hash::Hash;

pub mod group_by;

pub trait ReduceCollector<K, V> 
{
    fn reduce_with(&mut self, _key: K, _value: V);
    fn into_iter(self) -> Box<dyn Iterator<Item = (K, Box<dyn Iterator<Item = V>>)>>;
    fn combine<C: ReduceCollector<K, V>>(&mut self, other: C);
    fn merge(&mut self, other: Self);
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

impl <K: Hash + Eq + 'static, V: 'static> ReduceCollector<K, V> for LocalReduceCollector<K, V> 
{
    fn reduce_with(&mut self, key: K, value: V) {
        self.mapping.entry(key).or_insert_with(|| vec![]).push(value);
    } 
    fn into_iter(self) -> Box<dyn Iterator<Item = (K, Box<dyn Iterator<Item = V>>)>> {
        let res = self.mapping.into_iter().map(|(k, vs)| {
            let val_iter: Box<dyn Iterator<Item = V>> = Box::new(vs.into_iter());
            (k, val_iter)
        });
        Box::new(res)
    }
    fn combine<C: ReduceCollector<K, V>>(&mut self, other_collector: C) {
        for (k, vs) in other_collector.into_iter() {
            let vals = self.mapping.entry(k).or_insert_with(|| vec![]);
            for v in vs {
                vals.push(v);
            }
        }
    }
    fn merge(&mut self, other: Self) {
        for (k, mut vs) in other.mapping.into_iter() {
            let vals = self.mapping.entry(k).or_insert_with(|| vec![]);
            vals.append(&mut vs);
        }
    }
}

pub trait Reducer<K, V, O, KF, MF, C> 
    where 
        KF: Fn(&V) -> K, C: ReduceCollector<K, V>,
        MF: Fn(K, Box<dyn Iterator<Item = V>>) -> O,
        V: Clone 
{
    fn reduce(&self, data: &[V], key_func: KF, collector: &mut C) {
        for value in data {
            let key = key_func(value);
            collector.reduce_with(key, value.clone())
        }
    }
    fn map(&self, collector: C, func: MF) -> impl Iterator<Item = O> {
        collector.into_iter().map(move |(k, vs)| {
            func(k, vs)
        })
    }
}