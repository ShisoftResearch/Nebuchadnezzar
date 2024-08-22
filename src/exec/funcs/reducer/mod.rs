use std::collections::HashMap;
use std::hash::Hash;

use dovahkiin::types::SharedValue;

use crate::exec::partitioner::Partitioner;
use crate::exec::query;
use crate::exec::query::partitioning::get_hash_partitioner;

pub type PartitioningKeyValuePair<'a> = (u64, SharedValue<'a>);

pub mod group_by;
pub mod join;
pub mod reduce;

pub trait ReduceCollector<K, V> {
    fn reduce_with(&mut self, key: K, value: V);
    fn into_iter(self) -> impl Iterator<Item = (K, impl Iterator<Item = V>)>;
    fn combine<C: ReduceCollector<K, V>>(&mut self, other: C);
    fn merge(&mut self, other: Self);
}

pub struct LocalReduceCollector<K, V> {
    mapping: HashMap<K, Vec<V>>,
}

impl<K: Hash + Eq, V> LocalReduceCollector<K, V> {
    pub fn new() -> Self {
        Self {
            mapping: HashMap::new(),
        }
    }
}

impl<K: Hash + Eq, V> ReduceCollector<K, V> for LocalReduceCollector<K, V> {
    fn reduce_with(&mut self, key: K, value: V) {
        self.mapping
            .entry(key)
            .or_insert_with(|| vec![])
            .push(value);
    }
    fn into_iter(self) -> impl Iterator<Item = (K, impl Iterator<Item = V>)> {
        self.mapping.into_iter().map(|(k, vs)| (k, vs.into_iter()))
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

pub trait Reducer<K, V, RV, O, KF, MF, MV, C>
where
    KF: Fn(&V) -> K,
    C: ReduceCollector<K, RV>,
    MF: Fn(K, Box<dyn Iterator<Item = MV> + '_>) -> O,
    V: Sized + Clone,
{
    fn reduce(&self, data: impl Iterator<Item = V>, key_func: KF, collector: &mut C);
    fn map(&self, collector: C, func: MF) -> impl Iterator<Item = O>;
}

fn get_join_partitioner(
    env: &mut query::env::Environment,
) -> Result<Option<Box<dyn Partitioner>>, String> {
    get_hash_partitioner(env)
}
