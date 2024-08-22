use std::marker::PhantomData;

use dovahkiin::expr::serde::Expr;

use crate::exec::{partitioner::Partitioner, query::{self, partitioning::Partitioning}, symbols::objs};

use super::{get_join_partitioner, PartitioningKeyValuePair, ReduceCollector, Reducer};

pub struct Reduce<K, V, O, KF, MF, C> {
    _marker: PhantomData<(K, V, O, KF, MF, C)>,
}

impl<K, V, O, KF, MF, C> Reducer<K, V, V, O, KF, MF, V, C> for Reduce<K, V, O, KF, MF, C>
where
    KF: Fn(&V) -> K,
    C: ReduceCollector<K, V>,
    MF: Fn(K, Box<dyn Iterator<Item = V> + '_>) -> O,
    V: Sized + Clone,
{
    fn reduce(&self, data: impl Iterator<Item = V>, key_func: KF, collector: &mut C) {
        for value in data {
            let key = key_func(&value);
            collector.reduce_with(key, value)
        }
    }

    fn map(&self, collector: C, func: MF) -> impl Iterator<Item = O> {
        let iter = collector.into_iter().map(move |(k, vs)| {
            let vals: Box<dyn Iterator<Item = V>> = Box::new(vs);
            func(k, vals)
        });
        iter
    }
}

impl Partitioning for objs::Reduce {
    fn get_partitioner(
        &self,
        _expr: &Expr,
        env: &mut query::env::Environment,
    ) -> Result<Option<Box<dyn crate::exec::partitioner::Partitioner>>, String> {
        get_join_partitioner(env)
    }

    fn get_partition(
        &self,
        data_ptr: *mut (),
        _env: &mut query::env::Environment,
        partitioner: &Box<dyn Partitioner>,
    ) -> Option<u64> {
        let (key, _) = unsafe { &*(data_ptr as *mut PartitioningKeyValuePair) };
        partitioner.partition(*key)
    }
}