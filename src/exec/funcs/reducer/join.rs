use std::marker::PhantomData;

use dovahkiin::expr::serde::Expr;
use itertools::Itertools;

use crate::exec::{
    funcs::reducer::PartitioningKeyValuePair,
    partitioner::Partitioner,
    query::{
        self,
        partitioning::{get_hash_partitioner, Partitioning},
    },
    symbols::objs
};

use super::{get_join_partitioner, ReduceCollector, Reducer};

pub struct FullJoin<K, V, O, KF, MF, C> {
    _marker: PhantomData<(K, V, O, KF, MF, C)>,
}

impl<K, V, O, KF, MF, C> Reducer<K, V, V, O, KF, MF, (V, V), C> for FullJoin<K, V, O, KF, MF, C>
where
    KF: Fn(&V) -> K,
    C: ReduceCollector<K, V>,
    MF: Fn(K, Box<dyn Iterator<Item = (V, V)> + '_>) -> O,
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
            let left = vs.collect::<Vec<_>>();
            let right = left.clone();
            let product = left.into_iter().cartesian_product(right.into_iter());
            let val_joined: Box<dyn Iterator<Item = (V, V)>> = Box::new(product);
            func(k, val_joined)
        });
        iter
    }
}

pub struct NaturalJoin<K, V, O, KF, MF, C> {
    _marker: PhantomData<(K, V, O, KF, MF, C)>,
}

impl<K, V, O, KF, MF, C> Reducer<K, V, (usize, V), O, KF, MF, (V, V), C>
    for NaturalJoin<K, V, O, KF, MF, C>
where
    KF: Fn(&V) -> K,
    C: ReduceCollector<K, (usize, V)>,
    MF: Fn(K, Box<dyn Iterator<Item = (V, V)> + '_>) -> O,
    V: Sized + Clone,
{
    fn reduce(&self, data: impl Iterator<Item = V>, key_func: KF, collector: &mut C) {
        for (i, value) in data.enumerate() {
            let key = key_func(&value);
            collector.reduce_with(key, (i, value))
        }
    }

    fn map(&self, collector: C, func: MF) -> impl Iterator<Item = O> {
        let iter = collector.into_iter().filter_map(move |(k, vs)| {
            let groups = vs.sorted_by_key(|(i, _)| *i).group_by(|(i, _)| *i);
            let mut grouped = groups
                .into_iter()
                .map(|(_id, group)| group.map(|(_, v)| v).collect::<Vec<_>>());
            if let (Some(left), Some(right), None) =
                (grouped.next(), grouped.next(), grouped.next())
            // Assert 2-way join, for now
            {
                let product = left.into_iter().cartesian_product(right.into_iter());
                let val_joined: Box<dyn Iterator<Item = (V, V)>> = Box::new(product);
                Some(func(k, val_joined))
            } else {
                None
            }
        });
        iter
    }
}

impl Partitioning for objs::Join {
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

impl Partitioning for objs::NaturalJoin {
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
