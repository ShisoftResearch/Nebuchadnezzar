use std::marker::PhantomData;

use super::{ReduceCollector, Reducer};

pub struct GroupBy<K, V, O, KF, MF, C> {
    _marker: PhantomData<(K, V, O, KF, MF, C)>,
}

// Just carry out the trait implementation
impl<K, V, O, KF, MF, C> Reducer<K, V, V, O, KF, MF, V, C> for GroupBy<K, V, O, KF, MF, C>
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
            let val_iter: Box<dyn Iterator<Item = V>> = Box::new(vs);
            func(k, val_iter)
        });
        iter
    }
}
