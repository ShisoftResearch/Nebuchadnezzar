use std::marker::PhantomData;

use super::Mapper;

pub struct FilterMap<I, O, F> {
    _marker: PhantomData<(I, O, F)>,
}

impl<I, O, F> Mapper<I, O, F, I, Option<O>> for FilterMap<I, O, F>
where
    F: Fn(I) -> Option<O>,
{
    fn map(&self, input: impl Iterator<Item = I>, func: F) -> impl Iterator<Item = O> {
        input.filter_map(func)
    }
}
