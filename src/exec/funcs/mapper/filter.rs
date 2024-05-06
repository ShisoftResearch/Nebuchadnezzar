use std::marker::PhantomData;

use super::Mapper;

pub struct Filter<I, F> {
    _marker: PhantomData<(I, F)>,
}

impl<I, F> Mapper<I, I, F, &'_ I, bool> for Filter<I, F>
where
    F: Fn(&'_ I) -> bool,
{
    fn map(&self, input: impl Iterator<Item = I>, func: F) -> impl Iterator<Item = I> {
        input.filter(func)
    }
}
