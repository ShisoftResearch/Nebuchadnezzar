use std::marker::PhantomData;

use super::Mapper;

pub struct Map<I, O, F> {
    _marker: PhantomData<(I, O, F)>
}

impl <I, O, F> Mapper<I, O, F, I, O> for Map<I, O, F>
    where F: Fn(I) -> O 
{
    fn map(&self, data: impl Iterator<Item = I>, func: F) -> impl Iterator<Item = O> {
        data.map(func)
    }
}