use std::marker::PhantomData;

use super::Aggregator;

pub struct FindMap<I, O, F> {
    out: Option<O>,
    _marker: PhantomData<(I, F)>
}

impl<I, O, F> Aggregator<I, Option<O>, F, I, Option<O>> for FindMap<I, O, F>
    where F: Fn(&mut Self, I) -> Option<O>
{
    fn collect(&mut self, value: I, func: F) {
        if self.out.is_none() {
            self.out = func(self, value)
        }
    }

    fn fold(&mut self, other: Self) {
        if self.out.is_none() {
            self.out = other.out
        }
    }

    fn finish(self) -> Option<O> {
        self.out
    }
}

pub struct Find<I, F> {
    out: Option<I>,
    _marker: PhantomData<(I, F)>
}

impl<I, F> Aggregator<I, Option<I>, F, &I, bool> for Find<I, F>
    where F: Fn(&mut Self, &I) -> bool
{
    fn collect(&mut self, value: I, func: F) {
        if self.out.is_none() && func(self, &value) {
            self.out = Some(value)
        }
    }

    fn fold(&mut self, other: Self) {
        if self.out.is_none() {
            self.out = other.out
        }
    }

    fn finish(self) -> Option<I> {
        self.out
    }
}