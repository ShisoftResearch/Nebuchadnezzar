use std::marker::PhantomData;

use dovahkiin::types::{OwnedValue, Value};

use super::Aggregator;

pub struct Count<F> {
    accumlator: u64,
    _marker: PhantomData<F>
}

impl<T: Value, F> Aggregator<T, OwnedValue, F, T, bool> for Count<F>
    where F: Fn(&mut Self, T) -> bool
{
    fn collect(&mut self, value: T, func: F) {
        if func(self, value) {
            self.accumlator += 1;
        }
    }

    fn fold(&mut self, other: Self) {
        self.accumlator += other.accumlator
    }

    fn finish(self) -> Option<OwnedValue> {
        Some(OwnedValue::U64(self.accumlator))
    }
}
