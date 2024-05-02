use std::marker::PhantomData;

use dovahkiin::types::{referred::OwnedValueRef, OwnedValue};

use super::Aggregator;

pub struct All<F> {
    accumlator: bool, // init true
    _marker: PhantomData<F>
}

impl <F> Aggregator<OwnedValueRef, OwnedValue, F, OwnedValueRef, bool> for All<F>
    where F: Fn(&mut Self, OwnedValueRef) -> bool
{
    fn collect(&mut self, value: OwnedValueRef, func: F) {
        self.accumlator &= func(self, value);
    }

    fn fold(&mut self, other: Self) {
        self.accumlator &= other.accumlator
    }

    fn finish(self) -> Option<OwnedValue> {
        Some(OwnedValue::Bool(self.accumlator))
    }
}