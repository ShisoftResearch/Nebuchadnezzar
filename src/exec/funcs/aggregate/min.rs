use std::cmp::{min, Ord};

use super::{Aggregator, NoOp};

pub struct Min<T> {
    accumlator: T,
}

impl<T: Clone + Ord> Aggregator<T, T, NoOp<Self>, (), ()>for Min<T> {
    fn collect(&mut self, value: T, _fn: NoOp<Self>) {
        self.accumlator = min(self.accumlator.clone(), value);
    }

    fn fold(&mut self, other: Self) {
        self.accumlator = min(self.accumlator.clone(), other.accumlator)
    }

    fn finish(self) -> Option<T> {
        Some(self.accumlator)
    }
}
