use std::cmp::{max, Ord};

use super::{Aggregator, NoOp};

pub struct Max<T> {
    accumlator: T,
}

impl<T: Clone + Ord> Aggregator<T, T, NoOp<Self>, (), ()> for Max<T> {
    fn collect(&mut self, value: T, _f: NoOp<Self>) {
        self.accumlator = max(self.accumlator.clone(), value);
    }

    fn fold(&mut self, other: Self) {
        self.accumlator = max(self.accumlator.clone(), other.accumlator)
    }

    fn finish(self) -> T {
        self.accumlator
    }
}
