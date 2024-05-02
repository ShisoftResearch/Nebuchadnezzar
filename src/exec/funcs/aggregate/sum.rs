use std::ops::Add;

use super::{Aggregator, NoOp};

pub struct Sum<T> {
    accumlator: T,
}

impl<T: Clone + Add<Output = T>> Aggregator<T, T, NoOp<Self>, (), ()> for Sum<T> {
    fn collect(&mut self, value: T, _fn: NoOp<Self>) {
        self.accumlator = self.accumlator.clone() + value;
    }

    fn fold(&mut self, other: Self) {
        self.accumlator = self.accumlator.clone() + other.accumlator;
    }

    fn finish(self) -> Option<T> {
        Some(self.accumlator)
    }
}
