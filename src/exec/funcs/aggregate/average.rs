use std::ops::Add;

use dovahkiin::types::Value;

use super::{Aggregator, NoOp};

pub struct Average<T> {
    accumlator: T,
    count: u64,
}

impl<T: Value + Clone + Add<Output = T>> Aggregator<T, (T, u64), NoOp<Self>, (), ()>
    for Average<T>
{
    fn collect(&mut self, value: T, _fn: NoOp<Self>) {
        self.accumlator = self.accumlator.clone() + value;
        self.count += 1;
    }

    fn fold(&mut self, other: Self) {
        self.accumlator = self.accumlator.clone() + other.accumlator;
        self.count += other.count;
    }

    fn finish(self) -> (T, u64) {
        (self.accumlator, self.count)
    }
}
