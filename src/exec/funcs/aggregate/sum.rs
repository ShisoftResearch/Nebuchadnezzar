use std::ops::Add;

use super::Aggregator;

pub struct Sum<T> {
    accumlator: T,
}

impl<T: Clone + Add<Output = T>> Aggregator<T, T> for Sum<T> {
    fn collect(&mut self, value: T) {
        self.accumlator = self.accumlator.clone() + value;
    }

    fn fold(&mut self, other: &Self) {
        self.accumlator = self.accumlator.clone() + other.accumlator.clone();
    }

    fn finish(self) -> Option<T> {
        Some(self.accumlator)
    }
}
