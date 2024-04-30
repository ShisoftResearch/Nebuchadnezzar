use std::ops::Add;

use super::Aggregator;

pub struct Sum<T> {
    accumlator: T
}

impl <T: Clone + Add<Output = T>> Aggregator<T, T> for Sum<T> {
    fn collect(&mut self, value: T) {
        self.accumlator = self.accumlator.clone() + value;
    }

    fn collect_internal(&mut self, internal: T) {
        self.accumlator = self.accumlator.clone() + internal;
    }

    fn finish(self) -> Option<T> {
        Some(self.accumlator)
    }
}
