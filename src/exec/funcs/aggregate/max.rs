use std::cmp::{max, Ord};

use super::Aggregator;

pub struct Max<T> {
    accumlator: T
}

impl <T: Clone + Ord> Aggregator<T, T> for Max<T> {
    fn collect(&mut self, value: T) {
        self.accumlator = max(self.accumlator.clone(), value);
    }

    fn collect_internal(&mut self, internal: T) {
        self.accumlator = max(self.accumlator.clone(), internal);
    }

    fn finish(self) -> Option<T> {
        Some(self.accumlator)
    }
}
