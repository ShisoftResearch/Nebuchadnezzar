use std::cmp::{min, Ord};

use super::Aggregator;

pub struct Min<T> {
    accumlator: T
}

impl <T: Clone + Ord> Aggregator<T, T> for Min<T> {
    fn collect(&mut self, value: T) {
        self.accumlator = min(self.accumlator.clone(), value);
    }

    fn collect_internal(&mut self, internal: T) {
        self.accumlator = min(self.accumlator.clone(), internal);
    }

    fn finish(self) -> Option<T> {
        Some(self.accumlator)
    }
}
