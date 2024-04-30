use std::cmp::{max, Ord};

use super::Aggregator;

pub struct Max<T> {
    accumlator: T
}

impl <T: Clone + Ord> Aggregator<T, T> for Max<T> {
    fn collect(&mut self, value: T) {
        self.accumlator = max(self.accumlator.clone(), value);
    }

    fn fold(&mut self, other: &Self) {
        self.accumlator = max(self.accumlator.clone(), other.accumlator.clone())
    }

    fn finish(self) -> Option<T> {
        Some(self.accumlator)
    }
}
