use std::cmp::{min, Ord};

use super::Aggregator;

pub struct Min<T> {
    accumlator: T
}

impl <T: Clone + Ord> Aggregator<T, T> for Min<T> {
    fn collect(&mut self, value: T) {
        self.accumlator = min(self.accumlator.clone(), value);
    }

    fn fold(&mut self, other: &Self) {
        self.accumlator = min(self.accumlator.clone(), other.accumlator.clone())
    }

    fn finish(self) -> Option<T> {
        Some(self.accumlator)
    }
}
