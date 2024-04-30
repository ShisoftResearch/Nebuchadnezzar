use dovahkiin::types::{OwnedValue, Value};

use super::Aggregator;

pub struct Count {
    accumlator: u64,
}

impl <T: Value> Aggregator<T, OwnedValue> for Count {
    fn collect(&mut self, _value: T) {
        self.accumlator += 1;
    }

    fn fold(&mut self, other: &Self) {
        self.accumlator += other.accumlator
    }


    fn finish(self) -> Option<OwnedValue> {
        Some(OwnedValue::U64(self.accumlator))
    }
}
