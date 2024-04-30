use dovahkiin::types::{referred::OwnedValueRef, OwnedValue};

use super::Aggregator;

pub struct All {
    accumlator: bool,
}

impl Aggregator<OwnedValueRef, OwnedValue> for All {
    fn collect(&mut self, value: OwnedValueRef) {
        self.accumlator &= value.bool().unwrap();
    }

    fn fold(&mut self, other: &Self) {
        self.accumlator &= other.accumlator
    }

    fn finish(self) -> Option<OwnedValue> {
        Some(OwnedValue::Bool(self.accumlator))
    }
}