use dovahkiin::types::{referred::OwnedValueRef, OwnedValue};

use super::Aggregator;

pub struct Any {
    accumlator: bool,
}

impl Aggregator<OwnedValueRef, OwnedValue> for Any {
    fn collect(&mut self, value: OwnedValueRef) {
        self.accumlator |= value.bool().unwrap();
    }

    fn collect_internal(&mut self, internal: OwnedValue) {
        self.accumlator |= internal.bool().unwrap()
    }

    fn finish(self) -> Option<OwnedValue> {
        Some(OwnedValue::Bool(self.accumlator))
    }
}