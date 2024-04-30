use std::marker::PhantomData;

use dovahkiin::types::OwnedValue;

use super::Aggregator;

pub struct Count<T> {
    accumlator: u64,
    _marker: PhantomData<T>
}

impl <T> Aggregator<T, OwnedValue> for Count<T> {
    fn collect(&mut self, _value: T) {
        self.accumlator += 1;
    }

    fn collect_internal(&mut self, internal: OwnedValue) {
        self.accumlator += internal.u64().unwrap()
    }

    fn finish(self) -> Option<OwnedValue> {
        Some(OwnedValue::U64(self.accumlator))
    }
}
