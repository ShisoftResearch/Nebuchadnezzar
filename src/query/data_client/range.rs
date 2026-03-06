use crate::index::{
    entry::{MAX_FEATURE, MIN_FEATURE},
    ranged::tree::{
        btree::Ordering,
        service::{Range, RangeTerm},
    },
    EntryKey, Feature,
};
use dovahkiin::types::{Id, SharedValue};

#[derive(Clone)]
pub struct ValueRange {
    pub start: ValueRangeTerm,
    pub end: ValueRangeTerm,
}

#[derive(Clone)]
pub enum ValueRangeTerm {
    Inclusive(Feature),
    Exclusive(Feature),
    Open,
}

impl ValueRange {
    pub fn to_key_range(self, schema: u32, field: u64, ordering: Ordering) -> Range {
        Range {
            start: match self.start {
                ValueRangeTerm::Inclusive(v) => {
                    RangeTerm::Inclusive(EntryKey::for_schema_field_feature(schema, field, &v))
                }
                ValueRangeTerm::Exclusive(v) => {
                    RangeTerm::Exclusive(EntryKey::from_props(&Id::max_id(), &v, field, schema))
                }
                ValueRangeTerm::Open => RangeTerm::Inclusive(EntryKey::for_schema_field_feature(
                    schema,
                    field,
                    &MIN_FEATURE,
                )),
            },
            end: match self.end {
                ValueRangeTerm::Inclusive(v) => {
                    RangeTerm::Inclusive(EntryKey::from_props(&Id::max_id(), &v, field, schema))
                }
                ValueRangeTerm::Exclusive(v) => {
                    RangeTerm::Exclusive(EntryKey::for_schema_field_feature(schema, field, &v))
                }
                ValueRangeTerm::Open => RangeTerm::Inclusive(EntryKey::from_props(
                    &Id::max_id(),
                    &MAX_FEATURE,
                    field,
                    schema,
                )),
            },
            ordering,
        }
    }
}

impl ValueRangeTerm {
    pub fn inclusive_from(val: &SharedValue) -> Self {
        Self::Inclusive(val.feature())
    }

    pub fn exclusive_from(val: &SharedValue) -> Self {
        Self::Exclusive(val.feature())
    }

    pub fn open() -> Self {
        Self::Open
    }

    pub fn pos_of(&self, slice: &[Feature]) -> Option<usize> {
        match self {
            &ValueRangeTerm::Inclusive(x) | &ValueRangeTerm::Exclusive(x) => {
                Some(slice.binary_search(&x).unwrap_or_else(|p| p))
            }
            _ => None,
        }
    }
}
