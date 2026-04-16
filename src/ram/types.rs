use crate::ram::cell::CellHeader;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use std::cell::RefCell;

pub use dovahkiin::types::*;

thread_local! {
    static RAND_GEN: RefCell<SmallRng> = RefCell::new(SmallRng::from_os_rng());
}

fn with_rand<T>(f: impl FnOnce(&mut SmallRng) -> T) -> T {
    RAND_GEN.with(|rng| f(&mut rng.borrow_mut()))
}

pub trait RandValue {
    fn rand() -> Self;
}

impl RandValue for Id {
    fn rand() -> Self {
        with_rand(|rng| Id::new(rng.next_u64(), rng.next_u64()))
    }
}

pub trait RandId: RandValue {
    fn rand_lower() -> Self;
}

impl RandId for Id {
    fn rand_lower() -> Self {
        with_rand(|rng| Id::new(0, rng.next_u64()))
    }
}

pub trait FromHeader {
    fn from_header(header: &CellHeader) -> Self;
}

impl FromHeader for Id {
    fn from_header(header: &CellHeader) -> Id {
        Id {
            higher: header.partition,
            lower: header.hash,
        }
    }
}

fn flat_scalar_array_elements(value: &OwnedValue) -> Option<Vec<OwnedValue>> {
    let elements: Vec<_> = value.cloned_iter_value()?.collect();
    if elements.iter().any(|element| {
        matches!(
            element,
            OwnedValue::Map(_) | OwnedValue::Array(_) | OwnedValue::PrimArray(_)
        )
    }) {
        return None;
    }
    Some(elements)
}

pub fn index_query_scalars(value: &OwnedValue) -> Option<Vec<OwnedValue>> {
    match value {
        OwnedValue::Map(_) | OwnedValue::Null | OwnedValue::NA => None,
        OwnedValue::Array(_) | OwnedValue::PrimArray(_) => flat_scalar_array_elements(value),
        _ => Some(vec![value.clone()]),
    }
}

pub fn hash_indexable_owned_value(value: &OwnedValue) -> Option<[u8; 8]> {
    let scalars = index_query_scalars(value)?;
    if scalars.len() != 1 {
        return None;
    }
    Some(scalars[0].hash())
}

pub fn values_semantically_equal(left: &OwnedValue, right: &OwnedValue) -> bool {
    if left == right {
        return true;
    }

    let (Some(left_values), Some(right_values)) =
        (index_query_scalars(left), index_query_scalars(right))
    else {
        return false;
    };

    left_values.iter().any(|left_value| {
        right_values
            .iter()
            .any(|right_value| left_value == right_value)
    })
}
