use crate::ram::cell::CellHeader;
use lightning::rand;

pub use dovahkiin::types::*;

lazy_static! {
    static ref RAND_GEN: rand::XorRand = rand::XorRand::new(1024);
}

pub trait RandValue {
    fn rand() -> Self;
}

impl RandValue for Id {
    fn rand() -> Self {
        Id::new(RAND_GEN.rand() as u64, RAND_GEN.rand() as u64)
    }
}

pub trait RandId: RandValue {
    fn rand_lower() -> Self;
}

impl RandId for Id {
    fn rand_lower() -> Self {
        Id::new(0, RAND_GEN.rand() as u64)
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
