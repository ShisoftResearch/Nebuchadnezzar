use dovahkiin::types::{OwnedValue, Value};

pub trait Function: Sync + Send {
    fn compute(input: &[OwnedValue]) -> OwnedValue;
}