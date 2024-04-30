use dovahkiin::types::{referred::OwnedValueRef, OwnedValue, SharedValue, ToTypped, Value};

use super::Function;

pub trait Mapper<I, O>: Function<I, O> + Sync + Send {}

pub trait ValueMapper<I: Value + ToTypped, O: Value + ToTypped>: Mapper<I, O> {}

pub trait LocalMapper<'a>: ValueMapper<SharedValue<'a>, SharedValue<'a>> {}
pub trait OwnedMapper: ValueMapper<OwnedValueRef, OwnedValue> {}
