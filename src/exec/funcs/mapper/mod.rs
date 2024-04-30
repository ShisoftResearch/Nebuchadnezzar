use dovahkiin::types::{referred::OwnedValueRef, OwnedValue, SharedValue, ToTypped, Value};

pub trait Mapper<I, O>: Sync + Send {
    fn map(input: &[I]) -> Vec<O>;
}

pub trait ValueMapper<I: Value + ToTypped, O: Value + ToTypped>: Mapper<I, O> {}

pub trait LocalMapper<'a>: ValueMapper<SharedValue<'a>, SharedValue<'a>> {}
pub trait OwnedMapper: ValueMapper<OwnedValueRef, OwnedValue> {}
