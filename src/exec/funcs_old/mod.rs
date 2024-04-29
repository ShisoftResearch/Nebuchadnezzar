pub mod agg;
pub mod bool;
pub mod comp;
pub mod data_source;
pub mod reduce;
pub mod scalar;
pub mod terraform;
pub mod vectorization;

pub use arrow::array::{Array, ArrayRef};
pub use arrow::error::Result as ArrowResult;
pub use arrow::{array::*, compute::*, datatypes::*, error::ArrowError};
pub use bifrost::conshash::{CHError, ConsistentHashing};
pub use dovahkiin::types::owned_value::*;
pub use dovahkiin::types::*;
pub use dovahkiin::{data_map, types::owned_value::ToValue};
pub use num::{One, Zero};
pub use std::ops::{Add as StdAdd, Div, Mul, Sub};
pub use std::sync::Arc;

use std::collections::HashMap;

pub enum FuncType {
    Aggregate,
    Map,
    Reduce,
    Terraform,
    DataSource,
    Vectorization,
}

pub enum FunctionError {
    ArrowError(ArrowError),
    ReducerError(VecReducerError),
}

pub enum VecReducerError {
    ColumnNumberError,
    CannotFindColumn,
    ArgumentError,
}

pub struct VectorizationCache(HashMap<u64, ArrayRef>);

pub trait Function: Sync + Send {
    fn map(&self, _data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        unimplemented!()
    }
    fn reduce(
        &self,
        _data: &[&[OwnedValue]],
        _option: &OwnedValue,
        _reducer: &mut Reducer,
    ) -> Result<(), VecReducerError> {
        unimplemented!()
    }
    fn aggregate(&self, _data: &dyn Array) -> ArrowResult<OwnedValue> {
        unimplemented!()
    }
    fn terraform(&self, _data: &ArrayRef, _option: &OwnedValue) -> ArrowResult<UInt32Array> {
        unimplemented!()
    }
    fn data_source(
        &self,
        _options: &OwnedValue,
        _batch_size: usize,
        _servers: &Arc<ConsistentHashing>,
    ) -> Result<Box<dyn DataSource>, String> {
        unimplemented!()
    }
    fn vectorize(
        &self,
        _data_block: &Vec<&OwnedValue>,
        _options: &OwnedValue,
        _cache: &mut VectorizationCache,
    ) -> ArrowResult<ArrayRef> {
        unimplemented!()
    }
    fn func_type(&self) -> FuncType;
    fn num_params(&self) -> usize {
        1
    } // 0 for unlimited
}

// A thread local reducer
pub struct Reducer {
    map: HashMap<OwnedValue, Vec<OwnedValue>>,
}

impl Reducer {
    pub fn new() -> Self {
        Self {
            map: HashMap::with_capacity(128),
        }
    }

    pub fn reduce_with(&mut self, key: OwnedValue, value: OwnedValue) {
        self.map.entry(key).or_insert_with(|| vec![]).push(value);
    }
    pub fn merge(&mut self, other: Self) {
        for (key, mut value) in other.map {
            self.map
                .entry(key)
                .and_modify(|v| v.append(&mut value))
                .or_insert_with(|| value);
        }
    }
}

pub fn as_prim_arr<T: ArrowNumericType>(data: &dyn Array) -> ArrowResult<&PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeType,
{
    data.as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| ArrowError::InvalidArgumentError("Cannot cast".to_owned()))
}

pub trait DataSource {
    fn next_batch(&mut self) -> Option<&[OwnedValue]>;
    fn batch_id(&self) -> usize;
    fn affinity(&self) -> u64 {
        // In terms of server id
        return 0;
    }
}
