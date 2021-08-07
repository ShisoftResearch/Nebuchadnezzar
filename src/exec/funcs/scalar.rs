use super::*;
use std::ops::Rem;

macro_rules! scalar_op {
    ($fn: ident, $in: expr) => {{
        #[inline(always)]
        fn op_fn<T>(input: &[&dyn Array]) -> ArrowResult<ArrayRef>
        where
            T: ArrowNumericType,
            T::Native: StdAdd<Output = T::Native>
                + Sub<Output = T::Native>
                + Mul<Output = T::Native>
                + Div<Output = T::Native>
                + Rem<Output = T::Native>
                + Zero
                + One,
        {
            let mut last_res: Box<dyn Array> = box NullArray::new(input[0].len());
            let mut x = input[0];
            for i in 1..input.len() {
                let y = input[i];
                last_res = box $fn::<T>(as_prim_arr(x)?, as_prim_arr(y)?)?;
                x = &*last_res;
            }
            Ok(last_res.into())
        }
        match $in[0].data_type() {
            DataType::Int8 => op_fn::<Int8Type>($in),
            DataType::Int16 => op_fn::<Int16Type>($in),
            DataType::Int32 => op_fn::<Int32Type>($in),
            DataType::Int64 => op_fn::<Int64Type>($in),
            DataType::UInt8 => op_fn::<UInt8Type>($in),
            DataType::UInt16 => op_fn::<UInt16Type>($in),
            DataType::UInt32 => op_fn::<UInt32Type>($in),
            DataType::UInt64 => op_fn::<UInt64Type>($in),
            DataType::Float32 => op_fn::<Float32Type>($in),
            DataType::Float64 => op_fn::<Float64Type>($in),
            _ => {
                return Result::Err(ArrowError::InvalidArgumentError(format!(
                    "Don't know how to apply scalar op on {:?}",
                    $in
                )))
            }
        }
    }};
}

pub struct Add;
impl Function for Add {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        0
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        scalar_op!(add, data)
    }
}

pub struct Subtract;
impl Function for Subtract {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        0
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        scalar_op!(subtract, data)
    }
}

pub struct Divide;
impl Function for Divide {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        0
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        scalar_op!(divide, data)
    }
}

pub struct Multiply;
impl Function for Multiply {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        0
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        scalar_op!(multiply, data)
    }
}

pub struct Negate;
impl Function for Negate {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        let x = data[0];
        let res: ArrayRef = match x.data_type() {
            DataType::Int8 => Arc::new(negate::<Int8Type>(as_prim_arr(x)?)?),
            DataType::Int16 => Arc::new(negate::<Int16Type>(as_prim_arr(x)?)?),
            DataType::Int32 => Arc::new(negate::<Int32Type>(as_prim_arr(x)?)?),
            DataType::Int64 => Arc::new(negate::<Int64Type>(as_prim_arr(x)?)?),
            DataType::Float32 => Arc::new(negate::<Float32Type>(as_prim_arr(x)?)?),
            DataType::Float64 => Arc::new(negate::<Float64Type>(as_prim_arr(x)?)?),
            _ => {
                return Result::Err(ArrowError::InvalidArgumentError(format!(
                    "Don't know how to apply neg on {:?}",
                    x
                )))
            }
        };
        Ok(res)
    }
}
