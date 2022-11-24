use super::*;

macro_rules! str_cmp_op_fn {
    ($str_vec_fn: ident, $str_scalar_fn: ident) => {
        #[inline(always)]
        fn str_op_fn(input: &[&dyn Array]) -> ArrowResult<ArrayRef> {
            let x = input[0];
            let y = input[1];
            let res: Box<dyn Array>;
            let x_prim = x
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ArrowError::InvalidArgumentError("Cannot cast".to_owned()))?;
            let y_prim = y
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ArrowError::InvalidArgumentError("Cannot cast".to_owned()))?;
            if y.len() == 1 {
                // scalar
                let y = y_prim.value(0);
                res = box $str_scalar_fn(x_prim, y)?;
            } else {
                // vector
                res = box $str_vec_fn(x_prim, y_prim)?;
            }
            Ok(res.into())
        }
    };
}

macro_rules! cmp_op {
    ($vec_fn: ident, $scalar_fn: ident, $str_vec_fn: ident, $str_scalar_fn: ident, $in: expr) => {{
        #[inline(always)]
        fn op_fn<T>(input: &[&dyn Array]) -> ArrowResult<ArrayRef>
        where
            T: ArrowNumericType + ArrowPrimitiveType,
            T::Native: ArrowNativeTypeOp,
        {
            let x = input[0];
            let y = input[1];
            let res: Box<dyn Array>;
            let x_prim = as_prim_arr(x)?;
            let y_prim = as_prim_arr(y)?;
            if y.len() == 1 {
                // scalar
                let y = y_prim.value(0);
                res = box $scalar_fn::<T>(x_prim, y)?;
            } else {
                // vector
                res = box $vec_fn::<T>(x_prim, y_prim)?;
            }
            Ok(res.into())
        }
        str_cmp_op_fn!($str_vec_fn, $str_scalar_fn);
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
            DataType::Utf8 => str_op_fn($in),
            _ => {
                return Result::Err(ArrowError::InvalidArgumentError(format!(
                    "Don't know how to apply scalar op on {:?}",
                    $in
                )))
            }
        }
    }};
}

pub struct Equal;
impl Function for Equal {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        2
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        cmp_op!(eq, eq_scalar, eq_utf8, eq_utf8_scalar, data)
    }
}

pub struct NotEqual;
impl Function for NotEqual {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        2
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        cmp_op!(neq, neq_scalar, neq_utf8, neq_utf8_scalar, data)
    }
}

pub struct Greater;
impl Function for Greater {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        2
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        cmp_op!(gt, gt_scalar, gt_utf8, gt_utf8_scalar, data)
    }
}

pub struct GreaterEqual;
impl Function for GreaterEqual {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        2
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        cmp_op!(gt_eq, gt_eq_scalar, gt_eq_utf8, gt_eq_utf8_scalar, data)
    }
}

pub struct Less;
impl Function for Less {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        2
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        cmp_op!(lt, lt_scalar, lt_utf8, lt_utf8_scalar, data)
    }
}

pub struct LessEqual;
impl Function for LessEqual {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        2
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        cmp_op!(lt_eq, lt_eq_scalar, lt_eq_utf8, lt_eq_utf8_scalar, data)
    }
}

pub struct Like;
impl Function for Like {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        2
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        str_cmp_op_fn!(like_utf8, like_utf8_scalar);
        str_op_fn(data)
    }
}

pub struct NotLike;
impl Function for NotLike {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        2
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        str_cmp_op_fn!(nlike_utf8, nlike_utf8_scalar);
        str_op_fn(data)
    }
}
