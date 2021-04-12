use super::*;

macro_rules! bool_op {
    ($fn: ident, $in: expr) => {{
        let mut last_res: Box<dyn Array> = box NullArray::new($in[0].len());
        let mut x = $in[0];
        for i in 1..$in.len() {
            let y = $in[i];
            last_res = box $fn(as_bool_arr(x)?, as_bool_arr(y)?)?;
            x = &*last_res;
        }
        Ok(last_res.into())
    }};
}

pub struct And;
impl Function for And {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        0
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        bool_op!(and, data)
    }
}

pub struct Or;
impl Function for Or {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        0
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        bool_op!(or, data)
    }
}

pub struct Not;
impl Function for Not {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        not(as_bool_arr(data[0])?).map(|arr| {
            let arr: Arc<dyn Array> = Arc::new(arr);
            arr
        })
    }
}

pub struct AndNot;
impl Function for AndNot {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        2
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        let x = data[0];
        let y = data[1];
        let not_y = Not.map(&[y])?;
        And.map(&[x, &*not_y])
    }
}

pub struct Xor;
impl Function for Xor {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        2
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        let x = data[0];
        let y = data[1];
        let not_x = Not.map(&[x])?;
        let not_y = Not.map(&[y])?;
        let left = And.map(&[x, &*not_y])?;
        let right = And.map(&[&*not_x, y])?;
        Or.map(&[&*left, &*right])
    }
}

pub struct IsNull;
impl Function for IsNull {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        is_null(data[0]).map(|arr| {
            let r: Arc<dyn Array> = Arc::new(arr);
            r
        })
    }
}
pub struct IsNotNull;
impl Function for IsNotNull {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        is_not_null(data[0]).map(|arr| {
            let r: Arc<dyn Array> = Arc::new(arr);
            r
        })
    }
}

pub struct NullIf;
impl Function for NullIf {
    fn func_type(&self) -> FuncType {
        FuncType::Map
    }
    fn num_params(&self) -> usize {
        2
    }
    fn map(&self, data: &[&dyn Array]) -> ArrowResult<ArrayRef> {
        fn op_fn<T: ArrowNumericType>(input: &[&dyn Array]) -> ArrowResult<ArrayRef> {
            let x = input[0];
            let y = input[1];
            nullif(as_prim_arr::<T>(x)?, as_bool_arr(y)?).map(|arr| {
                let r: Arc<dyn Array> = Arc::new(arr);
                r
            })
        }
        match data[0].data_type() {
            DataType::Int8 => op_fn::<Int8Type>(data),
            DataType::Int16 => op_fn::<Int16Type>(data),
            DataType::Int32 => op_fn::<Int32Type>(data),
            DataType::Int64 => op_fn::<Int64Type>(data),
            DataType::UInt8 => op_fn::<UInt8Type>(data),
            DataType::UInt16 => op_fn::<UInt16Type>(data),
            DataType::UInt32 => op_fn::<UInt32Type>(data),
            DataType::UInt64 => op_fn::<UInt64Type>(data),
            DataType::Float32 => op_fn::<Float32Type>(data),
            DataType::Float64 => op_fn::<Float64Type>(data),
            _ => {
                return Result::Err(ArrowError::InvalidArgumentError(format!(
                    "Don't know how to apply null if op on {:?}",
                    data
                )))
            }
        }
    }
}

fn as_bool_arr(data: &dyn Array) -> ArrowResult<&BooleanArray> {
    data.as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ArrowError::InvalidArgumentError("Cannot cast to boolean array".to_owned()))
}
