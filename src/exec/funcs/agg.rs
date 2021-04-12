use super::*;

pub struct Count;
impl Function for Count {
    fn func_type(&self) -> FuncType {
        FuncType::Aggregate
    }
    fn aggregate(&self, data: &dyn Array) -> ArrowResult<OwnedValue> {
        Ok(OwnedValue::U64(data.len() as u64))
    }
}

pub struct Sum;
impl Function for Sum {
    fn func_type(&self) -> FuncType {
        FuncType::Aggregate
    }
    fn aggregate(&self, data: &dyn Array) -> ArrowResult<OwnedValue> {
        let res = match data.data_type() {
            DataType::Int8 => sum::<Int8Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Int16 => sum::<Int16Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Int32 => sum::<Int32Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Int64 => sum::<Int64Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt8 => sum::<UInt8Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt16 => sum::<UInt16Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt32 => sum::<UInt32Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt64 => sum::<UInt64Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Float32 => sum::<Float32Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Float64 => sum::<Float64Type>(as_prim_arr(data)?).map(|n| n.value()),
            _ => None,
        };
        Ok(res.unwrap_or(OwnedValue::NA))
    }
}

pub struct Max;
impl Function for Max {
    fn func_type(&self) -> FuncType {
        FuncType::Aggregate
    }
    fn aggregate(&self, data: &dyn Array) -> ArrowResult<OwnedValue> {
        let res = match data.data_type() {
            DataType::Int8 => max::<Int8Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Int16 => max::<Int16Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Int32 => max::<Int32Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Int64 => max::<Int64Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt8 => max::<UInt8Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt16 => max::<UInt16Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt32 => max::<UInt32Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt64 => max::<UInt64Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Float32 => max::<Float32Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Float64 => max::<Float64Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Utf8 => {
                max_string(data.as_any().downcast_ref::<StringArray>().unwrap()).map(|n| n.value())
            }
            DataType::LargeUtf8 => {
                max_string(data.as_any().downcast_ref::<LargeStringArray>().unwrap())
                    .map(|n| n.value())
            }
            DataType::Boolean => return Any.aggregate(data),
            _ => None,
        };
        Ok(res.unwrap_or(OwnedValue::NA))
    }
}

pub struct Min;
impl Function for Min {
    fn func_type(&self) -> FuncType {
        FuncType::Aggregate
    }
    fn aggregate(&self, data: &dyn Array) -> ArrowResult<OwnedValue> {
        let res = match data.data_type() {
            DataType::Int8 => min::<Int8Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Int16 => min::<Int16Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Int32 => min::<Int32Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Int64 => min::<Int64Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt8 => min::<UInt8Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt16 => min::<UInt16Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt32 => min::<UInt32Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::UInt64 => min::<UInt64Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Float32 => min::<Float32Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Float64 => min::<Float64Type>(as_prim_arr(data)?).map(|n| n.value()),
            DataType::Utf8 => {
                min_string(data.as_any().downcast_ref::<StringArray>().unwrap()).map(|n| n.value())
            }
            DataType::LargeUtf8 => {
                min_string(data.as_any().downcast_ref::<LargeStringArray>().unwrap())
                    .map(|n| n.value())
            }
            DataType::Boolean => return All.aggregate(data),
            _ => None,
        };
        Ok(res.unwrap_or(OwnedValue::NA))
    }
}

pub struct All;
impl Function for All {
    fn func_type(&self) -> FuncType {
        FuncType::Aggregate
    }
    fn aggregate(&self, data: &dyn Array) -> ArrowResult<OwnedValue> {
        let res = match data.data_type() {
            DataType::Boolean => min_boolean(data.as_any().downcast_ref::<BooleanArray>().unwrap())
                .map(|n| n.value()),
            _ => None,
        };
        Ok(res.unwrap_or(OwnedValue::NA))
    }
}

pub struct Any;
impl Function for Any {
    fn func_type(&self) -> FuncType {
        FuncType::Aggregate
    }
    fn aggregate(&self, data: &dyn Array) -> ArrowResult<OwnedValue> {
        let res = match data.data_type() {
            DataType::Boolean => max_boolean(data.as_any().downcast_ref::<BooleanArray>().unwrap())
                .map(|n| n.value()),
            _ => None,
        };
        Ok(res.unwrap_or(OwnedValue::NA))
    }
}

pub struct Average;
impl Function for Average {
    fn func_type(&self) -> FuncType {
        FuncType::Aggregate
    }
    fn aggregate(&self, data: &dyn Array) -> ArrowResult<OwnedValue> {
        let res = match data.data_type() {
            DataType::Struct(_fields) => {
                let array = data.as_any().downcast_ref::<StructArray>().unwrap();
                if let (Some(sums), Some(counts)) =
                    (array.column_by_name("_s_"), array.column_by_name("_c_"))
                {
                    let sum = Sum::aggregate(&Sum, &**sums)?;
                    let count = Sum::aggregate(&Sum, &**counts)?;
                    OwnedValue::Map(data_map! {s: sum, c: count})
                } else {
                    OwnedValue::NA
                }
            }
            _ => {
                let sum = Sum::aggregate(&Sum, data)?;
                let count = Count::aggregate(&Count, data)?;
                OwnedValue::Map(data_map! {_s_: sum, _c_: count}) // Do the division at exit
            }
        };
        Ok(res)
    }
}
