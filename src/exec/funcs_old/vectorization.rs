use super::*;

macro_rules! vec_by_key_ref_type {
    ($data: expr, $owned: ident => $builder: ty) => {{
        let data_len = $data.len();
        let mut builder = <$builder>::new();
        $data.iter().for_each(|v| match v.$owned() {
            Some(v) => builder.append_value((*v).into()),
            None => builder.append_null(),
        });
        let array: ArrayRef = Arc::new(builder.finish());
        array
    }};
}

macro_rules! vec_by_key_type {
    ($data: expr, $owned: ident => $builder: ty) => {{
        let data_len = $data.len();
        let mut builder = <$builder>::new();
        $data.iter().for_each(|v| match v.$owned() {
            Some(v) => {
                builder.append_value(v);
            }
            None => {
                builder.append_null();
            }
        });
        let array: ArrayRef = Arc::new(builder.finish());
        array
    }};
}

fn vectorize_col(col: &Vec<&OwnedValue>) -> ArrowResult<ArrayRef> {
    let array = match col[0] {
        OwnedValue::Bool(_) => vec_by_key_ref_type!(col, bool => BooleanBuilder),
        // OwnedValue::Char(_) => vec_by_key_type!(data_block, char => Int32Array),
        OwnedValue::I8(_) => vec_by_key_ref_type!(col, i8 => Int8Builder),
        OwnedValue::I16(_) => vec_by_key_ref_type!(col, i16 => Int16Builder),
        OwnedValue::I32(_) => vec_by_key_ref_type!(col, i32 => Int32Builder),
        OwnedValue::I64(_) => vec_by_key_ref_type!(col, i64 => Int64Builder),
        OwnedValue::U8(_) => vec_by_key_ref_type!(col, u8 => UInt8Builder),
        OwnedValue::U16(_) => vec_by_key_ref_type!(col, u16 => UInt16Builder),
        OwnedValue::U32(_) => vec_by_key_ref_type!(col, u32 => UInt32Builder),
        OwnedValue::U64(_) => vec_by_key_ref_type!(col, u64 => UInt64Builder),
        OwnedValue::F32(_) => vec_by_key_ref_type!(col, f32 => Float32Builder),
        OwnedValue::F64(_) => vec_by_key_ref_type!(col, f64 => Float64Builder),
        OwnedValue::String(_) => vec_by_key_type!(col, string => StringBuilder),
        OwnedValue::Bytes(_) => vec_by_key_type!(col, bytes => BinaryBuilder),
        OwnedValue::SmallBytes(_) => {
            vec_by_key_type!(col, small_bytes => BinaryBuilder)
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Cannot vectorize from value {:?}",
                col[0]
            )))
        }
    };
    Ok(array)
}

pub struct ToVec;
impl Function for ToVec {
    fn func_type(&self) -> FuncType {
        FuncType::Vectorization
    }
    fn vectorize(
        &self,
        data_block: &Vec<&OwnedValue>,
        _options: &OwnedValue,
        _cache: &mut VectorizationCache,
    ) -> ArrowResult<ArrayRef> {
        vectorize_col(data_block)
    }
}

pub struct Col;
impl Function for Col {
    fn func_type(&self) -> FuncType {
        FuncType::Vectorization
    }
    fn vectorize(
        &self,
        data_block: &Vec<&OwnedValue>,
        options: &OwnedValue,
        cache: &mut VectorizationCache,
    ) -> ArrowResult<ArrayRef> {
        if data_block.is_empty() {
            return Err(ArrowError::InvalidArgumentError("data is empty".to_owned()));
        }
        if let Some(key) = options.u64() {
            if let Some(array) = cache.0.get(key) {
                return Ok(array.clone());
            }
            let col = data_block
                .iter()
                .map(|val| match val {
                    &OwnedValue::Map(ref map) => map.get_by_key_id(*key),
                    _ => &NULL_OWNED_VALUE,
                })
                .collect::<Vec<_>>();
            let array = vectorize_col(&col)?;
            cache.0.insert(*key, array.clone());
            return Ok(array);
        } else {
            Err(ArrowError::InvalidArgumentError(format!(
                "Select key is not key but {:?}",
                options
            )))
        }
    }
}
