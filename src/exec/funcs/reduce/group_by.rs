use super::*;

pub struct GroupBy;
impl Function for GroupBy {
    fn func_type(&self) -> FuncType {
        FuncType::Reduce
    }
    fn reduce(
        &self,
        data: &[&[OwnedValue]],
        _option: &OwnedValue,
        reducer: &mut Reducer,
    ) -> Result<(), VecReducerError> {
        let base_vals = &data[0];
        let key_vals = &data[1..];
        let comp_keys = {
            // Build keys
            let mut comp_keys = Vec::with_capacity(data.len());
            for i in 0..base_vals.len() {
                let mut comp_key = Vec::with_capacity(key_vals.len());
                for val_col in key_vals {
                    if let Some(val) = val_col.get(i) {
                        comp_key.push(val.clone());
                    } else {
                        return Err(VecReducerError::ColumnNumberError);
                    }
                }
                comp_keys.push(OwnedValue::Array(comp_key));
            }
            comp_keys
        };
        for (i, key) in comp_keys.into_iter().enumerate() {
            reducer.reduce_with(key, base_vals[i].clone())
        }
        Ok(())
    }
    fn num_params(&self) -> usize {
        0
    }
}
