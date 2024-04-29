use super::*;

pub struct JoinBy;
impl Function for JoinBy {
    fn func_type(&self) -> FuncType {
        FuncType::Reduce
    }
    fn reduce(
        &self,
        data: &[&[OwnedValue]],
        option: &OwnedValue,
        reducer: &mut Reducer,
    ) -> Result<(), VecReducerError> {
        let field_ids = match option {
            &OwnedValue::PrimArray(OwnedPrimArray::U64(ref v)) => v,
            _ => return Err(VecReducerError::CannotFindColumn),
        };
        for table in data {
            for row in table.iter() {
                let comp_key =
                    OwnedValue::Array(field_ids.iter().map(|k| row[*k].clone()).collect());
                reducer.reduce_with(comp_key, row.clone());
            }
        }
        Ok(())
    }
}

pub struct NaturalJoin;
impl Function for NaturalJoin {
    fn func_type(&self) -> FuncType {
        FuncType::Reduce
    }
    fn reduce(
        &self,
        data: &[&[OwnedValue]],
        option: &OwnedValue,
        reducer: &mut Reducer,
    ) -> Result<(), VecReducerError> {
        // TODO: Move common_fields to preprocess
        let common_fields = match option {
            &OwnedValue::PrimArray(OwnedPrimArray::U64(ref v)) => v,
            _ => return Err(VecReducerError::ArgumentError),
        };
        // data
        //     .iter()
        //     .map(|t| {
        //         let first_row = &t[0];
        //         if let &OwnedValue::Map(m) = &first_row {
        //             m.map.keys().cloned().collect::<HashSet<u64>>()
        //         } else {
        //             HashSet::new()
        //         }
        //     })
        //     .fold_first(|a, b| a.intersection(&b).cloned().collect())
        //     .map(|s| {
        //         let mut v = s.into_iter().collect::<Vec<_>>();
        //         v.sort();
        //         v
        //     });
        for table in data {
            for row in table.iter() {
                let comp_key =
                    OwnedValue::Array(common_fields.iter().map(|f| row[*f].clone()).collect());
                reducer.reduce_with(comp_key, row.clone());
            }
        }
        Ok(())
    }
}
