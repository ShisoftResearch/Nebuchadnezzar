use super::*;

pub struct Limit;
impl Function for Limit {
    fn func_type(&self) -> FuncType {
        FuncType::Terraform
    }
    // Option: num: u32
    fn terraform(&self, data: &ArrayRef, option: &OwnedValue) -> ArrowResult<UInt32Array> {
        let len = data.len();
        let limit = *option.u32().unwrap();
        let bound = if (limit as usize) > len {
            limit
        } else {
            len as u32
        };
        Ok(UInt32Array::from((0..bound).collect::<Vec<_>>()))
    }
}

pub struct SortAsc;
impl Function for SortAsc {
    fn func_type(&self) -> FuncType {
        FuncType::Terraform
    }
    fn terraform(&self, data: &ArrayRef, _option: &OwnedValue) -> ArrowResult<UInt32Array> {
        sort(data, false)
    }
}

pub struct SortDesc;
impl Function for SortDesc {
    fn func_type(&self) -> FuncType {
        FuncType::Terraform
    }
    fn terraform(&self, data: &ArrayRef, _option: &OwnedValue) -> ArrowResult<UInt32Array> {
        sort(data, true)
    }
}

fn sort(values: &ArrayRef, desc: bool) -> ArrowResult<UInt32Array> {
    let opt = SortOptions {
        descending: desc,
        nulls_first: true,
    };
    sort_to_indices(values, Some(opt))
}

pub struct Filter;
impl Function for Filter {
    fn func_type(&self) -> FuncType {
        FuncType::Terraform
    }
    fn terraform(&self, data: &ArrayRef, option: &OwnedValue) -> ArrowResult<UInt32Array> {
        if let &OwnedValue::PrimArray(OwnedPrimArray::Bool(bools)) = &option {
            if bools.len() == data.len() {
                let ids = bools
                    .iter()
                    .enumerate()
                    .filter_map(|(i, b)| if *b { Some(i as u32) } else { None })
                    .collect::<Vec<_>>();
                Ok(UInt32Array::from(ids))
            } else {
                Err(ArrowError::InvalidArgumentError(format!(
                    "Data and cond size does not match {}/{}",
                    bools.len(),
                    data.len()
                )))
            }
        } else {
            Err(ArrowError::InvalidArgumentError(format!(
                "Condition required, got {:?}",
                option
            )))
        }
    }
}
