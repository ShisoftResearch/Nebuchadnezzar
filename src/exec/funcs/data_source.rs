use super::*;
use std::iter;
pub struct RepeatData {
    data: Vec<OwnedValue>,
}

impl DataSource for RepeatData {
    fn next_batch(&mut self) -> Option<&[OwnedValue]> {
        Some(&self.data)
    }

    fn batch_id(&self) -> usize {
        0
    }
}

pub struct Repeat;
impl Function for Repeat {
    fn func_type(&self) -> FuncType {
        FuncType::DataSource
    }
    fn data_source(
        &self,
        options: &OwnedValue,
        batch_size: usize,
        _servers: &Arc<ConsistentHashing>,
    ) -> Result<Box<dyn DataSource>, String> {
        Ok(Box::new(RepeatData {
            data: iter::repeat_with(|| options.clone())
                .take(batch_size)
                .collect(),
        }))
    }
}

pub struct SourceData {
    data: Vec<OwnedValue>,
    cursor: usize,
    batch_size: usize,
}

impl DataSource for SourceData {
    fn next_batch(&mut self) -> Option<&[OwnedValue]> {
        if self.cursor >= self.data.len() {
            return None;
        }
        let data = &self.data[self.cursor..(self.cursor + self.batch_size)];
        self.cursor += self.batch_size;
        Some(data)
    }

    fn batch_id(&self) -> usize {
        self.cursor / self.batch_size
    }
}

pub struct MakeSource;
impl Function for MakeSource {
    fn func_type(&self) -> FuncType {
        FuncType::DataSource
    }
    fn data_source(
        &self,
        options: &OwnedValue,
        batch_size: usize,
        _servers: &Arc<ConsistentHashing>,
    ) -> Result<Box<dyn DataSource>, String> {
        if let &OwnedValue::Array(arr) = &options {
            Ok(Box::new(SourceData {
                data: arr.clone(),
                cursor: 0,
                batch_size,
            }))
        } else {
            return Err(format!("Cannot take value {:?} into data source", options));
        }
    }
}

pub struct TreeLookup {}

impl Function for TreeLookup {
    fn func_type(&self) -> FuncType {
        FuncType::DataSource
    }
    fn data_source(
        &self,
        options: &OwnedValue,
        batch_size: usize,
        _servers: &Arc<ConsistentHashing>,
    ) -> Result<Box<dyn DataSource>, String> {
        if let &OwnedValue::Array(arr) = &options {
            Ok(Box::new(SourceData {
                data: arr.clone(),
                cursor: 0,
                batch_size,
            }))
        } else {
            return Err(format!("Cannot take value {:?} into data source", options));
        }
    }
}
