use dovahkiin::types::Id;
use crate::ram::{cell::SharedCell, chunk::Chunks};

use super::Adapter;

pub struct IdCellSelect<'a> {
    chunks: &'a Chunks,
    fields: Vec<u64>,
    iter: Box<dyn Iterator<Item = Id>>,
}

pub struct IdCellSelectParams<'a> {
    fields: Vec<u64>,
    chunks: &'a Chunks,
}

impl<'a> Adapter<Id, SharedCell<'a>, IdCellSelectParams<'a>> for IdCellSelect<'a> {
    fn from(
        input: impl Iterator<Item = Id> + 'static,
        params: IdCellSelectParams<'a>,
    ) -> Result<Self, String> {
        let iter: Box<dyn Iterator<Item = Id>> = Box::new(input);
        let r = Self {
            iter,
            chunks: params.chunks,
            fields: params.fields
        };
        return Ok(r);
    }
}

impl <'a> Iterator for IdCellSelect<'a> {
    type Item = SharedCell<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(id) = self.iter.next() {
                if let Ok(res) = self.chunks.read_selected(&id, &self.fields) {
                    return Some(res);
                }
            } else {
                return None;
            }
        }
    }
}
