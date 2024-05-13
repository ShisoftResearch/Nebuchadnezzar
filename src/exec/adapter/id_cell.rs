use dovahkiin::types::Id;
use crate::ram::{cell::SharedCell, chunk::Chunks};

use super::Adapter;

pub struct IdCell<'a> {
    chunks: &'a Chunks,
    iter: Box<dyn Iterator<Item = Id>>,
}

pub struct IdCellParams<'a> {
    chunks: &'a Chunks,
}

impl<'a> Adapter<Id, SharedCell<'a>, IdCellParams<'a>> for IdCell<'a> {
    fn from(
        input: impl Iterator<Item = Id> + 'static,
        params: IdCellParams<'a>,
    ) -> Result<Self, String> {
        let iter: Box<dyn Iterator<Item = Id>> = Box::new(input);
        let r = Self {
            iter,
            chunks: params.chunks,
        };
        return Ok(r);
    }
}

impl <'a> Iterator for IdCell<'a> {
    type Item = SharedCell<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(id) = self.iter.next() {
                match self.chunks.read_cell(&id) {
                    Ok(res) => return Some(res),
                    Err(e) => error!("Error on reading cell with id {:?}, error {:?}", id, e)
                }
            } else {
                return None;
            }
        }
    }
}
