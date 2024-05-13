use dovahkiin::types::SharedValue;

use crate::ram::cell::SharedCell;

use super::Adapter;

// Shared cell have the lock to the cell, SharedValue can only exists when
// the lock is held, so we cannot just convert SharedCell to SharedValue
// it have to be cloned from a SharedCell reference
pub struct BorrowCellValue<'a> {
    iter: Box<dyn Iterator<Item = &'a SharedCell<'a>>>,
}

impl <'a> Adapter<&'a SharedCell<'a>, SharedValue<'a>, (),> for BorrowCellValue<'a> {
    fn from(input: impl Iterator<Item = &'a SharedCell<'a>> + 'static, _params: ()) -> Result<Self, String> {
        let iter: Box<dyn Iterator<Item = &'a SharedCell<'a>>> = Box::new(input);
        Ok(Self { iter })
    }
}

impl <'a> Iterator for BorrowCellValue<'a> {
    type Item = SharedValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|c| {
            // To the down stream the SharedValue have to be owned, so a clone is justified
           c.data.clone()
        })
    }
}