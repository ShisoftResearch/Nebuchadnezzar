use crate::ram::cell::{OwnedCell, OwnedCellRef};

use super::Adapter;

pub struct ReferredCell {
    iter: Box<dyn Iterator<Item = OwnedCell>>,
}

impl <'a> Adapter<OwnedCell, OwnedCellRef, (),> for ReferredCell {
    fn from(input: impl Iterator<Item = OwnedCell> + 'static, _params: ()) -> Result<Self, String> {
        let iter: Box<dyn Iterator<Item = OwnedCell>> = Box::new(input);
        Ok(Self { iter })
    }
}

impl <'a> Iterator for ReferredCell {
    type Item = OwnedCellRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|c| c.into_ref())
    }
}