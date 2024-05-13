use crate::ram::cell::{OwnedCell, SharedCell};

use super::Adapter;

pub struct ToOwnedCell<'a> {
    iter: Box<dyn Iterator<Item = SharedCell<'a>>>,
}

impl <'a> Adapter<SharedCell<'a>, OwnedCell, (),> for ToOwnedCell<'a> {
    fn from(input: impl Iterator<Item = SharedCell<'a>> + 'static, _params: ()) -> Result<Self, String> {
        let iter: Box<dyn Iterator<Item = SharedCell<'a>>> = Box::new(input);
        Ok(Self { iter })
    }
}

impl <'a> Iterator for ToOwnedCell<'a> {
    type Item = OwnedCell;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|c| c.to_owned())
    }
}