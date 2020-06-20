use crate::ram::cell::CellHeader;
use crate::utils::rand;

pub use dovahkiin::types::*;

pub trait RandValue {
    fn rand() -> Self;
}

impl RandValue for Id {
    fn rand() -> Self {
        let (hi, lw) = rand::next_two();
        Id::new(hi, lw)
    }
}

pub trait FromHeader {
    fn from_header(header: &CellHeader) -> Self;
}

impl FromHeader for Id {
    fn from_header(header: &CellHeader) -> Id {
        Id {
            higher: header.partition,
            lower: header.hash,
        }
    }
}
