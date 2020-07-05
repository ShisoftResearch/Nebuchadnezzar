use crate::ram::cell::CellHeader;
use lightning::rand;

pub use dovahkiin::types::*;

lazy_static! {
    static ref RAND_GEN: rand::XorRand = rand::XorRand::new(1024);
}

pub trait RandValue {
    fn rand() -> Self;
}

impl RandValue for Id {
    fn rand() -> Self {
        Id::new(RAND_GEN.rand() as u64, RAND_GEN.rand() as u64)
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
