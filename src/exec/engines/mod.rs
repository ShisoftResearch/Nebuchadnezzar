use dovahkiin::{types::SharedValue, expr::serde::Expr};

use super::symbols::Symbol;

mod simd;

pub struct CPU {
    socket: u8,
    cores: u8,
    simd: bool,
}

pub struct GPU {
   // To be defined
}

pub struct Remote {
     uri: String
}

pub enum Device {
    CPU(CPU),
    GPU(GPU),
    Remote(Remote)
}

struct Rows<'a> {
    data: Vec<SharedValue<'a>>
}
