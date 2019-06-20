use byteorder::BigEndian;

#[macro_use]
mod macros;
mod cursor;
mod persistent;
pub mod placement;
pub mod service;
mod split;
#[cfg(test)]
mod test;
pub mod tree;
pub mod client;

pub struct RangedIndexer {}
