use neb::ram::cell;
use std::mem;

#[test]
pub fn size() {
    assert_eq!(mem::size_of::<cell::Header>(), cell::HEADER_SIZE);
}