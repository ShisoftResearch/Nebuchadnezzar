use dovahkiin::types::{self, Type};

use super::schema::PTR_ALIGN;

pub mod reader;
pub mod writer;

pub fn align_address(ty_align: usize, addr: usize) -> usize {
    if ty_align == 0 {
        return addr;
    }
    let alignment = ty_align;
    let mask = alignment - 1;
    let misalign = addr & mask;
    if misalign == 0 {
        addr
    } else {
        addr + alignment - misalign
    }
}

pub fn align_address_with_ty(ty: Type, addr: usize) -> usize {
    let ty_align = types::align_of_type(ty);
    align_address(ty_align, addr)
}

pub fn align_ptr_addr(addr: usize) -> usize {
    align_address(PTR_ALIGN, addr)
}
