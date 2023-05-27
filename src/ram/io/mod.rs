use super::schema::PTR_ALIGN;

pub mod reader;
pub mod writer;

pub fn align_address(ty_align: usize, addr: usize) -> usize {
    let alignment = ty_align;
    let mask = alignment - 1;
    let misalign = addr & mask;
    if misalign == 0 {
        addr
    } else {
        addr + alignment - misalign
    }
}

pub fn align_ptr_addr(addr: usize) -> usize {
    align_address(PTR_ALIGN, addr)
}