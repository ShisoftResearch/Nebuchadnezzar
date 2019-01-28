use std::io::Cursor;
use byteorder::LittleEndian;

pub type RawMemCursor = Cursor<Box<[u8]>>;
pub type Endian = LittleEndian;

#[macro_export]
macro_rules! def_raw_memory_cursor_for_size {
    ($size: expr, $name: ident) => {
        fn $name(addr: usize) -> RawMemCursor {
            unsafe {
                let ptr = addr as *mut [u8; $size];
                Cursor::new(Box::from_raw(ptr as *mut [u8]))
            }
        }
     };
}

pub fn release_cursor(cursor: RawMemCursor) {
    Box::into_raw(cursor.into_inner());
}