use std::collections::HashMap;
use std::ptr::{read, write};
use std::mem;
use std::string;
use libc;

macro_rules! gen_primitive_types_io {
    (
        $($t:ty : $r:ident , $w:ident);*
    ) => (
            $(
                fn $r (mem_ptr: usize) -> $t {
                    unsafe {
                        read(mem_ptr as *mut $t)
                    }
                }
                fn $w (mem_ptr: usize, val: $t) {
                    unsafe {
                        write(mem_ptr as *mut $t, val)
                    }
                }
            )*
    );
}

macro_rules! define_types {
    (
        $(
            [ $( $name:expr ),* ], $id:expr, $reader:expr, $writer:expr, $size:expr
         );*
    ) => (
        fn get_type_id (name: String) -> i32 {
           match name.as_ref() {
                $(
                    $($name => $id,)*
                )*
                _ => -1,
           }
        }
        fn get_id_type (id: i32) -> &'static str {
           match id {
                $(
                    $id => [$($name),*][0],
                )*
                _ => "N/A",
           }
        }
        fn get_size (id: i32) -> usize {
           match id {
                $(
                    $id => $size,
                )*
                _ => 0,
           }
        }

    );
}

macro_rules! rc {
    (
        $read:ident
    ) => (
        $read
    );
}

macro_rules! wc {
    (
        $rwrite:ident
    ) => (
        $rwrite
    );
}

macro_rules! size_of {
    (
        $t:ty
    ) => (
        {mem::size_of::<$t>()}
    );
}

gen_primitive_types_io!(
    bool:   read_bool,  write_bool;
    char:   read_char,  write_char;
    i8:     read_i8,    write_i8;
    i16:    read_i16,   write_i16;
    i32:    read_i32,   write_i32;
    i64:    read_i64,   write_i64;
    u8:     read_u8,    write_u8;
    u16:    read_u16,   write_u16;
    u32:    read_u32,   write_u32;
    u64:    read_u64,   write_u64;
    isize:  read_isize, write_isize;
    usize:  read_usize, write_usize;
    f32:    read_f32,   write_f32;
    f64:    read_f64,   write_f64
);

define_types!(
    ["bool", "bit"], 0, rc!(read_bool), wc!(write_bool), size_of!(bool);
    ["char"], 1, rc!(read_char), wc!(write_cahr), size_of!(char);
    ["i8"], 2, rc!(read_i8), wc!(write_i8), size_of!(i8);
    ["i16", "int"], 3, rc!(read_i16), wc!(write_i16), size_of!(i16);
    ["i32", "long"], 4, rc!(read_i32), wc!(write_i32), size_of!(i32);
    ["i64", "longlong"], 5, rc!(read_i16), wc!(write_i16), size_of!(i16);
    ["u8", "byte"], 6, rc!(read_u8), wc!(write_u8), size_of!(u8);
    ["u16"], 7, rc!(read_u16), wc!(write_u16), size_of!(u16);
    ["u32"], 8, rc!(read_u32), wc!(write_u32), size_of!(u32);
    ["u64"], 9, rc!(read_u64), wc!(write_u64), size_of!(u64);
    ["isize"], 10, rc!(read_isize), wc!(write_isize), size_of!(isize);
    ["usize"], 11, rc!(read_usize), wc!(write_usize), size_of!(usize);
    ["f32", "float"], 12, rc!(read_f32), wc!(write_f32), size_of!(f32);
    ["f64", "double"], 13, rc!(read_f64), wc!(write_f64), size_of!(f64)
);