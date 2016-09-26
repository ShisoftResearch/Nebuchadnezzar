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
    );
}

macro_rules! rc {
    (
        $read:ident
    ) => (
        {|pos| -> $read(pos)}
    );
}

macro_rules! wc {
    (
        $rwrite:ident
    ) => (
        {|pos, val| -> $write(pos, val)}
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
    ["char"], 1, rc!(read_char), wc!(write_cahr), size_of!(char)
);