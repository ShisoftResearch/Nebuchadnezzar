use libc;
use uuid::Uuid;
use std::cmp::PartialEq;
use std::string::String;

macro_rules! gen_primitive_types_io {
    (
        $($t:ty: $tmod:ident);*
    ) => (
            $(
                pub mod $tmod {
                    use std::ptr;
                    use std::mem;
                    pub fn read(mem_ptr: usize) -> $t {
                         unsafe {
                            ptr::read(mem_ptr as *mut $t)
                        }
                    }
                    pub fn write(val: $t, mem_ptr: usize) {
                        unsafe {
                            ptr::write(mem_ptr as *mut $t, val)
                        }
                    }
                    pub fn size(mem_ptr: usize) -> usize {
                        mem::size_of::<$t>()
                    }
                }
            )*
    );
}

macro_rules! gen_compound_types_io {
    (
        $($t:ident, $tmod:ident, $reader:expr, $writer: expr, $size:expr);*
    ) => (
            $(
                pub mod $tmod {
                    use ram::types::*;
                    pub fn read(mem_ptr: usize) -> $t {
                        let read = $reader;
                        read(mem_ptr)
                    }
                    pub fn write(val: &$t, mem_ptr: usize) {
                        let write = $writer;
                        write(val, mem_ptr)
                    }
                    pub fn size(mem_ptr: usize) -> usize {
                        $size
                    }
                }
            )*
    );
}

macro_rules! gen_variable_types_io {
    (
        $($t:ident, $tmod:ident, $reader:expr, $writer: expr, $size:expr);*
    ) => (
            $(
                pub mod $tmod {
                    use ram::types::*;
                    pub fn read(mem_ptr: usize) -> $t {
                        let read = $reader;
                        read(mem_ptr)
                    }
                    pub fn write(val: &$t, mem_ptr: usize) {
                        let write = $writer;
                        write(val, mem_ptr)
                    }
                    pub fn size(mem_ptr: usize) -> usize {
                        let size = $size;
                        size(mem_ptr)
                    }
                }
            )*
    );
}

macro_rules! define_types {
    (
        $(
            [ $( $name:expr ),* ], $id:expr, $t:ty, $io:ident
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
        fn get_size (id: i32, mem_ptr: usize) -> usize {
           match id {
                $(
                    $id => $io::size(mem_ptr),
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

gen_primitive_types_io!(
    bool:   bool_io       ;
    char:   char_io       ;
    i8:     i8_io         ;
    i16:    i16_io        ;
    i32:    i32_io        ;
    i64:    i64_io        ;
    u8:     u8_io         ;
    u16:    u16_io        ;
    u32:    u32_io        ;
    u64:    u64_io        ;
    isize:  isize_io      ;
    usize:  usize_io      ;
    f32:    f32_io        ;
    f64:    f64_io
);

pub struct pos2d32 {
    pub x: f32,
    pub y: f32,
}

pub struct pos2d64 {
    pub x: f64,
    pub y: f64,
}

pub struct pos3d32 {
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

pub struct pos3d64 {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl PartialEq for pos2d32 {
    fn eq(&self, other: &pos2d32) -> bool {
        self.x == other.x && self.y == other.y
    }
    fn ne(&self, other: &pos2d32) -> bool {
        self.x != other.x || self.y != other.y
    }
}

impl PartialEq for pos2d64 {
    fn eq(&self, other: &pos2d64) -> bool {
        self.x == other.x && self.y == other.y
    }
    fn ne(&self, other: &pos2d64) -> bool {
        self.x != other.x || self.y != other.y
    }
}

impl PartialEq for pos3d32 {
    fn eq(&self, other: &pos3d32) -> bool {
        self.x == other.x && self.y == other.y && self.z == other.z
    }
    fn ne(&self, other: &pos3d32) -> bool {
        self.x != other.x || self.y != other.y || self.z != other.z
    }
}

impl PartialEq for pos3d64 {
    fn eq(&self, other: &pos3d64) -> bool {
        self.x == other.x && self.y == other.y && self.z == other.z
    }
    fn ne(&self, other: &pos3d64) -> bool {
        self.x != other.x || self.y != other.y || self.z != other.z
    }
}

pub type uuid_ = Uuid;

gen_compound_types_io! (
    pos2d32, pos2d32_io, {
        |mem_ptr| {
            let x = f32_io::read(mem_ptr);
            let y = f32_io::read(mem_ptr + f32_io::size(0));
            pos2d32 {x: x, y: y}
        }
    }, {
        |val: &pos2d32, mem_ptr| {
            f32_io::write(val.x, mem_ptr);
            f32_io::write(val.y, mem_ptr + f32_io::size(0));
        }
    }, {
        f32_io::size(0) * 2
    };

    pos2d64, pos2d64_io, {
        |mem_ptr| {
            let x = f64_io::read(mem_ptr);
            let y = f64_io::read(mem_ptr + f64_io::size(0));
            pos2d64 {x: x, y: y}
        }
    }, {
        |val: &pos2d64, mem_ptr| {
            f64_io::write(val.x, mem_ptr);
            f64_io::write(val.y, mem_ptr + f64_io::size(0));
        }
    }, {
        f64_io::size(0) * 2
    };

    //////////////////////////////////////////////////////////////

    pos3d32, pos3d32_io, {
        |mem_ptr| {
            let x = f32_io::read(mem_ptr);
            let y = f32_io::read(mem_ptr + f32_io::size(0));
            let z = f32_io::read(mem_ptr + f32_io::size(0) * 2);
            pos3d32 {x: x, y: y, z: z}
        }
    }, {
        |val: &pos3d32, mem_ptr| {
            f32_io::write(val.x, mem_ptr);
            f32_io::write(val.y, mem_ptr + f32_io::size(0));
            f32_io::write(val.z, mem_ptr + f32_io::size(0) * 2);
        }
    }, {
        f32_io::size(0) * 3
    };

    pos3d64, pos3d64_io, {
        |mem_ptr| {
            let x = f64_io::read(mem_ptr);
            let y = f64_io::read(mem_ptr + f64_io::size(0));
            let z = f64_io::read(mem_ptr + f64_io::size(0) * 2);
            pos3d64 {x: x, y: y, z: z}
        }
    }, {
        |val: &pos3d64, mem_ptr| {
            f64_io::write(val.x, mem_ptr);
            f64_io::write(val.y, mem_ptr + f64_io::size(0));
            f64_io::write(val.z, mem_ptr + f64_io::size(0) * 2);
        }
    }, {
        f64_io::size(0) * 3
    };

    //////////////////////////////////////////////////////////

    uuid_, uuid_io, {
        |mem_ptr| {
            use uuid::{UuidBytes, Uuid};
            use std::ptr;
            unsafe {
                Uuid::from_bytes(
                    ptr::read(mem_ptr as *mut &UuidBytes) as &[u8]
                ).unwrap()
            }
        }
    }, {
        use uuid::{UuidBytes, Uuid};
        use std::ptr;
        |val: &uuid_, mem_ptr| {
            unsafe {
                ptr::write(mem_ptr as *mut &UuidBytes, val.as_bytes());
            }
        }
    }, {
        16
    }
);

gen_variable_types_io! (
    String, string_io, {
        use std::ptr;
        |mem_ptr| {
            let len = u32_io::read(mem_ptr) as usize;
            let smem_ptr = mem_ptr + u32_io::size(0);
            let mut bytes = Vec::with_capacity(len);
            for i in 0..len {
                let ptr = smem_ptr + i;
                let b = unsafe {ptr::read(ptr as *mut u8)};
                bytes.push(b);
            }
            String::from_utf8(bytes).unwrap()
        }
    }, {
        use std::ptr;
        |val: &String, mem_ptr| {
            let bytes = val.as_bytes();
            let len = bytes.len();
            u32_io::write(len as u32, mem_ptr);
            let mut smem_ptr = mem_ptr + u32_io::size(0);
            unsafe {
                for b in bytes {
                    ptr::write(smem_ptr as *mut u8, *b);
                    smem_ptr += 1;
                }
            }
        }
    }, {
        |mem_ptr| {
            let str_len = u32_io::read(mem_ptr) as usize;
            str_len + u32_io::size(0)

        }
    }
);

define_types!(
    ["bool", "bit"], 0, bool                            ,  bool_io       ;
    ["char"], 1, char                                   ,  char_io       ;
    ["i8"], 2, i8                                       ,  i8_io         ;
    ["i16", "int"], 3, i16                              ,  i16_io        ;
    ["i32", "long"], 4, i32                             ,  i32_io        ;
    ["i64", "longlong"], 5, i64                         ,  i64_io        ;
    ["u8", "byte"], 6, u8                               ,  u8_io         ;
    ["u16"], 7, u16                                     ,  u16_io        ;
    ["u32"], 8, u32                                     ,  u32_io        ;
    ["u64"], 9, u64                                     ,  u64_io        ;
    ["isize"], 10, isize                                ,  isize_io      ;
    ["usize"], 11, usize                                ,  usize_io      ;
    ["f32", "float"], 12, f32                           ,  f32_io        ;
    ["f64", "double"], 13, f64                          ,  f64_io        ;
    ["pos2d32", "pos2d", "pos", "pos32"], 14, pos2d32   ,  pos2d32_io    ;
    ["pos2d64", "pos64"], 15, pos2d64                   ,  pos2d64_io    ;
    ["pos3d32", "pos3d"], 16, pos3d32                   ,  pos3d32_io    ;
    ["pos3d64"], 17, pos3d64                            ,  pos3d64_io    ;
    ["uuid"], 18, uuid_                                 ,  uuid_io       ;
    ["string", "str"], 19, string::String               ,  string_io
);