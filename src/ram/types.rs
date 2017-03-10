use libc;
use std::cmp::PartialEq;
use std::string::String;
use std::any::Any;
use std::collections::hash_map;

use serde::{Serialize, Deserialize, Serializer, Deserializer};

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
                    pub fn size(_: usize) -> usize {
                        mem::size_of::<$t>()
                    }
                    pub fn val_size(_: $t) -> usize {
                        size(0)
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
                    pub fn size(_: usize) -> usize {
                        $size
                    }
                    pub fn val_size(_: &$t) -> usize {
                        size(0)
                    }
                }
            )*
    );
}

macro_rules! gen_variable_types_io {
    (
        $($t:ident, $tmod:ident, $reader:expr, $writer: expr, $size:expr, $val_size:expr);*
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
                    pub fn val_size(val: &$t) -> usize {
                        let size = $val_size;
                        size(val)
                    }
                }
            )*
    );
}

macro_rules! get_from_val {
    (true, $e:ident, $d:ident) => (
        match $d {
            &Value::$e(ref v) => Some(v),
            _ => None
        }
    );
    (false, $e:ident, $d:ident) => (
        match $d {
            &Value::$e(v) => Some(v),
            _ => None
        }
    )
}

macro_rules! get_from_val_fn {
    (true, $e:ident, $t:ty) => (
        pub fn $e(&self) -> Option<&$t> {
            get_from_val!(true, $e, self)
        }
    );
    (false, $e:ident, $t:ty) => (
        pub fn $e(&self) -> Option<$t> {
            get_from_val!(false, $e, self)
        }
    )
}

pub type Map<K, V> = hash_map::HashMap<K, V>;

macro_rules! define_types {
    (
        $(
            [ $( $name:expr ),* ], $id:expr, $t:ty, $e:ident, $r:ident, $io:ident
         );*
    ) => (

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub enum Value {
            $(
                $e($t),
            )*
            Map(Map<String, Value>),
            Array(Vec<Value>),
            NA,
            Null
        }
        impl Value {
            $(
                get_from_val_fn!($r, $e, $t);
            )*
            pub fn Map(&self) -> Option<&Map<String, Value>> {
                match self {
                    &Value::Map(ref m) => Some(m),
                    _ => None
                }
            }
        }
        pub fn get_type_id (name: String) -> u32 {
           match name.as_ref() {
                $(
                    $($name => $id,)*
                )*
                _ => 0,
           }
        }
        pub fn get_id_type (id: u32) -> &'static str {
           match id {
                $(
                    $id => [$($name),*][0],
                )*
                _ => "N/A",
           }
        }
        pub fn get_size (id: u32, mem_ptr: usize) -> usize {
           match id {
                $(
                    $id => $io::size(mem_ptr),
                )*
                _ => 0,
           }
        }
        pub fn get_val (id:u32, mem_ptr: usize) -> Value {
             match id {
                 $(
                     $id => Value::$e($io::read(mem_ptr)),
                 )*
                 _ => Value::NA,
             }
        }
        pub fn set_val (id:u32, val: &Value, mem_ptr: usize) {
             match id {
                 $(
                     $id => $io::write(get_from_val!($r, $e, val).unwrap() , mem_ptr),
                 )*
                 _ => (),
             }
        }
        pub fn get_vsize (id: u32, val: &Value) -> usize {
            match id {
                $(
                    $id => {
                        let val_opt = get_from_val!($r, $e, val);
                        if val_opt.is_none() {
                            panic!("value does not match id");
                        } else {
                            $io::val_size(val_opt.unwrap())
                        }
                    },
                )*
                _ => {panic!("type id does not found");},
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct pos2d32 {
    pub x: f32,
    pub y: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct pos2d64 {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct pos3d32 {
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct pos3d64 {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct id {
    pub higher: u64,
    pub lower:  u64,
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

impl PartialEq for id {
    fn eq(&self, other: &id) -> bool {
        self.higher == other.higher && self.lower == other.lower
    }
    fn ne(&self, other: &id) -> bool {
        self.higher != other.higher || self.lower != other.lower
    }
}

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

    id, id_io, {
        |mem_ptr| {
            let higher = u64_io::read(mem_ptr);
            let lower =  u64_io::read(mem_ptr + u64_io::size(0));
            id {higher: higher, lower: lower}
        }
    }, {
        |val: &id, mem_ptr| {
            u64_io::write(val.higher, mem_ptr);
            u64_io::write(val.lower,  mem_ptr + u64_io::size(0));
        }
    }, {
        u64_io::size(0) * 2
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
    }, {
        |val: &String| {
            val.as_bytes().len() + u32_io::size(0)
        }
    }
);

define_types!(
    ["bool", "bit"], 1, bool                           ,Bool     ,false ,  bool_io       ;
    ["char"], 2, char                                  ,Char     ,false ,  char_io       ;
    ["i8"], 3, i8                                      ,I8       ,false ,  i8_io         ;
    ["i16", "int"], 4, i16                             ,I16      ,false ,  i16_io        ;
    ["i32", "long"], 5, i32                            ,I32      ,false ,  i32_io        ;
    ["i64", "longlong"], 6, i64                        ,I64      ,false ,  i64_io        ;
    ["u8", "byte"], 7, u8                              ,U8       ,false ,  u8_io         ;
    ["u16"], 8, u16                                    ,U16      ,false ,  u16_io        ;
    ["u32"], 9, u32                                    ,U32      ,false ,  u32_io        ;
    ["u64"], 10, u64                                   ,U64      ,false ,  u64_io        ;
    ["isize"], 11, isize                               ,Isize    ,false ,  isize_io      ;
    ["usize"], 12, usize                               ,Usize    ,false ,  usize_io      ;
    ["f32", "float"], 13, f32                          ,F32      ,false ,  f32_io        ;
    ["f64", "double"], 14, f64                         ,F64      ,false ,  f64_io        ;
    ["pos2d32", "pos2d", "pos", "pos32"], 15, pos2d32  ,Pos2d32  ,true  ,  pos2d32_io    ;
    ["pos2d64", "pos64"], 16, pos2d64                  ,Pos2d64  ,true  ,  pos2d64_io    ;
    ["pos3d32", "pos3d"], 17, pos3d32                  ,Pos3d32  ,true  ,  pos3d32_io    ;
    ["pos3d64"], 18, pos3d64                           ,Pos3d64  ,true  ,  pos3d64_io    ;
    ["id"], 19, id                                     ,Id       ,true  ,  id_io         ;
    ["string", "str"], 20, String                      ,String   ,true  ,  string_io
);

pub const ARRAY_LEN_TYPE_ID: u32 = 8; //u16
pub const NULL_TYPE_ID: u32 = 7; //u8