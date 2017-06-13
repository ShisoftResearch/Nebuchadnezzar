use ram::cell::Header;
use std::cmp::PartialEq;
use std::string::String;
use std::collections::hash_map;
use std::cmp::Ordering;
use utils::rand;
use bifrost_hasher::hash_str;

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

macro_rules! define_types {
    (
        $(
            [ $( $name:expr ),* ], $id:expr, $t:ty, $e:ident, $r:ident, $io:ident
         );*
    ) => (

        #[derive(Copy, Clone)]
        pub enum TypeId {
            $(
                $e = $id,
            )*
            Map = 0 // No matter which id we pick for 'Map' because w/r planners will ignore it when sub_fields is not 'None'
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub enum Value {
            $(
                $e($t),
            )*
            Map(DataMap),
            Array(Vec<Value>),
            NA,
            Null
        }
        impl Value {
            $(
                get_from_val_fn!($r, $e, $t);
            )*
            pub fn Map(&self) -> Option<&DataMap> {
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
pub struct Pos2d32 {
    pub x: f32,
    pub y: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pos2d64 {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pos3d32 {
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pos3d64 {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, Hash)]
pub struct Id {
    pub higher: u64,
    pub lower:  u64,
}

pub fn key_hash(key: &String) -> u64 {
    hash_str(&key)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Map<V> {
    map: hash_map::HashMap<u64, V>,
    pub fields: Vec<String>
}

impl <V>Map<V> {
    pub fn new() -> Map<V> {
        Map {
            map: hash_map::HashMap::new(),
            fields: Vec::new()
        }
    }
    pub fn from_hash_map(map: hash_map::HashMap<String, V>) -> Map<V> {
        let mut target_map = hash_map::HashMap::new();
        let fields = map.keys().cloned().collect();
        for (key, value) in map {
            target_map.insert(key_hash(&key), value);
        }
        Map {
            map: target_map,
            fields: fields
        }
    }
    pub fn insert(&mut self, key: &String, value: V) -> Option<V> {
        self.fields.push(key.clone());
        self.insert_key_id(key_hash(key), value)
    }
    pub fn insert_key_id(&mut self, key: u64, value: V) -> Option<V> {
        self.map.insert(key, value)
    }
    pub fn get_key_id(&self, key: u64) -> Option<&V> {
        self.map.get(&key)
    }
    pub fn get(&self, key: &String) -> Option<&V> {
        self.get_key_id(key_hash(key))
    }
    pub fn get_static_key(&self, key: &'static str) -> Option<&V> {
        self.get(&String::from(key))
    }
    pub fn into_string_map(self) -> hash_map::HashMap<String, V> {
        let mut id_map: hash_map::HashMap<u64, String> =
            self.fields
                .into_iter()
                .map(|field| (hash_str(&field), field))
                .collect();
        self.map
            .into_iter()
            .map(|(fid, value)| (id_map.remove(&fid), value))
            .filter(|&(ref field, _)| field.is_some())
            .map(|(field, value)| (field.unwrap(), value))
            .collect()
    }
}

pub type DataMap = Map<Value>;

impl Id {
    pub fn new(higher: u64, lower: u64) -> Id {
        Id {
            higher: higher,
            lower: lower,
        }
    }
    pub fn rand() -> Id {
        let (hi, lw) = rand::next_two();
        Id::new(hi, lw)
    }
    pub fn from_header(header: &Header) -> Id {
        Id {
            higher: header.partition,
            lower: header.hash
        }
    }
    pub fn is_greater_than(&self, other: &Id) -> bool {
        self.higher >= other.higher && self.lower > other.lower
    }
}

impl PartialEq for Pos2d32 {
    fn eq(&self, other: &Pos2d32) -> bool {
        self.x == other.x && self.y == other.y
    }
    fn ne(&self, other: &Pos2d32) -> bool {
        self.x != other.x || self.y != other.y
    }
}

impl PartialEq for Pos2d64 {
    fn eq(&self, other: &Pos2d64) -> bool {
        self.x == other.x && self.y == other.y
    }
    fn ne(&self, other: &Pos2d64) -> bool {
        self.x != other.x || self.y != other.y
    }
}

impl PartialEq for Pos3d32 {
    fn eq(&self, other: &Pos3d32) -> bool {
        self.x == other.x && self.y == other.y && self.z == other.z
    }
    fn ne(&self, other: &Pos3d32) -> bool {
        self.x != other.x || self.y != other.y || self.z != other.z
    }
}

impl PartialEq for Pos3d64 {
    fn eq(&self, other: &Pos3d64) -> bool {
        self.x == other.x && self.y == other.y && self.z == other.z
    }
    fn ne(&self, other: &Pos3d64) -> bool {
        self.x != other.x || self.y != other.y || self.z != other.z
    }
}

impl PartialEq for Id {
    fn eq(&self, other: &Id) -> bool {
        self.higher == other.higher && self.lower == other.lower
    }
    fn ne(&self, other: &Id) -> bool {
        self.higher != other.higher || self.lower != other.lower
    }
}
impl PartialOrd for Id {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {return Some(Ordering::Equal)}
        if self.is_greater_than(other) {return Some(Ordering::Greater)}
        Some(Ordering::Less)
    }
}

impl Ord for Id {
    fn cmp(&self, other: &Self) -> Ordering {
        if self == other {return Ordering::Equal}
        if self.is_greater_than(other) {return Ordering::Greater}
        Ordering::Less
    }
}

gen_compound_types_io! (
    Pos2d32, pos2d32_io, {
        |mem_ptr| {
            let x = f32_io::read(mem_ptr);
            let y = f32_io::read(mem_ptr + f32_io::size(0));
            Pos2d32 {x: x, y: y}
        }
    }, {
        |val: &Pos2d32, mem_ptr| {
            f32_io::write(val.x, mem_ptr);
            f32_io::write(val.y, mem_ptr + f32_io::size(0));
        }
    }, {
        f32_io::size(0) * 2
    };

    Pos2d64, pos2d64_io, {
        |mem_ptr| {
            let x = f64_io::read(mem_ptr);
            let y = f64_io::read(mem_ptr + f64_io::size(0));
            Pos2d64 {x: x, y: y}
        }
    }, {
        |val: &Pos2d64, mem_ptr| {
            f64_io::write(val.x, mem_ptr);
            f64_io::write(val.y, mem_ptr + f64_io::size(0));
        }
    }, {
        f64_io::size(0) * 2
    };

    //////////////////////////////////////////////////////////////

    Pos3d32, pos3d32_io, {
        |mem_ptr| {
            let x = f32_io::read(mem_ptr);
            let y = f32_io::read(mem_ptr + f32_io::size(0));
            let z = f32_io::read(mem_ptr + f32_io::size(0) * 2);
            Pos3d32 {x: x, y: y, z: z}
        }
    }, {
        |val: &Pos3d32, mem_ptr| {
            f32_io::write(val.x, mem_ptr);
            f32_io::write(val.y, mem_ptr + f32_io::size(0));
            f32_io::write(val.z, mem_ptr + f32_io::size(0) * 2);
        }
    }, {
        f32_io::size(0) * 3
    };

    Pos3d64, pos3d64_io, {
        |mem_ptr| {
            let x = f64_io::read(mem_ptr);
            let y = f64_io::read(mem_ptr + f64_io::size(0));
            let z = f64_io::read(mem_ptr + f64_io::size(0) * 2);
            Pos3d64 {x: x, y: y, z: z}
        }
    }, {
        |val: &Pos3d64, mem_ptr| {
            f64_io::write(val.x, mem_ptr);
            f64_io::write(val.y, mem_ptr + f64_io::size(0));
            f64_io::write(val.z, mem_ptr + f64_io::size(0) * 2);
        }
    }, {
        f64_io::size(0) * 3
    };

    //////////////////////////////////////////////////////////

    Id, id_io, {
        |mem_ptr| {
            let higher = u64_io::read(mem_ptr);
            let lower =  u64_io::read(mem_ptr + u64_io::size(0));
            Id {higher: higher, lower: lower}
        }
    }, {
        |val: &Id, mem_ptr| {
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
    ["pos2d32", "pos2d", "pos", "pos32"], 15, Pos2d32  ,Pos2d32  ,true  ,  pos2d32_io    ;
    ["pos2d64", "pos64"], 16, Pos2d64                  ,Pos2d64  ,true  ,  pos2d64_io    ;
    ["pos3d32", "pos3d"], 17, Pos3d32                  ,Pos3d32  ,true  ,  pos3d32_io    ;
    ["pos3d64"], 18, Pos3d64                           ,Pos3d64  ,true  ,  pos3d64_io    ;
    ["id"], 19, Id                                     ,Id       ,true  ,  id_io         ;
    ["string", "str"], 20, String                      ,String   ,true  ,  string_io
);

pub const ARRAY_LEN_TYPE_ID: u32 = 8; //u16
pub const NULL_TYPE_ID: u32 = 7; //u8