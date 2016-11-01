use std::any::Any;
use std::collections::HashMap;
use std::string::String;
use concurrent_hashmap::ConcHashMap;
use serde_json;

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    pub id: u32,
    pub name: String,
    pub key_field: String,
    pub fields: Vec<Field>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Field {
    pub type_id: u32,
    pub name: String,
    pub nullable: bool,
    pub is_array: bool,
    pub sub: Option<Vec<Field>>
}

pub struct Schemas {
    pub schema_map: ConcHashMap<u32, Schema>,
    pub name_map: ConcHashMap<String, u32>,
}

impl Schemas {
    pub fn new() -> Schemas {
        Schemas {
            schema_map: ConcHashMap::<u32, Schema>::new(),
            name_map: ConcHashMap::<String, u32>::new()
        }
    }

}

