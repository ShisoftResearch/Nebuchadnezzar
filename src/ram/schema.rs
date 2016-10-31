use std::any::Any;
use std::collections::HashMap;
use std::string::String;

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
    pub schema_map: HashMap<u32, Schemas>,
    pub name_map: HashMap<String, u32>,
}

