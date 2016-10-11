use std::any::Any;
use std::collections::HashMap;
use std::string::String;

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    pub id: u32,
    pub name: String,
    pub key_field: u16,
    pub fields: Vec<Field>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Field {
    pub type_id: u32,
    pub name: String,
    pub sub: Option<Vec<Field>>
}