use std::any::Any;
use std::collections::HashMap;
use std::string::String;

pub struct Schema {
    pub id: u32,
    pub name: String,
    pub key_field: u16,
    pub fields: Vec<Field>
}

pub struct Field {
    pub type_id: u32,
    pub name: String,
    pub sub: Option<Vec<Field>>
}

pub fn to_json (schema: Schema) -> String {

}