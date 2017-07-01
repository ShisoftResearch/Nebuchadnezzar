use bifrost::raft::client::{RaftClient, SubscriptionError};
use bifrost::raft::state_machine::master::ExecError;
use bifrost_hasher::hash_str;

use std::collections::HashMap;
use parking_lot::{RwLock};

use std::sync::{Arc};
use std::string::String;
use core::borrow::Borrow;
use super::types;

pub mod sm;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub id: u32,
    pub name: String,
    pub key_field: Option<Vec<u64>>,
    pub str_key_field: Option<Vec<String>>,
    pub fields: Field
}

impl Schema {
    pub fn new(name: String, key_field: Option<Vec<String>>, fields: Field) -> Schema {
        Schema {
            id: 0,
            name: name,
            key_field: match key_field {
                None => None,
                Some(ref keys) => Some(keys
                    .iter()
                    .map(|f| hash_str(f))
                    .collect())             // field list into field ids
            },
            str_key_field: key_field,
            fields: fields
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Field {
    pub type_id: u32,
    pub nullable: bool,
    pub is_array: bool,
    pub sub_fields: Option<Vec<Field>>,
    pub name: String,
    pub name_id: u64,
}

impl Field {
    pub fn new(
        name: &String,
        type_id: u32,
        nullable: bool,
        is_array: bool,
        sub_fields: Option<Vec<Field>>)
        -> Field {
        Field {
            name: name.clone(),
            name_id: types::key_hash(name),
            type_id: type_id,
            nullable: nullable,
            is_array: is_array,
            sub_fields: sub_fields
        }
    }
}

pub struct SchemasMap {
    schema_map: HashMap<u32, Arc<Schema>>,
    name_map: HashMap<String, u32>,
    id_counter: u32,
}

pub struct SchemasServer {
    map: Arc<RwLock<SchemasMap>>,
    sm: Option<sm::client::SMClient>,
}

impl SchemasServer {
    pub fn new(raft_client: Option<&Arc<RaftClient>>) -> Result<SchemasServer, ExecError> {
        let map = Arc::new(RwLock::new(SchemasMap::new()));
        let sm = match raft_client {
            Some(raft) => {
                let m1 = map.clone();
                let m2 = map.clone();
                let sm = sm::client::SMClient::new(sm::DEFAULT_SM_ID, raft);
                let mut sm_data = sm.get_all()?.unwrap();
                {
                    let mut map = map.write();
                    for schema in sm_data {
                        map.new_schema(schema);
                    }
                }
                let _ = sm.on_schema_added(move |r| {
                    let mut m1 = m1.write();
                    m1.new_schema(r.unwrap());
                })?;
                let _ = sm.on_schema_deleted(move |r| {
                    let mut m2 = m2.write();
                    m2.del_schema(&r.unwrap());
                })?;
                Some(sm)
            },
            None => None
        };
        let schemas = SchemasServer {
            map: map,
            sm: sm
        };
        return Ok(schemas);
    }
    pub fn get(&self, id: &u32) -> Option<Arc<Schema>> {
        let m = self.map.read();
        m.get(id)
    }
    pub fn new_schema(&self, schema: Schema) { // for debug only
        let mut m = self.map.write();
        m.new_schema(schema)
    }
}

impl SchemasMap {
    pub fn new() -> SchemasMap {
        SchemasMap {
            schema_map: HashMap::new(),
            name_map: HashMap::new(),
            id_counter: 0,
        }
    }
    pub fn new_schema(&mut self, schema: Schema) {
        let name = schema.name.clone();
        let id = schema.id;
        self.schema_map.insert(id, Arc::new(schema.clone()));
        self.name_map.insert(name, id);
    }
    pub fn del_schema(&mut self, name: &String) -> Result<(), ()> {
        if let Some(id) = self.name_map.get(name) {
            self.schema_map.remove(&id);
        }
        self.name_map.remove(name);
        Ok(())
    }
    pub fn get_by_name(&self, name: &String) -> Option<Arc<Schema>> {
        if let Some(id) = self.name_map.get(name) {
            return self.get(&id)
        }
        return None;
    }
    pub fn get(&self, id: &u32) -> Option<Arc<Schema>> {
        if let Some(schema) = self.schema_map.get(id) {
            return Some(schema.clone())
        }
        return None;
    }
    fn next_id(&mut self) -> u32 {
        self.id_counter += 1;
        while self.schema_map.contains_key(&self.id_counter) {
            self.id_counter += 1;
        }
        self.id_counter
    }
    fn get_all(&self) -> Vec<Schema> {
        self.schema_map
            .values()
            .map(|s_ref| {
                let arc = s_ref.clone();
                let r: &Schema = arc.borrow();
                r.clone()
            })
            .collect()
    }
    fn load_from_list(&mut self, data: &Vec<Schema>) {
         for schema in data {
             let id = schema.id;
             self.name_map.insert(schema.name.clone(), id);
             self.schema_map.insert(id, Arc::new(schema.clone()));
         }
    }
}