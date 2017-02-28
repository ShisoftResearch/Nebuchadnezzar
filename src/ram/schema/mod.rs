use bifrost::raft::client::RaftClient;
use bifrost_hasher::hash_str;

use concurrent_hashmap::ConcHashMap;
use parking_lot::Mutex;

use std::sync::{Arc};
use std::collections::HashMap;
use std::string::String;

pub mod sm;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub id: u32,
    pub name: String,
    pub key_field: Option<String>,
    pub fields: Field
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Field {
    pub type_id: u32,
    pub name: String,
    pub nullable: bool,
    pub is_array: bool,
    pub sub: Option<Vec<Field>>
}

pub struct Schemas {
    schema_map: ConcHashMap<u32, Schema>,
    name_map: ConcHashMap<String, u32>,
    id_counter: Mutex<u32>,
    sm: Option<sm::client::SMClient>,
}

impl Schemas {
    pub fn new(raft_client: Option<&Arc<RaftClient>>) -> Arc<Schemas> {
        let sm = match raft_client {
            Some(raft) => Some(sm::client::SMClient::new(sm::DEFAULT_SM_ID, raft)),
            None => None
        };
        let schemas = Arc::new(Schemas {
            schema_map: ConcHashMap::<u32, Schema>::new(),
            name_map: ConcHashMap::<String, u32>::new(),
            id_counter: Mutex::new(0),
            sm: sm
        });
        if let Some(ref sm) = schemas.sm {
            let sc1 = schemas.clone();
            let sc2 = schemas.clone();
            sm.on_schema_added(move |r| {
                sc1.new_schema_(&r.unwrap());
            });
            sm.on_schema_deleted(move |r| {
                sc2.del_schema(&r.unwrap());
            });
        }
        return schemas;
    }
    pub fn new_schema(&self, schema: &mut Schema) {
        schema.id = self.get_id();
        self.new_schema_(schema)
    }
    fn new_schema_(&self, schema: &Schema) {
        let name = schema.name.clone();
        let id = schema.id;
        self.schema_map.insert(id, schema.clone());
        self.name_map.insert(name, id);
        if let Some(ref sm) = self.sm {
            sm.new_schema(schema.clone());
        }
    }
    pub fn del_schema(&self, name: &String) -> Result<(), ()> {
        if let Some(id) = self.name_map.find(name) {
            self.schema_map.remove(&id.get());
        }
        self.name_map.remove(name);
        if let Some(ref sm) = self.sm {
            sm.del_schema(name.clone());
        }
        Ok(())
    }
    pub fn get_by_name(&self, name: &String) -> Option<&Schema> {
        if let Some(id) = self.name_map.find(name) {
            let id = id.get();
            return self.get(&id)
        }
        return None;
    }
    pub fn get(&self, id: &u32) -> Option<&Schema> {
        if let Some(schema) = self.schema_map.find(id) {
            return Some(schema.get())
        }
        return None;
    }
    fn get_id(&self) -> u32 {
        let mut local_id_counter = self.id_counter.lock();
        if let Some(ref sm) = self.sm {
            let id = sm.next_id().unwrap().unwrap();
            *local_id_counter = id;
            return id
        } else {
            *local_id_counter += 1;
            return *local_id_counter;
        }
    }
}