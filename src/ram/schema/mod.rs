use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::master::ExecError;
use bifrost_hasher::hash_str;

use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::HashMap;

use super::types;
use core::borrow::Borrow;
use std::string::String;
use std::sync::Arc;

use bifrost::utils::fut_exec::wait;
use futures::Future;
use owning_ref::{OwningRef, OwningHandle};
use std::ops::Deref;

pub mod sm;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub id: u32,
    pub name: String,
    pub key_field: Option<Vec<u64>>,
    pub str_key_field: Option<Vec<String>>,
    pub fields: Field,
    pub is_dynamic: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum IndexType {
    Ranged,
    ByValue,
    Vectorization
}

impl Schema {
    pub fn new(
        name: &str,
        key_field: Option<Vec<String>>,
        fields: Field,
        is_dynamic: bool,
    ) -> Schema {
        Schema {
            id: 0,
            name: name.to_string(),
            key_field: match key_field {
                None => None,
                Some(ref keys) => Some(keys.iter().map(|f| hash_str(f)).collect()), // field list into field ids
            },
            str_key_field: key_field,
            fields,
            is_dynamic,
        }
    }
    pub fn new_with_id(
        id: u32,
        name: &str,
        key_field: Option<Vec<String>>,
        fields: Field,
        dynamic: bool,
    ) -> Schema {
        let mut schema = Schema::new(name, key_field, fields, dynamic);
        schema.id = id;
        schema
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub type_id: u32,
    pub nullable: bool,
    pub is_array: bool,
    pub sub_fields: Option<Vec<Field>>,
    pub name: String,
    pub name_id: u64,
    pub indices: Vec<IndexType>,
}

impl Field {
    pub fn new(
        name: &str,
        type_id: u32,
        nullable: bool,
        is_array: bool,
        sub_fields: Option<Vec<Field>>,
        indices: Vec<IndexType>,
    ) -> Field {
        Field {
            name: name.to_string(),
            name_id: types::key_hash(name),
            type_id,
            nullable,
            is_array,
            sub_fields,
            indices,
        }
    }
}

pub struct SchemasMap {
    schema_map: HashMap<u32, Schema>,
    name_map: HashMap<String, u32>,
    id_counter: u32,
}

pub struct LocalSchemasCache {
    map: Arc<RwLock<SchemasMap>>,
}

impl LocalSchemasCache {
    pub fn new(
        group: &str,
        raft_client: Option<&Arc<RaftClient>>,
    ) -> Result<LocalSchemasCache, ExecError> {
        let map = Arc::new(RwLock::new(SchemasMap::new()));
        let _sm = match raft_client {
            Some(raft) => {
                let m1 = map.clone();
                let m2 = map.clone();
                let sm = sm::client::SMClient::new(sm::generate_sm_id(group), raft);
                let mut sm_data = sm.get_all().wait()?.unwrap();
                {
                    let mut map = map.write();
                    for schema in sm_data {
                        map.new_schema(schema);
                    }
                }
                let _ = sm
                    .on_schema_added(move |r| {
                        let schema = r.unwrap();
                        debug!("Add schema {} from subscription", schema.id);
                        let mut m1 = m1.write();
                        m1.new_schema(schema);
                    })
                    .wait()?;
                let _ = sm
                    .on_schema_deleted(move |r| {
                        let mut m2 = m2.write();
                        m2.del_schema(&r.unwrap()).unwrap();
                    })
                    .wait()?;
                Some(sm)
            }
            None => None,
        };
        let schemas = LocalSchemasCache { map };
        return Ok(schemas);
    }
    pub fn get(&self, id: &u32) -> Option<ReadingSchema> {
        let m = self.map.read();
        let so = m.get(id).map(|s| unsafe { s as *const Schema });
        so.map(|s| {
            ReadingSchema {
                owner: m,
                reference: s
            }
        })
    }
    pub fn new_schema(&self, schema: Schema) {
        // for debug only
        let mut m = self.map.write();
        m.new_schema(schema)
    }
    pub fn name_to_id(&self, name: &str) -> Option<u32> {
        let m = self.map.read();
        m.name_to_id(name)
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
        self.schema_map.insert(id, schema);
        self.name_map.insert(name, id);
    }
    pub fn del_schema(&mut self, name: &str) -> Result<(), ()> {
        if let Some(id) = self.name_to_id(name) {
            self.schema_map.remove(&id);
        }
        self.name_map.remove(&name.to_string());
        Ok(())
    }
    pub fn get_by_name(&self, name: &str) -> Option<&Schema> {
        if let Some(id) = self.name_to_id(name) {
            return self.get(&id);
        }
        return None;
    }
    pub fn get(&self, id: &u32) -> Option<&Schema> {
        if let Some(schema) = self.schema_map.get(id) {
            return Some(schema);
        }
        return None;
    }
    pub fn name_to_id(&self, name: &str) -> Option<u32> {
        self.name_map.get(&name.to_string()).cloned()
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
    fn load_from_list(&mut self, data: Vec<Schema>) {
        for schema in data {
            let id = schema.id;
            self.name_map.insert(schema.name.clone(), id);
            self.schema_map.insert(id, schema);
        }
    }
}

pub struct ReadingRef<O, T: ?Sized> {
    owner: O,
    reference: *const T,
}

pub type ReadingSchema<'a> = ReadingRef<RwLockReadGuard<'a, SchemasMap>, Schema>;

impl<O, T: ?Sized> Deref for ReadingRef<O, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe {
            &*self.reference
        }
    }
}