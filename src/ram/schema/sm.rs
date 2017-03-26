use super::*;

use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::state_machine::callback::server::SMCallback;
use bifrost::raft::RaftService;

use std::collections::HashMap;
use std::sync::Arc;

pub static DEFAULT_SM_ID: u64 = hash_ident!(NEB_SCHEMAS_SM) as u64;

pub type SchemasData = (HashMap<u32, Schema>, HashMap<String, u32>);

pub struct SchemasSM {
    schema_map: HashMap<u32, Schema>,
    name_map: HashMap<String, u32>,
    callback: SMCallback,
    id_counter: u32,
}

raft_state_machine! {
    def qry get_all() -> SchemasData;
    def cmd new_schema(schema: Schema);
    def cmd del_schema(name: String);
    def cmd next_id() -> u32;
    def sub on_schema_added() -> Schema;
    def sub on_schema_deleted() -> String;
}

impl StateMachineCmds for SchemasSM {
    fn get_all(&self) -> Result<SchemasData, ()> {
        Ok(self.to_data())
    }
    fn new_schema(&mut self, schema: Schema) -> Result<(), ()> {
        let schema_clone = schema.clone();
        let name = schema.name.clone();
        let id = schema.id;
        self.schema_map.insert(id, schema);
        self.name_map.insert(name, id);
        self.callback.notify(&commands::on_schema_added::new(), Ok(schema_clone));
        Ok(())
    }
    fn del_schema(&mut self, name: String) -> Result<(), ()> {
        if let Some(id) = self.name_map.get(&name) {
            self.schema_map.remove(id);
        }
        self.name_map.remove(&name);
        self.callback.notify(&commands::on_schema_deleted::new(), Ok(name));
        Ok(())
    }
    fn next_id(&mut self) -> Result<u32, ()> {
        self.id_counter += 1;
        Ok(self.id_counter)
    }
}

impl StateMachineCtl for SchemasSM {
    raft_sm_complete!();
    fn snapshot(&self) -> Option<Vec<u8>> {
        Some(serialize!(&self.to_data()))
    }
    fn recover(&mut self, data: Vec<u8>) {
        let maps: SchemasData = deserialize!(&data);
        self.schema_map = maps.0;
        self.name_map = maps.1;
    }
    fn id(&self) -> u64 {DEFAULT_SM_ID}
}

impl SchemasSM {
    pub fn new(raft_service: &Arc<RaftService>) -> SchemasSM {
        SchemasSM {
            schema_map: HashMap::new(),
            name_map: HashMap::new(),
            callback: SMCallback::new(DEFAULT_SM_ID, raft_service.clone()),
            id_counter: 0,
        }
    }
    fn to_data(&self) -> SchemasData {
        (self.schema_map.clone(), self.name_map.clone())
    }
}