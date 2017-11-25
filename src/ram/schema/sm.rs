use super::*;

use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::state_machine::callback::server::{SMCallback, NotifyError};
use bifrost::raft::RaftService;
use bifrost::utils::bincode;
use bifrost_hasher::hash_str;

use std::sync::Arc;

pub static SM_ID_PREFIX: &'static str = "NEB_SCHEMAS_SM";

pub fn generate_sm_id<'a>(group: &'a str) -> u64 {
    hash_str(&format!("{}-{}", SM_ID_PREFIX, group))
}

pub struct SchemasSM {
    callback: SMCallback,
    map: SchemasMap,
    sm_id: u64
}

raft_state_machine! {
    def qry get_all() -> Vec<Schema>;
    def qry get(id: u32) -> Option<Schema>;
    def cmd new_schema(schema: Schema) | NotifyError;
    def cmd del_schema(name: String) | NotifyError;
    def cmd next_id() -> u32;
    def sub on_schema_added() -> Schema;
    def sub on_schema_deleted() -> String;
}

impl StateMachineCmds for SchemasSM {
    fn get_all(&self) -> Result<Vec<Schema>, ()> {
        Ok(self.map.get_all())
    }
    fn get(&self, id: u32) -> Result<Option<Schema>, ()> {
        Ok(self.map.get(&id).map(
            |r| -> Schema {
                let borrow: &Schema = r.borrow();
                borrow.clone()
            }
        ))
    }
    fn new_schema(&mut self, schema: Schema) -> Result<(), NotifyError> {
        self.map.new_schema(schema.clone());
        self.callback.notify(&commands::on_schema_added::new(), Ok(schema))?;
        Ok(())
    }
    fn del_schema(&mut self, name: String) -> Result<(), NotifyError> {
        self.map.del_schema(&name);
        self.callback.notify(&commands::on_schema_deleted::new(), Ok(name))?;
        Ok(())
    }
    fn next_id(&mut self) -> Result<u32, ()> {
        Ok(self.map.next_id())
    }
}

impl StateMachineCtl for SchemasSM {
    raft_sm_complete!();
    fn snapshot(&self) -> Option<Vec<u8>> {
        Some(bincode::serialize(&self.map.get_all()))
    }
    fn recover(&mut self, data: Vec<u8>) {
        let schemas: Vec<Schema> = bincode::deserialize(&data);
        self.map.load_from_list(&schemas);
    }
    fn id(&self) -> u64 {self.sm_id }
}

impl SchemasSM {
    pub fn new<'a>(group: &'a str, raft_service: &Arc<RaftService>) -> SchemasSM {
        let sm_id = generate_sm_id(group);
        SchemasSM {
            callback: SMCallback::new(sm_id, raft_service.clone()),
            map: SchemasMap::new(),
            sm_id
        }
    }
}