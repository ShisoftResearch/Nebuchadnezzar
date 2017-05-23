use super::*;

use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::state_machine::callback::server::SMCallback;
use bifrost::raft::RaftService;
use bifrost::utils::bincode;

use std::collections::HashMap;
use std::sync::Arc;

pub static DEFAULT_SM_ID: u64 = hash_ident!(NEB_SCHEMAS_SM) as u64;

pub struct SchemasSM {
    callback: SMCallback,
    map: Arc<RwLock<SchemasMap>>
}

raft_state_machine! {
    def qry get_all() -> Vec<Schema>;
    def cmd new_schema(schema: Schema);
    def cmd del_schema(name: String);
    def cmd next_id() -> u32;
    def sub on_schema_added() -> Schema;
    def sub on_schema_deleted() -> String;
}

impl StateMachineCmds for SchemasSM {
    fn get_all(&self) -> Result<Vec<Schema>, ()> {
        let m = self.map.read();
        Ok(m.get_all())
    }
    fn new_schema(&mut self, schema: Schema) -> Result<(), ()> {
        {
            let mut map = self.map.write();
            map.new_schema(&schema);
        }
        println!("{:?}", self.callback.notify(&commands::on_schema_added::new(), Ok(schema)));
        Ok(())
    }
    fn del_schema(&mut self, name: String) -> Result<(), ()> {
        {
            let mut map = self.map.write();
            map.del_schema(&name);
        }
        self.callback.notify(&commands::on_schema_deleted::new(), Ok(name));
        Ok(())
    }
    fn next_id(&mut self) -> Result<u32, ()> {
        let mut map = self.map.write();
        Ok(map.next_id())
    }
}

impl StateMachineCtl for SchemasSM {
    raft_sm_complete!();
    fn snapshot(&self) -> Option<Vec<u8>> {
        let map = self.map.read();
        Some(bincode::serialize(&map.get_all()))
    }
    fn recover(&mut self, data: Vec<u8>) {
        let schemas: Vec<Schema> = bincode::deserialize(&data);
        let mut map = self.map.write();
        map.load_from_list(&schemas);
    }
    fn id(&self) -> u64 {DEFAULT_SM_ID}
}

impl SchemasSM {
    pub fn new(raft_service: &Arc<RaftService>) -> SchemasSM {
        SchemasSM {
            callback: SMCallback::new(DEFAULT_SM_ID, raft_service.clone()),
            map: Arc::new(RwLock::new(SchemasMap::new()))
        }
    }
}