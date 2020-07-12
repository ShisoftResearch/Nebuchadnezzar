use super::*;

use bifrost::raft::state_machine::callback::server::{NotifyError, SMCallback};
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::RaftService;
use bifrost_hasher::hash_str;
use bifrost::utils;
use std::sync::Arc;

pub static SM_ID_PREFIX: &'static str = "NEB_SCHEMAS_SM";

pub fn generate_sm_id(group: &str) -> u64 {
    hash_str(&format!("{}-{}", SM_ID_PREFIX, group))
}

pub struct SchemasSM {
    callback: SMCallback,
    map: SchemasMap,
    sm_id: u64,
}

raft_state_machine! {
    def qry get_all() -> Vec<Schema>;
    def qry get(id: u32) -> Option<Schema>;
    def cmd new_schema(schema: Schema) -> Result<(), NotifyError>;
    def cmd del_schema(name: String) -> Result<(), NotifyError>;
    def cmd next_id() -> u32;
    def sub on_schema_added() -> Schema;
    def sub on_schema_deleted() -> String;
}

impl StateMachineCmds for SchemasSM {
    fn get_all(&self) -> BoxFuture<Vec<Schema>> {
        future::ready(self.map.get_all()).boxed()
    }
    fn get(&self, id: u32) -> BoxFuture<Option<Schema>> {
        future::ready(self.map.get(&id).map(|r| -> Schema {
            let borrow: &Schema = r.borrow();
            borrow.clone()
        })).boxed()
    }
    fn new_schema(&mut self, schema: Schema) -> BoxFuture<Result<(), NotifyError>> {
        self.map.new_schema(schema.clone());
        async move {
            self.callback
            .notify(commands::on_schema_added::new(), schema).await?;
            Ok(())
        }.boxed()
    }
    fn del_schema(&mut self, name: String) -> BoxFuture<Result<(), NotifyError>> {
        self.map.del_schema(&name).unwrap();
        async move {
            self.callback
                .notify(commands::on_schema_deleted::new(), name).await?;
            Ok(())
        }.boxed()
    }
    fn next_id(&mut self) -> BoxFuture<u32> {
        future::ready(self.map.next_id()).boxed()
    }
}

impl StateMachineCtl for SchemasSM {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        self.sm_id
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        Some(utils::serde::serialize(&self.map.get_all()))
    }
    fn recover(&mut self, data: Vec<u8>) {
        let schemas: Vec<Schema> = utils::serde::deserialize(&data).unwrap();
        self.map.load_from_list(schemas);
    }
}

impl SchemasSM {
    pub async fn new<'a>(group: &'a str, raft_service: &Arc<RaftService>) -> SchemasSM {
        let sm_id = generate_sm_id(group);
        SchemasSM {
            callback: SMCallback::new(sm_id, raft_service.clone()).await,
            map: SchemasMap::new(),
            sm_id,
        }
    }
}
