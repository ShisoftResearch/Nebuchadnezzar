use super::*;

use bifrost::raft::state_machine::callback::server::SMCallback;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::RaftService;
use bifrost::utils;
use bifrost_hasher::hash_str;
use std::sync::Arc;

pub static SM_ID_PREFIX: &'static str = "NEB_SCHEMAS_SM";

pub fn generate_sm_id(group: &str) -> u64 {
    hash_str(&format!("{}-{}", SM_ID_PREFIX, group))
}

struct SchemasMap {
    schema_map: HashMap<u32, Schema>,
    name_map: HashMap<String, u32>,
}

pub struct SchemasSM {
    callback: SMCallback,
    map: SchemasMap,
    id_count: u32,
    sm_id: u64,
}

raft_state_machine! {
    def qry get_all() -> Vec<Schema>;
    def qry get(id: u32) -> Option<Schema>;
    def cmd new_schema(schema: Schema) -> Result<(), NewSchemaError>;
    def cmd del_schema(name: String) -> Result<(), DelSchemaError>;
    def cmd next_id() -> u32;
    def sub on_schema_added() -> Schema;
    def sub on_schema_deleted() -> String;
}

impl StateMachineCmds for SchemasSM {
    fn get_all(&self) -> BoxFuture<Vec<Schema>> {
        future::ready(self.map.get_all()).boxed()
    }
    fn get(&self, id: u32) -> BoxFuture<Option<Schema>> {
        future::ready(self.map.schema_map.get(&id).map(|r| -> Schema {
            let borrow: &Schema = r.borrow();
            borrow.clone()
        }))
        .boxed()
    }
    fn new_schema(&mut self, schema: Schema) -> BoxFuture<Result<(), NewSchemaError>> {
        async move {
            self.map.new_schema(schema.clone())?;
            self.callback
                .notify(commands::on_schema_added::new(), schema)
                .await
                .map_err(|e| NewSchemaError::NotifyError(e))?;
            Ok(())
        }
        .boxed()
    }
    fn del_schema(&mut self, name: String) -> BoxFuture<Result<(), DelSchemaError>> {
        async move {
            self.map.del_schema(&name)?;
            self.callback
                .notify(commands::on_schema_deleted::new(), name)
                .await
                .map_err(|e| DelSchemaError::NotifyError(e))?;
            Ok(())
        }
        .boxed()
    }
    fn next_id(&mut self) -> BoxFuture<u32> {
        while self.map.schema_map.contains_key(&self.id_count) {
            self.id_count += 1;
        }
        future::ready(self.id_count).boxed()
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
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        let schemas: Vec<Schema> = utils::serde::deserialize(&data).unwrap();
        self.map.load_from_list(schemas);
        future::ready(()).boxed()
    }
}

impl SchemasSM {
    pub async fn new<'a>(group: &'a str, raft_service: &Arc<RaftService>) -> SchemasSM {
        let sm_id = generate_sm_id(group);
        SchemasSM {
            callback: SMCallback::new(sm_id, raft_service.clone()).await,
            map: SchemasMap::new(),
            id_count: 0,
            sm_id,
        }
    }
}

impl SchemasMap {
    fn new() -> Self {
        Self {
            schema_map: HashMap::new(),
            name_map: HashMap::new(),
        }
    }

    fn get_all(&self) -> Vec<Schema> {
        self.schema_map.values().map(|sr| sr.clone()).collect()
    }

    fn new_schema(&mut self, schema: Schema) -> Result<(), NewSchemaError> {
        let name = &schema.name;
        let id = schema.id;
        if self.name_map.contains_key(name) {
            return Err(NewSchemaError::NameExists(name.clone()));
        }
        self.name_map.insert(name.clone(), id);
        if self.schema_map.contains_key(&id) {
            return Err(NewSchemaError::IdExists(id));
        }
        self.schema_map.insert(id, schema);
        debug!("Schema map inserted with id {}, tid {}", id, thread_id());
        return Ok(());
    }
    fn del_schema(&mut self, name: &str) -> Result<(), DelSchemaError> {
        if let Some(id) = self.name_map.remove(&(name.to_owned())) {
            self.schema_map.remove(&id);
            debug!("Schema map removed {}", id);
            Ok(())
        } else {
            Err(DelSchemaError::SchemaDoesNotExisted)
        }
    }
    fn load_from_list(&mut self, data: Vec<Schema>) {
        for schema in data {
            let id = schema.id;
            self.name_map.insert(schema.name.clone(), id);
            self.schema_map.insert(id, schema);
            debug!("Inserted listed schema {}", id);
        }
    }
}
