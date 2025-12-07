use super::*;

use bifrost::raft::state_machine::callback::server::SMCallback;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::RaftService;
use bifrost::utils;
use bifrost_hasher::hash_str;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Flag to track if we're currently recovering from snapshots/logs
    /// Callbacks should be skipped during recovery to avoid trying to
    /// notify services that may not be initialized yet
    recovering: Arc<AtomicBool>,
}

raft_state_machine! {
    def qry get_all() -> Vec<Schema>;
    def qry get(id: u32) -> Option<Schema>;
    def qry get_by_name(name: String) -> Option<Schema>;
    def qry id_of_name(name: String) -> Option<u32>;
    def cmd new_schema(schema: Schema) -> Result<(), NewSchemaError>;
    def cmd del_schema(name: String) -> Result<(), DelSchemaError>;
    def cmd next_id() -> u32;
    def sub on_schema_added() -> Schema;
    def sub on_schema_deleted() -> String;
}

impl StateMachineCmds for SchemasSM {
    fn get_all(&self) -> BoxFuture<'_, Vec<Schema>> {
        future::ready(self.map.get_all()).boxed()
    }
    fn get(&self, id: u32) -> BoxFuture<'_, Option<Schema>> {
        future::ready(self.map.schema_map.get(&id).map(|r| -> Schema {
            let borrow: &Schema = r;
            borrow.clone()
        }))
        .boxed()
    }
    fn id_of_name(&self, name: String) -> BoxFuture<'_, Option<u32>> {
        future::ready(self.map.id_of_name(&name)).boxed()
    }
    fn get_by_name(&self, name: String) -> BoxFuture<'_, Option<Schema>> {
        let id = self.map.id_of_name(&name);
        if let Some(id) = id {
            self.get(id)
        } else {
            future::ready(None).boxed()
        }
    }
    fn new_schema(&mut self, schema: Schema) -> BoxFuture<'_, Result<(), NewSchemaError>> {
        let is_recovering = self.recovering.load(Ordering::Relaxed);
        async move {
            self.map.new_schema(schema.clone())?;
            // Skip callbacks during recovery to avoid notifying services
            // that haven't been initialized yet
            if !is_recovering {
                self.callback
                    .notify(commands::on_schema_added::new(), schema)
                    .await
                    .map_err(|e| NewSchemaError::NotifyError(e))?;
            } else {
                trace!("Skipping on_schema_added callback during recovery for schema: {}", schema.name);
            }
            Ok(())
        }
        .boxed()
    }
    fn del_schema(&mut self, name: String) -> BoxFuture<'_, Result<(), DelSchemaError>> {
        let is_recovering = self.recovering.load(Ordering::Relaxed);
        async move {
            self.map.del_schema(&name)?;
            // Skip callbacks during recovery to avoid notifying services
            // that haven't been initialized yet
            if !is_recovering {
                self.callback
                    .notify(commands::on_schema_deleted::new(), name.clone())
                    .await
                    .map_err(|e| DelSchemaError::NotifyError(e))?;
            } else {
                trace!("Skipping on_schema_deleted callback during recovery for schema: {}", name);
            }
            Ok(())
        }
        .boxed()
    }
    fn next_id(&mut self) -> BoxFuture<'_, u32> {
        // Always start from max existing ID to handle WAL replay scenarios
        // where schemas were added with explicit IDs
        let max_existing = self.map.schema_map.keys().max().copied().unwrap_or(0);
        if self.id_count < max_existing {
            self.id_count = max_existing;
        }

        self.id_count += 1;
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
    fn snapshot(&self) -> Vec<u8> {
        utils::serde::serialize(&self.map.get_all())
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<'_, ()> {
        trace!("========== SchemasSM::recover() CALLED ==========");
        trace!("Received {} bytes of snapshot data", data.len());

        let schemas: Vec<Schema> = match utils::serde::deserialize::<Vec<Schema>>(&data) {
            Some(s) => {
                trace!("Successfully deserialized {} schemas", s.len());
                s
            }
            None => {
                trace!("Failed to deserialize schemas from snapshot data");
                return future::ready(()).boxed();
            }
        };

        trace!("Loading {} schemas into map...", schemas.len());
        self.map.load_from_list(schemas.clone());
        trace!("Schemas loaded into map");

        // Calculate id_count from max schema ID to prevent duplicate IDs after recovery
        self.id_count = schemas.iter().map(|s| s.id).max().unwrap_or(0);
        trace!("Set id_count to {}", self.id_count);
        trace!("========== SchemasSM::recover() COMPLETE ==========");

        future::ready(()).boxed()
    }

    fn recoverable(&self) -> bool {
        true
    }
}

impl SchemasSM {
    /// Create a new state machine with a shared recovery flag
    /// The flag should be set to false after server initialization completes
    pub async fn new_with_recovery_flag<'a>(
        group: &'a str,
        raft_service: &Arc<RaftService>,
        recovering: Arc<AtomicBool>,
    ) -> SchemasSM {
        let sm_id = generate_sm_id(group);
        trace!(
            "Creating SchemasSM for group '{}' with SM ID: {} (recovery mode: {})",
            group,
            sm_id,
            recovering.load(Ordering::Relaxed)
        );
        SchemasSM {
            callback: SMCallback::new(sm_id, raft_service.clone()).await,
            map: SchemasMap::new(),
            id_count: 0,
            sm_id,
            recovering,
        }
    }
    
    /// Create a new state machine (legacy method, defaults to not recovering)
    pub async fn new<'a>(group: &'a str, raft_service: &Arc<RaftService>) -> SchemasSM {
        Self::new_with_recovery_flag(
            group,
            raft_service,
            Arc::new(AtomicBool::new(false)),
        ).await
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
        self.schema_map.insert(id, schema.clone());
        info!("Schema created in SchemasSM: {} ({})", id, name);
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
        error!("load_from_list: Loading {} schemas", data.len());
        for (idx, schema) in data.into_iter().enumerate() {
            let id = schema.id;
            self.name_map.insert(schema.name.clone(), id);
            self.schema_map.insert(id, schema);
            if idx % 100 == 0 {
                error!("load_from_list: Loaded {} schemas so far", idx);
            }
        }
        error!("load_from_list: All schemas loaded");
    }
    fn id_of_name(&self, name: &str) -> Option<u32> {
        self.name_map.get(name).cloned()
    }
}
