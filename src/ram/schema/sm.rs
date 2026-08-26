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
    generate_scoped_sm_id(group, group)
}

pub fn generate_scoped_sm_id(group: &str, database_name: &str) -> u64 {
    hash_str(&format!("{}-{}-{}", SM_ID_PREFIX, group, database_name))
}

/// What a schema family currently resolves to.
///
/// Derived state: every field is recomputable from the `Schema` records
/// themselves, and `load_from_list` does exactly that. Handles are therefore
/// NOT part of the snapshot -- the snapshot stays `Vec<Schema>`, and there is
/// no way for a handle to disagree with the records it describes after a
/// restart. This codebase has been bitten more than once by derived state that
/// was persisted separately and drifted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaHandle {
    pub uid: SchemaUid,
    pub current_name: String,
    pub current_vid: SchemaVid,
    pub generation: u32,
}

struct SchemasMap {
    /// Every generation ever created, live or superseded, by its own vid.
    schema_map: HashMap<SchemaVid, Schema>,
    /// A name binds to a FAMILY, and only the current generation's name is
    /// bound: after a rename the superseded generations keep the name they
    /// were created under, and those names must stop resolving.
    name_map: HashMap<String, SchemaUid>,
    /// One entry per family. Derived from `schema_map`; see [`SchemaHandle`].
    handles: HashMap<SchemaUid, SchemaHandle>,
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
        future::ready(self.map.schema_map.get(&SchemaVid(id)).map(|r| -> Schema {
            let borrow: &Schema = r;
            borrow.clone()
        }))
        .boxed()
    }
    fn id_of_name(&self, name: String) -> BoxFuture<'_, Option<u32>> {
        future::ready(self.map.id_of_name(&name).map(|vid| vid.get())).boxed()
    }
    fn get_by_name(&self, name: String) -> BoxFuture<'_, Option<Schema>> {
        let id = self.map.id_of_name(&name);
        if let Some(id) = id {
            self.get(id.get())
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
                trace!(
                    "Skipping on_schema_added callback during recovery for schema: {}",
                    schema.name
                );
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
                trace!(
                    "Skipping on_schema_deleted callback during recovery for schema: {}",
                    name
                );
            }
            Ok(())
        }
        .boxed()
    }
    fn next_id(&mut self) -> BoxFuture<'_, u32> {
        // Always start from max existing ID to handle WAL replay scenarios
        // where schemas were added with explicit IDs
        let max_existing = self
            .map
            .schema_map
            .keys()
            .map(|vid| vid.get())
            .max()
            .unwrap_or(0);
        if self.id_count < max_existing {
            self.id_count = max_existing;
        }

        self.id_count += 1;
        while self.map.schema_map.contains_key(&SchemaVid(self.id_count)) {
            self.id_count += 1;
        }
        future::ready(self.id_count).boxed()
    }
}

/// Decode a schema snapshot, or refuse the load.
///
/// `StateMachineCtl::recover` returns `()`, so there is no way to tell the
/// caller the snapshot was unreadable. What this used to do instead was log at
/// `trace!` and return, which left the state machine with an EMPTY schema map
/// -- and a database that comes up looking like it has no schemas rather than
/// one this build cannot read. Every cell then fails `SchemaDoesNotExisted`,
/// and `select_from_chunk_raw` panics on the way there in debug builds. It is
/// the same shape as the recovery bug that silently wiped the ranged index: an
/// unreadable input treated as an empty one.
///
/// That path was unreachable while the record format only ever grew defaulted
/// fields. The uid/vid split deliberately broke the format, which makes it the
/// EXPECTED path for every store written before it -- so refuse loudly instead.
/// A crash at startup is recoverable by rebuilding; a database that quietly
/// forgot its schemas is not.
///
/// Empty input is a different thing and stays benign: `recover` runs only when
/// a snapshot exists, and `snapshot()` never yields zero bytes, so an empty
/// buffer means there was nothing to restore rather than something unreadable.
fn decode_snapshot(data: &[u8]) -> Vec<Schema> {
    if data.is_empty() {
        warn!("Schema snapshot is empty; nothing to recover");
        return Vec::new();
    }
    match utils::serde::deserialize::<Vec<Schema>>(data) {
        Some(schemas) => {
            trace!("Successfully deserialized {} schemas", schemas.len());
            schemas
        }
        None => {
            error!(
                "Schema snapshot ({} bytes) cannot be decoded by this build. The \
                 most likely cause is a store written before the schema uid/vid \
                 split, whose records carry `id` and none of `vid`/`uid`/\
                 `generation`/`status`. That format is not supported and the \
                 store must be rebuilt. Refusing to continue, because loading \
                 zero schemas would leave every cell in this database \
                 undecodable while looking like an empty database.",
                data.len()
            );
            panic!(
                "unreadable schema snapshot ({} bytes): incompatible store, rebuild it",
                data.len()
            );
        }
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

        let schemas: Vec<Schema> = decode_snapshot(&data);

        trace!("Loading {} schemas into map...", schemas.len());
        self.map.load_from_list(schemas.clone());
        trace!("Schemas loaded into map");

        // Calculate id_count from max schema ID to prevent duplicate IDs after recovery
        self.id_count = schemas.iter().map(|s| s.vid.get()).max().unwrap_or(0);
        trace!("Set id_count to {}", self.id_count);
        trace!("========== SchemasSM::recover() COMPLETE ==========");

        future::ready(()).boxed()
    }

    fn recoverable(&self) -> bool {
        true
    }
}

impl SchemasSM {
    pub fn with_callback_and_recovery_flag(
        sm_id: u64,
        callback: SMCallback,
        recovering: Arc<AtomicBool>,
    ) -> SchemasSM {
        SchemasSM {
            callback,
            map: SchemasMap::new(),
            id_count: 0,
            sm_id,
            recovering,
        }
    }

    /// Create a new state machine with a shared recovery flag
    /// The flag should be set to false after server initialization completes
    pub async fn new_with_recovery_flag<'a>(
        group: &'a str,
        database_name: &'a str,
        raft_service: &Arc<RaftService>,
        recovering: Arc<AtomicBool>,
    ) -> SchemasSM {
        let sm_id = generate_scoped_sm_id(group, database_name);
        trace!(
            "Creating SchemasSM for group '{}' and database '{}' with SM ID: {} (recovery mode: {})",
            group,
            database_name,
            sm_id,
            recovering.load(Ordering::Relaxed)
        );
        SchemasSM::with_callback_and_recovery_flag(
            sm_id,
            SMCallback::new(sm_id, raft_service.clone()).await,
            recovering,
        )
    }

    /// Create a new state machine (legacy method, defaults to not recovering)
    pub async fn new<'a>(group: &'a str, raft_service: &Arc<RaftService>) -> SchemasSM {
        Self::new_with_recovery_flag(group, group, raft_service, Arc::new(AtomicBool::new(false)))
            .await
    }
}

impl SchemasMap {
    fn new() -> Self {
        Self {
            schema_map: HashMap::new(),
            name_map: HashMap::new(),
            handles: HashMap::new(),
        }
    }

    fn get_all(&self) -> Vec<Schema> {
        self.schema_map.values().map(|sr| sr.clone()).collect()
    }

    fn new_schema(&mut self, schema: Schema) -> Result<(), NewSchemaError> {
        let name = &schema.name;
        let vid = schema.vid;
        let uid = schema.uid;
        if self.name_map.contains_key(name) {
            return Err(NewSchemaError::NameExists(name.clone()));
        }
        if self.schema_map.contains_key(&vid) {
            return Err(NewSchemaError::IdExists(vid.get()));
        }
        // A brand new schema is a brand new family. Reusing a live family's
        // uid here would mean two schemas sharing an index namespace and a
        // keyed-cell id space, so refuse it rather than silently merge them.
        if self.handles.contains_key(&uid) {
            return Err(NewSchemaError::IdExists(uid.get()));
        }
        self.name_map.insert(name.clone(), uid);
        self.handles.insert(
            uid,
            SchemaHandle {
                uid,
                current_name: name.clone(),
                current_vid: vid,
                generation: schema.generation,
            },
        );
        self.schema_map.insert(vid, schema.clone());
        info!("Schema created in SchemasSM: {} ({})", vid, name);
        debug!("Schema map inserted with vid {}, tid {}", vid, thread_id());
        return Ok(());
    }
    fn del_schema(&mut self, name: &str) -> Result<(), DelSchemaError> {
        let Some(uid) = self.name_map.remove(&(name.to_owned())) else {
            return Err(DelSchemaError::SchemaDoesNotExisted);
        };
        self.handles.remove(&uid);
        // EVERY generation of the family, not just the current one. Deleting a
        // schema already means abandoning its cells; leaving superseded
        // records behind would leak one metadata record per evolution, with
        // nothing left able to name them.
        let doomed: Vec<SchemaVid> = self
            .schema_map
            .iter()
            .filter(|(_, schema)| schema.uid == uid)
            .map(|(vid, _)| *vid)
            .collect();
        for vid in &doomed {
            self.schema_map.remove(vid);
        }
        debug!(
            "Schema map removed family {} ({} generation(s): {:?})",
            uid,
            doomed.len(),
            doomed
        );
        Ok(())
    }
    fn load_from_list(&mut self, data: Vec<Schema>) {
        info!("load_from_list: Loading {} schemas", data.len());
        for schema in data {
            self.schema_map.insert(schema.vid, schema);
        }
        self.rebuild_handles();
        info!(
            "load_from_list: {} record(s) in {} famil(ies)",
            self.schema_map.len(),
            self.handles.len()
        );
    }

    /// Recompute every handle and name binding from the records.
    ///
    /// The name binding is the part that has to be done this way: only the
    /// CURRENT generation's name may resolve, or a family renamed before the
    /// snapshot would still answer to the name its superseded generations were
    /// created under.
    fn rebuild_handles(&mut self) {
        self.handles.clear();
        self.name_map.clear();
        for schema in self.schema_map.values() {
            if !schema.status.is_current() {
                continue;
            }
            if let Some(previous) = self.handles.insert(
                schema.uid,
                SchemaHandle {
                    uid: schema.uid,
                    current_name: schema.name.clone(),
                    current_vid: schema.vid,
                    generation: schema.generation,
                },
            ) {
                // Exactly one generation of a family is current at any
                // instant. Two would make "which layout do new writes use?"
                // ambiguous, and the answer would depend on hash order.
                error!(
                    "Schema family {} has more than one current generation: {} and {}. \
                     Keeping {}; this is a bug in whatever wrote the snapshot.",
                    schema.uid, previous.current_vid, schema.vid, schema.vid
                );
            }
            self.name_map.insert(schema.name.clone(), schema.uid);
        }
        let orphans = self
            .schema_map
            .values()
            .filter(|schema| !self.handles.contains_key(&schema.uid))
            .count();
        if orphans > 0 {
            error!(
                "{} schema record(s) belong to a family with no current generation; \
                 cells naming them are still readable, but the family cannot be written to",
                orphans
            );
        }
    }

    fn id_of_name(&self, name: &str) -> Option<SchemaVid> {
        self.uid_of_name(name)
            .and_then(|uid| self.handles.get(&uid))
            .map(|handle| handle.current_vid)
    }

    fn uid_of_name(&self, name: &str) -> Option<SchemaUid> {
        self.name_map.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A schema record as it was written BEFORE the uid/vid split: an `id`,
    /// and none of the identity fields this build requires. Serialized through
    /// the same helper the state machine uses, so it is a real snapshot in
    /// whichever format this build speaks (JSON in debug, CBOR in release).
    #[derive(Serialize)]
    struct PreSplitSchema {
        id: u32,
        name: String,
    }

    #[test]
    fn an_empty_snapshot_recovers_to_nothing() {
        // `recover` runs only when a snapshot exists and `snapshot()` never
        // yields zero bytes, so this is "there was nothing to restore" -- not
        // "something was unreadable". It must stay benign.
        assert!(decode_snapshot(&[]).is_empty());
    }

    #[test]
    fn a_well_formed_snapshot_round_trips() {
        let mut schema = Schema::new(
            "person",
            None,
            Field::new_schema(vec![Field::new_unindexed("age", Type::U32)]),
            false,
            false,
        );
        schema.assign_identity(7);
        let data = utils::serde::serialize(&vec![schema]);
        let recovered = decode_snapshot(&data);
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].vid, SchemaVid(7));
        assert_eq!(recovered[0].uid, SchemaUid(7));
        assert_eq!(recovered[0].generation, 0);
        assert_eq!(recovered[0].status, SchemaVersionStatus::Current);
    }

    #[test]
    #[should_panic(expected = "unreadable schema snapshot")]
    fn an_incompatible_snapshot_refuses_the_load_instead_of_emptying_it() {
        // The regression this guards: `recover` used to swallow a failed
        // decode at `trace!` and install an EMPTY map, so a store this build
        // cannot read came up looking like a database with no schemas -- and
        // every cell in it undecodable. Refusing loudly is the contract.
        let data = utils::serde::serialize(&vec![PreSplitSchema {
            id: 7,
            name: "person".to_owned(),
        }]);
        assert!(!data.is_empty(), "the fixture must be a non-empty snapshot");
        let _ = decode_snapshot(&data);
    }

    /// Build a second generation of `base`'s family by hand. `evolve_schema`
    /// does not exist until Task 7; this is the shape it will produce.
    fn superseded_by(base: &mut Schema, new_vid: u32) -> Schema {
        let mut next = base.clone();
        next.vid = SchemaVid(new_vid);
        next.generation = base.generation + 1;
        next.status = SchemaVersionStatus::Current;
        base.status = SchemaVersionStatus::Stale {
            superseded_by: next.vid,
        };
        next
    }

    fn schema_named(id: u32, name: &str) -> Schema {
        Schema::new_with_id(
            id,
            name,
            None,
            Field::new_schema(vec![Field::new_unindexed("v", Type::U32)]),
            false,
            false,
        )
    }

    #[test]
    fn two_schemas_get_distinct_families() {
        let mut map = SchemasMap::new();
        map.new_schema(schema_named(1, "a")).unwrap();
        map.new_schema(schema_named(2, "b")).unwrap();
        assert_eq!(map.uid_of_name("a"), Some(SchemaUid(1)));
        assert_eq!(map.uid_of_name("b"), Some(SchemaUid(2)));
        assert_eq!(map.handles.len(), 2);
    }

    #[test]
    fn deleting_a_schema_removes_every_generation_of_its_family() {
        let mut map = SchemasMap::new();
        let mut gen0 = schema_named(1, "evolving");
        let gen1 = superseded_by(&mut gen0, 900);
        map.schema_map.insert(gen0.vid, gen0);
        map.schema_map.insert(gen1.vid, gen1);
        map.rebuild_handles();
        assert_eq!(map.schema_map.len(), 2);

        map.del_schema("evolving").unwrap();

        assert!(
            map.schema_map.is_empty(),
            "a superseded generation left behind would be metadata nothing can name"
        );
        assert!(map.handles.is_empty());
        assert_eq!(map.uid_of_name("evolving"), None);
    }

    #[test]
    fn a_name_resolves_to_the_current_generation_not_a_superseded_one() {
        let mut map = SchemasMap::new();
        let mut gen0 = schema_named(1, "thing");
        let gen1 = superseded_by(&mut gen0, 900);
        map.schema_map.insert(gen0.vid, gen0);
        map.schema_map.insert(gen1.vid, gen1);
        map.rebuild_handles();

        assert_eq!(map.uid_of_name("thing"), Some(SchemaUid(1)));
        assert_eq!(
            map.id_of_name("thing"),
            Some(SchemaVid(900)),
            "writes by name must land in the current generation"
        );
        assert!(
            map.schema_map.contains_key(&SchemaVid(1)),
            "the superseded generation stays readable: cells still name it"
        );
    }

    /// Handles are derived, so a restart must reconstruct them exactly. The
    /// snapshot carries records only.
    #[test]
    fn handles_survive_a_snapshot_round_trip() {
        let mut map = SchemasMap::new();
        let mut gen0 = schema_named(1, "thing");
        let gen1 = superseded_by(&mut gen0, 900);
        map.schema_map.insert(gen0.vid, gen0);
        map.schema_map.insert(gen1.vid, gen1);
        map.rebuild_handles();
        let before = map.handles.clone();

        let snapshot = utils::serde::serialize(&map.get_all());
        let mut restored = SchemasMap::new();
        restored.load_from_list(decode_snapshot(&snapshot));

        assert_eq!(restored.handles, before);
        assert_eq!(restored.id_of_name("thing"), Some(SchemaVid(900)));
        assert_eq!(restored.schema_map.len(), 2);
    }

    /// The rule that makes rename safe across a restart: a superseded
    /// generation keeps the name it was created under, and that name must not
    /// resolve. Only the current generation binds a name.
    #[test]
    fn a_superseded_generations_old_name_does_not_resolve() {
        let mut map = SchemasMap::new();
        let mut gen0 = schema_named(1, "old_name");
        let mut gen1 = superseded_by(&mut gen0, 900);
        gen1.name = "new_name".to_owned();
        map.schema_map.insert(gen0.vid, gen0);
        map.schema_map.insert(gen1.vid, gen1);
        map.rebuild_handles();

        assert_eq!(map.uid_of_name("new_name"), Some(SchemaUid(1)));
        assert_eq!(
            map.uid_of_name("old_name"),
            None,
            "the name a superseded generation was created under must stop resolving"
        );
    }

    #[test]
    fn a_new_schema_may_not_reuse_a_live_family() {
        let mut map = SchemasMap::new();
        map.new_schema(schema_named(1, "first")).unwrap();
        let mut clash = schema_named(2, "second");
        clash.uid = SchemaUid(1);
        assert!(
            map.new_schema(clash).is_err(),
            "two schemas sharing a family would share an index namespace and a keyed-cell id space"
        );
    }

    #[test]
    fn scoped_schema_sm_ids_differ_between_databases() {
        let group = "shared_group";
        let db_a = "db_a";
        let db_b = "db_b";

        assert_eq!(generate_sm_id(group), generate_scoped_sm_id(group, group));
        assert_ne!(
            generate_scoped_sm_id(group, db_a),
            generate_scoped_sm_id(group, db_b)
        );
    }
}
