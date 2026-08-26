use bifrost::raft::client::AsRaftPlaneClient;
use bifrost::raft::state_machine::callback::server::NotifyError;
use bifrost::raft::state_machine::master::ExecError;
use bifrost_hasher::hash_str;

use dovahkiin::types::Type;
use lightning::map::{Map, PtrHashMap as LFHashMap};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::mem;

use crate::index::embedding::EmbeddingIndexConfig;
use crate::index::vector::VectorIndexConfig;
use crate::ram::io::align_address;
use crate::server::DatabaseRuntime;
use crate::utils::thread_id;

use super::types;
use std::string::String;
use std::sync::Arc;

use futures::prelude::*;
use futures::FutureExt;
use smallvec::SmallVec;
use std::ops::Deref;

pub mod sm;

pub type FieldPtr = u32;

pub const PTR_ALIGN: usize = mem::align_of::<u32>();

/// A schema's **logical family**: what a durable reference means when it says
/// "person". Immutable, and stable across both rename and shape change.
///
/// Everything that names a schema as a concept keys by this: cell identity,
/// every index namespace, statistics. Never stored in a cell header.
#[derive(
    Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct SchemaUid(pub u32);

/// A schema's **physical generation**: one exact field layout. Immutable.
///
/// This is what a cell header stores, and what a decode must use -- the exact
/// generation that produced those bytes, not whatever is current now.
#[derive(
    Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct SchemaVid(pub u32);

impl SchemaUid {
    /// The raw number, for the hand-rolled encoders and the numeric-keyed
    /// caches. Prefer passing the newtype wherever a signature can take it.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl SchemaVid {
    /// The raw number. See [`SchemaUid::get`].
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for SchemaUid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for SchemaVid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Where a generation sits in its family's history.
///
/// Exactly one generation of a [`SchemaUid`] is `Current` at any instant; it is
/// the only one new writes may land in. A `Stale` generation stays readable for
/// as long as any cell still names it, which is forever until something
/// rewrites those cells.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchemaVersionStatus {
    #[default]
    Current,
    Stale {
        superseded_by: SchemaVid,
    },
}

impl SchemaVersionStatus {
    pub fn is_current(&self) -> bool {
        matches!(self, SchemaVersionStatus::Current)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    /// The physical generation this record describes. Cell headers name this.
    pub vid: SchemaVid,
    /// The logical family this generation belongs to. Durable references and
    /// index namespaces name this.
    pub uid: SchemaUid,
    /// How many times this family has been evolved; 0 for a fresh schema.
    pub generation: u32,
    /// Whether new writes may still land in this generation.
    pub status: SchemaVersionStatus,
    pub name: String,
    pub key_field: Option<Vec<u64>>,
    pub str_key_field: Option<Vec<String>>,
    pub field_index: BTreeMap<u64, Vec<usize>>,
    pub id_index: BTreeMap<u64, Vec<u64>>,
    pub index_fields: BTreeMap<u64, Vec<IndexType>>,
    #[serde(default)]
    pub compound_index_fields: BTreeMap<u64, CompoundIndex>,
    pub fields: Field,
    #[serde(skip, default)]
    pub compression_plan: SchemaCompressionPlan,
    pub static_bound: usize,
    pub is_dynamic: bool,
    pub is_scannable: bool,
    #[serde(default)]
    pub blobs: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CompoundIndex {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    pub fields: Vec<String>,
    pub field_ids: Vec<u64>,
    pub indices: Vec<IndexType>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum IndexType {
    Ranged,
    Hashed,
    Null,
    Vector(VectorIndexConfig),
    Fulltext,
    /// Embedding index with configurable model.
    /// The model name is interpreted by the embedding implementation (e.g., Morpheus).
    Embedding(EmbeddingIndexConfig),
    Statistics,
}

/// Field-level compression configuration
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FieldCompression {
    /// LZ4 block compression (fast, moderate ratio)
    /// Only valid for Type::String and Type::Bytes
    Lz4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressedFieldKind {
    String,
    Bytes,
}

pub type CompressedFieldPath = SmallVec<[u64; 4]>;
pub type CompressedFieldPlans = SmallVec<[CompressedFieldPlan; 8]>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressedFieldPlan {
    pub path: CompressedFieldPath,
    pub kind: CompressedFieldKind,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaCompressionPlan {
    pub fields: CompressedFieldPlans,
}

impl Schema {
    pub fn new(
        name: &str,
        key_field: Option<Vec<String>>,
        mut fields: Field,
        is_dynamic: bool,
        is_scannable: bool,
    ) -> Schema {
        let mut bound = 0;
        let mut field_index = BTreeMap::new();
        let mut id_index = BTreeMap::new();
        let mut index_fields = BTreeMap::new();
        let compound_index_fields = BTreeMap::new();
        fields.assign_offsets(
            &mut bound,
            &mut field_index,
            &mut id_index,
            &mut index_fields,
            String::new(),
            vec![],
            vec![],
        );
        // CRITICAL: Align to 8 bytes for variable region, not just 4 bytes!
        // The variable region may contain u64 fields that require 8-byte alignment.
        // Using PTR_ALIGN (4 bytes) caused the +6 byte misalignment bug that crashed
        // at addresses ending in 0xE6. Example: wikidata_link schema had static_bound=44,
        // which is only 4-byte aligned, causing misaligned u64 reads in variable region.
        bound = align_address(8, bound);
        trace!("Schema {:?} has bound {} (8-byte aligned)", fields, bound);
        let mut schema = Schema {
            vid: SchemaVid(0),
            uid: SchemaUid(0),
            generation: 0,
            status: SchemaVersionStatus::Current,
            name: name.to_string(),
            key_field: match key_field {
                None => None,
                Some(ref keys) => Some(keys.iter().map(|f| hash_str(f)).collect()), // field list into field ids
            },
            str_key_field: key_field,
            static_bound: bound,
            fields,
            compression_plan: SchemaCompressionPlan::default(),
            is_dynamic,
            is_scannable,
            blobs: false,
            field_index,
            id_index,
            index_fields,
            compound_index_fields,
        };
        schema.refresh_compression_plan();
        schema
    }
    pub fn new_with_id(
        id: u32,
        name: &str,
        key_field: Option<Vec<String>>,
        fields: Field,
        dynamic: bool,
        scannable: bool,
    ) -> Schema {
        let mut schema = Schema::new(name, key_field, fields, dynamic, scannable);
        schema.assign_identity(id);
        schema
    }

    /// Stamp a freshly-created schema with the number the shared counter handed
    /// out.
    ///
    /// A new schema draws ONE number and uses it for both its family and its
    /// first generation. That is what keeps a value from ever being both a live
    /// uid and some unrelated schema's vid: the types stop a mix-up in code,
    /// and one allocator stops it in durable data, where there is no compiler.
    pub fn assign_identity(&mut self, id: u32) {
        self.vid = SchemaVid(id);
        self.uid = SchemaUid(id);
        self.generation = 0;
        self.status = SchemaVersionStatus::Current;
    }

    pub fn with_blobs(mut self, blobs: bool) -> Schema {
        self.blobs = blobs;
        self
    }

    pub fn field_by_id_path(&self, path: &[u64]) -> Option<&Field> {
        let mut field = &self.fields;
        for name_id in path {
            if let Some(new_field) = field.field_by_name_id(name_id) {
                field = new_field;
            } else {
                return None;
            }
        }
        Some(field)
    }

    pub fn add_compound_index(&mut self, name: &str, fields: Vec<String>, indices: Vec<IndexType>) {
        let field_ids = fields.iter().map(|field| hash_str(field)).collect();
        let name_id = hash_str(name);
        self.compound_index_fields.insert(
            name_id,
            CompoundIndex {
                name: name.to_string(),
                fields,
                field_ids,
                indices,
            },
        );
    }

    pub fn add_compound_index_with_ids(
        &mut self,
        name: &str,
        field_ids: Vec<u64>,
        indices: Vec<IndexType>,
    ) {
        let name_id = hash_str(name);
        self.compound_index_fields.insert(
            name_id,
            CompoundIndex {
                name: name.to_string(),
                fields: vec![],
                field_ids,
                indices,
            },
        );
    }

    pub fn refresh_compression_plan(&mut self) {
        self.compression_plan = SchemaCompressionPlan::from_schema(self);
    }

    pub fn validate_for_registration(&self) -> Result<(), NewSchemaError> {
        self.fields.validate_for_registration("*")?;

        for (compound_id, compound) in &self.compound_index_fields {
            for field_id in &compound.field_ids {
                if !self.id_index.contains_key(field_id) {
                    return Err(NewSchemaError::InvalidSchema(format!(
                        "compound index {compound_id} references unknown field {field_id}"
                    )));
                }
            }

            for index in &compound.indices {
                if !matches!(index, IndexType::Embedding(_)) {
                    return Err(NewSchemaError::InvalidSchema(format!(
                        "compound index {compound_id} only supports embedding indices"
                    )));
                }
            }
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub data_type: Type,
    pub nullable: bool,
    pub is_array: bool,
    pub vector_size: Option<u16>,
    pub sub_fields: Option<Vec<Field>>,
    pub sub_fields_map: Option<HashMap<u64, usize>>,
    pub name: String,
    pub name_id: u64,
    pub indices: Vec<IndexType>,
    pub offset: Option<usize>,
    #[serde(default)]
    pub compression: Option<FieldCompression>,
}

impl Field {
    pub fn new(
        name: &str,
        data_type: Type,
        nullable: bool,
        is_array: bool,
        sub_fields: Option<Vec<Field>>,
        indices: Vec<IndexType>,
    ) -> Field {
        let sub_fields_map = sub_fields.as_ref().map(|fields| {
            fields
                .iter()
                .enumerate()
                .map(|(id, field)| (field.name_id, id))
                .collect::<HashMap<_, _>>()
        });
        Field {
            name: name.to_string(),
            name_id: types::key_hash(name),
            data_type,
            nullable,
            is_array,
            sub_fields,
            sub_fields_map,
            indices,
            offset: None,
            vector_size: None,
            compression: None,
        }
    }
    pub fn new_map(name: &str, sub_fields: Vec<Field>) -> Field {
        Self::new(name, Type::Map, false, false, Some(sub_fields), vec![])
    }

    pub fn with_compression(mut self, compression: FieldCompression) -> Self {
        self.compression = Some(compression);
        self
    }

    pub fn new_map_array(name: &str, sub_fields: Vec<Field>) -> Field {
        Self::new(name, Type::Map, false, true, Some(sub_fields), vec![])
    }
    pub fn new_schema(fields: Vec<Field>) -> Field {
        Self::new_map("*", fields)
    }
    pub fn new_unindexed(name: &str, data_type: Type) -> Field {
        Self::new(name, data_type, false, false, None, vec![])
    }
    pub fn new_indexed(name: &str, data_type: Type, indices: Vec<IndexType>) -> Field {
        Self::new(name, data_type, false, false, None, indices)
    }
    pub fn new_unindexed_nullable(name: &str, data_type: Type) -> Field {
        Self::new(name, data_type, true, false, None, vec![])
    }
    pub fn new_indexed_nullable(name: &str, data_type: Type, indices: Vec<IndexType>) -> Field {
        Self::new(name, data_type, true, false, None, indices)
    }
    pub fn new_unindexed_array(name: &str, data_type: Type) -> Field {
        Self::new(name, data_type, false, true, None, vec![])
    }
    pub fn new_indexed_array(name: &str, data_type: Type, indices: Vec<IndexType>) -> Field {
        Self::new(name, data_type, false, true, None, indices)
    }
    pub fn new_indexed_array_nullable(
        name: &str,
        data_type: Type,
        indices: Vec<IndexType>,
    ) -> Field {
        Self::new(name, data_type, true, true, None, indices)
    }
    pub fn new_unindexed_array_nullable(name: &str, data_type: Type) -> Field {
        Self::new(name, data_type, true, true, None, vec![])
    }
    pub fn new_unindexed_vector(name: &str, data_type: Type, vector_size: u16) -> Field {
        Self::new_indexed_vector(name, data_type, vector_size, vec![])
    }
    pub fn new_indexed_vector(
        name: &str,
        data_type: Type,
        vector_size: u16,
        indices: Vec<IndexType>,
    ) -> Field {
        Self::new_vector(name, data_type, vector_size, indices, false)
    }
    pub fn new_indexed_vector_nullable(
        name: &str,
        data_type: Type,
        vector_size: u16,
        indices: Vec<IndexType>,
    ) -> Field {
        Self::new_vector(name, data_type, vector_size, indices, true)
    }
    pub fn new_unindexed_vector_nullable(name: &str, data_type: Type, vector_size: u16) -> Field {
        Self::new_vector(name, data_type, vector_size, vec![], true)
    }

    fn validate_for_registration(&self, path: &str) -> Result<(), NewSchemaError> {
        for index in &self.indices {
            self.validate_index_for_registration(path, index)?;
        }

        if let Some(sub_fields) = self.sub_fields.as_ref() {
            for field in sub_fields {
                let child_path = if path == "*" {
                    field.name.clone()
                } else {
                    format!("{path}.{}", field.name)
                };
                field.validate_for_registration(&child_path)?;
            }
        }

        Ok(())
    }

    fn validate_index_for_registration(
        &self,
        path: &str,
        index: &IndexType,
    ) -> Result<(), NewSchemaError> {
        match index {
            IndexType::Null => Ok(()),
            IndexType::Hashed => {
                if self.data_type == Type::Map {
                    Err(NewSchemaError::InvalidSchema(format!(
                        "field {path} is a map and only supports null indexing"
                    )))
                } else {
                    Ok(())
                }
            }
            IndexType::Ranged => {
                if self.data_type == Type::Map {
                    Err(NewSchemaError::InvalidSchema(format!(
                        "field {path} is a map and only supports null indexing"
                    )))
                } else if self.data_type == Type::String {
                    Err(NewSchemaError::InvalidSchema(format!(
                        "field {path} with type String does not support ranged indexing"
                    )))
                } else {
                    Ok(())
                }
            }
            IndexType::Embedding(_) | IndexType::Fulltext => {
                if self.data_type != Type::String {
                    Err(NewSchemaError::InvalidSchema(format!(
                        "field {path} with type {:?} does not support {:?} indexing",
                        self.data_type, index
                    )))
                } else {
                    Ok(())
                }
            }
            IndexType::Vector(_) => {
                if self.vector_size.is_none() {
                    Err(NewSchemaError::InvalidSchema(format!(
                        "field {path} requires vector_size for vector indexing"
                    )))
                } else if !Self::supports_vector_element_type(self.data_type) {
                    Err(NewSchemaError::InvalidSchema(format!(
                        "field {path} with type {:?} does not support vector indexing",
                        self.data_type
                    )))
                } else {
                    Ok(())
                }
            }
            IndexType::Statistics => Ok(()),
        }
    }

    fn supports_vector_element_type(data_type: Type) -> bool {
        matches!(
            data_type,
            Type::I8
                | Type::I16
                | Type::I32
                | Type::I64
                | Type::U8
                | Type::U16
                | Type::U32
                | Type::U64
                | Type::F32
                | Type::F64
        )
    }

    pub fn new_vector(
        name: &str,
        data_type: Type,
        vector_size: u16,
        indices: Vec<IndexType>,
        nullable: bool,
    ) -> Field {
        Field {
            data_type,
            nullable,
            is_array: false,
            vector_size: Some(vector_size),
            sub_fields: None,
            sub_fields_map: None,
            name: name.to_string(),
            name_id: types::key_hash(name),
            indices,
            offset: None,
            compression: None,
        }
    }
    fn assign_offsets(
        &mut self,
        offset: &mut usize,
        field_index: &mut BTreeMap<u64, Vec<usize>>,
        id_index: &mut BTreeMap<u64, Vec<u64>>,
        index_fields: &mut BTreeMap<u64, Vec<IndexType>>,
        name_path: String,
        field_path: Vec<usize>,
        id_path: Vec<u64>,
    ) {
        const POINTER_SIZE: usize = mem::size_of::<FieldPtr>();
        let is_field_var = self.is_var();
        let name_path_hash = hash_str(&name_path);
        let next_add;
        if self.is_array || self.nullable {
            // u32 as indication of the offset to the actual data
            // for nullable, it would be indicated as a pointer to the variable data area
            *offset = align_address(PTR_ALIGN, *offset);
            next_add = POINTER_SIZE;
        } else if let Some(ref mut subs) = self.sub_fields {
            next_add = 0; // Map add nothing
            let format_name = if name_path.is_empty() {
                name_path
            } else {
                format!("{}|", name_path)
            };
            subs.iter_mut().enumerate().for_each(|(i, f)| {
                let mut new_path = field_path.clone();
                let mut new_id = id_path.clone();
                new_path.push(i);
                new_id.push(f.name_id);
                let new_name_path = format!("{}{}", format_name, f.name);
                f.assign_offsets(
                    offset,
                    field_index,
                    id_index,
                    index_fields,
                    new_name_path,
                    new_path,
                    new_id,
                );
            });
        } else {
            if !is_field_var {
                let ty_align = types::align_of_type(self.data_type);
                *offset = align_address(ty_align, *offset);
                next_add = types::size_of_type(self.data_type);
            } else {
                *offset = align_address(PTR_ALIGN, *offset);
                next_add = POINTER_SIZE;
            }
        }
        if !field_path.is_empty() {
            field_index.insert(name_path_hash, field_path);
        }
        if !id_path.is_empty() {
            id_index.insert(name_path_hash, id_path);
            if !self.indices.is_empty() {
                index_fields.insert(name_path_hash, self.indices.clone());
            }
        }
        self.offset = Some(*offset);
        *offset += next_add;
        trace!(
            "Assigned field {} to {:?}, now at {}, var {}, offset moved {}",
            self.name,
            self.offset,
            offset,
            is_field_var,
            *offset - self.offset.unwrap()
        );
    }
    pub fn is_var(&self) -> bool {
        self.is_array
            || self.vector_size.is_some()
            || (!types::fixed_size(self.data_type) && self.sub_fields.is_none())
    }
    pub fn field_by_name_id(&self, name_id: &u64) -> Option<&Field> {
        self.sub_fields_map.as_ref().and_then(|m| {
            m.get(name_id)
                .and_then(|idx| self.sub_fields.as_ref().map(|f| &f[*idx]))
        })
    }
}

impl SchemaCompressionPlan {
    pub fn from_schema(schema: &Schema) -> Self {
        let mut fields = CompressedFieldPlans::new();
        let mut path = CompressedFieldPath::new();
        Self::collect(&schema.fields, &mut path, &mut fields);
        Self { fields }
    }

    fn collect(field: &Field, path: &mut CompressedFieldPath, out: &mut CompressedFieldPlans) {
        if let Some(sub_fields) = &field.sub_fields {
            for sub in sub_fields {
                path.push(sub.name_id);
                Self::collect(sub, path, out);
                path.pop();
            }
            return;
        }

        if !matches!(field.compression, Some(FieldCompression::Lz4)) {
            return;
        }

        if field.is_array || field.vector_size.is_some() {
            return;
        }

        let kind = match field.data_type {
            Type::String => CompressedFieldKind::String,
            Type::Bytes => CompressedFieldKind::Bytes,
            _ => return,
        };

        out.push(CompressedFieldPlan {
            path: path.clone(),
            kind,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

pub struct LocalSchemasMap {
    /// Every generation this node knows about, by its own vid. Reads resolve
    /// here, with the vid out of the cell header.
    schema_map: LFHashMap<SchemaVid, SchemaRef>,
    /// Name -> family. Only the current generation's name is bound.
    name_map: LFHashMap<String, SchemaUid>,
    /// Family -> the generation new writes belong in.
    handles: LFHashMap<SchemaUid, SchemaVid>,
}

pub struct LocalSchemasCache {
    map: Arc<LocalSchemasMap>,
}

impl LocalSchemasCache {
    pub async fn new<C>(group: &str, raft_client: &Arc<C>) -> Result<LocalSchemasCache, ExecError>
    where
        C: AsRaftPlaneClient + 'static,
    {
        Self::new_for_database(group, group, raft_client).await
    }

    pub async fn new_for_database<C>(
        group: &str,
        database_name: &str,
        raft_client: &Arc<C>,
    ) -> Result<LocalSchemasCache, ExecError>
    where
        C: AsRaftPlaneClient + 'static,
    {
        info!("Initializing local schema cache");
        let map = Arc::new(LocalSchemasMap::new());
        let m1 = map.clone();
        let m2 = map.clone();
        let m3 = map.clone();
        let sm =
            sm::client::SMClient::new(sm::generate_scoped_sm_id(group, database_name), raft_client);
        // SUBSCRIBE FIRST, THEN READ. The order is the correctness.
        //
        // `get_all` is a query, and queries round-robin across members gated
        // only by the CLIENT's own log cursor -- which, on a member that just
        // joined, is zero, so any stale peer (including this member's own
        // still-catching-up raft node) answers "successfully" with a schema
        // list from before the join. Events do not replay history, so a
        // schema created before the join was then missing FOREVER: measured
        // bimodal, in 0 ms or never (3 of 10 joins), as
        // `a_joining_member_is_filled_toward_the_mean` kept demonstrating,
        // and as Morpheus's later_cluster_member_caches_default_graph_base_schemas
        // flake has been showing from the outside.
        //
        // A subscription, though, registers through the COMMAND path, and a
        // committed command advances this client's cursor to its log index.
        // Subscribing first therefore pins `get_all` past the subscription
        // point: the LeftBehind protocol refuses any member that has not
        // applied at least that far. Every schema before the subscription is
        // then in the query answer, every one after arrives as an event, and
        // the overlap is harmless because `new_schema` is an idempotent
        // upsert.
        debug!("Subscribing schema events...");
        let _ = sm
            .on_schema_added(move |schema| {
                info!(
                    "Received schema_added event: schema {} ({})",
                    schema.vid, schema.name
                );
                m1.new_schema(schema);
                future::ready(()).boxed()
            })
            .await?;
        let _ = sm
            .on_schema_deleted(move |schema| {
                m2.del_schema(&schema);
                future::ready(()).boxed()
            })
            .await?;
        let _ = sm
            .on_schema_renamed(move |(uid, new_name)| {
                m3.rename_schema(uid, &new_name);
                future::ready(()).boxed()
            })
            .await?;
        let sm_data = sm.get_all().await?;
        {
            debug!("Importing {} schemas from cluster", sm_data.len());
            for schema in sm_data {
                trace!("Importing schema {}", schema.name);
                map.new_schema(schema);
            }
        }
        let schemas = LocalSchemasCache { map };
        info!("Local schema initialization completed");
        return Ok(schemas);
    }
    pub fn new_local(_group: &str) -> Self {
        let map = Arc::new(LocalSchemasMap::new());
        LocalSchemasCache { map }
    }
    pub fn get(&self, vid: &SchemaVid) -> Option<SchemaRef> {
        self.map.get(vid)
    }
    /// Resident bytes of the schema maps behind this cache.
    pub fn resident_bytes(&self) -> usize {
        self.map.resident_bytes()
    }
    pub fn debug_only_new_schema(&self, schema: Schema) {
        if !cfg!(debug_assertions) {
            panic!("for debug only");
        }
        let m = &self.map;
        m.new_schema(schema)
    }

    /// Register an internal/system schema locally (allowed in release builds)
    ///
    /// Use this for system schemas that must be registered before recovery
    /// (e.g., inverted index schemas). For user schemas, use the client API.
    ///
    /// **Important**: Internal schemas must have fixed, deterministic IDs
    /// (e.g., using `Schema::new_with_id()` with hash-based IDs) so that
    /// all nodes in the cluster independently register identical schemas
    /// without requiring raft consensus. Each node calls this during startup.
    pub fn register_internal_schema(&self, schema: Schema) {
        let m = &self.map;
        m.new_schema(schema)
    }
    /// Apply a committed rename locally. Normally driven by the
    /// `on_schema_renamed` subscription.
    pub fn apply_rename(&self, uid: SchemaUid, new_name: &str) {
        self.map.rename_schema(uid, new_name)
    }
    pub fn cache_schema_from_cluster(&self, schema: Schema) {
        let m = &self.map;
        m.new_schema(schema)
    }
    /// The generation a name currently writes into.
    pub fn name_to_id(&self, name: &str) -> Option<SchemaVid> {
        let m = &self.map;
        m.name_to_id(name)
    }

    /// The family a name belongs to. This is what a durable reference should
    /// keep: it survives both rename and evolution.
    pub fn uid_of_name(&self, name: &str) -> Option<SchemaUid> {
        self.map.uid_of_name(name)
    }

    /// The generation a family currently writes into.
    pub fn current_vid_of_uid(&self, uid: &SchemaUid) -> Option<SchemaVid> {
        self.map.current_vid_of_uid(uid)
    }
    pub fn count(&self) -> usize {
        let len = self.map.schema_map.len();
        debug!("Counted schema length {}", len);
        len
    }
    pub fn get_all(&self) -> Vec<Schema> {
        self.map.get_all()
    }
    pub fn fields_size(&self, schema_id: &SchemaVid, fields: &[u64]) -> Option<usize> {
        const DEFAULT_FIELD_SIZE: usize = 32; // Large default number for unknown field
        const DEFAULT_ARRAY_SIZE: usize = 32;
        self.get(schema_id).map(|schema| {
            fields
                .iter()
                .map(|field| {
                    if let Some(id_path) = schema.id_index.get(field) {
                        schema
                            .field_by_id_path(id_path.as_slice())
                            .map(|f| {
                                let mut type_size =
                                    f.data_type.size().unwrap_or(DEFAULT_FIELD_SIZE);
                                if f.is_array {
                                    type_size *= DEFAULT_ARRAY_SIZE;
                                }
                                type_size
                            })
                            .unwrap_or(DEFAULT_FIELD_SIZE)
                    } else {
                        DEFAULT_FIELD_SIZE
                    }
                })
                .sum::<usize>()
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NewSchemaError {
    NameExists(String),
    IdExists(u32),
    InvalidSchema(String),
    NotifyError(NotifyError),
    PostProcessError(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RenameSchemaError {
    SchemaDoesNotExist,
    NameExists(String),
    NotifyError(NotifyError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DelSchemaError {
    SchemaDoesNotExisted,
    NotifyError(NotifyError),
    PostProcessError(String),
}

impl LocalSchemasMap {
    /// Resident bytes of the two schema lookup maps.
    pub fn resident_bytes(&self) -> usize {
        (self.schema_map.resident_pages()
            + self.name_map.resident_pages()
            + self.handles.resident_pages())
            * 4096
    }

    pub fn new() -> Self {
        debug!("Schema map created");
        Self {
            schema_map: LFHashMap::with_capacity(32),
            name_map: LFHashMap::with_capacity(32),
            handles: LFHashMap::with_capacity(32),
        }
    }

    /// The generation a name currently writes into.
    pub fn get_by_name(&self, name: &str) -> Option<SchemaRef> {
        if let Some(id) = self.name_to_id(name) {
            return self.get(&id);
        }
        return None;
    }

    pub fn uid_of_name(&self, name: &str) -> Option<SchemaUid> {
        self.name_map.get(&name.to_string())
    }

    /// The generation a family currently writes into, if this node has heard
    /// of the family at all.
    pub fn current_vid_of_uid(&self, uid: &SchemaUid) -> Option<SchemaVid> {
        self.handles.get(uid)
    }

    pub fn get(&self, vid: &SchemaVid) -> Option<SchemaRef> {
        let res = self.schema_map.get(vid);
        debug!(
            "Gettting from schema map for {}, return res {}",
            vid,
            res.is_some()
        );
        return res;
    }

    /// Resolve a name to the generation new writes belong in: name -> family
    /// -> current generation. Two hops, because a name outlives any one
    /// layout.
    pub fn name_to_id(&self, name: &str) -> Option<SchemaVid> {
        self.uid_of_name(name)
            .and_then(|uid| self.current_vid_of_uid(&uid))
    }

    fn new_schema(&self, mut schema: Schema) {
        let name = schema.name.clone();
        let vid = schema.vid;
        let uid = schema.uid;
        let is_current = schema.status.is_current();
        schema.refresh_compression_plan();

        // The same schema legitimately arrives twice: `new_for_database`
        // subscribes before it reads, so anything created around that point
        // shows up in the `get_all` answer AND as an event. That overlap must
        // be an idempotent upsert.
        //
        // The guard compares FAMILIES now, not generations. A name binds to a
        // family for as long as the family lives, so a second generation of an
        // already-known schema is a redelivery to accept, not a collision --
        // whereas two different families claiming one name is still a real
        // conflict and still refused.
        if let Some(existing_uid) = self.name_map.get(&name) {
            if existing_uid != uid {
                error!(
                    "Schema name collision: name '{}' already belongs to family {}                      but an incoming schema claims it for family {}; keeping the existing binding",
                    name, existing_uid, uid
                );
                return;
            }
            debug!("Updating known schema family {} ({}) at {}", uid, name, vid);
        }

        // A superseded generation is still installed -- cells written under it
        // must stay readable -- but it must not claim the name or become what
        // new writes resolve to.
        self.schema_map.insert(vid, Arc::new(schema));
        if is_current {
            self.name_map.insert(name.clone(), uid);
            self.handles.insert(uid, vid);
            info!("Added schema to local cache: {} ({}) at {}", uid, name, vid);
        } else {
            debug!(
                "Cached superseded schema generation {} of family {} ({}), readable but not writable",
                vid, uid, name
            );
        }
    }

    fn get_all(&self) -> Vec<Schema> {
        let entries = self.schema_map.entries();
        entries
            .into_iter()
            .map(|(vid, s_ref)| {
                debug!(
                    "Get all local schema listed {}({}), tid {}",
                    vid,
                    s_ref.vid,
                    thread_id()
                );
                debug_assert_eq!(vid, s_ref.vid);
                (&*s_ref).clone()
            })
            .collect::<Vec<_>>()
    }

    /// Apply a rename that the state machine has already committed.
    ///
    /// Takes the family and the new name only: the old name is whatever this
    /// node currently has bound, which is the binding that actually needs
    /// removing. Trusting the event's idea of the old name instead would
    /// leave a stale binding behind on any node that had missed an earlier
    /// rename.
    fn rename_schema(&self, uid: SchemaUid, new_name: &str) {
        let Some(current_vid) = self.handles.get(&uid) else {
            debug!(
                "Ignoring rename of unknown schema family {} to {}",
                uid, new_name
            );
            return;
        };
        let Some(existing) = self.schema_map.get(&current_vid) else {
            debug!(
                "Ignoring rename of family {}: generation {} is not cached",
                uid, current_vid
            );
            return;
        };
        let old_name = existing.name.clone();
        if old_name == new_name {
            return;
        }
        let mut renamed = (*existing).clone();
        renamed.name = new_name.to_owned();
        self.schema_map.insert(current_vid, Arc::new(renamed));
        self.name_map.remove(&old_name);
        self.name_map.insert(new_name.to_owned(), uid);
        info!(
            "Local schema family {} renamed {} -> {}",
            uid, old_name, new_name
        );
    }

    fn del_schema(&self, name: &str) {
        let Some(uid) = self.name_map.remove(&(name.to_owned())) else {
            return;
        };
        self.handles.remove(&uid);
        // Every generation of the family, matching the state machine.
        let doomed: Vec<SchemaVid> = self
            .schema_map
            .entries()
            .into_iter()
            .filter(|(_, schema)| schema.uid == uid)
            .map(|(vid, _)| vid)
            .collect();
        for vid in &doomed {
            self.schema_map.remove(vid);
        }
        debug!(
            "Deleted local schema family {} ({}), {} generation(s)",
            uid,
            name,
            doomed.len()
        );
    }
}

pub struct ReadingRef<O, T: ?Sized> {
    _owner: O,
    reference: *const T,
}

pub type SchemaRef = Arc<Schema>;

impl<O, T: ?Sized> Deref for ReadingRef<O, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.reference }
    }
}

pub async fn post_schema_add(
    schema: &Schema,
    database_runtime: &Arc<DatabaseRuntime>,
) -> Result<(), String> {
    for (field, indices) in &schema.index_fields {
        for index in indices {
            let field_id = *field;
            let schema_id = schema.uid;
            match index {
                IndexType::Vector(config) => {
                    if let Some(indexer) = database_runtime.indexer() {
                        let _ = indexer
                            .clients
                            .vector_client
                            .new_index_with_config(schema_id, field_id, *config)
                            .await
                            .map_err(|e| format!("Error creating vector index: {:?}", e))?;
                    } else {
                        return Err(format!("Indexing not enabled"));
                    }
                }
                IndexType::Embedding(config) => {
                    if let Some(indexer) = database_runtime.indexer() {
                        let _ = indexer
                            .clients
                            .embedding_client
                            .new_index(schema_id, field_id, &config.model, config.vector)
                            .await
                            .map_err(|e| format!("Error creating embedding index: {:?}", e))?;
                    } else {
                        return Err(format!("Indexing not enabled"));
                    }
                }
                _ => {}
            }
        }
    }
    for (compound_id, compound) in &schema.compound_index_fields {
        for index in &compound.indices {
            let field_id = *compound_id;
            let schema_id = schema.uid;
            match index {
                IndexType::Vector(config) => {
                    if let Some(indexer) = database_runtime.indexer() {
                        let _ = indexer
                            .clients
                            .vector_client
                            .new_index_with_config(schema_id, field_id, *config)
                            .await
                            .map_err(|e| format!("Error creating vector index: {:?}", e))?;
                    } else {
                        return Err(format!("Indexing not enabled"));
                    }
                }
                IndexType::Embedding(config) => {
                    if let Some(indexer) = database_runtime.indexer() {
                        let _ = indexer
                            .clients
                            .embedding_client
                            .new_index(schema_id, field_id, &config.model, config.vector)
                            .await
                            .map_err(|e| format!("Error creating embedding index: {:?}", e))?;
                    } else {
                        return Err(format!("Indexing not enabled"));
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

pub async fn post_schema_delete(
    schema: &Schema,
    database_runtime: &Arc<DatabaseRuntime>,
) -> Result<(), String> {
    for (field, indices) in &schema.index_fields {
        for index in indices {
            let field_id = *field;
            let schema_id = schema.uid;
            match index {
                IndexType::Vector(_) => {
                    if let Some(indexer) = database_runtime.indexer() {
                        let _ = indexer
                            .clients
                            .vector_client
                            .delete_index(schema_id, field_id)
                            .await
                            .map_err(|e| format!("Error deleting vector index: {:?}", e))?;
                    } else {
                        return Err(format!("Indexing not enabled"));
                    }
                }
                IndexType::Embedding(_model) => {
                    if let Some(indexer) = database_runtime.indexer() {
                        let _ = indexer
                            .clients
                            .embedding_client
                            .delete_index(schema_id, field_id)
                            .await
                            .map_err(|e| format!("Error deleting embedding index: {:?}", e))?;
                    } else {
                        return Err(format!("Indexing not enabled"));
                    }
                }
                _ => {}
            }
        }
    }
    for (compound_id, compound) in &schema.compound_index_fields {
        for index in &compound.indices {
            let field_id = *compound_id;
            let schema_id = schema.uid;
            match index {
                IndexType::Vector(_) => {
                    if let Some(indexer) = database_runtime.indexer() {
                        let _ = indexer
                            .clients
                            .vector_client
                            .delete_index(schema_id, field_id)
                            .await
                            .map_err(|e| format!("Error deleting vector index: {:?}", e))?;
                    } else {
                        return Err(format!("Indexing not enabled"));
                    }
                }
                IndexType::Embedding(_model) => {
                    if let Some(indexer) = database_runtime.indexer() {
                        let _ = indexer
                            .clients
                            .embedding_client
                            .delete_index(schema_id, field_id)
                            .await
                            .map_err(|e| format!("Error deleting embedding index: {:?}", e))?;
                    } else {
                        return Err(format!("Indexing not enabled"));
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::builder::IndexError;
    use crate::index::embedding::{
        EmbeddingHit, EmbeddingIndexConfig, EmbeddingIndexerCore, EmbeddingModel,
        EmbeddingModelInfo,
    };
    use crate::index::vector::{
        CagraConfig, MetricEncoding, VectorHit, VectorIndexConfig, VectorIndexerCore,
    };
    use crate::server::{NebServer, ServerOptions, Service};
    use dovahkiin::types::Id;
    use dovahkiin::types::Type;
    use futures::{future::BoxFuture, FutureExt};
    use std::sync::{Arc, Mutex};

    fn cache_test_schema(id: u32, name: &str) -> Schema {
        Schema::new_with_id(
            id,
            name,
            None,
            Field::new_schema(vec![Field::new_unindexed("v", Type::U32)]),
            false,
            false,
        )
    }

    /// `new_for_database` subscribes BEFORE it reads, so a schema created
    /// around that moment arrives twice -- once in the `get_all` answer and
    /// once as an `on_schema_added` event. That overlap has to be a harmless
    /// upsert, because the alternative (read first) loses schemas forever on a
    /// joining member.
    #[test]
    fn redelivering_a_schema_is_an_idempotent_upsert() {
        let cache = LocalSchemasCache::new_local("");
        let schema = cache_test_schema(1, "person");

        cache.cache_schema_from_cluster(schema.clone());
        cache.cache_schema_from_cluster(schema.clone());

        assert_eq!(cache.count(), 1);
        assert_eq!(cache.uid_of_name("person"), Some(SchemaUid(1)));
        assert_eq!(cache.name_to_id("person"), Some(SchemaVid(1)));
    }

    /// Two different families claiming one name is a real conflict, and the
    /// binding that is already installed wins.
    #[test]
    fn a_name_claimed_by_a_second_family_is_refused() {
        let cache = LocalSchemasCache::new_local("");
        cache.cache_schema_from_cluster(cache_test_schema(1, "person"));
        cache.cache_schema_from_cluster(cache_test_schema(2, "person"));

        assert_eq!(
            cache.uid_of_name("person"),
            Some(SchemaUid(1)),
            "the established binding must survive a colliding one"
        );
    }

    /// A superseded generation still has to be cached -- cells written under
    /// it name it, and a read decodes with the exact vid in the header -- but
    /// it must not be what a write by name resolves to.
    #[test]
    fn a_superseded_generation_is_readable_but_not_writable() {
        let cache = LocalSchemasCache::new_local("");
        let mut gen0 = cache_test_schema(1, "person");
        let mut gen1 = gen0.clone();
        gen1.vid = SchemaVid(900);
        gen1.generation = 1;
        gen0.status = SchemaVersionStatus::Stale {
            superseded_by: gen1.vid,
        };

        cache.cache_schema_from_cluster(gen0);
        cache.cache_schema_from_cluster(gen1);

        assert!(
            cache.get(&SchemaVid(1)).is_some(),
            "a cell written under generation 0 must still decode"
        );
        assert_eq!(
            cache.name_to_id("person"),
            Some(SchemaVid(900)),
            "a write by name belongs in the current generation"
        );
        assert_eq!(
            cache.current_vid_of_uid(&SchemaUid(1)),
            Some(SchemaVid(900))
        );
    }

    #[test]
    fn a_local_rename_rebinds_the_name_and_keeps_the_generation() {
        let cache = LocalSchemasCache::new_local("");
        cache.cache_schema_from_cluster(cache_test_schema(1, "old"));

        cache.apply_rename(SchemaUid(1), "new");

        assert_eq!(cache.uid_of_name("new"), Some(SchemaUid(1)));
        assert_eq!(cache.uid_of_name("old"), None);
        assert_eq!(cache.name_to_id("new"), Some(SchemaVid(1)));
        assert_eq!(
            cache.get(&SchemaVid(1)).unwrap().name,
            "new",
            "the cached record must report the new name"
        );
    }

    /// A node that missed an earlier rename still has the right binding
    /// removed, because the handler uses its OWN idea of the current name
    /// rather than trusting one carried in the event.
    #[test]
    fn a_local_rename_removes_whatever_binding_this_node_actually_had() {
        let cache = LocalSchemasCache::new_local("");
        cache.cache_schema_from_cluster(cache_test_schema(1, "first"));
        cache.apply_rename(SchemaUid(1), "second");
        cache.apply_rename(SchemaUid(1), "third");

        assert_eq!(cache.uid_of_name("third"), Some(SchemaUid(1)));
        assert_eq!(cache.uid_of_name("second"), None);
        assert_eq!(cache.uid_of_name("first"), None);
        assert_eq!(cache.count(), 1, "renaming must not multiply records");
    }

    #[test]
    fn renaming_an_unknown_family_locally_is_ignored() {
        let cache = LocalSchemasCache::new_local("");
        cache.apply_rename(SchemaUid(404), "ghost");
        assert_eq!(cache.uid_of_name("ghost"), None);
        assert_eq!(cache.count(), 0);
    }

    /// The point of the whole exercise: a cell written before a rename is
    /// still readable after it. Nothing about a cell has ever mentioned a
    /// schema NAME -- its header names a generation -- so a rename cannot
    /// reach it.
    #[test]
    fn cells_written_before_a_rename_still_read_afterwards() {
        use crate::ram::cell::OwnedCell;
        use crate::ram::chunk::Chunks;
        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::types::{Map as _, OwnedMap, OwnedValue};
        use crate::server::ServerMeta;
        use dovahkiin::types::Id;

        let schema = Schema::new_with_id(
            1,
            "before",
            None,
            Field::new_schema(vec![Field::new_unindexed("v", Type::U32)]),
            false,
            false,
        );
        let schemas = LocalSchemasCache::new_local("");
        schemas.register_internal_schema(schema.clone());
        let meta = Arc::new(ServerMeta { schemas });
        let chunks = Chunks::new(1, SEGMENT_SIZE, meta.clone(), None, None, None, None);

        let mut value = OwnedMap::new();
        value.insert("v", OwnedValue::U32(7));
        let mut cell =
            OwnedCell::new_with_id(schema.vid, &Id::from_parts(1, 1), OwnedValue::Map(value));
        chunks.write_cell(&mut cell).unwrap();
        let id = cell.id();

        meta.schemas.apply_rename(SchemaUid(1), "after");
        assert_eq!(meta.schemas.uid_of_name("after"), Some(SchemaUid(1)));
        assert_eq!(
            meta.schemas.uid_of_name("before"),
            None,
            "the old name must stop resolving"
        );

        let read = chunks.read_cell(&id).expect("a cell must survive a rename");
        assert_eq!(read.data["v"].u32(), Some(&7));
    }

    fn post_schema_hook_server_options() -> ServerOptions {
        ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            raft_storage: None,
            index_enabled: true,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        }
    }

    async fn post_schema_hook_server(server_group: &str, _port: u16) -> Arc<NebServer> {
        let server_addr = crate::utils::test_port::unique_localhost_addr();
        NebServer::new_from_opts(
            &post_schema_hook_server_options(),
            &server_addr,
            server_group,
            async |_| {},
        )
        .await
        .unwrap()
    }

    fn cagra_vector_schema(schema_id: SchemaUid) -> Schema {
        Schema::new_with_id(
            schema_id.get(),
            "cagra_schema",
            None,
            Field::new_schema(vec![Field::new_indexed_vector(
                "embedding",
                Type::F32,
                4,
                vec![IndexType::Vector(VectorIndexConfig::cagra(
                    MetricEncoding::L2,
                    CagraConfig::default(),
                ))],
            )]),
            false,
            false,
        )
    }

    fn cagra_embedding_schema(schema_id: SchemaUid) -> Schema {
        Schema::new_with_id(
            schema_id.get(),
            "cagra_embedding_schema",
            None,
            Field::new_schema(vec![Field::new_indexed(
                "body",
                Type::String,
                vec![IndexType::Embedding(EmbeddingIndexConfig::new(
                    EmbeddingModel::from("test-model"),
                    VectorIndexConfig::cagra(MetricEncoding::L2, CagraConfig::default()),
                ))],
            )]),
            false,
            false,
        )
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum VectorCoreCall {
        NewIndex {
            schema_id: SchemaUid,
            field_id: u64,
            config: VectorIndexConfig,
        },
        DeleteIndex {
            schema_id: SchemaUid,
            field_id: u64,
        },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum EmbeddingCoreCall {
        NewIndex {
            schema_id: SchemaUid,
            field_id: u64,
            model: EmbeddingModel,
            vector_config: VectorIndexConfig,
        },
    }

    #[derive(Clone, Default)]
    struct RecordingVectorIndexerCore {
        calls: Arc<Mutex<Vec<VectorCoreCall>>>,
    }

    #[derive(Clone, Default)]
    struct RecordingEmbeddingIndexerCore {
        calls: Arc<Mutex<Vec<EmbeddingCoreCall>>>,
    }

    impl RecordingVectorIndexerCore {
        fn recorded_calls(&self) -> Vec<VectorCoreCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl RecordingEmbeddingIndexerCore {
        fn recorded_calls(&self) -> Vec<EmbeddingCoreCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl VectorIndexerCore for RecordingVectorIndexerCore {
        fn insert(
            &self,
            _cell_id: &Id,
            _schema_id: SchemaUid,
            _field_id: u64,
            _metric_encoding: MetricEncoding,
            _config: VectorIndexConfig,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            async { Ok(()) }.boxed()
        }

        fn remove(
            &self,
            _cell_id: &Id,
            _schema_id: SchemaUid,
            _field_id: u64,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            async { Ok(()) }.boxed()
        }

        fn search(
            &self,
            _schema_id: SchemaUid,
            _field_id: u64,
            _query_vector: &[f32],
            _limit: usize,
            _ef_search: Option<u16>,
        ) -> BoxFuture<'_, Result<Vec<VectorHit>, IndexError>> {
            async { Ok(vec![]) }.boxed()
        }

        fn new_index_with_config(
            &self,
            schema_id: SchemaUid,
            field_id: u64,
            config: VectorIndexConfig,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            let calls = self.calls.clone();
            async move {
                calls.lock().unwrap().push(VectorCoreCall::NewIndex {
                    schema_id,
                    field_id,
                    config,
                });
                Ok(())
            }
            .boxed()
        }

        fn delete_index(
            &self,
            schema_id: SchemaUid,
            field_id: u64,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            let calls = self.calls.clone();
            async move {
                calls.lock().unwrap().push(VectorCoreCall::DeleteIndex {
                    schema_id,
                    field_id,
                });
                Ok(())
            }
            .boxed()
        }
    }

    impl EmbeddingIndexerCore for RecordingEmbeddingIndexerCore {
        fn list_models(&self) -> BoxFuture<'_, Result<Vec<EmbeddingModelInfo>, IndexError>> {
            async { Ok(vec![]) }.boxed()
        }

        fn insert(
            &self,
            _cell_id: &Id,
            _schema_id: SchemaUid,
            _field_id: u64,
            _model: &EmbeddingModel,
            _text: &str,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            async { Ok(()) }.boxed()
        }

        fn remove(
            &self,
            _cell_id: &Id,
            _schema_id: SchemaUid,
            _field_id: u64,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            async { Ok(()) }.boxed()
        }

        fn search(
            &self,
            _schema_id: SchemaUid,
            _field_id: u64,
            _query: &str,
            _limit: usize,
        ) -> BoxFuture<'_, Result<Vec<EmbeddingHit>, IndexError>> {
            async { Ok(vec![]) }.boxed()
        }

        fn new_index(
            &self,
            schema_id: SchemaUid,
            field_id: u64,
            model: &EmbeddingModel,
            vector_config: VectorIndexConfig,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            let calls = self.calls.clone();
            let model = model.clone();
            async move {
                calls.lock().unwrap().push(EmbeddingCoreCall::NewIndex {
                    schema_id,
                    field_id,
                    model,
                    vector_config,
                });
                Ok(())
            }
            .boxed()
        }

        fn delete_index(
            &self,
            _schema_id: SchemaUid,
            _field_id: u64,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            async { Ok(()) }.boxed()
        }
    }

    fn install_recording_vector_core(server: &Arc<NebServer>) -> RecordingVectorIndexerCore {
        let vector_core = RecordingVectorIndexerCore::default();
        let added = server
            .current_database()
            .indexer()
            .expect("indexer should be enabled")
            .clients
            .vector_client
            .set_vector_index_core(vector_core.clone());

        assert!(added, "vector core should be installed once");

        vector_core
    }

    fn install_recording_embedding_core(server: &Arc<NebServer>) -> RecordingEmbeddingIndexerCore {
        let embedding_core = RecordingEmbeddingIndexerCore::default();
        let added = server
            .current_database()
            .indexer()
            .expect("indexer should be enabled")
            .clients
            .embedding_client
            .set_embedding_index_core(embedding_core.clone());

        assert!(added, "embedding core should be installed once");

        embedding_core
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn post_schema_add_passes_cagra_config_to_vector_core() {
        let _ = env_logger::try_init();
        let server =
            post_schema_hook_server("post_schema_add_passes_cagra_config_to_vector_core", 5481)
                .await;
        let vector_core = install_recording_vector_core(&server);
        let schema = cagra_vector_schema(SchemaUid(77));
        let field_id = *schema
            .index_fields
            .keys()
            .next()
            .expect("vector field should be indexed");
        let config = VectorIndexConfig::cagra(MetricEncoding::L2, CagraConfig::default());

        post_schema_add(&schema, &server.current_database())
            .await
            .expect("cagra config should be forwarded to the vector core");

        assert_eq!(
            vector_core.recorded_calls(),
            vec![VectorCoreCall::NewIndex {
                schema_id: SchemaUid(77),
                field_id,
                config,
            }]
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn post_schema_add_passes_cagra_config_to_embedding_core() {
        let _ = env_logger::try_init();
        let server = post_schema_hook_server(
            "post_schema_add_passes_cagra_config_to_embedding_core",
            5483,
        )
        .await;
        let embedding_core = install_recording_embedding_core(&server);
        let schema = cagra_embedding_schema(SchemaUid(79));
        let field_id = *schema
            .index_fields
            .keys()
            .next()
            .expect("embedding field should be indexed");
        let vector_config = VectorIndexConfig::cagra(MetricEncoding::L2, CagraConfig::default());

        post_schema_add(&schema, &server.current_database())
            .await
            .expect("embedding CAGRA config should be forwarded to embedding core");

        assert_eq!(
            embedding_core.recorded_calls(),
            vec![EmbeddingCoreCall::NewIndex {
                schema_id: SchemaUid(79),
                field_id,
                model: EmbeddingModel::from("test-model"),
                vector_config,
            }]
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn post_schema_delete_runs_vector_cleanup_for_cagra_schema() {
        let _ = env_logger::try_init();
        let server = post_schema_hook_server(
            "post_schema_delete_runs_vector_cleanup_for_cagra_schema",
            5482,
        )
        .await;
        let vector_core = install_recording_vector_core(&server);
        let schema = cagra_vector_schema(SchemaUid(78));
        let field_id = *schema
            .index_fields
            .keys()
            .next()
            .expect("vector field should be indexed");

        post_schema_delete(&schema, &server.current_database())
            .await
            .expect("vector cleanup should be forwarded to the vector core");

        assert_eq!(
            vector_core.recorded_calls(),
            vec![VectorCoreCall::DeleteIndex {
                schema_id: SchemaUid(78),
                field_id,
            }]
        );

        server.shutdown().await;
    }
}
