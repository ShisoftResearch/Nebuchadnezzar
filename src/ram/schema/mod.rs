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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub id: u32,
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
            id: 0,
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
        schema.id = id;
        schema
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
    schema_map: LFHashMap<u32, SchemaRef>,
    name_map: LFHashMap<String, u32>,
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
        let sm =
            sm::client::SMClient::new(sm::generate_scoped_sm_id(group, database_name), raft_client);
        let sm_data = sm.get_all().await?;
        {
            debug!("Importing {} schemas from cluster", sm_data.len());
            for schema in sm_data {
                trace!("Importing schema {}", schema.name);
                map.new_schema(schema);
            }
        }
        debug!("Subscribing schema events...");
        let _ = sm
            .on_schema_added(move |schema| {
                info!(
                    "Received schema_added event: schema {} ({})",
                    schema.id, schema.name
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
        let schemas = LocalSchemasCache { map };
        info!("Local schema initialization completed");
        return Ok(schemas);
    }
    pub fn new_local(_group: &str) -> Self {
        let map = Arc::new(LocalSchemasMap::new());
        LocalSchemasCache { map }
    }
    pub fn get(&self, id: &u32) -> Option<SchemaRef> {
        self.map.get(id)
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
    pub fn cache_schema_from_cluster(&self, schema: Schema) {
        let m = &self.map;
        m.new_schema(schema)
    }
    pub fn name_to_id(&self, name: &str) -> Option<u32> {
        let m = &self.map;
        m.name_to_id(name)
    }
    pub fn count(&self) -> usize {
        let len = self.map.schema_map.len();
        debug!("Counted schema length {}", len);
        len
    }
    pub fn get_all(&self) -> Vec<Schema> {
        self.map.get_all()
    }
    pub fn fields_size(&self, schema_id: &u32, fields: &[u64]) -> Option<usize> {
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
pub enum DelSchemaError {
    SchemaDoesNotExisted,
    NotifyError(NotifyError),
    PostProcessError(String),
}

impl LocalSchemasMap {
    pub fn new() -> Self {
        debug!("Schema map created");
        Self {
            schema_map: LFHashMap::with_capacity(32),
            name_map: LFHashMap::with_capacity(32),
        }
    }

    pub fn get_by_name(&self, name: &str) -> Option<SchemaRef> {
        if let Some(id) = self.name_to_id(name) {
            return self.get(&id);
        }
        return None;
    }

    pub fn get(&self, id: &u32) -> Option<SchemaRef> {
        let res = self.schema_map.get(id);
        debug!(
            "Gettting from schema map for {}, return res {}",
            id,
            res.is_some()
        );
        return res;
    }

    pub fn name_to_id(&self, name: &str) -> Option<u32> {
        self.name_map.get(&name.to_string()).map(|id| id as u32)
    }

    fn new_schema(&self, mut schema: Schema) {
        let name = schema.name.clone();
        let id = schema.id;
        schema.refresh_compression_plan();

        // Check if schema already exists (could happen during subscription race)
        if let Some(existing_id) = self.name_map.get(&name) {
            if existing_id != id {
                error!(
                    "Schema name collision: name '{}' already mapped to ID {} but new schema has ID {}",
                    name, existing_id, id
                );
                // Don't overwrite - keep the existing mapping
                return;
            } else {
                // Same ID, just update the schema (may have been modified)
                debug!("Updating existing schema {} ({})", id, name);
            }
        }

        self.name_map.insert(name.clone(), id);
        self.schema_map.insert(id, Arc::new(schema));
        info!("Added schema to local cache: {} ({})", id, name);
    }

    fn get_all(&self) -> Vec<Schema> {
        let entries = self.schema_map.entries();
        entries
            .into_iter()
            .map(|(id, s_ref)| {
                debug!(
                    "Get all local schema listed {}({}), tid {}",
                    id,
                    s_ref.id,
                    thread_id()
                );
                debug_assert_eq!(id, s_ref.id);
                (&*s_ref).clone()
            })
            .collect::<Vec<_>>()
    }

    fn del_schema(&self, name: &str) {
        if let Some(id) = self.name_map.remove(&(name.to_owned())) {
            self.schema_map.remove(&id);
            debug!("Deleted local schema {} with id {}", name, id);
        }
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
            let schema_id = schema.id;
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
            let schema_id = schema.id;
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
            let schema_id = schema.id;
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
            let schema_id = schema.id;
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

    fn post_schema_hook_server_options() -> ServerOptions {
        ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            history_retention_ms: 300_000,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: true,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        }
    }

    async fn post_schema_hook_server(server_group: &str, port: u16) -> Arc<NebServer> {
        let server_addr = format!("127.0.0.1:{port}");
        NebServer::new_from_opts(
            &post_schema_hook_server_options(),
            &server_addr,
            server_group,
            async |_| {},
        )
        .await
        .unwrap()
    }

    fn cagra_vector_schema(schema_id: u32) -> Schema {
        Schema::new_with_id(
            schema_id,
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

    fn cagra_embedding_schema(schema_id: u32) -> Schema {
        Schema::new_with_id(
            schema_id,
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
            schema_id: u32,
            field_id: u64,
            config: VectorIndexConfig,
        },
        DeleteIndex {
            schema_id: u32,
            field_id: u64,
        },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum EmbeddingCoreCall {
        NewIndex {
            schema_id: u32,
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
            _schema_id: u32,
            _field_id: u64,
            _metric_encoding: MetricEncoding,
            _config: VectorIndexConfig,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            async { Ok(()) }.boxed()
        }

        fn remove(
            &self,
            _cell_id: &Id,
            _schema_id: u32,
            _field_id: u64,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            async { Ok(()) }.boxed()
        }

        fn search(
            &self,
            _schema_id: u32,
            _field_id: u64,
            _query_vector: &[f32],
            _limit: usize,
            _ef_search: Option<u16>,
        ) -> BoxFuture<'_, Result<Vec<VectorHit>, IndexError>> {
            async { Ok(vec![]) }.boxed()
        }

        fn new_index_with_config(
            &self,
            schema_id: u32,
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
            schema_id: u32,
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
            _schema_id: u32,
            _field_id: u64,
            _model: &EmbeddingModel,
            _text: &str,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            async { Ok(()) }.boxed()
        }

        fn remove(
            &self,
            _cell_id: &Id,
            _schema_id: u32,
            _field_id: u64,
        ) -> BoxFuture<'_, Result<(), IndexError>> {
            async { Ok(()) }.boxed()
        }

        fn search(
            &self,
            _schema_id: u32,
            _field_id: u64,
            _query: &str,
            _limit: usize,
        ) -> BoxFuture<'_, Result<Vec<EmbeddingHit>, IndexError>> {
            async { Ok(vec![]) }.boxed()
        }

        fn new_index(
            &self,
            schema_id: u32,
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
            _schema_id: u32,
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
        let schema = cagra_vector_schema(77);
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
                schema_id: 77,
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
        let schema = cagra_embedding_schema(79);
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
                schema_id: 79,
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
        let schema = cagra_vector_schema(78);
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
                schema_id: 78,
                field_id,
            }]
        );

        server.shutdown().await;
    }
}
