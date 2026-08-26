use bifrost::raft::client::AsRaftPlaneClient;
use bifrost::raft::state_machine::callback::server::NotifyError;
use bifrost::raft::state_machine::master::ExecError;
use bifrost_hasher::hash_str;

use dovahkiin::types::{OwnedValue, Type};
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

/// How a generation was produced from the one it superseded.
///
/// Empty for a generation-0 record, which superseded nothing.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaTransform {
    /// Field names to purge from a DYNAMIC schema's dynamic region.
    ///
    /// Cumulative: each generation carries every name its family has ever
    /// dropped, not just the ones it dropped itself. That is what lets a
    /// generation-0 cell migrate straight to generation 3 -- the single hop
    /// resolution deliberately takes -- without replaying the generations in
    /// between to discover what they removed.
    ///
    /// A name that is later re-declared stops being a drop in practice: a
    /// declared field never reaches the dynamic region at all.
    pub dynamic_drops: Vec<u64>,
    /// Where a field's value used to live: `(historical name, current name)`,
    /// oldest first.
    ///
    /// Cumulative, like `dynamic_drops`, and for the same reason -- migration
    /// takes one hop, so a generation that knew only its own renames could not
    /// find a value last seen two generations ago. Folding each hop forward
    /// (`a -> b` then `b -> c` becomes `a -> c` plus `b -> c`) means the target
    /// record alone can resolve any ancestor's field name, and NO ordered
    /// replay of the generations in between is needed.
    pub renames: Vec<(u64, u64)>,
}

impl SchemaTransform {
    /// Every name a value for `current` might be stored under, newest first.
    ///
    /// Newest first because a cell only one generation behind holds the more
    /// recent name, and taking the oldest match would read a field that
    /// generation had already stopped using.
    pub fn historical_names(&self, current: u64) -> impl Iterator<Item = u64> + '_ {
        self.renames
            .iter()
            .rev()
            .filter(move |(_, to)| *to == current)
            .map(|(from, _)| *from)
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
    /// How this generation was produced from its predecessor.
    #[serde(default)]
    pub transform: SchemaTransform,
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
            transform: SchemaTransform::default(),
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

    /// Declare that these field names are being removed from a dynamic
    /// schema's dynamic region rather than merely undeclared.
    ///
    /// Needed because on an `is_dynamic` schema an undeclared field is not
    /// dropped -- it falls through to the dynamic region and is re-encoded, so
    /// removing it from the schema alone preserves it forever.
    pub fn with_dynamic_drops(mut self, names: &[&str]) -> Schema {
        for name in names {
            let id = types::key_hash(name);
            if !self.transform.dynamic_drops.contains(&id) {
                self.transform.dynamic_drops.push(id);
            }
        }
        self
    }

    /// Declare that a field's values used to be stored under another name.
    ///
    /// Without this an evolution that renames a field reads as a drop plus an
    /// add: the old values are abandoned and the new field comes back empty.
    pub fn with_renamed_field(mut self, from: &str, to: &str) -> Schema {
        let pair = (types::key_hash(from), types::key_hash(to));
        if !self.transform.renames.contains(&pair) {
            self.transform.renames.push(pair);
        }
        self
    }

    pub fn with_blobs(mut self, blobs: bool) -> Schema {
        self.blobs = blobs;
        self
    }

    /// What it would take to move a family's cells from `self` to `proposed`.
    ///
    /// `Identity` means the existing encoder already handles it: decode a cell
    /// under the old generation, encode it under the new one, done. That works
    /// for more than it looks like, because `plan_write_field` rejects exactly
    /// one shape -- a non-nullable field with no value -- and a map key that is
    /// absent decodes as `Null`. So adding a nullable field and dropping a
    /// field both fall out for free.
    ///
    /// Anything this cannot express mechanically is refused rather than
    /// guessed at, and waits for the transform engine.
    pub fn classify_evolution(&self, proposed: &Schema) -> EvolutionKind {
        // A cell's id is derived from its family and its key value, so moving
        // the key would change the id of every future write while leaving
        // every existing cell addressed the old way. There is no transform
        // that fixes that; the cells would simply stop being findable.
        if self.key_field != proposed.key_field {
            return EvolutionKind::Illegal(format!(
                "the key field defines every keyed cell's id, so changing it ({:?} -> {:?}) \
                 would orphan every cell already written",
                self.str_key_field, proposed.str_key_field
            ));
        }

        // A dynamic schema carries fields the schema does not declare, in a
        // region a static encoder does not write. Going static would drop them
        // silently on the next rewrite.
        if self.is_dynamic && !proposed.is_dynamic {
            return EvolutionKind::NeedsTransform(
                "a dynamic schema holds fields the schema does not declare; making it static \
                 would drop them on re-encode"
                    .to_owned(),
            );
        }

        // A swap -- `a -> b` and `b -> a` in one step -- is order-dependent, and
        // the lookup that resolves a rename has no idea which generation a cell
        // came from. Refuse rather than resolve it wrongly half the time.
        for (from, to) in &proposed.transform.renames {
            if proposed
                .transform
                .renames
                .iter()
                .any(|(other_from, other_to)| other_from == to && other_to == from)
            {
                return EvolutionKind::Illegal(
                    "a rename that swaps two field names cannot be resolved without knowing \
                     which generation each cell came from"
                        .to_owned(),
                );
            }
        }

        for (path_hash, id_path) in &self.id_index {
            let Some(before) = self.field_by_id_path(id_path) else {
                continue;
            };
            if before.sub_fields.is_some() {
                // A map node carries no value of its own; its leaves are
                // compared on their own entries.
                continue;
            }
            match proposed.id_index.get(path_hash) {
                None => {
                    // Renamed away, not dropped: its values are claimed by
                    // whatever field the rename points at.
                    if proposed
                        .transform
                        .renames
                        .iter()
                        .any(|(from, _)| *from == before.name_id)
                    {
                        continue;
                    }
                    // Dropped. The new encoder simply never reads it -- unless
                    // the schema is dynamic, in which case an undeclared field
                    // falls through to the dynamic region and is re-encoded
                    // rather than dropped, which is not what dropping means.
                    if proposed.is_dynamic
                        && !proposed.transform.dynamic_drops.contains(&before.name_id)
                    {
                        return EvolutionKind::NeedsTransform(format!(
                            "dropping `{}` from a dynamic schema would re-encode it into the \
                             dynamic region instead of removing it; list it in the schema's \
                             dynamic drops to remove it for real",
                            before.name
                        ));
                    }
                }
                Some(after_path) => {
                    let Some(after) = proposed.field_by_id_path(after_path) else {
                        continue;
                    };
                    if let Some(reason) = Self::field_needs_transform(before, after) {
                        return EvolutionKind::NeedsTransform(reason);
                    }
                }
            }
        }

        for (path_hash, id_path) in &proposed.id_index {
            if self.id_index.contains_key(path_hash) {
                continue;
            }
            let Some(added) = proposed.field_by_id_path(id_path) else {
                continue;
            };
            if added.sub_fields.is_some() {
                continue;
            }
            // An added field has no value in any existing cell. Nullable, that
            // encodes as null; otherwise the encoder refuses, and a default
            // has to come from somewhere the transform engine can supply.
            // A field that inherits an older field's values is not new, so it
            // needs no default -- the values are already there.
            let inherits = proposed
                .transform
                .renames
                .iter()
                .any(|(_, to)| *to == added.name_id);
            if inherits {
                // ...but only if nothing else still claims the name it takes
                // its values from. Two fields reading one source is ambiguous,
                // and a lookup cannot tell which was meant.
                for (from, _) in proposed.transform.renames.iter() {
                    if proposed.id_index.contains_key(from) {
                        return EvolutionKind::Illegal(format!(
                            "`{}` takes its values from a name this schema still declares; \
                             a rename cannot reuse a live field's name, because a lookup \
                             cannot tell the two apart",
                            added.name
                        ));
                    }
                }
                continue;
            }
            if !added.nullable && added.default.is_none() {
                return EvolutionKind::NeedsTransform(format!(
                    "`{}` is new and not nullable, so existing cells have no value to encode \
                     for it; give it a default to make this evolution expressible",
                    added.name
                ));
            }
            if added.default.is_some() && !Self::default_is_encodable(added) {
                return EvolutionKind::NeedsTransform(format!(
                    "`{}` has a default, but defaults are only injected for scalar fields; \
                     an array, vector or map default would be admitted here and then fail \
                     to encode during migration",
                    added.name
                ));
            }
        }

        EvolutionKind::Identity
    }

    /// Whether this field's default is one the encoder will actually inject.
    ///
    /// Only scalars. The array, vector and map paths reject a null value
    /// before the default substitution is reached, so admitting a composite
    /// default would produce an evolution that passes classification and then
    /// strands every cell it touches -- the failure mode this design exists to
    /// avoid.
    fn default_is_encodable(field: &Field) -> bool {
        !field.is_array && field.vector_size.is_none() && field.sub_fields.is_none()
    }

    /// Why re-encoding a value of `before` as `after` is not mechanical, if it
    /// is not. Index membership, compression and added nullability are all
    /// absent here on purpose: they change how a value is stored or found, not
    /// what it is.
    fn field_needs_transform(before: &Field, after: &Field) -> Option<String> {
        if before.data_type != after.data_type
            && !widening_exists(before.data_type, after.data_type)
        {
            return Some(format!(
                "`{}` changes type ({:?} -> {:?}), and that is not a widening this can perform \
                 without losing range or precision",
                before.name, before.data_type, after.data_type
            ));
        }
        if before.is_array != after.is_array {
            return Some(format!(
                "`{}` changes between a scalar and an array",
                before.name
            ));
        }
        if before.vector_size != after.vector_size {
            return Some(format!(
                "`{}` changes vector width ({:?} -> {:?})",
                before.name, before.vector_size, after.vector_size
            ));
        }
        if before.nullable
            && !after.nullable
            && after.default.is_some()
            && !Self::default_is_encodable(after)
        {
            return Some(format!(
                "`{}` stops being nullable and has a default, but defaults are only injected \
                 for scalar fields",
                before.name
            ));
        }
        if before.nullable && !after.nullable && after.default.is_none() {
            return Some(format!(
                "`{}` stops being nullable, and cells already holding null for it have no \
                 value to encode; give it a default to make this evolution expressible",
                before.name
            ));
        }
        None
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
    /// What a cell gets for this field when it has no value of its own: one
    /// written before the field existed, or one holding null for a field that
    /// has since stopped being nullable.
    ///
    /// `None` means genuinely required, and an evolution that introduces the
    /// field is refused rather than admitted and then stranded.
    #[serde(default)]
    pub default: Option<OwnedValue>,
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
            default: None,
        }
    }

    /// Give this field a value to fall back on when a cell has none.
    ///
    /// This is what lets a later evolution add the field as required, or make
    /// it stop being nullable, without stranding every cell written before the
    /// change.
    pub fn with_default(mut self, default: OwnedValue) -> Self {
        self.default = Some(default);
        self
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
            default: None,
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
        let m4 = map.clone();
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
        let _ = sm
            .on_schema_evolved(move |schema| {
                m4.evolve_schema(schema);
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
    /// Apply a committed evolution locally. Normally driven by the
    /// `on_schema_evolved` subscription.
    pub fn apply_evolution(&self, evolved: Schema) {
        self.map.evolve_schema(evolved)
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

    /// The generation a write naming `vid` must actually land in.
    ///
    /// The single place a write is redirected. A caller can be holding a vid
    /// that has since been superseded -- a long-lived client, a replayed
    /// batch, a migration re-applying a cell -- and persisting under it would
    /// keep producing cells in a layout that is supposed to be draining.
    ///
    /// Resolution is `vid -> family -> current generation`: one hop, whatever
    /// the generation depth, because the record carries its own family rather
    /// than a chain of `superseded_by` links.
    ///
    /// Returns `None` when the vid is unknown, or when its family has no
    /// current generation, so the caller keeps reporting
    /// `SchemaDoesNotExisted` exactly as before.
    pub fn resolve_for_write(&self, vid: SchemaVid) -> Option<SchemaRef> {
        let named = self.get(&vid)?;
        if named.status.is_current() {
            return Some(named);
        }
        let current_vid = self.current_vid_of_uid(&named.uid)?;
        if current_vid == vid {
            // The handle disagrees with the record's own status. Trust the
            // record and refuse, rather than write into a layout something has
            // already marked superseded.
            error!(
                "Schema family {} names {} as current, but that record is marked superseded",
                named.uid, vid
            );
            return None;
        }
        self.get(&current_vid)
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

/// A change to make to a schema, described as a delta rather than a shape.
///
/// Built against no particular schema and applied to whichever generation is
/// current at the time. The point is that unchanged fields are carried across
/// automatically: describing the whole target shape by hand means a field left
/// off the list is silently dropped, which is a poor way to lose a column.
///
/// The operations map onto the transforms the engine can perform, and an edit
/// that produces an inexpressible schema is refused by `classify_evolution`
/// exactly as a hand-built one would be.
#[derive(Debug, Clone, Default)]
pub struct SchemaEdit {
    ops: Vec<EditOp>,
}

#[derive(Debug, Clone)]
enum EditOp {
    AddField(Field),
    DropField(String),
    RenameField {
        from: String,
        to: String,
    },
    RetypeField {
        name: String,
        to: Type,
    },
    SetDefault {
        name: String,
        default: OwnedValue,
    },
    SetNullable {
        name: String,
        nullable: bool,
    },
    SetIndices {
        name: String,
        indices: Vec<IndexType>,
    },
    SetScannable(bool),
    SetDynamic(bool),
    SetBlobs(bool),
    AddCompoundIndex {
        name: String,
        fields: Vec<String>,
        indices: Vec<IndexType>,
    },
    DropCompoundIndex(String),
}

/// Why an edit could not be turned into a schema. Distinct from
/// [`EvolutionKind`], which judges the RESULT: these are complaints about the
/// edit itself, caught before anything is proposed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditError {
    /// The edit touches a field the current generation does not declare.
    NoSuchField(String),
    /// The edit drops a compound index that does not exist.
    NoSuchCompoundIndex(String),
    /// The edit adds a field that already exists.
    FieldExists(String),
    /// Only top-level fields can be edited this way.
    NotATopLevelField(String),
}

impl SchemaEdit {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a field. Give it a `default` if it is not nullable, or the
    /// evolution will be refused for the cells that have no value for it.
    pub fn add(mut self, field: Field) -> Self {
        self.ops.push(EditOp::AddField(field));
        self
    }

    pub fn drop(mut self, name: &str) -> Self {
        self.ops.push(EditOp::DropField(name.to_owned()));
        self
    }

    /// Rename a field, carrying its values across.
    pub fn rename(mut self, from: &str, to: &str) -> Self {
        self.ops.push(EditOp::RenameField {
            from: from.to_owned(),
            to: to.to_owned(),
        });
        self
    }

    /// Change a field's type. Only lossless widenings are accepted.
    pub fn retype(mut self, name: &str, to: Type) -> Self {
        self.ops.push(EditOp::RetypeField {
            name: name.to_owned(),
            to,
        });
        self
    }

    pub fn set_default(mut self, name: &str, default: OwnedValue) -> Self {
        self.ops.push(EditOp::SetDefault {
            name: name.to_owned(),
            default,
        });
        self
    }

    pub fn set_nullable(mut self, name: &str, nullable: bool) -> Self {
        self.ops.push(EditOp::SetNullable {
            name: name.to_owned(),
            nullable,
        });
        self
    }

    pub fn set_indices(mut self, name: &str, indices: Vec<IndexType>) -> Self {
        self.ops.push(EditOp::SetIndices {
            name: name.to_owned(),
            indices,
        });
        self
    }

    pub fn set_scannable(mut self, scannable: bool) -> Self {
        self.ops.push(EditOp::SetScannable(scannable));
        self
    }

    pub fn set_dynamic(mut self, dynamic: bool) -> Self {
        self.ops.push(EditOp::SetDynamic(dynamic));
        self
    }

    /// Move the family between blob and regular segments for FUTURE writes.
    ///
    /// Cells already written stay where they are, in the segment class that
    /// was current when they were written -- as with every other evolution,
    /// nothing is rewritten.
    pub fn set_blobs(mut self, blobs: bool) -> Self {
        self.ops.push(EditOp::SetBlobs(blobs));
        self
    }

    /// Add or replace a compound index. Replacing by name, because a compound
    /// index is identified by its name and redefining one is the ordinary way
    /// to change its field list.
    pub fn add_compound_index(
        mut self,
        name: &str,
        fields: &[&str],
        indices: Vec<IndexType>,
    ) -> Self {
        self.ops.push(EditOp::AddCompoundIndex {
            name: name.to_owned(),
            fields: fields.iter().map(|f| (*f).to_owned()).collect(),
            indices,
        });
        self
    }

    pub fn drop_compound_index(mut self, name: &str) -> Self {
        self.ops.push(EditOp::DropCompoundIndex(name.to_owned()));
        self
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Turn this edit into the schema it describes, against `base`.
    ///
    /// Rebuilds through `Schema::new` rather than mutating a clone, so offsets,
    /// the field indexes and the compression plan are all recomputed from the
    /// resulting field list. Editing those by hand is how a schema ends up
    /// describing a layout it does not actually produce.
    pub fn apply(&self, base: &Schema) -> Result<Schema, EditError> {
        let Some(mut fields) = base.fields.sub_fields.clone() else {
            return Err(EditError::NotATopLevelField(base.name.clone()));
        };
        let mut renames: Vec<(String, String)> = Vec::new();
        let mut drops: Vec<String> = Vec::new();
        let mut is_scannable = base.is_scannable;
        let mut is_dynamic = base.is_dynamic;
        let mut blobs = base.blobs;
        // Rebuilt from the base rather than carried wholesale, so an index
        // naming a field this edit drops can be caught rather than left
        // pointing at nothing.
        let mut compound: std::collections::BTreeMap<u64, (String, Vec<String>, Vec<IndexType>)> =
            base.compound_index_fields
                .iter()
                .map(|(id, ci)| {
                    (
                        *id,
                        (ci.name.clone(), ci.fields.clone(), ci.indices.clone()),
                    )
                })
                .collect();

        let position = |fields: &[Field], name: &str| {
            let id = types::key_hash(name);
            fields.iter().position(|f| f.name_id == id)
        };

        for op in &self.ops {
            match op {
                EditOp::AddField(field) => {
                    if position(&fields, &field.name).is_some() {
                        return Err(EditError::FieldExists(field.name.clone()));
                    }
                    fields.push(field.clone());
                }
                EditOp::DropField(name) => {
                    let Some(at) = position(&fields, name) else {
                        return Err(EditError::NoSuchField(name.clone()));
                    };
                    fields.remove(at);
                    drops.push(name.clone());
                }
                EditOp::RenameField { from, to } => {
                    let Some(at) = position(&fields, from) else {
                        return Err(EditError::NoSuchField(from.clone()));
                    };
                    let existing = fields[at].clone();
                    // Rebuilt rather than renamed in place: `name_id` is
                    // derived from the name, and a field whose id disagrees
                    // with its name is unfindable.
                    let mut renamed = Field::new(
                        to,
                        existing.data_type,
                        existing.nullable,
                        existing.is_array,
                        existing.sub_fields.clone(),
                        existing.indices.clone(),
                    );
                    renamed.vector_size = existing.vector_size;
                    renamed.compression = existing.compression.clone();
                    renamed.default = existing.default.clone();
                    fields[at] = renamed;
                    renames.push((from.clone(), to.clone()));
                }
                EditOp::RetypeField { name, to } => {
                    let Some(at) = position(&fields, name) else {
                        return Err(EditError::NoSuchField(name.clone()));
                    };
                    fields[at].data_type = *to;
                }
                EditOp::SetDefault { name, default } => {
                    let Some(at) = position(&fields, name) else {
                        return Err(EditError::NoSuchField(name.clone()));
                    };
                    fields[at].default = Some(default.clone());
                }
                EditOp::SetNullable { name, nullable } => {
                    let Some(at) = position(&fields, name) else {
                        return Err(EditError::NoSuchField(name.clone()));
                    };
                    fields[at].nullable = *nullable;
                }
                EditOp::SetIndices { name, indices } => {
                    let Some(at) = position(&fields, name) else {
                        return Err(EditError::NoSuchField(name.clone()));
                    };
                    fields[at].indices = indices.clone();
                }
                EditOp::SetScannable(v) => is_scannable = *v,
                EditOp::SetDynamic(v) => is_dynamic = *v,
                EditOp::SetBlobs(v) => blobs = *v,
                EditOp::AddCompoundIndex {
                    name,
                    fields: index_fields,
                    indices,
                } => {
                    for field in index_fields {
                        if position(&fields, field).is_none() {
                            return Err(EditError::NoSuchField(field.clone()));
                        }
                    }
                    compound.insert(
                        types::key_hash(name),
                        (name.clone(), index_fields.clone(), indices.clone()),
                    );
                }
                EditOp::DropCompoundIndex(name) => {
                    if compound.remove(&types::key_hash(name)).is_none() {
                        return Err(EditError::NoSuchCompoundIndex(name.clone()));
                    }
                }
            }
        }

        let mut evolved = Schema::new(
            &base.name,
            base.str_key_field.clone(),
            Field::new_schema(fields),
            is_dynamic,
            is_scannable,
        );
        evolved.blobs = blobs;
        for (_, (name, index_fields, indices)) in compound {
            let refs: Vec<&str> = index_fields.iter().map(|f| f.as_str()).collect();
            // Rebuilt through `add_compound_index` so `field_ids` is derived
            // from the names rather than copied; a compound index whose ids
            // disagree with its field list indexes nothing.
            evolved.add_compound_index(
                &name,
                refs.iter().map(|f| (*f).to_owned()).collect(),
                indices,
            );
        }
        for (from, to) in &renames {
            evolved = evolved.with_renamed_field(from, to);
        }
        if is_dynamic && !drops.is_empty() {
            let names: Vec<&str> = drops.iter().map(|s| s.as_str()).collect();
            evolved = evolved.with_dynamic_drops(&names);
        }
        Ok(evolved)
    }
}

/// Widen `value` to `target`, if that can be done without losing anything.
///
/// Returns `None` when the value is already the target type (nothing to do) or
/// when no lossless widening exists. Deliberately never returns a value that
/// would lose precision or range: an evolution that admits at proposal time
/// and then strands an arbitrary subset of cells at migration time is worse
/// than one that is refused outright, so narrowing is not a coercion here.
///
/// Float targets are limited by mantissa width, not by byte size: `f32` holds
/// integers exactly only to 2^24, so `u32`/`i32` widen to `f64` but not to
/// `f32`. Getting that backwards would silently round large ids.
pub fn coerce_value(value: &OwnedValue, target: Type) -> Option<OwnedValue> {
    use OwnedValue::*;
    if value.base_type() == target {
        return None;
    }
    Some(match (value, target) {
        // Signed widening.
        (I8(v), Type::I16) => I16(*v as i16),
        (I8(v), Type::I32) => I32(*v as i32),
        (I8(v), Type::I64) => I64(*v as i64),
        (I16(v), Type::I32) => I32(*v as i32),
        (I16(v), Type::I64) => I64(*v as i64),
        (I32(v), Type::I64) => I64(*v as i64),

        // Unsigned widening.
        (U8(v), Type::U16) => U16(*v as u16),
        (U8(v), Type::U32) => U32(*v as u32),
        (U8(v), Type::U64) => U64(*v as u64),
        (U16(v), Type::U32) => U32(*v as u32),
        (U16(v), Type::U64) => U64(*v as u64),
        (U32(v), Type::U64) => U64(*v as u64),

        // Unsigned into a signed type wide enough to hold all of it.
        (U8(v), Type::I16) => I16(*v as i16),
        (U8(v), Type::I32) => I32(*v as i32),
        (U8(v), Type::I64) => I64(*v as i64),
        (U16(v), Type::I32) => I32(*v as i32),
        (U16(v), Type::I64) => I64(*v as i64),
        (U32(v), Type::I64) => I64(*v as i64),

        // Into floats, only where every value is exactly representable.
        (I8(v), Type::F32) => F32(*v as f32),
        (I16(v), Type::F32) => F32(*v as f32),
        (U8(v), Type::F32) => F32(*v as f32),
        (U16(v), Type::F32) => F32(*v as f32),
        (I8(v), Type::F64) => F64(*v as f64),
        (I16(v), Type::F64) => F64(*v as f64),
        (I32(v), Type::F64) => F64(*v as f64),
        (U8(v), Type::F64) => F64(*v as f64),
        (U16(v), Type::F64) => F64(*v as f64),
        (U32(v), Type::F64) => F64(*v as f64),
        (F32(v), Type::F64) => F64(*v as f64),

        _ => return None,
    })
}

/// Whether every value of `from` widens losslessly into `to`.
///
/// Answered by asking [`coerce_value`] itself with a probe value, so
/// admission and migration can never disagree about what is possible -- a
/// second hand-written table here is exactly how an evolution gets admitted
/// and then strands the cells it was admitted for.
pub fn widening_exists(from: Type, to: Type) -> bool {
    let probe = match from {
        Type::I8 => OwnedValue::I8(0),
        Type::I16 => OwnedValue::I16(0),
        Type::I32 => OwnedValue::I32(0),
        Type::I64 => OwnedValue::I64(0),
        Type::U8 => OwnedValue::U8(0),
        Type::U16 => OwnedValue::U16(0),
        Type::U32 => OwnedValue::U32(0),
        Type::U64 => OwnedValue::U64(0),
        Type::F32 => OwnedValue::F32(0.0),
        Type::F64 => OwnedValue::F64(0.0),
        _ => return false,
    };
    coerce_value(&probe, to).is_some()
}

/// What moving a family's cells from one generation to the next would take.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvolutionKind {
    /// Decode under the old generation, encode under the new one. No transform.
    Identity,
    /// Mechanically expressible, but not by the encoder alone. Refused until
    /// the transform engine exists.
    NeedsTransform(String),
    /// Not expressible at all, whatever machinery is available.
    Illegal(String),
}

/// What an evolution produced.
///
/// Carries the generation it superseded as well as the one it created, so the
/// caller can reconcile index namespaces against the right predecessor. Asking
/// for the current generation separately, before issuing the command, would
/// race a concurrent evolution and diff against the wrong one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionOutcome {
    pub previous_vid: SchemaVid,
    pub new_vid: SchemaVid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EvolveSchemaError {
    SchemaDoesNotExist,
    /// The edit was computed against a generation that is no longer current --
    /// something else evolved this family first. Nothing was changed; refetch
    /// and reapply. Reported rather than merged, because an edit built on a
    /// stale base can silently undo whatever the other evolution did.
    StaleBase {
        expected: SchemaVid,
        actual: SchemaVid,
    },
    /// The edit could not be turned into a schema at all.
    BadEdit(String),
    TransformRequired(String),
    Illegal(String),
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
    /// Apply an evolution the state machine has already committed.
    ///
    /// Installs the new generation and supersedes whatever this node currently
    /// believes is current, in that order: a moment with no current generation
    /// would make every write to the family fail to resolve.
    fn evolve_schema(&self, evolved: Schema) {
        let uid = evolved.uid;
        let new_vid = evolved.vid;
        let previous = self.handles.get(&uid);
        self.new_schema(evolved);
        if let Some(previous_vid) = previous {
            if previous_vid != new_vid {
                if let Some(old) = self.schema_map.get(&previous_vid) {
                    let mut superseded = (*old).clone();
                    superseded.status = SchemaVersionStatus::Stale {
                        superseded_by: new_vid,
                    };
                    self.schema_map.insert(previous_vid, Arc::new(superseded));
                }
            }
        }
        info!(
            "Local schema family {} evolved to generation at {}",
            uid, new_vid
        );
    }

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

/// The index namespaces a schema needs standing up, as (field, index) pairs.
///
/// Only the kinds that own a durable structure of their own: a `Vector` or
/// `Embedding` index has to be created and destroyed explicitly, while ranged,
/// hashed and full-text entries are written per cell and need no namespace
/// lifecycle.
fn managed_index_namespaces(schema: &Schema) -> Vec<(u64, IndexType)> {
    let mut out = Vec::new();
    for (field, indices) in &schema.index_fields {
        for index in indices {
            if matches!(index, IndexType::Vector(_) | IndexType::Embedding(_)) {
                out.push((*field, index.clone()));
            }
        }
    }
    for (compound_id, compound) in &schema.compound_index_fields {
        for index in &compound.indices {
            if matches!(index, IndexType::Vector(_) | IndexType::Embedding(_)) {
                out.push((*compound_id, index.clone()));
            }
        }
    }
    out
}

/// Reconcile index namespaces across an evolution.
///
/// A family keeps its uid, and index namespaces are keyed by uid, so the
/// namespaces of an evolved schema are the SAME namespaces -- they must not be
/// recreated. Recreating a vector index would throw away everything already
/// indexed under it; deleting one that both generations declare would do the
/// same. So this creates only what the new generation adds and destroys only
/// what it drops, and leaves everything they agree on untouched.
pub async fn post_schema_evolve(
    previous: &Schema,
    evolved: &Schema,
    database_runtime: &Arc<DatabaseRuntime>,
) -> Result<(), String> {
    debug_assert_eq!(
        previous.uid, evolved.uid,
        "an evolution stays inside one family"
    );
    let before = managed_index_namespaces(previous);
    let after = managed_index_namespaces(evolved);

    let added: Vec<_> = after
        .iter()
        .filter(|entry| !before.contains(entry))
        .cloned()
        .collect();
    let dropped: Vec<_> = before
        .iter()
        .filter(|entry| !after.contains(entry))
        .cloned()
        .collect();

    if added.is_empty() && dropped.is_empty() {
        return Ok(());
    }
    info!(
        "Schema family {} evolution: {} index namespace(s) added, {} dropped, {} unchanged",
        evolved.uid,
        added.len(),
        dropped.len(),
        after.len() - added.len()
    );
    apply_index_namespaces(evolved.uid, &added, &dropped, database_runtime).await
}

async fn apply_index_namespaces(
    schema_id: SchemaUid,
    added: &[(u64, IndexType)],
    dropped: &[(u64, IndexType)],
    database_runtime: &Arc<DatabaseRuntime>,
) -> Result<(), String> {
    for (field_id, index) in added {
        let Some(indexer) = database_runtime.indexer() else {
            return Err("Indexing not enabled".to_owned());
        };
        match index {
            IndexType::Vector(config) => {
                indexer
                    .clients
                    .vector_client
                    .new_index_with_config(schema_id, *field_id, *config)
                    .await
                    .map_err(|e| format!("Error creating vector index: {:?}", e))?;
            }
            IndexType::Embedding(config) => {
                indexer
                    .clients
                    .embedding_client
                    .new_index(schema_id, *field_id, &config.model, config.vector)
                    .await
                    .map_err(|e| format!("Error creating embedding index: {:?}", e))?;
            }
            _ => {}
        }
    }
    for (field_id, index) in dropped {
        let Some(indexer) = database_runtime.indexer() else {
            return Err("Indexing not enabled".to_owned());
        };
        match index {
            IndexType::Vector(_) => {
                indexer
                    .clients
                    .vector_client
                    .delete_index(schema_id, *field_id)
                    .await
                    .map_err(|e| format!("Error deleting vector index: {:?}", e))?;
            }
            IndexType::Embedding(_) => {
                indexer
                    .clients
                    .embedding_client
                    .delete_index(schema_id, *field_id)
                    .await
                    .map_err(|e| format!("Error deleting embedding index: {:?}", e))?;
            }
            _ => {}
        }
    }
    Ok(())
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

    fn schema_of(fields: Vec<Field>) -> Schema {
        Schema::new_with_id(1, "s", None, Field::new_schema(fields), false, false)
    }

    fn keyed_schema_of(key: &str, fields: Vec<Field>) -> Schema {
        Schema::new_with_id(
            1,
            "s",
            Some(vec![key.to_owned()]),
            Field::new_schema(fields),
            false,
            false,
        )
    }

    fn base_fields() -> Vec<Field> {
        vec![
            Field::new_unindexed("a", Type::U32),
            Field::new_unindexed("b", Type::String),
        ]
    }

    #[test]
    fn adding_a_nullable_field_needs_no_transform() {
        let before = schema_of(base_fields());
        let mut fields = base_fields();
        fields.push(Field::new_unindexed_nullable("c", Type::U64));
        assert_eq!(
            before.classify_evolution(&schema_of(fields)),
            EvolutionKind::Identity
        );
    }

    #[test]
    fn adding_a_required_field_with_a_default_is_admitted() {
        let before = schema_of(base_fields());
        let mut fields = base_fields();
        fields.push(Field::new_unindexed("c", Type::U64).with_default(OwnedValue::U64(7)));
        assert_eq!(
            before.classify_evolution(&schema_of(fields)),
            EvolutionKind::Identity,
            "a default is what makes a required field expressible"
        );
    }

    #[test]
    fn losing_nullability_with_a_default_is_admitted() {
        let before = schema_of(vec![Field::new_unindexed_nullable("a", Type::U32)]);
        let after = schema_of(vec![
            Field::new_unindexed("a", Type::U32).with_default(OwnedValue::U32(0))
        ]);
        assert_eq!(before.classify_evolution(&after), EvolutionKind::Identity);
    }

    /// The encoder only injects defaults for scalars. Admitting a composite
    /// one would pass classification and then strand every cell it touched,
    /// which is the failure mode the whole admission step exists to prevent.
    #[test]
    fn a_composite_default_is_refused_rather_than_stranded() {
        let before = schema_of(base_fields());
        let mut fields = base_fields();
        fields.push(
            Field::new_unindexed_array("c", Type::U64)
                .with_default(OwnedValue::Array(vec![OwnedValue::U64(1)])),
        );
        assert!(matches!(
            before.classify_evolution(&schema_of(fields)),
            EvolutionKind::NeedsTransform(_)
        ));
    }

    #[test]
    fn adding_a_non_nullable_field_needs_a_transform() {
        let before = schema_of(base_fields());
        let mut fields = base_fields();
        fields.push(Field::new_unindexed("c", Type::U64));
        assert!(matches!(
            before.classify_evolution(&schema_of(fields)),
            EvolutionKind::NeedsTransform(_)
        ));
    }

    #[test]
    fn dropping_a_field_needs_no_transform() {
        let before = schema_of(base_fields());
        let after = schema_of(vec![Field::new_unindexed("a", Type::U32)]);
        assert_eq!(before.classify_evolution(&after), EvolutionKind::Identity);
    }

    fn edit_base() -> Schema {
        Schema::new_with_id(
            1,
            "person",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("keep", Type::String),
                Field::new_unindexed("age", Type::U32),
                Field::new_unindexed("old_name", Type::U64),
            ]),
            false,
            false,
        )
    }

    /// The reason the delta API exists: a field the edit never mentions must
    /// survive. Describing the whole target shape by hand loses whatever you
    /// forget to re-list, silently.
    #[test]
    fn an_edit_carries_every_field_it_does_not_mention() {
        let base = edit_base();
        let evolved = SchemaEdit::new()
            .drop("age")
            .apply(&base)
            .expect("edit should apply");

        assert!(
            evolved.id_index.contains_key(&types::key_hash("keep")),
            "an untouched field must survive an edit that never mentions it"
        );
        assert!(
            evolved.id_index.contains_key(&types::key_hash("old_name")),
            "and so must every other one"
        );
        assert!(!evolved.id_index.contains_key(&types::key_hash("age")));
    }

    #[test]
    fn an_edit_produces_an_admissible_evolution() {
        let base = edit_base();
        let evolved = SchemaEdit::new()
            .rename("old_name", "new_name")
            .retype("age", Type::U64)
            .add(Field::new_unindexed("rank", Type::U64).with_default(OwnedValue::U64(0)))
            .apply(&base)
            .expect("edit should apply");

        assert_eq!(
            base.classify_evolution(&evolved),
            EvolutionKind::Identity,
            "rename + widen + defaulted add is expressible, so the edit must be too"
        );
    }

    /// A rename through the edit API must declare itself, or the values are
    /// abandoned exactly as a hand-built rename would abandon them.
    #[test]
    fn an_edit_rename_declares_the_rename() {
        let base = edit_base();
        let evolved = SchemaEdit::new()
            .rename("old_name", "new_name")
            .apply(&base)
            .unwrap();
        assert!(
            evolved
                .transform
                .renames
                .contains(&(types::key_hash("old_name"), types::key_hash("new_name"))),
            "the edit must record the rename, not just move the field"
        );
    }

    /// A renamed field keeps its type, nullability and default: the rebuild
    /// exists to fix `name_id`, not to reset the field.
    #[test]
    fn an_edit_rename_preserves_the_field_itself() {
        let base = Schema::new_with_id(
            1,
            "s",
            None,
            Field::new_schema(vec![
                Field::new_unindexed_nullable("a", Type::U64).with_default(OwnedValue::U64(5))
            ]),
            false,
            false,
        );
        let evolved = SchemaEdit::new().rename("a", "b").apply(&base).unwrap();
        let path = evolved.id_index.get(&types::key_hash("b")).unwrap().clone();
        let field = evolved.field_by_id_path(&path).unwrap();
        assert_eq!(field.data_type, Type::U64);
        assert!(field.nullable);
        assert_eq!(field.default, Some(OwnedValue::U64(5)));
    }

    /// A blobs change was expressible through the whole-shape form and became
    /// unreachable when that went private. This is the op that closes it.
    #[test]
    fn an_edit_can_change_the_blobs_flag() {
        let base = edit_base();
        assert!(!base.blobs);
        let evolved = SchemaEdit::new().set_blobs(true).apply(&base).unwrap();
        assert!(evolved.blobs);
        assert_eq!(base.classify_evolution(&evolved), EvolutionKind::Identity);
    }

    #[test]
    fn an_edit_can_add_and_drop_a_compound_index() {
        let base = edit_base();
        let with_index = SchemaEdit::new()
            .add_compound_index("by_age_name", &["age", "keep"], vec![IndexType::Ranged])
            .apply(&base)
            .unwrap();
        let id = types::key_hash("by_age_name");
        let added = with_index.compound_index_fields.get(&id).expect("added");
        assert_eq!(added.fields, vec!["age".to_owned(), "keep".to_owned()]);
        assert_eq!(
            added.field_ids,
            vec![types::key_hash("age"), types::key_hash("keep")],
            "field_ids must be derived from the names, not copied"
        );
        assert_eq!(
            base.classify_evolution(&with_index),
            EvolutionKind::Identity,
            "an index-only change needs no transform"
        );

        let without = SchemaEdit::new()
            .drop_compound_index("by_age_name")
            .apply(&with_index)
            .unwrap();
        assert!(without.compound_index_fields.is_empty());
    }

    /// Compound indexes are carried across edits that never mention them --
    /// the same guarantee ordinary fields get.
    #[test]
    fn an_edit_carries_compound_indexes_it_does_not_mention() {
        let base = SchemaEdit::new()
            .add_compound_index("pair", &["age", "keep"], vec![IndexType::Ranged])
            .apply(&edit_base())
            .unwrap();
        let evolved = SchemaEdit::new().drop("old_name").apply(&base).unwrap();
        assert!(evolved
            .compound_index_fields
            .contains_key(&types::key_hash("pair")));
    }

    /// A compound index over a field that does not exist would index nothing,
    /// so the edit is refused rather than producing one.
    #[test]
    fn a_compound_index_over_an_absent_field_is_refused() {
        let base = edit_base();
        assert_eq!(
            SchemaEdit::new()
                .add_compound_index("bad", &["ghost"], vec![IndexType::Ranged])
                .apply(&base)
                .err(),
            Some(EditError::NoSuchField("ghost".to_owned()))
        );
    }

    #[test]
    fn dropping_an_absent_compound_index_is_refused() {
        let base = edit_base();
        assert_eq!(
            SchemaEdit::new()
                .drop_compound_index("ghost")
                .apply(&base)
                .err(),
            Some(EditError::NoSuchCompoundIndex("ghost".to_owned()))
        );
    }

    #[test]
    fn an_edit_naming_an_absent_field_is_refused() {
        let base = edit_base();
        assert_eq!(
            SchemaEdit::new().drop("ghost").apply(&base).err(),
            Some(EditError::NoSuchField("ghost".to_owned()))
        );
        assert_eq!(
            SchemaEdit::new().rename("ghost", "x").apply(&base).err(),
            Some(EditError::NoSuchField("ghost".to_owned()))
        );
    }

    #[test]
    fn an_edit_adding_an_existing_field_is_refused() {
        let base = edit_base();
        assert_eq!(
            SchemaEdit::new()
                .add(Field::new_unindexed("keep", Type::String))
                .apply(&base)
                .err(),
            Some(EditError::FieldExists("keep".to_owned()))
        );
    }

    /// An edit that produces an inexpressible schema is still refused by
    /// admission. The delta API is ergonomics, not a way around it.
    #[test]
    fn an_edit_cannot_smuggle_past_admission() {
        let base = edit_base();
        let evolved = SchemaEdit::new()
            .add(Field::new_unindexed("required", Type::U64))
            .apply(&base)
            .unwrap();
        assert!(matches!(
            base.classify_evolution(&evolved),
            EvolutionKind::NeedsTransform(_)
        ));
    }

    /// Dropping from a dynamic schema declares the drop, which is what makes
    /// the removal real rather than a fall-through to the dynamic region.
    #[test]
    fn an_edit_drop_on_a_dynamic_schema_declares_it() {
        let mut base = edit_base();
        base.is_dynamic = true;
        let evolved = SchemaEdit::new().drop("age").apply(&base).unwrap();
        assert!(evolved
            .transform
            .dynamic_drops
            .contains(&types::key_hash("age")));
        assert_eq!(base.classify_evolution(&evolved), EvolutionKind::Identity);
    }

    #[test]
    fn a_declared_rename_is_admitted_and_is_not_a_drop_plus_add() {
        let before = schema_of(base_fields());
        let after = schema_of(vec![
            Field::new_unindexed("a", Type::U32),
            // `b` becomes `label`, same type.
            Field::new_unindexed("label", Type::String),
        ])
        .with_renamed_field("b", "label");
        assert_eq!(
            before.classify_evolution(&after),
            EvolutionKind::Identity,
            "a declared rename carries the values across, so it is neither a drop nor an add"
        );
    }

    /// Without declaring it, the same change reads as dropping `b` and adding
    /// a required `label` -- and is refused for want of a default.
    #[test]
    fn an_undeclared_rename_is_still_refused() {
        let before = schema_of(base_fields());
        let after = schema_of(vec![
            Field::new_unindexed("a", Type::U32),
            Field::new_unindexed("label", Type::String),
        ]);
        assert!(matches!(
            before.classify_evolution(&after),
            EvolutionKind::NeedsTransform(_)
        ));
    }

    #[test]
    fn a_rename_that_swaps_two_names_is_illegal() {
        let before = schema_of(base_fields());
        let after = schema_of(base_fields())
            .with_renamed_field("a", "b")
            .with_renamed_field("b", "a");
        assert!(matches!(
            before.classify_evolution(&after),
            EvolutionKind::Illegal(_)
        ));
    }

    /// Renaming onto a name the schema still declares means two fields reading
    /// one source, which a lookup cannot disambiguate.
    #[test]
    fn a_rename_onto_a_live_field_name_is_illegal() {
        let before = schema_of(base_fields());
        let after = schema_of(vec![
            Field::new_unindexed("a", Type::U32),
            Field::new_unindexed("b", Type::String),
            Field::new_unindexed("c", Type::String),
        ])
        .with_renamed_field("b", "c");
        assert!(matches!(
            before.classify_evolution(&after),
            EvolutionKind::Illegal(_)
        ));
    }

    /// Cumulative renames are what let migration keep taking ONE hop: a
    /// generation-0 cell must reach generation 2's name directly.
    #[test]
    fn renames_fold_forward_so_the_oldest_name_still_resolves() {
        let mut transform = SchemaTransform::default();
        let (a, b, c) = (
            types::key_hash("a"),
            types::key_hash("b"),
            types::key_hash("c"),
        );
        // What `install_evolution` produces for a -> b then b -> c.
        transform.renames = vec![(a, c), (b, c)];
        let sources: Vec<u64> = transform.historical_names(c).collect();
        assert!(
            sources.contains(&a) && sources.contains(&b),
            "both the original and the intermediate name must resolve to the current one"
        );
        assert_eq!(
            sources.first(),
            Some(&b),
            "newest first: a cell one generation behind holds the more recent name"
        );
    }

    /// The end-to-end proof: a cell written under the old name reads back
    /// under the new one, with its value intact.
    #[test]
    fn a_cell_written_before_a_rename_keeps_its_value_under_the_new_name() {
        use crate::ram::cell::OwnedCell;
        use crate::ram::chunk::Chunks;
        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::types::{Map as _, OwnedMap, OwnedValue};
        use crate::server::ServerMeta;
        use dovahkiin::types::Id;

        let gen0 = Schema::new_with_id(
            1,
            "person",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("keep", Type::U32),
                Field::new_unindexed("old_name", Type::U64),
            ]),
            false,
            false,
        );
        let mut gen1 = Schema::new_with_id(
            1,
            "person",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("keep", Type::U32),
                Field::new_unindexed("new_name", Type::U64),
            ]),
            false,
            false,
        )
        .with_renamed_field("old_name", "new_name");
        assert_eq!(
            gen0.classify_evolution(&gen1),
            EvolutionKind::Identity,
            "a declared rename must be admissible"
        );
        gen1.vid = SchemaVid(900);
        gen1.generation = 1;

        let schemas = LocalSchemasCache::new_local("");
        schemas.register_internal_schema(gen0.clone());
        let meta = Arc::new(ServerMeta { schemas });
        let chunks = Chunks::new(1, SEGMENT_SIZE, meta.clone(), None, None, None, None);

        let mut value = OwnedMap::new();
        value.insert("keep", OwnedValue::U32(1));
        value.insert("old_name", OwnedValue::U64(42));
        let mut cell =
            OwnedCell::new_with_id(gen0.vid, &Id::from_parts(1, 1), OwnedValue::Map(value));
        chunks.write_cell(&mut cell).unwrap();
        let id = cell.id();

        meta.schemas.apply_evolution(gen1);

        // Touch it naming the old generation, supplying the OLD shape -- which
        // is exactly what a client holding a stale schema would send.
        let mut touched = OwnedMap::new();
        touched.insert("keep", OwnedValue::U32(2));
        touched.insert("old_name", OwnedValue::U64(42));
        let mut update = OwnedCell::new_with_id(SchemaVid(1), &id, OwnedValue::Map(touched));
        chunks.update_cell(&mut update).unwrap();

        let read = chunks.read_cell(&id).unwrap();
        assert_eq!(read.header.schema, SchemaVid(900));
        assert_eq!(read.data["keep"].u32(), Some(&2));
        assert_eq!(
            read.data["new_name"].u64(),
            Some(&42),
            "the value must follow the rename, not be abandoned with the old name"
        );
    }

    #[test]
    fn a_declared_dynamic_drop_is_admitted() {
        let mut before = schema_of(base_fields());
        before.is_dynamic = true;
        let mut after = schema_of(vec![Field::new_unindexed("a", Type::U32)]);
        after.is_dynamic = true;
        let after = after.with_dynamic_drops(&["b"]);
        assert_eq!(
            before.classify_evolution(&after),
            EvolutionKind::Identity,
            "saying so explicitly is what makes the removal real"
        );
    }

    /// The point of the whole increment: a value in the dynamic region is
    /// written back out unless it is on the drop list.
    #[test]
    fn a_dropped_dynamic_field_is_not_re_encoded() {
        use crate::ram::cell::OwnedCell;
        use crate::ram::chunk::Chunks;
        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::types::{Map as _, OwnedMap, OwnedValue};
        use crate::server::ServerMeta;
        use dovahkiin::types::Id;

        let mut gen0 = Schema::new_with_id(
            1,
            "dyn",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("keep", Type::U32),
                Field::new_unindexed("bulky", Type::U64),
            ]),
            true,
            false,
        );
        gen0.is_dynamic = true;
        // Generation 1 stops declaring `bulky` AND says to drop it.
        let mut gen1 = Schema::new_with_id(
            1,
            "dyn",
            None,
            Field::new_schema(vec![Field::new_unindexed("keep", Type::U32)]),
            true,
            false,
        );
        gen1.is_dynamic = true;
        let mut gen1 = gen1.with_dynamic_drops(&["bulky"]);
        assert_eq!(
            gen0.classify_evolution(&gen1),
            EvolutionKind::Identity,
            "a declared drop must be admissible"
        );
        gen1.vid = SchemaVid(900);
        gen1.generation = 1;

        let schemas = LocalSchemasCache::new_local("");
        schemas.register_internal_schema(gen0.clone());
        let meta = Arc::new(ServerMeta { schemas });
        let chunks = Chunks::new(1, SEGMENT_SIZE, meta.clone(), None, None, None, None);

        let mut value = OwnedMap::new();
        value.insert("keep", OwnedValue::U32(1));
        value.insert("bulky", OwnedValue::U64(2));
        let mut cell =
            OwnedCell::new_with_id(gen0.vid, &Id::from_parts(1, 1), OwnedValue::Map(value));
        chunks.write_cell(&mut cell).unwrap();
        let id = cell.id();
        assert_eq!(
            chunks.read_cell(&id).unwrap().data["bulky"].u64(),
            Some(&2),
            "generation 0 declared it, so it must be there to begin with"
        );

        meta.schemas.apply_evolution(gen1);

        let mut touched = OwnedMap::new();
        touched.insert("keep", OwnedValue::U32(9));
        touched.insert("bulky", OwnedValue::U64(2));
        let mut update = OwnedCell::new_with_id(SchemaVid(1), &id, OwnedValue::Map(touched));
        chunks.update_cell(&mut update).unwrap();

        let read = chunks.read_cell(&id).unwrap();
        assert_eq!(read.header.schema, SchemaVid(900));
        assert_eq!(read.data["keep"].u32(), Some(&9));
        assert!(
            read.data["bulky"].u64().is_none(),
            "a dropped field must be gone, not preserved in the dynamic region"
        );
    }

    /// On a dynamic schema an undeclared field is not dropped -- it falls
    /// through to the dynamic region and gets re-encoded, which is the
    /// opposite of what removing it from the schema asked for.
    #[test]
    fn dropping_a_field_from_a_dynamic_schema_needs_a_transform() {
        let mut before = schema_of(base_fields());
        before.is_dynamic = true;
        let mut after = schema_of(vec![Field::new_unindexed("a", Type::U32)]);
        after.is_dynamic = true;
        assert!(matches!(
            before.classify_evolution(&after),
            EvolutionKind::NeedsTransform(_)
        ));
    }

    #[test]
    fn an_index_only_change_needs_no_transform() {
        let before = schema_of(base_fields());
        let after = schema_of(vec![
            Field::new_indexed("a", Type::U32, vec![IndexType::Ranged]),
            Field::new_unindexed("b", Type::String),
        ]);
        assert_eq!(before.classify_evolution(&after), EvolutionKind::Identity);
    }

    #[test]
    fn a_scannable_or_blob_flag_change_needs_no_transform() {
        let before = schema_of(base_fields());
        let mut after = schema_of(base_fields());
        after.is_scannable = !before.is_scannable;
        after.blobs = !before.blobs;
        assert_eq!(before.classify_evolution(&after), EvolutionKind::Identity);
    }

    #[test]
    fn lossless_widenings_are_admitted() {
        for (from, to) in [
            (Type::U32, Type::U64),
            (Type::I32, Type::I64),
            (Type::U8, Type::U64),
            (Type::U32, Type::I64),
            (Type::U16, Type::F32),
            (Type::U32, Type::F64),
            (Type::F32, Type::F64),
        ] {
            let before = schema_of(vec![Field::new_unindexed("a", from)]);
            let after = schema_of(vec![Field::new_unindexed("a", to)]);
            assert_eq!(
                before.classify_evolution(&after),
                EvolutionKind::Identity,
                "{:?} -> {:?} loses nothing and should be admitted",
                from,
                to
            );
        }
    }

    /// Anything that could lose range or precision stays refused. Admitting
    /// one and then failing per-cell during migration would strand an
    /// arbitrary subset of the data.
    #[test]
    fn lossy_conversions_stay_refused() {
        for (from, to) in [
            (Type::U64, Type::U32),
            (Type::I64, Type::I32),
            (Type::U64, Type::F64),
            (Type::U32, Type::F32),
            (Type::F64, Type::F32),
            (Type::I32, Type::U32),
            (Type::String, Type::U64),
        ] {
            let before = schema_of(vec![Field::new_unindexed("a", from)]);
            let after = schema_of(vec![Field::new_unindexed("a", to)]);
            assert!(
                matches!(
                    before.classify_evolution(&after),
                    EvolutionKind::NeedsTransform(_)
                ),
                "{:?} -> {:?} can lose something and must be refused",
                from,
                to
            );
        }
    }

    /// `f32` holds integers exactly only to 2^24, so a u32 must NOT widen into
    /// it even though it is "bigger". Getting this backwards would silently
    /// round large values.
    #[test]
    fn float_widening_is_bounded_by_mantissa_not_byte_width() {
        assert!(coerce_value(&OwnedValue::U32(1), Type::F32).is_none());
        assert!(coerce_value(&OwnedValue::U16(1), Type::F32).is_some());
        assert!(coerce_value(&OwnedValue::U32(1), Type::F64).is_some());
        assert!(coerce_value(&OwnedValue::U64(1), Type::F64).is_none());
    }

    #[test]
    fn coercing_a_value_to_its_own_type_is_a_no_op() {
        assert!(coerce_value(&OwnedValue::U32(5), Type::U32).is_none());
    }

    #[test]
    fn a_widening_preserves_the_value() {
        assert_eq!(
            coerce_value(&OwnedValue::U32(4_000_000_000), Type::U64),
            Some(OwnedValue::U64(4_000_000_000))
        );
        assert_eq!(
            coerce_value(&OwnedValue::I32(-7), Type::I64),
            Some(OwnedValue::I64(-7))
        );
    }

    /// Was `widening_a_number_needs_a_transform` before the transform engine.
    /// U32 -> U64 is now performed rather than refused; the refusal case it
    /// used to cover lives in `lossy_conversions_stay_refused`.
    #[test]
    fn widening_a_number_is_now_performed() {
        let before = schema_of(base_fields());
        let after = schema_of(vec![
            Field::new_unindexed("a", Type::U64),
            Field::new_unindexed("b", Type::String),
        ]);
        assert_eq!(before.classify_evolution(&after), EvolutionKind::Identity);
    }

    /// Cells already holding null have nothing to encode for a field that has
    /// stopped accepting it.
    #[test]
    fn making_a_field_non_nullable_needs_a_transform() {
        let before = schema_of(vec![Field::new_unindexed_nullable("a", Type::U32)]);
        let after = schema_of(vec![Field::new_unindexed("a", Type::U32)]);
        assert!(matches!(
            before.classify_evolution(&after),
            EvolutionKind::NeedsTransform(_)
        ));
    }

    #[test]
    fn making_a_field_nullable_needs_no_transform() {
        let before = schema_of(vec![Field::new_unindexed("a", Type::U32)]);
        let after = schema_of(vec![Field::new_unindexed_nullable("a", Type::U32)]);
        assert_eq!(before.classify_evolution(&after), EvolutionKind::Identity);
    }

    #[test]
    fn changing_the_key_field_is_illegal() {
        let before = keyed_schema_of("a", base_fields());
        let after = keyed_schema_of("b", base_fields());
        assert!(matches!(
            before.classify_evolution(&after),
            EvolutionKind::Illegal(_)
        ));
    }

    /// Nested fields must be inspected, not skipped.
    ///
    /// Uses a NARROWING deliberately: a widening is now performed rather than
    /// refused, so asserting `Identity` here would pass even if sub-maps were
    /// ignored entirely. Only a change that must be refused proves the walk
    /// actually reaches inside the map.
    #[test]
    fn a_nested_field_change_is_seen() {
        let before = schema_of(vec![Field::new_map(
            "m",
            vec![Field::new_unindexed("inner", Type::U64)],
        )]);
        let after = schema_of(vec![Field::new_map(
            "m",
            vec![Field::new_unindexed("inner", Type::U32)],
        )]);
        assert!(
            matches!(
                before.classify_evolution(&after),
                EvolutionKind::NeedsTransform(_)
            ),
            "a narrowing buried in a sub-map must still be refused"
        );
    }

    /// The counterpart: a widening buried in a sub-map is performed, which
    /// only means anything alongside the narrowing test above.
    #[test]
    fn a_nested_widening_is_performed() {
        let before = schema_of(vec![Field::new_map(
            "m",
            vec![Field::new_unindexed("inner", Type::U32)],
        )]);
        let after = schema_of(vec![Field::new_map(
            "m",
            vec![Field::new_unindexed("inner", Type::U64)],
        )]);
        assert_eq!(before.classify_evolution(&after), EvolutionKind::Identity);
    }

    #[test]
    fn an_unchanged_schema_needs_no_transform() {
        let before = schema_of(base_fields());
        assert_eq!(
            before.classify_evolution(&schema_of(base_fields())),
            EvolutionKind::Identity
        );
    }

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

    /// Build a superseded generation-0 record and its current successor.
    fn evolved_pair(uid: u32, name: &str, new_vid: u32) -> (Schema, Schema) {
        let mut gen0 = cache_test_schema(uid, name);
        let mut gen1 = gen0.clone();
        gen1.vid = SchemaVid(new_vid);
        gen1.generation = 1;
        gen0.status = SchemaVersionStatus::Stale {
            superseded_by: gen1.vid,
        };
        (gen0, gen1)
    }

    #[test]
    fn a_write_naming_a_superseded_generation_resolves_to_the_current_one() {
        let cache = LocalSchemasCache::new_local("");
        let (gen0, gen1) = evolved_pair(1, "person", 900);
        cache.cache_schema_from_cluster(gen0);
        cache.cache_schema_from_cluster(gen1);

        let resolved = cache.resolve_for_write(SchemaVid(1)).expect("must resolve");
        assert_eq!(resolved.vid, SchemaVid(900));
        assert_eq!(
            cache.resolve_for_write(SchemaVid(900)).unwrap().vid,
            SchemaVid(900),
            "a current generation resolves to itself"
        );
    }

    /// Resolution is one hop however deep the history, because a record names
    /// its own family rather than chaining through `superseded_by`.
    #[test]
    fn resolution_does_not_chain_through_generations() {
        let cache = LocalSchemasCache::new_local("");
        let mut gen0 = cache_test_schema(1, "person");
        let mut gen1 = gen0.clone();
        gen1.vid = SchemaVid(900);
        gen1.generation = 1;
        let mut gen2 = gen0.clone();
        gen2.vid = SchemaVid(901);
        gen2.generation = 2;
        gen0.status = SchemaVersionStatus::Stale {
            superseded_by: gen1.vid,
        };
        gen1.status = SchemaVersionStatus::Stale {
            superseded_by: gen2.vid,
        };
        cache.cache_schema_from_cluster(gen0);
        cache.cache_schema_from_cluster(gen1);
        cache.cache_schema_from_cluster(gen2);

        assert_eq!(
            cache.resolve_for_write(SchemaVid(1)).unwrap().vid,
            SchemaVid(901),
            "the oldest generation must reach the newest in one hop"
        );
    }

    #[test]
    fn an_unknown_generation_does_not_resolve() {
        let cache = LocalSchemasCache::new_local("");
        assert!(cache.resolve_for_write(SchemaVid(404)).is_none());
    }

    /// A superseded record whose family has been deleted must not resolve to
    /// anything: there is no current layout to write into.
    #[test]
    fn a_superseded_generation_with_no_current_sibling_does_not_resolve() {
        let cache = LocalSchemasCache::new_local("");
        let (gen0, _gen1) = evolved_pair(1, "person", 900);
        cache.cache_schema_from_cluster(gen0);
        assert!(
            cache.resolve_for_write(SchemaVid(1)).is_none(),
            "writing into a superseded layout because the current one is missing would be worse than refusing"
        );
    }

    /// The redirect has to reach the DURABLE bytes, not just the in-memory
    /// header: a later read decodes with whatever the header on disk says.
    #[test]
    fn a_redirected_write_persists_the_current_generation_in_the_cell_header() {
        use crate::ram::cell::OwnedCell;
        use crate::ram::chunk::Chunks;
        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::types::{Map as _, OwnedMap, OwnedValue};
        use crate::server::ServerMeta;
        use dovahkiin::types::Id;

        let mut gen0 = Schema::new_with_id(
            1,
            "person",
            None,
            Field::new_schema(vec![Field::new_unindexed("v", Type::U32)]),
            false,
            false,
        );
        let mut gen1 = gen0.clone();
        gen1.vid = SchemaVid(900);
        gen1.generation = 1;
        gen0.status = SchemaVersionStatus::Stale {
            superseded_by: gen1.vid,
        };

        let schemas = LocalSchemasCache::new_local("");
        schemas.register_internal_schema(gen0.clone());
        schemas.register_internal_schema(gen1.clone());
        let meta = Arc::new(ServerMeta { schemas });
        let chunks = Chunks::new(1, SEGMENT_SIZE, meta.clone(), None, None, None, None);

        let mut value = OwnedMap::new();
        value.insert("v", OwnedValue::U32(7));
        // Deliberately name the SUPERSEDED generation, the way a long-lived
        // client or a replayed batch would.
        let mut cell =
            OwnedCell::new_with_id(SchemaVid(1), &Id::from_parts(1, 1), OwnedValue::Map(value));
        let header = chunks.write_cell(&mut cell).unwrap();

        assert_eq!(
            header.schema,
            SchemaVid(900),
            "the returned header must report where the cell actually landed"
        );
        let read = chunks.read_cell(&cell.id()).unwrap();
        assert_eq!(
            read.header.schema,
            SchemaVid(900),
            "the persisted header must name the current generation, or the redirect never happened"
        );
        assert_eq!(read.data["v"].u32(), Some(&7));
    }

    /// The transform engine's first real capability, end to end: a schema
    /// gains a REQUIRED field, and a cell written before that field existed
    /// comes back carrying the default rather than failing to encode.
    #[test]
    fn a_cell_written_before_a_required_field_existed_migrates_with_its_default() {
        use crate::ram::cell::OwnedCell;
        use crate::ram::chunk::Chunks;
        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::types::{Map as _, OwnedMap, OwnedValue};
        use crate::server::ServerMeta;
        use dovahkiin::types::Id;

        let gen0 = Schema::new_with_id(
            1,
            "person",
            None,
            Field::new_schema(vec![Field::new_unindexed("v", Type::U32)]),
            false,
            false,
        );
        let mut gen1 = Schema::new_with_id(
            1,
            "person",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("v", Type::U32),
                // Required, with a default. Without the default this evolution
                // is refused outright.
                Field::new_unindexed("rank", Type::U64).with_default(OwnedValue::U64(99)),
            ]),
            false,
            false,
        );
        assert_eq!(
            gen0.classify_evolution(&gen1),
            EvolutionKind::Identity,
            "a required field with a default must be admissible"
        );
        gen1.vid = SchemaVid(900);
        gen1.generation = 1;

        let schemas = LocalSchemasCache::new_local("");
        schemas.register_internal_schema(gen0.clone());
        let meta = Arc::new(ServerMeta { schemas });
        let chunks = Chunks::new(1, SEGMENT_SIZE, meta.clone(), None, None, None, None);

        let mut value = OwnedMap::new();
        value.insert("v", OwnedValue::U32(7));
        let mut cell =
            OwnedCell::new_with_id(gen0.vid, &Id::from_parts(1, 1), OwnedValue::Map(value));
        chunks.write_cell(&mut cell).unwrap();
        let id = cell.id();

        meta.schemas.apply_evolution(gen1);

        // Touch it the way any client would, still naming the old generation.
        let mut updated = OwnedMap::new();
        updated.insert("v", OwnedValue::U32(8));
        let mut update = OwnedCell::new_with_id(SchemaVid(1), &id, OwnedValue::Map(updated));
        chunks
            .update_cell(&mut update)
            .expect("the default is what makes this encode at all");

        let read = chunks.read_cell(&id).unwrap();
        assert_eq!(read.header.schema, SchemaVid(900));
        assert_eq!(read.data["v"].u32(), Some(&8));
        assert_eq!(
            read.data["rank"].u64(),
            Some(&99),
            "a field the cell never had must come back holding its default"
        );
    }

    /// The whole feature, end to end: a schema gains a nullable field, cells
    /// written before the change still read (the new field comes back null),
    /// and new writes land in the new generation.
    #[test]
    fn cells_survive_an_evolution_and_new_writes_use_the_new_generation() {
        use crate::ram::cell::OwnedCell;
        use crate::ram::chunk::Chunks;
        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::types::{Map as _, OwnedMap, OwnedValue};
        use crate::server::ServerMeta;
        use dovahkiin::types::Id;

        let gen0 = Schema::new_with_id(
            1,
            "person",
            None,
            Field::new_schema(vec![Field::new_unindexed("v", Type::U32)]),
            false,
            false,
        );
        let mut gen1 = Schema::new_with_id(
            1,
            "person",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("v", Type::U32),
                Field::new_unindexed_nullable("added", Type::U64),
            ]),
            false,
            false,
        );
        assert_eq!(
            gen0.classify_evolution(&gen1),
            EvolutionKind::Identity,
            "adding a nullable field must need no transform"
        );
        gen1.vid = SchemaVid(900);
        gen1.generation = 1;

        let schemas = LocalSchemasCache::new_local("");
        schemas.register_internal_schema(gen0.clone());
        let meta = Arc::new(ServerMeta { schemas });
        let chunks = Chunks::new(1, SEGMENT_SIZE, meta.clone(), None, None, None, None);

        // A cell written under generation 0.
        let mut old_value = OwnedMap::new();
        old_value.insert("v", OwnedValue::U32(7));
        let mut old_cell =
            OwnedCell::new_with_id(gen0.vid, &Id::from_parts(1, 1), OwnedValue::Map(old_value));
        chunks.write_cell(&mut old_cell).unwrap();
        let old_id = old_cell.id();

        meta.schemas.apply_evolution(gen1.clone());

        // The old cell is untouched on disk and still decodes, through the
        // generation that encoded it.
        //
        // Scoped deliberately: `read_cell` hands back a guard-holding
        // `SharedCell`, so reading the same cell again while one is still
        // alive waits on a lock this thread already owns.
        {
            let read_old = chunks
                .read_cell(&old_id)
                .expect("a cell written before the evolution must still read");
            assert_eq!(read_old.header.schema, SchemaVid(1));
            assert_eq!(read_old.data["v"].u32(), Some(&7));
            assert!(
                read_old.data["added"].u64().is_none(),
                "a field the cell was written without comes back absent, not corrupt"
            );
        }

        // A new write by name lands in generation 1.
        let mut new_value = OwnedMap::new();
        new_value.insert("v", OwnedValue::U32(9));
        new_value.insert("added", OwnedValue::U64(42));
        let target = meta.schemas.name_to_id("person").unwrap();
        assert_eq!(target, SchemaVid(900));
        let mut new_cell =
            OwnedCell::new_with_id(target, &Id::from_parts(1, 2), OwnedValue::Map(new_value));
        chunks.write_cell(&mut new_cell).unwrap();

        {
            let read_new = chunks.read_cell(&new_cell.id()).unwrap();
            assert_eq!(read_new.header.schema, SchemaVid(900));
            assert_eq!(read_new.data["added"].u64(), Some(&42));
        }

        // Both generations are readable through one store, which is what keeps
        // the query layer generation-agnostic.
        assert_eq!(chunks.read_cell(&old_id).unwrap().data["v"].u32(), Some(&7));
    }

    /// An ordinary update to a cell in a superseded generation IS a migration:
    /// it decodes under the old layout and re-encodes under the current one,
    /// keeping its id and bumping its version.
    #[test]
    fn updating_a_stale_cell_migrates_it_to_the_current_generation() {
        use crate::ram::cell::OwnedCell;
        use crate::ram::chunk::Chunks;
        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::types::{Map as _, OwnedMap, OwnedValue};
        use crate::server::ServerMeta;
        use dovahkiin::types::Id;

        let gen0 = Schema::new_with_id(
            1,
            "person",
            None,
            Field::new_schema(vec![Field::new_unindexed("v", Type::U32)]),
            false,
            false,
        );
        let mut gen1 = Schema::new_with_id(
            1,
            "person",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("v", Type::U32),
                Field::new_unindexed_nullable("added", Type::U64),
            ]),
            false,
            false,
        );
        gen1.vid = SchemaVid(900);
        gen1.generation = 1;

        let schemas = LocalSchemasCache::new_local("");
        schemas.register_internal_schema(gen0.clone());
        let meta = Arc::new(ServerMeta { schemas });
        let chunks = Chunks::new(1, SEGMENT_SIZE, meta.clone(), None, None, None, None);

        let mut value = OwnedMap::new();
        value.insert("v", OwnedValue::U32(7));
        let mut cell =
            OwnedCell::new_with_id(gen0.vid, &Id::from_parts(1, 1), OwnedValue::Map(value));
        chunks.write_cell(&mut cell).unwrap();
        let id = cell.id();
        let version_before = chunks.read_cell(&id).unwrap().header.version;

        meta.schemas.apply_evolution(gen1);

        // Update it exactly as any client would, still naming the old vid.
        let mut updated_value = OwnedMap::new();
        updated_value.insert("v", OwnedValue::U32(8));
        let mut updated = OwnedCell::new_with_id(SchemaVid(1), &id, OwnedValue::Map(updated_value));
        chunks.update_cell(&mut updated).unwrap();

        let read = chunks.read_cell(&id).unwrap();
        assert_eq!(
            read.header.schema,
            SchemaVid(900),
            "an update to a stale cell must land it in the current generation"
        );
        assert_eq!(read.id(), id, "migration preserves the cell's identity");
        assert!(
            read.header.version > version_before,
            "a migration is a new version of the cell"
        );
        assert_eq!(read.data["v"].u32(), Some(&8));
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
