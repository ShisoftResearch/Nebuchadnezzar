use bifrost_hasher::hash_str;
use index::client::IndexComps::Vectorized;
use index::EntryKey;
use ram::cell::Cell;
use ram::schema::{Field, IndexType, Schema};
use ram::types::{Id, Value};
use smallvec::SmallVec;

type FieldName = String;
type Component = [u8; 8];
const UNSETTLED: Component = [0u8; 8];

// Define index rules
// Index can be applied on scala value and scala arrays for both Ranged and Hashed
// Only scala can be use vectorization index
// String for ranged will take first 64-bit, hash will hash the string
// Index on nested fields are allowed

pub struct RangedIndexMeta {
    key: EntryKey,
}

pub struct HashedIndexMeta {
    id: Id,
}

pub struct VectorizedMeta {
    cell_id: Id,
    feature: Component,
    details: Option<VectorizedDetails>,
}

pub struct VectorizedDetails {
    vec_id: Id,
    index: u32,
}

pub enum IndexMeta {
    Ranged(RangedIndexMeta),
    Hashed(HashedIndexMeta),
    Vectorized(VectorizedMeta),
}

pub enum IndexComps {
    Ranged(Component),
    Hashed(Component),
    Vectorized(Component),
}

pub struct IndexRes {
    id: FieldName,
    meta: Vec<IndexMeta>,
}

pub fn make_indices(cell: &Cell, schema: &Schema) {}

pub fn remove_indies(indices: Vec<IndexMeta>) {}

pub fn ensure_indices(cell: &Cell, schema: &Schema, old_indices: Vec<IndexRes>) {}

pub fn probe_cell_indices(cell: &Cell, schema: &Schema) -> Vec<IndexRes> {
    let mut res = vec![];
    probe_field_indices(
        cell.id(),
        &"".to_string(),
        &schema.fields,
        schema.id,
        &cell.data,
        &mut res,
    );
    res
}

fn probe_field_indices(
    id: Id,
    prefix: &FieldName,
    field: &Field,
    schema_id: u32,
    value: &Value,
    metas: &mut Vec<IndexRes>,
) {
    if let &Some(ref fields) = &field.sub_fields {
        for field in fields {
            let value = &value[field.name_id];
            let prefix = format!("{} -> {}", prefix, field.name);
            probe_field_indices(
                id,
                &prefix,
                field,
                schema_id,
                value,
                metas,
            );
        }
    } else {
        let mut components = vec![];
        if let &Value::Array(ref array) = value {
            for val in array {
                probe_field_indices(id, &prefix, field, schema_id, value, metas);
            }
        } else if let &Value::PrimArray(ref array) = value {
            for index in &field.indices {
                match index {
                    &IndexType::Ranged => components.append(
                        &mut array
                            .features()
                            .into_iter()
                            .map(|vec| IndexComps::Ranged(vec))
                            .collect(),
                    ),
                    &IndexType::Hashed => components.append(
                        &mut array
                            .hashes()
                            .into_iter()
                            .map(|vec| IndexComps::Hashed(vec))
                            .collect(),
                    ),
                    &IndexType::Vectorized => components.append(
                        &mut array
                            .features()
                            .into_iter()
                            .map(|vec| IndexComps::Vectorized(vec))
                            .collect(),
                    ),
                }
            }
        } else {
            for index in &field.indices {
                match index {
                    &IndexType::Ranged => components.push(IndexComps::Ranged(value.feature())),
                    &IndexType::Hashed => components.push(IndexComps::Hashed(value.hash())),
                    &IndexType::Vectorized => {
                        components.push(IndexComps::Vectorized(value.feature()))
                    }
                }
            }
        }
        let mut metas = vec![];
        for comp in components {
            match comp {
                IndexComps::Hashed(c) => {
                    if c == UNSETTLED {
                        continue;
                    }
                    let id = Id::from_obj(&(schema_id, prefix.clone(), c));
                    metas.push(IndexMeta::Hashed(HashedIndexMeta { id }));
                }
                IndexComps::Ranged(c) => {
                    if c == UNSETTLED {
                        continue;
                    }
                    let mut key = EntryKey::new();
                    let field = hash_str(&prefix);
                    key.extend_from_slice(&id.to_binary()); // Id
                    key.extend_from_slice(&c); // value
                    key.extend_from_slice(&field.to_be_bytes()); // field
                    key.extend_from_slice(&schema_id.to_be_bytes()); // schema id
                    metas.push(IndexMeta::Ranged(RangedIndexMeta { key }));
                }
                IndexComps::Vectorized(c) => {
                    if c == UNSETTLED {
                        continue;
                    }
                    metas.push(IndexMeta::Vectorized(VectorizedMeta {
                        cell_id: id,
                        feature: c,
                        details: None,
                    }));
                }
            }
        }
    }
}
