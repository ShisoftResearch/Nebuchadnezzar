use bifrost_hasher::hash_str;
use index::builder::IndexComps::Vectorized;
use index::trees::EntryKey;
use ram::cell::Cell;
use ram::schema::{Field, IndexType, Schema};
use ram::types::{Id, Value};
use smallvec::SmallVec;

type FieldName = String;
type Feature = [u8; 8];
const UNSETTLED: Feature = [0u8; 8];

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
    feature: Feature,
    data_width: u8,
    cell_ver: u64,
}

pub enum IndexMeta {
    Ranged(RangedIndexMeta),
    Hashed(HashedIndexMeta),
    Vectorized(VectorizedMeta),
}

pub enum IndexComps {
    Ranged(Feature),
    Hashed(Feature),
    Vectorized(Feature, u8),
}

pub struct IndexRes {
    fields: FieldName,
    meta: Vec<IndexMeta>,
}

pub fn ensure_indices(cell: &Cell, schema: &Schema, old_indices: Option<Vec<IndexRes>>) {
    let new_index = probe_cell_indices(cell, schema);
    for index in new_index {
        let field_id = hash_str(&index.fields);
        for meta in index.meta {
            match meta {
                IndexMeta::Ranged(ranged) => {

                }
                IndexMeta::Hashed(_) => {}
                IndexMeta::Vectorized(_) => {}
            }
        }
    }
}

pub fn probe_cell_indices(cell: &Cell, schema: &Schema) -> Vec<IndexRes> {
    let mut res = vec![];
    probe_field_indices(
        &cell,
        &"".to_string(),
        &schema.fields,
        schema.id,
        &cell.data,
        &mut res,
    );
    res
}

fn probe_field_indices(
    cell: &Cell,
    fields_name: &FieldName,
    field: &Field,
    schema_id: u32,
    value: &Value,
    res: &mut Vec<IndexRes>,
) {
    if let &Some(ref fields) = &field.sub_fields {
        for field in fields {
            let value = &value[field.name_id];
            let field_name = format!("{} -> {}", fields_name, field.name);
            probe_field_indices(cell, &field_name, field, schema_id, value, res);
        }
    } else {
        let mut components = vec![];
        if let &Value::Array(ref array) = value {
            for val in array {
                probe_field_indices(cell, &fields_name, field, schema_id, value, res);
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
                            .map(|vec| IndexComps::Vectorized(vec, array.data_size()))
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
                        components.push(IndexComps::Vectorized(value.feature(), value.base_size() as u8))
                    }
                }
            }
        }
        let mut metas = vec![];
        let id = cell.id();
        for comp in components {
            match comp {
                IndexComps::Hashed(feat) => {
                    if feat == UNSETTLED {
                        continue;
                    }
                    let id = Id::from_obj(&(schema_id, fields_name.clone(), feat));
                    metas.push(IndexMeta::Hashed(HashedIndexMeta { id }));
                }
                IndexComps::Ranged(feat) => {
                    if feat == UNSETTLED {
                        continue;
                    }
                    let mut key = EntryKey::new();
                    let field = hash_str(&fields_name);
                    key.extend_from_slice(&id.to_binary()); // Id
                    key.extend_from_slice(&feat); // value
                    key.extend_from_slice(&field.to_be_bytes()); // field
                    key.extend_from_slice(&schema_id.to_be_bytes()); // schema id
                    metas.push(IndexMeta::Ranged(RangedIndexMeta { key }));
                }
                IndexComps::Vectorized(feat, size) => {
                    if feat == UNSETTLED {
                        continue;
                    }
                    metas.push(IndexMeta::Vectorized(VectorizedMeta {
                        cell_id: id,
                        feature: feat,
                        data_width: size,
                        cell_ver: cell.header.version,
                    }));
                }
            }
        }
        res.push(IndexRes {
            fields: fields_name.clone(),
            meta: metas,
        });
    }
}
