use super::{ranged::client::RangedQueryClient, EntryKey, Feature};
use crate::ram::schema::{Field, IndexType, Schema};
use crate::ram::types::{Id, Value};
use crate::{client::AsyncClient, ram::cell::Cell};
use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient, rpc::RPCError};
use bifrost_hasher::hash_str;
use std::sync::Arc;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};

type FieldName = String;
const UNSETTLED: Feature = [0u8; 8];

// Define index rules
// Index can be applied on scala value and scala arrays for both Ranged and Hashed
// Only scala can be use vectorization index
// String for ranged will take first 64-bit, hash will hash the string
// Index on nested fields are allowed

#[derive(Hash)]
pub struct RangedIndexMeta {
    key: EntryKey,
}


#[derive(Hash)]
pub struct HashedIndexMeta {
    hash_id: Id,
    cell_id: Id
}


#[derive(Hash)]
pub struct VectorizedMeta {
    cell_id: Id,
    feature: Feature,
    data_width: u8,
    cell_ver: u64,
}

#[derive(Hash)]
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

impl IndexRes {
    fn to_meta_hash_pairs(self) -> Vec<(u64, IndexMeta)> {
        self.meta
            .into_iter()
            .map(|meta| {
                let mut hasher = DefaultHasher::default();
                meta.hash(&mut hasher);
                (hasher.finish(), meta)
            })
            .collect()
    }
}

pub struct IndexBuilder {
    ranged_client: RangedQueryClient,
}

impl IndexBuilder {
    pub async fn new(
        conshash: &Arc<ConsistentHashing>,
        raft_client: &Arc<RaftClient>,
        neb_client: &Arc<AsyncClient>,
    ) -> Self {
        Self {
            ranged_client: RangedQueryClient::new(conshash, raft_client, neb_client).await,
        }
    }

    pub async fn ensure_indices(&self, cell: &Cell, schema: &Schema, old_indices: Option<Vec<IndexRes>>) -> Result<(), RPCError> {
        let new_indices = probe_cell_indices(cell, schema);
        let mut index_of_old_index = old_indices
            .unwrap_or_default()
            .into_iter()
            .flat_map(|res| res.to_meta_hash_pairs())
            .collect::<HashMap<_, _>>();
        let mut index_of_new_index = new_indices
            .into_iter()
            .flat_map(|res| res.to_meta_hash_pairs())
            .collect::<HashMap<_, _>>();
        for index in index_of_old_index.keys().cloned().collect::<Vec<_>>() {
            if index_of_new_index.contains_key(&index) {
                // Remove unchanged indeices
                index_of_new_index.remove(&index);
                index_of_old_index.remove(&index);
            }
        }
        for new_index in index_of_new_index.values() {
            match new_index {
                &IndexMeta::Ranged(ref meta) => {
                    self.ranged_client.insert(&meta.key).await?;
                }
                &IndexMeta::Hashed(ref meta) => {
                    unimplemented!();
                }
                &IndexMeta::Vectorized(ref meta) => {
                    unimplemented!();
                }
            }
        }
        for old_index in index_of_old_index.values() {
            match old_index {
                &IndexMeta::Ranged(ref meta) => {
                    self.ranged_client.delete(&meta.key).await?;
                }
                &IndexMeta::Hashed(ref meta) => {
                    unimplemented!();
                }
                &IndexMeta::Vectorized(ref meta) => {
                    unimplemented!();
                }
            }
        }
        Ok(())
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
            for sub_val in array {
                probe_field_indices(cell, &fields_name, field, schema_id, sub_val, res);
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
                    &IndexType::Vectorized => components.push(IndexComps::Vectorized(
                        value.feature(),
                        value.base_size() as u8,
                    )),
                }
            }
        }
        let mut metas = vec![];
        let cell_id = cell.id();
        for comp in components {
            match comp {
                IndexComps::Hashed(feat) => {
                    if feat == UNSETTLED {
                        continue;
                    }
                    let hash_id = Id::from_obj(&(schema_id, fields_name.clone(), feat));
                    metas.push(IndexMeta::Hashed(HashedIndexMeta { hash_id, cell_id }));
                }
                IndexComps::Ranged(feat) => {
                    if feat == UNSETTLED {
                        continue;
                    }
                    let field = hash_str(&fields_name);
                    let key = EntryKey::from_props(&cell_id, &feat, field, schema_id);
                    metas.push(IndexMeta::Ranged(RangedIndexMeta { key }));
                }
                IndexComps::Vectorized(feat, size) => {
                    if feat == UNSETTLED {
                        continue;
                    }
                    metas.push(IndexMeta::Vectorized(VectorizedMeta {
                        cell_id,
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
