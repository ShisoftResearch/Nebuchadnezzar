use super::{EntryKey, Feature, IndexerClients};
use crate::ram::cell::{OwnedCell, SharedCell};
use crate::ram::types::{Id, Value};
use crate::ram::{
    cell::Cell,
    schema::{Field, IndexType, Schema},
};
use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient, rpc::RPCError};
use bifrost_hasher::hash_str;
use futures::FutureExt;
use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
};
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::{cell::RefCell, sync::Arc};
use tokio::task::{JoinError, JoinHandle};

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
    cell_id: Id,
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

impl IndexMeta {
    async fn insert(&self, indexers: &IndexerClients) -> Result<(), RPCError> {
        match self {
            &IndexMeta::Ranged(ref meta) => {
                indexers.ranged_client.insert(&meta.key).await?;
            }
            &IndexMeta::Hashed(ref meta) => {
                unimplemented!();
            }
            &IndexMeta::Vectorized(ref meta) => {
                unimplemented!();
            }
        }
        Ok(())
    }
    async fn remove(&self, indexers: &IndexerClients) -> Result<(), RPCError> {
        match self {
            &IndexMeta::Ranged(ref meta) => {
                indexers.ranged_client.delete(&meta.key).await?;
            }
            &IndexMeta::Hashed(ref meta) => {
                unimplemented!();
            }
            &IndexMeta::Vectorized(ref meta) => {
                unimplemented!();
            }
        }
        Ok(())
    }
}

thread_local! {
    pub static PENDING_INDEX_TASKS: RefCell<Vec<JoinHandle<Result<(), RPCError>>>> = RefCell::new(Vec::new());
}

pub struct IndexBuilder {
    clients: Arc<IndexerClients>,
}

impl IndexBuilder {
    pub fn new(conshash: &Arc<ConsistentHashing>, raft_client: &Arc<RaftClient>) -> Self {
        Self {
            clients: Arc::new(IndexerClients::new(conshash, raft_client)),
        }
    }

    pub fn ensure_indices(
        &self,
        cell: &OwnedCell,
        schema: &Schema,
        old_indices: Option<Vec<IndexRes>>,
    ) {
        let indexers = self.clients.to_owned();
        if schema.is_scannable {
            self.ensure_scannable(cell, &indexers);
        }
        let new_indices = probe_cell_indices(cell, schema);
        let task =
            tokio::spawn(
                async move { Self::ensure_indices_(new_indices, old_indices, indexers).await },
            );
        PENDING_INDEX_TASKS.with(|task_list| {
            task_list.borrow_mut().push(task);
        });
    }

    fn ensure_scannable(&self, cell: &OwnedCell, indexers: &Arc<IndexerClients>) {
        let key = EntryKey::for_scannable(&cell.id(), cell.header.schema);
        let indexers = indexers.to_owned();
        let task = tokio::spawn(async move {
            indexers.ranged_client.insert(&key).await?;
            Ok(())
        });
        PENDING_INDEX_TASKS.with(|task_list| {
            task_list.borrow_mut().push(task);
        });
    }

    pub fn remove_indices(&self, cell: &SharedCell, schema: &Schema) {
        let indexers = self.clients.to_owned();
        if schema.is_scannable {
            self.remove_scannable(cell, &indexers);
        }
        let indices = probe_cell_indices(cell, schema);
        let task = tokio::spawn(async move { Self::remove_indices_(indices, indexers).await });
        PENDING_INDEX_TASKS.with(|task_list| {
            task_list.borrow_mut().push(task);
        });
    }

    fn remove_scannable(&self, cell: &SharedCell, indexers: &Arc<IndexerClients>) {
        let key = EntryKey::for_scannable(&cell.id(), cell.header.schema);
        let indexers = indexers.to_owned();
        let task = tokio::spawn(async move {
            indexers.ranged_client.delete(&key).await?;
            Ok(())
        });
        PENDING_INDEX_TASKS.with(|task_list| {
            task_list.borrow_mut().push(task);
        });
    }

    pub fn await_indices<'a>() -> BoxFuture<'a, Vec<Result<Result<(), RPCError>, JoinError>>> {
        PENDING_INDEX_TASKS
            .with(|task_list| {
                let tasks = std::mem::take(&mut *task_list.borrow_mut());
                tasks.into_iter().collect::<FuturesUnordered<_>>()
            })
            .collect::<Vec<_>>()
            .boxed()
    }

    async fn remove_indices_(
        indices: Vec<IndexRes>,
        indexers: Arc<IndexerClients>,
    ) -> Result<(), RPCError> {
        for index in indices.into_iter().flat_map(|res| res.meta) {
            index.remove(&indexers).await?;
        }
        Ok(())
    }

    async fn ensure_indices_(
        new_indices: Vec<IndexRes>,
        old_indices: Option<Vec<IndexRes>>,
        indexers: Arc<IndexerClients>,
    ) -> Result<(), RPCError> {
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
            new_index.insert(&*indexers).await?;
        }
        for old_index in index_of_old_index.values() {
            old_index.remove(&*indexers).await?;
        }
        Ok(())
    }
}

pub fn probe_cell_indices(cell: &dyn Cell, schema: &Schema) -> Vec<IndexRes> {
    let mut res = vec![];
    probe_field_indices(
        cell,
        &"".to_string(),
        &schema.fields,
        schema.id,
        cell.data(),
        &mut res,
    );
    res
}

fn probe_field_indices(
    cell: &dyn Cell,
    fields_name: &FieldName,
    field: &Field,
    schema_id: u32,
    value: &dyn Value,
    res: &mut Vec<IndexRes>,
) {
    if let &Some(ref fields) = &field.sub_fields {
        for field in fields {
            let value = &value.index_of(field.name_id as usize);
            let field_name = format!("{} -> {}", fields_name, field.name);
            probe_field_indices(cell, &field_name, field, schema_id, *value, res);
        }
    } else {
        let mut components = vec![];
        if let Some(ref array) = value.uni_array() {
            for sub_val in array {
                probe_field_indices(cell, &fields_name, field, schema_id, *sub_val, res);
            }
        } else if let Some(array_data_size) = value.prim_array_data_size() {
            for index in &field.indices {
                match index {
                    &IndexType::Ranged => components.append(
                        &mut value
                            .features()
                            .into_iter()
                            .map(|vec| IndexComps::Ranged(vec))
                            .collect(),
                    ),
                    &IndexType::Hashed => components.append(
                        &mut value
                            .hashes()
                            .into_iter()
                            .map(|vec| IndexComps::Hashed(vec))
                            .collect(),
                    ),
                    &IndexType::Vectorized => components.append(
                        &mut value
                            .features()
                            .into_iter()
                            .map(|vec| IndexComps::Vectorized(vec, array_data_size))
                            .collect(),
                    ),
                    &IndexType::Statistics => {}
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
                    &IndexType::Statistics => {}
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
                        cell_ver: cell.header().version,
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
