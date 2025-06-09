use crate::client::transaction::TxnError;
use crate::ram::cell::{CellHeader, ReadError, WriteError};
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use crate::{client::AsyncClient, ram::cell::OwnedCell};
use bifrost::rpc::RPCError;
use bifrost_hasher::hash_str;
use std::sync::Arc;

use super::Feature;

const HASH_SCHEMA: &'static str = "HASH_INDEX_SCHEMA";
const HASH_INDEX_FIELD: &'static str = "CELL_ID";

lazy_static! {
    pub static ref HASH_INDEX_SCHEMA_ID: u32 = key_hash(HASH_SCHEMA) as u32;
    pub static ref HASH_INDEX_FIELD_ID: u64 = hash_str(HASH_INDEX_FIELD);
}

pub struct HashIndexer {
    neb_client: Arc<AsyncClient>,
}

impl HashIndexer {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        HashIndexer { neb_client: neb_client.clone() }
    }

    pub async fn add_index(
        &self,
        cell_id: &Id,
        index_id: &Id,
    ) -> Result<(), TxnError> {
        self.neb_client.transaction(|txn| {
            debug!("Tempting to add index for cell_id: {:?}, index_id: {:?}", cell_id, index_id);
            async move {
                debug!("Adding index for cell_id: {:?}, index_id: {:?}", cell_id, index_id);
                match txn.read(*index_id).await? {
                    Some(mut cell) => {
                        let ids_val = &mut cell[*HASH_INDEX_FIELD_ID];
                        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids_val {
                            if !ids.contains(cell_id) {
                                ids.push(*cell_id);
                                txn.update(cell).await?;
                            }
                        }
                    }
                    None => {
                        let mut map = OwnedMap::new();
                        map.insert_key_id(*HASH_INDEX_FIELD_ID, OwnedValue::PrimArray(OwnedPrimArray::Id(vec![*cell_id])));
                        let cell = OwnedCell::new_with_id(
                            *HASH_INDEX_SCHEMA_ID, 
                            index_id, 
                            OwnedValue::Map(map)
                        );
                        txn.write(cell).await?;
                    }
                }
                Ok(())
            }
        })
        .await?;
        Ok(())
    }

    pub async fn remove_index(&self, cell_id: &Id, index_id: &Id) -> Result<Result<(), WriteError>, TxnError> {
        self.neb_client.transaction(|txn| {
            async move {
                match txn.read(*index_id).await? {
                    Some(mut cell) => {
                        let ids_val = &mut cell[*HASH_INDEX_FIELD_ID];
                        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids_val {
                            ids.retain(|id| *id != *cell_id);
                            if ids.is_empty() {
                                txn.remove(*index_id).await?;
                            } else {
                                txn.update(cell).await?;
                            }
                        }
                        Ok(Ok(()))
                    }
                    None => {
                        Err(TxnError::WriteError(WriteError::CellDoesNotExisted))
                    }
                }
            }
        })
        .await
    }

    pub async fn query(&self, index_id: Id, field_id: u64, value: &OwnedValue) -> Result<Result<Vec<Id>, ReadError>, RPCError> {
        let read_res = self.neb_client.read_cell(index_id).await;
        let mut result = Vec::new();
        match read_res {
            Ok(Ok(cell)) => {
                let cell_ids_val = &cell[*HASH_INDEX_FIELD_ID];
                if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = cell_ids_val {
                    for id in ids {
                        // Now we need to check each of the cell id that they have
                        // a field matching the field id and have exact the same value
                        let cell_res = self.neb_client.read_cell_select(*id, &vec![field_id], false).await;
                        if let Ok(Ok(cell)) = &cell_res {
                            let field_val = &cell[0usize];
                            if field_val == value {
                                result.push(*id);
                            } else {
                                debug!("Cell {:?} has field {:?} with value {:?}, but expected {:?}", id, field_id, field_val, value);
                            }
                        }
                    }
                }
                return Ok(Ok(result));
            }
            Ok(Err(ReadError::CellDoesNotExisted)) => {
                return Ok(Ok(vec![]));
            }
            Ok(Err(e)) => {
                return Ok(Err(e));
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}

pub fn hash_index_schema() -> Schema {
    Schema::new_with_id(
        *HASH_INDEX_SCHEMA_ID,
        &HASH_SCHEMA.to_string(),
        None,
        Field::new_schema(vec![
            Field::new_unindexed_array(HASH_INDEX_FIELD, Type::Id),
        ]),
        false,
        false,
    )
}

pub struct HashedIndexClient {
    pub client: Arc<AsyncClient>,
    pub indexer: HashIndexer,
}

impl HashedIndexClient {
    pub fn new(client: &Arc<AsyncClient>) -> Self {
        let indexer = HashIndexer::new(&client);
        HashedIndexClient { client: client.clone(), indexer }
    }

    pub async fn insert(&self, hash_id: &Id, cell_id: &Id) -> Result<(), TxnError> {
        self.indexer.add_index(cell_id, hash_id).await?;
        Ok(())
    }

    pub async fn query(&self, index_id: Id, field_id: u64, value: &OwnedValue) -> Result<Result<Vec<Id>, ReadError>, RPCError> {
        self.indexer.query(index_id, field_id, value).await
    }
}

pub fn get_hash_id(schema: u32, field: u64, hash_feat: Feature) -> Id {
    Id::from_obj(&(schema, field, hash_feat))
}

pub fn get_hash_id_from_value<V: Value>(schema: u32, field: u64, value: &V) -> Id {
    let hash_feat = value.hash();
    get_hash_id(schema, field, hash_feat)
}