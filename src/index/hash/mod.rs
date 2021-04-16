use crate::ram::cell::{Cell, CellHeader, ReadError, WriteError};
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use crate::{client::AsyncClient, ram::cell::OwnedCell};
use bifrost::rpc::RPCError;
use std::sync::Arc;

const HASH_SCHEMA: &'static str = "HASH_INDEX_SCHEMA";

lazy_static! {
    pub static ref HASH_INDEX_SCHEMA_ID: u32 = key_hash(HASH_SCHEMA) as u32;
}

pub struct HashIndexer {
    neb_client: Arc<AsyncClient>,
}

impl HashIndexer {
    pub async fn add_index(
        &self,
        cell_id: Id,
        index_id: Id,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        let cell =
            OwnedCell::new_with_id(*HASH_INDEX_SCHEMA_ID, &index_id, OwnedValue::Id(cell_id));
        self.neb_client.write_cell(cell).await
    }

    pub async fn remove_index(&self, index_id: Id) -> Result<Result<(), WriteError>, RPCError> {
        self.neb_client.remove_cell(index_id).await
    }

    pub async fn query(&self, index_id: Id) -> Result<Result<Id, ReadError>, RPCError> {
        let res = self.neb_client.read_cell(index_id).await;
        res.map(|read| read.map(|cell| *cell.data.id().unwrap()))
    }
}

#[allow(dead_code)]
fn hash_index_schema() -> Schema {
    Schema {
        id: *HASH_INDEX_SCHEMA_ID,
        name: HASH_SCHEMA.to_string(),
        key_field: None,
        str_key_field: None,
        fields: Field::new("*", Type::Id, false, false, None, vec![]),
        is_dynamic: false,
        is_scannable: false,
    }
}
