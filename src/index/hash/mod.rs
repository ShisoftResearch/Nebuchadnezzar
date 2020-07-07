use bifrost::rpc::RPCError;
use crate::client::AsyncClient;
use crate::ram::cell::{Cell, CellHeader, ReadError, WriteError};
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use std::sync::Arc;
use futures::prelude::*;

const HASH_SCHEMA: &'static str = "HASH_INDEX_SCHEMA";

lazy_static! {
    pub static ref HASH_INDEX_SCHEMA_ID: u32 = key_hash(HASH_SCHEMA) as u32;
}

pub struct HashIndexer {
    neb_client: Arc<AsyncClient>,
}

impl HashIndexer {
    pub fn add_index(
        &self,
        cell_id: Id,
        index_id: Id,
    ) -> impl Future<Output = Result<Result<CellHeader, WriteError>, RPCError>> {
        let cell = Cell::new_with_id(*HASH_INDEX_SCHEMA_ID, &index_id, Value::Id(cell_id));
        self.neb_client.write_cell(cell)
    }

    pub fn remove_index(
        &self,
        index_id: Id,
    ) -> impl Future<Output = Result<Result<(), WriteError>, RPCError>> {
        self.neb_client.remove_cell(index_id)
    }

    pub fn query(
        &self,
        index_id: Id,
    ) -> impl Future<Output = Result<Result<Id, ReadError>, RPCError>> {
        let res = self.neb_client.read_cell(index_id);
        res.then(|res| res.map(|read| read.map(|cell| *cell.data.Id().unwrap())))
    }
}

fn hash_index_schema() -> Schema {
    Schema {
        id: *HASH_INDEX_SCHEMA_ID,
        name: HASH_SCHEMA.to_string(),
        key_field: None,
        str_key_field: None,
        fields: Field::new("*", type_id_of(Type::Id), false, false, None, vec![]),
        is_dynamic: false,
    }
}
