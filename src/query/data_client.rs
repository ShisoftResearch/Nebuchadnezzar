use std::sync::Arc;

use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient};
use dovahkiin::types::{Id, OwnedValue};

use crate::{index::IndexerClients, server::NebServer};

pub struct IndexedDataClient {
    clients: Arc<IndexerClients>,
}

pub struct ServerDataClient {
    server: Arc<NebServer>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DataCursor {
    ServerChunk(ServerChunkScanCursor),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerChunkScanCursor {
    server_id: u64,
}

impl IndexedDataClient {
    pub fn new(conshash: &Arc<ConsistentHashing>, raft_client: &Arc<RaftClient>) -> Self {
        Self {
            clients: Arc::new(IndexerClients::new(conshash, raft_client)),
        }
    }
    pub async fn range_index_scan(
        &self,
        schema: u32,
        field: u64,
        key: u64,
        selection: OwnedValue,
        projection: OwnedValue,
    ) -> DataCursor {
        unimplemented!()
    }
    pub async fn scan_all(
        &self,
        schema: u32,
        selection: OwnedValue,
        projection: OwnedValue,
    ) -> DataCursor {
        unimplemented!()
    }
}

impl ServerDataClient {
    pub fn new(server: &Arc<NebServer>) -> Self {
        Self {
            server: server.clone(),
        }
    }
    pub fn scan_chunks(
        &self,
        schema: u32,
        selection: OwnedValue,
        projection: OwnedValue,
    ) -> DataCursor {
        unimplemented!()
    }
    pub fn select_chunks(
        &self,
        cell_ids: Vec<Id>,
        selection: OwnedValue,
        projection: OwnedValue,
    ) -> Vec<OwnedValue> {
        unimplemented!()
    }
}
