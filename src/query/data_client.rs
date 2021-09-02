use std::sync::Arc;

use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient, rpc::RPCError};
use dovahkiin::types::{Id, OwnedValue};
use itertools::Itertools;

use crate::{
    client::client_by_server_id,
    index::{ranged::lsm::btree::Ordering, EntryKey, IndexerClients},
    server::NebServer,
};

const SCAN_BUFFER_SIZE: u16 = 512;

pub struct IndexedDataClient {
    conshash: Arc<ConsistentHashing>,
    index_clients: Arc<IndexerClients>,
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
            conshash: conshash.clone(),
            index_clients: Arc::new(IndexerClients::new(conshash, raft_client)),
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
        selection: OwnedValue, // Checker expression
        projection: Vec<u64>,  // Column array
    ) -> Result<Option<DataCursor>, RPCError> {
        let key = EntryKey::for_schema(schema);
        self.index_clients
            .range_seek(&key, Ordering::Forward, SCAN_BUFFER_SIZE)
            .await
            .map(|opt_cursor| {
                opt_cursor.map(|cursor| {
                    let all_ids = cursor.current_block();
                    let tasks = all_ids
                        .iter()
                        .filter_map(|id| self.conshash.get_server_id_by(id).map(|sid| (sid, id)))
                        .group_by(|(sid, _id)| *sid)
                        .into_iter()
                        .map(|(sid, pairs)| {
                            let ids = pairs.map(|(_, id)| id).collect_vec();
                            async move {
                                let client = client_by_server_id(&self.conshash, sid).await;
                                client.map(|client| {

                                    // client.read_all_cells(keys)
                                })
                            }
                        });
                    unimplemented!()
                })
            })
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
