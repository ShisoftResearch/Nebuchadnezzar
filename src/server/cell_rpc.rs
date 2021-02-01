use crate::{index::builder::IndexBuilder, ram::cell::{OwnedCell, CellHeader, ReadError, WriteError}};
use crate::ram::types::Id;
use crate::server::NebServer;
use bifrost::rpc::*;
use futures::future::BoxFuture;
use futures::prelude::*;

use bifrost_plugins::hash_ident;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(NEB_CELL_RPC_SERVICE) as u64;

service! {
    rpc read_cell(key: Id) -> Result<OwnedCell, ReadError>;
    rpc read_all_cells(keys: Vec<Id>) -> Vec<Result<OwnedCell, ReadError>>;
    rpc write_cell(cell:OwnedCell) -> Result<CellHeader, WriteError>;
    rpc update_cell(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc upsert_cell(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc remove_cell(key: Id) -> Result<(), WriteError>;
    rpc count() -> u64;
}

pub struct NebRPCService {
    server: Arc<NebServer>,
}

impl Service for NebRPCService {
    fn read_cell(&self, key: Id) -> BoxFuture<Result<OwnedCell, ReadError>> {
        future::ready(self.server.chunks.read_cell(&key).map(|c| c.to_owned())).boxed()
    }
    fn read_all_cells(&self, keys: Vec<Id>) -> BoxFuture<Vec<Result<OwnedCell, ReadError>>> {
        future::ready(
            keys.into_iter()
                .map(|id| self.server.chunks.read_cell(&id).map(|c| c.to_owned()))
                .collect(),
        )
        .boxed()
    }
    fn write_cell(&self, mut cell: OwnedCell) -> BoxFuture<Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.server.chunks.write_cell(&mut cell))
    }

    fn update_cell(&self, mut cell: OwnedCell) -> BoxFuture<Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.server.chunks.update_cell(&mut cell))
    }
    fn remove_cell(&self, key: Id) -> BoxFuture<Result<(), WriteError>> {
        self.with_indices_ensured(self.server.chunks.remove_cell(&key))
    }
    fn upsert_cell(&self, mut cell: OwnedCell) -> BoxFuture<Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.server.chunks.upsert_cell(&mut cell))
    }
    fn count(&self) -> BoxFuture<u64> {
        future::ready(self.server.chunks.count() as u64).boxed()
    }
}

dispatch_rpc_service_functions!(NebRPCService);

impl NebRPCService {
    pub fn new(server: &Arc<NebServer>) -> Arc<NebRPCService> {
        Arc::new(NebRPCService {
            server: server.clone(),
        })
    }
    fn with_indices_ensured<'a, R>(&'a self, res: R) -> BoxFuture<R> where R: Send + 'a {
        if self.server.indexer.is_some() {
            IndexBuilder::await_indices().map(|_| res).boxed()
        } else {
            future::ready(res).boxed()
        }
    }
}
