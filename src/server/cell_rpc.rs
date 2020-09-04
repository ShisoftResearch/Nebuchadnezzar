use crate::ram::cell::{Cell, CellHeader, ReadError, WriteError};
use crate::ram::types::Id;
use crate::server::NebServer;
use bifrost::rpc::*;
use futures::future::BoxFuture;
use futures::prelude::*;

use bifrost_plugins::hash_ident;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(NEB_CELL_RPC_SERVICE) as u64;

service! {
    rpc read_cell(key: Id) -> Result<Cell, ReadError>;
    rpc write_cell(cell: Cell) -> Result<CellHeader, WriteError>;
    rpc update_cell(cell: Cell) -> Result<CellHeader, WriteError>;
    rpc upsert_cell(cell: Cell) -> Result<CellHeader, WriteError>;
    rpc remove_cell(key: Id) -> Result<(), WriteError>;
    rpc count() -> u64;
}

pub struct NebRPCService {
    server: Arc<NebServer>,
}

impl Service for NebRPCService {
    fn read_cell(&self, key: Id) -> BoxFuture<Result<Cell, ReadError>> {
        future::ready(self.server.chunks.read_cell(&key)).boxed()
    }
    fn write_cell(&self, mut cell: Cell) -> BoxFuture<Result<CellHeader, WriteError>> {
        future::ready(self.server.chunks.write_cell(&mut cell)).boxed()
    }
    fn update_cell(&self, mut cell: Cell) -> BoxFuture<Result<CellHeader, WriteError>> {
        future::ready(self.server.chunks.update_cell(&mut cell)).boxed()
    }
    fn remove_cell(&self, key: Id) -> BoxFuture<Result<(), WriteError>> {
        future::ready(self.server.chunks.remove_cell(&key)).boxed()
    }
    fn upsert_cell(&self, mut cell: Cell) -> BoxFuture<Result<CellHeader, WriteError>> {
        future::ready(self.server.chunks.upsert_cell(&mut cell)).boxed()
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
}
