use ram::cell::{Cell, ReadError, WriteError};
use ram::chunk::Chunks;
use ram::types::Id;
use server::Server;

use bifrost::rpc::*;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(NEB_CELL_RPC_SERVICE) as u64;

service! {
    rpc read_cell(key: Id) -> Cell | ReadError;
    rpc write_cell(cell: Cell) -> Cell | WriteError;
    rpc update_cell(cell: Cell) -> Cell | WriteError;
    rpc remove_cell(key: Id) -> () | WriteError;
}

pub struct NebRPCService {
    server: Arc<Server>
}

impl Service for NebRPCService {
    fn read_cell(&self, key: &Id) -> Result<Cell, ReadError> {
        self.server.chunks.read_cell(&key)
    }
    fn write_cell(&self, cell: &Cell) -> Result<Cell, WriteError> {
        let mut cell = cell.clone();
        match self.server.chunks.write_cell(&mut cell) {
            Ok(_) => Ok(cell),
            Err(e) => Err(e)
        }
    }
    fn update_cell(&self, cell: &Cell) -> Result<Cell, WriteError> {
        let mut cell = cell.clone();
        match self.server.chunks.update_cell(&mut cell) {
            Ok(_) => Ok(cell),
            Err(e) => Err(e)
        }
    }
    fn remove_cell(&self, key: &Id) -> Result<(), WriteError> {
        self.server.chunks.remove_cell(&key)
    }
}

dispatch_rpc_service_functions!(NebRPCService);

impl NebRPCService {
    pub fn new(server: &Arc<Server>) -> Arc<NebRPCService> {
        Arc::new(NebRPCService {
            server: server.clone()
        })
    }
}