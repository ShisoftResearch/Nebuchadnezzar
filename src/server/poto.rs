use ram::cell::{Cell, ReadError, WriteError};
use ram::chunk::Chunks;
use ram::types::Id;

use bifrost::rpc::*;

service! {
    rpc read_cell(key: Id) -> Cell | ReadError;
    rpc write_cell(cell: Cell) -> Cell | WriteError;
    rpc update_cell(cell: Cell) -> Cell | WriteError;
    rpc remove_cell(key: Id) -> () | WriteError;
}

pub struct NebRPCService {
    chunks: Arc<Chunks>
}

impl Service for NebRPCService {
    fn read_cell(&self, key: Id) -> Result<Cell, ReadError> {
        Err(ReadError::CellDoesNotExisted)
    }
    fn write_cell(&self, cell: Cell)-> Result<Cell, WriteError> {
        Err(WriteError::CellDoesNotExisted)
    }
    fn update_cell(&self, cell: Cell)-> Result<Cell, WriteError> {
        Err(WriteError::CellDoesNotExisted)
    }
    fn remove_cell(&self, key: Id) -> Result<(), WriteError> {
        Err(WriteError::CellDoesNotExisted)
    }
}

dispatch_rpc_service_functions!(NebRPCService);