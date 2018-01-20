use ram::cell::{Cell, Header, ReadError, WriteError};
use ram::types::Id;
use server::NebServer;
use bifrost::rpc::*;

use futures_cpupool::{CpuPool};
use num_cpus;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(NEB_CELL_RPC_SERVICE) as u64;

service! {
    rpc read_cell(key: Id) -> Cell | ReadError;
    rpc write_cell(cell: Cell) -> Header | WriteError;
    rpc update_cell(cell: Cell) -> Header | WriteError;
    rpc remove_cell(key: Id) -> () | WriteError;
}

pub struct NebRPCServiceInner {
    server: Arc<NebServer>,
    pool: CpuPool
}

impl NebRPCServiceInner {
    fn read_cell(this: Arc<Self>, key: Id)
        -> Box<Future<Item = Cell, Error = ReadError>>
    {
        box this.pool.spawn_fn(|| this.server.chunks.read_cell(&key))
    }
    fn write_cell(this: Arc<Self>, mut cell: Cell)
        -> Box<Future<Item = Header, Error = WriteError>>
    {
        box this.pool.spawn_fn(||
            match this.server.chunks.write_cell(&mut cell) {
                Ok(header) => Ok(header),
                Err(e) => Err(e)
            }
        )
    }
    fn update_cell(this: Arc<Self>, mut cell: Cell)
        -> Box<Future<Item = Header, Error = WriteError>>
    {
        box this.pool.spawn_fn(||
            match this.server.chunks.update_cell(&mut cell) {
                Ok(header) => Ok(header),
                Err(e) => Err(e)
            }
        )
    }
    fn remove_cell(this: Arc<Self>, key: Id)
        -> Box<Future<Item = (), Error = WriteError>>
    {
        box this.pool.spawn_fn(||this.server.chunks.remove_cell(&key))
    }
}

dispatch_rpc_service_functions!(NebRPCService);

impl NebRPCService {
    pub fn new(server: &Arc<NebServer>) -> Arc<NebRPCService> {
        Arc::new(NebRPCService {
            server: server.clone(),
            pool: CpuPool::new(4 * num_cpus::get())
        })
    }
}