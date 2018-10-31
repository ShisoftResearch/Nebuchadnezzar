use ram::cell::{Cell, CellHeader, ReadError, WriteError};
use ram::types::Id;
use server::NebServer;
use bifrost::rpc::*;

use futures::prelude::*;
use num_cpus;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(NEB_CELL_RPC_SERVICE) as u64;

service! {
    rpc read_cell(key: Id) -> Cell | ReadError;
    rpc write_cell(cell: Cell) -> CellHeader | WriteError;
    rpc update_cell(cell: Cell) -> CellHeader | WriteError;
    rpc upsert_cell(cell: Cell) -> CellHeader | WriteError;
    rpc remove_cell(key: Id) -> () | WriteError;
}

pub struct NebRPCService {
    inner: Arc<NebRPCServiceInner>
}

pub struct NebRPCServiceInner {
    server: Arc<NebServer>
}

impl Service for NebRPCService {
    fn read_cell(&self, key: Id) -> Box<Future<Item = Cell, Error = ReadError>> {
        NebRPCServiceInner::read_cell(self.inner.clone(), key)
    }
    fn write_cell(&self, mut cell: Cell) -> Box<Future<Item =CellHeader, Error = WriteError>> {
        NebRPCServiceInner::write_cell(self.inner.clone(), cell)
    }
    fn update_cell(&self, mut cell: Cell) -> Box<Future<Item =CellHeader, Error = WriteError>> {
        NebRPCServiceInner::update_cell(self.inner.clone(), cell)
    }
    fn upsert_cell(&self, mut cell: Cell) -> Box<Future<Item =CellHeader, Error = WriteError>> {
        NebRPCServiceInner::upsert_cell(self.inner.clone(), cell)
    }
    fn remove_cell(&self, key: Id) -> Box<Future<Item = (), Error = WriteError>> {
        NebRPCServiceInner::remove_cell(self.inner.clone(), key)
    }
}

impl NebRPCServiceInner {
    fn read_cell(this: Arc<Self>, key: Id)
        -> Box<Future<Item = Cell, Error = ReadError>>
    {
        box future::result(this.server.chunks.read_cell(&key))
    }
    fn write_cell(this: Arc<Self>, mut cell: Cell)
        -> Box<Future<Item =CellHeader, Error = WriteError>>
    {
        box future::result(this.server.chunks.write_cell(&mut cell))
    }
    fn update_cell(this: Arc<Self>, mut cell: Cell)
        -> Box<Future<Item =CellHeader, Error = WriteError>>
    {
        box future::result(this.server.chunks.update_cell(&mut cell))
    }
    fn remove_cell(this: Arc<Self>, key: Id)
        -> Box<Future<Item = (), Error = WriteError>>
    {
        box future::result(this.server.chunks.remove_cell(&key))
    }
    fn upsert_cell(this: Arc<Self>, mut cell: Cell)
        -> Box<Future<Item =CellHeader, Error = WriteError>>
    {
        box future::result(this.server.chunks.upsert_cell(&mut cell))
    }
}

dispatch_rpc_service_functions!(NebRPCService);

impl NebRPCService {
    pub fn new(server: &Arc<NebServer>) -> Arc<NebRPCService> {
        Arc::new(NebRPCService {
            inner: Arc::new(NebRPCServiceInner {
                server: server.clone()
            })
        })
    }
}