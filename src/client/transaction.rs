use server::transactions::TxnId;
use server::transactions::*;
use ram::cell::{Cell, ReadError, WriteError};
use ram::types::{Id};
use std::sync::Arc;
use std::io;

pub enum TxnError {
    CannotFindAServer,
    IoError(io::Error),
    CannotBegin,
    NotRealizable,
    TooManyRetry,
}

pub struct Transaction {
    pub tid: TxnId,
    pub client: Arc<manager::SyncServiceClient>
}

impl Transaction {

    pub fn read(id: &Id) -> Result<Option<Cell>, TxnError> {
        unimplemented!()
    }
    pub fn write(cell: &Cell) -> Result<(), TxnError> {
        unimplemented!()
    }
    pub fn update(cell: &Cell) -> Result<(), TxnError> {
        unimplemented!()
    }
    pub fn remove(id: &Id) -> Result<(), TxnError> {
        unimplemented!()
    }

    pub fn prepare() -> Result<(), TxnError> {
        unimplemented!()
    }
    pub fn commit() -> Result<(), TxnError> {
        unimplemented!()
    }
    pub fn abort() -> Result<(), TxnError> {
        unimplemented!()
    }
}