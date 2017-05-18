use server::transactions::TxnId;
use ram::cell::{Cell, ReadError, WriteError};
use ram::types::{Id};

pub enum TxnError {

}

pub struct Transaction {
    tid: TxnId
}

impl Transaction {
    pub fn read(id: &Id) -> Result<Cell, TxnError> {
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