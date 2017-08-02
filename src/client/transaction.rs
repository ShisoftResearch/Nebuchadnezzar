use server::transactions::TxnId;
use server::transactions::*;
use ram::cell::{Cell, ReadError, WriteError};
use ram::types::{Id, Value};
use std::sync::Arc;
use std::io;

use bifrost::rpc::RPCError;

#[derive(Debug)]
pub enum TxnError {
    CannotFindAServer,
    IoError(io::Error),
    CannotBegin,
    NotRealizable,
    TooManyRetry,
    InternalError,
    Aborted,
    RPCError(RPCError),
    ManagerError(TMError),
    ReadError(ReadError),
    WriteError(WriteError),
    PrepareError(TMPrepareResult),
    CommitError(EndResult),
    AbortError(AbortResult),
}

pub struct Transaction {
    pub tid: TxnId,
    pub state: TxnState,
    pub client: Arc<manager::SyncServiceClient>,
    pub changes: u32,
}

impl Transaction {

    pub fn read(&mut self, id: &Id) -> Result<Option<Cell>, TxnError> {
        self.changes += 1;
        match self.client.read(&self.tid, id) {
            Ok(Ok(TxnExecResult::Accepted(cell))) => Ok(Some(cell)),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))) => Ok(None),
            Ok(Ok(TxnExecResult::Error(re))) => Err(TxnError::ReadError(re)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn read_selected(&mut self, id: &Id, fields: &Vec<u64>) -> Result<Option<Vec<Value>>, TxnError> {
        self.changes += 1;
        match self.client.read_selected(&self.tid, id, fields) {
            Ok(Ok(TxnExecResult::Accepted(fields))) => Ok(Some(fields)),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))) => Ok(None),
            Ok(Ok(TxnExecResult::Error(re))) => Err(TxnError::ReadError(re)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn write(&mut self, cell: &Cell) -> Result<(), TxnError> {
        self.changes += 1;
        match self.client.write(&self.tid, cell) {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn update(&mut self, cell: &Cell) -> Result<(), TxnError> {
        self.changes += 1;
        match self.client.update(&self.tid, cell) {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn remove(&mut self, id: &Id) -> Result<(), TxnError> {
        self.changes += 1;
        match self.client.remove(&self.tid, id) {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }

    pub fn prepare(&mut self) -> Result<(), TxnError> {
        self.state = TxnState::Prepared;
        match self.client.prepare(&self.tid) {
            Ok(Ok(TMPrepareResult::Success)) => return Ok(()),
            Ok(Ok(TMPrepareResult::DMPrepareError
                  (DMPrepareResult::NotRealizable))) =>
                Err(TxnError::NotRealizable),
            Ok(Ok(TMPrepareResult::DMCommitError(
                      DMCommitResult::CellChanged(_)))) =>
                Err(TxnError::NotRealizable),
            Ok(Ok(rpr)) => Err(TxnError::PrepareError(rpr)),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn commit(&mut self) -> Result<(), TxnError> {
        self.state = TxnState::Committed;
        match self.client.commit(&self.tid) {
            Ok(Ok(EndResult::Success)) => return Ok(()),
            Ok(Ok(EndResult::SomeLocksNotReleased)) => return Ok(()),
            Ok(Ok(er)) => Err(TxnError::CommitError(er)),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn abort(&mut self) -> Result<(), TxnError> {
        if self.state == TxnState::Aborted {return Ok(());}
        self.state = TxnState::Aborted;
        match self.client.abort(&self.tid) {
            Ok(Ok(AbortResult::Success(_))) => Err(TxnError::Aborted),
            Ok(Ok(ar)) => Err(TxnError::AbortError(ar)),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
}