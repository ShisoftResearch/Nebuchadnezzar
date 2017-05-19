use server::transactions::TxnId;
use server::transactions::*;
use ram::cell::{Cell, ReadError, WriteError};
use ram::types::{Id};
use std::sync::Arc;
use std::io;

use bifrost::rpc::RPCError;

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
    pub client: Arc<manager::SyncServiceClient>
}

impl Transaction {

    pub fn read(&self, id: &Id) -> Result<Option<Cell>, TxnError> {
        match self.client.read(&self.tid, id) {
            Ok(Ok(TxnExecResult::Accepted(cell))) => Ok(Some(cell)),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))) => Ok(None),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Ok(Ok(TxnExecResult::Error(re))) => Err(TxnError::ReadError(re)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn write(&self, cell: &Cell) -> Result<(), TxnError> {
        match self.client.write(&self.tid, cell) {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn update(&self, cell: &Cell) -> Result<(), TxnError> {
        match self.client.update(&self.tid, cell) {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn remove(&self, id: &Id) -> Result<(), TxnError> {
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
                      DMCommitResult::CellChanged(_, _)))) =>
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