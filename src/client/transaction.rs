use server::transactions::TxnId;
use server::transactions::*;
use ram::cell::{Cell, ReadError, WriteError};
use ram::types::{Id, Value};
use std::sync::Arc;
use std::io;
use std::cell::{Cell as StdCell};

use bifrost::rpc::RPCError;
use futures::Future;

#[derive(Debug)]
pub enum TxnError {
    CannotFindAServer,
    IoError(io::Error),
    CannotBegin,
    NotRealizable,
    TooManyRetry,
    InternalError,
    Aborted(Option<Vec<RollbackFailure>>),
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
    pub state: StdCell<TxnState>,
    pub client: Arc<manager::AsyncServiceClient>,
}

impl Transaction {

    pub fn read(&self, id: &Id) -> Result<Option<Cell>, TxnError> {
        match self.client.read(&self.tid, id).wait() {
            Ok(Ok(TxnExecResult::Accepted(cell))) => Ok(Some(cell)),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))) => Ok(None),
            Ok(Ok(TxnExecResult::Error(re))) => Err(TxnError::ReadError(re)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn read_selected(&self, id: &Id, fields: &Vec<u64>) -> Result<Option<Vec<Value>>, TxnError> {
        match self.client.read_selected(&self.tid, id, fields).wait() {
            Ok(Ok(TxnExecResult::Accepted(fields))) => Ok(Some(fields)),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))) => Ok(None),
            Ok(Ok(TxnExecResult::Error(re))) => Err(TxnError::ReadError(re)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn write(&self, cell: &Cell) -> Result<(), TxnError> {
        match self.client.write(&self.tid, cell).wait() {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn update(&self, cell: &Cell) -> Result<(), TxnError> {
        match self.client.update(&self.tid, cell).wait() {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn remove(&self, id: &Id) -> Result<(), TxnError> {
        match self.client.remove(&self.tid, id).wait() {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }

    pub fn prepare(&self) -> Result<(), TxnError> {
        self.state.set(TxnState::Prepared);
        match self.client.prepare(&self.tid).wait() {
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
    pub fn commit(&self) -> Result<(), TxnError> {
        self.state.set(TxnState::Committed);
        match self.client.commit(&self.tid).wait() {
            Ok(Ok(EndResult::Success)) => return Ok(()),
            Ok(Ok(EndResult::SomeLocksNotReleased)) => return Ok(()),
            Ok(Ok(er)) => Err(TxnError::CommitError(er)),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
    pub fn abort(&self) -> Result<(), TxnError> {
        if self.state.get() == TxnState::Aborted {return Ok(());}
        self.state.set(TxnState::Aborted);
        match self.client.abort(&self.tid).wait() {
            Ok(Ok(AbortResult::Success(rollback_failures)))
                => Err(TxnError::Aborted(rollback_failures)),
            Ok(Ok(ar)) => Err(TxnError::AbortError(ar)),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e))
        }
    }
}