use bifrost::utils::fut_exec::wait;
use ram::cell::{Cell, CellHeader, ReadError, WriteError};
use ram::types::{Id, Value};
use server::transactions::TxnId;
use server::transactions::*;
use std::cell::Cell as StdCell;
use std::io;
use std::sync::Arc;

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
    pub fn read(&self, id: Id) -> Result<Option<Cell>, TxnError> {
        match wait(self.client.read(self.tid.to_owned(), id)) {
            Ok(Ok(TxnExecResult::Accepted(cell))) => Ok(Some(cell)),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))) => Ok(None),
            Ok(Ok(TxnExecResult::Error(re))) => Err(TxnError::ReadError(re)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub fn read_selected(&self, id: Id, fields: Vec<u64>) -> Result<Option<Vec<Value>>, TxnError> {
        match wait(self.client.read_selected(self.tid.to_owned(), id, fields)) {
            Ok(Ok(TxnExecResult::Accepted(fields))) => Ok(Some(fields)),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))) => Ok(None),
            Ok(Ok(TxnExecResult::Error(re))) => Err(TxnError::ReadError(re)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub fn write(&self, cell: Cell) -> Result<(), TxnError> {
        match wait(self.client.write(self.tid.to_owned(), cell)) {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub fn update(&self, cell: Cell) -> Result<(), TxnError> {
        match wait(self.client.update(self.tid.to_owned(), cell)) {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub fn remove(&self, id: Id) -> Result<(), TxnError> {
        match wait(self.client.remove(self.tid.to_owned(), id)) {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub fn head(&self, id: Id) -> Result<Option<CellHeader>, TxnError> {
        match wait(self.client.head(self.tid.to_owned(), id)) {
            Ok(Ok(TxnExecResult::Accepted(head))) => Ok(Some(head)),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable),
            Ok(Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))) => Ok(None),
            Ok(Ok(TxnExecResult::Error(re))) => Err(TxnError::ReadError(re)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub fn upsert(&self, cell: Cell) -> Result<(), TxnError> {
        match self.head(cell.id()) {
            Ok(Some(_)) => self.update(cell),
            Ok(None) => self.write(cell),
            Err(e) => Err(e),
        }
    }
    pub fn prepare(&self) -> Result<(), TxnError> {
        self.state.set(TxnState::Prepared);
        match wait(self.client.prepare(self.tid.to_owned())) {
            Ok(Ok(TMPrepareResult::Success)) => return Ok(()),
            Ok(Ok(TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable))) => {
                Err(TxnError::NotRealizable)
            }
            Ok(Ok(TMPrepareResult::DMCommitError(DMCommitResult::CellChanged(_)))) => {
                Err(TxnError::NotRealizable)
            }
            Ok(Ok(rpr)) => Err(TxnError::PrepareError(rpr)),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub fn commit(&self) -> Result<(), TxnError> {
        self.state.set(TxnState::Committed);
        match wait(self.client.commit(self.tid.to_owned())) {
            Ok(Ok(EndResult::Success)) => return Ok(()),
            Ok(Ok(EndResult::SomeLocksNotReleased)) => return Ok(()),
            Ok(Ok(er)) => Err(TxnError::CommitError(er)),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub fn abort(&self) -> Result<(), TxnError> {
        if self.state.get() == TxnState::Aborted {
            return Ok(());
        }
        self.state.set(TxnState::Aborted);
        match wait(self.client.abort(self.tid.to_owned())) {
            Ok(Ok(AbortResult::Success(rollback_failures))) => {
                Err(TxnError::Aborted(rollback_failures))
            }
            Ok(Ok(ar)) => Err(TxnError::AbortError(ar)),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
}
