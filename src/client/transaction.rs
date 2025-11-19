use crate::ram::cell::{CellHeader, OwnedCell, ReadError, WriteError};
use crate::ram::types::Id;
use crate::server::transactions::TxnId;
use crate::server::transactions::*;
use std::cell::Cell as StdCell;
use std::io;
use std::sync::Arc;

use bifrost::rpc::RPCError;

#[derive(Debug, Clone)]
pub enum NotRealizableReason {
    ReadTooLate(Id),
    EarlyConflict(Id),
    PrepareError,
    CellChanged(Id),
}

#[derive(Debug)]
pub struct RetryInfo {
    pub attempts: u32,
    pub reasons: Vec<NotRealizableReason>,
    pub last_reason: Option<NotRealizableReason>,
}

impl std::fmt::Display for RetryInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Failed after {} attempts. Last reason: {:?}",
            self.attempts,
            self.last_reason
        )?;

        // Count occurrences of each reason type
        let mut read_too_late_count = 0;
        let mut early_conflict_count = 0;
        let mut prepare_error_count = 0;
        let mut cell_changed_count = 0;

        for reason in &self.reasons {
            match reason {
                NotRealizableReason::ReadTooLate(_) => read_too_late_count += 1,
                NotRealizableReason::EarlyConflict(_) => early_conflict_count += 1,
                NotRealizableReason::PrepareError => prepare_error_count += 1,
                NotRealizableReason::CellChanged(_) => cell_changed_count += 1,
            }
        }

        write!(f, "\nFailure breakdown:")?;
        if read_too_late_count > 0 {
            write!(f, "\n  ReadTooLate: {} times", read_too_late_count)?;
        }
        if early_conflict_count > 0 {
            write!(f, "\n  EarlyConflict: {} times", early_conflict_count)?;
        }
        if prepare_error_count > 0 {
            write!(f, "\n  PrepareError: {} times", prepare_error_count)?;
        }
        if cell_changed_count > 0 {
            write!(f, "\n  CellChanged: {} times", cell_changed_count)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum TxnError {
    CannotFindAServer,
    IoError(io::Error),
    CannotBegin,
    NotRealizable(NotRealizableReason),
    TooManyRetry(RetryInfo),
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

#[derive(Clone)]
pub struct Transaction {
    pub tid: TxnId,
    pub state: StdCell<TxnState>,
    pub client: Arc<manager::AsyncServiceClient>,
}

unsafe impl Send for Transaction {}
unsafe impl Sync for Transaction {}

impl Transaction {
    pub async fn read(&self, id: Id) -> Result<Option<OwnedCell>, TxnError> {
        match self.client.read(self.tid.to_owned(), id).await {
            Ok(Ok(TxnExecResult::Accepted(cell))) => Ok(Some(cell)),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable(
                NotRealizableReason::ReadTooLate(id),
            )),
            Ok(Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))) => Ok(None),
            Ok(Ok(TxnExecResult::Error(re))) => Err(TxnError::ReadError(re)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub async fn read_selected(
        &self,
        id: Id,
        fields: Vec<u64>,
    ) -> Result<Option<OwnedCell>, TxnError> {
        match self
            .client
            .read_selected(self.tid.to_owned(), id, fields.clone())
            .await
        {
            Ok(Ok(TxnExecResult::Accepted(fields))) => Ok(Some(fields)),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable(
                NotRealizableReason::ReadTooLate(id),
            )),
            Ok(Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))) => Ok(None),
            Ok(Ok(TxnExecResult::Error(re))) => Err(TxnError::ReadError(re)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub async fn write(&self, cell: OwnedCell) -> Result<(), TxnError> {
        let cell_id = cell.id();
        match self.client.write(self.tid.to_owned(), cell).await {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable(
                NotRealizableReason::EarlyConflict(cell_id),
            )),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub async fn update(&self, cell: OwnedCell) -> Result<(), TxnError> {
        let cell_id = cell.id();
        match self.client.update(self.tid.to_owned(), cell).await {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable(
                NotRealizableReason::EarlyConflict(cell_id),
            )),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub async fn remove(&self, id: Id) -> Result<(), TxnError> {
        match self.client.remove(self.tid.to_owned(), id).await {
            Ok(Ok(TxnExecResult::Accepted(()))) => Ok(()),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable(
                NotRealizableReason::EarlyConflict(id),
            )),
            Ok(Ok(TxnExecResult::Error(we))) => Err(TxnError::WriteError(we)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub async fn head(&self, id: Id) -> Result<Option<CellHeader>, TxnError> {
        match self.client.head(self.tid.to_owned(), id).await {
            Ok(Ok(TxnExecResult::Accepted(head))) => Ok(Some(head)),
            Ok(Ok(TxnExecResult::Rejected)) => Err(TxnError::NotRealizable(
                NotRealizableReason::ReadTooLate(id),
            )),
            Ok(Ok(TxnExecResult::Error(ReadError::CellDoesNotExisted))) => Ok(None),
            Ok(Ok(TxnExecResult::Error(re))) => Err(TxnError::ReadError(re)),
            Ok(Ok(_)) => Err(TxnError::InternalError),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub async fn upsert(&self, cell: OwnedCell) -> Result<(), TxnError> {
        match self.head(cell.id()).await {
            Ok(Some(_)) => self.update(cell).await,
            Ok(None) => self.write(cell).await,
            Err(e) => Err(e),
        }
    }
    pub async fn prepare(&self) -> Result<(), TxnError> {
        self.state.set(TxnState::Prepared);
        match self.client.prepare(self.tid.to_owned()).await {
            Ok(Ok(TMPrepareResult::Success)) => return Ok(()),
            Ok(Ok(TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable))) => {
                Err(TxnError::NotRealizable(NotRealizableReason::PrepareError))
            }
            Ok(Ok(TMPrepareResult::DMCommitError(DMCommitResult::CellChanged(cell_id)))) => Err(
                TxnError::NotRealizable(NotRealizableReason::CellChanged(cell_id)),
            ),
            Ok(Ok(rpr)) => Err(TxnError::PrepareError(rpr)),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub async fn commit(&self) -> Result<(), TxnError> {
        self.state.set(TxnState::Committed);
        match self.client.commit(self.tid.to_owned()).await {
            Ok(Ok(EndResult::Success)) => return Ok(()),
            Ok(Ok(EndResult::SomeLocksNotReleased)) => return Ok(()),
            Ok(Ok(er)) => Err(TxnError::CommitError(er)),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
    pub async fn abort(&self) -> Result<(), TxnError> {
        if self.state.get() == TxnState::Aborted {
            return Ok(());
        }
        self.state.set(TxnState::Aborted);
        match self.client.abort(self.tid.to_owned()).await {
            Ok(Ok(AbortResult::Success(rollback_failures))) => {
                Err(TxnError::Aborted(rollback_failures))
            }
            Ok(Ok(ar)) => Err(TxnError::AbortError(ar)),
            Ok(Err(tme)) => Err(TxnError::ManagerError(tme)),
            Err(e) => Err(TxnError::RPCError(e)),
        }
    }
}
