//! The client side of a transaction.
//!
//! Contract: a cell takes plain (non-transactional) writes or transactions,
//! not both. If a caller mixes them on one cell, Neb does not guarantee that
//! an acknowledged plain write or delete survives the resolution of an
//! in-doubt transaction: after a restart with a transaction T in doubt, the
//! plain write and T's write can get the same version, and only one is kept;
//! an acknowledged delete is undone when the resolution installs T's cell.

use crate::ram::cell::{CellHeader, OwnedCell, ReadError, WriteError};
use crate::ram::types::Id;
use crate::server::transactions::TxnId;
use crate::server::transactions::*;
use std::cell::Cell as StdCell;
use std::collections::HashMap;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReasonType {
    ReadTooLate,
    EarlyConflict,
    PrepareError,
    CellChanged,
}

impl From<&NotRealizableReason> for ReasonType {
    fn from(reason: &NotRealizableReason) -> Self {
        match reason {
            NotRealizableReason::ReadTooLate(_) => ReasonType::ReadTooLate,
            NotRealizableReason::EarlyConflict(_) => ReasonType::EarlyConflict,
            NotRealizableReason::PrepareError => ReasonType::PrepareError,
            NotRealizableReason::CellChanged(_) => ReasonType::CellChanged,
        }
    }
}

#[derive(Debug)]
pub struct RetryInfo {
    pub attempts: u32,
    pub reason_counts: HashMap<ReasonType, u32>,
    pub last_reason: Option<NotRealizableReason>,
}

impl std::fmt::Display for RetryInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Failed after {} attempts. Last reason: {:?}",
            self.attempts, self.last_reason
        )?;

        write!(f, "\nFailure breakdown:")?;
        if let Some(&count) = self.reason_counts.get(&ReasonType::ReadTooLate) {
            write!(f, "\n  ReadTooLate: {} times", count)?;
        }
        if let Some(&count) = self.reason_counts.get(&ReasonType::EarlyConflict) {
            write!(f, "\n  EarlyConflict: {} times", count)?;
        }
        if let Some(&count) = self.reason_counts.get(&ReasonType::PrepareError) {
            write!(f, "\n  PrepareError: {} times", count)?;
        }
        if let Some(&count) = self.reason_counts.get(&ReasonType::CellChanged) {
            write!(f, "\n  CellChanged: {} times", count)?;
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
    AbortError(AbortResult),
    /// The commit request reached the coordinator, and the outcome is not
    /// known (D33): read the data again before any retry.
    OutcomeUnknown,
    /// No participant received this commit decision. A new transaction can retry.
    CommitNotApplied,
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
    /// Commit. The COMMIT is appended at every participant and its sync is
    /// queued (D19 option 1): the result survives a process crash, and a
    /// power loss may drop the whole transaction, never part of it.
    pub async fn commit(&self) -> Result<(), TxnError> {
        self.commit_with(false).await
    }

    /// Commit with the COMMIT synced at every participant before the result
    /// (D7): the result survives a power loss.
    pub async fn commit_durable(&self) -> Result<(), TxnError> {
        self.commit_with(true).await
    }

    async fn commit_with(&self, durable: bool) -> Result<(), TxnError> {
        // Do not send another commit after a request whose outcome can be unknown.
        if self.state.get() == TxnState::Committed {
            return Err(TxnError::OutcomeUnknown);
        }
        let previous_state = self.state.replace(TxnState::Committed);
        let result = if durable {
            self.client.commit_durable(self.tid.to_owned()).await
        } else {
            self.client.commit(self.tid.to_owned()).await
        };
        match result {
            Ok(Ok(EndResult::Success)) => return Ok(()),
            Ok(Ok(EndResult::SomeLocksNotReleased {
                released,
                total,
                ref failures,
            })) => {
                // Partial lock release - log warning but allow commit to succeed
                warn!(
                    "Transaction {:?} committed but some locks not released: {}/{} released, {} failures",
                    self.tid,
                    released,
                    total,
                    failures.len()
                );
                return Ok(());
            }
            Ok(Ok(EndResult::LockReleaseRetriesExhausted { ref failures })) => {
                // The decision is COMMIT even when lock release fails.
                warn!(
                    "Transaction {:?} committed; lock release retries exhausted, {} failures",
                    self.tid,
                    failures.len()
                );
                Ok(())
            }
            Ok(Ok(EndResult::DecidedPending)) => {
                // Decided COMMIT: every site keeps it and finishes its close
                // later. A retry would apply it twice.
                warn!(
                    "Transaction {:?} is committed; its sites finish closing it later",
                    self.tid
                );
                Ok(())
            }
            // One site can refuse after another site accepts COMMIT.
            Ok(Ok(_)) => Err(TxnError::OutcomeUnknown),
            // The manager resolves every participant before it sends any `end`.
            Ok(Err(TMError::CannotLocateCellServer)) => {
                self.state.set(previous_state);
                Err(TxnError::CommitNotApplied)
            }
            Ok(Err(
                error @ TMError::InvalidTransactionState(TxnState::Started | TxnState::Aborted),
            )) => {
                self.state.set(previous_state);
                Err(TxnError::ManagerError(error))
            }
            // Missing or closed manager state cannot prove that COMMIT did not run.
            Ok(Err(_)) => Err(TxnError::OutcomeUnknown),
            Err(error) if error.request_was_not_sent() => {
                self.state.set(previous_state);
                Err(TxnError::CommitNotApplied)
            }
            Err(bifrost::rpc::RPCError::RequestError(
                bifrost::rpc::RPCRequestError::FunctionIdNotFound
                | bifrost::rpc::RPCRequestError::ServiceIdNotFound
                | bifrost::rpc::RPCRequestError::BadRequest,
            )) => {
                self.state.set(previous_state);
                Err(TxnError::CommitNotApplied)
            }
            // The request may have been taken and `end` sent (D33).
            Err(_) => Err(TxnError::OutcomeUnknown),
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

#[cfg(test)]
mod commit_outcome_tests {
    use super::*;
    use bifrost::bytes::{BufMut, BytesMut};
    use bifrost::rpc::{RPCClient, ServiceClient};
    use std::pin::Pin;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn commit_response(
        response: Option<Result<EndResult, TMError>>,
        durable: bool,
    ) -> Result<(), TxnError> {
        let address = crate::utils::test_port::unique_localhost_addr();
        let listener = TcpListener::bind(&address).await.unwrap();
        let rpc = RPCClient::new_async(&address).await.unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let length = socket.read_u32().await.unwrap();
            let mut request = vec![0; length as usize];
            socket.read_exact(&mut request).await.unwrap();
            assert!(request.len() > 24, "a commit request must arrive");
            if let Some(response) = response {
                let mut reply = BytesMut::from(&request[..8]);
                reply.put_u8(0);
                reply.extend_from_slice(&bifrost::utils::serde::serialize(&response));
                socket.write_u32(reply.len() as u32).await.unwrap();
                socket.write_all(&reply).await.unwrap();
            }
        });
        let txn = Transaction {
            tid: TxnId::default(),
            state: StdCell::new(TxnState::Prepared),
            client: manager::AsyncServiceClient::new_with_service_id(0, &rpc),
        };
        let result = txn.commit_with(durable).await;
        peer.await.unwrap();
        result
    }

    #[tokio::test]
    async fn commit_outcome_unsent_is_retryable_control_lost_reply_is_unknown() {
        let address = crate::utils::test_port::unique_localhost_addr();
        let listener = TcpListener::bind(&address).await.unwrap();
        let rpc = RPCClient::new_async(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        drop(socket);
        drop(listener);
        // Drain the dead connection with a non-commit request. The next call must reconnect.
        assert!(RPCClient::send_async(Pin::new(&*rpc), 0, BytesMut::new())
            .await
            .is_err());
        let txn = Transaction {
            tid: TxnId::default(),
            state: StdCell::new(TxnState::Prepared),
            client: manager::AsyncServiceClient::new_with_service_id(0, &rpc),
        };
        let result = txn.commit().await;
        assert!(
            matches!(result, Err(TxnError::CommitNotApplied)),
            "{result:?}"
        );
        assert_eq!(txn.state.get(), TxnState::Prepared);
        assert!(matches!(
            commit_response(None, false).await,
            Err(TxnError::OutcomeUnknown)
        ));
    }

    #[tokio::test]
    async fn commit_outcome_refusals_remain_unknown() {
        for durable in [false, true] {
            for refusal in [
                EndResult::StoreFull(WriteError::CannotAllocateSpace),
                EndResult::WriteError(WriteError::TransactionHeadUnavailable),
                EndResult::CheckFailed(CheckError::CannotEnd),
            ] {
                let result = commit_response(Some(Ok(refusal)), durable).await;
                assert!(
                    matches!(result, Err(TxnError::OutcomeUnknown)),
                    "{result:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn commit_outcome_manager_errors_are_classified() {
        for error in [
            TMError::TransactionNotFound,
            TMError::TransactionIdExisted,
            TMError::RPCErrorFromCellServer,
            TMError::AssertionError,
            TMError::Other,
            TMError::InvalidTransactionState(TxnState::Prepared),
            TMError::InvalidTransactionState(TxnState::Committed),
            TMError::InvalidTransactionState(TxnState::Cleanup),
            TMError::OutcomeUnknown,
        ] {
            let result = commit_response(Some(Err(error)), false).await;
            assert!(
                matches!(result, Err(TxnError::OutcomeUnknown)),
                "{result:?}"
            );
        }
        assert!(matches!(
            commit_response(Some(Err(TMError::CannotLocateCellServer)), false).await,
            Err(TxnError::CommitNotApplied)
        ));
        for state in [TxnState::Started, TxnState::Aborted] {
            assert!(
                matches!(commit_response(Some(Err(TMError::InvalidTransactionState(state))), false).await,
                Err(TxnError::ManagerError(TMError::InvalidTransactionState(actual))) if actual == state)
            );
        }
    }

    #[tokio::test]
    async fn commit_outcome_success_control() {
        assert!(commit_response(Some(Ok(EndResult::Success)), false)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn commit_outcome_known_commit_controls() {
        for response in [
            EndResult::Success,
            EndResult::DecidedPending,
            EndResult::SomeLocksNotReleased {
                released: 0,
                total: 1,
                failures: vec![],
            },
            EndResult::LockReleaseRetriesExhausted { failures: vec![] },
        ] {
            assert!(commit_response(Some(Ok(response)), true).await.is_ok());
        }
    }
}
