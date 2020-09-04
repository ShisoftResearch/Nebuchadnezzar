use crate::ram::cell::{Cell, WriteError};
use crate::ram::types::Id;
use crate::server::Peer;
use bifrost::rpc::{RPCError, DEFAULT_CLIENT_POOL};
use bifrost::vector_clock::StandardVectorClock;
use std::io;
use std::sync::Arc;

pub mod data_site;
pub mod manager;
#[cfg(test)]
mod tests;

pub type TxnId = StandardVectorClock;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum TxnExecResult<A, E>
where
    A: Send + Clone,
    E: Send + Clone,
{
    Rejected,
    Wait,
    Accepted(A),
    Error(E),
    StateError(TxnState),
}

impl<A, E> TxnExecResult<A, E>
where
    A: Send + Clone,
    E: Send + Clone,
{
    pub fn unwrap(self) -> A {
        match self {
            TxnExecResult::Accepted(data) => data,
            _ => {
                panic!("no data for result because it is not accepted");
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataSiteResponse<T> {
    pub payload: T,
    pub clock: StandardVectorClock,
}

impl<T> DataSiteResponse<T> {
    pub fn new(peer: &Peer, data: T) -> DataSiteResponse<T> {
        DataSiteResponse {
            payload: data,
            clock: peer.clock.to_clock(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Copy, Clone)]
pub enum TxnState {
    Started,
    Aborted,
    Prepared,
    Committed,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DMPrepareResult {
    Wait,
    Success,
    TransactionNotExisted,
    NotRealizable,
    StateError(TxnState),
    NetworkError,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DMCommitResult {
    Success,
    WriteError(Id, WriteError),
    CellChanged(Id),
    CheckFailed(CheckError),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum AbortResult {
    CheckFailed(CheckError),
    Success(Option<Vec<RollbackFailure>>),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum EndResult {
    CheckFailed(CheckError),
    SomeLocksNotReleased,
    Success,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RollbackFailure {
    id: Id,
    error: WriteError,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum CheckError {
    CellNumberDoesNotMatch(usize, usize),
    NotExisted,
    NotCommitted,
    AlreadyCommitted,
    AlreadyAborted,
    CannotEnd,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CommitOp {
    Write(Cell),
    Update(Cell),
    Remove(Id),
    Read(Id, u64), // id, version
    None,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum TMPrepareResult {
    Success,
    DMPrepareError(DMPrepareResult),
    DMCommitError(DMCommitResult),
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum TMError {
    TransactionNotFound,
    TransactionIdExisted,
    CannotLocateCellServer,
    RPCErrorFromCellServer,
    AssertionError,
    InvalidTransactionState(TxnState),
    Other,
}

pub async fn new_async_client(address: &String) -> io::Result<Arc<manager::AsyncServiceClient>> {
    let client = DEFAULT_CLIENT_POOL.get(address).await?;
    Ok(manager::AsyncServiceClient::new(
        manager::DEFAULT_SERVICE_ID,
        &client,
    ))
}
