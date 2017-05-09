use bifrost::vector_clock::{VectorClock, StandardVectorClock, ServerVectorClock};
use bifrost::utils::time::get_time;
use bifrost::rpc::RPCError;
use ram::cell::{Cell, WriteError};
use ram::types::{Id};
use std::sync::Arc;
use rand::Rng;

pub mod manager;
pub mod data_site;

// Peer have a clock, meant to update with other servers in the cluster
pub struct Peer {
    pub clock: ServerVectorClock
}

impl Peer {
    pub fn new(server_address: &String) -> Peer {
        Peer {
            clock: ServerVectorClock::new(server_address)
        }
    }
}

pub type TransactionId = StandardVectorClock;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum TransactionExecResult<A, E>
where A: Clone, E: Clone {
    Rejected,
    Wait,
    Accepted(A),
    Error(E),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataSiteResponse<T> {
    pub payload: T,
    pub clock: StandardVectorClock
}

impl <T> DataSiteResponse <T> {
    pub fn new(peer: &Peer, data: T) -> DataSiteResponse<T> {
        DataSiteResponse {
            payload: data,
            clock: peer.clock.to_clock()
        }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Copy, Clone)]
enum TransactionState {
    Started,
    Aborted,
    Committing,
    Committed,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DMPrepareResult {
    Wait,
    Success,
    TransactionNotExisted,
    NotRealizable,
    TransactionStateError(TransactionState),
    NetworkError
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DMCommitResult {
    Success,
    WriteError(Id, WriteError, Vec<RollbackFailure>),
    CellChanged(Id, Vec<RollbackFailure>),
    CheckFailed(CheckError),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AbortResult {
    CheckFailed(CheckError),
    Success(Option<Vec<RollbackFailure>>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EndResult {
    CheckFailed(CheckError),
    SomeLocksNotReleased,
    Success,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RollbackFailure {
    id: Id,
    error: WriteError
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CheckError {
    CellNumberDoesNotMatch(usize, usize),
    TransactionNotExisted,
    TransactionNotCommitted,
    TransactionAlreadyCommitted,
    TransactionAlreadyAborted,
    TransactionCannotEnd,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CommitOp {
    Write(Cell),
    Update(Cell),
    Remove(Id)
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TMPrepareResult {
    Success,
    DMPrepareError(DMPrepareResult),
    DMCommitError(DMCommitResult),
    CheckFailed(CheckError),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TMCommitResult {
    Success,
    CheckFailed(CheckError),
}
