use crate::ram::cell::{OwnedCell, WriteError};
use crate::ram::types::Id;
use crate::server::Peer;
use bifrost::rpc::{RPCError, ServiceClient, ServiceClientWithId, DEFAULT_CLIENT_POOL};
use bifrost::vector_clock::{Relation, StandardVectorClock};
use std::cmp::Ordering;
use std::io;
use std::sync::Arc;

#[cfg(test)]
mod corruption_tests;
pub mod data_site;
pub mod manager;
#[cfg(test)]
mod tests;
pub mod undo_log;

pub type TxnId = StandardVectorClock;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum CellExpectation {
    Present(u64),
    Absent,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, PartialEq)]
pub enum PrepareIntent {
    Read,
    Write,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct PrepareOp {
    pub id: Id,
    pub expectation: CellExpectation,
    pub intent: PrepareIntent,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct TxnPriority {
    pub tid: TxnId,
    pub coordinator_id: u64,
}

#[derive(Deserialize)]
struct TxnIdWire {
    map: Vec<(u64, u64)>,
}

impl TxnPriority {
    pub fn new(tid: TxnId, coordinator_id: u64) -> Self {
        Self {
            tid,
            coordinator_id,
        }
    }

    pub fn compare_age(&self, other: &Self) -> Ordering {
        match semantic_relation(&self.tid, &other.tid) {
            Relation::Before => Ordering::Less,
            Relation::After => Ordering::Greater,
            Relation::Equal | Relation::Concurrent => self
                .coordinator_id
                .cmp(&other.coordinator_id)
                .then_with(|| {
                    bifrost::utils::serde::serialize(&self.tid)
                        .cmp(&bifrost::utils::serde::serialize(&other.tid))
                }),
        }
    }
}

fn semantic_relation(left: &TxnId, right: &TxnId) -> Relation {
    let left = normalized_clock_entries(left);
    let right = normalized_clock_entries(right);
    let mut left_before = false;
    let mut right_before = false;
    let mut left_idx = 0;
    let mut right_idx = 0;

    while left_idx < left.len() || right_idx < right.len() {
        match (left.get(left_idx), right.get(right_idx)) {
            (Some((left_key, left_value)), Some((right_key, right_value))) => {
                if left_key == right_key {
                    if left_value < right_value {
                        left_before = true;
                    } else if left_value > right_value {
                        right_before = true;
                    }
                    left_idx += 1;
                    right_idx += 1;
                } else if left_key < right_key {
                    if *left_value > 0 {
                        right_before = true;
                    }
                    left_idx += 1;
                } else {
                    if *right_value > 0 {
                        left_before = true;
                    }
                    right_idx += 1;
                }
            }
            (Some((_, left_value)), None) => {
                if *left_value > 0 {
                    right_before = true;
                }
                left_idx += 1;
            }
            (None, Some((_, right_value))) => {
                if *right_value > 0 {
                    left_before = true;
                }
                right_idx += 1;
            }
            (None, None) => break,
        }
    }

    match (left_before, right_before) {
        (false, false) => Relation::Equal,
        (true, false) => Relation::Before,
        (false, true) => Relation::After,
        (true, true) => Relation::Concurrent,
    }
}

fn normalized_clock_entries(clock: &TxnId) -> Vec<(u64, u64)> {
    let bytes = bifrost::utils::serde::serialize(clock);
    let mut wire = bifrost::utils::serde::deserialize::<TxnIdWire>(&bytes)
        .unwrap_or_else(|| panic!("failed to deserialize TxnId for semantic ordering"));
    wire.map.sort_unstable_by_key(|(server_id, _)| *server_id);
    for pair in wire.map.windows(2) {
        assert_ne!(
            pair[0].0, pair[1].0,
            "duplicate TxnId component for server {}",
            pair[0].0
        );
    }
    wire.map
}

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
    Cleanup,
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

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct LockReleaseFailure {
    pub cell_id: Id,
    pub reason: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum EndResult {
    CheckFailed(CheckError),
    SomeLocksNotReleased {
        released: usize,
        total: usize,
        failures: Vec<LockReleaseFailure>,
    },
    LockReleaseRetriesExhausted {
        failures: Vec<LockReleaseFailure>,
    },
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
    AlreadyCleanup,
    CannotEnd,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CommitOp {
    Write(OwnedCell),
    Update(OwnedCell),
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
    new_async_client_for_database(address, "", "").await
}

pub async fn new_async_client_for_database(
    address: &String,
    group_name: &str,
    database_name: &str,
) -> io::Result<Arc<manager::AsyncServiceClient>> {
    let client = DEFAULT_CLIENT_POOL.get(address).await?;
    let service_id = if group_name.is_empty() && database_name.is_empty() {
        manager::DEFAULT_SERVICE_ID
    } else {
        manager::generate_scoped_service_id(group_name, database_name)
    };
    Ok(manager::AsyncServiceClient::new_with_service_id(
        service_id, &client,
    ))
}

#[cfg(test)]
mod occ_type_tests {
    use super::{TxnId, TxnPriority};
    use bifrost::utils::serde::serialize;
    use bifrost::vector_clock::StandardVectorClock;
    use std::cmp::Ordering;

    fn clock(entries: &[(u64, u64)]) -> TxnId {
        StandardVectorClock::from_vec(entries.to_vec())
    }

    #[test]
    fn txn_priority_preserves_causal_order() {
        let older = TxnPriority::new(clock(&[(1, 1)]), 9);
        let newer = TxnPriority::new(clock(&[(1, 2)]), 9);

        assert_eq!(older.compare_age(&newer), Ordering::Less);
        assert_eq!(newer.compare_age(&older), Ordering::Greater);
    }

    #[test]
    fn txn_priority_totally_orders_concurrent_clocks_by_coordinator() {
        let left = TxnPriority::new(clock(&[(1, 1)]), 10);
        let right = TxnPriority::new(clock(&[(2, 1)]), 20);

        assert_eq!(left.compare_age(&right), Ordering::Less);
        assert_eq!(right.compare_age(&left), Ordering::Greater);
    }

    #[test]
    fn txn_priority_preserves_causal_order_with_missing_components() {
        let older = TxnPriority::new(clock(&[(1, 1)]), 20);
        let younger = TxnPriority::new(clock(&[(1, 1), (2, 1)]), 10);

        assert_eq!(older.compare_age(&younger), Ordering::Less);
        assert_eq!(younger.compare_age(&older), Ordering::Greater);
    }

    #[test]
    fn txn_priority_totally_orders_same_coordinator_concurrent_clocks_by_tid_bytes() {
        let left = TxnPriority::new(clock(&[(1, 1)]), 10);
        let right = TxnPriority::new(clock(&[(2, 1)]), 10);
        let expected = serialize(&left.tid).cmp(&serialize(&right.tid));

        assert_ne!(expected, Ordering::Equal);
        assert_eq!(left.compare_age(&right), expected);
        assert_eq!(right.compare_age(&left), expected.reverse());
    }
}
