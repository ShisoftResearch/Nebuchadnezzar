use crate::ram::cell::{OwnedCell, WriteError};
use crate::ram::types::Id;
use bifrost::hlc::Hlc;
use bifrost::rpc::{RPCError, ServiceClient, DEFAULT_CLIENT_POOL};
use std::cmp::Ordering;
use std::io;
use std::sync::Arc;

#[cfg(test)]
mod corruption_tests;
pub mod data_site;
pub mod manager;
#[cfg(test)]
mod occ_tests;
#[cfg(feature = "occ_phase_profile")]
pub mod phase_profile;
#[cfg(test)]
mod tests;
pub mod undo_log;

pub type TxnId = bifrost::hlc::Hlc;

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

impl TxnPriority {
    pub fn new(tid: TxnId, coordinator_id: u64) -> Self {
        Self {
            tid,
            coordinator_id,
        }
    }

    pub fn compare_age(&self, other: &Self) -> Ordering {
        // HLC is a total order that extends causality (node id is inside the
        // tid), so classic Wait-Die age comparison is a plain transitive
        // `cmp` — no partial-order relation, coordinator tie-break, or
        // deterministic canonicalization needed.
        self.tid.cmp(&other.tid)
    }
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
    pub clock: Hlc,
}

impl<T> DataSiteResponse<T> {
    pub fn new(clock: Hlc, data: T) -> DataSiteResponse<T> {
        DataSiteResponse {
            payload: data,
            clock,
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
    use bifrost::vector_clock::{Relation, StandardVectorClock};
    use serde_json::json;
    use std::cmp::Ordering;
    use std::panic::catch_unwind;

    fn clock(entries: &[(u64, u64)]) -> TxnId {
        StandardVectorClock::from_vec(entries.to_vec())
    }

    fn raw_clock(entries: &[(u64, u64)]) -> TxnId {
        serde_json::from_value(json!({ "map": entries })).unwrap()
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
    fn txn_priority_totally_orders_same_coordinator_concurrent_clocks_without_serialization() {
        let left = TxnPriority::new(clock(&[(1, 1)]), 10);
        let right = TxnPriority::new(clock(&[(2, 1)]), 10);
        let expected = left.tid.deterministic_cmp(&right.tid);

        assert_ne!(expected, Ordering::Equal);
        assert_eq!(left.compare_age(&right), expected);
        assert_eq!(right.compare_age(&left), expected.reverse());
    }

    #[test]
    fn txn_priority_canonicalizes_duplicate_components_by_max_counter() {
        let older = TxnPriority::new(raw_clock(&[(1, 1), (1, 3)]), 20);
        let younger = TxnPriority::new(clock(&[(1, 3), (2, 1)]), 10);

        assert_eq!(older.tid.relation(&younger.tid), Relation::Before);
        assert_eq!(younger.tid.relation(&older.tid), Relation::After);
        let forward = catch_unwind(|| older.compare_age(&younger));
        let reverse = catch_unwind(|| younger.compare_age(&older));

        assert_eq!(forward.unwrap(), Ordering::Less);
        assert_eq!(reverse.unwrap(), Ordering::Greater);
    }

    #[test]
    fn txn_priority_preserves_causal_order_for_unsorted_zero_components() {
        let older = TxnPriority::new(raw_clock(&[(3, 0), (2, 1), (1, 1)]), 20);
        let younger = TxnPriority::new(raw_clock(&[(4, 1), (2, 1), (1, 1), (3, 0)]), 10);

        assert_eq!(older.tid.relation(&younger.tid), Relation::Before);
        assert_eq!(younger.tid.relation(&older.tid), Relation::After);
        assert_eq!(older.compare_age(&younger), Ordering::Less);
        assert_eq!(younger.compare_age(&older), Ordering::Greater);
    }

    #[test]
    fn txn_priority_tie_breaks_semantically_equal_unsorted_zero_clocks_by_deterministic_cmp() {
        let left = TxnPriority::new(raw_clock(&[(2, 0), (1, 1)]), 10);
        let right = TxnPriority::new(raw_clock(&[(1, 1), (3, 0)]), 10);
        let expected = left.tid.deterministic_cmp(&right.tid);

        assert_eq!(left.tid.relation(&right.tid), Relation::Equal);
        assert_ne!(expected, Ordering::Equal);
        assert_eq!(left.compare_age(&right), expected);
        assert_eq!(right.compare_age(&left), expected.reverse());
    }
}
