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
    /// The writes were applied but could not be made durable: their WAL
    /// could not be synced, or the commit marker could not be written.
    ///
    /// Reporting success in that state would promise a durability that does
    /// not exist -- the data is in memory and in the index, but a crash
    /// would take it. The transaction is left for recovery to roll back,
    /// which is what its still-incomplete undo entries ask for.
    NotDurable(String),
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

/// Shared test-support constructor for transaction ids. Placed at module top
/// level (not inside a `#[cfg(test)] mod`) so child test modules can reach it
/// via `super::test_hlc` / `crate::server::transactions::test_hlc`. The old
/// vector-clock `from_vec(vec![(server, counter)])` maps to `test_hlc(counter,
/// server)`: the counter drove age ordering and is now the HLC `ts`, the server
/// id is now the HLC `node`.
#[cfg(test)]
pub fn test_hlc(ts: u64, node: u64) -> TxnId {
    bifrost::hlc::Hlc { ts, node }
}

#[cfg(test)]
mod occ_type_tests {
    use super::{test_hlc, TxnPriority};
    use std::cmp::Ordering;

    #[test]
    fn txn_priority_preserves_causal_order() {
        // Same node, increasing ts models a causal chain from one coordinator;
        // the older (smaller ts) event compares as `Less`.
        let older = TxnPriority::new(test_hlc(1, 1), 9);
        let newer = TxnPriority::new(test_hlc(2, 1), 9);

        assert_eq!(older.compare_age(&newer), Ordering::Less);
        assert_eq!(newer.compare_age(&older), Ordering::Greater);
    }

    #[test]
    fn txn_priority_totally_orders_concurrent_clocks() {
        // Two coordinators issuing at the same ts were "concurrent" under the
        // old partial order; HLC's `(ts, node)` gives them a plain total order,
        // so `compare_age` is exactly `tid.cmp` and is antisymmetric.
        let left = TxnPriority::new(test_hlc(1, 1), 10);
        let right = TxnPriority::new(test_hlc(1, 2), 20);

        assert_ne!(left.compare_age(&right), Ordering::Equal);
        assert_eq!(left.compare_age(&right), left.tid.cmp(&right.tid));
        assert_eq!(right.compare_age(&left), right.tid.cmp(&left.tid));
        assert_eq!(
            left.compare_age(&right),
            right.compare_age(&left).reverse(),
            "compare_age must be antisymmetric"
        );
    }

    #[test]
    fn txn_priority_preserves_causal_order_across_coordinators() {
        // The younger transaction observed the older's clock before issuing (a
        // causal edge), so its ts strictly exceeds the older's regardless of
        // which node minted it.
        let older = TxnPriority::new(test_hlc(1, 1), 20);
        let younger = TxnPriority::new(test_hlc(2, 2), 10);

        assert_eq!(older.compare_age(&younger), Ordering::Less);
        assert_eq!(younger.compare_age(&older), Ordering::Greater);
    }

    #[test]
    fn txn_priority_totally_orders_concurrent_clocks_from_one_coordinator() {
        // Even two concurrent tids minted by the same coordinator get a
        // deterministic total order straight from `(ts, node)` — no
        // serialization or coordinator tie-break required.
        let left = TxnPriority::new(test_hlc(1, 1), 10);
        let right = TxnPriority::new(test_hlc(1, 2), 10);
        let expected = left.tid.cmp(&right.tid);

        assert_ne!(expected, Ordering::Equal);
        assert_eq!(left.compare_age(&right), expected);
        assert_eq!(right.compare_age(&left), expected.reverse());
    }

    #[test]
    fn compare_age_is_a_total_transitive_order() {
        // Regression: the old causal + `deterministic_cmp` order produced a
        // CYCLE on this sparse-lexicographic triple. With vector clocks
        // A=[(2,5)], B=[(1,1),(2,5)], C=[(1,2)] it yielded A<B, B<C, C<A, so
        // sorting was ill-defined. HLC's `(ts, node)` is a genuine total order;
        // encode the same shape and prove transitivity plus a single
        // consistent sort.
        let a = TxnPriority::new(test_hlc(5, 2), 2);
        let b = TxnPriority::new(test_hlc(6, 1), 1);
        let c = TxnPriority::new(test_hlc(7, 1), 1);

        assert_eq!(a.compare_age(&b), Ordering::Less);
        assert_eq!(b.compare_age(&c), Ordering::Less);
        // a < b and b < c must imply a < c (the old order broke exactly here).
        assert_eq!(a.compare_age(&c), Ordering::Less);

        let mut sorted = vec![c.clone(), a.clone(), b.clone()];
        sorted.sort_by(|x, y| x.compare_age(y));
        assert_eq!(sorted, vec![a, b, c]);
    }
}
