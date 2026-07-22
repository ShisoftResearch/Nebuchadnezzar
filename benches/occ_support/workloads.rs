use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use dovahkiin::types::Map as _;
use neb::{
    ram::types::{Id, OwnedValue},
    server::transactions::{
        AbortResult, DMPrepareResult, EndResult, TMError, TMPrepareResult, TxnExecResult, TxnId,
    },
};

use super::fixture::OccFixture;
use super::metrics::BatchMetrics;

const MAX_ATTEMPTS_PER_LOGICAL_OPERATION: u64 = 10_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttemptOutcome {
    Committed,
    Retryable,
    Unexpected(String),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AttemptTally {
    pub attempts: u64,
    pub committed: u64,
    pub not_realizable: u64,
    pub unexpected: Vec<String>,
}

impl AttemptTally {
    pub fn from_outcomes<I>(outcomes: I) -> Self
    where
        I: IntoIterator<Item = AttemptOutcome>,
    {
        let mut tally = Self::default();
        for outcome in outcomes {
            tally.attempts += 1;
            match outcome {
                AttemptOutcome::Committed => {
                    tally.committed += 1;
                }
                AttemptOutcome::Retryable => {
                    tally.not_realizable += 1;
                }
                AttemptOutcome::Unexpected(message) => {
                    tally.unexpected.push(message);
                }
            }
        }
        tally
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BatchSpec {
    pub successes: u64,
    pub concurrency: usize,
    pub cells_per_txn: usize,
}

pub struct TimedBatch {
    pub metrics: BatchMetrics,
    pub elapsed: Duration,
}

async fn abort_started(fixture: &OccFixture, tid: TxnId) -> Result<(), String> {
    match fixture.txn.abort(tid.clone()).await {
        Ok(Ok(AbortResult::Success(None))) => Ok(()),
        Ok(Ok(AbortResult::Success(Some(failures)))) => Err(format!(
            "abort({tid:?}) retained rollback failures: {:?}",
            failures
        )),
        Ok(Ok(other)) => Err(format!("abort({tid:?}) returned {:?}", other)),
        Ok(Err(err)) => Err(format!("abort({tid:?}) manager error: {:?}", err)),
        Err(err) => Err(format!("abort({tid:?}) RPC error: {:?}", err)),
    }
}

async fn confirm_prepare_cleanup(fixture: &OccFixture, tid: TxnId) -> Result<(), String> {
    match fixture.txn.abort(tid.clone()).await {
        Ok(Ok(AbortResult::Success(None))) => Ok(()),
        Ok(Ok(AbortResult::Success(Some(failures)))) => Err(format!(
            "abort({tid:?}) retained rollback failures: {:?}",
            failures
        )),
        Ok(Ok(other)) => Err(format!("abort({tid:?}) returned {:?}", other)),
        Ok(Err(TMError::TransactionNotFound)) => Ok(()),
        Ok(Err(TMError::TransactionIdExisted)) => Err(format!(
            "abort({tid:?}) manager error: {:?}",
            TMError::TransactionIdExisted
        )),
        Ok(Err(TMError::CannotLocateCellServer)) => Err(format!(
            "abort({tid:?}) manager error: {:?}",
            TMError::CannotLocateCellServer
        )),
        Ok(Err(TMError::RPCErrorFromCellServer)) => Err(format!(
            "abort({tid:?}) manager error: {:?}",
            TMError::RPCErrorFromCellServer
        )),
        Ok(Err(TMError::AssertionError)) => Err(format!(
            "abort({tid:?}) manager error: {:?}",
            TMError::AssertionError
        )),
        Ok(Err(TMError::InvalidTransactionState(state))) => Err(format!(
            "abort({tid:?}) manager error: {:?}",
            TMError::InvalidTransactionState(state)
        )),
        Ok(Err(TMError::Other)) => Err(format!(
            "abort({tid:?}) manager error: {:?}",
            TMError::Other
        )),
        Err(err) => Err(format!("abort({tid:?}) RPC error: {:?}", err)),
    }
}

async fn retry_after_abort(
    fixture: &OccFixture,
    tid: TxnId,
    context: impl Into<String>,
) -> AttemptOutcome {
    let context = context.into();
    match abort_started(fixture, tid).await {
        Ok(()) => AttemptOutcome::Retryable,
        Err(abort_error) => {
            AttemptOutcome::Unexpected(format!("{context}; abort cleanup failed: {abort_error}"))
        }
    }
}

async fn unexpected_after_abort(
    fixture: &OccFixture,
    tid: TxnId,
    context: impl Into<String>,
) -> AttemptOutcome {
    let context = context.into();
    match abort_started(fixture, tid).await {
        Ok(()) => AttemptOutcome::Unexpected(context),
        Err(abort_error) => {
            AttemptOutcome::Unexpected(format!("{context}; abort cleanup failed: {abort_error}"))
        }
    }
}

async fn retry_after_prepare_cleanup(
    fixture: &OccFixture,
    tid: TxnId,
    context: impl Into<String>,
) -> AttemptOutcome {
    let context = context.into();
    match confirm_prepare_cleanup(fixture, tid).await {
        Ok(()) => AttemptOutcome::Retryable,
        Err(cleanup_error) => AttemptOutcome::Unexpected(format!(
            "{context}; cleanup confirmation failed: {cleanup_error}"
        )),
    }
}

async fn unexpected_after_prepare_cleanup(
    fixture: &OccFixture,
    tid: TxnId,
    context: impl Into<String>,
) -> AttemptOutcome {
    let context = context.into();
    match confirm_prepare_cleanup(fixture, tid).await {
        Ok(()) => AttemptOutcome::Unexpected(context),
        Err(cleanup_error) => AttemptOutcome::Unexpected(format!(
            "{context}; cleanup confirmation failed: {cleanup_error}"
        )),
    }
}

fn replace_score_value(map: &mut dovahkiin::types::OwnedMap, next_score: u64) {
    *map.get_mut("score") = OwnedValue::U64(next_score);
}

async fn read_modify_write_once(fixture: &OccFixture, ids: &[Id]) -> AttemptOutcome {
    let tid = match fixture.txn.begin().await {
        Ok(Ok(tid)) => tid,
        Ok(Err(err)) => {
            return AttemptOutcome::Unexpected(format!("begin manager error: {:?}", err));
        }
        Err(err) => {
            return AttemptOutcome::Unexpected(format!("begin RPC error: {:?}", err));
        }
    };

    for id in ids {
        let mut cell = match fixture.txn.read(tid.clone(), *id).await {
            Ok(Ok(TxnExecResult::Accepted(cell))) => cell,
            Ok(Ok(TxnExecResult::Rejected)) => {
                return retry_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional read rejected for {:?}", id),
                )
                .await;
            }
            Ok(Ok(TxnExecResult::Wait)) => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional read waited unexpectedly for {:?}", id),
                )
                .await;
            }
            Ok(Ok(TxnExecResult::Error(err))) => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional read error for {:?}: {:?}", id, err),
                )
                .await;
            }
            Ok(Ok(TxnExecResult::StateError(state))) => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional read state error for {:?}: {:?}", id, state),
                )
                .await;
            }
            Ok(Err(err)) => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional read manager error for {:?}: {:?}", id, err),
                )
                .await;
            }
            Err(err) => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional read RPC error for {:?}: {:?}", id, err),
                )
                .await;
            }
        };

        let score = match cell.data["score"].u64() {
            Some(score) => *score,
            None => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional read returned non-u64 score for {:?}", id),
                )
                .await;
            }
        };
        let next_score = match score.checked_add(1) {
            Some(next_score) => next_score,
            None => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("score overflow while incrementing {:?}", id),
                )
                .await;
            }
        };
        let mut map = match &cell.data {
            OwnedValue::Map(map) => map.owned(),
            other => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!(
                        "transactional read returned non-map cell data for {:?}: {:?}",
                        id, other
                    ),
                )
                .await;
            }
        };
        replace_score_value(&mut map, next_score);
        cell.data = OwnedValue::Map(map);

        match fixture.txn.update(tid.clone(), cell).await {
            Ok(Ok(TxnExecResult::Accepted(()))) => {}
            Ok(Ok(TxnExecResult::Rejected)) => {
                return retry_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional update rejected for {:?}", id),
                )
                .await;
            }
            Ok(Ok(TxnExecResult::Wait)) => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional update waited unexpectedly for {:?}", id),
                )
                .await;
            }
            Ok(Ok(TxnExecResult::Error(err))) => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional update error for {:?}: {:?}", id, err),
                )
                .await;
            }
            Ok(Ok(TxnExecResult::StateError(state))) => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional update state error for {:?}: {:?}", id, state),
                )
                .await;
            }
            Ok(Err(err)) => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional update manager error for {:?}: {:?}", id, err),
                )
                .await;
            }
            Err(err) => {
                return unexpected_after_abort(
                    fixture,
                    tid.clone(),
                    format!("transactional update RPC error for {:?}: {:?}", id, err),
                )
                .await;
            }
        }
    }

    match fixture.txn.prepare(tid.clone()).await {
        Ok(Ok(TMPrepareResult::Success)) => {}
        Ok(Ok(TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable))) => {
            return retry_after_prepare_cleanup(
                fixture,
                tid.clone(),
                format!(
                    "prepare({tid:?}) returned {:?}",
                    TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
                ),
            )
            .await;
        }
        Ok(Ok(other)) => {
            return unexpected_after_prepare_cleanup(
                fixture,
                tid.clone(),
                format!("prepare({tid:?}) returned {:?}", other),
            )
            .await;
        }
        Ok(Err(err)) => {
            return unexpected_after_prepare_cleanup(
                fixture,
                tid.clone(),
                format!("prepare({tid:?}) manager error: {:?}", err),
            )
            .await;
        }
        Err(err) => {
            return unexpected_after_prepare_cleanup(
                fixture,
                tid.clone(),
                format!("prepare({tid:?}) RPC error: {:?}", err),
            )
            .await;
        }
    }

    match fixture.txn.commit(tid.clone()).await {
        Ok(Ok(EndResult::Success)) => AttemptOutcome::Committed,
        Ok(Ok(other)) => {
            AttemptOutcome::Unexpected(format!("commit({tid:?}) returned {:?}", other))
        }
        Ok(Err(err)) => {
            AttemptOutcome::Unexpected(format!("commit({tid:?}) manager error: {:?}", err))
        }
        Err(err) => AttemptOutcome::Unexpected(format!("commit({tid:?}) RPC error: {:?}", err)),
    }
}

pub async fn run_fixed_success_rmw(
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
    spec: BatchSpec,
) -> TimedBatch {
    let expected_delta = validate_batch_spec(&ids, spec);

    let score_before = fixture.sum_scores(ids.as_ref()).await;
    let next = Arc::new(AtomicU64::new(0));
    let committed = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    let mut workers = Vec::with_capacity(spec.concurrency);
    for _ in 0..spec.concurrency {
        let fixture = fixture.clone();
        let ids = ids.clone();
        let next = next.clone();
        let committed = committed.clone();
        workers.push(tokio::spawn(async move {
            let mut metrics = BatchMetrics::default();
            loop {
                let logical_index = next.fetch_add(1, Ordering::Relaxed);
                if logical_index >= spec.successes {
                    break;
                }

                let logical_ids =
                    ids_for_logical_operation(ids.as_ref(), logical_index, spec.cells_per_txn);
                let logical_started = Instant::now();
                let mut attempts = 0u64;
                let mut retries = 0u64;

                loop {
                    attempts += 1;
                    if attempts > MAX_ATTEMPTS_PER_LOGICAL_OPERATION {
                        metrics.attempts += attempts - 1;
                        metrics.logical_retries += retries;
                        metrics.record_unexpected(format!(
                            "logical operation {} exceeded max attempts {}",
                            logical_index, MAX_ATTEMPTS_PER_LOGICAL_OPERATION
                        ));
                        return metrics;
                    }

                    match read_modify_write_once(&fixture, &logical_ids).await {
                        AttemptOutcome::Committed => {
                            committed.fetch_add(1, Ordering::Relaxed);
                            metrics.record_success(logical_started.elapsed(), attempts, retries);
                            break;
                        }
                        AttemptOutcome::Retryable => {
                            retries += 1;
                            metrics.record_retryable();
                        }
                        AttemptOutcome::Unexpected(message) => {
                            metrics.attempts += attempts;
                            metrics.logical_retries += retries;
                            metrics.record_unexpected(message);
                            return metrics;
                        }
                    }
                }
            }

            metrics
        }));
    }

    let mut metrics = BatchMetrics::default();
    for worker in workers {
        match worker.await {
            Ok(worker_metrics) => metrics.merge(worker_metrics),
            Err(err) => metrics.record_unexpected(format!("worker join failed: {:?}", err)),
        }
    }

    let elapsed = started.elapsed();
    let actual_commits = committed.load(Ordering::Relaxed);
    if actual_commits != spec.successes {
        metrics.record_unexpected(format!(
            "fixed-success RMW committed {} logical operations, expected {}",
            actual_commits, spec.successes
        ));
    }

    let score_after = fixture.sum_scores(ids.as_ref()).await;
    match score_after.checked_sub(score_before) {
        Some(actual_delta) if actual_delta == expected_delta => {}
        Some(actual_delta) => {
            metrics.record_unexpected(format!(
                "fixed-success RMW score invariant failed: actual delta {}, expected {}",
                actual_delta, expected_delta
            ));
        }
        None => {
            metrics.record_unexpected(format!(
                "fixed-success RMW score invariant underflow: before {}, after {}, expected {}",
                score_before, score_after, expected_delta
            ));
        }
    }

    TimedBatch { metrics, elapsed }
}

fn validate_batch_spec(ids: &[Id], spec: BatchSpec) -> u64 {
    assert!(
        !ids.is_empty(),
        "fixed-success RMW requires at least one id"
    );
    assert!(
        spec.concurrency > 0,
        "fixed-success RMW requires concurrency > 0"
    );
    assert!(
        spec.cells_per_txn > 0,
        "fixed-success RMW requires cells_per_txn > 0"
    );
    assert!(
        spec.cells_per_txn <= ids.len(),
        "fixed-success RMW requires at least {} ids to select {} distinct cells per transaction",
        spec.cells_per_txn,
        spec.cells_per_txn
    );

    let mut seen = HashSet::with_capacity(ids.len());
    for id in ids {
        assert!(
            seen.insert(*id),
            "fixed-success RMW requires globally unique ids"
        );
    }

    spec.successes
        .checked_mul(u64::try_from(spec.cells_per_txn).expect("cells_per_txn must fit in u64"))
        .unwrap_or_else(|| {
            panic!(
                "fixed-success RMW expected delta overflow for successes={} cells_per_txn={}",
                spec.successes, spec.cells_per_txn
            )
        })
}

fn ids_for_logical_operation(ids: &[Id], logical_index: u64, cells_per_txn: usize) -> Vec<Id> {
    let start = usize::try_from(logical_index % ids.len() as u64)
        .expect("logical operation index modulo ids length must fit in usize");
    (0..cells_per_txn)
        .map(|offset| ids[(start + offset) % ids.len()])
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dovahkiin::types::OwnedMap;

    #[test]
    #[should_panic(expected = "globally unique ids")]
    fn duplicate_input_ids_are_rejected_before_timing() {
        let id = Id::new(1, 1);
        let spec = BatchSpec {
            successes: 1,
            concurrency: 1,
            cells_per_txn: 1,
        };

        let _ = validate_batch_spec(&[id, id], spec);
    }

    #[test]
    #[should_panic(expected = "expected delta overflow")]
    fn expected_delta_overflow_is_rejected_before_timing() {
        let spec = BatchSpec {
            successes: u64::MAX,
            concurrency: 1,
            cells_per_txn: 2,
        };

        let _ = validate_batch_spec(&[Id::new(1, 1), Id::new(2, 2)], spec);
    }

    #[test]
    fn score_replacement_does_not_grow_owned_map_fields() {
        let mut map = OwnedMap::new();
        map.insert("id", OwnedValue::I64(7));
        map.insert("name", OwnedValue::String("counter".to_string()));
        map.insert("score", OwnedValue::U64(1));
        let initial_fields = map.fields.len();

        replace_score_value(&mut map, 2);
        replace_score_value(&mut map, 3);

        assert_eq!(map.fields.len(), initial_fields);
        assert_eq!(map.get("score").u64().copied(), Some(3));
    }
}
