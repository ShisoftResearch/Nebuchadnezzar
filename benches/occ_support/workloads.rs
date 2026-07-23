use std::{
    collections::HashSet,
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use dovahkiin::types::{Map as _, Value as _};
use neb::{
    ram::{
        cell::{OwnedCell, ReadError},
        types::{Id, OwnedValue},
    },
    server::transactions::{
        AbortResult, DMPrepareResult, EndResult, TMError, TMPrepareResult, TxnExecResult, TxnId,
    },
};

use super::fixture::{counter_cell, OccFixture};
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

#[derive(Clone, Copy, Debug)]
pub enum ProjectionMode {
    Head,
    Selected,
    Mixed,
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

async fn begin_transaction(fixture: &OccFixture) -> Result<TxnId, AttemptOutcome> {
    match fixture.txn.begin().await {
        Ok(Ok(tid)) => Ok(tid),
        Ok(Err(err)) => Err(AttemptOutcome::Unexpected(format!(
            "begin manager error: {:?}",
            err
        ))),
        Err(err) => Err(AttemptOutcome::Unexpected(format!(
            "begin RPC error: {:?}",
            err
        ))),
    }
}

async fn finish_once(fixture: &OccFixture, tid: TxnId) -> AttemptOutcome {
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

async fn read_modify_write_once(fixture: &OccFixture, ids: &[Id]) -> AttemptOutcome {
    let tid = match begin_transaction(fixture).await {
        Ok(tid) => tid,
        Err(outcome) => return outcome,
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

    finish_once(fixture, tid).await
}

fn validate_unique_ids(ids: &[Id], workload_name: &str) {
    assert!(!ids.is_empty(), "{workload_name} requires at least one id");

    let mut seen = HashSet::with_capacity(ids.len());
    for id in ids {
        assert!(
            seen.insert(*id),
            "{workload_name} requires globally unique ids"
        );
    }
}

fn cyclic_id(ids: &[Id], logical_index: u64) -> Id {
    let index = usize::try_from(logical_index % ids.len() as u64)
        .expect("logical operation index modulo ids length must fit in usize");
    ids[index]
}

fn finalize_sequential_fixed_success(
    mut metrics: BatchMetrics,
    elapsed: Duration,
    operations: u64,
    workload_name: &str,
) -> TimedBatch {
    if metrics.committed != operations {
        metrics.record_unexpected(format!(
            "{workload_name} committed {} logical operations, expected {}",
            metrics.committed, operations
        ));
    }

    TimedBatch { metrics, elapsed }
}

async fn run_sequential_fixed_success<
    Setup,
    SetupFn,
    SetupFut,
    AttemptFn,
    AttemptFut,
    PostFn,
    PostFut,
>(
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
    operations: u64,
    workload_name: &str,
    mut setup_fn: SetupFn,
    mut attempt_fn: AttemptFn,
    mut post_success_fn: PostFn,
) -> TimedBatch
where
    SetupFn: FnMut(Arc<OccFixture>, u64, Id) -> SetupFut,
    SetupFut: Future<Output = Result<Setup, String>>,
    AttemptFn: FnMut(Arc<OccFixture>, u64, Id, Setup) -> AttemptFut,
    AttemptFut: Future<Output = (Setup, AttemptOutcome)>,
    PostFn: FnMut(Arc<OccFixture>, u64, Id, Setup) -> PostFut,
    PostFut: Future<Output = Result<(), String>>,
{
    let mut metrics = BatchMetrics::default();
    let mut elapsed = Duration::default();

    for logical_index in 0..operations {
        let id = cyclic_id(ids.as_ref(), logical_index);
        let mut setup = match setup_fn(fixture.clone(), logical_index, id).await {
            Ok(setup) => setup,
            Err(error) => {
                metrics.record_unexpected(error);
                return finalize_sequential_fixed_success(
                    metrics,
                    elapsed,
                    operations,
                    workload_name,
                );
            }
        };

        let logical_started = Instant::now();
        let mut attempts = 0u64;
        let mut retries = 0u64;

        loop {
            attempts += 1;
            if attempts > MAX_ATTEMPTS_PER_LOGICAL_OPERATION {
                elapsed += logical_started.elapsed();
                metrics.attempts += attempts - 1;
                metrics.logical_retries += retries;
                metrics.record_unexpected(format!(
                    "{workload_name} logical operation {} exceeded max attempts {}",
                    logical_index, MAX_ATTEMPTS_PER_LOGICAL_OPERATION
                ));
                return finalize_sequential_fixed_success(
                    metrics,
                    elapsed,
                    operations,
                    workload_name,
                );
            }

            let (next_setup, outcome) = attempt_fn(fixture.clone(), logical_index, id, setup).await;
            setup = next_setup;

            match outcome {
                AttemptOutcome::Committed => {
                    let logical_elapsed = logical_started.elapsed();
                    elapsed += logical_elapsed;
                    metrics.record_success(logical_elapsed, attempts, retries);
                    if let Err(error) =
                        post_success_fn(fixture.clone(), logical_index, id, setup).await
                    {
                        metrics.record_unexpected(error);
                        return finalize_sequential_fixed_success(
                            metrics,
                            elapsed,
                            operations,
                            workload_name,
                        );
                    }
                    break;
                }
                AttemptOutcome::Retryable => {
                    retries += 1;
                    metrics.record_retryable();
                }
                AttemptOutcome::Unexpected(message) => {
                    elapsed += logical_started.elapsed();
                    metrics.attempts += attempts;
                    metrics.logical_retries += retries;
                    metrics.record_unexpected(message);
                    return finalize_sequential_fixed_success(
                        metrics,
                        elapsed,
                        operations,
                        workload_name,
                    );
                }
            }
        }
    }

    finalize_sequential_fixed_success(metrics, elapsed, operations, workload_name)
}

async fn blind_update_once(fixture: &OccFixture, template: OwnedCell) -> AttemptOutcome {
    let tid = match begin_transaction(fixture).await {
        Ok(tid) => tid,
        Err(outcome) => return outcome,
    };

    let id = template.id();
    let mut cell = template;
    let score = match cell.data["score"].u64() {
        Some(score) => *score,
        None => {
            return unexpected_after_abort(
                fixture,
                tid.clone(),
                format!("public read template returned non-u64 score for {:?}", id),
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
                    "public read template returned non-map cell data for {:?}: {:?}",
                    id, other
                ),
            )
            .await;
        }
    };
    replace_score_value(&mut map, next_score);
    cell.data = OwnedValue::Map(map);

    match fixture.txn.update(tid.clone(), cell).await {
        Ok(Ok(TxnExecResult::Accepted(()))) => finish_once(fixture, tid).await,
        Ok(Ok(TxnExecResult::Rejected)) => {
            retry_after_abort(
                fixture,
                tid.clone(),
                format!("transactional blind update rejected for {:?}", id),
            )
            .await
        }
        Ok(Ok(TxnExecResult::Wait)) => {
            unexpected_after_abort(
                fixture,
                tid.clone(),
                format!(
                    "transactional blind update waited unexpectedly for {:?}",
                    id
                ),
            )
            .await
        }
        Ok(Ok(TxnExecResult::Error(err))) => {
            unexpected_after_abort(
                fixture,
                tid.clone(),
                format!("transactional blind update error for {:?}: {:?}", id, err),
            )
            .await
        }
        Ok(Ok(TxnExecResult::StateError(state))) => {
            unexpected_after_abort(
                fixture,
                tid.clone(),
                format!(
                    "transactional blind update state error for {:?}: {:?}",
                    id, state
                ),
            )
            .await
        }
        Ok(Err(err)) => {
            unexpected_after_abort(
                fixture,
                tid.clone(),
                format!(
                    "transactional blind update manager error for {:?}: {:?}",
                    id, err
                ),
            )
            .await
        }
        Err(err) => {
            unexpected_after_abort(
                fixture,
                tid.clone(),
                format!(
                    "transactional blind update RPC error for {:?}: {:?}",
                    id, err
                ),
            )
            .await
        }
    }
}

async fn blind_remove_once(fixture: &OccFixture, id: Id) -> AttemptOutcome {
    let tid = match begin_transaction(fixture).await {
        Ok(tid) => tid,
        Err(outcome) => return outcome,
    };

    match fixture.txn.remove(tid.clone(), id).await {
        Ok(Ok(TxnExecResult::Accepted(()))) => finish_once(fixture, tid).await,
        Ok(Ok(TxnExecResult::Rejected)) => {
            retry_after_abort(
                fixture,
                tid.clone(),
                format!("transactional blind remove rejected for {:?}", id),
            )
            .await
        }
        Ok(Ok(TxnExecResult::Wait)) => {
            unexpected_after_abort(
                fixture,
                tid.clone(),
                format!(
                    "transactional blind remove waited unexpectedly for {:?}",
                    id
                ),
            )
            .await
        }
        Ok(Ok(TxnExecResult::Error(err))) => {
            unexpected_after_abort(
                fixture,
                tid.clone(),
                format!("transactional blind remove error for {:?}: {:?}", id, err),
            )
            .await
        }
        Ok(Ok(TxnExecResult::StateError(state))) => {
            unexpected_after_abort(
                fixture,
                tid.clone(),
                format!(
                    "transactional blind remove state error for {:?}: {:?}",
                    id, state
                ),
            )
            .await
        }
        Ok(Err(err)) => {
            unexpected_after_abort(
                fixture,
                tid.clone(),
                format!(
                    "transactional blind remove manager error for {:?}: {:?}",
                    id, err
                ),
            )
            .await
        }
        Err(err) => {
            unexpected_after_abort(
                fixture,
                tid.clone(),
                format!(
                    "transactional blind remove RPC error for {:?}: {:?}",
                    id, err
                ),
            )
            .await
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProjectionObservation {
    version: u64,
    score: Option<u64>,
}

fn projected_observations_match(observations: &[ProjectionObservation]) -> bool {
    let Some(first) = observations.first() else {
        return false;
    };

    let mut expected_score = None;
    for observation in observations {
        if observation.version != first.version {
            return false;
        }
        if let Some(score) = observation.score {
            match expected_score {
                Some(expected_score) if expected_score != score => return false,
                Some(_) => {}
                None => expected_score = Some(score),
            }
        }
    }

    true
}

fn selected_score(cell: &OwnedCell, id: Id) -> Result<u64, AttemptOutcome> {
    let values = cell.data.uni_array();
    values
        .as_ref()
        .and_then(|values| values.first())
        .and_then(|value| value.u64())
        .copied()
        .ok_or_else(|| {
            AttemptOutcome::Unexpected(format!(
                "transactional selected read returned no u64 score for {:?}: {:?}",
                id, cell.data
            ))
        })
}

fn full_score(cell: &OwnedCell, id: Id) -> Result<u64, AttemptOutcome> {
    match &cell.data {
        OwnedValue::Map(map) => map.get("score").u64().copied().ok_or_else(|| {
            AttemptOutcome::Unexpected(format!(
                "transactional full read returned no u64 score for {:?}: {:?}",
                id, cell.data
            ))
        }),
        other => Err(AttemptOutcome::Unexpected(format!(
            "transactional full read returned non-map data for {:?}: {:?}",
            id, other
        ))),
    }
}

async fn transactional_head_version(
    fixture: &OccFixture,
    tid: TxnId,
    id: Id,
) -> Result<ProjectionObservation, AttemptOutcome> {
    match fixture.txn.head(tid.clone(), id).await {
        Ok(Ok(TxnExecResult::Accepted(header))) => Ok(ProjectionObservation {
            version: header.version,
            score: None,
        }),
        Ok(Ok(TxnExecResult::Rejected)) => Err(AttemptOutcome::Retryable),
        Ok(Ok(TxnExecResult::Wait)) => Err(AttemptOutcome::Unexpected(format!(
            "transactional head waited unexpectedly for {:?}",
            id
        ))),
        Ok(Ok(TxnExecResult::Error(err))) => Err(AttemptOutcome::Unexpected(format!(
            "transactional head error for {:?}: {:?}",
            id, err
        ))),
        Ok(Ok(TxnExecResult::StateError(state))) => Err(AttemptOutcome::Unexpected(format!(
            "transactional head state error for {:?}: {:?}",
            id, state
        ))),
        Ok(Err(err)) => Err(AttemptOutcome::Unexpected(format!(
            "transactional head manager error for {:?}: {:?}",
            id, err
        ))),
        Err(err) => Err(AttemptOutcome::Unexpected(format!(
            "transactional head RPC error for {:?}: {:?}",
            id, err
        ))),
    }
}

async fn transactional_selected_version(
    fixture: &OccFixture,
    tid: TxnId,
    id: Id,
) -> Result<ProjectionObservation, AttemptOutcome> {
    match fixture
        .txn
        .read_selected(tid.clone(), id, vec![bifrost_hasher::hash_str("score")])
        .await
    {
        Ok(Ok(TxnExecResult::Accepted(cell))) => Ok(ProjectionObservation {
            version: cell.header.version,
            score: Some(selected_score(&cell, id)?),
        }),
        Ok(Ok(TxnExecResult::Rejected)) => Err(AttemptOutcome::Retryable),
        Ok(Ok(TxnExecResult::Wait)) => Err(AttemptOutcome::Unexpected(format!(
            "transactional selected read waited unexpectedly for {:?}",
            id
        ))),
        Ok(Ok(TxnExecResult::Error(err))) => Err(AttemptOutcome::Unexpected(format!(
            "transactional selected read error for {:?}: {:?}",
            id, err
        ))),
        Ok(Ok(TxnExecResult::StateError(state))) => Err(AttemptOutcome::Unexpected(format!(
            "transactional selected read state error for {:?}: {:?}",
            id, state
        ))),
        Ok(Err(err)) => Err(AttemptOutcome::Unexpected(format!(
            "transactional selected read manager error for {:?}: {:?}",
            id, err
        ))),
        Err(err) => Err(AttemptOutcome::Unexpected(format!(
            "transactional selected read RPC error for {:?}: {:?}",
            id, err
        ))),
    }
}

async fn transactional_full_version(
    fixture: &OccFixture,
    tid: TxnId,
    id: Id,
) -> Result<ProjectionObservation, AttemptOutcome> {
    match fixture.txn.read(tid.clone(), id).await {
        Ok(Ok(TxnExecResult::Accepted(cell))) => Ok(ProjectionObservation {
            version: cell.header.version,
            score: Some(full_score(&cell, id)?),
        }),
        Ok(Ok(TxnExecResult::Rejected)) => Err(AttemptOutcome::Retryable),
        Ok(Ok(TxnExecResult::Wait)) => Err(AttemptOutcome::Unexpected(format!(
            "transactional full read waited unexpectedly for {:?}",
            id
        ))),
        Ok(Ok(TxnExecResult::Error(err))) => Err(AttemptOutcome::Unexpected(format!(
            "transactional full read error for {:?}: {:?}",
            id, err
        ))),
        Ok(Ok(TxnExecResult::StateError(state))) => Err(AttemptOutcome::Unexpected(format!(
            "transactional full read state error for {:?}: {:?}",
            id, state
        ))),
        Ok(Err(err)) => Err(AttemptOutcome::Unexpected(format!(
            "transactional full read manager error for {:?}: {:?}",
            id, err
        ))),
        Err(err) => Err(AttemptOutcome::Unexpected(format!(
            "transactional full read RPC error for {:?}: {:?}",
            id, err
        ))),
    }
}

async fn projected_observations(
    fixture: &OccFixture,
    tid: TxnId,
    id: Id,
    mode: ProjectionMode,
) -> Result<Vec<ProjectionObservation>, AttemptOutcome> {
    match mode {
        ProjectionMode::Head => Ok(vec![
            transactional_head_version(fixture, tid.clone(), id).await?,
            transactional_head_version(fixture, tid.clone(), id).await?,
        ]),
        ProjectionMode::Selected => Ok(vec![
            transactional_selected_version(fixture, tid.clone(), id).await?,
            transactional_selected_version(fixture, tid.clone(), id).await?,
        ]),
        ProjectionMode::Mixed => Ok(vec![
            transactional_head_version(fixture, tid.clone(), id).await?,
            transactional_selected_version(fixture, tid.clone(), id).await?,
            transactional_full_version(fixture, tid.clone(), id).await?,
        ]),
    }
}

async fn projected_read_once(fixture: &OccFixture, id: Id, mode: ProjectionMode) -> AttemptOutcome {
    let tid = match begin_transaction(fixture).await {
        Ok(tid) => tid,
        Err(outcome) => return outcome,
    };

    let observations = match projected_observations(fixture, tid.clone(), id, mode).await {
        Ok(observations) => observations,
        Err(AttemptOutcome::Retryable) => {
            return retry_after_abort(
                fixture,
                tid.clone(),
                format!("projected read rejected for {:?} in {:?}", id, mode),
            )
            .await;
        }
        Err(AttemptOutcome::Unexpected(message)) => {
            return unexpected_after_abort(fixture, tid.clone(), message).await;
        }
        Err(AttemptOutcome::Committed) => {
            return unexpected_after_abort(
                fixture,
                tid.clone(),
                format!(
                    "projected read helper returned committed unexpectedly for {:?}",
                    id
                ),
            )
            .await;
        }
    };

    if !projected_observations_match(&observations) {
        return unexpected_after_abort(
            fixture,
            tid.clone(),
            format!(
                "projected read observed mismatched values for {:?} in {:?}: {:?}",
                id, mode, observations
            ),
        )
        .await;
    }

    finish_once(fixture, tid).await
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

pub async fn run_blind_update_batch(
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
    operations: u64,
) -> TimedBatch {
    validate_unique_ids(ids.as_ref(), "blind update");
    let expected_delta = operations;
    let score_before = fixture.sum_scores(ids.as_ref()).await;

    let mut metrics = BatchMetrics::default();
    let mut elapsed = Duration::default();

    'operations: for logical_index in 0..operations {
        let id = cyclic_id(ids.as_ref(), logical_index);
        let mut attempts = 0u64;
        let mut retries = 0u64;
        let mut logical_elapsed = Duration::default();

        loop {
            if attempts == MAX_ATTEMPTS_PER_LOGICAL_OPERATION {
                elapsed += logical_elapsed;
                metrics.attempts += attempts;
                metrics.logical_retries += retries;
                metrics.record_unexpected(format!(
                    "blind update logical operation {} exceeded max attempts {}",
                    logical_index, MAX_ATTEMPTS_PER_LOGICAL_OPERATION
                ));
                break 'operations;
            }

            let template = match fixture.client.read_cell(id).await {
                Ok(Ok(cell)) => cell,
                Ok(Err(err)) => {
                    elapsed += logical_elapsed;
                    metrics.attempts += attempts;
                    metrics.logical_retries += retries;
                    metrics.record_unexpected(format!(
                        "blind update setup read failed for logical operation {} on {:?}: {:?}",
                        logical_index, id, err
                    ));
                    break 'operations;
                }
                Err(err) => {
                    elapsed += logical_elapsed;
                    metrics.attempts += attempts;
                    metrics.logical_retries += retries;
                    metrics.record_unexpected(format!(
                        "blind update setup read RPC error for logical operation {} on {:?}: {:?}",
                        logical_index, id, err
                    ));
                    break 'operations;
                }
            };

            let attempt_started = Instant::now();
            let outcome = blind_update_once(&fixture, template).await;
            logical_elapsed += attempt_started.elapsed();
            attempts += 1;

            match outcome {
                AttemptOutcome::Committed => {
                    elapsed += logical_elapsed;
                    metrics.record_success(logical_elapsed, attempts, retries);
                    break;
                }
                AttemptOutcome::Retryable => {
                    retries += 1;
                    metrics.record_retryable();
                }
                AttemptOutcome::Unexpected(message) => {
                    elapsed += logical_elapsed;
                    metrics.attempts += attempts;
                    metrics.logical_retries += retries;
                    metrics.record_unexpected(message);
                    break 'operations;
                }
            }
        }
    }

    let mut batch = finalize_sequential_fixed_success(metrics, elapsed, operations, "blind update");

    let score_after = fixture.sum_scores(ids.as_ref()).await;
    match score_after.checked_sub(score_before) {
        Some(actual_delta) if actual_delta == expected_delta => {}
        Some(actual_delta) => {
            batch.metrics.record_unexpected(format!(
                "blind update score invariant failed: actual delta {}, expected {}",
                actual_delta, expected_delta
            ));
        }
        None => {
            batch.metrics.record_unexpected(format!(
                "blind update score invariant underflow: before {}, after {}, expected {}",
                score_before, score_after, expected_delta
            ));
        }
    }

    batch
}

pub async fn run_blind_remove_batch(
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
    operations: u64,
) -> TimedBatch {
    validate_unique_ids(ids.as_ref(), "blind remove");

    run_sequential_fixed_success(
        fixture.clone(),
        ids.clone(),
        operations,
        "blind remove",
        |fixture, logical_index, id| {
            Box::pin(async move {
                match fixture
                    .client
                    .write_cell(counter_cell(fixture.schema.id, id, logical_index, 0))
                    .await
                {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(err)) => Err(format!(
                        "blind remove setup write failed for logical operation {} on {:?}: {:?}",
                        logical_index, id, err
                    )),
                    Err(err) => Err(format!(
                        "blind remove setup write RPC error for logical operation {} on {:?}: {:?}",
                        logical_index, id, err
                    )),
                }
            })
        },
        |fixture, _logical_index, id, setup| {
            Box::pin(async move { (setup, blind_remove_once(&fixture, id).await) })
        },
        |fixture, logical_index, id, _setup| {
            Box::pin(async move {
                match fixture.client.read_cell(id).await {
                    Ok(Err(ReadError::CellDoesNotExisted)) => Ok(()),
                    Ok(Ok(cell)) => Err(format!(
                        "blind remove post-check found surviving cell after logical operation {} on {:?}: {:?}",
                        logical_index, id, cell
                    )),
                    Ok(Err(err)) => Err(format!(
                        "blind remove post-check failed for logical operation {} on {:?}: {:?}",
                        logical_index, id, err
                    )),
                    Err(err) => Err(format!(
                        "blind remove post-check RPC error for logical operation {} on {:?}: {:?}",
                        logical_index, id, err
                    )),
                }
            })
        },
    )
    .await
}

pub async fn run_projected_read_batch(
    fixture: Arc<OccFixture>,
    ids: Arc<Vec<Id>>,
    operations: u64,
    mode: ProjectionMode,
) -> TimedBatch {
    validate_unique_ids(ids.as_ref(), "projected read");

    run_sequential_fixed_success(
        fixture.clone(),
        ids.clone(),
        operations,
        "projected read",
        |_fixture, _logical_index, _id| Box::pin(async move { Ok(()) }),
        move |fixture, _logical_index, id, setup| {
            Box::pin(async move { (setup, projected_read_once(&fixture, id, mode).await) })
        },
        |_fixture, _logical_index, _id, _setup| Box::pin(async move { Ok(()) }),
    )
    .await
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

    #[test]
    fn projected_observations_reject_score_mismatches() {
        assert!(!projected_observations_match(&[
            ProjectionObservation {
                version: 7,
                score: None,
            },
            ProjectionObservation {
                version: 7,
                score: Some(3),
            },
            ProjectionObservation {
                version: 7,
                score: Some(4),
            },
        ]));
    }

    #[test]
    fn secondary_workload_signatures_are_stable() {
        let _ = run_blind_update_batch;
        let _ = run_blind_remove_batch;
        let _ = run_projected_read_batch;
        let _ = ProjectionMode::Head;
        let _ = ProjectionMode::Selected;
        let _ = ProjectionMode::Mixed;
    }
}
