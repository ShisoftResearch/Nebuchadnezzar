//! Per-locality admission within an exact process-wide scannable quota.

use crate::utils::budget::{Budget, Permit};
use lightning::map::PtrHashMap;
use std::sync::Arc;

fn process_budget() -> Arc<Budget> {
    static BUDGET: arc_swap::ArcSwapOption<Budget> = arc_swap::ArcSwapOption::const_empty();
    if let Some(budget) = BUDGET.load_full() {
        return budget;
    }
    let candidate = Budget::new(scannable_limit());
    let previous = BUDGET.compare_and_swap(&None::<Arc<Budget>>, Some(Arc::clone(&candidate)));
    previous.as_ref().cloned().unwrap_or(candidate)
}

fn scannable_limit() -> usize {
    std::env::var("NEB_SCANNABLE_INFLIGHT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|limit| *limit > 0)
        .unwrap_or(64)
}

pub(super) struct ScannablePermit {
    _local: Permit,
    _process: Permit,
}

pub(super) struct ScannableAdmission {
    localities: PtrHashMap<u16, Arc<Budget>>,
    limit: usize,
}

impl Default for ScannableAdmission {
    fn default() -> Self {
        Self::new(scannable_limit())
    }
}

impl ScannableAdmission {
    fn new(limit: usize) -> Self {
        Self {
            localities: PtrHashMap::with_capacity(64),
            limit,
        }
    }

    pub(super) fn for_locality(&self, locality: u16) -> Arc<Budget> {
        crate::utils::keyed_state::get_or_insert(&self.localities, locality, || {
            Budget::new(self.limit)
        })
    }
    pub(super) async fn acquire(&self, locality: u16) -> ScannablePermit {
        let local = self.for_locality(locality).acquire().await;
        let process = process_budget().acquire().await;
        ScannablePermit {
            _local: local,
            _process: process,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_saturated_locality_does_not_block_another_locality_or_client() {
        let first_client = ScannableAdmission::new(2);
        let second_client = ScannableAdmission::new(2);
        let first = first_client.for_locality(1);
        let _held = (first.acquire().await, first.acquire().await);
        assert!(first_client.for_locality(1).try_acquire().is_none());
        let other = first_client.for_locality(2);
        let _other = tokio::time::timeout(std::time::Duration::from_secs(1), other.acquire())
            .await
            .unwrap();
        let second = second_client.for_locality(1);
        let _second = tokio::time::timeout(std::time::Duration::from_secs(1), second.acquire())
            .await
            .unwrap();
    }

    #[test]
    fn concurrent_admission_lookups_share_the_same_locality_limit() {
        let admission = ScannableAdmission::new(2);
        let start = std::sync::Barrier::new(8);
        let handles = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        admission.for_locality(7)
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(handles
            .iter()
            .all(|handle| Arc::ptr_eq(handle, &handles[0])));
        let _held = (
            handles[0].try_acquire().unwrap(),
            handles[0].try_acquire().unwrap(),
        );
        assert!(handles.iter().all(|handle| handle.try_acquire().is_none()));
    }
    #[test]
    fn process_budget_publication_has_one_identity_under_race() {
        let start = std::sync::Barrier::new(16);
        let budgets = std::thread::scope(|scope| {
            let tasks: Vec<_> = (0..16)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        process_budget()
                    })
                })
                .collect();
            tasks
                .into_iter()
                .map(|task| task.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(budgets
            .iter()
            .all(|budget| Arc::ptr_eq(budget, &budgets[0])));
    }

    #[tokio::test]
    async fn scannable_total_across_clients_and_localities_is_64() {
        use futures::FutureExt;
        assert_eq!(
            scannable_limit(),
            64,
            "run this control with the default process limit"
        );
        let clients: Vec<_> = (0..8).map(|_| ScannableAdmission::new(64)).collect();
        let mut held = Vec::new();
        for locality in 0..64 {
            held.push(
                clients[locality % clients.len()]
                    .acquire(locality as u16)
                    .await,
            );
        }
        let mut extra = Box::pin(clients[0].acquire(1000));
        assert!(
            extra.as_mut().now_or_never().is_none(),
            "process admitted more than 64 scannable sequences"
        );
        drop(held.pop());
        let granted = tokio::time::timeout(std::time::Duration::from_secs(2), extra)
            .await
            .unwrap();
        drop(granted);
        drop(held);
        let mut all = Vec::new();
        for locality in 0..64 {
            all.push(
                clients[locality % clients.len()]
                    .acquire(locality as u16)
                    .await,
            );
        }
        let mut blocked = Box::pin(clients[0].acquire(1001));
        assert!(blocked.as_mut().now_or_never().is_none());
        drop(blocked);
        drop(all);
    }

    #[tokio::test]
    async fn cancelling_a_parked_scannable_sequence_returns_both_budgets() {
        use futures::FutureExt;
        let client = ScannableAdmission::new(64);
        let held: Vec<_> = (0..64)
            .map(|_| process_budget().try_acquire().unwrap())
            .collect();
        let mut pending = Box::pin(client.acquire(7));
        assert!(pending.as_mut().now_or_never().is_none());
        drop(pending);
        drop(held);
        let local = client.for_locality(7);
        let all_local: Vec<_> = (0..64).map(|_| local.try_acquire().unwrap()).collect();
        let all_process: Vec<_> = (0..64)
            .map(|_| process_budget().try_acquire().unwrap())
            .collect();
        assert!(local.try_acquire().is_none());
        assert!(process_budget().try_acquire().is_none());
        drop((all_local, all_process));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_scannable_sequences_keep_the_process_bound() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let active = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for id in 0..256 {
            let active = Arc::clone(&active);
            tasks.push(tokio::spawn(async move {
                let client = ScannableAdmission::new(64);
                for _ in 0..8 {
                    let _permit = client.acquire(id).await;
                    assert!(active.fetch_add(1, Ordering::SeqCst) < 64);
                    tokio::task::yield_now().await;
                    active.fetch_sub(1, Ordering::SeqCst);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }
}
