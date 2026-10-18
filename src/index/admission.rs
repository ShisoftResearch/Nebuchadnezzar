//! Independent admission for each cell locality in one index client.

use crate::utils::sharded::Sharded;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub(super) struct ScannableAdmission {
    localities: Sharded<Mutex<HashMap<u16, Arc<Semaphore>>>>,
    limit: usize,
}

impl Default for ScannableAdmission {
    fn default() -> Self {
        let limit = std::env::var("NEB_SCANNABLE_INFLIGHT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|limit| *limit > 0)
            .unwrap_or(64);
        Self::new(limit)
    }
}

impl ScannableAdmission {
    fn new(limit: usize) -> Self {
        Self {
            localities: Sharded::default(),
            limit,
        }
    }

    pub(super) fn for_locality(&self, locality: u16) -> Arc<Semaphore> {
        // Registry guards end before a caller waits for admission or performs I/O.
        self.localities
            .shard(&locality)
            .lock()
            .entry(locality)
            .or_insert_with(|| Arc::new(Semaphore::new(self.limit)))
            .clone()
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
        let _held = first.acquire_many(2).await.unwrap();
        assert!(first_client.for_locality(1).try_acquire().is_err());
        let other = first_client.for_locality(2);
        let _other = tokio::time::timeout(std::time::Duration::from_secs(1), other.acquire())
            .await
            .unwrap()
            .unwrap();
        let second = second_client.for_locality(1);
        let _second = tokio::time::timeout(std::time::Duration::from_secs(1), second.acquire())
            .await
            .unwrap()
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
        let _held = handles[0].try_acquire_many(2).unwrap();
        assert!(handles.iter().all(|handle| handle.try_acquire().is_err()));
    }
}
