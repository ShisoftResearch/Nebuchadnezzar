//! A per-database write fence.
//!
//! A logical dump has to read a database that is not changing underneath it.
//! The store has no snapshot scan (point-cell MVCC covers single-cell reads for
//! a short window, and says in its own design that ranges and predicates are
//! outside it), so the consistent copy is taken the other way: the writers are
//! held off for as long as the copy takes.
//!
//! # What is fenced
//!
//! Only writes to the schemas the fencer names (`engage`). Everything else
//! keeps working, and has to: the ranged-index write-back, statistics, vector
//! index maintenance and the like are derived state that a dump neither carries
//! nor needs frozen, and refusing them would fail background flushers whose
//! crash consistency depends on their writes landing.
//!
//! # Why it cannot be raced
//!
//! Every mutating entry point passes through [`WriteFence::enter`] *before* it
//! touches storage and keeps the returned guard until it is done. `enter` is
//! the classic store-then-load handshake over sequentially consistent atomics:
//!
//! * a writer publishes "I am in flight" in a counter, **then** reads the
//!   fenced flag;
//! * the fencer publishes the fenced flag, **then** waits for the counters to
//!   reach zero.
//!
//! Whichever side's store comes second in the total order sees the other: a
//! writer that saw the flag clear is in the counters the fencer waits on, and a
//! writer that was not in the counters when the fencer looked reads the flag
//! set and is refused. There is no window in which a write is neither.
//!
//! The counters are sharded across cache lines so the common case -- no fence
//! engaged -- costs one uncontended `fetch_add` and one load per write.

use crate::ram::cell::WriteError;
use crate::ram::schema::SchemaUid;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: usize = 32;

#[repr(align(128))]
struct Shard(AtomicI64);

/// Why a fence could not be engaged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceError {
    /// Another fence is already engaged on this database.
    AlreadyEngaged,
    /// Writes that were already in flight did not finish in time. The fence has
    /// been released again: nothing is left half-engaged.
    DrainTimeout {
        in_flight_writes: i64,
        waited: Duration,
    },
}

impl std::fmt::Display for FenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FenceError::AlreadyEngaged => write!(f, "a write fence is already engaged"),
            FenceError::DrainTimeout {
                in_flight_writes,
                waited,
            } => write!(
                f,
                "{in_flight_writes} write(s) still in flight after {waited:?}; fence released"
            ),
        }
    }
}

impl std::error::Error for FenceError {}

pub struct WriteFence {
    fenced: AtomicBool,
    shards: Box<[Shard; SHARDS]>,
    /// The families being protected. Written before `fenced` is set and read
    /// only after it is seen set, so the flag's ordering publishes it.
    protected: RwLock<Arc<HashSet<SchemaUid>>>,
    refused: AtomicU64,
}

impl Default for WriteFence {
    fn default() -> Self {
        Self::new()
    }
}

/// Held for the duration of one write. Dropping it ends the write's claim on
/// the fence's drain.
pub struct WriteGuard<'a> {
    counted: Option<(&'a WriteFence, usize)>,
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if let Some((fence, shard)) = self.counted.take() {
            fence.shards[shard].0.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// An engaged fence. Released on drop, so a dump that errors, panics or is
/// cancelled cannot leave a database permanently read-only.
pub struct FenceHold {
    fence: Arc<WriteFence>,
}

impl FenceHold {
    pub fn fence(&self) -> &Arc<WriteFence> {
        &self.fence
    }
}

impl Drop for FenceHold {
    fn drop(&mut self) {
        self.fence.fenced.store(false, Ordering::SeqCst);
    }
}

fn shard_of_current_thread() -> usize {
    thread_local! {
        static SHARD: usize = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            std::thread::current().id().hash(&mut hasher);
            (hasher.finish() as usize) % SHARDS
        };
    }
    SHARD.with(|s| *s)
}

impl WriteFence {
    pub fn new() -> Self {
        Self {
            fenced: AtomicBool::new(false),
            shards: Box::new(std::array::from_fn(|_| Shard(AtomicI64::new(0)))),
            protected: RwLock::new(Arc::new(HashSet::new())),
            refused: AtomicU64::new(0),
        }
    }

    pub fn is_engaged(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }

    /// Writes refused by the fence since it was created.
    pub fn refused_writes(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// Writes currently inside a guard.
    pub fn in_flight(&self) -> i64 {
        self.shards.iter().map(|s| s.0.load(Ordering::SeqCst)).sum()
    }

    /// Admit one write, or refuse it with [`WriteError::DatabaseFenced`].
    ///
    /// `family` is only consulted when a fence is engaged, so the caller can
    /// pass a closure that does a lookup (a header read for a remove, say)
    /// without paying for it on the normal path. `None` means the schema could
    /// not be determined -- a cell that does not exist, which the write will
    /// reject on its own -- and is admitted.
    pub fn enter(
        &self,
        family: impl FnOnce() -> Option<SchemaUid>,
    ) -> Result<WriteGuard<'_>, WriteError> {
        let shard = shard_of_current_thread();
        self.shards[shard].0.fetch_add(1, Ordering::SeqCst);
        if !self.fenced.load(Ordering::SeqCst) {
            return Ok(WriteGuard {
                counted: Some((self, shard)),
            });
        }
        // Fenced: this write is not going to count, whatever the verdict.
        self.shards[shard].0.fetch_sub(1, Ordering::SeqCst);
        let protected = match family() {
            Some(uid) => self.protected.read().contains(&uid),
            None => false,
        };
        if protected {
            self.refused.fetch_add(1, Ordering::Relaxed);
            Err(WriteError::DatabaseFenced)
        } else {
            Ok(WriteGuard { counted: None })
        }
    }

    /// Whether a write to `family` would be refused right now. For callers that
    /// need the verdict without a guard (a prepare that is about to hold one).
    pub fn would_refuse(&self, family: Option<SchemaUid>) -> bool {
        self.is_engaged()
            && family.is_some_and(|uid| self.protected.read().contains(&uid))
    }

    /// Engage the fence over `protected` and wait for writes already in flight
    /// to finish. On timeout the fence is released and the error says how many
    /// writers were still running.
    pub async fn engage(
        self: &Arc<Self>,
        protected: HashSet<SchemaUid>,
        drain_timeout: Duration,
    ) -> Result<FenceHold, FenceError> {
        *self.protected.write() = Arc::new(protected);
        if self
            .fenced
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(FenceError::AlreadyEngaged);
        }
        let hold = FenceHold {
            fence: Arc::clone(self),
        };
        let started = Instant::now();
        loop {
            let in_flight = self.in_flight();
            if in_flight <= 0 {
                return Ok(hold);
            }
            if started.elapsed() >= drain_timeout {
                drop(hold);
                return Err(FenceError::DrainTimeout {
                    in_flight_writes: in_flight,
                    waited: started.elapsed(),
                });
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn protect(ids: &[u32]) -> HashSet<SchemaUid> {
        ids.iter().map(|id| SchemaUid(*id)).collect()
    }

    #[tokio::test]
    async fn a_disengaged_fence_admits_everything() {
        let fence = Arc::new(WriteFence::new());
        let guard = fence.enter(|| Some(SchemaUid(1))).unwrap();
        assert_eq!(fence.in_flight(), 1);
        drop(guard);
        assert_eq!(fence.in_flight(), 0);
    }

    #[tokio::test]
    async fn an_engaged_fence_refuses_protected_families_only() {
        let fence = Arc::new(WriteFence::new());
        let hold = fence
            .engage(protect(&[7]), Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            fence.enter(|| Some(SchemaUid(7))).err(),
            Some(WriteError::DatabaseFenced)
        );
        // Derived state keeps flowing, and an unknown cell is the write's own
        // problem to report.
        assert!(fence.enter(|| Some(SchemaUid(8))).is_ok());
        assert!(fence.enter(|| None).is_ok());
        assert_eq!(fence.refused_writes(), 1);
        drop(hold);
        assert!(fence.enter(|| Some(SchemaUid(7))).is_ok());
    }

    #[tokio::test]
    async fn engage_waits_for_in_flight_writes_and_then_refuses_new_ones() {
        let fence = Arc::new(WriteFence::new());
        let guard = fence.enter(|| Some(SchemaUid(7))).unwrap();
        let engaging = {
            let fence = Arc::clone(&fence);
            tokio::spawn(async move { fence.engage(protect(&[7]), Duration::from_secs(5)).await })
        };
        // Give the fencer time to set the flag and start draining.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!engaging.is_finished(), "must wait for the writer in flight");
        assert_eq!(
            fence.enter(|| Some(SchemaUid(7))).err(),
            Some(WriteError::DatabaseFenced),
            "a writer arriving while the fence drains is refused"
        );
        drop(guard);
        let hold = engaging.await.unwrap().unwrap();
        drop(hold);
    }

    #[tokio::test]
    async fn drain_timeout_releases_the_fence() {
        let fence = Arc::new(WriteFence::new());
        let _stuck = fence.enter(|| Some(SchemaUid(7))).unwrap();
        let err = fence
            .engage(protect(&[7]), Duration::from_millis(20))
            .await
            .err()
            .unwrap();
        assert!(matches!(err, FenceError::DrainTimeout { .. }));
        assert!(!fence.is_engaged(), "a failed engage must not leave the fence up");
    }

    #[tokio::test]
    async fn a_second_engage_is_refused() {
        let fence = Arc::new(WriteFence::new());
        let _hold = fence
            .engage(protect(&[1]), Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            fence.engage(protect(&[1]), Duration::from_secs(1)).await.err(),
            Some(FenceError::AlreadyEngaged)
        );
    }

    /// The property the whole design rests on: with writers hammering `enter`
    /// across threads while a fence is engaged and released repeatedly, no
    /// protected write is ever admitted while the fence is held, and every
    /// admitted write was either complete before `engage` returned or started
    /// after the hold dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn racing_writers_never_slip_through_an_engaged_fence() {
        use std::sync::atomic::AtomicUsize;
        let fence = Arc::new(WriteFence::new());
        // Writes that have been admitted and are currently "writing".
        let writing = Arc::new(AtomicUsize::new(0));
        let violations = Arc::new(AtomicUsize::new(0));
        let hold_active = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));

        let mut writers = Vec::new();
        for _ in 0..6 {
            let fence = Arc::clone(&fence);
            let writing = Arc::clone(&writing);
            let violations = Arc::clone(&violations);
            let hold_active = Arc::clone(&hold_active);
            let stop = Arc::clone(&stop);
            writers.push(tokio::spawn(async move {
                let mut admitted = 0u64;
                let mut refused = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    match fence.enter(|| Some(SchemaUid(5))) {
                        Ok(_guard) => {
                            writing.fetch_add(1, Ordering::SeqCst);
                            // A write is "in progress" here. If the fence has
                            // been reported held, this is a slip-through.
                            if hold_active.load(Ordering::SeqCst) {
                                violations.fetch_add(1, Ordering::SeqCst);
                            }
                            tokio::task::yield_now().await;
                            if hold_active.load(Ordering::SeqCst) {
                                violations.fetch_add(1, Ordering::SeqCst);
                            }
                            writing.fetch_sub(1, Ordering::SeqCst);
                            admitted += 1;
                        }
                        Err(WriteError::DatabaseFenced) => {
                            refused += 1;
                            tokio::task::yield_now().await;
                        }
                        Err(other) => panic!("unexpected {other:?}"),
                    }
                }
                (admitted, refused)
            }));
        }

        for _ in 0..200 {
            let hold = fence
                .engage(protect(&[5]), Duration::from_secs(5))
                .await
                .unwrap();
            // The moment engage returns, nothing may be mid-write.
            assert_eq!(writing.load(Ordering::SeqCst), 0, "a writer survived the drain");
            hold_active.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_micros(300)).await;
            assert_eq!(writing.load(Ordering::SeqCst), 0, "a writer slipped in while held");
            hold_active.store(false, Ordering::SeqCst);
            drop(hold);
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
        stop.store(true, Ordering::Relaxed);
        let mut admitted = 0;
        let mut refused = 0;
        for w in writers {
            let (a, r) = w.await.unwrap();
            admitted += a;
            refused += r;
        }
        assert_eq!(violations.load(Ordering::SeqCst), 0);
        assert!(admitted > 0, "writers must make progress between fences");
        assert!(refused > 0, "the race must actually have been exercised");
    }
}
