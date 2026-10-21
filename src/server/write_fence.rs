//! A per-database write fence.
//!
//! A logical dump has to read a database that is not changing underneath it.
//! The store has no snapshot scan and no multi-version storage to build one on
//! (point-cell MVCC was abandoned, see
//! `docs/superpowers/specs/2026-08-02-point-cell-mvcc-retrospective.md`; the
//! engine is repeatable-read OCC with per-transaction snapshots and no history),
//! so the consistent copy is taken the other way: the writers are held off for as
//! long as the copy takes.
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
//!   gate;
//! * the fencer publishes the gate (turns it odd), **then** waits for the counters to
//!   reach zero.
//!
//! Whichever side's store comes second in the total order sees the other: a
//! writer that saw the flag clear is in the counters the fencer waits on, and a
//! writer that was not in the counters when the fencer looked reads the flag
//! set and is refused. There is no window in which a write is neither.
//!
//! The counters are sharded across cache lines so the common case -- no fence
//! engaged -- costs one `fetch_add` on the writer's own cache line, one load of
//! the shared gate (read-mostly, so it stays in every core's cache), and one
//! `fetch_sub` when the write leaves: two uncontended atomic read-modify-writes and
//! one load. Measured end to end in `benches/write_path.rs`.

use crate::ram::cell::WriteError;
use crate::ram::schema::SchemaUid;
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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

/// Which families an engaged fence refuses writes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Protection {
    /// These families and no others: a dump protects what it copies and lets
    /// derived state (index write-back, statistics) keep flowing.
    Only(HashSet<SchemaUid>),
    /// Every family except these, including families created after the fence
    /// went up: a database that must not take writes at all (an unfinished
    /// restore) still lets the engine's own derived writers flush.
    AllExcept(HashSet<SchemaUid>),
}

impl Protection {
    pub fn covers(&self, family: SchemaUid) -> bool {
        match self {
            Protection::Only(families) => families.contains(&family),
            Protection::AllExcept(exempt) => !exempt.contains(&family),
        }
    }
}

/// What a write that names only a cell id (a remove, a field set) learned about
/// the cell, for [`WriteFence::enter_keyed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyedFamily {
    /// The cell exists and belongs to this family.
    Family(SchemaUid),
    /// There is no such cell right now.
    Missing,
    /// The cell could not be examined.
    Unknown,
}

struct PublishedProtection {
    protection: Arc<Protection>,
    engaged: bool,
}

pub struct WriteFence {
    /// Odd while a fence is engaged, even otherwise, and it only ever goes up:
    /// engaging adds one (even -> odd), releasing adds one (odd -> even). A flag
    /// that counts engagements instead of just saying "engaged" lets an
    /// admission record WHICH fence it saw, which is what makes the ordering
    /// between a write's decision and a fence's publication checkable after the
    /// fact (see the trace under `cfg(test)`).
    gate: AtomicU64,
    shards: Box<[Shard; SHARDS]>,
    /// The families being protected. Written before `gate` turns odd and read
    /// only after it is seen odd, so the gate's ordering publishes it.
    protected: arc_swap::ArcSwap<PublishedProtection>,
    refused: AtomicU64,
    #[cfg(test)]
    trace: trace::Trace,
}

/// A record of the fence's decisions, in a single total order, for tests that
/// must prove the handshake rather than observe it from outside.
#[cfg(test)]
pub mod trace {
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Event {
        /// A write's admission decision. `seen` is the gate value its decision
        /// read; `counted` means it entered the in-flight count; `refused` means
        /// it got `DatabaseFenced`.
        Admit {
            note: u64,
            seen: u64,
            counted: bool,
            refused: bool,
            tick: u64,
        },
        /// A counted write finished (recorded BEFORE it leaves the count).
        Done { note: u64, seen: u64, tick: u64 },
        /// The gate turned odd: the fence is published.
        Published { gate: u64, tick: u64 },
        /// `engage` observed nothing in flight and returned.
        Returned { gate: u64, tick: u64 },
        /// The hold dropped: the gate turned even.
        Released { gate: u64, tick: u64 },
    }

    #[derive(Default)]
    pub struct Trace {
        clock: AtomicU64,
        events: Mutex<Vec<Event>>,
    }

    impl Trace {
        pub fn tick(&self) -> u64 {
            self.clock.fetch_add(1, Ordering::SeqCst)
        }
        pub fn push(&self, event: Event) {
            self.events.lock().push(event);
        }
        pub fn take(&self) -> Vec<Event> {
            std::mem::take(&mut *self.events.lock())
        }
    }
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
    #[cfg(test)]
    note: u64,
    #[cfg(test)]
    seen: u64,
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if let Some((fence, shard)) = self.counted.take() {
            #[cfg(test)]
            {
                // Before the decrement: if the drain returned while this write
                // was still running, its Done tick would be after the drain's.
                let tick = fence.trace.tick();
                fence.trace.push(trace::Event::Done {
                    note: self.note,
                    seen: self.seen,
                    tick,
                });
            }
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
        let gate = self.fence.gate.fetch_add(1, Ordering::SeqCst) + 1;
        #[cfg(test)]
        {
            let tick = self.fence.trace.tick();
            self.fence.trace.push(trace::Event::Released { gate, tick });
        }
        #[cfg(not(test))]
        let _ = gate;
        let previous = self.fence.protected.load_full();
        // Keep the previous set visible to a writer that observed the old
        // odd gate. The next engagement can claim publication after this.
        self.fence.protected.store(Arc::new(PublishedProtection {
            protection: previous.protection.clone(),
            engaged: false,
        }));
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
            gate: AtomicU64::new(0),
            shards: Box::new(std::array::from_fn(|_| Shard(AtomicI64::new(0)))),
            protected: arc_swap::ArcSwap::from_pointee(PublishedProtection {
                protection: Arc::new(Protection::Only(HashSet::new())),
                engaged: false,
            }),
            refused: AtomicU64::new(0),
            #[cfg(test)]
            trace: Default::default(),
        }
    }

    pub fn is_engaged(&self) -> bool {
        self.gate.load(Ordering::SeqCst) & 1 == 1
    }

    /// The trace of admissions and fence transitions so far, drained.
    #[cfg(test)]
    pub fn take_trace(&self) -> Vec<trace::Event> {
        self.trace.take()
    }

    /// A tick in the trace's clock, for correlating outside observations.
    #[cfg(test)]
    pub fn trace_tick(&self) -> u64 {
        self.trace.tick()
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
    /// not be determined, and is refused while the fence is engaged.
    pub fn enter(
        &self,
        family: impl FnOnce() -> Option<SchemaUid>,
    ) -> Result<WriteGuard<'_>, WriteError> {
        self.enter_noted(0, family)
    }

    /// [`Self::enter`] with a caller-chosen `note` (a cell id, say) carried into
    /// the test trace so an admission can be matched to the write it admitted.
    pub fn enter_noted(
        &self,
        note: u64,
        family: impl FnOnce() -> Option<SchemaUid>,
    ) -> Result<WriteGuard<'_>, WriteError> {
        #[cfg(not(test))]
        let _ = note;
        let shard = shard_of_current_thread();
        self.shards[shard].0.fetch_add(1, Ordering::SeqCst);
        // THE decision: one SeqCst load of the gate. What it returns is what
        // this write saw of the fence, and everything below follows from it.
        let seen = self.gate.load(Ordering::SeqCst);
        if seen & 1 == 0 {
            #[cfg(test)]
            {
                let tick = self.trace.tick();
                self.trace.push(trace::Event::Admit {
                    note,
                    seen,
                    counted: true,
                    refused: false,
                    tick,
                });
            }
            return Ok(WriteGuard {
                counted: Some((self, shard)),
                #[cfg(test)]
                note,
                #[cfg(test)]
                seen,
            });
        }
        // Fenced: this write is not going to count, whatever the verdict.
        self.shards[shard].0.fetch_sub(1, Ordering::SeqCst);
        // A family that cannot be determined is refused, not admitted. For a
        // write that carries its cell the only way to get here is a schema this
        // server's cache does not know yet -- and the write path refreshes the
        // cache and retries, so admitting it unguarded would let a write to a
        // protected family through the retry.
        let protected = match family() {
            Some(uid) => self.protected.load().protection.covers(uid),
            None => true,
        };
        #[cfg(test)]
        {
            let tick = self.trace.tick();
            self.trace.push(trace::Event::Admit {
                note,
                seen,
                counted: false,
                refused: protected,
                tick,
            });
        }
        if protected {
            self.refused.fetch_add(1, Ordering::Relaxed);
            Err(WriteError::DatabaseFenced)
        } else {
            Ok(WriteGuard {
                counted: None,
                #[cfg(test)]
                note,
                #[cfg(test)]
                seen,
            })
        }
    }

    /// Count an operation that decides for itself whether the fence applies to
    /// it (a transaction's prepare or commit): `Some(guard)` if no fence is
    /// engaged, so a fencer that engages afterwards waits for it; `None` if one
    /// is, in which case the caller applies its own rule. Never a refusal.
    pub fn count_if_open(&self) -> Option<WriteGuard<'_>> {
        let shard = shard_of_current_thread();
        self.shards[shard].0.fetch_add(1, Ordering::SeqCst);
        let seen = self.gate.load(Ordering::SeqCst);
        if seen & 1 == 0 {
            #[cfg(test)]
            {
                let tick = self.trace.tick();
                self.trace.push(trace::Event::Admit {
                    note: 0,
                    seen,
                    counted: true,
                    refused: false,
                    tick,
                });
            }
            return Some(WriteGuard {
                counted: Some((self, shard)),
                #[cfg(test)]
                note: 0,
                #[cfg(test)]
                seen,
            });
        }
        self.shards[shard].0.fetch_sub(1, Ordering::SeqCst);
        None
    }

    /// Admission for a write that names only a cell id.
    ///
    /// `lookup` runs only while a fence is engaged. The result is `Ok(guard)`
    /// to proceed, or an error the caller returns INSTEAD of performing the
    /// write:
    ///
    /// * the cell belongs to a protected family: `DatabaseFenced`;
    /// * the cell is missing: `CellDoesNotExisted`, and the write is NOT
    ///   performed. Proceeding "because there is nothing to protect" is a
    ///   check-then-act race: a write admitted before the fence can create the
    ///   cell between this lookup and the removal, and the removal would then
    ///   delete a protected cell under an engaged fence. Answering at the
    ///   lookup makes the write take effect at the instant it looked, when the
    ///   cell really was absent;
    /// * the cell could not be examined: `DatabaseFenced`.
    pub fn enter_keyed(
        &self,
        note: u64,
        lookup: impl FnOnce() -> KeyedFamily,
    ) -> Result<WriteGuard<'_>, WriteError> {
        let missing = std::cell::Cell::new(false);
        let result = self.enter_noted(note, || match lookup() {
            KeyedFamily::Family(uid) => Some(uid),
            KeyedFamily::Missing => {
                missing.set(true);
                None
            }
            KeyedFamily::Unknown => None,
        });
        match result {
            Err(WriteError::DatabaseFenced) if missing.get() => Err(WriteError::CellDoesNotExisted),
            other => other,
        }
    }

    /// Whether a write to `family` would be refused right now. For callers that
    /// need the verdict without a guard (a prepare that is about to hold one).
    pub fn would_refuse(&self, family: Option<SchemaUid>) -> bool {
        self.is_engaged() && family.is_some_and(|uid| self.protects(uid))
    }

    /// Whether the current protection covers `family`. Meaningful only while
    /// a fence is engaged; for use inside the `family` lookup of [`Self::enter`],
    /// which runs only then.
    pub fn protects(&self, family: SchemaUid) -> bool {
        self.protected.load().protection.covers(family)
    }

    /// Engage the fence over `protected` and wait for writes already in flight
    /// to finish. On timeout the fence is released and the error says how many
    /// writers were still running.
    pub async fn engage(
        self: &Arc<Self>,
        protected: HashSet<SchemaUid>,
        drain_timeout: Duration,
    ) -> Result<FenceHold, FenceError> {
        self.engage_with(Protection::Only(protected), drain_timeout)
            .await
    }

    /// [`Self::engage`] with any [`Protection`].
    pub async fn engage_with(
        self: &Arc<Self>,
        protected: Protection,
        drain_timeout: Duration,
    ) -> Result<FenceHold, FenceError> {
        // Only a publisher that claims the current immutable snapshot can
        // replace the set. A refused engagement cannot overwrite it.
        let before = self.protected.load_full();
        if before.engaged {
            return Err(FenceError::AlreadyEngaged);
        }
        let current = self.gate.load(Ordering::SeqCst);
        let next = Arc::new(PublishedProtection {
            protection: Arc::new(protected),
            engaged: true,
        });
        let previous = self.protected.compare_and_swap(&before, next);
        if !Arc::ptr_eq(&previous, &before) {
            return Err(FenceError::AlreadyEngaged);
        }
        self.gate
            .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
            .expect("the claimed fence publisher owns the gate transition");
        let published = current + 1;
        #[cfg(test)]
        {
            let tick = self.trace.tick();
            self.trace.push(trace::Event::Published {
                gate: published,
                tick,
            });
        }
        let hold = FenceHold {
            fence: Arc::clone(self),
        };
        let started = Instant::now();
        loop {
            let in_flight = self.in_flight();
            if in_flight <= 0 {
                #[cfg(test)]
                {
                    let tick = self.trace.tick();
                    self.trace.push(trace::Event::Returned {
                        gate: published,
                        tick,
                    });
                }
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
    use std::sync::atomic::AtomicBool;

    fn protect(ids: &[u32]) -> HashSet<SchemaUid> {
        ids.iter().map(|id| SchemaUid(*id)).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn racing_fence_publishers_do_not_overwrite_the_winner() {
        let fence = Arc::new(WriteFence::new());
        let start = Arc::new(tokio::sync::Barrier::new(8));
        let mut tasks = Vec::new();
        for uid in 1..=8 {
            let fence = fence.clone();
            let start = start.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                (
                    uid,
                    fence
                        .engage(
                            [SchemaUid(uid)].into_iter().collect(),
                            Duration::from_secs(1),
                        )
                        .await,
                )
            }));
        }
        let results = futures::future::join_all(tasks).await;
        let winners: Vec<_> = results
            .into_iter()
            .map(Result::unwrap)
            .filter_map(|(uid, hold)| hold.ok().map(|hold| (uid, hold)))
            .collect();
        assert_eq!(winners.len(), 1);
        for uid in 1..=8 {
            assert_eq!(fence.protects(SchemaUid(uid)), uid == winners[0].0);
        }
        drop(winners);
        assert!(!fence.is_engaged());
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
        // Derived state keeps flowing.
        assert!(fence.enter(|| Some(SchemaUid(8))).is_ok());
        // A family that cannot be determined is not let through: the cache
        // refresh-and-retry in the write path would carry it past the fence.
        assert_eq!(fence.enter(|| None).err(), Some(WriteError::DatabaseFenced));
        assert_eq!(fence.refused_writes(), 2);
        drop(hold);
        assert!(fence.enter(|| Some(SchemaUid(7))).is_ok());
    }

    #[tokio::test]
    async fn an_all_except_fence_refuses_every_family_but_the_exempt_ones() {
        let fence = Arc::new(WriteFence::new());
        let _hold = fence
            .engage_with(Protection::AllExcept(protect(&[9])), Duration::from_secs(1))
            .await
            .unwrap();
        // Known or not, every family is refused...
        assert_eq!(
            fence.enter(|| Some(SchemaUid(7))).err(),
            Some(WriteError::DatabaseFenced)
        );
        assert_eq!(
            fence.enter(|| Some(SchemaUid(12345))).err(),
            Some(WriteError::DatabaseFenced)
        );
        assert!(fence.would_refuse(Some(SchemaUid(7))));
        // ...but the exempt ones.
        assert!(fence.enter(|| Some(SchemaUid(9))).is_ok());
        assert!(!fence.would_refuse(Some(SchemaUid(9))));
    }

    #[tokio::test]
    async fn a_keyed_write_on_a_missing_cell_is_answered_not_performed() {
        let fence = Arc::new(WriteFence::new());
        let _hold = fence
            .engage(protect(&[7]), Duration::from_secs(1))
            .await
            .unwrap();
        // Missing: the write does not run, and says so the way it would have.
        assert_eq!(
            fence.enter_keyed(1, || KeyedFamily::Missing).err(),
            Some(WriteError::CellDoesNotExisted)
        );
        // Unexaminable: refused. Protected: refused. Unprotected: proceeds.
        assert_eq!(
            fence.enter_keyed(1, || KeyedFamily::Unknown).err(),
            Some(WriteError::DatabaseFenced)
        );
        assert_eq!(
            fence
                .enter_keyed(1, || KeyedFamily::Family(SchemaUid(7)))
                .err(),
            Some(WriteError::DatabaseFenced)
        );
        assert!(fence
            .enter_keyed(1, || KeyedFamily::Family(SchemaUid(8)))
            .is_ok());
        // The lookup is not consulted when nothing is engaged.
        drop(_hold);
        assert!(fence
            .enter_keyed(1, || panic!("no lookup without a fence"))
            .is_ok());
    }

    #[tokio::test]
    async fn count_if_open_counts_only_without_a_fence() {
        let fence = Arc::new(WriteFence::new());
        let guard = fence.count_if_open().expect("no fence");
        assert_eq!(fence.in_flight(), 1);
        drop(guard);
        let _hold = fence
            .engage(protect(&[7]), Duration::from_secs(1))
            .await
            .unwrap();
        assert!(fence.count_if_open().is_none());
        assert_eq!(fence.in_flight(), 0);
        assert_eq!(fence.refused_writes(), 0, "never a refusal");
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
        assert!(
            !engaging.is_finished(),
            "must wait for the writer in flight"
        );
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
        assert!(
            !fence.is_engaged(),
            "a failed engage must not leave the fence up"
        );
    }

    #[tokio::test]
    async fn a_second_engage_is_refused() {
        let fence = Arc::new(WriteFence::new());
        let _hold = fence
            .engage(protect(&[1]), Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            fence
                .engage(protect(&[1]), Duration::from_secs(1))
                .await
                .err(),
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
            assert_eq!(
                writing.load(Ordering::SeqCst),
                0,
                "a writer survived the drain"
            );
            hold_active.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_micros(300)).await;
            assert_eq!(
                writing.load(Ordering::SeqCst),
                0,
                "a writer slipped in while held"
            );
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
