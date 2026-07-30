//! Amortized revision-timestamp allocation for direct (non-transactional)
//! mutations.
//!
//! Every direct mutation needs a nonzero `revision_ts` that is strictly
//! greater than the revision it replaces and comparable with transactional
//! commit timestamps. Calling `HlcSource::try_now` per mutation pays a
//! wall-clock read plus a contended AcqRel compare-exchange on the one
//! server-wide clock word for every operation. This allocator instead
//! reserves timestamp ranges from the shared clock (`HlcSource::try_lease`,
//! Percolator-oracle style) and hands them out with an uncontended chunk-local
//! `fetch_add`.
//!
//! Because the reservation advances the shared clock past the lease end
//! before any leased value is issued, every transactional timestamp taken
//! afterwards is strictly greater than every leased value, and a restart —
//! which advances the clock beyond the maximum recovered `revision_ts` —
//! can never re-issue a leased value. Abandoned lease remainders only make
//! timestamps jump forward; gaps are already legal (failed writes create
//! them).

use bifrost::hlc::{Hlc, HlcError, HlcSource};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Timestamps reserved per refill. At the observed ~1M direct mutations/s a
/// span lasts about a millisecond, so leased values trail wall-clock time by
/// microseconds under load; nothing correctness-bearing reads wall-clock
/// meaning out of `revision_ts` (per-cell ordering uses the floor argument,
/// retention uses monotonic time, recovery compares stored values).
const REVISION_LEASE_SPAN: u64 = 1024;

pub struct RevisionAllocator {
    clock: Arc<HlcSource>,
    /// Last handed-out candidate; values are `fetch_add(1) + 1`.
    next: AtomicU64,
    /// Inclusive end of the installed lease. Zero means no usable lease.
    end: AtomicU64,
    /// Serializes refills so batching self-tunes with load: one refill in
    /// flight, and every taker that arrives during it consumes the result.
    refill: Mutex<()>,
}

impl RevisionAllocator {
    pub fn new(clock: Arc<HlcSource>) -> Self {
        Self {
            clock,
            next: AtomicU64::new(0),
            end: AtomicU64::new(0),
            refill: Mutex::new(()),
        }
    }

    /// Return a fresh revision timestamp strictly greater than `floor`.
    ///
    /// `floor` is the replaced revision for updates and deletes, and the
    /// chunk's assigned-revision watermark for inserts into an empty slot —
    /// which is what keeps a durable direct insert above any transactional
    /// tombstone recovery could otherwise prefer.
    pub fn take(&self, floor: u64) -> Result<u64, HlcError> {
        loop {
            let value = self.next.fetch_add(1, Ordering::Relaxed) + 1;
            if value <= self.end.load(Ordering::Acquire) && value > floor {
                return Ok(value);
            }
            self.refill(floor)?;
        }
    }

    fn refill(&self, floor: u64) -> Result<(), HlcError> {
        let _serialized = self.refill.lock();
        let end = self.end.load(Ordering::Relaxed);
        let next = self.next.load(Ordering::Relaxed);
        if next < end && end > floor {
            // Another taker refilled while we waited for the lock.
            return Ok(());
        }
        if floor != 0 {
            // A predecessor or watermark above the lease: advance the shared
            // clock beyond it so the fresh lease starts above the floor. The
            // node id is irrelevant — only the packed ts advances the clock.
            self.clock.try_observe(Hlc { ts: floor, node: 0 })?;
        }
        let range = self.clock.try_lease(REVISION_LEASE_SPAN)?;
        // Kill the old lease before moving `next` so a concurrent take cannot
        // pair an old-lease candidate with the new end; candidates consumed
        // between these stores are discarded, wasting at most a few values.
        self.end.store(0, Ordering::Release);
        self.next.store(range.start, Ordering::Relaxed);
        self.end.store(range.end, Ordering::Release);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_lease_for_test(&self, next: u64, end: u64) {
        self.end.store(0, Ordering::Release);
        self.next.store(next, Ordering::Relaxed);
        self.end.store(end, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_hands_out_strictly_increasing_nonzero_values() {
        let allocator = RevisionAllocator::new(Arc::new(HlcSource::new(1)));
        let mut prev = 0;
        for _ in 0..(REVISION_LEASE_SPAN * 3) {
            let value = allocator.take(0).unwrap();
            assert!(value > prev, "values must strictly increase across refills");
            prev = value;
        }
        assert!(prev > 0);
    }

    #[test]
    fn take_respects_a_floor_above_the_current_lease() {
        let clock = Arc::new(HlcSource::new(1));
        let allocator = RevisionAllocator::new(clock.clone());
        let low = allocator.take(0).unwrap();
        let floor = low + (REVISION_LEASE_SPAN * 10);
        let above = allocator.take(floor).unwrap();
        assert!(above > floor, "the floor must force a refill-with-observe");
        assert!(
            clock.try_now().unwrap().ts > above,
            "the shared clock must stay above every leased value"
        );
    }

    #[test]
    fn concurrent_takers_never_collide_or_regress() {
        let allocator = Arc::new(RevisionAllocator::new(Arc::new(HlcSource::new(1))));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let allocator = allocator.clone();
            handles.push(std::thread::spawn(move || {
                let mut values = Vec::with_capacity(4096);
                let mut prev = 0;
                for _ in 0..4096 {
                    let value = allocator.take(0).unwrap();
                    assert!(value > prev, "per-thread values must increase");
                    prev = value;
                    values.push(value);
                }
                values
            }));
        }
        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect();
        let count = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), count, "no two takers may receive the same value");
    }

    #[test]
    fn exhausted_clock_refuses_allocation() {
        let clock = Arc::new(HlcSource::new(1));
        let allocator = RevisionAllocator::new(clock);
        assert_eq!(
            allocator.take(u64::MAX - 1),
            Err(HlcError::Exhausted),
            "a floor at the top of the timestamp space must refuse"
        );
    }
}
