//! Allocated-class cell id allocation (see
//! `docs/superpowers/specs/2026-08-02-compact-cell-id-design.md`).
//!
//! Uniqueness rests on `(origin, sequence)` with two invariants:
//!
//! 1. *Single ownership of each origin slot* — enforced by the lease
//!    store (consensus-backed in production), fenced by an epoch: an
//!    allocator whose epoch is stale refuses to issue.
//! 2. *Durable-lease-before-issue* — a block lease is persisted through
//!    the store before any id from the block is handed out, so every
//!    issued id is ≤ its origin's durable lease end at all times.
//!    Abandoned block tails become gaps; gaps are legal.
//!
//! The in-memory fast path is the revision-allocator discipline: an
//! uncontended `fetch_add` per id, monotone `fetch_max` on refill (a
//! plain store can rewind past overshoot candidates — the exact race
//! fixed in the revision allocator), and one refill in flight so
//! batching self-tunes with load.

use dovahkiin::types::custom_types::id::{Id, ID_ORIGIN_MASK, ID_SEQUENCE_MASK};
use parking_lot::Mutex;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Ids handed out per durable lease. At 2^20 ids per block, a restart
/// wastes at most one block tail — noise against the 2^36 sequence
/// space — and consensus traffic amortizes to one round per million ids.
pub const ID_BLOCK_SPAN: u64 = 1 << 20;

/// A durable block lease: this allocator (`origin`, at `epoch`) may
/// issue sequences in `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockLease {
    pub origin: u16,
    pub epoch: u64,
    pub start: u64,
    pub end: u64,
}

/// Durability provider for block leases. The production implementation
/// replicates through consensus; tests and volatile deployments use the
/// in-memory store. The contract either way: `lease_block` returns only
/// after the lease is at least as durable as any data that could
/// reference the issued ids, and it must reject a stale epoch.
pub trait LeaseStore: Send + Sync {
    /// Persist a lease for the next `span` sequences at or above
    /// `floor`, returning the granted range. Rejects stale epochs.
    fn lease_block(&self, origin: u16, epoch: u64, floor: u64, span: u64)
        -> io::Result<BlockLease>;

    /// Highest durably-leased end for the origin, if any. Used by
    /// recovery to establish the resume floor.
    fn last_lease_end(&self, origin: u16) -> io::Result<Option<u64>>;
}

/// In-memory lease store for tests and volatile deployments. Volatile
/// deployments still use the same code path — distinguishing "nothing
/// can reference me" buys nothing and would add a mode.
#[derive(Default)]
pub struct VolatileLeaseStore {
    state: Mutex<std::collections::HashMap<u16, (u64, u64)>>, // origin -> (epoch, end)
}

impl LeaseStore for VolatileLeaseStore {
    fn lease_block(
        &self,
        origin: u16,
        epoch: u64,
        floor: u64,
        span: u64,
    ) -> io::Result<BlockLease> {
        let mut state = self.state.lock();
        let entry = state.entry(origin).or_insert((epoch, 0));
        if epoch < entry.0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "stale epoch {} for origin {} (current {})",
                    epoch, origin, entry.0
                ),
            ));
        }
        entry.0 = epoch;
        let start = entry.1.max(floor);
        let end = start.checked_add(span).ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "origin sequence space exhausted")
        })?;
        if end > ID_SEQUENCE_MASK {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("origin {} sequence space exhausted", origin),
            ));
        }
        entry.1 = end;
        Ok(BlockLease {
            origin,
            epoch,
            start,
            end,
        })
    }

    fn last_lease_end(&self, origin: u16) -> io::Result<Option<u64>> {
        Ok(self.state.lock().get(&origin).map(|(_, end)| *end))
    }
}

/// Per-database allocator for one origin slot.
pub struct IdAllocator {
    origin: u16,
    epoch: AtomicU64,
    /// Last handed-out sequence candidate; values are `fetch_add(1) + 1`.
    next: AtomicU64,
    /// Exclusive end of the installed lease. Zero means no usable lease.
    end: AtomicU64,
    /// Serializes refills; every taker arriving during one consumes its
    /// result.
    refill: Mutex<()>,
    store: Arc<dyn LeaseStore>,
}

impl IdAllocator {
    /// Restores an allocator for `origin` at `epoch`, resuming strictly
    /// above the store's durable lease end (the recovery floor rule:
    /// the floor comes from the durable record, never from scanning
    /// stored cells). `scanned_floor` is the belt-and-suspenders bound
    /// from the recovery scan; the effective floor is the max of both.
    pub fn recover(
        store: Arc<dyn LeaseStore>,
        origin: u16,
        epoch: u64,
        scanned_floor: u64,
    ) -> io::Result<Self> {
        assert_eq!(origin as u64 & !ID_ORIGIN_MASK, 0, "origin exceeds field");
        let durable_floor = store.last_lease_end(origin)?.unwrap_or(0);
        let floor = durable_floor.max(scanned_floor);
        let allocator = Self {
            origin,
            epoch: AtomicU64::new(epoch),
            next: AtomicU64::new(floor),
            end: AtomicU64::new(0),
            refill: Mutex::new(()),
            store,
        };
        Ok(allocator)
    }

    pub fn origin(&self) -> u16 {
        self.origin
    }

    /// Fences this allocator: a bumped epoch elsewhere makes every
    /// subsequent lease attempt fail, so a zombie holding a warm block
    /// can drain it (those ids were durably leased to it) but never
    /// lease again.
    pub fn fence_check_epoch(&self, current: u64) -> bool {
        self.epoch.load(Ordering::Acquire) >= current
    }

    /// Allocates one sequence value. The id is composed by the caller
    /// (locality policy) via [`compose`].
    pub fn take_sequence(&self) -> io::Result<u64> {
        loop {
            let value = self.next.fetch_add(1, Ordering::Relaxed) + 1;
            if value <= self.end.load(Ordering::Acquire) {
                return Ok(value);
            }
            self.refill()?;
        }
    }

    /// Allocates an id with locality copied from `anchor`
    /// (`Id::new_with` semantics).
    pub fn take_near(&self, anchor: &Id) -> io::Result<Id> {
        let sequence = self.take_sequence()?;
        Ok(Id::allocated_near(anchor, self.origin, sequence))
    }

    /// Allocates an id with mixer-derived uniform locality.
    pub fn take_uniform(&self) -> io::Result<Id> {
        let sequence = self.take_sequence()?;
        Ok(compose_uniform(self.origin, sequence))
    }

    fn refill(&self) -> io::Result<()> {
        let _serialized = self.refill.lock();
        let end = self.end.load(Ordering::Relaxed);
        let next = self.next.load(Ordering::Relaxed);
        if next < end {
            // Another taker refilled while we waited for the lock.
            return Ok(());
        }
        let lease = self.store.lease_block(
            self.origin,
            self.epoch.load(Ordering::Acquire),
            next,
            ID_BLOCK_SPAN,
        )?;
        // Kill the old lease before moving `next` so a concurrent take
        // cannot pair an old-lease candidate with the new end. `next`
        // must only move forward (fetch_max, never store): spinning
        // takers overshoot the old end, and a plain store of
        // `lease.start` below the overshoot would reissue values.
        self.end.store(0, Ordering::Release);
        self.next.fetch_max(lease.start, Ordering::Relaxed);
        self.end.store(lease.end, Ordering::Release);
        Ok(())
    }
}

/// Uniform-locality composition: a multiply-xor-shift mix of the unique
/// bits, so placement is statistically uniform, deterministic across
/// replays, and free of RNG state in the allocation hot path.
pub fn compose_uniform(origin: u16, sequence: u64) -> Id {
    let unique = ((origin as u64) << 36) | (sequence & ID_SEQUENCE_MASK);
    let mixed = splitmix64(unique);
    Id::allocated((mixed >> 49) as u16, origin, sequence)
}

#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn allocator(origin: u16) -> IdAllocator {
        IdAllocator::recover(Arc::new(VolatileLeaseStore::default()), origin, 1, 0).unwrap()
    }

    #[test]
    fn sequences_strictly_increase_across_refills() {
        let alloc = allocator(1);
        let mut prev = 0;
        for _ in 0..(ID_BLOCK_SPAN * 2 + 10) {
            let seq = alloc.take_sequence().unwrap();
            assert!(seq > prev, "sequences must strictly increase");
            prev = seq;
        }
    }

    #[test]
    fn concurrent_takers_never_collide_or_regress() {
        let alloc = Arc::new(allocator(2));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let alloc = alloc.clone();
            handles.push(std::thread::spawn(move || {
                let mut values = Vec::with_capacity(4096);
                let mut prev = 0;
                for _ in 0..4096 {
                    let v = alloc.take_sequence().unwrap();
                    assert!(v > prev, "per-thread sequences must increase");
                    prev = v;
                    values.push(v);
                }
                values
            }));
        }
        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        let count = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), count, "no two takers may receive the same value");
    }

    #[test]
    fn recovery_resumes_above_durable_lease_end() {
        let store = Arc::new(VolatileLeaseStore::default());
        let first = IdAllocator::recover(store.clone(), 3, 1, 0).unwrap();
        let mut last = 0;
        for _ in 0..10 {
            last = first.take_sequence().unwrap();
        }
        drop(first);
        // Simulated restart: the block tail is abandoned; the new
        // allocator resumes above the durable end, never reissuing.
        let second = IdAllocator::recover(store, 3, 2, 0).unwrap();
        let resumed = second.take_sequence().unwrap();
        assert!(resumed > last);
        assert!(resumed > ID_BLOCK_SPAN, "must resume above the leased block");
    }

    #[test]
    fn scanned_floor_raises_recovery_floor() {
        let store = Arc::new(VolatileLeaseStore::default());
        let alloc = IdAllocator::recover(store, 4, 1, 5_000_000).unwrap();
        assert!(alloc.take_sequence().unwrap() > 5_000_000);
    }

    #[test]
    fn stale_epoch_is_fenced_at_lease_time() {
        let store = Arc::new(VolatileLeaseStore::default());
        let newer = IdAllocator::recover(store.clone(), 5, 7, 0).unwrap();
        newer.take_sequence().unwrap(); // records epoch 7 in the store
        let zombie = IdAllocator::recover(store, 5, 3, 0).unwrap();
        // The zombie holds no warm block, so its first take must lease —
        // and the store rejects its stale epoch.
        assert!(zombie.take_sequence().is_err());
    }

    #[test]
    fn affinity_and_uniform_composition() {
        let alloc = allocator(6);
        let anchor = Id::allocated(0x1234, 9, 42);
        let near = alloc.take_near(&anchor).unwrap();
        assert_eq!(near.locality(), anchor.locality());
        assert_eq!(near.origin(), 6);

        let mut buckets = HashSet::new();
        for _ in 0..4096 {
            buckets.insert(alloc.take_uniform().unwrap().locality());
        }
        // 4096 draws over 32k buckets: expect wide spread, no gross
        // clumping (chi-squared level checks live in the design's test
        // plan; this is the smoke bound).
        assert!(buckets.len() > 3000, "uniform locality clumped: {}", buckets.len());
    }
}
