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
//! per-lane atomic cursor and an immutable upper bound per durable block.
//! A lane has one refill in flight. Different lanes lease independently.

use crate::utils::keyed_state::KeyedState;
use dovahkiin::types::custom_types::id::{Id, ID_ORIGIN_MASK, ID_SEQUENCE_MASK};
use futures::future::BoxFuture;
use futures::prelude::*;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Ids handed out per durable lease. At 2^20 ids per block, a restart
/// wastes at most one tail per active lane (64 lanes maximum). Consensus
/// traffic amortizes to one round per million IDs within each lane.
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
    /// `floor`, returning the granted range. Rejects stale epochs. The
    /// production store performs a consensus round; the fast path never
    /// reaches here (one call per ID_BLOCK_SPAN ids).
    fn lease_block(
        &self,
        origin: u16,
        epoch: u64,
        floor: u64,
        span: u64,
    ) -> BoxFuture<'_, io::Result<BlockLease>>;

    /// Highest durably-leased end for the origin, if any. Used by
    /// recovery to establish the resume floor.
    fn last_lease_end(&self, origin: u16) -> BoxFuture<'_, io::Result<Option<u64>>>;

    /// Durably raise `origin`'s lease end to at least `floor` (monotone,
    /// idempotent), so no later lease for the origin starts at or below it.
    /// Returns the lease end after the call. Needs no epoch: it is used to
    /// account for ids imported from elsewhere, including for origins whose
    /// holder has not claimed the slot yet.
    fn reserve_floor(&self, origin: u16, floor: u64) -> BoxFuture<'_, io::Result<u64>>;
}

/// In-memory lease store for tests and volatile deployments. Volatile
/// deployments still use the same code path — distinguishing "nothing
/// can reference me" buys nothing and would add a mode.
#[derive(Default)]
pub struct VolatileLeaseStore {
    state: KeyedState<u16, (u64, u64)>, // origin -> (epoch, end)
}

impl VolatileLeaseStore {
    fn lease_block_sync(
        &self,
        origin: u16,
        epoch: u64,
        floor: u64,
        span: u64,
    ) -> io::Result<BlockLease> {
        let mut state = crate::profile_lock!(C12, self.state.entry(&origin));
        let entry = state.get_or_insert((epoch, 0));
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
}

impl LeaseStore for VolatileLeaseStore {
    fn lease_block(
        &self,
        origin: u16,
        epoch: u64,
        floor: u64,
        span: u64,
    ) -> BoxFuture<'_, io::Result<BlockLease>> {
        future::ready(self.lease_block_sync(origin, epoch, floor, span)).boxed()
    }

    fn last_lease_end(&self, origin: u16) -> BoxFuture<'_, io::Result<Option<u64>>> {
        future::ready(Ok(crate::profile_lock!(
            C12,
            self.state.entry(&origin)
        )
        .as_ref()
        .map(|(_, end)| *end)))
        .boxed()
    }

    fn reserve_floor(&self, origin: u16, floor: u64) -> BoxFuture<'_, io::Result<u64>> {
        let result = if floor > ID_SEQUENCE_MASK {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "floor {} exceeds the sequence space of origin {}",
                    floor, origin
                ),
            ))
        } else {
            let mut state = crate::profile_lock!(C12, self.state.entry(&origin));
            let entry = state.get_or_insert((0, 0));
            entry.1 = entry.1.max(floor);
            Ok(entry.1)
        };
        future::ready(result).boxed()
    }
}

/// One durable range. Its upper bound never changes while takers hold it.
struct SequenceBlock {
    next: AtomicU64,
    end: u64,
}

impl SequenceBlock {
    fn take(&self, floor: u64) -> Option<u64> {
        self.next.fetch_max(floor, Ordering::Relaxed);
        let value = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        (value <= self.end).then_some(value)
    }
}

struct SequenceLane {
    block: arc_swap::ArcSwap<SequenceBlock>,
    refill: Mutex<()>,
}

/// Independent block leases for one origin, partitioned by caller task.
/// A task keeps its lane across worker migration, so its IDs stay increasing.
pub struct IdAllocator {
    origin: u16,
    epoch: AtomicU64,
    floor: AtomicU64,
    lanes: [SequenceLane; 64],
    store: Arc<dyn LeaseStore>,
}

impl IdAllocator {
    /// Restores an allocator for `origin` at `epoch`, resuming strictly
    /// above the store's durable lease end (the recovery floor rule:
    /// the floor comes from the durable record, never from scanning
    /// stored cells). `scanned_floor` is the belt-and-suspenders bound
    /// from the recovery scan; the effective floor is the max of both.
    pub async fn recover(
        store: Arc<dyn LeaseStore>,
        origin: u16,
        epoch: u64,
        scanned_floor: u64,
    ) -> io::Result<Self> {
        assert_eq!(origin as u64 & !ID_ORIGIN_MASK, 0, "origin exceeds field");
        let durable_floor = store.last_lease_end(origin).await?.unwrap_or(0);
        let floor = durable_floor.max(scanned_floor);
        let allocator = Self {
            origin,
            epoch: AtomicU64::new(epoch),
            floor: AtomicU64::new(floor),
            lanes: std::array::from_fn(|_| SequenceLane {
                block: arc_swap::ArcSwap::from_pointee(SequenceBlock {
                    next: AtomicU64::new(floor),
                    end: 0,
                }),
                refill: Mutex::new(()),
            }),
            store,
        };
        Ok(allocator)
    }

    pub fn origin(&self) -> u16 {
        self.origin
    }

    /// Guarantee that no sequence at or below `floor` is issued from here on.
    ///
    /// Persist the floor before publishing it. Every lane checks this bound
    /// before it returns an ID. Existing blocks below the floor expire.
    pub async fn raise_floor(&self, floor: u64) -> io::Result<()> {
        self.store.reserve_floor(self.origin, floor).await?;
        self.floor.fetch_max(floor, Ordering::AcqRel);
        Ok(())
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
    pub async fn take_sequence(&self) -> io::Result<u64> {
        use std::hash::{Hash, Hasher};
        let mut hash = std::hash::DefaultHasher::new();
        if let Some(task) = tokio::task::try_id() {
            task.hash(&mut hash);
        } else {
            std::thread::current().id().hash(&mut hash);
        }
        self.take_sequence_in_lane(hash.finish() as usize % self.lanes.len())
            .await
    }

    async fn take_sequence_in_lane(&self, lane: usize) -> io::Result<u64> {
        loop {
            let block = self.lanes[lane].block.load();
            if let Some(value) = block.take(self.floor.load(Ordering::Acquire)) {
                if value > self.floor.load(Ordering::Acquire) {
                    return Ok(value);
                }
                continue;
            }
            drop(block);
            self.refill(lane).await?;
        }
    }

    /// Allocates an id with locality copied from `anchor`
    /// (`Id::new_with` semantics).
    pub async fn take_near(&self, anchor: &Id) -> io::Result<Id> {
        let sequence = self.take_sequence().await?;
        Ok(Id::allocated_near(anchor, self.origin, sequence))
    }

    /// Allocates an id at an explicit locality (placement affinity
    /// chosen by the caller, e.g. a partitioner key).
    pub async fn take_at(&self, locality: u16) -> io::Result<Id> {
        let sequence = self.take_sequence().await?;
        Ok(Id::allocated(locality, self.origin, sequence))
    }

    /// Allocates an id with mixer-derived uniform locality.
    pub async fn take_uniform(&self) -> io::Result<Id> {
        let sequence = self.take_sequence().await?;
        Ok(compose_uniform(self.origin, sequence))
    }

    async fn refill(&self, lane: usize) -> io::Result<()> {
        let lane = &self.lanes[lane];
        let _serialized = crate::profile_lock!(H40, lane.refill.lock().await);
        let current = lane.block.load();
        let floor = self.floor.load(Ordering::Acquire);
        let next = current.next.load(Ordering::Relaxed).max(floor);
        if next < current.end {
            return Ok(());
        }
        let lease = self
            .store
            .lease_block(
                self.origin,
                self.epoch.load(Ordering::Acquire),
                next,
                ID_BLOCK_SPAN,
            )
            .await?;
        // A reader retains one immutable range. It cannot pair an overshoot
        // from an old range with the upper bound of a later lease.
        lane.block.store(Arc::new(SequenceBlock {
            next: AtomicU64::new(lease.start),
            end: lease.end,
        }));
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

/// Consensus-backed lease store: bridges the allocator to the
/// per-database `IdAllocSM` raft authority. One consensus round per
/// block lease; queries read the replicated state.
pub struct RaftLeaseStore {
    sm: Arc<crate::ram::id_alloc_sm::client::SMClient>,
}

impl RaftLeaseStore {
    pub fn new(sm: Arc<crate::ram::id_alloc_sm::client::SMClient>) -> Self {
        Self { sm }
    }
}

fn sm_err(context: &str, error: impl std::fmt::Debug) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("{}: {:?}", context, error))
}

impl LeaseStore for RaftLeaseStore {
    fn lease_block(
        &self,
        origin: u16,
        epoch: u64,
        floor: u64,
        span: u64,
    ) -> BoxFuture<'_, io::Result<BlockLease>> {
        async move {
            let granted = self
                .sm
                .lease_block(&origin, &epoch, &floor, &span)
                .await
                .map_err(|e| sm_err("lease_block consensus round failed", e))?
                .map_err(|e| sm_err("lease refused", e))?;
            Ok(BlockLease {
                origin: granted.origin,
                epoch: granted.epoch,
                start: granted.start,
                end: granted.end,
            })
        }
        .boxed()
    }

    fn last_lease_end(&self, origin: u16) -> BoxFuture<'_, io::Result<Option<u64>>> {
        async move {
            self.sm
                .last_lease_end(&origin)
                .await
                .map_err(|e| sm_err("last_lease_end query failed", e))
        }
        .boxed()
    }

    fn reserve_floor(&self, origin: u16, floor: u64) -> BoxFuture<'_, io::Result<u64>> {
        async move {
            self.sm
                .reserve_floor(&origin, &floor)
                .await
                .map_err(|e| sm_err("reserve_floor consensus round failed", e))?
                .map_err(|e| sm_err("reserve_floor refused", e))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    async fn allocator(origin: u16) -> IdAllocator {
        IdAllocator::recover(Arc::new(VolatileLeaseStore::default()), origin, 1, 0)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_block_refill_does_not_gate_another_sequence_lane() {
        let alloc = allocator(12).await;
        let held = alloc.lanes[0].refill.lock().await;
        let value = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            alloc.take_sequence_in_lane(1),
        )
        .await
        .expect("another block can refill independently")
        .unwrap();
        drop(held);
        let next = alloc.take_sequence_in_lane(0).await.unwrap();
        assert_ne!(value, next);
        let old = alloc.lanes[1].block.load_full();
        old.next.store(old.end, Ordering::Relaxed);
        let later = alloc.take_sequence_in_lane(1).await.unwrap();
        assert!(later > old.end);
        assert!(
            old.take(0).is_none(),
            "an old cursor cannot enter another lane's lease"
        );
        alloc.raise_floor(later + ID_BLOCK_SPAN).await.unwrap();
        for lane in 0..64 {
            assert!(alloc.take_sequence_in_lane(lane).await.unwrap() > later + ID_BLOCK_SPAN);
        }
    }

    #[test]
    fn volatile_lease_origins_do_not_share_one_gate() {
        let store = Arc::new(VolatileLeaseStore::default());
        let first = 1u16;
        let second = 2u16;
        let held = store.state.entry(&first);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = store.clone();
        let thread = std::thread::spawn(move || {
            let lease = worker.lease_block_sync(second, 1, 0, 100).unwrap();
            assert_eq!(lease.end, 100);
            assert!(worker.lease_block_sync(second, 0, 0, 100).is_err());
            done_tx.send(()).unwrap();
        });
        let result = done_rx.recv_timeout(std::time::Duration::from_secs(2));
        drop(held);
        thread.join().unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn sequences_strictly_increase_across_refills() {
        let alloc = allocator(1).await;
        let mut prev = 0;
        for _ in 0..(ID_BLOCK_SPAN * 2 + 10) {
            let seq = alloc.take_sequence().await.unwrap();
            assert!(seq > prev, "sequences must strictly increase");
            prev = seq;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_takers_never_collide_or_regress() {
        let alloc = Arc::new(allocator(2).await);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let alloc = alloc.clone();
            handles.push(tokio::spawn(async move {
                let mut values = Vec::with_capacity(4096);
                let mut prev = 0;
                for n in 0..4096 {
                    if n % 128 == 0 {
                        tokio::task::yield_now().await;
                    }
                    let v = alloc.take_sequence().await.unwrap();
                    assert!(v > prev, "per-task sequences must increase");
                    prev = v;
                    values.push(v);
                }
                values
            }));
        }
        let mut all: Vec<u64> = Vec::new();
        for h in handles {
            all.extend(h.await.unwrap());
        }
        let count = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), count, "no two takers may receive the same value");
    }

    #[tokio::test]
    async fn recovery_resumes_above_durable_lease_end() {
        let store = Arc::new(VolatileLeaseStore::default());
        let first = IdAllocator::recover(store.clone(), 3, 1, 0).await.unwrap();
        let mut last = 0;
        for _ in 0..10 {
            last = first.take_sequence().await.unwrap();
        }
        drop(first);
        // Simulated restart: the block tail is abandoned; the new
        // allocator resumes above the durable end, never reissuing.
        let second = IdAllocator::recover(store, 3, 2, 0).await.unwrap();
        let resumed = second.take_sequence().await.unwrap();
        assert!(resumed > last);
        assert!(
            resumed > ID_BLOCK_SPAN,
            "must resume above the leased block"
        );
    }

    #[tokio::test]
    async fn scanned_floor_raises_recovery_floor() {
        let store = Arc::new(VolatileLeaseStore::default());
        let alloc = IdAllocator::recover(store, 4, 1, 5_000_000).await.unwrap();
        assert!(alloc.take_sequence().await.unwrap() > 5_000_000);
    }

    #[tokio::test]
    async fn stale_epoch_is_fenced_at_lease_time() {
        let store = Arc::new(VolatileLeaseStore::default());
        let newer = IdAllocator::recover(store.clone(), 5, 7, 0).await.unwrap();
        newer.take_sequence().await.unwrap(); // records epoch 7 in the store
        let zombie = IdAllocator::recover(store, 5, 3, 0).await.unwrap();
        // The zombie holds no warm block, so its first take must lease —
        // and the store rejects its stale epoch.
        assert!(zombie.take_sequence().await.is_err());
    }

    #[tokio::test]
    async fn affinity_and_uniform_composition() {
        let alloc = allocator(6).await;
        let anchor = Id::allocated(0x1234, 9, 42);
        let near = alloc.take_near(&anchor).await.unwrap();
        assert_eq!(near.locality(), anchor.locality());
        assert_eq!(near.origin(), 6);

        let mut buckets = HashSet::new();
        for _ in 0..4096 {
            buckets.insert(alloc.take_uniform().await.unwrap().locality());
        }
        // 4096 draws over 32k buckets: expect wide spread, no gross
        // clumping (chi-squared level checks live in the design's test
        // plan; this is the smoke bound).
        assert!(
            buckets.len() > 3000,
            "uniform locality clumped: {}",
            buckets.len()
        );
    }

    #[tokio::test]
    async fn raise_floor_pushes_issuance_past_the_floor() {
        let alloc = allocator(7).await;
        let before = alloc.take_sequence().await.unwrap();
        let floor = before + 3 * ID_BLOCK_SPAN + 17;
        alloc.raise_floor(floor).await.unwrap();
        let after = alloc.take_sequence().await.unwrap();
        assert!(
            after > floor,
            "{after} must exceed the raised floor {floor}"
        );
    }

    #[tokio::test]
    async fn raise_floor_is_durable_across_a_restart() {
        let store = Arc::new(VolatileLeaseStore::default());
        let first = IdAllocator::recover(store.clone(), 8, 1, 0).await.unwrap();
        first.raise_floor(9_000_000).await.unwrap();
        drop(first);
        let second = IdAllocator::recover(store, 8, 2, 0).await.unwrap();
        assert!(second.take_sequence().await.unwrap() > 9_000_000);
    }

    #[tokio::test]
    async fn raise_floor_never_lowers_the_cursor() {
        let alloc = allocator(9).await;
        for _ in 0..100 {
            alloc.take_sequence().await.unwrap();
        }
        alloc.raise_floor(5).await.unwrap();
        assert!(alloc.take_sequence().await.unwrap() > 100);
    }

    #[tokio::test]
    async fn reserve_floor_on_a_foreign_origin_binds_a_later_holder() {
        let store = Arc::new(VolatileLeaseStore::default());
        store.reserve_floor(11, 4_000_000).await.unwrap();
        let later = IdAllocator::recover(store, 11, 1, 0).await.unwrap();
        assert!(later.take_sequence().await.unwrap() > 4_000_000);
    }
}
