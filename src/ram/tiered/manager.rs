use crate::ram::chunk::{Chunk, Chunks};
use crate::ram::segs::{Segment, SEGMENT_SIZE};
use crate::ram::tiered::clock::ClockEvictionPolicy;
use crate::ram::tiered::eviction::evict_segment;
use crate::ram::tiered::promotion::promote_segment;
use crate::ram::tiered::SharedMemoryPool;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

/// Whether an eviction request should be throttled by the background pacing
/// guards, or run immediately because a caller asked for it directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pacing {
    /// Skip if another pass is already running. For allocation, where eviction
    /// is advisory -- the allocation proceeds either way.
    Background,
    /// Wait for the in-flight pass, then run. For promotion, which cannot
    /// proceed until room actually exists: skipping lets hot memory grow past
    /// the limit (observed: 618GB against a 400GB limit, then OOM-killed).
    Blocking,
    /// Run immediately, unpaced. For callers that request eviction directly and
    /// assert on the result.
    Immediate,
}

/// How many global eviction passes may run concurrently. Bounds the stampede
/// without serialising every reader behind a single lock.
const EVICTION_SHARDS: usize = 16;

/// How long to skip global eviction after a pass that freed nothing.
const EVICTION_BACKOFF_MS: u64 = 50;

/// Minimum spacing between "eviction could not free anything" warnings.
const STALL_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// Releases the global-eviction latch on drop, including on early return or
/// panic, so a failed pass cannot wedge eviction off permanently.
struct EvictionFlight<'a>(&'a AtomicBool);

impl<'a> EvictionFlight<'a> {
    fn try_acquire(flag: &'a AtomicBool) -> Option<Self> {
        flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| EvictionFlight(flag))
    }
}

impl Drop for EvictionFlight<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct ChunkTierState {
    last_known_count: AtomicUsize,
    clock_policy: ClockEvictionPolicy,
    last_full_scan: parking_lot::Mutex<Instant>,
}

impl ChunkTierState {
    fn new(promotion_cooldown_ms: u64) -> Self {
        Self {
            last_known_count: AtomicUsize::new(0),
            clock_policy: ClockEvictionPolicy::new(promotion_cooldown_ms),
            last_full_scan: parking_lot::Mutex::new(Instant::now() - Duration::from_secs(100)),
        }
    }
}

/// Manages tiered memory globally across registered chunk collections.
///
/// All chunks across all databases share one `SharedMemoryPool` whose stripe
/// counter is indexed by the calling thread's CPU ID — zero cross-chunk
/// contention on the hot path, no per-chunk registration required.
pub struct TieredMemoryManager {
    /// Shared server-wide memory budget and CPU-striped counter.
    shared_pool: Arc<SharedMemoryPool>,

    /// Registered chunk collections participating in global eviction.
    registered_chunks: parking_lot::RwLock<Vec<Weak<Chunks>>>,

    /// Per-chunk reconciliation and CLOCK state keyed by chunk address.
    chunk_states: lightning::map::PtrHashMap<usize, Arc<ChunkTierState>>,

    /// Round-robin cursor across all registered chunks for global eviction.
    eviction_cursor: AtomicUsize,

    /// Bounds how many global eviction passes run at once.
    ///
    /// A single lock here throttled the whole server: promotion waits for room,
    /// so serialising eviction serialised every reader that touched cold data.
    /// Measured at ~893 evictions/s with 282 of 300 threads parked in
    /// futex_do_wait, 13% CPU and 20% disk -- nothing saturated but the lock.
    ///
    /// Sharding restores parallelism while still capping the stampede. Eviction
    /// re-checks the watermark after every segment, so N concurrent passes
    /// overshoot by at most N segments (a few MB), not by N targets.
    ///
    /// Every allocating thread that crosses the threshold would otherwise run
    /// its own eviction pass concurrently, and each pass only notices the
    /// others once it re-reads the counter — so the pool overshoots the
    /// watermark by roughly the number of threads in flight, and that many
    /// threads contend on eviction I/O to do redundant work. One evictor at a
    /// time is enough; the rest proceed with their allocation, since the
    /// threshold leaves headroom below the hard limit.
    eviction_locks: Vec<parking_lot::Mutex<()>>,

    /// Whether tiered memory is enabled
    enabled: bool,

    /// Whether to disable promotion (for benchmarking cold reads)
    /// When true, cold segments remain cold even when accessed
    pub disable_promotion: AtomicBool,

    /// Monotonic base for the eviction backoff clock.
    started_at: Instant,

    /// Milliseconds since `started_at` until which global eviction is skipped.
    ///
    /// When hot memory sits above the threshold but every candidate is pinned
    /// by an active reference, eviction cannot free anything -- yet the next
    /// allocation immediately retries, so a scan-heavy phase turns into a
    /// continuous stream of failing passes. Backing off briefly lets the
    /// references drain instead of burning CPU and log I/O rediscovering the
    /// same answer thousands of times a second.
    eviction_backoff_until_ms: AtomicU64,

    /// Rate limiter for the "could not free anything" warning: last emission,
    /// and how many attempts have been folded into the next one.
    stall_warn_at: parking_lot::Mutex<Instant>,
    stall_suppressed: AtomicU64,

    /// Metrics counters
    promotion_count: AtomicU64,
    eviction_count: AtomicU64,
    churn_count: AtomicU64,
    lower_watermark_evictions: AtomicU64,
    /// Reads served from a cold segment's backup without promoting it, and the
    /// bytes decompressed to do so. The ratio of those bytes to SEGMENT_SIZE is
    /// what tells us when promoting would have been the cheaper choice.
    cold_block_reads: AtomicU64,
    /// Cold segments whose faulted-in blocks were handed back under pressure.
    cold_blocks_reclaimed: AtomicU64,
    /// Promotions triggered because a segment had been faulted in piecemeal
    /// past the residency threshold.
    /// Bytes held by blocks faulted into cold segments to serve reads without
    /// promoting them.
    ///
    /// A cold segment adds nothing to the hot-segment counter, yet a partially
    /// resident one holds real memory, so this has to enter the pressure
    /// calculation or the limit would bound whole segments while partial
    /// residency grew underneath it. Owned by the manager rather than a global,
    /// so one server's residency cannot inflate another's threshold.
    cold_resident_bytes: AtomicUsize,
    /// Promotions refused because the hot tier was already at the hard limit.
    /// A rising count means reads are being served from cold to hold the limit,
    /// which is the intended trade rather than a fault.
    promotions_declined: AtomicU64,
}

impl TieredMemoryManager {
    /// Create a new shared tiered memory manager.
    pub fn new(shared_pool: Arc<SharedMemoryPool>) -> Self {
        TieredMemoryManager {
            shared_pool,
            registered_chunks: parking_lot::RwLock::new(Vec::new()),
            chunk_states: lightning::map::PtrHashMap::with_capacity(64),
            eviction_cursor: AtomicUsize::new(0),
            eviction_locks: (0..EVICTION_SHARDS)
                .map(|_| parking_lot::Mutex::new(()))
                .collect(),
            started_at: Instant::now(),
            eviction_backoff_until_ms: AtomicU64::new(0),
            stall_warn_at: parking_lot::Mutex::new(Instant::now() - STALL_WARN_INTERVAL),
            stall_suppressed: AtomicU64::new(0),
            enabled: true,
            disable_promotion: AtomicBool::new(false),
            promotion_count: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
            churn_count: AtomicU64::new(0),
            lower_watermark_evictions: AtomicU64::new(0),
            cold_block_reads: AtomicU64::new(0),
            cold_blocks_reclaimed: AtomicU64::new(0),
            cold_resident_bytes: AtomicUsize::new(0),
            promotions_declined: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn threshold_limit(&self) -> usize {
        self.shared_pool.threshold_limit()
    }

    #[inline]
    fn lower_watermark_limit(&self) -> usize {
        self.shared_pool.lower_watermark_limit()
    }

    /// Memory attributable to `hot_segments_count` whole hot segments plus the
    /// blocks faulted into cold segments.
    ///
    /// Both are resident, so both must count against the limit. Counting only
    /// whole segments would let partial residency grow underneath the budget.
    #[inline]
    fn hot_memory_bytes(&self, hot_segments_count: usize) -> usize {
        hot_segments_count
            .checked_mul(SEGMENT_SIZE)
            .map(|b| b.saturating_add(self.cold_resident_bytes.load(Ordering::Relaxed)))
            .unwrap_or_else(|| self.shared_pool.physical_memory_limit * 2)
    }

    #[inline]
    fn allocation_eviction_target(&self, hot_segments_count: usize) -> Option<usize> {
        let current_hot_memory = self.hot_memory_bytes(hot_segments_count);
        let after_alloc_memory =
            current_hot_memory
                .checked_add(SEGMENT_SIZE)
                .unwrap_or_else(|| {
                    warn!("Overflow calculating after-alloc memory");
                    self.shared_pool.physical_memory_limit * 2
                });

        let threshold_limit = self.threshold_limit();
        if after_alloc_memory > threshold_limit {
            let lower_limit = self.lower_watermark_limit();
            let target_bytes = after_alloc_memory.saturating_sub(lower_limit);
            Some((target_bytes + SEGMENT_SIZE - 1) / SEGMENT_SIZE)
        } else {
            None
        }
    }

    #[inline]
    fn cap_eviction_target_to_counts(
        &self,
        chunk_hot_segments: usize,
        head_is_hot: bool,
        requested: usize,
    ) -> usize {
        let evictable_upper_bound = chunk_hot_segments.saturating_sub(usize::from(head_is_hot));
        requested.min(evictable_upper_bound)
    }

    #[inline]
    fn chunk_key(chunk: &Chunk) -> usize {
        chunk as *const Chunk as usize
    }

    fn ensure_chunk_state(&self, chunk: &Chunk) -> Arc<ChunkTierState> {
        let key = Self::chunk_key(chunk);
        lightning::map::Map::get_or_insert(&self.chunk_states, key, || {
            Arc::new(ChunkTierState::new(self.shared_pool.promotion_cooldown_ms))
        })
    }

    pub fn register_chunks(&self, chunks: &Arc<Chunks>) {
        {
            let mut registered = self.registered_chunks.write();
            let already_registered = registered.iter().any(|weak| {
                weak.upgrade()
                    .map(|existing| Arc::ptr_eq(&existing, chunks))
                    .unwrap_or(false)
            });
            if !already_registered {
                registered.push(Arc::downgrade(chunks));
            }
        }

        for chunk in &chunks.list {
            self.ensure_chunk_state(chunk);
        }
    }

    pub fn unregister_chunks(&self, chunks: &Arc<Chunks>) {
        let removed_hot_segments: usize = chunks
            .list
            .iter()
            .map(|chunk| self.count_hot_segments(chunk))
            .sum();

        {
            let mut registered = self.registered_chunks.write();
            registered.retain(|weak| {
                weak.upgrade()
                    .map(|existing| !Arc::ptr_eq(&existing, chunks))
                    .unwrap_or(false)
            });
        }

        for key in chunks.list.iter().map(Self::chunk_key) {
            lightning::map::Map::remove(&self.chunk_states, &key);
        }

        self.decrement_hot_count_by(removed_hot_segments);
    }

    fn prune_chunk_states(&self, live_sets: &[Arc<Chunks>]) {
        let live_keys: std::collections::HashSet<usize> = live_sets
            .iter()
            .flat_map(|chunks| chunks.list.iter().map(Self::chunk_key))
            .collect();
        for (key, _) in lightning::map::Map::entries(&self.chunk_states) {
            if !live_keys.contains(&key) {
                lightning::map::Map::remove(&self.chunk_states, &key);
            }
        }
    }

    fn collect_registered_chunk_sets(&self) -> Vec<Arc<Chunks>> {
        let mut registered = self.registered_chunks.write();
        let mut live = Vec::with_capacity(registered.len());
        registered.retain(|weak| {
            if let Some(chunks) = weak.upgrade() {
                live.push(chunks);
                true
            } else {
                false
            }
        });
        drop(registered);
        self.prune_chunk_states(&live);
        live
    }

    fn reconcile_chunk_with_mode(&self, chunk: &Chunk, force_full_scan: bool) {
        let state = self.ensure_chunk_state(chunk);
        let should_full_scan = if force_full_scan {
            if let Some(mut last_scan) = state.last_full_scan.try_lock() {
                *last_scan = Instant::now();
                true
            } else {
                false
            }
        } else if let Some(mut last_scan) = state.last_full_scan.try_lock() {
            if last_scan.elapsed() >= Duration::from_secs(10) {
                *last_scan = Instant::now();
                true
            } else {
                false
            }
        } else {
            false
        };

        if should_full_scan {
            let actual = self.count_hot_segments(chunk);
            let prev = state.last_known_count.swap(actual, Ordering::Relaxed);
            let delta = actual as isize - prev as isize;
            if delta != 0 {
                self.shared_pool.adjust_delta(delta);
                trace!(
                    "Full scan reconciled: prev={}, actual={}, delta={}",
                    prev,
                    actual,
                    delta
                );
            }
        } else {
            let known = state.last_known_count.load(Ordering::Relaxed);
            let total_segments = chunk.segments().len();
            if known > total_segments {
                let actual = self.count_hot_segments(chunk);
                warn!(
                    "last_known_count out of range: known={}, total_segments={}, recalculated={}",
                    known, total_segments, actual
                );
                let prev = state.last_known_count.swap(actual, Ordering::Relaxed);
                self.shared_pool
                    .adjust_delta(actual as isize - prev as isize);
            }
        }
    }

    fn reconcile_chunk(&self, chunk: &Chunk) {
        self.reconcile_chunk_with_mode(chunk, false);
    }

    fn total_hot_segments_cached(&self) -> usize {
        let registered = self.collect_registered_chunk_sets();
        for chunk_set in &registered {
            for chunk in &chunk_set.list {
                self.reconcile_chunk(chunk);
            }
        }
        self.shared_pool.total_hot_segments()
    }

    fn force_reconcile_all_chunks(&self) -> usize {
        let registered = self.collect_registered_chunk_sets();
        let mut scanned_total = 0usize;
        for chunk_set in &registered {
            for chunk in &chunk_set.list {
                let state = self.ensure_chunk_state(chunk);
                let actual = self.count_hot_segments(chunk);
                state.last_known_count.store(actual, Ordering::Relaxed);
                if let Some(mut last_scan) = state.last_full_scan.try_lock() {
                    *last_scan = Instant::now();
                }
                scanned_total = scanned_total.saturating_add(actual);
            }
        }
        let shared_total = self.shared_pool.total_hot_segments();
        let delta = scanned_total as isize - shared_total as isize;
        self.shared_pool.adjust_delta(delta);
        scanned_total
    }

    fn all_registered_chunks<'a>(&self, sets: &'a [Arc<Chunks>]) -> Vec<&'a Chunk> {
        let mut chunks = Vec::new();
        for chunk_set in sets {
            for chunk in &chunk_set.list {
                chunks.push(chunk);
            }
        }
        chunks
    }

    /// Check if eviction is needed and evict segments if necessary (legacy/test-only)
    ///
    /// NOTE: This method is kept for tests/benchmarks compatibility.
    /// Production code should rely on passive global eviction via `evict_for_allocation()`.
    ///
    /// Returns the number of segments evicted
    pub fn check_and_evict(&self, chunk: &Chunk) -> Result<usize, io::Error> {
        let _ = chunk;
        // Immediate: a caller invoking this directly expects the pass to run and
        // then asserts on what it returned. Background pacing would make the
        // result depend on whether another pass happened to be in flight.
        self.evict_for_allocation_paced(Pacing::Immediate)
    }

    /// Explicitly evict a specific number of segments from one chunk (test-only)
    ///
    /// NOTE: This method is kept for tests compatibility.
    /// Production code should rely on passive global eviction via `evict_for_allocation()`.
    ///
    /// Returns the number of segments successfully evicted
    pub fn explicit_evict(&self, chunk: &Chunk, num_segments: usize) -> Result<usize, io::Error> {
        if !self.enabled {
            return Ok(0);
        }

        debug!("Explicit eviction requested for {} segments", num_segments);
        let evicted = self.evict_until_target(chunk, num_segments)?;
        self.decrement_hot_count_by(evicted);
        Self::retreat_known_count(&self.ensure_chunk_state(chunk), evicted);
        Ok(evicted)
    }

    /// Evict segments until we've evicted the target number from one chunk.
    ///
    /// Returns the number of segments successfully evicted.
    fn evict_until_target(&self, chunk: &Chunk, target: usize) -> Result<usize, io::Error> {
        let mut evicted_count = 0;
        let mut attempts_without_progress = 0;
        const MAX_ATTEMPTS_WITHOUT_PROGRESS: usize = 3;

        for attempt in 0..target.saturating_mul(2) {
            if evicted_count >= target {
                break;
            }

            let state = self.ensure_chunk_state(chunk);
            match state.clock_policy.select_victim(chunk) {
                Some(victim) => match evict_segment(&victim, chunk) {
                    Ok(()) => {
                        evicted_count += 1;
                        self.eviction_count.fetch_add(1, Ordering::Relaxed);
                        attempts_without_progress = 0;
                        debug!(
                            "Evicted segment {} from chunk {} ({}/{}, attempt {})",
                            victim.id,
                            chunk.id,
                            evicted_count,
                            target,
                            attempt + 1
                        );
                    }
                    Err(e) => {
                        warn!("Failed to evict segment {}: {}", victim.id, e);
                        attempts_without_progress += 1;
                    }
                },
                None => {
                    attempts_without_progress += 1;
                    debug!(
                        "CLOCK could not find victim in chunk {} (attempt {}), evicted {}/{} segments",
                        chunk.id,
                        attempt + 1,
                        evicted_count,
                        target
                    );
                }
            }

            if attempts_without_progress >= MAX_ATTEMPTS_WITHOUT_PROGRESS {
                warn!(
                    "Eviction stalled after {} attempts without progress, evicted {}/{} segments",
                    MAX_ATTEMPTS_WITHOUT_PROGRESS, evicted_count, target
                );
                break;
            }

            if attempts_without_progress > 0 && evicted_count < target {
                std::thread::yield_now();
            }
        }

        if evicted_count > 0 {
            debug!("Evicted {} segments to cold storage", evicted_count);
        } else if target > 0 {
            warn!(
                "Failed to evict any segments (target was {}). Hot segments: {}, all may be protected or have active references",
                target,
                chunk.segments().iter().filter(|s| s.is_hot()).count()
            );
        }

        Ok(evicted_count)
    }

    fn evict_globally_until_target(&self, target: usize) -> Result<usize, io::Error> {
        self.evict_globally_until_target_paced(target, Pacing::Background)
    }

    /// Global eviction with explicit control over pacing.
    ///
    /// `Pacing::Background` applies the single-flight latch and the no-progress
    /// backoff. Both guards live here, at the single choke point every caller
    /// funnels through, rather than at the allocation entry point: promotion
    /// reaches eviction via `evict_down_to_lower`, so guarding only allocation
    /// left the promotion path unserialised, and those passes stampeded exactly
    /// as the allocation path used to.
    ///
    /// `Pacing::Immediate` skips both. Callers that ask for eviction explicitly
    /// and then assert on the result need it to actually run -- pacing is for
    /// throttling the automatic paths, not for deciding whether a direct
    /// request is honoured.
    fn evict_globally_until_target_paced(
        &self,
        target: usize,
        pacing: Pacing,
    ) -> Result<usize, io::Error> {
        // Spread passes across shards so concurrent evictors rarely collide.
        let shard = self.eviction_cursor.load(Ordering::Relaxed) % EVICTION_SHARDS;
        let _flight = match pacing {
            Pacing::Background => {
                if self.in_eviction_backoff() {
                    return Ok(0);
                }
                // Someone is already evicting toward the watermark; joining in
                // only overshoots it and duplicates the I/O. Allocation does not
                // depend on the outcome, so skipping is safe here.
                match self.eviction_locks[shard].try_lock() {
                    Some(guard) => Some(guard),
                    None => return Ok(0),
                }
            }
            // Serialised like Background, but the caller waits instead of
            // proceeding without room. Concurrent evictors would stampede past
            // the watermark; skipping would let the caller promote anyway and
            // blow the limit. Waiting gives backpressure, which is the correct
            // response to memory pressure.
            Pacing::Blocking => Some(self.eviction_locks[shard].lock()),
            Pacing::Immediate => None,
        };

        let registered = self.collect_registered_chunk_sets();
        let chunks = self.all_registered_chunks(&registered);
        if chunks.is_empty() || target == 0 {
            return Ok(0);
        }

        // Give back cold residency first. Those blocks count against the same
        // limit as hot segments, but they are pure cache -- backed by a file
        // that is already written -- so reclaiming them costs a re-read rather
        // than an archive write. Evicting a hot segment to make room while a
        // cold segment sits on faulted-in blocks pays a write to free memory
        // that was free for the asking.
        //
        // This is also the only thing that bounds cold residency. Nothing else
        // returns it: a cold segment that is never freed holds its blocks
        // forever, and an import drove resident set to 91GB against a 40GB
        // limit before this existed.
        let reclaimed = self.reclaim_cold_residency(&chunks, target.saturating_mul(SEGMENT_SIZE));
        if reclaimed >= target.saturating_mul(SEGMENT_SIZE) {
            return Ok(0);
        }

        let mut evicted_count = 0;
        let mut attempts_without_progress = 0;
        const MAX_ATTEMPTS_WITHOUT_PROGRESS: usize = 3;

        // Every allocating thread sizes its own target from the hot count it
        // observed before evicting anything. Threads that cross the threshold
        // together therefore each carry a full target, and if each one runs its
        // target to completion they collectively evict N times what was needed:
        // hot memory lands at a fraction of the watermark, the segments fault
        // straight back in, and the cycle repeats.
        //
        // Re-reading the shared counter each round makes concurrent evictors
        // cooperate instead of compound -- whoever gets there first drives the
        // pool to the watermark and the rest observe it and stop.
        let watermark_segments = self.lower_watermark_limit() / SEGMENT_SIZE;

        while evicted_count < target {
            if self.shared_pool.total_hot_segments() <= watermark_segments {
                debug!(
                    "Global eviction stopping at watermark: {} hot segments <= {} watermark \
                     (evicted {}/{} of this thread's target)",
                    self.shared_pool.total_hot_segments(),
                    watermark_segments,
                    evicted_count,
                    target
                );
                break;
            }

            let start = self.eviction_cursor.fetch_add(1, Ordering::Relaxed) % chunks.len();
            let mut made_progress = false;

            for i in 0..chunks.len() {
                let chunk = chunks[(start + i) % chunks.len()];
                let state = self.ensure_chunk_state(chunk);
                let Some(victim) = state.clock_policy.select_victim(chunk) else {
                    continue;
                };

                match evict_segment(&victim, chunk) {
                    Ok(()) => {
                        self.shared_pool.decrement_by(1);
                        Self::retreat_known_count(&state, 1);
                        self.eviction_count.fetch_add(1, Ordering::Relaxed);
                        evicted_count += 1;
                        attempts_without_progress = 0;
                        made_progress = true;
                        debug!(
                            "Globally evicted segment {} from chunk {} ({}/{})",
                            victim.id, chunk.id, evicted_count, target
                        );
                        if evicted_count >= target {
                            break;
                        }
                    }
                    Err(e) => {
                        // Usually just "active references" -- expected and
                        // transient, so it must not log per occurrence.
                        debug!(
                            "Failed to evict segment {} from chunk {}: {}",
                            victim.id, chunk.id, e
                        );
                    }
                }
            }

            if !made_progress {
                attempts_without_progress += 1;
                if attempts_without_progress >= MAX_ATTEMPTS_WITHOUT_PROGRESS {
                    debug!(
                        "Global eviction stalled after {} attempts without progress, evicted {}/{} segments",
                        MAX_ATTEMPTS_WITHOUT_PROGRESS, evicted_count, target
                    );
                    break;
                }
                std::thread::yield_now();
            }
        }

        if evicted_count == 0 && target > 0 {
            self.note_eviction_made_no_progress();
        }

        Ok(evicted_count)
    }

    /// Reclaim faulted-in blocks from cold segments until `wanted` bytes have
    /// been given back, returning how many bytes that was.
    ///
    /// Sweeps from a rotating cursor so concurrent callers start at different
    /// points instead of contending on the same segments, and abandons any
    /// segment that is referenced rather than waiting for it -- waiting is what
    /// deadlocked promotion, and a sweep always has somewhere else to go.
    fn reclaim_cold_residency(&self, chunks: &[&Chunk], wanted: usize) -> usize {
        if wanted == 0 || self.cold_resident_bytes.load(Ordering::Relaxed) == 0 {
            return 0;
        }

        let mut freed = 0usize;
        let start = self.eviction_cursor.fetch_add(1, Ordering::Relaxed);

        for i in 0..chunks.len() {
            let chunk = chunks[(start + i) % chunks.len()];
            for segment in chunk.segments() {
                if let Some(bytes) = segment.try_reclaim_resident_blocks() {
                    self.release_cold_resident(bytes);
                    self.cold_blocks_reclaimed.fetch_add(1, Ordering::Relaxed);
                    freed = freed.saturating_add(bytes);
                    if freed >= wanted {
                        return freed;
                    }
                }
            }
        }
        freed
    }

    /// Check if allocating a new segment would exceed the threshold and evict globally if needed.
    ///
    /// Eviction is triggered when hot memory would exceed `physical_memory_limit * threshold`.
    /// For example, with 512GB limit and 0.8 threshold, eviction starts at ~410GB.
    ///
    /// Returns the number of segments evicted.
    pub fn evict_for_allocation(&self) -> Result<usize, io::Error> {
        self.evict_for_allocation_paced(Pacing::Background)
    }

    fn evict_for_allocation_paced(&self, pacing: Pacing) -> Result<usize, io::Error> {
        if !self.enabled {
            return Ok(0);
        }

        let hot_segments_count = self.total_hot_segments_cached();
        let max_reasonable_segments = usize::MAX / SEGMENT_SIZE;
        let hot_segments_count = hot_segments_count.min(max_reasonable_segments);
        if let Some(segments_to_evict) = self.allocation_eviction_target(hot_segments_count) {
            let current_hot_memory = self.hot_memory_bytes(hot_segments_count);
            let after_alloc_memory = current_hot_memory
                .checked_add(SEGMENT_SIZE)
                .unwrap_or_else(|| self.shared_pool.physical_memory_limit * 2);
            let threshold_limit = self.threshold_limit();

            debug!(
                "Global eviction before allocation: current {} global hot segments ({} MB), would be {} MB after allocation, threshold {} MB ({}% of {} MB), evicting {} segments",
                hot_segments_count,
                current_hot_memory / (1024 * 1024),
                after_alloc_memory / (1024 * 1024),
                threshold_limit / (1024 * 1024),
                (self.shared_pool.threshold * 100.0) as u32,
                self.shared_pool.physical_memory_limit / (1024 * 1024),
                segments_to_evict
            );

            self.evict_globally_until_target_paced(segments_to_evict, pacing)
        } else {
            Ok(0)
        }
    }

    /// Check if allocating a new segment would exceed the threshold after forcing a
    /// full global reconcile. This is intended for low-frequency background callers
    /// such as the cleaner to avoid acting on a stale shared counter.
    pub fn evict_for_allocation_reconciled(&self) -> Result<usize, io::Error> {
        if !self.enabled {
            return Ok(0);
        }

        let hot_segments_count = self.force_reconcile_all_chunks();
        let max_reasonable_segments = usize::MAX / SEGMENT_SIZE;
        let hot_segments_count = hot_segments_count.min(max_reasonable_segments);
        if let Some(segments_to_evict) = self.allocation_eviction_target(hot_segments_count) {
            let current_hot_memory = self.hot_memory_bytes(hot_segments_count);
            let after_alloc_memory = current_hot_memory
                .checked_add(SEGMENT_SIZE)
                .unwrap_or_else(|| self.shared_pool.physical_memory_limit * 2);
            let threshold_limit = self.threshold_limit();
            let scanned_hot_segments = self.scanned_hot_segments();
            let shared_counter_segments = self.shared_pool.total_hot_segments();

            debug!(
                "Global eviction before allocation after forced reconcile: shared_counter={} scanned={} hot={} MB, would be {} MB after allocation, threshold {} MB, evicting {} segments",
                shared_counter_segments,
                scanned_hot_segments,
                current_hot_memory / (1024 * 1024),
                after_alloc_memory / (1024 * 1024),
                threshold_limit / (1024 * 1024),
                segments_to_evict
            );

            self.evict_globally_until_target(segments_to_evict)
        } else {
            Ok(0)
        }
    }

    /// Promote a cold segment to hot.
    pub fn promote(&self, chunk: &Chunk, segment: &Segment) -> Result<(), io::Error> {
        if !self.enabled {
            return Ok(());
        }

        const MIN_ACCESSES_FOR_PROMOTION: u8 = 2;
        let access_count = segment.increment_access_count();
        if access_count < MIN_ACCESSES_FOR_PROMOTION {
            debug!(
                "Segment {} accessed {} time(s), needs {} before promotion",
                segment.id, access_count, MIN_ACCESSES_FOR_PROMOTION
            );
            return Ok(());
        }

        let hot_segments_count = self.hot_count_cached(chunk);
        let after_promotion_bytes = self.hot_memory_bytes(hot_segments_count.saturating_add(1));
        if after_promotion_bytes > self.threshold_limit() {
            // Promotion must WAIT for room, not skip and not refuse.
            //
            // Refusing is not an option: a cold segment is not readable in
            // place. `CellGuard::from_guard` drops the cell lock, calls promote,
            // and returns None so the caller retries expecting the segment to be
            // hot -- so a declined promotion spins that retry loop forever.
            //
            // Skipping is not an option either: when the eviction latch was held
            // by someone else the promotion proceeded anyway, so hot memory grew
            // unbounded past the configured limit (618GB against 400GB) until
            // the process was OOM-killed.
            //
            // Blocking pacing serialises eviction without letting the caller
            // continue before room exists. Under pressure that shows up as
            // slower reads rather than a dead process.
            self.evict_down_to_lower(hot_segments_count)?;
        }

        let churn_candidate =
            segment.recently_evicted_within(self.shared_pool.promotion_cooldown_ms);

        promote_segment(segment);
        segment.reset_access_count();

        self.increment_hot_count_for(chunk);
        self.promotion_count.fetch_add(1, Ordering::Relaxed);
        if churn_candidate {
            self.churn_count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    fn evict_down_to_lower(&self, current_hot_segments: usize) -> Result<usize, io::Error> {
        let current_bytes = self.hot_memory_bytes(current_hot_segments);
        let lower_limit = self.lower_watermark_limit();
        if current_bytes <= lower_limit {
            return Ok(0);
        }

        let excess_bytes = current_bytes.saturating_sub(lower_limit);
        let target_segments = (excess_bytes + SEGMENT_SIZE - 1) / SEGMENT_SIZE;
        let evicted = self.evict_globally_until_target_paced(target_segments, Pacing::Blocking)?;
        if evicted > 0 {
            self.lower_watermark_evictions
                .fetch_add(evicted as u64, Ordering::Relaxed);
        }
        Ok(evicted)
    }

    /// Return the server-wide hot segment count, reconciling this chunk's
    /// contribution via per-chunk state every 10 seconds to correct drift.
    pub fn hot_count_cached(&self, chunk: &Chunk) -> usize {
        self.reconcile_chunk(chunk);
        self.shared_pool.total_hot_segments()
    }

    /// Return the current shared-pool hot-segment counter without performing any reconciliation.
    pub fn shared_hot_segments(&self) -> usize {
        self.shared_pool.total_hot_segments()
    }

    /// Return the scanned hot-segment count across all registered chunks.
    pub fn scanned_hot_segments(&self) -> usize {
        let registered = self.collect_registered_chunk_sets();
        registered
            .iter()
            .flat_map(|chunk_set| chunk_set.list.iter())
            .map(|chunk| self.count_hot_segments(chunk))
            .sum()
    }

    /// Increment the server-wide hot-segment count for a segment owned by
    /// `chunk`.
    ///
    /// The periodic reconcile is delta-based: it applies
    /// `actual - last_known_count` to the shared counter. That is only correct
    /// while the counter equals the sum of every chunk's `last_known_count`, so
    /// a mutation that moves one without the other makes the next scan re-apply
    /// the same segments. Bumping the counter alone made the first full scan
    /// count every live segment a second time, pinning the counter at 2x
    /// reality -- which in turn made eviction targets exceed the number of
    /// segments that actually existed.
    pub fn increment_hot_count_for(&self, chunk: &Chunk) {
        self.shared_pool.increment();
        self.ensure_chunk_state(chunk)
            .last_known_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the server-wide hot-segment count for a segment owned by
    /// `chunk`, keeping that chunk's reconcile baseline in step.
    pub fn decrement_hot_count_for(&self, chunk: &Chunk) {
        self.shared_pool.decrement_by(1);
        Self::retreat_known_count(&self.ensure_chunk_state(chunk), 1);
    }

    #[inline]
    fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    /// Whether a recent eviction pass freed nothing and the backoff still holds.
    #[inline]
    fn in_eviction_backoff(&self) -> bool {
        self.elapsed_ms() < self.eviction_backoff_until_ms.load(Ordering::Relaxed)
    }

    /// Record that a pass could not free anything: arm the backoff and fold the
    /// event into a rate-limited warning.
    ///
    /// Pinned segments are an ordinary transient condition, so reporting each
    /// occurrence at warn level put this on a hot path -- a single constrained
    /// import emitted 1.6 million lines, around a thousand a second.
    fn note_eviction_made_no_progress(&self) {
        self.eviction_backoff_until_ms.store(
            self.elapsed_ms().saturating_add(EVICTION_BACKOFF_MS),
            Ordering::Relaxed,
        );
        self.stall_suppressed.fetch_add(1, Ordering::Relaxed);
        if let Some(mut last_warn) = self.stall_warn_at.try_lock() {
            if last_warn.elapsed() >= STALL_WARN_INTERVAL {
                *last_warn = Instant::now();
                let folded = self.stall_suppressed.swap(0, Ordering::Relaxed);
                warn!(
                    "Global eviction freed nothing on {} attempt(s) in the last {}s: hot segments \
                     are held by active references. Hot memory stays above the threshold until \
                     they drain.",
                    folded,
                    STALL_WARN_INTERVAL.as_secs()
                );
            }
        }
    }

    /// Lower a chunk's reconcile baseline by `by`, saturating at zero.
    fn retreat_known_count(state: &ChunkTierState, by: usize) {
        let _ = state.last_known_count.try_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(by)),
        );
    }

    /// Decrement the server-wide hot-segment count by `by`, saturating at zero.
    fn decrement_hot_count_by(&self, by: usize) {
        self.shared_pool.decrement_by(by);
    }

    /// Count hot segments in the chunk.
    fn count_hot_segments(&self, chunk: &Chunk) -> usize {
        chunk.segments().iter().filter(|s| s.is_hot()).count()
    }

    /// Count cold segments in the chunk.
    pub fn count_cold_segments(&self, chunk: &Chunk) -> usize {
        chunk.segments().iter().filter(|s| s.is_cold()).count()
    }

    /// Get memory statistics.
    pub fn stats(&self, chunk: &Chunk) -> TieredMemoryStats {
        let segments = chunk.segments();
        let total = segments.len();
        let hot = segments.iter().filter(|s| s.is_hot()).count();
        let cold = segments.iter().filter(|s| s.is_cold()).count();

        TieredMemoryStats {
            total_segments: total,
            hot_segments: hot,
            cold_segments: cold,
            threshold: (chunk.capacity / SEGMENT_SIZE) as f32 * self.shared_pool.threshold,
            promotions: self.promotion_count.load(Ordering::Relaxed),
            evictions: self.eviction_count.load(Ordering::Relaxed),
            churns: self.churn_count.load(Ordering::Relaxed),
            lower_watermark_evictions: self.lower_watermark_evictions.load(Ordering::Relaxed),
        }
    }

    /// Server-wide eviction counters, independent of any single chunk.
    ///
    /// `churns` is the diagnostic one: it counts promotions of a segment that
    /// was evicted within the cooldown, i.e. eviction of data still in use.
    /// Every churn costs a write on the way out and a read on the way back, so
    /// a churn rate near the eviction rate means most eviction I/O is wasted.
    pub fn global_counters(&self) -> TieredGlobalCounters {
        TieredGlobalCounters {
            promotions: self.promotion_count.load(Ordering::Relaxed),
            evictions: self.eviction_count.load(Ordering::Relaxed),
            churns: self.churn_count.load(Ordering::Relaxed),
            lower_watermark_evictions: self.lower_watermark_evictions.load(Ordering::Relaxed),
            promotions_declined: self.promotions_declined.load(Ordering::Relaxed),
            cold_block_reads: self.cold_block_reads.load(Ordering::Relaxed),
            cold_blocks_reclaimed: self.cold_blocks_reclaimed.load(Ordering::Relaxed),
            cold_resident_bytes: self.cold_resident_bytes.load(Ordering::Relaxed) as u64,
        }
    }

    /// Record blocks faulted into a cold segment.
    #[inline]
    pub fn add_cold_resident(&self, bytes: usize) {
        if bytes > 0 {
            self.cold_resident_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Release cold residency, saturating at zero.
    #[inline]
    pub fn release_cold_resident(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let _ = self.cold_resident_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |v| Some(v.saturating_sub(bytes)),
        );
    }

    /// Bytes currently held by partially-resident cold segments.
    #[inline]
    pub fn cold_resident_total(&self) -> usize {
        self.cold_resident_bytes.load(Ordering::Relaxed)
    }
    /// Record a cold read served from a single block.
    ///
    /// This only counts. An earlier version promoted a segment to hot once half
    /// its blocks were resident, on the theory that piecemeal faulting stops
    /// paying once most of the segment is committed. That deadlocked: a block
    /// read hands the caller a live reference to a still-cold segment, and
    /// `promote_segment` spins until every reference drains, so a caller that
    /// held one and then read another cell of the same segment waited on
    /// itself. Promotion is an optimisation and the block path always works, so
    /// the policy was removed rather than made conditional.
    pub fn note_cold_block_read(&self) {
        self.cold_block_reads.fetch_add(1, Ordering::Relaxed);
    }

    /// Access the shared server-wide memory pool.
    pub fn shared_pool(&self) -> &Arc<SharedMemoryPool> {
        &self.shared_pool
    }

    /// Enable or disable tiered memory.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Check if tiered memory is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Server-wide eviction counters, not scoped to a chunk.
#[derive(Debug, Clone, Copy, Default)]
pub struct TieredGlobalCounters {
    pub promotions: u64,
    pub evictions: u64,
    pub churns: u64,
    pub lower_watermark_evictions: u64,
    pub promotions_declined: u64,
    pub cold_block_reads: u64,
    /// Cold segments whose block cache was reclaimed under pressure.
    pub cold_blocks_reclaimed: u64,
    /// Bytes currently faulted into cold segments, which count against the hot
    /// limit just as hot segments do.
    pub cold_resident_bytes: u64,
}

/// Statistics about tiered memory usage
#[derive(Debug, Clone)]
pub struct TieredMemoryStats {
    pub total_segments: usize,
    pub hot_segments: usize,
    pub cold_segments: usize,
    pub threshold: f32,
    pub promotions: u64,
    pub evictions: u64,
    pub churns: u64,
    pub lower_watermark_evictions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::tiered::TieredConfig;

    fn make_pool(threshold: f32, limit: usize) -> Arc<SharedMemoryPool> {
        SharedMemoryPool::new(&TieredConfig {
            threshold,
            lower_watermark: 0.72,
            physical_memory_limit: limit,
            promotion_cooldown_ms: 2000,
        })
    }

    #[test]
    fn test_manager_creation() {
        let limit = 1024 * 1024 * 1024; // 1GB
        let pool = make_pool(0.8, limit);
        let manager = TieredMemoryManager::new(pool.clone());
        assert!(manager.is_enabled());
        assert_eq!(pool.threshold, 0.8);
        assert_eq!(pool.physical_memory_limit, limit);
    }

    #[test]
    fn test_threshold_clamping() {
        let limit = 1024 * 1024 * 1024; // 1GB
        let pool = make_pool(1.5_f32.clamp(0.0, 1.0), limit);
        assert_eq!(pool.threshold, 1.0);

        let pool = make_pool((-0.5_f32).clamp(0.0, 1.0), limit);
        assert_eq!(pool.threshold, 0.0);
    }

    #[test]
    fn test_manager_with_memory_limit() {
        let limit = 64 * 1024 * 1024; // 64MB
        let pool = make_pool(0.9, limit);
        let manager = TieredMemoryManager::new(pool.clone());
        assert!(manager.is_enabled());
        assert_eq!(pool.physical_memory_limit, limit);
        assert_eq!(pool.threshold, 0.9);
    }

    #[test]
    fn test_eviction_target_is_capped_to_chunk_capacity() {
        let limit = 64 * 1024 * 1024; // 64MB
        let pool = make_pool(0.9, limit);
        let manager = TieredMemoryManager::new(pool);

        assert_eq!(
            manager.cap_eviction_target_to_counts(138, false, 387_294),
            138
        );
        assert_eq!(
            manager.cap_eviction_target_to_counts(138, true, 387_294),
            137
        );
        assert_eq!(manager.cap_eviction_target_to_counts(1, true, 10), 0);
        assert_eq!(manager.cap_eviction_target_to_counts(0, false, 10), 0);
        assert_eq!(manager.cap_eviction_target_to_counts(10, false, 4), 4);
    }

    #[test]
    fn test_eviction_starts_only_after_threshold_is_exceeded() {
        let pool = SharedMemoryPool::new(&TieredConfig {
            threshold: 0.75,
            lower_watermark: 0.5,
            physical_memory_limit: 8 * SEGMENT_SIZE,
            promotion_cooldown_ms: 0,
        });
        let manager = TieredMemoryManager::new(pool);

        assert_eq!(
            manager.allocation_eviction_target(5),
            None,
            "eviction should not trigger when the next allocation only reaches the threshold"
        );
        assert_eq!(
            manager.allocation_eviction_target(6),
            Some(3),
            "eviction should trigger when the next allocation would exceed the threshold"
        );
    }
}
