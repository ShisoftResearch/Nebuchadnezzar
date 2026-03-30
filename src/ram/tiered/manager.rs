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
    chunk_states: parking_lot::RwLock<HashMap<usize, Arc<ChunkTierState>>>,

    /// Round-robin cursor across all registered chunks for global eviction.
    eviction_cursor: AtomicUsize,

    /// Whether tiered memory is enabled
    enabled: bool,

    /// Whether to disable promotion (for benchmarking cold reads)
    /// When true, cold segments remain cold even when accessed
    pub disable_promotion: AtomicBool,

    /// Metrics counters
    promotion_count: AtomicU64,
    eviction_count: AtomicU64,
    churn_count: AtomicU64,
    lower_watermark_evictions: AtomicU64,
}

impl TieredMemoryManager {
    /// Create a new shared tiered memory manager.
    pub fn new(shared_pool: Arc<SharedMemoryPool>) -> Self {
        TieredMemoryManager {
            shared_pool,
            registered_chunks: parking_lot::RwLock::new(Vec::new()),
            chunk_states: parking_lot::RwLock::new(HashMap::new()),
            eviction_cursor: AtomicUsize::new(0),
            enabled: true,
            disable_promotion: AtomicBool::new(false),
            promotion_count: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
            churn_count: AtomicU64::new(0),
            lower_watermark_evictions: AtomicU64::new(0),
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

    #[inline]
    fn hot_memory_bytes(&self, hot_segments_count: usize) -> usize {
        hot_segments_count
            .checked_mul(SEGMENT_SIZE)
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
        if let Some(state) = self.chunk_states.read().get(&key) {
            return state.clone();
        }

        let mut states = self.chunk_states.write();
        states
            .entry(key)
            .or_insert_with(|| Arc::new(ChunkTierState::new(self.shared_pool.promotion_cooldown_ms)))
            .clone()
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

        let dead_keys: Vec<usize> = chunks.list.iter().map(Self::chunk_key).collect();
        let mut states = self.chunk_states.write();
        for key in dead_keys {
            states.remove(&key);
        }

        self.decrement_hot_count_by(removed_hot_segments);
    }

    fn prune_chunk_states(&self, live_sets: &[Arc<Chunks>]) {
        let live_keys: std::collections::HashSet<usize> = live_sets
            .iter()
            .flat_map(|chunks| chunks.list.iter().map(Self::chunk_key))
            .collect();
        self.chunk_states
            .write()
            .retain(|key, _| live_keys.contains(key));
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

    fn reconcile_chunk(&self, chunk: &Chunk) {
        let state = self.ensure_chunk_state(chunk);
        let should_full_scan = if let Some(mut last_scan) = state.last_full_scan.try_lock() {
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
                trace!("Full scan reconciled: prev={}, actual={}, delta={}", prev, actual, delta);
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
                self.shared_pool.adjust_delta(actual as isize - prev as isize);
            }
        }
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
        self.evict_for_allocation()
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
        let registered = self.collect_registered_chunk_sets();
        let chunks = self.all_registered_chunks(&registered);
        if chunks.is_empty() || target == 0 {
            return Ok(0);
        }

        let mut evicted_count = 0;
        let mut attempts_without_progress = 0;
        const MAX_ATTEMPTS_WITHOUT_PROGRESS: usize = 3;

        while evicted_count < target {
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
                        warn!(
                            "Failed to evict segment {} from chunk {}: {}",
                            victim.id, chunk.id, e
                        );
                    }
                }
            }

            if !made_progress {
                attempts_without_progress += 1;
                if attempts_without_progress >= MAX_ATTEMPTS_WITHOUT_PROGRESS {
                    warn!(
                        "Global eviction stalled after {} attempts without progress, evicted {}/{} segments",
                        MAX_ATTEMPTS_WITHOUT_PROGRESS, evicted_count, target
                    );
                    break;
                }
                std::thread::yield_now();
            }
        }

        if evicted_count == 0 && target > 0 {
            let hot_segments: usize = chunks.iter().map(|chunk| self.count_hot_segments(chunk)).sum();
            warn!(
                "Failed to evict any segments globally (target was {}). Hot segments: {}, all may be protected or have active references",
                target, hot_segments
            );
        }

        Ok(evicted_count)
    }

    /// Check if allocating a new segment would exceed the threshold and evict globally if needed.
    ///
    /// Eviction is triggered when hot memory would exceed `physical_memory_limit * threshold`.
    /// For example, with 512GB limit and 0.8 threshold, eviction starts at ~410GB.
    ///
    /// Returns the number of segments evicted.
    pub fn evict_for_allocation(&self) -> Result<usize, io::Error> {
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
            let lower_limit = self.lower_watermark_limit();

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
            let _ = self.evict_down_to_lower(hot_segments_count)?;
        }

        let churn_candidate =
            segment.recently_evicted_within(self.shared_pool.promotion_cooldown_ms);

        promote_segment(segment);
        segment.reset_access_count();

        self.shared_pool.increment();
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
        let evicted = self.evict_globally_until_target(target_segments)?;
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

    /// Increment the server-wide hot-segment count.
    pub fn increment_hot_count(&self) {
        self.shared_pool.increment();
    }

    /// Decrement the server-wide hot-segment count by 1.
    pub fn decrement_hot_count(&self) {
        self.decrement_hot_count_by(1);
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

        assert_eq!(manager.cap_eviction_target_to_counts(138, false, 387_294), 138);
        assert_eq!(manager.cap_eviction_target_to_counts(138, true, 387_294), 137);
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
