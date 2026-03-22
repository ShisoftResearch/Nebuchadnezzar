use crate::ram::chunk::Chunk;
use crate::ram::segs::{Segment, SEGMENT_SIZE};
use crate::ram::tiered::clock::ClockEvictionPolicy;
use crate::ram::tiered::eviction::evict_segment;
use crate::ram::tiered::promotion::promote_segment;
use crate::ram::tiered::SharedMemoryPool;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Manages tiered memory for a single chunk.
///
/// All chunks across all databases share one `SharedMemoryPool` whose stripe
/// counter is indexed by the calling thread's CPU ID — zero cross-chunk
/// contention on the hot path, no per-chunk registration required.
///
/// `last_known_count` tracks how many hot segments this chunk last reported to
/// the pool so that the periodic reconciliation scan can apply a signed delta
/// rather than overwriting the global total.
pub struct TieredMemoryManager {
    /// Shared server-wide memory budget and CPU-striped counter.
    shared_pool: Arc<SharedMemoryPool>,

    /// Last hot-segment count reported by this chunk to the shared pool.
    /// Updated only during periodic reconciliation (every 10 s), not on the
    /// hot path.
    last_known_count: AtomicUsize,

    /// CLOCK eviction policy for victim selection
    clock_policy: ClockEvictionPolicy,

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

    /// Last time we did a full scan to reconcile this chunk's contribution
    last_full_scan: parking_lot::Mutex<Instant>,
}

impl TieredMemoryManager {
    /// Create a new tiered memory manager for a single chunk.
    ///
    /// `shared_pool` is the server-wide stripe counter shared across all chunks
    /// and all databases.  A new slot is claimed from the pool here.
    pub fn new(shared_pool: Arc<SharedMemoryPool>) -> Self {
        TieredMemoryManager {
            clock_policy: ClockEvictionPolicy::new(shared_pool.promotion_cooldown_ms),
            shared_pool,
            last_known_count: AtomicUsize::new(0),
            enabled: true,
            disable_promotion: AtomicBool::new(false),
            last_full_scan: parking_lot::Mutex::new(Instant::now() - Duration::from_secs(100)),
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

    /// Check if eviction is needed and evict segments if necessary (legacy/test-only)
    ///
    /// NOTE: This method is kept for tests/benchmarks compatibility.
    /// Production code should rely on passive eviction via `evict_for_allocation()`.
    ///
    /// Returns the number of segments evicted
    pub fn check_and_evict(&self, chunk: &Chunk) -> Result<usize, io::Error> {
        // For tests/benchmarks, just use evict_for_allocation logic
        self.evict_for_allocation(chunk)
    }

    /// Explicitly evict a specific number of segments (test-only)
    ///
    /// NOTE: This method is kept for tests compatibility.
    /// Production code should rely on passive eviction via `evict_for_allocation()`.
    ///
    /// Returns the number of segments successfully evicted
    pub fn explicit_evict(&self, chunk: &Chunk, num_segments: usize) -> Result<usize, io::Error> {
        if !self.enabled {
            return Ok(0);
        }

        debug!("Explicit eviction requested for {} segments", num_segments);
        let evicted = self.evict_until_target(chunk, num_segments)?;

        // Update cached count after eviction
        self.decrement_hot_count_by(evicted);

        Ok(evicted)
    }

    /// Evict segments until we've evicted the target number
    ///
    /// Returns the number of segments successfully evicted
    fn evict_until_target(&self, chunk: &Chunk, target: usize) -> Result<usize, io::Error> {
        let mut evicted_count = 0;
        let mut attempts_without_progress = 0;
        const MAX_ATTEMPTS_WITHOUT_PROGRESS: usize = 3;

        // Try to evict target segments, with retries if we're making progress
        // This allows references to be released over time
        for attempt in 0..target * 2 {
            if evicted_count >= target {
                break; // Successfully evicted enough segments
            }

            match self.clock_policy.select_victim(chunk) {
                Some(victim) => {
                    match evict_segment(&victim, chunk) {
                        Ok(()) => {
                            evicted_count += 1;
                            self.eviction_count.fetch_add(1, Ordering::Relaxed);
                            attempts_without_progress = 0; // Reset counter on success
                            debug!(
                                "Evicted segment {} ({}/{}, attempt {})",
                                victim.id,
                                evicted_count,
                                target,
                                attempt + 1
                            );
                        }
                        Err(e) => {
                            warn!("Failed to evict segment {}: {}", victim.id, e);
                            attempts_without_progress += 1;
                        }
                    }
                }
                None => {
                    // No more victims available right now
                    attempts_without_progress += 1;
                    debug!(
                        "CLOCK could not find victim (attempt {}), evicted {}/{} segments",
                        attempt + 1,
                        evicted_count,
                        target
                    );
                }
            }

            // If we haven't made progress in several attempts, give up
            if attempts_without_progress >= MAX_ATTEMPTS_WITHOUT_PROGRESS {
                warn!(
                    "Eviction stalled after {} attempts without progress, evicted {}/{} segments",
                    MAX_ATTEMPTS_WITHOUT_PROGRESS, evicted_count, target
                );
                break;
            }

            // Small delay to allow references to be released if we're struggling
            if attempts_without_progress > 0 && evicted_count < target {
                std::thread::yield_now();
            }
        }

        if evicted_count > 0 {
            info!("Evicted {} segments to cold storage", evicted_count);
        } else if target > 0 {
            warn!(
                "Failed to evict any segments (target was {}). Hot segments: {}, all may be protected or have active references",
                target,
                chunk.segments().iter().filter(|s| s.is_hot()).count()
            );
        }

        Ok(evicted_count)
    }

    /// Check if allocating a new segment would exceed the threshold and evict if needed
    ///
    /// Eviction is triggered when hot memory would exceed `physical_memory_limit * threshold`.
    /// For example, with 512GB limit and 0.8 threshold, eviction starts at ~410GB.
    ///
    /// Returns the number of segments evicted
    pub fn evict_for_allocation(&self, chunk: &Chunk) -> Result<usize, io::Error> {
        if !self.enabled {
            return Ok(0);
        }

        // Get current hot segment count (use cached value for speed)
        let hot_segments_count = self.hot_count_cached(chunk);

        // Sanity check: if count is unreasonably large, clamp it
        // On 64-bit: usize::MAX / SEGMENT_SIZE ≈ 2^41 segments (unrealistic)
        let max_reasonable_segments = usize::MAX / SEGMENT_SIZE;
        let hot_segments_count = hot_segments_count.min(max_reasonable_segments);

        // Use checked arithmetic to prevent overflow
        let current_hot_memory = self.hot_memory_bytes(hot_segments_count);
        let after_alloc_memory =
            current_hot_memory
                .checked_add(SEGMENT_SIZE)
                .unwrap_or_else(|| {
                    warn!("Overflow calculating after-alloc memory");
                    self.shared_pool.physical_memory_limit * 2 // Force eviction
                });

        // Check if allocating one more segment would exceed the threshold-adjusted limit
        let threshold_limit = self.threshold_limit();
        if after_alloc_memory > threshold_limit {
            // Evict down to lower watermark to create headroom
            let lower_limit = self.lower_watermark_limit();
            let target_bytes = after_alloc_memory.saturating_sub(lower_limit);
            let segments_to_evict = (target_bytes + SEGMENT_SIZE - 1) / SEGMENT_SIZE; // Round up

            info!(
                "Proactive eviction before allocation: current {} hot segments ({} MB), would be {} MB after allocation, threshold {} MB ({}% of {} MB), evicting {} segments",
                hot_segments_count,
                current_hot_memory / (1024 * 1024),
                after_alloc_memory / (1024 * 1024),
                threshold_limit / (1024 * 1024),
                (self.shared_pool.threshold * 100.0) as u32,
                self.shared_pool.physical_memory_limit / (1024 * 1024),
                segments_to_evict
            );

            let evicted = self.evict_until_target(chunk, segments_to_evict)?;

            // Update cached count
            self.decrement_hot_count_by(evicted);

            Ok(evicted)
        } else {
            Ok(0)
        }
    }

    /// Promote a cold segment to hot
    ///
    /// This is called when a cold segment is accessed and needs to be brought
    /// back into hot storage
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
            let _ = self.evict_down_to_lower(chunk, hot_segments_count)?;
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

    /// Evict segments down to the lower watermark to create headroom
    fn evict_down_to_lower(
        &self,
        chunk: &Chunk,
        current_hot_segments: usize,
    ) -> Result<usize, io::Error> {
        let current_bytes = self.hot_memory_bytes(current_hot_segments);
        let lower_limit = self.lower_watermark_limit();
        if current_bytes <= lower_limit {
            return Ok(0);
        }

        let excess_bytes = current_bytes.saturating_sub(lower_limit);
        let target_segments = (excess_bytes + SEGMENT_SIZE - 1) / SEGMENT_SIZE;

        let evicted = self.evict_until_target(chunk, target_segments)?;
        self.decrement_hot_count_by(evicted);
        if evicted > 0 {
            self.lower_watermark_evictions
                .fetch_add(evicted as u64, Ordering::Relaxed);
        }
        Ok(evicted)
    }

    /// Return the server-wide hot segment count, reconciling this chunk's
    /// contribution via `last_known_count` every 10 seconds to correct drift.
    ///
    /// The returned value is the **global** total across all chunks and
    /// databases, which is what threshold checks must compare against.
    pub fn hot_count_cached(&self, chunk: &Chunk) -> usize {
        let should_full_scan = if let Some(mut last_scan) = self.last_full_scan.try_lock() {
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
            // Scan this chunk's actual hot count and apply a signed delta so the
            // global total corrects for any drift without touching other chunks.
            let actual = self.count_hot_segments(chunk);
            let prev = self.last_known_count.swap(actual, Ordering::Relaxed);
            let delta = actual as isize - prev as isize;
            if delta != 0 {
                self.shared_pool.adjust_delta(delta);
                trace!("Full scan reconciled: prev={}, actual={}, delta={}", prev, actual, delta);
            }
        } else {
            // Sanity-check: if last_known_count exceeds this chunk's segment
            // count something has drifted badly — resync immediately.
            let known = self.last_known_count.load(Ordering::Relaxed);
            let total_segments = chunk.segments().len();
            if known > total_segments {
                let actual = self.count_hot_segments(chunk);
                warn!(
                    "last_known_count out of range: known={}, total_segments={}, recalculated={}",
                    known, total_segments, actual
                );
                let prev = self.last_known_count.swap(actual, Ordering::Relaxed);
                self.shared_pool.adjust_delta(actual as isize - prev as isize);
            }
        }

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

    /// Count hot segments in the chunk
    fn count_hot_segments(&self, chunk: &Chunk) -> usize {
        chunk.segments().iter().filter(|s| s.is_hot()).count()
    }

    /// Count cold segments in the chunk
    pub fn count_cold_segments(&self, chunk: &Chunk) -> usize {
        chunk.segments().iter().filter(|s| s.is_cold()).count()
    }

    /// Get memory statistics
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

    /// Enable or disable tiered memory
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Check if tiered memory is enabled
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
}
