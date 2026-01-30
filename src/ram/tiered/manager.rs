use crate::ram::chunk::Chunk;
use crate::ram::segs::{Segment, SEGMENT_SIZE};
use crate::ram::tiered::clock::ClockEvictionPolicy;
use crate::ram::tiered::eviction::evict_segment;
use crate::ram::tiered::promotion::promote_segment;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Manages tiered memory for a chunk
///
/// Coordinates eviction of hot segments to cold storage and promotion of cold segments
/// back to hot storage based on access patterns and memory pressure.
pub struct TieredMemoryManager {
    /// Physical memory limit in bytes (hot segments cannot exceed this)
    /// When hot segment memory usage exceeds this limit, eviction is triggered
    pub physical_memory_limit: usize,

    /// Eviction threshold as percentage of physical memory limit (0.0 to 1.0)
    /// Default: 0.8 (80%)
    eviction_threshold_percent: f32,

    /// CLOCK eviction policy for victim selection
    clock_policy: ClockEvictionPolicy,

    /// Whether tiered memory is enabled
    enabled: bool,

    /// Whether to disable promotion (for benchmarking cold reads)
    /// When true, cold segments remain cold even when accessed
    pub disable_promotion: AtomicBool,

    /// Cached count of hot segments (updated incrementally)
    /// This avoids scanning all segments on every check
    cached_hot_count: AtomicUsize,

    /// Last time we did a full scan to verify the cached count
    last_full_scan: parking_lot::Mutex<Instant>,
}

impl TieredMemoryManager {
    /// Create a new tiered memory manager
    ///
    /// # Arguments
    /// * `physical_memory_limit` - Physical memory limit in bytes for hot segments
    /// * `eviction_threshold_percent` - Percentage (0.0-1.0) of limit before eviction
    pub fn new(physical_memory_limit: usize, eviction_threshold_percent: f32) -> Self {
        TieredMemoryManager {
            physical_memory_limit,
            eviction_threshold_percent: eviction_threshold_percent.clamp(0.0, 1.0),
            clock_policy: ClockEvictionPolicy::new(),
            enabled: true,
            disable_promotion: AtomicBool::new(false),
            cached_hot_count: AtomicUsize::new(0),
            last_full_scan: parking_lot::Mutex::new(Instant::now() - Duration::from_secs(100)),
        }
    }

    /// Get the threshold-adjusted memory limit
    ///
    /// This is `physical_memory_limit * eviction_threshold_percent`, the point
    /// at which eviction will be triggered.
    #[inline]
    pub fn threshold_limit(&self) -> usize {
        (self.physical_memory_limit as f64 * self.eviction_threshold_percent as f64) as usize
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
        let current_hot_memory =
            hot_segments_count
                .checked_mul(SEGMENT_SIZE)
                .unwrap_or_else(|| {
                    error!(
                        "Overflow calculating hot memory: {} segments * {} bytes",
                        hot_segments_count, SEGMENT_SIZE
                    );
                    self.physical_memory_limit * 2 // Force eviction
                });
        let after_alloc_memory =
            current_hot_memory
                .checked_add(SEGMENT_SIZE)
                .unwrap_or_else(|| {
                    warn!("Overflow calculating after-alloc memory");
                    self.physical_memory_limit * 2 // Force eviction
                });

        // Check if allocating one more segment would exceed the threshold-adjusted limit
        let threshold_limit =
            (self.physical_memory_limit as f64 * self.eviction_threshold_percent as f64) as usize;
        if after_alloc_memory > threshold_limit {
            let excess_bytes = after_alloc_memory.saturating_sub(threshold_limit);
            let segments_to_evict = (excess_bytes + SEGMENT_SIZE - 1) / SEGMENT_SIZE; // Round up

            info!(
                "Proactive eviction before allocation: current {} hot segments ({} MB), would be {} MB after allocation, threshold {} MB ({}% of {} MB), evicting {} segments",
                hot_segments_count,
                current_hot_memory / (1024 * 1024),
                after_alloc_memory / (1024 * 1024),
                threshold_limit / (1024 * 1024),
                (self.eviction_threshold_percent * 100.0) as u32,
                self.physical_memory_limit / (1024 * 1024),
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
    pub fn promote(&self, segment: &Segment) -> Result<(), io::Error> {
        if !self.enabled {
            return Ok(());
        }

        promote_segment(segment);

        // Update cached count after promotion
        self.cached_hot_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Get hot segment count using cached value with periodic full scans
    ///
    /// This avoids scanning all segments on every check, only doing full scans
    /// every 10 seconds to verify/update the cached count.
    pub fn hot_count_cached(&self, chunk: &Chunk) -> usize {
        // Check if we need to do a full scan (every 10 seconds)
        let should_full_scan = if let Some(mut last_scan) = self.last_full_scan.try_lock() {
            if last_scan.elapsed() >= Duration::from_secs(10) {
                *last_scan = Instant::now();
                true
            } else {
                false
            }
        } else {
            // Another thread is doing a full scan, use cached value
            false
        };

        if should_full_scan {
            // Do a full scan and update cache
            let actual_count = self.count_hot_segments(chunk);
            self.cached_hot_count.store(actual_count, Ordering::Relaxed);
            debug!("Full scan updated hot segment count: {}", actual_count);
            actual_count
        } else {
            // Use cached value
            let count = self.cached_hot_count.load(Ordering::Relaxed);
            let total_segments = chunk.segments().len();
            if count == 0 || count > total_segments {
                let actual_count = self.count_hot_segments(chunk);
                if count > total_segments {
                    warn!(
                        "Cached hot count out of range: cached={}, total_segments={}, recalculated={}",
                        count,
                        total_segments,
                        actual_count
                    );
                }
                self.cached_hot_count.store(actual_count, Ordering::Relaxed);
                debug!("Full scan updated hot segment count: {}", actual_count);
                return actual_count;
            }
            debug!("Using cached hot segment count: {}", count);
            count
        }
    }

    /// Increment the cached hot segment count
    /// Called when a new hot segment is created
    pub fn increment_hot_count(&self) {
        self.cached_hot_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the cached hot segment count
    /// Called when a hot segment is removed or evicted
    pub fn decrement_hot_count(&self) {
        self.decrement_hot_count_by(1);
    }

    /// Decrement the cached hot segment count by N, saturating at zero
    fn decrement_hot_count_by(&self, by: usize) {
        let res = self.cached_hot_count.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(by)),
        );
        if let Ok(previous) = res {
            if previous < by {
                warn!(
                    "Hot count underflow avoided: previous={}, decrement_by={}",
                    previous, by
                );
            }
        }
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
            threshold: (chunk.capacity / SEGMENT_SIZE) as f32 * self.eviction_threshold_percent,
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manager_creation() {
        let limit = 1024 * 1024 * 1024; // 1GB
        let manager = TieredMemoryManager::new(limit, 0.8);
        assert!(manager.is_enabled());
        assert_eq!(manager.eviction_threshold_percent, 0.8);
        assert_eq!(manager.physical_memory_limit, limit);
    }

    #[test]
    fn test_threshold_clamping() {
        let limit = 1024 * 1024 * 1024; // 1GB
        let manager = TieredMemoryManager::new(limit, 1.5);
        assert_eq!(manager.eviction_threshold_percent, 1.0);

        let manager = TieredMemoryManager::new(limit, -0.5);
        assert_eq!(manager.eviction_threshold_percent, 0.0);
    }

    #[test]
    fn test_manager_with_memory_limit() {
        let limit = 64 * 1024 * 1024; // 64MB
        let manager = TieredMemoryManager::new(limit, 0.9);
        assert!(manager.is_enabled());
        assert_eq!(manager.physical_memory_limit, limit);
        assert_eq!(manager.eviction_threshold_percent, 0.9);
    }
}
