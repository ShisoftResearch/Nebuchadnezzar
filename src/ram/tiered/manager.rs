use crate::ram::chunk::Chunk;
use crate::ram::segs::SEGMENT_SIZE;
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
    last_full_scan: std::sync::Mutex<Instant>,
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
            last_full_scan: std::sync::Mutex::new(Instant::now() - Duration::from_secs(100)),
        }
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
        self.cached_hot_count.fetch_sub(evicted, Ordering::Relaxed);

        Ok(evicted)
    }

    /// Evict segments until we've evicted the target number
    ///
    /// Returns the number of segments successfully evicted
    fn evict_until_target(&self, chunk: &Chunk, target: usize) -> Result<usize, io::Error> {
        let mut evicted_count = 0;

        for _ in 0..target {
            match self.clock_policy.select_victim(chunk) {
                Some(victim) => {
                    match evict_segment(&victim, chunk) {
                        Ok(()) => {
                            evicted_count += 1;
                            debug!(
                                "Evicted segment {} ({}/{})",
                                victim.id, evicted_count, target
                            );
                        }
                        Err(e) => {
                            warn!("Failed to evict segment {}: {}", victim.id, e);
                            // Continue trying other segments
                        }
                    }
                }
                None => {
                    // No more victims available
                    debug!(
                        "CLOCK could not find more victims, evicted {}/{} segments",
                        evicted_count, target
                    );
                    break;
                }
            }
        }

        if evicted_count > 0 {
            info!("Evicted {} segments to cold storage", evicted_count);
        }

        Ok(evicted_count)
    }


    /// Check if allocating a new segment would exceed the limit and evict if needed
    ///
    /// This is more aggressive than check_and_evict - it doesn't use the threshold,
    /// instead it checks if adding one more segment would exceed the physical limit.
    ///
    /// Returns the number of segments evicted
    pub fn evict_for_allocation(&self, chunk: &Chunk) -> Result<usize, io::Error> {
        if !self.enabled {
            return Ok(0);
        }

        // Get current hot segment count (use cached value for speed)
        let hot_segments_count = self.get_hot_count_cached(chunk);

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

        // Check if allocating one more segment would exceed the physical limit
        if after_alloc_memory > self.physical_memory_limit {
            let excess_bytes = after_alloc_memory.saturating_sub(self.physical_memory_limit);
            let segments_to_evict = (excess_bytes + SEGMENT_SIZE - 1) / SEGMENT_SIZE; // Round up

            info!(
                "Proactive eviction before allocation: current {} hot segments ({} MB), would be {} MB after allocation, limit {} MB, evicting {} segments",
                hot_segments_count,
                current_hot_memory / (1024 * 1024),
                after_alloc_memory / (1024 * 1024),
                self.physical_memory_limit / (1024 * 1024),
                segments_to_evict
            );

            let evicted = self.evict_until_target(chunk, segments_to_evict)?;

            // Update cached count
            self.cached_hot_count.fetch_sub(evicted, Ordering::Relaxed);

            Ok(evicted)
        } else {
            Ok(0)
        }
    }

    /// Promote a cold segment to hot
    ///
    /// This is called when a cold segment is accessed and needs to be brought
    /// back into hot storage
    pub fn promote(
        &self,
        segment: &crate::ram::segs::Segment,
        chunk: &Chunk,
    ) -> Result<(), io::Error> {
        if !self.enabled {
            return Ok(());
        }

        promote_segment(segment, chunk);

        // Update cached count after promotion
        self.cached_hot_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Get hot segment count using cached value with periodic full scans
    ///
    /// This avoids scanning all segments on every check, only doing full scans
    /// every 10 seconds to verify/update the cached count.
    fn get_hot_count_cached(&self, chunk: &Chunk) -> usize {
        // Check if we need to do a full scan (every 10 seconds)
        let should_full_scan = if let Ok(mut last_scan) = self.last_full_scan.try_lock() {
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
            self.cached_hot_count.load(Ordering::Relaxed)
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
        self.cached_hot_count.fetch_sub(1, Ordering::Relaxed);
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
