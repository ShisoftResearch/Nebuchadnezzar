pub mod cell_locking;
pub mod clock;
pub mod eviction;
pub mod manager;
pub mod promotion;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod bench;

use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;

/// Number of cache-line-padded stripes in the shared hot-segment counter.
/// Must be a power of two.  64 covers systems up to 64 logical CPUs without
/// any cross-stripe contention; on systems with more CPUs a few stripes will
/// be shared, which is still far better than a single global atomic.
const STRIPE_COUNT: usize = 64;
const STRIPE_MASK: usize = STRIPE_COUNT - 1;

/// A cache-line-padded atomic counter.
/// `#[repr(align(64))]` puts each stripe on its own 64-byte cache line so
/// concurrent writes to different stripes never cause false sharing.
#[repr(align(64))]
struct CachePadded(AtomicUsize);

use std::thread;

/// Returns this thread's stripe index, assigning one on first call.
#[inline]
fn thread_stripe() -> usize {
    thread::current().id().as_u64().get() as usize & STRIPE_MASK
}

/// Server-wide physical memory budget shared across all databases.
///
/// Writes (increment / decrement) go to the stripe indexed by the calling
/// thread's current CPU ID, so concurrent writers on different CPUs never
/// touch the same cache line.  No per-chunk registration is required.
///
/// Reads (total_hot_segments) sum all stripes and are only triggered at
/// segment-allocation boundaries (~every 8 MB of new data per chunk).
pub struct SharedMemoryPool {
    /// Total server-wide physical memory limit for hot segments, in bytes.
    pub physical_memory_limit: usize,
    /// Fraction of the limit at which eviction is triggered (0.0–1.0).
    pub threshold: f32,
    /// Fraction of the limit to evict down to once the threshold is crossed.
    pub lower_watermark: f32,
    /// Cooldown in milliseconds after promotion during which eviction skips the segment.
    pub promotion_cooldown_ms: u64,
    /// CPU-indexed stripe counters.
    stripes: [CachePadded; STRIPE_COUNT],
    /// Global signed correction applied by reconciliation logic.
    correction: AtomicIsize,
}

impl SharedMemoryPool {
    pub fn new(config: &TieredConfig) -> Arc<Self> {
        Arc::new(Self {
            physical_memory_limit: config.physical_memory_limit,
            threshold: config.threshold,
            lower_watermark: config.lower_watermark,
            promotion_cooldown_ms: config.promotion_cooldown_ms,
            stripes: std::array::from_fn(|_| CachePadded(AtomicUsize::new(0))),
            correction: AtomicIsize::new(0),
        })
    }

    /// Increment the server-wide hot-segment count by 1.
    /// Routes to this thread's assigned stripe — a single TLS read.
    #[inline]
    pub fn increment(&self) {
        self.stripes[thread_stripe()]
            .0
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the server-wide hot-segment count by `n`, saturating at zero.
    #[inline]
    pub fn decrement_by(&self, n: usize) {
        self.stripes[thread_stripe()]
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(n))
            })
            .ok();
    }

    /// Apply a signed delta globally.
    /// Used by reconciliation scans to correct drift regardless of which stripes
    /// previous increments landed on.
    #[inline]
    pub fn adjust_delta(&self, delta: isize) {
        if delta == 0 {
            return;
        }
        self.correction.fetch_add(delta, Ordering::Relaxed);
    }

    /// Sum all stripes to get the server-wide hot segment count.
    /// O(STRIPE_COUNT) — only called at segment-allocation boundaries.
    #[inline]
    pub fn total_hot_segments(&self) -> usize {
        let striped_total: isize = self
            .stripes
            .iter()
            .map(|s| s.0.load(Ordering::Relaxed) as isize)
            .sum();
        striped_total
            .saturating_add(self.correction.load(Ordering::Relaxed))
            .max(0) as usize
    }

    #[inline]
    pub fn threshold_limit(&self) -> usize {
        (self.physical_memory_limit as f64 * self.threshold as f64) as usize
    }

    #[inline]
    pub fn lower_watermark_limit(&self) -> usize {
        (self.physical_memory_limit as f64 * self.lower_watermark as f64) as usize
    }
}

/// Configuration for tiered memory management
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TieredConfig {
    /// Memory usage threshold (0.0-1.0) to trigger eviction
    pub threshold: f32,
    /// Lower watermark (0.0-1.0) to evict down to when threshold is crossed
    pub lower_watermark: f32,
    /// Physical memory limit in bytes for hot segments
    pub physical_memory_limit: usize,
    /// Cooldown in milliseconds after promotion during which eviction should skip the segment
    pub promotion_cooldown_ms: u64,
}

impl TieredConfig {
    /// Get total system memory in bytes
    fn get_system_memory() -> usize {
        #[cfg(target_os = "linux")]
        {
            // Read from /proc/meminfo
            if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
                for line in meminfo.lines() {
                    if line.starts_with("MemTotal:") {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<usize>() {
                                return kb * 1024; // Convert KB to bytes
                            }
                        }
                    }
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            if let Ok(output) = Command::new("sysctl").arg("-n").arg("hw.memsize").output() {
                if let Ok(mem_str) = String::from_utf8(output.stdout) {
                    if let Ok(mem) = mem_str.trim().parse::<usize>() {
                        return mem;
                    }
                }
            }
        }

        // Default fallback: 16GB if we can't detect
        log::warn!("Could not detect system memory, defaulting to 16GB");
        16 * 1024 * 1024 * 1024
    }

    /// Create a new tiered config with default threshold of 0.8 and system memory as limit
    pub fn new() -> Self {
        Self {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit: Self::get_system_memory(),
            promotion_cooldown_ms: 2000,
        }
    }

    /// Create a new tiered config with explicit memory limit
    pub fn with_memory_limit(physical_memory_limit: usize) -> Self {
        Self {
            threshold: 0.8,
            lower_watermark: 0.72,
            physical_memory_limit,
            promotion_cooldown_ms: 2000,
        }
    }

    /// Create a new tiered config with custom threshold and explicit memory limit
    pub fn with_threshold(threshold: f32, physical_memory_limit: usize) -> Self {
        Self {
            threshold,
            lower_watermark: 0.72,
            physical_memory_limit,
            promotion_cooldown_ms: 2000,
        }
    }

    /// Read tiered config from environment variables
    pub fn from_env() -> Option<Self> {
        let enabled = std::env::var("NEB_TIERED_MEMORY_ENABLED")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        if !enabled {
            return None;
        }

        let threshold = std::env::var("NEB_TIERED_MEMORY_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<f32>().ok())
            .unwrap_or(0.8);

        let lower_watermark = std::env::var("NEB_TIERED_MEMORY_LOWER_WATERMARK")
            .ok()
            .and_then(|v| v.parse::<f32>().ok())
            .unwrap_or(0.72);

        let promotion_cooldown_ms = std::env::var("NEB_TIERED_PROMOTION_COOLDOWN_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(2000);

        let physical_memory_limit = std::env::var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(Self::get_system_memory);

        Some(Self {
            threshold,
            lower_watermark,
            physical_memory_limit,
            promotion_cooldown_ms,
        })
    }
}

impl Default for TieredConfig {
    fn default() -> Self {
        Self::new()
    }
}
