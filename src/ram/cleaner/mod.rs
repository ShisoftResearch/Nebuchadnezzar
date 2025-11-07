use crate::ram::chunk::{Chunk, Chunks};
use rayon::prelude::*;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

pub mod combine;
pub mod compact;
#[cfg(test)]
mod tests;

#[allow(dead_code)]
pub struct Cleaner {
    chunks: Arc<Chunks>,
    stopped: Arc<AtomicBool>,
    _handle: Option<std::thread::JoinHandle<()>>,
}

/// Track last operation times to rate-limit promotion/eviction
struct RateLimiter {
    last_promotion_check: Instant,
    last_eviction_check: Instant,
    last_eviction_triggered: Instant,
    promotion_check_interval: Duration,
    eviction_check_interval: Duration,
    eviction_cooldown: Duration,
}

impl RateLimiter {
    fn new() -> Self {
        let now = Instant::now();
        // Get intervals from environment or use defaults
        let promotion_interval_ms = env::var("NEB_PROMOTION_CHECK_INTERVAL_MS")
            .unwrap_or("2000".to_string()) // Check every 2s (was every 100ms, more conservative)
            .parse::<u64>()
            .unwrap_or(2000);

        let eviction_interval_ms = env::var("NEB_EVICTION_CHECK_INTERVAL_MS")
            .unwrap_or("1000".to_string()) // Check every 1s (was every 100ms)
            .parse::<u64>()
            .unwrap_or(1000);

        let eviction_cooldown_ms = env::var("NEB_EVICTION_COOLDOWN_MS")
            .unwrap_or("2000".to_string()) // Wait 2s after eviction before checking again
            .parse::<u64>()
            .unwrap_or(2000);

        Self {
            last_promotion_check: now,
            last_eviction_check: now,
            last_eviction_triggered: now - Duration::from_secs(10), // Allow immediate first eviction
            promotion_check_interval: Duration::from_millis(promotion_interval_ms),
            eviction_check_interval: Duration::from_millis(eviction_interval_ms),
            eviction_cooldown: Duration::from_millis(eviction_cooldown_ms),
        }
    }

    fn should_check_promotion(&mut self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last_promotion_check) >= self.promotion_check_interval {
            self.last_promotion_check = now;
            true
        } else {
            false
        }
    }

    fn should_check_eviction(&mut self) -> bool {
        let now = Instant::now();
        // Check two conditions:
        // 1. Enough time since last check
        // 2. Enough cooldown time since last eviction (avoid thrashing)
        let enough_time_since_check =
            now.duration_since(self.last_eviction_check) >= self.eviction_check_interval;
        let cooldown_passed =
            now.duration_since(self.last_eviction_triggered) >= self.eviction_cooldown;

        if enough_time_since_check && cooldown_passed {
            self.last_eviction_check = now;
            true
        } else {
            false
        }
    }

    fn mark_eviction_triggered(&mut self) {
        self.last_eviction_triggered = Instant::now();
    }
}

// The two-level cleaner
impl Cleaner {
    pub fn new_and_start(chunks: Arc<Chunks>) -> Cleaner {
        debug!("Starting cleaner for {} chunks", chunks.list.len());
        let stop_tag = Arc::new(AtomicBool::new(false));
        let stop_tag_ref_clone = stop_tag.clone();
        let checks_ref_clone = chunks.clone();
        let sleep_interval_ms = env::var("NEB_CLEANER_SLEEP_INTERVAL_MS")
            .unwrap_or("100".to_string())
            .parse::<u64>()
            .unwrap();
        // Put follwing procedures in separate threads for real-time scheduling
        let handle = thread::Builder::new()
            .name("Cleaner main".into())
            .spawn(move || {
                let mut rate_limiter = RateLimiter::new();

                while !stop_tag_ref_clone.load(Ordering::Relaxed) {
                    // Check if we should do promotion/eviction this iteration
                    let should_promote = rate_limiter.should_check_promotion();
                    let should_evict = rate_limiter.should_check_eviction();

                    // Track if any evictions actually occurred across all chunks
                    let any_evictions = AtomicBool::new(false);

                    checks_ref_clone.list.par_iter().for_each(|chunk| {
                        // Pre-clean: handle promotions (rate-limited)
                        if should_promote {
                            Self::pre_clean(chunk);
                        }

                        // Main cleaning: always run
                        Self::clean(chunk, false);

                        // Post-clean: handle evictions (rate-limited)
                        if should_evict {
                            if let Some(evicted) = Self::post_clean(chunk) {
                                if evicted > 0 {
                                    // Mark that at least one chunk performed eviction
                                    any_evictions.store(true, Ordering::Relaxed);
                                }
                            }
                        }
                    });

                    // Only trigger eviction cooldown if actual evictions occurred
                    if should_evict && any_evictions.load(Ordering::Relaxed) {
                        rate_limiter.mark_eviction_triggered();
                    }

                    thread::sleep(Duration::from_millis(sleep_interval_ms));
                }
                warn!("Cleaner main thread stopped");
            })
            .unwrap();

        let cleaner = Cleaner {
            chunks: chunks.clone(),
            stopped: stop_tag.clone(),
            _handle: Some(handle),
        };
        return cleaner;
    }
    pub fn clean(chunk: &Chunk, full: bool) {
        debug!("Ready for clean {}, full {}", chunk.id, full);
        let guard = if full {
            Some(chunk.gc_lock.lock())
        } else {
            chunk.gc_lock.try_lock()
        };
        if guard.is_none() {
            debug!("GC in progress, will not wait it unless full GC");
            return;
        }
        let num_segs = chunk.segs.len();
        debug!(
            "Cleaning chunk {}, full {}, segs {}",
            chunk.id, full, num_segs
        );
        let segments_compact_per_turn = if full { num_segs } else { num_segs / 5 + 1 };
        let segments_combine_per_turn = if full { num_segs } else { num_segs / 5 + 2 };
        // have to put it right here for cleaners will clear the tombstone death counter
        chunk.scan_tombstone_survival();
        let mut cleaned_space: usize = 0;

        #[cfg(feature = "compact_cleaner")]
        {
            debug!("Starting compact {}", chunk.id);
            let segments_for_compact = chunk.segs_for_compact_cleaner();
            debug!(
                "Selected {} segments for compaction",
                segments_for_compact.len()
            );
            if !segments_for_compact.is_empty() {
                trace!(
                    "Chunk {} have {} segments to compact, overflow {}",
                    chunk.id,
                    segments_for_compact.len(),
                    segments_compact_per_turn
                );
                cleaned_space += segments_for_compact
                    .into_par_iter()
                    .take(segments_compact_per_turn) // limit max segment to clean per turn
                    .map(|segment| compact::CompactCleaner::clean_segment(chunk, &segment))
                    .sum::<usize>();
            }
        }
        #[cfg(feature = "combine_cleaner")]
        {
            debug!("Starting combine {}", chunk.id);
            let segments_candidates_for_combine = chunk.segs_for_combine_cleaner();
            let num_segments_candidates_for_combine = segments_candidates_for_combine.len();
            let mut segments_for_combine = vec![];
            let mut combining_size = 0f32;
            let max_combining_size = segments_combine_per_turn as f32;
            for (seg, util) in segments_candidates_for_combine {
                let new_size = combining_size + util;
                if new_size > max_combining_size {
                    break;
                }
                segments_for_combine.push(seg);
                combining_size = new_size;
            }
            if !segments_for_combine.is_empty() {
                debug!(
                    "Have {} segments to combine, candidates {}",
                    segments_for_combine.len(),
                    num_segments_candidates_for_combine
                );
                cleaned_space +=
                    combine::CombinedCleaner::combine_segments(chunk, &segments_for_combine);
            }
        }

        chunk
            .total_space
            .fetch_sub(cleaned_space, Ordering::Relaxed);
        debug!("Archiving segments for chunk {}", chunk.id);
        chunk.check_and_archive_segments();
        debug!("Chunk Cleaned {}", chunk.id);
    }

    fn pre_clean(chunk: &Chunk) {
        if let Some(ref tiered_manager) = chunk.tiered_manager {
            // Promote cold segments that have been referenced
            Self::handle_promotion_requests(chunk, tiered_manager);
        }
    }

    fn post_clean(chunk: &Chunk) -> Option<usize> {
        if let Some(ref tiered_manager) = chunk.tiered_manager {
            // Check for memory pressure and evict if needed
            match tiered_manager.check_and_evict(chunk) {
                Ok(evicted) => Some(evicted),
                Err(e) => {
                    error!("Tiered memory eviction failed in cleaner: {:?}", e);
                    None
                }
            }
        } else {
            None
        }
    }

    /// Handle promotion requests in the cleaner thread to avoid race conditions
    ///
    /// Uses sampling to check only a subset of segments for promotion, reducing overhead.
    /// This method checks cold segments that have been referenced (accessed)
    /// and promotes them to hot storage. This eliminates the race condition
    /// where user threads would promote segments while cleaners are iterating.
    fn handle_promotion_requests(
        chunk: &Chunk,
        tiered_manager: &crate::ram::tiered::manager::TieredMemoryManager,
    ) {
        // Skip if promotion is disabled (e.g., for benchmarking)
        if tiered_manager
            .disable_promotion
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }

        // Get sample size from env or use default (5% of segments, min 5, max 50)
        // Reduced from 10% to be more conservative since promotion is expensive
        let sample_pct = env::var("NEB_PROMOTION_SAMPLE_PERCENT")
            .unwrap_or("5".to_string())
            .parse::<f32>()
            .unwrap_or(5.0)
            .clamp(1.0, 100.0)
            / 100.0;

        let all_segments = chunk.segments();
        let total_segs = all_segments.len();

        if total_segs == 0 {
            return;
        }

        // Calculate sample size (more conservative limits)
        let sample_size = ((total_segs as f32 * sample_pct) as usize)
            .max(5)
            .min(50)
            .min(total_segs);

        // Use step size for sampling - this provides good coverage over time
        // as segment indices change due to creation/deletion
        let step = if sample_size > 0 {
            total_segs / sample_size
        } else {
            total_segs
        }
        .max(1);

        // Sample segments using stride pattern for even distribution
        let segments_to_promote: Vec<_> = all_segments
            .into_iter()
            .step_by(step)
            .take(sample_size)
            .filter(|seg| seg.is_cold() && seg.get_reference_bit())
            .collect();

        if !segments_to_promote.is_empty() {
            // Cap promotions per check to avoid overwhelming the system
            let max_promotions = env::var("NEB_MAX_PROMOTIONS_PER_CHECK")
                .unwrap_or("10".to_string())
                .parse::<usize>()
                .unwrap_or(10)
                .max(1);

            let actual_promotions = segments_to_promote.len().min(max_promotions);

            debug!(
                "Cleaner promoting {} cold segments in chunk {} (sampled {}/{}, capped at {})",
                actual_promotions, chunk.id, sample_size, total_segs, max_promotions
            );

            for segment in segments_to_promote.into_iter().take(max_promotions) {
                if let Err(e) = tiered_manager.promote(&segment, chunk) {
                    error!("Tiered memory promotion failed in cleaner: {:?}", e);
                }
            }
        }
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }
}

impl Drop for Cleaner {
    fn drop(&mut self) {
        // Signal the cleaner thread to stop
        self.stopped.store(true, Ordering::Relaxed);

        // Wait for the thread to finish
        if let Some(handle) = self._handle.take() {
            let _ = handle.join();
        }
    }
}
