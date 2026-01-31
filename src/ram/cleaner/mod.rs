use crate::ram::chunk::{Chunk, Chunks};
use crate::ram::segs::{SEGMENT_SIZE, Segment};
use rayon::prelude::*;
use std::env;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub mod combine;
pub mod compact;
#[cfg(test)]
mod tests;

lazy_static! {
    /// Global thread pool for compact cleaning operations
    static ref COMPACT_CLEAN_POOL: rayon::ThreadPool = rayon::ThreadPoolBuilder::new()
        .thread_name(|idx| format!("compact-clean-t{}", idx))
        .build()
        .unwrap();
}

#[allow(dead_code)]
#[allow(dead_code)]
pub struct Cleaner {
    chunks: Arc<Chunks>,
    stopped: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    _handle: Option<std::thread::JoinHandle<()>>,
}

// The two-level cleaner
impl Cleaner {
    pub fn new_and_start(chunks: Arc<Chunks>) -> Cleaner {
        Self::new_internal(chunks, false)
    }

    pub fn new_paused(chunks: Arc<Chunks>) -> Cleaner {
        Self::new_internal(chunks, true)
    }

    fn new_internal(chunks: Arc<Chunks>, initial_paused: bool) -> Cleaner {
        debug!(
            "Starting cleaner for {} chunks (paused={})",
            chunks.list.len(),
            initial_paused
        );
        let stop_tag = Arc::new(AtomicBool::new(false));
        let paused_tag = Arc::new(AtomicBool::new(initial_paused));
        let stop_tag_ref_clone = stop_tag.clone();
        let paused_tag_ref_clone = paused_tag.clone();
        let checks_ref_clone = chunks.clone();
        let sleep_interval_ms = env::var("NEB_CLEANER_SLEEP_INTERVAL_MS")
            .unwrap_or("100".to_string())
            .parse::<u64>()
            .unwrap();

        // Create thread pools once before the loop for reuse
        let clean_pool = rayon::ThreadPoolBuilder::new()
            .num_threads((num_cpus::get() / 4).max(1))
            .thread_name(|idx| format!("cleaner-clean-t{}", idx))
            .build()
            .unwrap();

        #[cfg(feature = "tiered_memory")]
        let evict_pool = rayon::ThreadPoolBuilder::new()
            .num_threads((num_cpus::get() / 8).min(8).max(1))
            .thread_name(|idx| format!("cleaner-evict-t{}", idx))
            .build()
            .unwrap();
        // Put follwing procedures in separate threads for real-time scheduling
        let handle = thread::Builder::new()
            .name("Cleaner main".into())
            .spawn(move || {
                #[cfg(feature = "cleaner")]
                {
                    let mut idle_rounds: u32 = 0;
                    while !stop_tag_ref_clone.load(Ordering::Relaxed) {
                        // Check if paused
                        if paused_tag_ref_clone.load(Ordering::Relaxed) {
                            thread::sleep(Duration::from_millis(100)); // Sleep while paused
                            continue;
                        }

                        let progress = AtomicBool::new(false);
                        // Main cleaning: compact and combine segments
                        clean_pool.install(|| {
                            checks_ref_clone.list.par_iter().for_each(|chunk| {
                                if Self::clean(chunk, false, false) {
                                    progress.store(true, Ordering::Relaxed);
                                }
                            });
                        });

                        // Background eviction: check and evict if memory limit exceeded
                        #[cfg(feature = "tiered_memory")]
                        evict_pool.install(|| {
                            checks_ref_clone.list.par_iter().for_each(|chunk| {
                                if let Some(ref tiered_manager) = chunk.tiered_manager {
                                    // Cheap skip: only evict if we're above threshold-adjusted limit
                                    let hot = tiered_manager.hot_count_cached(chunk);
                                    let threshold_limit = tiered_manager.threshold_limit();
                                    let hot_bytes = if hot > usize::MAX / SEGMENT_SIZE {
                                        warn!(
                                            "Hot segment count overflow risk: hot={}, seg_size={}",
                                            hot, SEGMENT_SIZE
                                        );
                                        usize::MAX
                                    } else {
                                        hot * SEGMENT_SIZE
                                    };
                                    if hot_bytes > threshold_limit {
                                        match tiered_manager.evict_for_allocation(chunk) {
                                            Ok(evicted) => {
                                                if evicted > 0 {
                                                    progress.store(true, Ordering::Relaxed);
                                                    debug!(
                                                        "Background eviction: evicted {} segments from chunk {}",
                                                        evicted, chunk.id
                                                    );
                                                }
                                            }
                                            Err(e) => {
                                                warn!(
                                                    "Background eviction failed for chunk {}: {}",
                                                    chunk.id, e
                                                );
                                            }
                                        }
                                    }
                                }
                            });
                        });

                        if progress.load(Ordering::Relaxed) {
                            idle_rounds = 0;
                            thread::sleep(Duration::from_millis(sleep_interval_ms));
                        } else {
                            // Back off when no work is done to avoid spinning
                            idle_rounds = (idle_rounds + 1).min(50);
                            let backoff_ms =
                                sleep_interval_ms.saturating_mul((idle_rounds + 1) as u64);
                            thread::sleep(Duration::from_millis(backoff_ms.min(5_000)));
                        }
                    }
                    warn!("Cleaner main thread stopped");
                }

                #[cfg(not(feature = "cleaner"))]
                {
                    warn!("Cleaner is disabled, the memory would likely to overflow");
                }
            })
            .unwrap();

        let cleaner = Cleaner {
            chunks: chunks.clone(),
            stopped: stop_tag.clone(),
            paused: paused_tag.clone(),
            _handle: Some(handle),
        };
        return cleaner;
    }
    /// Returns true if any cleaning work reclaimed space or reduced segments.
    pub fn clean(chunk: &Chunk, full: bool, wait: bool) -> bool {
        trace!("Cleaner: ready for clean {}, full {}", chunk.id, full);
        let guard = if wait {
            Some(chunk.gc_lock.lock())
        } else {
            chunk.gc_lock.try_lock()
        };
        if guard.is_none() {
            debug!(
                "Cleaner: Chunk {} GC in progress, will not wait it unless full GC",
                chunk.id
            );
            return false;
        }
        let num_segs = chunk.segs.len();
        trace!(
            "Cleaning chunk {}, full {}, segs {}, head seg {}",
            chunk.id,
            full,
            num_segs,
            chunk.get_head_seg_id()
        );
        let segments_compact_per_turn = if full { num_segs } else { num_segs / 5 + 1 };
        let segments_combine_per_turn = if full { num_segs } else { num_segs / 5 + 2 };

        let mut combiner_cleaned_space: usize = 0;
        let mut compacter_cleaned_space: usize = 0;
        let mut reduced_segments_count: usize = 0;
        #[cfg(feature = "combine_cleaner")]
        {
            trace!("Starting combine for chunk {}", chunk.id);
            let segments_candidates_for_combine = if full {
                chunk.segs_for_combine_cleaner_full()
            } else {
                chunk.segs_for_combine_cleaner()
            };
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
            if segments_for_combine.len() > 2 {
                debug!(
                    "Have {} segments to combine, candidates {}",
                    segments_for_combine.len(),
                    num_segments_candidates_for_combine
                );

                let (cleaned_space, num_reduced_segments) =
                    combine::CombinedCleaner::combine_segments(chunk, &segments_for_combine);
                combiner_cleaned_space += cleaned_space;
                reduced_segments_count += num_reduced_segments;
            }
        }
        #[cfg(feature = "compact_cleaner")]
        {
            trace!("Starting compact for chunk {}", chunk.id);
            let segments_for_compact = if full {
                chunk.segs_for_compact_cleaner_full()
            } else {
                chunk.segs_for_compact_cleaner()
            };
            if !segments_for_compact.is_empty() {
                debug!(
                    "Selected {} segments for compaction from chunk {}",
                    segments_for_compact.len(),
                    chunk.id
                );

                // Use global thread pool for compact cleaning
                compacter_cleaned_space += COMPACT_CLEAN_POOL.install(|| {
                    segments_for_compact
                        .into_par_iter()
                        .take(segments_compact_per_turn) // limit max segment to clean per turn
                        .map(|segment| compact::CompactCleaner::clean_segment(chunk, segment))
                        .sum::<usize>()
                });
            }
        }
        let combined_cleaned_space = combiner_cleaned_space + compacter_cleaned_space;
        chunk
            .total_space
            .fetch_sub(combiner_cleaned_space, Ordering::Relaxed); // only subtract combiner cleaned space, compacter cleaned does not reclaim segments
        if combined_cleaned_space > 0 {
            info!(
                "Chunk {} cleaned total {} bytes, reduced {} segments (combiner {} bytes, compacter {} bytes)",
                chunk.id, combined_cleaned_space, reduced_segments_count, combiner_cleaned_space, compacter_cleaned_space
            );
        }
        combined_cleaned_space > 0 || reduced_segments_count > 0
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        info!("Cleaner stopped");
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
        info!("Cleaner paused");
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
        info!("Cleaner resumed");
    }

    pub fn dummy(chunks: &Arc<Chunks>) -> Self {
        Cleaner {
            chunks: chunks.clone(),
            stopped: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            _handle: None,
        }
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

pub struct SegmentCandidate {
    segment: lightning::aarc::Arc<Segment>,
}

impl SegmentCandidate {
    pub fn new(segment: &lightning::aarc::Arc<Segment>) -> Option<Self> {
        if !segment.incr_references() {
            return None;
        }
        if !segment.lock_hot() {
            segment.decr_references();
            return None;
        }
        Some(Self { segment: segment.clone() })
    }
}

impl Drop for SegmentCandidate {
    fn drop(&mut self) {
        self.segment.decr_references();
        self.segment.set_hot();
    }
}

impl Deref for SegmentCandidate {
    type Target = Segment;
    fn deref(&self) -> &Self::Target {
        &self.segment
    }
}