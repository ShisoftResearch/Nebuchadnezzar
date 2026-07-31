use crate::ram::chunk::{Chunk, Chunks};
use crate::ram::segs::Segment;
use rayon::prelude::*;
use std::env;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub mod combine;
#[cfg(test)]
mod tests;

/// Foreground-to-cleaner wake signal. Allocation-pressure paths request a
/// cleaning pass instead of running one synchronously: an inline combine on
/// the mutation path stalls the writer for a whole collection cycle, so
/// backpressure comes from waking the background thread and, near capacity,
/// from its pressure-proportional pacing.
pub struct CleanerWake {
    thread: parking_lot::Mutex<Option<thread::Thread>>,
    pending: AtomicBool,
}

impl CleanerWake {
    pub fn new() -> Self {
        Self {
            thread: parking_lot::Mutex::new(None),
            pending: AtomicBool::new(false),
        }
    }

    /// Ask the cleaner to run soon. Cheap and wait-free on the fast path.
    pub fn request(&self) {
        if self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(thread) = self.thread.lock().as_ref() {
            thread.unpark();
        }
    }

    fn register(&self, thread: thread::Thread) {
        *self.thread.lock() = Some(thread);
    }

    fn take_pending(&self) -> bool {
        self.pending.swap(false, Ordering::AcqRel)
    }
}

impl Default for CleanerWake {
    fn default() -> Self {
        Self::new()
    }
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
                        if let Some(ref tiered_manager) = checks_ref_clone.tiered_manager {
                            evict_pool.install(|| {
                                match tiered_manager.evict_for_allocation_reconciled() {
                                    Ok(evicted) => {
                                        if evicted > 0 {
                                            progress.store(true, Ordering::Relaxed);
                                            debug!(
                                                "Background global eviction: evicted {} segments",
                                                evicted
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Background global eviction failed: {}", e);
                                    }
                                }
                            });
                        }

                        // Pressure-proportional pacing: without an inline
                        // clean on the mutation path this is what stands
                        // between a full-rate insert workload and the
                        // capacity wall. Two tiers keep it honest both ways:
                        // above 7/8 full run passes back to back with a short
                        // yield; above 3/4 keep passes coming but with a
                        // longer yield so continuous cleaning cannot tax
                        // workloads that are merely warm.
                        let fill_ratio_x8 = checks_ref_clone
                            .list
                            .iter()
                            .map(|chunk| {
                                let used = chunk.segs.len() * crate::ram::segs::SEGMENT_SIZE;
                                used.saturating_mul(8) / chunk.capacity.max(1)
                            })
                            .max()
                            .unwrap_or(0);
                        // Progress-gated: a chunk can sit near capacity with
                        // nothing reclaimable (static working set), and
                        // spinning passes against it is pure CPU tax on the
                        // foreground. Pressure pacing therefore holds only
                        // while passes keep reclaiming; allocation-threshold
                        // wakes and the reserve-boundary emergency cover the
                        // moment new garbage appears.
                        if progress.load(Ordering::Relaxed) {
                            if fill_ratio_x8 >= 7 {
                                idle_rounds = 0;
                                thread::park_timeout(Duration::from_millis(10));
                                continue;
                            }
                            if fill_ratio_x8 >= 6 {
                                idle_rounds = 0;
                                thread::park_timeout(Duration::from_millis(50));
                                continue;
                            }
                        }
                        if checks_ref_clone.cleaner_wake.take_pending() {
                            // A mutation path is under allocation pressure:
                            // run the next pass immediately.
                            idle_rounds = 0;
                            continue;
                        }
                        if progress.load(Ordering::Relaxed) {
                            idle_rounds = 0;
                            thread::park_timeout(Duration::from_millis(sleep_interval_ms));
                        } else {
                            // Back off when no work is done to avoid spinning;
                            // a wake request cuts the backoff short.
                            idle_rounds = (idle_rounds + 1).min(50);
                            let backoff_ms =
                                sleep_interval_ms.saturating_mul((idle_rounds + 1) as u64);
                            thread::park_timeout(Duration::from_millis(backoff_ms.min(5_000)));
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
        chunks.cleaner_wake.register(handle.thread().clone());

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
        let segments_combine_per_turn = if full { num_segs } else { num_segs / 5 + 2 };

        let mut combiner_cleaned_space: usize = 0;
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
            if segments_for_combine.len() >= 2 {
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
        let combined_cleaned_space = combiner_cleaned_space;
        chunk
            .total_space
            .fetch_sub(combiner_cleaned_space, Ordering::Relaxed); // only subtract combiner cleaned space, compacter cleaned does not reclaim segments
        if combined_cleaned_space > 0 {
            debug!(
                "Chunk {} cleaned total {} bytes, reduced {} segments (combiner {} bytes)",
                chunk.id, combined_cleaned_space, reduced_segments_count, combiner_cleaned_space
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
        Some(Self {
            segment: segment.clone(),
        })
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
