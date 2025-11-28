use crate::ram::chunk::{Chunk, Chunks};
use rayon::prelude::*;
use std::env;
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
pub struct Cleaner {
    chunks: Arc<Chunks>,
    stopped: Arc<AtomicBool>,
    _handle: Option<std::thread::JoinHandle<()>>,
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
                #[cfg(feature = "cleaner")]
                {
                    // Create thread pools once before the loop for reuse
                    let clean_pool = rayon::ThreadPoolBuilder::new()
                        .thread_name(|idx| format!("cleaner-clean-t{}", idx))
                        .build()
                        .unwrap();
                    
                    #[cfg(feature = "tiered_memory")]
                    let evict_pool = rayon::ThreadPoolBuilder::new()
                        .thread_name(|idx| format!("cleaner-evict-t{}", idx))
                        .build()
                        .unwrap();
                    
                    while !stop_tag_ref_clone.load(Ordering::Relaxed) {
                        // Main cleaning: compact and combine segments
                        clean_pool.install(|| {
                            checks_ref_clone.list.par_iter().for_each(|chunk| {
                                Self::clean(chunk, false);
                            });
                        });

                        // Background eviction: check and evict if memory limit exceeded
                        #[cfg(feature = "tiered_memory")]
                        evict_pool.install(|| {
                            checks_ref_clone.list.par_iter().for_each(|chunk| {
                                if let Some(ref tiered_manager) = chunk.tiered_manager {
                                    match tiered_manager.evict_for_allocation(chunk) {
                                        Ok(evicted) => {
                                            if evicted > 0 {
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
                            });
                        });

                        thread::sleep(Duration::from_millis(sleep_interval_ms));
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
            _handle: Some(handle),
        };
        return cleaner;
    }
    pub fn clean(chunk: &Chunk, full: bool) {
        trace!("Cleaner: ready for clean {}, full {}", chunk.id, full);
        let guard = if full {
            Some(chunk.gc_lock.lock())
        } else {
            chunk.gc_lock.try_lock()
        };
        if guard.is_none() {
            debug!(
                "Cleaner: Chunk {} GC in progress, will not wait it unless full GC",
                chunk.id
            );
            return;
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
            let segments_for_compact = chunk.segs_for_compact_cleaner();
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
                        .map(|segment| compact::CompactCleaner::clean_segment(chunk, &segment))
                        .sum::<usize>()
                });
            }
        }
        let combined_cleaned_space = combiner_cleaned_space + compacter_cleaned_space;
        chunk
            .total_space
            .fetch_sub(combined_cleaned_space, Ordering::Relaxed);
        if combined_cleaned_space > 0 {
            info!(
                "Chunk {} cleaned total {} bytes, reduced {} segments (combiner {} bytes, compacter {} bytes)",
                chunk.id, combined_cleaned_space, reduced_segments_count, combiner_cleaned_space, compacter_cleaned_space
            );
        }
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        println!("Cleaner stopped");
    }

    pub fn dummy(chunks: &Arc<Chunks>) -> Self {
        Cleaner {
            chunks: chunks.clone(),
            stopped: Arc::new(AtomicBool::new(false)),
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
