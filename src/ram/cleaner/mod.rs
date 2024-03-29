use crate::ram::chunk::{Chunk, Chunks};
use itertools::Itertools;
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

#[allow(dead_code)]
pub struct Cleaner {
    chunks: Arc<Chunks>,
    stopped: Arc<AtomicBool>,
}

// The two-level cleaner
impl Cleaner {
    pub fn new_and_start(chunks: Arc<Chunks>) -> Cleaner {
        debug!("Starting cleaner for {} chunks", chunks.list.len());
        let stop_tag = Arc::new(AtomicBool::new(false));
        let cleaner = Cleaner {
            chunks: chunks.clone(),
            stopped: stop_tag.clone(),
        };
        let stop_tag_ref_clone = stop_tag.clone();
        let checks_ref_clone = chunks.clone();
        let sleep_interval_ms = env::var("NEB_CLEANER_SLEEP_INTERVAL_MS")
            .unwrap_or("100".to_string())
            .parse::<u64>()
            .unwrap();
        // Put follwing procedures in separate threads for real-time scheduling
        thread::Builder::new()
            .name("Cleaner main".into())
            .spawn(move || {
                while !stop_tag_ref_clone.load(Ordering::Relaxed) {
                    checks_ref_clone.list.par_iter().for_each(|chunk| {
                        Self::clean(chunk, false);
                    });
                    thread::sleep(Duration::from_millis(sleep_interval_ms));
                }
                warn!("Cleaner main thread stopped");
            })
            .unwrap();
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
        debug!("Archiving segments");
        chunk.check_and_archive_segments();
        debug!("Chunk Cleaned {}", chunk.id);
    }
}
