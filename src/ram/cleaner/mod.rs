use crate::ram::chunk::Chunks;
use lightning::map::Map;
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
            .name("Cleaner sweeper".into())
            .spawn(move || {
                while !stop_tag.load(Ordering::Relaxed) {
                    trace!("Apply dead entry");
                    chunks.list.par_iter().for_each(|chunk| {
                        chunk.apply_dead_entry();
                    });
                    thread::sleep(Duration::from_millis(sleep_interval_ms));
                }
            })
            .unwrap();
        thread::Builder::new()
            .name("Cleaner main".into())
            .spawn(move || {
                while !stop_tag_ref_clone.load(Ordering::Relaxed) {
                    checks_ref_clone.list.par_iter().for_each(|chunk| {
                        let segments_compact_per_turn = chunk.segs.len() / 10 + 1;
                        let segments_combine_per_turn = chunk.segs.len() / 20 + 2;
                        // have to put it right here for cleaners will clear the tombstone death counter
                        chunk.scan_tombstone_survival();
                        trace!("Cleaning chunk {}", chunk.id);
                        let mut cleaned_space: usize = 0;
                        {
                            // compact
                            let segments_for_compact = chunk.segs_for_compact_cleaner();
                            if !segments_for_compact.is_empty() {
                                debug!(
                                    "Chunk {} have {} segments to compact, overflow {}",
                                    chunk.id,
                                    segments_for_compact.len(),
                                    segments_compact_per_turn
                                );
                                cleaned_space += segments_for_compact
                                    .into_par_iter()
                                    .take(segments_compact_per_turn) // limit max segment to clean per turn
                                    .map(|segment| {
                                        compact::CompactCleaner::clean_segment(chunk, &segment)
                                    })
                                    .sum::<usize>();
                            }
                        }

                        {
                            // combine
                            let segments_candidates_for_combine: Vec<_> =
                                chunk.segs_for_combine_cleaner();
                            let segments_for_combine: Vec<_> = segments_candidates_for_combine
                                .into_iter()
                                .take(segments_combine_per_turn)
                                .collect();
                            if !segments_for_combine.is_empty() {
                                debug!(
                                    "Chunk {} have {} segments to combine, overflow {}",
                                    chunk.id,
                                    segments_for_combine.len(),
                                    segments_combine_per_turn
                                );
                                cleaned_space += combine::CombinedCleaner::combine_segments(
                                    chunk,
                                    &segments_for_combine,
                                );
                            }
                        }

                        chunk
                            .total_space
                            .fetch_sub(cleaned_space, Ordering::Relaxed);
                        chunk.check_and_archive_segments();
                    });
                    thread::sleep(Duration::from_millis(sleep_interval_ms));
                }
                warn!("Cleaner main thread stopped");
            })
            .unwrap();
        return cleaner;
    }
}
