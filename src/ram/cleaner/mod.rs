use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use ram::chunk::Chunks;
use ram::cell::CellHeader;
use ram::tombstone::Tombstone;

pub mod combine;
pub mod compact;

pub struct Cleaner {
    chunks: Arc<Chunks>,
    stopped: Arc<AtomicBool>,
    segments_compact_per_turn: usize,
    segments_combine_per_turn: usize,
}

// The two-level cleaner
impl Cleaner {
    pub fn new_and_start(chunks: Arc<Chunks>) -> Cleaner {
        debug!("Starting cleaner for {} chunks", chunks.list.len());
        let stop_tag = Arc::new(AtomicBool::new(false));
        let segments_compact_per_turn = chunks.list[0].segs.len() / 10 + 1;
        let segments_combine_per_turn = chunks.list[0].segs.len() / 20 + 2;
        let cleaner = Cleaner {
            chunks: chunks.clone(),
            stopped: stop_tag.clone(),
            segments_compact_per_turn,
            segments_combine_per_turn
        };
        let stop_tag_ref_clone = stop_tag.clone();
        let checks_ref_clone = chunks.clone();
        thread::Builder::new()
            .name("Cleaner sweeper".into())
            .spawn(move || {
                while !stop_tag.load(Ordering::Relaxed) {
                    for chunk in &chunks.list {
                        chunk.apply_dead_entry();
                        chunk.scan_tombstone_survival();
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            });
        thread::Builder::new()
            .name("Cleaner main".into())
            .spawn(move || {
                while !stop_tag_ref_clone.load(Ordering::Relaxed) {
                    for chunk in &checks_ref_clone.list {
                        trace!("Cleaning chunk {}", chunk.id);
                        let mut cleaned_space: usize = 0;
                        {
                            // compact
                            let segments_for_compact = chunk.segs_for_compact_cleaner();
                            if !segments_for_compact.is_empty() {
                                debug!("Chunk {} have {} segments to compact, overflow {}",
                                       chunk.id, segments_for_compact.len(), segments_compact_per_turn);
                                cleaned_space += segments_for_compact.into_iter()
                                    .take(segments_compact_per_turn) // limit max segment to clean per turn
                                    .map(|segment|
                                        compact::CompactCleaner::clean_segment(chunk, &segment))
                                    .sum::<usize>();
                            }
                        }

                        {
                            // combine
                            let segments_candidates_for_combine: Vec<_> = chunk.segs_for_combine_cleaner();
                            let segments_for_combine: Vec<_> = segments_candidates_for_combine.into_iter()
                                    .take(segments_combine_per_turn)
                                    .collect();
                            if !segments_for_combine.is_empty() {
                                debug!("Chunk {} have {} segments to combine, overflow {}",
                                       chunk.id, segments_for_combine.len(), segments_combine_per_turn);
                                cleaned_space += combine::CombinedCleaner::combine_segments(chunk, &segments_for_combine);
                            }
                        }

                        chunk.total_space.fetch_sub(cleaned_space, Ordering::Relaxed);
                        chunk.check_and_archive_segments();
                    }
                    debug!("Cleaner round finished");
                    thread::sleep(Duration::from_millis(100));
                }
                warn!("Cleaner main thread stopped");
            });
        return cleaner;
    }
}