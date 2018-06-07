use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use ram::chunk::Chunks;

pub mod combine;
pub mod compact;

pub struct Cleaner {
    chunks: Arc<Chunks>,
    stopped: Arc<AtomicBool>,
    pub segment_compact_per_turn: usize
}

impl Cleaner {
    pub fn new_and_start(chunks: Arc<Chunks>) -> Cleaner {
        let stop_tag = Arc::new(AtomicBool::new(false));
        let segment_compact_per_turn = chunks.list[0].segs.len() / 10 + 1;
        let cleaner = Cleaner {
            chunks: chunks.clone(),
            stopped: stop_tag.clone(),
            segment_compact_per_turn
        };
        thread::spawn(move || {
            while !stop_tag.load(Ordering::Relaxed) {
                for chunk in &chunks.list {
                    chunk.apply_dead_entry();
                    chunk.scan_tombstone_survival();
                    let ordered_segs_for_compact = chunk.segs_for_compact_cleaner();
                    // compact
                    ordered_segs_for_compact.iter()
                        .take(segment_compact_per_turn)
                        .for_each(|segment|
                            compact::CompactCleaner::clean_segment(chunk, segment));
                }
                thread::sleep(Duration::from_millis(10));
            }
            warn!("Cleaner main thread stopped");
        });
        return cleaner;
    }
}