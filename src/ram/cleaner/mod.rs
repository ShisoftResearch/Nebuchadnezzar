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
    pub fn new(chunks: Arc<Chunks>) -> Cleaner {
        let stop_tag = Arc::new(AtomicBool::new(false));
        let segment_compact_per_turn = chunks.list[0].segs.len() / 10 + 1;
        let cleaner = Cleaner {
            chunks: chunks.clone(),
            stopped: stop_tag.clone(),
            segment_compact_per_turn
        };
        thread::spawn(|| {
            while !stop_tag.load(Ordering::Relaxed) {
                for chunk in chunks.list {
                    chunk.apply_dead_entry();
                    chunk.scan_tombstone_survival();
                    let segs_to_compact = chunk
                        .ordered_segs_for_compact_cleaner().iter()
                        .take(segment_compact_per_turn);
                    
                }
                thread::sleep(Duration::from_millis(10));
            }
            warn!("Cleaner main thread stopped");
        });
        return cleaner;
    }
}