use super::chunk::Chunks;
use super::segs::Segment;

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::sync::Arc;
use std::time::Duration;

pub struct Cleaner {
    chunks: Arc<Chunks>,
    closed: AtomicBool
}

impl Cleaner {
    pub fn new(chunks: &Arc<Chunks>) -> Arc<Cleaner> {
        let cleaner = Arc::new(Cleaner {
            chunks: chunks.clone(),
            closed: AtomicBool::new(false)
        });
        let cleaner_clone = cleaner.clone();
        thread::spawn(move || {
            let chunks = &cleaner_clone.chunks;
            while !cleaner_clone.closed.load(Ordering::Relaxed) {
                for chunk in &chunks.list { // consider put this in separate thread or fiber
                    for seg in &chunk.segs {
                        Cleaner::clean_segment(seg);
                    }
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        return cleaner;
    }
    pub fn clean_segment(seg: &Segment) {
        while !seg.tombstones.is_empty() {

        }
    }
}