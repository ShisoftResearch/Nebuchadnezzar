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
        // Clean only if segment have fragments
        let mut frags = seg.frags.lock();
        if !frags.is_empty() {return;}
        // Lock segment exclusively only if it is not rw locked to avoid waiting for disk backups.
        // There is one potential deadlock with writing cells. Because every cell write operations
        // locks on cell first and then lock it's underlying segment to acquire space, but cleaner
        // will lock segments first then lock the cell to move it to fill the fragments.
        // The solution is to lock the segment first. Before moving the cells, segment lock have to
        // be released and then lock the cell lock. After cell moved, release the cell lock and do
        // further operations on segment with a new segment lock guard.
        // Because the segment will be locked twice, there is no guarantee for not modifying the
        // segment when moving the cell, extra efforts need to taken care of to ensure correctness.
        while true {
            let seg_lock = seg.lock.try_write();
            if seg_lock.is_none() {return;}

        }

    }
}