use std::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::RwLock;

pub const SEGMENT_SIZE: usize = 8 * 1024 * 1024;

pub struct Segment {
    pub addr: usize,
    pub id: usize,
    pub bound: usize,
    pub last: AtomicUsize,
    pub lock: RwLock<()>,
    pub tombstones: Vec<usize>,
}

impl Segment {
    pub fn try_acquire(&self, size: usize) -> Option<usize> {
        let _ = self.lock.read();
        loop {
            let curr_last = self.last.load(Ordering::SeqCst);
            let exp_last = curr_last + size;
            if exp_last > self.bound {
                return None;
            } else {
                if self.last.compare_and_swap(curr_last, exp_last, Ordering::SeqCst) != curr_last {
                    continue;
                } else {
                    return Some(curr_last);
                }
            }
        }
    }
    pub fn del_cell(&self, location: usize) {

    }
}