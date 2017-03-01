use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::BTreeSet;
use parking_lot::{RwLock, Mutex};

use super::cell::Header;

pub const SEGMENT_SIZE: usize = 8 * 1024 * 1024;

pub struct Segment {
    pub addr: usize,
    pub id: usize,
    pub bound: usize,
    pub last: AtomicUsize,
    pub lock: RwLock<()>,
    pub tombstones: Mutex<BTreeSet<usize>>,
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
                    Header::reserve(curr_last, size);
                    return Some(curr_last);
                }
            }
        }
    }
    pub fn put_cell_tombstone(&self, location: usize) {
        unsafe {
            let mut ver_field = location as *mut u64;
            *ver_field = 0; // set version to 0 to represent tombstone
            let mut tombstones = self.tombstones.lock();
            tombstones.insert(location);
        }
    }
}