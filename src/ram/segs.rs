use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::BTreeSet;
use parking_lot::{RwLock, RwLockReadGuard, Mutex};

use super::cell::Header;

pub const SEGMENT_SIZE: usize = 8 * 1024 * 1024;

pub struct Segment {
    pub addr: usize,
    pub id: usize,
    pub bound: usize,
    pub append_header: AtomicUsize,
    pub lock: RwLock<()>,
    pub frags: Mutex<BTreeSet<usize>>,
}

impl Segment {
    pub fn try_acquire(&self, size: usize) -> Option<(usize, RwLockReadGuard<()>)> {
        let rl = self.lock.read();
        loop {
            let curr_last = self.append_header.load(Ordering::SeqCst);
            let exp_last = curr_last + size;
            if exp_last > self.bound {
                return None;
            } else {
                if self.append_header.compare_and_swap(curr_last, exp_last, Ordering::SeqCst) != curr_last {
                    continue;
                } else {
                    Header::reserve(curr_last, size);
                    return Some((curr_last, rl));
                }
            }
        }
    }
//    TODO: Review if write lock segment is preferred when acquiring space from the segment
//    pub fn try_acquire(&self, size: usize) -> Option<(usize, RwLockReadGuard<()>)> {
//        let rl = self.lock.write();
//        let curr_last = self.last.load(Ordering::SeqCst);
//        let exp_last = curr_last + size;
//        if exp_last > self.bound {
//            return None;
//        } else {
//            // once we use write lock here, atomic CAS loop is not required
//            self.last.store(exp_last, Ordering::SeqCst);
//            Header::reserve(curr_last, size);
//            return Some((curr_last, rl.downgrade()));
//            // we only want exclusive lock when acquire space and reserve seats.
//            // further cell write operations should use read lock to allow parallel writing to one segment
//        }
//    }
    pub fn cell_val_from_raw<T>(&self, location: usize, offset: usize) -> *mut T {
        (location + offset) as *mut T
    }
    pub fn cell_version(&self, location: usize) -> *mut u64 {
        self.cell_val_from_raw(location, 0)
    }
    pub fn cell_size(&self, location: usize) -> *mut u32 {
        self.cell_val_from_raw(location, 8)
    }
    pub fn cell_hash(&self, location: usize) -> *mut u64 {
        self.cell_val_from_raw(location, 24)
    }
    pub fn put_cell_tombstone(&self, location: usize) {
        unsafe {
            *self.cell_version(location) = 0; // set version to 0 to represent tombstone
        }
    }
    pub fn put_frag(&self, location: usize) {
        let mut frags = self.frags.lock();
        frags.insert(location);
    }
    pub fn no_frags(&self) -> bool {
        let mut frags = self.frags.lock();
        frags.is_empty()
    }
}