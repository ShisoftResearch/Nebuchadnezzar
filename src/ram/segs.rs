use libc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::BTreeSet;
use bifrost::utils::async_locks::{RwLock, RwLockReadGuard};

use super::cell::CellHeader;

pub const SEGMENT_SIZE: usize = 8 * 1024 * 1024;

pub struct Segment {
    pub id: u64,
    pub addr: usize,
    pub bound: usize,
    pub append_header: AtomicUsize,
    pub live_space: AtomicUsize,
    pub dead_space: AtomicUsize,
    pub lock: RwLock<()>,
}

impl Segment {
    pub fn new(id: u64, size: usize) -> Segment {
        let buffer_ptr = libc::malloc(size) as usize;
        Segment {
            addr: buffer_ptr,
            id,
            bound: buffer_ptr + size,
            append_header: AtomicUsize::new(buffer_ptr),
            live_space: AtomicUsize::new(0),
            dead_space: AtomicUsize::new(0),
            lock: RwLock::new(())
        }
    }

    // TODO: employ async lock
    pub fn try_acquire(&self, size: u32) -> Option<(usize, RwLockReadGuard<()>)> {
        let size = size as usize;
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
                    CellHeader::reserve(curr_last, size);
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

    pub fn no_frags(&self) -> bool {
        return self.dead_space.load(Ordering::Relaxed) == 0;
    }

    fn dispose (&self) {
        debug!("disposing chunk at {}", self.addr);
        unsafe {
            libc::free(self.addr as *mut libc::c_void)
        }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        self.dispose()
    }
}