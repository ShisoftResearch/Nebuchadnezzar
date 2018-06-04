use libc;
use ram::repr;
use std::sync::atomic::{AtomicUsize, AtomicU32, AtomicI64, Ordering};
use std::collections::BTreeSet;
use bifrost::utils::async_locks::{RwLock, RwLockReadGuard};

use super::cell::CellHeader;

pub const SEGMENT_SIZE: usize = 8 * 1024 * 1024;

pub struct Segment {
    pub id: u64,
    pub addr: usize,
    pub bound: usize,
    pub append_header: AtomicUsize,
    pub total_space: AtomicU32,
    pub dead_space: AtomicU32,
    pub live_tombstones: AtomicU32,
    pub dead_tombstones: AtomicU32,
    pub last_tombstones_scanned: AtomicI64,
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
            total_space: AtomicU32::new(size as u32),
            dead_space: AtomicU32::new(0),
            live_tombstones: AtomicU32::new(0),
            dead_tombstones: AtomicU32::new(0),
            last_tombstones_scanned: AtomicI64::new(0),
            lock: RwLock::new(()),
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
                    return Some((curr_last, rl));
                }
            }
        }
    }

    pub fn entry_iter(&self) -> SegmentEntryIter {
        SegmentEntryIter {
            bound: self.bound,
            cursor: self.addr,
            lock_guard: self.lock.read()
        }
    }

    fn dispose (&self) {
        debug!("disposing chunk at {}", self.addr);
        unsafe {
            libc::free(self.addr as *mut libc::c_void)
        }
    }
}

pub struct SegmentEntryIter {
    bound: usize,
    cursor: usize,
    lock_guard: RwLockReadGuard<()>,
}

impl Iterator for SegmentEntryIter {
    type Item = (repr::Entry, usize);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let cursor = self.cursor;
        if cursor >= self.bound {
            return None;
        }
        let (entry, entry_size) = repr::Entry::decode_from(
            cursor,
            |body_pos, entry| {
                let entry_header_size = body_pos - cursor;
                return entry_header_size + entry.content_length as usize;
            });
        self.cursor += entry_size;
        return Some((entry, cursor));
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        self.dispose()
    }
}