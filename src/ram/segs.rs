use libc;
use ram::entry;
use ram::tombstone::{TOMBSTONE_SIZE_U32, Tombstone};
use ram::cell::Cell;
use ram::chunk::Chunk;
use ram::entry::EntryMeta;
use std::sync::atomic::{AtomicUsize, AtomicU32, AtomicI64, AtomicBool, Ordering};
use std::collections::BTreeSet;
use std::fs::{File, remove_file};
use std::io::BufWriter;
use std::io::prelude::*;
use std::io;
use std::path::Path;
use crc32c::crc32c;
use bifrost::utils::async_locks::{RwLock, RwLockReadGuard};

use super::cell::CellHeader;

pub const SEGMENT_SIZE: usize = 8 * 1024 * 1024;

pub struct Segment {
    pub id: u64,
    pub addr: usize,
    pub bound: usize,
    pub append_header: AtomicUsize,
    pub dead_space: AtomicU32,
    pub tombstones: AtomicU32,
    pub dead_tombstones: AtomicU32,
    pub last_tombstones_scanned: AtomicI64,
    pub backup_storage: Option<String>,
    pub archived: AtomicBool
}

impl Segment {
    pub fn new(id: u64, size: usize, backup_storage: &Option<String>) -> Segment {
        let buffer_ptr = unsafe { libc::malloc(size) as usize };
        Segment {
            addr: buffer_ptr,
            id,
            bound: buffer_ptr + size,
            append_header: AtomicUsize::new(buffer_ptr),
            dead_space: AtomicU32::new(0),
            tombstones: AtomicU32::new(0),
            dead_tombstones: AtomicU32::new(0),
            last_tombstones_scanned: AtomicI64::new(0),
            backup_storage: backup_storage.clone().map(|path| format!("{}/{}.backup", path, id)),
            archived: AtomicBool::new(false)
        }
    }

    pub fn try_acquire(&self, size: u32) -> Option<usize> {
        let size = size as usize;
        loop {
            let curr_last = self.append_header.load(Ordering::SeqCst);
            let exp_last = curr_last + size;
            if exp_last > self.bound {
                return None;
            } else {
                if self.append_header.compare_and_swap(curr_last, exp_last, Ordering::SeqCst) != curr_last {
                    continue;
                } else {
                    return Some(curr_last);
                }
            }
        }
    }

    pub fn entry_iter(&self) -> SegmentEntryIter {
        SegmentEntryIter {
            bound: self.bound,
            cursor: self.addr
        }
    }

    pub fn total_dead_space(&self) -> u32 {
        let dead_tombstones_space = self.dead_tombstones.load(Ordering::Relaxed) * TOMBSTONE_SIZE_U32;
        let dead_cells_space = self.dead_space.load(Ordering::Relaxed);
        return dead_tombstones_space + dead_cells_space;
    }

    pub fn living_rate(&self) -> f32 {
        let used_spaces = (self.append_header.load(Ordering::Relaxed) - self.addr) as f32;
        if used_spaces == 0f32 { return 1f32 }
        let total_dead_space = self.total_dead_space() as f32;
        let living_space = used_spaces - total_dead_space;
        return living_space / used_spaces;
    }

    // archive this segment and write the data to backup storage
    pub fn archive(&self) -> Result<bool, io::Error> {
        if let &Some(ref backup_storage) = &self.backup_storage {
            let path = Path::new(backup_storage);
            if path.exists() {
                warn!("Segment backup {} exists and can't archive twice", backup_storage);
                return Ok(false);
            }
            let file = File::open(path)?;
            let mut buffer = BufWriter::new(file);
            let seg_size = self.append_header.load(Ordering::Relaxed) - self.addr;
            unsafe {
                for offset in 0..seg_size {
                    let ptr = (self.addr + offset) as *const u8;
                    let byte = *ptr;
                    buffer.write(&[byte]);
                }
            }
            buffer.flush()?;
            return Ok(true);
        }
        return Ok(false);
    }

    fn mem_drop(&self) {
        debug!("disposing segment at {}", self.addr);
        unsafe {
            libc::free(self.addr as *mut libc::c_void)
        }
    }

    // remove the backup if it have one
    pub fn dispense(&self) {
        if let &Some(ref backup_storage) = &self.backup_storage {
            let path = Path::new(backup_storage);
            if path.exists() {
                if let Err(e) = remove_file(path) {
                    error!("cannot reclaim segment file on dispense {}", backup_storage)
                }
            } else {
                error!("cannot find segment backup {}", backup_storage)
            }
        }
    }
}

pub struct SegmentEntryIter {
    bound: usize,
    cursor: usize
}

impl Iterator for SegmentEntryIter {
    type Item = EntryMeta;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let cursor = self.cursor;
        if cursor >= self.bound {
            return None;
        }
        let (_, entry_meta) = entry::Entry::decode_from(
            cursor,
            |body_pos, entry| {
                let entry_header_size = body_pos - cursor;
                let entry_size = entry_header_size + entry.content_length as usize;
                return EntryMeta {
                    body_pos, entry_header: entry, entry_size, entry_pos: cursor
                };
            });
        self.cursor += entry_meta.entry_size;
        Some(entry_meta)
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        self.mem_drop()
    }
}