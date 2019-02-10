use bifrost::utils::async_locks::{RwLock, RwLockReadGuard};
use libc;
use parking_lot;
use ram::cell;
use ram::cell::Cell;
use ram::chunk::Chunk;
use ram::entry;
use ram::entry::EntryMeta;
use ram::tombstone::{Tombstone, TOMBSTONE_SIZE_U32};
use std::collections::BTreeSet;
use std::fs::{copy, create_dir_all, remove_file, File};
use std::io;
use std::io::prelude::*;
use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicUsize, Ordering};

use super::cell::CellHeader;

pub const MAX_SEGMENT_SIZE_U32: u32 = 8 * 1024 * 1024;
pub const MAX_SEGMENT_SIZE: usize = MAX_SEGMENT_SIZE_U32 as usize;

pub struct Segment {
    pub id: u64,
    pub addr: usize,
    pub bound: usize,
    pub append_header: AtomicUsize,
    pub dead_space: AtomicU32,
    pub tombstones: AtomicU32,
    pub dead_tombstones: AtomicU32,
    pub last_tombstones_scanned: AtomicI64,
    pub references: AtomicUsize,
    pub backup_file_name: Option<String>,
    pub wal_file: Option<parking_lot::Mutex<BufWriter<File>>>,
    pub wal_file_name: Option<String>,
    pub archived: AtomicBool,
    pub dropped: AtomicBool,
}

impl Segment {
    pub fn new(
        id: u64,
        size: usize,
        backup_storage: &Option<String>,
        wal_storage: &Option<String>,
    ) -> Segment {
        let buffer_ptr = unsafe { libc::malloc(size) as usize };
        let mut wal_file_name = None;
        let mut wal_file = None;
        if let Some(wal_storage) = wal_storage {
            create_dir_all(wal_storage);
            let file_name = format!("{}/{}.log", wal_storage, id);
            let file = BufWriter::with_capacity(
                4096, // most common disk block size
                File::create(&file_name).unwrap(),
            ); // fast fail
            wal_file_name = Some(file_name);
            wal_file = Some(parking_lot::Mutex::new(file));
        }
        debug!(
            "Creating new segment with id {}, size {}, address {}",
            id, size, buffer_ptr
        );
        Segment {
            addr: buffer_ptr,
            id,
            bound: buffer_ptr + size,
            append_header: AtomicUsize::new(buffer_ptr),
            dead_space: AtomicU32::new(0),
            tombstones: AtomicU32::new(0),
            dead_tombstones: AtomicU32::new(0),
            last_tombstones_scanned: AtomicI64::new(0),
            references: AtomicUsize::new(0),
            backup_file_name: backup_storage
                .clone()
                .map(|path| format!("{}/{}.backup", path, id)),
            archived: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
            wal_file,
            wal_file_name,
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
                if self
                    .append_header
                    .compare_and_swap(curr_last, exp_last, Ordering::SeqCst)
                    != curr_last
                {
                    continue;
                } else {
                    return Some(curr_last);
                }
            }
        }
    }

    fn append_header(&self) -> usize {
        self.append_header.load(Ordering::Relaxed)
    }

    pub fn entry_iter(&self) -> SegmentEntryIter {
        SegmentEntryIter {
            bound: self.append_header(),
            cursor: self.addr,
        }
    }

    pub fn dead_space(&self) -> u32 {
        self.dead_space.load(Ordering::Relaxed)
    }

    // dead space plus tombstone spaces
    pub fn total_dead_space(&self) -> u32 {
        let dead_tombstones_space =
            self.dead_tombstones.load(Ordering::Relaxed) * TOMBSTONE_SIZE_U32;
        let dead_cells_space = self.dead_space();
        return dead_tombstones_space + dead_cells_space;
    }

    pub fn used_spaces(&self) -> u32 {
        let space = self.append_header.load(Ordering::Relaxed) as usize - self.addr;
        debug_assert!(space <= MAX_SEGMENT_SIZE);
        return space as u32;
    }

    pub fn living_space(&self) -> u32 {
        let total_dead_space = self.total_dead_space();
        let used_space = self.used_spaces();
        if total_dead_space <= used_space {
            used_space - total_dead_space
        } else {
            warn!(
                "living space check error for segment {}, used {}, dead {}",
                self.id, used_space, total_dead_space
            );
            0
        }
    }

    pub fn valid_space(&self) -> u32 {
        return self.used_spaces() - self.dead_space();
    }

    pub fn living_rate(&self) -> f32 {
        let used_space = self.used_spaces() as f32;
        if used_space == 0f32 {
            // empty segment
            return 1f32;
        }
        return self.living_space() as f32 / used_space;
    }

    // archive this segment and write the data to backup storage
    pub fn archive(&self) -> Result<bool, io::Error> {
        if let &Some(ref backup_file) = &self.backup_file_name {
            while !self.no_references() { /* wait until all references released */ }
            let backup_file_path = Path::new(backup_file);
            if backup_file_path.exists() {
                warn!(
                    "Segment backup {} exists and can't archive twice",
                    backup_file
                );
                return Ok(false);
            }
            if let Some(ref wal_file) = self.wal_file_name {
                // if there is a WAL file ready, copy this file to backup
                if let Some(ref file_mutex) = self.wal_file {
                    // this should be redundant but I don't want to take the chance
                    // obtain the writer lock before continue
                    let _ = file_mutex.lock();
                    copy(wal_file, backup_file);
                    remove_file(wal_file);
                } else {
                    panic!()
                }
            } else {
                let backup_file = File::create(backup_file_path)?;
                let seg_size = self.append_header.load(Ordering::Relaxed) - self.addr;
                let mut buffer = BufWriter::with_capacity(seg_size, backup_file);
                unsafe {
                    for offset in 0..seg_size {
                        let ptr = (self.addr + offset) as *const u8;
                        let byte = *ptr;
                        buffer.write(&[byte])?;
                    }
                }
                buffer.flush()?;
                return Ok(true);
            }
        }
        return Ok(false);
    }

    pub fn write_wal(&self, addr: usize, size: u32) -> io::Result<()> {
        if let Some(ref wal_file) = self.wal_file {
            let mut file = wal_file.lock();
            unsafe {
                for offset in 0..size as usize {
                    let ptr = (addr + offset) as *const u8;
                    let byte = *ptr;
                    file.write(&[byte])?;
                }
            }
            file.flush()?;
        }
        return Ok(());
    }

    pub fn no_references(&self) -> bool {
        self.references.load(Ordering::Relaxed) == 0
    }

    pub fn mem_drop(&self) {
        if !self
            .dropped
            .compare_and_swap(false, true, Ordering::Relaxed)
        {
            debug!("disposing segment at {}", self.addr);
            unsafe { libc::free(self.addr as *mut libc::c_void) }
        }
    }

    // remove the backup if it have one
    pub fn dispense(&self) {
        debug!("dispense segment {}", self.id);
        if let &Some(ref backup_storage) = &self.backup_file_name {
            let path = Path::new(backup_storage);
            if path.exists() {
                if let Err(_e) = remove_file(path) {
                    error!("cannot reclaim segment file on dispense {}", backup_storage)
                }
            } else {
                error!("cannot find segment backup to dispense {}", backup_storage)
            }
        }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        debug!("Memory dropping segment {}", self.id);
        assert_eq!(self.references.load(Ordering::Relaxed), 0);
        self.mem_drop()
    }
}

pub struct SegmentEntryIter {
    bound: usize,
    cursor: usize,
}

impl Iterator for SegmentEntryIter {
    type Item = EntryMeta;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let cursor = self.cursor;
        if cursor >= self.bound {
            return None;
        }
        let (_, entry_meta) = entry::Entry::decode_from(cursor, |body_pos, header| {
            let entry_header_size = body_pos - cursor;
            let entry_size = entry_header_size + header.content_length as usize;
            debug!("Found body pos {}, entry header {:?}. Header size: {}, entry size: {}, entry pos: {}, content length {}, bound {}",
                       body_pos, header, entry_header_size, entry_size, cursor, header.content_length, self.bound);
            return EntryMeta {
                body_pos,
                entry_header: header,
                entry_size,
                entry_pos: cursor,
            };
        });
        self.cursor += entry_meta.entry_size;
        Some(entry_meta)
    }
}
