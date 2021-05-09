use crate::ram::chunk::Chunk;
use crate::ram::entry;
use crate::ram::entry::EntryMeta;
use crate::ram::tombstone::TOMBSTONE_SIZE_U32;
use libc::*;
use lightning::list::WordList;
use parking_lot;
use std::fs::{copy, create_dir_all, remove_file, File};
use std::io;
use std::io::prelude::*;
use std::io::BufWriter;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicU32, AtomicUsize, Ordering, Ordering::*,
};

pub const SEGMENT_SIZE_U32: u32 = 8 * 1024 * 1024;
pub const SEGMENT_SIZE: usize = SEGMENT_SIZE_U32 as usize;
pub const SEGMENT_MASK: usize = !(SEGMENT_SIZE - 1);
pub const SEGMENT_BITS_SHIFT: u32 = SEGMENT_SIZE.trailing_zeros();

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
        buffer_ptr: usize,
        backup_storage: &Option<String>,
        wal_storage: &Option<String>,
    ) -> Segment {
        let mut wal_file_name = None;
        let mut wal_file = None;
        let size = SEGMENT_SIZE;
        if let Some(wal_storage) = wal_storage {
            create_dir_all(wal_storage).unwrap();
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
            let curr_last = self.append_header.load(Ordering::Acquire);
            let exp_last = curr_last + size;
            if exp_last > self.bound {
                return None;
            } else {
                if self
                    .append_header
                    .compare_exchange(curr_last, exp_last, Ordering::AcqRel, Ordering::Relaxed)
                    .is_err()
                {
                    continue;
                } else {
                    return Some(curr_last);
                }
            }
        }
    }

    pub fn shrink(&self, size: usize) {
        debug_assert!(
            size < SEGMENT_SIZE,
            "Shrink to {} max {}",
            size,
            SEGMENT_SIZE
        );
        punch_hole(self.addr, size);
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
        debug_assert!(space <= SEGMENT_SIZE);
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
                    copy(wal_file, backup_file)?;
                    remove_file(wal_file)?;
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

    pub fn mem_drop(&self, chunk: &Chunk) {
        if self
            .dropped
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            chunk.allocator.free(self.addr);
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
            trace!("Found body pos {}, entry header {:?}. Header size: {}, entry size: {}, entry pos: {}, content length {}, bound {}",
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

pub const PAGE_SHIFT: usize = 12; // 4K
pub const PAGE_SIZE: usize = 1 << PAGE_SHIFT;

pub struct SegmentAllocator {
    base: usize,
    offset: AtomicUsize,
    limit: usize,
    gc_threshold: usize,
    free: WordList,
}

impl SegmentAllocator {
    pub fn new(chunk_size: usize) -> Self {
        let overflow = SEGMENT_SIZE - PAGE_SIZE;
        let aligned_size = chunk_size + overflow;
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                aligned_size,
                PROT_READ | PROT_WRITE,
                MAP_ANONYMOUS | MAP_PRIVATE,
                -1,
                0,
            )
        };
        let addr = ptr as usize;
        let start = addr + overflow;
        let aligned_addr = start & SEGMENT_MASK;
        Self {
            base: aligned_addr,
            offset: AtomicUsize::new(aligned_addr),
            limit: aligned_addr + chunk_size,
            gc_threshold: aligned_addr + (chunk_size as f64 * 0.9) as usize - SEGMENT_SIZE,
            free: WordList::with_capacity(chunk_size / SEGMENT_SIZE / 2),
        }
    }

    pub fn meet_gc_threshold(&self) -> bool {
        self.offset.load(Relaxed) > self.gc_threshold
    }

    pub fn alloc_seg(
        &self,
        backup_storage: &Option<String>,
        wal_storage: &Option<String>,
    ) -> Option<Segment> {
        self.free
            .pop()
            .or_else(|| loop {
                debug!("Allocate segment by bump pointer");
                let addr = self.offset.load(Relaxed);
                let new_addr = addr + SEGMENT_SIZE;
                if new_addr > self.limit {
                    // Check the right boundary
                    return None;
                } else {
                    if self.offset.compare_exchange(addr, new_addr, AcqRel, Relaxed).is_ok() {
                        return Some(addr);
                    }
                }
            })
            .map(|addr| {
                let id = self.id_by_addr(addr);
                Segment::new(id as u64, addr, backup_storage, wal_storage)
            })
    }

    pub fn free(&self, seg_addr: usize) {
        debug_assert!(seg_addr >= self.base);
        debug_assert!(seg_addr < self.limit);
        debug!("Segment {} freed", seg_addr);
        self.free.push(seg_addr);
    }

    pub fn id_by_addr(&self, addr: usize) -> usize {
        let offset = addr - self.base;
        let id = offset >> SEGMENT_BITS_SHIFT;
        id
    }
}

#[cfg(target_os = "linux")]
unsafe fn madvise_free(addr: usize, size: usize) {
    madvise(addr as *mut c_void, size, MADV_REMOVE);
}

#[cfg(not(target_os = "linux"))]
unsafe fn madvise_free(addr: usize, size: usize) {
    madvise(addr as *mut c_void, size, MADV_DONTNEED);
}

fn punch_hole(seg_addr: usize, seg_size: usize) {
    let right_boundary = seg_addr + seg_size;
    let aligned_addr = (((right_boundary - 1) >> PAGE_SHIFT) + 1) << PAGE_SHIFT;
    let hole_length = (seg_addr + SEGMENT_SIZE) - aligned_addr;
    if hole_length > PAGE_SIZE {
        // Have pages to release
        debug!(
            "Partially free the segment by puching hole with size {}",
            hole_length
        );
        unsafe {
            madvise_free(aligned_addr, hole_length);
        }
    }
}
