use crate::ram::chunk::Chunk;
use crate::ram::entry;
use crate::ram::entry::EntryMeta;
use crate::ram::io::align_address;
use crate::ram::tombstone::TOMBSTONE_SIZE_U32;
use libc::*;
use lightning::list::LinkedRingBufferList;
use parking_lot;
use std::fs::{copy, create_dir_all, remove_file, File};
use std::{io, slice};
use std::io::prelude::*;
use std::io::BufWriter;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicUsize, Ordering, Ordering::*};

use super::entry::ENTRY_HEAD_SIZE;

pub const SEGMENT_SIZE_U32: u32 = 8 * 1024 * 1024;
pub const SEGMENT_SIZE: usize = SEGMENT_SIZE_U32 as usize;
pub const SEGMENT_MASK: usize = !(SEGMENT_SIZE - 1);
pub const SEGMENT_BITS_SHIFT: u32 = SEGMENT_SIZE.trailing_zeros();

#[derive(Default)]
#[repr(C, align(64))] // Ensure consistent memory layout and cache line alignment
pub struct Segment {
    pub id: u64,
    pub seq_id: u64,
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
    // Tiered memory fields
    pub cold_file_fd: AtomicI32,  // -1 = hot, >= 0 = file descriptor for cold
    pub reference_bit: AtomicBool, // For CLOCK eviction algorithm
    pub promoting: AtomicBool,      // True when promotion is in progress
    // Padding to maintain struct size (prevents cache line sharing issues)
    _padding: [u8; 8], // 8 bytes padding to keep struct at 128 bytes
}

impl Segment {
    pub fn new(
        id: u64,
        seq_id: u64,
        chunk_id: usize,
        buffer_ptr: usize,
        backup_storage: &Option<String>,
        wal_storage: &Option<String>,
    ) -> Segment {
        let mut wal_file_name = None;
        let mut wal_file = None;
        let size = SEGMENT_SIZE;
        if let Some(backup_storage) = backup_storage {
            create_dir_all(backup_storage).unwrap();
        }
        if let Some(wal_storage) = wal_storage {
            create_dir_all(wal_storage).unwrap();
            let file_name = format!("{}/{}-{}-{}.nlog", wal_storage, chunk_id, id, seq_id);
            let file = BufWriter::with_capacity(
                4096, // most common disk block size
                File::create(&file_name).unwrap(),
            ); // fast fail
            wal_file_name = Some(file_name);
            wal_file = Some(parking_lot::Mutex::new(file));
        }
        debug!(
            "Creating new segment chunk {}, id {}, seq_id {}, size {}, address {}",
            chunk_id, id, seq_id, size, buffer_ptr
        );
        Segment {
            addr: buffer_ptr,
            id,
            seq_id,
            bound: buffer_ptr + size,
            append_header: AtomicUsize::new(buffer_ptr),
            dead_space: AtomicU32::new(0),
            tombstones: AtomicU32::new(0),
            dead_tombstones: AtomicU32::new(0),
            last_tombstones_scanned: AtomicI64::new(0),
            references: AtomicUsize::new(0),
            backup_file_name: backup_storage
                .clone()
                .map(|path| format!("{}/{}-{}-{}.nbackup", path, chunk_id, id, seq_id)),
            wal_file,
            wal_file_name,
            archived: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
            cold_file_fd: AtomicI32::new(-1),  // Start as hot
            reference_bit: AtomicBool::new(false),
            promoting: AtomicBool::new(false),
            _padding: [0u8; 8], // Initialize padding
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
                    debug_assert_eq!(
                        align_address(8, curr_last),
                        curr_last,
                        "Acquired address is not aligned"
                    );
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
        debug!("archive() called for segment {}, backup_file_name={:?}", self.id, self.backup_file_name);
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
            
            // Ensure parent directory exists before creating backup file
            if let Some(parent) = backup_file_path.parent() {
                create_dir_all(parent)?;
            }
            
            if let Some(ref wal_file) = self.wal_file_name {
                // if there is a WAL file ready, copy this file to backup
                if let Some(ref file_mutex) = self.wal_file {
                    // this should be redundant but I don't want to take the chance
                    // obtain the writer lock before continue
                    let mut writer = file_mutex.lock();
                    writer.flush()?;
                    writer.get_ref().sync_all()?;
                    drop(writer);
                    copy(wal_file, backup_file)?;
                    // Sync the backup file after copy
                    let backup_file_handle = File::open(backup_file_path)?;
                    backup_file_handle.sync_all()?;
                    remove_file(wal_file)?;
                    return Ok(true);
                } else {
                    panic!()
                }
            } else {
                let backup_file = File::create(backup_file_path)?;
                let seg_size = self.append_header.load(Ordering::Relaxed) - self.addr;
                let mut buffer = BufWriter::with_capacity(seg_size, backup_file);
                unsafe {
                    let data_block = slice::from_raw_parts(self.addr as *const u8, seg_size);
                    buffer.write(data_block)?;
                }
                buffer.flush()?;
                buffer.get_ref().sync_all()?;
                return Ok(true);
            }
        }
        return Ok(false);
    }

    pub fn write_wal(&self, addr: usize, size: u32, skip_sync: bool) -> io::Result<()> {
        if let Some(ref wal_file) = self.wal_file {
            let mut file = wal_file.lock();
            unsafe {
                let data_block = slice::from_raw_parts(addr as *const u8, size as usize);
                file.write(data_block)?;
            }
            file.flush()?;
            // Skip fsync for transactional writes - they will be synced during commit
            if !skip_sync {
                file.get_ref().sync_data()?;
                trace!("WAL synced for segment {} (non-transactional write)", self.id);
            } else {
                trace!("WAL sync skipped for segment {} (transactional write, will sync at commit)", self.id);
            }
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
                warn!("cannot find segment backup to dispense {}", backup_storage)
            }
        }
    }
    
    // Tiered memory helper methods (stubs when tiered memory is disabled)
    
    /// Check if segment is hot (in anonymous memory)
    /// Always returns true when tiered memory is disabled
    #[inline]
    pub fn is_hot(&self) -> bool {
        self.cold_file_fd.load(Ordering::Relaxed) == -1
    }
    
    /// Check if segment is cold (backed by file mmap)
    /// Always returns false when tiered memory is disabled
    #[inline]
    pub fn is_cold(&self) -> bool {
        self.cold_file_fd.load(Ordering::Relaxed) >= 0
    }
    
    /// Mark segment as recently accessed (for CLOCK algorithm)
    /// No-op when tiered memory is disabled
    #[inline]
    pub fn mark_referenced(&self) {
        self.reference_bit.store(true, Ordering::Relaxed);
    }
    
    /// Clear reference bit and return old value (for CLOCK algorithm)
    /// Always returns false when tiered memory is disabled
    #[inline]
    pub fn clear_reference_bit(&self) -> bool {
        self.reference_bit.swap(false, Ordering::Relaxed)
    }
    
    /// Get current reference bit value without clearing
    /// Always returns false when tiered memory is disabled
    #[inline]
    pub fn get_reference_bit(&self) -> bool {
        self.reference_bit.load(Ordering::Relaxed)
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
            let entry_size = ENTRY_HEAD_SIZE + header.content_length as usize;
            debug!("Found body pos {}. Header: {:?}, entry size: {}, entry pos: {}, content length {}, bound {}",
                       body_pos, header, entry_size, cursor, header.content_length, self.bound);
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
    free: LinkedRingBufferList<usize, 64>,
    pub next_seq_id: AtomicUsize,
    chunk_id: usize,
}

impl SegmentAllocator {
    pub fn new(chunk_id: usize, chunk_size: usize) -> Self {
        Self::new_with_base(chunk_id, 0, chunk_size, true)
    }
    
    /// Create allocator with pre-allocated base address
    /// If allocate_memory=false, assumes memory at base_addr already exists
    pub fn new_with_base(
        chunk_id: usize,
        base_addr: usize,
        chunk_size: usize,
        allocate_memory: bool,
    ) -> Self {
        let (base, addr, limit) = if allocate_memory {
            // Old behavior: allocate our own mmap
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
            (aligned_addr, aligned_addr, aligned_addr + chunk_size)
        } else {
            // New behavior: use provided base from global allocation
            (base_addr, base_addr, base_addr + chunk_size)
        };
        
        Self {
            base,
            offset: AtomicUsize::new(addr),
            limit,
            gc_threshold: base + (chunk_size as f64 * 0.9) as usize - SEGMENT_SIZE,
            free: LinkedRingBufferList::new(),
            next_seq_id: AtomicUsize::new(0),
            chunk_id,
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
            .pop_front()
            .or_else(|| loop {
                debug!("Allocate segment by bump pointer");
                let addr = self.offset.load(Relaxed);
                let new_addr = addr + SEGMENT_SIZE;
                if new_addr > self.limit {
                    // Check the right boundary
                    return None;
                } else {
                    if self
                        .offset
                        .compare_exchange(addr, new_addr, AcqRel, Relaxed)
                        .is_ok()
                    {
                        return Some(addr);
                    }
                }
            })
            .map(|addr| {
                let id = self.id_by_addr(addr);
                let seq_id = self.next_seq_id.fetch_add(1, Ordering::AcqRel);
                Segment::new(id as u64, seq_id as u64, self.chunk_id, addr, backup_storage, wal_storage)
            })
    }

    /// Allocate a segment with a specific seq_id (for recovery purposes)
    /// This preserves the original seq_id from recovered files
    pub fn alloc_seg_with_seq_id(
        &self,
        seq_id: u64,
        backup_storage: &Option<String>,
        wal_storage: &Option<String>,
    ) -> Option<Segment> {
        // First allocate the address
        self.free
            .pop_front()
            .or_else(|| loop {
                debug!("Allocate segment by bump pointer (recovery)");
                let addr = self.offset.load(Relaxed);
                let new_addr = addr + SEGMENT_SIZE;
                if new_addr > self.limit {
                    return None;
                } else {
                    if self
                        .offset
                        .compare_exchange(addr, new_addr, AcqRel, Relaxed)
                        .is_ok()
                    {
                        return Some(addr);
                    }
                }
            })
            .map(|addr| {
                let id = self.id_by_addr(addr);
                // Use the provided seq_id instead of fetching a new one
                Segment::new(id as u64, seq_id, self.chunk_id, addr, backup_storage, wal_storage)
            })
    }

    pub fn free(&self, seg_addr: usize) {
        debug_assert!(seg_addr >= self.base);
        debug_assert!(seg_addr < self.limit);
        debug!("Segment {} freed", seg_addr);
        self.free.push_front(seg_addr);
    }

    pub fn id_by_addr(&self, addr: usize) -> usize {
        let offset = addr - self.base;
        let id = offset >> SEGMENT_BITS_SHIFT;
        id
    }
    
    #[inline]
    pub fn addr_by_id(&self, id: usize) -> usize {
        self.base + (id << SEGMENT_BITS_SHIFT)
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
