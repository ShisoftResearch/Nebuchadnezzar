use libc;
use std::thread;
use std::rc::Rc;
use std::sync::{Arc};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::collections::BTreeSet;
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard};
use concurrent_hashmap::ConcHashMap;
use ram::schema::Schemas;
use ram::segs::{Segment, SEGMENT_SIZE};
use ram::cell::{Cell, ReadError, WriteError};
use server::ServerMeta;

pub struct Chunk {
    pub id: usize,
    pub addr: usize,
    pub index: ConcHashMap<u64, Mutex<usize>>,
    pub segs: Vec<Segment>,
    pub seg_round: AtomicUsize,
    pub meta: Arc<ServerMeta>,
    pub backup_storage: Option<String>,
}

pub struct Chunks {
    pub list: Vec<Chunk>,
}

impl Chunk {
    fn new (id: usize, size: usize, meta: Arc<ServerMeta>, back_storage: Option<String>) -> Chunk {
        let mem_ptr = unsafe {libc::malloc(size)} as usize;
        let seg_count = size / SEGMENT_SIZE;
        let mut segments = Vec::<Segment>::new();
        for seg_idx in 0..seg_count {
            let seg_addr = mem_ptr + seg_idx * SEGMENT_SIZE;
            segments.push(Segment {
                addr: seg_addr,
                id: seg_idx,
                bound: seg_addr + SEGMENT_SIZE,
                append_header: AtomicUsize::new(seg_addr),
                lock: RwLock::new(()),
                frags: Mutex::new(BTreeSet::new()),
            });
        }
        debug!("creating chunk at {}, segments {}", mem_ptr, seg_count + 1);
        Chunk {
            id: id,
            addr: mem_ptr,
            index: ConcHashMap::<u64, Mutex<usize>>::new(),
            meta: meta,
            segs: segments,
            seg_round: AtomicUsize::new(0),
            backup_storage: back_storage
        }
    }
    pub fn try_acquire(&self, size: usize) -> Option<(usize, RwLockReadGuard<()>)> {
        let mut retried = 0;
        loop {
            let n = self.seg_round.load(Ordering::Relaxed);
            let seg_id = n % self.segs.len();
            let seg_acquire = self.segs[seg_id].try_acquire(size);
            match seg_acquire {
                None => {
                    if retried > self.segs.len() * 2 {return None;}
                    self.seg_round.fetch_add(1, Ordering::Relaxed);
                    retried += 1;
                },
                _ => {return seg_acquire;}
            }
        }
    }
    fn locate_segment(&self, location: usize) -> &Segment {
        let offset = location - self.addr;
        let seg_id = offset / SEGMENT_SIZE;
        return &self.segs[seg_id];
    }
    pub fn location_of(&self, hash: u64) -> Option<MutexGuard<usize>> {
        match self.index.find(&hash) {
            Some(index) => {
                let index = index.get();
                let index_lock = index.lock();
                if *index_lock == 0 {
                    return None
                }
                return Some(index_lock);
            },
            None => None
        }
    }
    fn put_tombstone(&self, location: usize) {
        let seg = self.locate_segment(location);
        seg.put_cell_tombstone(location);
        seg.put_frag(location);
    }
    fn read_cell(&self, hash: u64) -> Result<Cell, ReadError> {
        match self.location_of(hash) {
            Some(loc) => {
                Cell::from_chunk_raw(*loc, self)
            },
            None => Err(ReadError::CellDoesNotExisted)
        }
    }
    fn write_cell(&self, cell: &mut Cell) -> Result<usize, WriteError> {
        let hash = cell.header.hash;
        if self.location_of(hash).is_some() {
            return Err(WriteError::CellAlreadyExisted);
        } else {
            let written = cell.write_to_chunk(self);
            let need_rollback = AtomicBool::new(false);
            if let Ok(loc) = written {
                self.index.upsert(
                    hash,
                    Mutex::new(loc),
                    &|m| {
                        let mut inserted_loc = m.lock();
                        if *inserted_loc == 0 {
                            *inserted_loc = loc
                        } else {
                            need_rollback.store(true, Ordering::Relaxed);
                        }
                    }
                );
                if need_rollback.load(Ordering::Relaxed) {
                    self.put_tombstone(loc);
                    return Err(WriteError::CellAlreadyExisted)
                }
            }
            return written
        }
    }
    fn update_cell(&self, cell: &mut Cell) -> Result<usize, WriteError> {
        let hash = cell.header.hash;
        if let Some(mut cell_location) = self.location_of(hash) {
            let written = cell.write_to_chunk(self);
            if let Ok(new_location) = written {
                let old_location = *cell_location;
                *cell_location = new_location;
                self.put_tombstone(old_location);
            }
            return written;
        } else {
            return Err(WriteError::CellDoesNotExisted)
        }
    }
    fn update_cell_by<U>(&self, hash: u64, update: U) -> Result<usize, WriteError>
        where U: Fn(Cell) -> Option<Cell> {
        if let Some(mut cell_location) = self.location_of(hash) {
            let cell = Cell::from_chunk_raw(*cell_location, self);
            match cell {
                Ok(cell) => {
                    let mut new_cell = update(cell);
                    if let Some(mut new_cell) = new_cell {
                        let written = new_cell.write_to_chunk(self);
                        if let Ok(new_location) = written {
                            let old_location = *cell_location;
                            *cell_location = new_location;
                            self.put_tombstone(old_location);
                        }
                        return written;
                    } else {
                        return Err(WriteError::UserCanceledUpdate);
                    }
                },
                Err(e) => Err(WriteError::ReadError(e))
            }
        } else {
            return Err(WriteError::CellDoesNotExisted)
        }
    }
    fn remove_cell(&self, hash: u64) -> Result<(), WriteError> {
        if let Some(cell_location) = self.location_of(hash) {
            self.index.remove(&hash);
            self.put_tombstone(*cell_location);
            return Ok(());
        } else {
            return Err(WriteError::CellDoesNotExisted);
        }
    }
    fn remove_cell_by<P>(&self, hash: u64, predict: P) -> Result<(), WriteError>
        where P: Fn(Cell) -> bool {
        if let Some(cell_location) = self.location_of(hash) {
            let cell = Cell::from_chunk_raw(*cell_location, self);
            match cell {
                Ok(cell) => {
                    if predict(cell) {
                        self.index.remove(&hash);
                        self.put_tombstone(*cell_location);
                        return Ok(());
                    } else {
                        return Err(WriteError::DeletionPredictionFailed);
                    }
                },
                Err(e) => Err(WriteError::ReadError(e))
            }
        } else {
            return Err(WriteError::CellDoesNotExisted);
        }
    }
    fn dispose (&mut self) {
        debug!("disposing chunk at {}", self.addr);
        unsafe {
            libc::free(self.addr as *mut libc::c_void)
        }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        self.dispose();
    }
}

impl Chunks {
    pub fn new (count: usize, size: usize, meta: Arc<ServerMeta>, backup_storage: Option<String>) -> Chunks {
        let chunk_size = size / count;
        let mut chunks = Vec::new();
        debug!("Creating {} chunks, total {} bytes", count, size);
        for i in 0..count {
            let backup_storage = match backup_storage {
                Some(ref dir) => Some(format!("{}/data-{}.bak", dir, i)),
                None => None
            };
            chunks.push(Chunk::new(i, chunk_size, meta.clone(), backup_storage));
        }
        Chunks {
            list: chunks
        }
    }
    pub fn new_dummy(count: usize, size: usize) -> Chunks {
        Chunks::new(count, size, Arc::<ServerMeta>::new(ServerMeta {
            schemas: Schemas::new(None)
        }), None)
    }
    fn locate_chunk_by_partition(&self, partition: u64) -> &Chunk {
        let chunk_id = partition as usize % self.list.len();
        return &self.list[chunk_id];
    }
    fn locate_chunk_by_key(&self, key: (u64, u64)) -> (&Chunk, u64) {
        let (partition, hash) = key;
        return (self.locate_chunk_by_partition(partition), hash);
    }
    pub fn read_cell(&self, key: (u64, u64)) -> Result<Cell, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_cell(hash);
    }
    pub fn write_cell(&self, cell: &mut Cell) -> Result<usize, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.write_cell(cell);
    }
    pub fn update_cell(&self, cell: &mut Cell) -> Result<usize, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.update_cell(cell);
    }
    pub fn update_cell_by<U>(&self, key: (u64, u64), update: U) -> Result<usize, WriteError>
        where U: Fn(Cell) -> Option<Cell>{
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.update_cell_by(hash, update);
    }
    pub fn remove_cell(&self, key: (u64, u64)) -> Result<(), WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell(hash);
    }
    pub fn remove_cell_by<P>(&self, key: (u64, u64), predict: P) -> Result<(), WriteError>
        where P: Fn(Cell) -> bool {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell_by(hash, predict);
    }
}