use std::sync::{Arc};
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::collections::BTreeMap;
use std::ops::Bound::Included;
use parking_lot::{Mutex, RwLock};
use bifrost::utils::async_locks::{RwLockReadGuard as AsyncRwLockReadGuard};
use bifrost::utils::time::get_time;
use chashmap::{CHashMap, ReadGuard, WriteGuard};
use ram::schema::LocalSchemasCache;
use ram::types::{Id, Value};
use ram::segs::{Segment, SEGMENT_SIZE};
use ram::cell::{Cell, ReadError, WriteError, CellHeader};
use ram::tombstone::{Tombstone, TOMBSTONE_SIZE, TOMBSTONE_SIZE_U32};
use ram::repr;
use utils::ring_buffer::RingBuffer;
use server::ServerMeta;

pub type CellReadGuard<'a> = ReadGuard<'a, u64, usize>;
pub type CellWriteGuard<'a> = WriteGuard<'a, u64, usize>;

pub struct Chunk {
    pub id: usize,
    pub index: CHashMap<u64, usize>,
    // Used only for locating segment for address
    // when putting tombstone, not normal data access
    pub addrs_seg: RwLock<BTreeMap<usize, u64>>,
    pub segs: CHashMap<u64, Arc<Segment>>,
    pub seg_counter: AtomicU64,
    pub head_seg: RwLock<Arc<Segment>>,
    pub meta: Arc<ServerMeta>,
    pub backup_storage: Option<String>,
    pub total_space: AtomicUsize,
    pub dead_entries: RingBuffer,
    pub capacity: usize
}

impl Chunk {
    fn new (id: usize, size: usize, meta: Arc<ServerMeta>, backup_storage: Option<String>) -> Chunk {
        let first_seg_id = 0;
        let bootstrap_segment_ref = Arc::new(Segment::new(first_seg_id, SEGMENT_SIZE, &backup_storage));
        let segs = CHashMap::new();
        let index = CHashMap::new();
        let chunk = Chunk {
            id,
            segs,
            index,
            meta,
            backup_storage,
            capacity: size,
            total_space: AtomicUsize::new(0),
            seg_counter: AtomicU64::new(0),
            head_seg: RwLock::new(bootstrap_segment_ref.to_owned()),
            addrs_seg: RwLock::new(BTreeMap::new()),
            dead_entries: RingBuffer::new(size / SEGMENT_SIZE * 10)
        };
        chunk.put_segment(bootstrap_segment_ref);
        return chunk;
    }

    pub fn try_acquire(&self, size: u32) -> Option<(usize, Arc<Segment>)> {
        let mut retried = 0;
        loop {
            let head = self.head_seg.read().clone();
            let mut head_seg_id = head.id;
            match head.try_acquire(size) {
                Some(pair) => return Some((pair, head)),
                None => {
                    if self.total_space.load(Ordering::Relaxed) >= self.capacity - SEGMENT_SIZE {
                        // No space left
                        return None;
                    }
                    let mut acquired_header = self.head_seg.write();
                    if head_seg_id == acquired_header.id {
                        // head segment did not changed and locked, suitable for creating a new segment and point it to
                        let new_seg_id = self.seg_counter.fetch_add(1, Ordering::Relaxed);
                        let new_seg = Segment::new(new_seg_id, SEGMENT_SIZE, &self.backup_storage);
                        // for performance, won't CAS total_space
                        self.total_space.fetch_add(SEGMENT_SIZE, Ordering::Relaxed);
                        let new_seg_ref = Arc::new(new_seg);
                        self.put_segment(new_seg_ref.clone());
                        *acquired_header = new_seg_ref;
                    }
                    // whether the segment acquisition success or not,
                    // try to get the new segment and try again
                }
            }
        }
    }

    pub fn location_for_read<'a>(&self, hash: u64)
                                 -> Result<CellReadGuard, ReadError> {
        match self.index.get(&hash) {
            Some(index) => {
                if *index == 0 {
                    return Err(ReadError::CellDoesNotExisted)
                }
                return Ok(index);
            },
            None => if hash == 0 {
                Err(ReadError::CellIdIsUnitId)
            } else {
                Err(ReadError::CellDoesNotExisted)
            }
        }
    }

    pub fn location_for_write(&self, hash: u64)
                              -> Option<CellWriteGuard> {
        match self.index.get_mut(&hash) {
            Some(index) => {
                if *index == 0 {
                    return None
                }
                return Some(index);
            },
            None => None
        }
    }

    fn head_cell(&self, hash: u64) -> Result<CellHeader, ReadError> {
        Cell::header_from_chunk_raw(*self.location_for_read(hash)?).map(|pair| pair.0)
    }

    fn read_cell(&self, hash: u64) -> Result<Cell, ReadError> {
        Cell::from_chunk_raw(*self.location_for_read(hash)?, self)
    }

    fn read_selected(&self, hash: u64, fields: &[u64]) -> Result<Vec<Value>, ReadError> {
        let loc = self.location_for_read(hash)?;
        let selected_data = Cell::select_from_chunk_raw(*loc, self, fields)?;
        let mut result = Vec::with_capacity(fields.len());
        match selected_data {
            Value::Map(map) => {
                for field_id in fields {
                    result.push(map.get_by_key_id(*field_id).clone())
                }
                return Ok(result)
            },
            _ => Err(ReadError::CellTypeIsNotMapForSelect)
        }
    }

    fn read_partial_raw(&self, hash: u64, offset: usize, len: usize) -> Result<Vec<u8>, ReadError> {
        let loc = self.location_for_read(hash)?;
        let mut head_ptr = *loc + offset;
        let mut data = Vec::with_capacity(len);
        for ptr in head_ptr..(head_ptr + len) {
            data.push(unsafe {(*(ptr as *const u8))});
        }
        Ok(data.to_vec())
    }

    fn write_cell(&self, cell: &mut Cell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.hash;
        if self.location_for_read(hash).is_ok() {
            return Err(WriteError::CellAlreadyExisted);
        } else {
            let loc = cell.write_to_chunk(self)?;
            let mut need_rollback = false;
            self.index.upsert(
                hash,
                ||{loc},
                |inserted_loc| {
                    if *inserted_loc == 0 {
                        *inserted_loc = loc
                    } else {
                        need_rollback = true;
                    }
                }
            );
            if need_rollback {
                return Err(WriteError::CellAlreadyExisted)
            }
            return Ok(cell.header)
        }
    }

    fn update_cell(&self, cell: &mut Cell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.hash;
        if let Some(mut cell_location) = self.location_for_write(hash) {
            let old_location = *cell_location;
            let new_location = cell.write_to_chunk(self)?;
            *cell_location = new_location;
            self.mark_dead_entry(old_location);
            return Ok(cell.header);
        } else {
            return Err(WriteError::CellDoesNotExisted)
        }
    }
    fn update_cell_by<U>(&self, hash: u64, update: U) -> Result<Cell, WriteError>
        where U: Fn(Cell) -> Option<Cell> {
        if let Some(mut cell_location) = self.location_for_write(hash) {
            let cell = Cell::from_chunk_raw(*cell_location, self);
            match cell {
                Ok(cell) => {
                    let mut new_cell = update(cell);
                    if let Some(mut new_cell) = new_cell {
                        let old_location = *cell_location;
                        let new_location = new_cell.write_to_chunk(self)?;
                        *cell_location = new_location;
                        self.mark_dead_entry(old_location);
                        return Ok(new_cell);
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
        if let Some(cell_location) = self.index.remove(&hash) {
            self.put_tombstone_by_cell_loc(cell_location)?;
            Ok(())
        } else {
            Err(WriteError::CellDoesNotExisted)
        }
    }
    fn remove_cell_by<P>(&self, hash: u64, predict: P) -> Result<(), WriteError>
        where P: Fn(Cell) -> bool {
        let mut result = Ok(());
        self.index.alter(hash, |loc_opt|{
            match loc_opt {
                Some(cell_location) => {
                    let cell = Cell::from_chunk_raw(cell_location, self);
                    match cell {
                        Ok(cell) => {
                            if predict(cell) {
                                let put_tombstone_result = self.put_tombstone_by_cell_loc(cell_location);
                                if put_tombstone_result.is_err() {
                                    result = put_tombstone_result;
                                    loc_opt
                                } else {
                                    None
                                }
                            } else {
                                result = Err(WriteError::CellDoesNotExisted);
                                loc_opt
                            }
                        },
                        Err(e) => {
                            result = Err(WriteError::ReadError(e));
                            None
                        }
                    }
                },
                None => {
                    result = Err(WriteError::CellDoesNotExisted);
                    None
                }
            }
        });
        return result;
    }

    pub fn put_segment(&self, segment: Arc<Segment>) {
        let segment_id = segment.id;
        let segment_addr = segment.addr;
        self.addrs_seg.write().insert(segment_addr, segment_id);
        self.segs.insert(segment_id, segment);
    }

    pub fn remove_segment(&self, segment_id: u64) {
        if let Some(seg) = self.segs.remove(&segment_id) {
            self.addrs_seg.write().remove(&seg.addr).unwrap();
            seg.dispense();
        }
    }

    fn locate_segment(&self, addr: usize) -> Option<Arc<Segment>> {
        let segs_addr_range = self.addrs_seg.read();
        debug!("locating segment addr {} in {:?}", addr, *segs_addr_range);
        segs_addr_range
            .range((Included(addr - SEGMENT_SIZE), Included(addr)))
            .last()
            .and_then(|(_, seg_id)| {
                self.segs
                    .get(seg_id)
                    .map(|guard| guard.clone())
                    .and_then(|seg| {
                        if addr < seg.bound {
                            return Some(seg);
                        } else {
                            return None;
                        }
                    })
            })
    }

    #[inline]
    fn put_tombstone(&self, cell_location: usize,cell_header: &CellHeader) {
        let cell_seg = self
            .locate_segment(cell_location)
            .expect(format!("cannot locate cell segment for tombstone. Cell id: {:?} at {}",
                            cell_header.id(), cell_location).as_str());
        let (tombstone_addr, head_seg) = (||{
            loop {
                if let Some(pair) = self.try_acquire(TOMBSTONE_SIZE_U32) {
                    return pair;
                }
                warn!("Chunk {} is too full to put a tombstone. Will retry.", self.id)
            }
        })();
        Tombstone::put(
            tombstone_addr, cell_seg.id,
            cell_header.version,
            cell_header.partition,
            cell_header.hash
        );
        head_seg.tombstones.fetch_add(1, Ordering::Relaxed);
    }

    fn put_tombstone_by_cell_loc(&self, cell_location: usize) -> Result<(), WriteError> {
        let header =
            Cell::header_from_chunk_raw(cell_location)
                .map_err(|e| WriteError::ReadError(e))?.0;
        self.put_tombstone(cell_location, &header);
        self.mark_dead_entry(cell_location);
        Ok(())
    }

    // put dead entry address in a ideally non-blocking queue and wait for a worker to
    // make the changes in corresponding segments.
    // Because calculate segment from location is computation intensive, it have to be done lazily
    #[inline]
    fn mark_dead_entry(&self, loc: usize) {
        self.dead_entries.push(loc);
    }

    pub fn segments(&self) -> Vec<Arc<Segment>> {
        let addr_segs = self.addrs_seg.read();
        let segs = addr_segs.values();
        let mut list = Vec::with_capacity(self.segs.len());
        for seg_id in segs {
            if let Some(segment) = self.segs.get(seg_id){
                list.push(segment.clone());
            }
        }
        return list;
    }

    // this function should be invoked repeatedly to flush the queue
    pub fn apply_dead_entry(&self) {
        let marks = self.dead_entries.iter();
        for addr in marks {
            if let Some(seg) = self.locate_segment(addr) {
                let (entry, _) = repr::Entry::decode_from(addr, |_, _| {});
                seg.dead_space.fetch_add(entry.content_length, Ordering::Relaxed);
            }
        }
    }

    #[inline]
    pub fn segment_ids(&self) -> Vec<u64> {
        let addrs_guard = self.addrs_seg.read();
        addrs_guard.values().cloned().collect()
    }

    // Scan for dead tombstone. This will scan the whole segment, decoding all entry header
    // and looking for those with entry type tombstone.
    // It is resource intensive so there will be some rules to skip the scan.
    // This function should be invoked repeatedly by cleaner
    // Actual cleaning will be performed by cleaner regardless tombstone survival condition
    pub fn scan_tombstone_survival(&self) {
        let seg_ids = self.segment_ids();
        for seg_id in seg_ids {
            if let Some(segment) = self.segs.get(&seg_id){
                let now = get_time();
                let tombstones = segment.tombstones.load(Ordering::Relaxed);
                let dead_tombstones = segment.dead_tombstones.load(Ordering::Relaxed);
                let mut death_count = 0;
                if  // have not much tombstones
                    (tombstones as f64) * (TOMBSTONE_SIZE as f64) < (SEGMENT_SIZE as f64) * 0.2 ||
                        // large partition have been scanned
                        (dead_tombstones as f32 / tombstones as f32) > 0.8 ||
                        // have been scanned recently
                        now - segment.last_tombstones_scanned.load(Ordering::Relaxed) < 5000 {
                    continue;
                }
                for entry_meta in segment.entry_iter() {
                    if entry_meta.entry_header.entry_type == repr::EntryType::Tombstone {
                        let tombstone = Tombstone::read(entry_meta.body_pos);
                        if !self.segs.contains_key(&tombstone.segment_id) {
                            // segment that the tombstone pointed to have been cleaned by compact or combined cleaner
                            death_count += 1;
                        }
                    }
                }
                segment.dead_tombstones.fetch_add(death_count, Ordering::Relaxed);
                segment.last_tombstones_scanned.store(now, Ordering::Relaxed);
            } else {
                warn!("leaked segment in addrs_seg: {}", seg_id)
            }
        }
    }

    pub fn segs_for_compact_cleaner(&self) -> Vec<Arc<Segment>> {
        let utilization_selection =
            self.segments().into_iter()
                .map(|seg| {
                    let rate = seg.living_rate();
                    (seg, rate)
                })
                .filter(|(_, utilization)| *utilization < 80f32);
        let head_seg = self.head_seg.read();
        let mut list: Vec<_> = utilization_selection
            .filter(|(seg, _)| seg.id != head_seg.id)
            .collect();
        list.sort_by(|pair1, pair2|
            pair1.1.partial_cmp(&pair2.1).unwrap());
        return list.into_iter().map(|pair| pair.0).collect();
    }

    pub fn check_and_archive_segments(&self) {
        let seg_ids = self.segment_ids();
        let head_id = self.head_seg.read().id;
        for seg_id in seg_ids {
            if seg_id == head_id { continue; } // never archive head segments
            if let Some(segment) = self.segs.get(&seg_id) {
                if !segment.archived.compare_and_swap(false, true, Ordering::Relaxed) {
                    if let Err(e) = segment.archive() {
                        error!("cannot archive segment {}, reason:{:?}", self.id, e)
                    }
                }
            }
        }
    }
}

pub struct Chunks {
    pub list: Vec<Chunk>,
}



impl Chunks {
    pub fn new (count: usize, size: usize, meta: Arc<ServerMeta>, backup_storage: Option<String>) -> Arc<Chunks> {
        let chunk_size = size / count;
        let mut chunks = Vec::new();
        debug!("Creating {} chunks, total {} bytes", count, size);
        for i in 0..count {
            let backup_storage = match backup_storage {
                Some(ref dir) => Some(format!("{}/chunk-{}", dir, i)),
                None => None
            };
            chunks.push(Chunk::new(i, chunk_size, meta.clone(), backup_storage));
        }
        Arc::new(Chunks {
            list: chunks
        })
    }
    pub fn new_dummy(count: usize, size: usize) -> Arc<Chunks> {
        Chunks::new(count, size, Arc::<ServerMeta>::new(ServerMeta {
            schemas: LocalSchemasCache::new("", None).unwrap()
        }), None)
    }
    fn locate_chunk_by_partition(&self, partition: u64) -> &Chunk {
        let chunk_id = partition as usize % self.list.len();
        return &self.list[chunk_id];
    }
    fn locate_chunk_by_key(&self, key: &Id) -> (&Chunk, u64) {
        return (self.locate_chunk_by_partition(key.higher), key.lower);
    }
    pub fn read_cell(&self, key: &Id) -> Result<Cell, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_cell(hash);
    }
    pub fn read_selected(&self, key: &Id, fields: &[u64]) -> Result<Vec<Value>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_selected(hash, fields)
    }
    pub fn read_partial_raw(&self, key: &Id, offset: usize, len: usize) -> Result<Vec<u8>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_partial_raw(hash, offset, len)
    }
    pub fn head_cell(&self, key: &Id) -> Result<CellHeader, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.head_cell(hash);
    }
    pub fn location_for_read(&self, key: &Id) -> Result<CellReadGuard, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        chunk.location_for_read(hash)
    }
    pub fn write_cell(&self, cell: &mut Cell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.write_cell(cell);
    }
    pub fn update_cell(&self, cell: &mut Cell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.update_cell(cell);
    }
    pub fn update_cell_by<U>(&self, key: &Id, update: U) -> Result<Cell, WriteError>
        where U: Fn(Cell) -> Option<Cell>{
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.update_cell_by(hash, update);
    }
    pub fn remove_cell(&self, key: &Id) -> Result<(), WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell(hash);
    }
    pub fn remove_cell_by<P>(&self, key: &Id, predict: P) -> Result<(), WriteError>
        where P: Fn(Cell) -> bool {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell_by(hash, predict);
    }
    pub fn chunk_ptr(&self, key: &Id) -> usize {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return *chunk.location_for_read(hash).unwrap()
    }
}