use crate::ram::cell::{Cell, CellHeader, ReadError, WriteError};
use crate::ram::cleaner::Cleaner;
use crate::ram::entry::{Entry, EntryContent, EntryType};
use crate::ram::schema::LocalSchemasCache;
use crate::ram::segs::{Segment, SegmentAllocator, SEGMENT_SIZE, SEGMENT_SIZE_U32};
use crate::ram::tombstone::{Tombstone, TOMBSTONE_ENTRY_SIZE, TOMBSTONE_SIZE};
use crate::ram::types::{Id, Value};
use crate::server::ServerMeta;
use crate::utils::raii_mutex_table::RAIIMutexTable;

#[cfg(feature = "fast_map")]
use crate::utils::upper_power_of_2;
use bifrost::utils::time::get_time;
#[cfg(feature = "slow_map")]
use chashmap::*;
use lightning::map::*;
use parking_lot::Mutex;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use lightning::linked_map::{LinkedObjectMap, NodeRef as MapNodeRef};

#[cfg(feature = "fast_map")]
pub type CellReadGuard<'a> = lightning::map::WordMutexGuard<'a>;
#[cfg(feature = "fast_map")]
pub type CellWriteGuard<'a> = lightning::map::WordMutexGuard<'a>;

#[cfg(feature = "slow_map")]
pub type CellReadGuard<'a> = chashmap::ReadGuard<'a, u64, usize>;
#[cfg(feature = "slow_map")]
pub type CellWriteGuard<'a> = chashmap::WriteGuard<'a, u64, usize>;

pub struct Chunk {
    pub id: usize,
    #[cfg(feature = "fast_map")]
    pub index: Arc<WordMap>,
    #[cfg(feature = "fast_map")]
    pub segs: Arc<LinkedObjectMap<Segment>>,
    #[cfg(feature = "slow_map")]
    pub index: Arc<CHashMap<u64, usize>>,
    #[cfg(feature = "slow_map")]
    pub segs: Arc<CHashMap<usize, Segment>>,
    pub seg_counter: AtomicU64,
    pub head_seg_id: AtomicU64,
    pub meta: Arc<ServerMeta>,
    pub backup_storage: Option<String>,
    pub wal_storage: Option<String>,
    pub total_space: AtomicUsize,
    pub capacity: usize,
    pub unstable_cells: RAIIMutexTable<u64>,
    pub gc_lock: Mutex<()>,
    pub allocator: SegmentAllocator,
    pub alloc_lock: Mutex<()>,
}

impl Chunk {
    fn new(
        id: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
    ) -> Chunk {
        let allocator = SegmentAllocator::new(size);
        let first_seg_id = 0;
        let bootstrap_segment = allocator.alloc_seg(
            first_seg_id,
            &backup_storage,
            &wal_storage,
        ).unwrap();
        let num_segs = {
            let n = size / SEGMENT_SIZE;
            if n > 0 {
                n
            } else {
                n + 1
            }
        };
        #[cfg(feature = "fast_map")]
        let segs = LinkedObjectMap::with_capacity(upper_power_of_2(num_segs));
        #[cfg(feature = "fast_map")]
        let index = WordMap::with_capacity(64);
        #[cfg(feature = "slow_map")]
        let segs = CHashMap::new();
        #[cfg(feature = "slow_map")]
        let index = CHashMap::new();

        let segs = Arc::new(segs);
        let index = Arc::new(index);

        let chunk = Chunk {
            id,
            segs,
            index,
            meta,
            backup_storage,
            wal_storage,
            allocator,
            capacity: size,
            total_space: AtomicUsize::new(0),
            seg_counter: AtomicU64::new(0),
            head_seg_id: AtomicU64::new(bootstrap_segment.id),
            unstable_cells: RAIIMutexTable::new(),
            gc_lock: Mutex::new(()),
            alloc_lock: Mutex::new(()),
        };
        chunk.put_segment(bootstrap_segment);
        return chunk;
    }

    fn get_head_seg_id(&self) -> u64 {
        self.head_seg_id.load(Ordering::Relaxed)
    }

    pub fn try_acquire(&self, size: u32) -> Option<PendingEntry> {
        let mut tried_gc = false;
        loop {
            let head_seg_id = self.get_head_seg_id() as usize;
            let head = self.segs.get(&head_seg_id).expect("Cannot get header");
            match head.try_acquire(size) {
                Some(addr) => {
                    debug!(
                        "Chunk {} acquired address {} for size {} in segment {}",
                        self.id, addr, size, head.id
                    );
                    head.references.fetch_add(1, Ordering::Relaxed);
                    return Some(PendingEntry {
                        addr,
                        seg: head,
                        size,
                    });
                }
                None => {
                    drop(head);
                    if self.total_space.load(Ordering::Relaxed) >= self.capacity - SEGMENT_SIZE {
                        // No space left
                        if tried_gc {
                            return None;
                        } else {
                            debug!("No space left for chunk {}, emergency full GC", self.id);
                            Cleaner::clean(self, true);
                            tried_gc = true;
                            continue;
                        }
                    }
                    let _alloc_guard = self.alloc_lock.lock();
                    let header_id = self.get_head_seg_id() as usize;
                    if head_seg_id == header_id {
                        // head segment did not changed and locked, suitable for creating a new segment and point it to
                        let new_seg_id = self.next_segment_id();
                        let new_seg_opt = SegmentAllocator::alloc(
                            self,
                            new_seg_id,
                            &self.backup_storage,
                            &self.wal_storage
                        );
                        let new_seg = new_seg_opt.expect("No space left after full GCs");
                        // for performance, won't CAS total_space
                        self.total_space
                            .fetch_add(SEGMENT_SIZE, Ordering::Relaxed);
                        let new_seg_id = new_seg.id;
                        self.put_segment(new_seg);
                        self.head_seg_id.store(new_seg_id, Ordering::Relaxed);
                    }
                    // whether the segment acquisition success or not,
                    // try to get the new segment and try again
                }
            }
        }
    }

    pub fn next_segment_id(&self) -> u64 {
        self.seg_counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn location_for_read<'a>(&self, hash: u64) -> Result<CellReadGuard, ReadError> {
        #[cfg(feature = "fast_map")]
        let guard = self.index.lock(hash as usize);
        #[cfg(feature = "slow_map")]
        let guard = self.index.get(&hash);
        match guard {
            Some(index) => {
                if *index == 0 {
                    return Err(ReadError::CellDoesNotExisted);
                }
                return Ok(index);
            }
            None => {
                if hash == 0 {
                    Err(ReadError::CellIdIsUnitId)
                } else {
                    Err(ReadError::CellDoesNotExisted)
                }
            }
        }
    }

    pub fn location_for_write(&self, hash: u64) -> Option<CellWriteGuard> {
        #[cfg(feature = "fast_map")]
        let guard = self.index.lock(hash as usize);
        #[cfg(feature = "slow_map")]
        let guard = self.index.get_mut(&hash);

        match guard {
            Some(index) => {
                if *index == 0 {
                    return None;
                }
                return Some(index);
            }
            None => None,
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
                return Ok(result);
            }
            _ => Err(ReadError::CellTypeIsNotMapForSelect),
        }
    }

    fn read_partial_raw(&self, hash: u64, offset: usize, len: usize) -> Result<Vec<u8>, ReadError> {
        let loc = self.location_for_read(hash)?;
        let head_ptr = *loc + offset;
        let mut data = Vec::with_capacity(len);
        for ptr in head_ptr..(head_ptr + len) {
            data.push(unsafe { *(ptr as *const u8) });
        }
        Ok(data.to_vec())
    }

    #[cfg(feature = "fast_map")]
    fn write_cell(&self, cell: &mut Cell) -> Result<CellHeader, WriteError> {
        debug!("Writing cell {:?} to chunk {}", cell.id(), self.id);
        match self.index.try_insert_locked(cell.header.hash as usize) {
            Some(mut guard) => {
                let loc = cell.write_to_chunk(self, &guard)?;
                *guard = loc;
                Ok(cell.header)
            }
            None => Err(WriteError::CellAlreadyExisted),
        }
    }

    #[cfg(feature = "slow_map")]
    fn write_cell(&self, cell: &mut Cell) -> Result<CellHeader, WriteError> {
        debug!("Writing cell {:?} to chunk {}", cell.id(), self.id);
        let header = cell.header;
        let mut new = false;
        self.index.upsert(
            header.hash,
            || {
                new = true;
                0
            },
            |_| {},
        );
        // This one have hazard, only use it in debug
        if new {
            debug!("Will insert cell {}", header.hash);
            let mut guard = self.index.get_mut(&header.hash).unwrap();
            let loc = cell.write_to_chunk(self, &guard).unwrap();
            *guard = loc;
            debug!("Cell inserted for {}", header.hash);
            Ok(header)
        } else {
            debug!("Cell already existed {}", header.hash);
            Err(WriteError::CellAlreadyExisted)
        }
    }

    #[inline]
    fn update_cell_from_guard(
        &self,
        cell: &mut Cell,
        guard: &mut CellWriteGuard,
    ) -> Result<CellHeader, WriteError> {
        let _old_location = **guard;
        let new_location = cell.write_to_chunk(self, guard)?;
        **guard = new_location;
        return Ok(cell.header);
    }

    fn update_cell(&self, cell: &mut Cell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.hash;
        if let Some(mut guard) = self.location_for_write(hash) {
            let cell_location = *guard;
            let res = self.update_cell_from_guard(cell, &mut guard);
            self.mark_dead_entry(cell_location);
            res
        } else {
            Err(WriteError::CellDoesNotExisted)
        }
    }

    #[cfg(feature = "fast_map")]
    fn upsert_cell(&self, cell: &mut Cell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.hash;
        loop {
            if let Some(mut guard) = self.location_for_write(hash) {
                let cell_location = *guard;
                let res = self.update_cell_from_guard(cell, &mut guard);
                self.mark_dead_entry(cell_location);
                return res;
            } else {
                if let Some(mut guard) = self.index.try_insert_locked(hash as usize) {
                    // New cell
                    let loc = cell.write_to_chunk(self, &guard)?;
                    *guard = loc;
                    return Ok(cell.header);
                } else {
                    continue;
                }
            }
        }
    }

    #[cfg(feature = "slow_map")]
    fn upsert_cell(&self, cell: &mut Cell) -> Result<CellHeader, WriteError> {
        // This one is also not safe, should only be used for specific tests
        let header = cell.header;
        let hash = header.hash;
        if let Some(mut guard) = self.index.get_mut(&hash) {
            debug!("Upsert {} by update", header.hash);
            let cell_location = *guard;
            let res = self.update_cell_from_guard(cell, &mut guard);
            self.mark_dead_entry(cell_location);
            res
        } else {
            debug!("Upsert {} by write", header.hash);
            self.write_cell(cell)
        }
    }

    fn update_cell_by<U>(&self, hash: u64, update: U) -> Result<Cell, WriteError>
    where
        U: Fn(Cell) -> Option<Cell>,
    {
        let (res, loc) = if let Some(mut cell_location) = self.location_for_write(hash) {
            let cell = Cell::from_chunk_raw(*cell_location, self);
            match cell {
                Ok(cell) => {
                    let new_cell = update(cell);
                    if let Some(mut new_cell) = new_cell {
                        let old_location = *cell_location;
                        let new_location = new_cell.write_to_chunk(self, &mut cell_location)?;
                        *cell_location = new_location;
                        (Ok(new_cell), old_location)
                    } else {
                        return Err(WriteError::UserCanceledUpdate);
                    }
                }
                Err(e) => return Err(WriteError::ReadError(e)),
            }
        } else {
            return Err(WriteError::CellDoesNotExisted);
        };
        self.mark_dead_entry(loc);
        return res;
    }
    fn remove_cell(&self, hash: u64) -> Result<(), WriteError> {
        let _unstable_guard = self.unstable_cells.lock(hash);
        let hash_key = hash as usize;
        #[cfg(feature = "fast_map")]
        let res = self.index.remove(&hash_key);
        #[cfg(feature = "slow_map")]
        let res = self.index.remove(&hash);
        if let Some(cell_location) = res {
            self.put_tombstone_by_cell_loc(cell_location)?;
            Ok(())
        } else {
            Err(WriteError::CellDoesNotExisted)
        }
    }

    fn remove_cell_by<P>(&self, hash: u64, predict: P) -> Result<(), WriteError>
    where
        P: Fn(Cell) -> bool,
    {
        #[cfg(feature = "fast_map")]
        let guard = self.index.lock(hash as usize);
        #[cfg(feature = "slow_map")]
        let guard = self.index.get(&hash);
        if let Some(guard) = guard {
            let cell_location = *guard;
            let cell = Cell::from_chunk_raw(cell_location, self);
            match cell {
                Ok(cell) => {
                    if predict(cell) {
                        let put_tombstone_result = self.put_tombstone_by_cell_loc(cell_location);
                        if put_tombstone_result.is_err() {
                            put_tombstone_result
                        } else {
                            #[cfg(feature = "fast_map")]
                            guard.remove();
                            Ok(())
                        }
                    } else {
                        Err(WriteError::CellDoesNotExisted)
                    }
                }
                Err(e) => Err(WriteError::ReadError(e)),
            }
        } else {
            Err(WriteError::CellDoesNotExisted)
        }
    }

    pub fn put_segment(&self, segment: Segment) {
        debug!(
            "Putting segment for chunk {} with id {}",
            self.id, segment.id
        );
        let segment_id = segment.id;
        let segment_key = segment_id as usize;
        let segment_addr = segment.addr;
        #[cfg(feature = "fast_map")]
        self.segs.insert_back(&segment_key, segment);
        #[cfg(feature = "slow_map")]
        self.segs.insert(segment_key, segment);
    }

    pub fn remove_segment(&self, segment_id: u64) {
        debug!(
            "Removing segment for chunk {} with id {}",
            self.id, segment_id
        );
        if let Some(seg) = self.segs.remove(&(segment_id as usize)) {
            seg.dispense();
        }
    }

    fn locate_segment(&self, addr: usize) -> Option<MapNodeRef<Segment>> {
        let seg_id = self.allocator.id_by_addr(addr);
        let res = self.segs.get(&seg_id);
        if res.is_none() {
            warn!(
                "Cannot locate segment for {}, got id {}, chunk segs {:?}", 
                addr, seg_id,
                self.segs.all_keys()
            );
        }
        return res;
    }

    #[inline]
    fn put_tombstone(&self, cell_location: usize, cell_header: &CellHeader) {
        let cell_seg = self.locate_segment(cell_location).expect(
            format!(
                "cannot locate cell segment for tombstone. Cell id: {:?} at {}",
                cell_header.id(),
                cell_location
            )
            .as_str(),
        );
        let pending_entry = (|| loop {
            if let Some(pending_entry) = self.try_acquire(*TOMBSTONE_ENTRY_SIZE) {
                return pending_entry;
            }
            warn!(
                "Chunk {} is too full to put a tombstone. Will retry.",
                self.id
            )
        })();
        Tombstone::put(
            pending_entry.addr,
            cell_seg.id,
            cell_header.version,
            cell_header.partition,
            cell_header.hash,
        );
        pending_entry.seg.tombstones.fetch_add(1, Ordering::Relaxed);
    }

    fn put_tombstone_by_cell_loc(&self, cell_location: usize) -> Result<(), WriteError> {
        debug!(
            "Put tombstone for chunk {} for cell {}",
            self.id, cell_location
        );
        let header = Cell::header_from_chunk_raw(cell_location)
            .map_err(|e| WriteError::ReadError(e))?
            .0;
        self.put_tombstone(cell_location, &header);
        self.mark_dead_entry(cell_location);
        Ok(())
    }

    // put dead entry address in a ideally non-blocking queue and wait for a worker to
    // make the changes in corresponding segments.
    // Because calculate segment from location is computation intensive, it have to be done lazily
    #[inline]
    fn mark_dead_entry(&self, addr: usize) {
        if let Some(seg) = self.locate_segment(addr) {
            let (entry, _) = Entry::decode_from(addr, |_, _| {});
            seg.dead_space
                .fetch_add(entry.content_length, Ordering::Relaxed);
        }
    }

    #[cfg(feature = "slow_map")]
    pub fn contains_seg(&self, seg_id: u64) -> bool {
        self.segs.contains_key(&(seg_id as usize))
    }

    #[cfg(feature = "fast_map")]
    pub fn contains_seg(&self, seg_id: u64) -> bool {
        self.segs.contains_key(&(seg_id as usize))
    }

    pub fn segment_ids(&self) -> Vec<usize> {
        self.segs.all_keys()
    }

    pub fn segments(&self) -> Vec<MapNodeRef<Segment>> {
        self.segs.all_values()
    }

    // Scan for dead tombstone. This will scan the whole segment, decoding all entry header
    // and looking for those with entry type tombstone.
    // It is resource intensive so there will be some rules to skip the scan.
    // This function should be invoked repeatedly by cleaner
    // Actual cleaning will be performed by cleaner regardless tombstone survival condition
    pub fn scan_tombstone_survival(&self) {
        trace!("Scanning tombstones");
        let seg_ids = self.segment_ids();
        for seg_id in seg_ids {
            let seg_key = seg_id as usize;
            if let Some(segment) = self.segs.get(&seg_key).map(|s| s.clone()) {
                let now = get_time();
                let tombstones = segment.tombstones.load(Ordering::Relaxed);
                let dead_tombstones = segment.dead_tombstones.load(Ordering::Relaxed);
                let mut death_count = 0;
                if
                // have not much tombstones
                (tombstones as f64) * (TOMBSTONE_SIZE as f64) < (SEGMENT_SIZE as f64) * 0.2 ||
                        // large partition have been scanned
                        (dead_tombstones as f32 / tombstones as f32) > 0.8 ||
                        // have been scanned recently
                        now - segment.last_tombstones_scanned.load(Ordering::Relaxed) < 5000
                {
                    continue;
                }
                debug!(
                    "Scanning tombstones in chunk {}, segment {}",
                    self.id, seg_id
                );
                for entry_meta in segment.entry_iter() {
                    if entry_meta.entry_header.entry_type == EntryType::TOMBSTONE {
                        let tombstone =
                            Tombstone::read_from_entry_content_addr(entry_meta.body_pos);
                        if !self.contains_seg(tombstone.segment_id) {
                            // segment that the tombstone pointed to have been cleaned by compact or combined cleaner
                            death_count += 1;
                        }
                    }
                }
                // store the death count for following cleaners will just reset it
                segment
                    .dead_tombstones
                    .store(death_count, Ordering::Relaxed);
                segment
                    .last_tombstones_scanned
                    .store(now, Ordering::Relaxed);
                debug!(
                    "Scanned tombstones in chunk {}, segment {}, death count {}",
                    self.id, seg_id, death_count
                );
            } else {
                warn!("leaked segment in addrs_seg: {}", seg_id)
            }
        }
    }

    pub fn segs_for_compact_cleaner(&self) -> Vec<MapNodeRef<Segment>> {
        let utilization_selection = self
            .segments()
            .into_iter()
            .map(|seg| {
                let rate = seg.living_rate();
                (seg, rate)
            })
            .filter(|(_, utilization)| *utilization < 90f32);
        let head_seg_id = self.get_head_seg_id();
        let mut list: Vec<_> = utilization_selection
            .filter(|(seg, _)| seg.id != head_seg_id && seg.no_references())
            .collect();
        list.sort_by(|pair1, pair2| pair1.1.partial_cmp(&pair2.1).unwrap());
        return list.into_iter().map(|pair| pair.0).collect();
    }

    pub fn segs_for_combine_cleaner(&self) -> Vec<MapNodeRef<Segment>> {
        let head_seg_id = self.get_head_seg_id();
        let mut mapping: Vec<_> = self
            .segments()
            .into_iter()
            .map(|seg| {
                let living = seg.living_space() as f32;
                let segment_utilization = living / SEGMENT_SIZE_U32 as f32;
                (seg, segment_utilization)
            })
            .filter(|(seg, utilization)| {
                *utilization < 50f32 && head_seg_id != seg.id && seg.no_references()
            })
            .collect();
        mapping.sort_by(|pair1, pair2| pair1.1.partial_cmp(&pair2.1).unwrap());
        return mapping.into_iter().map(|(seg, _)| seg).collect();
    }

    pub fn check_and_archive_segments(&self) {
        let seg_ids = self.segment_ids();
        let head_id = self.get_head_seg_id();
        for seg_id in seg_ids {
            if seg_id as u64 == head_id {
                continue;
            } // never archive head segments
            let seg_key = seg_id as usize;
            if let Some(segment) = self.segs.get(&seg_key) {
                if !segment
                    .archived
                    .compare_and_swap(false, true, Ordering::Relaxed)
                {
                    if let Err(e) = segment.archive() {
                        error!("cannot archive segment {}, reason:{:?}", self.id, e)
                    }
                }
            }
        }
    }

    pub fn live_entries<'a>(&'a self, seg: &'a Segment) -> impl Iterator<Item = Entry> + 'a {
        seg.entry_iter()
            .filter_map(move |entry_meta| {
                let chunk_id = &self.id;
                let chunk_index = &self.index;
                let chunk_segs = &self.segs;
                let entry_size = entry_meta.entry_size;
                let entry_header = entry_meta.entry_header;
                debug!("Iterating live entries on chunk {} segment {}. Got {:?} at {} size {}",
                       chunk_id, seg.id, entry_header.entry_type, entry_meta.entry_pos, entry_size);
                match entry_header.entry_type {
                    EntryType::CELL => {
                        debug!("Entry at {} is a cell", entry_meta.entry_pos);
                        let cell_header =
                            Cell::cell_header_from_entry_content_addr(
                                entry_meta.body_pos, &entry_header);
                        debug!("Cell header read, id is {:?}", cell_header.id());

                        #[cfg(feature = "slow_map")]
                        let matches = chunk_index.get(&cell_header.hash).map(|g| *g) == Some(entry_meta.entry_pos);
                        #[cfg(feature = "fast_map")]
                        let matches = chunk_index.get(&(cell_header.hash as usize)) == Some(entry_meta.entry_pos);

                        if matches {
                            debug!("Cell entry {:?} is valid", cell_header.id());
                            return Some(Entry {
                                meta: entry_meta,
                                content: EntryContent::Cell(cell_header)
                            });
                        } else {
                            debug!("Cell entry index mismatch for {:?}, will be ditched", cell_header.id());
                        }
                    },
                    EntryType::TOMBSTONE => {
                        debug!("Entry at {} is a tombstone", entry_meta.entry_pos);
                        let tombstone =
                            Tombstone::read_from_entry_content_addr(entry_meta.body_pos);

                        #[cfg(feature = "slow_map")]
                        let contains_seg = chunk_segs.contains_key(&(tombstone.segment_id as usize));
                        #[cfg(feature = "fast_map")]
                        let contains_seg = chunk_segs.contains_key(&(tombstone.segment_id as usize));

                        if contains_seg {
                            debug!("Tomestone entry {:?} - {:?} at {} is valid",
                                   tombstone.partition, tombstone.hash, tombstone.segment_id);
                            return Some(Entry {
                                meta: entry_meta,
                                content: EntryContent::Tombstone(tombstone)
                            });
                        } else {
                            debug!("Tombstone target at {} have been removed, will be ditched", tombstone.segment_id)
                        }
                    },
                    _ => panic!("Unexpected cell type on getting live entries at {}: type {:?}, size {}, append header {}, ends at {}",
                                entry_meta.entry_pos, entry_header, entry_size,
                                seg.append_header.load(Ordering::Relaxed),
                                entry_meta.entry_pos + entry_size)
                }
                return None
            })
    }

    pub fn cell_count(&self) -> usize {
        self.index.len()
    }

    pub fn seg_count(&self) -> usize {
        self.segs.len()
    }

    pub fn count(&self) -> usize {
        self.index.len()
    }
}

pub struct PendingEntry {
    pub seg: MapNodeRef<Segment>,
    pub addr: usize,
    pub size: u32,
}

impl Drop for PendingEntry {
    // dealing with entry write ahead log
    fn drop(&mut self) {
        self.seg.write_wal(self.addr, self.size).unwrap();
        self.seg.references.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct Chunks {
    pub list: Vec<Chunk>,
}

impl Chunks {
    pub fn new(
        count: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
    ) -> Arc<Chunks> {
        let chunk_size = size / count;
        let mut chunks = Vec::new();
        debug!("Creating {} chunks, total {} bytes", count, size);
        for i in 0..count {
            let backup_storage = backup_storage
                .clone()
                .map(|dir| format!("{}/chunk-bk-{}", dir, i));
            let wal_storage = wal_storage
                .clone()
                .map(|dir| format!("{}/chunk-wal-{}", dir, i));
            chunks.push(Chunk::new(
                i,
                chunk_size,
                meta.clone(),
                backup_storage,
                wal_storage,
            ));
        }
        Arc::new(Chunks { list: chunks })
    }
    pub fn new_dummy(count: usize, size: usize) -> Arc<Chunks> {
        Chunks::new(
            count,
            size,
            Arc::<ServerMeta>::new(ServerMeta {
                schemas: LocalSchemasCache::new_local(""),
            }),
            None,
            None,
        )
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
        return chunk.read_selected(hash, fields);
    }
    pub fn read_partial_raw(
        &self,
        key: &Id,
        offset: usize,
        len: usize,
    ) -> Result<Vec<u8>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_partial_raw(hash, offset, len);
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
    where
        U: Fn(Cell) -> Option<Cell>,
    {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.update_cell_by(hash, update);
    }
    pub fn upsert_cell(&self, cell: &mut Cell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.upsert_cell(cell);
    }
    pub fn remove_cell(&self, key: &Id) -> Result<(), WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell(hash);
    }
    pub fn remove_cell_by<P>(&self, key: &Id, predict: P) -> Result<(), WriteError>
    where
        P: Fn(Cell) -> bool,
    {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell_by(hash, predict);
    }
    pub fn address_of(&self, key: &Id) -> usize {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return *chunk.location_for_read(hash).unwrap();
    }

    pub fn count(&self) -> usize {
        self.list.iter().map(|c| c.count()).sum()
    }
}
