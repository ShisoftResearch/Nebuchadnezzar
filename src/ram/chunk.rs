use crate::query::statistics::{merge_statistics, ChunkStatistics, SchemaStatistics};
use crate::ram::entry::{Entry, EntryContent, EntryType};
use crate::ram::schema::{LocalSchemasCache, SchemaRef};
use crate::ram::segs::{Segment, SegmentAllocator, SEGMENT_SIZE, SEGMENT_SIZE_U32};
use crate::ram::tombstone::{Tombstone, TOMBSTONE_ENTRY_SIZE, TOMBSTONE_SIZE};
use crate::ram::types::Id;
use crate::server::ServerMeta;
use crate::{index::builder::IndexBuilder, ram::cell::*};
use crate::{
    index::builder::{probe_cell_indices, IndexRes},
    ram::cleaner::Cleaner,
};

use super::schema::Schema;
use crate::utils::upper_power_of_2;
use bifrost::utils::time::get_time;
use itertools::Itertools;
use lightning::linked_map::{LinkedObjectMap, NodeRef as MapNodeRef};
use lightning::map::*;
use lightning::ttl_cache::TTLCache;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub type CellReadGuard<'a> = lightning::map::WordMutexGuard<'a>;
pub type CellWriteGuard<'a> = lightning::map::WordMutexGuard<'a>;

pub struct Chunk {
    pub id: usize,
    pub cell_index: WordMap,
    pub segs: LinkedObjectMap<Segment>,
    pub head_seg_id: AtomicU64,
    pub meta: Arc<ServerMeta>,
    pub backup_storage: Option<String>,
    pub wal_storage: Option<String>,
    pub total_space: AtomicUsize,
    pub capacity: usize,
    pub gc_lock: Mutex<()>,
    pub allocator: SegmentAllocator,
    pub alloc_lock: Mutex<()>,
    pub index_builder: Option<Arc<IndexBuilder>>,
    pub statistics: ChunkStatistics,
}

impl Chunk {
    fn new(
        id: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
    ) -> Chunk {
        let allocator = SegmentAllocator::new(size);
        let bootstrap_segment = allocator
            .alloc_seg(&backup_storage, &wal_storage)
            .expect(&format!("No space left for first segment in chunk {}", id));
        let num_segs = {
            let n = size / SEGMENT_SIZE;
            if n > 0 {
                n
            } else {
                n + 1
            }
        };
        debug!("Creating chunk {}, num segments {}", id, num_segs);
        let segs = LinkedObjectMap::with_capacity(upper_power_of_2(num_segs));
        let index = WordMap::with_capacity(64);
        let chunk = Chunk {
            id,
            segs,
            cell_index: index,
            meta,
            backup_storage,
            wal_storage,
            allocator,
            index_builder,
            capacity: size,
            total_space: AtomicUsize::new(0),
            head_seg_id: AtomicU64::new(bootstrap_segment.id),
            gc_lock: Mutex::new(()),
            alloc_lock: Mutex::new(()), // TODO: optimize this
            statistics: ChunkStatistics::new(),
        };
        chunk.put_segment(bootstrap_segment);
        return chunk;
    }

    fn get_head_seg_id(&self) -> u64 {
        self.head_seg_id.load(Ordering::Acquire)
    }

    pub fn try_acquire(&self, size: u32) -> Option<PendingEntry> {
        let mut tried_gc = false;
        loop {
            let head_seg_id = self.get_head_seg_id() as usize;
            let head = self.segs.get(&head_seg_id).expect("Cannot get header");
            match head.try_acquire(size) {
                Some(addr) => {
                    trace!(
                        "Chunk {} acquired address {} for size {} in segment {}",
                        self.id,
                        addr,
                        size,
                        head.id
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
                    if self.allocator.meet_gc_threshold() {
                        debug!("Allocator meet GC threshold, will try partial GC");
                        Cleaner::clean(self, false);
                    }
                    let _alloc_guard = self.alloc_lock.lock();
                    let header_id = self.get_head_seg_id() as usize;
                    if head_seg_id == header_id {
                        // head segment did not changed and locked, suitable for creating a new segment and point it to
                        let new_seg_opt = self
                            .allocator
                            .alloc_seg(&self.backup_storage, &self.wal_storage);
                        let new_seg = new_seg_opt.expect("No space left after full GCs");
                        // for performance, won't CAS total_space
                        self.total_space.fetch_add(SEGMENT_SIZE, Ordering::Relaxed);
                        let new_seg_id = new_seg.id;
                        self.put_segment(new_seg);
                        self.head_seg_id.store(new_seg_id, Ordering::Release);
                    }
                    // whether the segment acquisition success or not,
                    // try to get the new segment and try again
                }
            }
        }
    }

    pub fn location_for_read<'a>(&self, hash: u64) -> Result<CellReadGuard, ReadError> {
        let guard = self.cell_index.lock(hash as usize);
        match guard {
            Some(index) => {
                if *index == 0 {
                    warn!("Cannot find cell with hash {} for index is zero", hash);
                    return Err(ReadError::CellDoesNotExisted);
                }
                return Ok(index);
            }
            None => {
                if hash == 0 {
                    Err(ReadError::CellIdIsUnitId)
                } else {
                    trace!(
                        "Cannot find cell with hash {} for it is not in the map",
                        hash
                    );
                    Err(ReadError::CellDoesNotExisted)
                }
            }
        }
    }

    pub fn location_for_write(&self, hash: u64) -> Option<CellWriteGuard> {
        let guard = self.cell_index.lock(hash as usize);
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
        header_from_chunk_raw(*self.location_for_read(hash)?).map(|pair| pair.0)
    }

    fn read_cell(&self, hash: u64) -> Result<SharedCell, ReadError> {
        SharedCell::from_chunk_raw(self.location_for_read(hash)?, self)
            .map(|(c, _)| c)
            .map_err(|(e, _)| e)
    }

    fn read_selected(&self, hash: u64, fields: &[u64]) -> Result<SharedCell, ReadError> {
        let loc = self.location_for_read(hash)?;
        let (val, hdr) = select_from_chunk_raw(*loc, self, fields)?;
        Ok(SharedCell::compose(
            SharedCellData::from_data(hdr, val),
            loc,
        ))
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

    pub fn write_cell_to_chunk(
        &self,
        cell: &mut OwnedCell,
    ) -> Result<(usize, SchemaRef), WriteError> {
        let schema_id = cell.header.schema;
        if let Some(schema) = self.meta.schemas.get(&schema_id) {
            Ok((cell.write_to_chunk_with_schema(self, &*schema)?, schema))
        } else {
            Err(WriteError::SchemaDoesNotExisted(schema_id))
        }
    }

    fn ensure_indices(&self, new_cell: &OwnedCell, old_cell: Option<&SharedCell>, schema: &Schema) {
        if let Some(index_builder) = &self.index_builder {
            let old_indices = old_cell.map(|cell| probe_cell_indices(cell, &*schema));
            index_builder.ensure_indices(new_cell, &*schema, old_indices);
        }
    }

    fn remove_indices(&self, cell: &SharedCell, schema: &Schema) {
        if let Some(indexer) = &self.index_builder {
            indexer.remove_indices(&cell, &*schema)
        }
    }

    fn ensure_indices_with_res(
        &self,
        cell: &OwnedCell,
        old_indices: Option<Vec<IndexRes>>,
        schema: &Schema,
    ) {
        if let Some(index_builder) = &self.index_builder {
            index_builder.ensure_indices(cell, schema, old_indices)
        }
    }

    fn write_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        debug!("Writing cell {:?} to chunk {}", cell.id(), self.id);
        let (cell_loc, schema) = self.write_cell_to_chunk(cell)?;
        match self.cell_index.try_insert_locked(cell.header.hash as usize) {
            Some(mut guard) => {
                *guard = cell_loc;
                drop(guard);
                self.ensure_indices(cell, None, &*schema);
                self.refresh_statistics();
            }
            None => return Err(WriteError::CellAlreadyExisted),
        }
        Ok(cell.header)
    }

    fn old_index_res<'a>(
        &'a self,
        cell_loc: &WordMutexGuard<'a>,
        schema: &Schema,
    ) -> Result<Option<Vec<IndexRes>>, WriteError> {
        if self.index_builder.is_some() {
            SharedCellData::from_chunk_raw(**cell_loc, self)
                .map(|(c, _)| Some(probe_cell_indices(&c, schema)))
                .map_err(|e| WriteError::ReadError(e))
        } else {
            Ok(None)
        }
    }

    fn update_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.hash;
        // Write first, lock second to avoid deadlock with cleaner
        let (new_cell_loc, schema) = self.write_cell_to_chunk(cell)?;
        if let Some(mut guard) = self.location_for_write(hash) {
            let cell_location = *guard;
            let old_indices = self.old_index_res(&guard, &*schema)?;
            self.ensure_indices_with_res(cell, old_indices, &*schema);
            *guard = new_cell_loc;
            drop(guard);
            self.mark_dead_entry_with_cell(cell_location, cell);
            self.refresh_statistics();
        } else {
            // Optimistic update will remove the new inserted one
            self.mark_dead_entry_with_cell(new_cell_loc, cell);
            return Err(WriteError::CellDoesNotExisted);
        }
        Ok(cell.header)
    }

    fn upsert_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.hash;
        // Write first, lock second to avoid deadlock with cleaner
        let (new_cell_loc, schema) = self.write_cell_to_chunk(cell)?;
        loop {
            if let Some(mut guard) = self.location_for_write(hash) {
                trace!("Cell {} exists, will update for upsert", hash);
                let cell_location = *guard;
                let old_indices = self.old_index_res(&guard, &*schema)?;
                *guard = new_cell_loc;
                drop(guard);
                self.ensure_indices_with_res(cell, old_indices, &*schema);
                self.mark_dead_entry_with_cell(cell_location, cell);
                self.refresh_statistics();
            } else {
                let reservation = self.cell_index.try_insert_locked(hash as usize);
                if let Some(mut guard) = reservation {
                    // New cell
                    trace!("Cell {} does not exists, will insert for upsert", hash);
                    *guard = new_cell_loc;
                    drop(guard);
                    self.ensure_indices(cell, None, &*schema);
                    self.refresh_statistics();
                } else {
                    trace!("Cell {} was not exists, but found exists, will try", hash);
                    continue;
                }
            }
            return Ok(cell.header);
        }
    }

    fn update_cell_by<U>(&self, hash: u64, update: U) -> Result<OwnedCell, WriteError>
    where
        U: Fn(SharedCell) -> Option<OwnedCell>,
    {
        let backoff = crossbeam::utils::Backoff::new();
        loop {
            let ((cell, schema), old_loc) = {
                if let Some(cell_guard) = self.location_for_write(hash) {
                    let loc = *cell_guard;
                    match SharedCell::from_chunk_raw(cell_guard, self) {
                        Ok(cell) => (cell, loc),
                        Err((e, _)) => return Err(WriteError::ReadError(e)),
                    }
                } else {
                    return Err(WriteError::CellDoesNotExisted);
                }
            };
            let old_indices = self
                .index_builder
                .as_ref()
                .map(|_| probe_cell_indices(&cell, &*schema));
            let new_cell = update(cell);
            if let Some(mut new_cell) = new_cell {
                let (new_cell_loc, schema) = self.write_cell_to_chunk(&mut new_cell)?;
                if let Some(mut cell_guard) = self.location_for_write(hash) {
                    // Ensure location unchanged
                    if *cell_guard == old_loc {
                        let old_location = *cell_guard;
                        *cell_guard = new_cell_loc;
                        drop(cell_guard);
                        if let Some(indexer) = &self.index_builder {
                            indexer.ensure_indices(&new_cell, &*schema, old_indices);
                        }
                        self.mark_dead_entry_with_cell(old_location, &new_cell);
                        self.refresh_statistics();
                        return Ok(new_cell);
                    }
                }
                // Failed on check, cleanup and try. This may produce a lot of garbage.
                self.mark_dead_entry_with_cell(new_cell_loc, &new_cell);
                backoff.spin();
            } else {
                return Err(WriteError::UserCanceledUpdate);
            }
        }
    }

    fn remove_cell(&self, hash: u64) -> Result<(), WriteError> {
        let hash_key = hash as usize;
        let guard_opt = self.cell_index.lock(hash_key);
        if let Some(mut guard) = guard_opt {
            if let Some(indexer) = &self.index_builder {
                match SharedCell::from_chunk_raw(guard, self) {
                    Ok((cell, schema)) => {
                        indexer.remove_indices(&cell, &*schema);
                        guard = cell.into_guard();
                    }
                    Err((e, _)) => return Err(WriteError::ReadError(e)),
                }
            }
            let cell_location = *guard;
            self.put_tombstone_by_cell_loc(cell_location)?;
            guard.remove();
            Ok(())
        } else {
            Err(WriteError::CellDoesNotExisted)
        }
    }

    fn remove_cell_by<P>(&self, hash: u64, predict: P) -> Result<(), WriteError>
    where
        P: Fn(&SharedCell) -> bool,
    {
        let guard = self.cell_index.lock(hash as usize);
        if let Some(guard) = guard {
            let cell_location = *guard;
            match SharedCell::from_chunk_raw(guard, self) {
                Ok((cell, schema)) => {
                    if predict(&cell) {
                        let put_tombstone_result = self.put_tombstone_by_cell_loc(cell_location);
                        if put_tombstone_result.is_err() {
                            put_tombstone_result
                        } else {
                            self.remove_indices(&cell, &schema);
                            cell.into_guard().remove();
                            Ok(())
                        }
                    } else {
                        Err(WriteError::CellDoesNotExisted)
                    }
                }
                Err((e, _)) => Err(WriteError::ReadError(e)),
            }
        } else {
            Err(WriteError::CellDoesNotExisted)
        }
    }

    #[inline(always)]
    pub fn put_segment(&self, segment: Segment) {
        debug!(
            "Putting segment for chunk {} with id {}",
            self.id, segment.id
        );
        let segment_id = segment.id;
        let segment_key = segment_id as usize;
        self.segs.insert_back(&segment_key, segment);
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

    fn locate_segment(&self, addr: usize, cell_id: &Id) -> Option<MapNodeRef<Segment>> {
        let seg_id = self.allocator.id_by_addr(addr);
        let res = self.segs.get(&seg_id);
        if res.is_none() {
            warn!(
                "Cannot locate segment for {:?}@{}, got id {}, chunk segs {:?}",
                cell_id,
                addr,
                seg_id,
                self.segs.all_keys()
            );
        }
        return res;
    }

    #[inline]
    fn put_tombstone(&self, cell_header: &CellHeader, cell_seg: &MapNodeRef<Segment>) {
        let pending_entry = (|| loop {
            if let Some(pending_entry) = self.try_acquire(TOMBSTONE_ENTRY_SIZE) {
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

    pub fn put_tombstone_by_cell_loc(&self, cell_location: usize) -> Result<(), WriteError> {
        debug!(
            "Put tombstone for chunk {} for cell {}",
            self.id, cell_location
        );
        let header = header_from_chunk_raw(cell_location)
            .map_err(|e| WriteError::ReadError(e))?
            .0;
        let cell_seg = self.locate_segment_ensured(cell_location, &header.id());
        self.put_tombstone(&header, &cell_seg);
        self.mark_dead_entry_with_seg(cell_location, &cell_seg);
        Ok(())
    }

    fn locate_segment_ensured(&self, cell_location: usize, cell_id: &Id) -> MapNodeRef<Segment> {
        self.locate_segment(cell_location, cell_id).expect(
            format!(
                "Cannot locate cell segment for cell id: {:?} at {}",
                cell_id, cell_location
            )
            .as_str(),
        )
    }

    // put dead entry address in a ideally non-blocking queue and wait for a worker to
    // make the changes in corresponding segments.
    // Because calculate segment from location is computation intensive, it have to be done lazily
    #[inline]
    pub fn mark_dead_entry_with_seg(&self, addr: usize, seg: &Segment) {
        let (entry, _) = Entry::decode_from(addr, |_, _| {});
        seg.dead_space
            .fetch_add(entry.content_length, Ordering::Relaxed);
    }

    pub fn mark_dead_entry_with_cell(&self, addr: usize, cell: &dyn Cell) {
        let seg = self.locate_segment_ensured(addr, &cell.id());
        self.mark_dead_entry_with_seg(addr, &seg)
    }

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
                let chunk_index = &self.cell_index;
                let chunk_segs = &self.segs;
                let entry_size = entry_meta.entry_size;
                let entry_header = entry_meta.entry_header;
                trace!("Iterating live entries on chunk {} segment {}. Got {:?} at {} size {}",
                       chunk_id, seg.id, entry_header.entry_type, entry_meta.entry_pos, entry_size);
                match entry_header.entry_type {
                    EntryType::CELL => {
                        trace!("Entry at {} is a cell", entry_meta.entry_pos);
                        let cell_header =
                            cell_header_from_entry_content_addr(
                                entry_meta.body_pos, &entry_header);
                        trace!("Cell header read, id is {:?}", cell_header.id());
                        let expect = Some(entry_meta.entry_pos);
                        let actual = chunk_index.get_from_mutex(&(cell_header.hash as usize));
                        if expect == actual {
                            trace!(
                                "Cell entry {:?} is valid", cell_header.id()
                            );
                            return Some(Entry {
                                meta: entry_meta,
                                content: EntryContent::Cell(cell_header)
                            });
                        } else {
                            trace!(
                                "Cell entry index mismatch for {:?}. Expect {:?}, actual {:?}, will be ditched", 
                                cell_header.id(), expect, actual
                            );
                        }
                    },
                    EntryType::TOMBSTONE => {
                        trace!("Entry at {} is a tombstone", entry_meta.entry_pos);
                        let tombstone =
                            Tombstone::read_from_entry_content_addr(entry_meta.body_pos);
                        let contains_seg = chunk_segs.contains_key(&(tombstone.segment_id as usize));
                        if contains_seg {
                            trace!("Tomestone entry {:?} - {:?} at {} is valid",
                                   tombstone.partition, tombstone.hash, tombstone.segment_id);
                            return Some(Entry {
                                meta: entry_meta,
                                content: EntryContent::Tombstone(tombstone)
                            });
                        } else {
                            trace!("Tombstone target at {} have been removed, will be ditched", tombstone.segment_id)
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
        self.cell_index.len()
    }

    pub fn seg_count(&self) -> usize {
        self.segs.len()
    }

    pub fn count(&self) -> usize {
        self.cell_index.len()
    }

    #[inline]
    fn refresh_statistics(&self) {
        self.statistics.refresh_from_chunk(self)
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
    pub statistics: TTLCache<Arc<SchemaStatistics>>,
}

impl Chunks {
    pub fn new(
        count: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
    ) -> Arc<Chunks> {
        let chunk_size = size / count;
        let mut chunks = Vec::new();
        assert!(size >= SEGMENT_SIZE);
        debug!("Creating chunks, count {} , total {} bytes", count, size);
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
                index_builder.clone(),
                backup_storage,
                wal_storage,
            ));
        }
        let num_schemas = meta.schemas.count() + 1;
        Arc::new(Chunks {
            list: chunks,
            statistics: TTLCache::with_capacity(num_schemas.next_power_of_two()),
        })
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
    pub fn read_cell(&self, key: &Id) -> Result<SharedCell, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_cell(hash);
    }
    pub fn read_selected(&self, key: &Id, fields: &[u64]) -> Result<SharedCell, ReadError> {
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
    pub fn write_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.write_cell(cell);
    }
    pub fn update_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.update_cell(cell);
    }
    pub fn update_cell_by<U>(&self, key: &Id, update: U) -> Result<OwnedCell, WriteError>
    where
        U: Fn(SharedCell) -> Option<OwnedCell>,
    {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.update_cell_by(hash, update);
    }
    pub fn upsert_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.partition);
        return chunk.upsert_cell(cell);
    }
    pub fn remove_cell(&self, key: &Id) -> Result<(), WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell(hash);
    }
    pub fn remove_cell_by<P>(&self, key: &Id, predict: P) -> Result<(), WriteError>
    where
        P: Fn(&SharedCell) -> bool,
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

    pub fn all_chunk_statistics(&self, schema_id: u32) -> Vec<Option<Arc<SchemaStatistics>>> {
        self.list
            .iter()
            .map(|c| c.statistics.schemas.get(&(schema_id as usize)))
            .collect_vec()
    }
    pub fn ensure_statistics(&self) {
        self.list
            .iter()
            .for_each(|c| c.statistics.ensured_refresh_chunk(c));
    }
    pub fn overall_statistics(&self, schema: u32) -> Arc<SchemaStatistics> {
        self.statistics
            .get(schema as usize, 5 * 60, |schema| {
                let schema = schema as u32;
                let all_stats = self
                    .all_chunk_statistics(schema)
                    .into_iter()
                    .filter_map(|s| s)
                    .collect::<Vec<_>>();
                merge_statistics(all_stats).map(|s| Arc::new(s))
            })
            .unwrap()
    }
}
