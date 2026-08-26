use crate::ram::cell;
use crate::ram::chunk::Chunk;
use crate::ram::cleaner::SegmentCandidate;
use crate::ram::entry::EntryContent;
use crate::ram::segs::{Segment, SegmentClass, SEGMENT_SIZE};
use itertools::Itertools;
use lightning::map::Map;
use rayon::prelude::*;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use libc;
use std::sync::Arc;

// Rayon threads default to a small stack; combining can touch large frames when
// copying entries. Give the pools a larger stack to avoid overflow under heavy loads.
const COMBINE_THREAD_STACK_SIZE: usize = 8 * 1024 * 1024; // 8MB

lazy_static! {
    /// Global thread pool for segment allocation during combine operations
    static ref COMBINE_ALLOC_POOL: rayon::ThreadPool = rayon::ThreadPoolBuilder::new()
        .thread_name(|idx| format!("combine-alloc-t{}", idx))
        .stack_size(COMBINE_THREAD_STACK_SIZE)
        .build()
        .unwrap();

    /// Global thread pool for cell index updates during combine operations
    static ref COMBINE_UPDATE_POOL: rayon::ThreadPool = rayon::ThreadPoolBuilder::new()
        .thread_name(|idx| format!("combine-update-t{}", idx))
        .stack_size(COMBINE_THREAD_STACK_SIZE)
        .build()
        .unwrap();
}

/// Re-encode a cell whose schema generation has been superseded, if that can
/// be done here and now.
///
/// Returns the encoded entry image and its size, or `None` to relocate the
/// cell verbatim. Every `None` is a cell left stale, which is always safe: it
/// stays readable through the generation its header names, and the next
/// ordinary update migrates it anyway. Migration here is opportunistic, and
/// nothing about it may risk the cell.
///
/// Deliberately declines when the two generations declare different index
/// sets. Migrating a cell then implies adding or removing its index entries,
/// and the indexer clients are async RPC while this runs on a synchronous
/// rayon worker -- so the honest move is to leave those cells to the write
/// path, which already has the machinery.
fn try_migrate_entry(
    chunk: &Chunk,
    header: &cell::CellHeader,
    entry_addr: usize,
) -> Option<(Arc<Vec<u64>>, usize)> {
    let schemas = &chunk.meta.schemas;
    let named = schemas.get(&header.schema)?;
    if named.status.is_current() {
        return None;
    }
    let current = schemas.resolve_for_write(header.schema)?;
    if current.vid == named.vid {
        return None;
    }
    if named.index_fields != current.index_fields
        || named.compound_index_fields != current.compound_index_fields
        || named.is_scannable != current.is_scannable
    {
        trace!(
            "Not migrating cell {:?}: generations {} and {} index differently",
            header.id,
            named.vid,
            current.vid
        );
        return None;
    }

    // Decode THIS entry, at the address being relocated -- not whatever the
    // cell index currently points at. A concurrent update moves the index to a
    // newer copy, and re-encoding that one into this slot would put a version
    // here that does not belong to it. The index swap would then refuse the
    // slot and mark it dead, so nothing would be corrupted, but the work would
    // be wrong and the reasoning about it worse.
    let hash = header.id.bits();
    let decoded = match chunk.read_cell_at(hash, entry_addr) {
        Ok(cell) => cell,
        Err(e) => {
            debug!("Not migrating cell {:?}: unreadable ({:?})", header.id, e);
            return None;
        }
    };

    // Name the current generation; `plan_write` resolves and the encoder
    // stamps whatever it resolved to.
    let mut migrating = decoded;
    migrating.header.schema = current.vid;
    let plan = match migrating.plan_write(chunk) {
        Ok(plan) => plan,
        Err(e) => {
            debug!(
                "Not migrating cell {:?} to generation {}: {:?}",
                header.id, current.vid, e
            );
            return None;
        }
    };
    let total_size = plan.total_size as usize;
    let mut buffer: Vec<u64> = vec![0; (total_size + 7) / 8];
    let addr = buffer.as_mut_ptr() as usize;
    debug_assert_eq!(addr % 8, 0, "Vec<u64> must be 8-byte aligned");

    // The SAME version, not a bump. A migration re-encodes a cell without
    // changing what it says, so presenting it as a new version would abort
    // every OCC transaction that had read the cell -- turning background
    // cleaning into user-visible write conflicts. `write_to_addr` computes
    // `old + 1`, so it is handed one less than the version being preserved.
    let preserved = header.version;
    if let Err(e) = migrating.write_to_addr(&plan, addr, preserved.saturating_sub(1)) {
        debug!(
            "Not migrating cell {:?}: encode failed ({:?})",
            header.id, e
        );
        return None;
    }
    trace!(
        "Migrating cell {:?} from generation {} to {} ({} -> {} bytes)",
        header.id,
        named.vid,
        current.vid,
        0,
        total_size
    );
    Some((Arc::new(buffer), total_size))
}

pub struct CombinedCleaner;

#[derive(Clone)]
struct DummyEntry {
    size: usize,
    addr: usize,
    cell_ver: u64,
    cell_hash: Option<u64>,
    timestamp: u32,
    /// A re-encoded copy of this cell, when its schema generation has been
    /// superseded and it is being migrated rather than relocated.
    ///
    /// `Vec<u64>` for its alignment, not its element type: the entry encoder
    /// asserts an 8-byte-aligned destination, and a `Vec<u8>` gives no such
    /// guarantee. `Arc` because `DummyEntry` is cloned during deduplication
    /// and the buffer must not be.
    migrated: Option<Arc<Vec<u64>>>,
}

struct DummySegment {
    head: usize,
    segment_class: SegmentClass,
    entries: Vec<DummyEntry>,
}

impl DummySegment {
    fn new(segment_class: SegmentClass) -> DummySegment {
        DummySegment {
            head: 0,
            segment_class,
            entries: Vec::new(),
        }
    }
}

// Combine small segments into larger segments
// This cleaner will perform a greedy approach to relocate entries from old segments to fewer
// new segments to reduce number or segments and reclaim spaces from tombstones

// for higher hit rate for fetching cells in segments, we need to put data with close timestamp together
// this optimization is intended for enabling neb to contain data more than it's memory
impl CombinedCleaner {
    fn select_candidate_segments(
        chunk: &Chunk,
        selected_segments: &[lightning::aarc::Arc<Segment>],
    ) -> Vec<SegmentCandidate> {
        // Remove the head segment, cold segments, and segments locked by tiered operations
        // Skip locked segments (eviction/promotion in progress) to avoid conflicts
        // Skip cold segments to avoid accessing evicted data (would trigger promotion)
        let segments = selected_segments
            .iter()
            .filter_map(|seg| {
                if chunk.is_active_head(seg.id) {
                    return None;
                }
                // Check references first to avoid locking if busy (fast path)
                if !seg.no_references() {
                    return None;
                }

                SegmentCandidate::new(&seg)
            })
            .collect_vec();

        if segments.len() < 2 {
            trace!(
                "too few segments to combine, chunk {}, segments {}",
                chunk.id,
                segments.len()
            );
            return Vec::new();
        }

        let preferred_class = segments
            .first()
            .map(|seg| seg.segment_class())
            .unwrap_or(SegmentClass::Regular);
        let (blob_segments, regular_segments): (Vec<_>, Vec<_>) = segments
            .into_iter()
            .partition(|seg| seg.segment_class() == SegmentClass::Blob);

        if preferred_class == SegmentClass::Blob && blob_segments.len() >= 2 {
            return blob_segments;
        }

        if preferred_class == SegmentClass::Regular && regular_segments.len() >= 2 {
            return regular_segments;
        }

        if blob_segments.len() >= 2 {
            return blob_segments;
        }

        if regular_segments.len() >= 2 {
            return regular_segments;
        }

        trace!(
            "too few same-class segments to combine, chunk {}, blob={}, regular={}",
            chunk.id,
            blob_segments.len(),
            regular_segments.len()
        );
        Vec::new()
    }

    fn collect_and_deduplicate_entries(
        chunk: &Chunk,
        segments: &[SegmentCandidate],
        segment_ids_to_combine: &HashSet<u64>,
    ) -> Vec<DummyEntry> {
        debug!(
            "Starting combining segments, candidates {:?}, head seg {}",
            segment_ids_to_combine,
            chunk.get_head_seg_id()
        );

        // Get all entries in segments to combine and order them by data temperature and size
        // Step 1: Collect and deduplicate using HashMap (faster than sort+chunk_by)
        use std::collections::HashMap;
        let mut deduped_cells: HashMap<u64, DummyEntry> = HashMap::new();
        let mut tombstones: Vec<DummyEntry> = Vec::new();

        for entry in segments
            .iter()
            .flat_map(|seg| chunk.live_entries(seg).map(|entry| (entry, seg.id)))
            .filter(|(entry, _seg_id)| {
                // live entries have done a lot of filtering work already
                // but we still need to remove those tombstones that pointed to segments we are about to combine
                if let EntryContent::Tombstone(ref tombstone) = entry.content {
                    // Exclude tombstones pointing to segments being combined
                    // Look up the segment by seq_id to get its segment_id
                    let tombstone_seg_id = chunk
                        .segs
                        .get_by_seq_id(tombstone.segment_seq_id)
                        .map(|seg| seg.id);
                    let is_pointing_to_combined_seg = tombstone_seg_id
                        .map_or(false, |seg_id| segment_ids_to_combine.contains(&seg_id));
                    return !is_pointing_to_combined_seg // Tombstone is not pointing to a segment we are about to combine
                        && !chunk.cell_index.contains_key(&(tombstone.id.bits() as usize));
                    // Tombstone is not pointing to a cell that is already in the chunk;
                }
                return true;
            })
            .map(|(entry, _seg_id)| entry)
        {
            let entry_size = entry.meta.entry_size;
            let entry_addr = entry.meta.entry_pos;
            let cell_header = match entry.content {
                EntryContent::Cell(header) => Some(header),
                _ => None,
            };
            // A cell whose generation has been superseded is re-encoded HERE,
            // before the layout is planned, so the planner sees the size the
            // destination will actually receive. Handing it the pre-migration
            // size is what would let a copy run past the end of its segment,
            // which is the failure the overrun probe below exists to catch.
            let migrated = cell_header
                .as_ref()
                .and_then(|header| try_migrate_entry(chunk, header, entry_addr));
            let (entry_size, migrated) = match migrated {
                Some((buffer, size)) => (size, Some(buffer)),
                None => (entry_size, None),
            };
            let dummy_entry = DummyEntry {
                size: entry_size,
                addr: entry_addr,
                timestamp: cell_header.map(|h| h.timestamp).unwrap_or(0),
                cell_hash: cell_header.map(|h| h.id.bits()),
                cell_ver: cell_header.map(|h| h.version).unwrap_or(0),
                migrated,
            };

            if let Some(hash) = dummy_entry.cell_hash {
                // Cell with hash: keep only the latest version
                deduped_cells
                    .entry(hash)
                    .and_modify(|existing| {
                        if dummy_entry.cell_ver > existing.cell_ver {
                            *existing = dummy_entry.clone();
                        }
                    })
                    .or_insert(dummy_entry);
            } else {
                // Tombstone or entry without hash: keep all
                tombstones.push(dummy_entry);
            }
        }

        // Step 2: Combine deduplicated cells and tombstones, then sort by timestamp
        let mut all_entries: Vec<_> = deduped_cells
            .into_values()
            .chain(tombstones.into_iter())
            .collect();

        // Sort by timestamp and then by size within timestamp buckets
        all_entries.sort_by_key(|entry| (entry.timestamp, entry.size));

        // Reverse to get hottest/largest first
        all_entries.reverse();

        debug!("Found {} entries to combine", all_entries.len());
        all_entries
    }

    fn plan_segment_layout(
        entries: &[DummyEntry],
        segment_class: SegmentClass,
    ) -> Vec<DummySegment> {
        if entries.is_empty() {
            return Vec::new();
        }
        let mut pending_segments = Vec::new();
        pending_segments.push(DummySegment::new(segment_class));
        for entry in entries {
            let entry_size = entry.size;
            {
                let segment_space_remains = SEGMENT_SIZE - pending_segments.last().unwrap().head;
                if entry_size > segment_space_remains {
                    pending_segments.push(DummySegment::new(segment_class));
                }
            }
            let last_segment = pending_segments.last_mut().unwrap();
            last_segment.entries.push(entry.clone());

            // pump dummy segment head pointer
            last_segment.head += entry_size;
        }
        pending_segments
    }

    /// Reserve every destination segment before anything is copied.
    ///
    /// A combine that runs out of space halfway cannot be unwound: by then
    /// some cells have been copied and repointed at new segments while others
    /// still live in the sources, and there is no consistent state to return
    /// to. Allocating up front makes exhaustion a decision taken while
    /// nothing has changed yet. Returns `None` when the chunk cannot host the
    /// destinations, handing back anything already taken.
    fn reserve_destinations(
        chunk: &Chunk,
        pending_segments: &[DummySegment],
    ) -> Option<Vec<Segment>> {
        let mut reserved: Vec<Segment> = Vec::with_capacity(pending_segments.len());
        for dummy_seg in pending_segments {
            match chunk
                .allocator
                .alloc_seg_with_class(&chunk.file_manager, dummy_seg.segment_class)
            {
                Some(seg) => reserved.push(seg),
                None => {
                    warn!(
                        "Combine needs {} destination segments in chunk {} but the allocator ran \
                         out after {}; skipping this round. The sources keep their data and a \
                         later round retries with whatever eviction has freed.",
                        pending_segments.len(),
                        chunk.id,
                        reserved.len()
                    );
                    for seg in reserved {
                        seg.dispense();
                        seg.mem_drop(chunk);
                    }
                    return None;
                }
            }
        }
        Some(reserved)
    }

    fn execute_combine_phases(
        chunk: &Chunk,
        pending_segments: Vec<DummySegment>,
        cleaned_total_live_space: &AtomicUsize,
    ) -> Option<Vec<usize>> {
        let reserved = Self::reserve_destinations(chunk, &pending_segments)?;
        // Use global thread pool for segment allocation
        Some(COMBINE_ALLOC_POOL.install(|| {
            pending_segments
                .into_par_iter()
                .zip(reserved.into_par_iter())
                .map(|(dummy_seg, new_seg)| {
                    let dummy_seg = &dummy_seg;
                    let new_seg_id = new_seg.id;
                    let mut cell_mapping = Vec::with_capacity(dummy_seg.entries.len());
                    let mut seg_cursor = new_seg.addr;
                    trace!(
                        "Combining segment to new one with id {} with {} cells",
                        new_seg_id,
                        dummy_seg.entries.len()
                    );
                    for entry in &dummy_seg.entries {
                        let entry_addr = entry.addr;
                        // PROBE: a copy that would cross the destination's end
                        // writes into whatever segment owns the NEXT address
                        // range -- measured as a cold neighbour's resident
                        // pages holding this segment's cells, served through
                        // the fault-in present-block fast path. Refuse and
                        // scream rather than corrupt.
                        if seg_cursor + entry.size > new_seg.addr + SEGMENT_SIZE {
                            error!(
                                "COMBINE OVERRUN: destination segment {} (chunk {}) cursor {:#x} \
                                 + entry {} bytes crosses bound {:#x}; entry from {:#x}",
                                new_seg_id,
                                chunk.id,
                                seg_cursor,
                                entry.size,
                                new_seg.addr + SEGMENT_SIZE,
                                entry_addr
                            );
                            break;
                        }
                        // A migrated cell is copied from the image built
                        // during collection; everything else is the verbatim
                        // relocation this cleaner has always done.
                        let source_addr = match entry.migrated.as_ref() {
                            Some(buffer) => buffer.as_ptr() as usize,
                            None => entry_addr,
                        };
                        unsafe {
                            libc::memcpy(
                                seg_cursor as *mut libc::c_void,
                                source_addr as *mut libc::c_void,
                                entry.size,
                            );
                        }
                        if let Some(cell_hash) = entry.cell_hash {
                            trace!(
                                "Marked cell relocation hash {}, addr {} to segment {}",
                                cell_hash,
                                entry_addr,
                                new_seg_id
                            );
                            // Include entry.size so we can mark dead space without decoding if cell was updated
                            cell_mapping.push((
                                seg_cursor,
                                entry_addr,
                                cell_hash,
                                entry.cell_ver,
                                entry.size,
                            ));
                        }
                        seg_cursor += entry.size;
                    }
                    // Use Release ordering to ensure the append_header update is visible
                    // to other threads that might archive or read this segment
                    new_seg
                        .append_header
                        .store(seg_cursor, Ordering::Release);
                    let used_size = seg_cursor - new_seg.addr;
                    if used_size < SEGMENT_SIZE {
                        new_seg.shrink(used_size);
                    }
                    cleaned_total_live_space.fetch_add(new_seg.used_spaces() as usize, Relaxed);
                    return (new_seg, cell_mapping);
                })
                .map(|(segment, cells)| {
                    trace!("Putting new segment {}, cells {}", segment.id, cells.len());
                    let archive_result = segment.archive();
                    match archive_result {
                        Ok(true) => {
                            trace!("Segment {} archived successfully", segment.id);
                        }
                        Ok(false) => {
                            // Archive returned false - this happens when backup storage is not configured
                            // In test mode without tiered memory, this is expected
                            // In production with tiered memory, this indicates a configuration issue
                            #[cfg(feature = "tiered_memory")]
                            {
                                debug!(
                                    "[COMBINE WARNING] Segment {} (chunk={}, seq_id={}) archive returned Ok(false) - backup storage may not be configured",
                                    segment.id, segment.chunk_id, segment.seq_id
                                );
                            }
                            #[cfg(not(feature = "tiered_memory"))]
                            {
                                trace!("Segment {} not archived (no backup storage configured)", segment.id);
                            }
                        }
                        Err(e) => {
                            debug!(
                                "[COMBINE CRITICAL] Segment {} (chunk={}, seq_id={}) archive failed: {} - NOT putting in list",
                                segment.id, segment.chunk_id, segment.seq_id, e
                            );
                            panic!(
                                "Combine failed: segment {} archive error: {}",
                                segment.id, e
                            );
                        }
                    }
                    let new_seg_id = segment.id as usize;
                    chunk.put_segment(segment);
                    let new_seg = chunk.segs.get(&new_seg_id).unwrap();
                    // Sort cells by hash to ensure consistent lock ordering across parallel threads
                    // This prevents deadlocks when multiple threads acquire locks in different orders
                    let mut sorted_cells = cells;
                    sorted_cells.sort_by_key(|(_, _, hash, _, _)| *hash);

                    // Use global thread pool for cell index updates
                    COMBINE_UPDATE_POOL.install(|| {
                        sorted_cells
                            .into_par_iter()
                            .for_each(|(new, old, hash, ver, entry_size)| {
                                trace!("Reset cell {} ptr from {} to {}", hash, old, new);
                                let index = chunk.cell_index.lock(hash as usize);
                                if let Some(mut actual_addr) = index {
                                    if *actual_addr == old {
                                        *actual_addr = new;
                                        trace!(
                                            "Cell addr for hash {} set from {} to {} for combine, ver {}",
                                            hash,
                                            old,
                                            new,
                                            ver
                                        );
                                    } else {
                                        #[cfg(debug_assertions)]
                                        {
                                            let current_version =
                                                cell::cell_version_from_chunk_raw(*actual_addr)
                                                    .unwrap();
                                            assert!(
                                        current_version >= ver,
                                        "Cell {} with address {} changed to {} but version running backwards {} -> {}",
                                        hash,
                                        old,
                                        *actual_addr,
                                        ver,
                                        current_version
                                    );
                                        }
                                        trace!(
                                            "cell {} with address {}, have been changed to {} on combine, ver {}",
                                            hash,
                                            old,
                                            *actual_addr,
                                            ver
                                        );
                                        // SAFETY FIX: Use mark_dead_entry_with_size instead of mark_dead_entry_with_seg
                                        // because the entry may contain garbage if the cell was updated during combine.
                                        chunk.mark_dead_entry_with_size(
                                            new,
                                            entry_size as u32,
                                            &new_seg,
                                        );
                                    }
                                } else {
                                    trace!("cell {} address {} have been removed on combine", hash, old);
                                    // Cell was deleted - the copy in new segment is wasted space
                                    // Use mark_dead_entry_with_size to avoid decoding potentially corrupt data
                                    chunk.mark_dead_entry_with_size(new, entry_size as u32, &new_seg);
                                }
                            });
                    });
                    new_seg_id
                })
                .collect::<Vec<_>>()
        }))
    }

    /// Unpublish the combined-away sources and hand them to QSBR reclamation.
    ///
    /// Takes the candidates by value on purpose. Each one holds a *shared*
    /// reference on its segment, so the segment can never be quiescent while
    /// they are alive -- reclaiming here would either free under our own
    /// reference or wait on ourselves, which is the same self-wait that
    /// livelocked promotion. Unpublish first, drop the candidates, and let the
    /// drain do the destructive part once readers have left.
    fn cleanup_segments(chunk: &Chunk, segments: Vec<SegmentCandidate>) {
        debug!("Unpublishing {} source segments", segments.len());
        for old_seg in &segments {
            chunk.remove_segment(old_seg.id);
        }
        // Releases our references; from here the segments can go quiescent.
        drop(segments);
        // Reclaim what is already safe, so the common case costs no delay.
        let freed = chunk.drain_retired_segments();
        debug!("Reclaimed {} retired segments after combine", freed);
    }

    pub fn combine_segments(
        chunk: &Chunk,
        selected_segments: &Vec<lightning::aarc::Arc<Segment>>,
    ) -> (usize, usize) {
        let mut segments = Self::select_candidate_segments(chunk, selected_segments);
        if segments.is_empty() {
            return (0, 0);
        }

        let space_to_collect;
        let segment_ids_to_combine: HashSet<_>;
        let all_entries;
        // A round that cannot reserve enough destination segments is retried
        // with fewer sources instead of being skipped: in a nearly full chunk
        // the allocator may only have the cleaner's reserve segment to give,
        // and a two-into-one combine through that single destination is
        // exactly how such a chunk frees its first segment. Skipping meant a
        // full chunk with plenty of dead space could never reclaim any of it.
        loop {
            let ids: HashSet<_> = segments.iter().map(|seg| seg.id).collect();
            let entries = Self::collect_and_deduplicate_entries(chunk, &segments, &ids);
            if entries.is_empty() {
                segment_ids_to_combine = ids;
                all_entries = entries;
                space_to_collect = segments
                    .iter()
                    .map(|seg| seg.used_spaces() as usize)
                    .sum::<usize>();
                break;
            }
            let segment_class = segments[0].segment_class();
            let planned = Self::plan_segment_layout(&entries, segment_class);
            let available = chunk.allocator.available_segments();
            if planned.len() > available && segments.len() > 2 {
                let keep = (segments.len() / 2).max(2);
                debug!(
                    "Combine round needs {} destinations but chunk {} has {}; retrying \
                     with {} of {} sources",
                    planned.len(),
                    chunk.id,
                    available,
                    keep,
                    segments.len()
                );
                for seg in segments.drain(keep..) {
                    seg.mark_clean_no_progress();
                }
                continue;
            }
            segment_ids_to_combine = ids;
            all_entries = entries;
            space_to_collect = segments
                .iter()
                .map(|seg| seg.used_spaces() as usize)
                .sum::<usize>();
            break;
        }

        let mut num_reduced_segments: isize = 0;
        let mut space_cleaned = 0;

        if all_entries.len() > 0 {
            let segment_class = segments[0].segment_class();
            // Simulate the combine process to determine the efficiency
            let pending_segments = Self::plan_segment_layout(&all_entries, segment_class);

            debug!("Checking combine feasibility");
            let pending_segments_len = pending_segments.len() as isize;
            let segments_to_combine_len = segments.len() as isize;
            let cleaned_total_live_space = AtomicUsize::new(0);
            num_reduced_segments = segments_to_combine_len - pending_segments_len;
            if num_reduced_segments <= 0 {
                debug!(
                "Trying to combine segments but resulting segments still does not go down {}/{}",
                pending_segments_len, segments_to_combine_len
            );
                for seg in segments.iter() {
                    seg.mark_clean_no_progress();
                }
                return (0, 0);
            }

            debug!(
                "Updating cell reference, pending segments {}",
                pending_segments.len()
            );

            // Use global thread pool for segment allocation
            let Some(new_segs) =
                Self::execute_combine_phases(chunk, pending_segments, &cleaned_total_live_space)
            else {
                // Nothing was copied and nothing repointed, so the sources are
                // still whole: leave them alone. Removing them here is what
                // used to destroy data when combine panicked mid-round.
                for seg in segments.iter() {
                    seg.mark_clean_no_progress();
                }
                return (0, 0);
            };

            space_cleaned = space_to_collect - cleaned_total_live_space.load(Relaxed);
            debug!(
                "Combined {} segments to {}, total {} bytes, new segs {:?}",
                segments_to_combine_len,
                new_segs.len(),
                space_cleaned,
                new_segs
            );
        } else {
            // "No entries" is only a reason to delete these segments if they
            // really hold nothing. If they claim used space, the entry walk
            // failed to decode them -- which is exactly what a segment whose
            // pages were dropped looks like -- and removing them would destroy
            // every cell the index still points at, plus their backups and
            // WALs. One unreadable segment would take every segment selected
            // in the round with it.
            let claimed_bytes: usize = segments.iter().map(|seg| seg.used_spaces() as usize).sum();
            if claimed_bytes > 0 {
                // Zero LIVE entries with a nonzero claim is two very different
                // stories, and only one of them is danger:
                //
                //   * The image is UNREADABLE -- zeroed pages under a hot flag,
                //     a claimed-but-never-filled span. Removing such segments
                //     destroys every cell whose index still points there, plus
                //     their backups; TB13 lost 15 segments to exactly that.
                //     Refuse, loudly.
                //
                //   * The image is READABLE and every entry is simply DEAD --
                //     which is what a DONOR's segments look like after a
                //     migration's reclaim dropped every cell they held.
                //     Removing them is not just safe, it is combine's whole
                //     job; refusing here made drained space unreclaimable
                //     forever, ten refusals per 8 GB reshard.
                //
                // One raw entry header distinguishes them: a readable image
                // decodes its first entry whatever its liveness, a zeroed one
                // decodes nothing.
                let all_readable = segments.iter().all(|seg| {
                    let frontier = seg.append_header.load(Ordering::Relaxed);
                    frontier > seg.addr
                        && crate::ram::segs::SegmentEntryIter {
                            bound: frontier,
                            cursor: seg.addr,
                        }
                        .next()
                        .is_some()
                });
                if !all_readable {
                    let states: Vec<String> = segments
                        .iter()
                        .map(|seg| {
                            let first = unsafe { *(seg.addr as *const u64) };
                            format!(
                                "seg {} (seq {}) {} {} refs={} frontier={} first_word={:#x} \
                                 evicted={} promoted={}",
                                seg.id,
                                seg.seq_id,
                                if seg.is_hot() { "HOT" } else { "COLD" },
                                if seg.is_dirty() { "dirty" } else { "clean" },
                                seg.references_count(),
                                seg.append_header.load(Ordering::Relaxed) - seg.addr,
                                first,
                                seg.last_evicted_ms.load(Ordering::Relaxed),
                                seg.last_promoted_ms.load(Ordering::Relaxed),
                            )
                        })
                        .collect();
                    error!(
                        "Combine decoded 0 entries from {} segments in chunk {} that claim {} \
                         used bytes; refusing to remove them. Their resident images are \
                         unreadable, not empty -- removing them would delete live cells along \
                         with their backups. States: {states:?}",
                        segments.len(),
                        chunk.id,
                        claimed_bytes
                    );
                    for seg in segments.iter() {
                        seg.mark_clean_no_progress();
                    }
                    return (0, 0);
                }
                debug!(
                    "Combine removing {} fully-dead segments in chunk {} ({} bytes reclaimed)",
                    segments.len(),
                    chunk.id,
                    claimed_bytes
                );
            }
            debug!("No entries to work on, will remove all selected segments instead");
        }

        let len_cleaned_segments = segments.len();
        debug!(
            "Removing {} old segments, {:?}, now head seg {}",
            len_cleaned_segments,
            segment_ids_to_combine,
            chunk.get_head_seg_id()
        );

        Self::cleanup_segments(chunk, segments);

        debug!(
            "End combining segments, totally cleaned {} bytes, with {} segments.",
            space_cleaned, len_cleaned_segments
        );
        debug_assert!(num_reduced_segments >= 0);
        (space_cleaned, num_reduced_segments as usize)
    }
}
