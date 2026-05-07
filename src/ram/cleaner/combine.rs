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

pub struct CombinedCleaner;

#[derive(Clone)]
struct DummyEntry {
    size: usize,
    addr: usize,
    cell_ver: u64,
    cell_hash: Option<u64>,
    timestamp: u32,
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
                        && !chunk.cell_index.contains_key(&(tombstone.hash as usize));
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
            let dummy_entry = DummyEntry {
                size: entry_size,
                addr: entry_addr,
                timestamp: cell_header.map(|h| h.timestamp).unwrap_or(0),
                cell_hash: cell_header.map(|h| h.hash),
                cell_ver: cell_header.map(|h| h.version).unwrap_or(0),
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

    fn execute_combine_phases(
        chunk: &Chunk,
        pending_segments: Vec<DummySegment>,
        cleaned_total_live_space: &AtomicUsize,
    ) -> Vec<usize> {
        // Use global thread pool for segment allocation
        COMBINE_ALLOC_POOL.install(|| {
            pending_segments
                .par_iter()
                .map(|dummy_seg| {
                    let new_seg = chunk
                        .allocator
                        .alloc_seg_with_class(&chunk.file_manager, dummy_seg.segment_class)
                        .expect("No space left during combine");
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
                        unsafe {
                            libc::memcpy(
                                seg_cursor as *mut libc::c_void,
                                entry_addr as *mut libc::c_void,
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
        })
    }

    fn cleanup_segments(chunk: &Chunk, segments: &[SegmentCandidate]) {
        debug!("Released references for {} source segments", segments.len());
        for old_seg in segments {
            chunk.remove_segment(old_seg.id);
            old_seg.mem_drop(chunk);
        }
    }

    pub fn combine_segments(
        chunk: &Chunk,
        selected_segments: &Vec<lightning::aarc::Arc<Segment>>,
    ) -> (usize, usize) {
        let segments = Self::select_candidate_segments(chunk, selected_segments);
        if segments.is_empty() {
            return (0, 0);
        }

        let space_to_collect = segments
            .iter()
            .map(|seg| seg.used_spaces() as usize)
            .sum::<usize>();

        let segment_ids_to_combine: HashSet<_> = segments.iter().map(|seg| seg.id).collect();

        let all_entries =
            Self::collect_and_deduplicate_entries(chunk, &segments, &segment_ids_to_combine);

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
            let new_segs =
                Self::execute_combine_phases(chunk, pending_segments, &cleaned_total_live_space);

            space_cleaned = space_to_collect - cleaned_total_live_space.load(Relaxed);
            debug!(
                "Combined {} segments to {}, total {} bytes, new segs {:?}",
                segments_to_combine_len,
                new_segs.len(),
                space_cleaned,
                new_segs
            );
        } else {
            debug!("No entries to work on, will remove all selected segments instead");
        }

        let len_cleaned_segments = segments.len();
        debug!(
            "Removing {} old segments, {:?}, now head seg {}",
            len_cleaned_segments,
            segment_ids_to_combine,
            chunk.get_head_seg_id()
        );

        Self::cleanup_segments(chunk, &segments);

        debug!(
            "End combining segments, totally cleaned {} bytes, with {} segments.",
            space_cleaned, len_cleaned_segments
        );
        debug_assert!(num_reduced_segments >= 0);
        (space_cleaned, num_reduced_segments as usize)
    }
}
