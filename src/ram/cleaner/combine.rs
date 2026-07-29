use crate::ram::cell;
use crate::ram::chunk::{Chunk, CurrentAddressRelocation};
use crate::ram::cleaner::SegmentCandidate;
use crate::ram::entry::EntryType;
use crate::ram::history::RelocateResult;
use crate::ram::segs::{Segment, SegmentClass, SEGMENT_SIZE};
use crate::ram::tombstone::Tombstone;
use crate::ram::types::{FromHeader, Id};
use itertools::Itertools;
use rayon::prelude::*;
use std::collections::HashSet;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};

use libc;

#[cfg(test)]
type BeforeRelocateHook<'a> = dyn Fn(Id, u64, usize, usize) + Sync + 'a;
#[cfg(test)]
type AfterHistoryRelocateHook<'a> = dyn Fn(Id, u64, usize, usize) + Sync + 'a;

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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct RevisionKey {
    id: Id,
    revision_ts: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RevisionKind {
    Cell,
    Tombstone,
}

#[derive(Clone)]
struct DummyEntry {
    size: usize,
    addr: usize,
    key: RevisionKey,
    kind: RevisionKind,
    history_live: bool,
    direct_tombstone_live: bool,
}

struct Relocation {
    new: usize,
    old: usize,
    key: RevisionKey,
    kind: RevisionKind,
    entry_size: usize,
    history_live: bool,
    direct_tombstone_live: bool,
}

struct UnpublishedSegment<'a> {
    segment: Option<Segment>,
    chunk: &'a Chunk,
}

impl<'a> UnpublishedSegment<'a> {
    fn new(chunk: &'a Chunk, segment: Segment) -> Self {
        Self {
            segment: Some(segment),
            chunk,
        }
    }

    fn segment(&self) -> &Segment {
        self.segment
            .as_ref()
            .expect("unpublished segment guard must own its segment")
    }

    fn publish(mut self) -> Segment {
        self.segment
            .take()
            .expect("unpublished segment guard must own its segment")
    }

    fn discard(mut self, cause: io::Error) -> io::Error {
        let segment = self
            .segment
            .take()
            .expect("unpublished segment guard must own its segment");
        let cleanup = segment.dispense();
        segment.mem_drop(self.chunk);
        match cleanup {
            Ok(()) => cause,
            Err(cleanup_error) => io::Error::new(
                cause.kind(),
                format!(
                    "{cause}; also failed to durably discard unpublished cleaner destination {}: {cleanup_error}",
                    segment.id
                ),
            ),
        }
    }
}

impl Drop for UnpublishedSegment<'_> {
    fn drop(&mut self) {
        let Some(segment) = self.segment.take() else {
            return;
        };
        if let Err(error) = segment.dispense() {
            error!(
                "Failed to durably discard unpublished cleaner destination {} during unwinding: {}",
                segment.id, error
            );
        }
        segment.mem_drop(self.chunk);
    }
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

        // Retained logical revisions, not the lower cell hash, are the cleaner's
        // liveness and deduplication identity.
        use std::collections::HashMap;
        let mut revisions: HashMap<RevisionKey, DummyEntry> = HashMap::new();
        let oldest_resident_seq = chunk
            .segments()
            .into_iter()
            .map(|segment| segment.seq_id)
            .min();

        for entry in segments.iter().flat_map(|segment| segment.entry_iter()) {
            let (key, kind, direct_tombstone_live) = match entry.entry_header.entry_type {
                EntryType::CELL => {
                    let header = cell::cell_header_from_entry_content_addr(entry.body_pos);
                    (
                        RevisionKey {
                            id: Id::from_header(&header),
                            revision_ts: header.revision_ts,
                        },
                        RevisionKind::Cell,
                        false,
                    )
                }
                EntryType::TOMBSTONE => {
                    let tombstone = Tombstone::read_from_entry_content_addr(entry.body_pos);
                    (
                        RevisionKey {
                            id: Id::new(tombstone.partition, tombstone.hash),
                            revision_ts: tombstone.revision_ts,
                        },
                        RevisionKind::Tombstone,
                        oldest_resident_seq
                            .is_some_and(|oldest| oldest <= tombstone.segment_seq_id),
                    )
                }
                EntryType::UNDECIDED => continue,
            };
            let history_live = chunk
                .history
                .is_live_at(key.id, key.revision_ts, entry.entry_pos);
            let current_index_target = kind == RevisionKind::Cell
                && chunk.cell_index.get_from_mutex(&(key.id.lower as usize))
                    == Some(entry.entry_pos);
            if !history_live && !current_index_target && !direct_tombstone_live {
                continue;
            }

            let candidate = DummyEntry {
                size: entry.entry_size,
                addr: entry.entry_pos,
                key,
                kind,
                history_live,
                direct_tombstone_live,
            };
            revisions
                .entry(key)
                .and_modify(|existing| {
                    // A physical duplicate can survive a crash or a prior lost
                    // cleaner publication. Prefer the address selected by the
                    // history node over a current-index-only fallback.
                    if candidate.history_live && !existing.history_live {
                        *existing = candidate.clone();
                    }
                })
                .or_insert(candidate);
        }

        let mut all_entries: Vec<_> = revisions.into_values().collect();

        // Sort by revision and then by size within revision buckets.
        all_entries.sort_by_key(|entry| (entry.key.revision_ts, entry.size));

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
        source_segment_ids: &HashSet<u64>,
        #[cfg(test)] before_relocate: &BeforeRelocateHook<'_>,
        #[cfg(test)] after_history_relocate: &AfterHistoryRelocateHook<'_>,
    ) -> io::Result<(Vec<usize>, bool)> {
        let safe_to_reclaim = AtomicBool::new(true);
        // Use global thread pool for segment allocation
        let segments = COMBINE_ALLOC_POOL.install(|| {
            pending_segments
                .par_iter()
                .map(|dummy_seg| -> io::Result<_> {
                    let new_seg = chunk
                        .allocator
                        .alloc_seg_with_class(&chunk.file_manager, dummy_seg.segment_class)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::OutOfMemory,
                                "no space left during combine",
                            )
                        })?;
                    let unpublished = UnpublishedSegment::new(chunk, new_seg);
                    let new_seg = unpublished.segment();
                    let new_seg_id = new_seg.id;
                    let mut relocations = Vec::with_capacity(dummy_seg.entries.len());
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
                        if entry.kind == RevisionKind::Tombstone {
                            new_seg.tombstones.fetch_add(1, Ordering::Relaxed);
                        }
                        relocations.push(Relocation {
                            new: seg_cursor,
                            old: entry_addr,
                            key: entry.key,
                            kind: entry.kind,
                            entry_size: entry.size,
                            history_live: entry.history_live,
                            direct_tombstone_live: entry.direct_tombstone_live,
                        });
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
                    if new_seg.wal_storage_configured() {
                        if let Err(error) = new_seg
                            .write_wal(new_seg.addr, used_size as u32, true)
                            .and_then(|()| new_seg.force_wal_sync_required())
                        {
                            return Err(unpublished.discard(error));
                        }
                    }
                    cleaned_total_live_space.fetch_add(new_seg.used_spaces() as usize, Relaxed);
                    Ok((unpublished, relocations))
                })
                .map(|result| -> io::Result<usize> {
                    let (unpublished, relocations) = result?;
                    let segment = unpublished.segment();
                    trace!(
                        "Putting new segment {}, revisions {}",
                        segment.id,
                        relocations.len()
                    );
                    match segment.archive() {
                        Err(error) => return Err(unpublished.discard(error)),
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
                    }
                    let segment = unpublished.publish();
                    let new_seg_id = segment.id as usize;
                    chunk.put_segment(segment);
                    let new_seg = chunk.segs.get(&new_seg_id).unwrap();
                    let mut sorted_relocations = relocations;
                    sorted_relocations.sort_by_key(|relocation| {
                        (
                            relocation.key.id.lower,
                            relocation.key.id.higher,
                            relocation.key.revision_ts,
                        )
                    });

                    COMBINE_UPDATE_POOL.install(|| {
                        sorted_relocations
                            .into_par_iter()
                            .for_each(|relocation| {
                                #[cfg(test)]
                                before_relocate(
                                    relocation.key.id,
                                    relocation.key.revision_ts,
                                    relocation.old,
                                    relocation.new,
                                );
                                let history_result = chunk.history.relocate(
                                    relocation.key.id,
                                    relocation.key.revision_ts,
                                    relocation.old,
                                    relocation.new,
                                );
                                #[cfg(test)]
                                after_history_relocate(
                                    relocation.key.id,
                                    relocation.key.revision_ts,
                                    relocation.old,
                                    relocation.new,
                                );
                                match history_result {
                                    RelocateResult::LostRace => {
                                        if relocation.kind == RevisionKind::Cell
                                            && !relocation.history_live
                                        {
                                            match chunk.compare_exchange_current_only_address(
                                                relocation.key.id,
                                                relocation.key.revision_ts,
                                                relocation.old,
                                                relocation.new,
                                                source_segment_ids,
                                            ) {
                                                CurrentAddressRelocation::Moved => return,
                                                CurrentAddressRelocation::NoLongerCurrent => {}
                                                CurrentAddressRelocation::Inconsistent => {
                                                    safe_to_reclaim
                                                        .store(false, Ordering::Release);
                                                }
                                            }
                                        } else if relocation.kind == RevisionKind::Tombstone
                                            && relocation.direct_tombstone_live
                                        {
                                            return;
                                        }
                                        chunk.mark_dead_entry_with_size(
                                            relocation.new,
                                            relocation.entry_size as u32,
                                            &new_seg,
                                        );
                                    }
                                    RelocateResult::HistoricalMoved
                                    | RelocateResult::CurrentPresentMoved => {
                                        // Decode supplied the physical kind before
                                        // publication. This is required for Aborted,
                                        // whose state tag intentionally does not retain
                                        // present-vs-deleted kind.
                                        if relocation.kind == RevisionKind::Cell
                                            && chunk.compare_exchange_current_address(
                                                relocation.key.id,
                                                relocation.key.revision_ts,
                                                relocation.old,
                                                relocation.new,
                                            ) == CurrentAddressRelocation::Inconsistent
                                        {
                                            // The history word moved first, so a mirror
                                            // failure must either be proven historical
                                            // under the cell-index guard or rolled back
                                            // before the exclusive source can be freed.
                                            // The source remains registered on this error
                                            // path. A successful reverse CAS makes this
                                            // destination copy unreachable and therefore
                                            // dead exactly once.
                                            match chunk.history.relocate(
                                                relocation.key.id,
                                                relocation.key.revision_ts,
                                                relocation.new,
                                                relocation.old,
                                            ) {
                                                RelocateResult::HistoricalMoved
                                                | RelocateResult::CurrentPresentMoved => {
                                                    chunk.mark_dead_entry_with_size(
                                                        relocation.new,
                                                        relocation.entry_size as u32,
                                                        &new_seg,
                                                    );
                                                }
                                                RelocateResult::LostRace => {
                                                    error!(
                                                        "Cleaner could not roll revision {:?} timestamp {} back from {} to {} after mirror publication failed",
                                                        relocation.key.id,
                                                        relocation.key.revision_ts,
                                                        relocation.new,
                                                        relocation.old
                                                    );
                                                }
                                            }
                                            error!(
                                                "Cleaner moved current history node {:?} revision {} from {} to {} but could not reconcile its cell-index mirror",
                                                relocation.key.id,
                                                relocation.key.revision_ts,
                                                relocation.old,
                                                relocation.new
                                            );
                                            safe_to_reclaim.store(false, Ordering::Release);
                                        }
                                    }
                                }
                            });
                    });
                    Ok(new_seg_id)
                })
                .collect::<io::Result<Vec<_>>>()
        })?;
        Ok((segments, safe_to_reclaim.load(Ordering::Acquire)))
    }

    fn cleanup_segments(chunk: &Chunk, segments: &[SegmentCandidate]) -> io::Result<()> {
        debug!("Released references for {} source segments", segments.len());
        for old_seg in segments {
            if !chunk.remove_segment(old_seg.id)? {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("cleaner source segment {} disappeared", old_seg.id),
                ));
            }
            old_seg.mem_drop(chunk);
        }
        Ok(())
    }

    pub fn combine_segments(
        chunk: &Chunk,
        selected_segments: &Vec<lightning::aarc::Arc<Segment>>,
    ) -> io::Result<(usize, usize)> {
        #[cfg(test)]
        {
            return Self::combine_segments_with_hook(
                chunk,
                selected_segments,
                &|_, _, _, _| {},
                &|_, _, _, _| {},
            );
        }
        #[cfg(not(test))]
        {
            Self::combine_segments_with_hook(chunk, selected_segments)
        }
    }

    #[cfg(test)]
    pub(crate) fn combine_segments_with_relocation_hook<F>(
        chunk: &Chunk,
        selected_segments: &Vec<lightning::aarc::Arc<Segment>>,
        before_relocate: F,
    ) -> io::Result<(usize, usize)>
    where
        F: Fn(Id, u64, usize, usize) + Sync,
    {
        Self::combine_segments_with_hook(
            chunk,
            selected_segments,
            &before_relocate,
            &|_, _, _, _| {},
        )
    }

    #[cfg(test)]
    pub(crate) fn combine_segments_with_relocation_hooks<F, G>(
        chunk: &Chunk,
        selected_segments: &Vec<lightning::aarc::Arc<Segment>>,
        before_relocate: F,
        after_history_relocate: G,
    ) -> io::Result<(usize, usize)>
    where
        F: Fn(Id, u64, usize, usize) + Sync,
        G: Fn(Id, u64, usize, usize) + Sync,
    {
        Self::combine_segments_with_hook(
            chunk,
            selected_segments,
            &before_relocate,
            &after_history_relocate,
        )
    }

    fn combine_segments_with_hook(
        chunk: &Chunk,
        selected_segments: &Vec<lightning::aarc::Arc<Segment>>,
        #[cfg(test)] before_relocate: &BeforeRelocateHook<'_>,
        #[cfg(test)] after_history_relocate: &AfterHistoryRelocateHook<'_>,
    ) -> io::Result<(usize, usize)> {
        let segments = Self::select_candidate_segments(chunk, selected_segments);
        if segments.is_empty() {
            return Ok((0, 0));
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
                return Ok((0, 0));
            }

            debug!(
                "Updating cell reference, pending segments {}",
                pending_segments.len()
            );

            // Use global thread pool for segment allocation
            let (new_segs, safe_to_reclaim) = Self::execute_combine_phases(
                chunk,
                pending_segments,
                &cleaned_total_live_space,
                &segment_ids_to_combine,
                #[cfg(test)]
                before_relocate,
                #[cfg(test)]
                after_history_relocate,
            )?;
            if !safe_to_reclaim {
                error!(
                    "Cleaner retained source segments {:?} because current history and cell-index publication could not be reconciled",
                    segment_ids_to_combine
                );
                return Ok((0, 0));
            }

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

        Self::cleanup_segments(chunk, &segments)?;

        debug!(
            "End combining segments, totally cleaned {} bytes, with {} segments.",
            space_cleaned, len_cleaned_segments
        );
        debug_assert!(num_reduced_segments >= 0);
        Ok((space_cleaned, num_reduced_segments as usize))
    }
}
