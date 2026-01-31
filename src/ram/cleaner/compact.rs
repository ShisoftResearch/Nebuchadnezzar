use super::super::chunk::Chunk;
use super::super::segs::{Segment, SEGMENT_SIZE};
use crate::ram::cleaner::SegmentCandidate;
use crate::ram::entry::*;

use std::sync::atomic::Ordering;

use itertools::Itertools;
use libc;
use lightning::map::Map;

pub struct CompactCleaner;

impl CompactCleaner {
    pub fn clean_segment(chunk: &Chunk, seg: SegmentCandidate) -> usize {
        // Safety check: Never clean the head segment
        // Although segs_for_compact_cleaner filters this, race conditions could cause
        // the head to change or a new head to be created that looks like a candidate.
        if seg.id == chunk.get_head_seg_id() {
            debug!("Segment {} is the current head, skipping cleaning", seg.id);
            return 0;
        }

        // Clean only if segment have fragments
        let dead_space = seg.total_dead_space();
        if dead_space == 0 {
            debug!(
                "Skip cleaning chunk {} segment {} for it have no dead spaces",
                chunk.id, dead_space
            );
            return 0;
        }

        // Previous implementation is inplace compaction. Segments are mutable and subject to changes.
        // Log-structured cleaner suggests new segment allocation and copy living entries from the
        // old segment to new segment. The new segment should have smaller sizes than the old one.
        // In this way locks can be straight forward, copy those entries to the new segment, change
        // cell indices by lock cells first, remove the old segment.
        // Segment locks will no long be required for transfer process will ensure there will be no
        // on going read operations to the old segment when the segment to be deleted.

        // Some comments regards to RAMCloud seglets. To compress the actual memory spaces consumed by
        // segments, RAMCloud introduces seglets as the minimal unit of the memory. It records a mapping
        // from segment to seglets it consumed. In this case, compacted segments will take less seglets.
        // Freed seglets will be used by other new allocated segments, which can leads to incontinently.
        // Actually, malloc already handled this situation to overcome fragmentation, we can simply use
        // malloc to allocate new memory spaces for segments than maintaining seglets mappings in userspace.
        debug!(
            "Compact cleaning segment {} from chunk {}, is hot {}, segment address {:#x}",
            seg.id,
            chunk.id,
            seg.is_hot(),
            seg.addr
        );

        // scan and mark live entries
        let entries = chunk.live_entries(&*seg).collect_vec();

        if entries.len() == 0 {
            chunk.remove_segment(seg.id);
            seg.mem_drop(chunk);
            debug!(
                "Compact segment {} leades to remove the segment for it is empty",
                seg.id
            );
            return SEGMENT_SIZE;
        }

        // Record original used space before compaction
        let original_used_space = seg.used_spaces() as usize;
        debug!(
            "Segment {} from chunk {}. Compacting {} entries, original used space {} bytes.",
            seg.id,
            chunk.id,
            entries.len(),
            original_used_space
        );

        let seg_addr = seg.addr;
        let mut cursor = seg_addr;
        let mut released_tombstones = 0;
        // Compact in place
        entries
            .into_iter()
            .for_each(|entry: Entry| {
                let entry_size = entry.meta.entry_size;
                let entry_pos = entry.meta.entry_pos;
                if cursor != entry_pos {
                    // Need to move
                    if entry.meta.entry_header.entry_type == EntryType::CELL {
                        // Is cell - acquire lock FIRST before deciding whether to move
                        let header = entry.content.as_cell_header();
                        trace!(
                            "Acquiring cell guard for update on compact {:?}",
                            header.id()
                        );
                        let cell_guard = chunk.cell_index.lock(header.hash as usize);

                        // Check if cell is still at expected location BEFORE moving
                        if let Some(mut guard) = cell_guard {
                            let actual_addr = *guard;
                            if actual_addr == entry_pos {
                                // Cell still at expected location, safe to move
                                let old_addr = entry_pos;
                                let new_addr = cursor;

                                trace!(
                                    "Memcpy cell entry, size: {}, from {} to {}, seg_addr {}, for {:?}",
                                    entry_size,
                                    old_addr,
                                    new_addr,
                                    seg_addr,
                                    entry.content
                                );

                                // Now safe to move - we hold the lock
                                unsafe {
                                    libc::memmove(
                                        new_addr as *mut libc::c_void,
                                        old_addr as *mut libc::c_void,
                                        entry_size,
                                    );
                                }

                                // Update cell_index to point to new location
                                *guard = new_addr;
                                cursor += entry_size;
                            } else {
                                // Cell address changed - another thread updated it
                                // Don't move this stale entry, don't advance cursor
                                trace!(
                                    "Cell {:?} address changed from {} to {} during compact - skipping stale entry",
                                    header.id(),
                                    entry_pos,
                                    actual_addr
                                );
                            }
                        } else {
                            // Cell was deleted - don't move, don't advance cursor
                            trace!(
                                "Cell {:?} was deleted during compact - skipping entry at {}",
                                header.id(),
                                entry_pos
                            );
                        }
                    } else {
                        // Tombstone - always safe to move (no cell_index entry)
                        let old_addr = entry_pos;
                        let new_addr = cursor;
                        let tombstone = entry.content.as_tombstone();
                        let cell_hash = tombstone.hash;
                        let seg_id = tombstone.segment_id;
                        if chunk.cell_index.contains_key(&(cell_hash as usize)) || !chunk.contains_seg(seg_id) {
                            trace!("Tombstone entry {:?} is obsolete, skipping moving", cell_hash);
                            released_tombstones += 1;
                            return;
                        }

                        trace!(
                            "Memcpy tombstone entry, size: {}, from {} to {}",
                            entry_size,
                            old_addr,
                            new_addr
                        );

                        unsafe {
                            libc::memmove(
                                new_addr as *mut libc::c_void,
                                old_addr as *mut libc::c_void,
                                entry_size,
                            );
                        }

                        cursor += entry_size;
                    }
                } else {
                    // Entry already at correct position, just advance cursor
                    cursor += entry_size;
                }
            });
        seg.append_header.store(cursor, Ordering::Release);
        seg.tombstones
            .fetch_sub(released_tombstones as u32, Ordering::Relaxed);
        seg.set_dirty();
        let used_size = cursor - seg_addr;
        if used_size < SEGMENT_SIZE {
            seg.shrink(used_size);
        }

        // Calculate space cleaned from original to final used space
        // Note: final space may include entries we kept that weren't in live_entries scan
        // due to concurrent updates, so use actual used_spaces() values
        let final_used_space = seg.used_spaces() as usize;
        let space_cleaned = if original_used_space >= final_used_space {
            original_used_space - final_used_space
        } else {
            // Can happen if entries were added during compaction (shouldn't normally happen)
            warn!(
                "Segment {} used space increased during compaction: {} -> {}",
                seg.id, original_used_space, final_used_space
            );
            0
        };
        debug!(
            "Clean finished for segment {} from chunk {}, cleaned {} bytes ({} -> {}) released {} tombstones",
            seg.id, chunk.id, space_cleaned, original_used_space, final_used_space, released_tombstones
        );

        if space_cleaned == 0 {
            seg.mark_clean_no_progress();
        } else {
            seg.clear_clean_no_progress();
            // After compaction all live entries have been packed, reset dead bytes counter
            seg.dead_space.store(0, Ordering::Relaxed);
        }

        // We are not going to archive here to reduce write amplification.
        // Eviction will archive the segment for tiered memory
        // For recovery, even if the segment is not archived, it will still be able to recover from the old backup file.

        space_cleaned
    }
}
