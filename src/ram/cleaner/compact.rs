use super::super::chunk::Chunk;
use super::super::segs::{Segment, SEGMENT_SIZE};
use crate::ram::entry::*;

use std::sync::atomic::Ordering;

use itertools::Itertools;
use libc;

pub struct CompactCleaner;

impl CompactCleaner {
    pub fn clean_segment(chunk: &Chunk, seg: &Segment) -> usize {
        // Clean only if segment have fragments
        let dead_space = seg.total_dead_space();
        if dead_space == 0 {
            trace!(
                "Skip cleaning chunk {} segment {} for it have no dead spaces",
                chunk.id,
                dead_space
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
            "Compact cleaning segment {} from chunk {}",
            seg.id, chunk.id
        );

        // scan and mark live entries
        // estimate segment live size for new allocation
        let mut live_size: usize = 0;
        let entries = chunk
            .live_entries(seg)
            .map(|entry| {
                live_size += entry.meta.entry_size;
                entry
            })
            .collect_vec();
        if entries.len() == 0 {
            chunk.remove_segment(seg.id);
            seg.mem_drop(chunk);
            debug!(
                "Compact segment {} leades to remove the segment for it is empty",
                seg.id
            );
            return SEGMENT_SIZE;
        }
        debug!(
            "Segment {} from chunk {}. Total size {} bytes for new segment.",
            seg.id, chunk.id, live_size
        );
        let new_seg = chunk
            .allocator
            .alloc_seg(&chunk.backup_storage, &chunk.wal_storage)
            .expect("No space left during compact");
        let seg_addr = new_seg.addr;
        new_seg
            .append_header
            .store(seg_addr + live_size, Ordering::Relaxed);
        let new_seg_id = new_seg.id as usize;
        // Put the segment into the chunk right after it was allocated *BEFORE* any cell have been moved on it.
        chunk.put_segment(new_seg);
        let new_seg = chunk.segs.get(&new_seg_id).unwrap();
        let mut cursor = seg_addr;
        entries
            .into_iter()
            .map(|e: Entry| {
                let entry_size = e.meta.entry_size;
                let entry_pos = e.meta.entry_pos;
                trace!(
                    "Memcpy entry, size: {}, from {} to {}, bond {}, base {}, range {} for {:?}",
                    entry_size,
                    entry_pos,
                    cursor,
                    seg_addr + live_size,
                    seg_addr,
                    live_size,
                    e.content
                );
                let result = (e, cursor);
                unsafe {
                    libc::memcpy(
                        cursor as *mut libc::c_void,
                        entry_pos as *mut libc::c_void,
                        entry_size,
                    );
                }
                cursor += entry_size;
                return result;
            })
            .filter(|pair| pair.0.meta.entry_header.entry_type == EntryType::CELL)
            .for_each(|(entry, new_addr)| {
                let header = entry.content.as_cell_header();
                trace!(
                    "Acquiring cell guard for update on compact {:?}",
                    header.id()
                );
                #[cfg(feature = "fast_map")]
                let index = chunk.cell_index.lock(header.hash as usize);
                #[cfg(feature = "slow_map")]
                let index = chunk.index.get_mut(&header.hash);

                let old_addr = entry.meta.entry_pos;
                if let Some(mut cell_guard) = index {
                    if *cell_guard == old_addr {
                        *cell_guard = new_addr;
                    } else {
                        trace!(
                            "Cell {:?} address {} have been changed to {} on relocating on compact",
                            entry.content,
                            old_addr,
                            *cell_guard
                        );
                        drop(cell_guard);
                        chunk.mark_dead_entry_with_seg(new_addr, &new_seg);
                    }
                } else {
                    trace!(
                        "Cell {:?} address {} have been remove during compact",
                        entry.content,
                        old_addr
                    );
                    let _ = chunk.put_tombstone_by_cell_loc(new_addr);
                }
            });
        new_seg.shrink(cursor - seg_addr);
        chunk.remove_segment(seg.id);
        seg.mem_drop(chunk);

        let space_cleaned = seg.used_spaces() as usize - live_size;
        debug!(
            "Clean finished for segment {} from chunk {}, cleaned {}",
            seg.id, chunk.id, space_cleaned
        );
        space_cleaned
    }
}
