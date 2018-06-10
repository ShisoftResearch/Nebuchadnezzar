use super::*;
use super::super::chunk::{Chunk, Chunks};
use super::super::segs::{Segment};
use ram::entry::*;
use ram::cell::{Cell, CellHeader};
use ram::tombstone::Tombstone;
use dovahkiin::types::Id;

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::sync::Arc;
use std::time::Duration;
use std::collections::BTreeSet;
use std::collections::Bound::{Included, Unbounded};

use libc;
use parking_lot::MutexGuard;

pub struct CompactCleaner {
    chunks: Arc<Chunks>,
    closed: AtomicBool
}

impl CompactCleaner {
    pub fn clean_segment(chunk: &Chunk, seg: &Arc<Segment>) {
        // Clean only if segment have fragments
        if seg.total_dead_space() == 0 {return;}
        
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
        debug!("Cleaning segment {} from chunk {}", seg.id, chunk.id);

        // scan and mark live entries
        let entries = chunk.live_entries(seg);
        let live_size: usize = entries.iter().map(|e| e.meta.entry_size).sum();
        debug!("Segment {} from chunk {} have {} live objects. Total size {} bytes for new segment.",
               seg.id, chunk.id, entries.len(), live_size);
        let new_seg_id = chunk.seg_counter.fetch_add(1, Ordering::Relaxed);
        let new_seg = Arc::new(Segment::new(new_seg_id, live_size, &chunk.backup_storage));
        let mut cursor = new_seg.addr;
        let copied_entries =
            entries
                .iter()
                .map(|e: &Entry| {
                    let entry_size = e.meta.entry_size;
                    let result= (e, cursor);
                    unsafe {
                        libc::memcpy(
                            cursor as *mut libc::c_void,
                            e.meta.entry_pos as *mut libc::c_void,
                            entry_size);
                    }
                    cursor += entry_size;
                    return result;
                });

        new_seg.append_header.store(new_seg.addr + live_size, Ordering::Relaxed);
        // put segment directly into the segment map prior to resetting cell addresses as side logs
        chunk.put_segment(new_seg);

        // update cell address chunk index
        copied_entries
            .filter(|pair|
                pair.0.meta.entry_header.entry_type == EntryType::Cell)
            .for_each(|(entry, new_addr)| {
                if let EntryContent::Cell(header) = entry.content {
                    if let Some(mut cell_guard) = chunk.index.get_mut(&header.hash) {
                        let old_addr = entry.meta.entry_pos;
                        if *cell_guard == old_addr {
                            *cell_guard = new_addr;
                        } else {
                            warn!("cell address {} have been changed to {} on relocating on cleaning",
                                  old_addr, *cell_guard);
                        }
                    }
                } else {
                    panic!("not cell after filter")
                }
            });

        chunk.remove_segment(seg.id);
        debug!("Clean finished for segment {} from chunk {}", seg.id, chunk.id);
    }
}
