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
use itertools::Itertools;

pub struct CompactCleaner;

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
        debug!("Compact cleaning segment {} from chunk {}", seg.id, chunk.id);

        // scan and mark live entries
        // estimate segment live size for new allocation
        let mut live_size: usize = 0;
        let mut entries = chunk
            .live_entries(seg)
            .map(|entry| {
                live_size += entry.meta.entry_size;
                entry
            })
            .collect_vec();
        debug!("Segment {} from chunk {}. Total size {} bytes for new segment.",
               seg.id, chunk.id, live_size);
        let new_seg = Arc::new(Segment::new(seg.id, live_size, &chunk.backup_storage, &chunk.wal_storage));
        let seg_addr = new_seg.addr;
        let mut cursor = seg_addr;
        entries
            .into_iter()
            .map(|e: Entry| {
                let entry_size = e.meta.entry_size;
                let entry_pos = e.meta.entry_pos;
                let result= (e, cursor);
                debug!("memcpy entry, size: {}, from {} to {}, bond {}, base {}, range {}",
                       entry_size, entry_pos, cursor, seg_addr + live_size, seg_addr, live_size);
                unsafe {
                    libc::memcpy(
                        cursor as *mut libc::c_void,
                        entry_pos as *mut libc::c_void,
                        entry_size);
                }
                cursor += entry_size;
                return result;
            })
            .filter(|pair|
                pair.0.meta.entry_header.entry_type == EntryType::Cell)
            .for_each(|(entry, new_addr)| {
                if let EntryContent::Cell(header) = entry.content {
                    debug!("Acquiring cell guard for update on compact {:?}", header.id());
                    if let Some(mut cell_guard) = chunk.index.get_mut(&header.hash) {
                        let old_addr = entry.meta.entry_pos;
                        if *cell_guard == old_addr {
                            *cell_guard = new_addr;
                        } else {
                            warn!("cell address {} have been changed to {} on relocating on compact",
                                  old_addr, *cell_guard);
                        }
                    }
                    debug!("Cell location updated for compact");
                } else {
                    panic!("not cell after filter")
                }
            });

        new_seg.append_header.store(new_seg.addr + live_size, Ordering::Relaxed);
        // put segment directly into the segment map after to resetting cell addresses as side logs to replace the old one
        chunk.put_segment(new_seg);

        debug!("Clean finished for segment {} from chunk {}", seg.id, chunk.id);
    }
}
