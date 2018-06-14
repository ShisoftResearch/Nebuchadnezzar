use ram::chunk::Chunk;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use ram::segs::{Segment, MAX_SEGMENT_SIZE};
use ram::entry::{EntryContent, Entry, EntryType};
use libc;

pub struct CombinedCleaner;

struct DummyEntry {
    size: usize,
    addr: usize,
    cell: Option<u64>
}

struct DummySegment {
    head: usize,
    entries: Vec<DummyEntry>
}

impl DummySegment {
    fn new() -> DummySegment {
        DummySegment {
            head: 0, entries: Vec::new()
        }
    }
}

// Combine small segments into larger segments
// This cleaner will perform a greedy approach to relocate entries from old segments to fewer
// new segments to reduce number or segments and reclaim spaces from tombstones

impl CombinedCleaner {
    pub fn combine_segments(chunk: &Chunk, segments: &Vec<Arc<Segment>>) {
        if segments.len() < 2 { return }
        debug!("Combining segments");

        // get all entries in segments to combine
        let mut entries: Vec<_> = segments
            .iter()
            .flat_map(|seg| {
                chunk.live_entries(seg)
            })
            .collect();

        // sort entries from larger one to smaller
        entries.sort_by(|entry1,entry2| {
            let size1 = entry1.meta.entry_size;
            let size2 = entry2.meta.entry_size;
            size1.cmp(&size2).reverse()
        });

        // provide additional state for whether entry have been claimed on simulation
        let mut entries: Vec<_> = entries.into_iter().map(|e| (e, false)).collect();

        // simulate the combine process to determine the efficiency
        let mut pending_segments = Vec::with_capacity(segments.len());
        let entries_num = entries.len();
        let mut entries_to_claim = entries_num;
        let mut cursor = 0;
        pending_segments.push(DummySegment::new());
        while entries_to_claim > 0 {
            let segment_space_remains =
                MAX_SEGMENT_SIZE - pending_segments.last().unwrap().head;
            let index = cursor % entries_num;
            let entry_pair = entries.get_mut(index).unwrap();
            if entry_pair.1 {
                // entry claimed
                cursor += 1;
                continue;
            }
            let entry: &Entry = &entry_pair.0;
            let entry_size = entry.meta.entry_size;
            let entry_addr = entry.meta.entry_pos;
            if entry_size > segment_space_remains {
                if index == entries_num - 1 {
                    // iterated to the last one, which means no more entries can be placed
                    // in the segment, then create a new segment
                    pending_segments.push(DummySegment::new());
                }
                cursor += 1;
                continue;
            }
            let mut last_segment = pending_segments.last_mut().unwrap();
            last_segment.entries.push(DummyEntry {
                size: entry_size,
                addr: entry_addr,
                cell: match entry.content {
                    EntryContent::Cell(header) => Some(header.hash),
                    _ => None
                }
            });

            // pump dummy segment head pointer
            last_segment.head += entry_size;

            // mark entry claimed
            entry_pair.1 = true;
            entries_to_claim -= 1;

            // move to next entry
            cursor += 1;
        }

        let pending_segments_len = pending_segments.len();
        let segments_to_combine_len = segments.len();
        if pending_segments_len >= segments_to_combine_len  {
            warn!("Trying to combine segments but resulting segments still does not go down {}/{}",
                  pending_segments_len, segments_to_combine_len);
        }
        pending_segments
            .iter()
            .map(|dummy_seg| {
                let new_seg_id = chunk.seg_counter.fetch_add(1, Ordering::Relaxed);
                let new_seg = Segment::new(new_seg_id, dummy_seg.head, &chunk.backup_storage);
                let mut cell_mapping = Vec::with_capacity(dummy_seg.entries.len());
                let mut seg_cursor = new_seg.addr;
                for entry in &dummy_seg.entries {
                    let entry_addr = entry.addr;
                    unsafe {
                        libc::memcpy(
                            seg_cursor as *mut libc::c_void,
                            entry_addr as *mut libc::c_void,
                            entry.size);
                    }
                    if let Some(cell_hash) = entry.cell {
                        cell_mapping.push((seg_cursor, entry_addr, cell_hash));
                    }
                    seg_cursor += entry.size;
                }
                return (new_seg, cell_mapping);
            })
            .flat_map(|(segment, cells)| {
                let seg_ref = Arc::new(segment);
                chunk.put_segment(seg_ref.clone());
                seg_ref.archive();
                return cells;
            })
            .for_each(|(new, old, hash)| {
                if let Some(mut actual_addr) = chunk.index.get_mut(&hash) {
                    if *actual_addr == old {
                        *actual_addr = new
                    } else {
                        warn!("cell address {}, have been changed to {} on combine", old, *actual_addr);
                    }
                }
            });
        for old_seg in segments {
            chunk.remove_segment(old_seg.id);
        }
    }

}