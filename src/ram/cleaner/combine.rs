use ram::chunk::Chunk;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::collections::HashSet;
use ram::segs::{Segment, MAX_SEGMENT_SIZE};
use ram::entry::{EntryContent, Entry, EntryType};
use itertools::Itertools;

use libc;

pub struct CombinedCleaner;

#[derive(Clone)]
struct DummyEntry {
    size: usize,
    addr: usize,
    cell_hash: Option<u64>,
    timestamp: u32,
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

// for higher hit rate for fetching cells in segments, we need to put data with close timestamp together
// this optimization is intended for enabling neb to contain data more than it's memory

impl CombinedCleaner {
    pub fn combine_segments(chunk: &Chunk, segments: &Vec<Arc<Segment>>) {
        if segments.len() < 2 { return }
        debug!("Combining segments");

        let segment_ids_to_combine: HashSet<_> = segments.iter().map(|seg| seg.id).collect();

        debug!("get all entries in segments to combine and order them by data temperature and size");
        let nested_entries = segments
            .iter()
            .flat_map(|seg| {
                chunk.live_entries(seg)
            })
            .filter(|entry| {
                // live entries have done a lot of filtering work already
                // but we still need to remove those tombstones that pointed to segments we are about to combine
                if let EntryContent::Tombstone(ref tombstone) = entry.content {
                    return !segment_ids_to_combine.contains(&tombstone.segment_id);
                }
                return true;
            })
            .map(|entry| {
                let entry_size = entry.meta.entry_size;
                let entry_addr = entry.meta.entry_pos;
                let cell_header = match entry.content {
                    EntryContent::Cell(header) => Some(header),
                    _ => None
                };
                DummyEntry {
                    size: entry_size,
                    addr: entry_addr,
                    timestamp: cell_header.map(|h| h.timestamp).unwrap_or(0),
                    cell_hash: cell_header.map(|h| h.hash),
                }
            })
            .group_by(|entry| {
                entry.timestamp / 10
            })
            .into_iter()
            .map(|(t, group)| {
                let mut group: Vec<_> = group.collect();
                group.sort_by_key(|entry| entry.size);
                return (t, group.into_iter());
            })
            .sorted_by_key(|&(t, _)| t)
            .into_iter()
            .map(|(_, group)| group);

        let mut entries: Vec<_> = Iterator::flatten(nested_entries).collect();

        // order by temperature and size from greater to lesser
        entries.reverse();

        // provide additional state for whether entry have been claimed on simulation
        let mut entries: Vec<_> = entries.into_iter().map(|e| (e, false)).collect();

        debug!("simulate the combine process to determine the efficiency");
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

            let entry = &entry_pair.0;
            let entry_size = entry.size;
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
            last_segment.entries.push(entry.clone());

            // pump dummy segment head pointer
            last_segment.head += entry_size;

            // mark entry claimed
            entry_pair.1 = true;
            entries_to_claim -= 1;

            // move to next entry
            cursor += 1;
        }

        debug!("Checking combine feasibility");
        let pending_segments_len = pending_segments.len();
        let segments_to_combine_len = segments.len();
        if pending_segments_len >= segments_to_combine_len  {
            warn!("Trying to combine segments but resulting segments still does not go down {}/{}",
                  pending_segments_len, segments_to_combine_len);
        }
        
        debug!("Updating cell reference");
        pending_segments
            .iter()
            .map(|dummy_seg| {
                let new_seg_id = chunk.next_segment_id();
                let new_seg = Segment::new(new_seg_id, dummy_seg.head, &chunk.backup_storage, &chunk.wal_storage);
                let mut cell_mapping = Vec::with_capacity(dummy_seg.entries.len());
                let mut seg_cursor = new_seg.addr;
                debug!("Combining segment to new one with id {}", new_seg_id);
                for entry in &dummy_seg.entries {
                    let entry_addr = entry.addr;
                    unsafe {
                        libc::memcpy(
                            seg_cursor as *mut libc::c_void,
                            entry_addr as *mut libc::c_void,
                            entry.size);
                    }
                    if let Some(cell_hash) = entry.cell_hash {
                        debug!("Marked cell relocation hash {}, addr {} to segment {}", cell_hash, entry_addr, new_seg_id);
                        cell_mapping.push((seg_cursor, entry_addr, cell_hash));
                    }
                    seg_cursor += entry.size;
                }
                new_seg.append_header.store(seg_cursor, Ordering::Relaxed);
                return (new_seg, cell_mapping);
            })
            .flat_map(|(segment, cells)| {
                debug!("Putting new segment {}", segment.id);
                let seg_ref = Arc::new(segment);
                chunk.put_segment(seg_ref.clone());
                seg_ref.archive();
                return cells;
            })
            .for_each(|(new, old, hash)| {
                debug!("Reset cell {} ptr from {} to {}", hash, old, new);
                if let Some(mut actual_addr) = chunk.index.get_mut(&hash) {
                    if *actual_addr == old {
                        *actual_addr = new
                    } else {
                        warn!("cell {} with address {}, have been changed to {} on combine", hash, old, *actual_addr);
                    }
                } else {
                    warn!("cell {} address {} have been removed on combine", hash, old);
                }
            });

        debug!("Removing old segments");
        for old_seg in segments {
            chunk.remove_segment(old_seg.id);
        }
    }

}