use crate::ram::chunk::Chunk;
use crate::ram::entry::EntryContent;
use crate::ram::segs::{Segment, SEGMENT_SIZE};
use itertools::Itertools;
use rayon::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

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
    entries: Vec<DummyEntry>,
}

impl DummySegment {
    fn new() -> DummySegment {
        DummySegment {
            head: 0,
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
    pub fn combine_segments(chunk: &Chunk, segments: &Vec<Arc<Segment>>) -> usize {
        if segments.len() < 2 {
            trace!(
                "too few segments to combine, chunk {}, segments {}",
                chunk.id,
                segments.len()
            );
            return 0;
        }
        debug!("Combining segments");

        let space_to_collect = segments
            .iter()
            .map(|seg| seg.used_spaces() as usize)
            .sum::<usize>();
        let segment_ids_to_combine: HashSet<_> = segments.iter().map(|seg| seg.id).collect();

        // Get all entries in segments to combine and order them by data temperature and size
        let nested_entries = segments
            .iter()
            .flat_map(|seg| chunk.live_entries(seg))
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
                    _ => None,
                };
                DummyEntry {
                    size: entry_size,
                    addr: entry_addr,
                    timestamp: cell_header.map(|h| h.timestamp).unwrap_or(0),
                    cell_hash: cell_header.map(|h| h.hash),
                }
            })
            .group_by(|entry| entry.timestamp / 10)
            .into_iter()
            .map(|(t, group)| {
                let mut group: Vec<_> = group.collect();
                group.sort_by_key(|entry| entry.size);
                return (t, group.into_iter());
            })
            .sorted_by_key(|&(t, _)| t)
            .into_iter()
            .map(|(_, group)| group);

        let mut entries: Vec<_> = Iterator::flatten(nested_entries)
            // order by temperature and size from greater to lesser
            .rev()
            // provide additional state for whether entry have been claimed on simulation
            .map(|e| (e, false))
            .collect();

        debug!("Found {} entries to combine", entries.len());

        let mut space_cleaned = 0;
        if entries.len() > 0 {
            // Simulate the combine process to determine the efficiency
            let mut pending_segments = Vec::with_capacity(segments.len());
            pending_segments.push(DummySegment::new());
            let entries_num = entries.len();
            for (index, entry_pair) in entries.iter_mut().enumerate() {
                let segment_space_remains = SEGMENT_SIZE - pending_segments.last().unwrap().head;
                let entry = &entry_pair.0;
                let entry_size = entry.size;
                if entry_pair.1 {
                    // entry claimed
                    continue;
                } else if entry_size > segment_space_remains {
                    if index == entries_num - 1 {
                        // iterated to the last one, which means no more entries can be placed
                        // in the segment, then create a new segment
                        pending_segments.push(DummySegment::new());
                    }
                } else {
                    let last_segment = pending_segments.last_mut().unwrap();
                    last_segment.entries.push(entry.clone());

                    // pump dummy segment head pointer
                    last_segment.head += entry_size;

                    // mark entry claimed
                    entry_pair.1 = true;
                }
            }

            debug!("Checking combine feasibility");
            let pending_segments_len = pending_segments.len();
            let segments_to_combine_len = segments.len();
            let cleaned_total_live_space = AtomicUsize::new(0);
            if pending_segments_len >= segments_to_combine_len {
                warn!(
                    "Trying to combine segments but resulting segments still does not go down {}/{}",
                    pending_segments_len, segments_to_combine_len
                );
            }

            debug!(
                "Updating cell reference, pending segments {}",
                pending_segments.len()
            );
            pending_segments
                .par_iter()
                .map(|dummy_seg| {
                    let new_seg = chunk
                        .allocator
                        .alloc_seg(&chunk.backup_storage, &chunk.wal_storage)
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
                            cell_mapping.push((seg_cursor, entry_addr, cell_hash));
                        }
                        seg_cursor += entry.size;
                    }
                    new_seg.append_header.store(seg_cursor, Ordering::Relaxed);
                    new_seg.shrink(seg_cursor - new_seg.addr);
                    cleaned_total_live_space.fetch_add(new_seg.used_spaces() as usize, Relaxed);
                    return (new_seg, cell_mapping);
                })
                .for_each(|(segment, cells)| {
                    trace!("Putting new segment {}, cells {}", segment.id, cells.len());
                    segment.archive().unwrap();
                    let new_seg_id = segment.id as usize;
                    chunk.put_segment(segment);
                    let new_seg = chunk.segs.get(&new_seg_id).unwrap();
                    cells.into_par_iter().for_each(|(new, old, hash)| {
                        trace!("Reset cell {} ptr from {} to {}", hash, old, new);
                        let index = chunk.cell_index.lock(hash as usize);
                        if let Some(mut actual_addr) = index {
                            if *actual_addr == old {
                                *actual_addr = new;
                                trace!(
                                    "Cell addr for hash {} set from {} to {} for combine",
                                    hash,
                                    old,
                                    new
                                );
                            } else {
                                trace!(
                                    "cell {} with address {}, have been changed to {} on combine",
                                    hash,
                                    old,
                                    *actual_addr
                                );
                                chunk.mark_dead_entry_with_seg(new, &new_seg);
                            }
                        } else {
                            trace!("cell {} address {} have been removed on combine", hash, old);
                            let _ = chunk.put_tombstone_by_cell_loc(new);
                        }
                    });
                });
            space_cleaned = space_to_collect - cleaned_total_live_space.load(Relaxed);
            debug!(
                "Combined {} segments to {}, total {} bytes",
                segments_to_combine_len,
                pending_segments.len(),
                space_cleaned
            );
        } else {
            debug!("No entries to work on, will remove all selected segments instead");
        }

        debug!("Removing {} old segments", segments.len());
        for old_seg in segments {
            chunk.remove_segment(old_seg.id);
            old_seg.mem_drop(chunk);
        }
        space_cleaned
    }
}
