use super::super::chunk::{Chunk, Chunks};
use super::super::segs::Segment;

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::sync::Arc;
use std::time::Duration;
use std::collections::BTreeSet;
use std::collections::Bound::{Included, Unbounded};

use libc;
use parking_lot::MutexGuard;

static MAX_CLEAN_RETRY: u16 = 100;

pub fn ceiling_frag(frags: &MutexGuard<BTreeSet<usize>>, location: usize) -> Option<usize> {
    match frags.range((Included(&location), Unbounded)).next() {
        Some(l) => Some(*l),
        None => None
    }
}

pub struct CompactCleaner {
    chunks: Arc<Chunks>,
    closed: AtomicBool
}

impl CompactCleaner {
    pub fn clean_segment(chunk: &Chunk, seg: &Segment) {
        let total_dead_space = seg.total_dead_space();
        // Clean only if segment have fragments
        if total_dead_space == 0 {return;}
        // Retry cleaning the segment if unexpected state discovered
        let mut retried = 0;
        // Previous implementation is inplace compaction. Segments are mutable and subject to changes.
        // Log-structured cleaner suggests new segment allocation and copy living entries from the
        // old segment to new segment. The new segment should have smaller sizes than the old one.
        // In this way lock can be straight forward, copy those entries to the new segment, change
        // cell indices by lock them first, write lock the old segment, remove the old segment.
        debug!("Cleaning segment: {}", seg.addr);
        let mut defrag_pos = seg.addr;
        while retried < MAX_CLEAN_RETRY {
            // those are for moving cell next to the fragment after the segment locking block
            // it will be assigned later
            let  mut next_loc: usize = 0;
            let  mut frag_loc: usize = 0;
            {
                let seg_lock = seg.lock.try_write();
                if seg_lock.is_none() {return;}
                // Cleaner defrag pointers only move forward when cleaning fragments. Which means if new
                // fragments produced before our defrag_pos when cleaning the segment, those fragments
                // will be ignored in this cleaning turn and leave to be cleaned up in next turn. This
                // design meant to prevent long term cleaning and give back spaces by resetting append
                // header as soon as possible if the segment procedures fragments all
                // the time
                let mut frags = seg.frags.lock();
                let frag_opt = ceiling_frag(&frags, defrag_pos);
                if frag_opt.is_none() {
                    debug!("No fragments, will exit for segment: {}", seg.addr);
                    return;
                }  // return if there is no fragments to cleaned
                frag_loc = frag_opt.unwrap();
                debug!("Cleaning fragment at {} for segment: {}", frag_loc, seg.addr);
                // The first things we need to do is read the length of the fragment and check it there
                // is a tombstone at the location to make sure there is no corruption. Corruption is
                // unexpected, we can retry in next iteration but I don't think it can be self-healed.
                let frag_version = unsafe {*seg.cell_version(frag_loc)};
                if frag_version != 0 {
                    error!("There is no tombstone at the fragment location: {} - Version: {}",
                    frag_loc, frag_version);
                    retried += 1; continue;
                }
                let frag_len = unsafe {*seg.cell_size(frag_loc)};
                // Next we need to get the location of the cell or fragment next to the fragment
                next_loc = frag_loc + frag_len as usize;
                // Check if it have reached the append header, which is the last fragment. In this case
                // we need to perform a atomic cas on the append header to move it right at the location
                // of the fragment
                if next_loc == seg.append_header.load(Ordering::SeqCst) {
                    if seg.append_header.compare_and_swap(next_loc, frag_loc, Ordering::SeqCst) != next_loc {
                        // it may failed for some reason, we need to retry it
                        debug!("Segment append header moved when cleaning");
                        retried += 1; continue;
                    } else {
                        // if it succeed, the segment have been cleaned in this turn
                        debug!("Clean fragments completed, will exit for segment: {}", seg.addr);
                        frags.remove(&frag_loc); return;
                    }
                }
                // Then we need to discuss the two type of the unit we may encounter
                let next_version = unsafe {*seg.cell_version(next_loc)};
                // if it is a fragment
                if next_version == 0 {
                    if frags.contains(&next_loc) {
                        debug!("Unit next to fragment {} is another fragment {} on record", frag_loc, next_loc);
                        // if there is any record in the segment for the next fragment, we need to
                        // combine it with the fragment we are working on
                        let next_len: u32 = unsafe {*seg.cell_size(next_loc)};
                        debug!("Size of next fragment for {} is : {}", next_loc, next_len);
                        // remove the next fragment
                        frags.remove(&next_loc);
                        // reset size of the tombstone for the fragment we are working on
                        let new_frag_len = seg.cell_size(frag_loc);
                        let new_frag_len_val = frag_len + next_len;
                        unsafe {*new_frag_len = new_frag_len_val;}
                        debug!("Resize fragment {} from {} to {}", frag_loc, frag_len, new_frag_len_val);
                        // reset retry
                        retried = 0;
                        // and move to next iteration
                        continue;
                    } else {
                        // if not, we need to try again
                        retried += 1; continue;
                        debug!("Next fragment not in segment record: {}", next_loc);
                    }
                }
                // if not, it should be an alive cell, we need to release the segment lock to proceed
            }
            // first ensure there is a valid location in 'next_loc'
            if next_loc <= 0 || frag_loc <= 0 {
                error!("next_loc not assigned when trying to move a cell for cleaning");
                retried += 1; continue;
            }
            // we will fetch the cell by it's hash
            let cell_hash = unsafe {*seg.cell_hash(next_loc)};
            // 'cell_loc' will contain a lock to the cell if succeed
            let cell_loc = chunk.location_for_write(cell_hash);
            // TODO: check transaction versions
            // check if the hash exists in the chunk. If not, retry
            if cell_loc.is_none() {
                debug!("Cell hash not exists in the chunk: {}", cell_hash);
                retried += 1; continue;
            }
            let mut cell_loc = cell_loc.unwrap();
            //check if the location of the hash is exactly the as our 'next_loc'. If not, retry
            if *cell_loc != next_loc {
                debug!("Cell location changed: {}", cell_hash);
                retried += 1; continue;
            }
            // after everything all fine, we can lock the segment again with the cell locked first
            let seg_lock = seg.lock.write();
            let mut frags = seg.frags.lock();
            // because the cell is locked, it will always be there in the segment
            let cell_len = unsafe {*seg.cell_size(next_loc)} as usize;
            let frag_len = unsafe {*seg.cell_size(frag_loc)} as usize;
            debug!("Moving cell {} of size {} for fragment {} of size {}", next_loc, cell_len, frag_loc, frag_len);
            // There is only one cleaner for each segment at a time, the fragment will be there for
            // sure. Next we need to do is move the cell to the location of the fragment, update
            // cell index in chunk, put new fragment and tombstone next to the moved cell.
            unsafe {
                libc::memmove(
                    frag_loc as *mut libc::c_void,
                    *cell_loc as *mut libc::c_void,
                    cell_len
                );
            }
            let original_cell_loc = *cell_loc;
            *cell_loc = frag_loc;
            // remove the fragment
            debug!("Removing fragment {}", frag_loc);
            frags.remove(&frag_loc);
            let new_frag_loc = frag_loc + cell_len;
            let new_frag_len = original_cell_loc + cell_len - new_frag_loc;
            debug!("New fragment next to the moved cell {} is {}, size {}", next_loc, new_frag_loc, new_frag_len);
            // insert new fragment next to the moved cell
            frags.insert(new_frag_loc);
            // put tombstone
            seg.put_cell_tombstone(new_frag_loc);
            // write length to the tombstone;
            unsafe {*seg.cell_size(new_frag_loc) = new_frag_len as u32};
            retried = 0;
            defrag_pos = new_frag_loc;
        }
        debug!("Clean segment completed: {}", seg.addr);
    }
}