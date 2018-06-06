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
        // In this way locks can be straight forward, copy those entries to the new segment, change
        // cell indices by lock cells first, remove the old segment.
        // Segment locks will no long be required for transfer process will ensure there will be no
        // on going read operations to the old segment when the segment to be deleted.
        debug!("Cleaning segment: {}", seg.addr);

        // scan and mark dead cells and tombstones

    }
}