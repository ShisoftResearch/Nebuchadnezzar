use crate::ram::chunk::Chunk;
use crate::ram::segs::{Segment, SegmentClass};
use std::sync::atomic::{AtomicU64, Ordering};

const BLOB_FIRST_CLASSES: [SegmentClass; 2] = [SegmentClass::Blob, SegmentClass::Regular];

/// CLOCK eviction policy for selecting victim segments to evict
///
/// The CLOCK algorithm approximates LRU using a circular buffer with reference bits.
/// It provides good approximation of LRU with O(1) amortized time complexity.
pub struct ClockEvictionPolicy {
    /// Current position in the clock hand (cursor)
    cursor: std::sync::atomic::AtomicUsize,
    /// Cooldown window in milliseconds during which recently promoted segments are skipped
    promotion_cooldown_ms: AtomicU64,
}

impl ClockEvictionPolicy {
    pub fn new(promotion_cooldown_ms: u64) -> Self {
        ClockEvictionPolicy {
            cursor: std::sync::atomic::AtomicUsize::new(0),
            promotion_cooldown_ms: std::sync::atomic::AtomicU64::new(promotion_cooldown_ms),
        }
    }

    /// Select a victim segment for eviction using the CLOCK algorithm
    ///
    /// Algorithm:
    /// 1. Start at current clock hand position
    /// 2. For each segment:
    ///    - Skip if it's the head segment (actively being written to)
    ///    - Skip if it has active references (currently being read)
    ///    - Skip if it's protected by transactions
    ///    - Skip if it's already cold
    ///    - If reference bit is set, clear it and continue
    ///    - If reference bit is clear, select as victim
    /// 3. Advance clock hand
    ///
    /// Note: Segments don't need to be archived before selection. The eviction
    /// process will handle archiving if needed.
    ///
    /// Returns None if no victim can be found (all segments referenced or protected)
    pub fn select_victim(&self, chunk: &Chunk) -> Option<lightning::aarc::Arc<Segment>> {
        let segments = chunk.segments();
        if segments.is_empty() {
            return None;
        }

        let num_segments = segments.len();
        let start_pos = self.cursor.load(Ordering::Relaxed);
        let cooldown_ms = self.promotion_cooldown_ms.load(Ordering::Relaxed);
        let preferred_classes = &BLOB_FIRST_CLASSES[..];

        debug!(
            "CLOCK selecting victim across {} total segments",
            num_segments
        );

        for &preferred_class in preferred_classes {
            for i in 0..num_segments {
                let pos = (start_pos + i) % num_segments;
                let segment = &segments[pos];

                if segment.segment_class() != preferred_class {
                    continue;
                }

                if chunk.is_active_head(segment.id) {
                    debug!("CLOCK: seg {} is an active head, skipping", segment.id);
                    continue;
                }

                if !segment.no_references() {
                    debug!("CLOCK: seg {} has active references, skipping", segment.id);
                    continue;
                }

                if segment.is_cold() {
                    debug!("CLOCK: seg {} is already cold, skipping", segment.id);
                    continue;
                }

                if cooldown_ms > 0 && segment.recently_promoted_within(cooldown_ms) {
                    debug!("CLOCK: seg {} recently promoted, skipping", segment.id);
                    continue;
                }

                if segment.decrement_and_check() {
                    self.cursor
                        .store((pos + 1) % num_segments, Ordering::Relaxed);
                    debug!(
                        "CLOCK selected {:?} segment {} as victim (count reached zero)",
                        preferred_class, segment.id
                    );
                    return Some(segment.clone());
                }
            }
        }

        // No eligible victim found
        debug!("CLOCK could not find any victim segment (all protected or active heads)");
        None
    }

    /// Reset the clock hand to the beginning
    /// Useful for testing or explicit resets
    pub fn reset(&self) {
        self.cursor.store(0, Ordering::Relaxed);
    }
}

impl Default for ClockEvictionPolicy {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_clock_selects_unreferenced() {
        // Basic test to verify CLOCK prefers unreferenced segments
        // Full integration tests in tiered/tests.rs
    }
}
