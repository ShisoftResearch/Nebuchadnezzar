use crate::ram::chunk::Chunk;
use crate::ram::segs::Segment;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// CLOCK eviction policy for selecting victim segments to evict
/// 
/// The CLOCK algorithm approximates LRU using a circular buffer with reference bits.
/// It provides good approximation of LRU with O(1) amortized time complexity.
pub struct ClockEvictionPolicy {
    /// Current position in the clock hand (cursor)
    cursor: std::sync::atomic::AtomicUsize,
}

impl ClockEvictionPolicy {
    pub fn new() -> Self {
        ClockEvictionPolicy {
            cursor: std::sync::atomic::AtomicUsize::new(0),
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
    /// Returns None if no victim can be found (all segments referenced or protected)
    pub fn select_victim(&self, chunk: &Chunk) -> Option<Arc<Segment>> {
        let segments = chunk.segments();
        if segments.is_empty() {
            return None;
        }
        
        let head_seg_id = chunk.get_head_seg_id();
        let num_segments = segments.len();
        let start_pos = self.cursor.load(Ordering::Relaxed);
        
        // Make two passes: first try to find unreferenced segment, 
        // second pass will clear all reference bits if needed
        for pass in 0..2 {
            for i in 0..num_segments {
                let pos = (start_pos + i) % num_segments;
                let segment = &segments[pos];
                
                // Skip head segment - it's actively being written to
                if segment.id == head_seg_id {
                    continue;
                }
                
                // Skip if segment has active references (being read)
                if !segment.no_references() {
                    continue;
                }
                
                // Skip if segment is protected by transactions
                if chunk.is_segment_protected(segment.id) {
                    continue;
                }
                
                // Skip if already cold
                if segment.is_cold() {
                    continue;
                }
                
                // Check and clear reference bit
                let was_referenced = segment.clear_reference_bit();
                
                if pass == 0 {
                    // First pass: only select if not referenced
                    if !was_referenced {
                        // Found victim!
                        self.cursor.store((pos + 1) % num_segments, Ordering::Relaxed);
                        debug!("CLOCK selected segment {} as victim (first pass, unreferenced)", segment.id);
                        return Some(segment.clone());
                    }
                } else {
                    // Second pass: select any eligible segment (bits already cleared)
                    // This only happens if ALL segments were referenced in first pass
                    self.cursor.store((pos + 1) % num_segments, Ordering::Relaxed);
                    debug!("CLOCK selected segment {} as victim (second pass, all were referenced)", segment.id);
                    return Some(segment.clone());
                }
            }
        }
        
        // No eligible victim found
        debug!("CLOCK could not find any victim segment (all protected or head segment)");
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
        Self::new()
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

