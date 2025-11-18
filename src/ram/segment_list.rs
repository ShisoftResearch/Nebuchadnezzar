use crate::ram::segs::Segment;
use lightning::aarc::{Arc as AArc, AtomicArc};
use std::sync::atomic::{AtomicU64, Ordering};

/// A lock-free array-based segment list that provides O(1) access by segment ID.
/// Uses AtomicArc from Lightning to allow concurrent reads and updates without locks.
/// Similar to LinkedHashMap but optimized for dense, sequential segment IDs.
/// 
/// A bitmap is maintained for fast iteration and counting:
/// - Iteration: O(capacity/64) instead of O(capacity)
/// - Count: O(capacity/64) using popcnt instead of full scan
pub struct SegmentList {
    /// Array of AtomicArc to Segment
    /// Index corresponds to segment ID
    segments: Vec<AtomicArc<Segment>>,
    /// Bitmap tracking which segments are occupied
    /// Each AtomicU64 covers 64 segments
    /// Bit i in bitmap[j] indicates if segment (j*64 + i) is occupied
    bitmap: Vec<AtomicU64>,
}

impl SegmentList {
    /// Create a new SegmentList with pre-allocated capacity
    pub fn new(capacity: usize) -> Self {
        let mut segments = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            segments.push(AtomicArc::null());
        }
        
        // Create bitmap: one u64 per 64 segments
        // Formula: (capacity + 63) / 64 ensures we have enough words
        // For capacity=64: (64+63)/64 = 1 word (covers 0..63)
        // For capacity=65: (65+63)/64 = 2 words (covers 0..127, safe)
        let bitmap_size = if capacity == 0 {
            0
        } else {
            (capacity + 63) / 64
        };
        let mut bitmap = Vec::with_capacity(bitmap_size);
        for _ in 0..bitmap_size {
            bitmap.push(AtomicU64::new(0));
        }
        
        // Verify bitmap size is sufficient for all valid indices
        if capacity > 0 {
            let max_index = capacity - 1;
            let required_word_index = max_index >> 6;
            debug_assert!(
                required_word_index < bitmap_size,
                "Bitmap size {} insufficient for capacity {} (max_index={}, required_word_index={})",
                bitmap_size, capacity, max_index, required_word_index
            );
        }
        
        SegmentList { segments, bitmap }
    }
    
    /// Set a bit in the bitmap
    #[inline]
    fn set_bit(&self, index: usize) {
        let word_index = index >> 6; // Divide by 64
        let bit_index = index & 63;  // Modulo 64
        // Runtime bounds check to prevent heap corruption
        #[cfg(debug_assertions)]
        if word_index >= self.bitmap.len() {
            panic!(
                "Bitmap word_index {} out of bounds (bitmap len: {}, index: {}, capacity: {})",
                word_index, self.bitmap.len(), index, self.segments.len()
            );
        }
        let mask = 1u64 << bit_index;
        self.bitmap[word_index].fetch_or(mask, Ordering::Release);
    }
    
    /// Clear a bit in the bitmap
    #[inline]
    fn clear_bit(&self, index: usize) {
        let word_index = index >> 6; // Divide by 64
        let bit_index = index & 63;  // Modulo 64
        // Runtime bounds check to prevent heap corruption
        #[cfg(debug_assertions)]
        if word_index >= self.bitmap.len() {
            panic!(
                "Bitmap word_index {} out of bounds (bitmap len: {}, index: {}, capacity: {})",
                word_index, self.bitmap.len(), index, self.segments.len()
            );
        }
        let mask = !(1u64 << bit_index);
        self.bitmap[word_index].fetch_and(mask, Ordering::Release);
    }
    
    /// Check if a bit is set in the bitmap
    #[inline]
    fn is_bit_set(&self, index: usize) -> bool {
        let word_index = index >> 6; // Divide by 64
        let bit_index = index & 63;  // Modulo 64
        if word_index >= self.bitmap.len() {
            return false;
        }
        let word = self.bitmap[word_index].load(Ordering::Acquire);
        (word & (1u64 << bit_index)) != 0
    }

    /// Insert or update a segment at the given key (segment ID)
    /// Returns the old segment if one existed
    pub fn insert(&self, key: usize, segment: AArc<Segment>) -> Option<AArc<Segment>> {
        if key >= self.segments.len() {
            panic!(
                "Segment key {} exceeds capacity {}",
                key,
                self.segments.len()
            );
        }

        let old = self.segments[key].swap_ref(segment);
        
        // Update bitmap: set the bit for this segment
        self.set_bit(key);
        
        if old.is_null() {
            None
        } else {
            Some(old)
        }
    }

    /// Get a segment by key (segment ID)
    /// Returns a cloned Arc if the segment exists
    pub fn get(&self, key: &usize) -> Option<AArc<Segment>> {
        if *key >= self.segments.len() {
            return None;
        }

        let arc = self.segments[*key].load();
        if arc.is_null() {
            None
        } else {
            Some(arc)
        }
    }

    /// Remove a segment by key (segment ID)
    /// Returns the removed segment if it existed
    pub fn remove(&self, key: &usize) -> Option<AArc<Segment>> {
        if *key >= self.segments.len() {
            return None;
        }

        let old = self.segments[*key].swap_ref(AArc::null());
        
        if old.is_null() {
            None
        } else {
            // Update bitmap: clear the bit for this segment
            self.clear_bit(*key);
            Some(old)
        }
    }

    /// Check if a segment exists at the given key
    pub fn contains_key(&self, key: &usize) -> bool {
        if *key >= self.segments.len() {
            return false;
        }
        // Fast path: check bitmap first, but verify with actual segment
        // This handles race conditions where bitmap might be out of sync
        if !self.is_bit_set(*key) {
            return false;
        }
        // Double-check by actually loading the segment to handle race conditions
        !self.segments[*key].is_null()
    }

    /// Get the capacity of the segment list
    pub fn capacity(&self) -> usize {
        self.segments.len()
    }

    /// Iterate over all segment IDs that have active segments
    /// Returns an iterator over keys (segment IDs)
    /// Uses bitmap for efficient iteration (O(capacity/64) instead of O(capacity))
    pub fn iter_keys(&self) -> impl Iterator<Item = usize> + '_ {
        BitmapIterator {
            segment_list: self,
            word_index: 0,
            current_word: if !self.bitmap.is_empty() { 
                self.bitmap[0].load(Ordering::Acquire) 
            } else { 
                0 
            },
        }
    }

    /// Iterate over all active segments
    /// Returns an iterator over Arc<Segment>
    /// Uses bitmap for efficient iteration
    pub fn iter_values(&self) -> impl Iterator<Item = AArc<Segment>> + '_ {
        self.iter_keys().filter_map(move |i| {
            let arc = self.segments[i].load();
            if arc.is_null() {
                None
            } else {
                Some(arc)
            }
        })
    }

    /// Iterate over all (key, segment) pairs
    /// Uses bitmap for efficient iteration
    pub fn iter(&self) -> impl Iterator<Item = (usize, AArc<Segment>)> + '_ {
        self.iter_keys().filter_map(move |i| {
            let arc = self.segments[i].load();
            if arc.is_null() {
                None
            } else {
                Some((i, arc))
            }
        })
    }

    /// Count the number of active segments
    /// Uses bitmap with population count for O(capacity/64) performance
    pub fn len(&self) -> usize {
        self.bitmap
            .iter()
            .map(|word| word.load(Ordering::Acquire).count_ones() as usize)
            .sum()
    }

    /// Check if the list is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Alias for iter_keys() to match LinkedHashMap API
    pub fn iter_front_keys(&self) -> impl Iterator<Item = usize> + '_ {
        self.iter_keys()
    }

    /// Alias for iter_values() to match LinkedHashMap API
    pub fn iter_front_values(&self) -> impl Iterator<Item = AArc<Segment>> + '_ {
        self.iter_values()
    }

    /// Alias for insert() to match LinkedHashMap API
    pub fn insert_back(&self, key: usize, segment: AArc<Segment>) -> Option<AArc<Segment>> {
        self.insert(key, segment)
    }
}

/// Iterator that efficiently traverses the bitmap to find occupied segments
struct BitmapIterator<'a> {
    segment_list: &'a SegmentList,
    word_index: usize,
    current_word: u64,
}

impl<'a> Iterator for BitmapIterator<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Process current word
            if self.current_word != 0 {
                // Find the position of the least significant set bit
                let bit_index = self.current_word.trailing_zeros() as usize;
                // Clear that bit
                self.current_word &= self.current_word - 1;
                // Calculate the segment ID
                let segment_id = self.word_index * 64 + bit_index;
                // Make sure we don't exceed capacity
                if segment_id < self.segment_list.segments.len() {
                    return Some(segment_id);
                }
            }
            
            // Move to next word
            self.word_index += 1;
            if self.word_index >= self.segment_list.bitmap.len() {
                return None;
            }
            
            // Load next word
            self.current_word = self.segment_list.bitmap[self.word_index].load(Ordering::Acquire);
        }
    }
}

// SegmentList is automatically Send and Sync because AArc is Send + Sync

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::file_manager::SegmentFileManager;
    use crate::ram::segs::Segment;
    use std::sync::Arc;

    fn new_test_file_manager() -> Arc<SegmentFileManager> {
        Arc::new(SegmentFileManager::new(None, None))
    }

    #[test]
    fn test_segment_list_basic() {
        let list = SegmentList::new(10);

        // Create a dummy segment
        let segment = AArc::new(Segment::new(0, 0, 0, 0x1000, true, new_test_file_manager()));

        // Insert
        assert!(list.insert(0, segment.clone()).is_none());

        // Get
        assert!(list.get(&0).is_some());
        assert_eq!(list.get(&0).unwrap().id, 0);

        // Contains
        assert!(list.contains_key(&0));
        assert!(!list.contains_key(&1));

        // Remove
        assert!(list.remove(&0).is_some());
        assert!(!list.contains_key(&0));
    }

    #[test]
    fn test_segment_list_iteration() {
        let list = SegmentList::new(10);
        let file_manager = new_test_file_manager();

        // Insert a few segments
        for i in 0..5 {
            let segment = AArc::new(Segment::new(
                i as u64,
                0,
                0,
                0x1000 * i,
                true,
                Arc::clone(&file_manager),
            ));
            list.insert(i, segment);
        }

        // Check length
        assert_eq!(list.len(), 5);

        // Check iteration
        let keys: Vec<_> = list.iter_keys().collect();
        assert_eq!(keys, vec![0, 1, 2, 3, 4]);

        // Check values iteration
        let values: Vec<_> = list.iter_values().collect();
        assert_eq!(values.len(), 5);
        for (i, seg) in values.iter().enumerate() {
            assert_eq!(seg.id, i as u64);
        }
    }

    #[test]
    fn test_segment_list_replace() {
        let list = SegmentList::new(10);
        let file_manager = new_test_file_manager();

        let seg1 = AArc::new(Segment::new(
            0,
            1,
            0,
            0x1000,
            true,
            Arc::clone(&file_manager),
        ));
        let seg2 = AArc::new(Segment::new(
            0,
            2,
            0,
            0x1000,
            true,
            Arc::clone(&file_manager),
        ));

        // Insert first segment
        assert!(list.insert(0, seg1.clone()).is_none());

        // Replace with second segment
        let old = list.insert(0, seg2.clone());
        assert!(old.is_some());
        assert_eq!(old.unwrap().seq_id, 1);

        // Verify new segment is in place
        assert_eq!(list.get(&0).unwrap().seq_id, 2);
    }

    #[test]
    fn test_segment_list_bitmap_operations() {
        let list = SegmentList::new(200); // Test across multiple bitmap words
        let file_manager = new_test_file_manager();

        // Insert segments at various positions
        let positions = vec![0, 5, 63, 64, 65, 127, 128, 150, 199];
        for &pos in &positions {
            let segment = AArc::new(Segment::new(
                pos as u64,
                0,
                0,
                0x1000 * pos,
                true,
                Arc::clone(&file_manager),
            ));
            list.insert(pos, segment);
        }

        // Verify count is correct
        assert_eq!(list.len(), positions.len());

        // Verify contains_key works
        for &pos in &positions {
            assert!(list.contains_key(&pos));
        }
        assert!(!list.contains_key(&1));
        assert!(!list.contains_key(&100));

        // Verify iteration returns correct keys
        let keys: Vec<_> = list.iter_keys().collect();
        assert_eq!(keys, positions);

        // Remove some segments
        list.remove(&5);
        list.remove(&128);

        // Verify count updated
        assert_eq!(list.len(), positions.len() - 2);

        // Verify contains_key updated
        assert!(!list.contains_key(&5));
        assert!(!list.contains_key(&128));
        assert!(list.contains_key(&63));
        assert!(list.contains_key(&127));

        // Verify iteration skips removed segments
        let remaining_keys: Vec<_> = list.iter_keys().collect();
        assert_eq!(remaining_keys, vec![0, 63, 64, 65, 127, 150, 199]);
    }

    #[test]
    fn test_segment_list_sparse_iteration() {
        // Test with a large capacity but few segments
        // This demonstrates the performance benefit of bitmap iteration
        let list = SegmentList::new(10000);
        let file_manager = new_test_file_manager();

        // Insert only 10 segments in a sparse array
        for i in (0..10).map(|x| x * 1000) {
            let segment = AArc::new(Segment::new(
                i as u64,
                0,
                0,
                0x1000 * i,
                true,
                Arc::clone(&file_manager),
            ));
            list.insert(i, segment);
        }

        // Count should be fast (O(capacity/64) = ~156 iterations instead of 10000)
        assert_eq!(list.len(), 10);

        // Iteration should be fast (only visit occupied segments)
        let keys: Vec<_> = list.iter_keys().collect();
        assert_eq!(keys.len(), 10);
        assert_eq!(keys[0], 0);
        assert_eq!(keys[9], 9000);
    }
}
