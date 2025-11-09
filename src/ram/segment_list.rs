use crate::ram::segs::Segment;
use lightning::aarc::{Arc as AArc, AtomicArc};

/// A lock-free array-based segment list that provides O(1) access by segment ID.
/// Uses AtomicArc from Lightning to allow concurrent reads and updates without locks.
/// Similar to LinkedHashMap but optimized for dense, sequential segment IDs.
pub struct SegmentList {
    /// Array of AtomicArc to Segment
    /// Index corresponds to segment ID
    segments: Vec<AtomicArc<Segment>>,
}

impl SegmentList {
    /// Create a new SegmentList with pre-allocated capacity
    pub fn new(capacity: usize) -> Self {
        let mut segments = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            segments.push(AtomicArc::null());
        }
        SegmentList { segments }
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
            Some(old)
        }
    }

    /// Check if a segment exists at the given key
    pub fn contains_key(&self, key: &usize) -> bool {
        if *key >= self.segments.len() {
            return false;
        }
        !self.segments[*key].is_null()
    }

    /// Get the capacity of the segment list
    pub fn capacity(&self) -> usize {
        self.segments.len()
    }

    /// Iterate over all segment IDs that have active segments
    /// Returns an iterator over keys (segment IDs)
    pub fn iter_keys(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.segments.len()).filter(move |i| !self.segments[*i].is_null())
    }

    /// Iterate over all active segments
    /// Returns an iterator over Arc<Segment>
    pub fn iter_values(&self) -> impl Iterator<Item = AArc<Segment>> + '_ {
        (0..self.segments.len()).filter_map(move |i| {
            let arc = self.segments[i].load();
            if arc.is_null() {
                None
            } else {
                Some(arc)
            }
        })
    }

    /// Iterate over all (key, segment) pairs
    pub fn iter(&self) -> impl Iterator<Item = (usize, AArc<Segment>)> + '_ {
        (0..self.segments.len()).filter_map(move |i| {
            let arc = self.segments[i].load();
            if arc.is_null() {
                None
            } else {
                Some((i, arc))
            }
        })
    }

    /// Count the number of active segments
    pub fn len(&self) -> usize {
        self.iter_keys().count()
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
}
