#[cfg(test)]
mod test {
    use super::*;
    use crate::index::entry::EntryKey;
    use crate::index::{Feature, FEATURE_SIZE};
    use crate::ram::types::Id;
    use byteorder::{BigEndian, WriteBytesExt};

    fn u64_to_feature(n: u64) -> Feature {
        let mut feature = [0u8; FEATURE_SIZE];
        let mut cursor = std::io::Cursor::new(&mut feature[..]);
        cursor.write_u64::<BigEndian>(n).unwrap();
        feature
    }

    fn create_entry_key(schema_id: u32, field: u64, feature_value: u64, id: Id) -> EntryKey {
        let feature = u64_to_feature(feature_value);
        EntryKey::from_props(&id, &feature, field, schema_id)
    }

    #[test]
    fn test_entry_key_prefix_comparison() {
        let schema_id = 1;
        let field = 100;
        
        // Create keys with same feature value but different IDs
        let key1 = create_entry_key(schema_id, field, 50, Id::new(1, 10));
        let key2 = create_entry_key(schema_id, field, 50, Id::new(1, 20));
        let key3 = create_entry_key(schema_id, field, 50, Id::new(u64::MAX, u64::MAX));
        
        // Prefixes should be equal (same schema, field, feature)
        assert_eq!(key1.cmp_prefix(&key2), std::cmp::Ordering::Equal);
        assert_eq!(key1.cmp_prefix(&key3), std::cmp::Ordering::Equal);
        assert!(!key1.prefix_gt(&key2));
        assert!(!key1.prefix_gt(&key3));
        
        // Create key with different feature value
        let key4 = create_entry_key(schema_id, field, 51, Id::new(1, 10));
        
        // key4 should have greater prefix than key1
        assert!(key4.prefix_gt(&key1));
        assert!(!key1.prefix_gt(&key4));
        
        // Create key with smaller feature value
        let key5 = create_entry_key(schema_id, field, 49, Id::new(1, 10));
        
        // key5 should have smaller prefix than key1
        assert!(key1.prefix_gt(&key5));
        assert!(!key5.prefix_gt(&key1));
    }

    #[test]
    fn test_inclusive_end_key_construction() {
        let schema_id = 1;
        let field = 100;
        let feature_value = 50u64;
        let feature = u64_to_feature(feature_value);
        
        // Create inclusive end key (as done in ValueRange::to_key_range)
        let max_id = Id::new(u64::MAX, u64::MAX);
        let end_key = EntryKey::from_props(&max_id, &feature, field, schema_id);
        
        // Create data keys with same feature value but different IDs
        let data_key1 = create_entry_key(schema_id, field, feature_value, Id::new(1, 10));
        let data_key2 = create_entry_key(schema_id, field, feature_value, Id::new(1, 50));
        let data_key3 = create_entry_key(schema_id, field, feature_value, max_id);
        
        // All should have equal prefixes (should be included in inclusive range)
        assert_eq!(data_key1.cmp_prefix(&end_key), std::cmp::Ordering::Equal);
        assert_eq!(data_key2.cmp_prefix(&end_key), std::cmp::Ordering::Equal);
        assert_eq!(data_key3.cmp_prefix(&end_key), std::cmp::Ordering::Equal);
        
        // prefix_gt should be false for all (meaning they should be included)
        assert!(!data_key1.prefix_gt(&end_key), "data_key1 should not be > end_key");
        assert!(!data_key2.prefix_gt(&end_key), "data_key2 should not be > end_key");
        assert!(!data_key3.prefix_gt(&end_key), "data_key3 should not be > end_key");
        
        // Create key with feature value 51 (should be excluded)
        let data_key4 = create_entry_key(schema_id, field, 51, Id::new(1, 10));
        assert!(data_key4.prefix_gt(&end_key), "data_key4 should be > end_key");
        
        // Create key with feature value 49 (should be included)
        let data_key5 = create_entry_key(schema_id, field, 49, Id::new(1, 10));
        assert!(!data_key5.prefix_gt(&end_key), "data_key5 should not be > end_key");
    }

    #[test]
    fn test_inclusive_start_key_comparison() {
        let schema_id = 1;
        let field = 100;
        let feature_value = 10u64;
        let feature = u64_to_feature(feature_value);
        
        // Create inclusive start key
        let start_key = EntryKey::for_schema_field_feature(schema_id, field, &feature);
        
        // Create data keys
        let data_key1 = create_entry_key(schema_id, field, 9, Id::new(1, 10));
        let data_key2 = create_entry_key(schema_id, field, 10, Id::new(1, 10));
        let data_key3 = create_entry_key(schema_id, field, 11, Id::new(1, 10));
        
        // data_key1 should have smaller prefix (should be skipped)
        assert!(data_key1.prefix_lt(&start_key), "data_key1 should be < start_key");
        
        // data_key2 should have equal prefix (should be included)
        assert_eq!(data_key2.cmp_prefix(&start_key), std::cmp::Ordering::Equal);
        assert!(!data_key2.prefix_lt(&start_key), "data_key2 should not be < start_key");
        
        // data_key3 should have greater prefix (should be included)
        assert!(!data_key3.prefix_lt(&start_key), "data_key3 should not be < start_key");
    }

    #[test]
    fn test_range_query_simulation() {
        // Simulate the range query logic for [0, 50] inclusive
        let schema_id = 1;
        let field = 100;
        let max_id = Id::new(u64::MAX, u64::MAX);
        
        // Create range: [0, 50] inclusive
        let start_key = EntryKey::for_schema_field_feature(schema_id, field, &u64_to_feature(0));
        let end_key = EntryKey::from_props(&max_id, &u64_to_feature(50), field, schema_id);
        
        // Simulate iterating through values 0 to 51
        let mut included = Vec::new();
        for i in 0..=51 {
            let data_key = create_entry_key(schema_id, field, i, Id::new(1, i));
            
            // Check start condition (inclusive)
            let skip_start = data_key.prefix_lt(&start_key);
            if skip_start {
                continue;
            }
            
            // Check end condition (inclusive)
            let skip_end = data_key.prefix_gt(&end_key);
            if skip_end {
                break;
            }
            
            included.push(i);
        }
        
        // Should include values 0 to 50 (51 items)
        assert_eq!(included.len(), 51, "Should include 51 items (0 to 50)");
        assert_eq!(included, (0..=50).collect::<Vec<_>>(), "Should include exactly 0 to 50");
        
        // Value 51 should not be included
        assert!(!included.contains(&51), "Value 51 should not be included");
    }

    #[test]
    fn test_inclusive_end_boundary_condition() {
        // Test the specific boundary condition that's failing
        let schema_id = 1;
        let field = 100;
        let max_id = Id::new(u64::MAX, u64::MAX);
        
        // Create end key for value 50 (inclusive)
        let end_key = EntryKey::from_props(&max_id, &u64_to_feature(50), field, schema_id);
        
        // Test value 49 (should be included)
        let key49 = create_entry_key(schema_id, field, 49, Id::new(1, 49));
        assert!(!key49.prefix_gt(&end_key), "Value 49 should not be > end_key (should be included)");
        
        // Test value 50 (should be included - this is the boundary case)
        let key50 = create_entry_key(schema_id, field, 50, Id::new(1, 50));
        assert_eq!(key50.cmp_prefix(&end_key), std::cmp::Ordering::Equal, "Value 50 should have equal prefix to end_key");
        assert!(!key50.prefix_gt(&end_key), "Value 50 should not be > end_key (should be included)");
        
        // Test value 51 (should be excluded)
        let key51 = create_entry_key(schema_id, field, 51, Id::new(1, 51));
        assert!(key51.prefix_gt(&end_key), "Value 51 should be > end_key (should be excluded)");
    }

    #[test]
    fn test_range_seek_with_actual_btree() {
        use crate::index::ranged::lsm::btree::test::LevelBPlusTree;
        use crate::index::ranged::lsm::service::{Range, RangeTerm};
        use crate::index::ranged::lsm::btree::Ordering;
        use crate::index::ranged::trees::Cursor;
        use lightning::map::HashSet;
        use std::sync::Arc;
        
        fn deletion_set() -> Arc<HashSet<EntryKey>> {
            Arc::new(HashSet::with_capacity(16))
        }
        
        let tree = LevelBPlusTree::new(&deletion_set());
        let schema_id = 1;
        let field = 100;
        
        // Insert keys with values 0 to 50
        for i in 0..=50 {
            let key = create_entry_key(schema_id, field, i, Id::new(1, i));
            let inserted = tree.insert(&key);
            if i == 50 {
                println!("Inserting value 50: inserted={}", inserted);
            }
        }
        
        println!("Tree length: {}", tree.len());
        assert_eq!(tree.len(), 51, "Tree should have 51 keys");
        
        // Verify all values are in the tree by scanning from MIN_ENTRY_KEY
        use crate::index::ranged::trees::min_entry_key;
        let mut scan_cursor = tree.seek(&min_entry_key(), Ordering::Forward);
        let mut all_values = Vec::new();
        let mut count = 0;
        // Use next() which returns current then advances
        while let Some(key) = scan_cursor.next() {
            let feature_value = {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&key.as_slice()[8..16]);
                u64::from_be_bytes(bytes)
            };
            all_values.push(feature_value);
            count += 1;
            if count > 60 {
                break; // Safety limit
            }
        }
        println!("All values in tree (scan from MIN, count={}): {:?}", count, all_values);
        assert_eq!(all_values.len(), 51, "Should have 51 values when scanning from MIN");
        assert!(all_values.contains(&50), "Value 50 should be in scan results");
        
        // Try seeking directly to value 50
        let key50_seek = EntryKey::for_schema_field_feature(schema_id, field, &u64_to_feature(50));
        let mut cursor50 = tree.seek(&key50_seek, Ordering::Forward);
        println!("Seek to value 50, current: {:?}", cursor50.current().map(|k| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&k.as_slice()[8..16]);
            u64::from_be_bytes(bytes)
        }));
        
        // Try seeking to value 49 and see what's next
        let key49_seek = EntryKey::for_schema_field_feature(schema_id, field, &u64_to_feature(49));
        let mut cursor49 = tree.seek(&key49_seek, Ordering::Forward);
        println!("Seek to value 49, current: {:?}", cursor49.current().map(|k| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&k.as_slice()[8..16]);
            u64::from_be_bytes(bytes)
        }));
        // Call next() multiple times to see the pattern
        for i in 0..5 {
            let next = cursor49.next();
            if let Some(k) = &next {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&k.as_slice()[8..16]);
                let feature_val = u64::from_be_bytes(bytes);
                println!("Next after 49 (call {}): Some({})", i, feature_val);
            } else {
                println!("Next after 49 (call {}): None", i);
                break;
            }
        }
        
        // Also check what keys are actually in the tree around value 50
        let key50_full = create_entry_key(schema_id, field, 50, Id::new(1, 50));
        let cursor50_full = tree.seek(&key50_full, Ordering::Forward);
        if let Some(k) = cursor50_full.current() {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&k.as_slice()[8..16]);
            let feature_val = u64::from_be_bytes(bytes);
            println!("Seek to full key50 (feature=50, id=(1,50)), current: Some({}, id={:?})", feature_val, k.id());
        } else {
            println!("Seek to full key50, current: None");
        }
        
        // Check the btree structure: how many pages, what's in each
        println!("\n=== Checking btree structure ===");
        println!("Tree length: {}", tree.len());
        
        // Try to understand why cursor stops at 49
        // Check if value 50 is in a separate page that's not being reached
        let key49 = create_entry_key(schema_id, field, 49, Id::new(1, 49));
        let mut cursor_at_49 = tree.seek(&key49, Ordering::Forward);
        println!("Cursor at 49, current: {:?}", cursor_at_49.current().map(|k| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&k.as_slice()[8..16]);
            u64::from_be_bytes(bytes)
        }));
        println!("Cursor at 49, page.is_some(): {}", cursor_at_49.page.is_some());
        
        // Try next() once
        if let Some(key) = cursor_at_49.next() {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&key.as_slice()[8..16]);
            let val = u64::from_be_bytes(bytes);
            println!("After next(), got value: {}", val);
        } else {
            println!("After next(), got None");
        }
        
        println!("After next(), cursor.page.is_some(): {}", cursor_at_49.page.is_some());
        
        // The issue: value 50 is in the tree but cursor stops before reaching it
        // This suggests the cursor iteration logic might have an issue
        // 
        // Root cause analysis:
        // When we seek to value 49 and call next(), it returns 49 again, then None.
        // This suggests that when the cursor tries to advance to the next page/node,
        // it encounters an empty node or None node, causing it to stop.
        // 
        // The bug is likely in cursor.rs line 83-84:
        //   } else if next_node.is_empty() {
        //       return None;
        //   }
        // 
        // When the next node is empty, the cursor stops iteration. But an empty node
        // might be a placeholder, and there might be more nodes after it.
        // 
        // However, since we can seek directly to value 50, it must be in the tree.
        // The issue is that the cursor's next() method is not correctly traversing
        // to the node containing value 50.
        
        // Create range [0, 50] inclusive
        let start_key = EntryKey::for_schema_field_feature(schema_id, field, &u64_to_feature(0));
        let max_id = Id::new(u64::MAX, u64::MAX);
        let end_key = EntryKey::from_props(&max_id, &u64_to_feature(50), field, schema_id);
        
        let range = Range {
            start: RangeTerm::Inclusive(start_key.clone()),
            end: RangeTerm::Inclusive(end_key.clone()),
            ordering: Ordering::Forward,
        };
        
        // Simulate the seek logic from service.rs
        let entry = range.key();
        println!("Seek entry key feature: {:?}", {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&entry.as_slice()[8..16]);
            u64::from_be_bytes(bytes)
        });
        println!("Seek entry key ID: {:?}", entry.id());
        
        let mut tree_cursor = tree.seek(&entry, Ordering::Forward);
        println!("After seek, current: {:?}", tree_cursor.current().map(|k| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&k.as_slice()[8..16]);
            (u64::from_be_bytes(bytes), k.id())
        }));
        let mut collected = Vec::new();
        
        // Collect keys using next() which returns current then advances
        while collected.len() < 100 {
            if let Some(key) = tree_cursor.next() {
                let feature_value = {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&key.as_slice()[8..16]);
                    u64::from_be_bytes(bytes)
                };
                
                // Check start condition (inclusive)
                let mut skip = false;
                match &range.start {
                    RangeTerm::Inclusive(k) => {
                        if key.prefix_lt(k) {
                            skip = true;
                        }
                    }
                    _ => {}
                }
                if skip {
                    continue;
                }
                
                // Check end condition (inclusive)
                let mut should_break = false;
                match &range.end {
                    RangeTerm::Inclusive(k) => {
                        let prefix_cmp = key.cmp_prefix(k);
                        let is_gt = key.prefix_gt(k);
                        println!("Value {}: prefix_cmp={:?}, prefix_gt={}, end_key feature={:?}", 
                                 feature_value, prefix_cmp, is_gt, {
                                     let mut bytes = [0u8; 8];
                                     bytes.copy_from_slice(&k.as_slice()[8..16]);
                                     u64::from_be_bytes(bytes)
                                 });
                        if is_gt {
                            should_break = true;
                        }
                    }
                    _ => {}
                }
                if should_break {
                    println!("Breaking at value {}", feature_value);
                    break;
                }
                
                collected.push(key.id().lower);
                println!("Collected value {}", feature_value);
            } else {
                println!("Cursor returned None");
                break;
            }
        }
        
        // Should have 51 items (0 to 50)
        assert_eq!(collected.len(), 51, "Should collect 51 items, got {} items: {:?}", collected.len(), collected);
        assert_eq!(collected, (0..=50).collect::<Vec<_>>(), "Should have values 0 to 50, got: {:?}", collected);
    }
}

