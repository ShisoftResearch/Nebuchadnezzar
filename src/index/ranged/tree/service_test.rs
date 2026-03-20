#[cfg(test)]
mod test {
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
        assert!(
            !data_key1.prefix_gt(&end_key),
            "data_key1 should not be > end_key"
        );
        assert!(
            !data_key2.prefix_gt(&end_key),
            "data_key2 should not be > end_key"
        );
        assert!(
            !data_key3.prefix_gt(&end_key),
            "data_key3 should not be > end_key"
        );

        // Create key with feature value 51 (should be excluded)
        let data_key4 = create_entry_key(schema_id, field, 51, Id::new(1, 10));
        assert!(
            data_key4.prefix_gt(&end_key),
            "data_key4 should be > end_key"
        );

        // Create key with feature value 49 (should be included)
        let data_key5 = create_entry_key(schema_id, field, 49, Id::new(1, 10));
        assert!(
            !data_key5.prefix_gt(&end_key),
            "data_key5 should not be > end_key"
        );
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
        assert!(
            data_key1.prefix_lt(&start_key),
            "data_key1 should be < start_key"
        );

        // data_key2 should have equal prefix (should be included)
        assert_eq!(data_key2.cmp_prefix(&start_key), std::cmp::Ordering::Equal);
        assert!(
            !data_key2.prefix_lt(&start_key),
            "data_key2 should not be < start_key"
        );

        // data_key3 should have greater prefix (should be included)
        assert!(
            !data_key3.prefix_lt(&start_key),
            "data_key3 should not be < start_key"
        );
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
        assert_eq!(
            included,
            (0..=50).collect::<Vec<_>>(),
            "Should include exactly 0 to 50"
        );

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
        assert!(
            !key49.prefix_gt(&end_key),
            "Value 49 should not be > end_key (should be included)"
        );

        // Test value 50 (should be included - this is the boundary case)
        let key50 = create_entry_key(schema_id, field, 50, Id::new(1, 50));
        assert_eq!(
            key50.cmp_prefix(&end_key),
            std::cmp::Ordering::Equal,
            "Value 50 should have equal prefix to end_key"
        );
        assert!(
            !key50.prefix_gt(&end_key),
            "Value 50 should not be > end_key (should be included)"
        );

        // Test value 51 (should be excluded)
        let key51 = create_entry_key(schema_id, field, 51, Id::new(1, 51));
        assert!(
            key51.prefix_gt(&end_key),
            "Value 51 should be > end_key (should be excluded)"
        );
    }

    #[test]
    fn test_range_seek_with_actual_btree() {
        use crate::index::ranged::tree::btree::test::LevelBPlusTree;
        use crate::index::ranged::tree::btree::Ordering;
        use crate::index::ranged::tree::service::{Range, RangeTerm};
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
        println!(
            "All values in tree (scan from MIN, count={}): {:?}",
            count, all_values
        );
        assert_eq!(
            all_values.len(),
            51,
            "Should have 51 values when scanning from MIN"
        );
        assert!(
            all_values.contains(&50),
            "Value 50 should be in scan results"
        );

        // Try seeking directly to value 50
        let key50_seek = EntryKey::for_schema_field_feature(schema_id, field, &u64_to_feature(50));
        let cursor50 = tree.seek(&key50_seek, Ordering::Forward);
        println!(
            "Seek to value 50, current: {:?}",
            cursor50.current().map(|k| {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&k.as_slice()[8..16]);
                u64::from_be_bytes(bytes)
            })
        );

        // Try seeking to value 49 and see what's next
        let key49_seek = EntryKey::for_schema_field_feature(schema_id, field, &u64_to_feature(49));
        let mut cursor49 = tree.seek(&key49_seek, Ordering::Forward);
        println!(
            "Seek to value 49, current: {:?}",
            cursor49.current().map(|k| {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&k.as_slice()[8..16]);
                u64::from_be_bytes(bytes)
            })
        );
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
            println!(
                "Seek to full key50 (feature=50, id=(1,50)), current: Some({}, id={:?})",
                feature_val,
                k.id()
            );
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
        println!(
            "Cursor at 49, current: {:?}",
            cursor_at_49.current().map(|k| {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&k.as_slice()[8..16]);
                u64::from_be_bytes(bytes)
            })
        );
        println!(
            "Cursor at 49, page.is_some(): {}",
            cursor_at_49.page.is_some()
        );

        // Try next() once
        if let Some(key) = cursor_at_49.next() {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&key.as_slice()[8..16]);
            let val = u64::from_be_bytes(bytes);
            println!("After next(), got value: {}", val);
        } else {
            println!("After next(), got None");
        }

        println!(
            "After next(), cursor.page.is_some(): {}",
            cursor_at_49.page.is_some()
        );

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
        println!(
            "After seek, current: {:?}",
            tree_cursor.current().map(|k| {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&k.as_slice()[8..16]);
                (u64::from_be_bytes(bytes), k.id())
            })
        );
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
                        println!(
                            "Value {}: prefix_cmp={:?}, prefix_gt={}, end_key feature={:?}",
                            feature_value,
                            prefix_cmp,
                            is_gt,
                            {
                                let mut bytes = [0u8; 8];
                                bytes.copy_from_slice(&k.as_slice()[8..16]);
                                u64::from_be_bytes(bytes)
                            }
                        );
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
        assert_eq!(
            collected.len(),
            51,
            "Should collect 51 items, got {} items: {:?}",
            collected.len(),
            collected
        );
        assert_eq!(
            collected,
            (0..=50).collect::<Vec<_>>(),
            "Should have values 0 to 50, got: {:?}",
            collected
        );
    }

    /// Test that range queries work correctly after LSM tree recovery from persistent storage.
    ///
    /// Key requirements for recovery:
    /// 1. Create both page_schema and RANGED_TREE_SCHEMA
    /// 2. Start the external node writeback background task
    /// 3. Merge data from memory to disk trees
    /// 4. Update the LSM tree cell with new head IDs after merge
    /// 5. Wait for async persistence to complete
    #[tokio::test(flavor = "multi_thread")]
    async fn test_range_query_survives_recovery() {
        use crate::client;
        use crate::index::ranged::tree::btree::{page_schema, Ordering};
        use crate::index::ranged::tree::service::{Range, RangeTerm};
        use crate::index::ranged::tree::tree::{RangedTree, RANGED_TREE_SCHEMA};
        use crate::index::ranged::trees::Cursor;
        use crate::server::*;
        use std::sync::Arc;

        let _ = env_logger::try_init();
        let server_group = "lsm-range-recovery";
        let server_addr = String::from("127.0.0.1:5610");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await;
        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                server_group,
            )
            .await
            .unwrap(),
        );

        // Create the schemas required for LSM tree storage
        client
            .new_schema_with_id(page_schema())
            .await
            .unwrap()
            .unwrap();
        client
            .new_schema_with_id(RANGED_TREE_SCHEMA.clone())
            .await
            .unwrap()
            .unwrap();

        // CRITICAL: Start the background task that writes external nodes to storage
        use crate::index::ranged::tree::btree::storage;
        storage::start_external_nodes_write_back(&client);

        let lsm_tree_id = Id::new(999, 999);
        let schema_id = 1;
        let field = 200;

        // Create LSM tree and insert test data
        let tree = RangedTree::create(&client, &lsm_tree_id).await;

        // Insert keys with feature values 10..=100
        for i in 10..=100 {
            let key = create_entry_key(schema_id, field, i, Id::new(2, i));
            tree.insert(&key);
        }

        println!("=== Tree state after insertion ===");
        println!("Tree count: {}", tree.count());
        println!("Tree ideal_capacity: {}", tree.ideal_capacity());
        println!("Tree oversized: {}", tree.oversized());

        // Helper function to perform range query and collect results
        let collect_range = |tree: &RangedTree, start: u64, end: u64| -> Vec<u64> {
            let start_key =
                EntryKey::for_schema_field_feature(schema_id, field, &u64_to_feature(start));
            let max_id = Id::new(u64::MAX, u64::MAX);
            let end_key = EntryKey::from_props(&max_id, &u64_to_feature(end), field, schema_id);

            let range = Range {
                start: RangeTerm::Inclusive(start_key.clone()),
                end: RangeTerm::Inclusive(end_key.clone()),
                ordering: Ordering::Forward,
            };

            let entry = range.key();
            let mut tree_cursor = tree.seek(&entry, Ordering::Forward);
            let mut collected = Vec::new();

            while let Some(key) = tree_cursor.next() {
                let feature_value = {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&key.as_slice()[8..16]);
                    u64::from_be_bytes(bytes)
                };

                // Check start condition
                if key.prefix_lt(&start_key) {
                    continue;
                }

                // Check end condition
                if key.prefix_gt(&end_key) {
                    break;
                }

                collected.push(feature_value);
            }

            collected
        };

        // Test various range queries before recovery
        println!("=== Testing range queries BEFORE recovery ===");

        // Query 1: [10, 20] inclusive
        let results_1_before = collect_range(&tree, 10, 20);
        println!("Range [10, 20]: {} items", results_1_before.len());
        assert_eq!(
            results_1_before.len(),
            11,
            "Should have 11 items for range [10, 20]"
        );
        assert_eq!(results_1_before, (10..=20).collect::<Vec<_>>());

        // Query 2: [50, 60] inclusive
        let results_2_before = collect_range(&tree, 50, 60);
        println!("Range [50, 60]: {} items", results_2_before.len());
        assert_eq!(
            results_2_before.len(),
            11,
            "Should have 11 items for range [50, 60]"
        );
        assert_eq!(results_2_before, (50..=60).collect::<Vec<_>>());

        // Query 3: [90, 100] inclusive (boundary test)
        let results_3_before = collect_range(&tree, 90, 100);
        println!("Range [90, 100]: {} items", results_3_before.len());
        assert_eq!(
            results_3_before.len(),
            11,
            "Should have 11 items for range [90, 100]"
        );
        assert_eq!(results_3_before, (90..=100).collect::<Vec<_>>());

        // Query 4: Full range [10, 100]
        let results_4_before = collect_range(&tree, 10, 100);
        println!("Range [10, 100]: {} items", results_4_before.len());
        assert_eq!(
            results_4_before.len(),
            91,
            "Should have 91 items for range [10, 100]"
        );

        // Force merge to persist to disk
        println!("=== Forcing tree merge to persist data ===");
        let merged = tree.merge_levels().await;
        println!("Merge result: {}", merged);
        println!("Tree count after merge: {}", tree.count());

        // Give time for async writes to complete and merge multiple times to ensure all data is on disk
        for i in 0..5 {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            let merged_again = tree.merge_levels().await;
            println!(
                "Additional merge {} result: {}, count: {}",
                i,
                merged_again,
                tree.count()
            );
        }

        // Update the LSM tree cell with the current head IDs (critical for recovery!)
        println!("=== Updating LSM tree cell with new head IDs ===");
        println!("Tree head ID: {:?}", tree.head_id());
        tree.mark_migration(&lsm_tree_id, None, &client)
            .await
            .expect("Failed to mark migration in test");
        println!("LSM tree cell updated");

        // Wait for cell update to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify the cell was updated by reading it back
        use crate::index::ranged::tree::tree::RANGED_TREE_HEAD_HASH;
        let verify_cell = client.read_cell(lsm_tree_id).await.unwrap().unwrap();
        let stored_head_id = verify_cell.data[*RANGED_TREE_HEAD_HASH].id().unwrap();
        println!("Stored tree head ID in cell: {:?}", stored_head_id);

        // Drop the tree to simulate server restart
        drop(tree);

        println!("=== Recovering LSM tree from storage ===");

        // Recover the tree
        let recovered_tree = RangedTree::recover(&client, &lsm_tree_id).await;

        println!("=== Recovered tree state ===");
        println!("Recovered tree count: {}", recovered_tree.count());
        println!(
            "Recovered tree ideal_capacity: {}",
            recovered_tree.ideal_capacity()
        );
        println!("Recovered tree oversized: {}", recovered_tree.oversized());

        println!("=== Testing range queries AFTER recovery ===");

        // Repeat the same queries and verify results match
        let results_1_after = collect_range(&recovered_tree, 10, 20);
        println!(
            "Range [10, 20] after recovery: {} items",
            results_1_after.len()
        );
        assert_eq!(
            results_1_after, results_1_before,
            "Range [10, 20] results should match after recovery"
        );

        let results_2_after = collect_range(&recovered_tree, 50, 60);
        println!(
            "Range [50, 60] after recovery: {} items",
            results_2_after.len()
        );
        assert_eq!(
            results_2_after, results_2_before,
            "Range [50, 60] results should match after recovery"
        );

        let results_3_after = collect_range(&recovered_tree, 90, 100);
        println!(
            "Range [90, 100] after recovery: {} items",
            results_3_after.len()
        );
        assert_eq!(
            results_3_after, results_3_before,
            "Range [90, 100] results should match after recovery"
        );

        let results_4_after = collect_range(&recovered_tree, 10, 100);
        println!(
            "Range [10, 100] after recovery: {} items",
            results_4_after.len()
        );
        assert_eq!(
            results_4_after, results_4_before,
            "Full range [10, 100] results should match after recovery"
        );

        println!("=== All recovery tests passed! ===");
    }

    /// Test that backward range queries work correctly after LSM tree recovery.
    /// This validates that the B+tree bidirectional links are preserved through recovery.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_range_query_backward_survives_recovery() {
        use crate::client;
        use crate::index::ranged::tree::btree::{page_schema, Ordering};
        use crate::index::ranged::tree::service::{Range, RangeTerm};
        use crate::index::ranged::tree::tree::{RangedTree, RANGED_TREE_SCHEMA};
        use crate::index::ranged::trees::Cursor;
        use crate::server::*;
        use std::sync::Arc;

        let _ = env_logger::try_init();
        let server_group = "lsm-range-backward-recovery";
        let server_addr = String::from("127.0.0.1:5611");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await;
        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                server_group,
            )
            .await
            .unwrap(),
        );

        // Create the schemas required for LSM tree storage
        client
            .new_schema_with_id(page_schema())
            .await
            .unwrap()
            .unwrap();
        client
            .new_schema_with_id(RANGED_TREE_SCHEMA.clone())
            .await
            .unwrap()
            .unwrap();

        // CRITICAL: Start the background task that writes external nodes to storage
        use crate::index::ranged::tree::btree::storage;
        storage::start_external_nodes_write_back(&client);

        let lsm_tree_id = Id::new(888, 888);
        let schema_id = 1;
        let field = 300;

        // Create LSM tree and insert test data
        let tree = RangedTree::create(&client, &lsm_tree_id).await;

        // Insert keys with feature values 10..=100 (91 items to ensure oversized mem tree)
        for i in 10..=100 {
            let key = create_entry_key(schema_id, field, i, Id::new(3, i));
            tree.insert(&key);
        }

        println!("=== Tree state after insertion ===");
        println!("Tree count: {}", tree.count());
        println!("Tree ideal_capacity: {}", tree.ideal_capacity());

        // Helper function to perform backward range query
        let collect_range_backward = |tree: &RangedTree, start: u64, end: u64| -> Vec<u64> {
            let start_key =
                EntryKey::for_schema_field_feature(schema_id, field, &u64_to_feature(start));
            let max_id = Id::new(u64::MAX, u64::MAX);
            let end_key = EntryKey::from_props(&max_id, &u64_to_feature(end), field, schema_id);

            let range = Range {
                start: RangeTerm::Inclusive(start_key.clone()),
                end: RangeTerm::Inclusive(end_key.clone()),
                ordering: Ordering::Backward,
            };

            let entry = range.key();
            let mut tree_cursor = tree.seek(&entry, Ordering::Backward);
            let mut collected = Vec::new();

            while let Some(key) = tree_cursor.next() {
                let feature_value = {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&key.as_slice()[8..16]);
                    u64::from_be_bytes(bytes)
                };

                // Check end condition (for backward, check end first)
                if key.prefix_gt(&end_key) {
                    continue;
                }

                // Check start condition
                if key.prefix_lt(&start_key) {
                    break;
                }

                collected.push(feature_value);
            }

            collected
        };

        println!("=== Testing BACKWARD range queries BEFORE recovery ===");

        // Query 1: [30, 40] backward
        let results_1_before = collect_range_backward(&tree, 30, 40);
        println!("Backward range [30, 40]: {} items", results_1_before.len());
        assert_eq!(results_1_before.len(), 11, "Should have 11 items");
        // Backward should return in descending order
        let expected: Vec<u64> = (30..=40).rev().collect();
        assert_eq!(results_1_before, expected);

        // Query 2: [70, 80] backward (boundary test)
        let results_2_before = collect_range_backward(&tree, 70, 80);
        println!("Backward range [70, 80]: {} items", results_2_before.len());
        assert_eq!(results_2_before.len(), 11, "Should have 11 items");
        let expected: Vec<u64> = (70..=80).rev().collect();
        assert_eq!(results_2_before, expected);

        println!("=== Forcing tree merge and recovering ===");
        tree.merge_levels().await;
        println!("Tree count after first merge: {}", tree.count());

        // Give time for async writes to complete and merge multiple times to ensure all data is on disk
        for _ in 0..5 {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            tree.merge_levels().await;
        }
        println!("Tree count after additional merges: {}", tree.count());

        // Update the LSM tree cell with the current head IDs (critical for recovery!)
        tree.mark_migration(&lsm_tree_id, None, &client)
            .await
            .expect("Failed to mark migration in test");

        // Wait for writeback to complete - longer wait for test isolation
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        drop(tree);

        println!("=== Recovering tree ===");
        let recovered_tree = RangedTree::recover(&client, &lsm_tree_id).await;
        println!("Recovered tree count: {}", recovered_tree.count());

        println!("=== Testing BACKWARD range queries AFTER recovery ===");

        let results_1_after = collect_range_backward(&recovered_tree, 30, 40);
        println!(
            "Backward range [30, 40] after recovery: {} items",
            results_1_after.len()
        );
        assert_eq!(
            results_1_after, results_1_before,
            "Backward range results should match after recovery"
        );

        let results_2_after = collect_range_backward(&recovered_tree, 70, 80);
        println!(
            "Backward range [70, 80] after recovery: {} items",
            results_2_after.len()
        );
        assert_eq!(
            results_2_after, results_2_before,
            "Backward range results should match after recovery"
        );

        println!("=== Backward range recovery tests passed! ===");
    }

    /// End-to-end test: Range index survives recovery using schema-level API
    ///
    /// Tests the complete flow:
    /// 1. Define schema with ranged index
    /// 2. Insert cells with indexed values
    /// 3. Query using range_index_scan
    /// 4. Simulate server restart by dropping and recreating server
    /// 5. Query again and verify results match
    #[tokio::test(flavor = "multi_thread")]
    async fn test_e2e_range_index_recovery_with_schema() {
        use crate::index::ranged::tree::btree::Ordering;
        use crate::query::data_client::{QueryOrdering, ValueRange, ValueRangeTerm};
        use crate::ram::cell::OwnedCell;
        use crate::ram::schema::{Field, IndexType, Schema};
        use crate::ram::types::Type;
        use crate::server::*;
        use bifrost_hasher::hash_str;
        use dovahkiin::{expr::serde::Expr, types::*};

        let _ = env_logger::try_init();
        let server_group = "e2e-range-recovery";
        let server_addr = String::from("127.0.0.1:5620");

        const PRICE_FIELD: &'static str = "price";
        const NAME_FIELD: &'static str = "name";
        const QUANTITY_FIELD: &'static str = "quantity";

        // Create temporary directories for persistent storage
        let test_dir = std::env::temp_dir().join("neb_e2e_range_recovery_test");
        let backup_dir = test_dir.join("backup");
        let wal_dir = test_dir.join("wal");
        let undo_dir = test_dir.join("undo");
        let raft_dir = test_dir.join("raft");

        // Clean up any existing test data
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&backup_dir).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&undo_dir).unwrap();
        std::fs::create_dir_all(&raft_dir).unwrap();

        println!("=== Using storage directories ===");
        println!("Backup: {:?}", backup_dir);
        println!("WAL: {:?}", wal_dir);
        println!("Undo: {:?}", undo_dir);
        println!("Raft: {:?}", raft_dir);

        // Create initial server with ranged indexer and persistent storage
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: Some(backup_dir.to_str().unwrap().to_string()),
                wal_storage: Some(wal_dir.to_str().unwrap().to_string()),
                undo_log_storage: Some(undo_dir.to_str().unwrap().to_string()),
                raft_storage: Some(raft_dir.to_str().unwrap().to_string()),
                index_enabled: true, // Enable indexing
                services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
                enable_recovery: false, // First start, no recovery
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await;

        // Define schema with ranged index on price field
        let fields = Field::new_schema(vec![
            Field::new_indexed(PRICE_FIELD, Type::U64, vec![IndexType::Ranged]),
            Field::new_unindexed(NAME_FIELD, Type::String),
            Field::new_unindexed(QUANTITY_FIELD, Type::U32),
        ]);

        let schema_id = 300;
        let schema = Schema::new_with_id(
            schema_id, "products", None, fields, false, true, // Scannable
        );

        let client = server
            .data_client(&vec![server_addr.clone()])
            .await
            .unwrap();
        client
            .new_schema_with_id(schema.clone())
            .await
            .unwrap()
            .unwrap();

        println!("=== Inserting test data ===");
        // Insert products with prices ranging from 10 to 100
        for _i in 10..=100 {
            let id = Id::new(2, _i);
            let mut value = OwnedValue::Map(OwnedMap::new());
            value[PRICE_FIELD] = OwnedValue::U64(_i);
            value[NAME_FIELD] = OwnedValue::String(format!("Product {}", _i));
            value[QUANTITY_FIELD] = OwnedValue::U32((_i * 5) as u32);

            let cell = OwnedCell::new_with_id(schema_id, &id, value);
            client.write_cell(cell).await.unwrap().unwrap();
        }

        println!("Inserted 91 products");

        // Helper function to query a price range and collect results
        async fn query_price_range(
            idx_client: &crate::query::data_client::IndexedDataClient,
            schema_id: u32,
            min: u64,
            max: u64,
        ) -> Vec<u64> {
            let field_id = hash_str(PRICE_FIELD);
            let val_range = ValueRange {
                start: ValueRangeTerm::inclusive_from(&OwnedValue::U64(min).shared()),
                end: ValueRangeTerm::inclusive_from(&OwnedValue::U64(max).shared()),
            };

            let mut cursor = idx_client
                .range_index_scan(
                    schema_id,
                    field_id,
                    val_range,
                    vec![],
                    Expr::nothing(),
                    Expr::nothing(),
                    Ordering::Forward,
                )
                .await
                .unwrap();

            let mut prices = vec![];
            while let Ok(Some(cell)) = cursor.next().await {
                if let OwnedValue::U64(price) = &cell.data[PRICE_FIELD] {
                    prices.push(*price);
                }
            }
            prices
        }

        println!("=== Testing range queries BEFORE recovery ===");
        let idx_client = server.indexed_data_client();

        // Test query 1: [20, 30]
        let results_1_before = query_price_range(&idx_client, schema_id, 20, 30).await;
        println!("Range [20, 30]: {} items", results_1_before.len());
        assert_eq!(results_1_before.len(), 11, "Should have 11 items");
        assert_eq!(results_1_before, (20..=30).collect::<Vec<_>>());

        // Test query 2: [50, 60]
        let results_2_before = query_price_range(&idx_client, schema_id, 50, 60).await;
        println!("Range [50, 60]: {} items", results_2_before.len());
        assert_eq!(results_2_before.len(), 11, "Should have 11 items");

        // Test query 3: [85, 95]
        let results_3_before = query_price_range(&idx_client, schema_id, 85, 95).await;
        println!("Range [85, 95]: {} items", results_3_before.len());
        assert_eq!(results_3_before.len(), 11, "Should have 11 items");

        // Test scan_all before recovery
        println!("=== Testing scan_all BEFORE recovery ===");
        let mut scan_cursor_before = idx_client
            .scan_all(
                schema_id,
                vec![],
                Expr::nothing(),
                Expr::nothing(), QueryOrdering::Asc,
            )
            .await
            .unwrap();

        let mut all_prices_before = Vec::new();
        let mut all_ids_before = Vec::new();
        while let Ok(Some(cell)) = scan_cursor_before.next().await {
            if let OwnedValue::U64(price) = &cell.data[PRICE_FIELD] {
                all_prices_before.push(*price);
                all_ids_before.push(cell.id());
            }
        }
        all_prices_before.sort();
        println!(
            "scan_all before recovery: {} items",
            all_prices_before.len()
        );
        assert_eq!(
            all_prices_before.len(),
            91,
            "Should have 91 items before recovery"
        );
        assert_eq!(
            all_prices_before,
            (10..=100).collect::<Vec<_>>(),
            "All prices from 10 to 100 should be present"
        );

        // Proper server shutdown to simulate restart
        println!("=== Simulating server restart ===");
        drop(idx_client);
        drop(client);

        // Use NebServer::shutdown() which handles LSM tree flushing and graceful shutdown
        println!("Shutting down server gracefully...");
        server.shutdown().await;
        drop(server);

        // Create new server instance (recovery) - use same address
        println!("=== Starting new server (recovery) ===");
        println!("Recovery server will use address: {}", server_addr);
        let server_recovered = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                total_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: Some(backup_dir.to_str().unwrap().to_string()),
                wal_storage: Some(wal_dir.to_str().unwrap().to_string()),
                undo_log_storage: Some(undo_dir.to_str().unwrap().to_string()),
                raft_storage: Some(raft_dir.to_str().unwrap().to_string()),
                index_enabled: true,
                services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
                enable_recovery: true, // Enable recovery from persistent storage
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await;

        // Re-register the schema after recovery (schemas are recovered from Raft but need to be loaded into cache)
        println!("Re-registering schema...");
        server_recovered
            .meta
            .schemas
            .debug_only_new_schema(schema.clone());

        println!("=== Testing range queries AFTER recovery ===");
        let idx_client_recovered = server_recovered.indexed_data_client();

        // Repeat the same queries
        println!("Attempting first query after recovery...");
        let results_1_after = query_price_range(&idx_client_recovered, schema_id, 20, 30).await;
        println!(
            "Range [20, 30] after recovery: {} items - SUCCESS!",
            results_1_after.len()
        );
        assert_eq!(
            results_1_after, results_1_before,
            "Range [20, 30] should match after recovery"
        );

        let results_2_after = query_price_range(&idx_client_recovered, schema_id, 50, 60).await;
        println!(
            "Range [50, 60] after recovery: {} items",
            results_2_after.len()
        );
        assert_eq!(
            results_2_after, results_2_before,
            "Range [50, 60] should match after recovery"
        );

        let results_3_after = query_price_range(&idx_client_recovered, schema_id, 85, 95).await;
        println!(
            "Range [85, 95] after recovery: {} items",
            results_3_after.len()
        );
        assert_eq!(
            results_3_after, results_3_before,
            "Range [85, 95] should match after recovery"
        );

        // Test scan_all after recovery
        println!("=== Testing scan_all AFTER recovery ===");
        let mut scan_cursor_after = idx_client_recovered
            .scan_all(
                schema_id,
                vec![],
                Expr::nothing(),
                Expr::nothing(), QueryOrdering::Asc,
            )
            .await
            .unwrap();

        let mut all_prices_after = Vec::new();
        let mut all_ids_after = Vec::new();
        while let Ok(Some(cell)) = scan_cursor_after.next().await {
            if let OwnedValue::U64(price) = &cell.data[PRICE_FIELD] {
                all_prices_after.push(*price);
                all_ids_after.push(cell.id());
            }
        }
        all_prices_after.sort();
        println!("scan_all after recovery: {} items", all_prices_after.len());
        assert_eq!(
            all_prices_after.len(),
            91,
            "Should have 91 items after recovery"
        );
        assert_eq!(
            all_prices_after,
            (10..=100).collect::<Vec<_>>(),
            "All prices from 10 to 100 should be present after recovery"
        );

        // Verify IDs match (order may differ, so sort them)
        all_ids_before.sort();
        all_ids_after.sort();
        assert_eq!(
            all_ids_before, all_ids_after,
            "All IDs should match after recovery"
        );

        println!("=== End-to-end recovery test passed! ===");

        // Cleanup
        drop(server_recovered);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let _ = std::fs::remove_dir_all(&test_dir);
    }
}
