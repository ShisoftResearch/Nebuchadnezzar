#[cfg(test)]
mod test {
    use crate::index::entry::EntryKey;
    use crate::index::{Feature, FEATURE_SIZE};
    use crate::ram::schema::SchemaUid;
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
        EntryKey::from_props(&id, &feature, field, SchemaUid(schema_id))
    }

    #[test]
    fn test_entry_key_prefix_comparison() {
        let schema_id = 1;
        let field = 100;

        // Create keys with same feature value but different IDs
        let key1 = create_entry_key(schema_id, field, 50, Id::from_parts(1, 10));
        let key2 = create_entry_key(schema_id, field, 50, Id::from_parts(1, 20));
        let key3 = create_entry_key(schema_id, field, 50, Id::from_parts(u64::MAX, u64::MAX));

        // Prefixes should be equal (same schema, field, feature)
        assert_eq!(key1.cmp_prefix(&key2), std::cmp::Ordering::Equal);
        assert_eq!(key1.cmp_prefix(&key3), std::cmp::Ordering::Equal);
        assert!(!key1.prefix_gt(&key2));
        assert!(!key1.prefix_gt(&key3));

        // Create key with different feature value
        let key4 = create_entry_key(schema_id, field, 51, Id::from_parts(1, 10));

        // key4 should have greater prefix than key1
        assert!(key4.prefix_gt(&key1));
        assert!(!key1.prefix_gt(&key4));

        // Create key with smaller feature value
        let key5 = create_entry_key(schema_id, field, 49, Id::from_parts(1, 10));

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
        let max_id = Id::from_parts(u64::MAX, u64::MAX);
        let end_key = EntryKey::from_props(&max_id, &feature, field, SchemaUid(schema_id));

        // Create data keys with same feature value but different IDs
        let data_key1 = create_entry_key(schema_id, field, feature_value, Id::from_parts(1, 10));
        let data_key2 = create_entry_key(schema_id, field, feature_value, Id::from_parts(1, 50));
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
        let data_key4 = create_entry_key(schema_id, field, 51, Id::from_parts(1, 10));
        assert!(
            data_key4.prefix_gt(&end_key),
            "data_key4 should be > end_key"
        );

        // Create key with feature value 49 (should be included)
        let data_key5 = create_entry_key(schema_id, field, 49, Id::from_parts(1, 10));
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
        let start_key = EntryKey::for_schema_field_feature(SchemaUid(schema_id), field, &feature);

        // Create data keys
        let data_key1 = create_entry_key(schema_id, field, 9, Id::from_parts(1, 10));
        let data_key2 = create_entry_key(schema_id, field, 10, Id::from_parts(1, 10));
        let data_key3 = create_entry_key(schema_id, field, 11, Id::from_parts(1, 10));

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
        let max_id = Id::from_parts(u64::MAX, u64::MAX);

        // Create range: [0, 50] inclusive
        let start_key =
            EntryKey::for_schema_field_feature(SchemaUid(schema_id), field, &u64_to_feature(0));
        let end_key =
            EntryKey::from_props(&max_id, &u64_to_feature(50), field, SchemaUid(schema_id));

        // Simulate iterating through values 0 to 51
        let mut included = Vec::new();
        for i in 0..=51 {
            let data_key = create_entry_key(schema_id, field, i, Id::from_parts(1, i));

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
        let max_id = Id::from_parts(u64::MAX, u64::MAX);

        // Create end key for value 50 (inclusive)
        let end_key =
            EntryKey::from_props(&max_id, &u64_to_feature(50), field, SchemaUid(schema_id));

        // Test value 49 (should be included)
        let key49 = create_entry_key(schema_id, field, 49, Id::from_parts(1, 49));
        assert!(
            !key49.prefix_gt(&end_key),
            "Value 49 should not be > end_key (should be included)"
        );

        // Test value 50 (should be included - this is the boundary case)
        let key50 = create_entry_key(schema_id, field, 50, Id::from_parts(1, 50));
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
        let key51 = create_entry_key(schema_id, field, 51, Id::from_parts(1, 51));
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
        use crate::index::ranged::tree::tree::DeletionSet;
        use std::sync::Arc;

        fn deletion_set() -> Arc<DeletionSet> {
            Arc::new(DeletionSet::with_capacity(16))
        }

        let tree = LevelBPlusTree::new(&deletion_set());
        let schema_id = 1;
        let field = 100;

        // Insert keys with values 0 to 50
        for i in 0..=50 {
            let key = create_entry_key(schema_id, field, i, Id::from_parts(1, i));
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
        let key50_seek =
            EntryKey::for_schema_field_feature(SchemaUid(schema_id), field, &u64_to_feature(50));
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
        let key49_seek =
            EntryKey::for_schema_field_feature(SchemaUid(schema_id), field, &u64_to_feature(49));
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
        let key50_full = create_entry_key(schema_id, field, 50, Id::from_parts(1, 50));
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
        let key49 = create_entry_key(schema_id, field, 49, Id::from_parts(1, 49));
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
        let start_key =
            EntryKey::for_schema_field_feature(SchemaUid(schema_id), field, &u64_to_feature(0));
        let max_id = Id::from_parts(u64::MAX, u64::MAX);
        let end_key =
            EntryKey::from_props(&max_id, &u64_to_feature(50), field, SchemaUid(schema_id));

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

                collected.push(key.id().bits() & ((1u64 << 48) - 1));
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
        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();
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

        let lsm_tree_id = Id::from_parts(999, 999);
        let schema_id = 1;
        let field = 200;

        // Create LSM tree and insert test data
        let tree = RangedTree::create(&client, &lsm_tree_id).await;

        // Insert keys with feature values 10..=100
        for i in 10..=100 {
            let key = create_entry_key(schema_id, field, i, Id::from_parts(2, i));
            tree.insert(&key);
        }

        println!("=== Tree state after insertion ===");
        println!("Tree count: {}", tree.count());
        println!("Tree ideal_capacity: {}", tree.ideal_capacity());
        println!("Tree oversized: {}", tree.oversized());

        // Helper function to perform range query and collect results
        let collect_range = |tree: &RangedTree, start: u64, end: u64| -> Vec<u64> {
            let start_key = EntryKey::for_schema_field_feature(
                SchemaUid(schema_id),
                field,
                &u64_to_feature(start),
            );
            let max_id = Id::from_parts(u64::MAX, u64::MAX);
            let end_key =
                EntryKey::from_props(&max_id, &u64_to_feature(end), field, SchemaUid(schema_id));

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

        // Persist to disk: one explicit drain, which is what the old
        // per-tree merge no-op did anyway.
        println!("=== Draining write-back to persist data ===");
        storage::wait_until_updated().await;
        println!("Tree count after drain: {}", tree.count());

        for i in 0..5 {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            let merged_again = storage::wait_until_updated().await;
            println!(
                "Additional drain {} result: {}, count: {}",
                i,
                merged_again,
                tree.count()
            );
        }

        // Update the LSM tree cell with the current head IDs (critical for recovery!)
        println!("=== Updating LSM tree cell with new head IDs ===");
        println!("Tree head ID: {:?}", tree.head_id());
        tree.publish_head(&lsm_tree_id, &client)
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
        let recovered_tree = RangedTree::recover(&client, &lsm_tree_id)
            .await
            .expect("the tree should load back from storage");

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
        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();
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

        let lsm_tree_id = Id::from_parts(888, 888);
        let schema_id = 1;
        let field = 300;

        // Create LSM tree and insert test data
        let tree = RangedTree::create(&client, &lsm_tree_id).await;

        // Insert keys with feature values 10..=100 (91 items to ensure oversized mem tree)
        for i in 10..=100 {
            let key = create_entry_key(schema_id, field, i, Id::from_parts(3, i));
            tree.insert(&key);
        }

        println!("=== Tree state after insertion ===");
        println!("Tree count: {}", tree.count());
        println!("Tree ideal_capacity: {}", tree.ideal_capacity());

        // Helper function to perform backward range query
        let collect_range_backward = |tree: &RangedTree, start: u64, end: u64| -> Vec<u64> {
            let start_key = EntryKey::for_schema_field_feature(
                SchemaUid(schema_id),
                field,
                &u64_to_feature(start),
            );
            let max_id = Id::from_parts(u64::MAX, u64::MAX);
            let end_key =
                EntryKey::from_props(&max_id, &u64_to_feature(end), field, SchemaUid(schema_id));

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
        storage::wait_until_updated().await;
        println!("Tree count after first merge: {}", tree.count());

        // Give time for async writes to complete and merge multiple times to ensure all data is on disk
        for _ in 0..5 {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            storage::wait_until_updated().await;
        }
        println!("Tree count after additional merges: {}", tree.count());

        // Update the LSM tree cell with the current head IDs (critical for recovery!)
        tree.publish_head(&lsm_tree_id, &client)
            .await
            .expect("Failed to mark migration in test");

        // Wait for writeback to complete - longer wait for test isolation
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        drop(tree);

        println!("=== Recovering tree ===");
        let recovered_tree = RangedTree::recover(&client, &lsm_tree_id)
            .await
            .expect("the tree should load back from storage");
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
        use crate::index::builder::IndexBuilder;
        use crate::index::ranged::tree::btree::storage;
        use crate::index::ranged::tree::btree::Ordering;
        use crate::query::data_client::{QueryOrdering, ValueRange, ValueRangeTerm};
        use crate::ram::cell::OwnedCell;
        use crate::ram::schema::{Field, IndexType, Schema};
        use crate::ram::schema::{SchemaUid, SchemaVid};
        use crate::ram::types::Type;
        use crate::server::*;
        use bifrost_hasher::hash_str;
        use dovahkiin::{expr::serde::Expr, types::*};
        use std::time::{Duration, Instant};
        use tempfile::TempDir;

        let _ = env_logger::try_init();
        let server_group = "e2e-range-recovery";
        let server_addr = crate::utils::test_port::unique_localhost_addr();

        const PRICE_FIELD: &'static str = "price";
        const NAME_FIELD: &'static str = "name";
        const QUANTITY_FIELD: &'static str = "quantity";

        // Use a dedicated temp directory so repeated runs cannot inherit stale state.
        let test_dir = TempDir::new().unwrap();
        let backup_dir = test_dir.path().join("backup");
        let wal_dir = test_dir.path().join("wal");
        let undo_dir = test_dir.path().join("undo");
        let raft_dir = test_dir.path().join("raft");

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
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: Some(backup_dir.to_str().unwrap().to_string()),
                wal_storage: Some(wal_dir.to_str().unwrap().to_string()),
                raft_storage: Some(raft_dir.to_str().unwrap().to_string()),
                index_enabled: true, // Enable indexing
                services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
                enable_recovery: false, // First start, no recovery
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

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
            let id = Id::from_parts(2, _i);
            let mut value = OwnedValue::Map(OwnedMap::new());
            value[PRICE_FIELD] = OwnedValue::U64(_i);
            value[NAME_FIELD] = OwnedValue::String(format!("Product {}", _i));
            value[QUANTITY_FIELD] = OwnedValue::U32((_i * 5) as u32);

            let cell = OwnedCell::new_with_id(SchemaVid(schema_id), &id, value);
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
                    SchemaUid(schema_id),
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

        async fn scan_all_prices(
            idx_client: &crate::query::data_client::IndexedDataClient,
            schema_id: u32,
        ) -> (Vec<u64>, Vec<Id>) {
            let mut scan_cursor = idx_client
                .scan_all(
                    SchemaUid(schema_id),
                    vec![],
                    Expr::nothing(),
                    Expr::nothing(),
                    QueryOrdering::Asc,
                )
                .await
                .unwrap();

            let mut prices = Vec::new();
            let mut ids = Vec::new();
            while let Ok(Some(cell)) = scan_cursor.next().await {
                if let OwnedValue::U64(price) = &cell.data[PRICE_FIELD] {
                    prices.push(*price);
                    ids.push(cell.id());
                }
            }
            prices.sort();
            (prices, ids)
        }

        println!("=== Testing range queries BEFORE recovery ===");
        let idx_client = server.indexed_data_client();
        let expected_range_1: Vec<u64> = (20..=30).collect();
        let expected_all_prices: Vec<u64> = (10..=100).collect();

        let _ = IndexBuilder::await_all_indices().await;
        storage::wait_until_updated().await;

        let deadline = Instant::now() + Duration::from_secs(30);
        let (results_1_before, all_prices_before, mut all_ids_before) = loop {
            let results_1 = query_price_range(&idx_client, schema_id, 20, 30).await;
            let (all_prices, all_ids) = scan_all_prices(&idx_client, schema_id).await;
            if results_1 == expected_range_1 && all_prices == expected_all_prices {
                break (results_1, all_prices, all_ids);
            }

            assert!(
                Instant::now() < deadline,
                "range index did not converge before recovery: range [20, 30]={:?}, scan_all={} items",
                results_1,
                all_prices.len()
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        };

        // Test query 1: [20, 30]
        println!("Range [20, 30]: {} items", results_1_before.len());
        assert_eq!(results_1_before.len(), 11, "Should have 11 items");
        assert_eq!(results_1_before, expected_range_1);

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
            all_prices_before, expected_all_prices,
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
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: Some(backup_dir.to_str().unwrap().to_string()),
                wal_storage: Some(wal_dir.to_str().unwrap().to_string()),
                raft_storage: Some(raft_dir.to_str().unwrap().to_string()),
                index_enabled: true,
                services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
                enable_recovery: true, // Enable recovery from persistent storage
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

        // Re-register the schema after recovery (schemas are recovered from Raft but need to be loaded into cache)
        println!("Re-registering schema...");
        server_recovered
            .meta()
            .schemas
            .debug_only_new_schema(schema.clone());

        println!("=== Testing range queries AFTER recovery ===");
        let idx_client_recovered = server_recovered.indexed_data_client();

        let recovery_deadline = Instant::now() + Duration::from_secs(30);
        let (results_1_after, all_prices_after, mut all_ids_after) = loop {
            let results_1 = query_price_range(&idx_client_recovered, schema_id, 20, 30).await;
            let (all_prices, all_ids) = scan_all_prices(&idx_client_recovered, schema_id).await;
            if results_1 == expected_range_1 && all_prices == expected_all_prices {
                break (results_1, all_prices, all_ids);
            }

            assert!(
                Instant::now() < recovery_deadline,
                "recovered range index did not converge: range [20, 30]={:?}, scan_all={} items",
                results_1,
                all_prices.len()
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        };

        // Repeat the same queries
        println!("Attempting first query after recovery...");
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
        println!("scan_all after recovery: {} items", all_prices_after.len());
        assert_eq!(
            all_prices_after.len(),
            91,
            "Should have 91 items after recovery"
        );
        assert_eq!(
            all_prices_after, expected_all_prices,
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
        drop(idx_client_recovered);
        server_recovered.shutdown().await;
        drop(server_recovered);
    }

    #[tokio::test]
    async fn test_split_target_tree_is_visible_before_routing() {
        use crate::client;
        use crate::index::ranged::tree::btree::{page_schema, storage, Ordering};
        use crate::index::ranged::tree::tree::{RangedTree, RANGED_TREE_SCHEMA};
        use crate::index::ranged::trees::Cursor;
        use crate::server::{NebServer, ServerOptions, Service};
        use std::sync::Arc;

        let _ = env_logger::try_init();

        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "split_target_publish_test";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();
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

        storage::start_external_nodes_write_back(&client);

        let source_tree_id = Id::from_parts(777, 777);
        let target_tree_id = Id::from_parts(778, 778);
        let schema_id = 1;
        let field = 400;
        let source_tree = RangedTree::create(&client, &source_tree_id).await;
        let target_tree = RangedTree::create(&client, &target_tree_id).await;

        for i in 10..=100 {
            let key = create_entry_key(schema_id, field, i, Id::from_parts(4, i));
            source_tree.insert(&key);
        }

        let pivot = create_entry_key(schema_id, field, 60, Id::from_parts(0, 0));
        let mut cursor = source_tree.seek(&pivot, Ordering::Forward);
        let mut moved_keys = Vec::new();
        while let Some(entry) = cursor.next() {
            moved_keys.push(entry);
        }
        assert!(
            !moved_keys.is_empty(),
            "expected to move some keys into the split target"
        );

        target_tree.merge_keys(moved_keys.clone());
        storage::wait_until_updated().await;
        target_tree
            .publish_head(&target_tree_id, &client)
            .await
            .expect("target tree head should be published before routing");

        let recovered_target = RangedTree::recover(&client, &target_tree_id)
            .await
            .expect("the target tree should load back from storage");
        assert_eq!(
            recovered_target.count(),
            moved_keys.len(),
            "recovered split target should expose the moved keys once metadata is published"
        );

        server.shutdown().await;
    }

    #[ignore = "stress test"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_writes_during_split_remain_scannable() {
        use crate::client;
        use crate::index::ranged::client::RangedIndexerClient;
        use crate::index::ranged::tree::btree::{self, storage, Ordering};
        use crate::index::ranged::trees::{Cursor, Range};
        use crate::index::EntryKey;
        use crate::ram::schema::{Field, Schema};
        use crate::ram::types::Type;
        use crate::server::{NebServer, ServerOptions, Service};
        use futures::stream::FuturesUnordered;
        use futures::StreamExt;
        use itertools::Itertools;
        use rand::seq::SliceRandom;
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        let _ = env_logger::try_init();

        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "split_concurrent_write_stress";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 128 * 1024 * 1024,
                db_size: 128 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell, Service::RangedIndexer],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.clone()],
                server_group,
            )
            .await
            .unwrap(),
        );
        client
            .new_schema_with_id(Schema::new_with_id(
                11,
                &String::from("split_stress"),
                None,
                Field::new_schema(vec![Field::new_unindexed("data", Type::U8)]),
                false,
                false,
            ))
            .await
            .unwrap()
            .unwrap();

        // The placement SM lives on the database's meta plane; the pre-plane
        // constructor queried the default plane and every locate came back
        // SmNotFound (this test was written before planes and then ignored).
        let meta_plane_client = server.raft_client.plane(crate::server::database_meta_plane_id(
            server_group,
            server_group,
        ));
        let ranged_client = Arc::new(RangedIndexerClient::new_for_database(
            &server.consh,
            &meta_plane_client,
            server_group,
            server_group,
        ));
        let expected_total =
            btree::ideal_capacity_from_node_size(btree::level::BTREE_NODE_SIZE) * 4;
        let mut shuffled = (0..expected_total).collect_vec();
        shuffled.as_mut_slice().shuffle(&mut rand::thread_rng());

        let mut writers = FuturesUnordered::new();
        for value in shuffled {
            let ranged_client = ranged_client.clone();
            writers.push(tokio::time::timeout(Duration::from_secs(240), async move {
                let id = Id::from_parts(1, value as u64);
                let key = EntryKey::from_id(&id);
                ranged_client.insert(&key).await
            }));
        }
        while let Some(result) = writers.next().await {
            assert!(result.unwrap().unwrap(), "insertion returned false");
        }

        assert!(
            !ranged_client.tree_stats().await.unwrap().is_empty(),
            "expected ranged tree stats after concurrent write stress"
        );

        storage::wait_until_updated().await;

        let start_id = Id::from_parts(1, 0);
        let mut cursor = RangedIndexerClient::seek(
            &ranged_client,
            Range::new_inclusive_opened(EntryKey::from_id(&start_id), Ordering::Forward),
            256,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(cursor.current(), Some(&start_id));

        // On a miss, say which key, whether a point lookup still finds it,
        // and how many keys the trees report -- that separates "the split
        // lost entries" from "the scan broke at a boundary".
        let diagnose = |stats: Vec<crate::index::ranged::tree::service::TreeStat>,
                        value: usize,
                        contains: bool,
                        current: Option<Id>| {
            let total: usize = stats.iter().map(|s| s.trees.iter().map(|t| t.count).sum::<usize>()).sum();
            panic!(
                "ordered scan broke at position {} of {} (cursor at {:?}): contains({}) = {}, \
                 trees hold {} keys across {} trees -- {}",
                value,
                expected_total,
                current,
                value,
                contains,
                total,
                stats.len(),
                if total == expected_total {
                    "nothing lost, the SCAN is wrong"
                } else {
                    "the index LOST entries"
                }
            );
        };
        for value in 0..expected_total {
            let expected_id = Id::from_parts(1, value as u64);
            let current = cursor.current().cloned();
            if current != Some(expected_id) {
                let contains = ranged_client
                    .contains(&EntryKey::from_id(&expected_id))
                    .await
                    .unwrap_or(false);
                let stats = ranged_client.tree_stats().await.unwrap_or_default();
                diagnose(stats, value, contains, current);
            }
            let _ = cursor.next().await.unwrap();
        }
        assert!(
            cursor.next().await.unwrap().is_none(),
            "expected ordered scan to end immediately after the final inserted key"
        );

        server.shutdown().await;
    }

    /// The missing ingredient from
    /// `test_concurrent_writes_during_split_remain_scannable`: readers
    /// CONCURRENT with the inserts and the structural splits. The bulk import
    /// always runs this shape -- every insert spawns an `ensure_scannable`
    /// verification seek -- and that is where the "fresh root descent
    /// regressed" storms lived (2026-08-31, `server_final4.log`: 8,700
    /// give-ups on a fresh store with ZERO structural splits, so plain
    /// insert+seek concurrency is sufficient).
    ///
    /// Three reader shapes run against the insert storm:
    /// - every writer verifies its own insert immediately (the
    ///   ensure_scannable shape: seek at the key, first element must be it);
    /// - scanner tasks seek random already-plausible positions and assert
    ///   every returned block is strictly ascending and starts at/after the
    ///   seek key;
    /// - the final full ordered scan of the original test.
    ///
    /// `NEB_SEEK_REGRESSION_PANIC` makes the service panic at the first
    /// ordering violation with the tree id and both keys, instead of the
    /// production restart guard papering over it. Depth 2 makes structural
    /// splits fire early and often at this scale.
    #[ignore = "stress test"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_seeks_during_inserts_and_splits_never_regress() {
        use crate::client;
        use crate::index::ranged::client::RangedIndexerClient;
        use crate::index::ranged::tree::btree::{self, storage, Ordering};
        use crate::index::ranged::trees::Range;
        use crate::index::EntryKey;
        use crate::ram::schema::{Field, Schema};
        use crate::ram::types::Type;
        use crate::server::{NebServer, ServerOptions, Service};
        use itertools::Itertools;
        use rand::seq::SliceRandom;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let _ = env_logger::try_init();

        // Before any seek can run: the hook is read once, lazily.
        std::env::set_var("NEB_SEEK_REGRESSION_PANIC", "1");
        btree::set_tree_depth(2);

        // A hung stress test must die loudly, not sit at 100% on one core
        // forever: abort the whole process after the deadline so a run under
        // gdb hands over every thread's stack (the first pre-fix run of this
        // test wedged a seek exactly like the production imports did, and
        // ptrace_scope=1 made a live process undebuggable without root).
        let watchdog_secs = std::env::var("NEB_TEST_ABORT_AFTER_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1200);

        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "seek_insert_split_stress";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 128 * 1024 * 1024,
                db_size: 128 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell, Service::RangedIndexer],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.clone()],
                server_group,
            )
            .await
            .unwrap(),
        );
        client
            .new_schema_with_id(Schema::new_with_id(
                11,
                &String::from("seek_stress"),
                None,
                Field::new_schema(vec![Field::new_unindexed("data", Type::U8)]),
                false,
                false,
            ))
            .await
            .unwrap()
            .unwrap();

        let meta_plane_client = server.raft_client.plane(crate::server::database_meta_plane_id(
            server_group,
            server_group,
        ));
        let ranged_client = Arc::new(RangedIndexerClient::new_for_database(
            &server.consh,
            &meta_plane_client,
            server_group,
            server_group,
        ));

        // Depth 2: capacity ~32K keys per tree. Keys go in in WAVES of one
        // capacity each with a short pause between, until at least two
        // structural splits have fired: the balancer's split cycle (its
        // snapshot write-back barrier plus the placement RPCs) takes seconds
        // under load, so one continuous burst can finish before the first
        // split even starts. The pause mirrors the bulk import's inter-batch
        // gaps; the splits themselves then run under the NEXT wave's
        // insert+verify storm and the scanners -- the shape that was never
        // tested.
        let wave_size = btree::ideal_capacity_from_node_size(btree::level::BTREE_NODE_SIZE) * 2;
        let max_waves = 24usize;

        let writers_done = Arc::new(AtomicBool::new(false));
        let scanned_blocks = Arc::new(AtomicUsize::new(0));
        // Values below this are fully inserted; scanners sample below it.
        let watermark = Arc::new(AtomicUsize::new(0));
        // What the clients were told when an operation failed: the wedge
        // diagnosis lives in these strings (`too_many_retry` carries the
        // last refusal reason).
        let error_reasons: Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>> =
            Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let note_reason = {
            let error_reasons = error_reasons.clone();
            move |e: String| {
                let mut map = error_reasons.lock().unwrap();
                let count = map.entry(e).or_insert(0);
                *count += 1;
            }
        };
        {
            let error_reasons = error_reasons.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(watchdog_secs));
                eprintln!(
                    "watchdog: test still running after {}s; aborting for stacks. \
                     client error reasons so far: {:?}",
                    watchdog_secs,
                    error_reasons.lock().unwrap()
                );
                std::process::abort();
            });
        }

        // Scanners: random-position range seeks, asserting each block is
        // strictly ascending and never starts before the seek key. They run
        // for the whole life of the insert storm.
        let mut scanners = JoinSet::new();
        for scanner in 0..4u64 {
            let ranged_client = ranged_client.clone();
            let writers_done = writers_done.clone();
            let scanned_blocks = scanned_blocks.clone();
            let watermark = watermark.clone();
            let note_reason = note_reason.clone();
            scanners.spawn(async move {
                // Cheap deterministic per-task RNG; thread_rng is not Send.
                let mut state = 0x9E3779B97F4A7C15u64.wrapping_mul(scanner + 1);
                let mut iterations = 0u64;
                while !writers_done.load(AtomicOrdering::Acquire) {
                    iterations += 1;
                    if iterations % 256 == 0 {
                        // Stay a storm, not a monopoly: an unyielding seek
                        // loop can pin a worker for the whole run.
                        tokio::task::yield_now().await;
                    }
                    if iterations % 100_000 == 0 {
                        println!("scanner {} at {} iterations", scanner, iterations);
                    }
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let bound = watermark.load(AtomicOrdering::Acquire).max(1);
                    let start = (state >> 16) as usize % bound;
                    let start_id = Id::from_parts(1, start as u64);
                    let start_key = EntryKey::from_id(&start_id);
                    let seek_res = match tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        RangedIndexerClient::seek(
                            &ranged_client,
                            Range::new_inclusive_opened(start_key.clone(), Ordering::Forward),
                            256,
                            None,
                        ),
                    )
                    .await
                    {
                        Ok(res) => res,
                        Err(_) => {
                            note_reason("scanner seek TIMED OUT after 30s".to_string());
                            continue;
                        }
                    };
                    match seek_res {
                        Ok(Some(cursor)) => {
                            let block: &Vec<Id> = cursor.current_block();
                            if let Some(first) = block.first() {
                                assert!(
                                    EntryKey::from_id(first) >= start_key,
                                    "scan block starts BEFORE its seek key: sought {:?}, got {:?}",
                                    start_id,
                                    first
                                );
                            }
                            for pair in block.windows(2) {
                                assert!(
                                    pair[0].bits() < pair[1].bits(),
                                    "scan block not strictly ascending: {:?} then {:?} (seek {:?})",
                                    pair[0],
                                    pair[1],
                                    start_id
                                );
                            }
                            if !block.is_empty() {
                                scanned_blocks.fetch_add(1, AtomicOrdering::Relaxed);
                            }
                        }
                        Ok(None) => {}
                        // Transient routing errors (mid-split placement moves)
                        // are the client's to retry; the scanner backs off a
                        // beat and moves on -- an immediate re-seek against a
                        // wedged tree turns one stuck task into a pinned core.
                        Err(e) => {
                            note_reason(format!("scanner seek: {:?}", e));
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        }
                    }
                }
            });
        }

        // Writers: the ensure_scannable shape. Insert, then immediately seek
        // the inserted key and require the first element to be exactly it --
        // an element ordered BEFORE the key is the regression this test
        // exists to catch (fail fast), one after it means the insert is not
        // visible (retry, then fail loudly).
        let mut inserted_end = 0usize;
        let mut split_seen = false;
        for _wave in 0..max_waves {
            let mut shuffled = (inserted_end..inserted_end + wave_size).collect_vec();
            shuffled.as_mut_slice().shuffle(&mut rand::thread_rng());
            let mut writers = JoinSet::new();
            for shard in shuffled.chunks(wave_size / 16 + 1) {
                let shard = shard.to_vec();
                let ranged_client = ranged_client.clone();
                let note_reason = note_reason.clone();
                writers.spawn(async move {
                    for value in shard {
                        let id = Id::from_parts(1, value as u64);
                        let key = EntryKey::from_id(&id);
                        // Timeout + retry so a single RPC whose answer never
                        // arrives names itself instead of parking the writer
                        // silently forever -- and so a retry distinguishes a
                        // lost response (retry succeeds) from wedged state
                        // (times out again).
                        let mut inserted = None;
                        for round in 0..10 {
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(30),
                                ranged_client.insert(&key),
                            )
                            .await
                            {
                                Ok(res) => {
                                    let landed = res.unwrap_or_else(|e| {
                                        panic!("insert rpc failed for {:?}: {:?}", id, e)
                                    });
                                    // Every v is inserted exactly once by its
                                    // owner: a first-round "already present"
                                    // means the index held a ghost.
                                    if !landed && round == 0 {
                                        println!("SOAK_INSERT_DUP id={:?}", id);
                                    }
                                    inserted = Some(landed);
                                    break;
                                }
                                Err(_) => {
                                    note_reason(format!(
                                        "insert TIMED OUT after 30s (round {})",
                                        round
                                    ));
                                }
                            }
                        }
                        assert!(
                            inserted.unwrap_or_else(|| panic!(
                                "insert for {:?} timed out 10 rounds; the RPC never answers",
                                id
                            )),
                            "insertion returned false for {:?}",
                            id
                        );
                        let mut visible = false;
                        for _attempt in 0..240 {
                            let seek_res = match tokio::time::timeout(
                                std::time::Duration::from_secs(30),
                                RangedIndexerClient::seek(
                                    &ranged_client,
                                    Range::new_inclusive_opened(key.clone(), Ordering::Forward),
                                    1,
                                    None,
                                ),
                            )
                            .await
                            {
                                Ok(res) => res,
                                Err(_) => {
                                    note_reason("verify seek TIMED OUT after 30s".to_string());
                                    continue;
                                }
                            };
                            match seek_res {
                                Ok(Some(cursor)) => match cursor.current_block().first() {
                                    Some(first) if *first == id => {
                                        visible = true;
                                        break;
                                    }
                                    Some(first) => {
                                        assert!(
                                            EntryKey::from_id(first) >= key,
                                            "verification seek positioned BEFORE its key: \
                                             sought {:?}, got {:?}",
                                            id,
                                            first
                                        );
                                        // A later key first: the insert is not
                                        // visible yet; retry.
                                    }
                                    None => {}
                                },
                                Ok(None) => {}
                                Err(e) => {
                                    note_reason(format!("verify seek: {:?}", e));
                                }
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                        }
                        assert!(visible, "inserted key never became visible: {:?}", id);
                    }
                });
            }
            while let Some(result) = writers.join_next().await {
                result.unwrap();
            }
            inserted_end += wave_size;
            watermark.store(inserted_end, AtomicOrdering::Release);
            let mut trees = 1;
            for round in 0..5 {
                match tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    ranged_client.tree_stats(),
                )
                .await
                {
                    Ok(stats) => {
                        trees = stats.map(|s| s.len()).unwrap_or(1);
                        break;
                    }
                    Err(_) => {
                        note_reason(format!("tree_stats TIMED OUT after 30s (round {})", round));
                    }
                }
            }
            println!(
                "wave {} complete: {} keys inserted, {} tree(s), client errors so far: {:?}",
                _wave,
                inserted_end,
                trees,
                error_reasons.lock().unwrap()
            );
            if trees >= 3 {
                // At least two structural splits happened, and they ran under
                // this wave's insert+verify storm and the scanners.
                split_seen = true;
                break;
            }
            // Let the balancer get through its write-back barrier and start
            // the split; the split then completes under the next wave's load.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        writers_done.store(true, AtomicOrdering::Release);
        while let Some(result) = scanners.join_next().await {
            result.unwrap();
        }
        // Guard against a vacuous pass on both reader and writer sides.
        assert!(
            scanned_blocks.load(AtomicOrdering::Relaxed) > 0,
            "scanners never saw a non-empty block; the test exercised nothing"
        );
        assert!(
            split_seen,
            "no structural split completed within {} waves ({} keys); \
             the split path was not exercised",
            max_waves, inserted_end
        );

        storage::wait_until_updated().await;

        // Final full ordered scan: every key, in order, nothing lost.
        let start_id = Id::from_parts(1, 0);
        let mut cursor = RangedIndexerClient::seek(
            &ranged_client,
            Range::new_inclusive_opened(EntryKey::from_id(&start_id), Ordering::Forward),
            256,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(cursor.current(), Some(&start_id));
        for value in 0..inserted_end {
            let expected_id = Id::from_parts(1, value as u64);
            let current = cursor.current().cloned();
            if current != Some(expected_id) {
                let contains = ranged_client
                    .contains(&EntryKey::from_id(&expected_id))
                    .await
                    .unwrap_or(false);
                panic!(
                    "final scan broke at position {} of {} (cursor at {:?}): contains = {}",
                    value, inserted_end, current, contains
                );
            }
            let _ = cursor.next().await.unwrap();
        }

        server.shutdown().await;
    }

    /// A range seek whose covering tree's remaining range has been deleted
    /// out must CONTINUE into the neighboring tree, not report end-of-scan.
    ///
    /// The server answered an exhausted tree with an empty block and
    /// next=None, which the client rightly reads as "the scan is over" --
    /// so every key in later trees silently vanished from the scan. The 3h
    /// soak's exact stripe audit caught it 37 minutes in: a seek returning
    /// NOTHING with 498,968 keys live to the right of an emptied straddling
    /// tree (deletes are the trigger, which is why the import-shaped tests
    /// never saw it). The fix hands back the tree's boundary as the resume
    /// point unless the tree is the last one in scan direction.
    #[ignore = "stress test"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_seek_resumes_past_an_emptied_tree_range() {
        use crate::client;
        use crate::index::ranged::client::RangedIndexerClient;
        use crate::index::ranged::tree::btree::{self, Ordering};
        use crate::index::ranged::trees::Range;
        use crate::index::EntryKey;
        use crate::ram::schema::{Field, Schema};
        use crate::ram::types::Type;
        use crate::server::{NebServer, ServerOptions, Service};
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        let _ = env_logger::try_init();
        std::env::set_var("NEB_SEEK_REGRESSION_PANIC", "1");
        btree::set_tree_depth(2);

        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "seek_resume_emptied_range";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 128 * 1024 * 1024,
                db_size: 128 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell, Service::RangedIndexer],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();
        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.clone()],
                server_group,
            )
            .await
            .unwrap(),
        );
        client
            .new_schema_with_id(Schema::new_with_id(
                11,
                &String::from("seek_resume"),
                None,
                Field::new_schema(vec![Field::new_unindexed("data", Type::U8)]),
                false,
                false,
            ))
            .await
            .unwrap()
            .unwrap();
        let meta_plane_client = server.raft_client.plane(crate::server::database_meta_plane_id(
            server_group,
            server_group,
        ));
        let ranged_client = Arc::new(RangedIndexerClient::new_for_database(
            &server.consh,
            &meta_plane_client,
            server_group,
            server_group,
        ));

        // Fill past capacity in waves until a structural split lands, so two
        // trees partition the key space (pivot ~= n/2 for sequential fill).
        let n: u64 = 65536;
        for v in 0..n {
            let key = EntryKey::from_id(&Id::from_parts(1, v));
            assert!(ranged_client.insert(&key).await.unwrap());
        }
        let split_deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let trees = ranged_client.tree_stats().await.map(|s| s.len()).unwrap_or(1);
            if trees >= 2 {
                break;
            }
            assert!(
                Instant::now() < split_deadline,
                "no structural split within 180s; cannot stage the two-tree shape"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Empty the straddle window: everything from n/4 up to well past any
        // mid pivot. The first tree's remaining range [n/4, pivot) is now
        // logically empty; the second tree keeps live keys from `first_live`.
        let del_from = n / 4;
        let first_live = (n * 65) / 100;
        for v in del_from..first_live {
            let key = EntryKey::from_id(&Id::from_parts(1, v));
            ranged_client.delete(&key).await.unwrap();
        }

        // Forward: a seek at the emptied window's start must yield the first
        // live key beyond it -- which lives in the SECOND tree.
        let seek_key = EntryKey::from_id(&Id::from_parts(1, del_from));
        let cursor = RangedIndexerClient::seek(
            &ranged_client,
            Range::new_inclusive_opened(seek_key, Ordering::Forward),
            16,
            None,
        )
        .await
        .unwrap()
        .unwrap_or_else(|| {
            panic!(
                "forward scan reported END with {} live keys beyond the emptied range",
                n - first_live
            )
        });
        assert_eq!(
            cursor.current_block().first(),
            Some(&Id::from_parts(1, first_live)),
            "forward scan must resume at the first live key past the emptied range"
        );

        // Backward: a seek inside the second tree's emptied head must yield
        // the last live key BELOW the window -- which lives in the FIRST tree.
        let back_from = (n * 55) / 100;
        let back_key = EntryKey::from_id(&Id::from_parts(1, back_from));
        let cursor = RangedIndexerClient::seek(
            &ranged_client,
            Range::new_inclusive_opened(back_key, Ordering::Backward),
            16,
            None,
        )
        .await
        .unwrap()
        .unwrap_or_else(|| {
            panic!(
                "backward scan reported END with {} live keys below the emptied range",
                del_from
            )
        });
        assert_eq!(
            cursor.current_block().first(),
            Some(&Id::from_parts(1, del_from - 1)),
            "backward scan must resume at the last live key below the emptied range"
        );

        server.shutdown().await;
    }

    /// Hours-long soak of the ranged index under the production workload
    /// mix, on ONE long-lived server (a fresh process per run hides
    /// everything that only accumulates):
    ///
    /// - writers own disjoint id stripes and burst mostly-ascending inserts
    ///   with local disorder (the BANC arrival shape), verifying every
    ///   insert ensure_scannable style;
    /// - each writer deletes ~20% as it goes and verifies the tombstone
    ///   (contains -> false), so compaction runs against the scans all soak;
    /// - every 32 batches a writer audits its OWN stripe exactly: the scan
    ///   must yield precisely its live set, strictly ascending -- only the
    ///   owner mutates the stripe, so the audit has no tolerance;
    /// - scanners hammer random positions asserting block monotonicity;
    /// - a roamer walks the WHOLE index end to end every 5 minutes,
    ///   asserting global ascending order across every tree boundary
    ///   (the next_tree / refill paths, under live splits);
    /// - depth 2 keeps structural splits firing for the entire duration;
    /// - NEB_SEEK_REGRESSION_PANIC is armed, and a watchdog aborts a wedged
    ///   process for stacks.
    ///
    /// Duration and scale come from NEB_SOAK_SECS (default 10800) and
    /// NEB_SOAK_TARGET_KEYS (default 12M inserted over the run; writers pace
    /// themselves to spread it). A minute reporter prints SOAK_MIN lines
    /// (ops, trees, RSS, threads) so the log tells the whole story.
    #[ignore = "soak test"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_soak_ranged_index() {
        use crate::client;
        use crate::index::ranged::client::RangedIndexerClient;
        use crate::index::ranged::tree::btree::{self, Ordering};
        use crate::index::ranged::trees::Range;
        use crate::index::EntryKey;
        use crate::ram::schema::{Field, Schema};
        use crate::ram::types::Type;
        use crate::server::{NebServer, ServerOptions, Service};
        use std::collections::BTreeSet;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        use tokio::task::JoinSet;

        let _ = env_logger::try_init();
        // "initial": a fresh descent positioning before its key (the
        // client-visible contract) still panics; mid-scan regressions take
        // the production restart path but log SEEK_RESTART lines -- the soak
        // collects evidence over hours instead of dying on a transient the
        // guard is documented to absorb (one killed attempt 3 in minute 1).
        std::env::set_var("NEB_SEEK_REGRESSION_PANIC", "initial");
        btree::set_tree_depth(2);

        fn env_u64(name: &str, default: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }
        let soak_secs = env_u64("NEB_SOAK_SECS", 10800);
        let target_inserts = env_u64("NEB_SOAK_TARGET_KEYS", 12_000_000);
        const WRITERS: u64 = 8;
        const SCANNERS: u64 = 3;
        const BATCH: u64 = 512;
        let start = Instant::now();
        let deadline = start + Duration::from_secs(soak_secs);

        fn proc_status(field: &str) -> u64 {
            std::fs::read_to_string("/proc/self/status")
                .ok()
                .and_then(|s| {
                    s.lines()
                        .find(|l| l.starts_with(field))
                        .and_then(|l| l.split_whitespace().nth(1))
                        .and_then(|v| v.parse().ok())
                })
                .unwrap_or(0)
        }

        // Abort with stacks if the soak wedges past its deadline + margin.
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(soak_secs + 900));
            eprintln!(
                "watchdog: soak still running {}s past its deadline; aborting for stacks",
                900
            );
            std::process::abort();
        });

        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "ranged_soak";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 256 * 1024 * 1024,
                db_size: 6 * 1024 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell, Service::RangedIndexer],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();
        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.clone()],
                server_group,
            )
            .await
            .unwrap(),
        );
        client
            .new_schema_with_id(Schema::new_with_id(
                11,
                &String::from("ranged_soak"),
                None,
                Field::new_schema(vec![Field::new_unindexed("data", Type::U8)]),
                false,
                false,
            ))
            .await
            .unwrap()
            .unwrap();
        let meta_plane_client = server.raft_client.plane(crate::server::database_meta_plane_id(
            server_group,
            server_group,
        ));
        let ranged_client = Arc::new(RangedIndexerClient::new_for_database(
            &server.consh,
            &meta_plane_client,
            server_group,
            server_group,
        ));

        const STRIPE: u64 = 1 << 40;
        fn id_of(stripe_base: u64, v: u64) -> Id {
            Id::from_parts(1, stripe_base + v)
        }

        async fn insert_verified(
            rc: &Arc<RangedIndexerClient>,
            id: Id,
            key: &EntryKey,
        ) -> Result<(), String> {
            let mut inserted = false;
            for _round in 0..10 {
                match tokio::time::timeout(Duration::from_secs(30), rc.insert(key)).await {
                    Ok(Ok(_)) => {
                        inserted = true;
                        break;
                    }
                    Ok(Err(e)) => return Err(format!("insert {:?} failed: {:?}", id, e)),
                    Err(_) => {}
                }
            }
            if !inserted {
                return Err(format!("insert {:?}: rpc never answered (10x30s)", id));
            }
            for _attempt in 0..240 {
                match tokio::time::timeout(
                    Duration::from_secs(30),
                    RangedIndexerClient::seek(
                        rc,
                        Range::new_inclusive_opened(key.clone(), Ordering::Forward),
                        1,
                        None,
                    ),
                )
                .await
                {
                    Ok(Ok(Some(cursor))) => match cursor.current_block().first() {
                        Some(first) if *first == id => return Ok(()),
                        Some(first) => {
                            if EntryKey::from_id(first) < *key {
                                return Err(format!(
                                    "verify seek positioned BEFORE its key: sought {:?}, got {:?}",
                                    id, first
                                ));
                            }
                        }
                        None => {}
                    },
                    Ok(Ok(None)) | Ok(Err(_)) | Err(_) => {}
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(format!("insert {:?} never became visible", id))
        }

        async fn delete_verified(
            rc: &Arc<RangedIndexerClient>,
            id: Id,
            key: &EntryKey,
        ) -> Result<(), String> {
            let mut acked = false;
            for round in 0..10 {
                match tokio::time::timeout(Duration::from_secs(30), rc.delete(key)).await {
                    Ok(Ok(existed)) => {
                        // Every victim is a live, single-owner key, so a
                        // first-round "nothing to delete" means the delete's
                        // internal seek MISSED a live key -- the exact shape
                        // that would later read as a "resurrection" (no
                        // tombstone was ever placed; the contains check below
                        // can false-negative on the same misposition and
                        // wave it through). Tripwire it loudly.
                        if !existed && round == 0 {
                            println!("SOAK_DELETE_MISS id={:?}", id);
                        }
                        acked = true;
                        break;
                    }
                    Ok(Err(e)) => return Err(format!("delete {:?} failed: {:?}", id, e)),
                    Err(_) => {}
                }
            }
            if !acked {
                return Err(format!("delete {:?}: rpc never answered (10x30s)", id));
            }
            for _attempt in 0..240 {
                match tokio::time::timeout(Duration::from_secs(30), rc.contains(key)).await {
                    Ok(Ok(false)) => return Ok(()),
                    Ok(Ok(true)) => {}
                    Ok(Err(_)) | Err(_) => {}
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(format!("delete {:?} never became invisible", id))
        }

        // Exact audit of one stripe: the scan must yield the owner's live
        // set precisely, strictly ascending. 10-minute timeout so a wedged
        // scan names itself instead of parking the writer.
        // Err carries (message, offending v) -- the v of a yielded key that
        // should not have been yielded, when there is one, so the caller can
        // probe and classify before judging.
        async fn audit_stripe(
            rc: &Arc<RangedIndexerClient>,
            stripe_base: u64,
            live: &BTreeSet<u64>,
            bound_v: u64,
        ) -> Result<(), (String, Option<u64>)> {
            let audit = async {
                let start_key = EntryKey::from_id(&id_of(stripe_base, 0));
                let mut cursor = match RangedIndexerClient::seek(
                    rc,
                    Range::new_inclusive_opened(start_key, Ordering::Forward),
                    256,
                    None,
                )
                .await
                {
                    Ok(Some(c)) => c,
                    Ok(None) => {
                        return if live.is_empty() {
                            Ok(())
                        } else {
                            Err((
                                format!(
                                    "stripe {:#x}: scan empty but {} keys live",
                                    stripe_base,
                                    live.len()
                                ),
                                None,
                            ))
                        }
                    }
                    Err(e) => {
                        return Err((
                            format!("stripe {:#x}: seek failed: {:?}", stripe_base, e),
                            None,
                        ))
                    }
                };
                let mut expect = live.iter();
                let mut seen = 0u64;
                let mut prev_bits: Option<u64> = None;
                let mut steps = 0u64;
                loop {
                    let Some(cur) = cursor.current().copied() else { break };
                    let bits = cur.bits();
                    let base_bits = id_of(stripe_base, 0).bits();
                    let bound_bits = id_of(stripe_base, bound_v).bits();
                    if bits >= bound_bits || bits < base_bits {
                        break;
                    }
                    if let Some(p) = prev_bits {
                        if bits <= p {
                            return Err((
                                format!(
                                    "stripe {:#x}: scan not ascending: {:#x} after {:#x}",
                                    stripe_base, bits, p
                                ),
                                None,
                            ));
                        }
                    }
                    prev_bits = Some(bits);
                    let v = bits - base_bits;
                    match expect.next() {
                        Some(&want) if want == v => {}
                        Some(&want) => {
                            return Err((
                                format!(
                                    "stripe {:#x}: scan yields v={} but expected v={} (live {})",
                                    stripe_base,
                                    v,
                                    want,
                                    live.len()
                                ),
                                Some(v),
                            ))
                        }
                        None => {
                            return Err((
                                format!(
                                    "stripe {:#x}: scan yields v={} beyond the live set (live {})",
                                    stripe_base,
                                    v,
                                    live.len()
                                ),
                                Some(v),
                            ))
                        }
                    }
                    seen += 1;
                    steps += 1;
                    if steps % 4096 == 0 {
                        tokio::task::yield_now().await;
                    }
                    match cursor.next().await {
                        Ok(_) => {}
                        Err(e) => {
                            return Err((
                                format!(
                                    "stripe {:#x}: cursor.next failed at {} keys: {:?}",
                                    stripe_base, seen, e
                                ),
                                None,
                            ))
                        }
                    }
                }
                if seen != live.len() as u64 {
                    return Err((
                        format!(
                            "stripe {:#x}: scan saw {} keys, {} live",
                            stripe_base,
                            seen,
                            live.len()
                        ),
                        None,
                    ));
                }
                Ok(())
            };
            match tokio::time::timeout(Duration::from_secs(600), audit).await {
                Ok(r) => r,
                Err(_) => Err((
                    format!("stripe {:#x}: audit timed out after 600s", stripe_base),
                    None,
                )),
            }
        }

        // Runs an audit; on failure, diagnoses before judging. A yielded
        // dead key gets an exact `contains` probe and a settle-then-re-audit:
        // a clean second pass means the visibility violation was TRANSIENT
        // (a filter race, not corrupted state) -- counted and logged with
        // everything a hunt needs, and the soak keeps collecting instead of
        // dying at the first occurrence. A second failure, or any structural
        // failure, still panics on the spot. The final assertion fails the
        // soak if ANY transient occurred; continuing is for evidence, not
        // forgiveness.
        async fn audit_with_diagnosis(
            rc: &Arc<RangedIndexerClient>,
            consh: &Arc<bifrost::conshash::ConsistentHashing>,
            group: &str,
            stripe_base: u64,
            live: &BTreeSet<u64>,
            bound_v: u64,
            w: u64,
            t_secs: u64,
            resurrections: &AtomicU64,
        ) {
            let Err((msg, offending)) = audit_stripe(rc, stripe_base, live, bound_v).await else {
                return;
            };
            let contains_now = if let Some(v) = offending {
                let key = EntryKey::from_id(&id_of(stripe_base, v));
                match tokio::time::timeout(Duration::from_secs(30), rc.contains(&key)).await {
                    Ok(Ok(b)) => format!("{}", b),
                    other => format!("probe-failed(timeout={})", other.is_err()),
                }
            } else {
                "n/a".to_string()
            };
            tokio::time::sleep(Duration::from_secs(2)).await;
            match audit_stripe(rc, stripe_base, live, bound_v).await {
                Ok(()) if offending.is_some() => {
                    resurrections.fetch_add(1, AtomicOrdering::Relaxed);
                    println!(
                        "SOAK_RESURRECTION t={}s writer={} TRANSIENT [{}] contains_after={} re-audit=clean",
                        t_secs, w, msg, contains_now
                    );
                }
                Ok(()) => {
                    println!(
                        "SOAK_AUDIT_FLAP t={}s writer={} [{}] re-audit=clean",
                        t_secs, w, msg
                    );
                }
                Err((msg2, _)) => {
                    // Name the holder before dying: ask EVERY tree whether it
                    // answers for the key (epoch u64::MAX bypasses the epoch
                    // gate; the boundary gate stays, which is itself
                    // informative). Two holders = overlapping boundaries with
                    // two physical copies; one holder names the tree whose
                    // birth the split/rollback log can then be searched for.
                    if let Some(v) = offending {
                        let key = EntryKey::from_id(&id_of(stripe_base, v));
                        let mut holders = Vec::new();
                        let mut answered = 0usize;
                        if let Ok(stats) = rc.tree_stats().await {
                            for st in &stats {
                                let Ok(tc) =
                                    crate::index::ranged::tree::service::locate_tree_server_from_conshash(
                                        &st.id, consh, group, group,
                                    )
                                    .await
                                else {
                                    continue;
                                };
                                if let Ok(res) = tc.contains(st.id, key.clone(), u64::MAX).await {
                                    if let crate::index::ranged::tree::service::OpResult::Successful(
                                        held,
                                    ) = res
                                    {
                                        answered += 1;
                                        if held {
                                            holders.push((st.id, format!("{:?}", st.prop)));
                                        }
                                    }
                                }
                            }
                        }
                        println!(
                            "SOAK_HOLDERS t={}s writer={} v={} answered={} holding={} {:#?}",
                            t_secs,
                            w,
                            v,
                            answered,
                            holders.len(),
                            holders
                        );
                    }
                    panic!(
                        "writer {} audit PERSISTENT: first [{}] contains_now={} then [{}]",
                        w, msg, contains_now, msg2
                    );
                }
            }
        }

        let stop = Arc::new(AtomicBool::new(false));
        let inserts = Arc::new(AtomicU64::new(0));
        let deletes = Arc::new(AtomicU64::new(0));
        let scans = Arc::new(AtomicU64::new(0));
        let audits = Arc::new(AtomicU64::new(0));
        let roams = Arc::new(AtomicU64::new(0));
        let resurrections = Arc::new(AtomicU64::new(0));
        let progress: Arc<Vec<AtomicU64>> =
            Arc::new((0..WRITERS).map(|_| AtomicU64::new(0)).collect());

        // Writers.
        let mut writers = JoinSet::new();
        for w in 0..WRITERS {
            let rc = ranged_client.clone();
            let inserts = inserts.clone();
            let deletes = deletes.clone();
            let audits = audits.clone();
            let resurrections = resurrections.clone();
            let progress = progress.clone();
            let consh = server.consh.clone();
            let per_writer_rate =
                (target_inserts / WRITERS / soak_secs.max(1)).max(16) as f64;
            writers.spawn(async move {
                let stripe_base = w * STRIPE;
                let mut live: BTreeSet<u64> = BTreeSet::new();
                let mut next_v: u64 = 0;
                let mut batches: u64 = 0;
                let mut state = 0x9E3779B97F4A7C15u64.wrapping_mul(w + 7);
                while Instant::now() < deadline {
                    let batch_started = Instant::now();
                    // Mostly-ascending with local disorder, like a real
                    // import's arrival order.
                    let mut vals: Vec<u64> = (next_v..next_v + BATCH).collect();
                    for i in (1..vals.len()).rev() {
                        state = state
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        let j = (state >> 16) as usize % (i + 1);
                        vals.swap(i, j);
                    }
                    next_v += BATCH;
                    for v in vals {
                        let id = id_of(stripe_base, v);
                        let key = EntryKey::from_id(&id);
                        if let Err(e) = insert_verified(&rc, id, &key).await {
                            panic!("writer {}: {}", w, e);
                        }
                        live.insert(v);
                    }
                    inserts.fetch_add(BATCH, AtomicOrdering::Relaxed);
                    progress[w as usize].store(next_v, AtomicOrdering::Release);
                    // Delete ~20% of the batch volume from anywhere in the
                    // stripe's history, so tombstones land in old pages too.
                    if live.len() > 2048 {
                        for _ in 0..(BATCH / 5) {
                            state = state
                                .wrapping_mul(6364136223846793005)
                                .wrapping_add(1442695040888963407);
                            let probe = (state >> 8) % next_v;
                            let Some(&victim) =
                                live.range(probe..).next().or_else(|| live.iter().next())
                            else {
                                break;
                            };
                            let id = id_of(stripe_base, victim);
                            let key = EntryKey::from_id(&id);
                            if let Err(e) = delete_verified(&rc, id, &key).await {
                                panic!("writer {}: {}", w, e);
                            }
                            live.remove(&victim);
                            deletes.fetch_add(1, AtomicOrdering::Relaxed);
                        }
                    }
                    batches += 1;
                    if batches % 32 == 0 {
                        audit_with_diagnosis(
                            &rc,
                            &consh,
                            server_group,
                            stripe_base,
                            &live,
                            next_v,
                            w,
                            start.elapsed().as_secs(),
                            &resurrections,
                        )
                        .await;
                        audits.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    // Pace to the target rate; the burst itself ran at full
                    // speed.
                    let target = Duration::from_secs_f64(BATCH as f64 / per_writer_rate);
                    let elapsed = batch_started.elapsed();
                    if elapsed < target {
                        tokio::time::sleep(target - elapsed).await;
                    }
                }
                // Final exact audit before reporting this stripe done.
                audit_with_diagnosis(
                    &rc,
                    &consh,
                    server_group,
                    stripe_base,
                    &live,
                    next_v,
                    w,
                    start.elapsed().as_secs(),
                    &resurrections,
                )
                .await;
                audits.fetch_add(1, AtomicOrdering::Relaxed);
                (w, live.len() as u64, next_v)
            });
        }

        // Scanners: random-position block seeks, monotonicity asserted.
        let mut readers = JoinSet::new();
        for s in 0..SCANNERS {
            let rc = ranged_client.clone();
            let stop = stop.clone();
            let scans = scans.clone();
            let progress = progress.clone();
            readers.spawn(async move {
                let mut state = 0xD1B54A32D192ED03u64.wrapping_mul(s + 3);
                let mut iterations = 0u64;
                while !stop.load(AtomicOrdering::Acquire) {
                    iterations += 1;
                    if iterations % 256 == 0 {
                        tokio::task::yield_now().await;
                    }
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let w = (state >> 33) % WRITERS;
                    let prog = progress[w as usize].load(AtomicOrdering::Acquire);
                    if prog == 0 {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    let v = (state >> 13) % prog;
                    let start_id = id_of(w * STRIPE, v);
                    let start_key = EntryKey::from_id(&start_id);
                    let res = tokio::time::timeout(
                        Duration::from_secs(30),
                        RangedIndexerClient::seek(
                            &rc,
                            Range::new_inclusive_opened(start_key.clone(), Ordering::Forward),
                            256,
                            None,
                        ),
                    )
                    .await;
                    match res {
                        Ok(Ok(Some(cursor))) => {
                            let block: &Vec<Id> = cursor.current_block();
                            if let Some(first) = block.first() {
                                assert!(
                                    EntryKey::from_id(first) >= start_key,
                                    "scan block starts BEFORE its seek key: sought {:?}, got {:?}",
                                    start_id,
                                    first
                                );
                            }
                            for pair in block.windows(2) {
                                assert!(
                                    pair[0].bits() < pair[1].bits(),
                                    "scan block not ascending: {:?} then {:?}",
                                    pair[0],
                                    pair[1]
                                );
                            }
                            scans.fetch_add(1, AtomicOrdering::Relaxed);
                        }
                        Ok(Ok(None)) => {}
                        Ok(Err(_)) | Err(_) => {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    }
                }
            });
        }

        // Roamer: the whole index end to end, global order asserted.
        {
            let rc = ranged_client.clone();
            let stop = stop.clone();
            let roams = roams.clone();
            readers.spawn(async move {
                while !stop.load(AtomicOrdering::Acquire) {
                    tokio::time::sleep(Duration::from_secs(300)).await;
                    if stop.load(AtomicOrdering::Acquire) {
                        break;
                    }
                    let roam_started = Instant::now();
                    let start_key = EntryKey::from_id(&Id::from_parts(1, 0));
                    let mut cursor = match RangedIndexerClient::seek(
                        &rc,
                        Range::new_inclusive_opened(start_key, Ordering::Forward),
                        256,
                        None,
                    )
                    .await
                    {
                        Ok(Some(c)) => c,
                        _ => continue,
                    };
                    let mut prev: Option<u64> = None;
                    let mut count = 0u64;
                    loop {
                        let Some(cur) = cursor.current().copied() else { break };
                        let bits = cur.bits();
                        if let Some(p) = prev {
                            assert!(
                                bits > p,
                                "ROAM: global scan not ascending: {:#x} after {:#x} at key {}",
                                bits,
                                p,
                                count
                            );
                        }
                        prev = Some(bits);
                        count += 1;
                        if count % 4096 == 0 {
                            tokio::task::yield_now().await;
                        }
                        if cursor.next().await.is_err() {
                            break;
                        }
                    }
                    roams.fetch_add(1, AtomicOrdering::Relaxed);
                    println!(
                        "SOAK_ROAM t={}s keys={} took={:.1}s",
                        start.elapsed().as_secs(),
                        count,
                        roam_started.elapsed().as_secs_f64()
                    );
                }
            });
        }

        // Minute reporter.
        {
            let rc = ranged_client.clone();
            let stop = stop.clone();
            let inserts = inserts.clone();
            let deletes = deletes.clone();
            let scans = scans.clone();
            let audits = audits.clone();
            readers.spawn(async move {
                let mut minutes = 0u64;
                while !stop.load(AtomicOrdering::Acquire) {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    minutes += 1;
                    let trees = match tokio::time::timeout(
                        Duration::from_secs(30),
                        rc.tree_stats(),
                    )
                    .await
                    {
                        Ok(Ok(s)) => s.len() as i64,
                        _ => -1,
                    };
                    // The single-copy invariant, checked from the index side:
                    // no scan can see a duplicate (cursors dedup ids), so this
                    // raw audit is the only observation of it. Every 10
                    // minutes: a raw walk of every tree is ~20M key
                    // materializations at soak scale, which would otherwise
                    // tax the very timings the soak is measuring.
                    if minutes % 10 == 0 {
                    let (audit_keys, audit_dups, audit_tombs) =
                        match tokio::time::timeout(Duration::from_secs(60), rc.audit_trees()).await
                        {
                            Ok(Ok(v)) => v.iter().fold((0u64, 0u64, 0u64), |acc, a| {
                                (
                                    acc.0 + a.keys,
                                    acc.1 + a.duplicates,
                                    acc.2 + a.tombstoned_present,
                                )
                            }),
                            _ => (0, 0, 0),
                        };
                    assert_eq!(
                        audit_dups, 0,
                        "RAW AUDIT: {} duplicate key copies across the index -- the \
                         single-copy invariant is broken",
                        audit_dups
                    );
                    println!(
                        "SOAK_AUDIT_RAW t={}s keys={} duplicates={} tombstoned_present={} scan_dedup_drops={}",
                        start.elapsed().as_secs(),
                        audit_keys,
                        audit_dups,
                        audit_tombs,
                        crate::index::ranged::client::cursor::scan_dedup_drops()
                    );
                    }
                    let line = format!(
                        "t={}s inserts={} deletes={} scans={} audits={} trees={} rss_mb={} threads={}",
                        start.elapsed().as_secs(),
                        inserts.load(AtomicOrdering::Relaxed),
                        deletes.load(AtomicOrdering::Relaxed),
                        scans.load(AtomicOrdering::Relaxed),
                        audits.load(AtomicOrdering::Relaxed),
                        trees,
                        proc_status("VmRSS:") / 1024,
                        proc_status("Threads:")
                    );
                    if minutes % 60 == 0 {
                        println!("SOAK_HOUR {}", line);
                    } else {
                        println!("SOAK_MIN {}", line);
                    }
                }
            });
        }

        // Run to the deadline.
        let mut stripe_reports = Vec::new();
        while let Some(res) = writers.join_next().await {
            stripe_reports.push(res.unwrap());
        }
        stop.store(true, AtomicOrdering::Release);
        while let Some(res) = readers.join_next().await {
            res.unwrap();
        }

        let trees = ranged_client.tree_stats().await.map(|s| s.len()).unwrap_or(0);
        let total_live: u64 = stripe_reports.iter().map(|(_, l, _)| l).sum();
        println!(
            "SOAK_DONE t={}s inserts={} deletes={} live={} scans={} audits={} roams={} trees={} resurrections={}",
            start.elapsed().as_secs(),
            inserts.load(AtomicOrdering::Relaxed),
            deletes.load(AtomicOrdering::Relaxed),
            total_live,
            scans.load(AtomicOrdering::Relaxed),
            audits.load(AtomicOrdering::Relaxed),
            roams.load(AtomicOrdering::Relaxed),
            trees,
            resurrections.load(AtomicOrdering::Relaxed)
        );
        assert_eq!(
            resurrections.load(AtomicOrdering::Relaxed),
            0,
            "transient resurrections were observed; the SOAK_RESURRECTION lines carry the evidence"
        );
        // This workload deletes a key and never inserts it again, so the
        // only in-tree path that can undo a delete must never have run.
        assert_eq!(
            crate::index::ranged::tree::tree::undeletes(),
            0,
            "an insert consumed a tombstone, but this workload never re-inserts a deleted key"
        );
        assert!(
            trees > 4,
            "soak ended with only {} tree(s); structural splits were not exercised",
            trees
        );
        assert!(
            audits.load(AtomicOrdering::Relaxed) >= WRITERS,
            "soak ended with fewer audits than writers; the exact checks never ran"
        );

        server.shutdown().await;
    }

    #[ignore = "stress test"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_cell_query_concurrent_writes_eventually_scan_all() {
        use crate::index::builder::IndexBuilder;
        use crate::query::data_client::QueryOrdering;
        use crate::ram::cell::OwnedCell;
        use crate::ram::schema::SchemaUid;
        use crate::ram::schema::{Field, IndexType, Schema};
        use crate::ram::types::Type;
        use crate::server::{NebServer, ServerOptions, Service};
        use dovahkiin::{
            expr::serde::Expr,
            types::{Map, OwnedMap, OwnedValue},
        };
        use std::collections::HashSet;
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        use tokio::task::JoinSet;

        const SCORE_FIELD: &'static str = "score";
        const PAYLOAD_FIELD: &'static str = "payload";

        let _ = env_logger::try_init();

        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "split_cell_query_stress";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 128 * 1024 * 1024,
                db_size: 128 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: true,
                services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

        let schema = Schema::new_with_id(
            302,
            "split_cell_query_stress",
            None,
            Field::new_schema(vec![
                Field::new_indexed(SCORE_FIELD, Type::U64, vec![IndexType::Ranged]),
                Field::new_unindexed(PAYLOAD_FIELD, Type::U32),
            ]),
            false,
            true,
        );

        let client = Arc::new(
            server
                .data_client(&vec![server_addr.clone()])
                .await
                .unwrap(),
        );
        client.new_schema_with_id(schema).await.unwrap().unwrap();

        let workers = 16usize;
        let writes_per_worker = 2048usize;
        let expected_total = workers * writes_per_worker;
        let mut writers = JoinSet::new();
        for worker in 0..workers {
            let client = client.clone();
            writers.spawn(async move {
                let worker_base = worker * writes_per_worker;
                for offset in 0..writes_per_worker {
                    let ordinal = worker_base + offset;
                    let mut value = OwnedValue::Map(OwnedMap::new());
                    value[SCORE_FIELD] = OwnedValue::U64(ordinal as u64);
                    value[PAYLOAD_FIELD] = OwnedValue::U32(worker as u32);
                    let cell = OwnedCell::new_with_id(
                        crate::ram::schema::SchemaVid(302),
                        &Id::from_parts(1, ordinal as u64),
                        value,
                    );
                    client.upsert_cell(cell).await.unwrap().unwrap();
                    if offset % 128 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            });
        }
        while let Some(result) = writers.join_next().await {
            result.unwrap();
        }

        let _ = IndexBuilder::await_all_indices().await;

        let ranged_client = server
            .database_runtime
            .indexer()
            .expect("indexer should be enabled")
            .clients
            .ranged_client
            .clone();
        assert!(
            !ranged_client.tree_stats().await.unwrap().is_empty(),
            "expected ranged tree stats after cell/query stress"
        );

        let idx_client = server.indexed_data_client();
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let mut cursor = idx_client
                .scan_all(
                    SchemaUid(302),
                    vec![],
                    Expr::nothing(),
                    Expr::nothing(),
                    QueryOrdering::Asc,
                )
                .await
                .unwrap();
            let mut seen_ids = HashSet::with_capacity(expected_total);
            while let Ok(Some(cell)) = cursor.next().await {
                seen_ids.insert(cell.id());
            }
            if seen_ids.len() == expected_total {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "scan_all did not converge after concurrent indexed writes: expected {}, observed {}",
                expected_total,
                seen_ids.len()
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        server.shutdown().await;
    }
}
