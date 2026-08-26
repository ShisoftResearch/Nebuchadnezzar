//! Focused tests for InvertedIndexer lock-based atomic operations and concurrent behavior
//!
//! These tests focus on the atomic operations provided by cell-level locking:
//! - `lock_or_insert_cell`: Atomically locks a cell or inserts if it doesn't exist
//! - `upsert_cell` (on CellGuard): Updates cell while holding the lock
//!
//! Unlike HashIndexer which uses explicit compare-and-swap with retries,
//! InvertedIndexer uses lock-based atomicity for posting list management.

#[cfg(test)]
mod tests {
    use crate::client::AsyncClient;
    use crate::index::full_text::shard::{inverted_segment_schema, InvertedIndexer};
    use crate::index::full_text::{
        build_index_meta, inverted_stats_schema, FullTextIndexMeta, TokenStat,
    };
    use crate::ram::cell::OwnedCell;
    use crate::ram::chunk::Chunks;
    use crate::ram::schema::LocalSchemasCache;
    use crate::ram::schema::SchemaUid;
    use crate::ram::types::{Id, OwnedValue};
    use crate::server::ServerMeta;
    use bifrost_hasher::hash_str;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::task::JoinSet;

    /// Helper to create test chunks
    fn create_test_chunks() -> Arc<Chunks> {
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(inverted_segment_schema());
        schemas.debug_only_new_schema(inverted_stats_schema());

        Chunks::new(
            1,
            128 * 1024 * 1024,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        )
    }

    /// Helper to create a mock AsyncClient (dummy for tests that don't need it)
    async fn create_mock_client() -> Arc<AsyncClient> {
        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server = crate::server::NebServer::new_from_opts(
            &crate::server::ServerOptions {
                chunk_size: 128 * 1024 * 1024,
                db_size: 128 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![crate::server::Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            "mock_test",
            async |_| {},
        )
        .await
        .unwrap();

        Arc::new(
            AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.clone()],
                "mock_test",
            )
            .await
            .unwrap(),
        )
    }

    /// Test: Concurrent appends to the same posting list (same term)
    /// This tests lock_or_insert_cell atomicity - all concurrent appends should succeed
    /// even when multiple threads try to append to the same term simultaneously.
    #[tokio::test]
    async fn test_cas_concurrent_append_same_term() {
        let _ = env_logger::try_init();
        let chunks = create_test_chunks();
        let client = create_mock_client().await;
        let indexer = InvertedIndexer::new(1, chunks.clone(), client, Duration::from_secs(30));

        let schema_id = 100u32;
        let field_id = hash_str("content");
        let term_hash = hash_str("concurrent");

        // Create 20 documents with the same term
        let mut handles = JoinSet::new();
        let indexer_arc = Arc::new(indexer);

        for i in 0..20 {
            let indexer_clone = indexer_arc.clone();
            let doc_id = Id::from_parts(i, i);

            handles.spawn(async move {
                let meta = FullTextIndexMeta {
                    cell_id: doc_id,
                    version: 1,
                    schema_id: SchemaUid(schema_id),
                    field_id,
                    doc_length: 5,
                    tokens: vec![TokenStat {
                        term_hash,
                        term_freq: 1,
                    }],
                };

                indexer_clone.add_document(&meta)
            });
        }

        // Collect results
        let mut success_count = 0;
        while let Some(result) = handles.join_next().await {
            if result.unwrap().is_ok() {
                success_count += 1;
            }
        }

        assert_eq!(success_count, 20, "All concurrent appends should succeed");

        // Verify all postings are present
        let postings = indexer_arc.get_term_postings(SchemaUid(schema_id), field_id, term_hash);
        assert_eq!(
            postings.len(),
            20,
            "Should have 20 postings after concurrent appends"
        );

        // Verify all doc_ids are unique
        let unique_ids: HashSet<Id> = postings.iter().map(|(id, _, _)| *id).collect();
        assert_eq!(unique_ids.len(), 20, "All doc_ids should be unique");
    }

    /// Test: Concurrent appends causing segment overflow
    /// Tests the prepend logic when head segment becomes full under concurrent load
    #[tokio::test]
    async fn test_cas_segment_overflow_concurrent() {
        let _ = env_logger::try_init();
        let chunks = create_test_chunks();
        let client = create_mock_client().await;
        let indexer = InvertedIndexer::new(1, chunks.clone(), client, Duration::from_secs(30));

        let schema_id = 101u32;
        let field_id = hash_str("content");
        let term_hash = hash_str("overflow");

        // Add enough documents to cause segment overflow (>1000 entries)
        let num_docs = 1200;
        let mut handles = JoinSet::new();
        let indexer_arc = Arc::new(indexer);

        for i in 0..num_docs {
            let indexer_clone = indexer_arc.clone();
            let doc_id = Id::from_parts(i, i);

            handles.spawn(async move {
                let meta = FullTextIndexMeta {
                    cell_id: doc_id,
                    version: 1,
                    schema_id: SchemaUid(schema_id),
                    field_id,
                    doc_length: 5,
                    tokens: vec![TokenStat {
                        term_hash,
                        term_freq: 1,
                    }],
                };

                indexer_clone.add_document(&meta)
            });
        }

        // All should succeed despite segment overflow
        let mut success_count = 0;
        while let Some(result) = handles.join_next().await {
            if result.unwrap().is_ok() {
                success_count += 1;
            }
        }

        assert_eq!(
            success_count, num_docs,
            "All appends should succeed despite segment overflow"
        );

        // Verify all postings are retrievable
        let postings = indexer_arc.get_term_postings(SchemaUid(schema_id), field_id, term_hash);
        assert_eq!(
            postings.len(),
            num_docs as usize,
            "Should have all postings after overflow"
        );
    }

    /// Test: Version preservation in posting entries
    /// Verifies that cell versions are correctly stored with each posting entry
    #[tokio::test]
    async fn test_cas_version_preservation() {
        let _ = env_logger::try_init();
        let chunks = create_test_chunks();
        let client = create_mock_client().await;
        let indexer = InvertedIndexer::new(1, chunks.clone(), client, Duration::from_secs(30));

        let schema_id = 102u32;
        let field_id = hash_str("content");
        let term_hash = hash_str("versioned");

        // Add documents with different versions
        let versions = vec![1u64, 5u64, 10u64, 15u64, 20u64];
        for (i, &version) in versions.iter().enumerate() {
            let doc_id = Id::from_parts(i as u64, i as u64);
            let meta = FullTextIndexMeta {
                cell_id: doc_id,
                version,
                schema_id: SchemaUid(schema_id),
                field_id,
                doc_length: 5,
                tokens: vec![TokenStat {
                    term_hash,
                    term_freq: 1,
                }],
            };

            indexer.add_document(&meta).unwrap();
        }

        // Verify versions are stored correctly
        let postings =
            indexer.get_term_postings_with_version(SchemaUid(schema_id), field_id, term_hash);
        assert_eq!(postings.len(), 5, "Should have 5 postings");

        // Check that versions match
        let stored_versions: HashSet<u64> = postings.iter().map(|(_, v, _, _)| *v).collect();
        let expected_versions: HashSet<u64> = versions.into_iter().collect();
        assert_eq!(
            stored_versions, expected_versions,
            "Stored versions should match input versions"
        );
    }

    /// Test: Concurrent stats updates
    /// Tests that field stats are correctly updated under concurrent document additions
    #[tokio::test]
    async fn test_cas_concurrent_stats_updates() {
        let _ = env_logger::try_init();
        let chunks = create_test_chunks();
        let client = create_mock_client().await;
        let indexer = Arc::new(InvertedIndexer::new(
            1,
            chunks.clone(),
            client,
            Duration::from_secs(30),
        ));

        let schema_id = 103u32;
        let field_id = hash_str("content");

        // Concurrently add documents and update stats
        let mut handles = JoinSet::new();
        let num_docs = 50;

        for i in 0..num_docs {
            let indexer_clone = indexer.clone();
            let doc_id = Id::from_parts(i, i);

            handles.spawn(async move {
                let meta = FullTextIndexMeta {
                    cell_id: doc_id,
                    version: 1,
                    schema_id: SchemaUid(schema_id),
                    field_id,
                    doc_length: 10,
                    tokens: vec![
                        TokenStat {
                            term_hash: hash_str("term1"),
                            term_freq: 2,
                        },
                        TokenStat {
                            term_hash: hash_str("term2"),
                            term_freq: 3,
                        },
                    ],
                };

                indexer_clone.add_document(&meta)?;
                indexer_clone.update_stats_for_add(&meta);
                Ok::<_, crate::index::builder::IndexError>(())
            });
        }

        // Wait for all operations
        while let Some(result) = handles.join_next().await {
            assert!(result.unwrap().is_ok(), "All stats updates should succeed");
        }

        // Verify final stats
        let stats = indexer.get_field_stats(SchemaUid(schema_id), field_id);
        assert_eq!(
            stats.doc_count, num_docs,
            "Doc count should match number of added documents"
        );
        assert_eq!(
            stats.total_length,
            num_docs * 10,
            "Total length should be sum of all doc lengths"
        );
    }

    /// Test: Update existing document (tests version increment)
    /// Verifies that updating a document creates a new posting with new version
    #[tokio::test]
    async fn test_cas_document_update_version_increment() {
        let _ = env_logger::try_init();
        let chunks = create_test_chunks();
        let client = create_mock_client().await;
        let indexer = InvertedIndexer::new(1, chunks.clone(), client, Duration::from_secs(30));

        let schema_id = 104u32;
        let field_id = hash_str("content");
        let doc_id = Id::from_parts(1, 1);
        let term_hash = hash_str("updated");

        // Add document with version 1
        let meta_v1 = FullTextIndexMeta {
            cell_id: doc_id,
            version: 1,
            schema_id: SchemaUid(schema_id),
            field_id,
            doc_length: 5,
            tokens: vec![TokenStat {
                term_hash,
                term_freq: 1,
            }],
        };
        indexer.add_document(&meta_v1).unwrap();
        indexer.update_stats_for_add(&meta_v1);

        // Update document with version 2
        let meta_v2 = FullTextIndexMeta {
            cell_id: doc_id,
            version: 2,
            schema_id: SchemaUid(schema_id),
            field_id,
            doc_length: 8,
            tokens: vec![TokenStat {
                term_hash,
                term_freq: 2,
            }],
        };
        indexer.add_document(&meta_v2).unwrap();
        indexer.update_stats_for_add(&meta_v2);

        // Verify both versions are in posting list (append-only)
        let postings =
            indexer.get_term_postings_with_version(SchemaUid(schema_id), field_id, term_hash);
        assert_eq!(
            postings.len(),
            2,
            "Should have 2 entries (append-only for same doc)"
        );

        // Verify versions
        let versions: Vec<u64> = postings.iter().map(|(_, v, _, _)| *v).collect();
        assert!(
            versions.contains(&1) && versions.contains(&2),
            "Should have both version 1 and version 2"
        );

        // Verify stats reflect update (not double-counting)
        let stats = indexer.get_field_stats(SchemaUid(schema_id), field_id);
        assert_eq!(stats.doc_count, 1, "Should have 1 document (updated)");
        assert_eq!(stats.total_length, 8, "Should have updated length");
    }

    /// Test: Concurrent mixed operations (add + remove)
    /// Tests concurrent document additions and removals on the same field
    #[tokio::test]
    async fn test_cas_concurrent_mixed_operations() {
        let _ = env_logger::try_init();
        let chunks = create_test_chunks();
        let client = create_mock_client().await;
        let indexer = Arc::new(InvertedIndexer::new(
            1,
            chunks.clone(),
            client,
            Duration::from_secs(30),
        ));

        let schema_id = 105u32;
        let field_id = hash_str("content");
        let term_hash = hash_str("mixed");

        // Phase 1: Add 30 documents
        let mut handles = JoinSet::new();
        for i in 0..30 {
            let indexer_clone = indexer.clone();
            let doc_id = Id::from_parts(i, i);

            handles.spawn(async move {
                let meta = FullTextIndexMeta {
                    cell_id: doc_id,
                    version: 1,
                    schema_id: SchemaUid(schema_id),
                    field_id,
                    doc_length: 5,
                    tokens: vec![TokenStat {
                        term_hash,
                        term_freq: 1,
                    }],
                };

                indexer_clone.add_document(&meta)?;
                indexer_clone.update_stats_for_add(&meta);
                Ok::<_, crate::index::builder::IndexError>(())
            });
        }

        while let Some(result) = handles.join_next().await {
            assert!(result.unwrap().is_ok());
        }

        // Phase 2: Concurrently remove 10 and add 10 new ones
        let mut handles = JoinSet::new();

        // Remove first 10
        for i in 0..10 {
            let indexer_clone = indexer.clone();
            let doc_id = Id::from_parts(i, i);

            handles.spawn(async move {
                let meta = FullTextIndexMeta {
                    cell_id: doc_id,
                    version: 1,
                    schema_id: SchemaUid(schema_id),
                    field_id,
                    doc_length: 5,
                    tokens: vec![TokenStat {
                        term_hash,
                        term_freq: 1,
                    }],
                };

                indexer_clone.remove_document(&meta)?;
                Ok::<_, crate::index::builder::IndexError>(())
            });
        }

        // Add 10 new ones
        for i in 30..40 {
            let indexer_clone = indexer.clone();
            let doc_id = Id::from_parts(i, i);

            handles.spawn(async move {
                let meta = FullTextIndexMeta {
                    cell_id: doc_id,
                    version: 1,
                    schema_id: SchemaUid(schema_id),
                    field_id,
                    doc_length: 5,
                    tokens: vec![TokenStat {
                        term_hash,
                        term_freq: 1,
                    }],
                };

                indexer_clone.add_document(&meta)?;
                indexer_clone.update_stats_for_add(&meta);
                Ok::<_, crate::index::builder::IndexError>(())
            });
        }

        while let Some(result) = handles.join_next().await {
            assert!(result.unwrap().is_ok());
        }

        // Verify final stats: 30 - 10 + 10 = 30 documents
        let stats = indexer.get_field_stats(SchemaUid(schema_id), field_id);
        assert_eq!(
            stats.doc_count, 30,
            "Should have 30 documents after mixed operations"
        );
    }

    /// Test: Stress test with high contention on single term
    /// Creates extreme contention on a single posting list to test CAS behavior
    #[tokio::test]
    async fn test_cas_high_contention_stress() {
        let _ = env_logger::try_init();
        let chunks = create_test_chunks();
        let client = create_mock_client().await;
        let indexer = Arc::new(InvertedIndexer::new(
            1,
            chunks.clone(),
            client,
            Duration::from_secs(30),
        ));

        let schema_id = 106u32;
        let field_id = hash_str("content");
        let term_hash = hash_str("stress");

        // Create 100 concurrent appends to the same term
        let mut handles = JoinSet::new();
        let num_ops = 100;

        for i in 0..num_ops {
            let indexer_clone = indexer.clone();
            let doc_id = Id::from_parts(i, i);

            handles.spawn(async move {
                let meta = FullTextIndexMeta {
                    cell_id: doc_id,
                    version: 1,
                    schema_id: SchemaUid(schema_id),
                    field_id,
                    doc_length: 3,
                    tokens: vec![TokenStat {
                        term_hash,
                        term_freq: 1,
                    }],
                };

                // Rapid-fire operations
                for _ in 0..5 {
                    indexer_clone.add_document(&meta)?;
                }
                Ok::<_, crate::index::builder::IndexError>(())
            });
        }

        let mut success_count = 0;
        while let Some(result) = handles.join_next().await {
            if result.unwrap().is_ok() {
                success_count += 1;
            }
        }

        // All operations should complete successfully despite high contention
        assert_eq!(
            success_count, num_ops,
            "All operations should succeed under high contention"
        );

        // Verify posting list has all entries (100 docs × 5 operations each = 500 total)
        let postings = indexer.get_term_postings(SchemaUid(schema_id), field_id, term_hash);
        assert_eq!(
            postings.len(),
            (num_ops * 5) as usize,
            "Should have all posting entries"
        );
    }

    /// Test: Idempotent stats updates
    /// Verifies that updating the same document doesn't double-count in stats
    #[tokio::test]
    async fn test_cas_idempotent_stats_update() {
        let _ = env_logger::try_init();
        let chunks = create_test_chunks();
        let client = create_mock_client().await;
        let indexer = InvertedIndexer::new(1, chunks.clone(), client, Duration::from_secs(30));

        let schema_id = 107u32;
        let field_id = hash_str("content");
        let doc_id = Id::from_parts(1, 1);

        // Add document
        let meta1 = FullTextIndexMeta {
            cell_id: doc_id,
            version: 1,
            schema_id: SchemaUid(schema_id),
            field_id,
            doc_length: 10,
            tokens: vec![TokenStat {
                term_hash: hash_str("test"),
                term_freq: 1,
            }],
        };
        indexer.add_document(&meta1).unwrap();
        indexer.update_stats_for_add(&meta1);

        let stats1 = indexer.get_field_stats(SchemaUid(schema_id), field_id);
        assert_eq!(stats1.doc_count, 1);
        assert_eq!(stats1.total_length, 10);

        // Update same document with different length
        let meta2 = FullTextIndexMeta {
            cell_id: doc_id,
            version: 2,
            schema_id: SchemaUid(schema_id),
            field_id,
            doc_length: 15,
            tokens: vec![TokenStat {
                term_hash: hash_str("test"),
                term_freq: 2,
            }],
        };
        indexer.add_document(&meta2).unwrap();
        indexer.update_stats_for_add(&meta2);

        let stats2 = indexer.get_field_stats(SchemaUid(schema_id), field_id);
        assert_eq!(stats2.doc_count, 1, "Doc count should still be 1");
        assert_eq!(
            stats2.total_length, 15,
            "Total length should be updated, not added"
        );
    }
}
