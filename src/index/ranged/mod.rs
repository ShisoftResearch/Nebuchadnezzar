pub mod client;
pub mod sm;
pub mod tree;
pub mod trees;

#[cfg(test)]
mod tests {
    use super::tree::btree::storage;
    use super::tree::btree::Ordering;
    use super::*;
    use crate::client::*;
    use crate::index::ranged::tree::btree;
    use crate::index::ranged::trees::Range;
    use crate::index::EntryKey;
    use crate::ram::schema::*;
    use crate::ram::types::Id;
    use crate::server::*;
    use dovahkiin::types::Type;
    use futures::stream::FuturesUnordered;
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::time::timeout;
    use tokio_stream::StreamExt;

    async fn run_insert_pressure_test(server_group: &str, server_addr: &str, multiplier: usize) {
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 32 * 1024 * 1024 * 1024,
                db_size: 32 * 1024 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell, Service::RangedIndexer],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            server_addr,
            server_group,
            async |_| {},
        )
        .await
        .unwrap();
        let client = Arc::new(
            AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.to_string()],
                server_group,
            )
            .await
            .unwrap(),
        );
        let meta_plane_client = server
            .raft_client
            .plane(crate::server::database_meta_plane_id(
                server_group,
                server_group,
            ));
        let index_client = Arc::new(client::RangedIndexerClient::new_for_database(
            &server.consh,
            &meta_plane_client,
            server_group,
            server_group,
        ));
        client.new_schema_with_id(schema()).await.unwrap().unwrap();

        // This test derives its key count from the migration threshold and
        // needs migration to trigger at a small scale; pin depth to 2.
        btree::set_tree_depth(2);
        let test_capacity =
            btree::ideal_capacity_from_node_size(btree::level::BTREE_NODE_SIZE) * multiplier;
        let mut futs = FuturesUnordered::new();
        let mut rng = rand::thread_rng();
        let nums = (0..test_capacity).collect_vec();
        let mut shuffled_inserts = nums.clone();
        let mut shuffled_checks = nums.clone();
        shuffled_inserts.as_mut_slice().shuffle(&mut rng);
        shuffled_checks.as_mut_slice().shuffle(&mut rng);

        for i in shuffled_inserts {
            let index_client = index_client.clone();
            futs.push(tokio::time::timeout(Duration::from_secs(240), async move {
                let id = Id::from_parts(1, i as u64);
                let key = EntryKey::from_id(&id);
                index_client.insert(&key).await
            }));
        }
        while let Some(result) = futs.next().await {
            assert!(result.unwrap().unwrap(), "Insertion return false");
        }

        let mut futs = FuturesUnordered::new();
        for (i, num) in shuffled_checks.into_iter().enumerate() {
            let index_client = index_client.clone();
            futs.push(tokio::spawn(async move {
                let id = Id::from_parts(1, num as u64);
                let key = EntryKey::from_id(&id);
                let rt_cursor = client::RangedIndexerClient::seek(
                    &index_client,
                    Range::new_inclusive_opened(key, Ordering::Forward),
                    1,
                    None,
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(&id, rt_cursor.current().unwrap(), "at {}", i);
            }));
        }
        while futs.next().await.is_some() {}

        tokio::time::sleep(Duration::from_secs(5)).await;
        storage::wait_until_updated().await;

        let start_id = Id::from_parts(1, 0);
        let mut rt_cursor = client::RangedIndexerClient::seek(
            &index_client,
            Range::new_inclusive_opened(EntryKey::from_id(&start_id), Ordering::Forward),
            128,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(rt_cursor.current(), Some(&start_id));
        for (i, num) in nums.iter().enumerate() {
            let id = Id::from_parts(1, *num as u64);
            let current = rt_cursor.current().expect(&format!("Checking {}", num));
            assert_eq!(
                &id, current,
                "Expecting {:?}, got {:?}, list index {}, cursor ids {:?}, cursor pos {:?}",
                id, current, i, rt_cursor.ids, rt_cursor.pos
            );
            let _ = rt_cursor.next().await.unwrap();
        }
        assert!(
            rt_cursor.next().await.unwrap().is_none(),
            "Expected scan to finish exactly at end of list"
        );
    }

    async fn run_timed_insert_soak_test(
        server_group: &str,
        server_addr: &str,
        workers: usize,
        duration: Duration,
    ) {
        // Sized for large machines: at ~200 cores the 30-minute soak inserts
        // on the order of a billion keys; a 32GB chunk fills with page cells
        // and migrations retry forever against the full chunk.
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 256 * 1024 * 1024 * 1024,
                db_size: 256 * 1024 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell, Service::RangedIndexer],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            server_addr,
            server_group,
            async |_| {},
        )
        .await
        .unwrap();
        let client = Arc::new(
            AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.to_string()],
                server_group,
            )
            .await
            .unwrap(),
        );
        let meta_plane_client = server
            .raft_client
            .plane(crate::server::database_meta_plane_id(
                server_group,
                server_group,
            ));
        let index_client = Arc::new(client::RangedIndexerClient::new_for_database(
            &server.consh,
            &meta_plane_client,
            server_group,
            server_group,
        ));
        client.new_schema_with_id(schema()).await.unwrap().unwrap();

        let deadline = Instant::now() + duration;
        let total_inserted = Arc::new(AtomicU64::new(0));
        let last_successful_seq = Arc::new(
            (0..workers)
                .map(|_| AtomicU64::new(u64::MAX))
                .collect::<Vec<_>>(),
        );
        let mut futs = FuturesUnordered::new();

        for worker in 0..workers {
            let index_client = index_client.clone();
            let total_inserted = total_inserted.clone();
            let last_successful_seq = last_successful_seq.clone();
            futs.push(tokio::spawn(async move {
                let mut seq = 0u64;
                let per_insert_timeout = Duration::from_secs(10);
                while Instant::now() < deadline {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let id = Id::from_parts((worker as u64) + 1, seq);
                    let key = EntryKey::from_id(&id);
                    let inserted =
                        timeout(per_insert_timeout.min(remaining), index_client.insert(&key))
                            .await
                            .unwrap_or_else(|_| {
                                panic!(
                                    "insert timed out for worker={}, seq={}, timeout={:?}",
                                    worker,
                                    seq,
                                    per_insert_timeout.min(remaining)
                                )
                            })
                            .unwrap();
                    assert!(
                        inserted,
                        "insert returned false for worker={}, seq={}",
                        worker, seq
                    );
                    last_successful_seq[worker].store(seq, AtomicOrdering::Release);
                    total_inserted.fetch_add(1, AtomicOrdering::Relaxed);
                    seq += 1;
                }
            }));
        }

        while let Some(result) = futs.next().await {
            result.unwrap();
        }

        let inserted = total_inserted.load(AtomicOrdering::Acquire);
        assert!(inserted > 0, "soak test did not insert any keys");

        storage::wait_until_updated().await;

        for worker in 0..workers {
            let last_seq = last_successful_seq[worker].load(AtomicOrdering::Acquire);
            assert_ne!(last_seq, u64::MAX, "worker {} inserted no keys", worker);
            let id = Id::from_parts((worker as u64) + 1, last_seq);
            let key = EntryKey::from_id(&id);
            let cursor = client::RangedIndexerClient::seek(
                &index_client,
                Range::new_inclusive_opened(key, Ordering::Forward),
                1,
                None,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                cursor.current(),
                Some(&id),
                "failed to read back worker {} last inserted key {:?}",
                worker,
                id
            );
        }

        assert!(
            !index_client.tree_stats().await.unwrap().is_empty(),
            "expected ranged tree stats after soak test"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn general() {
        let _ = env_logger::try_init();
        run_insert_pressure_test(
            "ranged_index_test",
            &crate::utils::test_port::unique_localhost_addr(),
            3,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migration_stress_insert_only() {
        let _ = env_logger::try_init();
        run_insert_pressure_test(
            "ranged_index_migration_stress",
            &crate::utils::test_port::unique_localhost_addr(),
            6,
        )
        .await;
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 64)]
    async fn soak_migration_stress_30m_64_threads() {
        let _ = env_logger::try_init();
        run_timed_insert_soak_test(
            "ranged_index_migration_soak_30m",
            &crate::utils::test_port::unique_localhost_addr(),
            64,
            Duration::from_secs(30 * 60),
        )
        .await;
    }

    /// A field entry: schema 11, field 1, the value as the feature, one id.
    fn field_entry(value: u64, id: &Id) -> EntryKey {
        let mut key =
            EntryKey::for_schema_field_feature(SchemaUid(11), 1, &value.to_be_bytes());
        key.set_id(id);
        key
    }

    async fn collect(mut cursor: client::cursor::ClientCursor) -> Vec<Id> {
        let mut ids = Vec::new();
        while let Some(id) = cursor.next().await.unwrap() {
            ids.push(id);
        }
        ids
    }

    /// An equality scan on one field value used to come back with the first
    /// block of every tree after the one holding the value: the client
    /// cursor refilled from the next tree with an unbounded range once the
    /// value's own entries ran out. Here 64 values of one field are followed
    /// by enough entries of a later schema to split the index into several
    /// trees, and every scan must return its value's ids and nothing else.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_scan_ends_at_its_range_even_when_trees_follow() {
        let _ = env_logger::try_init();
        let server_group = "ranged_index_range_end_test";
        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 32 * 1024 * 1024 * 1024,
                db_size: 32 * 1024 * 1024 * 1024,
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
            server_group,
            async |_| {},
        )
        .await
        .unwrap();
        let client = Arc::new(
            AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr.to_string()],
                server_group,
            )
            .await
            .unwrap(),
        );
        let meta_plane_client = server
            .raft_client
            .plane(crate::server::database_meta_plane_id(
                server_group,
                server_group,
            ));
        let index_client = Arc::new(client::RangedIndexerClient::new_for_database(
            &server.consh,
            &meta_plane_client,
            server_group,
            server_group,
        ));
        client.new_schema_with_id(schema()).await.unwrap().unwrap();

        // Small trees, so the filler splits the index into several of them.
        btree::set_tree_depth(2);
        let filler = btree::ideal_capacity_from_node_size(btree::level::BTREE_NODE_SIZE) * 3;

        const VALUES: u64 = 64;
        const IDS_PER_VALUE: u64 = 32;
        let mut expected: Vec<Vec<Id>> = Vec::new();
        let mut futs = FuturesUnordered::new();
        for value in 0..VALUES {
            let mut ids = Vec::new();
            for n in 0..IDS_PER_VALUE {
                let id = Id::from_parts(1, value * IDS_PER_VALUE + n);
                ids.push(id);
                let index_client = index_client.clone();
                futs.push(async move { index_client.insert(&field_entry(value, &id)).await });
            }
            expected.push(ids);
        }
        while let Some(result) = futs.next().await {
            assert!(result.unwrap(), "insertion returned false");
        }
        // Entries of a later schema sort after every field entry above.
        let mut futs = FuturesUnordered::new();
        for n in 0..filler {
            let index_client = index_client.clone();
            futs.push(async move {
                let id = Id::from_parts(2, n as u64);
                let mut key =
                    EntryKey::for_schema_field_feature(SchemaUid(12), 1, &(n as u64).to_be_bytes());
                key.set_id(&id);
                index_client.insert(&key).await
            });
        }
        while let Some(result) = futs.next().await {
            assert!(result.unwrap(), "insertion returned false");
        }
        // The split is found by a 500 ms checker and then migrated; wait for
        // the state machine to place a tree after the one holding the field
        // entries, which is exactly what the cursor consults when it refills.
        let first_field_key = field_entry(0, &Id::from_parts(1, 0));
        let deadline = Instant::now() + Duration::from_secs(120);
        let trees = loop {
            let next = index_client
                .next_tree(&first_field_key, Ordering::Forward)
                .await
                .unwrap();
            if matches!(next, client::NextTree::Found(..)) {
                break 2;
            }
            if Instant::now() > deadline {
                break 1;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        };
        storage::wait_until_updated().await;
        assert!(trees > 1, "the fixture needs a tree after the field entries");
        let stats = index_client.tree_stats().await.unwrap().len();
        assert!(stats >= 2, "tree_stats walks every placement, saw {stats}");

        // Inclusive bounds the way the planner builds them: the start key
        // carries the unit id, the end key the max id, so every id of the
        // boundary value lies inside.
        let prefix = |value: u64| EntryKey::for_schema_field_feature(SchemaUid(11), 1, &value.to_be_bytes());
        let prefix_end =
            |value: u64| EntryKey::from_props(&Id::max_id(), &value.to_be_bytes(), 1, SchemaUid(11));
        for value in [0, 31, VALUES - 1] {
            for ordering in [Ordering::Forward, Ordering::Backward] {
                let range = Range {
                    start: crate::index::ranged::tree::service::RangeTerm::Inclusive(prefix(value)),
                    end: crate::index::ranged::tree::service::RangeTerm::Inclusive(prefix_end(value)),
                    ordering,
                };
                let cursor = client::RangedIndexerClient::seek(&index_client, range, 64, None)
                    .await
                    .unwrap()
                    .unwrap_or_else(|| panic!("value {value} scanned {ordering:?} found nothing"));
                let mut got = collect(cursor).await;
                got.sort();
                assert_eq!(
                    got, expected[value as usize],
                    "value {value} scanned {ordering:?} across {trees} trees"
                );
            }
        }
        // A bounded range over several values, likewise.
        let range = Range {
            start: crate::index::ranged::tree::service::RangeTerm::Inclusive(prefix(10)),
            end: crate::index::ranged::tree::service::RangeTerm::Inclusive(prefix_end(20)),
            ordering: Ordering::Forward,
        };
        let cursor = client::RangedIndexerClient::seek(&index_client, range, 64, None)
            .await
            .unwrap()
            .unwrap();
        let mut got = collect(cursor).await;
        got.sort();
        let mut want: Vec<Id> = expected[10..=20].concat();
        want.sort();
        assert_eq!(got, want, "values 10..=20 across {trees} trees");
    }

    fn schema() -> Schema {
        Schema::new_with_id(
            11,
            &String::from("test"),
            None,
            Field::new_schema(vec![Field::new_unindexed("data", Type::U8)]),
            false,
            false,
        )
    }
}
