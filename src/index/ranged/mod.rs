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
                history_retention_ms: 300_000,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
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
                let id = Id::new(1, i as u64);
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
                let id = Id::new(1, num as u64);
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

        let start_id = Id::new(1, 0);
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
            let id = Id::new(1, *num as u64);
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
                history_retention_ms: 300_000,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
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
                    let id = Id::new((worker as u64) + 1, seq);
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
            let id = Id::new((worker as u64) + 1, last_seq);
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
        run_insert_pressure_test("ranged_index_test", "127.0.0.1:5711", 3).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migration_stress_insert_only() {
        let _ = env_logger::try_init();
        run_insert_pressure_test("ranged_index_migration_stress", "127.0.0.1:5712", 6).await;
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 64)]
    async fn soak_migration_stress_30m_64_threads() {
        let _ = env_logger::try_init();
        run_timed_insert_soak_test(
            "ranged_index_migration_soak_30m",
            "127.0.0.1:5713",
            64,
            Duration::from_secs(30 * 60),
        )
        .await;
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
