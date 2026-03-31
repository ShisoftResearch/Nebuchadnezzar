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
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_stream::StreamExt;

    async fn run_insert_pressure_test(server_group: &str, server_addr: &str, multiplier: usize) {
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 32 * 1024 * 1024 * 1024,
                db_size: 32 * 1024 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell, Service::RangedIndexer],
                enable_recovery: false,
            },
            server_addr,
            server_group,
            async |_| {},
        )
        .await;
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
        let index_client = Arc::new(client::RangedIndexerClient::new(
            &server.consh,
            &server.raft_client,
        ));
        client.new_schema_with_id(schema()).await.unwrap().unwrap();

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
                &id,
                current,
                "Expecting {:?}, got {:?}, list index {}, cursor ids {:?}, cursor pos {:?}",
                id,
                current,
                i,
                rt_cursor.ids,
                rt_cursor.pos
            );
            let _ = rt_cursor.next().await.unwrap();
        }
        assert!(
            rt_cursor.next().await.unwrap().is_none(),
            "Expected scan to finish exactly at end of list"
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
