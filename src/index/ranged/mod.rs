pub mod client;
pub mod lsm;
pub mod sm;
mod trees;

#[cfg(test)]
mod tests {
    use super::lsm::btree::storage;
    use super::lsm::btree::Ordering;
    use super::*;
    use crate::client::*;
    use crate::index::ranged::lsm::btree;
    use crate::index::EntryKey;
    use crate::ram::cell::*;
    use crate::ram::schema::*;
    use crate::ram::types::Id;
    use crate::ram::types::*;
    use crate::server::*;
    use futures::stream::FuturesUnordered;
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_stream::StreamExt;

    // #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn general() {
        let _ = env_logger::try_init();
        let server_group = "ranged_index_test";
        let server_addr = String::from("127.0.0.1:5711");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 32 * 1024 * 1024 * 1024, // G
                backup_storage: None,
                wal_storage: None,
                index_enabled: false, // We don't use the high level index builder here
                services: vec![Service::Cell, Service::RangedIndexer],
            },
            &server_addr,
            server_group,
        )
        .await;
        let client = Arc::new(
            AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                server_group,
            )
            .await
            .unwrap(),
        );
        let index_client = Arc::new(
            client::RangedQueryClient::new(&server.consh, &server.raft_client, &client).await,
        );
        info!("Tree stat {:?}", index_client.tree_stats().await.unwrap());
        client.new_schema_with_id(schema()).await.unwrap().unwrap();
        let test_capacity = btree::ideal_capacity_from_node_size(btree::level::LEVEL_1) * 3;
        let mut futs = FuturesUnordered::new();
        info!(
            "Testing ranged indexer preesure test with {} items",
            test_capacity
        );
        info!("Generating test set");
        let mut rng = rand::thread_rng();
        let nums = (0..test_capacity).collect_vec();
        let mut nums_2 = nums.clone();
        let mut nums_3 = nums.clone();
        nums_2.as_mut_slice().shuffle(&mut rng);
        nums_3.as_mut_slice().shuffle(&mut rng);
        info!("Adding insertion tasks");
        for i in nums_2 {
            let index_client = index_client.clone();
            let client = client.clone();
            futs.push(tokio::spawn(async move {
                let id = Id::new(1, i as u64);
                let key = EntryKey::from_id(&id);
                let mut data_map = OwnedMap::new();
                data_map.insert("data", OwnedValue::U64(i as u64));
                let cell = OwnedCell::new_with_id(11, &id, OwnedValue::Map(data_map));
                client.write_cell(cell).await.unwrap().unwrap();
                index_client.insert(&key).await
            }));
        }
        info!("All tasks queued, waiting for finish");
        while let Some(result) = futs.next().await {
            assert!(result.unwrap().unwrap(), "Insertion return false");
        }
        info!("Verifiying insertion");
        for i in &nums {
            let id = Id::new(1, *i as u64);
            let cell_res = client.read_cell(id).await.unwrap();
            match cell_res {
                Ok(cell) => {
                    assert_eq!(*cell.data["data"].u64().unwrap(), *i as u64);
                }
                Err(e) => {
                    panic!("Expecting cell, found error {:?}", e);
                }
            }
        }
        info!("All keys inserted. The background task should merging trees. Doing searches.");
        let mut futs = FuturesUnordered::new();
        for (i, num) in nums_3.into_iter().enumerate() {
            let index_client = index_client.clone();
            futs.push(tokio::spawn(async move {
                trace!("Seeking Id at {}, index {}", num, i);
                let id = Id::new(1, num as u64);
                let key = EntryKey::from_id(&id);
                let rt_cursor =
                    client::RangedQueryClient::seek(&index_client, &key, Ordering::Forward, 1)
                        .await
                        .unwrap()
                        .unwrap();
                assert_eq!(id, rt_cursor.current().unwrap().0, "at {}", i);
                trace!("Id at {}, index {} have been checked", num, i);
            }));
        }
        while let Some(_) = futs.next().await {}
        debug!("Selection check pased, waiting for 10 secs");
        tokio::time::sleep(Duration::from_secs(10)).await;
        info!("Waiting tree storage");
        storage::wait_until_updated().await;
        info!(
            "Total cells {}, Tree stat {:?}",
            client.count().await.unwrap(),
            index_client.tree_stats().await.unwrap()
        );
        info!("Verifiying insertion round 2");
        for i in &nums {
            let id = Id::new(1, *i as u64);
            let cell_res = client.read_cell(id).await.unwrap();
            match cell_res {
                Ok(cell) => {
                    assert_eq!(*cell.data["data"].u64().unwrap(), *i as u64);
                }
                Err(e) => {
                    panic!("Expecting cell at round 2, found error {:?}", e);
                }
            }
        }
        info!("Scanning forward...");
        let mut rt_cursor = client::RangedQueryClient::seek(
            &index_client,
            &EntryKey::from_id(&Id::new(1, 0)),
            Ordering::Forward,
            128,
        )
        .await
        .unwrap()
        .unwrap();
        for num in &nums {
            let id = Id::new(1, *num as u64);
            let current = rt_cursor.current().expect(&format!("Checking {}", num)).0;
            assert_eq!(
                id,
                current,
                "Expecting {:?}, key {:?}, got {:?}",
                id,
                EntryKey::from_id(&id),
                current
            );
            let _ = rt_cursor.next().await.unwrap();
            if num % (test_capacity / 128) == 0 {
                debug!("Scanned {} of {}", num, test_capacity);
            }
        }
        info!("Scan finished");
        let end_of_list = rt_cursor.next().await.unwrap();
        assert!(
            end_of_list.is_none(),
            "End of the list have id {:?}",
            end_of_list.unwrap().0
        );
        assert!(
            end_of_list.is_none(),
            "After end of the list have id {:?}",
            end_of_list.unwrap().0
        );
        debug!("All check pased, waiting for 10 secs for statistics");
        tokio::time::sleep(Duration::from_secs(10)).await;
        info!(
            "Total cells {}, Tree stat {:?}",
            client.count().await.unwrap(),
            index_client.tree_stats().await.unwrap()
        );
    }

    fn schema() -> Schema {
        Schema {
            id: 11,
            name: String::from("test"),
            key_field: None,
            str_key_field: None,
            fields: Field::new(
                "*",
                0,
                false,
                false,
                Some(vec![Field::new("data", 10, false, false, None, vec![])]),
                vec![],
            ),
            is_dynamic: false,
        }
    }
}
