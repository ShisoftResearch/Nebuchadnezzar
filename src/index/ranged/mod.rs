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
    use crate::index::ranged::lsm::tree::LAST_LEVEL_MULT_FACTOR;
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
    use tokio::stream::StreamExt;

    #[ignore]
    #[tokio::test(threaded_scheduler)]
    async fn general() {
        let _ = env_logger::try_init();
        let server_group = "ranged_index_test";
        let server_addr = String::from("127.0.0.1:5711");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 32 * 1024 * 1024 * 1024, // 1G
                backup_storage: None,
                wal_storage: None,
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
        client.new_schema_with_id(schema()).await.unwrap().unwrap();
        let test_capacity = btree::ideal_capacity_from_node_size(btree::level::LEVEL_2)
            * LAST_LEVEL_MULT_FACTOR
            * 8;
        let mut futs = FuturesUnordered::new();
        info!(
            "Testing ranged indexer preesure test with {} items",
            test_capacity
        );
        debug!("Generating test set");
        let mut rng = rand::thread_rng();
        let mut nums = (0..test_capacity).collect_vec();
        let mut nums_2 = nums.clone();
        nums.as_mut_slice().shuffle(&mut rng);
        nums_2.as_mut_slice().shuffle(&mut rng);
        debug!("Adding insertion tasks");
        for i in nums {
            let index_client = index_client.clone();
            let client = client.clone();
            futs.push(tokio::spawn(async move {
                let id = Id::new(1, i as u64);
                let key = EntryKey::from_id(&id);
                let mut data_map = Map::new();
                data_map.insert("data", Value::U64(i as u64));
                let cell = Cell::new_with_id(11, &id, Value::Map(data_map));
                client.write_cell(cell).await.unwrap().unwrap();
                index_client.insert(&key).await
            }));
        }
        info!("All tasks queued, waiting for finish");
        while let Some(result) = futs.next().await {
            assert!(result.unwrap().unwrap(), "Insertion return false");
        }
        info!("All keys inserted. The background task should merging trees. Doing searches.");
        let mut futs = FuturesUnordered::new();
        for (i, num) in nums_2.into_iter().enumerate() {
            let index_client = index_client.clone();
            futs.push(tokio::spawn(async move {
                let id = Id::new(1, num as u64);
                let key = EntryKey::from_id(&id);
                let rt_cursor = client::RangedQueryClient::seek(&index_client, &key, Ordering::Forward)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(id, rt_cursor.current().unwrap().0);
                debug!("Id at {}, index {} have been checked", num, i);
            }));
        }
        while let Some(_) = futs.next().await {}
        debug!("Selection check pased, waiting for 10 secs");
        tokio::time::delay_for(Duration::from_secs(10)).await;
        info!("Waiting tree storage");
        storage::wait_until_updated().await;
        info!("Total cells {}", client.count().await.unwrap());
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
