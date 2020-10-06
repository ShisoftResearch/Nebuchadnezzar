pub mod client;
pub mod lsm;
pub mod sm;
mod trees;

#[cfg(test)]
mod tests {
    use super::lsm::btree::storage;
    use super::*;
    use crate::client::*;
    use crate::index::ranged::lsm::btree;
    use crate::index::ranged::lsm::tree::LAST_LEVEL_MULT_FACTOR;
    use crate::index::EntryKey;
    use crate::ram::types::Id;
    use crate::server::*;
    use futures::stream::FuturesUnordered;
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::stream::StreamExt;

    //#[ignore]
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
        nums.as_mut_slice().shuffle(&mut rng);
        debug!("Adding insertion tasks");
        for i in nums {
            let index_client = index_client.clone();
            futs.push(tokio::spawn(async move {
                let id = Id::new(1, i as u64);
                let key = EntryKey::from_id(&id);
                index_client.insert(&key).await
            }));
        }
        info!("All tasks queued, waiting for finish");
        while let Some(result) = futs.next().await {
            assert!(result.unwrap().unwrap(), "Insertion return false");
        }
        info!("Waiting for 10 secs");
        tokio::time::delay_for(Duration::from_secs(10)).await;
        info!("Waiting tree storage");
        storage::wait_until_updated().await;
        info!("Total cells {}", client.count().await.unwrap());
    }
}
