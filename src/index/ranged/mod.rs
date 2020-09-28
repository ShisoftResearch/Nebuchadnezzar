pub mod client;
pub mod lsm;
pub mod sm;
mod trees;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::*;
    use crate::server::*;
    use std::sync::Arc;
    use crate::index::EntryKey;
    use crate::index::ranged::lsm::btree;
    use crate::ram::types::Id;
    use futures::stream::FuturesUnordered;
    use tokio::stream::StreamExt;

    #[tokio::test(threaded_scheduler)]
    async fn general() {
        let _ = env_logger::try_init();
        let server_group = "ranged_index_test";
        let server_addr = String::from("127.0.0.1:5711");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 512 * 1024 * 1024,
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
        let index_client =
            Arc::new(client::RangedQueryClient::new(&server.consh, &server.raft_client, &client).await);
        let test_capacity = btree::ideal_capacity_from_node_size(btree::level::LEVEL_2) * 8;
        let mut futs = FuturesUnordered::new();
        info!("Testing ranged indexer preesure test with {} items", test_capacity);
        for i in 0..test_capacity { 
            let index_client = index_client.clone();
            futs.push(tokio::spawn(async move {
                let id = Id::new(1, i as u64);
                let key = EntryKey::from_id(&id);
                index_client.insert(&key).await
            }));
        }
        info!("All tasks queued, waiting for finish");
        while let Some(result) = futs.next().await {
            assert!(result.unwrap().unwrap());
        }
    }
}
