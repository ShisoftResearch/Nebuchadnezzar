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

    #[tokio::test(threaded_scheduler)]
    async fn general() {
        let server_group = "ranged_index_test";
        let server_addr = String::from("127.0.0.1:5711");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                memory_size: 16 * 1024 * 1024,
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
            client::RangedQueryClient::new(&server.consh, &server.raft_client, &client).await;
        let test_capacity = btree::ideal_capacity_from_node_size(btree::level::LEVEL_2) * 8;
        for i in 0..test_capacity {
            let id = Id::new(1, i as u64);
            let key = EntryKey::from_id(&id);
            assert!(index_client.insert(&key).await.unwrap());
        }
    }
}
