pub mod client;
pub mod lsm;
pub mod sm;
mod trees;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::*;
    use crate::ram::schema::*;
    use crate::server::*;
    use std::sync::Arc;

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
        // index_client.insert()
    }
}
