use super::*;
use client;
use index::btree;
use server;
use server::NebServer;
use server::ServerOptions;

#[test]
pub fn split() {
    let server_group = "lsm_service_split";
    let server_addr = String::from("127.0.0.1:5700");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 3 * 1024 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
            services: vec![server::Service::Cell, server::Service::LSMTreeIndex],
        },
        &server_addr,
        &server_group,
    );
    let client =
        Arc::new(client::AsyncClient::new(&server.rpc, &vec![server_addr], server_group).unwrap());
    client.new_schema_with_id(btree::page_schema()).wait();
}
