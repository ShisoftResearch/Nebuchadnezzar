use neb::server::*;
use neb::ram::schema::*;
use neb::ram::types::*;
use neb::ram::cell::*;
use neb::client;
use env_logger;

use super::*;

#[test]
pub fn data_site_wr() {
    let server_addr = String::from("127.0.0.1:5400");
    let server = NebServer::new(ServerOptions {
        chunk_count: 1,
        memory_size: 16 * 1024 * 1024,
        standalone: false,
        is_meta: true,
        meta_members: vec!(server_addr.clone()),
        address: server_addr.clone(),
        backup_storage: None,
        meta_storage: None,
        group_name: String::from("test"),
    }).unwrap();
    let mut schema = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        fields: default_fields()
    };
    let client = client::Client::new(
        &server.rpc, &vec!(server_addr),
        &String::from("test")).unwrap();
    
}