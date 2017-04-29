use neb::server::*;
use neb::ram::schema::Schemas;
use std::rc::Rc;

#[test]
pub fn init () {
    NebServer::new(ServerOptions {
        chunk_count: 1,
        memory_size: 16 * 1024,
        standalone: false,
        is_meta: true,
        meta_members: vec!(String::from("127.0.0.1:5100")),
        address: String::from("127.0.0.1:5100"),
        backup_storage: None,
        meta_storage: None,
        group_name: String::from("test"),
    }).unwrap();
}
