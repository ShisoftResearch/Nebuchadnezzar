use neb::server::*;
use neb::ram::schema::*;
use neb::server::transactions;
use std::rc::Rc;
use super::*;

#[test]
pub fn wr () {
    let server_addr = String::from("127.0.0.1:5200");
    let server = NebServer::new(ServerOptions {
        chunk_count: 1,
        memory_size: 16 * 1024,
        standalone: false,
        is_meta: true,
        meta_members: vec!(server_addr.clone()),
        address: server_addr.clone(),
        backup_storage: None,
        meta_storage: None,
        group_name: String::from("test"),
    }).unwrap();
    let fields = default_fields();
    let mut schema = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        fields: fields
    };
    server.meta.schemas.new_schema(&mut schema);
    let txn = transactions::new_client(&server_addr).unwrap();
    let txn_id = txn.begin().unwrap().unwrap();

}
