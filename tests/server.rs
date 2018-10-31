use neb::server::*;
use neb::ram::schema::LocalSchemasCache;
use bifrost::rpc;
use bifrost::raft;
use bifrost::membership::server::Membership;
use bifrost::raft::client::RaftClient;
use bifrost::membership::member::MemberService;
use bifrost::raft::state_machine::{master as sm_master};
use bifrost::rpc::Server;
use std::rc::Rc;
use std::sync::Arc;
use futures::prelude::*;
use std::env;
use neb::ram::schema::Schema;
use neb::ram::schema::Field;
use dovahkiin::types::type_id_of;
use neb::client;
use neb::ram::types::*;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::custom_types::map::Map;
use neb::ram::cell::Cell;

#[test]
pub fn init () {
    NebServer::new_from_opts(&ServerOptions {
        chunk_count: 1,
        memory_size: 16 * 1024,
        backup_storage: None,
        wal_storage: None
    },
    &String::from("127.0.0.1:5100"),
    &String::from("test"));
}

#[test]
pub fn smoke_test () {
    const DATA: &'static str = "DATA";
    let num = env::var("NEB_KV_SMOKE_TEST_ITEMS")
        .unwrap_or("1000".to_string())
        .parse::<u64>().unwrap();
    env_logger::init();
    let server_addr = String::from("127.0.0.1:5300");
    let server_group = String::from("smoke_test");
    let server = NebServer::new_from_opts(&ServerOptions {
        chunk_count: 1,
        memory_size: 512 * 1024 * 1024,
        backup_storage: None,
        wal_storage: None
    },
    &server_addr,
    &server_group);
    let schema_id = 123;
    let schema = Schema {
        id: schema_id,
        name: String::from("schema"),
        key_field: None,
        str_key_field: None,
        is_dynamic: false,
        fields: Field::new(
            "*", 0, false, false,
            Some(vec![
                Field::new(DATA, type_id_of(Type::U64), false, false, None)
            ])
        )
    };

    let client = Arc::new(client::AsyncClient::new(
        &server.rpc, &vec!(server_addr),
        &server_group).unwrap());
    client.new_schema_with_id(schema).wait();

    for i in 0..num * 2 {
        // intense upsert, half delete
        let id = Id::new(0, num / 2);
        let mut value = Value::Map(Map::new());
        value[DATA] = Value::U64(num);
        let cell = Cell::new_with_id(schema_id, &id, value);
        client.upsert_cell(cell).wait();

        // verify
        let read_cell = client.read_cell(id).wait().unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].U64().unwrap()), num);

        if i % 2 == 0 {
            client.remove_cell(id).wait();
        }
    }
}