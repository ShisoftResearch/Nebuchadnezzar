use bifrost::membership::member::MemberService;
use bifrost::membership::server::Membership;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::master as sm_master;
use bifrost::rpc;
use bifrost::rpc::Server;
use bifrost::utils::fut_exec::wait;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::custom_types::map::Map;
use dovahkiin::types::type_id_of;
use futures::prelude::*;
use itertools::Itertools;
use neb::client;
use neb::ram::cell::Cell;
use neb::ram::schema::Field;
use neb::ram::schema::LocalSchemasCache;
use neb::ram::schema::Schema;
use neb::ram::types::*;
use neb::server::*;
use parking_lot::deadlock;
use rayon::iter::IntoParallelRefIterator;
use rayon::prelude::*;
use std::env;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
pub fn init() {
    NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 16 * 1024,
            backup_storage: None,
            wal_storage: None,
        },
        &String::from("127.0.0.1:5100"),
        &String::from("test"),
    );
}

#[test]
pub fn smoke_test() {
    const DATA: &'static str = "DATA";
    let num = env::var("NEB_KV_SMOKE_TEST_ITEMS")
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .unwrap();
    env_logger::init();
    let server_addr = String::from("127.0.0.1:5300");
    let server_group = String::from("smoke_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 512 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
        },
        &server_addr,
        &server_group,
    );
    let schema_id = 123;
    let schema = Schema {
        id: schema_id,
        name: String::from("schema"),
        key_field: None,
        str_key_field: None,
        is_dynamic: false,
        fields: Field::new(
            "*",
            0,
            false,
            false,
            Some(vec![Field::new(
                DATA,
                type_id_of(Type::U64),
                false,
                false,
                None,
            )]),
        ),
    };

    let client =
        Arc::new(client::AsyncClient::new(&server.rpc, &vec![server_addr], &server_group).unwrap());
    wait(client.new_schema_with_id(schema));

    (0..num).collect::<Vec<_>>().into_iter().for_each(|i| {
        let client_clone = client.clone();
        // intense upsert, half delete
        let id = Id::new(0, i / 2);
        let mut value = Value::Map(Map::new());
        value[DATA] = Value::U64(i);
        let cell = Cell::new_with_id(schema_id, &id, value);
        wait(client_clone.upsert_cell(cell));

        // read
        let read_cell = wait(client_clone.read_cell(id)).unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].U64().unwrap()), i);

        if i % 2 == 0 {
            wait(client_clone.remove_cell(id));
        }
    });

    (0..num).collect::<Vec<_>>().into_iter().for_each(|i| {
        let id = Id::new(0, i);
        let mut value = Value::Map(Map::new());
        value[DATA] = Value::U64(i * 2);
        let cell = Cell::new_with_id(schema_id, &id, value);
        wait(client.upsert_cell(cell));

        // verify
        let read_cell = wait(client.read_cell(id)).unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].U64().unwrap()), i * 2);
    });
}

#[test]
pub fn smoke_test_parallel() {
    const DATA: &'static str = "DATA";
    let num = env::var("NEB_KV_SMOKE_TEST_ITEMS")
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .unwrap();
    env_logger::init();
    let server_addr = String::from("127.0.0.1:5300");
    let server_group = String::from("smoke_parallel_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 512 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
        },
        &server_addr,
        &server_group,
    );
    let schema_id = 123;
    let schema = Schema {
        id: schema_id,
        name: String::from("schema"),
        key_field: None,
        str_key_field: None,
        is_dynamic: false,
        fields: Field::new(
            "*",
            0,
            false,
            false,
            Some(vec![Field::new(
                DATA,
                type_id_of(Type::U64),
                false,
                false,
                None,
            )]),
        ),
    };

    let client =
        Arc::new(client::AsyncClient::new(&server.rpc, &vec![server_addr], &server_group).unwrap());
    wait(client.new_schema_with_id(schema));

    // Create a background thread which checks for deadlocks every 10s
    thread::Builder::new()
        .name("deadlock checker".to_string())
        .spawn(move || loop {
            thread::sleep(Duration::from_secs(1));
            let deadlocks = deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            error!("{} deadlocks detected", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                error!("Deadlock #{}", i);
                for t in threads {
                    error!("Thread Id {:#?}", t.thread_id());
                    error!("{:#?}", t.backtrace());
                }
            }
            panic!();
        });

    (0..num).collect::<Vec<_>>().into_par_iter().for_each(|i| {
        let client_clone = client.clone();
        // intense upsert, half delete
        let id = Id::new(0, i);
        let mut value = Value::Map(Map::new());
        value[DATA] = Value::U64(i);
        let cell = Cell::new_with_id(schema_id, &id, value);
        wait(client_clone.upsert_cell(cell));

        // read
        let read_cell = wait(client_clone.read_cell(id)).unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].U64().unwrap()), i);

        if i % 2 == 0 {
            wait(client_clone.remove_cell(id));
        }
    });

    (0..num).collect::<Vec<_>>().into_par_iter().for_each(|i| {
        if i % 2 == 0 { return }
        let id = Id::new(0, i);
        let mut value = Value::Map(Map::new());
        value[DATA] = Value::U64(i * 2);
        let cell = Cell::new_with_id(schema_id, &id, value);
        wait(client.upsert_cell(cell));

        // verify
        let read_cell = wait(client.read_cell(id)).unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].U64().unwrap()), i * 2);
    });
}
