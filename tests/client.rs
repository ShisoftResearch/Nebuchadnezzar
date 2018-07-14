use neb::server::*;
use neb::ram::schema::*;
use neb::ram::types;
use neb::ram::types::*;
use neb::ram::cell::*;
use neb::client;
use neb::client::transaction::TxnError;
use std::thread;
use std::sync::Arc;
use env_logger;
use std::time::Duration;
use parking_lot::Mutex;
use futures::Future;

use super::*;
use super::server;

#[test]
pub fn general() {
    let server_group = "general_test";
    let server_addr = String::from("127.0.0.1:5400");
    let server = server::new_from_opts(&ServerOptions {
        chunk_count: 1,
        memory_size: 16 * 1024 * 1024,
        backup_storage: None,
        wal_storage: None
    },
    &server_addr,
    &server_group);
    let mut schema = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        str_key_field: None,
        fields: default_fields(),
        is_dynamic: false
    };
    let client = Arc::new(client::AsyncClient::new(
        &server.rpc, &vec!(server_addr),
        server_group).unwrap());
    let schema_id = client.new_schema(schema).wait().unwrap().0;
    let mut data_map = Map::new();
    data_map.insert(&String::from("id"), Value::I64(100));
    data_map.insert(&String::from("score"), Value::U64(0));
    data_map.insert(&String::from("name"), Value::String(String::from("Jack")));
    let cell_1 = Cell::new_with_id(schema_id, &Id::rand(), Value::Map(data_map.clone()));
    client.write_cell(cell_1.clone()).wait().unwrap().unwrap();
    client.read_cell(cell_1.clone().id()).wait().unwrap().unwrap();
    client.transaction(|ref mut trans| {
        Ok(()) // empty transaction
    }).wait().unwrap();
    let should_aborted = client.transaction(|ref mut trans| {
        trans.abort()
    }).wait();
    match should_aborted {
        Err(TxnError::Aborted(_)) => {},
        _ => panic!("{:?}", should_aborted)
    }

    // TODO: investigate dead lock
//    client.transaction(|ref mut trans| {
//        trans.write(&cell_1) // regular fail case
//    }).err().unwrap();
    client.transaction(move |ref mut trans| {
        let empty_cell = Cell::new_with_id(schema_id, &Id::rand(), Value::Map(Map::new()));
        trans.write(empty_cell.to_owned()) // empty cell write should fail
    }).wait().err().unwrap();

    let cell_1_id = cell_1.id();
    let thread_count = 50;
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(thread_count);
    for _ in 0..thread_count {
        let client = client.clone();
        threads.push(thread::spawn(move || {
            client.transaction(move |ref mut txn| {
                let selected = txn.read_selected(cell_1_id.to_owned(), types::key_hashes(
                    &vec![String::from("score")]
                ))?.unwrap();
                let mut cell = txn.read(cell_1_id.to_owned())?.unwrap();
                let mut score = *cell.data["score"].U64().unwrap();
                assert_eq!(selected.first().unwrap().U64().unwrap(), &score);
                score += 1;
                let mut data = cell.data.Map().unwrap().clone();
                data.insert(&String::from("score"), Value::U64(score));
                cell.data = Value::Map(data);
                txn.update(cell.to_owned())?;
                let selected = txn.read_selected(cell_1_id.to_owned(), types::key_hashes(
                    &vec![String::from("score")]
                ))?.unwrap();
                assert_eq!(selected[0].U64().unwrap(), &score);
                Ok(())
            }).wait().unwrap();
        }));
    }
    for handle in threads {
        handle.join();
    }
    let mut cell_1_r = client.read_cell(cell_1.id()).wait().unwrap().unwrap();
    assert_eq!(cell_1_r.data["score"].U64().unwrap(), &(thread_count as u64));
}

#[test]
pub fn multi_cell_update() {
    let server_group = "multi_cell_update_test";
    let server_addr = String::from("127.0.0.1:5401");
    let server = server::new_from_opts(&ServerOptions {
        chunk_count: 1,
        memory_size: 16 * 1024 * 1024,
        backup_storage: None,
        wal_storage: None
    },
    &server_addr,
    server_group);
    let mut schema = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        str_key_field: None,
        fields: default_fields(),
        is_dynamic: false
    };
    let client = Arc::new(client::AsyncClient::new(
        &server.rpc, &vec!(server_addr),
        server_group).unwrap());
    let thread_count = 100;
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(thread_count);
    let schema_id = schema.id;
    client.new_schema(schema).wait().unwrap();
    let mut data_map = Map::new();
    data_map.insert(&String::from("id"), Value::I64(100));
    data_map.insert(&String::from("score"), Value::U64(0));
    data_map.insert(&String::from("name"), Value::String(String::from("Jack")));
    let cell_1 = Cell::new_with_id(schema_id, &Id::rand(), Value::Map(data_map.clone()));
    client.write_cell(cell_1.clone()).wait().unwrap().unwrap();
    client.read_cell(cell_1.id()).wait().unwrap().unwrap();
    let cell_1_id = cell_1.id();
    let mut cell_2 = cell_1.clone();
    cell_2.set_id(&Id::rand());
    client.write_cell(cell_2.clone()).wait().unwrap().unwrap();
    client.read_cell(cell_2.id()).wait().unwrap().unwrap();
    let cell_2_id = cell_2.id();
    threads = Vec::new();
    for i in 0..thread_count {
        let client = client.clone();
        threads.push(thread::spawn(move || {
            client.transaction(move |txn| {
                let mut score_1 = 0;
                let mut score_2 = 0;
                let mut cell_1 = txn.read(cell_1_id.to_owned())?.unwrap();
                let mut cell_2 = txn.read(cell_2_id.to_owned())?.unwrap();
                score_1 = *cell_1.data["score"].U64().unwrap();
                score_2 = *cell_2.data["score"].U64().unwrap();
                score_1 += 1;
                score_2 += 1;
                let mut data_1 = cell_1.data.Map().unwrap().clone();
                data_1.insert(&String::from("score"), Value::U64(score_1));
                cell_1.data = Value::Map(data_1);
                let mut data_2 = cell_2.data.Map().unwrap().clone();
                data_2.insert(&String::from("score"), Value::U64(score_2));
                cell_2.data = Value::Map(data_2);
                txn.update(cell_1.to_owned())?;
                txn.update(cell_2.to_owned())?;
                Ok(())
            }).wait().unwrap();
        }));
    }
    for handle in threads {
        handle.join();
    }
    let mut cell_1_r = client.read_cell(cell_1_id).wait().unwrap().unwrap();
    let mut cell_2_r = client.read_cell(cell_2_id).wait().unwrap().unwrap();
    let cell_1_score = cell_1_r.data["score"].U64().unwrap();
    let cell_2_score = cell_2_r.data["score"].U64().unwrap();
    assert_eq!(cell_1_score + cell_2_score, (thread_count * 2) as u64);
}

#[test]
pub fn write_skew() {
    let server_group = "write_skew_test";
    let server_addr = String::from("127.0.0.1:5402");
    let server = server::new_from_opts(&ServerOptions {
        chunk_count: 1,
        memory_size: 16 * 1024 * 1024,
        backup_storage: None,
        wal_storage: None
    },
    &server_addr,
    server_group);
    let mut schema = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        str_key_field: None,
        fields: default_fields(),
        is_dynamic: false
    };
    let client = Arc::new(client::AsyncClient::new(
        &server.rpc, &vec!(server_addr),
        server_group).unwrap());
    let thread_count = 100;
    let schema_id = client.new_schema(schema).wait().unwrap().0;
    let mut data_map = Map::new();
    data_map.insert(&String::from("id"), Value::I64(100));
    data_map.insert(&String::from("score"), Value::U64(0));
    data_map.insert(&String::from("name"), Value::String(String::from("Jack")));
    let cell_1 = Cell::new_with_id(schema_id, &Id::rand(), Value::Map(data_map.clone()));
    client.write_cell(cell_1.clone()).wait().unwrap().unwrap();
    client.read_cell(cell_1.id()).wait().unwrap().unwrap();
    let cell_1_id = cell_1.id();
    let client_c1 = client.clone();
    let skew_tried = Arc::new(Mutex::new(0));
    let normal_tried = Arc::new(Mutex::new(0));

    let skew_tried_c = skew_tried.clone();
    let normal_tried_c = normal_tried.clone();

    let t1 = thread::spawn(move || {
        client_c1.transaction(move |ref mut txn| {
            *skew_tried_c.lock() += 1;
            let mut cell_1 = txn.read(cell_1_id.to_owned())?.unwrap();
            let mut score_1 = *cell_1.data["score"].U64().unwrap();
            thread::sleep(Duration::new(2, 0)); // wait 2 secs to let late write occur
            score_1 += 1;
            let mut data_1 = cell_1.data.Map().unwrap().clone();
            data_1.insert(&String::from("score"), Value::U64(score_1));
            cell_1.data = Value::Map(data_1);
            txn.update(cell_1.to_owned())?;
            Ok(())
        }).wait();
    });
    let client_c2 = client.clone();
    let t2 = thread::spawn(move || {
        client_c2.transaction(move |ref mut txn| {
            thread::sleep(Duration::new(1, 0));
            *normal_tried_c.lock() += 1;
            let mut cell_1 = txn.read(cell_1_id.to_owned())?.unwrap();
            let mut score_1 = *cell_1.data["score"].U64().unwrap();
            score_1 += 1;
            let mut data_1 = cell_1.data.Map().unwrap().clone();
            data_1.insert(&String::from("score"), Value::U64(score_1));
            cell_1.data = Value::Map(data_1);
            txn.update(cell_1.to_owned())?;
            Ok(())
        }).wait();
    });
    t2.join();
    t1.join();
    let mut cell_1_r = client.read_cell(cell_1_id).wait().unwrap().unwrap();
    let cell_1_score = *cell_1_r.data["score"].U64().unwrap();
    assert_eq!(cell_1_score, 2);
//    assert_eq!(*skew_tried.lock(), 2);
//    assert_eq!(*normal_tried.lock(), 1);
    println!("Skew tried {}, normal tried {}", *skew_tried.lock(), *normal_tried.lock());
}

#[test]
pub fn server_isolation() {
    let server_1_group = "server_isolation_test_1";
    let server_2_group = "server_isolation_test_2";
    let server_address_1 = "127.0.0.1:5403";
    let server_address_2 = "127.0.0.1:5404";

    let server_1 = server::new_from_opts(&ServerOptions {
        chunk_count: 1,
        memory_size: 16 * 1024 * 1024,
        backup_storage: None,
        wal_storage: None
    },
    server_address_1,
    server_1_group);
    let client1 = Arc::new(client::AsyncClient::new(
        &server_1.rpc, &vec!(server_address_1.to_string()),
        server_1_group).unwrap());

    let server_2 = server::new_from_opts(&ServerOptions {
        chunk_count: 1,
        memory_size: 16 * 1024 * 1024,
        backup_storage: None,
        wal_storage: None
    },
    server_address_2,
    server_2_group);
    let client2 = Arc::new(client::AsyncClient::new(
        &server_2.rpc, &vec!(server_address_2.to_string()),
        server_2_group).unwrap());

    let mut schema1 = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        str_key_field: None,
        fields: default_fields(),
        is_dynamic: false
    };
    let mut schema2 = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        str_key_field: None,
        fields: Field::new (&String::from("*"), 0, false, false, Some(
            vec![
                Field::new(&String::from("-id"), 6, false, false, None),
                Field::new(&String::from("-name"), 20, false, false, None),
                Field::new(&String::from("-score"), 10, false, false, None),
            ]
        )),
        is_dynamic: false
    };

    client1.new_schema_with_id(schema1.clone()).wait().unwrap().unwrap();
    client2.new_schema_with_id(schema2.clone()).wait().unwrap().unwrap();

    // println!("{:?}", client1.schema_client.get(&schema1.id));

    let schema_1_got: Schema = client1.get_all_schema().wait().unwrap().first().unwrap().clone();
    assert_eq!(schema_1_got.id, 1);
    let schema_1_fields = schema1.fields;
    assert_eq!(
        schema_1_fields.clone().sub_fields.unwrap().first().unwrap().name,
        default_fields().sub_fields.unwrap().first().unwrap().name
    );
    assert_eq!(
        schema_1_fields.clone().sub_fields.unwrap().get(1).unwrap().name,
        default_fields().sub_fields.unwrap().get(1).unwrap().name
    );
    assert_eq!(
        schema_1_fields.clone().sub_fields.unwrap().get(2).unwrap().name,
        default_fields().sub_fields.unwrap().get(2).unwrap().name
    );

    let schema_2_got: Schema = client2.get_all_schema().wait().unwrap().first().unwrap().clone();
    assert_eq!(schema_2_got.id, 1);
    let schema_2_fields = schema2.fields;
    assert_eq!(
        schema_2_fields.clone().sub_fields.unwrap().first().unwrap().name,
        "-id"
    );
    assert_eq!(
        schema_2_fields.clone().sub_fields.unwrap().get(1).unwrap().name,
        "-name"
    );
    assert_eq!(
        schema_2_fields.clone().sub_fields.unwrap().get(2).unwrap().name,
        "-score"
    );
}