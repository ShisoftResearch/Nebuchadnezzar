use neb::server::*;
use neb::ram::schema::*;
use neb::ram::types::*;
use neb::ram::cell::*;
use neb::client;
use neb::client::transaction::TxnError;
use std::thread;
use std::sync::Arc;
use env_logger;

use super::*;

#[test]
pub fn general() {
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
    let client = Arc::new(client::Client::new(
        &server.rpc, &vec!(server_addr),
        &String::from("test")).unwrap());
    client.new_schema(&mut schema).unwrap();
    let mut data_map = Map::<String, Value>::new();
    data_map.insert(String::from("id"), Value::I64(100));
    data_map.insert(String::from("score"), Value::U64(0));
    data_map.insert(String::from("name"), Value::String(String::from("Jack")));
    let cell_1 = Cell::new(schema.id, &Id::rand(), data_map.clone());
    client.write_cell(&cell_1).unwrap().unwrap();
    client.read_cell(&cell_1.id()).unwrap().unwrap();
    client.transaction(|ref mut trans| {
        Ok(()) // empty transaction
    }).unwrap();
    let should_aborted = client.transaction(|ref mut trans| {
        trans.abort()
    });
    match should_aborted {
        Err(TxnError::Aborted) => {},
        _ => panic!("{:?}", should_aborted)
    }
    let cell_1_id = cell_1.id();
    let thread_count = 100;
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(thread_count);
    for _ in 0..thread_count {
        let client = client.clone();
        threads.push(thread::spawn(move || {
            client.transaction(|ref mut txn| {
                let mut cell = txn.read(&cell_1_id)?.unwrap();
                let mut score = cell.data.Map().unwrap().get("score").unwrap().U64().unwrap();
                score += 1;
                let mut data = cell.data.Map().unwrap().clone();
                data.insert(String::from("score"), Value::U64(score));
                cell.data = Value::Map(data);
                txn.update(&cell)?;
                Ok(())
            }).unwrap();
        }));
    }
    for handle in threads {
        handle.join();
    }
    let mut cell_1_r = client.read_cell(&cell_1.id()).unwrap().unwrap();
    assert_eq!(cell_1_r.data.Map().unwrap().get("score").unwrap().U64().unwrap(), thread_count as u64);
}

#[test]
pub fn write_skew() {
    let server_addr = String::from("127.0.0.1:5401");
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
    let client = Arc::new(client::Client::new(
        &server.rpc, &vec!(server_addr),
        &String::from("test")).unwrap());
    let thread_count = 100;
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(thread_count);
    client.new_schema(&mut schema).unwrap();
    let mut data_map = Map::<String, Value>::new();
    data_map.insert(String::from("id"), Value::I64(100));
    data_map.insert(String::from("score"), Value::U64(0));
    data_map.insert(String::from("name"), Value::String(String::from("Jack")));
    let cell_1 = Cell::new(schema.id, &Id::rand(), data_map.clone());
    client.write_cell(&cell_1).unwrap().unwrap();
    client.read_cell(&cell_1.id()).unwrap().unwrap();
    let cell_1_id = cell_1.id();
    let mut cell_2 = cell_1.clone(); // clone one to test write skew
    cell_2.set_id(&Id::rand());
    client.write_cell(&cell_2).unwrap().unwrap();
    client.read_cell(&cell_2.id()).unwrap().unwrap();
    let cell_2_id = cell_2.id();
    threads = Vec::new();
    for i in 0..thread_count {
        let client = client.clone();
        threads.push(thread::spawn(move || {
            client.transaction(|ref mut txn| {
                let cell_1_id = if i % 2 == 1 {&cell_1_id} else {&cell_2_id};
                let cell_2_id = if i % 2 == 0 {&cell_1_id} else {&cell_2_id};
                let mut cell_1 = txn.read(cell_1_id)?.unwrap();
                let mut cell_2 = txn.read(cell_2_id)?.unwrap();
                let mut score = cell_1.data.Map().unwrap().get("score").unwrap().U64().unwrap();
                score += 1;
                let mut data = cell_1.data.Map().unwrap().clone();
                data.insert(String::from("score"), Value::U64(score));
                cell_1.data = Value::Map(data);
                txn.update(&cell_1)?;
                txn.update(&cell_2)?;
                Ok(())
            }).unwrap();
        }));
    }
    for handle in threads {
        handle.join();
    }
    let mut cell_1_r = client.read_cell(&cell_1_id).unwrap().unwrap();
    let mut cell_2_r = client.read_cell(&cell_2_id).unwrap().unwrap();
    let cell_1_score = cell_1_r.data.Map().unwrap().get("score").unwrap().U64().unwrap();
    let cell_2_score = cell_2_r.data.Map().unwrap().get("score").unwrap().U64().unwrap();
    println!("{} / {}", cell_1_score, cell_2_score);
    assert_eq!(cell_1_score + cell_2_score, thread_count as u64);
}