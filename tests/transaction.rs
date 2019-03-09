use super::*;
use env_logger;
use futures::future::Future;
use neb::ram::cell::*;
use neb::ram::schema::*;
use neb::ram::types::*;
use neb::server::transactions;
use neb::server::transactions::*;
use neb::server::*;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;

#[test]
pub fn workspace_wr() {
    let server_addr = String::from("127.0.0.1:5200");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 16 * 1024,
            backup_storage: None,
            wal_storage: None,
            services: vec![Service::Cell, Service::Transaction]
        },
        &server_addr,
        "test",
    );
    let schema = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        str_key_field: None,
        fields: default_fields(),
        is_dynamic: false,
    };
    server.meta.schemas.new_schema(schema.clone());
    let txn = transactions::new_async_client(&server_addr).unwrap();
    let txn_id = txn.begin().wait().unwrap().unwrap();
    let mut data_map = Map::new();
    data_map.insert(&String::from("id"), Value::I64(100));
    data_map.insert(&String::from("score"), Value::U64(70));
    data_map.insert(&String::from("name"), Value::String(String::from("Jack")));
    let cell_1 = Cell::new_with_id(schema.id, &Id::rand(), Value::Map(data_map.clone()));
    let cell_1_w_res = txn
        .write(txn_id.to_owned(), cell_1.to_owned())
        .wait()
        .unwrap()
        .unwrap();
    match cell_1_w_res {
        TxnExecResult::Accepted(()) => {}
        _ => panic!("write cell 1 not accepted {:?}", cell_1_w_res),
    }
    let cell_1_r_res = txn
        .read(txn_id.to_owned(), cell_1.id())
        .wait()
        .unwrap()
        .unwrap();
    match cell_1_r_res {
        TxnExecResult::Accepted(cell) => {
            assert_eq!(cell.id(), cell_1.id());
            assert_eq!(cell.data["id"].I64().unwrap(), &100);
            assert_eq!(cell.data["name"].String().unwrap(), "Jack");
            assert_eq!(cell.data["score"].U64().unwrap(), &70);
        }
        _ => panic!("read cell 1 not accepted {:?}", cell_1_r_res),
    }
    data_map.insert(&String::from("score"), Value::U64(90));
    let cell_1_w2 = Cell::new_with_id(schema.id, &cell_1.id(), Value::Map(data_map.clone()));
    let cell_1_w_res = txn
        .write(txn_id.to_owned(), cell_1_w2.to_owned())
        .wait()
        .unwrap()
        .unwrap();
    match cell_1_w_res {
        TxnExecResult::Accepted(()) => panic!("Write existed cell should fail"),
        TxnExecResult::Error(WriteError::CellAlreadyExisted) => {}
        _ => panic!("Wrong feedback {:?}", cell_1_w_res),
    }
    let cell_1_r_res = txn
        .read(txn_id.to_owned(), cell_1.id())
        .wait()
        .unwrap()
        .unwrap();
    match cell_1_r_res {
        TxnExecResult::Accepted(cell) => {
            assert_eq!(cell.id(), cell_1.id());
            assert_eq!(cell.data["score"].U64().unwrap(), &70);
        }
        _ => panic!("read cell 1 not accepted {:?}", cell_1_r_res),
    }
    let cell_1_u_res = txn
        .update(txn_id.to_owned(), cell_1_w2.to_owned())
        .wait()
        .unwrap()
        .unwrap();
    match cell_1_u_res {
        TxnExecResult::Accepted(()) => {}
        _ => panic!("update cell 1 not accepted"),
    }
    let cell_1_r_res = txn
        .read(txn_id.to_owned(), cell_1.id())
        .wait()
        .unwrap()
        .unwrap();
    match cell_1_r_res {
        TxnExecResult::Accepted(cell) => {
            assert_eq!(cell.id(), cell_1.id());
            assert_eq!(cell.data["score"].U64().unwrap(), &90);
        }
        _ => panic!("read cell 1 not accepted {:?}", cell_1_r_res),
    }
    let cell_1_rm_res = txn
        .remove(txn_id.to_owned(), cell_1.id())
        .wait()
        .unwrap()
        .unwrap();
    match cell_1_rm_res {
        TxnExecResult::Accepted(()) => {}
        _ => panic!("remove cell 1 not accepted {:?}", cell_1_rm_res),
    }
    let cell_1_r_res = txn
        .read(txn_id.to_owned(), cell_1.id())
        .wait()
        .unwrap()
        .unwrap();
    match cell_1_r_res {
        TxnExecResult::Error(ReadError::CellDoesNotExisted) => {}
        _ => panic!("read cell 1 not accepted {:?}", cell_1_r_res),
    }
    assert_eq!(
        txn.prepare(txn_id.to_owned()).wait().unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(txn_id.to_owned()).wait().unwrap().unwrap(),
        EndResult::Success
    );
    assert_eq!(
        txn.commit(txn_id.to_owned()).wait().unwrap(),
        Err(TMError::TransactionNotFound)
    );
    // committed transaction should have been disposed
}

#[test]
pub fn data_site_wr() {
    let server_addr = String::from("127.0.0.1:5201");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 16 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
            services: vec![Service::Cell, Service::Transaction]
        },
        &server_addr,
        "test",
    );
    let schema = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        str_key_field: None,
        fields: default_fields(),
        is_dynamic: true,
    };
    server.meta.schemas.new_schema(schema.clone());
    let txn = transactions::new_async_client(&server_addr).unwrap();
    let txn_id = txn.begin().wait().unwrap().unwrap();
    let mut data_map = Map::new();
    data_map.insert(&String::from("id"), Value::I64(100));
    data_map.insert(&String::from("score"), Value::U64(70));
    data_map.insert(&String::from("name"), Value::String(String::from("Jack")));
    let cell_1 = Cell::new_with_id(schema.id, &Id::rand(), Value::Map(data_map.clone()));
    let cell_1_non_exists_read = txn
        .read(txn_id.to_owned(), cell_1.id())
        .wait()
        .unwrap()
        .unwrap();
    match cell_1_non_exists_read {
        TxnExecResult::Error(ReadError::CellDoesNotExisted) => {}
        _ => panic!(
            "read non-existed cell should fail but got {:?}",
            cell_1_non_exists_read
        ),
    }
    let _cell_1_write = txn
        .write(txn_id.to_owned(), cell_1.to_owned())
        .wait()
        .unwrap()
        .unwrap();
    let cell_1_r_res = txn
        .read(txn_id.to_owned(), cell_1.id())
        .wait()
        .unwrap()
        .unwrap();
    match cell_1_r_res {
        TxnExecResult::Accepted(cell) => {
            assert_eq!(cell.id(), cell_1.id());
            assert_eq!(cell.data["id"].I64().unwrap(), &100);
            assert_eq!(cell.data["name"].String().unwrap(), "Jack");
            assert_eq!(cell.data["score"].U64().unwrap(), &70);
        }
        _ => panic!("read cell 1 not accepted {:?}", cell_1_r_res),
    }
    data_map.insert(&String::from("score"), Value::U64(90));
    let cell_1_w2 = Cell::new_with_id(schema.id, &cell_1.id(), Value::Map(data_map.clone()));
    let cell_1_w_res = txn
        .update(txn_id.to_owned(), cell_1_w2.to_owned())
        .wait()
        .unwrap()
        .unwrap();
    match cell_1_w_res {
        TxnExecResult::Accepted(()) => {}
        _ => panic!("Wrong feedback {:?}", cell_1_w_res),
    }
    assert!(server.chunks.read_cell(&cell_1.id()).is_err()); // isolation test
    assert_eq!(
        txn.prepare(txn_id.to_owned()).wait().unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(txn_id.to_owned()).wait().unwrap().unwrap(),
        EndResult::Success
    );
    let cell_r2 = server.chunks.read_cell(&cell_1.id()).unwrap();
    assert_eq!(cell_r2.id(), cell_1.id());
    assert_eq!(cell_r2.data["id"].I64().unwrap(), &100);
    assert_eq!(cell_r2.data["name"].String().unwrap(), "Jack");
    assert_eq!(cell_r2.data["score"].U64().unwrap(), &90);
}

#[test]
pub fn multi_transaction() {
    let server_addr = String::from("127.0.0.1:5202");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 16 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
            services: vec![Service::Cell, Service::Transaction]
        },
        &server_addr,
        "test",
    );
    let schema = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        str_key_field: None,
        fields: default_fields(),
        is_dynamic: false,
    };
    server.meta.schemas.new_schema(schema.clone());
    let txn = transactions::new_async_client(&server_addr).unwrap();
    let txn_1_id = txn.begin().wait().unwrap().unwrap();
    let txn_2_id = txn.begin().wait().unwrap().unwrap();
    let mut data_map_1 = Map::new();
    data_map_1.insert(&String::from("id"), Value::I64(100));
    data_map_1.insert(&String::from("score"), Value::U64(70));
    data_map_1.insert(&String::from("name"), Value::String(String::from("Jack")));
    let cell_1 = Cell::new_with_id(schema.id, &Id::rand(), Value::Map(data_map_1.clone()));
    let _cell_1_t1_write = txn
        .update(txn_1_id.to_owned(), cell_1.to_owned())
        .wait()
        .unwrap()
        .unwrap();
    let data_map_2 = data_map_1.clone();
    data_map_1.insert(&String::from("score"), Value::U64(90));
    let cell_2 = Cell::new_with_id(schema.id, &cell_1.id(), Value::Map(data_map_2.clone()));
    let _cell_1_t2_write = txn
        .write(txn_2_id.to_owned(), cell_2.to_owned())
        .wait()
        .unwrap()
        .unwrap();
    txn.prepare(txn_2_id.to_owned()).wait().unwrap().unwrap();
    txn.commit(txn_2_id.to_owned()).wait().unwrap().unwrap();
    assert_ne!(
        txn.prepare(txn_1_id.to_owned()).wait().unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert!(txn.commit(txn_1_id.to_owned()).wait().unwrap().is_err());
    ///////////////// PHASE 2 //////////////////
    let txn_1_id = txn.begin().wait().unwrap().unwrap();
    let txn_2_id = txn.begin().wait().unwrap().unwrap();
    match txn
        .read(txn_2_id.to_owned(), cell_1.id())
        .wait()
        .unwrap()
        .unwrap()
    {
        TxnExecResult::Accepted(_) => {}
        _ => {
            panic!("Cannot read cell 1 for txn 2");
        }
    }
    txn.update(txn_1_id.to_owned(), cell_1.to_owned())
        .wait()
        .unwrap()
        .unwrap();
    assert_eq!(
        txn.prepare(txn_1_id.to_owned()).wait().unwrap().unwrap(), // write too late
        TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
    );
    assert_eq!(
        txn.commit(txn_1_id.to_owned())
            .wait()
            .unwrap()
            .err()
            .unwrap(), // commit need prepared
        TMError::InvalidTransactionState(TxnState::Started)
    );
    let txn_1_id = txn.begin().wait().unwrap().unwrap();
    txn.update(txn_1_id.to_owned(), cell_1.to_owned())
        .wait()
        .unwrap()
        .unwrap(); // txn_1_id > txn_2_id, realizable
    assert_eq!(
        txn.prepare(txn_1_id.to_owned()).wait().unwrap().unwrap(),
        TMPrepareResult::Success
    );
    assert_eq!(
        txn.commit(txn_1_id.to_owned()).wait().unwrap().unwrap(),
        EndResult::Success
    );
}

#[test]
pub fn smoke_rw() {
    env_logger::init();
    // this test is likely to have unrealizable transactions and
    // should not cause any deadlock even if they failed
    let server_addr = String::from("127.0.0.1:5203");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 16 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
            services: vec![Service::Cell, Service::Transaction]
        },
        &server_addr,
        "test",
    );
    let schema = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        str_key_field: None,
        fields: default_fields(),
        is_dynamic: false,
    };
    server.meta.schemas.new_schema(schema.clone());
    let txn = transactions::new_async_client(&server_addr).unwrap();
    let mut data_map_1 = Map::new();
    data_map_1.insert(&String::from("id"), Value::I64(100));
    data_map_1.insert(&String::from("score"), Value::U64(0));
    data_map_1.insert(&String::from("name"), Value::String(String::from("Jack")));
    let mut cell_1 = Cell::new_with_id(schema.id, &Id::rand(), Value::Map(data_map_1.clone()));
    server.chunks.write_cell(&mut cell_1);
    let cell_id = cell_1.id();
    let thread_count = 200;
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::with_capacity(thread_count);
    for _ in 0..thread_count {
        let txn = txn.clone();
        threads.push(thread::spawn(move || {
            let txn_id = txn.begin().wait().unwrap().unwrap();
            let read_result = txn
                .read(txn_id.to_owned(), cell_id.to_owned())
                .wait()
                .unwrap();
            if let Ok(TxnExecResult::Accepted(mut cell)) = read_result {
                let mut score = *cell.data["score"].U64().unwrap();
                score += 1;
                let mut data = cell.data.Map().unwrap().clone();
                data.insert(&String::from("score"), Value::U64(score));
                cell.data = Value::Map(data);
                txn.update(txn_id.to_owned(), cell.to_owned());
            } else {
                // println!("Failed read, {:?}", read_result);
            }
            if txn.prepare(txn_id.to_owned()).wait().unwrap() == Ok(TMPrepareResult::Success) {
                assert_eq!(
                    txn.commit(txn_id.to_owned()).wait().unwrap().unwrap(),
                    EndResult::Success
                );
            }
        }));
    }
    for handle in threads {
        handle.join();
    }
}
