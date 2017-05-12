use neb::server::*;
use neb::ram::schema::*;
use neb::server::transactions;
use neb::ram::types::*;
use neb::server::transactions::*;
use std::rc::Rc;
use neb::ram::cell::*;
use super::*;

#[test]
pub fn workspace_wr() {
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
    let mut schema = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        fields: default_fields()
    };
    server.meta.schemas.new_schema(&mut schema);
    let txn = transactions::new_client(&server_addr).unwrap();
    let txn_id = txn.begin().unwrap().unwrap();
    let mut data_map = Map::<String, Value>::new();
    data_map.insert(String::from("id"), Value::I64(100));
    data_map.insert(String::from("score"), Value::U64(70));
    data_map.insert(String::from("name"), Value::String(String::from("Jack")));
    let cell_1 = Cell::new(schema.id, &Id::rand(), data_map.clone());
    let cell_1_w_res = txn.write(&txn_id, &cell_1).unwrap().unwrap();
    match cell_1_w_res {
        TransactionExecResult::Accepted(()) => {},
        _ => {panic!("write cell 1 not accepted {:?}", cell_1_w_res)}
    }
    let cell_1_r_res = txn.read(&txn_id, &cell_1.id()).unwrap().unwrap();
    match cell_1_r_res {
        TransactionExecResult::Accepted(cell) => {
            assert_eq!(cell.id(), cell_1.id());
            assert_eq!(cell.data.Map().unwrap().get("id").unwrap().I64().unwrap(), 100);
            assert_eq!(cell.data.Map().unwrap().get("name").unwrap().String().unwrap(), "Jack");
            assert_eq!(cell.data.Map().unwrap().get("score").unwrap().U64().unwrap(), 70);
        },
        _ => {panic!("read cell 1 not accepted {:?}", cell_1_w_res)}
    }
    data_map.insert(String::from("score"), Value::U64(90));
    let cell_1_w2 = Cell::new(schema.id, &cell_1.id(), data_map.clone());
    let cell_1_w_res = txn.write(&txn_id, &cell_1_w2).unwrap().unwrap();
    match cell_1_w_res {
        TransactionExecResult::Accepted(()) => {panic!("Write existed cell should fail")}
        TransactionExecResult::Error(WriteError::CellAlreadyExisted) => {}
        _ => {panic!("Wrong feedback {:?}", cell_1_w_res)}
    }
    let cell_1_r_res = txn.read(&txn_id, &cell_1.id()).unwrap().unwrap();
    match cell_1_r_res {
        TransactionExecResult::Accepted(cell) => {
            assert_eq!(cell.id(), cell_1.id());
            assert_eq!(cell.data.Map().unwrap().get("score").unwrap().U64().unwrap(), 70);
        },
        _ => {panic!("read cell 1 not accepted {:?}", cell_1_w_res)}
    }
    let cell_1_u_res = txn.update(&txn_id, &cell_1_w2).unwrap().unwrap();
    match cell_1_u_res {
        TransactionExecResult::Accepted(()) => {},
        _ => {panic!("update cell 1 not accepted")}
    }
    let cell_1_r_res = txn.read(&txn_id, &cell_1.id()).unwrap().unwrap();
    match cell_1_r_res {
        TransactionExecResult::Accepted(cell) => {
            assert_eq!(cell.id(), cell_1.id());
            assert_eq!(cell.data.Map().unwrap().get("score").unwrap().U64().unwrap(), 90);
        },
        _ => {panic!("read cell 1 not accepted {:?}", cell_1_w_res)}
    }
    let cell_1_rm_res = txn.remove(&txn_id, &cell_1.id()).unwrap().unwrap();
    match cell_1_rm_res {
        TransactionExecResult::Accepted(()) => {},
        _ => {panic!("remove cell 1 not accepted {:?}", cell_1_rm_res)}
    }
    let cell_1_r_res = txn.read(&txn_id, &cell_1.id()).unwrap().unwrap();
    match cell_1_r_res {
        TransactionExecResult::Error(ReadError::CellDoesNotExisted) => {},
        _ => {panic!("read cell 1 not accepted {:?}", cell_1_w_res)}
    }

}

#[test]
pub fn data_site_wr() {
    let server_addr = String::from("127.0.0.1:5201");
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
    let mut schema = Schema {
        id: 1,
        name: String::from("test"),
        key_field: None,
        fields: default_fields()
    };
    server.meta.schemas.new_schema(&mut schema);
    let txn = transactions::new_client(&server_addr).unwrap();
    let txn_id = txn.begin().unwrap().unwrap();
    let mut data_map = Map::<String, Value>::new();
    data_map.insert(String::from("id"), Value::I64(100));
    data_map.insert(String::from("score"), Value::U64(70));
    data_map.insert(String::from("name"), Value::String(String::from("Jack")));
    let cell_1 = Cell::new(schema.id, &Id::rand(), data_map.clone());
//    let cell_1_non_exists_read = txn.read(&txn_id, &cell_1.id()).unwrap().unwrap();
//    let cell_1_write = txn.write(&txn_id, &cell_1).unwrap().unwrap();
}