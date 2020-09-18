use crate::client;
use crate::ram::cell::Cell;
use crate::ram::schema::Field;
use crate::ram::schema::Schema;
use crate::ram::types::*;
use crate::server::*;
use dovahkiin::types::custom_types::id::Id;
use dovahkiin::types::custom_types::map::Map;
use dovahkiin::types::type_id_of;
use std::env;
use std::sync::Arc;
use test::Bencher;

extern crate test;

const DATA: &'static str = "DATA";

pub async fn init_service(port: usize) -> (Arc<NebServer>, Arc<AsyncClient>, u32) {
    let _ = env_logger::try_init();
    let server_addr = String::from(format!("127.0.0.1:{}", port));
    let server_group = String::from("bench_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 512 * 1024 * 1024,
            backup_storage: Some(format!("test-data/{}-bak", port)),
            wal_storage: Some(format!("test-data/{}-wal", port)),
            services: vec![Service::Cell, Service::Transaction, Service::RangedIndexer],
        },
        &server_addr,
        &server_group,
    )
    .await;
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
                vec![],
            )]),
            vec![],
        ),
    };

    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            &server_group,
        )
        .await
        .unwrap(),
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();
    (server, client, schema_id)
}

// #[bench]
// fn wr(b: &mut Bencher) {
//     let (_, client, schema_id) = init_service(5302);
//     let id = Id::new(0, 1);
//     let mut value = Value::Map(Map::new());
//     value[DATA] = Value::U64(2);
//     let cell = Cell::new_with_id(schema_id, &id, value);
//     b.iter(|| {
//         client.upsert_cell(cell.clone()).await;
//     })
// }

// #[bench]
// fn w(b: &mut Bencher) {
//     let (_, client, schema_id) = init_service(5306);
//     let id = Id::new(0, 1);
//     let mut value = Value::Map(Map::new());
//     value[DATA] = Value::U64(2);
//     let cell = Cell::new_with_id(schema_id, &id, value);
//     let mut i = 0;
//     b.iter(|| {
//         let mut cell = cell.clone();
//         cell.header.hash = i;
//         client.write_cell(cell).await;
//         i += 1
//     })
// }

// #[bench]
// fn txn_upsert(b: &mut Bencher) {
//     let (_, client, schema_id) = init_service(5303);
//     b.iter(|| {
//         client
//             .transaction(move |txn| {
//                 let id = Id::new(0, 1);
//                 let mut value = Value::Map(Map::new());
//                 value[DATA] = Value::U64(2);
//                 let cell = Cell::new_with_id(schema_id, &id, value);
//                 txn.upsert(cell)
//             })
//             .await
//     })
// }

// #[bench]
// fn txn_insert(b: &mut Bencher) {
//     let (_, client, schema_id) = init_service(5305);
//     let mut i = 0;
//     b.iter(|| {
//         client
//             .transaction(move |txn| {
//                 let id = Id::new(0, i);
//                 let mut value = Value::Map(Map::new());
//                 value[DATA] = Value::U64(i);
//                 let cell = Cell::new_with_id(schema_id, &id, value);
//                 txn.write(cell)
//             })
//             .await;
//         i += 1;
//     })
// }

// #[bench]
// fn noop_txn(b: &mut Bencher) {
//     let (_, client, _schema_id) = init_service(5304);
//     b.iter(|| client.transaction(move |_txn| Ok(())).await)
// }

#[bench]
fn cell_construct(b: &mut Bencher) {
    b.iter(|| {
        let id = Id::new(0, 1);
        let mut value = Value::Map(Map::new());
        value["DATA"] = Value::U64(2);
        Cell::new_with_id(1, &id, value);
    })
}

#[bench]
fn cell_clone(b: &mut Bencher) {
    let id = Id::new(0, 1);
    let mut value = Value::Map(Map::new());
    value["DATA"] = Value::U64(2);
    let cell = Cell::new_with_id(1, &id, value);
    b.iter(|| {
        let _ = cell.clone();
    })
}

#[tokio::test]
pub async fn init() {
    let _ = env_logger::try_init();
    NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 16 * 1024,
            backup_storage: None,
            wal_storage: None,
            services: vec![],
        },
        &String::from("127.0.0.1:5100"),
        &String::from("test"),
    )
    .await;
}

#[tokio::test(threaded_scheduler)]
pub async fn smoke_test() {
    let _ = env_logger::try_init();
    const DATA: &'static str = "DATA";
    let num = env::var("NEB_KV_SMOKE_TEST_ITEMS")
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .unwrap();
    let server_addr = String::from("127.0.0.1:5500");
    let server_group = String::from("smoke_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 512 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
            services: vec![Service::Cell],
        },
        &server_addr,
        &server_group,
    )
    .await;
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
                vec![],
            )]),
            vec![],
        ),
    };

    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            &server_group,
        )
        .await
        .unwrap(),
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();

    for i in 0..num {
        let client_clone = client.clone();
        // intense upsert, half delete
        let id = Id::new(0, i / 2);
        let mut value = Value::Map(Map::new());
        value[DATA] = Value::U64(i);
        let cell = Cell::new_with_id(schema_id, &id, value);
        client_clone.upsert_cell(cell).await.unwrap().unwrap();

        // read
        let read_cell = client_clone.read_cell(id).await.unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].U64().unwrap()), i);

        if i % 2 == 0 {
            client_clone.remove_cell(id).await.unwrap().unwrap();
        }
    }

    for i in 0..num {
        let id = Id::new(0, i);
        let mut value = Value::Map(Map::new());
        value[DATA] = Value::U64(i * 2);
        let cell = Cell::new_with_id(schema_id, &id, value);
        client.upsert_cell(cell).await.unwrap().unwrap();

        // verify
        let read_cell = client.read_cell(id).await.unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].U64().unwrap()), i * 2);
    }
}

#[tokio::test(threaded_scheduler)]
pub async fn smoke_test_parallel() {
    let _ = env_logger::try_init();
    const DATA: &'static str = "DATA";
    let num = env::var("NEB_KV_SMOKE_TEST_ITEMS")
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .unwrap();
    let server_addr = String::from("127.0.0.1:5301");
    let server_group = String::from("smoke_parallel_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 512 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
            services: vec![Service::Cell],
        },
        &server_addr,
        &server_group,
    )
    .await;
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
                vec![],
            )]),
            vec![],
        ),
    };

    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            &server_group,
        )
        .await
        .unwrap(),
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();

    // // Create a background thread which checks for deadlocks every 10s
    // thread::Builder::new()
    //     .name("deadlock checker".to_string())
    //     .spawn(move || loop {
    //         thread::sleep(Duration::from_secs(1));
    //         error!("{} deadlocks detected", deadlocks.len());
    //         for (i, threads) in deadlocks.iter().enumerate() {
    //             error!("Deadlock #{}", i);
    //             for t in threads {
    //                 error!("Thread Id {:#?}", t.thread_id());
    //                 error!("{:#?}", t.backtrace());
    //             }
    //         }
    //         panic!();
    //     });

    for i in 0..num {
        let client_clone = client.clone();
        // intense upsert, half delete
        let id = Id::new(0, i);
        let mut value = Value::Map(Map::new());
        value[DATA] = Value::U64(i);
        let cell = Cell::new_with_id(schema_id, &id, value);
        client_clone.upsert_cell(cell).await.unwrap().unwrap();

        // read
        let read_cell = client_clone.read_cell(id).await.unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].U64().unwrap()), i);

        if i % 2 == 0 {
            client_clone.remove_cell(id).await.unwrap().unwrap();
        }
    }

    for i in 0..num {
        if i % 2 == 0 {
            return;
        }
        let id = Id::new(0, i);
        let mut value = Value::Map(Map::new());
        value[DATA] = Value::U64(i * 2);
        let cell = Cell::new_with_id(schema_id, &id, value);
        client.upsert_cell(cell).await.unwrap().unwrap();

        // verify
        let read_cell = client.read_cell(id).await.unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].U64().unwrap()), i * 2);
    }
}

#[tokio::test(threaded_scheduler)]
pub async fn txn() {
    let _ = env_logger::try_init();
    const DATA: &'static str = "DATA";
    let num = env::var("NEB_KV_TXN_TEST_ITEMS")
        .unwrap_or("1000".to_string())
        .parse::<u64>()
        .unwrap();
    let server_addr = String::from("127.0.0.1:5303");
    let server_group = String::from("bench_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 512 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
            services: vec![Service::Cell, Service::Transaction],
        },
        &server_addr,
        &server_group,
    )
    .await;
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
                vec![],
            )]),
            vec![],
        ),
    };

    let client = Arc::new(
        client::AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr],
            &server_group,
        )
        .await
        .unwrap(),
    );
    client.new_schema_with_id(schema).await.unwrap().unwrap();

    for _ in 0..num {
        client
            .transaction(async move |txn| {
                let id = Id::new(0, 1);
                let mut value = Value::Map(Map::new());
                value[DATA] = Value::U64(2);
                let cell = Cell::new_with_id(schema_id, &id, value);
                txn.upsert(cell).await
            })
            .await
            .unwrap();
    }
}
