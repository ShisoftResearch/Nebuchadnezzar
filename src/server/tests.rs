use crate::ram::schema::Field;
use crate::ram::schema::Schema;
use crate::ram::types::*;
use crate::server::*;
use crate::{client, ram::cell::OwnedCell};
use dovahkiin::types::custom_types::id::Id;
use futures::stream::FuturesUnordered;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::env;
use std::sync::Arc;
use test::Bencher;
use tokio_stream::StreamExt;

#[bench]
fn cell_construct(b: &mut Bencher) {
    b.iter(|| {
        let id = Id::new(0, 1);
        let mut value = OwnedValue::Map(OwnedMap::new());
        value["DATA"] = OwnedValue::U64(2);
        OwnedCell::new_with_id(1, &id, value);
    })
}

#[bench]
fn cell_clone(b: &mut Bencher) {
    let id = Id::new(0, 1);
    let mut value = OwnedValue::Map(OwnedMap::new());
    value["DATA"] = OwnedValue::U64(2);
    let cell = OwnedCell::new_with_id(1, &id, value);
    b.iter(|| {
        let _ = cell.clone();
    })
}

#[tokio::test]
#[ignore]
pub async fn init() {
    let _ = env_logger::try_init();
    NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            memory_size: 16 * 1024,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![],
        },
        &String::from("127.0.0.1:5100"),
        &String::from("test"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
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
            index_enabled: false,
            services: vec![Service::Cell],
        },
        &server_addr,
        &server_group,
    )
    .await;
    let schema_id = 123;
    let schema = Schema::new_with_id(
        schema_id,
        &String::from("schema"),
        None,
        Field::new(
            "*",
            Type::Map,
            false,
            false,
            Some(vec![Field::new(
                DATA,
                Type::U64,
                false,
                false,
                None,
                vec![],
            )]),
            vec![],
        ),
        false,
        false,
    );

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
        // intense upsert, half delete
        let id = Id::new(1, i / 2);
        let mut value = OwnedValue::Map(OwnedMap::new());
        value[DATA] = OwnedValue::U64(i);
        let cell = OwnedCell::new_with_id(schema_id, &id, value);
        client.upsert_cell(cell).await.unwrap().unwrap();

        // read
        let read_cell = client.read_cell(id).await.unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].u64().unwrap()), i);

        if i % 2 == 0 {
            client.remove_cell(id).await.unwrap().unwrap();
        }
    }

    for i in 0..num {
        let id = Id::new(1, i);
        let mut value = OwnedValue::Map(OwnedMap::new());
        value[DATA] = OwnedValue::U64(i * 2);
        let cell = OwnedCell::new_with_id(schema_id, &id, value);
        client.upsert_cell(cell).await.unwrap().unwrap();

        // verify
        let read_cell = client.read_cell(id).await.unwrap().unwrap();
        assert_eq!(*(read_cell.data[DATA].u64().unwrap()), i * 2);
    }
}

#[tokio::test(flavor = "multi_thread")]
pub async fn smoke_test_parallel() {
    let _ = env_logger::try_init();
    const DATA: &'static str = "DATA";
    const ARRAY: &'static str = "ARRAY";
    let num = env::var("NEB_KV_SMOKE_TEST_ITEMS")
        .unwrap_or("1024".to_string())
        .parse::<u64>()
        .unwrap();
    let server_addr = String::from("127.0.0.1:5301");
    let server_group = String::from("smoke_parallel_test");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 4,
            memory_size: 16 * 1024 * 1024 * 1024,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
        },
        &server_addr,
        &server_group,
    )
    .await;
    let schema_id = 123;
    let schema = Schema::new_with_id(
        schema_id,
        &String::from("schema"),
        None,
        Field::new(
            "*",
            Type::Map,
            false,
            false,
            Some(vec![
                Field::new(DATA, Type::U64, false, false, None, vec![]),
                Field::new(ARRAY, Type::U64, false, true, None, vec![]),
            ]),
            vec![],
        ),
        false,
        false,
    );

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

    let num_tasks = 1024;
    let mut tasks: FuturesUnordered<_> = FuturesUnordered::new();

    info!("Schduling test cases");

    for i in 0..num_tasks {
        let client_clone = client.clone();
        info!("Schduling test task {}", i);
        tasks.push(tokio::spawn(async move {
            let id = Id::new(1, i as u64);
            let mut rng = SmallRng::from_entropy();
            for j in 0..num {
                debug!("Smoke test i {}, j {}", i, j);
                if j > 1 && rng.gen_range(0..8) == 4 {
                    debug!("Removing i {}, j {}", i, j);
                    client_clone.remove_cell(id).await.unwrap().unwrap();
                }
                let mut value = OwnedValue::Map(OwnedMap::new());
                value[DATA] = OwnedValue::U64(j);
                value[ARRAY] = (1..rng.gen_range(1..1024)).collect::<Vec<u64>>().value();
                let cell = OwnedCell::new_with_id(schema_id, &id, value);
                debug!("Upsert {:?}, i {}, j {}", id, i, j);
                client_clone.upsert_cell(cell).await.unwrap().unwrap();
                // read
                let read_cell = client_clone
                    .read_cell(id)
                    .await
                    .unwrap()
                    .expect(&format!("Finally expecting {:?} at {}", id, j));
                assert_eq!(
                    *(read_cell.data[DATA].u64().unwrap()),
                    j,
                    "Parallel final read i {}, j {}",
                    i,
                    j
                );
                debug!("Iteration i {}, j {} completed", i, j);
            }
            true
        }));
    }
    info!("Waiting for all tasks to finish");
    while let Some(r) = tasks.next().await {
        assert!(r.unwrap());
    }
}

#[tokio::test(flavor = "multi_thread")]
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
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
        },
        &server_addr,
        &server_group,
    )
    .await;
    let schema_id = 123;
    let schema = Schema::new_with_id(
        schema_id,
        &String::from("schema"),
        None,
        Field::new(
            "*",
            Type::Map,
            false,
            false,
            Some(vec![Field::new(
                DATA,
                Type::U64,
                false,
                false,
                None,
                vec![],
            )]),
            vec![],
        ),
        false,
        false,
    );

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
                let mut value = OwnedValue::Map(OwnedMap::new());
                value[DATA] = OwnedValue::U64(2);
                let cell = OwnedCell::new_with_id(schema_id, &id, value);
                txn.upsert(cell).await
            })
            .await
            .unwrap();
    }
}
