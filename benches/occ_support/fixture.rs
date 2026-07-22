use std::sync::Arc;

use dovahkiin::types::{Map as _, Type};
use neb::{
    client::AsyncClient,
    ram::{
        cell::OwnedCell,
        schema::{Field, Schema},
        segs::SEGMENT_SIZE,
        types::{Id, OwnedMap, OwnedValue},
    },
    server::{transactions, NebServer, ServerOptions, Service},
};
use tokio::time::{sleep, Duration};

#[derive(Clone, Copy, Debug)]
pub struct PortPlan {
    pub base: u16,
}

impl PortPlan {
    pub fn new(base: u16) -> Self {
        Self { base }
    }

    pub fn single_server(&self) -> String {
        self.single(0)
    }

    pub fn single(&self, slot: u16) -> String {
        format!("127.0.0.1:{}", self.base + slot * 10)
    }

    pub fn cluster(&self, slot: u16) -> Vec<String> {
        let cluster_base = self.base + slot * 10;
        (0..3)
            .map(|offset| format!("127.0.0.1:{}", cluster_base + offset))
            .collect()
    }
}

pub struct OccFixture {
    pub group: String,
    pub addresses: Vec<String>,
    pub servers: Vec<Arc<NebServer>>,
    pub client: Arc<AsyncClient>,
    pub txn: Arc<transactions::manager::AsyncServiceClient>,
    pub schema: Schema,
}

impl OccFixture {
    pub async fn single(address: impl Into<String>, group: impl Into<String>) -> Self {
        let address = address.into();
        let group = group.into();
        let server = NebServer::new_from_opts(
            &benchmark_server_options(),
            address.as_str(),
            group.as_str(),
            async |_| {},
        )
        .await
        .expect("start OCC benchmark server");
        Self::finish(vec![server], vec![address], group).await
    }

    pub async fn cluster(addresses: Vec<String>, group: impl Into<String>) -> Self {
        assert!(
            !addresses.is_empty(),
            "OCC benchmark cluster requires at least one address"
        );
        let group = group.into();
        let opts = benchmark_server_options();
        let mut servers = Vec::with_capacity(addresses.len());
        for address in &addresses {
            servers.push(
                NebServer::new_cluster_from_opts(
                    &opts,
                    address.as_str(),
                    &addresses,
                    group.as_str(),
                    async |_| {},
                )
                .await
                .expect("start OCC benchmark cluster member"),
            );
        }
        sleep(Duration::from_millis(500)).await;
        Self::finish(servers, addresses, group).await
    }

    async fn finish(servers: Vec<Arc<NebServer>>, addresses: Vec<String>, group: String) -> Self {
        assert!(
            !addresses.is_empty(),
            "OCC benchmark fixture requires at least one address"
        );

        let client = Arc::new(
            AsyncClient::new(
                &servers[0].rpc,
                &servers[0].membership,
                &addresses,
                group.as_str(),
            )
            .await
            .expect("create OCC benchmark client"),
        );

        let mut schema = Schema::new(
            "occ_benchmark",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("id", Type::I64),
                Field::new_unindexed("name", Type::String),
                Field::new_unindexed("score", Type::U64),
            ]),
            false,
            false,
        );
        schema.id = client
            .new_schema(schema.clone())
            .await
            .expect("register OCC benchmark schema via RPC")
            .expect("create OCC benchmark schema");

        let txn = transactions::new_async_client_for_database(&addresses[0], &group, &group)
            .await
            .expect("create OCC benchmark transaction client");

        Self {
            group,
            addresses,
            servers,
            client,
            txn,
            schema,
        }
    }

    pub async fn seed_counter(&self, id: Id, score: u64) {
        self.client
            .write_cell(counter_cell(self.schema.id, id, score, 0))
            .await
            .expect("seed OCC benchmark counter via RPC")
            .expect("write OCC benchmark counter");
    }

    pub fn ids_for_server(&self, server_id: u64, count: usize, start: u64) -> Vec<Id> {
        let mut ids = Vec::with_capacity(count);
        let mut candidate = start;
        while ids.len() < count {
            let id = Id::new(candidate, candidate.rotate_left(17));
            if self
                .client
                .locate_server_id(&id)
                .expect("route OCC benchmark id")
                == server_id
            {
                ids.push(id);
            }
            candidate += 1;
        }
        ids
    }

    pub async fn shutdown(self) {
        for server in self.servers {
            server.shutdown().await;
        }
    }
}

pub fn counter_cell(schema: u32, id: Id, score: u64, payload_bytes: usize) -> OwnedCell {
    let mut data = OwnedMap::new();
    data.insert(
        &String::from("id"),
        OwnedValue::I64(i64::try_from(id.lower).expect("counter cell id must fit in i64")),
    );
    data.insert(&String::from("score"), OwnedValue::U64(score));
    data.insert(
        &String::from("name"),
        OwnedValue::String("x".repeat(payload_bytes.max(1))),
    );
    OwnedCell::new_with_id(schema, &id, OwnedValue::Map(data))
}

fn benchmark_server_options() -> ServerOptions {
    ServerOptions {
        chunk_size: SEGMENT_SIZE,
        db_size: SEGMENT_SIZE * 4,
        tiered_config: None,
        backup_storage: None,
        wal_storage: None,
        undo_log_storage: None,
        raft_storage: None,
        services: vec![Service::Cell, Service::Transaction],
        index_enabled: false,
        enable_recovery: false,
        disable_storage_locks: true,
    }
}
