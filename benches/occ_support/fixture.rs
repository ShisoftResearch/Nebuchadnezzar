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

const PORT_SLOT_STRIDE: u16 = 10;
const PORT_CLUSTER_WIDTH: u16 = 3;
const MIN_IDS_PROBE_BUDGET: usize = 10_000;
const BENCHMARK_CHUNKS: usize = 4;
const FULL_CRITERION_SEGMENTS_PER_CHUNK: usize = 32;
const BENCHMARK_CHUNK_SIZE: usize = SEGMENT_SIZE * FULL_CRITERION_SEGMENTS_PER_CHUNK;
const BENCHMARK_DB_SIZE: usize = BENCHMARK_CHUNK_SIZE * BENCHMARK_CHUNKS;

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
        format!("127.0.0.1:{}", checked_port_plan_port(self.base, slot, 0))
    }

    pub fn cluster(&self, slot: u16) -> Vec<String> {
        (0..PORT_CLUSTER_WIDTH)
            .map(|offset| {
                format!(
                    "127.0.0.1:{}",
                    checked_port_plan_port(self.base, slot, offset)
                )
            })
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

    pub async fn score(&self, id: Id) -> u64 {
        let cell = self
            .client
            .read_cell(id)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "read OCC benchmark score RPC failed for {:?}: {:?}",
                    id, err
                )
            })
            .unwrap_or_else(|err| {
                panic!("read OCC benchmark score failed for {:?}: {:?}", id, err)
            });
        *cell.data["score"].u64().unwrap_or_else(|| {
            panic!(
                "OCC benchmark cell {:?} is missing a u64 score field: {:?}",
                id, cell.data
            )
        })
    }

    pub async fn sum_scores(&self, ids: &[Id]) -> u64 {
        let mut total = 0u64;
        for id in ids {
            total = total.checked_add(self.score(*id).await).unwrap_or_else(|| {
                panic!("OCC benchmark score sum overflow while reading {:?}", id)
            });
        }
        total
    }

    pub fn ids_for_server(&self, server_id: u64, count: usize, start: u64) -> Vec<Id> {
        if count == 0 {
            return Vec::new();
        }

        let available_server_ids: Vec<u64> =
            self.servers.iter().map(|server| server.server_id).collect();
        assert!(
            available_server_ids.contains(&server_id),
            "OCC fixture group={} does not contain server_id={} in active servers {:?}",
            self.group,
            server_id,
            available_server_ids
        );

        let max_probes = ids_probe_budget(count);
        let mut ids = Vec::with_capacity(count);
        for probe in 0..max_probes {
            let candidate = start.checked_add(probe as u64).unwrap_or_else(|| {
                panic!(
                    "OCC fixture probe overflow for group={} server_id={} count={} start={} max_probes={}",
                    self.group, server_id, count, start, max_probes
                )
            });
            let id = Id::from_parts(candidate, candidate.rotate_left(17));
            if self
                .client
                .locate_server_id(&id)
                .expect("route OCC benchmark id")
                == server_id
            {
                ids.push(id);
            }
            if ids.len() == count {
                return ids;
            }
        }

        panic!(
            "OCC fixture exhausted id probes for group={} server_id={} count={} start={} max_probes={}",
            self.group, server_id, count, start, max_probes
        );
    }

    pub async fn shutdown(self) {
        for server in self.servers {
            server.shutdown().await;
        }
    }
}

pub(crate) fn ids_probe_budget(count: usize) -> usize {
    count.saturating_mul(1024).max(MIN_IDS_PROBE_BUDGET)
}

pub fn counter_cell(schema: u32, id: Id, score: u64, payload_bytes: usize) -> OwnedCell {
    let mut data = OwnedMap::new();
    data.insert(
        &String::from("id"),
        OwnedValue::I64(i64::try_from(id.bits() & ((1 << 48) - 1)).expect("counter cell id must fit in i64")),
    );
    data.insert(&String::from("score"), OwnedValue::U64(score));
    data.insert(
        &String::from("name"),
        OwnedValue::String("x".repeat(payload_bytes.max(1))),
    );
    OwnedCell::new_with_id(schema, &id, OwnedValue::Map(data))
}

fn checked_port_plan_port(base: u16, slot: u16, offset: u16) -> u16 {
    slot.checked_mul(PORT_SLOT_STRIDE)
        .and_then(|slot_offset| base.checked_add(slot_offset))
        .and_then(|slot_base| slot_base.checked_add(offset))
        .unwrap_or_else(|| {
            panic!(
                "Port plan overflow for base={} slot={} offset={}",
                base, slot, offset
            )
        })
}

fn benchmark_server_options() -> ServerOptions {
    ServerOptions {
        chunk_size: BENCHMARK_CHUNK_SIZE,
        db_size: BENCHMARK_DB_SIZE,
        tiered_config: None,
        backup_storage: None,
        wal_storage: None,
        raft_storage: None,
        services: vec![Service::Cell, Service::Transaction],
        index_enabled: false,
        enable_recovery: false,
        disable_storage_locks: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED_BENCHMARK_CHUNKS: usize = 4;
    const MIN_FULL_CRITERION_SEGMENTS_PER_CHUNK: usize = 32;

    #[test]
    fn benchmark_server_options_preserve_topology_and_capacity_for_full_criterion_run() {
        let options = benchmark_server_options();
        let chunk_count = options.db_size / options.chunk_size;

        assert_eq!(
            chunk_count, EXPECTED_BENCHMARK_CHUNKS,
            "OCC benchmark routing requires {EXPECTED_BENCHMARK_CHUNKS} chunks, got {chunk_count}"
        );
        assert!(
            options.chunk_size >= SEGMENT_SIZE * MIN_FULL_CRITERION_SEGMENTS_PER_CHUNK,
            "full Criterion OCC runs require at least {MIN_FULL_CRITERION_SEGMENTS_PER_CHUNK} segments per chunk, got {}",
            options.chunk_size / SEGMENT_SIZE
        );
    }
}
