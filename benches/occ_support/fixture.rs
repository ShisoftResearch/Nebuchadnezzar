use std::{collections::BTreeMap, sync::Arc};

use bifrost::hlc::Hlc;
use dovahkiin::types::{Map as _, Type};
use neb::{
    client::AsyncClient,
    ram::{
        cell::{CellHeader, OwnedCell, ReadError},
        schema::{Field, Schema},
        segs::SEGMENT_SIZE,
        types::{Id, OwnedMap, OwnedValue},
    },
    server::{transactions, NebServer, ServerOptions, Service},
};
use tokio::time::{sleep, timeout, Duration};

const PORT_SLOT_STRIDE: u16 = 10;
const PORT_CLUSTER_WIDTH: u16 = 3;
const MIN_IDS_PROBE_BUDGET: usize = 10_000;
const BENCHMARK_CHUNKS: usize = 4;
const FULL_CRITERION_SEGMENTS_PER_CHUNK: usize = 32;
const BENCHMARK_CHUNK_SIZE: usize = SEGMENT_SIZE * FULL_CRITERION_SEGMENTS_PER_CHUNK;
const BENCHMARK_DB_SIZE: usize = BENCHMARK_CHUNK_SIZE * BENCHMARK_CHUNKS;
const CLUSTER_READINESS_TIMEOUT: Duration = Duration::from_secs(5);
const CLUSTER_READINESS_POLL: Duration = Duration::from_millis(10);
const CLUSTER_ROUTING_CONFIRMATIONS: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetainedRevision {
    pub id: Id,
    pub revision_ts: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryChain {
    pub id: Id,
    pub predecessors: Vec<RetainedRevision>,
    pub current_revision_ts: u64,
    pub oldest_snapshot_ts: u64,
    pub oldest_score: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionTelemetry {
    pub retained_revisions: u64,
    pub retained_bytes: u64,
    pub segment_count: u64,
}

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
        Self::single_with_history_retention(address, group, 300_000).await
    }

    pub async fn single_with_history_retention(
        address: impl Into<String>,
        group: impl Into<String>,
        history_retention_ms: u64,
    ) -> Self {
        let address = address.into();
        let group = group.into();
        let mut options = benchmark_server_options();
        options.history_retention_ms = history_retention_ms;
        let server =
            NebServer::new_from_opts(&options, address.as_str(), group.as_str(), async |_| {})
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
        wait_for_stable_cluster_routing(&servers).await;
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

    pub async fn seed_counter(&self, id: Id, score: u64) -> CellHeader {
        self.client
            .write_cell(counter_cell(self.schema.id, id, score, 0))
            .await
            .expect("seed OCC benchmark counter via RPC")
            .expect("write OCC benchmark counter")
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

    /// Assert that every seeded cluster cell is visible through the transaction
    /// manager. The caller must first synchronize the coordinator HLC with the
    /// direct-write revisions; this is deliberately one-shot rather than a
    /// wall-clock retry loop.
    pub async fn assert_transactional_visibility(&self, ids: &[Id]) {
        if self.servers.len() <= 1 || ids.is_empty() {
            return;
        }
        let tid = self
            .txn
            .begin()
            .await
            .expect("begin cluster transactional visibility RPC")
            .expect("begin cluster transactional visibility");
        let reads = futures::future::join_all(
            ids.iter()
                .map(|id| async { (*id, self.txn.read(tid.clone(), *id).await) }),
        )
        .await;
        let abort = self.txn.abort(tid).await;
        assert!(
            matches!(abort, Ok(Ok(transactions::AbortResult::Success(_)))),
            "abort after cluster visibility read: {abort:?}"
        );
        for (id, read) in reads {
            assert!(
                matches!(read, Ok(Ok(transactions::TxnExecResult::Accepted(_)))),
                "transactional visibility read for {id:?}: {read:?}"
            );
        }
    }

    /// Advance the transaction coordinator past revisions installed by direct
    /// distributed seed writes. A transaction snapshot is strict: if a remote
    /// seed's HLC is ahead of the coordinator's first transaction ID, that
    /// correctly looks absent until the coordinator observes the revision.
    pub fn observe_distributed_seed_revisions<I>(&self, revision_timestamps: I) -> u64
    where
        I: IntoIterator<Item = u64>,
    {
        if self.servers.len() <= 1 {
            return 0;
        }
        let max_revision_ts = revision_timestamps
            .into_iter()
            .max()
            .expect("distributed benchmark seed must include a revision");
        let coordinator = &self.servers[0];
        let observed = coordinator.hlc.observe(Hlc {
            ts: max_revision_ts,
            node: coordinator.hlc.node(),
        });
        assert!(
            observed.ts > max_revision_ts,
            "transaction coordinator HLC must advance past distributed seed revision"
        );
        observed.ts
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
            let id = Id::new(candidate, candidate.rotate_left(17));
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

    pub fn retention_telemetry(&self, revisions: &[RetainedRevision]) -> RetentionTelemetry {
        let mut retained_revisions = 0u64;
        let mut retained_bytes = 0u64;
        for revision in revisions {
            let server = self
                .servers
                .iter()
                .find(|server| {
                    self.client
                        .locate_server_id(&revision.id)
                        .is_ok_and(|server_id| server_id == server.server_id)
                })
                .unwrap_or_else(|| {
                    panic!(
                        "retained revision {:?}@{} did not route to a fixture server",
                        revision.id, revision.revision_ts
                    )
                });
            let chunks = server.chunks();
            let Some(location) = chunks.history_location(&revision.id, revision.revision_ts) else {
                continue;
            };
            let chunk = chunks.locate_chunk_by_partition(revision.id.higher);
            let cell = chunks
                .read_cell_at(&revision.id, location)
                .unwrap_or_else(|error| {
                    panic!(
                        "read retained revision {:?}@{} at {}: {:?}",
                        revision.id, revision.revision_ts, location, error
                    )
                });
            let bytes = cell
                .plan_write(chunk)
                .unwrap_or_else(|error| {
                    panic!(
                        "size retained revision {:?}@{}: {:?}",
                        revision.id, revision.revision_ts, error
                    )
                })
                .total_size();
            retained_revisions = retained_revisions.saturating_add(1);
            retained_bytes = retained_bytes.saturating_add(u64::from(bytes));
        }
        let segment_count = self
            .servers
            .iter()
            .flat_map(|server| server.chunks().list.iter())
            .map(|chunk| u64::try_from(chunk.segment_ids().len()).unwrap_or(u64::MAX))
            .sum();
        RetentionTelemetry {
            retained_revisions,
            retained_bytes,
            segment_count,
        }
    }

    pub async fn expire_retained_revisions_and_clean(&self) {
        let retention_ms = self
            .servers
            .first()
            .expect("OCC fixture must retain a server")
            .chunks()
            .history_retention_ms
            .max(1);
        sleep(Duration::from_millis(retention_ms.saturating_add(1))).await;
        for server in &self.servers {
            for chunk in &server.chunks().list {
                let _ = neb::ram::cleaner::Cleaner::clean(chunk, true, true);
            }
        }
    }

    pub async fn wait_for_history_expiration(&self, chain: &HistoryChain) {
        let retention_ms = self
            .servers
            .first()
            .expect("OCC fixture must retain a server")
            .chunks()
            .history_retention_ms;
        sleep(Duration::from_millis(retention_ms.saturating_add(1))).await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let server = self
                .servers
                .iter()
                .find(|server| {
                    self.client
                        .locate_server_id(&chain.id)
                        .is_ok_and(|server_id| server_id == server.server_id)
                })
                .expect("history chain must route to a fixture server");
            match server
                .chunks()
                .read_cell_snapshot(&chain.id, chain.oldest_snapshot_ts)
            {
                Err(ReadError::SnapshotTooOld) => return,
                result if tokio::time::Instant::now() < deadline => {
                    let _ = result;
                    sleep(Duration::from_millis(1)).await;
                }
                result => panic!(
                    "history {:?} did not expire after {} ms: {:?}",
                    chain.id, retention_ms, result
                ),
            }
        }
    }
}

async fn wait_for_stable_cluster_routing(servers: &[Arc<NebServer>]) {
    let expected_server_ids = servers
        .iter()
        .map(|server| server.server_id)
        .collect::<Vec<_>>();
    timeout(CLUSTER_READINESS_TIMEOUT, async {
        let mut previous: Option<Vec<(Id, u64)>> = None;
        let mut confirmations = 0;
        loop {
            let mut candidates = BTreeMap::new();
            for partition in 1..=8_192u64 {
                let id = Id::new(partition, partition.rotate_left(17));
                if let Some(server_id) = servers[0].get_server_id_by_id(&id) {
                    candidates.entry(server_id).or_insert(id);
                }
                if candidates.len() == expected_server_ids.len() {
                    break;
                }
            }
            let candidate = (candidates.len() == expected_server_ids.len()
                && expected_server_ids
                    .iter()
                    .all(|server_id| candidates.contains_key(server_id))
                && candidates.iter().all(|(server_id, id)| {
                    servers
                        .iter()
                        .all(|server| server.get_server_id_by_id(id) == Some(*server_id))
                }))
            .then(|| {
                candidates
                    .into_iter()
                    .map(|(server_id, id)| (id, server_id))
                    .collect()
            });
            if candidate.is_some() && candidate == previous {
                confirmations += 1;
            } else {
                previous = candidate;
                confirmations = usize::from(previous.is_some());
            }
            if confirmations >= CLUSTER_ROUTING_CONFIRMATIONS {
                return;
            }
            sleep(CLUSTER_READINESS_POLL).await;
        }
    })
    .await
    .expect("cluster routing should converge on every active server with identical ownership maps");
}

pub(crate) fn ids_probe_budget(count: usize) -> usize {
    count.saturating_mul(1024).max(MIN_IDS_PROBE_BUDGET)
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
        history_retention_ms: 300_000,
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
