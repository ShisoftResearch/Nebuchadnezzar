//! Cross-site decision evidence.
//!
//! A participant that is in doubt asks its peers what became of the
//! transaction. A peer that committed answers from its live decision memory,
//! or after a restart from the COMMIT records that recovery found. These
//! tests remove that evidence on the committed peer -- by compaction, by a
//! restart, or by later decisions that evict it from memory -- while the other
//! participant is still in doubt, and then check that the transaction is
//! still all-or-nothing across both sites.
//!
//! Both sync rounds stay on in every test: this is the production default.

use super::*;
use crate::client::AsyncClient;
use crate::ram::cell::OwnedCell;
use crate::ram::chunk::{Chunk, DurableTxnStatus};
use crate::ram::entry::EntryType;
use crate::ram::schema::Schema;
use crate::ram::schema::SchemaVid;
use crate::ram::tests::default_fields;
use crate::ram::types::{Id, Map, OwnedMap, OwnedValue};

use crate::server::{NebServer, ServerOptions, Service};
use std::sync::Arc;
use std::time::Duration;

const SCHEMA_ID: u32 = 4343;
/// Slot moved to site A.
const HERE: u16 = 91;
/// Slot moved to site B.
const THERE: u16 = 92;
/// Index of the seed server. It runs the coordinator, holds no part of the
/// transaction and never restarts, so the meta raft group keeps its leader
/// and a majority while a site restarts.
const SEED: usize = 0;
/// Index of site A, which is left in doubt.
const A: usize = 1;
/// Index of site B, which is told to commit.
const B: usize = 2;
const SERVERS: usize = 3;

struct Cluster {
    group: String,
    addresses: Vec<String>,
    temp: Vec<tempfile::TempDir>,
    servers: Vec<Option<Arc<NebServer>>>,
}

fn opts_for(dir: &tempfile::TempDir) -> ServerOptions {
    ServerOptions {
        // Big enough that a transaction gets a head lease and so writes a
        // bracket; see `termination_tests::start_txn_cluster`.
        chunk_size: 64 * 1024 * 1024,
        db_size: 256 * 1024 * 1024,
        tiered_config: None,
        backup_storage: Some(dir.path().join("backup").to_string_lossy().to_string()),
        wal_storage: Some(dir.path().join("wal").to_string_lossy().to_string()),
        raft_storage: Some(dir.path().join("raft").to_string_lossy().to_string()),
        index_enabled: false,
        services: vec![Service::Cell, Service::Transaction],
        enable_recovery: true,
        disable_storage_locks: true,
    }
}

impl Cluster {
    async fn start(group: &str) -> (Cluster, Arc<AsyncClient>) {
        let _ = env_logger::try_init();
        let temp: Vec<tempfile::TempDir> = (0..SERVERS)
            .map(|_| tempfile::TempDir::new().unwrap())
            .collect();
        let addresses: Vec<String> = (0..SERVERS)
            .map(|_| crate::utils::test_port::unique_localhost_addr())
            .collect();
        let mut servers = Vec::new();
        for (address, dir) in addresses.iter().zip(temp.iter()) {
            servers.push(Some(
                NebServer::new_cluster_from_opts(
                    &opts_for(dir),
                    address,
                    &addresses,
                    group,
                    async |_| {},
                )
                .await
                .unwrap(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        let first = servers[SEED].as_ref().unwrap();
        let client = Arc::new(
            AsyncClient::new(&first.rpc, &first.membership, &addresses, group)
                .await
                .unwrap(),
        );
        client.reload_slot_owners().await;
        for server in servers.iter().flatten() {
            server.meta().schemas.debug_only_new_schema(schema());
        }
        (
            Cluster {
                group: group.to_string(),
                addresses,
                temp,
                servers,
            },
            client,
        )
    }

    fn site(&self, index: usize) -> &Arc<NebServer> {
        self.servers[index].as_ref().expect("the site is running")
    }

    async fn stop(&mut self, index: usize) {
        let server = self.servers[index].take().expect("the site is running");
        server.shutdown().await;
        drop(server);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    async fn start_site(&mut self, index: usize) {
        assert!(self.servers[index].is_none());
        let server = NebServer::new_cluster_from_opts(
            &opts_for(&self.temp[index]),
            &self.addresses[index],
            &self.addresses,
            &self.group,
            async |_| {},
        )
        .await
        .unwrap();
        // Registered locally, as at the first start: recovery does not need
        // it, the reads in these tests do.
        server.meta().schemas.debug_only_new_schema(schema());
        self.servers[index] = Some(server);
    }

    async fn restart(&mut self, index: usize) {
        self.stop(index).await;
        self.start_site(index).await;
    }

    async fn shutdown(mut self) {
        for index in 0..self.servers.len() {
            if self.servers[index].is_some() {
                self.stop(index).await;
            }
        }
    }
}

fn schema() -> Schema {
    Schema::new_with_id(
        SCHEMA_ID,
        &String::from("crosssite_schema"),
        None,
        default_fields(),
        false,
        false,
    )
}

fn cell_of(id: Id, score: u64) -> OwnedCell {
    let mut data = OwnedMap::new();
    data.insert(
        &String::from("id"),
        OwnedValue::I64((id.bits() & ((1u64 << 48) - 1)) as i64),
    );
    data.insert(&String::from("score"), OwnedValue::U64(score));
    data.insert(&String::from("name"), OwnedValue::String(String::from("x")));
    OwnedCell::new_with_id(SchemaVid(SCHEMA_ID), &id, OwnedValue::Map(data))
}

/// The score the transaction wrote, if this site holds the cell.
fn score_on(server: &NebServer, id: &Id) -> Option<u64> {
    let cell = server.chunks().read_cell(id).ok()?.to_owned();
    cell.data["score"].u64().copied()
}

/// A transaction over both sites, with the decision told to B only.
///
/// Returns the transaction id, the two ids (A's first) and the two scores.
/// After this, A holds durable entries and no COMMIT (in doubt) and B holds a
/// durable COMMIT.
async fn torn_transaction(
    cluster: &Cluster,
    client: &Arc<AsyncClient>,
    base_score: u64,
) -> (TxnId, Vec<Id>, Vec<u64>) {
    let seed_id = cluster.site(SEED).server_id;
    let a_id = cluster.site(A).server_id;
    let b_id = cluster.site(B).server_id;
    let plan = crate::migration::MigrationPlan {
        batch_cells: 8,
        ..Default::default()
    };
    for (slot, to) in [(HERE, a_id), (THERE, b_id)] {
        crate::migration::migrate_slot(client, slot as u32, seed_id, to, &plan)
            .await
            .expect("slot should move to its site");
    }
    client.reload_slot_owners().await;
    let ids = vec![Id::from_parts(HERE as u64, 1), Id::from_parts(THERE as u64, 1)];
    assert_eq!(client.locate_server_id(&ids[0]).unwrap(), a_id);
    assert_eq!(client.locate_server_id(&ids[1]).unwrap(), b_id);

    // The coordinator runs on the seed, whose slot table the migrations above
    // updated. Its death is simulated by the end fan-out limit below.
    let txn = super::new_async_client_for_database(&cluster.addresses[SEED], &cluster.group, &cluster.group)
        .await
        .unwrap();
    let tid = txn.begin().await.unwrap().unwrap();
    let scores = vec![base_score, base_score + 1];
    for (id, score) in ids.iter().zip(scores.iter()) {
        txn.write(tid.clone(), cell_of(*id, *score))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();
    let limit = super::manager::stop_end_fanout_after_server(tid.clone(), b_id);
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        cluster.site(A).chunks().durable_txn_status(&tid),
        DurableTxnStatus::Prepared,
        "test setup: site A must be in doubt"
    );
    assert_eq!(
        cluster.site(B).chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "test setup: site B must hold a durable COMMIT"
    );
    (tid, ids, scores)
}

/// What each site holds of the transaction.
fn halves(cluster: &Cluster, ids: &[Id], scores: &[u64]) -> (bool, bool) {
    let a = score_on(cluster.site(A), &ids[0]) == Some(scores[0]);
    let b = score_on(cluster.site(B), &ids[1]) == Some(scores[1]);
    (a, b)
}

fn assert_all_or_nothing(cluster: &Cluster, ids: &[Id], scores: &[u64], what: &str) {
    let (a, b) = halves(cluster, ids, scores);
    warn!("CROSSSITE {}: site A holds its half: {}, site B holds its half: {}", what, a, b);
    assert_eq!(
        a, b,
        "{}: the transaction is torn -- site A holds its half: {}, site B holds its half: {}",
        what, a, b
    );
}

fn segment_holds_commit_of(segment: &crate::ram::segs::Segment, tid: &TxnId) -> bool {
    segment.entry_iter().any(|meta| {
        if meta.entry_header.entry_type != EntryType::COMMIT {
            return false;
        }
        let content = unsafe {
            std::slice::from_raw_parts(
                meta.body_pos as *const u8,
                meta.entry_header.content_length as usize,
            )
        };
        crate::ram::bracket::decode_commit(content).map_or(false, |(txn, _)| txn == *tid)
    })
}

/// Take a head out of the pool the way rotation does, so the cleaner may
/// pick it. Same steps as the recovery tests' `rotate_out`.
fn rotate_out(chunk: &Chunk, seg_id: u64) {
    for slot in chunk.head_pool.iter().chain(chunk.blob_head_pool.iter()) {
        let _ = slot.compare_exchange(
            seg_id,
            u64::MAX,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        );
    }
    assert!(!chunk.is_active_head(seg_id));
    chunk.finish_rotated_head(seg_id);
}

/// Compact every segment of `server` that holds the COMMIT of `tid`.
///
/// This is what the cleaner does to such a segment once it is no longer a
/// head: the live cells move to a new segment and the bracket markers stay
/// behind. Done explicitly so the test does not depend on cleaner timing.
fn compact_commit_evidence(server: &NebServer, tid: &TxnId) {
    let chunks = server.chunks();
    let mut compacted = 0;
    for chunk in chunks.list.iter() {
        let holding: Vec<u64> = chunk
            .segments()
            .iter()
            .filter(|seg| segment_holds_commit_of(seg, tid))
            .map(|seg| seg.id)
            .collect();
        if holding.is_empty() {
            continue;
        }
        for seg_id in &holding {
            if chunk.is_active_head(*seg_id) {
                rotate_out(chunk, *seg_id);
            }
        }
        // A second, unrelated candidate in the same chunk: the combiner works
        // on two or more segments.
        let locality = (0u16..u16::MAX)
            .find(|l| *l as usize % chunks.list.len() == chunk.id)
            .unwrap();
        let filler = Id::from_parts(locality as u64, 900_001);
        chunks
            .write_cell(&mut cell_of(filler, 1))
            .expect("filler cell");
        let filler_seg = chunk
            .locate_segment(chunks.address_of(&filler))
            .expect("filler segment")
            .id;
        assert!(!holding.contains(&filler_seg));
        if chunk.is_active_head(filler_seg) {
            rotate_out(chunk, filler_seg);
        }
        let candidates: Vec<_> = chunk
            .segments()
            .into_iter()
            .filter(|seg| holding.contains(&seg.id) || seg.id == filler_seg)
            .collect();
        let (_, reduced) =
            crate::ram::cleaner::combine::CombinedCleaner::combine_segments(chunk, &candidates);
        assert!(reduced > 0, "test setup: the combine must run");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while chunk
            .retired_segment_files
            .load(std::sync::atomic::Ordering::Acquire)
            != 0
        {
            assert!(
                std::time::Instant::now() < deadline,
                "test setup: the combined sources were never retired"
            );
            chunk.drain_retired_segments();
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            chunk
                .segments()
                .iter()
                .all(|seg| !holding.contains(&seg.id)),
            "test setup: the segments with the COMMIT must be gone"
        );
        compacted += holding.len();
    }
    assert!(compacted > 0, "test setup: no segment held the COMMIT");
    assert_ne!(
        chunks.durable_txn_status(tid),
        DurableTxnStatus::Committed,
        "test setup: the COMMIT must be gone after compaction"
    );
}

/// Ask the live in-doubt site to resolve, the way its lease sweeper does.
///
/// A long grace, so an unreachable peer holds the transaction instead of
/// presuming abort: only a peer that ANSWERS can make site A decide here.
async fn resolve_live(cluster: &Cluster, tid: &TxnId) {
    let site = cluster
        .site(A)
        .database_runtime()
        .data_site()
        .expect("site A runs a data site");
    for _ in 0..40 {
        // The sweep the background task runs once the lease is idle: ask the
        // participants, then commit, hold, or presume abort and roll back.
        site.sweep_idle_transactions(Duration::ZERO, Duration::from_secs(300))
            .await;
        if crate::ram::chunk::transaction_lease_count(tid, &cluster.site(A).chunks().address_range())
            == 0
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("site A could not reach site B to resolve {:?}", tid);
}

fn outcome_on(cluster: &Cluster, index: usize, tid: &TxnId) -> TxnOutcome {
    cluster
        .site(index)
        .database_runtime()
        .data_site()
        .expect("the site runs a data site")
        .txn_outcome(tid)
}

/// Variant (i), restart path: B's COMMIT is compacted, B restarts, then A
/// restarts in doubt and asks B.
#[tokio::test(flavor = "multi_thread")]
async fn a_compacted_peer_commit_then_peer_restart_keeps_the_restarted_site_atomic() {
    let (mut cluster, client) = Cluster::start("crosssite_compact_restart").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 1000).await;
    drop(client);

    compact_commit_evidence(cluster.site(B), &tid);
    assert_eq!(score_on(cluster.site(B), &ids[1]), Some(scores[1]));
    cluster.restart(B).await;
    warn!(
        "CROSSSITE after compaction and restart, site B answers {:?} for {:?}",
        outcome_on(&cluster, B, &tid),
        tid
    );
    cluster.restart(A).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "compacted COMMIT, B restart, A restart");
    cluster.shutdown().await;
}

/// Variant (i), live path: B's COMMIT is compacted and B restarts, while A
/// stays live and in doubt, then resolves through its lease sweeper.
#[tokio::test(flavor = "multi_thread")]
async fn a_compacted_peer_commit_then_peer_restart_keeps_the_live_site_atomic() {
    let (mut cluster, client) = Cluster::start("crosssite_compact_live").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 2000).await;
    drop(client);

    compact_commit_evidence(cluster.site(B), &tid);
    cluster.restart(B).await;
    warn!(
        "CROSSSITE after compaction and restart, site B answers {:?} for {:?}",
        outcome_on(&cluster, B, &tid),
        tid
    );
    resolve_live(&cluster, &tid).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "compacted COMMIT, B restart, A live");
    cluster.shutdown().await;
}

/// Variant (ii), live path: B stays up with its COMMIT on disk, but later
/// decisions evict the transaction from B's decision memory. A resolves.
#[tokio::test(flavor = "multi_thread")]
async fn an_evicted_peer_decision_keeps_the_live_site_atomic() {
    let (cluster, client) = Cluster::start("crosssite_evict_live").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 3000).await;
    drop(client);

    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    cluster
        .site(B)
        .database_runtime()
        .data_site()
        .unwrap()
        .fill_decision_memory_with_later_decisions();
    warn!(
        "CROSSSITE after eviction, site B answers {:?} for {:?}; its durable status is {:?}",
        outcome_on(&cluster, B, &tid),
        tid,
        cluster.site(B).chunks().durable_txn_status(&tid)
    );
    resolve_live(&cluster, &tid).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "evicted decision, A live");
    cluster.shutdown().await;
}

/// Variant (ii), restart path: B's decision is evicted, then A restarts in
/// doubt and asks B.
#[tokio::test(flavor = "multi_thread")]
async fn an_evicted_peer_decision_keeps_the_restarted_site_atomic() {
    let (mut cluster, client) = Cluster::start("crosssite_evict_restart").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 4000).await;
    drop(client);

    cluster
        .site(B)
        .database_runtime()
        .data_site()
        .unwrap()
        .fill_decision_memory_with_later_decisions();
    warn!(
        "CROSSSITE after eviction, site B answers {:?} for {:?}",
        outcome_on(&cluster, B, &tid),
        tid
    );
    cluster.restart(A).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "evicted decision, A restart");
    cluster.shutdown().await;
}

/// Control: B restarts with its COMMIT still on disk. B answers from the
/// COMMIT that recovery found, so A commits. This is the case the current
/// design covers.
#[tokio::test(flavor = "multi_thread")]
async fn control_an_uncompacted_peer_commit_survives_the_peer_restart() {
    let (mut cluster, client) = Cluster::start("crosssite_control").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 5000).await;
    drop(client);

    cluster.restart(B).await;
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    cluster.restart(A).await;

    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "the control must commit on both sites: A {}, B {}", a, b);
    cluster.shutdown().await;
}

/// The resolved site restarts a second time. At the first restart A asks B,
/// learns COMMIT and writes its own COMMIT record. Then B forgets (eviction),
/// and A restarts again: A must not depend on B a second time.
#[tokio::test(flavor = "multi_thread")]
async fn a_resolved_site_stays_committed_through_a_second_restart() {
    let (mut cluster, client) = Cluster::start("crosssite_second_restart").await;
    let (_tid, ids, scores) = torn_transaction(&cluster, &client, 6000).await;
    drop(client);

    cluster.restart(A).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "test setup: the first restart must resolve COMMIT");

    cluster
        .site(B)
        .database_runtime()
        .data_site()
        .unwrap()
        .fill_decision_memory_with_later_decisions();
    cluster.restart(A).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "resolved site, second restart");
    cluster.shutdown().await;
}
