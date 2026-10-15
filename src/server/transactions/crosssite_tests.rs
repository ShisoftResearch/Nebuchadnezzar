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
    torn_transaction_with(cluster, client, base_score, 0).await
}

/// `torn_transaction`, with `bulk_on_a` extra 512 KiB cells written on A in
/// the same transaction. They keep A's bracket segment mostly live, so the
/// background cleaner does not pick it.
async fn torn_transaction_with(
    cluster: &Cluster,
    client: &Arc<AsyncClient>,
    base_score: u64,
    bulk_on_a: u64,
) -> (TxnId, Vec<Id>, Vec<u64>) {
    let (txn, tid, ids, scores) =
        prepared_transaction(cluster, client, base_score, bulk_on_a).await;
    let limit =
        super::manager::stop_end_fanout_after_server(tid.clone(), cluster.site(B).server_id);
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

/// A transaction over both sites, prepared: both sites hold durable entries
/// and no decision yet. Returns the coordinator client too.
async fn prepared_transaction(
    cluster: &Cluster,
    client: &Arc<AsyncClient>,
    base_score: u64,
    bulk_on_a: u64,
) -> (
    Arc<super::manager::AsyncServiceClient>,
    TxnId,
    Vec<Id>,
    Vec<u64>,
) {
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
    let ids = vec![
        Id::from_parts(HERE as u64, 1),
        Id::from_parts(THERE as u64, 1),
    ];
    assert_eq!(client.locate_server_id(&ids[0]).unwrap(), a_id);
    assert_eq!(client.locate_server_id(&ids[1]).unwrap(), b_id);

    // The coordinator runs on the seed, whose slot table the migrations above
    // updated. Its death is simulated by the end fan-out limit below.
    let txn = super::new_async_client_for_database(
        &cluster.addresses[SEED],
        &cluster.group,
        &cluster.group,
    )
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
    for n in 0..bulk_on_a {
        let mut cell = cell_of(Id::from_parts(HERE as u64, 100 + n), base_score);
        if let OwnedValue::Map(map) = &mut cell.data {
            map.insert(
                &String::from("name"),
                OwnedValue::String("b".repeat(512 * 1024)),
            );
        }
        txn.write(tid.clone(), cell).await.unwrap().unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();
    (txn, tid, ids, scores)
}

/// What each site holds of the transaction.
fn halves(cluster: &Cluster, ids: &[Id], scores: &[u64]) -> (bool, bool) {
    let a = score_on(cluster.site(A), &ids[0]) == Some(scores[0]);
    let b = score_on(cluster.site(B), &ids[1]) == Some(scores[1]);
    (a, b)
}

fn assert_all_or_nothing(cluster: &Cluster, ids: &[Id], scores: &[u64], what: &str) {
    let (a, b) = halves(cluster, ids, scores);
    warn!(
        "CROSSSITE {}: site A holds its half: {}, site B holds its half: {}",
        what, a, b
    );
    assert_eq!(
        a, b,
        "{}: the transaction is torn -- site A holds its half: {}, site B holds its half: {}",
        what, a, b
    );
}

fn segment_holds_begin_of(segment: &crate::ram::segs::Segment, tid: &TxnId) -> bool {
    segment.entry_iter().any(|meta| {
        if meta.entry_header.entry_type != EntryType::BEGIN {
            return false;
        }
        let content = unsafe {
            std::slice::from_raw_parts(
                meta.body_pos as *const u8,
                meta.entry_header.content_length as usize,
            )
        };
        crate::ram::bracket::decode_begin(content).map_or(false, |txn| txn == *tid)
    })
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

/// One combine of the segments of `chunk` that hold the COMMIT of `tid`.
enum CompactAttempt {
    /// No segment of this chunk holds the COMMIT (any more).
    Gone,
    /// The combiner refused, and every such segment is evidence-pinned.
    Pinned,
    /// The combiner did not run for another reason: a reader, or the
    /// background cleaner, held a segment. Try again.
    Busy,
}

fn compact_attempt(
    chunks: &crate::ram::chunk::Chunks,
    chunk: &Chunk,
    tid: &TxnId,
) -> CompactAttempt {
    let holding: Vec<u64> = chunk
        .segments()
        .iter()
        .filter(|seg| segment_holds_commit_of(seg, tid))
        .map(|seg| seg.id)
        .collect();
    if holding.is_empty() {
        return CompactAttempt::Gone;
    }
    for seg_id in &holding {
        if chunk.is_active_head(*seg_id) {
            rotate_out(chunk, *seg_id);
        }
    }
    // A second, unrelated candidate in the same chunk: the combiner works on
    // two or more segments.
    let locality = (0u16..u16::MAX)
        .find(|l| *l as usize % chunks.list.len() == chunk.id)
        .unwrap();
    static FILLERS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(900_001);
    let filler = Id::from_parts(
        locality as u64,
        FILLERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    );
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
    drop(candidates);
    if reduced == 0 {
        let pinned = holding.iter().all(|seg_id| {
            chunk
                .segments()
                .iter()
                .any(|seg| seg.id == *seg_id && chunk.is_evidence_pinned(seg.seq_id))
        });
        return if pinned {
            CompactAttempt::Pinned
        } else {
            CompactAttempt::Busy
        };
    }
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
    CompactAttempt::Busy
}

/// Ask the cleaner to compact every segment of `server` that holds the
/// COMMIT of `tid`, and return whether the COMMIT is gone afterwards.
///
/// This is what the cleaner does to such a segment once it is no longer a
/// head: the live cells move to a new segment and the bracket markers stay
/// behind. Done explicitly so the test does not depend on cleaner timing. The
/// segments are handed to the combiner directly; the combiner itself decides
/// which of them it may take. The background cleaner runs too, so a round
/// that it disturbs is tried again.
fn compact_commit_evidence(server: &NebServer, tid: &TxnId) -> bool {
    let chunks = server.chunks();
    if chunks.durable_txn_status(tid) != DurableTxnStatus::Committed {
        // The background cleaner was first.
        warn!("CROSSSITE the COMMIT of {:?} was already compacted", tid);
        return true;
    }
    for _ in 0..50 {
        let attempts: Vec<CompactAttempt> = chunks
            .list
            .iter()
            .map(|chunk| compact_attempt(chunks, chunk, tid))
            .collect();
        if attempts.iter().all(|a| matches!(a, CompactAttempt::Gone)) {
            break;
        }
        if attempts
            .iter()
            .all(|a| matches!(a, CompactAttempt::Gone | CompactAttempt::Pinned))
        {
            warn!(
                "CROSSSITE compaction of the COMMIT segments of {:?}: refused, pinned",
                tid
            );
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let gone = chunks.durable_txn_status(tid) != DurableTxnStatus::Committed;
    warn!(
        "CROSSSITE compaction of the COMMIT segments of {:?}: COMMIT removed: {}",
        tid, gone
    );
    assert!(
        gone,
        "test setup: the combiner never took the unpinned COMMIT segments"
    );
    gone
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
        if crate::ram::chunk::transaction_lease_count(
            tid,
            &cluster.site(A).chunks().address_range(),
        ) == 0
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

    assert_all_or_nothing(
        &cluster,
        &ids,
        &scores,
        "compacted COMMIT, B restart, A restart",
    );
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

    assert_all_or_nothing(
        &cluster,
        &ids,
        &scores,
        "compacted COMMIT, B restart, A live",
    );
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
    assert!(
        a && b,
        "the control must commit on both sites: A {}, B {}",
        a,
        b
    );
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

fn retained_on(cluster: &Cluster, index: usize) -> (usize, usize) {
    cluster.site(index).chunks().evidence().counts()
}

fn pinned_on(cluster: &Cluster, index: usize) -> usize {
    cluster
        .site(index)
        .chunks()
        .list
        .iter()
        .map(|chunk| chunk.evidence_pinned_segments())
        .sum()
}

/// Run release rounds until the site no longer retains `tid`. The background
/// loop runs them too; either may release it.
async fn wait_released(cluster: &Cluster, index: usize, tid: &TxnId) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while cluster.site(index).chunks().holds_commit_evidence(tid) {
        assert!(
            std::time::Instant::now() < deadline,
            "the evidence of {:?} was never released",
            tid
        );
        release_round(cluster, index).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn release_round(cluster: &Cluster, index: usize) -> usize {
    cluster
        .site(index)
        .database_runtime()
        .data_site()
        .expect("the site runs a data site")
        .release_decision_evidence()
        .await
}

/// Liveness: the evidence goes once every participant has decided, and the
/// cleaner can then compact the COMMIT segment.
///
/// While A is in doubt, a release round keeps the evidence and the cleaner
/// does not take the segment. After A resolves, one round releases it.
#[tokio::test(flavor = "multi_thread")]
async fn the_peer_evidence_is_released_once_every_participant_decided() {
    let (cluster, client) = Cluster::start("crosssite_release").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 7000).await;
    drop(client);

    assert_eq!(retained_on(&cluster, B).0, 1, "B retains the commit");
    assert!(pinned_on(&cluster, B) > 0, "B pins its COMMIT segment");
    assert_eq!(
        release_round(&cluster, B).await,
        0,
        "A is in doubt: keep it"
    );
    assert!(
        !compact_commit_evidence(cluster.site(B), &tid),
        "the cleaner must not take the COMMIT while A is in doubt"
    );

    resolve_live(&cluster, &tid).await;
    assert_all_or_nothing(&cluster, &ids, &scores, "release, A resolved live");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "A must commit what B was told to commit");

    wait_released(&cluster, B, &tid).await;
    assert_eq!(retained_on(&cluster, B), (0, 0));
    assert_eq!(pinned_on(&cluster, B), 0);
    assert!(
        compact_commit_evidence(cluster.site(B), &tid),
        "once released, the cleaner compacts the COMMIT segment"
    );
    assert_eq!(score_on(cluster.site(B), &ids[1]), Some(scores[1]));
    cluster.shutdown().await;
}

/// Liveness after a restart: a restarted site retains every commit recovery
/// found, because it lost the participant sets. Once the membership view has
/// settled and every member reports nothing open, it releases them all.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_site_releases_recovered_evidence_when_no_member_holds_it_open() {
    let (mut cluster, client) = Cluster::start("crosssite_release_restart").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 8000).await;
    drop(client);
    resolve_live(&cluster, &tid).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "test setup: A must commit");

    cluster.restart(B).await;
    assert!(
        cluster.site(B).chunks().holds_commit_evidence(&tid),
        "a restarted site retains what recovery found"
    );
    assert!(pinned_on(&cluster, B) > 0);
    wait_released(&cluster, B, &tid).await;
    assert_eq!(retained_on(&cluster, B), (0, 0));
    assert_eq!(pinned_on(&cluster, B), 0);
    cluster.shutdown().await;
}

/// The resolved site's own COMMIT record must outlive the peer's evidence.
///
/// After a restart, A resolves the transaction and writes its COMMIT into a
/// new segment; the BEGIN stays in the old one. B then releases its evidence
/// (A no longer holds the transaction open) and forgets the decision. If the
/// cleaner could now compact A's new segment, A's next restart would find the
/// BEGIN without a COMMIT, ask B, hear Unknown and drop its half.
#[tokio::test(flavor = "multi_thread")]
async fn a_resolved_sites_own_commit_record_outlives_the_peer_evidence() {
    let (mut cluster, client) = Cluster::start("crosssite_fragile").await;
    let (tid, ids, scores) = torn_transaction_with(&cluster, &client, 9000, 6).await;
    drop(client);

    cluster.restart(A).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "test setup: the restart must resolve COMMIT");

    // B lets go: A decided, and the decision memory moves on.
    wait_released(&cluster, B, &tid).await;
    cluster
        .site(B)
        .database_runtime()
        .data_site()
        .unwrap()
        .fill_decision_memory_with_later_decisions();
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Unknown);

    // The cleaner reaches A's resolved COMMIT record.
    let removed = compact_commit_evidence(cluster.site(A), &tid);
    warn!(
        "CROSSSITE resolved COMMIT record on A removed by compaction: {}",
        removed
    );
    assert!(
        cluster.site(A).chunks().list.iter().any(|chunk| chunk
            .segments()
            .iter()
            .any(|seg| segment_holds_begin_of(seg, &tid))),
        "test setup: A's old segment with the open BEGIN must still be there"
    );
    cluster.restart(A).await;

    assert_all_or_nothing(
        &cluster,
        &ids,
        &scores,
        "resolved record, peer forgot, A restart",
    );
    cluster.shutdown().await;
}

/// A COMMIT refused on one site must not tear the transaction.
///
/// The coordinator sends the decision to every site at once. Site A closes
/// its bracket; site B cannot (a full store or a failed sync). B must not
/// roll back: A already committed. B keeps the transaction decided and
/// finishes it later, through its lease sweeper, and the commit survives a
/// restart of B.
#[tokio::test(flavor = "multi_thread")]
async fn a_commit_refused_on_one_site_does_not_tear_the_transaction() {
    let (mut cluster, client) = Cluster::start("crosssite_refused").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 10_000, 0).await;
    drop(client);

    crate::ram::chunk::refuse_next_commit_for_test(cluster.site(B).chunks().base_addr, tid.clone());
    let result = txn.commit(tid.clone()).await;
    warn!(
        "CROSSSITE commit with a refusal on site B returned {:?}",
        result
    );
    assert_eq!(
        cluster.site(A).chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "test setup: site A must commit"
    );
    assert_ne!(
        cluster.site(B).chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "test setup: site B must have refused its COMMIT"
    );
    assert_all_or_nothing(&cluster, &ids, &scores, "refused COMMIT on B, right after");

    // B's sweeper finishes the decided transaction.
    let site = cluster.site(B).database_runtime().data_site().unwrap();
    site.sweep_idle_transactions(Duration::ZERO, Duration::from_secs(300))
        .await;
    assert_eq!(
        cluster.site(B).chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "site B must finish the commit it was told to make"
    );
    cluster.restart(B).await;
    assert_all_or_nothing(
        &cluster,
        &ids,
        &scores,
        "refused COMMIT on B, after B restart",
    );
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "the transaction must be committed on both sites");
    cluster.shutdown().await;
}

/// A site whose COMMIT was refused still holds the decision, and must say so.
///
/// The coordinator reaches B only, and B's COMMIT is refused. A is in doubt
/// and asks B. B was told to commit, so it must answer `Committed`: an
/// `InDoubt` answer makes A presume abort while B later finishes the commit.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_with_a_refused_commit_still_answers_committed() {
    let (cluster, client) = Cluster::start("crosssite_refused_answer").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 11_000, 0).await;
    drop(client);

    crate::ram::chunk::refuse_next_commit_for_test(cluster.site(B).chunks().base_addr, tid.clone());
    let limit =
        super::manager::stop_end_fanout_after_server(tid.clone(), cluster.site(B).server_id);
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    assert_ne!(
        cluster.site(B).chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "test setup: site B must have refused its COMMIT"
    );
    warn!(
        "CROSSSITE site B with a refused COMMIT answers {:?}",
        outcome_on(&cluster, B, &tid)
    );

    resolve_live(&cluster, &tid).await;
    let site = cluster.site(B).database_runtime().data_site().unwrap();
    site.sweep_idle_transactions(Duration::ZERO, Duration::from_secs(300))
        .await;
    for index in [A, B] {
        assert_eq!(
            cluster.site(index).chunks().durable_txn_status(&tid),
            DurableTxnStatus::Committed,
            "site {} must hold a durable COMMIT",
            index
        );
    }
    assert_all_or_nothing(&cluster, &ids, &scores, "refused COMMIT on B, A asked B");
    cluster.shutdown().await;
}
