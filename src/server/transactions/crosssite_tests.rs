//! Cross-site decision evidence and the settle lifetime (D28).
//!
//! A participant that is in doubt asks its peers what became of the
//! transaction. A peer that committed answers from its live decision memory,
//! or from the evidence it keeps for every unsettled multi-site commit. These
//! tests try to remove that evidence on the committed peer -- by compaction,
//! by a restart, by later decisions that evict it from memory, or by a power
//! loss -- while the other participant is still in doubt, and then check that
//! the transaction is still all-or-nothing across both sites.
//!
//! Both sync rounds stay on in every test: this is the production default.
//!
//! Roles: the seed runs the coordinator and holds no part of the
//! transaction. Site B has the lower server id of the two participants; the
//! coordinator hook `stop_end_fanout_after_commit_point` tells B only, so A
//! is left in doubt.

use super::*;
use crate::client::AsyncClient;
use crate::ram::cell::OwnedCell;
use crate::ram::chunk::{Chunk, DurableTxnStatus};
use crate::ram::entry::EntryType;
use crate::ram::schema::Schema;
use crate::ram::schema::SchemaVid;
use crate::ram::segs::Segment;
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
/// A second slot on site A, in another chunk than `HERE` (93 % 4 != 91 % 4).
const HERE_TOO: u16 = 93;
/// Index of the seed server. It runs the coordinator, holds no part of the
/// transaction and never restarts, so the meta raft group keeps its leader
/// and a majority while a site restarts.
const SEED: usize = 0;
/// Index of site A, which is left in doubt.
const A: usize = 1;
/// Index of site B, which is told to commit. It has the lower server id.
const B: usize = 2;

struct Cluster {
    group: String,
    addresses: Vec<String>,
    temp: Vec<tempfile::TempDir>,
    servers: Vec<Option<Arc<NebServer>>>,
    services: Vec<Vec<Service>>,
}

fn opts_for(dir: &tempfile::TempDir, services: &[Service]) -> ServerOptions {
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
        services: services.to_vec(),
        enable_recovery: true,
        disable_storage_locks: true,
    }
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    if !from.exists() {
        return;
    }
    std::fs::create_dir_all(to).unwrap();
    for item in std::fs::read_dir(from).unwrap() {
        let item = item.unwrap();
        let target = to.join(item.file_name());
        if item.file_type().unwrap().is_dir() {
            copy_dir(&item.path(), &target);
        } else {
            std::fs::copy(item.path(), target).unwrap();
        }
    }
}

impl Cluster {
    async fn start(group: &str) -> (Cluster, Arc<AsyncClient>) {
        Self::start_with(group, Vec::new()).await
    }

    /// The seed, A and B, then one more member per entry of `extra`, with
    /// those services.
    async fn start_with(group: &str, extra: Vec<Vec<Service>>) -> (Cluster, Arc<AsyncClient>) {
        let _ = env_logger::try_init();
        let mut services = vec![vec![Service::Cell, Service::Transaction]; 3];
        services.extend(extra);
        let count = services.len();
        let temp: Vec<tempfile::TempDir> = (0..count)
            .map(|_| tempfile::TempDir::new().unwrap())
            .collect();
        let mut addresses: Vec<String> = (0..count)
            .map(|_| crate::utils::test_port::unique_localhost_addr())
            .collect();
        // Server ids are hashes of the addresses: give B the lower one.
        if bifrost_hasher::hash_str(&addresses[B]) > bifrost_hasher::hash_str(&addresses[A]) {
            addresses.swap(A, B);
        }
        let mut servers = Vec::new();
        for index in 0..count {
            servers.push(Some(
                NebServer::new_cluster_from_opts(
                    &opts_for(&temp[index], &services[index]),
                    &addresses[index],
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
        let cluster = Cluster {
            group: group.to_string(),
            addresses,
            temp,
            servers,
            services,
        };
        assert!(cluster.site(B).server_id < cluster.site(A).server_id);
        (cluster, client)
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
        // A start waits for every listed peer to be a member, so a member
        // that is down is not listed.
        let peers: Vec<String> = (0..self.addresses.len())
            .filter(|peer| *peer == index || self.servers[*peer].is_some())
            .map(|peer| self.addresses[peer].clone())
            .collect();
        let server = NebServer::new_cluster_from_opts(
            &opts_for(&self.temp[index], &self.services[index]),
            &self.addresses[index],
            &peers,
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

    /// A power loss of site `index` at this instant, then a start.
    ///
    /// The WAL and backup files are copied as a power loss leaves them
    /// (`segs::power_loss_model`): each WAL holds only the bytes a sync
    /// covered. The raft directory is taken after a clean stop, so the
    /// membership is not part of the experiment.
    async fn power_loss_restart(&mut self, index: usize) {
        let image = tempfile::TempDir::new().unwrap();
        let live = self.temp[index].path().to_path_buf();
        crate::ram::segs::power_loss_model::copy_store_as_after_power_loss(
            &live.join("wal"),
            &live.join("backup"),
            &image.path().join("wal"),
            &image.path().join("backup"),
        )
        .unwrap();
        self.stop(index).await;
        copy_dir(&live.join("raft"), &image.path().join("raft"));
        self.temp[index] = image;
        self.start_site(index).await;
    }

    /// A power loss of every site in `sites` at one instant: the images of
    /// all of them are taken first. Then each site restarts from its image,
    /// one at a time and in the order given, so the meta raft group keeps a
    /// majority (stopping most members at once trips a bifrost raft
    /// assertion, `bifrost/src/raft/mod.rs:2987`).
    async fn power_cut_all(&mut self, sites: &[usize]) {
        let mut images = Vec::new();
        for index in sites {
            let image = tempfile::TempDir::new().unwrap();
            let live = self.temp[*index].path().to_path_buf();
            crate::ram::segs::power_loss_model::copy_store_as_after_power_loss(
                &live.join("wal"),
                &live.join("backup"),
                &image.path().join("wal"),
                &image.path().join("backup"),
            )
            .unwrap();
            images.push(image);
        }
        for (index, image) in sites.iter().zip(images) {
            self.stop(*index).await;
            copy_dir(
                &self.temp[*index].path().join("raft"),
                &image.path().join("raft"),
            );
            self.temp[*index] = image;
            self.start_site(*index).await;
        }
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

/// Move `slot` to the server `to`, unless it is there already.
async fn place_slot(cluster: &Cluster, client: &Arc<AsyncClient>, slot: u16, to: usize) {
    let target = cluster.site(to).server_id;
    let probe = Id::from_parts(slot as u64, 1);
    let from = client.locate_server_id(&probe).unwrap();
    if from == target {
        return;
    }
    let plan = crate::migration::MigrationPlan {
        batch_cells: 8,
        ..Default::default()
    };
    crate::migration::migrate_slot(client, slot as u32, from, target, &plan)
        .await
        .expect("slot should move to its site");
    client.reload_slot_owners().await;
    assert_eq!(client.locate_server_id(&probe).unwrap(), target);
}

async fn coordinator(cluster: &Cluster) -> Arc<super::manager::AsyncServiceClient> {
    // The coordinator runs on the seed, whose slot table the migrations
    // updated.
    super::new_async_client_for_database(
        &cluster.addresses[SEED],
        &cluster.group,
        &cluster.group,
    )
    .await
    .unwrap()
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
    let (txn, tid, ids, scores) = prepared_transaction(cluster, client, base_score, 1).await;
    let limit = super::manager::stop_end_fanout_after_commit_point(tid.clone());
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
/// and no decision yet. `seq` picks the cell ids. Returns the coordinator
/// client too.
async fn prepared_transaction(
    cluster: &Cluster,
    client: &Arc<AsyncClient>,
    base_score: u64,
    seq: u64,
) -> (
    Arc<super::manager::AsyncServiceClient>,
    TxnId,
    Vec<Id>,
    Vec<u64>,
) {
    place_slot(cluster, client, HERE, A).await;
    place_slot(cluster, client, THERE, B).await;
    let ids = vec![
        Id::from_parts(HERE as u64, seq),
        Id::from_parts(THERE as u64, seq),
    ];
    let txn = coordinator(cluster).await;
    let tid = txn.begin().await.unwrap().unwrap();
    let scores = vec![base_score, base_score + 1];
    for (id, score) in ids.iter().zip(scores.iter()) {
        txn.write(tid.clone(), cell_of(*id, *score))
            .await
            .unwrap()
            .unwrap();
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

fn segment_holds_record_of(segment: &Segment, tid: &TxnId, kind: EntryType) -> bool {
    segment.entry_iter().any(|meta| {
        if meta.entry_header.entry_type != kind {
            return false;
        }
        let content = unsafe {
            std::slice::from_raw_parts(
                meta.body_pos as *const u8,
                meta.entry_header.content_length as usize,
            )
        };
        match kind {
            EntryType::BEGIN => {
                crate::ram::bracket::decode_begin(content).map_or(false, |begin| begin.txn == *tid)
            }
            _ => crate::ram::bracket::decode_commit(content).map_or(false, |(txn, _)| txn == *tid),
        }
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
    wait_for_free_segment_jobs(chunk);
    let seq_id = chunk.segs.get(&(seg_id as usize)).unwrap().seq_id;
    chunk.finish_rotated_head(seg_id, seq_id);
    wait_for_free_segment_jobs(chunk);
}

/// `Chunk::wait_for_segment_jobs_for_test`, but for the segments no
/// transaction holds: a held segment keeps its deferred seal queued.
fn wait_for_free_segment_jobs(chunk: &Chunk) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while chunk.segments().iter().any(|segment| {
        !segment.protection.transaction_pending()
            && (segment
                .wal_sync_queued
                .load(std::sync::atomic::Ordering::Acquire)
                || segment
                    .protection
                    .seal_pending
                    .load(std::sync::atomic::Ordering::Acquire)
                || !segment.protection.jobs_drained())
    }) {
        assert!(
            std::time::Instant::now() < deadline,
            "segment jobs did not drain"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// One combine of the segments of `chunk` that hold a `kind` record of
/// `tid`.
enum CompactAttempt {
    /// No segment of this chunk holds such a record (any more).
    Gone,
    /// The combiner refused, and every such segment is in progress for
    /// `tid`.
    Pinned,
    /// The combiner did not run for another reason: a reader, or the
    /// background cleaner, held a segment. Try again.
    Busy,
}

fn compact_attempt(
    chunks: &crate::ram::chunk::Chunks,
    chunk: &Chunk,
    tid: &TxnId,
    kind: EntryType,
) -> CompactAttempt {
    let holding: Vec<u64> = chunk
        .segments()
        .iter()
        .filter(|seg| segment_holds_record_of(seg, tid, kind))
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
                .any(|seg| seg.id == *seg_id && seg.protection.holds_transaction(tid))
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

/// Ask the cleaner to compact every segment of `server` that holds a `kind`
/// record of `tid`, and return whether those records are gone afterwards.
///
/// This is what the cleaner does to such a segment once it is no longer a
/// head: the live cells move to a new segment and the bracket markers stay
/// behind. Done explicitly so the test does not depend on cleaner timing.
/// The segments are handed to the combiner directly; the combiner itself
/// decides which of them it may take. The background cleaner runs too, so a
/// round that it disturbs is tried again.
fn compact_records(server: &NebServer, tid: &TxnId, kind: EntryType) -> bool {
    let chunks = server.chunks();
    for _ in 0..50 {
        let attempts: Vec<CompactAttempt> = chunks
            .list
            .iter()
            .map(|chunk| compact_attempt(chunks, chunk, tid, kind))
            .collect();
        if attempts.iter().all(|a| matches!(a, CompactAttempt::Gone)) {
            warn!("CROSSSITE {:?} records of {:?}: compacted", kind, tid);
            return true;
        }
        if attempts
            .iter()
            .all(|a| matches!(a, CompactAttempt::Gone | CompactAttempt::Pinned))
        {
            warn!(
                "CROSSSITE {:?} records of {:?}: compaction refused, in progress",
                kind, tid
            );
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("test setup: the combiner never decided about the segments of {:?}", tid);
}

fn compact_commit_evidence(server: &NebServer, tid: &TxnId) -> bool {
    compact_records(server, tid, EntryType::COMMIT)
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

fn evict_decisions(cluster: &Cluster, index: usize) {
    cluster
        .site(index)
        .database_runtime()
        .data_site()
        .unwrap()
        .fill_decision_memory_with_later_decisions();
}

fn retained_on(cluster: &Cluster, index: usize) -> usize {
    cluster.site(index).chunks().evidence().counts().0
}

/// Segments of site `index` that `tid` keeps in progress.
fn held_for(cluster: &Cluster, index: usize, tid: &TxnId) -> usize {
    cluster
        .site(index)
        .chunks()
        .list
        .iter()
        .flat_map(|chunk| chunk.segments())
        .filter(|segment| segment.protection.holds_transaction(tid))
        .count()
}

/// Run release rounds until site `index` no longer keeps `tid`. The
/// background loop runs them too; either may release it.
async fn wait_released(cluster: &Cluster, index: usize, tid: &TxnId) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while cluster.site(index).chunks().holds_commit_evidence(tid) {
        assert!(
            std::time::Instant::now() < deadline,
            "the evidence of {:?} was never released on site {}",
            tid,
            index
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

/// Compaction, restart path: the cleaner may not compact B's COMMIT while A
/// is in doubt (D28). B restarts, then A restarts in doubt and asks B.
#[tokio::test(flavor = "multi_thread")]
async fn a_compacted_peer_commit_then_peer_restart_keeps_the_restarted_site_atomic() {
    let (mut cluster, client) = Cluster::start("crosssite_compact_restart").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 1000).await;
    drop(client);

    let compacted = compact_commit_evidence(cluster.site(B), &tid);
    warn!("CROSSSITE compaction of B's unsettled COMMIT removed it: {}", compacted);
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

/// Compaction, live path: as above, while A stays live and in doubt, then
/// resolves through its lease sweeper.
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

/// Eviction, live path: B stays up with its COMMIT on disk, but later
/// decisions evict the transaction from B's decision memory. A resolves.
#[tokio::test(flavor = "multi_thread")]
async fn an_evicted_peer_decision_keeps_the_live_site_atomic() {
    let (cluster, client) = Cluster::start("crosssite_evict_live").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 3000).await;
    drop(client);

    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    evict_decisions(&cluster, B);
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

/// Eviction, restart path: B's decision is evicted, then A restarts in
/// doubt and asks B.
#[tokio::test(flavor = "multi_thread")]
async fn an_evicted_peer_decision_keeps_the_restarted_site_atomic() {
    let (mut cluster, client) = Cluster::start("crosssite_evict_restart").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 4000).await;
    drop(client);

    evict_decisions(&cluster, B);
    warn!(
        "CROSSSITE after eviction, site B answers {:?} for {:?}",
        outcome_on(&cluster, B, &tid),
        tid
    );
    cluster.restart(A).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "evicted decision, A restart");
    cluster.shutdown().await;
}

/// Control: B restarts with its COMMIT on disk. B answers from what recovery
/// found, so A commits.
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
/// learns COMMIT and makes the commit durable here. Then B forgets
/// (eviction), and A restarts again: A must not depend on B a second time.
#[tokio::test(flavor = "multi_thread")]
async fn a_resolved_site_stays_committed_through_a_second_restart() {
    let (mut cluster, client) = Cluster::start("crosssite_second_restart").await;
    let (_tid, ids, scores) = torn_transaction(&cluster, &client, 6000).await;
    drop(client);

    cluster.restart(A).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "test setup: the first restart must resolve COMMIT");

    evict_decisions(&cluster, B);
    cluster.restart(A).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "resolved site, second restart");
    cluster.shutdown().await;
}

/// D28 liveness: the transaction stays in progress on BOTH sites until it is
/// settled, and then the cleaner can compact it.
///
/// While A is in doubt, a release round on B keeps it and the cleaner does
/// not take B's COMMIT segment. After A resolves, the rounds release it on
/// both sites, and the next compaction turns the entries into plain cells.
#[tokio::test(flavor = "multi_thread")]
async fn the_peer_evidence_is_released_once_every_participant_decided() {
    let (cluster, client) = Cluster::start("crosssite_release").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 7000).await;
    drop(client);

    assert_eq!(retained_on(&cluster, B), 1, "B retains the commit");
    assert!(held_for(&cluster, B, &tid) > 0, "B holds its segments");
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
    assert!(
        held_for(&cluster, A, &tid) > 0,
        "A, a participant too, holds its segments until the transaction is settled"
    );

    wait_released(&cluster, B, &tid).await;
    wait_released(&cluster, A, &tid).await;
    assert_eq!(held_for(&cluster, B, &tid), 0);
    assert_eq!(held_for(&cluster, A, &tid), 0);
    assert!(
        compact_commit_evidence(cluster.site(B), &tid),
        "once settled, the cleaner compacts the COMMIT segment"
    );
    assert_eq!(score_on(cluster.site(B), &ids[1]), Some(scores[1]));
    cluster.shutdown().await;
}

/// Liveness after a restart: a restarted site keeps every multi-site commit
/// recovery found, and its participants come from the BEGIN records. Once
/// the other participant reports nothing open, it releases them.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_site_releases_recovered_evidence_when_no_participant_holds_it_open() {
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
    assert!(held_for(&cluster, B, &tid) > 0);
    wait_released(&cluster, B, &tid).await;
    assert_eq!(held_for(&cluster, B, &tid), 0);
    cluster.shutdown().await;
}

/// The resolved site keeps the transaction's segments until it is settled,
/// and keeps its data after its old bracket and the carrier are compacted.
///
/// After a restart, A resolves the transaction from B: a carrier, then the
/// cells as synced plain entries. B then releases its evidence and forgets
/// the decision. The cleaner may now compact A's old bracket and the
/// carrier. A's next restart must still hold its half.
#[tokio::test(flavor = "multi_thread")]
async fn a_resolved_site_keeps_its_parts_until_settled_and_its_data_after() {
    let (mut cluster, client) = Cluster::start("crosssite_resolved_settle").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 9000).await;
    drop(client);

    cluster.restart(A).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "test setup: the restart must resolve COMMIT");
    assert!(
        cluster.site(A).chunks().holds_commit_evidence(&tid),
        "the resolved site keeps the multi-site transaction until it is settled"
    );

    wait_released(&cluster, B, &tid).await;
    wait_released(&cluster, A, &tid).await;
    evict_decisions(&cluster, B);
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Unknown);

    let begins = compact_records(cluster.site(A), &tid, EntryType::BEGIN);
    warn!("CROSSSITE A's old bracket and carrier compacted: {}", begins);
    cluster.restart(A).await;

    assert_all_or_nothing(
        &cluster,
        &ids,
        &scores,
        "resolved site, settled, compacted, A restart",
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
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 10_000, 1).await;
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
    assert!(
        matches!(result, Ok(Ok(EndResult::Success))),
        "one site committed, so the client must hear `Success`, not {:?}",
        result
    );

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
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 11_000, 1).await;
    drop(client);

    crate::ram::chunk::refuse_next_commit_for_test(cluster.site(B).chunks().base_addr, tid.clone());
    let limit = super::manager::stop_end_fanout_after_commit_point(tid.clone());
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

/// T1 (review item 1): a power loss of the resolved site after the peer
/// released its evidence.
///
/// A restarts in doubt and resolves from B. B then releases its evidence
/// and forgets the decision. Then A loses power inside the group-sync
/// interval: each WAL keeps only what a sync covered. A must still hold its
/// half, because the resolution synced its records before A reported the
/// transaction closed.
#[tokio::test(flavor = "multi_thread")]
async fn a_resolved_commit_survives_a_power_loss_after_the_peer_released() {
    crate::ram::segs::power_loss_model::enable();
    let (mut cluster, client) = Cluster::start("crosssite_t1_power").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 12_000).await;
    drop(client);

    // From here only explicit syncs reach the disk.
    crate::ram::segs::power_loss_model::pause_group_sync(true);
    cluster.restart(A).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "test setup: the restart must resolve COMMIT");
    wait_released(&cluster, B, &tid).await;
    wait_released(&cluster, A, &tid).await;
    evict_decisions(&cluster, B);
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Unknown);

    cluster.power_loss_restart(A).await;
    crate::ram::segs::power_loss_model::pause_group_sync(false);

    assert_all_or_nothing(&cluster, &ids, &scores, "resolved site, power loss");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "A must keep the commit it resolved");
    cluster.shutdown().await;
}

/// T2 (review item 3): the release waits only on the participants.
///
/// The group has one more member, which is not a participant: `extra`
/// says what it runs, and `offline` whether it is stopped before B
/// restarts. After the restart, B keeps the multi-site commit it recovered,
/// and none of its single-site commits. It releases the multi-site commit as
/// soon as A reports it closed, and the cleaner can then compact it.
async fn a_non_participant_member_does_not_stall_the_release(
    group: &str,
    extra: Vec<Service>,
    offline: bool,
) {
    const C: usize = 3;
    let (mut cluster, client) = Cluster::start_with(group, vec![extra]).await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 13_000).await;
    // A transaction of B alone.
    let txn = coordinator(&cluster).await;
    let single = txn.begin().await.unwrap().unwrap();
    txn.write(single.clone(), cell_of(Id::from_parts(THERE as u64, 77), 1))
        .await
        .unwrap()
        .unwrap();
    txn.prepare(single.clone()).await.unwrap().unwrap();
    assert!(matches!(
        txn.commit(single.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    assert!(
        !cluster.site(B).chunks().holds_commit_evidence(&single),
        "a transaction of one site is not kept"
    );
    drop(client);
    resolve_live(&cluster, &tid).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "test setup: A must commit");

    if offline {
        cluster.stop(C).await;
    }
    cluster.restart(B).await;
    assert!(
        !cluster.site(B).chunks().holds_commit_evidence(&single),
        "recovery does not keep a COMMIT of one site"
    );
    assert!(
        cluster.site(B).chunks().holds_commit_evidence(&tid),
        "recovery keeps the multi-site commit"
    );
    let held = held_for(&cluster, B, &tid);
    warn!(
        "CROSSSITE B holds {} segment(s) for {:?} after its restart, {} pinned in total",
        held,
        tid,
        cluster.site(B).chunks().pinned_segments()
    );
    assert!(held > 0);
    wait_released(&cluster, B, &tid).await;
    assert_eq!(held_for(&cluster, B, &tid), 0);
    assert!(
        compact_commit_evidence(cluster.site(B), &tid),
        "the cleaner compacts the settled COMMIT segment"
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_offline_member_does_not_stall_the_release() {
    a_non_participant_member_does_not_stall_the_release(
        "crosssite_t2_offline",
        vec![Service::Cell, Service::Transaction],
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_without_a_data_site_does_not_stall_the_release() {
    a_non_participant_member_does_not_stall_the_release(
        "crosssite_t2_no_data_site",
        vec![Service::Cell],
        false,
    )
    .await;
}

/// T3 (review item 2): a resolution that fails keeps the in-doubt parts
/// through the cleaner and a restart.
///
/// A restarts in doubt; B reports COMMIT, but A cannot write its records (a
/// full store). The parts stay in doubt and in progress, so the cleaner
/// cannot drop them as dead. At the next restart A resolves.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_resolution_keeps_the_in_doubt_parts_through_the_cleaner_and_a_restart() {
    std::env::set_var("NEB_TERMINATION_ANSWER_SECS", "3");
    let (mut cluster, client) = Cluster::start("crosssite_t3_failed").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 14_000).await;
    drop(client);

    crate::ram::chunk::refuse_resolved_commits_for_test(true);
    cluster.restart(A).await;
    assert_eq!(
        score_on(cluster.site(A), &ids[0]),
        None,
        "test setup: the resolution must fail"
    );
    assert!(
        !compact_records(cluster.site(A), &tid, EntryType::BEGIN),
        "the cleaner must not take the parts of an in-doubt transaction"
    );
    crate::ram::chunk::refuse_resolved_commits_for_test(false);
    cluster.restart(A).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "failed resolution, cleaner, restart");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "A must commit at the next restart");
    cluster.shutdown().await;
}

/// Q4: a transaction of one site whose COMMIT is durable in one segment
/// and refused in another is committed, and the client hears `Success`.
///
/// The transaction writes on A only, in two chunks. The sync of the second
/// COMMIT segment fails after the first one succeeded. Recovery commits the
/// whole transaction from the first COMMIT, so a rollback here would tell
/// the client "failed" about a transaction that a power loss then brings
/// back. The test cuts the WALs to their synced length to see that.
#[tokio::test(flavor = "multi_thread")]
async fn a_single_site_commit_with_one_durable_commit_is_reported_committed() {
    crate::ram::segs::power_loss_model::enable();
    let (mut cluster, client) = Cluster::start("crosssite_q4").await;
    place_slot(&cluster, &client, HERE, A).await;
    place_slot(&cluster, &client, HERE_TOO, A).await;
    drop(client);
    let ids = vec![
        Id::from_parts(HERE as u64, 5),
        Id::from_parts(HERE_TOO as u64, 5),
    ];
    let txn = coordinator(&cluster).await;
    let tid = txn.begin().await.unwrap().unwrap();
    for (n, id) in ids.iter().enumerate() {
        txn.write(tid.clone(), cell_of(*id, 15_000 + n as u64))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();
    assert!(
        crate::ram::chunk::transaction_lease_count(&tid, &cluster.site(A).chunks().address_range())
            >= 2,
        "test setup: the transaction must hold a head in two chunks"
    );
    crate::ram::chunk::fail_commit_sync_after_first_for_test(
        cluster.site(A).chunks().base_addr,
        tid.clone(),
    );
    let result = txn.commit(tid.clone()).await;
    warn!("CROSSSITE Q4 commit returned {:?}", result);

    cluster.power_loss_restart(A).await;
    let recovered: Vec<bool> = ids
        .iter()
        .enumerate()
        .map(|(n, id)| score_on(cluster.site(A), id) == Some(15_000 + n as u64))
        .collect();
    warn!("CROSSSITE Q4 after the power loss, A holds {:?}", recovered);
    assert!(
        recovered.iter().all(|held| *held),
        "recovery commits the transaction from its durable COMMIT: {:?}",
        recovered
    );
    assert!(
        matches!(result, Ok(Ok(EndResult::Success))),
        "the client must hear that the transaction committed, not {:?}",
        result
    );
    cluster.shutdown().await;
}

/// D28: a transaction of one site cannot tear across sites, so it is never
/// held after its COMMIT is synced, and recovery does not keep it.
#[tokio::test(flavor = "multi_thread")]
async fn a_single_site_transaction_is_not_held_after_its_commit() {
    let (mut cluster, client) = Cluster::start("crosssite_single_site").await;
    place_slot(&cluster, &client, THERE, B).await;
    drop(client);
    let txn = coordinator(&cluster).await;
    let tid = txn.begin().await.unwrap().unwrap();
    txn.write(tid.clone(), cell_of(Id::from_parts(THERE as u64, 9), 1))
        .await
        .unwrap()
        .unwrap();
    txn.prepare(tid.clone()).await.unwrap().unwrap();
    assert!(matches!(
        txn.commit(tid.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while held_for(&cluster, B, &tid) != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "a single-site commit must not stay in progress"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!cluster.site(B).chunks().holds_commit_evidence(&tid));
    cluster.restart(B).await;
    assert!(!cluster.site(B).chunks().holds_commit_evidence(&tid));
    assert_eq!(held_for(&cluster, B, &tid), 0);
    cluster.shutdown().await;
}

/// D28 notice: when every participant closed its brackets, the coordinator
/// tells them in a batch, and each releases the transaction without a
/// release round.
#[tokio::test(flavor = "multi_thread")]
async fn the_settled_notice_releases_a_multi_site_commit_on_every_site() {
    let (cluster, client) = Cluster::start("crosssite_settled_notice").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 16_000, 1).await;
    drop(client);
    assert!(matches!(
        txn.commit(tid.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while [A, B].iter().any(|index| {
        cluster.site(*index).chunks().holds_commit_evidence(&tid)
            || held_for(&cluster, *index, &tid) != 0
    }) {
        assert!(
            std::time::Instant::now() < deadline,
            "the settled notice must release the transaction on every site"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cluster.shutdown().await;
}

/// C11: the coordinator dies after `end` and before the `settled` flush.
/// The slow release sweep releases the transaction once every participant
/// reports it closed.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_settled_notice_is_released_by_the_slow_sweep() {
    std::env::set_var("NEB_SETTLE_SWEEP_SECS", "2");
    let (cluster, client) = Cluster::start("crosssite_c11_lost_notice").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 17_000, 1).await;
    drop(client);
    super::manager::suppress_settled_for_test(tid.clone());
    assert!(matches!(
        txn.commit(tid.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b);
    tokio::time::sleep(Duration::from_millis(500)).await;
    for index in [A, B] {
        assert!(
            cluster.site(index).chunks().holds_commit_evidence(&tid),
            "test setup: no notice, so site {} still keeps the transaction",
            index
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while [A, B].iter().any(|index| {
        cluster.site(*index).chunks().holds_commit_evidence(&tid)
            || held_for(&cluster, *index, &tid) != 0
    }) {
        assert!(
            std::time::Instant::now() < deadline,
            "the slow sweep must release the transaction"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.shutdown().await;
}

/// C7: B's close is refused while A commits. The client hears `Success`. B
/// keeps the transaction decided. Then B loses power: its retracted COMMIT is
/// gone, B is in doubt at the start, and resolves from A, which still keeps
/// the unsettled commit.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_close_survives_a_power_cut_of_the_refusing_site() {
    crate::ram::segs::power_loss_model::enable();
    let (mut cluster, client) = Cluster::start("crosssite_c7_refused_cut").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 18_000, 1).await;
    drop(client);
    crate::ram::chunk::refuse_next_commit_for_test(cluster.site(B).chunks().base_addr, tid.clone());
    let result = txn.commit(tid.clone()).await;
    assert!(
        matches!(result, Ok(Ok(EndResult::Success))),
        "A committed, so the client must hear `Success`, not {:?}",
        result
    );
    assert!(
        cluster.site(A).chunks().holds_commit_evidence(&tid),
        "B did not close, so A keeps the transaction"
    );
    cluster.power_loss_restart(B).await;
    assert_all_or_nothing(&cluster, &ids, &scores, "refused close on B, power cut of B");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "B must resolve COMMIT from A");
    cluster.shutdown().await;
}

/// C8: three participants; the coordinator tells only one of them, then
/// every participant loses power at one instant. The site that was told
/// starts first; the others resolve from it. All commit.
#[tokio::test(flavor = "multi_thread")]
async fn three_sites_power_cut_after_one_commit_all_commit() {
    const C: usize = 3;
    const HERE_C: u16 = 94;
    crate::ram::segs::power_loss_model::enable();
    let (mut cluster, client) = Cluster::start_with(
        "crosssite_c8_three_sites",
        vec![vec![Service::Cell, Service::Transaction]],
    )
    .await;
    place_slot(&cluster, &client, HERE, A).await;
    place_slot(&cluster, &client, THERE, B).await;
    place_slot(&cluster, &client, HERE_C, C).await;
    drop(client);
    let sites = [A, B, C];
    let ids: Vec<Id> = [HERE, THERE, HERE_C]
        .iter()
        .map(|slot| Id::from_parts(*slot as u64, 1))
        .collect();
    let txn = coordinator(&cluster).await;
    let tid = txn.begin().await.unwrap().unwrap();
    for (n, id) in ids.iter().enumerate() {
        txn.write(tid.clone(), cell_of(*id, 19_000 + n as u64))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();
    let limit = super::manager::stop_end_fanout_after_commit_point(tid.clone());
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    let told = *sites
        .iter()
        .min_by_key(|index| cluster.site(**index).server_id)
        .unwrap();
    for index in sites {
        let expected = if index == told {
            DurableTxnStatus::Committed
        } else {
            DurableTxnStatus::Prepared
        };
        assert_eq!(
            cluster.site(index).chunks().durable_txn_status(&tid),
            expected,
            "test setup: only the lowest participant is told"
        );
    }
    let mut order = vec![told];
    order.extend(sites.iter().copied().filter(|index| *index != told));
    cluster.power_cut_all(&order).await;
    for (n, (index, id)) in sites.iter().zip(ids.iter()).enumerate() {
        assert_eq!(
            score_on(cluster.site(*index), id),
            Some(19_000 + n as u64),
            "site {} must commit its part",
            index
        );
    }
    cluster.shutdown().await;
}

/// C10: one participant stays down for the whole test. The commit point
/// keeps that transaction's segments, and only those: the cleaner still
/// compacts a settled transaction's segment on the same site.
#[tokio::test(flavor = "multi_thread")]
async fn a_down_participant_pins_only_its_own_transactions() {
    let (mut cluster, client) = Cluster::start("crosssite_c10_down_participant").await;
    let (tid, _ids, _scores) = torn_transaction(&cluster, &client, 20_000).await;
    drop(client);
    cluster.stop(A).await;
    // A later transaction must not land in the held segment, or there is
    // nothing to tell apart.
    for chunk in &cluster.site(B).chunks().list {
        for segment in chunk.segments() {
            if segment.protection.holds_transaction(&tid) && chunk.is_active_head(segment.id) {
                rotate_out(chunk, segment.id);
            }
        }
    }

    let txn = coordinator(&cluster).await;
    let single = txn.begin().await.unwrap().unwrap();
    txn.write(single.clone(), cell_of(Id::from_parts(THERE as u64, 88), 1))
        .await
        .unwrap()
        .unwrap();
    txn.prepare(single.clone()).await.unwrap().unwrap();
    assert!(matches!(
        txn.commit(single.clone()).await,
        Ok(Ok(EndResult::Success))
    ));

    let held = held_for(&cluster, B, &tid);
    let pinned = cluster.site(B).chunks().pinned_segments();
    warn!(
        "CROSSSITE C10: with A down, B keeps {} segment(s) for the unsettled transaction; {} \
         segment(s), {} bytes, pinned in total",
        held,
        pinned,
        cluster.site(B).chunks().pinned_bytes()
    );
    assert!(held > 0, "the unsettled transaction stays pinned");
    assert!(
        compact_commit_evidence(cluster.site(B), &single),
        "the cleaner still compacts the settled single-site transaction"
    );
    assert!(
        !compact_commit_evidence(cluster.site(B), &tid),
        "the cleaner never compacts the unsettled one"
    );
    assert_eq!(
        cluster.site(B).chunks().pinned_segments(),
        held_for(&cluster, B, &tid),
        "only the unsettled transaction pins segments"
    );
    cluster.shutdown().await;
}
