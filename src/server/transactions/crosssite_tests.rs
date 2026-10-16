//! Cross-site decisions: the status cell, the participant fence and the
//! queued COMMIT sync (D31), as the cut matrix C1-C16 of the TLA+ round.
//!
//! A participant that is in doubt asks its peers what became of the
//! transaction. A peer that committed answers from its live decision memory
//! or from its status cell: a hidden cell inside the bracket, under the same
//! sync as the COMMIT. These tests try to remove that answer -- by
//! compaction, by a restart, by later decisions that evict it from memory, or
//! by a power loss -- while another participant is in doubt, and then check
//! that the transaction is still all-or-nothing on every site.
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
    torn_transaction_seq(cluster, client, base_score, 1).await
}

/// `torn_transaction` on the cells of `seq`.
async fn torn_transaction_seq(
    cluster: &Cluster,
    client: &Arc<AsyncClient>,
    base_score: u64,
    seq: u64,
) -> (TxnId, Vec<Id>, Vec<u64>) {
    let (txn, tid, ids, scores) = prepared_transaction(cluster, client, base_score, seq).await;
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
        site.sweep_idle_transactions(Duration::ZERO)
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

/// Whether site `index` holds the status cell of `tid`.
fn holds_status(cluster: &Cluster, index: usize, tid: &TxnId) -> bool {
    cluster.site(index).chunks().txn_status().get(tid).is_some()
}

/// Whether site `index` holds the status cell of `tid` and its COMMIT is
/// synced.
fn status_synced(cluster: &Cluster, index: usize, tid: &TxnId) -> bool {
    cluster
        .site(index)
        .chunks()
        .txn_status()
        .get(tid)
        .is_some_and(|entry| entry.self_synced)
}

/// Segments of site `index` that `tid` keeps in progress (D20).
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

async fn release_round(cluster: &Cluster, index: usize) -> usize {
    data_site(cluster, index).status_release_round(true).await
}

/// Run release rounds until site `index` has tombstoned the status cell of
/// `tid`. The background loop runs them too.
async fn wait_released(cluster: &Cluster, index: usize, tid: &TxnId) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while holds_status(cluster, index, tid) {
        assert!(
            std::time::Instant::now() < deadline,
            "the status cell of {:?} was never released on site {}",
            tid,
            index
        );
        release_round(cluster, index).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait until site `index` has synced its COMMIT and status cell of `tid`.
async fn wait_status_synced(cluster: &Cluster, index: usize, tid: &TxnId) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !status_synced(cluster, index, tid) {
        assert!(
            std::time::Instant::now() < deadline,
            "site {} never synced the status cell of {:?}",
            index,
            tid
        );
        data_site(cluster, index).status_release_round(false).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn data_site(cluster: &Cluster, index: usize) -> Arc<super::data_site::DataManager> {
    cluster
        .site(index)
        .database_runtime()
        .data_site()
        .expect("the site runs a data site")
}

/// Whether site `index` still has `tid` in doubt: live with leases, or
/// parked after a restart.
fn in_doubt_on(cluster: &Cluster, index: usize, tid: &TxnId) -> bool {
    let chunks = cluster.site(index).chunks();
    crate::ram::chunk::transaction_lease_count(tid, &chunks.address_range()) > 0
        || chunks
            .unresolved_transactions()
            .iter()
            .any(|(txn, _)| txn == tid)
}

async fn wait_until(what: &str, seconds: u64, mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(seconds);
    while !condition() {
        assert!(std::time::Instant::now() < deadline, "{} did not happen", what);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A `prepare` of `tid` that writes `id`, sent to the data site of `index`
/// directly, as a coordinator that still collects votes sends it.
async fn late_prepare(cluster: &Cluster, index: usize, tid: &TxnId, id: Id) -> DMPrepareResult {
    let site = data_site(cluster, index);
    let participants = vec![cluster.site(A).server_id, cluster.site(B).server_id];
    <super::data_site::DataManager as super::data_site::Service>::prepare(
        &site,
        cluster.site(SEED).server_id,
        site_clock(cluster, index),
        tid.clone(),
        vec![PrepareOp {
            id,
            expectation: CellExpectation::Absent,
            intent: PrepareIntent::Write,
        }],
        participants,
    )
    .await
    .payload
}

fn site_clock(cluster: &Cluster, index: usize) -> bifrost::hlc::Hlc {
    bifrost::hlc::HlcSource::new(cluster.site(index).server_id).now()
}

// ---------------------------------------------------------------------------
// Evidence: the status cell (E2), C4, C5, C6
// ---------------------------------------------------------------------------

/// C4, restart path: B committed (default), its group sync landed, and the
/// cleaner compacts B's COMMIT segment. The status cell is carried as a live
/// cell. B restarts, then A restarts in doubt and asks B.
#[tokio::test(flavor = "multi_thread")]
async fn a_compacted_peer_commit_then_peer_restart_keeps_the_restarted_site_atomic() {
    let (mut cluster, client) = Cluster::start("crosssite_compact_restart").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 1000).await;
    drop(client);

    wait_status_synced(&cluster, B, &tid).await;
    wait_until("B's local pin to drop", 30, || held_for(&cluster, B, &tid) == 0).await;
    assert!(
        compact_commit_evidence(cluster.site(B), &tid),
        "after its COMMIT is synced, B's segment is no longer in progress"
    );
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    cluster.restart(B).await;
    assert_eq!(
        outcome_on(&cluster, B, &tid),
        TxnOutcome::Committed,
        "B answers from its recovered status cell"
    );
    cluster.restart(A).await;

    assert_all_or_nothing(
        &cluster,
        &ids,
        &scores,
        "compacted COMMIT, B restart, A restart",
    );
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b);
    cluster.shutdown().await;
}

/// C4, live path: as above, while A stays live and in doubt.
#[tokio::test(flavor = "multi_thread")]
async fn a_compacted_peer_commit_then_peer_restart_keeps_the_live_site_atomic() {
    let (mut cluster, client) = Cluster::start("crosssite_compact_live").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 2000).await;
    drop(client);

    wait_status_synced(&cluster, B, &tid).await;
    wait_until("B's local pin to drop", 30, || held_for(&cluster, B, &tid) == 0).await;
    assert!(compact_commit_evidence(cluster.site(B), &tid));
    cluster.restart(B).await;
    resolve_live(&cluster, &tid).await;

    assert_all_or_nothing(
        &cluster,
        &ids,
        &scores,
        "compacted COMMIT, B restart, A live",
    );
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b);
    cluster.shutdown().await;
}

/// C4 with a cut: B's segment is compacted after its sync, then B loses
/// power. B's status cell (synced, carried) still answers `Committed`.
#[tokio::test(flavor = "multi_thread")]
async fn a_status_cell_survives_compaction_and_a_power_cut() {
    crate::ram::segs::power_loss_model::enable();
    let (mut cluster, client) = Cluster::start("crosssite_c4_cut").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 2500).await;
    drop(client);
    wait_status_synced(&cluster, B, &tid).await;
    wait_until("B's local pin to drop", 30, || held_for(&cluster, B, &tid) == 0).await;
    assert!(compact_commit_evidence(cluster.site(B), &tid));
    cluster.power_loss_restart(B).await;
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    resolve_live(&cluster, &tid).await;
    assert_all_or_nothing(&cluster, &ids, &scores, "compacted, power cut of B");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b);
    cluster.shutdown().await;
}

/// C5 (void under E2, kept as a check): later decisions evict the
/// transaction from B's decision memory. B answers from its status cell.
#[tokio::test(flavor = "multi_thread")]
async fn an_evicted_peer_decision_keeps_the_live_site_atomic() {
    let (cluster, client) = Cluster::start("crosssite_evict_live").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 3000).await;
    drop(client);

    wait_status_synced(&cluster, B, &tid).await;
    evict_decisions(&cluster, B);
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    resolve_live(&cluster, &tid).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "evicted decision, A live");
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_evicted_peer_decision_keeps_the_restarted_site_atomic() {
    let (mut cluster, client) = Cluster::start("crosssite_evict_restart").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 4000).await;
    drop(client);

    wait_status_synced(&cluster, B, &tid).await;
    evict_decisions(&cluster, B);
    cluster.restart(A).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "evicted decision, A restart");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b);
    cluster.shutdown().await;
}

/// Control: B restarts with its COMMIT on disk. A commits.
#[tokio::test(flavor = "multi_thread")]
async fn control_an_uncompacted_peer_commit_survives_the_peer_restart() {
    let (mut cluster, client) = Cluster::start("crosssite_control").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 5000).await;
    drop(client);

    wait_status_synced(&cluster, B, &tid).await;
    cluster.restart(B).await;
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    cluster.restart(A).await;

    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "the control must commit on both sites: A {}, B {}", a, b);
    cluster.shutdown().await;
}

/// The resolved site restarts a second time. At the first restart A
/// resolves from B with an ordinary synced bracket. Then B forgets, and A
/// restarts again: A does not depend on B a second time.
#[tokio::test(flavor = "multi_thread")]
async fn a_resolved_site_stays_committed_through_a_second_restart() {
    let (mut cluster, client) = Cluster::start("crosssite_second_restart").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 6000).await;
    drop(client);

    wait_status_synced(&cluster, B, &tid).await;
    cluster.restart(A).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "test setup: the first restart must resolve COMMIT");
    assert!(
        status_synced(&cluster, A, &tid),
        "the resolved site holds its own synced status cell"
    );

    evict_decisions(&cluster, B);
    cluster.restart(A).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "resolved site, second restart");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b);
    cluster.shutdown().await;
}

/// C6 (T1): the resolved site loses power after the peer released. Its
/// resolution bracket is synced before it answers, so it commits again.
#[tokio::test(flavor = "multi_thread")]
async fn a_resolved_commit_survives_a_power_loss_after_the_peer_released() {
    crate::ram::segs::power_loss_model::enable();
    let (mut cluster, client) = Cluster::start("crosssite_t1_power").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 12_000).await;
    drop(client);

    wait_status_synced(&cluster, B, &tid).await;
    // From here only explicit syncs reach A's disk: the group syncer and
    // the queued COMMIT syncs (S1) are held back.
    let a_dir = cluster.temp[A].path().to_path_buf();
    crate::ram::segs::power_loss_model::pause_group_sync(true);
    crate::ram::durability::pause_receipt_syncs_under(&a_dir, true);
    cluster.restart(A).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "test setup: the restart must resolve COMMIT");
    wait_released(&cluster, B, &tid).await;
    wait_released(&cluster, A, &tid).await;
    evict_decisions(&cluster, B);
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Unknown);

    cluster.power_loss_restart(A).await;
    crate::ram::segs::power_loss_model::pause_group_sync(false);
    crate::ram::durability::pause_receipt_syncs_under(&a_dir, false);

    assert_all_or_nothing(&cluster, &ids, &scores, "resolved site, power loss");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "A must keep the commit it resolved");
    cluster.shutdown().await;
}

/// The resolved site keeps its data after the old open bracket is
/// compacted, the peer released, and a restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_resolved_site_keeps_its_data_after_compaction_and_release() {
    let (mut cluster, client) = Cluster::start("crosssite_resolved_compact").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 9000).await;
    drop(client);

    wait_status_synced(&cluster, B, &tid).await;
    cluster.restart(A).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "test setup: the restart must resolve COMMIT");
    wait_released(&cluster, B, &tid).await;
    wait_released(&cluster, A, &tid).await;
    evict_decisions(&cluster, B);
    let compacted = compact_records(cluster.site(A), &tid, EntryType::BEGIN);
    warn!("CROSSSITE A's old open bracket compacted: {}", compacted);
    cluster.restart(A).await;

    assert_all_or_nothing(&cluster, &ids, &scores, "resolved site, released, compacted");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b);
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Release (F4), C11, C14, T2
// ---------------------------------------------------------------------------

/// F4 liveness: a status cell stays while a participant has not synced its
/// COMMIT, and goes on every site once all have.
#[tokio::test(flavor = "multi_thread")]
async fn the_status_cell_is_released_once_every_participant_synced() {
    let (cluster, client) = Cluster::start("crosssite_release").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 7000).await;
    drop(client);

    wait_status_synced(&cluster, B, &tid).await;
    release_round(&cluster, B).await;
    assert!(
        holds_status(&cluster, B, &tid),
        "A is in doubt, with no COMMIT: B keeps its status cell"
    );

    resolve_live(&cluster, &tid).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "A must commit what B was told to commit");

    wait_released(&cluster, B, &tid).await;
    wait_released(&cluster, A, &tid).await;
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    cluster.shutdown().await;
}

/// C14: the release waits for every participant's sync, not for the `end`
/// replies. A's sync is held back; B must keep its status cell. Then A loses
/// power, its COMMIT is gone, and A resolves from B.
#[tokio::test(flavor = "multi_thread")]
async fn the_release_waits_for_every_participants_sync() {
    crate::ram::segs::power_loss_model::enable();
    let (mut cluster, client) = Cluster::start("crosssite_c14_release_sync").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 13_500, 1).await;
    drop(client);
    let a_dir = cluster.temp[A].path().to_path_buf();
    crate::ram::durability::pause_receipt_syncs_under(&a_dir, true);
    assert!(matches!(
        txn.commit(tid.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    wait_status_synced(&cluster, B, &tid).await;
    for _ in 0..3 {
        release_round(&cluster, B).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(
        holds_status(&cluster, B, &tid),
        "A has not synced its COMMIT: B must keep its status cell"
    );
    assert_eq!(outcome_on(&cluster, A, &tid), TxnOutcome::Committing);
    cluster.power_loss_restart(A).await;
    crate::ram::durability::pause_receipt_syncs_under(&a_dir, false);
    assert_all_or_nothing(&cluster, &ids, &scores, "A cut before its sync");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "A must resolve COMMIT from B");
    cluster.shutdown().await;
}

/// C11: the `synced` notices are lost. The slow sweep asks the participants
/// and releases the status cells.
#[tokio::test(flavor = "multi_thread")]
async fn lost_synced_notices_are_released_by_the_slow_sweep() {
    std::env::set_var("NEB_SETTLE_SWEEP_SECS", "2");
    super::data_site::SYNCED_NOTICES_SKIPPED_FOR_TEST
        .store(true, std::sync::atomic::Ordering::Release);
    let (cluster, client) = Cluster::start("crosssite_c11_lost_notice").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 17_000, 1).await;
    drop(client);
    assert!(matches!(
        txn.commit(tid.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b);
    wait_until("the release by the slow sweep", 60, || {
        !holds_status(&cluster, A, &tid) && !holds_status(&cluster, B, &tid)
    })
    .await;
    cluster.shutdown().await;
}

/// T2: a member that is not a participant does not stall the release.
async fn a_non_participant_member_does_not_stall_the_release(
    group: &str,
    extra: Vec<Service>,
    offline: bool,
) {
    const C: usize = 3;
    let (mut cluster, client) = Cluster::start_with(group, vec![extra]).await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 13_000).await;
    // A transaction of B alone: no status cell.
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
        !holds_status(&cluster, B, &single),
        "a transaction of one site has no status cell"
    );
    drop(client);
    if offline {
        cluster.stop(C).await;
    }
    resolve_live(&cluster, &tid).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "test setup: A must commit");
    wait_released(&cluster, B, &tid).await;
    wait_released(&cluster, A, &tid).await;
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

/// The status cell is hidden: a cell scan and a cell count never see it.
#[tokio::test(flavor = "multi_thread")]
async fn a_status_cell_is_hidden_from_scans_and_counts() {
    let (cluster, client) = Cluster::start("crosssite_hidden").await;
    let (txn, tid, _ids, _scores) = prepared_transaction(&cluster, &client, 18_500, 1).await;
    super::data_site::SYNCED_NOTICES_SKIPPED_FOR_TEST
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(matches!(
        txn.commit(tid.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    let entry = cluster
        .site(B)
        .chunks()
        .txn_status()
        .get(&tid)
        .expect("test setup: B holds the status cell");
    let chunks = cluster.site(B).chunks();
    for chunk in 0..chunks.chunk_count() {
        assert!(
            !chunks.live_cell_ids_of_chunk(chunk).contains(&entry.id.bits()),
            "a cell scan must not list the status cell"
        );
    }
    let slots: std::collections::HashSet<u16> = [entry.id.locality()].into_iter().collect();
    assert!(!chunks.cell_ids_in_slots(&slots).contains(&entry.id));
    let mut scan = client.cell_id_scan().await.unwrap();
    while let Some(page) = scan.next_page(1024).await.unwrap() {
        assert!(
            page.iter().all(|id| *id != entry.id),
            "the client cell scan must not list the status cell"
        );
    }
    drop(scan);
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// ff70c476 core, F1, C7, C13
// ---------------------------------------------------------------------------

/// A COMMIT refused on one site does not tear the transaction, and the
/// client hears `Success`.
#[tokio::test(flavor = "multi_thread")]
async fn a_commit_refused_on_one_site_does_not_tear_the_transaction() {
    let (mut cluster, client) = Cluster::start("crosssite_refused").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 10_000, 1).await;
    drop(client);

    crate::ram::chunk::refuse_next_commit_for_test(cluster.site(B).chunks().base_addr, tid.clone());
    let result = txn.commit(tid.clone()).await;
    assert!(
        matches!(result, Ok(Ok(EndResult::Success))),
        "one site committed, so the client must hear `Success`, not {:?}",
        result
    );
    assert_ne!(
        cluster.site(B).chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "test setup: site B must have refused its COMMIT"
    );
    assert_all_or_nothing(&cluster, &ids, &scores, "refused COMMIT on B, right after");

    data_site(&cluster, B)
        .sweep_idle_transactions(Duration::ZERO)
        .await;
    assert_eq!(
        cluster.site(B).chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "site B must finish the commit it was told to make"
    );
    cluster.restart(B).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "the transaction must be committed on both sites");
    cluster.shutdown().await;
}

/// C13 (F1): a site whose close was refused is decided, not synced. It
/// answers `Committing`, and the site in doubt waits until it is synced.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_with_a_refused_commit_answers_committing_until_synced() {
    let (cluster, client) = Cluster::start("crosssite_c13_refused_answer").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 11_000, 1).await;
    drop(client);

    crate::ram::chunk::refuse_next_commit_for_test(cluster.site(B).chunks().base_addr, tid.clone());
    let limit = super::manager::stop_end_fanout_after_commit_point(tid.clone());
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    assert_eq!(
        outcome_on(&cluster, B, &tid),
        TxnOutcome::Committing,
        "a decided close that is not synced answers `Committing`"
    );
    data_site(&cluster, A)
        .sweep_idle_transactions(Duration::ZERO)
        .await;
    assert!(in_doubt_on(&cluster, A, &tid), "A must wait, not commit and not abort");

    data_site(&cluster, B)
        .sweep_idle_transactions(Duration::ZERO)
        .await;
    wait_status_synced(&cluster, B, &tid).await;
    resolve_live(&cluster, &tid).await;
    for index in [A, B] {
        assert_eq!(
            cluster.site(index).chunks().durable_txn_status(&tid),
            DurableTxnStatus::Committed,
            "site {} must hold a COMMIT",
            index
        );
    }
    assert_all_or_nothing(&cluster, &ids, &scores, "refused COMMIT on B, A waited");
    cluster.shutdown().await;
}

/// C13b (F1, default mode): a COMMIT that is appended and not synced
/// answers `Committing`; after its group sync, `Committed`.
#[tokio::test(flavor = "multi_thread")]
async fn an_unsynced_commit_answers_committing() {
    let (cluster, client) = Cluster::start("crosssite_c13b_unsynced").await;
    let (txn, tid, _ids, _scores) = prepared_transaction(&cluster, &client, 14_500, 1).await;
    drop(client);
    let b_dir = cluster.temp[B].path().to_path_buf();
    crate::ram::durability::pause_receipt_syncs_under(&b_dir, true);
    assert!(matches!(
        txn.commit(tid.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    data_site(&cluster, B).status_release_round(false).await;
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committing);
    crate::ram::durability::pause_receipt_syncs_under(&b_dir, false);
    wait_status_synced(&cluster, B, &tid).await;
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    cluster.shutdown().await;
}

/// C7: B's close is refused while A commits. The client hears `Success`.
/// B loses power; its retracted COMMIT is gone, B is in doubt at the start,
/// and resolves from A's status cell.
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
    wait_status_synced(&cluster, A, &tid).await;
    cluster.power_loss_restart(B).await;
    assert_all_or_nothing(&cluster, &ids, &scores, "refused close on B, power cut of B");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "B must resolve COMMIT from A");
    cluster.shutdown().await;
}

/// A status cell inside an open bracket decides it at the start: B's close
/// is refused after B wrote its status cell, the group sync takes the cell
/// to disk, and B loses power while A is silent. B commits from its own
/// status cell, without an answer from A.
#[tokio::test(flavor = "multi_thread")]
async fn an_open_bracket_with_its_own_synced_status_cell_commits_at_the_start() {
    crate::ram::segs::power_loss_model::enable();
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "2");
    let (mut cluster, client) = Cluster::start("crosssite_own_status_cell").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 18_800, 1).await;
    drop(client);
    crate::ram::chunk::refuse_next_commit_for_test(cluster.site(B).chunks().base_addr, tid.clone());
    let limit = super::manager::stop_end_fanout_after_commit_point(tid.clone());
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    assert_ne!(
        cluster.site(B).chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "test setup: B must have refused its COMMIT"
    );
    // The group sync takes the appended status cell to disk.
    cluster.site(B).chunks().sync_all();
    let a_id = cluster.site(A).server_id;
    super::data_site::silent_peers_for_test().lock().insert(a_id);
    cluster.power_loss_restart(B).await;
    assert_eq!(
        score_on(cluster.site(B), &ids[1]),
        Some(scores[1]),
        "B must commit from its own status cell"
    );
    assert!(!in_doubt_on(&cluster, B, &tid));
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    super::data_site::silent_peers_for_test().lock().remove(&a_id);
    resolve_live(&cluster, &tid).await;
    assert_all_or_nothing(&cluster, &ids, &scores, "own status cell, B cut, A live");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b);
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Cuts of several sites: C1, C8
// ---------------------------------------------------------------------------

/// C1: one participant's COMMIT is synced, the other was never told, and
/// both lose power. Both commit.
#[tokio::test(flavor = "multi_thread")]
async fn two_sites_power_cut_after_one_synced_commit_both_commit() {
    crate::ram::segs::power_loss_model::enable();
    let (mut cluster, client) = Cluster::start("crosssite_c1_cut").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 19_500).await;
    drop(client);
    wait_status_synced(&cluster, B, &tid).await;
    cluster.power_cut_all(&[B, A]).await;
    wait_until("A's resolution", 60, || {
        score_on(cluster.site(A), &ids[0]) == Some(scores[0])
    })
    .await;
    assert_all_or_nothing(&cluster, &ids, &scores, "both cut");
    cluster.shutdown().await;
}

/// C8: three participants; the coordinator tells only one, its COMMIT is
/// synced, and every participant loses power. All commit.
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
    wait_status_synced(&cluster, told, &tid).await;
    let mut order = vec![told];
    order.extend(sites.iter().copied().filter(|index| *index != told));
    cluster.power_cut_all(&order).await;
    for (n, (index, id)) in sites.iter().zip(ids.iter()).enumerate() {
        let index = *index;
        let id = *id;
        wait_until("every site's commit", 60, || {
            score_on(cluster.site(index), &id) == Some(19_000 + n as u64)
        })
        .await;
    }
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// D20 pins are local again: C10, T3, single-site
// ---------------------------------------------------------------------------

/// C10: one participant stays down. The other's segments are pinned only
/// until its own COMMIT is synced (D20), and the cleaner then compacts them;
/// the status cell keeps the answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_down_participant_pins_nothing_after_the_local_sync() {
    let (mut cluster, client) = Cluster::start("crosssite_c10_down_participant").await;
    let (tid, _ids, _scores) = torn_transaction(&cluster, &client, 20_000).await;
    drop(client);
    cluster.stop(A).await;
    wait_status_synced(&cluster, B, &tid).await;
    wait_until("B's local pin to drop", 30, || held_for(&cluster, B, &tid) == 0).await;
    assert!(
        compact_commit_evidence(cluster.site(B), &tid),
        "the cleaner compacts B's segment while A is down"
    );
    assert_eq!(cluster.site(B).chunks().pinned_segments(), 0);
    assert!(holds_status(&cluster, B, &tid), "the status cell keeps the answer");
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    cluster.shutdown().await;
}

/// T3: a resolution that fails keeps the in-doubt parts through the cleaner
/// and a restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_resolution_keeps_the_in_doubt_parts_through_the_cleaner_and_a_restart() {
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "3");
    let (mut cluster, client) = Cluster::start("crosssite_t3_failed").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 14_000).await;
    drop(client);
    wait_status_synced(&cluster, B, &tid).await;

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

/// Q4: a durable single-site commit whose COMMIT is synced in one segment
/// and refused in another is committed, and the client hears `Success`.
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
    let result = txn.commit_durable(tid.clone()).await;
    cluster.power_loss_restart(A).await;
    let recovered: Vec<bool> = ids
        .iter()
        .enumerate()
        .map(|(n, id)| score_on(cluster.site(A), id) == Some(15_000 + n as u64))
        .collect();
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

/// A transaction of one site has no status cell, and its segments are held
/// only until its COMMIT is synced.
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
    assert!(!holds_status(&cluster, B, &tid));
    wait_until("the local sync", 10, || held_for(&cluster, B, &tid) == 0).await;
    cluster.restart(B).await;
    assert!(!holds_status(&cluster, B, &tid));
    assert_eq!(held_for(&cluster, B, &tid), 0);
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// The participant fence (T2): C2, C3, C9, C12, C15, C16, operator tool
// ---------------------------------------------------------------------------

/// C3: a participant that is down makes the others wait, live and after a
/// restart, and the transaction commits everywhere once it is back.
///
/// Live, B is stopped. For the restart of A, B is running and marked silent
/// to the fence instead: the harness cannot have two of its three raft
/// members down at once (`bifrost/src/raft/mod.rs:2987`).
#[tokio::test(flavor = "multi_thread")]
async fn a_down_participant_keeps_the_others_waiting_without_a_tear() {
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "2");
    let (mut cluster, client) = Cluster::start("crosssite_c3_down").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 21_000).await;
    drop(client);
    wait_status_synced(&cluster, B, &tid).await;
    cluster.stop(B).await;

    for _ in 0..5 {
        data_site(&cluster, A)
            .sweep_idle_transactions(Duration::ZERO)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(in_doubt_on(&cluster, A, &tid), "A must wait, live");

    cluster.start_site(B).await;
    let b_id = cluster.site(B).server_id;
    super::data_site::silent_peers_for_test().lock().insert(b_id);
    cluster.restart(A).await;
    assert!(in_doubt_on(&cluster, A, &tid), "A must wait after its restart");
    assert!(
        !compact_records(cluster.site(A), &tid, EntryType::BEGIN),
        "the cleaner must not take the parts A waits with"
    );

    super::data_site::silent_peers_for_test().lock().remove(&b_id);
    wait_until("A's resolution", 60, || {
        score_on(cluster.site(A), &ids[0]) == Some(scores[0])
    })
    .await;
    assert_all_or_nothing(&cluster, &ids, &scores, "participant down, then back");
    cluster.shutdown().await;
}

/// C9: a late `end` after the fence is refused, and nothing commits.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_end_after_the_fence_is_refused() {
    let (cluster, client) = Cluster::start("crosssite_c9_late_end").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 22_000, 1).await;
    drop(client);

    data_site(&cluster, A)
        .sweep_idle_transactions(Duration::ZERO)
        .await;
    assert!(!in_doubt_on(&cluster, A, &tid), "A must abort once B is fenced");
    let result = txn.commit(tid.clone()).await;
    assert!(
        !matches!(
            result,
            Ok(Ok(EndResult::Success)) | Ok(Ok(EndResult::DecidedPending))
        ),
        "the client must not hear that the transaction committed: {:?}",
        result
    );
    data_site(&cluster, B)
        .sweep_idle_transactions(Duration::ZERO)
        .await;
    for index in [A, B] {
        assert_ne!(
            cluster.site(index).chunks().durable_txn_status(&tid),
            DurableTxnStatus::Committed,
            "site {} must not commit after the fence",
            index
        );
    }
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(!a && !b, "nothing may commit: A {}, B {}", a, b);
    cluster.shutdown().await;
}

/// C2: no site was told, and the whole cluster loses power. Every
/// participant restarts in doubt, fences the others, and aborts.
#[tokio::test(flavor = "multi_thread")]
async fn a_whole_cluster_power_loss_with_no_decision_aborts_everywhere() {
    crate::ram::segs::power_loss_model::enable();
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "5");
    let (mut cluster, client) = Cluster::start("crosssite_c2_power").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 23_000, 1).await;
    drop(client);
    let limit = super::manager::stop_end_fanout_before_commit_point(tid.clone());
    let _ = txn.commit(tid.clone()).await;
    drop(limit);

    cluster.power_cut_all(&[A, B]).await;
    wait_until("the abort on both sites", 60, || {
        !in_doubt_on(&cluster, A, &tid) && !in_doubt_on(&cluster, B, &tid)
    })
    .await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(!a && !b, "nothing may commit: A {}, B {}", a, b);
    cluster.shutdown().await;
}

/// The fences recorded in the WAL tree of site `index`, as a power loss
/// leaves them.
fn fences_after_power_loss(cluster: &Cluster, index: usize) -> std::collections::BTreeSet<TxnId> {
    let live = cluster.temp[index].path().to_path_buf();
    let image = tempfile::TempDir::new().unwrap();
    crate::ram::segs::power_loss_model::copy_store_as_after_power_loss(
        &live.join("wal"),
        &live.join("backup"),
        &image.path().join("wal"),
        &image.path().join("backup"),
    )
    .unwrap();
    fn find(dir: &std::path::Path) -> Option<std::path::PathBuf> {
        for item in std::fs::read_dir(dir).ok()? {
            let path = item.ok()?.path();
            if path.is_dir() {
                if let Some(found) = find(&path) {
                    return Some(found);
                }
            } else if path.file_name().and_then(|n| n.to_str())
                == Some(crate::ram::fence::FENCE_LOG_FILE)
            {
                return Some(path);
            }
        }
        None
    }
    let Some(path) = find(&image.path().join("wal")) else {
        return std::collections::BTreeSet::new();
    };
    let log = crate::ram::fence::FenceLog::new(path.parent().unwrap().to_str());
    log.load(true).unwrap()
}

/// F3: the fence is durable before the fenced site answers `Fenced`.
#[tokio::test(flavor = "multi_thread")]
async fn a_fence_is_durable_before_the_site_answers() {
    crate::ram::segs::power_loss_model::enable();
    let (cluster, client) = Cluster::start("crosssite_f3_durable_fence").await;
    let (_txn, tid, _ids, _scores) = prepared_transaction(&cluster, &client, 24_000, 1).await;
    drop(client);
    crate::ram::segs::power_loss_model::pause_group_sync(true);
    assert_eq!(data_site(&cluster, B).fence_here(&tid), FenceAnswer::Fenced);
    let durable = fences_after_power_loss(&cluster, B);
    crate::ram::segs::power_loss_model::pause_group_sync(false);
    assert!(
        durable.contains(&tid),
        "the fence must be on disk when B answers `Fenced`: {:?}",
        durable
    );
    cluster.shutdown().await;
}

/// C12 (F3): a site with no bracket of the transaction answers `Unknown` to
/// a fence, loses power, and then gets the coordinator's pending `prepare`.
/// It refuses it: the fence is durable, also without a bracket.
#[tokio::test(flavor = "multi_thread")]
async fn a_fenced_site_without_a_bracket_refuses_a_late_prepare_after_a_power_cut() {
    crate::ram::segs::power_loss_model::enable();
    let (mut cluster, client) = Cluster::start("crosssite_c12_bracketless").await;
    place_slot(&cluster, &client, THERE, B).await;
    drop(client);
    let tid = bifrost::hlc::HlcSource::new(cluster.site(SEED).server_id).now();
    assert_eq!(
        data_site(&cluster, B).fence_here(&tid),
        FenceAnswer::Unknown,
        "test setup: B holds no part of the transaction"
    );
    cluster.power_loss_restart(B).await;
    let answer = late_prepare(&cluster, B, &tid, Id::from_parts(THERE as u64, 31)).await;
    assert!(
        matches!(answer, DMPrepareResult::StateError(_)),
        "a fenced transaction must not be prepared, not {:?}",
        answer
    );
    cluster.shutdown().await;
}

/// Review finding 1: a fence lands while B's `prepare` is between the fence
/// check and the transaction object. B answers the fence, and then must
/// refuse that `prepare` or the coordinator's `end`: a fencer that saw every
/// answer `Fenced`/`Unknown` aborts. Here A is fenced too, so A refuses its
/// `end`; if B commits, the transaction is torn.
#[tokio::test(flavor = "multi_thread")]
async fn a_fence_during_a_prepare_in_flight_is_kept() {
    let (cluster, client) = Cluster::start("crosssite_r1_fence_in_prepare").await;
    place_slot(&cluster, &client, HERE, A).await;
    place_slot(&cluster, &client, THERE, B).await;
    drop(client);
    let ids = vec![
        Id::from_parts(HERE as u64, 41),
        Id::from_parts(THERE as u64, 41),
    ];
    let scores = vec![29_000, 29_001];
    let txn = coordinator(&cluster).await;
    let tid = txn.begin().await.unwrap().unwrap();
    for (id, score) in ids.iter().zip(scores.iter()) {
        txn.write(tid.clone(), cell_of(*id, *score))
            .await
            .unwrap()
            .unwrap();
    }
    let hold = super::data_site::install_prepare_delay_after_fence_check(tid.clone(), ids[1]);
    let prepare = {
        let txn = txn.clone();
        let tid = tid.clone();
        tokio::spawn(async move { txn.prepare(tid).await })
    };
    hold.wait_until_entered().await;
    let b_answer = data_site(&cluster, B).fence_here(&tid);
    hold.release();
    let prepared = prepare.await.unwrap();
    let a_answer = data_site(&cluster, A).fence_here(&tid);
    warn!(
        "CROSSSITE finding 1: B answered {:?} during its prepare, A answered {:?}, the \
         prepare returned {:?}",
        b_answer, a_answer, prepared
    );
    assert!(matches!(
        b_answer,
        FenceAnswer::Fenced | FenceAnswer::Unknown
    ));
    let committed = if matches!(prepared, Ok(Ok(TMPrepareResult::Success))) {
        let result = txn.commit(tid.clone()).await;
        warn!("CROSSSITE finding 1: the commit returned {:?}", result);
        matches!(
            result,
            Ok(Ok(EndResult::Success)) | Ok(Ok(EndResult::DecidedPending))
        )
    } else {
        false
    };
    assert_ne!(
        cluster.site(B).chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "B answered the fence, so it must not commit the transaction"
    );
    assert!(!committed, "the client must not hear that the transaction committed");
    assert_all_or_nothing(&cluster, &ids, &scores, "fence during a prepare in flight");
    cluster.shutdown().await;
}

/// C16 (F6): A aborts after the fence; the cleaner runs on A; A restarts;
/// a duplicate `prepare` arrives. A refuses it: the fence survives the
/// compaction and the start.
#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_site_refuses_a_duplicate_prepare_after_compaction_and_restart() {
    let (mut cluster, client) = Cluster::start("crosssite_c16_duplicate_prepare").await;
    let (_txn, tid, ids, _scores) = prepared_transaction(&cluster, &client, 25_500, 1).await;
    drop(client);
    data_site(&cluster, A)
        .sweep_idle_transactions(Duration::ZERO)
        .await;
    assert!(!in_doubt_on(&cluster, A, &tid), "test setup: A must abort");
    let compacted = compact_records(cluster.site(A), &tid, EntryType::BEGIN);
    warn!("CROSSSITE A's aborted bracket compacted: {}", compacted);
    cluster.restart(A).await;
    let answer = late_prepare(&cluster, A, &tid, ids[0]).await;
    assert!(
        matches!(answer, DMPrepareResult::StateError(_)),
        "a duplicate prepare after the fence must be refused, not {:?}",
        answer
    );
    cluster.shutdown().await;
}

/// C15 (F2): without the prepare sync, a power cut after the yes vote
/// tears the transaction. This is why the prepare round syncs (P0).
#[tokio::test(flavor = "multi_thread")]
async fn without_the_prepare_sync_a_power_cut_tears_the_transaction() {
    crate::ram::segs::power_loss_model::enable();
    let (mut cluster, client) = Cluster::start("crosssite_c15_prepare_sync").await;
    super::data_site::PREPARE_SYNC_SKIPPED_FOR_TEST
        .store(true, std::sync::atomic::Ordering::Release);
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 26_500, 1).await;
    super::data_site::PREPARE_SYNC_SKIPPED_FOR_TEST
        .store(false, std::sync::atomic::Ordering::Release);
    drop(client);
    crate::ram::segs::power_loss_model::pause_group_sync(true);
    let limit = super::manager::stop_end_fanout_after_commit_point(tid.clone());
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    wait_status_synced(&cluster, B, &tid).await;
    cluster.power_loss_restart(A).await;
    crate::ram::segs::power_loss_model::pause_group_sync(false);
    let (a, b) = halves(&cluster, &ids, &scores);
    warn!("CROSSSITE C15 without the prepare sync: A {}, B {}", a, b);
    assert!(
        !a && b,
        "the demonstration expects the tear that P0 prevents: A {}, B {}",
        a,
        b
    );
    cluster.shutdown().await;
}

/// Restarts during the fence: B is fenced, then A and B restart. Every
/// site ends aborted; a late `end` commits nothing.
#[tokio::test(flavor = "multi_thread")]
async fn restarts_during_the_fence_abort_everywhere() {
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "5");
    let (mut cluster, client) = Cluster::start("crosssite_f5_restart_fence").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 25_000, 1).await;
    drop(client);
    assert_eq!(data_site(&cluster, B).fence_here(&tid), FenceAnswer::Fenced);
    cluster.restart(A).await;
    cluster.restart(B).await;
    let result = txn.commit(tid.clone()).await;
    warn!("CROSSSITE late end after restarts returned {:?}", result);
    wait_until("the abort on both sites", 60, || {
        !in_doubt_on(&cluster, A, &tid) && !in_doubt_on(&cluster, B, &tid)
    })
    .await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(!a && !b, "nothing may commit: A {}, B {}", a, b);
    cluster.shutdown().await;
}

/// The operator tool: B committed and is lost for good. A waits for it; the
/// operator resolves A's half, live (B stopped) and after a restart (B
/// silent to the fence).
#[tokio::test(flavor = "multi_thread")]
async fn the_force_resolve_tool_decides_for_a_lost_participant() {
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "1");
    let (mut cluster, client) = Cluster::start("crosssite_f6_force").await;
    let (live, live_ids, live_scores) = torn_transaction_seq(&cluster, &client, 26_000, 1).await;
    let (parked, parked_ids, _) = torn_transaction_seq(&cluster, &client, 27_000, 2).await;
    drop(client);
    cluster.stop(B).await;

    let address = cluster.addresses[A].clone();
    let group = cluster.group.clone();
    assert_eq!(
        super::force_resolve_in_doubt(&address, &group, &group, live.clone(), ForcedOutcome::Commit)
            .await
            .unwrap(),
        ForceResolveResult::Resolved
    );
    assert_eq!(score_on(cluster.site(A), &live_ids[0]), Some(live_scores[0]));
    assert!(!in_doubt_on(&cluster, A, &live));
    assert_eq!(
        super::force_resolve_in_doubt(&address, &group, &group, live.clone(), ForcedOutcome::Commit)
            .await
            .unwrap(),
        ForceResolveResult::NotInDoubt
    );

    // B stays lost to the fence; the harness cannot have two of its three
    // raft members down at once (`bifrost/src/raft/mod.rs:2987`).
    cluster.start_site(B).await;
    let b_id = cluster.site(B).server_id;
    super::data_site::silent_peers_for_test().lock().insert(b_id);
    cluster.restart(A).await;
    assert!(in_doubt_on(&cluster, A, &parked), "test setup: A must wait for B");
    assert_eq!(
        super::force_resolve_in_doubt(&address, &group, &group, parked.clone(), ForcedOutcome::Abort)
            .await
            .unwrap(),
        ForceResolveResult::Resolved
    );
    assert!(!in_doubt_on(&cluster, A, &parked));
    assert_eq!(score_on(cluster.site(A), &parked_ids[0]), None);
    super::data_site::silent_peers_for_test().lock().remove(&b_id);
    cluster.shutdown().await;
}

/// D33: a coordinator that loses a site's answer after `end` was sent
/// reports `OutcomeUnknown`, and the client maps it to a typed error.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_end_answer_is_reported_as_outcome_unknown() {
    let (cluster, client) = Cluster::start("crosssite_d33_unknown").await;
    let (txn, tid, _ids, _scores) = prepared_transaction(&cluster, &client, 28_000, 1).await;
    drop(client);
    let limit = super::manager::stop_end_fanout_after_commit_point(tid.clone());
    let result = txn.commit(tid.clone()).await;
    drop(limit);
    assert!(
        matches!(result, Ok(Err(TMError::OutcomeUnknown))),
        "the coordinator must report the outcome as unknown, not {:?}",
        result
    );
    cluster.shutdown().await;
}
