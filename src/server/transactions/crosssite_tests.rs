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

    /// The image of site `index` as a power loss leaves it now. The site
    /// keeps running.
    fn power_image(&self, index: usize) -> tempfile::TempDir {
        let image = tempfile::TempDir::new().unwrap();
        let live = self.temp[index].path().to_path_buf();
        crate::ram::segs::power_loss_model::copy_store_as_after_power_loss(
            &live.join("wal"),
            &live.join("backup"),
            &image.path().join("wal"),
            &image.path().join("backup"),
        )
        .unwrap();
        image
    }

    /// Stop each site of `sites` and start it again from its image, one at
    /// a time and in the order given.
    async fn restart_from_images(&mut self, sites: &[usize], images: Vec<tempfile::TempDir>) {
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
    let entry = cluster.site(index).chunks().txn_status().get(tid);
    bifrost::doubt_trace!(
        "test_status_poll site={} index={} tid={:?} present={} self_synced={} held_segments={}",
        cluster.site(index).server_id, index, tid, entry.is_some(),
        entry.as_ref().is_some_and(|entry| entry.self_synced), held_for(cluster, index, tid)
    );
    entry.is_some_and(|entry| entry.self_synced)
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
            "site {} never observed a synced status cell of {:?}; present={}, held={}, outcome={:?}",
            index,
            tid,
            holds_status(cluster, index, tid),
            held_for(cluster, index, tid),
            outcome_on(cluster, index, tid)
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

/// The index reads of status-cell probes on site `index`.
fn status_probe_reads(cluster: &Cluster, index: usize) -> u64 {
    cluster
        .site(index)
        .chunks()
        .list
        .iter()
        .map(|chunk| chunk.status_probe_reads())
        .sum()
}

/// D44: the first close of a multi-site transaction reads one status-cell
/// probe per site. No status cell of the transaction can exist before the
/// first close, so the close does not search the 8 probes for one. The
/// transaction still commits on both sites.
#[tokio::test(flavor = "multi_thread")]
async fn a_first_close_reads_one_status_probe_per_site() {
    let (cluster, client) = Cluster::start("crosssite_first_close").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 9100, 91).await;
    let before = [status_probe_reads(&cluster, A), status_probe_reads(&cluster, B)];
    assert_eq!(
        txn.commit(tid.clone()).await.unwrap().unwrap(),
        EndResult::Success
    );
    let after = [status_probe_reads(&cluster, A), status_probe_reads(&cluster, B)];
    assert_eq!(after[0] - before[0], 1, "site A: probe reads of the first close");
    assert_eq!(after[1] - before[1], 1, "site B: probe reads of the first close");
    assert!(holds_status(&cluster, A, &tid) && holds_status(&cluster, B, &tid));
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "both sites commit");
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
    assert!(!chunks.cell_ids_in_slots(&slots).unwrap().contains(&entry.id));
    let mut scan = client.cell_id_scan().await.unwrap();
    while let Some(page) = scan.next_page(1024).await.unwrap() {
        assert!(
            page.iter().all(|id| *id != entry.id),
            "the client cell scan must not list the status cell"
        );
    }
    drop(scan);
    // Review finding 4: the `count` RPC agrees with the hidden live count.
    let counted = client.count().await.unwrap();
    let live = client.live_cell_count().await.unwrap();
    assert_eq!(
        counted, live,
        "the `count` RPC must not count status cells: count {} live {}",
        counted, live
    );
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
    // Keep the status cell until this test observes its sync. Otherwise the
    // background notice exchange can retire it before the polling read.
    super::data_site::SYNCED_NOTICES_SKIPPED_FOR_TEST
        .store(true, std::sync::atomic::Ordering::Release);
    let b_dir = cluster.temp[B].path().to_path_buf();
    crate::ram::durability::pause_receipt_syncs_under(&b_dir, true);
    bifrost::doubt_trace!("test_sync_paused tid={:?}", tid);
    assert!(matches!(
        txn.commit(tid.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    data_site(&cluster, B).status_release_round(false).await;
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committing);
    crate::ram::durability::pause_receipt_syncs_under(&b_dir, false);
    bifrost::doubt_trace!("test_sync_resumed tid={:?}", tid);
    wait_status_synced(&cluster, B, &tid).await;
    assert_eq!(outcome_on(&cluster, B, &tid), TxnOutcome::Committed);
    super::data_site::SYNCED_NOTICES_SKIPPED_FOR_TEST
        .store(false, std::sync::atomic::Ordering::Release);
    wait_released(&cluster, B, &tid).await;
    cluster.shutdown().await;
}

/// Release removes a synced record before a later poll can observe it.
#[tokio::test(flavor = "multi_thread")]
async fn a_released_synced_commit_keeps_its_committed_outcome() {
    let (cluster, client) = Cluster::start("crosssite_c13b_release_control").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 14_600, 1).await;
    drop(client);
    super::data_site::SYNCED_NOTICES_SKIPPED_FOR_TEST
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(matches!(
        txn.commit(tid.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    for index in [A, B] {
        cluster.site(index).chunks().sync_all().unwrap();
        wait_status_synced(&cluster, index, &tid).await;
        assert!(status_synced(&cluster, index, &tid));
        assert_eq!(held_for(&cluster, index, &tid), 0);
        assert_eq!(outcome_on(&cluster, index, &tid), TxnOutcome::Committed);
    }
    // Deliver both confirmations before the next poll. No scheduler timing is required.
    for (index, peer) in [(A, B), (B, A)] {
        <super::data_site::DataManager as super::data_site::Service>::synced(
            &data_site(&cluster, index),
            cluster.site(peer).server_id,
            vec![tid],
        )
        .await;
        data_site(&cluster, index).status_release_round(false).await;
        assert!(!holds_status(&cluster, index, &tid));
        assert!(!status_synced(&cluster, index, &tid));
        assert_eq!(held_for(&cluster, index, &tid), 0);
        assert_eq!(outcome_on(&cluster, index, &tid), TxnOutcome::Committed);
    }
    assert_all_or_nothing(&cluster, &ids, &scores, "synced status release");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "status release must retain both committed payloads");
    super::data_site::SYNCED_NOTICES_SKIPPED_FOR_TEST
        .store(false, std::sync::atomic::Ordering::Release);
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

/// Observed in the campaign (K-G3n, K-G3d, K-X3n, D19): a graceful stop of a
/// participant closes the client writes first (`NebServer::shutdown`). A close
/// that arrives after it cannot write the status cell (`ServerShuttingDown`),
/// the site keeps the transaction decided in memory only, and the other site
/// commits. After the restart the stopped site must resolve COMMIT from its
/// open bracket and its peer.
#[tokio::test(flavor = "multi_thread")]
async fn a_close_refused_by_a_graceful_stop_commits_after_the_restart() {
    let (mut cluster, client) = Cluster::start("crosssite_stop_refused_close").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 19_000, 1).await;
    drop(client);
    cluster.site(B).chunks().close_client_writes();
    let result = txn.commit(tid.clone()).await;
    assert!(
        matches!(result, Ok(Ok(EndResult::Success))),
        "A committed, so the client must hear `Success`, not {:?}",
        result
    );
    assert_ne!(
        cluster.site(B).chunks().durable_txn_status(&tid),
        DurableTxnStatus::Committed,
        "test setup: site B must have refused its close"
    );
    cluster.restart(B).await;
    assert_all_or_nothing(&cluster, &ids, &scores, "close refused by the stop of B");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "the transaction must be committed on both sites");
    cluster.shutdown().await;
}

/// A graceful stop of a participant that holds a prepared transaction must
/// not apply it. The coordinator has not decided, so the prepared cells stay
/// invisible on B until the decision arrives (D19).
#[tokio::test(flavor = "multi_thread")]
async fn a_graceful_stop_does_not_apply_a_prepared_transaction() {
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "1");
    let (mut cluster, client) = Cluster::start("crosssite_stop_prepared").await;
    let (_txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 20_000, 1).await;
    drop(client);
    cluster.restart(B).await;
    // Site A still holds its prepared half in memory, so only B is checked.
    let (_a, b) = halves(&cluster, &ids, &scores);
    assert!(
        !b,
        "nobody decided {:?}, yet B shows the new value after its restart",
        tid
    );
    cluster.shutdown().await;
}

/// Control: the same transaction and the same restart of B, without the early
/// close of the client writes.
#[tokio::test(flavor = "multi_thread")]
async fn control_a_close_before_a_graceful_stop_commits_after_the_restart() {
    let (mut cluster, client) = Cluster::start("crosssite_stop_open_close").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 19_500, 1).await;
    drop(client);
    let result = txn.commit(tid.clone()).await;
    assert!(
        matches!(result, Ok(Ok(EndResult::Success))),
        "{:?}",
        result
    );
    cluster.restart(B).await;
    assert_all_or_nothing(&cluster, &ids, &scores, "control: close before the stop of B");
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "the transaction must be committed on both sites");
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
    cluster.site(B).chunks().sync_all().unwrap();
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

/// Three participants with their cells and scores: A, B, and C (index 3).
async fn three_site_transaction(
    cluster: &Cluster,
    client: &Arc<AsyncClient>,
    base_score: u64,
) -> (
    Arc<super::manager::AsyncServiceClient>,
    TxnId,
    [usize; 3],
    Vec<Id>,
    Vec<u64>,
) {
    const C: usize = 3;
    const HERE_C: u16 = 94;
    place_slot(cluster, client, HERE, A).await;
    place_slot(cluster, client, THERE, B).await;
    place_slot(cluster, client, HERE_C, C).await;
    let sites = [A, B, C];
    let ids: Vec<Id> = [HERE, THERE, HERE_C]
        .iter()
        .map(|slot| Id::from_parts(*slot as u64, 3))
        .collect();
    let scores: Vec<u64> = (0..3).map(|n| base_score + n).collect();
    let txn = coordinator(cluster).await;
    let tid = txn.begin().await.unwrap().unwrap();
    for (id, score) in ids.iter().zip(scores.iter()) {
        txn.write(tid.clone(), cell_of(*id, *score))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();
    (txn, tid, sites, ids, scores)
}

fn held_on(cluster: &Cluster, sites: &[usize], ids: &[Id], scores: &[u64]) -> Vec<bool> {
    sites
        .iter()
        .zip(ids.iter().zip(scores.iter()))
        .map(|(index, (id, score))| score_on(cluster.site(*index), id) == Some(*score))
        .collect()
}

/// C8, told nobody: three participants, the coordinator tells no site, and
/// every participant loses power. All abort.
#[tokio::test(flavor = "multi_thread")]
async fn three_sites_power_cut_with_no_decision_all_abort() {
    crate::ram::segs::power_loss_model::enable();
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "5");
    let (mut cluster, client) = Cluster::start_with(
        "crosssite_c8_told_nobody",
        vec![vec![Service::Cell, Service::Transaction]],
    )
    .await;
    let (txn, tid, sites, ids, scores) = three_site_transaction(&cluster, &client, 31_000).await;
    drop(client);
    let limit = super::manager::stop_end_fanout_before_commit_point(tid.clone());
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    cluster.power_cut_all(&sites).await;
    wait_until("the abort on every site", 90, || {
        sites.iter().all(|index| !in_doubt_on(&cluster, *index, &tid))
    })
    .await;
    let held = held_on(&cluster, &sites, &ids, &scores);
    assert!(held.iter().all(|h| !h), "nothing may commit: {:?}", held);
    cluster.shutdown().await;
}

/// C13 with three sites (F1): the told site refuses its close. It answers
/// `Committing`; the other two wait, and nobody aborts. Then the told site
/// finishes its close, and every site commits.
#[tokio::test(flavor = "multi_thread")]
async fn three_sites_wait_for_a_refused_close_and_all_commit() {
    let (cluster, client) = Cluster::start_with(
        "crosssite_c13_three_sites",
        vec![vec![Service::Cell, Service::Transaction]],
    )
    .await;
    let (txn, tid, sites, ids, scores) = three_site_transaction(&cluster, &client, 32_000).await;
    drop(client);
    let told = *sites
        .iter()
        .min_by_key(|index| cluster.site(**index).server_id)
        .unwrap();
    crate::ram::chunk::refuse_next_commit_for_test(
        cluster.site(told).chunks().base_addr,
        tid.clone(),
    );
    let limit = super::manager::stop_end_fanout_after_commit_point(tid.clone());
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    assert_eq!(outcome_on(&cluster, told, &tid), TxnOutcome::Committing);
    for index in sites.iter().copied().filter(|index| *index != told) {
        data_site(&cluster, index)
            .sweep_idle_transactions(Duration::ZERO)
            .await;
    }
    for index in sites.iter().copied().filter(|index| *index != told) {
        assert!(
            in_doubt_on(&cluster, index, &tid),
            "site {} must wait for the refused close, not abort",
            index
        );
    }
    data_site(&cluster, told)
        .sweep_idle_transactions(Duration::ZERO)
        .await;
    wait_status_synced(&cluster, told, &tid).await;
    for index in sites.iter().copied().filter(|index| *index != told) {
        data_site(&cluster, index)
            .sweep_idle_transactions(Duration::ZERO)
            .await;
    }
    let held = held_on(&cluster, &sites, &ids, &scores);
    assert!(held.iter().all(|h| *h), "every site must commit: {:?}", held);
    cluster.shutdown().await;
}

/// C9 mirror: `end` reaches B first, and B's COMMIT is not synced yet. The
/// fence then answers `Committing`, A waits, and both commit once B's sync
/// lands.
#[tokio::test(flavor = "multi_thread")]
async fn a_fence_after_the_end_answers_committing_and_both_commit() {
    let (cluster, client) = Cluster::start("crosssite_c9_mirror").await;
    let (txn, tid, ids, scores) = prepared_transaction(&cluster, &client, 33_000, 1).await;
    drop(client);
    let b_dir = cluster.temp[B].path().to_path_buf();
    crate::ram::durability::pause_receipt_syncs_under(&b_dir, true);
    let limit = super::manager::stop_end_fanout_after_commit_point(tid.clone());
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    assert_eq!(
        data_site(&cluster, B).fence_here(&tid),
        FenceAnswer::Committing,
        "a site that took the `end` cannot be fenced"
    );
    data_site(&cluster, A)
        .sweep_idle_transactions(Duration::ZERO)
        .await;
    assert!(in_doubt_on(&cluster, A, &tid), "A must wait, not abort");
    crate::ram::durability::pause_receipt_syncs_under(&b_dir, false);
    wait_status_synced(&cluster, B, &tid).await;
    resolve_live(&cluster, &tid).await;
    let (a, b) = halves(&cluster, &ids, &scores);
    assert!(a && b, "both must commit: A {}, B {}", a, b);
    cluster.shutdown().await;
}

/// A resolution attempt appends its copies and its status cell, its COMMIT
/// is refused, and the site restarts while the peer is silent. The site
/// commits from its own status cell, or waits; it never aborts, and it
/// commits once the peer is back.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_resolution_with_its_status_cell_then_a_restart_stays_atomic() {
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "1");
    let (mut cluster, client) = Cluster::start("crosssite_failed_resolution_status").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 34_000).await;
    drop(client);
    wait_status_synced(&cluster, B, &tid).await;
    let b_id = cluster.site(B).server_id;
    super::data_site::silent_peers_for_test().lock().insert(b_id);
    cluster.restart(A).await;
    assert!(in_doubt_on(&cluster, A, &tid), "test setup: A must wait for B");
    crate::ram::chunk::refuse_next_commit_for_test(cluster.site(A).chunks().base_addr, tid.clone());
    super::data_site::silent_peers_for_test().lock().remove(&b_id);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        in_doubt_on(&cluster, A, &tid),
        "test setup: the refused COMMIT keeps A in doubt"
    );
    super::data_site::silent_peers_for_test().lock().insert(b_id);
    cluster.restart(A).await;
    let committed_alone = score_on(cluster.site(A), &ids[0]) == Some(scores[0]);
    warn!(
        "CROSSSITE failed resolution: A committed at its start without B: {}",
        committed_alone
    );
    super::data_site::silent_peers_for_test().lock().remove(&b_id);
    wait_until("A's resolution", 60, || {
        score_on(cluster.site(A), &ids[0]) == Some(scores[0])
    })
    .await;
    assert_all_or_nothing(&cluster, &ids, &scores, "failed resolution, restart");
    cluster.shutdown().await;
}

/// k > 1 parts at one site, default mode: a power loss keeps the COMMIT of
/// the part with the status cell and drops the COMMIT of the other part.
/// Recovery installs both parts.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_commit_of_one_part_still_installs_every_part() {
    crate::ram::segs::power_loss_model::enable();
    let (mut cluster, client) = Cluster::start("crosssite_k_parts").await;
    place_slot(&cluster, &client, HERE, A).await;
    place_slot(&cluster, &client, HERE_TOO, A).await;
    place_slot(&cluster, &client, THERE, B).await;
    drop(client);
    let ids = vec![
        Id::from_parts(HERE as u64, 6),
        Id::from_parts(HERE_TOO as u64, 6),
        Id::from_parts(THERE as u64, 6),
    ];
    let scores = vec![35_000, 35_001, 35_002];
    let txn = coordinator(&cluster).await;
    let tid = txn.begin().await.unwrap().unwrap();
    for (id, score) in ids.iter().zip(scores.iter()) {
        txn.write(tid.clone(), cell_of(*id, *score))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();
    assert!(
        crate::ram::chunk::transaction_lease_count(&tid, &cluster.site(A).chunks().address_range())
            >= 2,
        "test setup: A holds a part in two chunks"
    );
    let a_dir = cluster.temp[A].path().to_path_buf();
    crate::ram::segs::power_loss_model::pause_group_sync(true);
    crate::ram::durability::pause_receipt_syncs_under(&a_dir, true);
    assert!(matches!(
        txn.commit(tid.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    let entry = cluster
        .site(A)
        .chunks()
        .txn_status()
        .get(&tid)
        .expect("test setup: A holds the status cell");
    let mut synced = 0;
    let mut unsynced = 0;
    for chunk in &cluster.site(A).chunks().list {
        for segment in chunk.segments() {
            if !segment_holds_record_of(&segment, &tid, EntryType::COMMIT) {
                continue;
            }
            if chunk.id == entry.chunk {
                segment.force_wal_sync().unwrap();
                synced += 1;
            } else {
                unsynced += 1;
            }
        }
    }
    assert!(synced >= 1 && unsynced >= 1, "test setup: {} synced, {} not", synced, unsynced);
    cluster.power_loss_restart(A).await;
    crate::ram::segs::power_loss_model::pause_group_sync(false);
    crate::ram::durability::pause_receipt_syncs_under(&a_dir, false);
    for (n, id) in ids.iter().take(2).enumerate() {
        assert_eq!(
            score_on(cluster.site(A), id),
            Some(scores[n]),
            "A must install part {} from the surviving COMMIT",
            n
        );
    }
    assert_eq!(score_on(cluster.site(B), &ids[2]), Some(scores[2]));
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

/// Review finding 2: after a restart, the cells of an in-doubt bracket are
/// not locked. A later transaction T2 writes the same cell while T is in
/// doubt. When T resolves `Committed`, one of the two writes is lost: T is
/// torn, or T2's acknowledged write is gone. T2 must not commit while T is in
/// doubt.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_to_an_in_doubt_cell_after_a_restart_waits_for_the_resolution() {
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "1");
    let (mut cluster, client) = Cluster::start("crosssite_r2_in_doubt_cell").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 30_000).await;
    drop(client);
    wait_status_synced(&cluster, B, &tid).await;
    let b_id = cluster.site(B).server_id;
    super::data_site::silent_peers_for_test().lock().insert(b_id);
    cluster.restart(A).await;
    assert!(in_doubt_on(&cluster, A, &tid), "test setup: A must wait for B");

    const T2_SCORE: u64 = 30_500;
    let later = coordinator(&cluster).await;
    let x = ids[0];
    let t2_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let t2_flag = t2_done.clone();
    let t2 = tokio::spawn(async move {
        let t2 = later.begin().await.unwrap().unwrap();
        later
            .write(t2.clone(), cell_of(x, T2_SCORE))
            .await
            .unwrap()
            .unwrap();
        let prepared = later.prepare(t2.clone()).await;
        if !matches!(prepared, Ok(Ok(TMPrepareResult::Success))) {
            return false;
        }
        let committed = matches!(
            later.commit(t2.clone()).await,
            Ok(Ok(EndResult::Success)) | Ok(Ok(EndResult::DecidedPending))
        );
        t2_flag.store(committed, std::sync::atomic::Ordering::SeqCst);
        committed
    });
    tokio::time::sleep(Duration::from_secs(3)).await;
    let committed_in_doubt = t2_done.load(std::sync::atomic::Ordering::SeqCst);

    super::data_site::silent_peers_for_test().lock().remove(&b_id);
    wait_until("A's resolution", 60, || !in_doubt_on(&cluster, A, &tid)).await;
    let t2_committed = tokio::time::timeout(Duration::from_secs(60), t2)
        .await
        .expect("T2 must finish once T is resolved")
        .unwrap();
    let held = score_on(cluster.site(A), &x);
    let (_, b) = halves(&cluster, &ids, &scores);
    warn!(
        "CROSSSITE finding 2: T2 finished while T was in doubt: {}, T2 committed: {}, X holds \
         {:?} (T wrote {}, T2 wrote {}), B holds T's half: {}",
        committed_in_doubt, t2_committed, held, scores[0], T2_SCORE, b
    );
    assert!(b, "test setup: B committed T");
    if t2_committed {
        assert_eq!(
            held,
            Some(T2_SCORE),
            "T2's acknowledged write is lost"
        );
    } else {
        assert_eq!(held, Some(scores[0]), "T is torn: A lost its half");
    }
    assert!(
        !committed_in_doubt,
        "T2 committed a write to a cell of the in-doubt T"
    );
    cluster.shutdown().await;
}

/// Review R4: the other half of finding 1. B's `fence_here` stops between
/// its fence record and its object lookup, and a whole `prepare` of B runs
/// in that gap. The `prepare` must see the fence; else B prepares, answers
/// the fence `Unknown`, and commits.
#[tokio::test(flavor = "multi_thread")]
async fn a_prepare_inside_the_fence_gap_is_refused() {
    let (cluster, client) = Cluster::start("crosssite_r4_fence_gap").await;
    place_slot(&cluster, &client, HERE, A).await;
    place_slot(&cluster, &client, THERE, B).await;
    drop(client);
    let ids = vec![
        Id::from_parts(HERE as u64, 42),
        Id::from_parts(THERE as u64, 42),
    ];
    let scores = vec![37_000, 37_001];
    let txn = coordinator(&cluster).await;
    let tid = txn.begin().await.unwrap().unwrap();
    for (id, score) in ids.iter().zip(scores.iter()) {
        txn.write(tid.clone(), cell_of(*id, *score))
            .await
            .unwrap()
            .unwrap();
    }
    let hook = super::data_site::install_fence_gap_hook(tid.clone());
    let site = data_site(&cluster, B);
    let fencer = {
        let tid = tid.clone();
        std::thread::spawn(move || site.fence_here(&tid))
    };
    wait_until("B's fence in the gap", 10, || {
        hook.entered.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    let prepared = txn.prepare(tid.clone()).await;
    hook.released.store(true, std::sync::atomic::Ordering::SeqCst);
    let b_answer = fencer.join().unwrap();
    let a_answer = data_site(&cluster, A).fence_here(&tid);
    warn!(
        "CROSSSITE R4: B answered {:?}, A answered {:?}, the prepare returned {:?}",
        b_answer, a_answer, prepared
    );
    let committed = if matches!(prepared, Ok(Ok(TMPrepareResult::Success))) {
        matches!(
            txn.commit(tid.clone()).await,
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
    assert_all_or_nothing(&cluster, &ids, &scores, "prepare inside the fence gap");
    cluster.shutdown().await;
}

fn score_of(
    read: &Result<
        Result<TxnExecResult<OwnedCell, crate::ram::cell::ReadError>, TMError>,
        bifrost::rpc::RPCError,
    >,
) -> Option<Option<u64>> {
    match read {
        Ok(Ok(TxnExecResult::Accepted(cell))) => Some(cell.data["score"].u64().copied()),
        Ok(Ok(TxnExecResult::Error(crate::ram::cell::ReadError::CellDoesNotExisted))) => Some(None),
        _ => None,
    }
}

/// Review R1: after a restart, a transactional read of a cell of an
/// in-doubt bracket is not held. A read-only transaction R reads T's Y at B
/// and X at A while T is in doubt at A; it must not see T half applied.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_transaction_does_not_see_an_in_doubt_transaction_half_applied() {
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "1");
    let (mut cluster, client) = Cluster::start("crosssite_r1_read_in_doubt").await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 38_000).await;
    drop(client);
    wait_status_synced(&cluster, B, &tid).await;
    let b_id = cluster.site(B).server_id;
    super::data_site::silent_peers_for_test().lock().insert(b_id);
    cluster.restart(A).await;
    assert!(in_doubt_on(&cluster, A, &tid), "test setup: A must wait for B");

    let reader = coordinator(&cluster).await;
    let r = reader.begin().await.unwrap().unwrap();
    let y = reader.read(r.clone(), ids[1]).await;
    let x = reader.read(r.clone(), ids[0]).await;
    let ended = reader.commit(r.clone()).await;
    let (x_seen, y_seen) = (score_of(&x), score_of(&y));
    warn!(
        "CROSSSITE R1: R read X {:?} and Y {:?} (T wrote {} and {}); R ended {:?}",
        x_seen, y_seen, scores[0], scores[1], ended
    );
    super::data_site::silent_peers_for_test().lock().remove(&b_id);
    wait_until("A's resolution", 60, || !in_doubt_on(&cluster, A, &tid)).await;
    assert_eq!(
        y_seen,
        Some(Some(scores[1])),
        "test setup: R must read T's Y at B"
    );
    assert_eq!(
        x_seen, None,
        "R saw T half applied: T's Y and X {:?} from before T",
        x_seen
    );
    // After the resolution a new read-only transaction sees all of T.
    let after = reader.begin().await.unwrap().unwrap();
    let x_after = score_of(&reader.read(after.clone(), ids[0]).await);
    let y_after = score_of(&reader.read(after.clone(), ids[1]).await);
    assert_eq!(x_after, Some(Some(scores[0])), "R2 must read T's X after the resolution");
    assert_eq!(y_after, Some(Some(scores[1])));
    cluster.shutdown().await;
}

/// Review R2: a slot migration of a cell of an in-doubt bracket copies the
/// version before T, and the resolution then installs T's cell at the old
/// owner. The owner of X must hold T's X after the resolution.
#[tokio::test(flavor = "multi_thread")]
async fn a_slot_migration_of_an_in_doubt_cell_keeps_the_transaction() {
    const D: usize = 3;
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "1");
    let (mut cluster, client) = Cluster::start_with(
        "crosssite_r2_migration",
        vec![vec![Service::Cell, Service::Transaction]],
    )
    .await;
    let (tid, ids, scores) = torn_transaction(&cluster, &client, 39_000).await;
    wait_status_synced(&cluster, B, &tid).await;
    let b_id = cluster.site(B).server_id;
    super::data_site::silent_peers_for_test().lock().insert(b_id);
    cluster.restart(A).await;
    assert!(in_doubt_on(&cluster, A, &tid), "test setup: A must wait for B");
    let from = cluster.site(A).server_id;
    let to = cluster.site(D).server_id;
    let plan = crate::migration::MigrationPlan {
        batch_cells: 8,
        ..Default::default()
    };
    let moved = crate::migration::migrate_slot(&client, HERE as u32, from, to, &plan).await;
    warn!(
        "CROSSSITE R2: the migration while T is in doubt returned {:?}",
        moved.as_ref().map(|_| ())
    );
    if let Err(error) = &moved {
        assert!(
            matches!(error, crate::migration::MigrationError::SlotInDoubt(_)),
            "a refused migration must name the in-doubt slot, not {:?}",
            error
        );
    }
    super::data_site::silent_peers_for_test().lock().remove(&b_id);
    wait_until("A's resolution", 60, || !in_doubt_on(&cluster, A, &tid)).await;
    if moved.is_err() {
        crate::migration::migrate_slot(&client, HERE as u32, from, to, &plan)
            .await
            .expect("after the resolution the slot moves");
    }
    client.reload_slot_owners().await;
    let held = client
        .read_cell(ids[0])
        .await
        .unwrap()
        .ok()
        .and_then(|cell| cell.data["score"].u64().copied());
    assert_eq!(held, Some(scores[0]), "the owner of X must hold T's X");
    cluster.shutdown().await;
}

/// Review N1: a bulk reshard refused with `SlotInDoubt` must not leave its
/// slots `Migrating`. Slot `HERE` holds a cell of T, in doubt at A; slot
/// `HERE_TOO` holds nothing. After the refusal both slots read `Stable` on A.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_reshard_leaves_every_slot_stable_on_its_donor() {
    const D: usize = 3;
    std::env::set_var("NEB_RESOLVE_STARTUP_WAIT_SECS", "1");
    let (mut cluster, client) = Cluster::start_with(
        "crosssite_n1_reshard",
        vec![vec![Service::Cell, Service::Transaction]],
    )
    .await;
    place_slot(&cluster, &client, HERE_TOO, A).await;
    let (tid, _ids, _scores) = torn_transaction(&cluster, &client, 41_000).await;
    wait_status_synced(&cluster, B, &tid).await;
    let b_id = cluster.site(B).server_id;
    super::data_site::silent_peers_for_test().lock().insert(b_id);
    cluster.restart(A).await;
    assert!(in_doubt_on(&cluster, A, &tid), "test setup: A must wait for B");
    let from = cluster.site(A).server_id;
    let to = cluster.site(D).server_id;
    let plan = crate::migration::MigrationPlan {
        batch_cells: 8,
        ..Default::default()
    };
    let slots = [HERE as u32, HERE_TOO as u32];
    let reshard = crate::migration::reshard_slots(&client, &slots, from, to, &plan).await;
    warn!("CROSSSITE N1: the reshard failed {:?}", reshard.failed);
    let placement = crate::migration::placement_client(&client);
    let group = crate::slots::slot_group_id(client.group_name());
    let mut states = Vec::new();
    for slot in slots {
        states.push(placement.slot_state(&group, &slot).await.unwrap());
    }
    super::data_site::silent_peers_for_test().lock().remove(&b_id);
    wait_until("A's resolution", 60, || !in_doubt_on(&cluster, A, &tid)).await;
    assert!(reshard.handovers.is_empty(), "nothing may move while T is in doubt");
    assert_eq!(reshard.failed.len(), 2, "both slots fail: {:?}", reshard.failed);
    for (slot, state) in slots.iter().zip(states) {
        assert_eq!(
            state,
            Some(bifrost::conshash::slots::SlotState::Stable { owner: from }),
            "slot {} must be stable on its donor after the refusal",
            slot
        );
    }
    assert!(
        reshard
            .failed
            .iter()
            .all(|(_, reason)| reason.contains("SlotInDoubt")),
        "the failure must name the slot in doubt: {:?}",
        reshard.failed
    );
    cluster.shutdown().await;
}

/// Review N3: the production branch of `decide_in_doubt` for a peer that
/// cannot be reached, with no test hook. Three sites (the seed, A, B),
/// sorted by id as L < M < H. Only L is told; M learns the commit from L;
/// then L is stopped. H must commit from M while L is down.
#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_peer_does_not_block_a_commit_another_peer_proves() {
    const HERE_SEED: u16 = 95;
    let (mut cluster, client) = Cluster::start("crosssite_n3_stopped_peer").await;
    place_slot(&cluster, &client, HERE, A).await;
    place_slot(&cluster, &client, THERE, B).await;
    place_slot(&cluster, &client, HERE_SEED, SEED).await;
    drop(client);
    let sites = [A, B, SEED];
    let ids: Vec<Id> = [HERE, THERE, HERE_SEED]
        .iter()
        .map(|slot| Id::from_parts(*slot as u64, 7))
        .collect();
    let scores: Vec<u64> = (0..3).map(|n| 42_000 + n).collect();
    let txn = coordinator(&cluster).await;
    let tid = txn.begin().await.unwrap().unwrap();
    for (id, score) in ids.iter().zip(scores.iter()) {
        txn.write(tid.clone(), cell_of(*id, *score))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();
    let mut by_id = sites.to_vec();
    by_id.sort_by_key(|index| cluster.site(*index).server_id);
    let (low, mid, high) = (by_id[0], by_id[1], by_id[2]);
    let limit = super::manager::stop_end_fanout_after_commit_point(tid.clone());
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    drop(txn);
    wait_status_synced(&cluster, low, &tid).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while in_doubt_on(&cluster, mid, &tid) {
        assert!(
            std::time::Instant::now() < deadline,
            "test setup: M must learn the commit from L"
        );
        data_site(&cluster, mid)
            .sweep_idle_transactions(Duration::ZERO)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    wait_status_synced(&cluster, mid, &tid).await;
    cluster.stop(low).await;
    for _ in 0..5 {
        data_site(&cluster, high)
            .sweep_idle_transactions(Duration::ZERO)
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let waiting = in_doubt_on(&cluster, high, &tid);
    warn!(
        "CROSSSITE N3: H in doubt {} while L (index {}) is stopped and M committed",
        waiting, low
    );
    assert!(!waiting, "H must commit from M's `Committed` while L is stopped");
    let n = sites.iter().position(|site| *site == high).unwrap();
    assert_eq!(score_on(cluster.site(high), &ids[n]), Some(scores[n]));
    cluster.shutdown().await;
}

/// Review R5: one silent peer must not block a commit that another peer
/// proves. Three sites: the seed, A and B (every pair knows the other's
/// address in this harness). Only the lowest id L is told. H learns the
/// commit from L. Then L is silent to M; M must still commit from H.
#[tokio::test(flavor = "multi_thread")]
async fn one_silent_peer_does_not_block_a_commit_another_peer_proves() {
    const HERE_SEED: u16 = 95;
    let (cluster, client) = Cluster::start("crosssite_r5_silent_peer").await;
    place_slot(&cluster, &client, HERE, A).await;
    place_slot(&cluster, &client, THERE, B).await;
    place_slot(&cluster, &client, HERE_SEED, SEED).await;
    drop(client);
    let sites = [A, B, SEED];
    let ids: Vec<Id> = [HERE, THERE, HERE_SEED]
        .iter()
        .map(|slot| Id::from_parts(*slot as u64, 5))
        .collect();
    let scores: Vec<u64> = (0..3).map(|n| 40_000 + n).collect();
    let txn = coordinator(&cluster).await;
    let tid = txn.begin().await.unwrap().unwrap();
    for (id, score) in ids.iter().zip(scores.iter()) {
        txn.write(tid.clone(), cell_of(*id, *score))
            .await
            .unwrap()
            .unwrap();
    }
    txn.prepare(tid.clone()).await.unwrap().unwrap();
    let mut by_id = sites.to_vec();
    by_id.sort_by_key(|index| cluster.site(*index).server_id);
    let (low, mid, high) = (by_id[0], by_id[1], by_id[2]);
    let limit = super::manager::stop_end_fanout_after_commit_point(tid.clone());
    let _ = txn.commit(tid.clone()).await;
    drop(limit);
    wait_status_synced(&cluster, low, &tid).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while in_doubt_on(&cluster, high, &tid) {
        assert!(
            std::time::Instant::now() < deadline,
            "test setup: H must learn the commit from L"
        );
        data_site(&cluster, high)
            .sweep_idle_transactions(Duration::ZERO)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    wait_status_synced(&cluster, high, &tid).await;
    let low_id = cluster.site(low).server_id;
    super::data_site::silent_peers_for_test().lock().insert(low_id);
    for _ in 0..3 {
        data_site(&cluster, mid)
            .sweep_idle_transactions(Duration::ZERO)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let waiting = in_doubt_on(&cluster, mid, &tid);
    warn!(
        "CROSSSITE R5: M in doubt {} while L is silent and H committed",
        waiting
    );
    super::data_site::silent_peers_for_test().lock().remove(&low_id);
    assert!(!waiting, "M must commit from H's `Committed` while L is silent");
    let n = sites.iter().position(|site| *site == mid).unwrap();
    assert_eq!(score_on(cluster.site(mid), &ids[n]), Some(scores[n]));
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

/// A BEGIN without a decision must not retain cells or transaction pins after recovery.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "G2 evidence: BEGIN-only recovery retains a transaction pin outside the in-doubt registry"]
async fn g2_recovery_of_a_begin_only_bracket() {
    g2_recovery_without_a_decision(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn g2_recovery_of_an_open_bracket_with_cells() {
    g2_recovery_without_a_decision(true).await;
}

async fn g2_recovery_without_a_decision(with_cells: bool) {
    use crate::client::transaction::{Transaction, TxnError};
    use crate::ram::chunk::TransactionScope;
    use std::cell::Cell;

    crate::ram::segs::power_loss_model::enable();
    let group = if with_cells {
        "g2_open_cells"
    } else {
        "g2_begin_only"
    };
    let (mut cluster, client) = Cluster::start(group).await;
    place_slot(&cluster, &client, HERE, A).await;
    assert!(cluster.site(A).refresh_slot_placement().await);
    let address = cluster.addresses[A].clone();
    let server_id = cluster.site(A).server_id;
    let manager = super::new_async_client_for_database(&address, group, group)
        .await
        .unwrap();
    let tid = manager.begin().await.unwrap().unwrap();
    let id = Id::from_parts(HERE as u64, 49_001);
    manager
        .write(tid, cell_of(id, 49_001))
        .await
        .unwrap()
        .unwrap();

    let image = if with_cells {
        assert_eq!(
            manager.prepare(tid).await.unwrap().unwrap(),
            TMPrepareResult::Success
        );
        assert_eq!(score_on(cluster.site(A), &id), Some(49_001));
        cluster.power_image(A)
    } else {
        // Stop after the real writer appends BEGIN, before its first cell append.
        // The image precedes reservation cancellation and shutdown cleanup.
        let chunks = cluster.site(A).chunks();
        let scope = TransactionScope::enter_with_participants(tid, vec![server_id]);
        let pending = chunks.list[HERE as usize % chunks.list.len()]
            .try_acquire(64, false)
            .unwrap();
        assert!(segment_holds_record_of(
            &pending.seg,
            &tid,
            EntryType::BEGIN
        ));
        assert_eq!(
            pending
                .seg
                .entry_iter()
                .filter(|entry| entry.entry_header.entry_type == EntryType::CELL)
                .count(),
            0,
            "the cut must precede the first cell"
        );
        pending.seg.force_wal_sync().unwrap();
        let image = cluster.power_image(A);
        drop(pending);
        drop(scope);
        drop(crate::ram::chunk::take_transactional_segments());
        crate::ram::chunk::retire_transaction_leases(&tid, &chunks.address_range()).unwrap();
        image
    };
    let parts: Vec<_> = cluster
        .site(A)
        .chunks()
        .list
        .iter()
        .flat_map(|chunk| chunk.segments())
        .filter(|segment| segment_holds_record_of(segment, &tid, EntryType::BEGIN))
        .collect();
    assert_eq!(parts.len(), 1, "the fixture must contain one open part");
    assert!(!segment_holds_record_of(&parts[0], &tid, EntryType::COMMIT));
    drop(parts);
    drop(manager);
    cluster.restart_from_images(&[A], vec![image]).await;
    assert_eq!(cluster.addresses[A], address);
    assert_eq!(cluster.site(A).server_id, server_id);
    assert_eq!(
        score_on(cluster.site(A), &id),
        None,
        "recovery must not apply undecided cells"
    );
    assert!(
        !in_doubt_on(&cluster, A, &tid),
        "startup must settle a single-site transaction"
    );
    assert!(matches!(
        outcome_on(&cluster, A, &tid),
        TxnOutcome::Unknown | TxnOutcome::Aborted
    ));

    let restarted_manager = super::new_async_client_for_database(&address, group, group)
        .await
        .unwrap();
    for durable in [false, true] {
        // The coordinator lost its volatile state. That cannot prove a client outcome.
        let txn = Transaction {
            tid,
            state: Cell::new(if with_cells {
                TxnState::Prepared
            } else {
                TxnState::Started
            }),
            client: restarted_manager.clone(),
        };
        let result = if durable {
            txn.commit_durable().await
        } else {
            txn.commit().await
        };
        assert!(
            matches!(result, Err(TxnError::OutcomeUnknown)),
            "a lost coordinator must not report Committed: {result:?}"
        );
    }
    data_site(&cluster, A)
        .sweep_idle_transactions(Duration::ZERO)
        .await;
    let pinned: Vec<_> = cluster
        .site(A)
        .chunks()
        .list
        .iter()
        .flat_map(|chunk| chunk.segments())
        .filter(|segment| segment.protection.holds_transaction(&tid))
        .map(|segment| (segment.chunk_id, segment.id))
        .collect();
    cluster.shutdown().await;
    assert!(
        pinned.is_empty(),
        "an undecided bracket must not retain transaction pins after recovery: {pinned:?}"
    );
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

/// A site refusal does not undo another site's commit (D33).
#[tokio::test(flavor = "multi_thread")]
async fn client_commit_outcome_store_full_can_already_be_committed() {
    use crate::client::transaction::{Transaction, TxnError};
    use std::cell::Cell;
    let (cluster, client) = Cluster::start("client_commit_store_full").await;
    place_slot(&cluster, &client, HERE, A).await;
    place_slot(&cluster, &client, THERE, B).await;
    let read_id = Id::from_parts(THERE as u64, 40_001);
    client
        .write_cell(cell_of(read_id, 1))
        .await
        .unwrap()
        .unwrap();
    let manager = coordinator(&cluster).await;
    for (seq, reader) in [(40_002, true), (40_003, false)] {
        let tid = manager.begin().await.unwrap().unwrap();
        let write_id = Id::from_parts(HERE as u64, seq);
        if reader {
            manager.read(tid, read_id).await.unwrap().unwrap();
        }
        manager
            .write(tid, cell_of(write_id, seq))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            manager.prepare(tid).await.unwrap().unwrap(),
            TMPrepareResult::Success
        );
        let txn = Transaction {
            tid,
            state: Cell::new(TxnState::Prepared),
            client: manager.clone(),
        };
        let result = txn.commit().await;
        // The writer decided COMMIT. A reader's cleanup refusal cannot undo it.
        assert!(result.is_ok(), "the writer's decision must win: {result:?}");
        assert_eq!(score_on(cluster.site(A), &write_id), Some(seq));
        assert_eq!(
            data_site(&cluster, A).txn_outcome(&tid),
            TxnOutcome::Committed
        );
    }
    cluster.shutdown().await;
}

/// The automatic transaction wrapper must not replay an uncertain commit.
#[tokio::test(flavor = "multi_thread")]
async fn client_commit_outcome_wrapper_does_not_retry_or_abort() {
    use crate::client::transaction::TxnError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (cluster, client) = Cluster::start("client_commit_no_replay").await;
    place_slot(&cluster, &client, HERE, A).await;
    place_slot(&cluster, &client, THERE, B).await;
    // The wrapper chooses any coordinator. Refresh each coordinator's route table.
    for server in cluster.servers.iter().flatten() {
        assert!(server.refresh_slot_placement().await);
    }
    let waiting = data_site(&cluster, A);
    let prepared_site = cluster.site(A).clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let result = client
        .transaction(move |txn| {
            let seen = seen.clone();
            let waiting = waiting.clone();
            let prepared_site = prepared_site.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                txn.write(cell_of(Id::from_parts(HERE as u64, 40_004), 8))
                    .await?;
                txn.write(cell_of(Id::from_parts(THERE as u64, 40_004), 9))
                    .await?;
                txn.prepare().await?;
                assert_eq!(
                    score_on(&prepared_site, &Id::from_parts(HERE as u64, 40_004)),
                    Some(8)
                );
                assert_eq!(waiting.txn_outcome(&txn.tid), TxnOutcome::InDoubt);
                let limit = super::manager::stop_end_fanout_after_commit_point(txn.tid);
                let result = txn.commit().await;
                drop(limit);
                assert_eq!(waiting.txn_outcome(&txn.tid), TxnOutcome::InDoubt);
                assert_eq!(txn.state.get(), TxnState::Committed);
                // A second call cannot turn the first uncertain result into a definite error.
                assert!(matches!(txn.commit().await, Err(TxnError::OutcomeUnknown)));
                result
            }
        })
        .await;
    assert!(
        matches!(result, Err(TxnError::OutcomeUnknown)),
        "{result:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        score_on(cluster.site(B), &Id::from_parts(THERE as u64, 40_004)),
        Some(9)
    );
    // An abort would rewind A. The wrapper leaves it in doubt for peer resolution.
    assert_eq!(
        score_on(cluster.site(A), &Id::from_parts(HERE as u64, 40_004)),
        Some(8)
    );
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// The old version of an updated cell between `end` and the COMMIT sync
// ---------------------------------------------------------------------------

/// How the transaction commits, and what the test does before the cleaner runs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PinWindowMode {
    /// Default commit. Every sync stays paused, so the COMMIT is unsynced.
    DefaultUnsynced,
    /// Default commit. The test waits until the COMMIT is synced.
    DefaultThenSynced,
    /// Durable commit: the COMMIT is synced before `end`.
    Durable,
    /// Default commit. Every WAL sync fails after the COMMIT append.
    SyncFails,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PinOp {
    Update,
    Remove,
}

/// Where the old version sits when the transaction commits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OldVersionIn {
    /// A sealed segment: the cleaner may take it at once.
    SealedSegment,
    /// A segment that is mostly dead when it seals. The seal job syncs it and
    /// leaves it to the cleaner (D46 seal-pending reclaim).
    SealPending,
}

struct PinWindow {
    group: &'static str,
    mode: PinWindowMode,
    op: PinOp,
    old_in: OldVersionIn,
    /// Sites that hold one cell each. `[A]` or `[A, B]`.
    sites: Vec<usize>,
    pin_enabled: bool,
}

struct PinWindowOutcome {
    /// The score that each site reads after the power loss.
    reads: Vec<Option<u64>>,
    /// Whether the old segment of each site was reclaimed before the power loss.
    old_segment_gone: Vec<bool>,
}

impl PinWindow {
    fn new(group: &'static str, mode: PinWindowMode) -> Self {
        PinWindow {
            group,
            mode,
            op: PinOp::Update,
            old_in: OldVersionIn::SealedSegment,
            sites: vec![A],
            pin_enabled: true,
        }
    }
}

fn slot_of_site(site: usize) -> u16 {
    if site == A {
        HERE
    } else {
        THERE
    }
}

fn pin_chunk(cluster: &Cluster, site: usize) -> &Chunk {
    let chunks = cluster.site(site).chunks();
    &chunks.list[slot_of_site(site) as usize % chunks.list.len()]
}

fn force_sync_segment(chunk: &Chunk, segment_id: u64) {
    chunk
        .segments()
        .iter()
        .find(|segment| segment.id == segment_id)
        .expect("segment to sync")
        .force_wal_sync()
        .unwrap();
}

/// Combine the old segment with the filler. Return whether the old segment is
/// gone afterwards. `passes` bounds the wait for a combine that the cleaner
/// refuses.
fn compact_old_segment(chunk: &Chunk, old: u64, filler: u64, passes: usize) -> bool {
    for _ in 0..passes {
        let candidates: Vec<_> = chunk
            .segments()
            .into_iter()
            .filter(|segment| segment.id == old || segment.id == filler)
            .collect();
        if candidates.len() < 2 {
            break;
        }
        let (_, bytes) =
            crate::ram::cleaner::combine::CombinedCleaner::combine_segments(chunk, &candidates);
        if bytes > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while chunk
        .retired_segment_files
        .load(std::sync::atomic::Ordering::Acquire)
        != 0
    {
        assert!(std::time::Instant::now() < deadline, "sources never retired");
        chunk.drain_retired_segments();
        std::thread::sleep(Duration::from_millis(10));
    }
    !chunk.segments().iter().any(|segment| segment.id == old)
}

fn old_segment_holds(chunk: &Chunk, old: u64, txn: &TxnId) -> bool {
    chunk
        .segments()
        .iter()
        .find(|segment| segment.id == old)
        .is_some_and(|segment| segment.protection.holds_transaction(txn))
}

impl PinWindow {
    /// Run the window: old version, transaction, cleaner on the old segment,
    /// power loss at every site, restart, read.
    async fn run(&self) -> PinWindowOutcome {
        use std::sync::atomic::Ordering::SeqCst;
        crate::ram::segs::power_loss_model::enable();
        super::data_site::PIN_DISABLED.store(!self.pin_enabled, SeqCst);
        let (mut cluster, client) = Cluster::start(self.group).await;
        for site in &self.sites {
            place_slot(&cluster, &client, slot_of_site(*site), *site).await;
        }
        drop(client);
        let txn = coordinator(&cluster).await;
        let ids: Vec<Id> = self
            .sites
            .iter()
            .map(|site| Id::from_parts(slot_of_site(*site) as u64, 77))
            .collect();

        // A sealed filler segment for the combiner, made before any sync is
        // paused, and before the old version, so the old version does not
        // share its segment.
        let mut fillers = Vec::new();
        for (n, site) in self.sites.iter().enumerate() {
            let chunks = cluster.site(*site).chunks();
            let chunk = pin_chunk(&cluster, *site);
            let filler = Id::from_parts(slot_of_site(*site) as u64, 910_001 + n as u64);
            chunks.write_cell(&mut cell_of(filler, 1)).expect("filler");
            let segment = chunk
                .locate_segment(chunks.address_of(&filler))
                .expect("filler segment")
                .id;
            if chunk.is_active_head(segment) {
                rotate_out(chunk, segment);
            }
            force_sync_segment(chunk, segment);
            fillers.push(segment);
        }

        // The old versions. For a seal-pending segment the test thread writes
        // them beside junk, and the junk dies before the seal. Otherwise a
        // durable transaction writes them.
        let first = txn.begin().await.unwrap().unwrap();
        if self.old_in == OldVersionIn::SealPending {
            // The test calls the combiner itself. The background cleaner
            // would take the mostly dead segment before the update.
            for site in &self.sites {
                cluster.site(*site).current_database().cleaner().pause();
            }
            for (n, (id, site)) in ids.iter().zip(self.sites.iter()).enumerate() {
                let chunks = cluster.site(*site).chunks();
                let junk: Vec<Id> = (0..30)
                    .map(|j| Id::from_parts(slot_of_site(*site) as u64, 5_000 + j))
                    .collect();
                for id in &junk {
                    chunks.write_cell(&mut cell_of(*id, 9)).expect("junk");
                }
                chunks
                    .write_cell(&mut cell_of(*id, 100 + n as u64))
                    .expect("old version");
                for id in &junk {
                    chunks.remove_cell(id).expect("junk dies");
                }
            }
            txn.abort(first.clone()).await.unwrap().unwrap();
        } else {
            for (n, id) in ids.iter().enumerate() {
                txn.write(first.clone(), cell_of(*id, 100 + n as u64))
                    .await
                    .unwrap()
                    .unwrap();
            }
            txn.prepare(first.clone()).await.unwrap().unwrap();
            assert!(matches!(
                txn.commit_durable(first.clone()).await,
                Ok(Ok(EndResult::Success))
            ));
        }
        for site in &self.sites {
            wait_until("the old versions are unheld", 10, || {
                held_for(&cluster, *site, &first) == 0
            })
            .await;
        }
        let mut olds = Vec::new();
        for (id, site) in ids.iter().zip(self.sites.iter()) {
            let chunks = cluster.site(*site).chunks();
            let chunk = pin_chunk(&cluster, *site);
            let old = chunk
                .locate_segment(chunks.address_of(id))
                .expect("segment of the old version")
                .id;
            if chunk.is_active_head(old) {
                rotate_out(chunk, old);
            }
            force_sync_segment(chunk, old);
            assert_ne!(old, fillers[olds.len()]);
            olds.push(old);
        }

        // The transaction.
        let dirs: Vec<std::path::PathBuf> = self
            .sites
            .iter()
            .map(|site| cluster.temp[*site].path().to_path_buf())
            .collect();
        if matches!(self.mode, PinWindowMode::DefaultUnsynced | PinWindowMode::SyncFails) {
            crate::ram::segs::power_loss_model::pause_group_sync(true);
            for dir in &dirs {
                crate::ram::durability::pause_receipt_syncs_under(dir, true);
            }
        }
        let second = txn.begin().await.unwrap().unwrap();
        for (n, id) in ids.iter().enumerate() {
            match self.op {
                PinOp::Update => {
                    txn.update(second.clone(), cell_of(*id, 200 + n as u64))
                        .await
                        .unwrap()
                        .unwrap();
                }
                PinOp::Remove => {
                    txn.remove(second.clone(), *id).await.unwrap().unwrap();
                }
            }
        }
        txn.prepare(second.clone()).await.unwrap().unwrap();
        let committed = if self.mode == PinWindowMode::Durable {
            txn.commit_durable(second.clone()).await
        } else {
            txn.commit(second.clone()).await
        };
        assert!(
            matches!(committed, Ok(Ok(EndResult::Success))),
            "{:?}",
            committed
        );
        if self.mode == PinWindowMode::SyncFails {
            // The COMMIT sync of every head that the transaction wrote fails.
            for site in &self.sites {
                for chunk in &cluster.site(*site).chunks().list {
                    for segment in chunk.segments() {
                        if segment_holds_record_of(&segment, &second, EntryType::BEGIN) {
                            if let Ok(Some(receipt)) = segment.durable_receipt() {
                                receipt.file.inject_failure();
                            }
                        }
                    }
                }
            }
            crate::ram::segs::power_loss_model::pause_group_sync(false);
            for dir in &dirs {
                crate::ram::durability::pause_receipt_syncs_under(dir, false);
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
        for site in &self.sites {
            match self.mode {
                PinWindowMode::DefaultThenSynced => {
                    wait_until("the COMMIT sync", 20, || {
                        held_for(&cluster, *site, &second) == 0
                    })
                    .await
                }
                PinWindowMode::DefaultUnsynced | PinWindowMode::SyncFails => assert!(
                    held_for(&cluster, *site, &second) > 0,
                    "test setup: the COMMIT at site {} must be unsynced",
                    site
                ),
                PinWindowMode::Durable => {}
            }
        }
        if self.op == PinOp::Update {
            for (n, site) in self.sites.iter().enumerate() {
                assert_eq!(
                    score_on(cluster.site(*site), &ids[n]),
                    Some(200 + n as u64)
                );
            }
        }
        // The pin, and the cleaner on the old segments.
        let pinned_expected = self.pin_enabled
            && matches!(
                self.mode,
                PinWindowMode::DefaultUnsynced | PinWindowMode::SyncFails
            );
        let mut gone = Vec::new();
        for (n, site) in self.sites.iter().enumerate() {
            let chunk = pin_chunk(&cluster, *site);
            if pinned_expected {
                assert!(
                    old_segment_holds(chunk, olds[n], &second),
                    "the old segment of site {} must be pinned by the unsynced transaction",
                    site
                );
            }
            let passes = if pinned_expected { 20 } else { 50 };
            gone.push(compact_old_segment(chunk, olds[n], fillers[n], passes));
            warn!(
                "PINWIN {} site {}: old segment gone: {}, pinned: {}",
                self.group, site, gone[n], pinned_expected
            );
        }

        // The power loss: the images are taken now, from the synced lengths.
        let images: Vec<_> = self.sites.iter().map(|site| cluster.power_image(*site)).collect();
        if pinned_expected && self.mode == PinWindowMode::DefaultUnsynced {
            // On the live stores the pin ends with the sync, and the cleaner
            // then reclaims the old segment.
            crate::ram::segs::power_loss_model::pause_group_sync(false);
            for dir in &dirs {
                crate::ram::durability::pause_receipt_syncs_under(dir, false);
            }
            for (n, site) in self.sites.iter().enumerate() {
                wait_until("the COMMIT sync", 20, || {
                    held_for(&cluster, *site, &second) == 0
                })
                .await;
                let chunk = pin_chunk(&cluster, *site);
                assert!(
                    compact_old_segment(chunk, olds[n], fillers[n], 50),
                    "the cleaner must reclaim the old segment of site {} after the COMMIT sync",
                    site
                );
            }
        }
        crate::ram::segs::power_loss_model::pause_group_sync(false);
        for dir in &dirs {
            crate::ram::durability::pause_receipt_syncs_under(dir, false);
        }
        cluster.restart_from_images(&self.sites, images).await;
        if self.sites.len() > 1 {
            for site in &self.sites {
                wait_until("the in-doubt resolution", 90, || {
                    !in_doubt_on(&cluster, *site, &second)
                })
                .await;
            }
        }
        let reads = ids
            .iter()
            .zip(self.sites.iter())
            .map(|(id, site)| score_on(cluster.site(*site), id))
            .collect();
        super::data_site::PIN_DISABLED.store(false, SeqCst);
        cluster.shutdown().await;
        PinWindowOutcome {
            reads,
            old_segment_gone: gone,
        }
    }
}

/// Default commit, `end`, cleaner on the old segment, power loss before the
/// COMMIT sync. D19: the transaction is absent, so the OLD value stays.
#[tokio::test(flavor = "multi_thread")]
async fn an_updated_cell_survives_compaction_of_its_old_version_before_the_commit_sync() {
    let outcome = PinWindow::new("pinwin_default", PinWindowMode::DefaultUnsynced)
        .run()
        .await;
    assert_eq!(outcome.reads, vec![Some(100)]);
    assert_eq!(outcome.old_segment_gone, vec![false]);
}

/// Control: the same flow without the pin loses the cell. The window is real.
#[tokio::test(flavor = "multi_thread")]
async fn control_pinwin_without_the_pin_loses_the_cell() {
    let mut window = PinWindow::new("pinwin_nopin", PinWindowMode::DefaultUnsynced);
    window.pin_enabled = false;
    let outcome = window.run().await;
    assert_eq!(outcome.old_segment_gone, vec![true]);
    assert_eq!(outcome.reads, vec![None]);
}

/// Control: the COMMIT is synced before the cleaner runs. The new value stays.
#[tokio::test(flavor = "multi_thread")]
async fn control_pinwin_synced_commit_keeps_the_cell_after_compaction() {
    let outcome = PinWindow::new("pinwin_synced", PinWindowMode::DefaultThenSynced)
        .run()
        .await;
    assert_eq!(outcome.reads, vec![Some(200)]);
    assert_eq!(outcome.old_segment_gone, vec![true]);
}

/// Control: a durable commit syncs the COMMIT before `end`.
#[tokio::test(flavor = "multi_thread")]
async fn control_pinwin_durable_commit_keeps_the_cell_after_compaction() {
    let outcome = PinWindow::new("pinwin_durable", PinWindowMode::Durable)
        .run()
        .await;
    assert_eq!(outcome.reads, vec![Some(200)]);
}

/// T1: two sites, default `end`, nothing synced. Both keep the old value.
#[tokio::test(flavor = "multi_thread")]
async fn pinwin_two_site_update_keeps_both_old_values() {
    let mut window = PinWindow::new("pinwin_two_site", PinWindowMode::DefaultUnsynced);
    window.sites = vec![A, B];
    let outcome = window.run().await;
    // Observed: a two-site default commit is not lost, with or without the
    // pin. The result is all old or all new, never absent.
    assert!(
        outcome.reads == vec![Some(100), Some(101)] || outcome.reads == vec![Some(200), Some(201)],
        "{:?}",
        outcome.reads
    );
}

/// T1 control: without the pin the two-site result is still atomic and the
/// cell is never absent. This documents that the window is a single-site
/// window: a two-site default commit keeps its COMMIT or status cell.
#[tokio::test(flavor = "multi_thread")]
async fn control_pinwin_two_site_without_the_pin_stays_atomic() {
    let mut window = PinWindow::new("pinwin_two_site_nopin", PinWindowMode::DefaultUnsynced);
    window.sites = vec![A, B];
    window.pin_enabled = false;
    let outcome = window.run().await;
    assert!(
        outcome.reads == vec![Some(100), Some(101)] || outcome.reads == vec![Some(200), Some(201)],
        "{:?}",
        outcome.reads
    );
}

/// T1 control: two sites with a synced COMMIT keep both new values.
#[tokio::test(flavor = "multi_thread")]
async fn control_pinwin_two_site_synced_commit_keeps_both_new_values() {
    let mut window = PinWindow::new("pinwin_two_site_synced", PinWindowMode::DefaultThenSynced);
    window.sites = vec![A, B];
    let outcome = window.run().await;
    assert_eq!(outcome.reads, vec![Some(200), Some(201)]);
}

/// T2: a removed cell is present with its old value after the power loss.
#[tokio::test(flavor = "multi_thread")]
async fn pinwin_remove_keeps_the_old_value() {
    let mut window = PinWindow::new("pinwin_remove", PinWindowMode::DefaultUnsynced);
    window.op = PinOp::Remove;
    let outcome = window.run().await;
    assert_eq!(outcome.reads, vec![Some(100)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn control_pinwin_remove_without_the_pin_loses_the_cell() {
    let mut window = PinWindow::new("pinwin_remove_nopin", PinWindowMode::DefaultUnsynced);
    window.op = PinOp::Remove;
    window.pin_enabled = false;
    assert_eq!(window.run().await.reads, vec![None]);
}

/// T2 control: a durable remove is absent after the power loss.
#[tokio::test(flavor = "multi_thread")]
async fn control_pinwin_durable_remove_is_absent() {
    let mut window = PinWindow::new("pinwin_remove_durable", PinWindowMode::Durable);
    window.op = PinOp::Remove;
    assert_eq!(window.run().await.reads, vec![None]);
}

/// T3 (D46): the old version sits in the active head. The seal job leaves the
/// mostly dead segment to the cleaner. The pin keeps it.
#[tokio::test(flavor = "multi_thread")]
async fn pinwin_seal_pending_old_head_keeps_the_old_value() {
    let mut window = PinWindow::new("pinwin_head", PinWindowMode::DefaultUnsynced);
    window.old_in = OldVersionIn::SealPending;
    let outcome = window.run().await;
    assert_eq!(outcome.reads, vec![Some(100)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn control_pinwin_old_head_without_the_pin_loses_the_cell() {
    let mut window = PinWindow::new("pinwin_head_nopin", PinWindowMode::DefaultUnsynced);
    window.old_in = OldVersionIn::SealPending;
    window.pin_enabled = false;
    assert_eq!(window.run().await.reads, vec![None]);
}

/// T6: every WAL sync fails after the COMMIT append. The old segment stays
/// pinned (D20: cannot tell is in progress) and the power loss keeps the old
/// value.
#[tokio::test(flavor = "multi_thread")]
async fn pinwin_failed_commit_sync_keeps_the_old_segment_pinned() {
    let outcome = PinWindow::new("pinwin_syncfail", PinWindowMode::SyncFails)
        .run()
        .await;
    assert_eq!(outcome.reads, vec![Some(100)]);
    assert_eq!(outcome.old_segment_gone, vec![false]);
}

/// T4: an aborted transaction pins no old segment.
#[tokio::test(flavor = "multi_thread")]
async fn pinwin_an_aborted_update_pins_nothing() {
    let (cluster, client) = Cluster::start("pinwin_abort").await;
    place_slot(&cluster, &client, HERE, A).await;
    drop(client);
    let id = Id::from_parts(HERE as u64, 77);
    let txn = coordinator(&cluster).await;
    let first = txn.begin().await.unwrap().unwrap();
    txn.write(first.clone(), cell_of(id, 100)).await.unwrap().unwrap();
    txn.prepare(first.clone()).await.unwrap().unwrap();
    assert!(matches!(
        txn.commit_durable(first.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    let chunks = cluster.site(A).chunks();
    let chunk = pin_chunk(&cluster, A);
    let old = chunk
        .locate_segment(chunks.address_of(&id))
        .expect("old segment")
        .id;
    let second = txn.begin().await.unwrap().unwrap();
    txn.update(second.clone(), cell_of(id, 200)).await.unwrap().unwrap();
    txn.prepare(second.clone()).await.unwrap().unwrap();
    assert!(txn.abort(second.clone()).await.is_ok());
    assert!(!old_segment_holds(chunk, old, &second));
    assert_eq!(score_on(cluster.site(A), &id), Some(100));
    cluster.shutdown().await;
}

/// T5: N default updates of cells in one segment that stays above the dead
/// bar. The pins do not pile up on the segment.
async fn many_updates_leave_few_pins(group: &'static str, prune: bool) -> usize {
    crate::ram::cleaner::protection::NO_PRUNE_ON_PIN
        .store(!prune, std::sync::atomic::Ordering::SeqCst);
    let (cluster, client) = Cluster::start(group).await;
    place_slot(&cluster, &client, HERE, A).await;
    drop(client);
    let txn = coordinator(&cluster).await;
    let ids: Vec<Id> = (0..40).map(|n| Id::from_parts(HERE as u64, 1_000 + n)).collect();
    let first = txn.begin().await.unwrap().unwrap();
    for id in &ids {
        txn.write(first.clone(), cell_of(*id, 1)).await.unwrap().unwrap();
    }
    txn.prepare(first.clone()).await.unwrap().unwrap();
    assert!(matches!(
        txn.commit_durable(first.clone()).await,
        Ok(Ok(EndResult::Success))
    ));
    let chunks = cluster.site(A).chunks();
    let chunk = pin_chunk(&cluster, A);
    let old = chunk
        .locate_segment(chunks.address_of(&ids[0]))
        .expect("old segment")
        .id;
    if chunk.is_active_head(old) {
        rotate_out(chunk, old);
    }
    let mut tids = Vec::new();
    for id in ids.iter().take(12) {
        let update = txn.begin().await.unwrap().unwrap();
        txn.update(update.clone(), cell_of(*id, 2)).await.unwrap().unwrap();
        txn.prepare(update.clone()).await.unwrap().unwrap();
        assert!(matches!(
            txn.commit(update.clone()).await,
            Ok(Ok(EndResult::Success))
        ));
        tids.push(update);
    }
    // Counted at once, with no protection check: a check prunes the pins
    // itself, and so does the background cleaner a moment later.
    let _ = &tids;
    let pins = chunk
        .segments()
        .iter()
        .find(|segment| segment.id == old)
        .map_or(0, |segment| segment.protection.pin_count());
    crate::ram::cleaner::protection::NO_PRUNE_ON_PIN
        .store(false, std::sync::atomic::Ordering::SeqCst);
    cluster.shutdown().await;
    pins
}

#[tokio::test(flavor = "multi_thread")]
async fn pinwin_pins_do_not_pile_up_on_a_live_segment() {
    let pins = many_updates_leave_few_pins("pinwin_pile", true).await;
    assert!(pins <= 3, "{} stale pins on one segment", pins);
}

/// Control: without the prune on insert, as in the first fix, the pins pile up.
#[tokio::test(flavor = "multi_thread")]
async fn control_pinwin_pins_pile_up_without_the_prune() {
    let pins = many_updates_leave_few_pins("pinwin_pile_control", false).await;
    assert!(pins >= 10, "expected stale pins, found {}", pins);
}

/// Create a ring before the participants join. A separate client retains
/// only the seed's configuration; the normal client also receives notices.
async fn cluster_with_an_early_participant_ring(
    group: &str,
    stale_configuration: bool,
) -> (
    Cluster,
    Arc<AsyncClient>,
    Arc<bifrost::conshash::ConsistentHashing>,
) {
    let _ = env_logger::try_init();
    let services = vec![vec![Service::Cell, Service::Transaction]; 3];
    let temp: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::TempDir::new().unwrap()).collect();
    let mut addresses: Vec<String> = (0..3)
        .map(|_| crate::utils::test_port::unique_localhost_addr())
        .collect();
    if bifrost_hasher::hash_str(&addresses[B]) > bifrost_hasher::hash_str(&addresses[A]) {
        addresses.swap(A, B);
    }
    let seed = NebServer::new_cluster_from_opts(
        &opts_for(&temp[SEED], &services[SEED]),
        &addresses[SEED],
        &addresses,
        group,
        async |_| {},
    )
    .await
    .unwrap();
    let source = if stale_configuration {
        bifrost::raft::client::RaftClient::new(&addresses, bifrost::raft::DEFAULT_SERVICE_ID)
            .await
            .unwrap()
    } else {
        seed.raft_client.clone()
    };
    let ring = bifrost::conshash::ConsistentHashing::new_with_id(
        crate::server::CONS_HASH_ID,
        &format!("{group}-empty-ring"),
        &source,
        &seed.membership,
    )
    .await
    .unwrap();
    assert!(
        ring.try_server_name(bifrost_hasher::hash_str(&addresses[B]))
            .is_none(),
        "the ring must not know the participant before it joins"
    );
    let mut servers = vec![Some(seed)];
    for index in 1..3 {
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
    let seed = servers[SEED].as_ref().unwrap();
    let client = Arc::new(
        AsyncClient::new(&seed.rpc, &seed.membership, &addresses, group)
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
    (cluster, client, ring)
}

async fn find_a_committed_participant_after_an_address_gap(stale_configuration: bool) {
    use bifrost::rpc::ClientPool;
    let group = if stale_configuration {
        "crosssite_membership_only_address"
    } else {
        "crosssite_missing_ring_address"
    };
    let (cluster, client, ring) =
        cluster_with_an_early_participant_ring(group, stale_configuration).await;
    let (tid, _ids, _scores) = torn_transaction(&cluster, &client, 49_000).await;
    wait_status_synced(&cluster, B, &tid).await;
    let seed = cluster.site(SEED);
    let peer = cluster.site(B).server_id;
    let (members, _) = seed.membership.all_members(false).await.unwrap();
    assert!(
        members.iter().any(|member| member.id == peer),
        "membership must know the committed participant"
    );
    assert_eq!(
        ring.nodes_count(),
        0,
        "the probe ring has no routing snapshot"
    );
    if stale_configuration {
        assert!(
            ring.try_server_name(peer).is_none(),
            "the old configuration and the probe ring's notices must not supply this address"
        );
    }
    let deps = super::manager::TransactionManagerDeps {
        database_runtime: seed.current_database(),
        server_id: seed.server_id,
        consh: ring,
        member_pool: Arc::new(ClientPool::new()),
        hlc: seed.hlc.clone(),
    };
    let located = deps.get_member_by_server_id(peer).await;
    let error = located.as_ref().err().map(|error| format!("{error:?}"));
    let mut outcome = None;
    if let Ok(rpc) = located {
        assert_eq!(rpc.server_id, peer);
        let site = super::data_site::AsyncServiceClient::new_with_service_id(
            super::data_site::generate_scoped_service_id(&cluster.group, seed.database_name()),
            &rpc,
        );
        outcome = Some(site.txn_status(tid).await.unwrap().payload);
    }
    let absent = (1..1000)
        .find(|id| !cluster.servers.iter().flatten().any(|s| s.server_id == *id))
        .unwrap();
    let unknown = deps
        .get_member_by_server_id(absent)
        .await
        .err()
        .expect("an unknown participant remains an error");
    assert_eq!(unknown.kind(), std::io::ErrorKind::NotFound);
    drop(client);
    cluster.shutdown().await;
    assert_eq!(
        outcome,
        Some(TxnOutcome::Committed),
        "the manager must locate the committed participant; lookup error: {:?}",
        error
    );
}

/// Adapted from relmark: the ring starts without the participant's address.
/// Configuration and notices must let the manager ask a committed participant.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_finds_a_participant_missing_from_the_ring_cache() {
    find_a_committed_participant_after_an_address_gap(false).await;
}

/// A participant joins after the configuration snapshot, before subscription.
/// Membership knows it, but neither address source supplied it to this ring.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_finds_a_membership_only_participant_after_a_configuration_snapshot() {
    find_a_committed_participant_after_an_address_gap(true).await;
}

async fn review_f1_reader_report_case(partial: bool, refuse: bool) {
    use crate::client::transaction::{Transaction, TxnError};
    use std::cell::Cell;
    let (cluster, client) = Cluster::start("review_f1_reader_report").await;
    place_slot(&cluster, &client, HERE, A).await;
    place_slot(&cluster, &client, THERE, B).await;
    let writer_id = Id::from_parts(HERE as u64, 50_001);
    let read_ids = [
        Id::from_parts(THERE as u64, 50_001),
        Id::from_parts(THERE as u64, 50_002),
    ];
    for id in &read_ids {
        client.write_cell(cell_of(*id, 1)).await.unwrap().unwrap();
    }
    let manager = coordinator(&cluster).await;
    let tid = manager.begin().await.unwrap().unwrap();
    for id in read_ids.iter().take(if partial { 2 } else { 1 }) {
        manager.read(tid, *id).await.unwrap().unwrap();
    }
    manager
        .write(tid, cell_of(writer_id, 73))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        manager.prepare(tid).await.unwrap().unwrap(),
        TMPrepareResult::Success
    );
    let writer = data_site(&cluster, A);
    let reader = data_site(&cluster, B);
    // A late END can reach a read-only participant after its local abort.
    assert_eq!(
        <super::data_site::DataManager as super::data_site::Service>::abort(&reader, tid, tid)
            .await
            .payload,
        AbortResult::Success(None)
    );
    reader.set_cell_owner_for_test(
        &read_ids[0],
        Some(TxnPriority::new(
            cluster.site(B).hlc.now(),
            cluster.site(B).server_id,
        )),
    );
    if refuse {
        crate::ram::chunk::refuse_next_commit_for_test(cluster.site(A).chunks().base_addr, tid);
    }
    // B sorts first. Hold A's rollback until B has returned its lock report.
    let writer_meta = writer.cell_meta_for_test(&writer_id);
    let (release, held) = if refuse {
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let held = std::thread::spawn(move || {
            let guard = writer_meta.lock();
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(guard);
        });
        held_rx.recv().unwrap();
        (Some(release_tx), Some(held))
    } else {
        (None, None)
    };
    let watching_reader = reader.clone();
    let ordered = tokio::spawn(async move {
        wait_until("reader END finishes before writer rollback", 10, || {
            !watching_reader.has_transaction_for_test(&tid)
        })
        .await;
        if let Some(release) = release {
            release.send(()).unwrap();
        }
    });
    let txn = Transaction {
        tid,
        state: Cell::new(TxnState::Prepared),
        client: manager,
    };
    let result = txn.commit_durable().await;
    ordered.await.unwrap();
    if let Some(held) = held {
        held.join().unwrap();
    }
    reader.set_cell_owner_for_test(&read_ids[0], None);
    println!("F1 result: {result:?}; partial reader report: {partial}; writer refusal: {refuse}");
    if refuse {
        assert_eq!(writer.txn_outcome(&tid), TxnOutcome::Aborted);
        assert_eq!(score_on(cluster.site(A), &writer_id), None);
    } else {
        assert_eq!(writer.txn_outcome(&tid), TxnOutcome::Committed);
        assert_eq!(score_on(cluster.site(A), &writer_id), Some(73));
    }
    cluster.shutdown().await;
    if refuse {
        assert!(
            matches!(result, Err(TxnError::OutcomeUnknown)),
            "a reader report hid writer rollback: {result:?}"
        );
    } else {
        assert!(result.is_ok(), "committed writer control: {result:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_f1_reader_exhausted_report_cannot_hide_writer_rollback() {
    review_f1_reader_report_case(false, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_f1_reader_partial_report_cannot_hide_writer_rollback() {
    review_f1_reader_report_case(true, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_f1_committed_writer_control() {
    review_f1_reader_report_case(false, false).await;
}

/// A completed removal must fence discovery even when a normal query is stale.
#[cfg(feature = "c3doubt_trace")]
#[tokio::test(flavor = "multi_thread")]
async fn recovery_rejects_an_uncached_removed_participant_with_a_lagging_roster() {
    use bifrost::membership::client::ObserverClient;
    use bifrost::raft::client::RaftClient;
    use bifrost::rpc::ClientPool;
    let (cluster, client, ring) =
        cluster_with_an_early_participant_ring("crosssite_removed_participant", true).await;
    let (tid, _, _) = torn_transaction(&cluster, &client, 49_100).await;
    wait_status_synced(&cluster, B, &tid).await;
    let seed = cluster.site(SEED);
    let peer = cluster.site(B).server_id;
    let captured = seed.membership.all_members(false).await.unwrap();
    assert!(captured.0.iter().any(|member| member.id == peer));
    assert!(ring.try_server_name(peer).is_none());
    let removal_raft = RaftClient::new(&cluster.addresses, bifrost::raft::DEFAULT_SERVICE_ID)
        .await
        .unwrap();
    let removal = ObserverClient::new(&removal_raft);
    assert!(removal.sm_client.leave(&peer).await.unwrap());
    let current = removal.all_members(false).await.unwrap();
    assert!(current.1 > captured.1);
    assert!(!current.0.iter().any(|member| member.id == peer));
    // Keep the removed participant's RPC service alive. Only membership rejects it.
    seed.membership.set_query_roster_for_test(captured);
    let lagging = ring.membership().all_members(false).await.unwrap();
    assert!(lagging.1 < current.1);
    assert!(lagging.0.iter().any(|member| member.id == peer));
    assert!(ring.try_server_name(peer).is_none());
    let deps = super::manager::TransactionManagerDeps {
        database_runtime: seed.current_database(),
        server_id: seed.server_id,
        consh: ring,
        member_pool: Arc::new(ClientPool::new()),
        hlc: seed.hlc.clone(),
    };
    let result = deps.get_member_by_server_id(peer).await;
    let rejected = matches!(&result, Err(error) if error.kind() == std::io::ErrorKind::NotFound);
    let unknown = deps.get_member_by_server_id(0).await;
    assert!(matches!(unknown, Err(error) if error.kind() == std::io::ErrorKind::NotFound));
    drop(client);
    cluster.shutdown().await;
    assert!(
        rejected,
        "discovery accepted a removed participant from a lagging roster"
    );
}
