//! A graceful restart must give back exactly the cells that were live when it
//! began: deletes stay deleted, re-created ids keep their last value.
//!
//! These drive a real `NebServer` with WAL, backup and raft directories, shut
//! it down gracefully and start a fresh one on the same directories, then
//! compare every id against a model of what was acknowledged.

use crate::client;
use crate::ram::cell::OwnedCell;
use crate::ram::schema::{Field, Schema, SchemaVid};
use crate::ram::types::*;
use crate::server::*;
use dovahkiin::types::custom_types::id::Id;
use std::collections::HashMap;
use std::path::PathBuf;

const SCHEMA: u32 = 9301;

/// Scratch space on the big disk, never /tmp: a restart test leaves WAL and
/// backup files of several segments each.
fn scratch_dir() -> tempfile::TempDir {
    let base = PathBuf::from("/mnt/ext/morpheus-fixes/restart-bugs");
    std::fs::create_dir_all(&base).expect("scratch base");
    tempfile::Builder::new()
        .prefix("neb-restart-")
        .tempdir_in(base)
        .expect("scratch dir")
}

/// Chunk size in MiB. Several chunks (the default) make ids spread over
/// chunks the way a real store's do; `NEB_RD_CHUNK_MB=128` gives one.
/// The default has 4 segments so transactions can own a separate write head.
fn chunk_mb() -> usize {
    std::env::var("NEB_RD_CHUNK_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32)
}

fn options(dir: &std::path::Path, chunk_mb: usize) -> ServerOptions {
    ServerOptions {
        chunk_size: chunk_mb * 1024 * 1024,
        db_size: 128 * 1024 * 1024,
        tiered_config: None,
        backup_storage: Some(dir.join("backup").to_string_lossy().to_string()),
        wal_storage: Some(dir.join("wal").to_string_lossy().to_string()),
        raft_storage: Some(dir.join("raft").to_string_lossy().to_string()),
        index_enabled: false,
        services: vec![Service::Cell, Service::Transaction],
        enable_recovery: true,
        disable_storage_locks: true,
    }
}

fn schema() -> Schema {
    Schema::new_with_id(
        SCHEMA,
        "restart_delete_schema",
        None,
        Field::new_schema(vec![
            Field::new_unindexed("DATA", Type::U64),
            Field::new_unindexed("PAD", Type::String),
        ]),
        false,
        false,
    )
}

#[derive(Clone, Copy, Debug)]
enum IdClass {
    Allocated,
    Hashed,
}

impl IdClass {
    fn id(self, index: u64) -> Id {
        match self {
            IdClass::Allocated => Id::allocated((index % 4) as u16, 11, index + 1),
            IdClass::Hashed => Id::from_obj(&format!("restart-delete-{index}")),
        }
    }
}

fn cell(id: &Id, data: u64, pad: usize) -> OwnedCell {
    let mut value = OwnedValue::Map(OwnedMap::new());
    value["DATA"] = OwnedValue::U64(data);
    value["PAD"] = OwnedValue::String("p".repeat(pad));
    OwnedCell::new_with_id(SchemaVid(SCHEMA), id, value)
}

/// How writes reach the store. Morpheus writes every vertex through a
/// transaction, so the restart guarantees must hold for both paths.
#[derive(Clone, Copy, Debug)]
enum Mode {
    Direct,
    Txn,
    /// Bracketed writes, plain deletes: a delete written AFTER the bracket.
    TxnWritesDirectDeletes,
    /// Plain writes, bracketed deletes.
    DirectWritesTxnDeletes,
}

impl Mode {
    fn writes_in_txn(self) -> bool {
        matches!(self, Mode::Txn | Mode::TxnWritesDirectDeletes)
    }
    fn deletes_in_txn(self) -> bool {
        matches!(self, Mode::Txn | Mode::DirectWritesTxnDeletes)
    }
}

struct Store {
    server: Arc<NebServer>,
    client: Arc<client::AsyncClient>,
    mode: Mode,
    chunk_mb: usize,
}

impl Store {
    async fn start(dir: &std::path::Path, group: &str, mode: Mode) -> Store {
        Store::start_in(dir, group, mode, chunk_mb()).await
    }

    /// Use one chunk when the chunk size equals the database size (128 MiB).
    async fn start_in(dir: &std::path::Path, group: &str, mode: Mode, chunk_mb: usize) -> Store {
        let addr = crate::utils::test_port::unique_localhost_addr();
        let server = NebServer::new_from_opts(&options(dir, chunk_mb), &addr, group, async |_| {})
            .await
            .expect("server start");
        server.meta().schemas.register_internal_schema(schema());
        let client = Arc::new(server.data_client(&vec![addr]).await.expect("data client"));
        Store { server, client, mode, chunk_mb }
    }

    async fn restart(self, dir: &std::path::Path, group: &str) -> Store {
        let (mode, chunk_mb) = (self.mode, self.chunk_mb);
        self.server.shutdown().await;
        drop(self.client);
        drop(self.server);
        Store::start_in(dir, group, mode, chunk_mb).await
    }

    async fn write(&self, c: OwnedCell) {
        match self.mode.writes_in_txn() {
            false => {
                let mut c = c;
                self.server.chunks().write_cell(&mut c).expect("write");
            }
            true => self
                .client
                .transaction(move |t| {
                    let c = c.clone();
                    async move { t.write(c).await }
                })
                .await
                .expect("txn write"),
        }
    }

    async fn update(&self, c: OwnedCell) {
        match self.mode.writes_in_txn() {
            false => {
                let mut c = c;
                self.server.chunks().update_cell(&mut c).expect("update");
            }
            true => self
                .client
                .transaction(move |t| {
                    let c = c.clone();
                    async move { t.update(c).await }
                })
                .await
                .expect("txn update"),
        }
    }

    async fn remove(&self, id: Id) {
        match self.mode.deletes_in_txn() {
            false => self.server.chunks().remove_cell(&id).expect("remove"),
            true => self
                .client
                .transaction(move |t| async move { t.remove(id).await })
                .await
                .expect("txn remove"),
        }
    }
}

/// `expected` maps an id to its last acknowledged value, `None` once deleted.
/// Every divergence is collected so one failure reports the whole shape.
fn check(server: &NebServer, expected: &HashMap<u64, (Id, Option<u64>)>, what: &str) {
    let mut resurrected = Vec::new();
    let mut lost = Vec::new();
    let mut stale = Vec::new();
    for (index, (id, want)) in expected {
        match (server.chunks().read_cell(id), want) {
            (Ok(_), None) => resurrected.push(*index),
            (Err(e), Some(_)) => lost.push((*index, format!("{e:?}"))),
            (Ok(read), Some(want)) => {
                let got = Some(read.to_owned().data["DATA"].u64().copied());
                if got != Some(Some(*want)) {
                    stale.push((*index, got, *want));
                }
            }
            (Err(_), None) => {}
        }
    }
    resurrected.sort();
    lost.sort();
    stale.sort();
    let live = expected.values().filter(|(_, v)| v.is_some()).count();
    assert!(
        resurrected.is_empty() && lost.is_empty() && stale.is_empty(),
        "{what}: of {} ids ({live} should be live) {} deleted ids came back {:?}, {} live ids \
         were lost {:?}, {} live ids carry a stale value {:?}",
        expected.len(),
        resurrected.len(),
        &resurrected[..resurrected.len().min(8)],
        lost.len(),
        &lost[..lost.len().min(4)],
        stale.len(),
        &stale[..stale.len().min(4)],
    );
}

/// (a) written, updated, deleted: nothing may come back.
async fn updated_then_deleted(class: IdClass, mode: Mode, group: &str) {
    let _ = env_logger::try_init();
    let dir = scratch_dir();
    let mut store = Store::start(dir.path(), group, mode).await;
    let mut model = HashMap::new();
    for index in 0..200u64 {
        let id = class.id(index);
        store.write(cell(&id, index, 8)).await;
        store.update(cell(&id, index + 1000, 8)).await;
        store.update(cell(&id, index + 2000, 8)).await;
        model.insert(index, (id, Some(index + 2000)));
    }
    for index in (0..200u64).filter(|i| i % 2 == 0) {
        // Delete half so a store that loses EVERYTHING also fails.
        store.remove(model[&index].0).await;
        model.get_mut(&index).unwrap().1 = None;
    }
    check(&store.server, &model, "before the restart");
    store = store.restart(dir.path(), group).await;
    check(&store.server, &model, &format!("after a graceful restart ({class:?}, {mode:?})"));
    store.server.shutdown().await;
}

/// (b) the same ids created, deleted, created, deleted, created: the last
/// create is the truth.
async fn recreated_ids(class: IdClass, mode: Mode, group: &str) {
    let _ = env_logger::try_init();
    let dir = scratch_dir();
    let mut store = Store::start(dir.path(), group, mode).await;
    let mut model = HashMap::new();
    for index in 0..200u64 {
        let id = class.id(index);
        for round in 0..3u64 {
            store.write(cell(&id, index * 10 + round, 8)).await;
            if round < 2 {
                store.remove(id).await;
            }
        }
        model.insert(index, (id, Some(index * 10 + 2)));
    }
    check(&store.server, &model, "before the restart");
    store = store.restart(dir.path(), group).await;
    check(&store.server, &model, &format!("after a graceful restart ({class:?}, {mode:?})"));
    store.server.shutdown().await;
}

/// (c) the cells being deleted live in segments that were sealed (and so
/// archived) long before the delete, with a tombstone in a later segment.
async fn deleted_from_sealed_segments(class: IdClass, mode: Mode, group: &str) {
    let _ = env_logger::try_init();
    let dir = scratch_dir();
    let mut store = Store::start(dir.path(), group, mode).await;
    let mut model = HashMap::new();
    // ~100 KiB each: 200 cells span several 8 MiB segments.
    for index in 0..200u64 {
        let id = class.id(index);
        store.write(cell(&id, index, 100 * 1024)).await;
        model.insert(index, (id, Some(index)));
    }
    let segments: usize = store.server.chunks().list.iter().map(|c| c.seg_count()).sum();
    assert!(segments >= 3, "the cells must span several segments, got {segments}");
    for index in (0..200u64).filter(|i| i % 3 != 0) {
        store.remove(model[&index].0).await;
        model.get_mut(&index).unwrap().1 = None;
    }
    check(&store.server, &model, "before the restart");
    store = store.restart(dir.path(), group).await;
    check(&store.server, &model, &format!("after a graceful restart ({class:?}, {mode:?})"));
    // And once more: the first restart must not have un-deleted anything for
    // the second to expose.
    store = store.restart(dir.path(), group).await;
    check(&store.server, &model, &format!("after a second graceful restart ({class:?}, {mode:?})"));
    store.server.shutdown().await;
}

/// A re-created id restarts at version 1 while the tombstone of the previous
/// incarnation carries its last version (here 3): the re-create must win.
async fn recreated_after_updates(class: IdClass, mode: Mode, group: &str) {
    let _ = env_logger::try_init();
    let dir = scratch_dir();
    let mut store = Store::start(dir.path(), group, mode).await;
    let mut model = HashMap::new();
    for index in 0..100u64 {
        let id = class.id(index);
        store.write(cell(&id, 1, 8)).await;
        store.update(cell(&id, 2, 8)).await;
        store.update(cell(&id, 3, 8)).await;
        store.remove(id).await;
        store.write(cell(&id, 10 + index, 8)).await;
        // Every third id is deleted for good after its second life.
        let last = if index % 3 == 0 {
            store.remove(id).await;
            None
        } else {
            Some(10 + index)
        };
        model.insert(index, (id, last));
    }
    check(&store.server, &model, "before the restart");
    store = store.restart(dir.path(), group).await;
    check(&store.server, &model, &format!("after a graceful restart ({class:?}, {mode:?})"));
    store.server.shutdown().await;
}

macro_rules! scenarios {
    ($($name:ident: $runner:ident($class:expr, $mode:expr, $group:expr);)*) => {
        $(
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                $runner($class, $mode, $group).await;
            }
        )*
    };
}

scenarios! {
    updated_deleted_allocated_direct: updated_then_deleted(IdClass::Allocated, Mode::Direct, "rd_a_ad");
    updated_deleted_hashed_direct: updated_then_deleted(IdClass::Hashed, Mode::Direct, "rd_a_hd");
    updated_deleted_allocated_txn: updated_then_deleted(IdClass::Allocated, Mode::Txn, "rd_a_at");
    updated_deleted_hashed_txn: updated_then_deleted(IdClass::Hashed, Mode::Txn, "rd_a_ht");
    recreated_allocated_direct: recreated_ids(IdClass::Allocated, Mode::Direct, "rd_b_ad");
    recreated_hashed_direct: recreated_ids(IdClass::Hashed, Mode::Direct, "rd_b_hd");
    recreated_allocated_txn: recreated_ids(IdClass::Allocated, Mode::Txn, "rd_b_at");
    recreated_hashed_txn: recreated_ids(IdClass::Hashed, Mode::Txn, "rd_b_ht");
    sealed_deletes_allocated_direct: deleted_from_sealed_segments(IdClass::Allocated, Mode::Direct, "rd_c_ad");
    sealed_deletes_hashed_direct: deleted_from_sealed_segments(IdClass::Hashed, Mode::Direct, "rd_c_hd");
    sealed_deletes_allocated_txn: deleted_from_sealed_segments(IdClass::Allocated, Mode::Txn, "rd_c_at");
    sealed_deletes_hashed_txn: deleted_from_sealed_segments(IdClass::Hashed, Mode::Txn, "rd_c_ht");
    after_updates_allocated_direct: recreated_after_updates(IdClass::Allocated, Mode::Direct, "rd_d_ad");
    after_updates_hashed_txn: recreated_after_updates(IdClass::Hashed, Mode::Txn, "rd_d_ht");
    after_updates_allocated_txn_then_plain_deletes: recreated_after_updates(IdClass::Allocated, Mode::TxnWritesDirectDeletes, "rd_d_atp");
    after_updates_hashed_plain_then_txn_deletes: recreated_after_updates(IdClass::Hashed, Mode::DirectWritesTxnDeletes, "rd_d_hpt");
    mixed_updated_deleted_txn_then_plain: updated_then_deleted(IdClass::Hashed, Mode::TxnWritesDirectDeletes, "rd_a_htp");
    mixed_updated_deleted_plain_then_txn: updated_then_deleted(IdClass::Allocated, Mode::DirectWritesTxnDeletes, "rd_a_apt");
}

/// Writes 256 KiB cells with fresh keyed ids until the (only) chunk opens a
/// new segment. Returns the ids; the last one lives in the new segment.
fn fill_until_new_segment(store: &Store, tag: &str, counter: &mut u64) -> Vec<Id> {
    let chunk = &store.server.chunks().list[0];
    let before = chunk.seg_count();
    let mut ids = Vec::new();
    while chunk.seg_count() == before {
        *counter += 1;
        let id = Id::from_obj(&format!("filler-{tag}-{counter}"));
        store
            .server
            .chunks()
            .write_cell(&mut cell(&id, *counter, 256 * 1024))
            .expect("filler write");
        ids.push(id);
    }
    ids
}

fn tombstones_for(store: &Store, id: Id) -> Vec<(u64, u64, u64)> {
    use crate::ram::entry::EntryType;
    use crate::ram::tombstone::Tombstone;
    store.server.chunks().list[0]
        .segments()
        .iter()
        .flat_map(|segment| {
            segment.entry_iter().filter_map(move |entry| {
                if entry.entry_header.entry_type != EntryType::TOMBSTONE {
                    return None;
                }
                let tombstone = Tombstone::read_from_entry_content_addr(entry.body_pos);
                (tombstone.id == id).then_some((
                    segment.seq_id,
                    tombstone.segment_seq_id,
                    tombstone.version,
                ))
            })
        })
        .collect()
}

/// A keyed id is deleted after updates (tombstone at version 4), then
/// re-created (version 2) in a LATER segment than the tombstone.
async fn recreated_in_another_segment_below_the_tombstone_version(mode: Mode, group: &str) {
    let _ = env_logger::try_init();
    let dir = scratch_dir();
    let mut store = Store::start_in(dir.path(), group, mode, 128).await;
    store.server.cleaner().pause();
    let mut model = HashMap::new();
    let mut counter = 0u64;
    for index in 0..20u64 {
        let id = Id::from_obj(&format!("recreate-key-{index}"));
        store.write(cell(&id, 1, 8)).await;
        store.update(cell(&id, 2, 8)).await;
        store.update(cell(&id, 3, 8)).await;
        store.remove(id).await;
        model.insert(index, (id, None));
    }
    let tombstone_segments = store.server.chunks().list[0].seg_count();
    fill_until_new_segment(&store, "r", &mut counter);
    assert!(store.server.chunks().list[0].seg_count() > tombstone_segments);
    store = store.restart(dir.path(), group).await;
    {
        let floors = store.server.chunks().list[0].deleted_versions.read();
        assert_eq!(floors.len(), 20, "recovery must rebuild the deleted versions");
        println!("deleted-version map: ids={}, capacity={}, entry_bytes={}",
            floors.len(), floors.capacity(),
            std::mem::size_of::<(u64, crate::ram::chunk::DeletedVersion)>());
    }
    for index in 0..20u64 {
        let id = model[&index].0;
        let tombstones = tombstones_for(&store, id);
        assert_eq!(tombstones.len(), 1, "one tombstone per deleted key");
        assert_eq!(tombstones[0].2, 4, "the deleted version must be 4");
        store.write(cell(&id, 100 + index, 8)).await;
        let chunk = &store.server.chunks().list[0];
        let (location, version) = chunk.cell_location_and_version(id.bits()).unwrap();
        let segment = chunk
            .segments()
            .into_iter()
            .find(|s| s.contains_address(location))
            .unwrap();
        assert_eq!(version, 5, "a re-create must exceed the deleted version");
        assert!(
            segment.seq_id > tombstones[0].0,
            "the re-create must follow the tombstone segment"
        );
        model.get_mut(&index).unwrap().1 = Some(100 + index);
    }
    check(&store.server, &model, "before the restart");
    store = store.restart(dir.path(), group).await;
    check(
        &store.server,
        &model,
        &format!("after a graceful restart ({mode:?})"),
    );
    store.server.shutdown().await;
}

macro_rules! other_segment_scenarios {
    ($($name:ident: $mode:expr, $group:expr;)*) => {
        $(
            // Recovery loses a lower-version re-create in a later segment.
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                recreated_in_another_segment_below_the_tombstone_version($mode, $group).await;
            }
        )*
    };
}

other_segment_scenarios! {
    recreate_in_other_segment_direct: Mode::Direct, "rd_e_d";
    recreate_in_other_segment_txn: Mode::Txn, "rd_e_t";
}

/// The cleaner removes the segment a tombstone names, then drops the
/// tombstone, while an older version of the same cell still sits in another
/// segment.
// Recovery resurrects an older cell after the cleaner removes its tombstone.
#[tokio::test(flavor = "multi_thread")]
async fn a_cleaned_tombstone_does_not_bring_back_an_older_version() {
    use crate::ram::cleaner::combine::CombinedCleaner;
    let _ = env_logger::try_init();
    let dir = scratch_dir();
    let group = "rd_f_clean";
    let mut store = Store::start_in(dir.path(), group, Mode::Direct, 128).await;
    store.server.cleaner().pause();
    let chunk = &store.server.chunks().list[0];
    let mut counter = 0u64;
    let mut model: HashMap<u64, (Id, Option<u64>)> = HashMap::new();
    let mut next = 0u64;
    let mut track = |model: &mut HashMap<u64, (Id, Option<u64>)>, id: Id, value: Option<u64>| {
        next += 1;
        model.insert(next, (id, value));
    };

    // Segment A: the key at version 2, and live fillers that keep A out of
    // the cleaner's reach.
    let key = Id::from_obj(&"cleaned-key");
    store.write(cell(&key, 1, 8)).await;
    let a_fillers = fill_until_new_segment(&store, "a", &mut counter);
    for (i, id) in a_fillers.iter().enumerate() {
        track(
            &mut model,
            *id,
            Some(counter - a_fillers.len() as u64 + 1 + i as u64),
        );
    }
    // Segment B: the key at version 3.
    store.update(cell(&key, 2, 8)).await;
    let b_fillers = fill_until_new_segment(&store, "b", &mut counter);
    // Segment C: the tombstones. Everything in B dies.
    store.remove(key).await;
    track(&mut model, key, None);
    for id in b_fillers {
        store.remove(id).await;
        track(&mut model, id, None);
    }
    let c_fillers = fill_until_new_segment(&store, "c", &mut counter);
    for id in &c_fillers {
        store.remove(*id).await;
        track(&mut model, *id, None);
    }
    let _d_fillers = fill_until_new_segment(&store, "d", &mut counter);

    // Combine B with C. The cleaner removes B and its tombstones.
    let mut by_seq = chunk.segments();
    by_seq.sort_by_key(|segment| segment.seq_id);
    println!(
        "segments before the cleaner: {:?}",
        by_seq.iter().map(|s| s.seq_id).collect::<Vec<_>>()
    );
    let pick = |seqs: &[usize],
                by_seq: &Vec<lightning::aarc::Arc<crate::ram::segs::Segment>>|
     -> Vec<_> { seqs.iter().map(|i| by_seq[*i].clone()).collect() };
    let first = CombinedCleaner::combine_segments(chunk, &pick(&[1, 2], &by_seq));
    println!("first combine: {first:?}");
    // Combine again: the tombstones now name a segment that is gone.
    for _ in 0..3 {
        let mut now = chunk.segments();
        now.sort_by_key(|segment| segment.seq_id);
        let candidates: Vec<_> = now
            .iter()
            .filter(|s| !chunk.is_active_head(s.id))
            .cloned()
            .collect();
        // Never A: it holds the older version of the key.
        let candidates: Vec<_> = candidates
            .into_iter()
            .filter(|s| s.seq_id != by_seq[0].seq_id)
            .collect();
        println!(
            "round candidates: {:?}",
            candidates.iter().map(|s| s.seq_id).collect::<Vec<_>>()
        );
        if candidates.len() >= 2 {
            let r = CombinedCleaner::combine_segments(chunk, &candidates);
            println!("combine: {r:?}");
        }
    }
    let after: Vec<u64> = {
        let mut v: Vec<u64> = chunk.segments().iter().map(|s| s.seq_id).collect();
        v.sort();
        v
    };
    println!("segments after the cleaner: {after:?}");
    assert!(
        after.contains(&by_seq[0].seq_id),
        "segment A must still exist"
    );
    assert!(
        !after.contains(&by_seq[1].seq_id),
        "segment B must be gone, or the test proves nothing"
    );
    assert!(
        !tombstones_for(&store, key).is_empty(),
        "the cleaner must retain the tombstone while an older entry exists"
    );
    let old_versions: Vec<_> = by_seq[0]
        .entry_iter()
        .filter_map(|entry| {
            if entry.entry_header.entry_type != crate::ram::entry::EntryType::CELL {
                return None;
            }
            let header = crate::ram::cell::cell_header_from_entry_content_addr(entry.body_pos);
            (header.id == key).then_some(header.version)
        })
        .collect();
    assert_eq!(
        old_versions,
        vec![2],
        "segment A must retain the old physical entry"
    );
    check(
        &store.server,
        &model,
        "after the cleaner, before the restart",
    );
    store = store.restart(dir.path(), group).await;
    check(&store.server, &model, "after a graceful restart");
    store.server.shutdown().await;
}
