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

fn options(dir: &std::path::Path) -> ServerOptions {
    ServerOptions {
        chunk_size: 64 * 1024 * 1024,
        db_size: 64 * 1024 * 1024,
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
}

struct Store {
    server: Arc<NebServer>,
    client: Arc<client::AsyncClient>,
    mode: Mode,
}

impl Store {
    async fn start(dir: &std::path::Path, group: &str, mode: Mode) -> Store {
        let addr = crate::utils::test_port::unique_localhost_addr();
        let server = NebServer::new_from_opts(&options(dir), &addr, group, async |_| {})
            .await
            .expect("server start");
        server.meta().schemas.register_internal_schema(schema());
        let client = Arc::new(server.data_client(&vec![addr]).await.expect("data client"));
        Store { server, client, mode }
    }

    async fn restart(self, dir: &std::path::Path, group: &str) -> Store {
        let mode = self.mode;
        self.server.shutdown().await;
        drop(self.client);
        drop(self.server);
        Store::start(dir, group, mode).await
    }

    async fn write(&self, c: OwnedCell) {
        match self.mode {
            Mode::Direct => {
                let mut c = c;
                self.server.chunks().write_cell(&mut c).expect("write");
            }
            Mode::Txn => self
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
        match self.mode {
            Mode::Direct => {
                let mut c = c;
                self.server.chunks().update_cell(&mut c).expect("update");
            }
            Mode::Txn => self
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
        match self.mode {
            Mode::Direct => self.server.chunks().remove_cell(&id).expect("remove"),
            Mode::Txn => self
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
}
