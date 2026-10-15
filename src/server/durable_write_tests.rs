#[cfg(test)]
use super::*;
use crate::ram::schema::{Field, Schema, SchemaVid};
use crate::ram::types::Type;
use crate::server::{NebServer, ServerOptions};
use dovahkiin::types::{Map, OwnedMap};

const SCHEMA: u32 = 8901;
fn cell(i: u64, value: u64) -> OwnedCell {
    let mut data = OwnedValue::Map(OwnedMap::new());
    data["value"] = OwnedValue::U64(value);
    OwnedCell::new_with_id(SchemaVid(SCHEMA), &Id::from_parts(1, i), data)
}
fn schema() -> Schema {
    Schema::new_with_id(
        SCHEMA,
        "durable_ack",
        None,
        Field::new_schema(vec![Field::new_unindexed("value", Type::U64)]),
        false,
        false,
    )
}
async fn fixture(dir: &tempfile::TempDir) -> (Arc<NebServer>, Arc<NebRPCService>) {
    fixture_with_indices(dir, false).await
}

async fn fixture_with_indices(
    dir: &tempfile::TempDir,
    indexed: bool,
) -> (Arc<NebServer>, Arc<NebRPCService>) {
    // Isolated test processes keep timer scheduling out of the cut model.
    std::env::set_var("NEB_WAL_SYNC_INTERVAL_MS", "3600000");
    let addr = crate::utils::test_port::unique_localhost_addr();
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 128 * 1024 * 1024,
            db_size: 128 * 1024 * 1024,
            tiered_config: None,
            backup_storage: Some(dir.path().join("backup").to_string_lossy().into_owned()),
            wal_storage: Some(dir.path().join("wal").to_string_lossy().into_owned()),
            raft_storage: Some(dir.path().join("raft").to_string_lossy().into_owned()),
            services: if indexed {
                vec![
                    crate::server::Service::Cell,
                    crate::server::Service::RangedIndexer,
                    crate::server::Service::Query,
                ]
            } else {
                vec![crate::server::Service::Cell]
            },
            index_enabled: indexed,
            enable_recovery: true,
            disable_storage_locks: true,
        },
        &addr,
        &"durable_ack_test".to_string(),
        async |_| {},
    )
    .await
    .unwrap();
    let runtime = server.current_database();
    runtime.meta.schemas.debug_only_new_schema(schema());
    let rpc = NebRPCService::new(runtime.clone(), runtime.neb_client.clone());
    (server, rpc)
}
fn assert_covered(server: &NebServer) {
    let mut frames = 0;
    for chunk in &server.chunks().list {
        for seg in chunk.segs.iter_values() {
            let Some(file) = seg.durable_file() else {
                continue;
            };
            if file.end() > crate::ram::wal_format::FILE_HEADER_SIZE as u64 {
                assert!(
                    file.watermark() >= file.end(),
                    "acknowledgement preceded its durable watermark"
                );
                frames += 1;
            }
        }
    }
    assert!(frames > 0);
}

#[tokio::test]
async fn durable_ack_all_plain_rpc_handlers_cover_their_wal() {
    let dir = tempfile::tempdir().unwrap();
    let (server, rpc) = fixture(&dir).await;
    let h = rpc.write_cell_durable(cell(1, 1)).await.unwrap();
    assert_covered(&server);
    rpc.compare_version_and_update_cell_durable(h.id, h.version, cell(1, 2))
        .await
        .unwrap();
    assert_covered(&server);
    let h = rpc.update_cell_durable(cell(1, 3)).await.unwrap();
    assert_covered(&server);
    rpc.compare_version_and_set_field_durable(
        h.id,
        h.version,
        bifrost_hasher::hash_str("value"),
        OwnedValue::U64(4),
    )
    .await
    .unwrap();
    assert_covered(&server);
    rpc.upsert_cell_durable(cell(2, 2)).await.unwrap();
    assert_covered(&server);
    for result in rpc
        .upsert_all_cells_durable(vec![cell(3, 3), cell(4, 4)])
        .await
    {
        result.unwrap();
    }
    assert_covered(&server);
    rpc.remove_cell_durable(Id::from_parts(1, 1)).await.unwrap();
    assert_covered(&server);
    server.shutdown().await;
}

async fn cut_and_recover(durable: bool) -> usize {
    cut_with_batch(durable, 1024).await
}

async fn cut_with_batch(durable: bool, batch: u64) -> usize {
    let live = tempfile::tempdir().unwrap();
    let cut = tempfile::tempdir().unwrap();
    let (server, rpc) = fixture(&live).await;
    for i in 1..=64 {
        if durable {
            rpc.write_cell_durable(cell(i, 1)).await
        } else {
            rpc.write_cell(cell(i, 1)).await
        }
        .unwrap();
    }
    for i in 1..=16 {
        if durable {
            rpc.update_cell_durable(cell(i, 2)).await
        } else {
            rpc.update_cell(cell(i, 2)).await
        }
        .unwrap();
    }
    for i in 17..=32 {
        if durable {
            rpc.remove_cell_durable(Id::from_parts(1, i)).await
        } else {
            rpc.remove_cell(Id::from_parts(1, i)).await
        }
        .unwrap();
    }
    let cells = (65..65 + batch).map(|i| cell(i, 3)).collect();
    let results = if durable {
        rpc.upsert_all_cells_durable(cells).await
    } else {
        rpc.upsert_all_cells(cells).await
    };
    for result in results {
        result.unwrap();
    }
    // Copy every WAL and discard all bytes above its certified prefix.
    // Shutdown happens only after this independent crash image is complete.
    let wal = cut.path().join("wal");
    std::fs::create_dir_all(&wal).unwrap();
    for chunk in &server.chunks().list {
        for seg in chunk.segs.iter_values() {
            let source = chunk
                .file_manager
                .wal_path(seg.chunk_id, seg.id, seg.seq_id)
                .unwrap();
            if !std::path::Path::new(&source).exists() {
                continue;
            }
            let relative = std::path::Path::new(&source)
                .strip_prefix(server.chunks().list[0].file_manager.wal_storage().unwrap())
                .unwrap();
            let destination = wal.join(relative);
            std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
            std::fs::copy(source, &destination).unwrap();
            std::fs::OpenOptions::new()
                .write(true)
                .open(destination)
                .unwrap()
                .set_len(
                    seg.durable_file()
                        .map(|d| d.watermark())
                        .unwrap_or(crate::ram::wal_format::FILE_HEADER_SIZE as u64),
                )
                .unwrap();
        }
    }
    let schemas = crate::ram::schema::LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema());
    let recovered = crate::ram::chunk::Chunks::new_with_recovery(
        1,
        128 * 1024 * 1024,
        Arc::new(crate::server::ServerMeta { schemas }),
        None,
        Some(cut.path().join("backup").to_string_lossy().into_owned()),
        Some(wal.to_string_lossy().into_owned()),
        None,
        true,
        Some(cut.path().join("raft").to_string_lossy().into_owned()),
    );
    let mut missing = 0;
    for i in 1..65 + batch {
        let actual = recovered
            .read_cell(&Id::from_parts(1, i))
            .map(|c| c.to_owned());
        if (17..=32).contains(&i) {
            assert!(actual.is_err(), "acknowledged deletion was lost");
        } else {
            let value = if i <= 16 {
                2
            } else if i <= 64 {
                1
            } else {
                3
            };
            match actual {
                Ok(actual) => assert_eq!(actual.data, cell(i, value).data),
                Err(_) => missing += 1,
            }
        }
    }
    recovered.stop_background();
    server.shutdown().await;
    missing
}

#[tokio::test]
async fn durable_ack_power_loss_cut_preserves_acknowledgements_with_negative_control() {
    assert_eq!(
        cut_and_recover(true).await,
        0,
        "durable acknowledgements were lost"
    );
    assert!(
        cut_and_recover(false).await > 0,
        "negative control lost no acknowledged writes"
    );
}

#[tokio::test]
async fn durable_ack_cluster_flush_covers_default_and_derived_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    let (server, rpc) = fixture(&dir).await;
    for result in rpc
        .upsert_all_cells((1..=64).map(|i| cell(i, 1)).collect())
        .await
    {
        result.unwrap();
    }
    assert!(server
        .chunks()
        .list
        .iter()
        .flat_map(|c| c.segments())
        .all(|s| s.durable_file().is_none()));
    rpc.flush_wal_durable().await.unwrap();
    assert_covered(&server);
    rpc.update_cell(cell(1, 2)).await.unwrap();
    rpc.flush_wal_durable().await.unwrap();
    assert_covered(&server);
    server.shutdown().await;
}

#[tokio::test]
async fn durable_ack_indexed_write_covers_its_derived_wals() {
    let _ = env_logger::try_init();
    let dir = tempfile::tempdir().unwrap();
    let (server, rpc) = fixture_with_indices(&dir, true).await;
    let client = server.current_database().neb_client.clone();
    client
        .new_schema_with_id(Schema::new_with_id(
            8902,
            "durable_indexed",
            None,
            Field::new_schema(vec![Field::new_indexed(
                "value",
                Type::U64,
                vec![crate::ram::schema::IndexType::Hashed],
            )]),
            false,
            false,
        ))
        .await
        .unwrap()
        .unwrap();
    let mut indexed = cell(1, 1);
    indexed.header.schema = SchemaVid(8902);
    rpc.write_cell_durable(indexed).await.unwrap();
    let before: std::collections::HashMap<_, _> = server
        .chunks()
        .list
        .iter()
        .flat_map(|c| c.segments())
        .map(|s| ((s.chunk_id, s.id, s.seq_id), s.wal_append_position()))
        .collect();
    let waits = crate::ram::durability::WAIT_CALLS.load(atomic::Ordering::Relaxed);
    let cells = (2..=32)
        .map(|i| {
            let mut c = cell(i, i);
            c.header.schema = SchemaVid(8902);
            c
        })
        .collect();
    for result in rpc.upsert_all_cells_durable(cells).await {
        result.unwrap();
    }
    let changed = server
        .chunks()
        .list
        .iter()
        .flat_map(|c| c.segments())
        .filter(|s| {
            s.wal_append_position() > *before.get(&(s.chunk_id, s.id, s.seq_id)).unwrap_or(&0)
        })
        .count();
    assert_eq!(
        crate::ram::durability::WAIT_CALLS.load(atomic::Ordering::Relaxed) - waits,
        changed as u64,
        "an indexed batch must wait once per changed WAL"
    );
    let mut files = 0;
    for chunk in &server.chunks().list {
        for segment in chunk.segments() {
            if segment.file_state.lock().wal.is_none() {
                continue;
            }
            let file = segment
                .durable_file()
                .expect("a durable write must cover its derived WALs");
            assert!(
                file.watermark() >= segment.wal_append_position(),
                "derived WAL is not covered"
            );
            files += 1;
        }
    }
    assert!(files > 0);
    server.shutdown().await;
}

#[tokio::test]
async fn durable_ack_specialized_batch_keeps_successful_prefix_and_fences() {
    let dir = tempfile::tempdir().unwrap();
    let (server, rpc) = fixture(&dir).await;
    let before = super::SPECIALIZED_BATCH_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let mut missing = cell(2, 2);
    missing.header.schema = crate::ram::schema::SchemaVid(u32::MAX);
    let results = rpc
        .upsert_all_cells_durable(vec![cell(1, 1), missing, cell(3, 3)])
        .await;
    assert!(results[0].is_ok());
    assert!(matches!(
        results[1],
        Err(WriteError::SchemaDoesNotExisted(_))
    ));
    assert!(matches!(results[2], Err(WriteError::BatchAborted)));
    assert_covered(&server);
    assert_eq!(
        super::SPECIALIZED_BATCH_CALLS.load(std::sync::atomic::Ordering::Relaxed) - before,
        1
    );
    let token = rpc
        .fence_engage(&vec![schema().uid.0], 1000, 60000)
        .await
        .unwrap();
    let blocked = rpc.upsert_all_cells_durable(vec![cell(4, 4)]).await;
    assert!(
        blocked[0].is_err(),
        "specialized application bypassed the write fence"
    );
    rpc.fence_release(token).await;
    server.shutdown().await;
}

#[tokio::test]
async fn durable_ack_captured_calls_wait_once_for_the_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let (server, rpc) = fixture(&dir).await;
    let mut tokens = Vec::new();
    for i in 1..=1024 {
        let (result, captured) = rpc.write_cell_captured(cell(i, i)).await;
        assert!(result.is_ok());
        tokens.extend(captured);
    }
    assert_eq!(tokens.len(), 1024);
    let before = crate::ram::durability::WAIT_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let end_before = server
        .chunks()
        .list
        .iter()
        .flat_map(|chunk| chunk.segs.iter_values())
        .filter_map(|seg| seg.durable_file())
        .filter(|file| file.end() > file.watermark())
        .count();
    assert!(end_before > 0, "application performed a durable wait");
    rpc.finish_captured_receipts(tokens, true).await.unwrap();
    assert_covered(&server);
    assert_eq!(
        crate::ram::durability::WAIT_CALLS.load(std::sync::atomic::Ordering::Relaxed) - before,
        end_before as u64
    );
    assert!(rpc.captured_receipts.lock().entries.is_empty());
    server.shutdown().await;
}

#[tokio::test]
async fn durable_ack_captured_receipts_fail_on_sync_expiry_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (server, rpc) = fixture(&dir).await;
    let (result, tokens) = rpc.write_cell_captured(cell(1, 1)).await;
    result.unwrap();
    let old_token = tokens[0];
    for chunk in &server.chunks().list {
        for segment in chunk.segs.iter_values() {
            if let Some(file) = segment.durable_file() {
                file.inject_failure();
            }
        }
    }
    assert!(matches!(
        rpc.finish_captured_receipts(tokens, true).await,
        Err(WriteError::Durability(
            crate::ram::durability::DurabilityError::Sync { .. }
        ))
    ));
    assert!(rpc.captured_receipts.lock().entries.is_empty());
    // Missing tokens cannot be mistaken for records created after restart.
    let fresh = super::CapturedReceipts::default();
    let missing = Id::from_parts(fresh.incarnation.wrapping_add(1), 1);
    assert!(super::CapturedReceipts::default()
        .take(vec![missing])
        .is_err());
    server.shutdown().await;
    let dir = tempfile::tempdir().unwrap();
    let (server, rpc) = fixture(&dir).await;
    let (_, tokens) = rpc.write_cell_captured(cell(1, 1)).await;
    rpc.captured_receipts
        .lock()
        .entries
        .get_mut(&tokens[0])
        .unwrap()
        .1 -= std::time::Duration::from_secs(600);
    assert!(matches!(
        rpc.finish_captured_receipts(tokens, true).await,
        Err(WriteError::Durability(_))
    ));
    assert!(rpc.captured_receipts.lock().entries.is_empty());
    server.shutdown().await;
    let dir = tempfile::tempdir().unwrap();
    let (server, rpc) = fixture(&dir).await;
    let (result, new_tokens) = rpc.write_cell_captured(cell(1, 1)).await;
    result.unwrap();
    assert!(
        matches!(
            rpc.finish_captured_receipts(vec![old_token], true).await,
            Err(WriteError::Durability(_))
        ),
        "a previous runtime's token certified a replacement write"
    );
    rpc.finish_captured_receipts(new_tokens, true)
        .await
        .unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn durable_ack_captured_client_groups_by_rpc_target() {
    let dir = tempfile::tempdir().unwrap();
    let (server, _) = fixture(&dir).await;
    let capture = crate::client::CapturedWriteBatch::new();
    let client = server.current_database().neb_client.clone();
    let syncs_before =
        crate::ram::durability::GROUP_SYNCS.load(std::sync::atomic::Ordering::Relaxed);
    for i in 1..=128 {
        let h = client
            .write_cell_captured(&capture, cell(i, i))
            .await
            .unwrap()
            .unwrap();
        let h = client
            .compare_version_and_update_cell_captured(&capture, h.id, h.version, cell(i, i + 1))
            .await
            .unwrap()
            .unwrap();
        client
            .compare_version_and_set_field_captured(
                &capture,
                h.id,
                h.version,
                bifrost_hasher::hash_str("value"),
                OwnedValue::U64(i + 2),
            )
            .await
            .unwrap()
            .unwrap();
        if i % 4 == 0 {
            client
                .remove_cell_captured(&capture, h.id)
                .await
                .unwrap()
                .unwrap();
        }
    }
    assert_eq!(
        crate::ram::durability::GROUP_SYNCS.load(std::sync::atomic::Ordering::Relaxed),
        syncs_before,
        "a captured operation used an immediate durability barrier"
    );
    let touched = server
        .chunks()
        .list
        .iter()
        .flat_map(|chunk| chunk.segs.iter_values())
        .filter(|seg| seg.durable_file().is_some())
        .count();
    let before = crate::ram::durability::WAIT_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    capture.finish().await.unwrap().unwrap();
    assert_covered(&server);
    assert_eq!(
        crate::ram::durability::WAIT_CALLS.load(std::sync::atomic::Ordering::Relaxed) - before,
        touched as u64
    );
    assert!(
        capture.finish().await.unwrap().is_err(),
        "a deferred scope was certified twice"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn compose_durable_receipts_preserve_verified_reads() {
    use crate::index::verification::IndexCheck;
    use crate::ram::schema::{IndexType, SchemaUid};
    let dir = tempfile::tempdir().unwrap();
    let (server, rpc) = fixture_with_indices(&dir, true).await;
    let client = server.current_database().neb_client.clone();
    client
        .new_schema_with_id(Schema::new_with_id(
            8903,
            "compose_verified_durable",
            None,
            Field::new_schema(vec![Field::new_indexed(
                "value",
                Type::U64,
                vec![IndexType::Hashed],
            )]),
            false,
            false,
        ))
        .await
        .unwrap()
        .unwrap();
    let mut initial = cell(1, 1);
    initial.header.schema = SchemaVid(8903);
    rpc.write_cell_durable(initial).await.unwrap();
    let mut changed = cell(1, 2);
    changed.header.schema = SchemaVid(8903);
    let (result, tokens) = rpc
        .compare_version_and_update_cell_captured(
            changed.header.id,
            rpc.head_cell(changed.header.id).await.unwrap().version,
            changed,
        )
        .await;
    result.unwrap();
    assert!(!tokens.is_empty());
    let checks = |value| {
        vec![IndexCheck::Hashed {
            buckets: vec![
                crate::query::data_client::IndexedDataClient::hashed_index_id(
                    SchemaUid(8903),
                    bifrost_hasher::hash_str("value"),
                    &OwnedValue::U64(value),
                ),
            ],
            field: bifrost_hasher::hash_str("value"),
            value: OwnedValue::U64(value),
        }]
    };
    let ids = vec![Id::from_parts(1, 1)];
    let empty = Expr::List(vec![]);
    for _ in 0..2 {
        let rejected = rpc
            .read_all_cells_verified(&ids, &vec![], &empty, &empty, &checks(1))
            .await;
        assert!(matches!(&rejected[0], Err(ReadError::NotMatch)));
        let accepted = rpc
            .read_all_cells_verified(&ids, &vec![], &empty, &empty, &checks(2))
            .await;
        assert_eq!(
            accepted[0].as_ref().unwrap().data["value"],
            OwnedValue::U64(2)
        );
        if !rpc.captured_receipts.lock().entries.is_empty() {
            rpc.finish_captured_receipts(tokens.clone(), true)
                .await
                .unwrap();
        }
    }
    assert_covered(&server);
    server.shutdown().await;
}
