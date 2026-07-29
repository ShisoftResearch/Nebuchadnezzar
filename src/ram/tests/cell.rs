use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::durable_fs::fail_next_directory_sync_for_test;
use crate::ram::entry::ENTRY_HEAD_SIZE;
use crate::ram::schema::*;
use crate::ram::types;
use crate::ram::types::*;
use crate::server::ServerMeta;
use bifrost_hasher::hash_str;
use dovahkiin::types::Type;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::*;

pub const CHUNK_SIZE: usize = 8 * 1024 * 1024;

fn test_chunks() -> Arc<Chunks> {
    let schema = Schema::new_with_id(1, "snapshot-test", None, default_fields(), false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema);
    Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    )
}

#[test]
fn exact_transaction_output_segments_sync_insert_update_and_delete_once() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let schema = Schema::new_with_id(1, "durable-output", None, default_fields(), false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema);
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        Some(temp_dir.path().join("wal").to_string_lossy().into_owned()),
        None,
    );
    let insert_id = Id::new(1, 80);
    let update_id = Id::new(1, 81);
    let delete_id = Id::new(1, 82);

    crate::ram::chunk::set_transaction_context(true);
    let mut inserted = test_cell(insert_id, 1);
    let insert = chunks
        .write_cell_at_revision(&mut inserted, RevisionWrite::pending(200))
        .unwrap();
    let mut original = test_cell(update_id, 2);
    chunks
        .write_cell_at_revision(&mut original, RevisionWrite::committed(100))
        .unwrap();
    let mut updated = test_cell(update_id, 2);
    let update = chunks
        .update_cell_at_revision(&mut updated, RevisionWrite::pending(200))
        .unwrap();
    let mut deleted = test_cell(delete_id, 3);
    chunks
        .write_cell_at_revision(&mut deleted, RevisionWrite::committed(100))
        .unwrap();
    let delete = chunks
        .remove_cell_at_revision(&delete_id, RevisionWrite::pending(200))
        .unwrap();
    crate::ram::chunk::set_transaction_context(false);

    let segment = chunks.list[0]
        .locate_segment(insert.node.load().1)
        .expect("installed insert segment");
    let before = segment.force_wal_sync_count_for_test();
    chunks
        .force_sync_installed_revisions([&insert, &update, &delete])
        .unwrap();
    assert_eq!(
        segment.force_wal_sync_count_for_test(),
        before + 1,
        "all three exact outputs share one segment and must be deduplicated"
    );
}

fn test_cell(id: Id, value: u64) -> OwnedCell {
    OwnedCell::new_with_id(
        1,
        &id,
        data_map_value! {
            id: value as i64,
            score: value,
            name: format!("value-{value}")
        },
    )
}

fn physical_accounting(chunks: &Chunks) -> Vec<(u64, usize, u32, u32)> {
    chunks.list[0]
        .segs
        .iter_values()
        .map(|segment| {
            (
                segment.id,
                segment.append_header.load(Ordering::Acquire),
                segment.dead_space.load(Ordering::Acquire),
                segment.tombstones.load(Ordering::Acquire),
            )
        })
        .collect()
}

#[test]
fn failed_tombstone_wal_publication_accounts_orphan_without_tombstone_drift() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let schema = Schema::new_with_id(
        1,
        "failed-tombstone-publication",
        None,
        default_fields(),
        false,
        false,
    );
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema);
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        Some(wal_dir.to_string_lossy().into_owned()),
        None,
    );
    let id = Id::new(1, 89);
    let mut original = test_cell(id, 1);
    chunks
        .write_cell_at_revision(&mut original, RevisionWrite::committed(100))
        .unwrap();
    let chunk = &chunks.list[0];
    let head = chunk
        .segs
        .get(&(chunk.get_head_seg_id() as usize))
        .expect("active head");
    let wal_path = {
        let mut state = head.file_state.lock();
        let wal_path = state
            .manager
            .wal_path(head.chunk_id, head.id, head.seq_id)
            .expect("configured WAL path");
        drop(state.wal.take());
        state
            .manager
            .delete_wal(head.chunk_id, head.id, head.seq_id)
            .unwrap();
        std::path::PathBuf::from(wal_path)
    };
    assert!(!wal_path.exists(), "test must force lazy WAL recreation");
    let output_wal_dir = wal_path.parent().expect("WAL path should have a parent");
    let used_before = head.used_spaces();
    let dead_before = head.dead_space();
    let tombstones_before = head.tombstones.load(Ordering::Acquire);
    fail_next_directory_sync_for_test(output_wal_dir);

    let error = match chunks.remove_cell_at_revision(&id, RevisionWrite::pending(200)) {
        Ok(_) => panic!("WAL filename publication failure must reject the tombstone"),
        Err(error) => error,
    };
    assert!(matches!(error, WriteError::DurabilityFailure(_)));
    let used_delta = head.used_spaces() - used_before;
    assert!(used_delta > 0);
    assert_eq!(
        head.dead_space() - dead_before,
        used_delta,
        "the uninstalled allocation must be immediately dead-accounted"
    );
    assert_eq!(
        head.tombstones.load(Ordering::Acquire),
        tombstones_before,
        "a failed tombstone must not affect live tombstone accounting"
    );
}

fn assert_zero_write_rejected(result: Result<InstalledRevision, WriteError>) {
    assert!(matches!(result, Err(WriteError::CellRevisionMismatch)));
}

#[test]
fn snapshot_reads_old_address_after_current_update() {
    let chunks = test_chunks();
    let id = Id::new(1, 91);
    let mut first = test_cell(id, 10);
    chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    let old_address = chunks.address_of(&id);

    let mut second = test_cell(id, 20);
    chunks
        .update_cell_at_revision(&mut second, RevisionWrite::committed(200))
        .unwrap();

    let selected = chunks.read_cell_snapshot(&id, 150).unwrap();
    let SnapshotRead::Present(cell) = selected else {
        panic!("snapshot should select the old cell");
    };
    assert_eq!(cell.header.revision_ts, 100);
    assert_eq!(chunks.history_location(&id, 100), Some(old_address));
}

#[test]
fn direct_write_and_update_do_not_create_or_resolve_history() {
    let chunks = test_chunks();
    let id = Id::new(1, 105);
    let history = &chunks.list[0].history;
    crate::ram::history::take_revision_node_allocations_for_test();
    history.take_chain_map_resolutions_for_test();
    history.take_direct_path_activity_for_test();

    let mut first = test_cell(id, 10);
    chunks.write_cell(&mut first).unwrap();

    let mut second = test_cell(id, 20);
    chunks.update_cell(&mut second).unwrap();

    assert!(
        second.header.revision_ts > first.header.revision_ts,
        "the direct replacement must have a strictly newer revision"
    );
    assert_eq!(history.take_chain_map_resolutions_for_test(), 0);
    assert_eq!(
        crate::ram::history::take_revision_node_allocations_for_test(),
        0
    );
    assert_eq!(history.take_direct_path_activity_for_test(), (0, 0));
    assert_eq!(history.revision_count_for_test(&id), 0);
}

#[test]
fn direct_update_revision_timestamps_are_strictly_increasing() {
    let chunks = test_chunks();
    let id = Id::new(1, 106);
    let mut cell = test_cell(id, 10);
    chunks.write_cell(&mut cell).unwrap();
    let mut previous = cell.header.revision_ts;

    for value in 11..=14 {
        let mut updated = test_cell(id, value);
        chunks.update_cell(&mut updated).unwrap();
        assert!(updated.header.revision_ts > previous);
        previous = updated.header.revision_ts;
    }
}

#[test]
fn every_direct_update_entry_point_bypasses_history() {
    let chunks = test_chunks();
    let history = &chunks.list[0].history;

    let update_by_id = Id::new(1, 109);
    let mut update_by_original = test_cell(update_by_id, 10);
    crate::ram::history::take_revision_node_allocations_for_test();
    history.take_chain_map_resolutions_for_test();
    history.take_direct_path_activity_for_test();
    chunks.write_cell(&mut update_by_original).unwrap();

    let chunk = &chunks.list[0];
    let old_location = chunks.address_of(&update_by_id);
    let old_segment = chunk.locate_segment(old_location).unwrap();
    let (old_entry, ()) = crate::ram::entry::Entry::decode_from(old_location, |_, _| ());
    let old_entry_size = old_entry.content_length + crate::ram::entry::ENTRY_HEAD_SIZE as u32;
    let dead_before = old_segment.dead_space.load(Ordering::Acquire);

    let mut replacement = test_cell(update_by_id, 11);
    chunks.update_cell(&mut replacement).unwrap();

    assert_eq!(
        old_segment.dead_space.load(Ordering::Acquire) - dead_before,
        old_entry_size
    );
    let word = chunk
        .cell_index
        .get_from_mutex(&(update_by_id.lower as usize))
        .expect("present direct head");
    assert_eq!(word, chunks.address_of(&update_by_id));
    assert_eq!(word & 0b111, 0);

    chunks
        .update_cell_by(&update_by_id, |current| {
            let mut updated = current.to_owned();
            updated.data["score"] = OwnedValue::U64(12);
            Some(updated)
        })
        .unwrap();

    let guarded_id = Id::new(1, 110);
    let mut guarded_original = test_cell(guarded_id, 20);
    chunks.write_cell(&mut guarded_original).unwrap();
    let mut guarded_update = test_cell(guarded_id, 21);
    let mut guard = chunks.lock_cell_for_write(&guarded_id, true).unwrap();
    guard.update_cell(&mut guarded_update).unwrap();
    drop(guard);

    let upsert_id = Id::new(1, 111);
    let mut upsert_original = test_cell(upsert_id, 30);
    chunks.write_cell(&mut upsert_original).unwrap();
    let mut upsert_update = test_cell(upsert_id, 31);
    chunks.upsert_cell(&mut upsert_update).unwrap();

    let guarded_upsert_id = Id::new(1, 112);
    let mut guarded_upsert_original = test_cell(guarded_upsert_id, 40);
    chunks.write_cell(&mut guarded_upsert_original).unwrap();
    let mut guarded_upsert_update = test_cell(guarded_upsert_id, 41);
    let mut guard = chunks
        .lock_cell_for_write(&guarded_upsert_id, true)
        .unwrap();
    guard.upsert_cell(&mut guarded_upsert_update).unwrap();
    drop(guard);

    let empty_upsert_id = Id::new(1, 113);
    let mut empty_upsert = test_cell(empty_upsert_id, 50);
    chunks.upsert_cell(&mut empty_upsert).unwrap();

    let conditional_update_id = Id::new(1, 114);
    let mut conditional_update_original = test_cell(conditional_update_id, 60);
    let conditional_update_revision = chunks
        .write_cell(&mut conditional_update_original)
        .unwrap()
        .revision_ts;
    let mut conditional_update = test_cell(conditional_update_id, 61);
    chunks
        .compare_revision_and_update_cell(
            &conditional_update_id,
            conditional_update_revision,
            &mut conditional_update,
        )
        .unwrap();

    let conditional_field_id = Id::new(1, 115);
    let mut conditional_field_original = test_cell(conditional_field_id, 70);
    let conditional_field_revision = chunks
        .write_cell(&mut conditional_field_original)
        .unwrap()
        .revision_ts;
    chunks
        .compare_revision_and_set_field(
            &conditional_field_id,
            conditional_field_revision,
            hash_str("score"),
            OwnedValue::U64(71),
        )
        .unwrap();

    assert_eq!(history.take_chain_map_resolutions_for_test(), 0);
    assert_eq!(
        crate::ram::history::take_revision_node_allocations_for_test(),
        0
    );
    assert_eq!(history.take_direct_path_activity_for_test(), (0, 0));
    for id in [
        update_by_id,
        guarded_id,
        upsert_id,
        guarded_upsert_id,
        empty_upsert_id,
        conditional_update_id,
        conditional_field_id,
    ] {
        assert_eq!(history.revision_count_for_test(&id), 0);
    }
}

#[test]
fn direct_update_keeps_absent_and_tombstoned_cells_absent() {
    let chunks = test_chunks();
    let tombstoned_id = Id::new(1, 107);
    let mut original = test_cell(tombstoned_id, 10);
    chunks.write_cell(&mut original).unwrap();
    chunks.remove_cell(&tombstoned_id).unwrap();

    let mut tombstoned_update = test_cell(tombstoned_id, 20);
    assert!(matches!(
        chunks.update_cell(&mut tombstoned_update),
        Err(WriteError::CellDoesNotExisted)
    ));
    assert!(matches!(
        chunks.read_cell(&tombstoned_id),
        Err(ReadError::CellDoesNotExisted)
    ));

    let absent_id = Id::new(1, 108);
    let mut absent_update = test_cell(absent_id, 30);
    assert!(matches!(
        chunks.update_cell(&mut absent_update),
        Err(WriteError::CellDoesNotExisted)
    ));
    assert!(matches!(
        chunks.read_cell_snapshot(&absent_id, u64::MAX).unwrap(),
        SnapshotRead::Absent(None)
    ));
}

#[test]
fn delete_and_recreate_preserve_revision_aware_absence() {
    let chunks = test_chunks();
    let id = Id::new(1, 92);
    let mut first = test_cell(id, 10);
    chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    chunks
        .remove_cell_at_revision(&id, RevisionWrite::committed(200))
        .unwrap();

    assert!(matches!(
        chunks.read_cell_snapshot(&id, 250).unwrap(),
        SnapshotRead::Absent(Some(200))
    ));

    let mut second = test_cell(id, 30);
    chunks
        .write_cell_at_revision(&mut second, RevisionWrite::committed(300))
        .unwrap();
    assert!(matches!(
        chunks.read_cell_snapshot(&id, 250).unwrap(),
        SnapshotRead::Absent(Some(200))
    ));
}

#[test]
fn every_snapshot_read_shape_selects_the_same_old_revision() {
    let chunks = test_chunks();
    let id = Id::new(1, 93);
    let mut first = test_cell(id, 10);
    chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    let mut second = test_cell(id, 20);
    chunks
        .update_cell_at_revision(&mut second, RevisionWrite::committed(200))
        .unwrap();

    let SnapshotRead::Present(full) = chunks.read_cell_snapshot(&id, 150).unwrap() else {
        panic!("full snapshot should select revision 100");
    };
    assert_eq!(full.header.revision_ts, 100);
    assert_eq!(full.data["score"].u64(), Some(&10));

    let SnapshotRead::Present(selected) = chunks
        .read_selected_snapshot(&id, 150, &[hash_str("score")])
        .unwrap()
    else {
        panic!("selected snapshot should select revision 100");
    };
    assert_eq!(selected.header.revision_ts, 100);
    assert_eq!(selected.data.uni_array().unwrap()[0].u64(), Some(&10));

    let SnapshotRead::Present(header) = chunks.head_snapshot(&id, 150).unwrap() else {
        panic!("header snapshot should select revision 100");
    };
    assert_eq!(header.revision_ts, 100);

    let SnapshotRead::Present(raw_revision) = chunks
        .read_partial_raw_snapshot(&id, 150, ENTRY_HEAD_SIZE, size_of::<u64>())
        .unwrap()
    else {
        panic!("partial snapshot should select revision 100");
    };
    assert_eq!(raw_revision, 100u64.to_le_bytes());
}

#[test]
fn partial_snapshot_ranges_are_bounded_to_current_entry() {
    let chunks = test_chunks();
    let id = Id::new(1, 105);
    let mut cell = test_cell(id, 10);
    let installed = chunks
        .write_cell_at_revision(&mut cell, RevisionWrite::committed(100))
        .unwrap();
    let entry_size = installed.node.entry_size as usize;

    assert!(matches!(
        chunks
            .read_partial_raw_snapshot(&id, 200, entry_size - 1, 1)
            .unwrap(),
        SnapshotRead::Present(bytes) if bytes.len() == 1
    ));
    assert!(matches!(
        chunks.read_partial_raw_snapshot(&id, 200, entry_size, 1),
        Err(ReadError::CellDoesNotExisted)
    ));
    assert!(matches!(
        chunks.read_partial_raw_snapshot(&id, 200, usize::MAX, 1),
        Err(ReadError::CellDoesNotExisted)
    ));
}

#[test]
fn partial_snapshot_ranges_are_bounded_to_historical_entry() {
    let chunks = test_chunks();
    let id = Id::new(1, 106);
    let mut first = test_cell(id, 10);
    let historical = chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    let historical_entry_size = historical.node.entry_size as usize;
    let mut second = test_cell(id, 20);
    chunks
        .update_cell_at_revision(&mut second, RevisionWrite::committed(200))
        .unwrap();

    assert!(matches!(
        chunks
            .read_partial_raw_snapshot(&id, 150, historical_entry_size - 1, 1)
            .unwrap(),
        SnapshotRead::Present(bytes) if bytes.len() == 1
    ));
    assert!(matches!(
        chunks.read_partial_raw_snapshot(&id, 150, historical_entry_size, 1),
        Err(ReadError::CellDoesNotExisted)
    ));
    assert!(matches!(
        chunks.read_partial_raw_snapshot(&id, 150, usize::MAX, 1),
        Err(ReadError::CellDoesNotExisted)
    ));
}

#[test]
fn failed_delete_preserves_current_cell_and_secondary_indices() {
    let fields = Field::new_schema(vec![
        Field::new_indexed("id", Type::I64, vec![IndexType::Hashed, IndexType::Null]),
        Field::new_unindexed("name", Type::String),
        Field::new_indexed("score", Type::U64, vec![IndexType::Ranged]),
    ]);
    let schema = Schema::new_with_id(1, "delete-atomicity", None, fields, false, true);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema);
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );
    let id = Id::new(1, 107);
    let mut cell = test_cell(id, 10);
    chunks
        .write_cell_at_revision(&mut cell, RevisionWrite::committed(100))
        .unwrap();
    let original_address = chunks.address_of(&id);
    let original_accounting = physical_accounting(&chunks);
    chunks.fail_next_allocation_for_test(&id);

    assert!(matches!(
        chunks.remove_cell_at_revision(&id, RevisionWrite::committed(200)),
        Err(WriteError::CannotAllocateSpace)
    ));
    assert_eq!(chunks.address_of(&id), original_address);
    let stored = chunks.read_cell(&id).unwrap();
    assert_eq!(stored.id(), id);
    assert_eq!(stored.data["score"].u64(), Some(&10));
    assert_eq!(
        chunks.secondary_index_removal_attempts_for_test(&id),
        0,
        "a failed delete must not schedule removal of any secondary index"
    );
    assert_eq!(physical_accounting(&chunks), original_accounting);
    assert!(chunks.list[0]
        .history
        .current(&id)
        .is_some_and(|node| node.revision_ts == 100));
}

#[test]
fn zero_assigned_insert_is_side_effect_free() {
    for write in [RevisionWrite::committed(0), RevisionWrite::pending(0)] {
        let chunks = test_chunks();
        let id = Id::new(1, 108);
        let mut cell = test_cell(id, 10);
        let before = physical_accounting(&chunks);

        assert_zero_write_rejected(chunks.write_cell_at_revision(&mut cell, write));

        assert_eq!(chunks.list[0].cell_count(), 0);
        assert!(chunks.list[0].history.current(&id).is_none());
        assert_eq!(physical_accounting(&chunks), before);
        assert_eq!(cell.header.revision_ts, 0);
    }
}

#[test]
fn zero_assigned_update_is_side_effect_free() {
    for write in [RevisionWrite::committed(0), RevisionWrite::pending(0)] {
        let chunks = test_chunks();
        let id = Id::new(1, 109);
        let colliding_id = Id::new(2, id.lower);
        let mut first = test_cell(id, 10);
        chunks
            .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
            .unwrap();
        let original_address = chunks.address_of(&id);
        let original_accounting = physical_accounting(&chunks);
        let original_node = chunks.list[0].history.current(&id).unwrap();
        let mut update = test_cell(colliding_id, 20);

        assert_zero_write_rejected(chunks.update_cell_at_revision(&mut update, write));

        assert_eq!(chunks.address_of(&id), original_address);
        assert!(chunks.list[0].history.current(&colliding_id).is_none());
        assert!(chunks.list[0]
            .history
            .current(&id)
            .is_some_and(|node| Arc::ptr_eq(&node, &original_node)));
        assert_eq!(physical_accounting(&chunks), original_accounting);
        assert_eq!(update.header.revision_ts, 0);
    }
}

#[test]
fn zero_assigned_delete_is_side_effect_free() {
    for write in [RevisionWrite::committed(0), RevisionWrite::pending(0)] {
        let chunks = test_chunks();
        let id = Id::new(1, 110);
        let colliding_id = Id::new(2, id.lower);
        let mut cell = test_cell(id, 10);
        chunks
            .write_cell_at_revision(&mut cell, RevisionWrite::committed(100))
            .unwrap();
        let original_address = chunks.address_of(&id);
        let original_accounting = physical_accounting(&chunks);
        let original_node = chunks.list[0].history.current(&id).unwrap();

        assert_zero_write_rejected(chunks.remove_cell_at_revision(&colliding_id, write));

        assert_eq!(chunks.address_of(&id), original_address);
        assert!(chunks.list[0].history.current(&colliding_id).is_none());
        assert!(chunks.list[0]
            .history
            .current(&id)
            .is_some_and(|node| Arc::ptr_eq(&node, &original_node)));
        assert_eq!(physical_accounting(&chunks), original_accounting);
    }
}

#[test]
fn assigned_update_rejects_full_id_collision_without_side_effects() {
    let chunks = test_chunks();
    let original_id = Id::new(1, 111);
    let colliding_id = Id::new(2, original_id.lower);
    let mut original = test_cell(original_id, 10);
    let installed = chunks
        .write_cell_at_revision(&mut original, RevisionWrite::committed(100))
        .unwrap();
    let original_address = chunks.address_of(&original_id);
    let original_accounting = physical_accounting(&chunks);
    let mut colliding = test_cell(colliding_id, 20);

    assert!(matches!(
        chunks.update_cell_at_revision(&mut colliding, RevisionWrite::committed(200)),
        Err(WriteError::CellDoesNotExisted)
    ));
    assert_eq!(chunks.address_of(&original_id), original_address);
    assert!(chunks.list[0].history.current(&colliding_id).is_none());
    assert!(chunks.list[0]
        .history
        .current(&original_id)
        .is_some_and(|node| Arc::ptr_eq(&node, &installed.node)));
    assert_eq!(physical_accounting(&chunks), original_accounting);
    let stored = chunks.read_cell(&original_id).unwrap();
    assert_eq!(stored.id(), original_id);
    assert_eq!(stored.data["score"].u64(), Some(&10));
}

#[test]
fn assigned_delete_rejects_full_id_collision_without_side_effects() {
    let chunks = test_chunks();
    let original_id = Id::new(1, 112);
    let colliding_id = Id::new(2, original_id.lower);
    let mut original = test_cell(original_id, 10);
    let installed = chunks
        .write_cell_at_revision(&mut original, RevisionWrite::committed(100))
        .unwrap();
    let original_address = chunks.address_of(&original_id);
    let original_accounting = physical_accounting(&chunks);

    assert!(matches!(
        chunks.remove_cell_at_revision(&colliding_id, RevisionWrite::committed(200)),
        Err(WriteError::CellDoesNotExisted)
    ));
    assert_eq!(chunks.address_of(&original_id), original_address);
    assert!(chunks.list[0].history.current(&colliding_id).is_none());
    assert!(chunks.list[0]
        .history
        .current(&original_id)
        .is_some_and(|node| Arc::ptr_eq(&node, &installed.node)));
    assert_eq!(physical_accounting(&chunks), original_accounting);
    let stored = chunks.read_cell(&original_id).unwrap();
    assert_eq!(stored.id(), original_id);
    assert_eq!(stored.data["score"].u64(), Some(&10));
}

#[test]
fn pending_revision_waits_below_boundary_then_promotes_without_moving() {
    let chunks = test_chunks();
    let id = Id::new(1, 94);
    let mut cell = test_cell(id, 10);
    let installed = chunks
        .write_cell_at_revision(&mut cell, RevisionWrite::pending(100))
        .unwrap();
    let before = installed.node.load();

    assert!(matches!(
        chunks.read_cell_snapshot(&id, 100).unwrap(),
        SnapshotRead::Absent(None)
    ));
    assert!(matches!(
        chunks.read_cell_snapshot(&id, 101).unwrap(),
        SnapshotRead::Wait
    ));

    chunks.promote_revision(&installed).unwrap();
    let after = installed.node.load();
    assert_eq!(installed.node.revision_ts, 100);
    assert_eq!(before.1, after.1);
    assert!(matches!(
        chunks.read_cell_snapshot(&id, 101).unwrap(),
        SnapshotRead::Present(_)
    ));
    assert_eq!(
        chunks.promote_revision(&installed),
        Err(WriteError::CellRevisionMismatch)
    );
}

#[test]
fn abort_pending_update_skips_it_without_changing_identity() {
    let chunks = test_chunks();
    let id = Id::new(1, 95);
    let mut first = test_cell(id, 10);
    chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    let mut second = test_cell(id, 20);
    let installed = chunks
        .update_cell_at_revision(&mut second, RevisionWrite::pending(200))
        .unwrap();
    let before = installed.node.load();

    assert!(matches!(
        chunks.read_cell_snapshot(&id, 300).unwrap(),
        SnapshotRead::Wait
    ));
    chunks.abort_revision(&installed).unwrap();
    let after = installed.node.load();
    assert_eq!(installed.node.revision_ts, 200);
    assert_eq!(before.1, after.1);

    let SnapshotRead::Present(cell) = chunks.read_cell_snapshot(&id, 300).unwrap() else {
        panic!("aborted revision should be skipped");
    };
    assert_eq!(cell.header.revision_ts, 100);
    assert_eq!(cell.data["score"].u64(), Some(&10));
    assert_eq!(
        chunks.abort_revision(&installed),
        Err(WriteError::CellRevisionMismatch)
    );
    assert_eq!(
        chunks.promote_revision(&installed),
        Err(WriteError::CellRevisionMismatch)
    );
}

#[test]
fn pending_delete_promotes_to_revision_aware_absence() {
    let chunks = test_chunks();
    let id = Id::new(1, 96);
    let mut cell = test_cell(id, 10);
    chunks
        .write_cell_at_revision(&mut cell, RevisionWrite::committed(100))
        .unwrap();
    let installed = chunks
        .remove_cell_at_revision(&id, RevisionWrite::pending(200))
        .unwrap();

    assert!(matches!(
        chunks.read_cell_snapshot(&id, 300).unwrap(),
        SnapshotRead::Wait
    ));
    chunks.promote_revision(&installed).unwrap();
    assert!(matches!(
        chunks.read_cell_snapshot(&id, 300).unwrap(),
        SnapshotRead::Absent(Some(200))
    ));
}

#[test]
fn non_increasing_assigned_revisions_are_rejected() {
    let chunks = test_chunks();
    let id = Id::new(1, 97);
    let mut first = test_cell(id, 10);
    chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();

    let mut equal_update = test_cell(id, 20);
    assert!(matches!(
        chunks.update_cell_at_revision(&mut equal_update, RevisionWrite::committed(100)),
        Err(WriteError::CellRevisionMismatch)
    ));
    assert!(matches!(
        chunks.remove_cell_at_revision(&id, RevisionWrite::committed(99)),
        Err(WriteError::CellRevisionMismatch)
    ));

    chunks
        .remove_cell_at_revision(&id, RevisionWrite::committed(200))
        .unwrap();
    let mut equal_recreate = test_cell(id, 30);
    assert!(matches!(
        chunks.write_cell_at_revision(&mut equal_recreate, RevisionWrite::committed(200)),
        Err(WriteError::CellRevisionMismatch)
    ));
}

#[test]
fn expired_point_history_maps_to_snapshot_too_old() {
    let chunks = test_chunks();
    let id = Id::new(1, 104);
    let mut first = test_cell(id, 10);
    chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    let mut second = test_cell(id, 20);
    chunks
        .update_cell_at_revision(&mut second, RevisionWrite::committed(200))
        .unwrap();
    chunks.list[0].history.expire_due_for_test(u64::MAX);

    assert!(matches!(
        chunks.read_cell_snapshot(&id, 150),
        Err(ReadError::SnapshotTooOld)
    ));
}

#[test]
fn insert_is_rejected_over_a_present_logical_head() {
    let chunks = test_chunks();
    let id = Id::new(1, 98);
    let mut first = test_cell(id, 10);
    chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    let mut second = test_cell(id, 20);

    assert!(matches!(
        chunks.write_cell_at_revision(&mut second, RevisionWrite::committed(200)),
        Err(WriteError::CellAlreadyExisted)
    ));
}

#[test]
fn legacy_update_by_publishes_the_new_current_revision() {
    let chunks = test_chunks();
    let id = Id::new(1, 99);
    let mut first = test_cell(id, 10);
    chunks.write_cell(&mut first).unwrap();
    let updated = chunks
        .update_cell_by(&id, |cell| {
            let mut updated = cell.to_owned();
            updated.data["score"] = OwnedValue::U64(20);
            Some(updated)
        })
        .unwrap();

    let SnapshotRead::Present(selected) = chunks.read_cell_snapshot(&id, u64::MAX).unwrap() else {
        panic!("legacy update-by should publish a present head");
    };
    assert_eq!(selected.header.revision_ts, updated.header.revision_ts);
    assert_eq!(selected.data["score"].u64(), Some(&20));
}

#[test]
fn legacy_upsert_and_guard_update_publish_new_current_revisions() {
    let chunks = test_chunks();
    let id = Id::new(1, 100);
    let mut first = test_cell(id, 10);
    chunks.upsert_cell(&mut first).unwrap();

    let mut second = test_cell(id, 20);
    chunks.upsert_cell(&mut second).unwrap();
    let mut third = test_cell(id, 30);
    chunks
        .lock_cell_for_write(&id, true)
        .unwrap()
        .update_cell(&mut third)
        .unwrap();

    let SnapshotRead::Present(selected) = chunks.read_cell_snapshot(&id, u64::MAX).unwrap() else {
        panic!("legacy upsert/guard update should publish a present head");
    };
    assert_eq!(selected.header.revision_ts, third.header.revision_ts);
    assert_eq!(selected.data["score"].u64(), Some(&30));
}

#[test]
fn legacy_guard_upsert_of_empty_slot_publishes_history() {
    let chunks = test_chunks();
    let id = Id::new(1, 101);
    let mut cell = test_cell(id, 10);
    chunks.list[0]
        .lock_or_insert_cell(id.lower)
        .upsert_cell(&mut cell)
        .unwrap();

    assert!(matches!(
        chunks.read_cell_snapshot(&id, u64::MAX).unwrap(),
        SnapshotRead::Present(_)
    ));
}

#[test]
fn legacy_remove_paths_publish_revision_aware_tombstones() {
    let chunks = test_chunks();
    let direct_id = Id::new(1, 102);
    let mut direct = test_cell(direct_id, 10);
    chunks.write_cell(&mut direct).unwrap();
    chunks.remove_cell(&direct_id).unwrap();
    assert!(matches!(
        chunks.read_cell_snapshot(&direct_id, u64::MAX).unwrap(),
        SnapshotRead::Absent(Some(_))
    ));

    let predicted_id = Id::new(1, 103);
    let mut predicted = test_cell(predicted_id, 20);
    chunks.write_cell(&mut predicted).unwrap();
    chunks.remove_cell_by(&predicted_id, |_| true).unwrap();
    assert!(matches!(
        chunks.read_cell_snapshot(&predicted_id, u64::MAX).unwrap(),
        SnapshotRead::Absent(Some(_))
    ));
}

#[test]
pub fn cell_rw() {
    let fields = default_fields();
    let id1 = Id::new(1, 1);
    let id2 = Id::new(1, 2);
    let schema = Schema::new_with_id(1, &String::from("dummy"), None, fields, false, false);
    let mut data = data_map_value! {
        id: 100 as i64,
        score: 70 as u64,
        name: String::from("Jack")
    };
    let chunk = &Chunks::new_dummy(1, CHUNK_SIZE).list[0];
    chunk.meta.schemas.debug_only_new_schema(schema.clone());
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    let write_plan = cell.plan_write(chunk).unwrap();
    let pending_entry = write_plan.allocate(chunk, true).unwrap();
    let write_result = chunk
        .write_cell_to_chunk(&cell, &write_plan, &pending_entry, cell.header.revision_ts)
        .unwrap();
    let cell_1_ptr = write_result.addr;
    pending_entry.finish().unwrap();
    {
        let (stored_cell, _) =
            SharedCellData::from_chunk_raw(id1.lower, cell_1_ptr, &chunk).unwrap();
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
    }
    data = data_map_value! {
        id: 2 as i64,
        score: 80 as u64,
        name: "John"
    };
    cell = OwnedCell {
        header: CellHeader::new(schema.id, &id2),
        data,
    };
    let write_plan = cell.plan_write(chunk).unwrap();
    let pending_entry = write_plan.allocate(chunk, true).unwrap();
    let write_result = chunk
        .write_cell_to_chunk(&cell, &write_plan, &pending_entry, cell.header.revision_ts)
        .unwrap();
    let cell_2_ptr = write_result.addr;
    pending_entry.finish().unwrap();
    {
        let stored_cell = SharedCellData::from_chunk_raw(id2.lower, cell_2_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &2);
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &80);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "John");
    }
    {
        let stored_cell = SharedCellData::from_chunk_raw(id1.lower, cell_1_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
    }
}

#[cfg(feature = "fast_map")]
#[test]
pub fn dynamic() {
    let fields = default_fields();
    let id1 = Id::new(1, 1);
    let id2 = Id::new(1, 2);
    let schema = Schema::new_with_id(1, &String::from("dummy"), None, fields, true, true);
    let mut data_map = types::OwnedMap::new();
    data_map.insert("id", OwnedValue::I64(100));
    data_map.insert("score", OwnedValue::U64(70));
    data_map.insert("name", OwnedValue::String(String::from("Jack")));
    data_map.insert("year", OwnedValue::U16(2010));
    data_map.insert("major", OwnedValue::String(String::from("CS")));
    let mut data = OwnedValue::Map(data_map);
    let chunk = &Chunks::new_dummy(1, CHUNK_SIZE).list[0];
    chunk.meta.schemas.new_schema(schema.clone());
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    let mut loc = chunk.write_cell_to_chunk(&mut cell);
    let cell_1_ptr = loc.unwrap().0;
    {
        let stored_cell = SharedCellData::from_chunk_raw(cell_1_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
        assert_eq!(stored_cell.data["year"].u16().unwrap(), &2010);
        assert_eq!(stored_cell.data["major"].string().unwrap(), "CS");
    }

    data_map = types::OwnedMap::new();
    data_map.insert("id", OwnedValue::I64(2));
    data_map.insert("score", OwnedValue::U64(80));
    data_map.insert("name", OwnedValue::String(String::from("John")));
    data = OwnedValue::Map(data_map);
    cell = OwnedCell {
        header: CellHeader::new(schema.id, &id2),
        data,
    };
    loc = chunk.write_cell_to_chunk(&mut cell);
    let cell_2_ptr = loc.unwrap().0;
    {
        let stored_cell = SharedCellData::from_chunk_raw(cell_2_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &2);
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &80);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "John");
        assert!(stored_cell.data["major"].string().is_none());
    }
}
