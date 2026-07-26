use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::entry::ENTRY_HEAD_SIZE;
use crate::ram::schema::*;
use crate::ram::types;
use crate::ram::types::*;
use crate::server::ServerMeta;
use bifrost_hasher::hash_str;
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
