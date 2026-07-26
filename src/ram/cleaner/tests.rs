use crate::dovahkiin::types::Map;
use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::cleaner::combine;
use crate::ram::entry::{EntryContent, EntryType};
use crate::ram::file_manager::SegmentFileManager;
use crate::ram::schema::Field;
use crate::ram::schema::*;
use crate::ram::segs::{SegmentAllocator, SegmentReferenceGuard, SEGMENT_SIZE};
use crate::ram::types::*;
use crate::server::ServerMeta;
use lightning::map::Map as LightningMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex as StdMutex};
use std::thread;
use std::time::Duration;

pub const DATA_SIZE: usize = 1000 * 1024; // nearly 1MB
const MAX_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

fn default_cell(id: &Id) -> OwnedCell {
    let data: Vec<_> = std::iter::repeat(id.lower as u8).take(DATA_SIZE).collect();
    OwnedCell {
        header: CellHeader::new(0, id),
        data: data_map_value!(id: id.lower as i32, data: data),
    }
}

fn default_fields() -> Field {
    Field::new_schema(vec![
        Field::new_unindexed("id", Type::I32),
        Field::new_unindexed_array("data", Type::U8),
    ])
}

fn retained_revision_chunks(segment_count: usize) -> (Arc<Chunks>, Schema) {
    let schema = Schema::new(
        "cleaner_retained_revision_test",
        None,
        default_fields(),
        false,
        false,
    );
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        MAX_SEGMENT_SIZE * segment_count,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );
    (chunks, schema)
}

fn revision_cell(schema_id: u32, id: &Id, value: u8) -> OwnedCell {
    OwnedCell {
        header: CellHeader::new(schema_id, id),
        data: data_map_value!(
            id: value as i32,
            data: vec![value; 64]
        ),
    }
}

fn force_next_write_to_new_segment(chunk: &crate::ram::chunk::Chunk) {
    let head = chunk
        .segs
        .get(&(chunk.get_head_seg_id() as usize))
        .expect("active regular head");
    head.append_header.store(head.bound, Ordering::Release);
}

fn install_current_only_cell(
    chunks: &Chunks,
    schema_id: u32,
    id: &Id,
    revision_ts: u64,
    value: u8,
) -> (usize, u32) {
    let chunk = &chunks.list[0];
    let stored = write_physical_cell(chunks, schema_id, id, revision_ts, value);
    let mut mirror = chunk
        .cell_index
        .try_insert_locked(id.lower as usize)
        .expect("unused current mirror");
    *mirror = stored.0;
    stored
}

fn write_physical_cell(
    chunks: &Chunks,
    schema_id: u32,
    id: &Id,
    revision_ts: u64,
    value: u8,
) -> (usize, u32) {
    let chunk = &chunks.list[0];
    let cell = revision_cell(schema_id, id, value);
    let write_plan = cell.plan_write(chunk).unwrap();
    let entry_size = write_plan.total_size();
    let pending_entry = write_plan.allocate(chunk, true).unwrap();
    let write_result = chunk
        .write_cell_to_chunk(&cell, &write_plan, &pending_entry, revision_ts)
        .unwrap();
    drop(pending_entry);
    (write_result.addr, entry_size)
}

fn assert_relocated_revision(
    chunks: &Chunks,
    id: &Id,
    revision_ts: u64,
    old_location: usize,
) -> usize {
    let location = chunks
        .history_location(id, revision_ts)
        .expect("retained revision location");
    assert_ne!(
        location, old_location,
        "retained revision {revision_ts} was left in a reclaimed source segment"
    );
    assert!(
        chunks.list[0].locate_segment(location).is_some(),
        "retained revision {revision_ts} must point into a published destination segment"
    );
    location
}

#[test]
fn combine_relocates_a_current_only_cell_without_a_history_node() {
    let (chunks, schema) = retained_revision_chunks(5);
    let chunk = &chunks.list[0];
    let id = Id::new(70, 8_901);
    let (source, _) = install_current_only_cell(&chunks, schema.id, &id, 100, 10);
    force_next_write_to_new_segment(chunk);

    let filler_id = Id::new(70, 8_902);
    install_current_only_cell(&chunks, schema.id, &filler_id, 110, 11);
    force_next_write_to_new_segment(chunk);

    assert_eq!(
        chunks.history_location(&id, 100),
        None,
        "setup must exercise a current-index-only cell"
    );
    let selected = chunk.segments();
    assert_eq!(selected.len(), 2);
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);

    let (_, reduced) = combine::CombinedCleaner::combine_segments(chunk, &selected);

    assert_eq!(reduced, 1);
    let destination = chunks.address_of(&id);
    assert_ne!(
        destination, source,
        "the current mirror must publish the copied destination"
    );
    let destination_segment = chunk
        .locate_segment(destination)
        .expect("published destination segment");
    assert_eq!(
        chunks.read_cell(&id).unwrap().to_owned().data["id"].i32(),
        Some(&10)
    );

    chunk
        .head_seg_id
        .store(destination_segment.id, Ordering::Release);
    let mut successor = revision_cell(schema.id, &id, 20);
    chunks
        .update_cell_at_revision(&mut successor, RevisionWrite::committed(200))
        .unwrap();
    let SnapshotRead::Present(predecessor) = chunks.read_cell_snapshot(&id, 150).unwrap() else {
        panic!("the relocated current-only predecessor must become snapshot-readable");
    };
    assert_eq!(predecessor.header.revision_ts, 100);
    assert_eq!(predecessor.data["id"].i32(), Some(&10));
}

#[test]
fn current_only_lower_key_collision_does_not_move_another_full_id() {
    let (chunks, schema) = retained_revision_chunks(6);
    let chunk = &chunks.list[0];
    let source_id = Id::new(80, 8_911);
    let (source, source_size) = install_current_only_cell(&chunks, schema.id, &source_id, 100, 10);
    force_next_write_to_new_segment(chunk);

    let filler_id = Id::new(80, 8_912);
    install_current_only_cell(&chunks, schema.id, &filler_id, 110, 11);
    force_next_write_to_new_segment(chunk);
    let selected = chunk.segments();
    assert_eq!(selected.len(), 2);

    let colliding_id = Id::new(81, source_id.lower);
    let (colliding_location, _) = write_physical_cell(&chunks, schema.id, &colliding_id, 200, 20);
    let changed = AtomicBool::new(false);
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);

    let (_, reduced) = combine::CombinedCleaner::combine_segments_with_relocation_hook(
        chunk,
        &selected,
        |revision_id, revision_ts, _, _| {
            if revision_id == source_id
                && revision_ts == 100
                && !changed.swap(true, Ordering::AcqRel)
            {
                *chunk
                    .cell_index
                    .lock(source_id.lower as usize)
                    .expect("current mirror") = colliding_location;
            }
        },
    );

    assert_eq!(reduced, 1);
    assert!(changed.load(Ordering::Acquire));
    assert_eq!(chunks.address_of(&colliding_id), colliding_location);
    assert_eq!(
        chunks.read_cell(&colliding_id).unwrap().to_owned().data["id"].i32(),
        Some(&20)
    );
    assert!(chunk.locate_segment(source).is_none());
    let destination = chunk
        .segments()
        .into_iter()
        .find(|segment| segment.id != chunk.locate_segment(colliding_location).unwrap().id)
        .expect("combined destination");
    assert_eq!(
        destination.dead_space.load(Ordering::Acquire),
        source_size,
        "the unpublished colliding copy must be dead-accounted once"
    );
}

#[test]
fn current_only_mirror_change_keeps_the_successor_current() {
    let (chunks, schema) = retained_revision_chunks(6);
    let chunk = &chunks.list[0];
    let id = Id::new(82, 8_921);
    let (source, source_size) = install_current_only_cell(&chunks, schema.id, &id, 100, 10);
    force_next_write_to_new_segment(chunk);

    let filler_id = Id::new(82, 8_922);
    install_current_only_cell(&chunks, schema.id, &filler_id, 110, 11);
    force_next_write_to_new_segment(chunk);
    let selected = chunk.segments();
    assert_eq!(selected.len(), 2);

    let (successor, _) = write_physical_cell(&chunks, schema.id, &id, 200, 20);
    let changed = AtomicBool::new(false);
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);

    let (_, reduced) = combine::CombinedCleaner::combine_segments_with_relocation_hook(
        chunk,
        &selected,
        |revision_id, revision_ts, _, _| {
            if revision_id == id && revision_ts == 100 && !changed.swap(true, Ordering::AcqRel) {
                *chunk
                    .cell_index
                    .lock(id.lower as usize)
                    .expect("current mirror") = successor;
            }
        },
    );

    assert_eq!(reduced, 1);
    assert!(changed.load(Ordering::Acquire));
    assert_eq!(chunks.address_of(&id), successor);
    assert_eq!(
        chunks.read_cell(&id).unwrap().to_owned().data["id"].i32(),
        Some(&20)
    );
    assert!(chunk.locate_segment(source).is_none());
    let destination = chunk
        .segments()
        .into_iter()
        .find(|segment| segment.id != chunk.locate_segment(successor).unwrap().id)
        .expect("combined destination");
    assert_eq!(destination.dead_space.load(Ordering::Acquire), source_size);
}

#[test]
fn current_only_mirror_change_into_another_source_retains_sources() {
    let (chunks, schema) = retained_revision_chunks(5);
    let chunk = &chunks.list[0];
    let id = Id::new(86, 8_941);
    let (source, source_size) = install_current_only_cell(&chunks, schema.id, &id, 100, 10);
    force_next_write_to_new_segment(chunk);
    let (successor, _) = write_physical_cell(&chunks, schema.id, &id, 200, 20);
    force_next_write_to_new_segment(chunk);

    let selected = chunk.segments();
    let selected_ids: HashSet<_> = selected.iter().map(|segment| segment.id).collect();
    assert_eq!(selected.len(), 2);
    let changed = AtomicBool::new(false);
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);

    let result = combine::CombinedCleaner::combine_segments_with_relocation_hook(
        chunk,
        &selected,
        |revision_id, revision_ts, _, _| {
            if revision_id == id && revision_ts == 100 && !changed.swap(true, Ordering::AcqRel) {
                *chunk
                    .cell_index
                    .lock(id.lower as usize)
                    .expect("current mirror") = successor;
            }
        },
    );

    assert_eq!(
        result,
        (0, 0),
        "a changed mirror inside the cleanup set must retain every source"
    );
    assert!(changed.load(Ordering::Acquire));
    assert!(
        selected
            .iter()
            .all(|segment| chunk.contains_seg(segment.id)),
        "the current successor source must remain registered"
    );
    assert_eq!(chunks.address_of(&id), successor);
    assert_eq!(
        chunks.read_cell(&id).unwrap().to_owned().data["id"].i32(),
        Some(&20)
    );
    assert!(chunk.locate_segment(source).is_some());
    let destination = chunk
        .segments()
        .into_iter()
        .find(|segment| !selected_ids.contains(&segment.id))
        .expect("unpublished destination remains registered");
    assert_eq!(
        destination.dead_space.load(Ordering::Acquire),
        source_size,
        "the unused copy must be dead-accounted exactly once"
    );
}

#[test]
fn unresolved_current_only_mirror_retains_the_readable_source() {
    let (chunks, schema) = retained_revision_chunks(6);
    let chunk = &chunks.list[0];
    let id = Id::new(83, 8_931);
    let (source, source_size) = install_current_only_cell(&chunks, schema.id, &id, 100, 10);
    force_next_write_to_new_segment(chunk);

    let filler_id = Id::new(83, 8_932);
    install_current_only_cell(&chunks, schema.id, &filler_id, 110, 11);
    force_next_write_to_new_segment(chunk);
    let selected = chunk.segments();
    let source_ids: HashSet<_> = selected.iter().map(|segment| segment.id).collect();
    let unresolved = AtomicBool::new(false);
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);

    let result = combine::CombinedCleaner::combine_segments_with_relocation_hook(
        chunk,
        &selected,
        |revision_id, revision_ts, _, _| {
            if revision_id == id && revision_ts == 100 && !unresolved.swap(true, Ordering::AcqRel) {
                *chunk
                    .cell_index
                    .lock(id.lower as usize)
                    .expect("current mirror") = 0;
            }
        },
    );

    assert_eq!(result, (0, 0));
    assert!(unresolved.load(Ordering::Acquire));
    assert!(
        selected
            .iter()
            .all(|segment| chunk.contains_seg(segment.id)),
        "an unresolved mirror must suppress source reclamation"
    );
    assert_eq!(
        chunks.read_cell_at(&id, source).unwrap().data["id"].i32(),
        Some(&10)
    );
    let destination = chunk
        .segments()
        .into_iter()
        .find(|segment| !source_ids.contains(&segment.id))
        .expect("unpublished destination remains registered");
    assert_eq!(
        destination.dead_space.load(Ordering::Acquire),
        source_size,
        "the unresolved copied destination must be dead-accounted once"
    );

    *chunk
        .cell_index
        .lock(id.lower as usize)
        .expect("current mirror") = source;
    assert_eq!(
        chunks.read_cell(&id).unwrap().to_owned().data["id"].i32(),
        Some(&10)
    );
}

#[test]
fn combine_relocates_current_and_historical_revisions_for_one_id() {
    let (chunks, schema) = retained_revision_chunks(5);
    let chunk = &chunks.list[0];
    let id = Id::new(71, 9_001);
    let mut first = revision_cell(schema.id, &id, 10);
    let first = chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    let first_source = first.node.load().1;
    force_next_write_to_new_segment(chunk);

    let mut second = revision_cell(schema.id, &id, 20);
    let second = chunks
        .update_cell_at_revision(&mut second, RevisionWrite::committed(200))
        .unwrap();
    let second_source = second.node.load().1;
    force_next_write_to_new_segment(chunk);

    let selected = chunk.segments();
    assert_eq!(
        selected.len(),
        2,
        "setup should produce two source segments"
    );
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);

    let (_, reduced) = combine::CombinedCleaner::combine_segments(chunk, &selected);

    assert_eq!(reduced, 1);
    let first_destination = assert_relocated_revision(&chunks, &id, 100, first_source);
    let second_destination = assert_relocated_revision(&chunks, &id, 200, second_source);
    assert_ne!(first_destination, second_destination);
    assert_eq!(chunks.address_of(&id), second_destination);

    let SnapshotRead::Present(first_read) = chunks.read_cell_snapshot(&id, 150).unwrap() else {
        panic!("snapshot should materialize relocated revision 100");
    };
    assert_eq!(first_read.header.revision_ts, 100);
    assert_eq!(first_read.data["id"].i32(), Some(&10));

    let SnapshotRead::Present(second_read) = chunks.read_cell_snapshot(&id, 250).unwrap() else {
        panic!("snapshot should materialize relocated revision 200");
    };
    assert_eq!(second_read.header.revision_ts, 200);
    assert_eq!(second_read.data["id"].i32(), Some(&20));
}

#[test]
fn combine_skips_a_source_with_an_active_shared_lease() {
    let (chunks, schema) = retained_revision_chunks(5);
    let chunk = &chunks.list[0];
    let first_id = Id::new(72, 9_101);
    let mut first = revision_cell(schema.id, &first_id, 10);
    chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    force_next_write_to_new_segment(chunk);

    let second_id = Id::new(72, 9_102);
    let mut second = revision_cell(schema.id, &second_id, 20);
    chunks
        .write_cell_at_revision(&mut second, RevisionWrite::committed(200))
        .unwrap();
    force_next_write_to_new_segment(chunk);

    let selected = chunk.segments();
    assert_eq!(
        selected.len(),
        2,
        "setup should produce two source segments"
    );
    let leased_source = selected[0].clone();
    let lease = SegmentReferenceGuard::try_new(leased_source.clone()).expect("shared source lease");
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);

    assert_eq!(
        combine::CombinedCleaner::combine_segments(chunk, &selected),
        (0, 0)
    );
    assert!(
        chunk.contains_seg(leased_source.id),
        "a shared reader lease must prevent source reclamation"
    );

    drop(lease);
}

#[test]
fn combine_preserves_current_and_historical_tombstones_for_colliding_full_ids() {
    let (chunks, schema) = retained_revision_chunks(8);
    let chunk = &chunks.list[0];
    let first_id = Id::new(73, 9_201);
    let second_id = Id::new(74, first_id.lower);

    let mut first = revision_cell(schema.id, &first_id, 10);
    let first_present = chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    force_next_write_to_new_segment(chunk);
    let first_deleted = chunks
        .remove_cell_at_revision(&first_id, RevisionWrite::committed(200))
        .unwrap();
    force_next_write_to_new_segment(chunk);

    let mut second = revision_cell(schema.id, &second_id, 30);
    let second_present = chunks
        .write_cell_at_revision(&mut second, RevisionWrite::committed(300))
        .unwrap();
    force_next_write_to_new_segment(chunk);
    let second_deleted = chunks
        .remove_cell_at_revision(&second_id, RevisionWrite::committed(400))
        .unwrap();
    force_next_write_to_new_segment(chunk);
    let mut recreated = revision_cell(schema.id, &second_id, 50);
    let second_recreated = chunks
        .write_cell_at_revision(&mut recreated, RevisionWrite::committed(500))
        .unwrap();
    force_next_write_to_new_segment(chunk);

    let sources = [
        (first_id, 100, first_present.node.load().1),
        (first_id, 200, first_deleted.node.load().1),
        (second_id, 300, second_present.node.load().1),
        (second_id, 400, second_deleted.node.load().1),
        (second_id, 500, second_recreated.node.load().1),
    ];
    let selected = chunk.segments();
    assert_eq!(
        selected.len(),
        5,
        "setup should produce five source segments"
    );
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);

    let (_, reduced) = combine::CombinedCleaner::combine_segments(chunk, &selected);

    assert_eq!(reduced, 4);
    for (id, revision_ts, old_location) in sources {
        assert_relocated_revision(&chunks, &id, revision_ts, old_location);
    }

    assert!(matches!(
        chunks.read_cell_snapshot(&first_id, 250).unwrap(),
        SnapshotRead::Absent(Some(200))
    ));
    let SnapshotRead::Present(second_before_delete) =
        chunks.read_cell_snapshot(&second_id, 350).unwrap()
    else {
        panic!("snapshot should materialize colliding full ID revision 300");
    };
    assert_eq!(second_before_delete.data["id"].i32(), Some(&30));
    assert!(matches!(
        chunks.read_cell_snapshot(&second_id, 450).unwrap(),
        SnapshotRead::Absent(Some(400))
    ));
    let SnapshotRead::Present(second_after_recreate) =
        chunks.read_cell_snapshot(&second_id, 550).unwrap()
    else {
        panic!("snapshot should materialize recreated revision 500");
    };
    assert_eq!(second_after_recreate.data["id"].i32(), Some(&50));

    let stable_locations: Vec<_> = sources
        .iter()
        .map(|(id, revision_ts, _)| chunks.history_location(id, *revision_ts).unwrap())
        .collect();
    assert_eq!(
        combine::CombinedCleaner::combine_segments(chunk, &chunk.segments()),
        (0, 0)
    );
    let repeated_locations: Vec<_> = sources
        .iter()
        .map(|(id, revision_ts, _)| chunks.history_location(id, *revision_ts).unwrap())
        .collect();
    assert_eq!(repeated_locations, stable_locations);
}

#[test]
fn relocation_lost_to_expiration_marks_the_destination_dead_once() {
    let (chunks, schema) = retained_revision_chunks(5);
    let chunk = &chunks.list[0];
    let id = Id::new(75, 9_301);
    let mut first = revision_cell(schema.id, &id, 10);
    let first = chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    force_next_write_to_new_segment(chunk);
    let mut second = revision_cell(schema.id, &id, 20);
    chunks
        .update_cell_at_revision(&mut second, RevisionWrite::committed(200))
        .unwrap();
    force_next_write_to_new_segment(chunk);
    let selected = chunk.segments();
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);
    let expired = AtomicBool::new(false);

    let (_, reduced) = combine::CombinedCleaner::combine_segments_with_relocation_hook(
        chunk,
        &selected,
        |revision_id, revision_ts, _, _| {
            if revision_id == id && revision_ts == 100 && !expired.swap(true, Ordering::AcqRel) {
                chunk.history.expire_due_for_test(u64::MAX);
            }
        },
    );

    assert_eq!(reduced, 1);
    assert!(expired.load(Ordering::Acquire));
    let destination = chunk
        .segments()
        .into_iter()
        .next()
        .expect("combined destination");
    assert_eq!(
        destination.dead_space.load(Ordering::Acquire),
        first.node.entry_size,
        "the copied destination that lost relocation must be marked dead once"
    );
    chunk.drain_history_dead();
    chunk.drain_history_dead();
    assert_eq!(
        destination.dead_space.load(Ordering::Acquire),
        first.node.entry_size,
        "draining the expired source after reclamation must not double-account the destination"
    );
}

#[test]
fn unreconciled_current_mirror_keeps_source_and_retires_destination_copy() {
    let (chunks, schema) = retained_revision_chunks(5);
    let chunk = &chunks.list[0];
    let id = Id::new(77, 9_501);
    let mut first = revision_cell(schema.id, &id, 10);
    chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    force_next_write_to_new_segment(chunk);
    let mut second = revision_cell(schema.id, &id, 20);
    let second = chunks
        .update_cell_at_revision(&mut second, RevisionWrite::committed(200))
        .unwrap();
    let current_source = second.node.load().1;
    force_next_write_to_new_segment(chunk);
    let selected = chunk.segments();
    let source_ids: HashSet<_> = selected.iter().map(|segment| segment.id).collect();
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);
    let removed_mirror = AtomicBool::new(false);

    let result = combine::CombinedCleaner::combine_segments_with_relocation_hook(
        chunk,
        &selected,
        |revision_id, revision_ts, _, _| {
            if revision_id == id
                && revision_ts == 200
                && !removed_mirror.swap(true, Ordering::AcqRel)
            {
                chunk
                    .cell_index
                    .lock(id.lower as usize)
                    .expect("current mirror")
                    .remove();
            }
        },
    );

    assert_eq!(result, (0, 0));
    assert!(removed_mirror.load(Ordering::Acquire));
    assert!(
        selected.iter().all(|source| chunk.contains_seg(source.id)),
        "an unreconciled current mirror must suppress every source reclamation"
    );
    assert_eq!(
        chunks.history_location(&id, 200),
        Some(current_source),
        "failed mirror publication must roll the logical current back to its registered source"
    );
    let destination = chunk
        .segments()
        .into_iter()
        .find(|segment| !source_ids.contains(&segment.id))
        .expect("unpublished destination remains registered for dead accounting");
    assert_eq!(
        destination.dead_space.load(Ordering::Acquire),
        second.node.entry_size,
        "the rolled-back current destination must be accounted dead exactly once"
    );
    let SnapshotRead::Present(read) = chunks.read_cell_snapshot(&id, 250).unwrap() else {
        panic!("the source revision must remain readable after publication rollback");
    };
    assert_eq!(read.header.revision_ts, 200);
    assert_eq!(read.data["id"].i32(), Some(&20));
}

#[test]
fn historical_reader_retries_from_exclusive_source_to_relocated_destination() {
    let (chunks, schema) = retained_revision_chunks(5);
    let chunk = &chunks.list[0];
    let id = Id::new(76, 9_401);
    let mut first = revision_cell(schema.id, &id, 10);
    let first = chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    let first_source = first.node.load().1;
    force_next_write_to_new_segment(chunk);
    let mut second = revision_cell(schema.id, &id, 20);
    chunks
        .update_cell_at_revision(&mut second, RevisionWrite::committed(200))
        .unwrap();
    force_next_write_to_new_segment(chunk);
    let selected = chunk.segments();
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);

    let begin_read = Arc::new(Barrier::new(2));
    let (result_tx, result_rx) = mpsc::channel();
    let (lease_tx, lease_rx) = mpsc::channel();
    let lease_rx = StdMutex::new(lease_rx);
    chunk.set_snapshot_read_lease_hook_for_test(Some(Arc::new(
        move |read_id, location, acquired| {
            if read_id == id {
                lease_tx.send((location, acquired)).unwrap();
            }
        },
    )));
    let reader_chunks = chunks.clone();
    let reader_begin = begin_read.clone();
    let reader = thread::spawn(move || {
        reader_begin.wait();
        result_tx
            .send(reader_chunks.read_cell_snapshot(&id, 150))
            .unwrap();
    });
    let hook_started = AtomicBool::new(false);

    let (_, reduced) = combine::CombinedCleaner::combine_segments_with_relocation_hook(
        chunk,
        &selected,
        |revision_id, revision_ts, _, _| {
            if revision_id == id && revision_ts == 100 && !hook_started.swap(true, Ordering::AcqRel)
            {
                begin_read.wait();
                assert_eq!(
                    lease_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(2))
                        .expect("reader must attempt its source lease"),
                    (first_source, false),
                    "cleaner exclusivity must reject the source lease before relocation"
                );
            }
        },
    );

    assert_eq!(reduced, 1);
    let SnapshotRead::Present(read) = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("historical reader must finish after relocation")
        .unwrap()
    else {
        panic!("historical reader should return revision 100");
    };
    assert_eq!(read.header.revision_ts, 100);
    assert_eq!(read.data["id"].i32(), Some(&10));
    let relocated = chunks.history_location(&id, 100).unwrap();
    let destination_lease = loop {
        let event = lease_rx
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .expect("reader must retry its destination lease");
        if event.1 {
            break event;
        }
        assert_eq!(
            event,
            (first_source, false),
            "failed retries must remain pinned to the exclusive source"
        );
    };
    assert_eq!(destination_lease, (relocated, true));
    reader.join().unwrap();
    chunk.set_snapshot_read_lease_hook_for_test(None);
}

#[test]
fn historical_reader_lease_prevents_source_reclamation() {
    let (chunks, schema) = retained_revision_chunks(5);
    let chunk = &chunks.list[0];
    let id = Id::new(85, 9_701);
    let mut first = revision_cell(schema.id, &id, 10);
    let first = chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    let first_source = first.node.load().1;
    force_next_write_to_new_segment(chunk);
    let mut second = revision_cell(schema.id, &id, 20);
    chunks
        .update_cell_at_revision(&mut second, RevisionWrite::committed(200))
        .unwrap();
    force_next_write_to_new_segment(chunk);
    let selected = chunk.segments();
    chunk.head_seg_id.store(u64::MAX - 7, Ordering::Release);

    let lease_acquired = Arc::new(Barrier::new(2));
    let release_reader = Arc::new(Barrier::new(2));
    let hook_once = Arc::new(AtomicBool::new(false));
    let hook_acquired = lease_acquired.clone();
    let hook_release = release_reader.clone();
    let hook_once_flag = hook_once.clone();
    chunk.set_snapshot_read_lease_hook_for_test(Some(Arc::new(
        move |read_id, location, acquired| {
            if read_id == id
                && location == first_source
                && acquired
                && !hook_once_flag.swap(true, Ordering::AcqRel)
            {
                hook_acquired.wait();
                hook_release.wait();
            }
        },
    )));

    let reader_chunks = chunks.clone();
    let (result_tx, result_rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        result_tx
            .send(reader_chunks.read_cell_snapshot(&id, 150))
            .unwrap();
    });
    lease_acquired.wait();

    assert_eq!(
        combine::CombinedCleaner::combine_segments(chunk, &selected),
        (0, 0),
        "normal snapshot reader lease must reject cleaner exclusivity"
    );
    assert!(
        chunk.locate_segment(first_source).is_some(),
        "leased source must remain registered until materialization"
    );

    release_reader.wait();
    let SnapshotRead::Present(read) = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("reader must materialize from its leased source")
        .unwrap()
    else {
        panic!("historical source revision must remain present");
    };
    assert_eq!(read.header.revision_ts, 100);
    assert_eq!(read.data["id"].i32(), Some(&10));
    reader.join().unwrap();
    chunk.set_snapshot_read_lease_hook_for_test(None);
}

#[test]
fn assigned_writer_retries_after_history_relocation_and_wins_current() {
    let (chunks, schema) = retained_revision_chunks(7);
    let chunk = &chunks.list[0];
    let id = Id::new(84, 9_601);
    let mut first = revision_cell(schema.id, &id, 10);
    let first = chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    let first_source = first.node.load().1;
    force_next_write_to_new_segment(chunk);

    let mut second = revision_cell(schema.id, &id, 20);
    let second = chunks
        .update_cell_at_revision(&mut second, RevisionWrite::committed(200))
        .unwrap();
    let second_source = second.node.load().1;
    force_next_write_to_new_segment(chunk);
    let selected = chunk.segments();
    let selected_ids: HashSet<_> = selected.iter().map(|segment| segment.id).collect();
    assert_eq!(selected.len(), 2);

    // Allocate an unselected active head for the writer's successor. Cleaner
    // exclusivity prevents the writer from retaining or reading either source;
    // its normal CellGuard acquisition must fail and retry until the mirror is
    // reconciled to the destination.
    let writer_head_id = Id::new(84, 9_602);
    write_physical_cell(&chunks, schema.id, &writer_head_id, 250, 25);

    let begin_writer = Arc::new(Barrier::new(2));
    let writer_retry_sent = Arc::new(AtomicBool::new(false));
    let (writer_retry_tx, writer_retry_rx) = mpsc::channel();
    let writer_retry_rx = StdMutex::new(writer_retry_rx);
    let retry_sent = writer_retry_sent.clone();
    chunk.set_cell_guard_retry_hook_for_test(Some(Arc::new(move |hash| {
        if hash == id.lower && !retry_sent.swap(true, Ordering::AcqRel) {
            writer_retry_tx.send(hash).unwrap();
        }
    })));

    let writer_chunks = chunks.clone();
    let writer_begin = begin_writer.clone();
    let (writer_result_tx, writer_result_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        writer_begin.wait();
        let mut successor = revision_cell(schema.id, &id, 30);
        writer_result_tx
            .send(
                writer_chunks
                    .update_cell_at_revision(&mut successor, RevisionWrite::committed(300))
                    .map(|installed| (installed, successor)),
            )
            .unwrap();
    });

    let after_history = AtomicBool::new(false);
    let (_, reduced) = combine::CombinedCleaner::combine_segments_with_relocation_hooks(
        chunk,
        &selected,
        |_, _, _, _| {},
        |revision_id, revision_ts, _, _| {
            if revision_id == id
                && revision_ts == 200
                && !after_history.swap(true, Ordering::AcqRel)
            {
                begin_writer.wait();
                assert_eq!(
                    writer_retry_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(2))
                        .expect("writer must fail its exclusive-source CellGuard attempt"),
                    id.lower
                );
            }
        },
    );

    assert_eq!(reduced, 1);
    assert!(after_history.load(Ordering::Acquire));
    assert!(writer_retry_sent.load(Ordering::Acquire));
    let (_, successor) = writer_result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("writer must finish after cleaner reconciliation")
        .expect("assigned update must retry and publish its successor");
    writer.join().unwrap();
    chunk.set_cell_guard_retry_hook_for_test(None);

    assert_eq!(successor.header.revision_ts, 300);
    assert_eq!(
        chunks.address_of(&id),
        chunks.history_location(&id, 300).unwrap()
    );
    assert!(
        selected
            .iter()
            .all(|segment| !chunk.contains_seg(segment.id)),
        "both exclusive source segments must be reclaimed"
    );
    assert!(chunk.locate_segment(first_source).is_none());
    assert!(chunk.locate_segment(second_source).is_none());

    let moved_predecessor = chunks.history_location(&id, 200).unwrap();
    let predecessor_segment = chunk
        .locate_segment(moved_predecessor)
        .expect("moved predecessor destination");
    assert!(!selected_ids.contains(&predecessor_segment.id));
    assert_eq!(
        predecessor_segment.dead_space.load(Ordering::Acquire),
        0,
        "writer/cleaner publication must not dead-account the moved destination"
    );
    let SnapshotRead::Present(predecessor) = chunks.read_cell_snapshot(&id, 250).unwrap() else {
        panic!("moved predecessor must remain snapshot-readable");
    };
    assert_eq!(predecessor.header.revision_ts, 200);
    assert_eq!(predecessor.data["id"].i32(), Some(&20));
    assert_eq!(
        chunks.read_cell(&id).unwrap().to_owned().data["id"].i32(),
        Some(&30)
    );
}

#[test]
pub fn full_clean_cycle_without_compact() {
    let schema = Schema::new("cleaner_test", None, default_fields(), false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema);
    let chunks = Chunks::new(
        1,                    // single chunk
        MAX_SEGMENT_SIZE * 3, // chunk three segments
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );
    let chunk = &chunks.list[0];

    // provision test data
    {
        assert_eq!(chunk.segments().len(), 1);

        // put 16 cells to fill up all of those segments allocated
        for i in 0..16 {
            let mut cell = default_cell(&Id::new(0, i));
            chunks.write_cell(&mut cell).unwrap();
        }

        assert_eq!(chunk.segments().len(), 2);
        assert_eq!(chunk.cell_index.len(), 16);

        // delete half the cells to create tombstones and fragmentation
        for i in 0..8 {
            chunks.remove_cell(&Id::new(0, i * 2)).unwrap();
        }

        assert_eq!(chunk.segments().len(), 2);

        //count entries, including dead ones
        assert_eq!(chunk.segs.get(&0).unwrap().entry_iter().count(), 8); // all 8 cells
        assert_eq!(chunk.segs.get(&1).unwrap().entry_iter().count(), 16); // 8 cells and 8 tombstones
    }

    // integrity checks before cleaning
    let _ = chunk
        .live_entries(&chunk.segs.get(&0).unwrap())
        .collect::<Vec<_>>();
    let _ = chunk
        .live_entries(&chunk.segs.get(&1).unwrap())
        .collect::<Vec<_>>();

    // The first combine must retain every historical cell and therefore
    // cannot reduce two nearly-full source segments.
    {
        // Cleaner refuses to work on head segment; set head to dummy to include both segments
        chunk
            .head_seg_id
            .store(1234, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            combine::CombinedCleaner::combine_segments(chunk, &chunk.segments()),
            (0, 0)
        );
        assert_eq!(chunk.segments().len(), 2);
    }

    // Once the superseded cells expire, a repeated combine may reclaim them,
    // but the eight logical current tombstones still have to move with the
    // eight current present cells.
    chunk.history.expire_due_for_test(u64::MAX);
    chunk.drain_history_dead();
    {
        let (_, reduced) = combine::CombinedCleaner::combine_segments(chunk, &chunk.segments());
        assert_eq!(reduced, 1);
        let survival_cells: HashSet<_> = chunk
            .live_entries(&chunk.segments()[0])
            .map(|entry| {
                assert_eq!(entry.meta.entry_header.entry_type, EntryType::CELL);
                if let EntryContent::Cell(ref header) = entry.content {
                    return header.hash;
                } else {
                    panic!()
                }
            })
            .collect();
        assert_eq!(survival_cells.len(), 8);
        assert_eq!(chunk.segments().len(), 1);
        assert_eq!(
            chunk.segments()[0].entry_iter().count(),
            16,
            "current tombstones remain physical retained revisions"
        );
        (0..8)
            .map(|n| n as u64 * 2 + 1)
            .for_each(|hash| assert!(survival_cells.contains(&hash)));
    }

    // validate cells
    (0..8).map(|n| n * 2 + 1).for_each(|id| {
        let id = Id::new(0, id);
        let cell = chunks.read_cell(&id).unwrap();
        assert_eq!(cell.to_owned().data, default_cell(&id).data);
    });
}

#[test]
fn test_shrink_fully_utilized_segment() {
    let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 3);
    let file_manager = Arc::new(SegmentFileManager::new(None, None));

    let segment = allocator
        .alloc_seg(&file_manager)
        .expect("Failed to allocate segment");

    segment
        .append_header
        .store(segment.bound, Ordering::Relaxed);

    let used_size = segment.append_header.load(Ordering::Relaxed) - segment.addr;
    assert_eq!(used_size, SEGMENT_SIZE, "Segment should be fully utilized");

    segment.shrink(SEGMENT_SIZE);

    let used_size_after = segment.append_header.load(Ordering::Relaxed) - segment.addr;
    assert_eq!(
        used_size_after, SEGMENT_SIZE,
        "Segment should still be fully utilized"
    );
}

#[test]
fn test_shrink_larger_than_segment_size() {
    let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 3);
    let file_manager = Arc::new(SegmentFileManager::new(None, None));

    let segment = allocator
        .alloc_seg(&file_manager)
        .expect("Failed to allocate segment");

    segment.shrink(SEGMENT_SIZE + 1);

    let used_size = segment.append_header.load(Ordering::Relaxed) - segment.addr;
    assert_eq!(used_size, 0, "Segment should still be empty");
}
