use crate::ram::cell::{
    CellHeader, OwnedCell, WriteError, CELL_HEADER_SIZE, MAX_BLOB_CELL_SIZE, MAX_CELL_SIZE,
};
use crate::ram::chunk::Chunks;
use crate::ram::cleaner::combine;
use crate::ram::segs::SegmentClass;
use crate::ram::entry::ENTRY_HEAD_SIZE;
use crate::ram::schema::{Field, Schema};
use crate::ram::segs::SEGMENT_SIZE;
use crate::ram::types::{Bytes, Id, Map, OwnedMap, OwnedValue, Type};
use std::collections::BTreeSet;

use super::cell::CHUNK_SIZE;

const BYTES_LENGTH_PREFIX_SIZE: usize = std::mem::size_of::<u32>();
const BYTES_ALIGNMENT: usize = std::mem::align_of::<u32>();

fn bytes_cell(schema_id: u32, id: Id, payload_len: usize) -> OwnedCell {
    let mut map = OwnedMap::new();
    map.insert(
        "payload",
        OwnedValue::Bytes(Bytes::from_vec(vec![7_u8; payload_len])),
    );
    OwnedCell {
        header: CellHeader::new(schema_id, &id),
        data: OwnedValue::Map(map),
    }
}

fn bytes_schema(schema_id: u32, name: &str, blobs: bool) -> Schema {
    let schema = Schema::new_with_id(
        schema_id,
        name,
        None,
        Field::new_schema(vec![Field::new_unindexed("payload", Type::Bytes)]),
        false,
        false,
    );
    if blobs {
        schema.with_blobs(true)
    } else {
        schema
    }
}

fn written_segment_class(chunks: &Chunks, id: &Id) -> SegmentClass {
    let addr = {
        let loc = chunks.location_for_read(id).unwrap();
        *loc
    };
    let chunk = chunks.locate_chunk_by_partition(id.higher);
    chunk
        .locate_segment(addr)
        .unwrap()
        .segment_class()
}

fn written_segment_id(chunks: &Chunks, id: &Id) -> u64 {
    let chunk = chunks.locate_chunk_by_partition(id.higher);
    chunk.locate_segment(chunks.address_of(id)).unwrap().id
}

fn align_to(alignment: usize, value: usize) -> usize {
    let remainder = value % alignment;
    if remainder == 0 {
        value
    } else {
        value + alignment - remainder
    }
}

fn modeled_total_size(schema: &Schema, payload_len: usize) -> usize {
    let variable_offset = align_to(BYTES_ALIGNMENT, schema.static_bound);
    let stored_bytes_size = align_to(BYTES_ALIGNMENT, payload_len + BYTES_LENGTH_PREFIX_SIZE);
    ENTRY_HEAD_SIZE + align_to(8, CELL_HEADER_SIZE + variable_offset + stored_bytes_size)
}

fn boundary_payload_lengths(schema: &Schema, max_cell_size: u32) -> (usize, usize) {
    let limit = max_cell_size as usize;
    let mut accepted_payload_len = limit;
    while modeled_total_size(schema, accepted_payload_len) > limit {
        accepted_payload_len -= 1;
    }

    let mut rejected_payload_len = accepted_payload_len + 1;
    while modeled_total_size(schema, rejected_payload_len) <= limit {
        rejected_payload_len += 1;
    }

    (accepted_payload_len, rejected_payload_len)
}

#[test]
fn blob_schema_serde_is_persisted_and_defaults_to_false_when_missing() {
    let schema = Schema::new_with_id(
        41,
        "blob_schema_serde",
        None,
        Field::new_schema(vec![Field::new_unindexed("payload", Type::Bytes)]),
        false,
        false,
    )
    .with_blobs(true);

    let mut serialized = serde_json::to_value(&schema).unwrap();
    assert_eq!(serialized["blobs"], serde_json::Value::Bool(true));

    serialized.as_object_mut().unwrap().remove("blobs");
    let deserialized: Schema = serde_json::from_value(serialized).unwrap();

    assert!(!deserialized.blobs);
    assert_eq!(deserialized.id, schema.id);
    assert_eq!(deserialized.name, schema.name);
}

#[test]
fn blob_schema_regular_schema_accepts_exact_one_mib_boundary_and_rejects_next_edge() {
    let schema = Schema::new_with_id(
        42,
        "blob_schema_regular_limit",
        None,
        Field::new_schema(vec![Field::new_unindexed("payload", Type::Bytes)]),
        false,
        false,
    );
    let chunk = &Chunks::new_dummy(1, CHUNK_SIZE).list[0];
    chunk.meta.schemas.debug_only_new_schema(schema.clone());

    let (accepted_payload_len, rejected_payload_len) =
        boundary_payload_lengths(&schema, MAX_CELL_SIZE);
    let rejected_size = modeled_total_size(&schema, rejected_payload_len);

    assert_eq!(modeled_total_size(&schema, accepted_payload_len), MAX_CELL_SIZE as usize);
    assert!(rejected_size > MAX_CELL_SIZE as usize);

    let accepted = bytes_cell(schema.id, Id::new(42, 1), accepted_payload_len);
    let accepted_plan = accepted.plan_write(chunk).unwrap();
    assert_eq!(accepted_plan.total_size() as usize, MAX_CELL_SIZE as usize);

    let rejected = bytes_cell(schema.id, Id::new(42, 2), rejected_payload_len);

    assert!(matches!(
        rejected.plan_write(chunk),
        Err(WriteError::CellIsTooLarge(actual_size)) if actual_size == rejected_size
    ));
}

#[test]
fn blob_schema_blob_schema_accepts_exact_two_mib_boundary_and_rejects_next_edge() {
    let schema = Schema::new_with_id(
        43,
        "blob_schema_blob_limit",
        None,
        Field::new_schema(vec![Field::new_unindexed("payload", Type::Bytes)]),
        false,
        false,
    )
    .with_blobs(true);
    let chunk = &Chunks::new_dummy(1, CHUNK_SIZE).list[0];
    chunk.meta.schemas.debug_only_new_schema(schema.clone());

    let (accepted_payload_len, rejected_payload_len) =
        boundary_payload_lengths(&schema, MAX_BLOB_CELL_SIZE);
    let rejected_size = modeled_total_size(&schema, rejected_payload_len);

    assert_eq!(
        modeled_total_size(&schema, accepted_payload_len),
        MAX_BLOB_CELL_SIZE as usize
    );
    assert!(rejected_size > MAX_BLOB_CELL_SIZE as usize);

    let accepted = bytes_cell(schema.id, Id::new(43, 1), accepted_payload_len);
    let accepted_plan = accepted.plan_write(chunk).unwrap();
    assert_eq!(accepted_plan.total_size() as usize, MAX_BLOB_CELL_SIZE as usize);

    let rejected = bytes_cell(schema.id, Id::new(43, 2), rejected_payload_len);
    assert!(matches!(
        rejected.plan_write(chunk),
        Err(WriteError::CellIsTooLarge(actual_size)) if actual_size == rejected_size
    ));
}

#[test]
fn blob_schema_blob_and_regular_cells_land_in_different_segment_classes() {
    let chunks = Chunks::new_dummy(1, CHUNK_SIZE * 3);
    let chunk = &chunks.list[0];
    let regular_schema = bytes_schema(44, "blob_schema_regular_lane", false);
    let blob_schema = bytes_schema(45, "blob_schema_blob_lane", true);
    chunk.meta.schemas.debug_only_new_schema(regular_schema.clone());
    chunk.meta.schemas.debug_only_new_schema(blob_schema.clone());

    let regular_id = Id::new(44, 1);
    let blob_id = Id::new(44, 2);
    let mut regular_cell = bytes_cell(regular_schema.id, regular_id, 1024);
    let mut blob_cell = bytes_cell(blob_schema.id, blob_id, 1024);

    chunks.write_cell(&mut regular_cell).unwrap();
    chunks.write_cell(&mut blob_cell).unwrap();

    assert_eq!(written_segment_class(&chunks, &regular_id), SegmentClass::Regular);
    assert_eq!(written_segment_class(&chunks, &blob_id), SegmentClass::Blob);
}

#[test]
fn blob_schema_chunk_keeps_independent_blob_and_regular_heads() {
    let chunks = Chunks::new_dummy(1, CHUNK_SIZE * 3);
    let chunk = &chunks.list[0];
    let regular_schema = bytes_schema(46, "blob_schema_regular_head", false);
    let blob_schema = bytes_schema(47, "blob_schema_blob_head", true);
    chunk.meta.schemas.debug_only_new_schema(regular_schema.clone());
    chunk.meta.schemas.debug_only_new_schema(blob_schema.clone());

    let (initial_regular_head, initial_blob_head) = chunk.head_seg_ids_for_test();
    assert_eq!(chunk.get_head_seg_id(), initial_regular_head);
    assert_eq!(initial_blob_head, None);

    let regular_id = Id::new(46, 1);
    let mut regular_cell = bytes_cell(regular_schema.id, regular_id, 128);
    chunks.write_cell(&mut regular_cell).unwrap();

    assert_eq!(chunk.head_seg_ids_for_test(), (initial_regular_head, None));

    let blob_id = Id::new(46, 2);
    let mut blob_cell = bytes_cell(blob_schema.id, blob_id, 128);
    chunks.write_cell(&mut blob_cell).unwrap();

    let (regular_head, blob_head) = chunk.head_seg_ids_for_test();
    let blob_addr = {
        let loc = chunks.location_for_read(&blob_id).unwrap();
        *loc
    };
    let blob_segment_id = chunk.locate_segment(blob_addr).unwrap().id;

    assert_eq!(regular_head, initial_regular_head);
    assert_eq!(chunk.get_head_seg_id(), regular_head);
    assert_eq!(blob_head, Some(blob_segment_id));
    assert_ne!(blob_head, Some(regular_head));
}

#[test]
fn blob_schema_active_blob_head_is_excluded_from_cleaner_candidates() {
    let chunks = Chunks::new_dummy(1, SEGMENT_SIZE * 3);
    let chunk = &chunks.list[0];
    let blob_schema = bytes_schema(48, "blob_schema_blob_cleaner_head", true);
    chunk.meta.schemas.debug_only_new_schema(blob_schema.clone());

    let id = Id::new(48, 1);
    let mut original = bytes_cell(blob_schema.id, id, 256 * 1024);
    chunks.write_cell(&mut original).unwrap();

    let mut updated = bytes_cell(blob_schema.id, id, 384 * 1024);
    chunks.update_cell(&mut updated).unwrap();

    let (_, blob_head) = chunk.head_seg_ids_for_test();
    let blob_head = blob_head.expect("blob writes should allocate a blob head segment");
    assert_eq!(written_segment_id(&chunks, &id), blob_head);

    let cleaner_candidates: Vec<_> = chunk
        .segs_for_combine_cleaner_full()
        .into_iter()
        .map(|(seg, _)| seg.id)
        .collect();

    assert!(
        !cleaner_candidates.contains(&blob_head),
        "active blob head must not be selected by the combine cleaner"
    );
}

#[test]
fn blob_schema_partial_cleaner_candidates_stay_class_aware_in_mixed_workloads() {
    let chunks = Chunks::new_dummy(1, SEGMENT_SIZE * 8);
    let chunk = &chunks.list[0];
    let regular_schema = bytes_schema(49, "blob_schema_regular_cleaner_lane", false);
    let blob_schema = bytes_schema(50, "blob_schema_blob_cleaner_lane", true);
    chunk.meta.schemas.debug_only_new_schema(regular_schema.clone());
    chunk.meta.schemas.debug_only_new_schema(blob_schema.clone());

    let mut regular_segments = BTreeSet::new();
    let mut regular_cells = Vec::new();
    for index in 0..64_u64 {
        let id = Id::new(4_900, 10_000 + index);
        let mut cell = bytes_cell(regular_schema.id, id, 512 * 1024);
        chunks.write_cell(&mut cell).unwrap();

        let segment_id = written_segment_id(&chunks, &id);
        regular_segments.insert(segment_id);
        regular_cells.push((id, segment_id));

        if regular_segments.len() >= 2 {
            break;
        }
    }

    let (regular_head, _) = chunk.head_seg_ids_for_test();
    let regular_candidate = regular_segments
        .into_iter()
        .find(|segment_id| *segment_id != regular_head)
        .expect("setup should create a non-head regular segment");

    let mut kept_regular = false;
    for (id, segment_id) in &regular_cells {
        if *segment_id != regular_candidate {
            continue;
        }

        if kept_regular {
            chunks.remove_cell(id).unwrap();
        } else {
            kept_regular = true;
        }
    }
    assert!(kept_regular, "setup should keep one regular survivor cell");

    let payload_len = 1_500_000;
    let mut blob_segments = BTreeSet::new();
    let mut blob_cells = Vec::new();
    for index in 0..64_u64 {
        let id = Id::new(5_000, 20_000 + index);
        let mut cell = bytes_cell(blob_schema.id, id, payload_len);
        chunks.write_cell(&mut cell).unwrap();

        let segment_id = written_segment_id(&chunks, &id);
        blob_segments.insert(segment_id);
        blob_cells.push((id, segment_id));

        if blob_segments.len() >= 3 {
            break;
        }
    }

    let (_, blob_head) = chunk.head_seg_ids_for_test();
    let blob_head = blob_head.expect("blob writes should leave an active blob head");
    let blob_source_segments: BTreeSet<_> = blob_segments
        .into_iter()
        .filter(|segment_id| *segment_id != blob_head)
        .take(2)
        .collect();
    assert_eq!(
        blob_source_segments.len(),
        2,
        "setup should create two non-head blob segments"
    );

    let mut kept_blob_segments = BTreeSet::new();
    for (id, segment_id) in &blob_cells {
        if !blob_source_segments.contains(segment_id) {
            continue;
        }

        if kept_blob_segments.insert(*segment_id) {
            continue;
        }

        chunks.remove_cell(id).unwrap();
    }

    let candidate_segments = chunk.segs_for_combine_cleaner();
    let candidate_ids: BTreeSet<_> = candidate_segments.iter().map(|(seg, _)| seg.id).collect();

    assert!(
        candidate_segments.len() >= 2,
        "mixed workload should still yield a combine candidate set"
    );
    assert!(
        candidate_segments
            .iter()
            .all(|(seg, _)| seg.segment_class() == SegmentClass::Blob),
        "partial cleaner candidates should stay on one segment class before downstream slicing"
    );
    assert_eq!(
        candidate_ids, blob_source_segments,
        "the blob lane should win when it is the only class with multiple reclaimable segments"
    );
    assert!(
        !candidate_ids.contains(&regular_candidate),
        "a lone lower-utilization regular segment must not displace two reclaimable blob segments"
    );
}

#[test]
fn blob_schema_combine_preserves_blob_segment_class() {
    let chunks = Chunks::new_dummy(1, SEGMENT_SIZE * 5);
    let chunk = &chunks.list[0];
    let blob_schema = bytes_schema(51, "blob_schema_combine_blob_lane", true);
    chunk.meta.schemas.debug_only_new_schema(blob_schema.clone());

    let payload_len = 1_500_000;
    let mut distinct_blob_segments = BTreeSet::new();
    let mut blob_cells = Vec::new();

    for index in 0..32_u64 {
        let id = Id::new(51, index);
        let mut cell = bytes_cell(blob_schema.id, id, payload_len);
        chunks.write_cell(&mut cell).unwrap();

        let segment_id = written_segment_id(&chunks, &id);
        distinct_blob_segments.insert(segment_id);
        blob_cells.push((id, segment_id));

        if distinct_blob_segments.len() >= 3 {
            break;
        }
    }

    let (_, blob_head) = chunk.head_seg_ids_for_test();
    let blob_head = blob_head.expect("blob writes should leave an active blob head");
    let source_segments: BTreeSet<_> = distinct_blob_segments
        .into_iter()
        .filter(|segment_id| *segment_id != blob_head)
        .take(2)
        .collect();
    assert_eq!(source_segments.len(), 2, "setup should produce two non-head blob segments");

    let mut kept_segments = BTreeSet::new();
    let mut survivor_ids = Vec::new();
    for (id, segment_id) in &blob_cells {
        if !source_segments.contains(segment_id) {
            continue;
        }

        if kept_segments.insert(*segment_id) {
            survivor_ids.push(*id);
        } else {
            chunks.remove_cell(id).unwrap();
        }
    }

    assert_eq!(
        survivor_ids.len(),
        source_segments.len(),
        "setup should keep one live blob cell per source segment"
    );

    let selected_segments: Vec<_> = chunk
        .segments()
        .into_iter()
        .filter(|seg| source_segments.contains(&seg.id))
        .collect();
    assert!(selected_segments.iter().all(|seg| seg.segment_class() == SegmentClass::Blob));

    let (_, reduced_segments) = combine::CombinedCleaner::combine_segments(chunk, &selected_segments);
    assert!(
        reduced_segments > 0,
        "combine should collapse fragmented blob segments into fewer replacements"
    );

    for survivor_id in survivor_ids {
        assert_eq!(
            written_segment_class(&chunks, &survivor_id),
            SegmentClass::Blob,
            "combined blob cells must stay on blob-class segments"
        );
    }
}