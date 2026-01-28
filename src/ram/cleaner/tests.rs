use super::*;
use crate::dovahkiin::types::Map;
use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::entry::{EntryContent, EntryType};
use crate::ram::file_manager::SegmentFileManager;
use crate::ram::schema::Field;
use crate::ram::schema::*;
use crate::ram::types::*;
use crate::server::ServerMeta;
use env_logger;
use lightning::map::Map as LFMap;
use std;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

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

#[test]
pub fn full_clean_cycle() {
    let _ = env_logger::try_init();
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

        println!("trying to delete cells");

        assert_eq!(chunk.seg_count(), 2);
        assert_eq!(chunk.cell_count(), 16);

        assert_eq!(chunk.segs.get(&0).unwrap().entry_iter().count(), 8);
        assert_eq!(chunk.segs.get(&1).unwrap().entry_iter().count(), 8);

        for i in 0..8 {
            chunks.remove_cell(&Id::new(0, i * 2)).unwrap();
        }

        assert_eq!(chunk.segments().len(), 2);

        //count entries, including dead ones
        assert_eq!(chunk.segs.get(&0).unwrap().entry_iter().count(), 8); // all 8 cells
        assert_eq!(chunk.segs.get(&1).unwrap().entry_iter().count(), 16); // 8 cells and 8 tombstones

        // try to scan first segment expect no panic
        println!("Scanning first segment...");
        chunk
            .live_entries(&chunk.segs.get(&0).unwrap())
            .for_each(|_| {});

        println!("Scanning second segment for tombstones...");
        let seg = &chunk.segs.get(&1).unwrap();
        let live_entries = chunk.live_entries(seg);
        let tombstones: Vec<_> = live_entries
            .filter(|e| e.meta.entry_header.entry_type == EntryType::TOMBSTONE)
            .collect();
        assert_eq!(tombstones.len(), 8);
        for i in 0..tombstones.len() {
            let hash = (i * 2) as u64;
            let e = &tombstones[i];
            assert_eq!(e.meta.entry_header.entry_type, EntryType::TOMBSTONE);
            if let EntryContent::Tombstone(ref t) = e.content {
                assert_eq!(t.hash, hash);
                assert_eq!(t.partition, 0);
            } else {
                panic!();
            }
        }

        assert_eq!(chunk.cell_count(), 8, "Cell count does not match");
    }

    // check integrity
    let _ = chunk
        .live_entries(&chunk.segs.get(&0).unwrap())
        .collect::<Vec<_>>();
    let _ = chunk
        .live_entries(&chunk.segs.get(&1).unwrap())
        .collect::<Vec<_>>();

    // compact
    {
        // Cleaner refuses to work on head segment, set head segment to a dummy value
        chunk
            .head_seg_id
            .store(1234, std::sync::atomic::Ordering::Relaxed);
        // Compact all segments order by id
        chunk.segments().into_iter().for_each(|seg| {
            compact::CompactCleaner::clean_segment(chunk, SegmentCandidate::new(&seg).unwrap());
        });

        assert_eq!(chunk.seg_count(), 2);
        assert_eq!(chunk.cell_count(), 8);

        // scan segments to check entries
        let seg0 = &chunk.segs.get(&0).unwrap();
        let seg1 = &chunk.segs.get(&1).unwrap();
        let compacted_segment_0_entries = chunk.live_entries(seg0).collect::<Vec<_>>();
        let compacted_segment_1_entries = chunk.live_entries(seg1).collect::<Vec<_>>();
        let compacted_segment_0_ids = (0..4).map(|num| num as u64 * 2 + 1);
        let compacted_segment_1_ids = (4..8)
            .map(|num| num as i64 * 2 + 1)
            .chain((0..8).map(|i| -1 * i * 2));
        assert_eq!(seg0.id, 0);
        assert_eq!(seg1.id, 1);
        // check for cells continuity
        compacted_segment_0_entries
            .iter()
            .zip(compacted_segment_0_ids)
            .for_each(|(entry, hash)| {
                assert_eq!(entry.meta.entry_header.entry_type, EntryType::CELL);
                if let EntryContent::Cell(header) = entry.content {
                    assert_eq!(header.hash, hash)
                } else {
                    panic!();
                }
            });
        assert_eq!(compacted_segment_0_entries.len(), 4);
        assert_eq!(seg0.entry_iter().count(), 4);

        // check for cells and 4 tombstones
        compacted_segment_1_entries
            .iter()
            .zip(compacted_segment_1_ids)
            .for_each(|(entry, hash)| {
                if hash > 1 {
                    // cell
                    assert_eq!(entry.meta.entry_header.entry_type, EntryType::CELL);
                    if let EntryContent::Cell(header) = entry.content {
                        assert_eq!(header.hash, hash as u64)
                    } else {
                        panic!();
                    }
                } else {
                    // tombstone
                    assert_eq!(entry.meta.entry_header.entry_type, EntryType::TOMBSTONE);
                    let tombstone = if let EntryContent::Tombstone(ref tombstone) = entry.content {
                        tombstone
                    } else {
                        panic!()
                    };
                    assert_eq!((hash * -1) as u64, tombstone.hash);
                }
            });
        // 4 remaining cells and 8 deleted cell tombstones
        assert_eq!(compacted_segment_1_entries.len(), 12);
        assert_eq!(
            seg1.entry_iter().count(),
            12,
            "Head segment should not be compacted, but get entries: {:?}",
            seg1.entry_iter()
                .map(|e| e.entry_header.entry_type)
                .collect::<Vec<_>>()
        );
        // Restore head segment id for subsequent operations
        chunk
            .head_seg_id
            .store(1, std::sync::atomic::Ordering::Relaxed);
    }

    // combine
    {
        // Cleaner refused to work on head segment, set head segment to something else
        chunk
            .head_seg_id
            .store(1234, std::sync::atomic::Ordering::Relaxed);
        combine::CombinedCleaner::combine_segments(chunk, &chunk.segments());
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
        assert_eq!(chunk.segments()[0].entry_iter().count(), 8);
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
fn compact_marks_no_progress_and_skips_segment() {
    let _ = env_logger::try_init();
    let schema = Schema::new("cleaner_skip_test", None, default_fields(), false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema);
    let chunks = Chunks::new(
        1,
        MAX_SEGMENT_SIZE * 3,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );
    let chunk = &chunks.list[0];

    // Write enough data to force the chunk to allocate a second (non-head) segment.
    for i in 0..10 {
        let mut cell = default_cell(&Id::new(0, i));
        chunks.write_cell(&mut cell).unwrap();
    }
    assert!(
        chunk.segments().len() >= 2,
        "expected at least two segments after writes"
    );

    let head_id = chunk.get_head_seg_id();
    let victim_seg = chunk
        .segments()
        .into_iter()
        .find(|seg| seg.id != head_id)
        .expect("need a non-head segment to compact");

    // Artificially mark the segment as fragmented without actually removing cells.
    let fake_dead = victim_seg.used_spaces() / 2 + 1;
    victim_seg.dead_space.store(fake_dead, Ordering::Relaxed);
    victim_seg.note_dead_bytes_change();

    let initial_candidates = chunk.segs_for_compact_cleaner();
    assert!(
        initial_candidates.iter().any(|seg| seg.id == victim_seg.id),
        "segment should be considered for compaction when utilization drops"
    );

    let reclaimed = compact::CompactCleaner::clean_segment(chunk, SegmentCandidate::new(&victim_seg).unwrap());
    assert_eq!(
        reclaimed, 0,
        "no space should be reclaimed without dead cells"
    );
    assert!(
        victim_seg.cleaned_without_progress(),
        "segment should record a no-progress clean"
    );

    let after_candidates = chunk.segs_for_compact_cleaner();
    assert!(
        after_candidates.iter().all(|seg| seg.id != victim_seg.id),
        "segment cleaned without progress should be skipped until state changes"
    );

    // New dead bytes should clear the marker and allow the segment to be cleaned again.
    chunk.mark_dead_entry_with_size(victim_seg.addr, 8, &victim_seg);
    let refreshed_candidates = chunk.segs_for_compact_cleaner();
    assert!(
        refreshed_candidates
            .iter()
            .any(|seg| seg.id == victim_seg.id),
        "new dead bytes should make segment eligible for compaction again"
    );
}

#[test]
fn test_shrink_fully_utilized_segment() {
    use crate::ram::segs::{SegmentAllocator, SEGMENT_SIZE};
    use std::sync::atomic::Ordering;

    let _ = env_logger::try_init();

    // Create a segment allocator
    let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 3);
    let file_manager = Arc::new(SegmentFileManager::new(None, None));

    // Allocate a segment
    let segment = allocator
        .alloc_seg(&file_manager)
        .expect("Failed to allocate segment");

    // Set append_header to the bound, making the segment fully utilized
    // This simulates the scenario where used_size == SEGMENT_SIZE
    segment
        .append_header
        .store(segment.bound, Ordering::Relaxed);

    // Verify the segment is fully utilized
    let used_size = segment.append_header.load(Ordering::Relaxed) - segment.addr;
    assert_eq!(used_size, SEGMENT_SIZE, "Segment should be fully utilized");

    // Call shrink with SEGMENT_SIZE - this should not panic
    // Before the fix, this would panic with "Shrink to 8388608 max 8388608"
    // After the fix, it should return early without doing anything
    segment.shrink(SEGMENT_SIZE);

    // Verify shrink didn't modify anything (since segment is full, there's nothing to shrink)
    let used_size_after = segment.append_header.load(Ordering::Relaxed) - segment.addr;
    assert_eq!(
        used_size_after, SEGMENT_SIZE,
        "Segment should still be fully utilized"
    );
}

#[test]
fn test_shrink_larger_than_segment_size() {
    use crate::ram::segs::{SegmentAllocator, SEGMENT_SIZE};
    use std::sync::atomic::Ordering;

    let _ = env_logger::try_init();

    // Create a segment allocator
    let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 3);
    let file_manager = Arc::new(SegmentFileManager::new(None, None));

    // Allocate a segment
    let segment = allocator
        .alloc_seg(&file_manager)
        .expect("Failed to allocate segment");

    // Call shrink with size larger than SEGMENT_SIZE - this should not panic
    // This is an edge case that should be handled gracefully
    segment.shrink(SEGMENT_SIZE + 1);

    // Verify segment state is unchanged
    let used_size = segment.append_header.load(Ordering::Relaxed) - segment.addr;
    assert_eq!(used_size, 0, "Segment should still be empty");
}
