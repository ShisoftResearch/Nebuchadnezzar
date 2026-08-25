use crate::dovahkiin::types::Map;
use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::cleaner::combine;
use crate::ram::entry::{EntryContent, EntryType};
use crate::ram::file_manager::SegmentFileManager;
use crate::ram::schema::Field;
use crate::ram::schema::*;
use crate::ram::segs::{SegmentAllocator, SEGMENT_SIZE};
use crate::ram::types::*;
use crate::server::ServerMeta;
use env_logger;
use lightning::map::Map as LightningMap;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub const DATA_SIZE: usize = 1000 * 1024; // nearly 1MB
const MAX_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

fn default_cell(id: &Id) -> OwnedCell {
    let data: Vec<_> = std::iter::repeat(id.bits() as u8).take(DATA_SIZE).collect();
    OwnedCell {
        header: CellHeader::new(SchemaVid(0), id),
        data: data_map_value!(id: id.bits() as i32, data: data),
    }
}

fn default_fields() -> Field {
    Field::new_schema(vec![
        Field::new_unindexed("id", Type::I32),
        Field::new_unindexed_array("data", Type::U8),
    ])
}

#[test]
pub fn full_clean_cycle_without_compact() {
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
            let mut cell = default_cell(&Id::allocated(0, 0, i));
            chunks.write_cell(&mut cell).unwrap();
        }

        assert_eq!(chunk.segments().len(), 2);
        assert_eq!(chunk.cell_index.len(), 16);

        // delete half the cells to create tombstones and fragmentation
        for i in 0..8 {
            chunks.remove_cell(&Id::allocated(0, 0, i * 2)).unwrap();
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

    // combine only (compact cleaner removed)
    {
        // Cleaner refuses to work on head segment; set head to dummy to include both segments
        chunk.head_pool[0].store(1234, std::sync::atomic::Ordering::Relaxed);
        combine::CombinedCleaner::combine_segments(chunk, &chunk.segments());
        let survival_cells: HashSet<_> = chunk
            .live_entries(&chunk.segments()[0])
            .map(|entry| {
                assert_eq!(entry.meta.entry_header.entry_type, EntryType::CELL);
                if let EntryContent::Cell(ref header) = entry.content {
                    return header.id.bits();
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
        let id = Id::allocated(0, 0, id);
        let cell = chunks.read_cell(&id).unwrap();
        assert_eq!(cell.to_owned().data, default_cell(&id).data);
    });
}

#[test]
fn test_shrink_fully_utilized_segment() {
    let _ = env_logger::try_init();

    let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 3);
    let file_manager = Arc::new(SegmentFileManager::new(None, None));

    let segment = allocator
        .alloc_seg(&file_manager)
        .expect("Failed to allocate segment");

    segment
        .append_header
        .store(segment.bound(), Ordering::Relaxed);

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
    let _ = env_logger::try_init();

    let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 3);
    let file_manager = Arc::new(SegmentFileManager::new(None, None));

    let segment = allocator
        .alloc_seg(&file_manager)
        .expect("Failed to allocate segment");

    segment.shrink(SEGMENT_SIZE + 1);

    let used_size = segment.append_header.load(Ordering::Relaxed) - segment.addr;
    assert_eq!(used_size, 0, "Segment should still be empty");
}
