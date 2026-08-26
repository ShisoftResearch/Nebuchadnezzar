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

/// Two generations of one family: generation 0, and a generation 1 that adds a
/// nullable field. The pair an identity-transform evolution produces.
fn evolving_pair() -> (Schema, Schema) {
    let mut gen0 = Schema::new_with_id(1, "evolving", None, default_fields(), false, false);
    let mut gen1 = Schema::new_with_id(
        1,
        "evolving",
        None,
        Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed_array("data", Type::U8),
            Field::new_unindexed_nullable("added", Type::U64),
        ]),
        false,
        false,
    );
    assert_eq!(
        gen0.classify_evolution(&gen1),
        EvolutionKind::Identity,
        "this fixture depends on the evolution needing no transform"
    );
    gen1.vid = SchemaVid(900);
    gen1.generation = 1;
    gen0.status = SchemaVersionStatus::Current;
    (gen0, gen1)
}

fn evolving_cell(schema: SchemaVid, id: &Id) -> OwnedCell {
    let data: Vec<_> = std::iter::repeat(id.bits() as u8).take(DATA_SIZE).collect();
    OwnedCell {
        header: CellHeader::new(schema, id),
        data: data_map_value!(id: id.bits() as i32, data: data),
    }
}

/// Set up a chunk that the combiner will actually act on.
///
/// Built to the same recipe as `full_clean_cycle_without_compact`, which is
/// the only shape known to make `combine_segments` do anything: sixteen cells
/// to fill two segments, then eight deleted so the live data fits in the one
/// destination the chunk can still spare. Returns the surviving ids.
fn provision_combinable_chunk(chunks: &Arc<Chunks>) -> Vec<Id> {
    for i in 0..16 {
        let mut cell = evolving_cell(SchemaVid(1), &Id::allocated(0, 0, i));
        chunks.write_cell(&mut cell).unwrap();
    }
    for i in 0..8 {
        chunks.remove_cell(&Id::allocated(0, 0, i * 2)).unwrap();
    }
    (0..8).map(|n| Id::allocated(0, 0, n * 2 + 1)).collect()
}

fn combinable_chunks(schemas: LocalSchemasCache) -> (Arc<Chunks>, Arc<ServerMeta>) {
    let meta = Arc::new(ServerMeta { schemas });
    let chunks = Chunks::new(
        1,
        MAX_SEGMENT_SIZE * 3,
        meta.clone(),
        None,
        None,
        None,
        None,
    );
    (chunks, meta)
}

/// Combining a segment of superseded cells migrates them: same ids, same
/// values, now encoded under the generation new writes use.
#[test]
pub fn combining_migrates_cells_left_in_a_superseded_generation() {
    let _ = env_logger::try_init();
    let (gen0, gen1) = evolving_pair();
    let schemas = LocalSchemasCache::new_local("");
    schemas.register_internal_schema(gen0);
    let (chunks, meta) = combinable_chunks(schemas);
    let chunk = &chunks.list[0];

    let survivors = provision_combinable_chunk(&chunks);
    for id in &survivors {
        assert_eq!(chunks.read_cell(id).unwrap().header.schema, SchemaVid(1));
    }

    // The evolution lands. Nothing rewrites the cells.
    meta.schemas.apply_evolution(gen1);
    for id in &survivors {
        assert_eq!(
            chunks.read_cell(id).unwrap().header.schema,
            SchemaVid(1),
            "an evolution must not touch cells already written"
        );
    }

    chunk.head_pool[0].store(1234, Ordering::Relaxed);
    combine::CombinedCleaner::combine_segments(chunk, &chunk.segments());
    assert_eq!(
        chunk.segments().len(),
        1,
        "the combine did not run, so this test proves nothing"
    );

    for id in &survivors {
        let cell = chunks
            .read_cell(id)
            .expect("a migrated cell must still be readable");
        assert_eq!(
            cell.header.schema,
            SchemaVid(900),
            "combining should have migrated this cell to the current generation"
        );
        assert_eq!(cell.id(), *id, "migration preserves identity");
        assert_eq!(cell.data["id"].i32(), Some(&(id.bits() as i32)));
    }
}

/// With no newer generation there is nothing to migrate to, and the combine
/// must relocate the cells exactly as it always has.
#[test]
pub fn combining_relocates_cells_it_cannot_migrate() {
    let _ = env_logger::try_init();
    let (gen0, _) = evolving_pair();
    let schemas = LocalSchemasCache::new_local("");
    schemas.register_internal_schema(gen0);
    let (chunks, _meta) = combinable_chunks(schemas);
    let chunk = &chunks.list[0];

    let survivors = provision_combinable_chunk(&chunks);
    chunk.head_pool[0].store(1234, Ordering::Relaxed);
    combine::CombinedCleaner::combine_segments(chunk, &chunk.segments());
    assert_eq!(chunk.segments().len(), 1, "the combine did not run");

    for id in &survivors {
        let cell = chunks
            .read_cell(id)
            .expect("a cell that cannot be migrated is still a cell");
        assert_eq!(cell.header.schema, SchemaVid(1));
        assert_eq!(cell.data["id"].i32(), Some(&(id.bits() as i32)));
    }
}

/// Generations that index differently are left alone: migrating those cells
/// would mean adding and removing index entries, which this path cannot do.
#[test]
pub fn combining_declines_to_migrate_across_an_index_change() {
    let _ = env_logger::try_init();
    let gen0 = Schema::new_with_id(1, "indexed", None, default_fields(), false, false);
    let mut gen1 = Schema::new_with_id(
        1,
        "indexed",
        None,
        Field::new_schema(vec![
            Field::new_indexed("id", Type::I32, vec![IndexType::Ranged]),
            Field::new_unindexed_array("data", Type::U8),
        ]),
        false,
        false,
    );
    gen1.vid = SchemaVid(900);
    gen1.generation = 1;

    let schemas = LocalSchemasCache::new_local("");
    schemas.register_internal_schema(gen0);
    let (chunks, meta) = combinable_chunks(schemas);
    let chunk = &chunks.list[0];

    let survivors = provision_combinable_chunk(&chunks);
    meta.schemas.apply_evolution(gen1);

    chunk.head_pool[0].store(1234, Ordering::Relaxed);
    combine::CombinedCleaner::combine_segments(chunk, &chunk.segments());
    assert_eq!(chunk.segments().len(), 1, "the combine did not run");

    for id in &survivors {
        let cell = chunks.read_cell(id).expect("the cell must survive");
        assert_eq!(
            cell.header.schema,
            SchemaVid(1),
            "an index-set change must leave migration to the write path"
        );
        assert_eq!(cell.data["id"].i32(), Some(&(id.bits() as i32)));
    }
}
