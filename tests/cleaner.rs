use neb::ram::cell;
use neb::ram::cell::*;
use neb::ram::schema::*;
use neb::ram::chunk::Chunks;
use neb::ram::types;
use neb::ram::types::*;
use neb::ram::io::writer;
use neb::ram::cleaner::Cleaner;
use neb::ram::schema::Field;
use neb::server::ServerMeta;
use neb::ram::cleaner::*;
use neb::ram::tombstone::Tombstone;
use neb::ram::entry::{EntryType, EntryContent};
use env_logger;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std;
use std::rc::Rc;
use super::*;
use neb::ram::chunk::Chunk;

pub const DATA_SIZE: usize = 1000 * 1024; // nearly 1MB
const MAX_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

fn default_cell(id: &Id) -> Cell {
    let data: Vec<_> =
        std::iter::repeat(Value::U8(id.lower as u8))
            .take(DATA_SIZE)
            .collect();
    Cell {
        header: CellHeader::new(0, 0, id, 0),
        data: data_map_value!(id: id.lower as i32, data: data)
    }
}

fn default_fields () -> Field {
    Field::new ("*", 0, false, false, Some(
        vec![
            Field::new("id", type_id_of(Type::I32), false, false, None),
            Field::new("data", type_id_of(Type::U8), false, true, None)
        ]
    ))
}

fn seg_positions(chunk: &Chunk) -> Vec<(u64, usize)> {
    chunk.addrs_seg
        .read()
        .iter()
        .map(|(pos, hash)| (*hash, *pos))
        .collect()
}

#[test]
pub fn full_clean_cycle() {
    env_logger::init();
    let schema = Schema::new(
        "cleaner_test",
        None,
        default_fields(),
        false);
    let schemas = LocalSchemasCache::new("", None).unwrap();
    schemas.new_schema(schema);
    let chunks = Chunks::new(
        1, // single chunk
        MAX_SEGMENT_SIZE * 2, // chunk two segments
        Arc::new(ServerMeta { schemas }),
        None);
    let chunk = &chunks.list[0];

    assert_eq!(chunk.segments().len(), 1);

    // put 16 cells to fill up all of those segments allocated
    for i in 0..16 {
        let mut cell = default_cell(&Id::new(0, i));
        chunks.write_cell(&mut cell).unwrap();

    }

    assert_eq!(chunk.segments().len(), 2);
    assert_eq!(chunk.index.len(), 16);

    println!("trying to delete cells");

    let all_seg_positions = seg_positions(chunk);
    let all_cell_addresses = chunk.cell_addresses();
    assert_eq!(all_seg_positions.len(), 2);
    assert_eq!(all_cell_addresses.len(), 16);

    for i in 0..8 {
        chunks.remove_cell(&Id::new(0, i * 2)).unwrap();
    }

    assert_eq!(chunk.segments().len(), 2);

    //count entries, including dead ones
    assert_eq!(chunk.segs.get(&0).unwrap().entry_iter().count(), 8); // all 8 cells
    assert_eq!(chunk.segs.get(&1).unwrap().entry_iter().count(), 16); // 8 cells and 8 tombstones

    // try to scan first segment expect no panic
    println!("Scanning first segment...");
    chunk.live_entries(&chunk.segs.get(&0).unwrap());

    println!("Scanning second segment for tombstones...");
    let live_entries = chunk.live_entries(&chunk.segs.get(&1).unwrap());
    let tombstones: Vec<_> = live_entries
        .iter()
        .filter(|e| e.meta.entry_header.entry_type == EntryType::Tombstone)
        .collect();
    assert_eq!(tombstones.len(), 8);
    for i in 0..tombstones.len() {
        let hash = (i * 2) as u64;
        let e = &tombstones[i];
        assert_eq!(e.meta.entry_header.entry_type, EntryType::Tombstone);
        if let EntryContent::Tombstone(ref t) = e.content {
            assert_eq!(t.hash, hash);
            assert_eq!(t.partition, 0);
        } else { panic!(); }
    }

    assert_eq!(chunk.cell_addresses().len(), 8);

    chunk.apply_dead_entry();

    // Compact all segments order by id
    let mut segments = chunk.segments();
    segments.sort_by_key(|seg| seg.id);
    segments.into_iter()
        .for_each(|seg|
            compact::CompactCleaner::clean_segment(chunk, &seg));

    let compacted_seg_positions = seg_positions(chunk);
    let compacted_cell_addresses = chunk.cell_addresses();
    assert_eq!(compacted_seg_positions.len(), 2);
    assert_eq!(compacted_cell_addresses.len(), 8);

    // scan segments to check entries
    let seg0 = &chunk.segs.get(&0).unwrap();
    let seg1 = &chunk.segs.get(&1).unwrap();
    let compacted_segment_0_entries = chunk.live_entries(seg0);
    let compacted_segment_1_entries = chunk.live_entries(seg1);
    let compacted_segment_0_ids = (0..4).map(|num| num as u64 * 2 + 1);
    let compacted_segment_1_ids =
        (4..8).map(|num| num as i64 * 2 + 1)
        .chain((0..8).map(|i| -1 * i * 2));
    assert_eq!(seg0.id, 0);
    assert_eq!(seg1.id, 1);
    // check for cells continuity
    compacted_segment_0_entries
        .iter()
        .zip(compacted_segment_0_ids)
        .for_each(|(entry, hash)| {
            assert_eq!(entry.meta.entry_header.entry_type, EntryType::Cell);
            if let EntryContent::Cell(header) = entry.content {
                assert_eq!(header.hash, hash)
            } else { panic!(); }
        });
    assert_eq!(compacted_segment_0_entries.len(), 4);
    assert_eq!(seg0.entry_iter().count(), 4);

    // check for cells and 4 tombstones
    compacted_segment_1_entries
        .iter()
        .zip(compacted_segment_1_ids)
        .for_each(|(entry, hash)|{
            if hash > 1 {
                // cell
                assert_eq!(entry.meta.entry_header.entry_type, EntryType::Cell);
                if let EntryContent::Cell(header) = entry.content {
                    assert_eq!(header.hash, hash as u64)
                } else { panic!(); }
            } else {
                // tombstone
                assert_eq!(entry.meta.entry_header.entry_type, EntryType::Tombstone);
                let tombstone = if let EntryContent::Tombstone(ref tombstone) = entry.content {
                    tombstone
                } else { panic!() };
                assert_eq!((hash * -1) as u64, tombstone.hash);
            }
        });
    // 4 remaining cells and 8 deleted cell tombstones
    assert_eq!(compacted_segment_1_entries.len(), 12);
    assert_eq!(seg1.entry_iter().count(), 12);
}