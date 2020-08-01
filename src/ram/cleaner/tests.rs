use super::*;
use env_logger;
use crate::ram::cell;
use crate::ram::cell::*;
use crate::ram::chunk::Chunk;
use crate::ram::chunk::Chunks;
use crate::ram::cleaner::Cleaner;
use crate::ram::cleaner::*;
use crate::ram::entry::{EntryContent, EntryType};
use crate::ram::io::writer;
use crate::ram::schema::Field;
use crate::ram::schema::*;
use crate::ram::tombstone::Tombstone;
use crate::ram::types;
use crate::ram::types::*;
use crate::server::ServerMeta;
use std;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

pub const DATA_SIZE: usize = 1000 * 1024; // nearly 1MB
const MAX_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

fn default_cell(id: &Id) -> Cell {
    let data: Vec<_> = std::iter::repeat(id.lower as u8).take(DATA_SIZE).collect();
    Cell {
        header: CellHeader::new(0, 0, id),
        data: data_map_value!(id: id.lower as i32, data: data),
    }
}

fn default_fields() -> Field {
    Field::new(
        "*",
        0,
        false,
        false,
        Some(vec![
            Field::new("id", type_id_of(Type::I32), false, false, None, vec![]),
            Field::new("data", type_id_of(Type::U8), false, true, None, vec![]),
        ]),
        vec![],
    )
}

fn seg_positions(chunk: &Chunk) -> Vec<(u64, usize)> {
    chunk
        .addrs_seg
        .read()
        .iter()
        .map(|(pos, hash)| (*hash, *pos))
        .collect()
}

#[test]
pub fn full_clean_cycle() {
    env_logger::init();
    let schema = Schema::new("cleaner_test", None, default_fields(), false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema);
    let chunks = Chunks::new(
        1,                    // single chunk
        MAX_SEGMENT_SIZE * 2, // chunk two segments
        Arc::new(ServerMeta { schemas }),
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
        assert_eq!(chunk.index.len(), 16);

        println!("trying to delete cells");

        let all_seg_positions = seg_positions(chunk);();
        assert_eq!(all_seg_positions.len(), 2);
        assert_eq!(chunk.cell_count(), 16);

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
            } else {
                panic!();
            }
        }

        assert_eq!(chunk.cell_count(), 8);

        chunk.apply_dead_entry();
    }

    // check integrity
    chunk
        .live_entries(&chunk.segs.get(&0).unwrap())
        .collect::<Vec<_>>();
    chunk
        .live_entries(&chunk.segs.get(&1).unwrap())
        .collect::<Vec<_>>();

    // compact
    {
        // Compact all segments order by id
        chunk.segments().into_iter().for_each(|seg| {
            compact::CompactCleaner::clean_segment(chunk, &seg);
        });

        let compacted_seg_positions = seg_positions(chunk);();
        assert_eq!(compacted_seg_positions.len(), 2);
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
                assert_eq!(entry.meta.entry_header.entry_type, EntryType::Cell);
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
                    assert_eq!(entry.meta.entry_header.entry_type, EntryType::Cell);
                    if let EntryContent::Cell(header) = entry.content {
                        assert_eq!(header.hash, hash as u64)
                    } else {
                        panic!();
                    }
                } else {
                    // tombstone
                    assert_eq!(entry.meta.entry_header.entry_type, EntryType::Tombstone);
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
        assert_eq!(seg1.entry_iter().count(), 12);
    }

    // combine
    {
        combine::CombinedCleaner::combine_segments(chunk, &chunk.segments());
        let survival_cells: HashSet<_> = chunk
            .live_entries(&chunk.segments()[0])
            .map(|entry| {
                assert_eq!(entry.meta.entry_header.entry_type, EntryType::Cell);
                if let EntryContent::Cell(ref header) = entry.content {
                    return header.hash;
                } else {
                    panic!()
                }
            })
            .collect();
        (0..8)
            .map(|n| n as u64 * 2 + 1)
            .for_each(|hash| assert!(survival_cells.contains(&hash)));
        assert_eq!(chunk.segments().len(), 1);
        assert_eq!(survival_cells.len(), 8);
        assert_eq!(chunk.segments()[0].entry_iter().count(), 8);
    }

    // validate cells
    (0..8).map(|n| n * 2 + 1).for_each(|id| {
        let id = Id::new(0, id);
        let cell = chunks.read_cell(&id).unwrap();
        assert_eq!(cell.data, default_cell(&id).data);
    });
}
