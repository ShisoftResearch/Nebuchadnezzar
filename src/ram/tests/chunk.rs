use super::*;
use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::schema::*;
use crate::ram::types::*;
use crate::server::ServerMeta;
use bifrost_hasher::hash_str;
use env_logger;
use std;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

pub const CHUNK_SIZE: usize = 1 * 8 * 1024 * 1024;

#[test]
pub fn round_robin_segment() {
    let num = AtomicU8::new(std::u8::MAX);
    assert_eq!(num.load(Ordering::SeqCst), 255);
    assert_eq!(num.fetch_add(1, Ordering::SeqCst), 255);
    assert_eq!(num.fetch_add(1, Ordering::SeqCst), 0);
    assert_eq!(num.fetch_add(1, Ordering::SeqCst), 1);
}

#[test]
pub fn cell_rw() {
    let _ = env_logger::try_init();
    info!("START");
    let id1 = Id::new(1, 1);
    let id2 = Id::new(1, 2);
    let fields = default_fields();
    let schema = Schema::new("dummy", None, fields, false, false);
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(100));
    data_map.insert(&String::from("score"), OwnedValue::U64(70));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Jack")),
    );
    let mut data = OwnedValue::Map(data_map);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema.clone());
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );
    let header = chunks.write_cell(&mut cell).unwrap();
    let _cell_1_ptr = chunks.address_of(&Id::from_header(&header));
    {
        let stored_cell = chunks.read_cell(&id1).unwrap();
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
    }
    data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(2));
    data_map.insert(&String::from("score"), OwnedValue::U64(80));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("John")),
    );
    data = OwnedValue::Map(data_map);
    cell = OwnedCell {
        header: CellHeader::new(schema.id, &id2),
        data,
    };
    let header = chunks.write_cell(&mut cell).unwrap();
    let _cell_2_ptr = chunks.address_of(&Id::from_header(&header));
    {
        let stored_cell = chunks.read_cell(&id2).unwrap();
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &2);
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &80);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "John");
    }
    {
        let stored_cell = chunks.read_cell(&id1).unwrap();
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
    }
    data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(2));
    data_map.insert(&String::from("score"), OwnedValue::U64(95));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("John")),
    );
    data = OwnedValue::Map(data_map);
    cell = OwnedCell {
        header: CellHeader::new(schema.id, &id2),
        data,
    };
    let header = chunks.update_cell(&mut cell).unwrap();
    let _cell_2_ptr = chunks.address_of(&Id::from_header(&header));
    {
        let stored_cell = chunks.read_cell(&id2).unwrap();
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &2);
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &95);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "John");
    }
    {
        let stored_cell = chunks.read_cell(&id1).unwrap();
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
    }
    chunks
        .update_cell_by(&id2, |cell| {
            let mut data_map = OwnedMap::new();
            data_map.insert(&String::from("id"), OwnedValue::I64(2));
            data_map.insert(&String::from("score"), OwnedValue::U64(100));
            data_map.insert(
                &String::from("name"),
                OwnedValue::String(String::from("John")),
            );
            let data = OwnedValue::Map(data_map);
            let mut cell = (*cell).to_owned();
            cell.data = data;
            Some(cell)
        })
        .unwrap();
    {
        let stored_cell = chunks.read_cell(&id2).unwrap();
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &2);
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "John");
    }
    {
        let sel_cell = chunks
            .read_selected(&id2, &[hash_str("score"), hash_str("name")], true)
            .unwrap();
        assert_eq!(
            sel_cell.data.uni_array().unwrap()[0].u64(),
            Some(&100),
            "cell {:?}",
            sel_cell.data
        );
        assert_eq!(
            sel_cell.data.uni_array().unwrap()[1].string().unwrap(),
            "John"
        );
    }
    chunks.remove_cell(&id1).unwrap();
    assert!(chunks.read_cell(&id1).is_err());
}

#[test]
pub fn simple_cell_rw() {
    let _ = env_logger::try_init();
    let id1 = Id::new(1, 1);
    let fields = simple_fields();
    let schema = Schema::new("simple", None, fields, false, true);
    let data = OwnedValue::U64(128);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema.clone());
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );
    chunks.write_cell(&mut cell).unwrap();
    {
        let stored_cell = chunks.read_cell(&id1).unwrap();
        assert_eq!(stored_cell.data.u64(), Some(&128));
    }
}

#[test]
pub fn array_dyn_map() {
    let _ = env_logger::try_init();
    let id1 = Id::new(1, 1);
    let fields = Field::new_schema(vec![
        Field::new_unindexed("fixed", Type::U32),
        dyn_map_field("dynamic"),
    ]);
    let schema = Schema::new("array_dyn_map", None, fields, false, true);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );
    let data = data_map_value!(
        fixed: OwnedValue::U32(42),
        dynamic: dyn_map_value()
    );
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();
    {
        let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
        assert_eq!(cell.data, read_cell.data);
    }
}

#[test]
pub fn complex_cell_sel_read() {
    let _ = env_logger::try_init();
    let id1 = Id::new(1, 1);
    let fields = complex_fields();
    let schema = Schema::new("complex", None, fields, false, true);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );
    let data = data_map_value!(
        id: OwnedValue::I64(128),
        strings: OwnedValue::PrimArray(OwnedPrimArray::String(vec![
            String::from("aaaa"),
            String::from("bbbb"),
            String::from("cccc")
        ])),
        num: OwnedValue::U64(256),
        vec1: OwnedValue::PrimArray(OwnedPrimArray::F64(vec![
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0
        ])),
        vec2: OwnedValue::PrimArray(OwnedPrimArray::F32(vec![
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0
        ])),
        nums: OwnedValue::PrimArray(OwnedPrimArray::U64(vec![
            512, 1024, 2048
        ])),
        sub: data_map_value!(
            sub1: OwnedValue::U32(4096),
            sub2: OwnedValue::PrimArray(OwnedPrimArray::U32(vec![
                8192, 16384
            ])),
            sub3: OwnedValue::U32(1),
            sub4: data_map_value!(
                sub4sub1: OwnedValue::U32(2),
                sub4sub2: OwnedValue::PrimArray(OwnedPrimArray::U32(vec![
                    3, 4, 5
                ])),
                sub4sub3: OwnedValue::PrimArray(OwnedPrimArray::U64(vec![
                    6, 7
                ])),
                sub4sub4: OwnedValue::U16(8)
            ),
            sub5: dyn_map_value(),
            subend: OwnedValue::U32(24)
        )
    );
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();
    {
        let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
        assert_eq!(read_cell.data, cell.data);
    }
    {
        // Basic select case
        let partial_cell = chunks
            .read_selected(
                &id1,
                &key_hashes(&vec![String::from("id"), String::from("num")]),
                true,
            )
            .unwrap()
            .data
            .owned();
        assert_eq!(&partial_cell[0usize], &cell["id"]);
        assert_eq!(&partial_cell[1usize], &cell["num"]);
    }
    {
        // Verify schema id
        let partial_cell = chunks
            .read_selected(
                &id1,
                &key_hashes(&vec![String::from("id"), String::from("num")]),
                true,
            )
            .unwrap();
        assert_eq!(partial_cell.header.schema, schema.id);
    }
    {
        // Verify schema id with minimal header
        let partial_cell = chunks
            .read_selected(
                &id1,
                &key_hashes(&vec![String::from("id"), String::from("num")]),
                false,
            )
            .unwrap();
        assert_eq!(partial_cell.header.schema, schema.id);
    }
    {
        // Selecting one in nested map
        let partial_cell = chunks
            .read_selected(&id1, &key_hashes(&vec![String::from("sub|sub1")]), true)
            .unwrap()
            .data
            .owned();
        assert_eq!(&partial_cell[0usize], &cell["sub"]["sub1"]);
    }
    {
        // Selecting one array in nested map
        let partial_cell = chunks
            .read_selected(&id1, &key_hashes(&vec![String::from("sub|sub2")]), true)
            .unwrap()
            .data
            .owned();
        assert_eq!(&partial_cell[0usize], &cell["sub"]["sub2"]);
    }
    {
        // Selecting one string in nested map
        let partial_cell = chunks
            .read_selected(&id1, &key_hashes(&vec![String::from("sub|sub3")]), false)
            .unwrap()
            .data
            .owned();
        assert_eq!(&partial_cell[0usize], &cell["sub"]["sub3"]);
    }
    {
        // Selecting one map array in nested map
        let partial_cell = chunks
            .read_selected(&id1, &key_hashes(&vec![String::from("sub|sub4")]), true)
            .unwrap()
            .data
            .owned();
        assert_eq!(&partial_cell[0usize], &cell["sub"]["sub4"]);
    }
    {
        // Selecting one deeper in nested map
        let partial_cell = chunks
            .read_selected(
                &id1,
                &key_hashes(&vec![String::from("sub|sub4|sub4sub1")]),
                false,
            )
            .unwrap()
            .data
            .owned();
        assert_eq!(&partial_cell[0usize], &cell["sub"]["sub4"]["sub4sub1"]);
    }
    {
        // Selecting one deeper nullable array in nested map
        let partial_cell = chunks
            .read_selected(
                &id1,
                &key_hashes(&vec![String::from("sub|sub4|sub4sub3")]),
                true,
            )
            .unwrap()
            .data
            .owned();
        assert_eq!(&partial_cell[0usize], &cell["sub"]["sub4"]["sub4sub3"]);
    }
    {
        // Selecting vector
        let partial_cell = chunks
            .read_selected(&id1, &key_hashes(&vec![String::from("vec1")]), true)
            .unwrap()
            .data
            .owned();
        assert_eq!(&partial_cell[0usize], &cell["vec1"]);
    }
    {
        // Selecting nullable vector
        let partial_cell = chunks
            .read_selected(&id1, &key_hashes(&vec![String::from("vec2")]), true)
            .unwrap()
            .data
            .owned();
        assert_eq!(&partial_cell[0usize], &cell["vec2"]);
    }
    {
        // Selecting multiple
        let partial_cell = chunks
            .read_selected(
                &id1,
                &key_hashes(&vec![
                    String::from("sub|sub3"),
                    String::from("sub|sub4|sub4sub3"),
                ]),
                true,
            )
            .unwrap()
            .data
            .owned();
        assert_eq!(partial_cell.len().unwrap(), 2);
        assert_eq!(partial_cell[0usize], cell["sub"]["sub3"]);
        assert_eq!(partial_cell[1usize], cell["sub"]["sub4"]["sub4sub3"]);
    }
}

fn dyn_map_value() -> OwnedValue {
    OwnedValue::Array(vec![
        data_map_value!(
            sub5sub1: OwnedValue::U32(9),
            sub5sub2: OwnedValue::PrimArray(OwnedPrimArray::U32(vec![
                10, 11
            ])),
            sub5sub3: OwnedValue::PrimArray(OwnedPrimArray::U64(vec![
                12, 13, 14
            ])),
            sub5sub4: OwnedValue::U16(15)
        ),
        data_map_value!(
            sub5sub1: OwnedValue::U32(16),
            sub5sub2: OwnedValue::PrimArray(OwnedPrimArray::U32(vec![
                17, 18, 19, 20
            ])),
            sub5sub3: OwnedValue::PrimArray(OwnedPrimArray::U64(vec![
                21, 22
            ])),
            sub5sub4: OwnedValue::U16(23)
        ),
    ])
}

#[test]
pub fn test_unified_chunk_address_space() {
    use crate::ram::chunk::{get_global_chunk_base, get_chunk_size_bits, chunk_and_segment_from_addr};
    use crate::ram::segs::SEGMENT_SIZE;
    
    let _ = env_logger::try_init();
    
    // Create chunks with unified address space
    let fields = default_fields();
    let schema = Schema::new("test", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.new_schema(schema.clone());
    
    let chunk_count = 4;
    let total_size = CHUNK_SIZE * chunk_count;
    let chunks = Chunks::new(
        chunk_count,
        total_size,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );
    
    // Verify global state is set
    let base = get_global_chunk_base();
    let size_bits = get_chunk_size_bits();
    assert!(base != 0, "Global chunk base should be set");
    assert!(size_bits > 0, "Chunk size bits should be set");
    
    info!("Global chunk base: {:#x}, size_bits: {}", base, size_bits);
    
    // Test address calculation for each chunk
    for i in 0..chunk_count {
        let chunk = &chunks.list[i];
        let segment_addr = chunk.segments()[0].addr;
        
        // Calculate chunk ID and segment ID from address
        let result = chunk_and_segment_from_addr(segment_addr);
        assert!(result.is_some(), "Address should be in range");
        
        let (chunk_id, segment_id) = result.unwrap();
        assert_eq!(chunk_id, i, "Chunk ID should match for chunk {}", i);
        assert_eq!(segment_id, 0, "First segment should have ID 0 in chunk {}", i);
        
        info!("Chunk {} segment 0 addr: {:#x} -> chunk_id={}, seg_id={}", 
              i, segment_addr, chunk_id, segment_id);
        
        // Test an address in the middle of the segment
        let mid_addr = segment_addr + SEGMENT_SIZE / 2;
        let result2 = chunk_and_segment_from_addr(mid_addr);
        assert!(result2.is_some(), "Mid-address should be in range");
        let (chunk_id2, segment_id2) = result2.unwrap();
        assert_eq!(chunk_id2, i, "Chunk ID should still match for mid-address");
        assert_eq!(segment_id2, 0, "Segment ID should still be 0 for mid-address");
    }
    
    // Test address outside range
    let invalid_addr = base - 1;
    let result = chunk_and_segment_from_addr(invalid_addr);
    assert!(result.is_none(), "Address before base should return None");
    
    // Test global segment access
    use crate::ram::chunk::get_segment_for_fault;
    for i in 0..chunk_count {
        let segment = get_segment_for_fault(i, 0);
        assert!(segment.is_some(), "Should be able to access segment via global pointer");
        let seg = segment.unwrap();
        info!("Global access: chunk {} segment 0 at addr {:#x}, id {}", i, seg.addr, seg.id);
        
        // Verify segment address is within expected chunk range
        let expected_chunk_base = base + (i * (1 << size_bits));
        assert!(seg.addr >= expected_chunk_base, 
                "Segment addr should be >= chunk base");
        assert!(seg.addr < expected_chunk_base + (1 << size_bits), 
                "Segment addr should be < chunk end");
    }
    
    // Test invalid access
    let invalid_seg = get_segment_for_fault(chunk_count + 1, 0);
    assert!(invalid_seg.is_none(), "Should return None for out-of-bounds chunk");
    
    info!("Unified chunk address space test passed!");
}
