use super::{complex_fields, default_fields, dyn_map_field, simple_fields};
use crate::index::ranged::tree::btree::page_schema;
use crate::index::ranged::tree::tree::{
    RANGED_TREE_HEAD_HASH, RANGED_TREE_MIGRATION_HASH, RANGED_TREE_SCHEMA, RANGED_TREE_SCHEMA_ID,
};
use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::schema::*;
use crate::ram::types::*;
use crate::server::ServerMeta;
use bifrost::hlc::HlcSource;
use bifrost_hasher::hash_str;
use env_logger;
use std;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

pub const CHUNK_SIZE: usize = 1 * 8 * 1024 * 1024;

#[test]
fn writes_allocate_monotonic_revision_timestamps() {
    let id = Id::new(11, 22);
    let schema = Schema::new("revision-test", None, simple_fields(), false, true);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema.clone());
    let revision_clock = Arc::new(HlcSource::new(0));
    let chunks = Chunks::new_with_clock(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
        revision_clock,
        300_000,
    );

    let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::U64(1));
    let first = chunks.write_cell(&mut cell).unwrap();
    cell.data = OwnedValue::U64(2);
    let second = chunks.update_cell(&mut cell).unwrap();

    assert!(second.revision_ts > first.revision_ts);
}

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
    schemas.debug_only_new_schema(schema.clone());
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
    schemas.debug_only_new_schema(schema.clone());
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
pub fn ranged_tree_metadata_updates_do_not_refresh_chunk_statistics() {
    let _ = env_logger::try_init();
    let tree_id = Id::new(11, 7);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(RANGED_TREE_SCHEMA.clone());
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );

    let mut first_map = OwnedMap::new();
    first_map.insert_key_id(*RANGED_TREE_HEAD_HASH, OwnedValue::Id(Id::new(100, 1)));
    first_map.insert_key_id(*RANGED_TREE_MIGRATION_HASH, OwnedValue::Null);
    let mut tree_cell =
        OwnedCell::new_with_id(*RANGED_TREE_SCHEMA_ID, &tree_id, OwnedValue::Map(first_map));
    chunks.write_cell(&mut tree_cell).unwrap();
    assert_eq!(
        chunks.list[0].statistics.changes.load(Ordering::Relaxed),
        0,
        "internal ranged-tree metadata writes should not arm chunk statistics refresh"
    );

    let mut update_map = OwnedMap::new();
    update_map.insert_key_id(*RANGED_TREE_HEAD_HASH, OwnedValue::Id(Id::new(100, 2)));
    update_map.insert_key_id(*RANGED_TREE_MIGRATION_HASH, OwnedValue::Null);
    let mut updated_tree_cell = OwnedCell::new_with_id(
        *RANGED_TREE_SCHEMA_ID,
        &tree_id,
        OwnedValue::Map(update_map),
    );
    chunks.update_cell(&mut updated_tree_cell).unwrap();
    assert_eq!(
        chunks.list[0].statistics.changes.load(Ordering::Relaxed),
        0,
        "internal ranged-tree metadata updates should not accumulate stats refresh changes"
    );
}

#[test]
pub fn ranged_btree_page_cells_do_not_build_chunk_statistics_when_forced() {
    let _ = env_logger::try_init();
    let page_schema = page_schema();
    let page_schema_id = page_schema.id;
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(page_schema);
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );

    let mut page_map = OwnedMap::new();
    page_map.insert(&String::from("next"), OwnedValue::Id(Id::unit_id()));
    page_map.insert(&String::from("prev"), OwnedValue::Id(Id::unit_id()));
    page_map.insert(
        &String::from("keys"),
        vec![SmallBytes::from_vec(vec![1u8; 32])].value(),
    );
    let page_id = Id::new(12, 1);
    let mut page_cell = OwnedCell::new_with_id(page_schema_id, &page_id, OwnedValue::Map(page_map));
    chunks.write_cell(&mut page_cell).unwrap();

    chunks.ensure_statistics();

    assert!(
        chunks.all_chunk_statistics(page_schema_id)[0].is_none(),
        "forced statistics rebuild should skip internal ranged B-tree page schemas"
    );
}

#[test]
pub fn sparse_sidecar_system_updates_do_not_refresh_chunk_statistics() {
    let _ = env_logger::try_init();
    let sidecar_schema_id = 0xF015;
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(Schema::new_with_id(
        sidecar_schema_id,
        "_sys_sparse_sidecar_matrix_fragments",
        None,
        default_fields(),
        false,
        false,
    ));
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );

    let cell_id = Id::new(100, 1);
    let mut sidecar_cell = create_test_cell(sidecar_schema_id, &cell_id, "fragment", 1);
    chunks.write_cell(&mut sidecar_cell).unwrap();
    assert_eq!(
        chunks.list[0].statistics.changes.load(Ordering::Relaxed),
        0,
        "internal sparse sidecar writes should not arm chunk statistics refresh"
    );

    let mut updated_sidecar_cell = create_test_cell(sidecar_schema_id, &cell_id, "fragment", 2);
    chunks.update_cell(&mut updated_sidecar_cell).unwrap();
    assert_eq!(
        chunks.list[0].statistics.changes.load(Ordering::Relaxed),
        0,
        "internal sparse sidecar updates should not accumulate stats refresh changes"
    );
}

#[test]
pub fn sparse_sidecar_system_cells_do_not_build_chunk_statistics_when_forced() {
    let _ = env_logger::try_init();
    let sidecar_schema_id = 0xF015;
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(Schema::new_with_id(
        sidecar_schema_id,
        "_sys_sparse_sidecar_matrix_fragments",
        None,
        default_fields(),
        false,
        false,
    ));
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );

    for offset in 0..16 {
        let cell_id = Id::new(100, 1 + offset);
        let mut sidecar_cell =
            create_test_cell(sidecar_schema_id, &cell_id, "fragment", offset as u64);
        chunks.write_cell(&mut sidecar_cell).unwrap();
    }

    chunks.ensure_statistics();

    assert!(
        chunks.all_chunk_statistics(sidecar_schema_id)[0].is_none(),
        "forced statistics rebuild should skip internal sparse sidecar schemas"
    );
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
    schemas.debug_only_new_schema(schema.clone());
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
    schemas.debug_only_new_schema(schema.clone());
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
    use crate::ram::chunk::{
        chunk_and_segment_from_addr, get_chunk_size_bits, get_global_chunk_base,
    };
    use crate::ram::segs::SEGMENT_SIZE;

    let _ = env_logger::try_init();

    // Create chunks with unified address space
    let fields = default_fields();
    let schema = Schema::new("test", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema.clone());

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
        assert_eq!(
            segment_id, 0,
            "First segment should have ID 0 in chunk {}",
            i
        );

        info!(
            "Chunk {} segment 0 addr: {:#x} -> chunk_id={}, seg_id={}",
            i, segment_addr, chunk_id, segment_id
        );

        // Test an address in the middle of the segment
        let mid_addr = segment_addr + SEGMENT_SIZE / 2;
        let result2 = chunk_and_segment_from_addr(mid_addr);
        assert!(result2.is_some(), "Mid-address should be in range");
        let (chunk_id2, segment_id2) = result2.unwrap();
        assert_eq!(chunk_id2, i, "Chunk ID should still match for mid-address");
        assert_eq!(
            segment_id2, 0,
            "Segment ID should still be 0 for mid-address"
        );
    }

    // Test address outside range
    let invalid_addr = base - 1;
    let result = chunk_and_segment_from_addr(invalid_addr);
    assert!(result.is_none(), "Address before base should return None");

    // Test global segment access
    use crate::ram::chunk::get_segment_for_fault;
    for i in 0..chunk_count {
        let segment = get_segment_for_fault(i, 0);
        assert!(
            segment.is_some(),
            "Should be able to access segment via global pointer"
        );
        let seg = segment.unwrap();
        info!(
            "Global access: chunk {} segment 0 at addr {:#x}, id {}",
            i, seg.addr, seg.id
        );

        // Verify segment address is within expected chunk range
        let expected_chunk_base = base + (i * (1 << size_bits));
        assert!(
            seg.addr >= expected_chunk_base,
            "Segment addr should be >= chunk base"
        );
        assert!(
            seg.addr < expected_chunk_base + (1 << size_bits),
            "Segment addr should be < chunk end"
        );
    }

    // Test invalid access
    let invalid_seg = get_segment_for_fault(chunk_count + 1, 0);
    assert!(
        invalid_seg.is_none(),
        "Should return None for out-of-bounds chunk"
    );

    info!("Unified chunk address space test passed!");
}

#[test]
fn test_wal_cleanup_after_archive() {
    use std::path::Path;
    use tempfile::TempDir;

    let _ = env_logger::try_init();

    // Create temporary directories for WAL and backup storage
    let wal_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    let wal_path = wal_dir.path().to_str().unwrap().to_string();
    let backup_path = backup_dir.path().to_str().unwrap().to_string();

    info!("WAL directory: {}", wal_path);
    info!("Backup directory: {}", backup_path);

    // Create chunks with WAL and backup storage
    let fields = default_fields();
    let schema = Schema::new("test_wal", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema.clone());

    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None, // index_builder
        Some(backup_path.clone()),
        Some(wal_path.clone()),
        None, // tiered_config
    );

    // Create a cell to write to the segment
    let cell_id = Id::new(1, 1);
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(100));
    data_map.insert(&String::from("score"), OwnedValue::U64(50));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String("Test".to_string()),
    );
    let data = OwnedValue::Map(data_map);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &cell_id),
        data,
    };

    // Write cell to create a segment with WAL file
    chunks.write_cell(&mut cell).unwrap();
    info!("Cell written, WAL file should be created");

    // Get the segment
    let chunk = &chunks.list[0];
    let seg_ids = chunk.segment_ids();
    assert!(!seg_ids.is_empty(), "Should have at least one segment");

    let first_seg_id = seg_ids[0];
    let segment = chunk.segs.get(&first_seg_id).unwrap();

    // Verify WAL file exists
    let wal_file_name = chunk
        .file_manager
        .wal_path(segment.chunk_id, segment.id, segment.seq_id)
        .expect("Segment should have WAL file");
    assert!(
        Path::new(&wal_file_name).exists(),
        "WAL file should exist: {}",
        wal_file_name
    );
    info!("WAL file exists: {}", wal_file_name);

    // Get the expected backup file name using file_manager
    let backup_file_name = chunk
        .file_manager
        .backup_path(segment.chunk_id, segment.id, segment.seq_id)
        .expect("Should have backup file path");
    info!("Expected backup file: {}", backup_file_name);

    // Archive the segment
    info!("Archiving segment...");
    let result = segment.archive().unwrap();
    assert!(result, "Archive should succeed");

    // Verify backup file exists
    assert!(
        Path::new(&backup_file_name).exists(),
        "Backup file should exist: {}",
        backup_file_name
    );
    info!("Backup file created: {}", backup_file_name);

    // CRITICAL TEST: Verify WAL file is deleted
    assert!(
        !Path::new(&wal_file_name).exists(),
        "WAL file should be deleted after archive: {}",
        wal_file_name
    );
    info!("✓ WAL file successfully deleted after archive");

    // Verify both files are not present at the same time
    let wal_exists = Path::new(&wal_file_name).exists();
    let backup_exists = Path::new(&backup_file_name).exists();
    assert!(
        !wal_exists && backup_exists,
        "Only backup should exist, not WAL. WAL={}, Backup={}",
        wal_exists,
        backup_exists
    );

    info!("WAL cleanup after archive test passed!");
}

#[test]
fn test_wal_file_handle_properly_closed() {
    use std::fs::OpenOptions;
    use std::path::Path;
    use tempfile::TempDir;

    let _ = env_logger::try_init();

    // Create temporary directories
    let wal_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    let wal_path = wal_dir.path().to_str().unwrap().to_string();
    let backup_path = backup_dir.path().to_str().unwrap().to_string();

    // Create chunks with storage
    let fields = default_fields();
    let schema = Schema::new("test_handle", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema.clone());

    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None, // index_builder
        Some(backup_path),
        Some(wal_path),
        None, // tiered_config
    );

    // Write some data
    let cell_id = Id::new(1, 1);
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(200));
    data_map.insert(&String::from("score"), OwnedValue::U64(60));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String("Test2".to_string()),
    );
    let data = OwnedValue::Map(data_map);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &cell_id),
        data,
    };

    chunks.write_cell(&mut cell).unwrap();

    // Get segment and file path
    let chunk = &chunks.list[0];
    let seg_ids = chunk.segment_ids();
    let segment = chunk.segs.get(&seg_ids[0]).unwrap();
    let wal_file_path = chunk
        .file_manager
        .wal_path(segment.chunk_id, segment.id, segment.seq_id)
        .expect("Should have WAL file path");

    info!("Testing file handle closure for: {}", wal_file_path);

    // Archive the segment
    segment.archive().unwrap();

    // Try to open the WAL file path (should fail because it's deleted)
    let open_result = OpenOptions::new().read(true).open(&wal_file_path);
    assert!(
        open_result.is_err(),
        "WAL file should not exist and cannot be opened"
    );

    // Try to create a new file with the same name (should succeed, proving file is closed and deleted)
    let create_result = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&wal_file_path);
    assert!(
        create_result.is_ok(),
        "Should be able to create new file at WAL path, proving old file is closed and deleted"
    );

    // Clean up the test file
    std::fs::remove_file(&wal_file_path).ok();

    info!("✓ WAL file handle properly closed and file deleted");
}

#[test]
fn test_multiple_segments_wal_cleanup() {
    use std::path::Path;
    use tempfile::TempDir;

    let _ = env_logger::try_init();

    // Create temporary directories
    let wal_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    let wal_path = wal_dir.path().to_str().unwrap().to_string();
    let backup_path = backup_dir.path().to_str().unwrap().to_string();

    // Create chunks
    let fields = default_fields();
    let schema = Schema::new("test_multi", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema.clone());

    let chunks = Chunks::new(
        1,
        CHUNK_SIZE * 2, // Larger to fit multiple segments
        Arc::new(ServerMeta { schemas }),
        None, // index_builder
        Some(backup_path),
        Some(wal_path.clone()),
        None, // tiered_config
    );

    // Create enough data to fill multiple segments
    let mut cell_ids = Vec::new();
    for i in 0..100 {
        let cell_id = Id::new(1, i);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        data_map.insert(&String::from("score"), OwnedValue::U64(i));
        // Add large data to fill segments faster
        data_map.insert(&String::from("name"), OwnedValue::String("x".repeat(50000)));
        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &cell_id),
            data,
        };
        chunks.write_cell(&mut cell).unwrap();
        cell_ids.push(cell_id);
    }

    let chunk = &chunks.list[0];
    let seg_ids = chunk.segment_ids();
    info!("Created {} segments", seg_ids.len());

    let mut wal_files = Vec::new();
    let mut backup_files = Vec::new();

    // Collect WAL and backup file names
    for seg_id in &seg_ids {
        if let Some(segment) = chunk.segs.get(seg_id) {
            if let Some(wal_name) =
                chunk
                    .file_manager
                    .wal_path(segment.chunk_id, segment.id, segment.seq_id)
            {
                wal_files.push(wal_name);
            }
            if let Some(backup_name) =
                chunk
                    .file_manager
                    .backup_path(segment.chunk_id, segment.id, segment.seq_id)
            {
                backup_files.push(backup_name);
            }
        }
    }

    info!(
        "Found {} WAL files and {} backup file paths",
        wal_files.len(),
        backup_files.len()
    );

    // Archive all segments
    for seg_id in &seg_ids {
        if let Some(segment) = chunk.segs.get(seg_id) {
            // Skip if already archived
            if !segment.is_dirty() {
                continue;
            }

            match segment.archive() {
                Ok(true) => {
                    info!("Archived segment {}", seg_id);
                }
                Ok(false) => {
                    info!("Segment {} already archived or skipped", seg_id);
                }
                Err(e) => {
                    error!("Failed to archive segment {}: {:?}", seg_id, e);
                }
            }
        }
    }

    // Verify WAL files are deleted
    let mut wal_deleted_count = 0;
    let mut backup_created_count = 0;

    for wal_file in &wal_files {
        if !Path::new(wal_file).exists() {
            wal_deleted_count += 1;
        }
    }

    for backup_file in &backup_files {
        if Path::new(backup_file).exists() {
            backup_created_count += 1;
        }
    }

    info!(
        "✓ {} WAL files deleted, {} backup files created",
        wal_deleted_count, backup_created_count
    );

    // At least some WAL files should be deleted (head segment might still have WAL)
    assert!(
        wal_deleted_count > 0,
        "At least some WAL files should be deleted"
    );
    assert!(
        backup_created_count > 0,
        "At least some backup files should be created"
    );

    info!("Multiple segments WAL cleanup test passed!");
}

#[test]
fn test_archive_idempotency() {
    use std::path::Path;
    use tempfile::TempDir;

    let _ = env_logger::try_init();

    // Create temporary directories
    let wal_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    let wal_path = wal_dir.path().to_str().unwrap().to_string();
    let backup_path = backup_dir.path().to_str().unwrap().to_string();

    // Create chunks
    let fields = default_fields();
    let schema = Schema::new("test_idempotent", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema.clone());

    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None, // index_builder
        Some(backup_path),
        Some(wal_path),
        None, // tiered_config
    );

    // Write data
    let cell_id = Id::new(1, 1);
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(300));
    data_map.insert(&String::from("score"), OwnedValue::U64(70));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String("Test3".to_string()),
    );
    let data = OwnedValue::Map(data_map);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &cell_id),
        data,
    };

    chunks.write_cell(&mut cell).unwrap();

    // Get segment
    let chunk = &chunks.list[0];
    let seg_ids = chunk.segment_ids();
    let segment = chunk.segs.get(&seg_ids[0]).unwrap();

    let wal_file = chunk
        .file_manager
        .wal_path(segment.chunk_id, segment.id, segment.seq_id)
        .expect("Should have WAL file path");
    let backup_file = chunk
        .file_manager
        .backup_path(segment.chunk_id, segment.id, segment.seq_id)
        .expect("Should have backup file path");

    // First archive
    let result1 = segment.archive().unwrap();
    assert!(result1, "First archive should succeed");
    assert!(
        !Path::new(&wal_file).exists(),
        "WAL should be deleted after first archive"
    );
    assert!(
        Path::new(&backup_file).exists(),
        "Backup should exist after first archive"
    );

    // Second archive attempt (should be idempotent)
    let result2 = segment.archive().unwrap();
    assert!(
        result2,
        "Second archive should return true (repeated archive)"
    );
    assert!(!Path::new(&wal_file).exists(), "WAL should still not exist");
    assert!(
        Path::new(&backup_file).exists(),
        "Backup should still exist"
    );

    info!("✓ Archive is idempotent - multiple calls are safe");
}

#[test]
fn test_multiple_segments_in_chunk() {
    use std::sync::Arc as StdArc;
    use std::thread;

    let _ = env_logger::try_init();

    info!("Testing multiple segments in a chunk with concurrent writes");

    // Create chunks with a small chunk size to force multiple segments
    let fields = default_fields();
    let schema = Schema::new("test_multi_seg", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema.clone());

    // Use a chunk size that can hold multiple segments
    // SEGMENT_SIZE is 8MB, each cell is ~50KB, so 160 cells = 8MB
    // We want to trigger 3-4 segments, so need ~480-640 cells
    let chunk_size = CHUNK_SIZE * 8; // 64MB chunk to hold multiple 8MB segments
    let chunks = StdArc::new(Chunks::new(
        1,
        chunk_size,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    ));

    info!("Created chunks with size {}", chunk_size);

    // Spawn multiple threads to write cells concurrently to trigger segment allocation
    // We need to fill up a segment to trigger allocation of a new one
    // SEGMENT_SIZE is typically 8MB, so we need enough cells to fill that
    let num_threads = 8;
    let cells_per_thread = 60; // 8 * 60 * ~50KB = 24MB, enough for 3 segments
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let chunks_clone = StdArc::clone(&chunks);
        let schema_id = schema.id;

        let handle = thread::spawn(move || {
            for i in 0..cells_per_thread {
                let cell_id = Id::new(1, thread_id * cells_per_thread + i);
                let mut data_map = OwnedMap::new();
                data_map.insert(
                    &String::from("id"),
                    OwnedValue::I64((thread_id * cells_per_thread + i) as i64),
                );
                data_map.insert(&String::from("score"), OwnedValue::U64(i));
                // Add large data to fill segments faster and trigger multiple segment allocations
                data_map.insert(&String::from("name"), OwnedValue::String("x".repeat(50000)));
                let data = OwnedValue::Map(data_map);
                let mut cell = OwnedCell {
                    header: CellHeader::new(schema_id, &cell_id),
                    data,
                };

                match chunks_clone.write_cell(&mut cell) {
                    Ok(_) => {
                        trace!("Thread {} wrote cell {}", thread_id, i);
                    }
                    Err(e) => {
                        error!("Thread {} failed to write cell {}: {:?}", thread_id, i, e);
                        panic!("Write failed: {:?}", e);
                    }
                }
            }
            info!(
                "Thread {} completed writing {} cells",
                thread_id, cells_per_thread
            );
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    info!("All threads completed successfully");

    // Verify we have multiple segments
    let chunk = &chunks.list[0];
    let seg_ids = chunk.segment_ids();
    info!("Created {} segments: {:?}", seg_ids.len(), seg_ids);

    assert!(
        seg_ids.len() > 1,
        "Should have created multiple segments, but only have {}",
        seg_ids.len()
    );

    // Verify all segments are accessible
    for seg_id in &seg_ids {
        let segment = chunk.segs.get(seg_id);
        assert!(segment.is_some(), "Segment {} should be accessible", seg_id);
        info!("Segment {} is accessible", seg_id);
    }

    // Verify head_seg_id points to a valid segment
    let head_seg_id = chunk.get_head_seg_id();
    info!("Head segment id: {}", head_seg_id);
    let head_seg = chunk.segs.get(&(head_seg_id as usize));
    assert!(
        head_seg.is_some(),
        "Head segment {} should be accessible. Available segments: {:?}",
        head_seg_id,
        seg_ids
    );

    info!("✓ Multiple segments test passed!");
}

#[test]
fn test_concurrent_segment_allocation_and_cleanup() {
    use crate::ram::cleaner::Cleaner;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::sync::Arc as StdArc;
    use std::thread;
    use std::time::Duration;

    let _ = env_logger::try_init();

    info!("Testing concurrent segment allocation and cleanup to stress test head_seg_id race conditions");

    // Create chunks with moderate size
    let fields = default_fields();
    let schema = Schema::new("test_race", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema.clone());

    let chunk_size = CHUNK_SIZE * 16; // 128MB to accommodate many segments
    let chunks = StdArc::new(Chunks::new(
        1,
        chunk_size,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    ));

    info!("Created chunks with size {}", chunk_size);

    let stop_flag = StdArc::new(AtomicBool::new(false));
    let chunks_for_cleaner = StdArc::clone(&chunks);
    let stop_flag_for_cleaner = StdArc::clone(&stop_flag);

    // Start a cleaner thread that aggressively runs GC
    let cleaner_handle = thread::spawn(move || {
        let mut iteration = 0;
        while !stop_flag_for_cleaner.load(AtomicOrdering::Relaxed) {
            iteration += 1;
            if iteration % 10 == 0 {
                debug!("Cleaner iteration {}", iteration);
            }
            // Run partial GC to trigger segment cleanup
            let _ = Cleaner::clean(&chunks_for_cleaner.list[0], false, false);
            // Small sleep to let writers make progress
            thread::sleep(Duration::from_micros(100));
        }
        info!("Cleaner thread completed {} iterations", iteration);
    });

    // Spawn multiple writer threads that continuously write and delete cells
    let num_threads = 8;
    let cells_per_thread = 150;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let chunks_clone = StdArc::clone(&chunks);
        let schema_id = schema.id;

        let handle = thread::spawn(move || {
            for i in 0..cells_per_thread {
                let cell_id = Id::new(1, thread_id * cells_per_thread + i);
                let mut data_map = OwnedMap::new();
                data_map.insert(
                    &String::from("id"),
                    OwnedValue::I64((thread_id * cells_per_thread + i) as i64),
                );
                data_map.insert(&String::from("score"), OwnedValue::U64(i));
                // Add large data to fill segments faster
                data_map.insert(&String::from("name"), OwnedValue::String("x".repeat(50000)));
                let data = OwnedValue::Map(data_map);
                let mut cell = OwnedCell {
                    header: CellHeader::new(schema_id, &cell_id),
                    data,
                };

                match chunks_clone.write_cell(&mut cell) {
                    Ok(_) => {
                        trace!("Thread {} wrote cell {}", thread_id, i);

                        // Occasionally delete old cells to create dead space for cleaner
                        if i > 10 && i % 5 == 0 {
                            let old_cell_id = Id::new(1, thread_id * cells_per_thread + (i - 10));
                            let _ = chunks_clone.remove_cell(&old_cell_id);
                            trace!("Thread {} deleted cell {}", thread_id, i - 10);
                        }
                    }
                    Err(e) => {
                        error!("Thread {} failed to write cell {}: {:?}", thread_id, i, e);
                        panic!("Write failed: {:?}", e);
                    }
                }
            }
            info!(
                "Thread {} completed writing {} cells",
                thread_id, cells_per_thread
            );
        });

        handles.push(handle);
    }

    // Wait for all writer threads to complete
    for handle in handles {
        handle.join().expect("Writer thread panicked");
    }

    info!("All writer threads completed successfully");

    // Stop the cleaner thread
    stop_flag.store(true, AtomicOrdering::Relaxed);
    cleaner_handle.join().expect("Cleaner thread panicked");

    info!("Cleaner thread stopped");

    // Verify we have multiple segments
    let chunk = &chunks.list[0];
    let seg_ids = chunk.segment_ids();
    info!("Final state: {} segments: {:?}", seg_ids.len(), seg_ids);

    // Verify head_seg_id points to a valid segment
    let head_seg_id = chunk.get_head_seg_id();
    info!("Head segment id: {}", head_seg_id);
    let head_seg = chunk.segs.get(&(head_seg_id as usize));
    assert!(
        head_seg.is_some(),
        "Head segment {} should be accessible. Available segments: {:?}",
        head_seg_id,
        seg_ids
    );

    info!("✓ Concurrent segment allocation and cleanup test passed!");
}

// ============================================================================
// Helper functions to reduce test boilerplate
// ============================================================================

fn setup_test_chunks() -> (Arc<Chunks>, Schema) {
    let fields = default_fields();
    let schema = Schema::new("test", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );
    (chunks, schema)
}

fn create_test_cell(schema_id: u32, id: &Id, name: &str, score: u64) -> OwnedCell {
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(100));
    data_map.insert(&String::from("score"), OwnedValue::U64(score));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from(name)),
    );
    OwnedCell {
        header: CellHeader::new(schema_id, id),
        data: OwnedValue::Map(data_map),
    }
}

// ============================================================================
// Basic success/failure tests
// ============================================================================

#[test]
pub fn test_compare_revision_and_update_cell_success() {
    let _ = env_logger::try_init();
    let (chunks, schema) = setup_test_chunks();
    let id = Id::new(1, 1);

    // Write initial cell
    let mut cell = create_test_cell(schema.id, &id, "Alice", 70);
    let header = chunks.write_cell(&mut cell).unwrap();
    let initial_version = header.revision_ts;

    // Verify initial state (drop the guard before attempting update)
    {
        let stored_cell = chunks.read_cell(&id).unwrap();
        assert_eq!(stored_cell.header.revision_ts, initial_version);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Alice");
    }

    // Update with matching version - should succeed
    let mut updated_cell = create_test_cell(schema.id, &id, "Bob", 85);
    let result = chunks.compare_revision_and_update_cell(&id, initial_version, &mut updated_cell);
    assert!(
        result.is_ok(),
        "Update should succeed with matching version"
    );

    // Verify the update was applied atomically
    let stored_cell = chunks.read_cell(&id).unwrap();
    assert_eq!(stored_cell.data["name"].string().unwrap(), "Bob");
    assert_eq!(stored_cell.data["score"].u64().unwrap(), &85);
    assert!(stored_cell.header.revision_ts > initial_version);
}

#[test]
pub fn test_compare_revision_and_update_cell_version_mismatch() {
    let _ = env_logger::try_init();
    let (chunks, schema) = setup_test_chunks();
    let id = Id::new(1, 1);

    // Write initial cell
    let mut cell = create_test_cell(schema.id, &id, "Alice", 70);
    let header = chunks.write_cell(&mut cell).unwrap();
    let initial_version = header.revision_ts;

    // Try to update with FUTURE version (higher than current) - should fail
    let mut updated_cell = create_test_cell(schema.id, &id, "Bob", 85);
    let result =
        chunks.compare_revision_and_update_cell(&id, initial_version + 1, &mut updated_cell);
    assert!(result.is_err(), "Update should fail with future version");
    assert_eq!(result.unwrap_err(), WriteError::CellRevisionMismatch);

    // Verify the cell was NOT updated (atomicity)
    let stored_cell = chunks.read_cell(&id).unwrap();
    assert_eq!(stored_cell.data["name"].string().unwrap(), "Alice");
    assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
    assert_eq!(stored_cell.header.revision_ts, initial_version);
}

#[test]
pub fn test_compare_revision_and_update_cell_stale_version() {
    let _ = env_logger::try_init();
    let (chunks, schema) = setup_test_chunks();
    let id = Id::new(1, 1);

    // Write initial cell
    let mut cell = create_test_cell(schema.id, &id, "Alice", 70);
    let header = chunks.write_cell(&mut cell).unwrap();
    let initial_version = header.revision_ts;

    // Perform a successful update to increment version
    let mut updated_cell = create_test_cell(schema.id, &id, "Bob", 80);
    chunks
        .compare_revision_and_update_cell(&id, initial_version, &mut updated_cell)
        .unwrap();

    // Now try to update with STALE version (the old initial_version)
    let mut stale_cell = create_test_cell(schema.id, &id, "Charlie", 90);
    let result = chunks.compare_revision_and_update_cell(&id, initial_version, &mut stale_cell);
    assert!(result.is_err(), "Update should fail with stale version");
    assert_eq!(result.unwrap_err(), WriteError::CellRevisionMismatch);

    // Verify the cell still has Bob's data (stale update was rejected)
    let stored_cell = chunks.read_cell(&id).unwrap();
    assert_eq!(stored_cell.data["name"].string().unwrap(), "Bob");
    assert_eq!(stored_cell.data["score"].u64().unwrap(), &80);
    assert!(stored_cell.header.revision_ts > initial_version);
}

#[test]
pub fn test_compare_revision_and_set_field_success() {
    let _ = env_logger::try_init();
    let (chunks, schema) = setup_test_chunks();
    let id = Id::new(1, 1);

    // Write initial cell
    let mut cell = create_test_cell(schema.id, &id, "Alice", 70);
    let header = chunks.write_cell(&mut cell).unwrap();
    let initial_version = header.revision_ts;

    // Update field with matching version - should succeed
    let score_hash = hash_str("score");
    let result = chunks.compare_revision_and_set_field(
        &id,
        initial_version,
        score_hash,
        OwnedValue::U64(95),
    );
    assert!(
        result.is_ok(),
        "Field update should succeed with matching version"
    );

    // Verify the update was applied atomically
    let stored_cell = chunks.read_cell(&id).unwrap();
    assert_eq!(
        stored_cell.data["score"].u64().unwrap(),
        &95,
        "Score should be updated"
    );
    assert_eq!(
        stored_cell.data["name"].string().unwrap(),
        "Alice",
        "Name should remain unchanged"
    );
    assert!(stored_cell.header.revision_ts > initial_version);
}

// ============================================================================
// Sequential updates test - multiple successful updates with version tracking
// ============================================================================

#[test]
pub fn test_compare_revision_sequential_updates() {
    let _ = env_logger::try_init();
    let (chunks, schema) = setup_test_chunks();
    let id = Id::new(1, 1);

    // Write initial cell
    let mut cell = create_test_cell(schema.id, &id, "v1", 10);
    let header = chunks.write_cell(&mut cell).unwrap();
    let mut current_version = header.revision_ts;

    // Perform multiple sequential updates, each tracking the new version
    for i in 2..=5 {
        let mut updated_cell = create_test_cell(schema.id, &id, &format!("v{}", i), i * 10);
        let result =
            chunks.compare_revision_and_update_cell(&id, current_version, &mut updated_cell);
        assert!(result.is_ok(), "Update {} should succeed", i);
        current_version = result.unwrap().revision_ts;
    }

    // Verify final state
    let stored_cell = chunks.read_cell(&id).unwrap();
    assert_eq!(stored_cell.data["name"].string().unwrap(), "v5");
    assert_eq!(stored_cell.data["score"].u64().unwrap(), &50);
    assert_eq!(stored_cell.header.revision_ts, current_version);
}

// ============================================================================
// Optimistic retry pattern test - demonstrates real-world usage
// ============================================================================

#[test]
pub fn test_compare_revision_optimistic_retry_pattern() {
    let _ = env_logger::try_init();
    let (chunks, schema) = setup_test_chunks();
    let id = Id::new(1, 1);

    // Write initial cell with score=100
    let mut cell = create_test_cell(schema.id, &id, "Player", 100);
    chunks.write_cell(&mut cell).unwrap();

    // Simulate an optimistic update: read-modify-write with retry on conflict
    // This is the typical pattern users should follow
    let mut retries = 0;
    let max_retries = 3;

    loop {
        // Step 1: Read current state
        let (current_score, current_version) = {
            let stored = chunks.read_cell(&id).unwrap();
            (
                *stored.data["score"].u64().unwrap(),
                stored.header.revision_ts,
            )
        };

        // Step 2: Compute new value (increment score by 10)
        let new_score = current_score + 10;
        let mut updated_cell = create_test_cell(schema.id, &id, "Player", new_score);

        // Step 3: Attempt compare-and-swap
        match chunks.compare_revision_and_update_cell(&id, current_version, &mut updated_cell) {
            Ok(_) => break, // Success!
            Err(WriteError::CellRevisionMismatch) => {
                retries += 1;
                if retries >= max_retries {
                    panic!("Too many retries");
                }
                // Retry with fresh read
                continue;
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    // Verify the update was applied
    let stored_cell = chunks.read_cell(&id).unwrap();
    assert_eq!(stored_cell.data["score"].u64().unwrap(), &110);
    assert_eq!(retries, 0, "Should succeed on first try (no contention)");
}

#[test]
pub fn test_compare_revision_and_set_field_version_mismatch() {
    let _ = env_logger::try_init();
    let (chunks, schema) = setup_test_chunks();
    let id = Id::new(1, 1);

    // Write initial cell
    let mut cell = create_test_cell(schema.id, &id, "Alice", 70);
    let header = chunks.write_cell(&mut cell).unwrap();
    let initial_version = header.revision_ts;

    // Try to update field with wrong version - should fail
    let score_hash = hash_str("score");
    let result = chunks.compare_revision_and_set_field(
        &id,
        initial_version + 1,
        score_hash,
        OwnedValue::U64(95),
    );
    assert!(
        result.is_err(),
        "Field update should fail with mismatched version"
    );
    assert_eq!(result.unwrap_err(), WriteError::CellRevisionMismatch);

    // Verify the field was NOT updated (atomicity)
    let stored_cell = chunks.read_cell(&id).unwrap();
    assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
    assert_eq!(stored_cell.data["name"].string().unwrap(), "Alice");
    assert_eq!(stored_cell.header.revision_ts, initial_version);
}

// ============================================================================
// Concurrent atomicity tests - verify only one concurrent update succeeds
// ============================================================================

#[test]
pub fn test_compare_revision_and_update_cell_concurrent_atomicity() {
    let _ = env_logger::try_init();
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    let (chunks, schema) = setup_test_chunks();
    let id = Id::new(1, 1);

    // Write initial cell
    let mut cell = create_test_cell(schema.id, &id, "Initial", 0);
    let header = chunks.write_cell(&mut cell).unwrap();
    let initial_version = header.revision_ts;

    let num_threads = 5;
    let barrier = Arc::new(Barrier::new(num_threads));
    let success_count = Arc::new(AtomicU64::new(0));
    let failure_count = Arc::new(AtomicU64::new(0));
    let winner_id = Arc::new(AtomicU64::new(u64::MAX));

    // Spawn threads that all try to update with the same version simultaneously.
    // Using a barrier ensures true concurrency (all threads start at the same time).
    // Only ONE should succeed; others fail with CellRevisionMismatch.
    let mut handles = vec![];
    for i in 0..num_threads {
        let chunks_clone = chunks.clone();
        let barrier_clone = barrier.clone();
        let success_clone = success_count.clone();
        let failure_clone = failure_count.clone();
        let winner_clone = winner_id.clone();

        let handle = thread::spawn(move || {
            // Wait for all threads to be ready
            barrier_clone.wait();

            let mut updated_cell =
                create_test_cell(schema.id, &id, &format!("Thread{}", i), i as u64);
            match chunks_clone.compare_revision_and_update_cell(
                &id,
                initial_version,
                &mut updated_cell,
            ) {
                Ok(_) => {
                    success_clone.fetch_add(1, Ordering::SeqCst);
                    winner_clone.store(i as u64, Ordering::SeqCst);
                }
                Err(WriteError::CellRevisionMismatch) => {
                    failure_clone.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread should complete");
    }

    // Exactly one update should succeed
    assert_eq!(
        success_count.load(Ordering::SeqCst),
        1,
        "Exactly one update should succeed"
    );
    assert_eq!(
        failure_count.load(Ordering::SeqCst),
        4,
        "Four updates should fail"
    );

    // Verify the winner's data is stored
    let winner = winner_id.load(Ordering::SeqCst);
    let stored_cell = chunks.read_cell(&id).unwrap();
    assert_eq!(stored_cell.data["score"].u64().unwrap(), &winner);
    assert_eq!(
        stored_cell.data["name"].string().unwrap(),
        format!("Thread{}", winner)
    );
    assert!(stored_cell.header.revision_ts > initial_version);
}

#[test]
pub fn test_compare_revision_and_set_field_concurrent_atomicity() {
    let _ = env_logger::try_init();
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    let (chunks, schema) = setup_test_chunks();
    let id = Id::new(1, 1);

    // Write initial cell
    let mut cell = create_test_cell(schema.id, &id, "Initial", 0);
    let header = chunks.write_cell(&mut cell).unwrap();
    let initial_version = header.revision_ts;

    let num_threads = 5;
    let barrier = Arc::new(Barrier::new(num_threads));
    let success_count = Arc::new(AtomicU64::new(0));
    let failure_count = Arc::new(AtomicU64::new(0));
    let winning_score = Arc::new(AtomicU64::new(u64::MAX));
    let score_hash = hash_str("score");

    // Spawn threads that all try to set the same field simultaneously.
    // Only ONE should succeed; others fail with CellRevisionMismatch.
    let mut handles = vec![];
    for i in 0..num_threads {
        let chunks_clone = chunks.clone();
        let barrier_clone = barrier.clone();
        let success_clone = success_count.clone();
        let failure_clone = failure_count.clone();
        let winning_clone = winning_score.clone();

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            match chunks_clone.compare_revision_and_set_field(
                &id,
                initial_version,
                score_hash,
                OwnedValue::U64(i as u64),
            ) {
                Ok(_) => {
                    success_clone.fetch_add(1, Ordering::SeqCst);
                    winning_clone.store(i as u64, Ordering::SeqCst);
                }
                Err(WriteError::CellRevisionMismatch) => {
                    failure_clone.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread should complete");
    }

    // Exactly one update should succeed
    assert_eq!(
        success_count.load(Ordering::SeqCst),
        1,
        "Exactly one field update should succeed"
    );
    assert_eq!(
        failure_count.load(Ordering::SeqCst),
        4,
        "Four field updates should fail"
    );

    // Verify the winner's score is stored, and name is unchanged
    let winner = winning_score.load(Ordering::SeqCst);
    let stored_cell = chunks.read_cell(&id).unwrap();
    assert_eq!(stored_cell.data["score"].u64().unwrap(), &winner);
    assert_eq!(
        stored_cell.data["name"].string().unwrap(),
        "Initial",
        "Name should remain unchanged"
    );
    assert!(stored_cell.header.revision_ts > initial_version);
}

// ============================================================================
// Non-existent cell tests
// ============================================================================

#[test]
pub fn test_compare_revision_and_update_cell_nonexistent_cell() {
    let _ = env_logger::try_init();
    let (chunks, schema) = setup_test_chunks();
    let id = Id::new(1, 999); // Non-existent cell

    let mut cell = create_test_cell(schema.id, &id, "Test", 70);
    let result = chunks.compare_revision_and_update_cell(&id, 1, &mut cell);

    assert!(result.is_err(), "Update should fail for non-existent cell");
    match result.unwrap_err() {
        WriteError::ReadError(ReadError::CellDoesNotExisted) => {}
        e => panic!("Expected CellDoesNotExisted, got {:?}", e),
    }
}

#[test]
pub fn test_compare_revision_and_set_field_nonexistent_cell() {
    let _ = env_logger::try_init();
    let (chunks, _schema) = setup_test_chunks();
    let id = Id::new(1, 999); // Non-existent cell

    let score_hash = hash_str("score");
    let result = chunks.compare_revision_and_set_field(&id, 1, score_hash, OwnedValue::U64(100));

    assert!(
        result.is_err(),
        "Field update should fail for non-existent cell"
    );
    match result.unwrap_err() {
        WriteError::ReadError(ReadError::CellDoesNotExisted) => {}
        e => panic!("Expected CellDoesNotExisted, got {:?}", e),
    }
}
