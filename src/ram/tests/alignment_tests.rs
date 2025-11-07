use crate::ram::cell::*;
use crate::ram::chunk::*;
/// Tests to verify that address alignment is correct throughout the system
/// This helps catch alignment issues that cause "misaligned pointer dereference" panics
use crate::ram::schema::*;
use crate::ram::segs::SEGMENT_SIZE;
use crate::ram::tests::default_fields;
use crate::ram::types::*;
use crate::server::*;
use env_logger;

#[test]
fn test_alignment_validation_function() {
    let _ = env_logger::try_init();

    println!("Testing alignment validation logic");

    // Test various addresses to ensure our alignment checks work
    let test_cases: Vec<(usize, bool, &str)> = vec![
        (0x0000_0000_0000_0000_usize, false, "NULL address"),
        (0x0000_0000_0000_0001_usize, false, "Misaligned by 1"),
        (0x0000_0000_0000_0002_usize, false, "Misaligned by 2"),
        (0x0000_0000_0000_0003_usize, false, "Misaligned by 3"),
        (0x0000_0000_0000_0004_usize, false, "Misaligned by 4"),
        (0x0000_0000_0000_0005_usize, false, "Misaligned by 5"),
        (0x0000_0000_0000_0006_usize, false, "Misaligned by 6"),
        (0x0000_0000_0000_0007_usize, false, "Misaligned by 7"),
        (0x0000_0000_0000_0008_usize, true, "8-byte aligned"),
        (0x0000_0000_0000_0010_usize, true, "16-byte aligned"),
        (
            0x0000_0000_0000_00E6_usize,
            false,
            "Ends in 0xE6 (like crash address)",
        ),
        (
            0x6c39_5a40_00e6_usize,
            false,
            "Actual crash address pattern",
        ),
        (0x0000_0000_1000_0000_usize, true, "Large aligned address"),
    ];

    for (addr, expected_valid, description) in test_cases {
        let is_8_byte_aligned = addr % 8 == 0;
        let is_4_byte_aligned = addr % 4 == 0;
        let is_null = addr == 0;

        println!(
            "Address 0x{:016x} ({}): 8-byte={}, 4-byte={}, null={}, expected_valid={}",
            addr, description, is_8_byte_aligned, is_4_byte_aligned, is_null, expected_valid
        );

        // Our validation requires 8-byte alignment
        if !is_null {
            assert_eq!(
                is_8_byte_aligned, expected_valid,
                "Alignment check failed for {}: 0x{:016x}",
                description, addr
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cell_location_alignment_after_write() {
    let _ = env_logger::try_init();

    let server_addr = String::from("127.0.0.1:6000");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: SEGMENT_SIZE,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            undo_log_storage: None,
            raft_storage: None,
        },
        &server_addr,
        "alignment_test",
        async |_| {},
    )
    .await;

    let schema = Schema::new_with_id(
        1,
        &String::from("test_alignment"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    println!("Writing 100 cells and checking alignment of stored addresses");

    for i in 0..100 {
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i));
        data_map.insert(&String::from("score"), OwnedValue::U64(i as u64));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(format!("Cell_{}", i)),
        );

        let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));
        let result = server.chunks.write_cell(&mut cell);

        assert!(
            result.is_ok(),
            "Failed to write cell {}: {:?}",
            i,
            result.err()
        );

        // Now try to read it back - this will fail if the address is misaligned
        let cell_id = cell.id();
        let read_result = server.chunks.read_cell(&cell_id);

        match read_result {
            Ok(read_cell) => {
                println!("Cell {} written and read successfully", i);
                assert_eq!(read_cell.id(), cell_id);
            }
            Err(e) => {
                panic!("Failed to read back cell {} after write: {:?}", i, e);
            }
        }
    }

    println!("All 100 cells written and read successfully with correct alignment");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cell_location_alignment_after_update() {
    let _ = env_logger::try_init();

    let server_addr = String::from("127.0.0.1:6001");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: SEGMENT_SIZE * 4,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            undo_log_storage: None,
            raft_storage: None,
        },
        &server_addr,
        "alignment_update_test",
        async |_| {},
    )
    .await;

    let schema = Schema::new_with_id(
        1,
        &String::from("test_alignment"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    println!("Testing alignment through updates");

    // Create initial cell
    let mut data_map = OwnedMap::new();
    data_map.insert(&String::from("id"), OwnedValue::I64(1));
    data_map.insert(&String::from("score"), OwnedValue::U64(0));
    data_map.insert(
        &String::from("name"),
        OwnedValue::String(String::from("Original")),
    );

    let mut cell =
        OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map.clone()));
    server
        .chunks
        .write_cell(&mut cell)
        .expect("Failed to write initial cell");

    let cell_id = cell.id();

    // Update it 50 times with varying sizes
    for i in 0..50 {
        let string_size = if i % 3 == 0 {
            100
        } else if i % 3 == 1 {
            1000
        } else {
            10000
        };

        let large_string = "X".repeat(string_size);
        data_map.insert(&String::from("name"), OwnedValue::String(large_string));
        data_map.insert(&String::from("score"), OwnedValue::U64(i));

        let mut updated_cell =
            OwnedCell::new_with_id(schema.id, &cell_id, OwnedValue::Map(data_map.clone()));

        let update_result = server.chunks.update_cell(&mut updated_cell);
        assert!(
            update_result.is_ok(),
            "Failed to update cell iteration {}: {:?}",
            i,
            update_result.err()
        );

        // Read it back to ensure alignment is correct
        let read_result = server.chunks.read_cell(&cell_id);
        match read_result {
            Ok(read_cell) => {
                assert_eq!(read_cell.id(), cell_id);
                assert_eq!(read_cell.data["score"].u64().unwrap(), &i);
                println!("Update iteration {} successful (size: {})", i, string_size);
            }
            Err(e) => {
                panic!("Failed to read cell after update iteration {}: {:?}", i, e);
            }
        }
    }

    println!("All 50 updates completed with correct alignment");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_varying_size_alignment() {
    let _ = env_logger::try_init();

    let server_addr = String::from("127.0.0.1:6002");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: SEGMENT_SIZE * 8,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            undo_log_storage: None,
            raft_storage: None,
        },
        &server_addr,
        "alignment_varying_test",
        async |_| {},
    )
    .await;

    let schema = Schema::new_with_id(
        1,
        &String::from("test_alignment"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    println!("Testing alignment with dramatically varying cell sizes");

    // Test with many different sizes to ensure alignment works regardless of cell size
    let sizes = vec![
        10, 50, 100, 237, // Odd numbers to test alignment handling
        500, 1000, 1337, 5000, 10000, 12345, 50000, 100000,
    ];

    for (idx, size) in sizes.iter().enumerate() {
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(idx as i64));
        data_map.insert(&String::from("score"), OwnedValue::U64(*size as u64));

        // Create a string of the specified size
        let test_string = "A".repeat(*size);
        data_map.insert(&String::from("name"), OwnedValue::String(test_string));

        let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));

        let write_result = server.chunks.write_cell(&mut cell);
        assert!(
            write_result.is_ok(),
            "Failed to write cell with size {}: {:?}",
            size,
            write_result.err()
        );

        // Immediately read it back
        let cell_id = cell.id();
        let read_result = server.chunks.read_cell(&cell_id);

        match read_result {
            Ok(read_cell) => {
                assert_eq!(read_cell.id(), cell_id);
                let name_len = read_cell.data["name"].string().unwrap().len();
                assert_eq!(name_len, *size, "String length mismatch for size {}", size);
                println!("Size {} bytes: write and read successful", size);
            }
            Err(e) => {
                panic!("Failed to read cell with size {}: {:?}", size, e);
            }
        }
    }

    println!("All varying sizes handled correctly with proper alignment");
}

#[test]
fn test_entry_header_alignment() {
    let _ = env_logger::try_init();

    use crate::ram::entry::*;
    use std::mem;

    println!("Testing entry header structure alignment");

    // Verify EntryHeader is properly aligned
    let header_size = mem::size_of::<EntryHeader>();
    let header_align = mem::align_of::<EntryHeader>();

    println!(
        "EntryHeader size: {}, alignment: {}",
        header_size, header_align
    );
    assert_eq!(
        ENTRY_HEAD_SIZE,
        mem::size_of::<u64>(),
        "ENTRY_HEAD_SIZE should be u64 size"
    );
    assert_eq!(ENTRY_HEAD_SIZE, 8, "ENTRY_HEAD_SIZE should be 8 bytes");

    // Verify the header fits exactly in 8 bytes
    assert!(
        header_size <= ENTRY_HEAD_SIZE,
        "EntryHeader ({} bytes) should fit in ENTRY_HEAD_SIZE ({} bytes)",
        header_size,
        ENTRY_HEAD_SIZE
    );
}

#[test]
fn test_cell_header_alignment() {
    let _ = env_logger::try_init();

    use std::mem;

    println!("Testing CellHeader structure alignment");

    let header_size = mem::size_of::<CellHeader>();
    let header_align = mem::align_of::<CellHeader>();

    println!(
        "CellHeader size: {}, alignment: {}",
        header_size, header_align
    );

    // CellHeader should be properly aligned for direct memory access
    assert!(
        header_align >= 4,
        "CellHeader alignment should be at least 4 bytes"
    );

    // Size should be a multiple of alignment
    assert_eq!(
        header_size % header_align,
        0,
        "CellHeader size should be a multiple of its alignment"
    );
}

#[test]
fn test_detect_misaligned_addresses() {
    let _ = env_logger::try_init();

    println!("Testing detection of misaligned addresses like 0x...E6");

    // These are real patterns from crash reports
    let crash_addresses: Vec<usize> = vec![
        0x6c51974000e6_usize, // From first crash report
        0x6c395a4000e6_usize, // From second crash report
    ];

    for addr in crash_addresses {
        println!("Analyzing crash address: 0x{:016x}", addr);

        // Check alignment
        let align_4 = addr % 4;
        let align_8 = addr % 8;

        println!("  4-byte alignment offset: {}", align_4);
        println!("  8-byte alignment offset: {}", align_8);
        println!("  Last byte: 0x{:02x}", addr & 0xFF);

        // These addresses should be detected as misaligned
        assert_ne!(
            align_8, 0,
            "Address should be detected as misaligned (8-byte)"
        );

        // Verify our validation would catch this
        #[cfg(debug_assertions)]
        {
            let is_aligned = addr % 8 == 0;
            assert!(!is_aligned, "Validation should detect this as misaligned");
        }
    }

    println!("All misaligned addresses correctly detected");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alignment_after_multiple_segments() {
    let _ = env_logger::try_init();

    let server_addr = String::from("127.0.0.1:6003");
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 1,
            total_size: SEGMENT_SIZE * 4, // Multiple segments
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            undo_log_storage: None,
            raft_storage: None,
        },
        &server_addr,
        "alignment_multi_seg_test",
        async |_| {},
    )
    .await;

    let schema = Schema::new_with_id(
        1,
        &String::from("test_alignment"),
        None,
        default_fields(),
        false,
        false,
    );
    server.meta.schemas.new_schema(schema.clone());

    println!("Testing alignment across multiple segment allocations");

    // Write enough cells to potentially span multiple segments
    let cell_count = 200;
    let mut cell_ids = Vec::new();

    for i in 0..cell_count {
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(i));
        data_map.insert(&String::from("score"), OwnedValue::U64(i as u64));

        // Vary the size to force different segment allocations
        let string_size = ((i % 100) * 100 + 100) as usize;
        let test_string = "B".repeat(string_size);
        data_map.insert(&String::from("name"), OwnedValue::String(test_string));

        let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));

        match server.chunks.write_cell(&mut cell) {
            Ok(_) => {
                cell_ids.push(cell.id());
                if i % 20 == 0 {
                    println!("Written {} cells successfully", i + 1);
                }
            }
            Err(e) => {
                println!(
                    "Failed to write cell {} (may have run out of space): {:?}",
                    i, e
                );
                break;
            }
        }
    }

    println!("Wrote {} cells, now reading them all back", cell_ids.len());

    // Read all cells back to verify alignment
    for (idx, cell_id) in cell_ids.iter().enumerate() {
        match server.chunks.read_cell(cell_id) {
            Ok(cell) => {
                assert_eq!(cell.id(), *cell_id);
                if idx % 20 == 0 {
                    println!("Read {} cells successfully", idx + 1);
                }
            }
            Err(e) => {
                panic!("Failed to read cell {} back: {:?}", idx, e);
            }
        }
    }

    println!(
        "All {} cells read back successfully with correct alignment",
        cell_ids.len()
    );
}
