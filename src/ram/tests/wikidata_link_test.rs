/// Test for the actual wikidata_link schema to verify string alignment

use crate::ram::cell::OwnedCell;
use crate::ram::chunk::Chunks;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::{OwnedMap, OwnedValue, RandValue};
use dovahkiin::types::{Id, Map as DovahkiinMap, Type};

const CHUNK_SIZE: usize = 64 * 1024 * 1024;

#[test]
fn test_wikidata_link_schema_write_read() {
    println!("\n=== Testing Wikidata Link Schema ===\n");
    
    // Create the exact wikidata_link schema structure
    let fields = Field::new_schema(vec![
        Field::new_unindexed("_inbound", Type::Id),       // offset 0, 16 bytes
        Field::new_unindexed("_outbound", Type::Id),      // offset 16, 16 bytes
        Field::new_unindexed("statement_id", Type::String), // offset 32, 4 bytes (ptr)
        Field::new_unindexed("property_id", Type::String),  // offset 36, 4 bytes (ptr)
        Field::new_unindexed("literal_id", Type::String),   // offset 40, 4 bytes (ptr)
    ]);
    
    let schema = Schema::new_with_id(3313777299, "wikidata_link", None, fields, false, false);
    
    println!("Schema static_bound: {}", schema.static_bound);
    println!("Schema static_bound % 8: {}", schema.static_bound % 8);
    assert_eq!(schema.static_bound, 48, "Schema should have static_bound of 48 (8-byte aligned)");
    assert_eq!(schema.static_bound % 8, 0, "Schema static_bound must be 8-byte aligned");
    
    // Check field offsets
    if let Some(sub_fields) = &schema.fields.sub_fields {
        for field in sub_fields {
            println!("Field '{}': offset={:?}, type={:?}", 
                     field.name, field.offset, field.data_type);
        }
    }
    println!();
    
    let chunks = Chunks::new_dummy(1, CHUNK_SIZE);
    chunks.list[0].meta.schemas.new_schema(schema.clone());
    
    // Test various string combinations  
    let long_prop = format!("Property_{}", "X".repeat(50));
    let long_lit = format!("lit_{}", "Y".repeat(100));
    let test_cases = vec![
        ("P31", "Q5", "s123"),
        ("P106", "Q12345", "statement-456"),
        (long_prop.as_str(), "Q999", long_lit.as_str()),
        ("Short", "VeryLongPropertyIdHere", "AnotherLongLiteralValue"),
        ("🎉", "世界", "🚀"), // Unicode
    ];
    
    for (idx, (stmt_id, prop_id, lit_id)) in test_cases.iter().enumerate() {
        println!("Test case {}: stmt={} prop={} lit={} bytes", 
                 idx, stmt_id.len(), prop_id.len(), lit_id.len());
        
        let inbound_id = Id::new(idx as u64 * 1000, idx as u64 * 1000 + 1);
        let outbound_id = Id::new(idx as u64 * 2000, idx as u64 * 2000 + 1);
        
        // Create cell data
        let mut data_map = <OwnedMap as DovahkiinMap>::new();
        data_map.insert(&String::from("_inbound"), OwnedValue::Id(inbound_id));
        data_map.insert(&String::from("_outbound"), OwnedValue::Id(outbound_id));
        data_map.insert(&String::from("statement_id"), OwnedValue::String(stmt_id.to_string()));
        data_map.insert(&String::from("property_id"), OwnedValue::String(prop_id.to_string()));
        data_map.insert(&String::from("literal_id"), OwnedValue::String(lit_id.to_string()));
        
        let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));
        let cell_id = cell.id();
        
        // Write to chunk
        chunks.write_cell(&mut cell)
            .expect(&format!("Failed to write cell for case {}", idx));
        
        // Read back
        let read_cell = chunks.read_cell(&cell_id)
            .expect(&format!("Failed to write cell for case {}", idx));
        
        // Verify data
        let read_inbound = read_cell.data["_inbound"].id().unwrap();
        let read_outbound = read_cell.data["_outbound"].id().unwrap();
        assert_eq!(*read_inbound, inbound_id, "Inbound ID mismatch");
        assert_eq!(*read_outbound, outbound_id, "Outbound ID mismatch");
        
        // Check strings
        let read_stmt = read_cell.data["statement_id"].string().unwrap();
        let read_prop = read_cell.data["property_id"].string().unwrap();
        let read_lit = read_cell.data["literal_id"].string().unwrap();
        
        assert_eq!(read_stmt, *stmt_id, "Statement ID string mismatch");
        assert_eq!(read_prop, *prop_id, "Property ID string mismatch");
        assert_eq!(read_lit, *lit_id, "Literal ID string mismatch");
        
        println!("  ✓ Case {} passed", idx);
    }
    
    println!("\n✓ All wikidata_link schema tests passed!");
}

#[test]
fn test_wikidata_link_various_string_lengths() {
    println!("\n=== Testing Wikidata Link with Various String Lengths ===\n");
    
    let fields = Field::new_schema(vec![
        Field::new_unindexed("_inbound", Type::Id),
        Field::new_unindexed("_outbound", Type::Id),
        Field::new_unindexed("statement_id", Type::String),
        Field::new_unindexed("property_id", Type::String),
        Field::new_unindexed("literal_id", Type::String),
    ]);
    
    let schema = Schema::new_with_id(3313777299, "wikidata_link", None, fields, false, false);
    let chunks = Chunks::new_dummy(1, CHUNK_SIZE);
    chunks.list[0].meta.schemas.new_schema(schema.clone());
    
    // Test with various string lengths that cross alignment boundaries
    let string_lengths = vec![
        (0, 0, 0),      // All empty
        (1, 1, 1),      // Minimal
        (4, 4, 4),      // At 4-byte boundary
        (7, 8, 9),      // Around 8-byte boundary
        (15, 16, 17),   // Around 16-byte boundary
        (31, 32, 33),   // Around 32-byte boundary
        (50, 100, 150), // Medium
        (100, 200, 300), // Large
    ];
    
    for (len1, len2, len3) in string_lengths {
        let stmt_id = "S".repeat(len1);
        let prop_id = "P".repeat(len2);
        let lit_id = "L".repeat(len3);
        
        println!("Testing lengths: stmt={}, prop={}, lit={}", len1, len2, len3);
        
        let mut data_map = <OwnedMap as DovahkiinMap>::new();
        data_map.insert(&String::from("_inbound"), OwnedValue::Id(Id::new(1, 2)));
        data_map.insert(&String::from("_outbound"), OwnedValue::Id(Id::new(3, 4)));
        data_map.insert(&String::from("statement_id"), OwnedValue::String(stmt_id.clone()));
        data_map.insert(&String::from("property_id"), OwnedValue::String(prop_id.clone()));
        data_map.insert(&String::from("literal_id"), OwnedValue::String(lit_id.clone()));
        
        let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));
        let cell_id = cell.id();
        
        // Write
        chunks.write_cell(&mut cell)
            .expect(&format!("Failed to write for lengths ({}, {}, {})", len1, len2, len3));
        
        // Read back
        let read_cell = chunks.read_cell(&cell_id)
            .expect(&format!("Failed to read for lengths ({}, {}, {})", len1, len2, len3));
        
        // Verify
        let read_stmt = read_cell.data["statement_id"].string().unwrap();
        let read_prop = read_cell.data["property_id"].string().unwrap();
        let read_lit = read_cell.data["literal_id"].string().unwrap();
        
        assert_eq!(read_stmt, &stmt_id, "Statement ID mismatch at lengths ({}, {}, {})", len1, len2, len3);
        assert_eq!(read_prop, &prop_id, "Property ID mismatch at lengths ({}, {}, {})", len1, len2, len3);
        assert_eq!(read_lit, &lit_id, "Literal ID mismatch at lengths ({}, {}, {})", len1, len2, len3);
        
        println!("  ✓ Lengths ({}, {}, {}) passed", len1, len2, len3);
    }
    
    println!("\n✓ All string length variations passed!");
}

#[test]
fn test_wikidata_link_stress() {
    println!("\n=== Stress Testing Wikidata Link Schema ===\n");
    
    let fields = Field::new_schema(vec![
        Field::new_unindexed("_inbound", Type::Id),
        Field::new_unindexed("_outbound", Type::Id),
        Field::new_unindexed("statement_id", Type::String),
        Field::new_unindexed("property_id", Type::String),
        Field::new_unindexed("literal_id", Type::String),
    ]);
    
    let schema = Schema::new_with_id(3313777299, "wikidata_link", None, fields, false, false);
    let chunks = Chunks::new_dummy(1, CHUNK_SIZE);
    chunks.list[0].meta.schemas.new_schema(schema.clone());
    
    let num_cells = 100;
    let mut cell_locations = Vec::new();
    
    // Write many cells
    println!("Writing {} cells...", num_cells);
    for i in 0..num_cells {
        let mut data_map = <OwnedMap as DovahkiinMap>::new();
        data_map.insert(&String::from("_inbound"), OwnedValue::Id(Id::new(i, i + 1)));
        data_map.insert(&String::from("_outbound"), OwnedValue::Id(Id::new(i + 100, i + 101)));
        data_map.insert(&String::from("statement_id"), OwnedValue::String(format!("stmt_{}", i)));
        data_map.insert(&String::from("property_id"), OwnedValue::String(format!("prop_{}_value", i)));
        data_map.insert(&String::from("literal_id"), OwnedValue::String(format!("literal_{}_{}", i, "x".repeat((i as usize) % 50))));
        
        let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));
        let cell_id = cell.id();
        
        chunks.write_cell(&mut cell)
            .expect(&format!("Failed to write cell {}", i));
        
        cell_locations.push((cell_id, i));
    }
    
    println!("Wrote {} cells successfully", num_cells);
    
    // Read all cells back
    println!("Reading {} cells back...", num_cells);
    for (cell_id, i) in cell_locations {
        let read_cell = chunks.read_cell(&cell_id)
            .expect(&format!("Failed to read cell {}", i));
        
        let read_inbound = read_cell.data["_inbound"].id().unwrap();
        let read_stmt = read_cell.data["statement_id"].string().unwrap();
        
        assert_eq!(read_inbound.higher, i as u64, "Inbound ID mismatch for cell {}", i);
        assert_eq!(read_stmt, &format!("stmt_{}", i), "Statement ID mismatch for cell {}", i);
        
        if i % 10 == 0 {
            println!("  ✓ Read {} cells", i + 1);
        }
    }
    
    println!("\n✓ Stress test passed! All {} cells read successfully", num_cells);
}

