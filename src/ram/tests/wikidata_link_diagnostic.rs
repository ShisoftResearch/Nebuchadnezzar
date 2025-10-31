/// Deep diagnostic test that prints exact pointer values being written and read

use crate::ram::cell::OwnedCell;
use crate::ram::chunk::Chunks;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::{OwnedMap, OwnedValue, RandValue, u32_io};
use dovahkiin::types::{Id, Map as DovahkiinMap, Type};

const CHUNK_SIZE: usize = 64 * 1024 * 1024;

#[test]
fn diagnose_string_pointer_values() {
    println!("\n=== DIAGNOSTIC: String Pointer Values ===\n");
    
    let fields = Field::new_schema(vec![
        Field::new_unindexed("_inbound", Type::Id),
        Field::new_unindexed("_outbound", Type::Id),
        Field::new_unindexed("statement_id", Type::String),
    ]);
    
    let schema = Schema::new_with_id(1, "test", None, fields, false, false);
    
    println!("Schema static_bound: {}", schema.static_bound);
    println!("Expected: statement_id pointer should point to offset {}", schema.static_bound);
    println!();
    
    // Check field offsets
    if let Some(sub_fields) = &schema.fields.sub_fields {
        for field in sub_fields {
            println!("Field '{}': schema offset={:?}, type={:?}, is_var={}", 
                     field.name, field.offset, field.data_type, field.is_var());
        }
    }
    println!();
    
    let chunks = Chunks::new_dummy(1, CHUNK_SIZE);
    chunks.list[0].meta.schemas.new_schema(schema.clone());
    
    // Create test cell
    let mut data_map = <OwnedMap as DovahkiinMap>::new();
    data_map.insert(&String::from("_inbound"), OwnedValue::Id(Id::new(100, 200)));
    data_map.insert(&String::from("_outbound"), OwnedValue::Id(Id::new(300, 400)));
    data_map.insert(&String::from("statement_id"), OwnedValue::String("TEST_STRING".to_string()));
    
    let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));
    let cell_id = cell.id();
    
    // Write
    chunks.write_cell(&mut cell).expect("Write failed");
    
    println!("✓ Cell written successfully");
    
    // Now let's manually inspect what was written
    // We need to find the cell in memory and read the raw pointer values
    let read_result = chunks.read_cell(&cell_id);
    
    match read_result {
        Ok(read_cell) => {
            println!("✓ Cell read successfully");
            
            let stmt = read_cell.data["statement_id"].string().unwrap();
            println!("✓ String value read: '{}'", stmt);
            assert_eq!(stmt, "TEST_STRING");
            
            println!("\n=== SUCCESS: All pointers are correct! ===");
        }
        Err(e) => {
            println!("❌ READ FAILED: {:?}", e);
            panic!("Read failed - this reveals the pointer issue!");
        }
    }
}

#[test]
fn test_alignment_with_multiple_strings() {
    println!("\n=== Testing Multiple String Fields ===\n");
    
    let fields = Field::new_schema(vec![
        Field::new_unindexed("_inbound", Type::Id),       // 0-15
        Field::new_unindexed("_outbound", Type::Id),      // 16-31
        Field::new_unindexed("str1", Type::String),       // 32 (ptr)
        Field::new_unindexed("str2", Type::String),       // 36 (ptr)
        Field::new_unindexed("str3", Type::String),       // 40 (ptr)
    ]);
    
    let schema = Schema::new_with_id(2, "multi_string_test", None, fields, false, false);
    
    println!("Schema static_bound: {} (should be 48)", schema.static_bound);
    assert_eq!(schema.static_bound, 48);
    
    let chunks = Chunks::new_dummy(1, CHUNK_SIZE);
    chunks.list[0].meta.schemas.new_schema(schema.clone());
    
    // Test with strings of various lengths
    let s1_long = "X".repeat(10);
    let s2_long = "Y".repeat(20);
    let s3_long = "Z".repeat(30);
    let test_cases = vec![
        ("a", "b", "c"),
        ("short", "medium", "longer_string"),
        (s1_long.as_str(), s2_long.as_str(), s3_long.as_str()),
    ];
    
    for (idx, (s1, s2, s3)) in test_cases.iter().enumerate() {
        println!("\nTest {}: len({}, {}, {})", idx, s1.len(), s2.len(), s3.len());
        
        let mut data_map = <OwnedMap as DovahkiinMap>::new();
        data_map.insert(&String::from("_inbound"), OwnedValue::Id(Id::new(1, 2)));
        data_map.insert(&String::from("_outbound"), OwnedValue::Id(Id::new(3, 4)));
        data_map.insert(&String::from("str1"), OwnedValue::String(s1.to_string()));
        data_map.insert(&String::from("str2"), OwnedValue::String(s2.to_string()));
        data_map.insert(&String::from("str3"), OwnedValue::String(s3.to_string()));
        
        let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(data_map));
        let cell_id = cell.id();
        
        chunks.write_cell(&mut cell).expect("Write failed");
        
        let read_cell = chunks.read_cell(&cell_id).expect("Read failed");
        
        let read_s1 = read_cell.data["str1"].string().unwrap();
        let read_s2 = read_cell.data["str2"].string().unwrap();
        let read_s3 = read_cell.data["str3"].string().unwrap();
        
        assert_eq!(read_s1, *s1);
        assert_eq!(read_s2, *s2);
        assert_eq!(read_s3, *s3);
        
        println!("  ✓ All strings match");
    }
    
    println!("\n✓ All multiple string tests passed!");
}

