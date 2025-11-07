/// Simple test to verify schema static_bound alignment fix
use crate::ram::schema::{Field, Schema};
use dovahkiin::types::Type;

#[test]
fn test_schema_alignment_fix() {
    println!("\n=== Verifying Schema Alignment Fix ===");

    // Create the problematic wikidata_link schema (44 bytes of fields)
    let fields = Field::new_schema(vec![
        Field::new_unindexed("_inbound", Type::Id),  // 16 bytes
        Field::new_unindexed("_outbound", Type::Id), // 16 bytes
        Field::new_unindexed("statement_id", Type::String), // 4 bytes (u32 ptr)
        Field::new_unindexed("property_id", Type::String), // 4 bytes (u32 ptr)
        Field::new_unindexed("literal_id", Type::String), // 4 bytes (u32 ptr)
                                                     // Total: 44 bytes
    ]);

    let schema = Schema::new("wikidata_link", None, fields, false, false);

    println!("Schema static_bound: {}", schema.static_bound);
    println!(
        "Alignment check: {} % 8 = {}",
        schema.static_bound,
        schema.static_bound % 8
    );

    // CRITICAL: Must be 48 (aligned), not 44 (misaligned)
    assert_eq!(
        schema.static_bound, 48,
        "Schema static_bound should be 48 (aligned from 44), got {}",
        schema.static_bound
    );

    assert_eq!(
        schema.static_bound % 8,
        0,
        "Schema static_bound must be 8-byte aligned!"
    );

    println!("✓ Schema is properly 8-byte aligned");
    println!("✓ Fix confirmed: 44 → 48 bytes");
}

#[test]
fn test_id_list_schema_alignment() {
    println!("\n=== Testing _NEB_ID_LIST Schema Alignment ===");

    let fields = Field::new_schema(vec![
        Field::new_unindexed("_next", Type::Id), // 16 bytes
        Field::new_unindexed_array("_list", Type::Id), // 4 bytes (u32 ptr)
                                                 // Total: 20 bytes
    ]);

    let schema = Schema::new("_NEB_ID_LIST", None, fields, false, false);

    println!("Schema static_bound: {}", schema.static_bound);
    println!(
        "Alignment check: {} % 8 = {}",
        schema.static_bound,
        schema.static_bound % 8
    );

    // Should be 24 (aligned), not 20 (misaligned)
    assert_eq!(schema.static_bound, 24);
    assert_eq!(schema.static_bound % 8, 0);

    println!("✓ ID_LIST schema properly aligned: 20 → 24 bytes");
}

#[test]
fn test_type_list_schema_alignment() {
    println!("\n=== Testing _NEB_TYPE_ID_LIST Schema Alignment ===");

    let fields = Field::new_schema(vec![
        Field::new_map_array(
            "_edges",
            vec![
                Field::new_unindexed("_type", Type::U32),
                Field::new_unindexed("_type_list", Type::Id),
            ],
        ), // 4 bytes (u32 ptr to array)
           // Total: 4 bytes
    ]);

    let schema = Schema::new("_NEB_TYPE_ID_LIST", None, fields, false, false);

    println!("Schema static_bound: {}", schema.static_bound);
    println!(
        "Alignment check: {} % 8 = {}",
        schema.static_bound,
        schema.static_bound % 8
    );

    // Should be 8 (aligned), not 4 (misaligned)
    assert_eq!(schema.static_bound, 8);
    assert_eq!(schema.static_bound % 8, 0);

    println!("✓ TYPE_LIST schema properly aligned: 4 → 8 bytes");
}
