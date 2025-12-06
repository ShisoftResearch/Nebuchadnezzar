/// Comprehensive tests for String PrimArrays
/// Testing various scenarios including UTF-8, alignment, and edge cases
use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::schema::*;
use crate::ram::types::*;
use crate::server::ServerMeta;
use std::string::String;
use std::sync::Arc;

pub const CHUNK_SIZE: usize = 1 * 8 * 1024 * 1024;

/// Test 1: Basic String array with simple ASCII strings
#[test]
fn test_string_array_basic() {
    let _ = env_logger::try_init();

    let fields = Field::new_schema(vec![Field::new_unindexed_array(
        "strings",
        dovahkiin::types::Type::String,
    )]);

    let schema = Schema::new("basic_string_array", None, fields, false, false);
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

    let test_strings = vec![
        String::from("aaaa"),
        String::from("bbbb"),
        String::from("cccc"),
    ];

    let data = data_map_value!(
        strings: OwnedValue::PrimArray(OwnedPrimArray::String(test_strings.clone()))
    );

    let id1 = Id::new(1, 1);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();

    let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
    assert_eq!(read_cell.data, cell.data);
}

/// Test 2: UTF-8 characters and emojis  
#[test]
fn test_string_array_utf8() {
    let _ = env_logger::try_init();

    let fields = Field::new_schema(vec![Field::new_unindexed_array(
        "strings",
        dovahkiin::types::Type::String,
    )]);

    let schema = Schema::new("utf8_string_array", None, fields, false, false);
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

    let test_strings = vec![
        String::from(""),
        String::from("ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"),
        String::from("中文测试文本"),
        String::from("Hello World"),
        String::from("🏳️‍🌈"),
        String::from("Привет мир"),
        String::from("مرحبا"),
        String::from("🙂😊😂🤣😎🥳"),
    ];

    let data = data_map_value!(
        strings: OwnedValue::PrimArray(OwnedPrimArray::String(test_strings.clone()))
    );

    let id1 = Id::new(2, 2);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();

    let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
    assert_eq!(read_cell.data, cell.data);
}

/// Test 3: Empty string array
#[test]
fn test_string_array_empty() {
    let _ = env_logger::try_init();

    let fields = Field::new_schema(vec![Field::new_unindexed_array(
        "strings",
        dovahkiin::types::Type::String,
    )]);

    let schema = Schema::new("empty_string_array", None, fields, false, false);
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

    let empty_strings: Vec<String> = vec![];

    let data = data_map_value!(
        strings: OwnedValue::PrimArray(OwnedPrimArray::String(empty_strings))
    );

    let id1 = Id::new(3, 3);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();

    let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
    assert_eq!(read_cell.data, cell.data);
}

/// Test 4: Very long strings to test alignment
#[test]
fn test_string_array_long_strings() {
    let _ = env_logger::try_init();

    let fields = Field::new_schema(vec![Field::new_unindexed_array(
        "strings",
        dovahkiin::types::Type::String,
    )]);

    let schema = Schema::new("long_string_array", None, fields, false, false);
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

    let test_strings = vec![
        "A".repeat(100),
        "B".repeat(255),
        "C".repeat(1000),
        "D".repeat(5000),
        "E".repeat(10000),
    ];

    let data = data_map_value!(
        strings: OwnedValue::PrimArray(OwnedPrimArray::String(test_strings.clone()))
    );

    let id1 = Id::new(4, 4);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();

    let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
    assert_eq!(read_cell.data, cell.data);
}

/// Test 5: Mixed content array
#[test]
fn test_string_array_mixed() {
    let _ = env_logger::try_init();

    let fields = Field::new_schema(vec![Field::new_unindexed_array(
        "strings",
        dovahkiin::types::Type::String,
    )]);

    let schema = Schema::new("mixed_string_array", None, fields, false, false);
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

    let test_strings = vec![
        String::from(""),
        String::from("hello"),
        String::from("ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"),
        String::from("中文测试"),
        String::from("🏳️‍🌈"),
        String::from("A".repeat(500)),
        String::from("normal"),
        String::from(""),
        String::from("🙂😊😂🤣😎🥳"),
        String::from("end"),
    ];

    let data = data_map_value!(
        strings: OwnedValue::PrimArray(OwnedPrimArray::String(test_strings.clone()))
    );

    let id1 = Id::new(5, 5);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();

    let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
    assert_eq!(read_cell.data, cell.data);
}

/// Test 6: Array with other fields to test alignment interactions
#[test]
fn test_string_array_with_other_fields() {
    let _ = env_logger::try_init();

    let fields = Field::new_schema(vec![
        Field::new_unindexed("id", dovahkiin::types::Type::I64),
        Field::new_unindexed("name", dovahkiin::types::Type::String),
        Field::new_unindexed_array("strings", dovahkiin::types::Type::String),
        Field::new_unindexed("count", dovahkiin::types::Type::U64),
    ]);

    let schema = Schema::new("string_array_with_fields", None, fields, false, false);
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

    let test_strings = vec![
        String::from("first"),
        String::from("second"),
        String::from("third"),
    ];

    let data = data_map_value!(
        id: OwnedValue::I64(12345),
        name: OwnedValue::String(String::from("test_entry")),
        strings: OwnedValue::PrimArray(OwnedPrimArray::String(test_strings.clone())),
        count: OwnedValue::U64(999)
    );

    let id1 = Id::new(10, 10);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();

    let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
    assert_eq!(read_cell.data, cell.data);
}

/// Test 7: String array with vector size
#[test]
fn test_string_array_vector_size() {
    let _ = env_logger::try_init();

    let fields = Field::new_schema(vec![Field::new_unindexed_vector(
        "strings",
        dovahkiin::types::Type::String,
        5,
    )]);

    let schema = Schema::new("vector_string_array", None, fields, false, false);
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

    let test_strings = vec![
        String::from("one"),
        String::from("two"),
        String::from("three"),
        String::from("four"),
        String::from("five"),
    ];

    let data = data_map_value!(
        strings: OwnedValue::PrimArray(OwnedPrimArray::String(test_strings.clone()))
    );

    let id1 = Id::new(11, 11);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();

    let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
    assert_eq!(read_cell.data, cell.data);
}

/// Test 8: Single element array
#[test]
fn test_string_array_single_element() {
    let _ = env_logger::try_init();

    let fields = Field::new_schema(vec![Field::new_unindexed_array(
        "strings",
        dovahkiin::types::Type::String,
    )]);

    let schema = Schema::new("single_element_array", None, fields, false, false);
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

    let test_strings = vec![String::from("single_element")];

    let data = data_map_value!(
        strings: OwnedValue::PrimArray(OwnedPrimArray::String(test_strings.clone()))
    );

    let id1 = Id::new(12, 12);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();

    let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
    assert_eq!(read_cell.data, cell.data);
}

/// Test 9: Array with empty strings
#[test]
fn test_string_array_with_empty_strings() {
    let _ = env_logger::try_init();

    let fields = Field::new_schema(vec![Field::new_unindexed_array(
        "strings",
        dovahkiin::types::Type::String,
    )]);

    let schema = Schema::new("empty_strings_in_array", None, fields, false, false);
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

    let test_strings = vec![
        String::from(""),
        String::from("hello"),
        String::from(""),
        String::from("world"),
        String::from(""),
        String::from(""),
        String::from("end"),
    ];

    let data = data_map_value!(
        strings: OwnedValue::PrimArray(OwnedPrimArray::String(test_strings.clone()))
    );

    let id1 = Id::new(13, 13);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();

    let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
    assert_eq!(read_cell.data, cell.data);
}

/// Test 10: Nested string arrays in maps
#[test]
fn test_string_array_in_nested_map() {
    let _ = env_logger::try_init();

    let fields = Field::new_schema(vec![
        Field::new_unindexed("id", dovahkiin::types::Type::I64),
        Field::new_map(
            "metadata",
            vec![
                Field::new_unindexed("author", dovahkiin::types::Type::String),
                Field::new_unindexed_array("tags", dovahkiin::types::Type::String),
                Field::new_unindexed("version", dovahkiin::types::Type::U32),
            ],
        ),
        Field::new_unindexed_array("comments", dovahkiin::types::Type::String),
    ]);

    let schema = Schema::new("nested_string_array", None, fields, false, false);
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
        id: OwnedValue::I64(1),
        metadata: data_map_value!(
            author: OwnedValue::String(String::from("Alice")),
            tags: OwnedValue::PrimArray(OwnedPrimArray::String(vec![
                String::from("rust"),
                String::from("database"),
                String::from("test")
            ])),
            version: OwnedValue::U32(42)
        ),
        comments: OwnedValue::PrimArray(OwnedPrimArray::String(vec![
            String::from("Great!"),
            String::from("Works well"),
            String::from("🏳️‍🌈"),
        ]))
    );

    let id1 = Id::new(20, 20);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();

    let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
    assert_eq!(read_cell.data, cell.data);
}

/// Test 11: Large array
#[test]
fn test_string_array_large_size() {
    let _ = env_logger::try_init();

    let fields = Field::new_schema(vec![Field::new_unindexed_array(
        "strings",
        dovahkiin::types::Type::String,
    )]);

    let schema = Schema::new("large_string_array", None, fields, false, false);
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

    let test_strings: Vec<String> = (0..100).map(|i| format!("element_{}", i)).collect();

    let data = data_map_value!(
        strings: OwnedValue::PrimArray(OwnedPrimArray::String(test_strings.clone()))
    );

    let id1 = Id::new(30, 30);
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    chunks.write_cell(&mut cell).unwrap();

    let read_cell = chunks.read_cell(&id1).unwrap().to_owned();
    assert_eq!(read_cell.data, cell.data);
}
