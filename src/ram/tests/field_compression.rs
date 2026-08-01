use std::sync::Arc;

use crate::ram::cell::{CellHeader, OwnedCell};
use crate::ram::chunk::Chunks;
use crate::ram::schema::{
    CompressedFieldKind, Field, FieldCompression, LocalSchemasCache, Schema, SchemaCompressionPlan,
};
use crate::ram::types::{Bytes, Id, Map, OwnedMap, OwnedValue, Type};
use crate::server::ServerMeta;

use super::chunk::CHUNK_SIZE;

fn new_chunks_with_schema(schema: Schema) -> Arc<Chunks> {
    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(schema);
    Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    )
}

#[test]
fn compressed_string_roundtrip_to_owned() {
    let compressed =
        Field::new_unindexed("content", Type::String).with_compression(FieldCompression::Lz4);
    let schema = Schema::new(
        "compressed_string",
        None,
        Field::new_schema(vec![
            Field::new_unindexed("id", Type::U64),
            compressed,
            Field::new_unindexed("plain", Type::String),
        ]),
        false,
        false,
    );

    let chunks = new_chunks_with_schema(schema.clone());
    let id = Id::allocated(1, 0, 101);
    let content = "lz4-content-".repeat(256);

    let mut map = OwnedMap::new();
    map.insert("id", OwnedValue::U64(7));
    map.insert("content", OwnedValue::String(content.clone()));
    map.insert("plain", OwnedValue::String("plain-value".to_string()));

    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id),
        data: OwnedValue::Map(map),
    };
    chunks.write_cell(&mut cell).unwrap();

    let shared = chunks.read_cell(&id).unwrap();
    assert!(shared.data["content"].bytes().is_some());
    assert_eq!(shared.data["plain"].string().unwrap(), "plain-value");
    assert_eq!(shared.string("content").unwrap(), content);
    assert_eq!(shared.string("plain").unwrap(), "plain-value");

    let owned = shared.to_owned();
    assert_eq!(owned.data["id"].u64().unwrap(), &7);
    assert_eq!(owned.data["content"].string().unwrap(), content.as_str());
    assert_eq!(owned.data["plain"].string().unwrap(), "plain-value");
}

#[test]
fn compressed_bytes_roundtrip_to_owned() {
    let compressed =
        Field::new_unindexed("blob", Type::Bytes).with_compression(FieldCompression::Lz4);
    let schema = Schema::new(
        "compressed_bytes",
        None,
        Field::new_schema(vec![Field::new_unindexed("id", Type::U64), compressed]),
        false,
        false,
    );

    let chunks = new_chunks_with_schema(schema.clone());
    let id = Id::allocated(1, 0, 202);
    let payload: Vec<u8> = (0..8192).map(|i| (i % 251) as u8).collect();

    let mut map = OwnedMap::new();
    map.insert("id", OwnedValue::U64(9));
    map.insert("blob", OwnedValue::Bytes(Bytes::from_vec(payload.clone())));

    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id),
        data: OwnedValue::Map(map),
    };
    chunks.write_cell(&mut cell).unwrap();

    let shared = chunks.read_cell(&id).unwrap();
    assert!(shared.data["blob"].bytes().is_some());
    assert_eq!(shared.bytes("blob").unwrap(), payload);

    let owned = shared.to_owned();
    assert_eq!(owned.data["id"].u64().unwrap(), &9);
    assert_eq!(
        owned.data["blob"].bytes().unwrap().data.as_slice(),
        payload.as_slice()
    );
}

#[test]
fn compression_plan_contains_type_tags() {
    let compressed_string =
        Field::new_unindexed("content", Type::String).with_compression(FieldCompression::Lz4);
    let compressed_bytes =
        Field::new_unindexed("blob", Type::Bytes).with_compression(FieldCompression::Lz4);

    let schema = Schema::new_with_id(
        4242,
        "compression_plan_tags",
        None,
        Field::new_schema(vec![
            Field::new_unindexed("id", Type::U64),
            compressed_string,
            compressed_bytes,
        ]),
        false,
        false,
    );

    let plan = SchemaCompressionPlan::from_schema(&schema);

    assert!(plan.fields.iter().any(|f| {
        f.path.len() == 1
            && f.path[0] == crate::ram::types::key_hash("content")
            && f.kind == CompressedFieldKind::String
    }));
    assert!(plan.fields.iter().any(|f| {
        f.path.len() == 1
            && f.path[0] == crate::ram::types::key_hash("blob")
            && f.kind == CompressedFieldKind::Bytes
    }));
}

#[test]
fn shared_helpers_work_for_uncompressed_fields() {
    let schema = Schema::new(
        "uncompressed_helpers",
        None,
        Field::new_schema(vec![
            Field::new_unindexed("id", Type::U64),
            Field::new_unindexed("plain_str", Type::String),
            Field::new_unindexed("plain_bytes", Type::Bytes),
        ]),
        false,
        false,
    );

    let chunks = new_chunks_with_schema(schema.clone());
    let id = Id::allocated(1, 0, 303);
    let plain_str = "plain-runtime-string".to_string();
    let plain_bytes = vec![1_u8, 2, 3, 4, 5, 8, 13, 21];

    let mut map = OwnedMap::new();
    map.insert("id", OwnedValue::U64(11));
    map.insert("plain_str", OwnedValue::String(plain_str.clone()));
    map.insert(
        "plain_bytes",
        OwnedValue::Bytes(Bytes::from_vec(plain_bytes.clone())),
    );

    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id),
        data: OwnedValue::Map(map),
    };
    chunks.write_cell(&mut cell).unwrap();

    let shared = chunks.read_cell(&id).unwrap();
    assert_eq!(shared.string("plain_str").unwrap(), plain_str);
    assert_eq!(shared.bytes("plain_bytes").unwrap(), plain_bytes);
}
