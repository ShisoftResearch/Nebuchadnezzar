use super::*;
use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::schema::*;
use crate::ram::types::*;
use crate::server::ServerMeta;
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
    let schema = Schema::new("dummy", None, fields, false);
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
        header: CellHeader::new(0, schema.id, &id1),
        data,
    };
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::<ServerMeta>::new(ServerMeta { schemas }),
        None,
        None,
        None,
    );
    let header = chunks.write_cell(&mut cell).unwrap();
    let cell_1_ptr = chunks.address_of(&Id::from_header(&header));
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
        header: CellHeader::new(0, schema.id, &id2),
        data,
    };
    let header = chunks.write_cell(&mut cell).unwrap();
    let cell_2_ptr = chunks.address_of(&Id::from_header(&header));
    assert_eq!(cell_2_ptr, cell_1_ptr + cell.header.size as usize);
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
        header: CellHeader::new(0, schema.id, &id2),
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
            let mut cell = cell.to_owned();
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
    chunks.remove_cell(&id1).unwrap();
    assert!(chunks.read_cell(&id1).is_err());
}
