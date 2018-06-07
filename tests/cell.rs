use neb::ram::cell;
use neb::ram::cell::*;
use neb::ram::schema::*;
use neb::ram::chunk::Chunks;
use neb::ram::types::*;
use neb::ram::io::writer;

use std::mem;
use super::*;

pub const CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[test]
pub fn header_size() {
    assert_eq!(mem::size_of::<cell::CellHeader>(), cell::CELL_HEADER_SIZE);
}

#[test]
pub fn cell_rw () {
    let fields = default_fields();
    let id1 = Id::new(1, 1);
    let id2 = Id::new(1, 2);
    let mut schema = Schema {
        id: 1,
        name: String::from("dummy"),
        key_field: None,
        str_key_field: None,
        fields,
        is_dynamic: false
    };
    let mut data = data_map_value!{
        id: 100 as i64,
        score: 70 as u64,
        name: String::from("Jack")
    };
    let chunk = &Chunks::new_dummy(1, CHUNK_SIZE).list[0];
    chunk.meta.schemas.new_schema(schema.clone());
    let mut cell = Cell {
        header: CellHeader::new(0, schema.id, &id1, 0),
        data: data
    };
    let mut loc = cell.write_to_chunk(&chunk);
    let cell_1_ptr = loc.unwrap();
    {
        let stored_cell = Cell::from_chunk_raw(cell_1_ptr, &chunk).unwrap();
        assert!(stored_cell.header.size > (4 + CELL_HEADER_SIZE) as u32);
        assert!(stored_cell.header.size > (4 + CELL_HEADER_SIZE) as u32);
        assert_eq!(stored_cell.data["id"].I64().unwrap(), 100);
        assert_eq!(stored_cell.data["name"].String().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].U64().unwrap(), 70);
    }
    data = data_map_value!{
        id: 2 as i64,
        score: 80 as u64,
        name: "John"
    };
    cell = Cell {
        header: CellHeader::new(0, schema.id, &id2, 0),
        data
    };
    loc = cell.write_to_chunk(&chunk);
    let cell_2_ptr = loc.unwrap();

    assert_eq!(cell_2_ptr, cell_1_ptr + cell.header.size as usize);
    {
        let stored_cell = Cell::from_chunk_raw(cell_2_ptr, &chunk).unwrap();
        assert!(stored_cell.header.size > (4 + CELL_HEADER_SIZE) as u32);
        assert_eq!(stored_cell.data["id"].I64().unwrap(), 2);
        assert_eq!(stored_cell.data["score"].U64().unwrap(), 80);
        assert_eq!(stored_cell.data["name"].String().unwrap(), "John");
    }
    {
        let stored_cell = Cell::from_chunk_raw(cell_1_ptr, &chunk).unwrap();
        assert!(stored_cell.header.size > (4 + CELL_HEADER_SIZE) as u32);
        assert_eq!(stored_cell.data["id"].I64().unwrap(), 100);
        assert_eq!(stored_cell.data["name"].String().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].U64().unwrap(), 70);
    }
}

#[test]
pub fn dynamic() {
    let fields = default_fields();
    let id1 = Id::new(1, 1);
    let id2 = Id::new(1, 2);
    let mut schema = Schema {
        id: 1,
        name: String::from("dummy"),
        key_field: None,
        str_key_field: None,
        fields,
        is_dynamic: true
    };
    let mut data_map = Map::new();
    data_map.insert("id", Value::I64(100));
    data_map.insert("score", Value::U64(70));
    data_map.insert("name", Value::String(String::from("Jack")));
    data_map.insert("year", Value::U16(2010));
    data_map.insert("major", Value::String(String::from("CS")));
    let mut data = Value::Map(data_map);
    let chunk = &Chunks::new_dummy(1, CHUNK_SIZE).list[0];
    chunk.meta.schemas.new_schema(schema.clone());
    let mut cell = Cell {
        header: CellHeader::new(0, schema.id, &id1, 0),
        data
    };
    let mut loc = cell.write_to_chunk(&chunk);
    let cell_1_ptr = loc.unwrap();
    {
        let stored_cell = Cell::from_chunk_raw(cell_1_ptr, &chunk).unwrap();
        assert!(stored_cell.header.size > (4 + CELL_HEADER_SIZE) as u32);
        assert!(stored_cell.header.size > (4 + CELL_HEADER_SIZE) as u32);
        assert_eq!(stored_cell.data["id"].I64().unwrap(), 100);
        assert_eq!(stored_cell.data["name"].String().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].U64().unwrap(), 70);
        assert_eq!(stored_cell.data["year"].U16().unwrap(), 2010);
        assert_eq!(stored_cell.data["major"].String().unwrap(), "CS");
    }

    data_map = Map::new();
    data_map.insert("id", Value::I64(2));
    data_map.insert("score", Value::U64(80));
    data_map.insert("name", Value::String(String::from("John")));
    data = Value::Map(data_map);
    cell = Cell {
        header: CellHeader::new(0, schema.id, &id2, 0),
        data
    };
    loc = cell.write_to_chunk(&chunk);
    let cell_2_ptr = loc.unwrap();
    {
        let stored_cell = Cell::from_chunk_raw(cell_2_ptr, &chunk).unwrap();
        assert!(stored_cell.header.size > (4 + CELL_HEADER_SIZE) as u32);
        assert_eq!(stored_cell.data["id"].I64().unwrap(), 2);
        assert_eq!(stored_cell.data["score"].U64().unwrap(), 80);
        assert_eq!(stored_cell.data["name"].String().unwrap(), "John");
        assert!(stored_cell.data["major"].String().is_none());
    }
}