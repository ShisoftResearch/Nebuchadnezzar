use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::schema::*;
use crate::ram::types;
use crate::ram::types::*;

use super::*;

pub const CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[cfg(feature = "fast_map")]
#[test]
pub fn cell_rw() {
    let fields = default_fields();
    let id1 = Id::new(1, 1);
    let id2 = Id::new(1, 2);
    let schema = Schema {
        id: 1,
        name: String::from("dummy"),
        key_field: None,
        str_key_field: None,
        fields,
        is_dynamic: false,
    };
    let mut data = data_map_value! {
        id: 100 as i64,
        score: 70 as u64,
        name: String::from("Jack")
    };
    let chunk = &Chunks::new_dummy(1, CHUNK_SIZE).list[0];
    chunk.meta.schemas.new_schema(schema.clone());
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    let mut loc = chunk.write_cell_to_chunk(&mut cell);
    let cell_1_ptr = loc.unwrap().0;
    {
        let (stored_cell, _) = SharedCellData::from_chunk_raw(cell_1_ptr, &chunk).unwrap();
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
    }
    data = data_map_value! {
        id: 2 as i64,
        score: 80 as u64,
        name: "John"
    };
    cell = OwnedCell {
        header: CellHeader::new(schema.id, &id2),
        data,
    };
    loc = chunk.write_cell_to_chunk(&mut cell);
    let cell_2_ptr = loc.unwrap().0;
    {
        let stored_cell = SharedCellData::from_chunk_raw(cell_2_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &2);
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &80);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "John");
    }
    {
        let stored_cell = SharedCellData::from_chunk_raw(cell_1_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
    }
}

#[cfg(feature = "fast_map")]
#[test]
pub fn dynamic() {
    let fields = default_fields();
    let id1 = Id::new(1, 1);
    let id2 = Id::new(1, 2);
    let schema = Schema {
        id: 1,
        name: String::from("dummy"),
        key_field: None,
        str_key_field: None,
        fields,
        is_dynamic: true,
    };
    let mut data_map = types::OwnedMap::new();
    data_map.insert("id", OwnedValue::I64(100));
    data_map.insert("score", OwnedValue::U64(70));
    data_map.insert("name", OwnedValue::String(String::from("Jack")));
    data_map.insert("year", OwnedValue::U16(2010));
    data_map.insert("major", OwnedValue::String(String::from("CS")));
    let mut data = OwnedValue::Map(data_map);
    let chunk = &Chunks::new_dummy(1, CHUNK_SIZE).list[0];
    chunk.meta.schemas.new_schema(schema.clone());
    let mut cell = OwnedCell {
        header: CellHeader::new(schema.id, &id1),
        data,
    };
    let mut loc = chunk.write_cell_to_chunk(&mut cell);
    let cell_1_ptr = loc.unwrap().0;
    {
        let stored_cell = SharedCellData::from_chunk_raw(cell_1_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &100);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "Jack");
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &70);
        assert_eq!(stored_cell.data["year"].u16().unwrap(), &2010);
        assert_eq!(stored_cell.data["major"].string().unwrap(), "CS");
    }

    data_map = types::OwnedMap::new();
    data_map.insert("id", OwnedValue::I64(2));
    data_map.insert("score", OwnedValue::U64(80));
    data_map.insert("name", OwnedValue::String(String::from("John")));
    data = OwnedValue::Map(data_map);
    cell = OwnedCell {
        header: CellHeader::new(schema.id, &id2),
        data,
    };
    loc = chunk.write_cell_to_chunk(&mut cell);
    let cell_2_ptr = loc.unwrap().0;
    {
        let stored_cell = SharedCellData::from_chunk_raw(cell_2_ptr, &chunk)
            .unwrap()
            .0;
        assert_eq!(stored_cell.data["id"].i64().unwrap(), &2);
        assert_eq!(stored_cell.data["score"].u64().unwrap(), &80);
        assert_eq!(stored_cell.data["name"].string().unwrap(), "John");
        assert!(stored_cell.data["major"].string().is_none());
    }
}
