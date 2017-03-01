use neb::ram::cell;
use neb::ram::cell::*;
use std::mem;
use neb::ram::schema::*;
use neb::ram::chunk::Chunks;
use neb::ram::types::*;
use neb::ram::io::writer;

pub const CHUNK_SIZE: usize = 8 * 1024 * 1024;

#[test]
pub fn header_size() {
    assert_eq!(mem::size_of::<cell::Header>(), cell::HEADER_SIZE);
}

#[test]
pub fn cell_rw () {
    let fields = Field {
        type_id: 0,
        name: String::from("*"),
        nullable: false,
        is_array: false,
        sub: Some(vec![
            Field {
                type_id: 6,
                name: String::from("id"),
                nullable:false,
                is_array:false,
                sub: None,
            },
            Field {
                type_id: 20,
                name: String::from("name"),
                nullable:false,
                is_array:false,
                sub: None,
            },
            Field {
                type_id: 10,
                name: String::from("score"),
                nullable:false,
                is_array:false,
                sub: None,
            }
        ])
    };
    let schema = Schema {
        id: 1,
        name: String::from("dummy"),
        key_field: None,
        fields: fields
    };
    let mut data_map = Map::<String, Value>::new();
    data_map.insert(String::from("id"), Value::I64(100));
    data_map.insert(String::from("score"), Value::U64(70));
    data_map.insert(String::from("name"), Value::String(String::from("Jack")));
    let mut data = Value::Map(data_map);
    let chunk = &Chunks::new_dummy(1, CHUNK_SIZE).list[0];
    let mut cell = Cell {
        header: Header::new(0, 0, 1, 1),
//        Header {
//            version: 1,
//            size: 0,
//            schema: 0,
//            hash: 1,
//            partition: 1,
//        },
        data: data
    };
    let mut loc = cell.write_to_chunk(&chunk, &schema);
    let cell_1_ptr = loc.unwrap();
    {
        let stored_cell = Cell::from_raw(cell_1_ptr, &schema);
        assert!(stored_cell.header.size > (4 + HEADER_SIZE) as u32);
        assert!(stored_cell.header.size > (4 + HEADER_SIZE) as u32);
        assert_eq!(stored_cell.data.Map().unwrap().get("id").unwrap().I64().unwrap(), 100);
        assert_eq!(stored_cell.data.Map().unwrap().get("name").unwrap().String().unwrap(), "Jack");
        assert_eq!(stored_cell.data.Map().unwrap().get("score").unwrap().U64().unwrap(), 70);
    }
    data_map = Map::<String, Value>::new();
    data_map.insert(String::from("id"), Value::I64(2));
    data_map.insert(String::from("score"), Value::U64(80));
    data_map.insert(String::from("name"), Value::String(String::from("John")));
    data = Value::Map(data_map);
    cell = Cell {
        header: Header::new(0, 0, 2, 1),
//        Header {
//            version: 1,
//            size: 0,
//            schema: 0,
//            hash: 2,
//            partition: 1,
//        },
        data: data
    };
    loc = cell.write_to_chunk(&chunk, &schema);
    let cell_2_ptr = loc.unwrap();
    assert_eq!(cell_2_ptr, cell_1_ptr + cell.header.size as usize);
    {
        let stored_cell = Cell::from_raw(cell_2_ptr, &schema);
        assert!(stored_cell.header.size > (4 + HEADER_SIZE) as u32);
        assert_eq!(stored_cell.data.Map().unwrap().get("id").unwrap().I64().unwrap(), 2);
        assert_eq!(stored_cell.data.Map().unwrap().get("score").unwrap().U64().unwrap(), 80);
        assert_eq!(stored_cell.data.Map().unwrap().get("name").unwrap().String().unwrap(), "John");
    }
    {
        let stored_cell = Cell::from_raw(cell_1_ptr, &schema);
        assert!(stored_cell.header.size > (4 + HEADER_SIZE) as u32);
        assert_eq!(stored_cell.data.Map().unwrap().get("id").unwrap().I64().unwrap(), 100);
        assert_eq!(stored_cell.data.Map().unwrap().get("name").unwrap().String().unwrap(), "Jack");
        assert_eq!(stored_cell.data.Map().unwrap().get("score").unwrap().U64().unwrap(), 70);
    }
}
