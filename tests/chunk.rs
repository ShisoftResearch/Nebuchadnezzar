use neb::ram::cell;
use neb::ram::cell::*;
use neb::ram::schema::*;
use neb::ram::chunk::Chunks;
use neb::ram::types::*;
use neb::ram::io::writer;
use neb::server::ServerMeta;
use env_logger;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std;
use std::rc::Rc;

pub const CHUNK_SIZE: usize = 32 * 8 * 1024 * 1024;

#[test]
pub fn round_robin_segment () {
    let num = AtomicU8::new(std::u8::MAX);
    assert_eq!(num.load(Ordering::SeqCst), 255);
    assert_eq!(num.fetch_add(1, Ordering::SeqCst), 255);
    assert_eq!(num.fetch_add(1, Ordering::SeqCst), 0);
    assert_eq!(num.fetch_add(1, Ordering::SeqCst), 1);
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
    let mut schema = Schema::new(String::from("dummy"), None, fields);
    let mut data_map = Map::<String, Value>::new();
    data_map.insert(String::from("id"), Value::I64(100));
    data_map.insert(String::from("score"), Value::U64(70));
    data_map.insert(String::from("name"), Value::String(String::from("Jack")));
    let mut data = Value::Map(data_map);
    let schemas = Schemas::new(None);
    let chunks = Chunks::new(1, CHUNK_SIZE, Rc::<ServerMeta>::new(ServerMeta {
        schemas: schemas.clone()
    }), None);
    schemas.new_schema(&mut schema);
    let mut cell = Cell {
        header: Header::new(0, schema.id, 1, 1),
        data: data
    };
    let mut loc = chunks.write_cell(&mut cell);
    let cell_1_ptr = loc.unwrap();
    {
        let stored_cell = chunks.read_cell((1, 1)).unwrap();
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
        header: Header::new(0, schema.id, 2, 1),
        data: data
    };
    loc = chunks.write_cell(&mut cell);
    let cell_2_ptr = loc.unwrap();
    assert_eq!(cell_2_ptr, cell_1_ptr + cell.header.size as usize);
    {
        let stored_cell = chunks.read_cell((1, 2)).unwrap();
        assert!(stored_cell.header.size > (4 + HEADER_SIZE) as u32);
        assert_eq!(stored_cell.data.Map().unwrap().get("id").unwrap().I64().unwrap(), 2);
        assert_eq!(stored_cell.data.Map().unwrap().get("score").unwrap().U64().unwrap(), 80);
        assert_eq!(stored_cell.data.Map().unwrap().get("name").unwrap().String().unwrap(), "John");
    }
    {
        let stored_cell = chunks.read_cell((1, 1)).unwrap();
        assert!(stored_cell.header.size > (4 + HEADER_SIZE) as u32);
        assert_eq!(stored_cell.data.Map().unwrap().get("id").unwrap().I64().unwrap(), 100);
        assert_eq!(stored_cell.data.Map().unwrap().get("name").unwrap().String().unwrap(), "Jack");
        assert_eq!(stored_cell.data.Map().unwrap().get("score").unwrap().U64().unwrap(), 70);
    }
    data_map = Map::<String, Value>::new();
    data_map.insert(String::from("id"), Value::I64(2));
    data_map.insert(String::from("score"), Value::U64(95));
    data_map.insert(String::from("name"), Value::String(String::from("John")));
    data = Value::Map(data_map);
    cell = Cell {
        header: Header::new(0, schema.id, 2, 1),
        data: data
    };
    loc = chunks.update_cell(&mut cell);
    let cell_2_ptr = loc.unwrap();
    {
        let stored_cell = chunks.read_cell((1, 2)).unwrap();
        assert_eq!(stored_cell.data.Map().unwrap().get("id").unwrap().I64().unwrap(), 2);
        assert_eq!(stored_cell.data.Map().unwrap().get("score").unwrap().U64().unwrap(), 95);
        assert_eq!(stored_cell.data.Map().unwrap().get("name").unwrap().String().unwrap(), "John");
    }
    {
        let stored_cell = chunks.read_cell((1, 1)).unwrap();
        assert_eq!(stored_cell.data.Map().unwrap().get("id").unwrap().I64().unwrap(), 100);
        assert_eq!(stored_cell.data.Map().unwrap().get("name").unwrap().String().unwrap(), "Jack");
        assert_eq!(stored_cell.data.Map().unwrap().get("score").unwrap().U64().unwrap(), 70);
    }
    chunks.remove_cell((1, 1)).unwrap();
    assert!(chunks.read_cell((1, 1)).is_err());
}