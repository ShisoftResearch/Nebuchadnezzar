use ram::schema::Schema;
use ram::chunk::Chunk;
use std::mem;
use std::ptr;
use ram::io::{reader, writer};
use ram::types::{Map, Value};

const MAX_CELL_SIZE :usize = 1 * 1024 * 1024;

pub type DataValue = Value;
pub type DataMap = Map<String, Value>;

#[repr(packed)]
#[derive(Debug, Copy, Clone)]
pub struct Header {
    pub version: u64,
    pub size: u32,
    pub schema: u32,
    pub hash: u64,
    pub partition: u64,
    pub lock: u16,
}

impl Header {
    pub fn new(size: u32, schema: u32, hash: u64, partition: u64) -> Header {
        Header {
            version: 1,
            size: size,
            schema: schema,
            hash: hash,
            partition: partition,
            lock: 1,
        }
    }
}

pub const HEADER_SIZE :usize = 34;

pub struct Cell {
    pub header: Header,
    pub data: DataValue
}

impl Cell {

    pub fn from_raw(ptr: usize, schema: &Schema) -> Cell {
        let header = unsafe {(*(ptr as *const Header))};
        let data_ptr = ptr + HEADER_SIZE;
        Cell {
            header: header,
            data: reader::read_by_schema(data_ptr, schema)
        }
    }

    pub fn write_to_chunk(&mut self, chunk: &Chunk, schema: &Schema) -> Option<usize> {
        let mut offset: usize = 0;
        let mut instructions = Vec::<writer::Instruction>::new();
        let plan = writer::plan_write_field(&mut offset, &schema.fields, &self.data, &mut instructions);
        let total_size = offset + HEADER_SIZE;
        if total_size > MAX_CELL_SIZE {return None}
        let addr_opt = chunk.try_acquire(total_size);
        self.header.schema = schema.id;
        self.header.size = total_size as u32;
        self.header.version += 1;
        match addr_opt {
            None => {
                error!("Cannot allocate new spaces in chunk");
                return None;
            },
            Some(addr) => {
                unsafe {
                    let header = self.header;
                    ptr::copy_nonoverlapping(&header as *const Header, addr as *mut Header, HEADER_SIZE);
                }
                writer::execute_plan(addr + HEADER_SIZE, instructions);
                return Some(addr);
            }
        }
    }

}