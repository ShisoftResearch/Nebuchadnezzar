use ram::schema::Schema;
use ram::chunk::Chunk;
use std::ptr;
use ram::io::{reader, writer};
use ram::types::{Map, Value, Id};
use std::collections::HashMap;

const MAX_CELL_SIZE :usize = 1 * 1024 * 1024;

pub type DataValue = Value;
pub type DataMap = Map;

#[repr(C, packed)]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct Header {
    pub version: u64,
    pub size: u32,
    pub schema: u32,
    pub partition: u64,
    pub hash: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum WriteError {
    SchemaDoesNotExisted(u32),
    CannotAllocateSpace,
    CellIsTooLarge(usize),
    CellAlreadyExisted,
    CellDoesNotExisted,
    ReadError(ReadError),
    UserCanceledUpdate,
    DeletionPredictionFailed,
    NetworkingError,
    DataMismatchSchema,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum ReadError {
    SchemaDoesNotExisted(u32),
    CellDoesNotExisted,
    NetworkingError,
}

impl Header {
    pub fn new(size: u32, schema: u32, id: &Id) -> Header {
        Header {
            version: 1,
            size: size,
            schema: schema,
            partition: id.higher,
            hash: id.lower,
        }
    }
    fn write(&self, location: usize) {
        unsafe {
            ptr::copy_nonoverlapping(self as *const Header, location as *mut Header, HEADER_SIZE);
        }
    }
    pub fn reserve(location: usize, size: usize) {
        Header {
            version: 0, // default a tombstone
            size: size as u32,
            schema: 0,
            hash: 0,
            partition: 0,
        }.write(location);
    }
    pub fn id(&self) -> Id {
        Id {
            higher: self.partition,
            lower: self.hash,
        }
    }
    pub fn set_id(&mut self, id: &Id) {
        self.partition = id.higher;
        self.hash = id.lower;
    }
}

pub const HEADER_SIZE :usize = 32;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Cell {
    pub header: Header,
    pub data: DataValue
}

impl Cell {

    pub fn new(schema_id: u32, id: &Id, value: Value) -> Cell {
        Cell {
            header: Header::new(0, schema_id, id),
            data: value
        }
    }
    pub fn header_from_chunk_raw(ptr: usize) -> Result<Header, ReadError> {
        if ptr == 0 {return Err(ReadError::CellDoesNotExisted)}
        Ok(unsafe {(*(ptr as *const Header))})
    }
    pub fn from_chunk_raw(ptr: usize, chunk: &Chunk) -> Result<Cell, ReadError> {
        let header = Cell::header_from_chunk_raw(ptr)?;
        let data_ptr = ptr + HEADER_SIZE;
        let schema_id = &header.schema;
        if let Some(schema) = chunk.meta.schemas.get(schema_id) {
            Ok(Cell {
                header: header,
                data: reader::read_by_schema(data_ptr, &schema)
            })
        } else {
            error!("Schema {} does not existed to read", schema_id);
            return Err(ReadError::SchemaDoesNotExisted(*schema_id));
        }
    }
    pub fn write_to_chunk(&mut self, chunk: &Chunk) -> Result<usize, WriteError> {
        let schema_id = self.header.schema;
        if let Some(schema) = chunk.meta.schemas.get(&schema_id) {
            return self.write_to_chunk_with_schema(chunk, &schema)
        } else {
            error!("Schema {} does not existed to write", schema_id);
            return Err(WriteError::SchemaDoesNotExisted(schema_id));
        }
    }
    pub fn write_to_chunk_with_schema(&mut self, chunk: &Chunk, schema: &Schema) -> Result<usize, WriteError> {
        let mut offset: usize = 0;
        let mut instructions = Vec::<writer::Instruction>::new();
        writer::plan_write_field(&mut offset, &schema.fields, &self.data, &mut instructions)?;
        let total_size = offset + HEADER_SIZE;
        if total_size > MAX_CELL_SIZE {return Err(WriteError::CellIsTooLarge(total_size))}
        let addr_opt = chunk.try_acquire(total_size);
        self.header.size = total_size as u32;
        self.header.version += 1;
        match addr_opt {
            None => {
                error!("Cannot allocate new spaces in chunk");
                return Err(WriteError::CannotAllocateSpace);
            },
            Some((addr, _lock)) => {
                self.header.write(addr);
                writer::execute_plan(addr + HEADER_SIZE, instructions);
                return Ok(addr);
            }
        }
    }
    pub fn id(&self) -> Id {
        self.header.id()
    }
    pub fn set_id(&mut self, id: &Id) {
        self.header.set_id(id)
    }
}