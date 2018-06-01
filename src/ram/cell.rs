use ram::schema::{Schema, Field};
use ram::chunk::Chunk;
use ram::io::{reader, writer};
use ram::types::{Map, Value, Id, RandValue};
use std::sync::Arc;
use std::ops::{Index, IndexMut};
use std::ptr;
use serde::Serialize;

pub const MAX_CELL_SIZE :usize = 1 * 1024 * 1024;

pub type DataMap = Map;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct CellHeader {
    pub version: u64,
    pub checksum: u32,
    pub schema: u32,
    pub partition: u64,
    pub hash: u64,
    // this shall be copied from entry header
    pub size: u32,
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
    DataMismatchSchema(Field, Value),
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum ReadError {
    SchemaDoesNotExisted(u32),
    CellDoesNotExisted,
    NetworkingError,
    CellTypeIsNotMapForSelect,
    CellIdIsUnitId
}

impl CellHeader {
    pub fn new(size: u32, schema: u32, id: &Id, checksum: u32) -> CellHeader {
        CellHeader {
            version: 1,
            size,
            schema,
            checksum,
            partition: id.higher,
            hash: id.lower,
        }
    }
    fn write(&self, location: usize) {
        unsafe {
            ptr::copy_nonoverlapping(self as *const CellHeader, location as *mut CellHeader, CELL_HEADER_SIZE);
        }
    }
    pub fn reserve(location: usize, size: usize) {
        CellHeader {
            version: 0, // default a tombstone
            size: size as u32,
            schema: 0,
            hash: 0,
            partition: 0,
            checksum: 0,
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

pub const CELL_HEADER_SIZE: usize = 32;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Cell {
    pub header: CellHeader,
    pub data: Value
}

impl Cell {

    pub fn new_with_id(schema_id: u32, id: &Id, value: Value) -> Cell {
        Cell {
            header: CellHeader::new(0, schema_id, id, 0),
            data: value
        }
    }

    pub fn encode_cell_key<V>(schema_id: u32, value: &V) -> Id
        where V: Serialize {
        Id::from_obj(&(schema_id, value))
    }

    pub fn new(schema: &Arc<Schema>, value: Value) -> Option<Cell> {
        let schema_id = schema.id;
        let id = if let Value::Map(ref data) = value {
            match schema.key_field {
                Some(ref keys) => {
                    let value = data.get_in_by_ids(keys.iter());
                    match value {
                        &Value::Null => return None,
                        _ => Cell::encode_cell_key(schema_id, value)
                    }
                },
                None => {
                    Id::rand()
                }
            }
        } else {
            Id::rand()
        };
        Some(Cell::new_with_id(schema_id, &id, value))
    }

    pub fn header_from_chunk_raw(ptr: usize) -> Result<CellHeader, ReadError> {
        if ptr == 0 {return Err(ReadError::CellIdIsUnitId)}
        Ok(unsafe {(*(ptr as *const CellHeader))})
    }
    pub fn from_chunk_raw(ptr: usize, chunk: &Chunk) -> Result<Cell, ReadError> {
        let header = Cell::header_from_chunk_raw(ptr)?;
        let data_ptr = ptr + CELL_HEADER_SIZE;
        let schema_id = &header.schema;
        if let Some(schema) = chunk.meta.schemas.get(schema_id) {
            Ok(Cell {
                header,
                data: reader::read_by_schema(data_ptr, &schema)
            })
        } else {
            error!("Schema {} does not existed to read", schema_id);
            return Err(ReadError::SchemaDoesNotExisted(*schema_id));
        }
    }
    pub fn select_from_chunk_raw(ptr: usize, chunk: &Chunk, fields: &[u64]) -> Result<Value, ReadError> {
        let header = Cell::header_from_chunk_raw(ptr)?;
        let data_ptr = ptr + CELL_HEADER_SIZE;
        let schema_id = &header.schema;
        if let Some(schema) = chunk.meta.schemas.get(schema_id) {
            Ok(reader::read_by_schema_selected(data_ptr, &schema, fields))
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
        if schema.is_dynamic {
            writer::plan_write_dynamic_fields(
                &mut offset, &schema.fields,
                &self.data, &mut instructions
            )?;
        }
        let total_size = offset + CELL_HEADER_SIZE;
        if total_size > MAX_CELL_SIZE {return Err(WriteError::CellIsTooLarge(total_size))}
        let addr_opt = chunk.try_acquire(total_size as u32);
        self.header.size = total_size as u32;
        self.header.version += 1;
        match addr_opt {
            None => {
                error!("Cannot allocate new spaces in chunk");
                return Err(WriteError::CannotAllocateSpace);
            },
            Some((addr, _lock)) => {
                self.header.write(addr);
                writer::execute_plan(addr + CELL_HEADER_SIZE, instructions);
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

impl Index<u64> for Cell {
    type Output = Value;

    fn index(&self, index: u64) -> &Self::Output {
        &self.data[index]
    }
}

impl <'a> Index<&'a str> for Cell {
    type Output = Value;

    fn index(&self, index: &'a str) -> &Self::Output {
        &self.data[index]
    }
}

impl <'a> IndexMut <&'a str> for Cell {
    fn index_mut<'b>(&'b mut self, index: &'a str) -> &'b mut Self::Output {
        &mut self.data[index]
    }
}

impl IndexMut <u64> for Cell {
    fn index_mut<'b>(&'b mut self, index:u64) -> &'b mut Self::Output {
        &mut self.data[index]
    }
}