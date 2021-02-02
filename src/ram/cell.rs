use crate::ram::chunk::Chunk;
use crate::ram::clock;
use crate::ram::entry::*;
use crate::ram::io::{reader, writer};
use crate::ram::mem_cursor::*;
use crate::ram::schema::{Field, Schema};
use crate::ram::types::{Id, OwnedValue, RandValue, SharedValue, Value};
use byteorder::{ReadBytesExt, WriteBytesExt};
use lightning::map::WordMutexGuard;
use serde::Serialize;
use std::io::Cursor;
use std::ops::Deref;
use std::ops::{Index, IndexMut};

use super::schema::ReadingSchema;

pub const MAX_CELL_SIZE: u32 = 1 * 1024 * 1024;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct CellHeader {
    pub version: u64,
    pub timestamp: u32,
    pub schema: u32,
    pub partition: u64,
    pub hash: u64,
    // this shall be calculated from entry header
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
    DataMismatchSchema(Field, OwnedValue),
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum ReadError {
    SchemaDoesNotExisted(u32),
    CellDoesNotExisted,
    NetworkingError,
    CellTypeIsNotMapForSelect,
    CellIdIsUnitId,
}

impl CellHeader {
    pub fn new(size: u32, schema: u32, id: &Id) -> CellHeader {
        let now = clock::now();
        CellHeader {
            version: 1,
            size,
            schema,
            timestamp: now,
            partition: id.higher,
            hash: id.lower,
        }
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

pub const CELL_HEADER_SIZE: usize = std::mem::size_of::<CellHeader>();
pub const CELL_HEADER_SIZE_U32: u32 = CELL_HEADER_SIZE as u32;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OwnedCell {
    pub header: CellHeader,
    pub data: OwnedValue,
}

def_raw_memory_cursor_for_size!(CELL_HEADER_SIZE as usize, addr_to_header_cursor);

impl OwnedCell {
    pub fn new_with_id(schema_id: u32, id: &Id, value: OwnedValue) -> Self {
        Self {
            header: CellHeader::new(0, schema_id, id),
            data: value,
        }
    }

    pub fn encode_cell_key<V>(schema_id: u32, value: &V) -> Id
    where
        V: Serialize,
    {
        Id::from_obj(&(schema_id, value))
    }

    pub fn new(schema: &Schema, value: OwnedValue) -> Option<Self> {
        let schema_id = schema.id;
        let id = if let OwnedValue::Map(ref data) = value {
            match schema.key_field {
                Some(ref keys) => {
                    let value = data.get_in_by_ids(keys.iter());
                    match value {
                        &OwnedValue::Null => return None,
                        _ => Self::encode_cell_key(schema_id, value),
                    }
                }
                None => Id::rand(),
            }
        } else {
            Id::rand()
        };
        Some(Self::new_with_id(schema_id, &id, value))
    }

    pub fn write_to_chunk_with_schema(
        &mut self,
        chunk: &Chunk,
        schema: &Schema,
    ) -> Result<usize, WriteError> {
        let mut offset: usize = 0;
        let mut instructions = Vec::<writer::Instruction>::new();
        writer::plan_write_field(&mut offset, &schema.fields, &self.data, &mut instructions)?;
        if schema.is_dynamic {
            writer::plan_write_dynamic_fields(
                &mut offset,
                &schema.fields,
                &self.data,
                &mut instructions,
            )?;
        }
        let entry_body_size = offset + CELL_HEADER_SIZE;
        let len_bytes = Entry::count_len_bytes(entry_body_size as u32);
        let total_size = Entry::size(len_bytes, entry_body_size as u32);
        if total_size > MAX_CELL_SIZE {
            return Err(WriteError::CellIsTooLarge(total_size as usize));
        }
        let addr_opt = chunk.try_acquire(total_size);
        self.header.size = total_size as u32;
        self.header.version += 1;
        match addr_opt {
            None => {
                error!(
                    "Cannot allocate new spaces in chunk, total cells {}",
                    chunk.cell_count()
                );
                return Err(WriteError::CannotAllocateSpace);
            }
            Some(pending_entry) => {
                let addr = pending_entry.addr;
                Entry::encode_to(
                    addr,
                    EntryType::CELL,
                    entry_body_size as u32,
                    len_bytes,
                    move |content_addr| {
                        // write cell header
                        let header = &self.header;
                        let mut cursor = addr_to_header_cursor(content_addr);
                        cursor.write_u64::<Endian>(header.version).unwrap();
                        cursor.write_u32::<Endian>(header.timestamp).unwrap();
                        cursor.write_u32::<Endian>(header.schema).unwrap();
                        cursor.write_u64::<Endian>(header.partition).unwrap();
                        cursor.write_u64::<Endian>(header.hash).unwrap();
                        release_cursor(cursor);
                        writer::execute_plan(content_addr + CELL_HEADER_SIZE, &instructions);
                    },
                );
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

impl Index<u64> for OwnedCell {
    type Output = OwnedValue;

    fn index(&self, index: u64) -> &Self::Output {
        &self.data[index]
    }
}

impl<'a> Index<&'a str> for OwnedCell {
    type Output = OwnedValue;

    fn index(&self, index: &'a str) -> &Self::Output {
        &self.data[index]
    }
}

impl<'a> IndexMut<&'a str> for OwnedCell {
    fn index_mut<'b>(&'b mut self, index: &'a str) -> &'b mut Self::Output {
        &mut self.data[index]
    }
}

impl IndexMut<u64> for OwnedCell {
    fn index_mut<'b>(&'b mut self, index: u64) -> &'b mut Self::Output {
        &mut self.data[index]
    }
}

#[derive(Debug)]
pub struct SharedCellData {
    pub header: CellHeader,
    pub data: SharedValue,
}

impl SharedCellData {
    //TODO: check or set checksum from crc32c cell content
    pub fn from_chunk_raw(ptr: usize, chunk: &Chunk) -> Result<(Self, ReadingSchema), ReadError> {
        let (header, data_ptr) = header_from_chunk_raw(ptr)?;
        let schema_id = &header.schema;
        if let Some(schema) = chunk.meta.schemas.get(schema_id) {
            let cell = Self {
                header,
                data: reader::read_by_schema(data_ptr, &*schema),
            };
            Ok((cell, schema))
        } else {
            error!("Schema {} does not existed to read", schema_id);
            return Err(ReadError::SchemaDoesNotExisted(*schema_id));
        }
    }
    pub fn id(&self) -> Id {
        self.header.id()
    }
    pub fn to_owned(&self) -> OwnedCell {
        OwnedCell {
            header: self.header.clone(),
            data: self.data.owned(),
        }
    }
    pub fn into_shared(self, guard: WordMutexGuard) -> SharedCell {
        SharedCell { guard, inner: self }
    }
}

pub struct SharedData<'a, T> {
    guard: WordMutexGuard<'a>,
    inner: T,
}

impl<'a, T> Deref for SharedData<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, T> SharedData<'a, T> {
    pub fn decompose(self) -> (T, WordMutexGuard<'a>) {
        (self.inner, self.guard)
    }
    pub fn compose(data: T, guard: WordMutexGuard<'a>) -> Self {
        Self { inner: data, guard }
    }
    pub fn guard(&self) -> &WordMutexGuard {
        &self.guard
    }
}

pub type SharedCell<'a> = SharedData<'a, SharedCellData>;

impl<'a> SharedCell<'a> {
    pub fn from_chunk_raw(
        ptr: WordMutexGuard<'a>,
        chunk: &'a Chunk,
    ) -> Result<(Self, ReadingSchema<'a>), ReadError> {
        SharedCellData::from_chunk_raw(*ptr, chunk).map(|(data, schema)| {
            let cell = Self {
                guard: ptr,
                inner: data,
            };
            (cell, schema)
        })
    }
    pub fn select_from_chunk_raw(
        ptr: WordMutexGuard<'a>,
        chunk: &'a Chunk,
        fields: &[u64],
    ) -> Result<SharedData<'a, SharedValue>, ReadError> {
        select_from_chunk_raw(*ptr, chunk, fields).map(|val| SharedData {
            guard: ptr,
            inner: val,
        })
    }
}

pub trait Cell {
    fn id(&self) -> Id;
    fn header(&self) -> &CellHeader;
    fn data(&self) -> &dyn Value;
}

impl Cell for OwnedCell {
    fn id(&self) -> Id {
        OwnedCell::id(self)
    }
    fn header(&self) -> &CellHeader {
        &self.header
    }
    fn data(&self) -> &dyn Value {
        &self.data
    }
}

impl<'a> Cell for SharedCell<'a> {
    fn id(&self) -> Id {
        self.inner.id()
    }
    fn header(&self) -> &CellHeader {
        &self.inner.header
    }
    fn data(&self) -> &dyn Value {
        &self.inner.data
    }
}

impl Cell for SharedCellData {
    fn id(&self) -> Id {
        self.id()
    }
    fn header(&self) -> &CellHeader {
        &self.header
    }
    fn data(&self) -> &dyn Value {
        &self.data
    }
}

pub fn cell_header_from_entry_content_addr(addr: usize, entry_header: &EntryHeader) -> CellHeader {
    let mut cursor = addr_to_header_cursor(addr);
    let header = CellHeader {
        version: cursor.read_u64::<Endian>().unwrap(),
        timestamp: cursor.read_u32::<Endian>().unwrap(),
        schema: cursor.read_u32::<Endian>().unwrap(),
        partition: cursor.read_u64::<Endian>().unwrap(),
        hash: cursor.read_u64::<Endian>().unwrap(),
        size: entry_header.content_length - CELL_HEADER_SIZE_U32,
    };
    release_cursor(cursor);
    return header;
}

pub fn header_from_chunk_raw(ptr: usize) -> Result<(CellHeader, usize), ReadError> {
    if ptr == 0 {
        return Err(ReadError::CellIdIsUnitId);
    }
    let (_, header) = Entry::decode_from(ptr, |addr, entry_header| {
        assert_eq!(entry_header.entry_type, EntryType::CELL);
        let header = cell_header_from_entry_content_addr(addr, &entry_header);
        (header, addr + CELL_HEADER_SIZE)
    });
    Ok(header)
}

pub fn select_from_chunk_raw(
    ptr: usize,
    chunk: &Chunk,
    fields: &[u64],
) -> Result<SharedValue, ReadError> {
    let (header, data_ptr) = header_from_chunk_raw(ptr)?;
    let schema_id = &header.schema;
    if let Some(schema) = chunk.meta.schemas.get(schema_id) {
        Ok(reader::read_by_schema_selected(data_ptr, &*schema, fields))
    } else {
        error!("Schema {} does not existed to read", schema_id);
        return Err(ReadError::SchemaDoesNotExisted(*schema_id));
    }
}
