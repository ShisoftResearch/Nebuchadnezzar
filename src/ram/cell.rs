use crate::ram::chunk::CellGuard;
use crate::ram::chunk::Chunk;
use crate::ram::chunk::PendingEntry;
use crate::ram::clock;
use crate::ram::compression;
use crate::ram::entry::*;
use crate::ram::io::align_address;
use crate::ram::io::{reader, writer};
use crate::ram::mem_cursor::*;
use crate::ram::schema::{CompressedFieldKind, Field, Schema, SchemaCompressionPlan};
use crate::ram::segs::SegmentClass;
use crate::ram::segs::SEGMENT_SIZE;
use crate::ram::types::{self, Bytes, Id, Map, OwnedValue, RandValue, SharedValue, Value};
use byteorder::{ReadBytesExt, WriteBytesExt};
use dovahkiin::types::referred::ARef;
use serde::Serialize;
use std::io::Cursor;
use std::io::Seek;
use std::ops::Deref;
use std::ops::{Index, IndexMut};
use std::ptr;

use super::io::writer::WriteInstructions;
use super::schema::SchemaRef;

pub const MAX_CELL_SIZE: u32 = 1 * 1024 * 1024;
pub const MAX_BLOB_CELL_SIZE: u32 = 2 * 1024 * 1024;

pub type OwnedCellRef = ARef<OwnedCell>;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Default)]
pub struct CellHeader {
    pub version: u64,
    pub timestamp: u32,
    pub schema: u32,
    pub id: Id,
}

pub struct WriteToChunkResult {
    pub new_timestamp: u32,
    pub new_version: u64,
    pub addr: usize,
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
    CellVersionMismatch,
    CompressionFailed(Field, String),
    /// Shutdown has begun archiving segments, so there is nowhere durable left
    /// to put a new entry. Nothing was written; the write must be retried
    /// against a live server.
    ServerShuttingDown,
    /// The cell was never attempted: an ordered batch stopped at an earlier
    /// failure. Applying later cells after an earlier one failed would break
    /// the caller's ordering contract (the B-tree write-back stream must be
    /// prefix-closed: a persisted page may only reference pages persisted
    /// before it). Always retryable -- nothing was written.
    BatchAborted,
    /// This member does not own the cell's slot; the named member does.
    ///
    /// Nothing was written, and the write is retryable at the named owner. The
    /// point is that it is refused *loudly* rather than accepted: a client
    /// holding a placement table one migration behind would otherwise write to
    /// a former owner, which succeeds, satisfies the client, and puts the data
    /// somewhere nothing will ever read it again.
    ///
    /// Carries the owner and the Raft log index that established it so a client
    /// can retry immediately without letting a late refusal roll back newer
    /// placement knowledge.
    NotSlotOwner { owner: u64, applied_index: u64 },
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum ReadError {
    SchemaDoesNotExisted(u32),
    CellDoesNotExisted,
    NetworkingError,
    CellTypeIsNotMapForSelect,
    CellIdIsUnitId,
    NotMatch,
    ExecError(String),
    SegmentPromotionFailed,
    DecompressionFailed(String),
    /// The index entry for this cell points at memory that does not hold it.
    ///
    /// NOT the same thing as the cell being absent, and it used to be reported
    /// as if it were. That misclassification is how a reshard came back with a
    /// silent shortfall: the donor read 129 cells whose entries pointed at
    /// ZEROED memory, `CellDoesNotExisted` told the migration they had been
    /// deleted, and the migration correctly concluded there was nothing to
    /// move. An absence is an ordinary outcome; this is the store disagreeing
    /// with itself, and every caller that can afford to should treat it as an
    /// error rather than an empty result.
    StaleCellPointer,
}

impl CellHeader {
    pub fn new(schema: u32, id: &Id) -> CellHeader {
        let now = clock::now();
        CellHeader {
            version: 1,
            schema,
            timestamp: now,
            id: *id,
        }
    }

    pub fn id(&self) -> Id {
        self.id
    }
    pub fn set_id(&mut self, id: &Id) {
        self.id = *id;
    }
}

pub const CELL_HEADER_SIZE: usize = std::mem::size_of::<CellHeader>();
pub const CELL_HEADER_SIZE_U32: u32 = CELL_HEADER_SIZE as u32;

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct OwnedCell {
    pub header: CellHeader,
    pub data: OwnedValue,
}

pub struct WritePlan<'a> {
    pub instructions: WriteInstructions<'a>,
    pub entry_body_size: usize,
    pub total_size: u32,
    pub schema: SchemaRef,
    pub segment_class: SegmentClass,
}

def_raw_memory_cursor_for_size!(CELL_HEADER_SIZE as usize, addr_to_header_cursor);

impl OwnedCell {
    pub fn new_with_id(schema_id: u32, id: &Id, value: OwnedValue) -> Self {
        Self {
            header: CellHeader::new(schema_id, id),
            data: value,
        }
    }

    pub fn encode_cell_key<V>(schema_id: u32, value: &V) -> Id
    where
        V: Serialize,
    {
        Id::from_obj(&(schema_id, value))
    }

    pub fn default_id(schema_id: u32, value: &OwnedValue, schema: &Schema) -> Id {
        let key_field = &schema.key_field;
        if let OwnedValue::Map(ref data) = value {
            match key_field {
                Some(ref keys) => {
                    let value = data.get_in_by_ids(keys.iter());
                    match value {
                        &OwnedValue::Null => {}
                        _ => return Self::encode_cell_key(schema_id, value),
                    }
                }
                None => {}
            }
        }
        return Id::rand();
    }

    pub fn new(schema: &Schema, value: OwnedValue) -> Self {
        let schema_id = schema.id;
        let id = Self::default_id(schema_id, &value, schema);
        Self::new_with_id(schema_id, &id, value)
    }

    pub fn plan_write(&self, chunk: &Chunk) -> Result<WritePlan, WriteError> {
        let schema_id = self.header.schema;
        let schema = if let Some(schema) = chunk.meta.schemas.get(&schema_id) {
            schema
        } else {
            return Err(WriteError::SchemaDoesNotExisted(schema_id));
        };
        let mut tail_offset: usize = schema.static_bound;
        let mut instructions = WriteInstructions::new();
        writer::plan_write_field(
            &mut tail_offset,
            &schema.fields,
            &self.data,
            &mut instructions,
            false,
        )?;
        if schema.is_dynamic {
            writer::plan_write_dynamic_fields(
                &mut tail_offset,
                &schema.fields,
                &self.data,
                &mut instructions,
            )?;
        }
        let entry_body_size = align_address(8, tail_offset + CELL_HEADER_SIZE);
        let total_size = (ENTRY_HEAD_SIZE + entry_body_size) as u32;
        let max_cell_size = if schema.blobs {
            MAX_BLOB_CELL_SIZE
        } else {
            MAX_CELL_SIZE
        };
        if total_size > max_cell_size {
            return Err(WriteError::CellIsTooLarge(total_size as usize));
        }
        let segment_class = if schema.blobs {
            SegmentClass::Blob
        } else {
            SegmentClass::Regular
        };
        Ok(WritePlan::new(
            instructions,
            entry_body_size,
            total_size,
            schema,
            segment_class,
        ))
    }

    pub fn write_to_chunk_with(
        &self,
        write_plan: &WritePlan,
        pending_entry: &PendingEntry,
        old_version: u64,
    ) -> Result<WriteToChunkResult, WriteError> {
        let addr = pending_entry.addr;
        let new_version = old_version + 1;
        let new_timestamp = clock::now();
        debug_assert_eq!(align_address(8, addr), addr, "Entry address is not aligned");
        Entry::encode_to(
            addr,
            EntryType::CELL,
            write_plan.entry_body_size() as u32,
            |content_addr| {
                // write cell header
                let header = &self.header;
                let mut cursor = addr_to_header_cursor(content_addr);
                cursor.write_u64::<Endian>(new_version).unwrap();
                cursor.write_u32::<Endian>(new_timestamp).unwrap();
                cursor.write_u32::<Endian>(header.schema).unwrap();
                cursor.write_u64::<Endian>(header.id.bits()).unwrap();
                release_cursor(cursor);
                let data_base_addr = content_addr + CELL_HEADER_SIZE;
                debug_assert_eq!(
                    align_address(8, content_addr),
                    content_addr,
                    "Content address is not aligned"
                );
                debug_assert_eq!(
                    align_address(8, data_base_addr),
                    data_base_addr,
                    "Data base address is not aligned"
                );
                writer::execute_plan(data_base_addr, &write_plan.instructions);
            },
        );
        debug!(
            "Written cell {:?} with total size {}",
            self.header,
            write_plan.total_size()
        );
        return Ok(WriteToChunkResult {
            new_timestamp,
            new_version,
            addr,
        });
    }
    pub fn id(&self) -> Id {
        self.header.id()
    }
    pub fn set_id(&mut self, id: &Id) {
        self.header.set_id(id)
    }
    pub fn into_ref(self) -> OwnedCellRef {
        OwnedCellRef::new(self)
    }
}

impl Index<u64> for OwnedCell {
    type Output = OwnedValue;

    fn index(&self, index: u64) -> &Self::Output {
        &self.data[index]
    }
}

impl IndexMut<u64> for OwnedCell {
    fn index_mut<'b>(&'b mut self, index: u64) -> &'b mut Self::Output {
        &mut self.data[index]
    }
}

impl Index<usize> for OwnedCell {
    type Output = OwnedValue;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl IndexMut<usize> for OwnedCell {
    fn index_mut<'b>(&'b mut self, index: usize) -> &'b mut Self::Output {
        &mut self.data[index]
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

impl<'a> WritePlan<'a> {
    pub fn new(
        instructions: WriteInstructions<'a>,
        entry_body_size: usize,
        total_size: u32,
        schema: SchemaRef,
        segment_class: SegmentClass,
    ) -> Self {
        Self {
            instructions,
            entry_body_size,
            total_size,
            schema,
            segment_class,
        }
    }

    pub fn allocate(&self, chunk: &Chunk, full_gc: bool) -> Result<PendingEntry, WriteError> {
        chunk.try_acquire_in_class(self.total_size, full_gc, self.segment_class)
    }
    pub fn entry_body_size(&self) -> usize {
        self.entry_body_size
    }
    pub fn total_size(&self) -> u32 {
        self.total_size
    }
}

#[derive(Debug)]
pub struct SharedCellData<'v> {
    pub header: CellHeader,
    pub data: SharedValue<'v>,
    compression_plan: Option<SchemaCompressionPlan>,
}

/// Keep the last stale-pointer verdict where a TEST can read it.
///
/// The verdict is otherwise only in a `warn!`, and the suite runs without
/// `RUST_LOG`, so env_logger swallows it at error-only -- which is exactly how a
/// residual stale pointer was caught with no explanation attached. A test that
/// depends on the log level to explain its own failure will keep losing that
/// race; this does not.
///
/// Process-global and best-effort: with tests running in parallel the verdict
/// may belong to a different test. The COUNT each test keeps for itself is the
/// exact number; this is context for it.
#[cfg(test)]
pub(crate) mod stale_pointer_record {
    use std::sync::Mutex;

    static LAST: Mutex<Option<String>> = Mutex::new(None);
    static COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    pub(crate) fn record(verdict: &str) {
        COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Ok(mut last) = LAST.lock() {
            *last = Some(verdict.to_string());
        }
    }

    /// `(how many stale pointers this process has seen, the most recent verdict)`.
    pub(crate) fn snapshot() -> (usize, Option<String>) {
        (
            COUNT.load(std::sync::atomic::Ordering::Relaxed),
            LAST.lock().ok().and_then(|last| last.clone()),
        )
    }
}

#[cfg(test)]
fn record_stale_pointer(verdict: &str) {
    stale_pointer_record::record(verdict);
}

#[cfg(not(test))]
fn record_stale_pointer(_verdict: &str) {}

/// Why does the memory at `ptr` not hold the cell the index says it does?
///
/// Diagnostic for the one branch that has already decided something is wrong;
/// see the call site in `SharedCellData::from_chunk_raw` for how to read it.
fn describe_stale_pointer(ptr: usize, chunk: &Chunk) -> String {
    use std::sync::atomic::Ordering;
    let Some(segment) = chunk.locate_segment(ptr) else {
        return format!(
            "segment for {:#x} is GONE from the chunk -- the cleaner relocated it and the \
             index kept the old address",
            ptr
        );
    };
    let offset = ptr.saturating_sub(segment.addr);
    let frontier = segment.append_header.load(Ordering::Relaxed);
    let within_written = ptr < frontier;
    // Report the raw stamps rather than a delta. The first version subtracted
    // them from `clock::now()`, which is a different clock base entirely, and
    // printed "evicted -1785315233373ms ago" -- a diagnostic that lies is worse
    // than one that says less.
    let stamp = |value: i64| {
        if value <= 0 {
            "never".to_string()
        } else {
            format!("at {value}")
        }
    };
    format!(
        "segment {} (seq {}) is {}, {}dirty; offset {} of the segment, write frontier at {}, \
         so the address is {} the written range; evicted {}, promoted {}",
        segment.id,
        segment.seq_id,
        if segment.is_cold() { "COLD" } else { "HOT" },
        if segment.is_dirty() { "" } else { "not " },
        offset,
        frontier.saturating_sub(segment.addr),
        if within_written { "INSIDE" } else { "PAST" },
        stamp(segment.last_evicted_ms.load(Ordering::Relaxed)),
        stamp(segment.last_promoted_ms.load(Ordering::Relaxed)),
    )
}

impl<'v> SharedCellData<'v> {
    //TODO: check or set checksum from crc32c cell content
    pub fn from_chunk_raw(
        hash: u64,
        ptr: usize,
        chunk: &Chunk,
    ) -> Result<(Self, SchemaRef), ReadError> {
        let (header, data_ptr) = header_from_chunk_raw(ptr)?;
        if header.id.bits() != hash {
            // Stale pointer: the entry at this address no longer belongs to
            // the requested cell (relocated or replaced). Parsing it would
            // misinterpret another cell's bytes.
            //
            // The segment's state at this instant is what says WHICH subsystem
            // is responsible, and it is only available here. Three readings, and
            // they point at different code:
            //
            //   * offset inside the segment's written range, segment COLD and
            //     recently evicted -> eviction freed the pages under a live
            //     reader (`evict_segment` has its cell locking commented out).
            //   * offset inside the written range, segment HOT and recently
            //     promoted -> promotion restored an image that never contained
            //     the cell: the archive-then-append window.
            //   * segment absent, or offset past the write frontier -> the
            //     cleaner relocated the cell and the index kept the old address.
            //
            // Only reached on the mismatch branch, which already warned, so this
            // costs nothing on the read path.
            let verdict = describe_stale_pointer(ptr, chunk);
            record_stale_pointer(&verdict);
            warn!(
                "stale cell read: requested id bits {} found {:?} at {:#x}; {}",
                hash, header.id, ptr, verdict
            );
            return Err(ReadError::StaleCellPointer);
        }
        let schema_id = &header.schema;
        if let Some(schema) = chunk.meta.schemas.get(schema_id) {
            let compression_plan = if schema.compression_plan.is_empty() {
                None
            } else {
                Some(schema.compression_plan.clone())
            };
            let cell = Self::from_data_with_plan(
                header,
                reader::read_by_schema(data_ptr, &*schema),
                compression_plan,
            );
            Ok((cell, schema))
        } else {
            let seg = chunk.locate_segment(ptr);
            let segment_dump = seg
                .as_ref()
                .map(|seg| {
                    if seg.is_hot() {
                        let preview_len = std::cmp::min(SEGMENT_SIZE, 64);
                        let preview = unsafe {
                            std::slice::from_raw_parts(seg.addr as *const u8, preview_len)
                        };
                        format!("{:02x?}", preview)
                    } else {
                        "<cold segment; dump skipped>".to_string()
                    }
                })
                .unwrap_or_else(|| "<segment not found>".to_string());
            let msg = format!(
                "Schema {} does not existed to read ptr {} from chunk {}, hash {}, segment {:?}, hot {:?}, is_head: {:?}, seq_id: {:?}, append_header: {:?}, dirty: {:?}, beyond_append_header: {:?}, segment_dump (first 64 bytes): {}. Backtrace: {:?}",
                schema_id,
                ptr,
                chunk.id,
                hash,
                seg.as_ref().map(|seg| seg.id),
                seg.as_ref().map(|seg| seg.is_hot()),
                seg.as_ref().map(|seg| seg.id == chunk.get_head_seg_id()),
                seg.as_ref().map(|seg| seg.seq_id),
                seg.as_ref().map(|seg| seg.append_header.load(std::sync::atomic::Ordering::Relaxed)),
                seg.as_ref().map(|seg| seg.is_dirty()),
                seg.as_ref().map(|seg| ptr > seg.append_header.load(std::sync::atomic::Ordering::Relaxed)),
                segment_dump,
                std::backtrace::Backtrace::capture()
            );
            error!("{}", msg);
            if cfg!(debug_assertions) {
                // This shall never happen, need to debug and fix it in testing
                panic!("{}", msg);
            }
            return Err(ReadError::SchemaDoesNotExisted(*schema_id));
        }
    }
    pub fn from_data(header: CellHeader, data: SharedValue<'v>) -> Self {
        Self {
            header,
            data,
            compression_plan: None,
        }
    }
    pub fn from_data_with_plan(
        header: CellHeader,
        data: SharedValue<'v>,
        compression_plan: Option<SchemaCompressionPlan>,
    ) -> Self {
        Self {
            header,
            data,
            compression_plan,
        }
    }
    pub fn id(&self) -> Id {
        self.header.id()
    }
    pub fn to_owned(&self) -> OwnedCell {
        let mut owned = OwnedCell {
            header: self.header.clone(),
            data: self.data.owned(),
        };
        if let Some(plan) = &self.compression_plan {
            decode_owned_by_plan(plan, &mut owned.data);
        }
        owned
    }

    pub fn string(&self, key: &str) -> Option<String> {
        self.string_by_path(&[types::key_hash(key)])
    }

    pub fn bytes(&self, key: &str) -> Option<Vec<u8>> {
        self.bytes_by_path(&[types::key_hash(key)])
    }

    pub fn string_by_path(&self, path: &[u64]) -> Option<String> {
        let value = shared_value_by_path(&self.data, path)?;
        let owned = value.owned();
        let kind = compression_kind_for_path(self.compression_plan.as_ref(), path);

        match (kind, owned) {
            (Some(CompressedFieldKind::String), OwnedValue::Bytes(bytes)) => {
                let decompressed = compression::decompress_field(bytes.data.as_slice()).ok()?;
                String::from_utf8(decompressed).ok()
            }
            (None, OwnedValue::String(s)) => Some(s),
            _ => None,
        }
    }

    pub fn bytes_by_path(&self, path: &[u64]) -> Option<Vec<u8>> {
        let value = shared_value_by_path(&self.data, path)?;
        let owned = value.owned();
        let kind = compression_kind_for_path(self.compression_plan.as_ref(), path);

        match (kind, owned) {
            (Some(CompressedFieldKind::Bytes), OwnedValue::Bytes(bytes)) => {
                compression::decompress_field(bytes.data.as_slice()).ok()
            }
            (None, OwnedValue::Bytes(bytes)) => Some(bytes.data),
            _ => None,
        }
    }

    pub fn into_shared(self, cell_guard: CellGuard<'v>) -> SharedCell<'v> {
        SharedCell {
            cell_guard,
            inner: self,
        }
    }
}

pub struct SharedData<'a, T> {
    cell_guard: CellGuard<'a>,
    inner: T,
}

impl<'a, T> Deref for SharedData<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, T> SharedData<'a, T> {
    pub fn new(data: T, cell_guard: CellGuard<'a>) -> Self {
        Self {
            inner: data,
            cell_guard,
        }
    }
    pub fn decompose(self) -> (T, CellGuard<'a>) {
        (self.inner, self.cell_guard)
    }
    pub fn compose(data: T, cell_guard: CellGuard<'a>) -> Self {
        Self {
            inner: data,
            cell_guard,
        }
    }
    pub fn cell_guard(&self) -> &CellGuard<'a> {
        &self.cell_guard
    }
    pub fn cell_guard_mut(&'a mut self) -> &'a mut CellGuard<'a> {
        &mut self.cell_guard
    }
    pub fn into_cell_guard(self) -> CellGuard<'a> {
        self.cell_guard
    }
}

pub type SharedCell<'a> = SharedData<'a, SharedCellData<'a>>;

impl<'a> SharedCell<'a> {
    pub fn from_chunk_raw(
        hash: u64,
        cell_guard: CellGuard<'a>,
        chunk: &'a Chunk,
    ) -> Result<(Self, SchemaRef), ReadError> {
        let ptr = cell_guard.get_ptr();
        match SharedCellData::from_chunk_raw(hash, ptr, chunk) {
            Ok((data, schema)) => {
                let cell = Self {
                    cell_guard,
                    inner: data,
                };
                Ok((cell, schema))
            }
            Err(e) => Err(e),
        }
    }
    pub fn select_from_chunk_raw(
        cell_guard: CellGuard<'a>,
        chunk: &'a Chunk,
        fields: &[u64],
        need_header: bool,
    ) -> Result<SharedCell<'a>, ReadError> {
        select_from_chunk_raw(cell_guard.get_ptr(), chunk, fields, need_header).map(|(val, hdr)| {
            SharedData {
                cell_guard,
                inner: SharedCellData::from_data(hdr, val),
            }
        })
    }

    pub fn to_owned(&self) -> OwnedCell {
        self.inner.to_owned()
    }

    pub fn string(&self, key: &str) -> Option<String> {
        self.inner.string(key)
    }

    pub fn bytes(&self, key: &str) -> Option<Vec<u8>> {
        self.inner.bytes(key)
    }

    pub fn string_by_path(&self, path: &[u64]) -> Option<String> {
        self.inner.string_by_path(path)
    }

    pub fn bytes_by_path(&self, path: &[u64]) -> Option<Vec<u8>> {
        self.inner.bytes_by_path(path)
    }
}

fn shared_value_by_path<'a>(
    value: &'a SharedValue<'a>,
    path: &[u64],
) -> Option<&'a SharedValue<'a>> {
    let mut current = value;
    for key in path {
        current = &current[*key];
    }
    Some(current)
}

fn compression_kind_for_path(
    plan: Option<&SchemaCompressionPlan>,
    path: &[u64],
) -> Option<CompressedFieldKind> {
    plan.and_then(|p| {
        p.fields
            .iter()
            .find(|entry| entry.path.as_slice() == path)
            .map(|entry| entry.kind)
    })
}

fn value_mut_by_path<'a>(value: &'a mut OwnedValue, path: &[u64]) -> Option<&'a mut OwnedValue> {
    let mut current = value;
    for key in path {
        current = match current {
            OwnedValue::Map(map) => map.get_mut_by_key_id(*key),
            _ => return None,
        };
    }
    Some(current)
}

fn decode_owned_by_plan(plan: &SchemaCompressionPlan, value: &mut OwnedValue) {
    if plan.is_empty() {
        return;
    }

    for entry in &plan.fields {
        let target = match value_mut_by_path(value, &entry.path) {
            Some(v) => v,
            None => continue,
        };

        let compressed = match target {
            OwnedValue::Bytes(bytes) => bytes.data.as_slice(),
            _ => continue,
        };

        let decompressed = compression::decompress_field(compressed)
            .unwrap_or_else(|e| panic!("Failed to decompress field {:?}: {}", entry.path, e));

        *target = match entry.kind {
            CompressedFieldKind::Bytes => OwnedValue::Bytes(Bytes::from_vec(decompressed)),
            CompressedFieldKind::String => {
                let decoded = String::from_utf8(decompressed).unwrap_or_else(|e| {
                    panic!(
                        "Failed to decode decompressed UTF-8 field {:?}: {}",
                        entry.path, e
                    )
                });
                OwnedValue::String(decoded)
            }
        };
    }
}

pub trait Cell {
    type Value: Value;
    fn id(&self) -> Id;
    fn header(&self) -> &CellHeader;
    fn data(&self) -> &Self::Value;
}

impl Cell for OwnedCell {
    type Value = OwnedValue;
    fn id(&self) -> Id {
        OwnedCell::id(self)
    }
    fn header(&self) -> &CellHeader {
        &self.header
    }
    fn data(&self) -> &Self::Value {
        &self.data
    }
}

impl<'a> Cell for SharedCell<'a> {
    type Value = SharedValue<'a>;
    fn id(&self) -> Id {
        self.inner.id()
    }
    fn header(&self) -> &CellHeader {
        &self.inner.header
    }
    fn data(&self) -> &Self::Value {
        &self.inner.data
    }
}

impl<'v> Cell for SharedCellData<'v> {
    type Value = SharedValue<'v>;
    fn id(&self) -> Id {
        self.id()
    }
    fn header(&self) -> &CellHeader {
        &self.header
    }
    fn data(&self) -> &Self::Value {
        &self.data
    }
}

pub fn cell_hash_from_entry_content_addr(addr: usize) -> u64 {
    // The cell-index key is the full id bits, stored at offset 16
    // (version u64 + timestamp u32 + schema u32).
    let mut cursor = addr_to_header_cursor(addr);
    cursor.seek(std::io::SeekFrom::Start(16)).unwrap();
    let hash = cursor.read_u64::<Endian>().unwrap();
    release_cursor(cursor);
    hash
}

pub fn cell_header_from_entry_content_addr(addr: usize) -> CellHeader {
    let mut cursor = addr_to_header_cursor(addr);
    let header = CellHeader {
        version: cursor.read_u64::<Endian>().unwrap(),
        timestamp: cursor.read_u32::<Endian>().unwrap(),
        schema: cursor.read_u32::<Endian>().unwrap(),
        id: Id::from_bits(cursor.read_u64::<Endian>().unwrap()),
    };
    release_cursor(cursor);
    return header;
}

/// Zero the persisted version of the CELL entry at `entry_addr` (the entry's
/// head address, as returned by the allocator).
///
/// Write paths append the full cell image BEFORE the index decides whether
/// the write wins: an insert that loses the exists race, or an update whose
/// target turned out not to exist, leaves a fully-formed CELL entry behind as
/// dead space. The running store never reads it -- but RECOVERY scans raw
/// segments and resolves each cell by MAX VERSION, and the abandoned image
/// carries `old_version + 1`: the same version as the racing winner (so the
/// scan-order tie-break resurrected whichever landed later in the segment),
/// or one MORE than the tombstone of a deleted cell (so the failed update
/// resurrected deleted data). The crash-churn fuzzer hit the first form on
/// every fresh start: the genesis `crate_tree` re-create lost the metadata
/// write race, its abandoned image named a head page nothing ever wrote to,
/// and every post-SIGKILL load served "B-tree loaded with 0 keys" from that
/// orphan.
///
/// Zeroing the version makes the abandoned image lose to every live version
/// and every tombstone. The patch happens while the entry's `PendingEntry`
/// is still alive, so the WAL append (which runs at its drop) carries the
/// patched bytes.
pub fn abandon_entry_version(entry_addr: usize) {
    let content_addr = Entry::content_pos(entry_addr);
    let mut cursor = addr_to_header_cursor(content_addr);
    cursor.write_u64::<Endian>(0).unwrap();
    release_cursor(cursor);
}

pub fn cell_version_from_entry_content_addr(addr: usize) -> u64 {
    let mut cursor = addr_to_header_cursor(addr);
    let version = cursor.read_u64::<Endian>().unwrap();
    release_cursor(cursor);
    version
}

pub fn header_from_chunk_raw(ptr: usize) -> Result<(CellHeader, usize), ReadError> {
    if ptr == 0 {
        return Err(ReadError::CellIdIsUnitId);
    }
    let addr = Entry::content_pos(ptr);
    let header = cell_header_from_entry_content_addr(addr);
    Ok((header, addr + CELL_HEADER_SIZE))
}

pub fn cell_version_from_chunk_raw(ptr: usize) -> Result<u64, ReadError> {
    if ptr == 0 {
        return Err(ReadError::CellIdIsUnitId);
    }
    let addr = Entry::content_pos(ptr);
    Ok(cell_version_from_entry_content_addr(addr))
}

pub fn minimal_header_from_chunk_raw(ptr: usize) -> Result<(CellHeader, usize), ReadError> {
    if ptr == 0 {
        return Err(ReadError::CellIdIsUnitId);
    }
    let mut header = CellHeader::default();
    let addr = Entry::content_pos(ptr);
    let schema = unsafe { ptr::read((addr + 8 + 4) as *const u32) };
    header.schema = schema;
    Ok((header, addr + CELL_HEADER_SIZE))
}

pub fn select_from_chunk_raw<'v>(
    ptr: usize,
    chunk: &Chunk,
    fields: &[u64],
    need_header: bool,
) -> Result<(SharedValue<'v>, CellHeader), ReadError> {
    let (header, data_ptr) = if need_header {
        header_from_chunk_raw(ptr)?
    } else {
        minimal_header_from_chunk_raw(ptr)?
    };
    let schema_id = &header.schema;
    if let Some(schema) = chunk.meta.schemas.get(schema_id) {
        Ok((
            reader::read_by_schema_selected(data_ptr, &*schema, fields),
            header,
        ))
    } else {
        let msg = format!(
            "Schema {} does not existed to select fields {:?} from ptr {} from chunk {}, segment {:?}",
            schema_id,
            fields,
            ptr,
            chunk.id,
            chunk.locate_segment(ptr).map(|seg| seg.id)
        );
        error!("{}", msg);
        if cfg!(debug_assertions) {
            // This shall never happen, need to debug and fix it in testing
            panic!("{}", msg);
        }
        return Err(ReadError::SchemaDoesNotExisted(*schema_id));
    }
}
