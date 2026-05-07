# Blob Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement schema-wide blob storage in universal chunks with 2 MiB blob cell support, dual hot-path head lanes per chunk, blob-priority eviction, unchanged cell headers, and cold-only blob recovery derived from schema ids.

**Architecture:** Keep the existing partition-to-chunk routing and universal chunk arena. Add a schema-level `blobs` flag, derive runtime segment class from schema ids, and split each chunk's writable path into two fixed lanes: regular and blob. Blob segments remain eligible for normal promotion-on-read, but recovery always restores them cold and eviction prefers them before regular segments.

**Tech Stack:** Rust, serde, lightning maps, parking_lot, existing RAM chunk and segment allocator, tiered memory manager, `cargo test`

---

## File Structure

- Modify: `src/ram/schema/mod.rs`
  Responsibility: add persisted `Schema.blobs` with backward-compatible defaults and a narrow opt-in helper.
- Modify: `src/ram/cell.rs`
  Responsibility: make `WritePlan` schema-aware for size limits and allocation lane selection.
- Modify: `src/ram/segs.rs`
  Responsibility: add runtime-only `SegmentClass` and class helpers on `Segment` and allocator.
- Modify: `src/ram/chunk.rs`
  Responsibility: replace the single head with two fixed head lanes, keep the fast path O(1), and protect both heads from cleaner selection.
- Modify: `src/ram/tiered/clock.rs`
  Responsibility: prefer blob victims before regular victims while still honoring active-head and safety rules.
- Modify: `src/ram/recovery.rs`
  Responsibility: classify segments from cell headers, force blob segments to cold recovery, and reset lane heads after recovery.
- Create: `src/ram/tests/blob_schema.rs`
  Responsibility: focused tests for schema metadata, size policy, lane separation, and dual-head behavior.
- Modify: `src/ram/tests/mod.rs`
  Responsibility: register the new blob schema test module.
- Modify: `src/ram/tiered/tests.rs`
  Responsibility: integration tests for blob-priority eviction and blob promotion after cold access.
- Modify: `src/ram/recovery.rs`
  Responsibility: extend the existing recovery test module with blob recovery and mixed-segment classification checks.

### Task 1: Add Schema Blob Policy And Size Limits

**Files:**
- Modify: `src/ram/schema/mod.rs`
- Modify: `src/ram/cell.rs`
- Modify: `src/ram/tests/mod.rs`
- Create: `src/ram/tests/blob_schema.rs`
- Test: `src/ram/tests/blob_schema.rs`

- [ ] **Step 1: Write the failing tests**

Add the new test module entry to `src/ram/tests/mod.rs`:

```rust
pub mod blob_schema;
```

Create `src/ram/tests/blob_schema.rs` with these focused tests:

```rust
use super::default_fields;
use crate::ram::cell::{CellHeader, OwnedCell, WriteError, MAX_CELL_SIZE};
use crate::ram::chunk::Chunks;
use crate::ram::schema::{LocalSchemasCache, Schema};
use crate::ram::types::{Id, OwnedMap, OwnedValue};
use crate::server::ServerMeta;
use std::sync::Arc;

fn make_chunks(schemas: LocalSchemasCache) -> Arc<Chunks> {
    Chunks::new(
        1,
        4 * 8 * 1024 * 1024,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    )
}

fn make_string_cell(schema_id: u32, partition: u64, payload_len: usize) -> OwnedCell {
    let id = Id::new(partition, payload_len as u64);
    let mut data = OwnedMap::new();
    data.insert(&String::from("id"), OwnedValue::I64(partition as i64));
    data.insert(
        &String::from("name"),
        OwnedValue::String("x".repeat(payload_len)),
    );
    data.insert(&String::from("score"), OwnedValue::U64(1));
    OwnedCell {
        header: CellHeader::new(schema_id, &id),
        data: OwnedValue::Map(data),
    }
}

#[test]
fn blob_schema_defaults_to_false_when_field_is_missing() {
    let schema = Schema::new("legacy", None, default_fields(), false, false);
    let mut value = serde_json::to_value(&schema).unwrap();
    value.as_object_mut().unwrap().remove("blobs");

    let decoded: Schema = serde_json::from_value(value).unwrap();

    assert!(!decoded.blobs, "legacy schemas should deserialize with blobs=false");
}

#[test]
fn blob_schema_allows_2m_cells_but_regular_schema_does_not() {
    let regular = Schema::new("regular", None, default_fields(), false, false);
    let blob = Schema::new("blob", None, default_fields(), false, false).with_blobs(true);

    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(regular.clone());
    schemas.debug_only_new_schema(blob.clone());
    let chunks = make_chunks(schemas);

    let over_regular_limit = MAX_CELL_SIZE as usize + 4 * 1024;
    let over_blob_limit = (2 * 1024 * 1024) + 4 * 1024;

    let regular_cell = make_string_cell(regular.id, 1, over_regular_limit);
    let blob_ok_cell = make_string_cell(blob.id, 2, over_regular_limit);
    let blob_too_large = make_string_cell(blob.id, 3, over_blob_limit);

    assert!(matches!(
        regular_cell.plan_write(&chunks.list[0]),
        Err(WriteError::CellIsTooLarge(_))
    ));
    assert!(blob_ok_cell.plan_write(&chunks.list[0]).is_ok());
    assert!(matches!(
        blob_too_large.plan_write(&chunks.list[0]),
        Err(WriteError::CellIsTooLarge(_))
    ));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
cargo test blob_schema --lib
```

Expected: FAIL because `Schema` does not yet have a `blobs` field or `with_blobs` helper, and blob schemas still use the 1 MiB cell limit.

- [ ] **Step 3: Write the minimal implementation**

Update `src/ram/schema/mod.rs` so blob metadata is persisted and backward-compatible:

```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub id: u32,
    pub name: String,
    pub key_field: Option<Vec<u64>>,
    pub str_key_field: Option<Vec<String>>,
    pub field_index: BTreeMap<u64, Vec<usize>>,
    pub id_index: BTreeMap<u64, Vec<u64>>,
    pub index_fields: BTreeMap<u64, Vec<IndexType>>,
    #[serde(default)]
    pub compound_index_fields: BTreeMap<u64, CompoundIndex>,
    pub fields: Field,
    #[serde(skip, default)]
    pub compression_plan: SchemaCompressionPlan,
    pub static_bound: usize,
    pub is_dynamic: bool,
    pub is_scannable: bool,
    #[serde(default)]
    pub blobs: bool,
}

impl Schema {
    pub fn new(
        name: &str,
        key_field: Option<Vec<String>>,
        mut fields: Field,
        is_dynamic: bool,
        is_scannable: bool,
    ) -> Schema {
        // existing offset assignment unchanged
        let mut schema = Schema {
            id: 0,
            name: name.to_string(),
            key_field: match key_field {
                None => None,
                Some(ref keys) => Some(keys.iter().map(|f| hash_str(f)).collect()),
            },
            str_key_field: key_field,
            static_bound: bound,
            fields,
            compression_plan: SchemaCompressionPlan::default(),
            is_dynamic,
            is_scannable,
            field_index,
            id_index,
            index_fields,
            compound_index_fields,
            blobs: false,
        };
        schema.refresh_compression_plan();
        schema
    }

    pub fn with_blobs(mut self, blobs: bool) -> Self {
        self.blobs = blobs;
        self
    }
}
```

Update `src/ram/cell.rs` so size enforcement is schema-aware:

```rust
pub const MAX_CELL_SIZE: u32 = 1 * 1024 * 1024;
pub const MAX_BLOB_CELL_SIZE: u32 = 2 * 1024 * 1024;

impl OwnedCell {
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
        let max_size = if schema.blobs {
            MAX_BLOB_CELL_SIZE
        } else {
            MAX_CELL_SIZE
        };
        if total_size > max_size {
            return Err(WriteError::CellIsTooLarge(total_size as usize));
        }

        Ok(WritePlan::new(
            instructions,
            entry_body_size,
            total_size,
            schema,
        ))
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
cargo test blob_schema --lib
```

Expected: PASS for `blob_schema_defaults_to_false_when_field_is_missing` and `blob_schema_allows_2m_cells_but_regular_schema_does_not`.

- [ ] **Step 5: Commit**

```bash
git add src/ram/schema/mod.rs src/ram/cell.rs src/ram/tests/mod.rs src/ram/tests/blob_schema.rs
git commit -m "feat: add blob schema metadata and size policy"
```

### Task 2: Add Runtime Segment Class And Dual Chunk Heads

**Files:**
- Modify: `src/ram/segs.rs`
- Modify: `src/ram/cell.rs`
- Modify: `src/ram/chunk.rs`
- Test: `src/ram/tests/blob_schema.rs`

- [ ] **Step 1: Extend the failing test module with lane and segment-class checks**

Append these tests to `src/ram/tests/blob_schema.rs`:

```rust
use crate::ram::segs::SegmentClass;

#[test]
fn blob_and_regular_cells_land_in_different_segment_classes() {
    let regular = Schema::new("regular_lane", None, default_fields(), false, false);
    let blob = Schema::new("blob_lane", None, default_fields(), false, false).with_blobs(true);

    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(regular.clone());
    schemas.debug_only_new_schema(blob.clone());
    let chunks = make_chunks(schemas);

    let regular_id = Id::new(11, 1);
    let blob_id = Id::new(11, 2);

    let mut regular_cell = make_string_cell(regular.id, regular_id.higher, 32 * 1024);
    regular_cell.header.set_id(&regular_id);
    let mut blob_cell = make_string_cell(blob.id, blob_id.higher, 512 * 1024);
    blob_cell.header.set_id(&blob_id);

    chunks.write_cell(&mut regular_cell).unwrap();
    chunks.write_cell(&mut blob_cell).unwrap();

    let regular_seg = chunks.list[0]
        .locate_segment(chunks.address_of(&regular_id))
        .unwrap();
    let blob_seg = chunks.list[0]
        .locate_segment(chunks.address_of(&blob_id))
        .unwrap();

    assert_eq!(regular_seg.class(), SegmentClass::Regular);
    assert_eq!(blob_seg.class(), SegmentClass::Blob);
    assert_ne!(regular_seg.id, blob_seg.id);
}

#[test]
fn chunk_keeps_independent_blob_and_regular_heads() {
    let regular = Schema::new("regular_head", None, default_fields(), false, false);
    let blob = Schema::new("blob_head", None, default_fields(), false, false).with_blobs(true);

    let schemas = LocalSchemasCache::new_local("");
    schemas.debug_only_new_schema(regular.clone());
    schemas.debug_only_new_schema(blob.clone());
    let chunks = make_chunks(schemas);
    let chunk = &chunks.list[0];

    for i in 0..64_u64 {
        let mut regular_cell = make_string_cell(regular.id, 21, 8 * 1024);
        regular_cell.header.set_id(&Id::new(21, i));
        chunks.write_cell(&mut regular_cell).unwrap();

        let mut blob_cell = make_string_cell(blob.id, 22, 128 * 1024);
        blob_cell.header.set_id(&Id::new(22, i));
        chunks.write_cell(&mut blob_cell).unwrap();
    }

    let (regular_head, blob_head) = chunk.head_segment_ids_for_test();
    assert_ne!(regular_head, blob_head, "blob and regular lanes must rotate independently");
    assert_eq!(chunk.segs.get(&(regular_head as usize)).unwrap().class(), SegmentClass::Regular);
    assert_eq!(chunk.segs.get(&(blob_head as usize)).unwrap().class(), SegmentClass::Blob);
}
```

- [ ] **Step 2: Run the test module to verify it fails**

Run:

```bash
cargo test blob_schema --lib
```

Expected: FAIL because `SegmentClass`, dual heads, lane-aware allocation, and `head_segment_ids_for_test()` do not exist yet.

- [ ] **Step 3: Implement runtime segment class and two fixed head lanes**

Add runtime-only segment classification in `src/ram/segs.rs`:

```rust
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentClass {
    Regular = 0,
    Blob = 1,
}

impl SegmentClass {
    #[inline(always)]
    pub fn lane(self) -> usize {
        self as usize
    }
}

pub struct Segment {
    pub id: u64,
    pub seq_id: u64,
    pub chunk_id: usize,
    pub addr: usize,
    pub bound: usize,
    pub append_header: AtomicUsize,
    pub dead_space: AtomicU32,
    pub tombstones: AtomicU32,
    dead_bytes_generation: AtomicU64,
    last_no_progress_clean_generation: AtomicU64,
    references: AtomicUsize,
    pub file_state: parking_lot::Mutex<SegmentFileState>,
    pub dropped: AtomicBool,
    runtime_class: AtomicU8,
    pub tiered_lock: AtomicU8,
    pub reference_count: AtomicU8,
    pub access_count: AtomicU8,
    pub last_promoted_ms: AtomicI64,
    pub last_evicted_ms: AtomicI64,
    is_dirty: AtomicBool,
    pub last_sync_time: AtomicI64,
    pub bytes_since_sync: AtomicUsize,
}

impl Segment {
    pub fn new(
        id: u64,
        seq_id: u64,
        chunk_id: usize,
        buffer_ptr: usize,
        hot: bool,
        class: SegmentClass,
        file_manager: Arc<SegmentFileManager>,
    ) -> Segment {
        Segment {
            addr: buffer_ptr,
            id,
            seq_id,
            chunk_id,
            bound: buffer_ptr + SEGMENT_SIZE,
            append_header: AtomicUsize::new(buffer_ptr),
            dead_space: AtomicU32::new(0),
            tombstones: AtomicU32::new(0),
            dead_bytes_generation: AtomicU64::new(0),
            last_no_progress_clean_generation: AtomicU64::new(0),
            references: AtomicUsize::new(0),
            file_state: parking_lot::Mutex::new(SegmentFileState {
                manager: file_manager,
                wal: None,
            }),
            dropped: AtomicBool::new(false),
            runtime_class: AtomicU8::new(class as u8),
            tiered_lock: AtomicU8::new(if hot { HOT_SEGMENT } else { COLD_SEGMENT }),
            reference_count: AtomicU8::new(0),
            access_count: AtomicU8::new(0),
            last_promoted_ms: AtomicI64::new(0),
            last_evicted_ms: AtomicI64::new(0),
            is_dirty: AtomicBool::new(true),
            last_sync_time: AtomicI64::new(0),
            bytes_since_sync: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn class(&self) -> SegmentClass {
        match self.runtime_class.load(Ordering::Acquire) {
            1 => SegmentClass::Blob,
            _ => SegmentClass::Regular,
        }
    }

    #[inline(always)]
    pub fn set_class(&self, class: SegmentClass) {
        self.runtime_class.store(class as u8, Ordering::Release);
    }
}

impl SegmentAllocator {
    pub fn alloc_seg(&self, file_manager: &Arc<SegmentFileManager>) -> Option<Segment> {
        self.alloc_seg_with_class(file_manager, SegmentClass::Regular)
    }

    pub fn alloc_seg_with_class(
        &self,
        file_manager: &Arc<SegmentFileManager>,
        class: SegmentClass,
    ) -> Option<Segment> {
        self.free
            .pop_front()
            .or_else(|| loop {
                let addr = self.offset.load(Relaxed);
                let new_addr = addr + SEGMENT_SIZE;
                if new_addr > self.limit {
                    return None;
                }
                if self
                    .offset
                    .compare_exchange(addr, new_addr, AcqRel, Relaxed)
                    .is_ok()
                {
                    return Some(addr);
                }
            })
            .map(|addr| {
                let id = self.id_by_addr(addr);
                let seq_id = self.next_seq_id.fetch_add(1, Ordering::AcqRel);
                Segment::new(
                    id as u64,
                    seq_id as u64,
                    self.chunk_id,
                    addr,
                    true,
                    class,
                    file_manager.clone(),
                )
            })
    }
}
```

Update `src/ram/cell.rs` so `WritePlan` carries the lane:

```rust
use crate::ram::segs::SegmentClass;

pub struct WritePlan<'a> {
    pub instructions: WriteInstructions<'a>,
    pub entry_body_size: usize,
    pub total_size: u32,
    pub schema: SchemaRef,
    pub segment_class: SegmentClass,
}

impl OwnedCell {
    pub fn plan_write(&self, chunk: &Chunk) -> Result<WritePlan, WriteError> {
        // existing schema lookup and size planning
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
        chunk.try_acquire_in_lane(self.segment_class, self.total_size, full_gc)
    }
}
```

Replace the single head in `src/ram/chunk.rs` with two fixed lanes and a lane-aware allocator:

```rust
use crate::ram::segs::SegmentClass;

const NO_HEAD_SEG_ID: u64 = u64::MAX - 1;
const HEAD_ALLOCATION_IN_PROGRESS: u64 = u64::MAX;

pub struct Chunk {
    pub id: usize,
    pub cell_index: WordMap,
    pub segs: SegmentList,
    head_seg_ids: [AtomicU64; 2],
    pub meta: Arc<ServerMeta>,
    pub backup_storage: Option<String>,
    pub wal_storage: Option<String>,
    pub file_manager: Arc<SegmentFileManager>,
    pub total_space: AtomicUsize,
    pub capacity: usize,
    pub gc_lock: Mutex<()>,
    pub allocator: SegmentAllocator,
    pub index_builder: Option<Arc<IndexBuilder>>,
    pub statistics: ChunkStatistics,
    pub tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
}

impl Chunk {
    #[inline(always)]
    fn head_slot(class: SegmentClass) -> usize {
        class.lane()
    }

    #[inline(always)]
    pub fn get_head_seg_id_for_class(&self, class: SegmentClass) -> u64 {
        self.head_seg_ids[Self::head_slot(class)].load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn get_head_seg_id(&self) -> u64 {
        self.get_head_seg_id_for_class(SegmentClass::Regular)
    }

    #[inline(always)]
    pub fn is_active_head(&self, seg_id: u64) -> bool {
        self.head_seg_ids
            .iter()
            .any(|head| head.load(Ordering::Acquire) == seg_id)
    }

    #[cfg(test)]
    pub fn head_segment_ids_for_test(&self) -> (u64, u64) {
        (
            self.get_head_seg_id_for_class(SegmentClass::Regular),
            self.get_head_seg_id_for_class(SegmentClass::Blob),
        )
    }

    pub fn try_acquire_in_lane(
        &self,
        class: SegmentClass,
        size: u32,
        full_gc: bool,
    ) -> Result<PendingEntry, WriteError> {
        let slot = Self::head_slot(class);
        let mut tried_gc = false;
        let backoff = Backoff::new();

        loop {
            let head_seg_id = self.head_seg_ids[slot].load(Ordering::Acquire);

            if head_seg_id == HEAD_ALLOCATION_IN_PROGRESS {
                backoff.spin();
                continue;
            }

            if head_seg_id != NO_HEAD_SEG_ID {
                if let Some(head) = self.segs.get(&(head_seg_id as usize)) {
                    if head.class() == class {
                        if let Some(addr) = head.try_acquire(size) {
                            head.incr_references();
                            return Ok(PendingEntry {
                                addr,
                                seg: head,
                                size,
                                skip_sync: is_in_transaction(),
                            });
                        }
                    }
                }
            }

            let total_space = self.segs.len() * SEGMENT_SIZE;
            if total_space >= self.capacity - SEGMENT_SIZE {
                if tried_gc {
                    return Err(WriteError::CannotAllocateSpace);
                }
                let _ = Cleaner::clean(self, true, full_gc);
                tried_gc = true;
                continue;
            }

            if self.allocator.meet_gc_threshold() {
                let _ = Cleaner::clean(self, false, false);
            }

            if self.head_seg_ids[slot]
                .compare_exchange(
                    head_seg_id,
                    HEAD_ALLOCATION_IN_PROGRESS,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                backoff.spin();
                continue;
            }

            let new_seg = self
                .allocator
                .alloc_seg_with_class(&self.file_manager, class)
                .expect("No space left after GC");
            let new_seg_id = new_seg.id;
            self.head_seg_ids[slot].store(new_seg_id, Ordering::Release);
            self.put_segment(new_seg);

            if head_seg_id != NO_HEAD_SEG_ID {
                if let Some(old_head) = self.segs.get(&(head_seg_id as usize)) {
                    if let Err(e) = old_head.force_wal_sync() {
                        warn!(
                            "Failed to sync WAL for old {:?} head segment {}: {}",
                            class,
                            head_seg_id,
                            e
                        );
                    }
                    let mut state = old_head.file_state.lock();
                    if let Some(wal) = state.wal.take() {
                        if let Err(e) = wal.sync_all() {
                            warn!(
                                "Failed to sync WAL during close for old {:?} head segment {}: {}",
                                class,
                                head_seg_id,
                                e
                            );
                        }
                        drop(wal);
                    }
                }
            }
        }
    }
}
```

Initialize the two head lanes explicitly in `Chunk::new_with_base`:

```rust
let bootstrap_segment = allocator
    .alloc_seg_with_class(&file_manager, SegmentClass::Regular)
    .expect(&format!("No space left for first segment in chunk {}", id));

let chunk = Chunk {
    id,
    segs,
    cell_index: index,
    meta,
    backup_storage,
    wal_storage,
    file_manager,
    allocator,
    index_builder,
    capacity: size,
    total_space: AtomicUsize::new(0),
    head_seg_ids: [
        AtomicU64::new(bootstrap_segment.id),
        AtomicU64::new(NO_HEAD_SEG_ID),
    ],
    gc_lock: Mutex::new(()),
    statistics: ChunkStatistics::new(),
    tiered_manager,
};
chunk.put_segment(bootstrap_segment);
```

- [ ] **Step 4: Run the test module to verify it passes**

Run:

```bash
cargo test blob_schema --lib
```

Expected: PASS for the new segment-class and dual-head tests, along with the Task 1 tests.

- [ ] **Step 5: Commit**

```bash
git add src/ram/segs.rs src/ram/cell.rs src/ram/chunk.rs src/ram/tests/blob_schema.rs
git commit -m "feat: add blob segment lanes in universal chunks"
```

### Task 3: Protect Both Heads And Prefer Blob Eviction

**Files:**
- Modify: `src/ram/chunk.rs`
- Modify: `src/ram/tiered/clock.rs`
- Modify: `src/ram/tiered/tests.rs`
- Test: `src/ram/tiered/tests.rs`

- [ ] **Step 1: Add the failing eviction and promotion integration tests**

Append these tests to `src/ram/tiered/tests.rs`:

```rust
#[test]
fn test_blob_segments_evict_before_regular_segments() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.5");
    std::env::set_var(
        "NEB_TIERED_PHYSICAL_MEMORY_LIMIT",
        &format!("{}", 2 * SEGMENT_SIZE),
    );

    let regular = Schema::new("regular_evict", None, default_fields(), false, false);
    let blob = Schema::new("blob_evict", None, default_fields(), false, false).with_blobs(true);
    let schemas = LocalSchemasCache::new_local("/tmp/neb_blob_priority_schema");
    schemas.debug_only_new_schema(regular.clone());
    schemas.debug_only_new_schema(blob.clone());

    let chunks = Chunks::new(
        1,
        8 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some("/tmp/neb_blob_priority_bk".to_string()),
        Some("/tmp/neb_blob_priority_wal".to_string()),
        crate::ram::tiered::TieredConfig::from_env().map(|c| {
            Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
                crate::ram::tiered::SharedMemoryPool::new(&c),
            ))
        }),
    );

    for i in 0..256_u64 {
        let mut regular_cell = OwnedCell::new_with_id(
            regular.id,
            &Id::new(1, i),
            data_map_value!(id: i as i64, name: format!("r-{i}"), score: 1_u64),
        );
        chunks.write_cell(&mut regular_cell).unwrap();

        let mut blob_map = OwnedMap::new();
        blob_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
        blob_map.insert(&String::from("name"), OwnedValue::String("x".repeat(128 * 1024)));
        blob_map.insert(&String::from("score"), OwnedValue::U64(1));
        let mut blob_cell = OwnedCell::new_with_id(blob.id, &Id::new(2, i), OwnedValue::Map(blob_map));
        chunks.write_cell(&mut blob_cell).unwrap();
    }

    let chunk = &chunks.list[0];
    let manager = chunk.tiered_manager.as_ref().unwrap();
    manager.explicit_evict(chunk, 1).unwrap();

    let cold_blob_segments = chunk
        .segments()
        .into_iter()
        .filter(|seg| seg.class() == crate::ram::segs::SegmentClass::Blob && seg.is_cold())
        .count();
    let cold_regular_segments = chunk
        .segments()
        .into_iter()
        .filter(|seg| seg.class() == crate::ram::segs::SegmentClass::Regular && seg.is_cold())
        .count();

    assert!(cold_blob_segments > 0, "blob segments should be evicted first");
    assert_eq!(cold_regular_segments, 0, "regular segments should stay hot while blob victims exist");
}

#[test]
fn test_blob_segments_promote_on_read_after_eviction() {
    let _guard = TEST_MUTEX.lock().unwrap();
    let _ = env_logger::try_init();

    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.5");
    std::env::set_var(
        "NEB_TIERED_PHYSICAL_MEMORY_LIMIT",
        &format!("{}", 2 * SEGMENT_SIZE),
    );

    let blob = Schema::new("blob_promote", None, default_fields(), false, false).with_blobs(true);
    let schemas = LocalSchemasCache::new_local("/tmp/neb_blob_promote_schema");
    schemas.debug_only_new_schema(blob.clone());

    let chunks = Chunks::new(
        1,
        8 * SEGMENT_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        Some("/tmp/neb_blob_promote_bk".to_string()),
        Some("/tmp/neb_blob_promote_wal".to_string()),
        crate::ram::tiered::TieredConfig::from_env().map(|c| {
            Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
                crate::ram::tiered::SharedMemoryPool::new(&c),
            ))
        }),
    );

    let blob_id = Id::new(4, 1);
    let mut blob_map = OwnedMap::new();
    blob_map.insert(&String::from("id"), OwnedValue::I64(1));
    blob_map.insert(&String::from("name"), OwnedValue::String("x".repeat(512 * 1024)));
    blob_map.insert(&String::from("score"), OwnedValue::U64(1));
    let mut cell = OwnedCell::new_with_id(blob.id, &blob_id, OwnedValue::Map(blob_map));
    chunks.write_cell(&mut cell).unwrap();

    let chunk = &chunks.list[0];
    let seg = chunk.locate_segment(chunks.address_of(&blob_id)).unwrap();
    chunk.tiered_manager.as_ref().unwrap().explicit_evict(chunk, 1).unwrap();
    assert!(seg.is_cold(), "blob segment should be cold after explicit eviction");

    let read_back = chunks.read_cell(&blob_id).unwrap();
    assert_eq!(read_back.data["id"].i64(), Some(&1));
    assert!(seg.is_hot(), "reading a cold blob segment should promote it");
}
```

- [ ] **Step 2: Run the new tiered tests to verify they fail**

Run:

```bash
cargo test test_blob_segments_evict_before_regular_segments --lib
cargo test test_blob_segments_promote_on_read_after_eviction --lib
```

Expected: FAIL because cleaner and CLOCK still only know about one active head and do not prefer blob-class victims.

- [ ] **Step 3: Implement dual-head protection and blob-first victim selection**

Update `src/ram/chunk.rs` cleaner filtering:

```rust
fn segs_for_combine_cleaner_impl(&self, full: bool) -> Vec<(AArc<Segment>, f32)> {
    let mut mapping: Vec<_> = self
        .segments()
        .into_iter()
        .map(|seg| {
            let living = seg.living_space() as f32;
            let segment_utilization = living / SEGMENT_SIZE_U32 as f32;
            (seg, segment_utilization)
        })
        .filter(|(seg, utilization)| {
            *utilization < 1.0
                && (full || *utilization < DEAD_RATE_FOR_COMBINE_CLEANER)
                && !self.is_active_head(seg.id)
                && seg.no_references()
                && seg.is_hot()
                && !seg.cleaned_without_progress()
        })
        .collect();
    mapping.sort_by(|(_, util1), (_, util2)| util1.partial_cmp(util2).unwrap());
    let max_segments = if full { mapping.len() } else { MAX_SEGMENTS_FOR_CLEANER };
    mapping.truncate(max_segments);
    mapping
}
```

Update `src/ram/tiered/clock.rs` to prefer blob victims and skip either active head:

```rust
use crate::ram::segs::SegmentClass;

impl ClockEvictionPolicy {
    pub fn select_victim(&self, chunk: &Chunk) -> Option<lightning::aarc::Arc<Segment>> {
        let segments = chunk.segments();
        if segments.is_empty() {
            return None;
        }

        let num_segments = segments.len();
        let start_pos = self.cursor.load(Ordering::Relaxed);
        let cooldown_ms = self.promotion_cooldown_ms.load(Ordering::Relaxed);

        for preferred_class in [SegmentClass::Blob, SegmentClass::Regular] {
            for i in 0..num_segments {
                let pos = (start_pos + i) % num_segments;
                let segment = &segments[pos];

                if segment.class() != preferred_class {
                    continue;
                }
                if chunk.is_active_head(segment.id) {
                    continue;
                }
                if !segment.no_references() || segment.is_cold() {
                    continue;
                }
                if cooldown_ms > 0 && segment.recently_promoted_within(cooldown_ms) {
                    continue;
                }

                if segment.decrement_and_check() {
                    self.cursor.store((pos + 1) % num_segments, Ordering::Relaxed);
                    return Some(segment.clone());
                }
            }
        }

        None
    }
}
```

- [ ] **Step 4: Run the new tiered tests to verify they pass**

Run:

```bash
cargo test test_blob_segments_evict_before_regular_segments --lib
cargo test test_blob_segments_promote_on_read_after_eviction --lib
```

Expected: PASS. The first test should show a cold blob segment and no cold regular segment after the first eviction. The second test should show a blob segment returning to hot state after a read.

- [ ] **Step 5: Commit**

```bash
git add src/ram/chunk.rs src/ram/tiered/clock.rs src/ram/tiered/tests.rs
git commit -m "feat: prioritize blob segments in eviction"
```

### Task 4: Classify Recovered Segments And Keep Blob Recovery Cold

**Files:**
- Modify: `src/ram/segs.rs`
- Modify: `src/ram/chunk.rs`
- Modify: `src/ram/recovery.rs`
- Test: `src/ram/recovery.rs`

- [ ] **Step 1: Add the failing recovery tests**

Append these tests inside the existing `mod tests` in `src/ram/recovery.rs`:

```rust
    fn setup_test_schemas_with_blob() -> LocalSchemasCache {
        let regular = Schema::new("recovery_regular", None, default_fields(), false, false);
        let blob = Schema::new("recovery_blob", None, default_fields(), false, false).with_blobs(true);
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(regular);
        schemas.debug_only_new_schema(blob);
        schemas
    }

    #[test]
    fn test_recovery_keeps_blob_segments_cold() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();
        let tiered = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
            crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
                threshold: 0.9,
                lower_watermark: 0.8,
                physical_memory_limit: 32 * 1024 * 1024,
                promotion_cooldown_ms: 0,
            }),
        ));

        let regular_id = Id::new(10, 1);
        let blob_id = Id::new(11, 1);

        {
            let schemas = setup_test_schemas_with_blob();
            let regular = schemas.get_by_name("recovery_regular").unwrap();
            let blob = schemas.get_by_name("recovery_blob").unwrap();

            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                Some(tiered.clone()),
                false,
                Some(raft_path.clone()),
            );

            let mut regular_cell = OwnedCell::new_with_id(
                regular.id,
                &regular_id,
                data_map_value!(id: 1_i32, data: vec![7_u8; DATA_SIZE]),
            );
            chunks.write_cell(&mut regular_cell).unwrap();

            let blob_payload: Vec<u8> = std::iter::repeat(9_u8).take(512 * 1024).collect();
            let mut blob_cell = OwnedCell::new_with_id(
                blob.id,
                &blob_id,
                data_map_value!(id: 2_i32, data: blob_payload),
            );
            chunks.write_cell(&mut blob_cell).unwrap();

            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }
        }

        {
            let schemas = setup_test_schemas_with_blob();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                Some(tiered.clone()),
                true,
                Some(raft_path.clone()),
            );

            assert!(total_cold_segments(&chunks) > 0, "blob recovery should leave a cold segment behind");
            assert!(total_hot_segments(&chunks) > 0, "regular recovery should still restore hot segments");

            let recovered = chunks.read_cell(&blob_id).unwrap();
            assert_eq!(recovered.data["id"].i32(), Some(&2));
        }
    }

    #[test]
    fn test_classify_segment_from_data_rejects_mixed_schema_classes() {
        let _ = env_logger::try_init();
        let schemas = setup_test_schemas_with_blob();
        let regular = schemas.get_by_name("recovery_regular").unwrap();
        let blob = schemas.get_by_name("recovery_blob").unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
            false,
            Some(raft_path),
        );
        let chunk = &chunks.list[0];

        let mut first = OwnedCell::new_with_id(
            regular.id,
            &Id::new(20, 1),
            data_map_value!(id: 1_i32, data: vec![1_u8; DATA_SIZE]),
        );
        let mut second = OwnedCell::new_with_id(
            regular.id,
            &Id::new(20, 2),
            data_map_value!(id: 2_i32, data: vec![2_u8; DATA_SIZE]),
        );

        chunks.write_cell(&mut first).unwrap();
        chunks.write_cell(&mut second).unwrap();

        let second_addr = chunks.address_of(&Id::new(20, 2));
        let seg = chunk.locate_segment(second_addr).unwrap();
        let used = seg.append_header.load(Ordering::Acquire) - seg.addr;
        let mut bytes = unsafe { std::slice::from_raw_parts(seg.addr as *const u8, used) }.to_vec();

        let schema_offset_in_header = 8 + 4;
        let second_offset = second_addr - seg.addr;
        bytes[second_offset + crate::ram::entry::ENTRY_HEAD_SIZE + schema_offset_in_header
            ..second_offset + crate::ram::entry::ENTRY_HEAD_SIZE + schema_offset_in_header + 4]
            .copy_from_slice(&blob.id.to_le_bytes());

        let err = classify_segment_from_data(chunk, &bytes).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
```

- [ ] **Step 2: Run the recovery tests to verify they fail**

Run:

```bash
cargo test test_recovery_keeps_blob_segments_cold --lib
cargo test test_classify_segment_from_data_rejects_mixed_schema_classes --lib
```

Expected: FAIL because recovery does not yet classify segments from cell headers, blob segments can still recover hot, and the mixed-schema classifier helper does not exist.

- [ ] **Step 3: Implement recovery classification, cold-only blob restore, and head reset**

Update `src/ram/segs.rs` so recovery can allocate directly with a class:

```rust
impl SegmentAllocator {
    pub fn alloc_seg_at_id(
        &self,
        seg_id: u64,
        seq_id: u64,
        file_manager: &Arc<SegmentFileManager>,
    ) -> Option<Segment> {
        self.alloc_seg_at_id_with_class(seg_id, seq_id, SegmentClass::Regular, file_manager)
    }

    pub fn alloc_seg_at_id_with_class(
        &self,
        seg_id: u64,
        seq_id: u64,
        class: SegmentClass,
        file_manager: &Arc<SegmentFileManager>,
    ) -> Option<Segment> {
        let addr = self.addr_by_id(seg_id as usize);
        if addr >= self.limit {
            return None;
        }

        let required_end = addr + SEGMENT_SIZE;
        loop {
            let current_offset = self.offset.load(Ordering::Relaxed);
            if current_offset >= required_end {
                break;
            }
            if self
                .offset
                .compare_exchange(
                    current_offset,
                    required_end,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }

        Some(Segment::new(
            seg_id,
            seq_id,
            self.chunk_id,
            addr,
            true,
            class,
            file_manager.clone(),
        ))
    }
}
```

Add recovery classification to `src/ram/recovery.rs`:

```rust
use crate::ram::segs::SegmentClass;

fn classify_segment_from_data(chunk: &Chunk, data: &[u8]) -> io::Result<SegmentClass> {
    let mut cursor = data.as_ptr() as usize;
    let bound = cursor + data.len();
    let mut seen: Option<SegmentClass> = None;

    while cursor < bound {
        let (entry_header, _) = Entry::decode_from(cursor, |_, header| header);
        let entry_size = ENTRY_HEAD_SIZE + entry_header.content_length as usize;
        if entry_size == 0 || entry_size > SEGMENT_SIZE || entry_size < ENTRY_HEAD_SIZE {
            break;
        }

        if entry_header.entry_type == EntryType::CELL {
            let content_addr = Entry::content_pos(cursor);
            let cell_header = cell_header_from_entry_content_addr(content_addr);
            let schema = chunk.meta.schemas.get(&cell_header.schema).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("missing schema {} during recovery", cell_header.schema),
                )
            })?;

            let current = if schema.blobs {
                SegmentClass::Blob
            } else {
                SegmentClass::Regular
            };

            if let Some(previous) = seen {
                if previous != current {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "mixed blob and regular cells found in one recovered segment",
                    ));
                }
            } else {
                seen = Some(current);
            }
        }

        cursor += entry_size;
    }

    Ok(seen.unwrap_or(SegmentClass::Regular))
}

fn should_recover_as_cold(
    chunk: &Chunk,
    file_info: &SegmentFileInfo,
    class: SegmentClass,
    current_hot_segments: usize,
) -> bool {
    if class == SegmentClass::Blob {
        return true;
    }

    if let Some(ref tiered_manager) = chunk.tiered_manager {
        let physical_limit = tiered_manager.shared_pool().physical_memory_limit;
        let existing_hot = chunk
            .segs
            .get(&(file_info.seg_id as usize))
            .map(|segment| segment.is_hot())
            .unwrap_or(false);
        let additional_hot_segments = usize::from(!existing_hot);
        let would_exceed_limit = current_hot_segments
            .checked_add(additional_hot_segments)
            .and_then(|segments| segments.checked_mul(SEGMENT_SIZE))
            .map(|bytes| bytes > physical_limit)
            .unwrap_or(true);

        file_info.is_backup && would_exceed_limit
    } else {
        false
    }
}
```

Change the recovery planning loop to classify before choosing hot or cold, then carry the class into allocation:

```rust
struct RecoveryPlanItem {
    file_info: SegmentFileInfo,
    class: SegmentClass,
    is_cold: bool,
}

let recovery_decisions: Vec<RecoveryPlanItem> = {
    let mut planned_global_hot_segments = chunks
        .iter()
        .find_map(|chunk| {
            chunk
                .tiered_manager
                .as_ref()
                .map(|manager| manager.shared_pool().total_hot_segments())
        })
        .unwrap_or(0);

    files
        .iter()
        .map(|file_info| {
            let chunk = &chunks[file_info.chunk_id];
            let existing_hot = chunk
                .segs
                .get(&(file_info.seg_id as usize))
                .map(|segment| segment.is_hot())
                .unwrap_or(false);
            let file_data = load_file_to_memory(&file_info.path)?;
            let class = classify_segment_from_data(chunk, &file_data)?;
            drop(file_data);

            let is_cold = should_recover_as_cold(chunk, file_info, class, planned_global_hot_segments);
            if is_cold {
                if existing_hot {
                    planned_global_hot_segments = planned_global_hot_segments.saturating_sub(1);
                }
            } else if !existing_hot {
                planned_global_hot_segments += 1;
            }

            Ok(RecoveryPlanItem {
                file_info: file_info.clone(),
                class,
                is_cold,
            })
        })
        .collect::<io::Result<Vec<_>>>()?
};
```

When allocating or reusing segments during recovery, set or preserve the class and reset lane heads after the full scan:

```rust
let segment = if let Some(existing_seg) = chunk.segs.get(&(item.file_info.seg_id as usize)) {
    existing_seg.set_class(item.class);
    existing_seg
} else {
    let new_seg = chunk
        .allocator
        .alloc_seg_at_id_with_class(
            item.file_info.seg_id,
            item.file_info.seq_id,
            item.class,
            &chunk.file_manager,
        )
        .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "Cannot allocate segment"))?;
    let seg_id = new_seg.id as usize;
    chunk.put_segment(new_seg);
    chunk.segs.get(&seg_id).unwrap()
};

// after segment processing is complete for all chunks
for chunk in chunks {
    chunk.reset_heads_after_recovery();
}
```

Add the head reset helper to `src/ram/chunk.rs`:

```rust
impl Chunk {
    pub fn reset_heads_after_recovery(&self) {
        let mut regular_head = NO_HEAD_SEG_ID;
        for seg in self.segments() {
            if seg.is_hot() && seg.class() == SegmentClass::Regular {
                regular_head = seg.id;
                break;
            }
        }

        self.head_seg_ids[SegmentClass::Regular.lane()].store(regular_head, Ordering::Release);
        self.head_seg_ids[SegmentClass::Blob.lane()].store(NO_HEAD_SEG_ID, Ordering::Release);
    }
}
```

- [ ] **Step 4: Run the recovery tests to verify they pass**

Run:

```bash
cargo test test_recovery_keeps_blob_segments_cold --lib
cargo test test_classify_segment_from_data_rejects_mixed_schema_classes --lib
```

Expected: PASS. Blob segments should be recovered cold even when a hot regular segment is also restored, and the mixed-schema byte slice should return `io::ErrorKind::InvalidData`.

- [ ] **Step 5: Commit**

```bash
git add src/ram/segs.rs src/ram/chunk.rs src/ram/recovery.rs
git commit -m "feat: recover blob segments as cold storage"
```

## Self-Check Before Execution

- [ ] Run `cargo test blob_schema --lib`
- [ ] Run `cargo test test_blob_segments_evict_before_regular_segments --lib`
- [ ] Run `cargo test test_blob_segments_promote_on_read_after_eviction --lib`
- [ ] Run `cargo test test_recovery_keeps_blob_segments_cold --lib`
- [ ] Run `cargo test test_classify_segment_from_data_rejects_mixed_schema_classes --lib`
- [ ] Run a broader regression sweep for touched modules:

```bash
cargo test ram::tests::chunk --lib
cargo test ram::tiered::tests --lib
cargo test ram::recovery::tests --lib
```

- [ ] Confirm no placeholder text remains in this plan.
- [ ] Confirm every use of blob identity resolves through `CellHeader.schema` and the schema table.
- [ ] Confirm both active heads are protected from cleaner and eviction selection.
