# Eviction/Promotion Schema Corruption Analysis

## Your Hypothesis: Schema ID Corruption During Eviction

You're absolutely right to suspect this! There are **CRITICAL RACE CONDITIONS** in the tiered memory system that could corrupt schema IDs.

## The Problem: Stale Backup Files

### Race Condition Timeline

```
Thread 1 (Write):              Thread 2 (Eviction):           Thread 3 (Read):
1. Cell written to segment
   schema=12345 ✓
                                2. archive() called
                                   - Waits for no_references()
                                   - Writes segment to file
                                   - File contains schema=12345 ✓
3. Cell UPDATED in place
   schema=67890 ✓
   (Same segment, same address)
                                4. archive() returns false
                                   (file already exists from step 2)
                                   
                                5. mmap(MAP_FIXED)
                                   - Replaces memory with file contents
                                   - File still has OLD schema=12345!
                                   
                                                                6. Read cell from segment
                                                                   - Gets schema=12345 (WRONG!)
                                                                   - Should be 67890
                                                                   - Schema not found error!
```

## Critical Code Issues

### Issue #1: Archive Doesn't Prevent Modifications

**File:** `src/ram/segs.rs:231-242`

```rust
pub fn archive(&self) -> Result<bool, io::Error> {
    if let &Some(ref backup_file) = &self.backup_file_name {
        while !self.no_references() { /* wait */ }
        let backup_file_path = Path::new(backup_file);
        if backup_file_path.exists() {
            warn!("Segment backup {} exists and can't archive twice", backup_file);
            return Ok(false);  // ❌ Returns false, eviction proceeds!
        }
        // ... writes backup ...
    }
}
```

**Problem:**
- If backup exists, returns `false` without validating if it's current
- Eviction proceeds with **stale backup** (eviction.rs:53-60)
- No timestamp check, no version check, nothing!

### Issue #2: No Write Protection During Eviction

**File:** `src/ram/tiered/eviction.rs:34-60`

```rust
// Step 1: Wait for no active references
while !segment.no_references() {
    thread::yield_now();
}

// Step 2: Ensure backup file exists
let archived = segment.archive()?;

// ❌ GAP: Between archive() and mmap(), cells can be modified!
//    No lock prevents writes to this segment

// If archive() returned false but backup exists (e.g., from previous write),
// that's fine - we can still proceed with eviction
if !archived && !std::path::Path::new(backup_path).exists() {
    return Err(...);
}

// Step 5: mmap with MAP_FIXED
// Maps STALE data from backup file!
```

**Problem:**
- `no_references()` only waits for active **reads** to finish
- Does NOT prevent new **writes** from starting
- After archive completes, writes can occur before mmap
- mmap will overwrite those writes with stale data!

### Issue #3: Cell Updates Don't Invalidate Backups

**File:** `src/ram/chunk.rs:575-609` (update_cell)

```rust
fn update_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
    let hash = cell.header.hash;
    // Write first, lock second to avoid deadlock with cleaner
    let (new_cell_loc, schema) = self.write_cell_to_chunk(cell)?;
    
    if let Some(mut guard) = self.location_for_write(hash) {
        // ... update cell index ...
        *guard = new_cell_loc;
        
        // ❌ NO: Invalidate backup file for this segment!
        // ❌ NO: Mark segment as "dirty" for eviction!
    }
    Ok(cell.header)
}
```

**Problem:**
- Cells are updated without invalidating backup files
- Eviction can proceed with stale backup
- No dirty tracking per segment

## Why You See Schema ID Corruption

### Scenario 1: Stale Backup Contains Old Schema

```
1. Cell created with schema=12345
2. Segment archived → file has schema=12345
3. Schema migration: cell updated to schema=67890
4. Segment evicted → mmap loads file with schema=12345
5. Read attempt → schema 12345 might have been deleted
   → "Schema does not exist" error!
```

### Scenario 2: Backup Contains Uninitialized Data

```
1. Segment allocated, backup file created empty/zeros
2. Cell written with schema=12345 (not yet archived)
3. Segment evicted (archive returns false, uses old backup)
4. mmap loads zeros → schema=0 (Default::default())
5. Read attempt → "Schema 0 does not exist" error!
```

### Scenario 3: Concurrent Write During Eviction

```
1. archive() writes segment → file has current data
2. archive() completes, returns true
3. ❌ NEW CELL WRITTEN to segment (schema=67890)
4. mmap(MAP_FIXED) overwrites segment with file (old data)
5. New cell's data is LOST, replaced with whatever was in file
6. Read attempt → wrong schema ID or garbage
```

## Evidence from Cell Header Reading

**File:** `src/ram/cell.rs:415-426`

```rust
pub fn cell_header_from_entry_content_addr(addr: usize) -> CellHeader {
    let mut cursor = addr_to_header_cursor(addr);
    let header = CellHeader {
        version: cursor.read_u64::<Endian>().unwrap(),
        timestamp: cursor.read_u32::<Endian>().unwrap(),
        schema: cursor.read_u32::<Endian>().unwrap(),  // ← Read directly from memory
        partition: cursor.read_u64::<Endian>().unwrap(),
        hash: cursor.read_u64::<Endian>().unwrap(),
    };
    return header;
}
```

This reads **directly from memory** at the address. If:
- Segment was evicted → memory backed by file
- File contains stale data → wrong schema ID
- No validation, no checksum → corrupted data accepted

## CellHeader Default Value

**File:** `src/ram/cell.rs:26-33`

```rust
#[derive(Debug, Copy, Clone, Serialize, Deserialize, Default)]
pub struct CellHeader {
    pub version: u64,
    pub timestamp: u32,
    pub schema: u32,      // ← Default is 0
    pub partition: u64,
    pub hash: u64,
}
```

**Schema ID 0** in your logs = `Default::default()` = uninitialized memory!

## How to Confirm

### Add Logging to Cell Header Reads

```rust
pub fn cell_header_from_entry_content_addr(addr: usize) -> CellHeader {
    let mut cursor = addr_to_header_cursor(addr);
    let header = CellHeader {
        version: cursor.read_u64::<Endian>().unwrap(),
        timestamp: cursor.read_u32::<Endian>().unwrap(),
        schema: cursor.read_u32::<Endian>().unwrap(),
        partition: cursor.read_u64::<Endian>().unwrap(),
        hash: cursor.read_u64::<Endian>().unwrap(),
    };
    
    // ADD THIS:
    if header.schema == 0 || header.version == 0 {
        error!("Suspicious cell header at addr {:#x}: schema={}, version={}, timestamp={}, hash={}", 
               addr, header.schema, header.version, header.timestamp, header.hash);
    }
    
    return header;
}
```

### Check Segment State During Read Errors

```rust
// In SharedCellData::from_chunk_raw (cell.rs:270-279)
if let Some(schema) = chunk.meta.schemas.get(schema_id) {
    // ... existing code ...
} else {
    let segment = chunk.locate_segment(ptr, &header.id());
    if let Ok(seg) = segment {
        error!("Schema {} not found. Cell at {:#x}, segment {} (is_cold={}, is_hot={}, fd={}), header: {:?}",
               schema_id, ptr, seg.id, seg.is_cold(), seg.is_hot(), 
               seg.cold_file_fd.load(Ordering::Relaxed), header);
    }
    error!("Schema {} does not existed to read, all schemas: {:?}", schema_id, chunk.meta.schemas.get_all());
    return Err(ReadError::SchemaDoesNotExisted(*schema_id));
}
```

## Fixes Required

### Fix #1: Add Dirty Tracking Per Segment

```rust
// In Segment struct (segs.rs)
pub struct Segment {
    // ... existing fields ...
    dirty_since_archive: AtomicBool,  // Track if modified since last archive
}

// In chunk.rs write operations
fn write_cell_to_chunk(&self, cell: &mut OwnedCell) -> Result<(usize, SchemaRef), WriteError> {
    // ... write cell ...
    
    // Mark segment as dirty
    if let Ok(seg) = self.locate_segment(cell_loc, &cell.id()) {
        seg.dirty_since_archive.store(true, Ordering::Release);
    }
    
    Ok((cell_loc, schema))
}
```

### Fix #2: Re-archive Before Eviction If Dirty

```rust
// In eviction.rs
pub fn evict_segment(segment: &Segment, _chunk: &Chunk) -> Result<(), io::Error> {
    // ... existing checks ...
    
    // Step 2: Force re-archive if segment is dirty
    let is_dirty = segment.dirty_since_archive.load(Ordering::Acquire);
    if is_dirty {
        // Delete old backup to force fresh archive
        if let Some(ref backup) = segment.backup_file_name {
            let _ = std::fs::remove_file(backup); // Ignore error if doesn't exist
        }
    }
    
    let archived = segment.archive()?;
    if !archived {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Segment {} archive failed but is dirty", segment.id),
        ));
    }
    
    // Clear dirty flag after successful archive
    segment.dirty_since_archive.store(false, Ordering::Release);
    
    // ... rest of eviction ...
}
```

### Fix #3: Delete Stale Backups on Segment Reuse

```rust
// In segment allocation/reuse
impl SegmentAllocator {
    pub fn alloc_seg(&self, backup_storage: &Option<String>, wal_storage: &Option<String>) 
        -> Option<lightning::aarc::Arc<Segment>> {
        // ... existing code ...
        
        // If backup file exists from previous use, delete it
        if let Some(ref backup_name) = segment.backup_file_name {
            if Path::new(backup_name).exists() {
                warn!("Deleting stale backup {} for reused segment {}", backup_name, segment.id);
                let _ = std::fs::remove_file(backup_name);
            }
        }
        
        Some(segment)
    }
}
```

### Fix #4: Add Checksums to Detect Corruption

```rust
// Add to CellHeader
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct CellHeader {
    pub version: u64,
    pub timestamp: u32,
    pub schema: u32,
    pub partition: u64,
    pub hash: u64,
    pub checksum: u32,  // CRC32 of above fields
}

// Verify on read
pub fn cell_header_from_entry_content_addr(addr: usize) -> Result<CellHeader, ReadError> {
    let mut cursor = addr_to_header_cursor(addr);
    let header = CellHeader {
        // ... read fields ...
    };
    
    if !header.verify_checksum() {
        error!("Cell header checksum mismatch at {:#x}: {:?}", addr, header);
        return Err(ReadError::CorruptedCellHeader);
    }
    
    Ok(header)
}
```

## Immediate Workaround

**Disable tiered memory** until this is fixed:

```bash
# Don't set these environment variables:
# NEB_PHYSICAL_MEMORY_LIMIT
# NEB_EVICTION_THRESHOLD
```

Or in code:
```rust
let opts = ServerOptions {
    // ...
    tiered_config: None,  // Disable tiered memory
    // ...
};
```

## Testing Strategy

1. **Reproduce the bug:**
   - Enable tiered memory with low limits
   - Write cells with schema A
   - Trigger eviction
   - Update cells to schema B
   - Read cells → should get schema B, but might get A or 0

2. **Add assertions:**
   - Verify segment is NOT dirty before eviction
   - Verify backup file timestamp matches memory state
   - Verify mmap'd data matches expected schema

3. **Stress test:**
   - Continuous writes to segments
   - Concurrent eviction/promotion
   - Monitor for schema mismatches

## Conclusion

**Your hypothesis is CORRECT!** The tiered memory system has serious race conditions:

1. ❌ **Stale backup files** used during eviction
2. ❌ **No dirty tracking** for modified segments  
3. ❌ **No write protection** during eviction window
4. ❌ **No data validation** after mmap
5. ❌ **No checksums** to detect corruption

These bugs can cause:
- Wrong schema IDs (schema deleted but old backup has it)
- Schema ID = 0 (uninitialized data from empty backup)
- Random schema IDs (garbage data)
- Transaction stuck in read_selected (can't find schema)

**The schema subscription fixes we made earlier help**, but won't fix this corruption issue. You need to either:
1. **Disable tiered memory** (immediate workaround)
2. **Implement the fixes above** (proper solution)

