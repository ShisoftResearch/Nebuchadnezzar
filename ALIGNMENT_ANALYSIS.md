# Memory Alignment Analysis for Misaligned Pointer Dereference

## Executive Summary

The misaligned pointer dereference error (`address must be a multiple of 0x4 but is 0x6c395a4000e6`) occurs when trying to read a `u32` from a corrupted cell location. The root cause is that **cell locations stored in the cell index are corrupted**, not that the alignment logic itself is broken.

## The Alignment Architecture

### 1. **Segment-Level Alignment (segs.rs)**

Segments allocate memory in 8MB chunks that are guaranteed to be 8-byte aligned:

```rust:140:164:src/ram/segs.rs
pub fn try_acquire(&self, size: u32) -> Option<usize> {
    let size = size as usize;
    loop {
        let curr_last = self.append_header.load(Ordering::Acquire);
        let exp_last = curr_last + size;
        if exp_last > self.bound {
            return None;
        } else {
            if self
                .append_header
                .compare_exchange(curr_last, exp_last, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                continue;
            } else {
                debug_assert_eq!(
                    align_address(8, curr_last),
                    curr_last,
                    "Acquired address is not aligned"
                );
                return Some(curr_last);
            }
        }
    }
}
```

**Key Observation**: The segment guarantees 8-byte aligned addresses via `debug_assert_eq!` on line 155-159.

### 2. **Cell-Level Alignment (cell.rs)**

When writing cells, the system:
1. Aligns the entry body size to 8 bytes
2. Acquires an aligned address from the segment
3. Validates the address is 8-byte aligned

```rust:154:171:src/ram/cell.rs
let entry_body_size = align_address(8, tail_offset + CELL_HEADER_SIZE);
let total_size = (ENTRY_HEAD_SIZE + entry_body_size) as u32;
if total_size > MAX_CELL_SIZE {
    return Err(WriteError::CellIsTooLarge(total_size as usize));
}
let addr_opt = chunk.try_acquire(total_size);
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
        debug_assert_eq!(align_address(8, addr), addr, "Entry address is not aligned");
```

**Key Observation**: Cell addresses are validated to be 8-byte aligned on line 171.

### 3. **Field-Level Alignment (io/reader.rs and io/writer.rs)**

Field reads and writes use `align_address_with_ty()` to ensure proper alignment RELATIVE to the base address:

```rust:77:84:src/ram/io/reader.rs
(_, _, false, true) => {
    *tail_offset = align_address_with_ty(field.data_type, *tail_offset); // Ensure proper alignment
    trace!(
        "Using non-nullable aligned tail offset for {}, offset {}",
        field.name,
        tail_offset
    );
    (*tail_offset, true)
```

```rust:8:20:src/ram/io/mod.rs
pub fn align_address(ty_align: usize, addr: usize) -> usize {
    if ty_align == 0 {
        return addr;
    }
    let alignment = ty_align;
    let mask = alignment - 1;
    let misalign = addr & mask;
    if misalign == 0 {
        addr
    } else {
        addr + alignment - misalign
    }
}
```

**Key Observation**: Alignment functions work on OFFSETS within a cell, not on absolute addresses.

## The Problem: Alignment Propagation

### Why Field Alignment Doesn't Help with Corrupted Base Addresses

The alignment logic assumes the **base address** (cell location) is correct. If the base is misaligned, ALL subsequent field reads will be misaligned:

```
Scenario: Corrupted Base Address
==================================
Base address (corrupted):  0x6c395a4000e6  (offset: 6 from 8-byte boundary)
Field offset (aligned):    0               (correctly aligned to 0)
Final address:             0x6c395a4000e6  (STILL misaligned!)

For u32 read: 0x6c395a4000e6 % 4 = 2 ❌ PANIC!
```

Even though the field offset (0) is "aligned", the absolute address inherits the misalignment from the base.

### The Mathematics of 0xE6 Addresses

Addresses ending in `0xE6` (230 in decimal) are particularly problematic:

```
0xE6 = 230 decimal
230 % 2 = 0 ✓ (2-byte alignment OK)
230 % 4 = 2 ✗ (4-byte alignment FAIL)
230 % 8 = 6 ✗ (8-byte alignment FAIL)
```

This means:
- ✗ Cannot read `u32` (needs 4-byte alignment)
- ✗ Cannot read `u64` (needs 8-byte alignment)  
- ✗ Cannot read most struct fields
- ✓ Can read `u8` or `u16` only

## What We Know About the Corruption

### Evidence from Crash Reports

1. **First crash**: `Cannot decode entry header: 1852795252`
   - Indicates invalid entry type bits
   - Suggests reading from wrong memory location

2. **Second crash**: `Cannot decode entry header: 1634296687`
   - Same pattern as first crash
   - Confirms systematic corruption

3. **Current crash**: `misaligned pointer dereference: address must be a multiple of 0x4 but is 0x6c395a4000e6`
   - Address stored in cell_index is corrupt
   - Stack trace shows: `dovahkiin::types::u32_io::read` → reading string length → misaligned address

### Where Corruption Could Occur

The cell location flows through this path:

```
1. Segment::try_acquire()  → returns 8-byte aligned address ✓
         ↓
2. Chunk::try_acquire()    → wraps segment address in PendingEntry ✓
         ↓
3. cell.write_to_chunk_with_schema() → validates address is aligned ✓
         ↓
4. chunk.write_cell_to_chunk() → returns (cell_loc, schema) ✓
         ↓
5. chunk.write_cell() or chunk.update_cell() → stores in cell_index ⚠️
         ↓
6. cell_index.insert() / cell_index update → WordMap storage ⚠️
```

Possible corruption points (marked with ⚠️):
- **Race condition in WordMap**: Concurrent updates overwriting data
- **Memory corruption**: Buffer overflow, use-after-free, etc.
- **Pointer arithmetic error**: Incorrect offset calculation somewhere
- **Serialization/recovery bug**: Corruption during backup/WAL replay

## Debug Validation Added

I've added comprehensive validation in `src/ram/chunk.rs` to catch corruption at the source:

```rust
#[cfg(debug_assertions)]
fn validate_cell_location(&self, addr: usize, context: &str) -> bool {
    // Check 1: NULL address
    if addr == 0 { return false; }
    
    // Check 2: 8-byte alignment (CRITICAL)
    if addr % 8 != 0 { return false; }
    
    // Check 3: Suspicious high bits
    if addr > 0x0000_FFFF_FFFF_FFFF { return false; }
    
    // Check 4: Within segment bounds
    if let Some(segment) = self.locate_segment(addr, &Id::new(0, 0)) {
        let seg_start = segment.addr;
        let seg_end = seg_start + SEGMENT_SIZE;
        if addr < seg_start || addr >= seg_end { return false; }
    }
    
    true
}
```

This validation is called at **every cell index update point**:
- `write_cell()` - when inserting new cells
- `update_cell()` - when updating existing cells (both old and new locations)
- `upsert_cell()` - when upserting cells (both old and new locations)
- `update_cell_by()` - when updating via callback (both old and new locations)

### What the Validation Will Catch

When corruption is detected, you'll see detailed error logs like:

```
[ERROR] [Chunk 0] Invalid cell location at update_cell new location(hash=12345):
        address 0x6c395a4000e6 is not 8-byte aligned
```

Or:

```
[ERROR] Corrupted cell location 0x6c395a4000e6 detected for hash 12345 in update_cell_by
        - aborting to prevent further corruption
```

## Comprehensive Test Coverage

### 1. **Alignment Root Cause Tests** (`alignment_root_cause_test.rs`)

Three tests demonstrating the mathematical principles:
- `test_alignment_propagation_from_base()` - Shows how misaligned base corrupts all reads
- `test_cell_location_must_be_8_byte_aligned()` - Validates cell location requirements
- `test_alignment_math()` - Explains why 0xE6 addresses fail

### 2. **Alignment Verification Tests** (`alignment_tests.rs`)

Eight tests verifying the system's alignment guarantees:
1. Internal validation function testing
2. Cell location alignment after writes
3. Cell location alignment after updates
4. Varying size alignment tests
5. Multi-segment alignment tests
6. Entry header alignment tests
7. Misaligned address detection tests
8. Concurrent alignment stress tests

### 3. **Corruption Detection Tests** (`corruption_tests.rs`)

Eight stress tests designed to trigger corruption:
1. Rapid concurrent updates on same cell
2. Varying size concurrent updates
3. Multi-cell concurrent transactions
4. Rapid commit sequences
5. Interleaved prepare/commit operations
6. Maximum concurrency stress (500 concurrent txns)
7. WikiData import scenario (64 workers, batched)
8. `update_cell_by` stress test

## How to Diagnose Your Issue

### Step 1: Run with Debug Assertions

Build your application with debug assertions enabled:

```bash
cargo build --profile dev-opt  # Or use dev profile
```

The validation will now catch corrupted addresses **before** they cause panics.

### Step 2: Run the Alignment Tests

```bash
# Test the alignment validation logic
cargo test --lib alignment_root_cause_test -- --nocapture

# Test actual alignment in the system
cargo test --lib alignment_tests -- --nocapture
```

All tests should pass, confirming the alignment infrastructure is sound.

### Step 3: Run the Corruption Tests

```bash
# Run all corruption tests
cargo test --lib corruption_tests -- --nocapture

# Run specific stress test
cargo test --lib test_wikidata_import_scenario -- --nocapture
```

If these trigger the corruption, you'll see detailed debug output.

### Step 4: Run Your Application

Run your WikiData import with debug build:

```bash
./wikidata_cli import wikidata20160104.json --config --workers 64 100000 2
```

Watch for error logs showing WHERE the corruption is introduced.

## Expected Output When Corruption is Found

When the debug validation catches corruption, you'll see:

```
[ERROR] [Chunk 2] Invalid cell location at update_cell new location(hash=8675309):
        address 0x6c395a4000e6 is not 8-byte aligned
        
[ERROR] Attempting to store invalid cell location 0x6c395a4000e6 in cell index for hash 8675309 (update)

thread 'tokio-runtime-worker' panicked at src/ram/chunk.rs:517:
assertion failed: self.validate_cell_location(...)
```

This tells you:
1. **Which chunk**: Chunk 2
2. **Which operation**: `update_cell`
3. **Which hash**: 8675309
4. **The corrupted address**: 0x6c395a4000e6
5. **The exact line**: src/ram/chunk.rs:517

## Next Steps for Investigation

If the validation catches corruption, investigate:

1. **WordMap implementation**: Check for race conditions in concurrent updates
2. **Segment allocation**: Verify the segment bump pointer logic
3. **Recovery/WAL replay**: Check if corruption occurs during database recovery
4. **Pointer arithmetic**: Look for any manual pointer calculations
5. **Memory safety**: Use Miri or valgrind to detect undefined behavior

## Conclusion

The alignment logic in `ram::io` is **working correctly**. The problem is not that fields aren't being aligned - it's that the **cell locations stored in the cell index are corrupted**.

The new debug validation will help pinpoint exactly WHERE and WHEN the corruption is introduced, which is the first step to fixing the root cause.

Key insights:
- ✓ Segments provide 8-byte aligned addresses
- ✓ Cells are written to 8-byte aligned addresses
- ✓ Fields are aligned RELATIVE to cell base addresses
- ✗ Cell locations in cell_index somehow become corrupted
- ✓ Debug validation will catch corruption at its source

**The corruption happens AFTER the initial allocation but BEFORE or DURING storage in the cell index.**

