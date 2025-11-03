# Bug Report: Misaligned Pointer Dereference - Systematic +6 Byte Offset Corruption

## Executive Summary

A systematic memory corruption bug causes cell memory addresses to be misaligned by exactly 6 bytes, resulting in crashes when attempting to read `u32` values from misaligned pointers. The corruption manifests consistently across multiple crashes with addresses ending in `0xE6` (offset 6 from 8-byte boundaries).

**Severity**: Critical  
**Impact**: Application crashes during normal operation  
**Reproducibility**: Reproducible under concurrent load  
**Status**: Under investigation with debug instrumentation added

## Symptoms

### Crash Pattern

```
misaligned pointer dereference: address must be a multiple of 0x4 but is 0x75b917a000e6
```

Stack trace shows crashes occur when:
- Reading string lengths (`dovahkiin::types::u32_io::read`)
- During cell field reads (`neb::ram::io::reader::read_field`)
- Within transaction operations (`neb::server::transactions`)

### Observed Crash Addresses

| Crash # | Address | Last Byte | 8-byte Offset | 4-byte Offset | Correct Address |
|---------|---------|-----------|---------------|---------------|-----------------|
| 1 | `0x6c395a4000e6` | `0xE6` (230) | 6 | 2 | `0x6c395a4000e0` |
| 2 | `0x6c51974000e6` | `0xE6` (230) | 6 | 2 | `0x6c51974000e0` |
| 3 | `0x75b917a000e6` | `0xE6` (230) | 6 | 2 | `0x75b917a000e0` |

**Pattern**: All crashes show:
- ✅ Same last byte: `0xE6` (230 decimal)
- ✅ Same 8-byte offset: 6 (addresses are NOT 8-byte aligned)
- ✅ Same 4-byte offset: 2 (causes `u32` read panics)
- ✅ Consistent difference: **Exactly +6 bytes** from correct address

## Root Cause Hypothesis

### The Corruption Point

Based on investigation, the corruption occurs in the **cell location storage/retrieval path**:

```
1. Segment::try_acquire()      → Returns 8-byte aligned address ✓ (VERIFIED)
2. Chunk::try_acquire()         → Wraps in PendingEntry { addr, seg, size, skip_sync }
3. cell.write_to_chunk_with_schema() → Uses pending_entry.addr
4. Returns addr to write_cell_to_chunk() → Returns (addr, schema)
5. write_cell/update_cell      → Stores in cell_index via *guard = cell_loc ⚠️
6. WordMap storage              → ✓ VERIFIED CORRECT (comprehensive tests)
7. Later retrieval              → *guard retrieves address
8. Address used for reads       → ❌ PANIC if misaligned
```

### Investigation Findings

#### 1. Alignment Infrastructure is Correct ✅

- **Segment allocation**: All segments return 8-byte aligned addresses (verified with tests)
- **Field alignment**: `ram::io` alignment logic works correctly (verified by code review)
- **WordMap**: Comprehensive testing proved WordMap stores/retrieves addresses correctly
  - 10,000 concurrent writes tested
  - Real segment addresses tested
  - No corruption observed in WordMap itself

#### 2. Cell Index Operations

The `cell_index` (WordMap) has only **2 data operations**:
- `lock(hash)` - Retrieve address
- `try_insert_locked(hash)` - Store address

Both operations verified correct via testing.

#### 3. The +6 Pattern

The consistent +6 offset suggests:
- **NOT random memory corruption** (would have varying offsets)
- **Systematic bug** (always adds/subtracts exactly 6)
- **Possible causes**:
  - Struct field offset confusion (accessing wrong field)
  - Pointer arithmetic error (`base + 6` instead of `base`)
  - Memory layout issue (struct padding/alignment)
  - Type confusion (reading `usize` as wrong type)

## Debug Instrumentation Added

### Alignment Assertions

Added comprehensive alignment checks at all `cell_index` interaction points:

**WRITE POINTS** (5 locations):
1. `write_cell` - New cell insertion
2. `update_cell` - Cell update
3. `upsert_cell` (update path) - Existing cell upsert
4. `upsert_cell` (insert path) - New cell upsert
5. `update_cell_by` - Callback-based update

**READ POINTS** (7 locations):
1. `location_for_read` - Get location for reading
2. `location_for_write` - Get location for writing
3. `update_cell` - Read old location
4. `upsert_cell` - Read old location
5. `update_cell_by` - Read old location
6. `remove_cell` - Read location for deletion
7. `remove_cell_by` - Read location for conditional deletion

Each assertion checks:
- Address is 8-byte aligned (`addr % 8 == 0`)
- Logs detailed error with address, offset, and operation name
- Uses helper functions: `assert_address_aligned_for_write()` and `assert_address_aligned_for_read()`

### Test Coverage

Created comprehensive test suite:

1. **Alignment Root Cause Tests** (`alignment_root_cause_test.rs`)
   - Demonstrates how misaligned base addresses propagate
   - Explains why 0xE6 addresses fail
   - Proves field alignment can't fix corrupted bases

2. **Alignment Verification Tests** (`alignment_tests.rs`) - 8 tests
   - Cell location alignment after writes/updates
   - Varying size alignment
   - Multi-segment alignment
   - Concurrent alignment stress tests

3. **Segment Alignment Tests** (`segment_alignment_test.rs`) - 6 tests
   - Segment allocation alignment
   - `try_acquire` alignment under concurrent stress
   - Address corruption pattern detection

4. **WordMap Corruption Tests** (`wordmap_test.rs`) - 3 tests
   - Basic storage/retrieval
   - 10,000 concurrent writes across 10 threads
   - Real segment address testing
   - **Result**: All tests pass, WordMap proven correct ✅

5. **Corruption Detection Tests** (`corruption_tests.rs`) - 8 stress tests
   - Rapid concurrent updates
   - Varying size concurrent updates
   - Multi-cell concurrent transactions
   - WikiData import scenario simulation

## Known Issue with Assertions

The assertions are behind `#[cfg(debug_assertions)]`, which may not be enabled in ASAN builds. The user reported crashes when running `./wikidata_cli_asan_debug`, suggesting assertions may not have fired.

**Recommendation**: Add unconditional logging to trace address flow in all builds.

## Data Flow Analysis

### Cell Address Source

All cell addresses originate from:
```rust
pending_entry.addr  // From Segment::try_acquire()
```

This flows through:
1. `write_to_chunk_with_schema()` uses `pending_entry.addr`
2. Returns `Ok(addr)` 
3. Stored via `*guard = cell_loc` in `cell_index`
4. Later retrieved via `*guard` from `cell_index`

### PendingEntry Struct Layout

```rust
pub struct PendingEntry {
    pub seg: AArc<Segment>,     // offset 0  (8 bytes)
    pub addr: usize,             // offset 8  (8 bytes) ← CORRECT
    pub size: u32,               // offset 16 (4 bytes)
    pub skip_sync: bool,         // offset 20 (1 byte)
}
```

**Note**: If code accidentally accesses offset 6 instead of offset 8, it would read part of `seg` pointer instead of `addr`, potentially explaining the +6 offset!

## Work Environment

- **Build**: ASAN debug build (`wikidata_cli_asan_debug`)
- **Workload**: WikiData import with 64 workers
- **Trigger**: High concurrency, batch processing
- **Frequency**: Reproducible under load

## Impact Assessment

### Functional Impact
- ❌ Application crashes during normal operation
- ❌ Data corruption risk (reading from wrong memory location)
- ❌ Transaction failures
- ❌ Inability to complete large imports

### Performance Impact
- Crash occurs after significant processing
- Potential data loss on restart
- Blocks production workloads

## Next Steps

### Immediate Actions

1. **Enable unconditional logging** in `location_for_read` and `location_for_write`:
   ```rust
   error!("TRACE: location_for_read hash={} addr=0x{:016x} aligned={} offset={}", 
          hash, addr, addr % 8 == 0, addr % 8);
   ```

2. **Verify assertion configuration** in ASAN builds - ensure `debug_assertions` is enabled

3. **Add logging around PendingEntry access** to verify `pending_entry.addr` value:
   ```rust
   debug!("TRACE: pending_entry.addr = 0x{:016x}", pending_entry.addr);
   ```

### Investigation Priorities

1. **Check PendingEntry field access** - Verify `pending_entry.addr` is accessed correctly (not offset 6)
2. **Pointer arithmetic review** - Search for any `+ 6` or offset calculations
3. **Struct field offsets** - Verify all struct field accesses use correct offsets
4. **Memory layout** - Check for `#[repr(C)]` vs default alignment issues
5. **Concurrency** - Examine race conditions in address storage

### Long-term Solutions

Once root cause identified:
1. Fix the specific code path causing +6 offset
2. Add permanent validation (not just debug assertions)
3. Add integration tests that reproduce the corruption
4. Consider defensive programming: validate addresses before use
5. Document alignment requirements clearly

## Files Modified

### Source Code
- `src/ram/chunk.rs` - Added alignment assertions and helper functions
- `src/ram/tests/alignment_root_cause_test.rs` - New test file
- `src/ram/tests/alignment_tests.rs` - Extended with new tests
- `src/ram/tests/segment_alignment_test.rs` - New test file
- `src/ram/tests/wordmap_test.rs` - New test file
- `src/ram/tests/mod.rs` - Added test modules
- `src/server/transactions/corruption_tests.rs` - Extended with new stress tests

### Documentation
- `ALIGNMENT_ANALYSIS.md` - Technical analysis of alignment architecture
- `FINDINGS_SUMMARY.md` - Investigation summary
- `CELL_INDEX_OPERATIONS.md` - Complete documentation of cell_index operations
- `WORDMAP_TEST_RESULTS.md` - WordMap verification results
- `DEBUG_VALIDATION.md` - Debug validation explanation
- `CORRUPTION_TEST_README.md` - Test execution guide
- `ALIGNMENT_ASSERTIONS_ADDED.md` - Assertion documentation
- `CRASH_ANALYSIS.md` - Crash pattern analysis
- `BUG_REPORT_CORRUPTION.md` - This file

## Related Issues

- Original issue: Entry header decoding panic (`Cannot decode entry header: 1852795252`)
- Related issue: Entry header decoding panic (`Cannot decode entry header: 1634296687`)
- Current issue: Misaligned pointer dereference (all ending in `0xE6`)

All related to memory corruption in cell address storage/retrieval.

## Timeline

1. **Initial Report**: Entry header decoding panic
2. **Investigation**: Added transaction corruption tests
3. **Second Report**: Misaligned pointer dereference (`0x6c395a4000e6`)
4. **Alignment Investigation**: Comprehensive alignment analysis
5. **WordMap Verification**: Proved WordMap is not the source
6. **Assertions Added**: Comprehensive debug assertions at all cell_index points
7. **Third Report**: Consistent pattern confirmed (`0x75b917a000e6`)

## Conclusion

This is a **critical, systematic bug** that adds exactly 6 bytes to cell memory addresses. The corruption is:

- ✅ **Consistent**: Same pattern across all crashes
- ✅ **Systematic**: Always +6 bytes, not random
- ✅ **Reproducible**: Occurs under concurrent load
- ⚠️ **Not yet isolated**: Root cause location unknown

The investigation has:
- ✅ Verified alignment infrastructure is correct
- ✅ Verified WordMap is not the source
- ✅ Identified the consistent +6 offset pattern
- ✅ Added comprehensive debug instrumentation
- ⏳ **Awaiting**: Execution with assertions enabled to pinpoint exact corruption location

**Next Critical Step**: Run application with assertions enabled and review logs to identify exact point where +6 offset is introduced.






