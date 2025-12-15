# Alignment Assertions Added to cell_index Operations

## Summary

Added comprehensive `debug_assert!` checks at **all 12 critical points** where addresses enter or leave the `cell_index`.

## What Will These Catch?

These assertions will pinpoint **exactly where** the +6 offset corruption occurs:

1. **If a WRITE assertion fails** → Address is already corrupted BEFORE being stored
2. **If a READ assertion fails** → Address was corrupted AFTER storage (but we already proved WordMap is correct)

## Assertion Points Added

### WRITE POINTS (5 locations)

Where addresses are **stored into** `cell_index`:

```rust
// 1. write_cell (line ~520)
debug_assert!(cell_loc % 8 == 0, "WRITE POINT: write_cell...");

// 2. update_cell (line ~593)  
debug_assert!(new_cell_loc % 8 == 0, "WRITE POINT: update_cell...");

// 3. upsert_cell - update path (line ~658)
debug_assert!(new_cell_loc % 8 == 0, "WRITE POINT: upsert_cell(update)...");

// 4. upsert_cell - insert path (line ~685)
debug_assert!(new_cell_loc % 8 == 0, "WRITE POINT: upsert_cell(insert)...");

// 5. update_cell_by (line ~751)
debug_assert!(new_cell_loc % 8 == 0, "WRITE POINT: update_cell_by...");
```

### READ POINTS (7 locations)

Where addresses are **retrieved from** `cell_index`:

```rust
// 1. location_for_read (line ~370)
debug_assert!(addr % 8 == 0, "READ POINT: location_for_read...");

// 2. location_for_write (line ~415)
debug_assert!(addr % 8 == 0, "READ POINT: location_for_write...");

// 3. update_cell - old location (line ~579)
debug_assert!(cell_location % 8 == 0, "READ POINT: update_cell read...");

// 4. upsert_cell - old location (line ~644)
debug_assert!(cell_location % 8 == 0, "READ POINT: upsert_cell(update) read...");

// 5. update_cell_by - old location (line ~722)
debug_assert!(old_loc % 8 == 0, "READ POINT: update_cell_by read...");

// 6. remove_cell (line ~791)
debug_assert!(cell_location % 8 == 0, "READ POINT: remove_cell read...");

// 7. remove_cell_by (line ~833)
debug_assert!(cell_location % 8 == 0, "READ POINT: remove_cell_by read...");
```

## Error Message Format

Each assertion provides detailed information:

```
READ POINT: location_for_read retrieved MISALIGNED address 0x00006c395a4000e6 (offset: 6) for hash 12345

WRITE POINT: update_cell attempting to store MISALIGNED address 0x00006c395a4000e6 (offset: 6) for hash 12345
```

This tells you:
- **What operation**: READ or WRITE
- **Which function**: write_cell, update_cell, etc.
- **The bad address**: 0x00006c395a4000e6
- **The misalignment**: offset 6 from 8-byte boundary
- **Which cell**: hash 12345

## How to Use

### Run Your Application in Debug Mode

```bash
# Build with debug assertions enabled
cargo build

# Or use dev profile
cargo build --profile dev

# Run your WikiData import
./wikidata_cli import wikidata20160104.json --config --workers 64 100000 2
```

### What to Expect

When corruption occurs, you'll see **exactly** which assertion fails:

**Scenario 1: Corruption BEFORE storage**
```
thread 'tokio-runtime-worker' panicked at src/ram/chunk.rs:520:
WRITE POINT: write_cell attempting to store MISALIGNED address 0x6c395a4000e6 (offset: 6) for hash 12345
```
→ This means `cell_loc` from `write_cell_to_chunk()` is already wrong!

**Scenario 2: Corruption AFTER storage** (unlikely - WordMap is proven correct)
```
thread 'tokio-runtime-worker' panicked at src/ram/chunk.rs:370:
READ POINT: location_for_read retrieved MISALIGNED address 0x6c395a4000e6 (offset: 6) for hash 12345
```
→ This would mean WordMap corrupted it (but our tests proved it doesn't)

## Expected Result

Based on our analysis:
- ✅ Segment addresses are 8-byte aligned (verified)
- ✅ WordMap storage/retrieval is correct (verified)
- ❌ **WRITE assertion will likely fail** → Address is corrupted BEFORE storage

This will pinpoint that the bug is in:
1. How `pending_entry.addr` is accessed
2. How `write_cell_to_chunk()` returns the address
3. Some pointer arithmetic BEFORE the address reaches `*guard = cell_loc`

## Next Steps After Assertion Fires

Once you know which assertion fails:

1. **If WRITE fails**: Trace backwards from the failed assertion:
   - Check the value from `write_cell_to_chunk()`
   - Check `pending_entry.addr` 
   - Check `Segment::try_acquire()` return value
   - Look for pointer arithmetic

2. **Add more detailed logging**:
```rust
let (cell_loc, schema) = self.write_cell_to_chunk(cell)?;
debug!("TRACE: cell_loc from write_cell_to_chunk = 0x{:016x}", cell_loc);
```

## The +6 Offset Pattern

Remember: All crash addresses end in `0xE6`:
```
Correct:  0x6c395a4000e0  (offset 0, 8-byte aligned) ✓
Corrupt:  0x6c395a4000e6  (offset 6, NOT aligned) ✗
          ↑↑↑↑↑↑↑↑↑↑↑↑↑↑ Same base
                      ↑↑ Different by exactly +6
```

Something is consistently adding +6 to correct addresses. The assertions will show us WHERE.

