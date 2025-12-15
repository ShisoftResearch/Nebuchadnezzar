# Debug Validation for Cell Location Corruption

## Overview

Added debug-only validation to detect when invalid/corrupted addresses are stored in the cell index. This validation only runs when compiled in debug mode (`cargo build` or `cargo test` without `--release`), ensuring zero performance impact in production.

## What Was Added

### 1. Validation Function (`src/ram/chunk.rs`)

A new method `validate_cell_location()` that checks:

- **NULL addresses** (0x0) - Obviously invalid
- **Alignment** - Addresses must be 8-byte aligned for struct access
- **Invalid high bits** - On x86-64, valid pointers use only lower 48 bits
- **Segment bounds** - Addresses must be within valid segment ranges

### 2. Validation Points

Debug assertions added at every point where cell locations are stored in the cell index:

#### `write_cell()` - Line ~417
```rust
#[cfg(debug_assertions)]
{
    debug_assert!(
        self.validate_cell_location(cell_loc, &format!("write_cell(hash={})", cell.header.hash)),
        "Attempting to store invalid cell location 0x{:x} in cell index for hash {}",
        cell_loc,
        cell.header.hash
    );
}
```

#### `update_cell()` - Lines ~458, ~471
- Validates **new** cell location before storing
- Validates **old** cell location before marking it dead

#### `upsert_cell()` - Lines ~507, ~522
- Validates **new** cell location before storing  
- Validates **old** cell location during update path

#### `update_cell_by()` - Lines ~569, ~599
- Validates **old** location before attempting to read it (prevents misaligned read panic!)
- Validates **new** location before storing
- Returns error early if old location is corrupted

## How It Works

### In Debug Mode

When you build with `cargo build` or `cargo test`:

1. **Validation executes** before every cell location store
2. **Assertions fire** if invalid addresses detected
3. **Detailed logs** show:
   - Which function detected the issue
   - The invalid address (in hex)
   - The cell hash
   - Why the address is invalid (alignment, bounds, etc.)

### In Release Mode

When you build with `cargo build --release`:

1. **All validation code removed** by compiler
2. **Zero runtime overhead**
3. **No log messages** from validation

## Usage

### Running Tests with Validation

```bash
# Build in debug mode (validation enabled)
cargo build

# Run corruption tests with validation
cargo test corruption_tests -- --nocapture

# Run specific test with logging
RUST_LOG=debug cargo test test_rapid_concurrent_updates_same_cell -- --nocapture

# Run wikidata import with validation
cargo run --bin wikidata_cli import <file> --config <config> --workers 64
```

### Expected Output When Corruption Detected

```
[ERROR neb::ram::chunk] [Chunk 0] Invalid cell location at update_cell(hash=12345): 
  address 0x6c51974000e6 is not 8-byte aligned

thread 'tokio-runtime-worker' panicked at src/ram/chunk.rs:461:13:
assertion `left == right` failed
  left: false
  right: true
```

Or if found during read:

```
[ERROR neb::ram::chunk] Found corrupted old cell location 0x6c51974000e6 for hash 12345 
  - this indicates prior corruption

[ERROR neb::ram::chunk] Corrupted cell location 0x6c51974000e6 detected for hash 12345 
  in update_cell_by - aborting to prevent further corruption
```

## Benefits

1. **Early Detection** - Catches corruption at source, not downstream
2. **Detailed Context** - Logs show exactly where/when/why
3. **Prevents Cascading Errors** - Stops before corruption spreads
4. **Zero Cost in Production** - Validation completely removed in release builds
5. **Pinpoints Root Cause** - Identifies which operation introduced bad address

## What to Do If Validation Fires

### Step 1: Identify the Pattern

Check the logs to see:
- Which function detected it? (`write_cell`, `update_cell`, `update_cell_by`, etc.)
- Is it the **new** or **old** location that's invalid?
- What's the specific failure? (alignment, bounds, null, etc.)

### Step 2: Check the Address

Invalid address examples and what they might mean:

| Address Pattern | Likely Cause |
|----------------|--------------|
| `0x0` (NULL) | Uninitialized or zeroed memory |
| `0x...E6` (misaligned) | Pointer arithmetic error, off-by-N bytes |
| `0x6c51974000e6` | Reading data as pointer (string bytes, etc.) |
| `0xFFFF_FFFF_...` | Corrupted high bits, possible memory corruption |
| Outside segment | Using deallocated memory or wrong segment |

### Step 3: Add More Logging

If you need more context, add logging around the failing operation:

```rust
debug!("About to update cell: hash={}, old_loc=0x{:x}, new_loc=0x{:x}", 
       hash, old_loc, new_cell_loc);
```

### Step 4: Check for Race Conditions

If validation fires **intermittently**:
- Likely a race condition in cell_index updates
- Check WordMap locking/synchronization
- Look for concurrent access to same cell hash

If validation fires **consistently**:
- Likely a bug in address calculation or storage
- Check write_cell_to_chunk implementation
- Verify segment allocation logic

## Root Cause Theories

Based on the panics observed, likely causes:

### Theory 1: Race Condition in Cell Index Updates

**Problem**: Between reading old_loc and storing new_loc, another thread modifies the cell index.

**Evidence**:
- Happens under high concurrency (64 workers)
- Invalid addresses look like data, not pointers
- Intermittent failures

**Solution**: Review locking in `update_cell`, `update_cell_by`, ensure atomicity

### Theory 2: Premature Segment Reuse

**Problem**: Segment memory is deallocated/reused while old_loc still references it.

**Evidence**:
- Addresses outside valid segment bounds
- Happens after many operations (memory pressure)

**Solution**: Reference counting for segments, delayed cleanup

### Theory 3: Pointer Arithmetic Error

**Problem**: Bug in calculating cell address from segment + offset.

**Evidence**:
- Consistent misalignment by fixed offset
- Always fails in same scenario

**Solution**: Review address calculation in write_to_chunk_with_schema

### Theory 4: Memory Corruption

**Problem**: Buffer overflow or use-after-free corrupts cell_index data structures.

**Evidence**:
- Random invalid addresses
- Crashes in unrelated code
- Address has data-like patterns (ASCII, etc.)

**Solution**: Run with AddressSanitizer, review unsafe code

## Testing Strategy

Run the corruption tests repeatedly to gather data:

```bash
# Run 100 iterations to catch race conditions
for i in {1..100}; do
  echo "=== Iteration $i ==="
  cargo test test_rapid_concurrent_updates_same_cell -- --nocapture || break
done

# Run with different concurrency levels
for workers in 8 16 32 64 128; do
  echo "=== Testing with $workers workers ==="
  cargo test test_maximum_concurrency_stress -- --nocapture --test-threads=$workers
done
```

## Next Steps

1. **Run tests** with validation enabled to collect failure data
2. **Analyze patterns** in the logged invalid addresses
3. **Add more instrumentation** if needed based on patterns
4. **Fix the root cause** based on evidence
5. **Verify fix** by running tests 1000+ times without failures

## Notes

- Validation is intentionally **conservative** - it may warn about addresses that are technically valid but suspicious
- The `warn!` for "segment not found" during validation is normal for newly allocated addresses
- All validation uses `#[cfg(debug_assertions)]` - confirms zero impact in release builds
- The `debug_assert!` will panic and stop execution when it catches corruption - this is intentional to prevent cascading damage

