# Transaction Corruption Detection Tests

## Overview

This document describes a suite of transaction tests specifically designed to detect and reproduce the data corruption panic:

```
thread 'tokio-runtime-worker' panicked at src/ram/entry.rs:82:13:
Cannot decode entry header: <invalid_value>
```

This panic occurs in `mark_dead_entry_with_cell` during transaction commits when the system tries to mark old cell entries as dead but encounters corrupted entry headers.

## The Bug

The issue manifests when:
1. A transaction updates a cell via `update_cell_by`
2. The new cell is written to a new location
3. The cell index is updated to point to the new location
4. The system tries to mark the old entry as dead
5. **BUG**: The old entry address is invalid or corrupted, causing `Entry::decode_from` to read garbage data

### Stack Trace Pattern
```
neb::ram::chunk::Chunk::mark_dead_entry_with_cell
  ↓
neb::ram::chunk::Chunks::update_cell_by
  ↓
<neb::server::transactions::data_site::DataManager>::commit
  ↓
<neb::server::transactions::manager::TransactionManager>::prepare
```

### Invalid Entry Type Values Observed
- `1852795252` (0x6E727374, ASCII: "nrst")
- `1634296687` (0x61726F6F, ASCII: "aroo")

These values suggest memory corruption or reading from wrong addresses.

## Test Suite

Located in: `src/server/transactions/corruption_tests.rs`

### Test 1: `test_rapid_concurrent_updates_same_cell`
**Purpose**: Detect race conditions in cell location tracking

- **Scenario**: 100 concurrent transactions all updating the same cell
- **Stress Factor**: High contention on a single cell's location in the cell index
- **Expected Failure Mode**: If there's a race condition in updating the cell index, transactions may read stale or invalid old_loc values

**Run**:
```bash
cargo test --package nebuchadnezzar --test '' test_rapid_concurrent_updates_same_cell -- --nocapture
```

### Test 2: `test_varying_size_concurrent_updates`
**Purpose**: Trigger segment allocation issues and corruption

- **Scenario**: Updates with dramatically varying data sizes (100 bytes to 100KB)
- **Stress Factor**: Forces frequent segment allocations and potential reuse
- **Expected Failure Mode**: If old segment memory is reused before dead entries are properly marked, addresses may become invalid

**Run**:
```bash
cargo test --package nebuchadnezzar --test '' test_varying_size_concurrent_updates -- --nocapture
```

### Test 3: `test_multi_cell_concurrent_transactions`
**Purpose**: Test cell index integrity under concurrent modifications

- **Scenario**: 100 transactions each updating multiple (5) cells from a pool of 20
- **Stress Factor**: Complex interleaving of cell index updates across multiple cells
- **Expected Failure Mode**: Cell index corruption when multiple transactions update overlapping sets of cells

**Run**:
```bash
cargo test --package nebuchadnezzar --test '' test_multi_cell_concurrent_transactions -- --nocapture
```

### Test 4: `test_rapid_commit_sequence`
**Purpose**: Expose timing issues in mark_dead_entry_with_cell

- **Scenario**: 50 sequential transactions with minimal delays (100μs between commits)
- **Stress Factor**: Rapid successive commits to the same cell
- **Expected Failure Mode**: If dead entry marking is asynchronous or delayed, locations may be invalidated before marking

**Run**:
```bash
cargo test --package nebuchadnezzar --test '' test_rapid_commit_sequence -- --nocapture
```

### Test 5: `test_interleaved_prepare_commit`
**Purpose**: Expose timing issues in transaction state management

- **Scenario**: 50 transactions with variable delays between prepare and commit
- **Stress Factor**: Out-of-order commit completions
- **Expected Failure Mode**: State inconsistencies if prepare/commit phases overlap incorrectly

**Run**:
```bash
cargo test --package nebuchadnezzar --test '' test_interleaved_prepare_commit -- --nocapture
```

### Test 6: `test_maximum_concurrency_stress`
**Purpose**: Maximum stress test with high concurrency

- **Scenario**: 500 concurrent transactions on a single cell with 16 worker threads
- **Stress Factor**: Extreme contention and concurrent access patterns
- **Expected Failure Mode**: Any race condition should manifest under this load

**Run**:
```bash
cargo test --package nebuchadnezzar --test '' test_maximum_concurrency_stress -- --nocapture
```

## Running All Corruption Tests

To run all corruption detection tests:

```bash
# Run with detailed output
cargo test --package nebuchadnezzar corruption_tests -- --nocapture --test-threads=1

# Run with full logging
RUST_LOG=debug cargo test --package nebuchadnezzar corruption_tests -- --nocapture --test-threads=1

# Run with backtraces on panic
RUST_BACKTRACE=full cargo test --package nebuchadnezzar corruption_tests -- --nocapture
```

## Debugging Tips

### If a Test Panics

1. **Check the invalid entry type value**:
   ```
   Cannot decode entry header: 1852795252
   ```
   Convert to hex and ASCII to see if it's data being misinterpreted as an entry type.

2. **Enable debug logging**:
   ```bash
   RUST_LOG=neb::ram::chunk=debug,neb::server::transactions=debug cargo test <test_name> -- --nocapture
   ```

3. **Check memory addresses**:
   Add logging in `mark_dead_entry_with_cell` to print:
   - The address being decoded
   - The cell ID
   - The segment ID

4. **Run with memory sanitizer** (if available):
   ```bash
   RUSTFLAGS="-Z sanitizer=address" cargo +nightly test <test_name>
   ```

### Common Root Causes to Investigate

1. **Race condition in cell_index updates**:
   - Old location might be read by another thread before being updated
   - Solution: Ensure atomic operations or proper locking

2. **Premature segment memory reuse**:
   - Segment memory deallocated or reused before all references cleared
   - Solution: Reference counting or delayed cleanup

3. **Stale cell location pointers**:
   - Cache or local copy of cell location not invalidated
   - Solution: Always read from authoritative source

4. **Concurrent cleaner interference**:
   - Cleaner might be modifying segments during transaction commit
   - Solution: Proper coordination between cleaner and transactions

5. **Missing memory barriers**:
   - CPU reordering causing visibility issues
   - Solution: Use proper atomic ordering (Acquire/Release)

## Expected Test Behavior

### Healthy System
- Tests should complete without panics
- Some transactions may fail due to conflicts (expected)
- Output should show successful commit messages

### Corrupted System  
- Tests will panic with "Cannot decode entry header"
- Look for patterns in which test fails most frequently
- Multiple consecutive runs may show different failure points

## Continuous Testing

These tests should be run:
1. **Before any commit** that modifies:
   - `src/ram/chunk.rs` (especially cell update logic)
   - `src/ram/entry.rs`
   - `src/server/transactions/data_site.rs`
   - `src/ram/segs.rs` (segment management)

2. **In CI/CD pipeline** with high iteration count:
   ```bash
   for i in {1..100}; do
     echo "Iteration $i"
     cargo test corruption_tests -- --nocapture || break
   done
   ```

3. **Under different memory pressure**:
   ```bash
   # Smaller chunk size increases segment reuse
   # Modify test to use: total_size: 16 * 1024 * 1024
   ```

## Related Files

- `src/ram/entry.rs` - Entry encoding/decoding (where panic occurs)
- `src/ram/chunk.rs` - Cell updates and dead entry marking
- `src/server/transactions/data_site.rs` - Transaction commit logic
- `src/ram/cell.rs` - Cell structure and operations
- `src/ram/segs.rs` - Segment management

## Reporting Issues

When reporting this bug, include:
1. Full stack trace with `RUST_BACKTRACE=full`
2. The invalid entry type value (in decimal, hex, and ASCII)
3. Which test failed (or workload characteristics)
4. Server configuration (chunk_count, total_size, etc.)
5. Debug logs if available

## Future Work

### Potential Fixes

1. **Add Result-based decoding**:
   ```rust
   pub fn try_decode_from<R, RR>(pos: usize, content_read: R) 
       -> Result<(EntryHeader, RR), ReadError>
   ```
   This would prevent panics and allow graceful error handling.

2. **Add corruption detection**:
   - Checksum validation on entry headers
   - Magic number at start of entries
   - Validate addresses before dereferencing

3. **Add defensive checks**:
   ```rust
   pub fn mark_dead_entry_with_seg(&self, addr: usize, seg: &Segment) {
       // Validate addr is within segment bounds
       if addr < seg.addr || addr >= seg.addr + SEGMENT_SIZE {
           error!("Invalid address for marking dead entry");
           return;
       }
       // ... proceed with decode
   }
   ```

4. **Improve synchronization**:
   - Use stronger memory ordering for cell location updates
   - Consider versioning for cell locations
   - Add read-write locks where appropriate

### Additional Tests Needed

1. Test with garbage collection running concurrently
2. Test with segment eviction/promotion (tiered memory)
3. Test recovery after crashes during commits
4. Test with WAL enabled
5. Fuzz testing of concurrent operations

## Questions?

If you encounter issues with these tests or need clarification on the corruption bug, please:
1. Check existing GitHub issues for similar reports
2. Review the stack trace patterns above
3. Enable debug logging and collect detailed information
4. Consider adding more instrumentation to the failing code path

