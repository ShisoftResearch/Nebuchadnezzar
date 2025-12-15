# WAL and Backup Performance Optimization

## Problem
The original implementation was capped at ~13MB/s write throughput due to calling `fsync()` on every single write operation. Typical SSDs can only handle 100-1000 fsyncs per second, creating a hard bottleneck.

## Solution
Implemented **Group Commit Batching** with two optimizations:

### 1. Larger Write Buffer (256KB)
- Increased from 4KB to 256KB
- Reduces system call overhead
- Better OS-level write coalescing

### 2. Batch Fsync Strategy
Non-transactional writes now sync when **either** threshold is reached:
- **512KB** of accumulated writes (batch size threshold)
- **10ms** since last sync (time threshold)

This reduces fsyncs by 50-100x while maintaining durability within a bounded loss window.

## Transaction Guarantees Preserved

### How Transactions Work
1. **During Transaction Writes**: 
   - `skip_sync=true` is set via `set_transaction_context(true)`
   - Writes go to WAL buffer but **no fsync** occurs
   - Data accumulates in kernel buffers

2. **At Commit Time** (`data_site.rs:730-749`):
   ```rust
   // Sync all segments that were written to during this transaction
   for (chunk_idx, seg_id) in &txn.protected_segments {
       let segment = chunk.segs.get(&(*seg_id as usize));
       segment.wal_file.lock().flush()?;
       segment.wal_file.lock().get_ref().sync_all()?;
   }
   ```
   - **Explicit sync** of all affected segments
   - Transaction only succeeds after **all data is on disk**
   - Full ACID durability guaranteed

3. **Key Safety Property**:
   - Group commit batching **only affects non-transactional writes**
   - Transactions bypass the batching and control their own sync
   - No change to transaction semantics or durability

## Performance Impact

### Before
- Fsync on **every write** 
- ~13 MB/s throughput (limited by fsync rate)
- ~1000 writes/second max

### After
- Fsync every **512KB or 10ms**
- Expected: **100-1000 MB/s** throughput (50-100x improvement)
- ~100,000+ writes/second possible

### Durability Tradeoff
- **Max potential data loss** in crash: 512KB or 10ms of non-transactional writes
- **Transactional writes**: NO data loss risk (synced at commit)
- Bounded loss window: predictable and configurable

## Configuration Tuning

Edit constants in `src/ram/segs.rs`:

```rust
// Current defaults (balanced)
pub const WAL_BUFFER_SIZE: usize = 256 * 1024;      // 256KB
pub const WAL_SYNC_BATCH_SIZE: usize = 512 * 1024;  // 512KB
pub const WAL_SYNC_INTERVAL_MS: i64 = 10;           // 10ms

// High throughput (relaxed durability)
pub const WAL_BUFFER_SIZE: usize = 1024 * 1024;     // 1MB
pub const WAL_SYNC_BATCH_SIZE: usize = 2 * 1024 * 1024; // 2MB
pub const WAL_SYNC_INTERVAL_MS: i64 = 50;           // 50ms

// Low latency (tighter durability)
pub const WAL_BUFFER_SIZE: usize = 128 * 1024;      // 128KB
pub const WAL_SYNC_BATCH_SIZE: usize = 256 * 1024;  // 256KB
pub const WAL_SYNC_INTERVAL_MS: i64 = 5;            // 5ms

// Strict durability (like before, but with larger buffer)
pub const WAL_BUFFER_SIZE: usize = 256 * 1024;      // 256KB
pub const WAL_SYNC_BATCH_SIZE: usize = 0;           // Sync every write
pub const WAL_SYNC_INTERVAL_MS: i64 = 0;            // No delay
```

## Testing Recommendations

1. **Functional Tests**: Verify transactions still maintain ACID properties
2. **Performance Tests**: Measure throughput improvement
3. **Crash Recovery**: Verify WAL recovery works correctly
4. **Mixed Workload**: Test transactional and non-transactional writes together

## Implementation Details

### Files Modified
- `src/ram/segs.rs`: Core WAL implementation
  - Added `WAL_BUFFER_SIZE`, `WAL_SYNC_BATCH_SIZE`, `WAL_SYNC_INTERVAL_MS` constants
  - Added `last_sync_time` and `bytes_since_sync` to `Segment` struct
  - Rewrote `write_wal()` with group commit logic
  - Added `force_wal_sync()` helper for explicit syncs

### Key Functions
- `write_wal()`: Implements group commit batching for non-transactional writes
- `force_wal_sync()`: Forces immediate sync (useful for shutdown/checkpoints)
- Transaction commit code (unchanged): Still does explicit sync

## Monitoring

Add these metrics to track behavior:
- Fsync rate (should be ~100/second instead of 1000s/second)
- Bytes between fsyncs (should average ~512KB)
- Time between fsyncs (should average ~10ms)
- Transaction commit latency (should be unchanged)

