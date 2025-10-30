# Troubleshooting Write Bottleneck

## Current Status
✅ **Group commit batching is working** - strace shows only 8 fsync calls in tests (down from 1000s)
✅ **Code compiles and tests pass**

## If You're Still Seeing ~13MB/s Bottleneck

### 1. Check Your Actual Workload Characteristics

**Question**: How are you measuring the 13MB/s?
- During benchmarks?
- In production with real data?
- What's your typical write size?
- How many concurrent writers?

### 2. Tune the Thresholds for Your Workload

The current settings may be too conservative for non-transactional writes:

```rust
// src/ram/segs.rs lines 48-50
pub const WAL_BUFFER_SIZE: usize = 256 * 1024;      // 256KB
pub const WAL_SYNC_BATCH_SIZE: usize = 512 * 1024;  // 512KB - sync after this much data
pub const WAL_SYNC_INTERVAL_MS: i64 = 10;           // 10ms - sync after this much time
```

**If you're writing small entries** (<1KB each):
- The 10ms timer might fire before accumulating 512KB
- This would cause more frequent fsyncs

**Try more aggressive batching**:
```rust
pub const WAL_SYNC_BATCH_SIZE: usize = 2 * 1024 * 1024;  // 2MB
pub const WAL_SYNC_INTERVAL_MS: i64 = 50;                // 50ms
```

**Or disable time-based sync entirely** (sync only on batch size):
```rust
pub const WAL_SYNC_INTERVAL_MS: i64 = i64::MAX;  // Never sync on timer
```

### 3. Other Potential Bottlenecks

#### A. Archive Function Still Syncs
The `archive()` function (lines 201-289 in segs.rs) still does sync on every call.
- Are you archiving segments frequently?
- This could be the bottleneck if archive is called often

#### B. Multiple Segments
-  Each segment has its own WAL file
- If you're writing to many segments simultaneously, each one tracks its own batch
- This could fragment the batching effectiveness

#### C. Disk Write Speed
- Is your disk actually capable of >13MB/s?
- Check with: `dd if=/dev/zero of=/tmp/test.img bs=1M count=1000 oflag=direct`

#### D. Lock Contention
- The WAL file mutex (line 292) could be a bottleneck with many concurrent writers
- Check CPU usage - high sys% could indicate lock contention

### 4. Monitoring & Diagnosis

Add this to your code to see actual fsync frequency:

```rust
// In src/ram/segs.rs, line 334 (after sync_data)
info!("WAL synced for segment {} - {} bytes, {} ms since last sync, {} fsyncs/sec", 
      self.id, bytes_written, time_since_sync, 
      if time_since_sync > 0 { 1000 / time_since_sync } else { 0 });
```

This will show you:
- How often fsyncs actually happen
- Whether it's time-based (10ms) or size-based (512KB)
- Actual fsync rate

### 5. Quick Test: Disable ALL Syncing Temporarily

To verify fsync is the bottleneck, try this temporarily:

```rust
// In src/ram/segs.rs write_wal(), comment out the sync:
if should_sync {
    // file.get_ref().sync_data()?;  // TEMPORARILY DISABLED FOR TESTING
    
    self.bytes_since_sync.store(0, Ordering::Relaxed);
    self.last_sync_time.store(current_time, Ordering::Relaxed);
}
```

**If throughput jumps to 100s of MB/s**, then fsync is definitely the bottleneck and you need more aggressive batching.

**If throughput stays at 13MB/s**, then the bottleneck is something else (disk speed, lock contention, CPU, etc.).

### 6. Benchmark Commands

Test WAL write throughput:
```bash
# Monitor fsync rate during test
watch -n 1 'iostat -x 1 1 | grep -A 5 "^Device"'

# Or use strace on your actual application
strace -e trace=fsync,fdatasync -c your_app

# Check disk write speed
fio --name=seqwrite --rw=write --bs=256k --size=1G --filename=/tmp/test --direct=1
```

## Next Steps

1. **Share your workload characteristics**: write size, frequency, concurrent writers
2. **Try more aggressive thresholds**: increase batch size to 2MB and interval to 50ms
3. **Monitor actual fsync rate** in your application
4. **Test with syncing disabled** to confirm it's the bottleneck

The optimization is working (8 fsyncs vs 1000s), but the thresholds might not match your workload pattern.

