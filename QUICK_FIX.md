# Quick Fix: Increase Batching Aggressiveness

## The Problem

Your test shows only 8 fsyncs (good!), but if you're still seeing 13MB/s, it's likely because:

1. **10ms interval is too short** - timer fires before accumulating enough data
2. With small writes, you're syncing every 10ms = **100 fsyncs/second**
3. At 100 fsyncs/sec with ~100-130KB per sync = ~13MB/s ← **This matches your observed bottleneck!**

## Solution: More Aggressive Batching

Edit `src/ram/segs.rs` lines 48-50:

### Option 1: Increase Both Thresholds (Recommended)
```rust
pub const WAL_BUFFER_SIZE: usize = 1024 * 1024;      // 1MB buffer
pub const WAL_SYNC_BATCH_SIZE: usize = 4 * 1024 * 1024;  // 4MB batch
pub const WAL_SYNC_INTERVAL_MS: i64 = 100;           // 100ms interval
```

**Expected**: ~40-400 MB/s (10 fsyncs/sec instead of 100)

### Option 2: Disable Time-Based Sync (Maximum Throughput)
```rust
pub const WAL_BUFFER_SIZE: usize = 1024 * 1024;      // 1MB buffer
pub const WAL_SYNC_BATCH_SIZE: usize = 4 * 1024 * 1024;  // 4MB batch
pub const WAL_SYNC_INTERVAL_MS: i64 = i64::MAX;      // Never sync on timer
```

**Expected**: 100s-1000s MB/s (sync only when buffer fills)

**Durability tradeoff**: Up to 4MB of data loss in crash (instead of 10ms window)

### Option 3: Conservative Improvement
```rust
pub const WAL_BUFFER_SIZE: usize = 512 * 1024;       // 512KB buffer
pub const WAL_SYNC_BATCH_SIZE: usize = 1 * 1024 * 1024;  // 1MB batch
pub const WAL_SYNC_INTERVAL_MS: i64 = 50;            // 50ms interval
```

**Expected**: ~20-200 MB/s (20 fsyncs/sec instead of 100)

## Quick Test

```bash
# 1. Edit src/ram/segs.rs with one of the options above
# 2. Rebuild
cargo build --release

# 3. Test throughput
# Run your benchmark and measure throughput
```

## Why This Works

Current settings (10ms interval) means:
- 1000ms / 10ms = **100 fsyncs per second maximum**
- If you're writing 130KB per 10ms period
- 130KB × 100 = **13 MB/s** ← Your current bottleneck!

With 100ms interval:
- 1000ms / 100ms = **10 fsyncs per second**
- If you accumulate 4MB per 100ms
- 4MB × 10 = **40 MB/s** minimum

## Recommendation

Start with **Option 1** (100ms interval, 4MB batch). This gives you:
- 10x throughput improvement (minimum)
- Still reasonable durability (100ms loss window)
- Good balance for most workloads

If you need maximum throughput and can accept larger loss window, use **Option 2**.

