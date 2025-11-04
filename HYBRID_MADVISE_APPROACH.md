# Hybrid madvise Approach for Cold Segment Memory Management

## Overview

Implemented a two-phase memory management strategy for cold segments:

1. **Pre-eviction (Hot Anonymous Memory)**: `madvise(MADV_DONTNEED)` - Aggressive
2. **Post-eviction (Cold File-backed Memory)**: `madvise(MADV_COLD)` - Cooperative

## Why This Hybrid Approach?

### Phase 1: Before File-Backed Mmap (Hot Memory)
**Problem**: Hot segments use anonymous memory that consumes physical RAM.

**Solution**: Use `MADV_DONTNEED` to **aggressively free** physical pages:
- Immediately releases all physical memory pages
- Ensures hot→cold transition actually frees memory
- Zero physical memory consumption at moment of conversion

### Phase 2: After File-Backed Mmap (Cold Memory)
**Problem**: File-backed mappings can still consume physical memory via page cache.

**Solution**: Use `MADV_COLD` to **cooperatively hint** that pages are low priority:
- Marks pages as candidates for eviction under memory pressure
- Doesn't force immediate eviction (kernel decides when)
- Allows kernel to keep frequently-accessed cold pages cached
- Better performance for "warm" cold segments that get occasional access

## Benefits of Hybrid Approach

### 1. Guaranteed Memory Release
```
Hot Segment (Anonymous Memory)
    ↓ madvise(MADV_DONTNEED)
    ↓ ✅ Physical pages immediately freed
File-backed mmap with MAP_FIXED
    ↓ madvise(MADV_COLD)
Cold Segment (File-backed + Low Priority)
```

### 2. Balanced Performance vs Memory
- **MADV_DONTNEED** on hot memory: Ensures transition actually frees RAM
- **MADV_COLD** on cold memory: Allows kernel to cache hot spots while respecting priority

### 3. Smart Page Cache Management
Without this approach:
- File-backed pages might stay resident indefinitely
- Memory "freed" by eviction still consumed by page cache
- Host could OOM even after eviction

With this approach:
- Hot memory aggressively freed during transition
- Cold memory marked as evictable under pressure
- Kernel intelligently manages page cache based on access patterns

## Implementation Details

### New Function: `madvise_cold()` (src/ram/segs.rs)

```rust
/// Mark pages as cold (low priority for eviction)
/// 
/// Uses MADV_COLD (Linux 5.4+) to hint to the kernel that these pages
/// should be evicted first under memory pressure.
pub unsafe fn madvise_cold(addr: usize, size: usize) {
    #[cfg(target_os = "linux")]
    {
        const MADV_COLD: i32 = 20; // Linux 5.4+
        let result = madvise(addr as *mut c_void, size, MADV_COLD);
        
        if result != 0 {
            let errno = std::io::Error::last_os_error();
            if errno.raw_os_error() == Some(libc::EINVAL) {
                // Kernel < 5.4, fall back to MADV_DONTNEED
                warn!("MADV_COLD not supported, falling back to MADV_DONTNEED");
                madvise(addr as *mut c_void, size, MADV_DONTNEED);
            }
        }
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        // Non-Linux: fall back to MADV_DONTNEED
        madvise(addr as *mut c_void, size, MADV_DONTNEED);
    }
}
```

**Fallback Strategy**:
- Modern Linux (≥5.4): Uses `MADV_COLD` for cooperative eviction
- Older Linux (<5.4): Falls back to `MADV_DONTNEED` (aggressive)
- Non-Linux: Falls back to `MADV_DONTNEED` (aggressive)

### Updated Eviction Flow (src/ram/tiered/eviction.rs)

```rust
pub fn evict_segment(segment: &Segment, _chunk: &Chunk) -> Result<(), io::Error> {
    // Wait for no references
    while !segment.no_references() {
        thread::yield_now();
    }
    
    // Archive to file
    segment.archive()?;
    
    // Step 3: Aggressively free hot anonymous memory BEFORE remapping
    unsafe {
        madvise_free(segment.addr, SEGMENT_SIZE);
    }
    debug!("Freed physical pages for hot segment {} before file-backed remapping", segment.id);
    
    // Step 4: Open backup file
    let fd = unsafe { open(backup_path, O_RDONLY) };
    
    // Step 5: Remap with MAP_FIXED (anonymous → file-backed)
    let result = unsafe {
        mmap(
            segment.addr as *mut c_void,
            SEGMENT_SIZE,
            PROT_READ,
            MAP_PRIVATE | MAP_FIXED,
            fd,
            0,
        )
    };
    
    // Step 6: Mark file-backed pages as COLD (low priority)
    unsafe {
        madvise_cold(segment.addr, SEGMENT_SIZE);
    }
    debug!("Marked file-backed segment {} as cold (low priority for page cache)", segment.id);
    
    // Step 7: Store fd (marks as cold)
    segment.cold_file_fd.store(fd, Ordering::Release);
    
    // Step 8: Protect with mprotect(PROT_NONE) for access tracking
    crate::ram::tiered::page_fault_tracker::protect_segment(segment.addr)?;
    
    Ok(())
}
```

## Memory Behavior

### Scenario 1: Cold Segment Never Accessed
```
1. Eviction triggered
2. madvise_free → 0 MB physical memory (hot anonymous freed)
3. File-backed mmap → 0 MB physical memory (not yet accessed)
4. madvise_cold → Still 0 MB (pages not resident)
5. Memory pressure → 0 MB (nothing to evict, already zero)
```
**Result**: Zero physical memory consumption ✅

### Scenario 2: Cold Segment Occasionally Accessed
```
1. Eviction triggered
2. madvise_free → 0 MB physical memory
3. File-backed mmap → 0 MB physical memory
4. madvise_cold → 0 MB (marked as low priority)
5. First access → 4KB page faulted in (marked cold)
6. Memory pressure → Kernel evicts cold pages first
7. Between accesses → Pages may be evicted by kernel
```
**Result**: Minimal physical memory, kernel manages page cache ✅

### Scenario 3: Cold Segment Frequently Accessed
```
1. Eviction triggered
2. madvise_free → 0 MB physical memory
3. File-backed mmap → 0 MB physical memory
4. madvise_cold → 0 MB (marked as low priority)
5. Frequent accesses → Pages fault in (marked cold)
6. Memory pressure → Kernel evicts cold pages first
7. Under pressure → Gets evicted despite frequent access
```
**Result**: Pages evicted preferentially, but cache if no pressure ✅

### Scenario 4: Promotion (Cold → Hot)
When a cold segment is promoted back to hot:
- Pages are copied from file-backed mapping to anonymous memory
- `madvise_cold` marking is replaced by hot status
- Future evictions would use same two-phase approach

## Performance Characteristics

### CPU Overhead
- `madvise_free` (MADV_DONTNEED): ~1-5 microseconds per segment
- `madvise_cold` (MADV_COLD): ~1-5 microseconds per segment
- **Total overhead per eviction**: ~2-10 microseconds

### Memory Overhead
- **Immediate savings**: Full segment size (8MB per segment)
- **Sustained savings**: Depends on kernel page cache pressure
- **Best case**: 100% of cold segments freed
- **Worst case** (no memory pressure): Pages remain cached but marked evictable

### I/O Characteristics
- **Cold segment first access**: One page fault per 4KB page
- **No difference from previous approach**: File-backed pages still fault from disk
- **Better cache hits**: Kernel can keep hot cold pages cached

## Comparison with Alternatives

### Alternative 1: Only MADV_DONTNEED
```rust
// Before file-backed mmap: madvise_free
// After file-backed mmap: madvise_free
```
**Pros**: Aggressive, guaranteed zero memory
**Cons**: Evicts ALL pages immediately, even frequently accessed ones
**Performance**: Worse - frequent page faults even for warm data

### Alternative 2: Only MADV_COLD
```rust
// Before file-backed mmap: (nothing)
// After file-backed mmap: madvise_cold
```
**Pros**: Better cache hit rate
**Cons**: Hot anonymous pages not freed, memory leak during transition
**Performance**: Memory keeps growing during evictions

### Alternative 3: No madvise (Original Bug)
```rust
// Before file-backed mmap: (nothing)
// After file-backed mmap: (nothing)
```
**Pros**: Highest cache hit rate
**Cons**: Cold segments consume physical memory indefinitely
**Performance**: OOM when host memory exhausted

### Our Hybrid Approach (Best)
```rust
// Before file-backed mmap: madvise_free (aggressive)
// After file-backed mmap: madvise_cold (cooperative)
```
**Pros**: 
- Guaranteed memory release during transition
- Intelligent page cache management
- Balance between memory savings and performance

**Cons**: 
- Slightly more complex code
- Requires Linux 5.4+ for MADV_COLD (graceful fallback)

**Performance**: Optimal balance

## Kernel Version Support

| Kernel Version | Behavior |
|----------------|----------|
| Linux ≥ 5.4 | Full support: MADV_COLD for cooperative eviction |
| Linux < 5.4 | Fallback: MADV_DONTNEED (more aggressive) |
| macOS/BSD | Fallback: MADV_DONTNEED (more aggressive) |

## Testing Recommendations

### 1. Memory Consumption Test
```bash
# Before eviction
cat /proc/$PID/status | grep VmRSS

# Trigger eviction
# (via tiered manager)

# After eviction - should be close to zero
cat /proc/$PID/status | grep VmRSS

# Access cold segment
# (trigger page fault)

# Check memory - should be minimal (only accessed pages)
cat /proc/$PID/status | grep VmRSS
```

### 2. Page Cache Monitoring
```bash
# Monitor page cache for cold segments
mincore /proc/$PID/maps | grep "segment_backup_file"

# Should show:
# - 0 resident pages immediately after eviction
# - Few resident pages after occasional access
# - Pages evicted under memory pressure
```

### 3. Performance Test
```bash
# Benchmark cold segment access latency
# Should show:
# - First access: ~100μs (disk I/O)
# - Second access (if cached): ~1μs (cache hit)
# - After memory pressure: ~100μs (refaulted from disk)
```

## Monitoring Metrics

Recommended metrics to track:

```rust
pub struct ColdSegmentMetrics {
    /// Number of segments evicted
    pub segments_evicted: AtomicUsize,
    
    /// Total physical memory freed by madvise_free (pre-eviction)
    pub hot_memory_freed_bytes: AtomicUsize,
    
    /// Number of MADV_COLD calls succeeded
    pub cold_hints_applied: AtomicUsize,
    
    /// Number of MADV_COLD fallbacks to MADV_DONTNEED
    pub cold_hint_fallbacks: AtomicUsize,
    
    /// Page faults on cold segments (from mprotect handler)
    pub cold_page_faults: AtomicUsize,
}
```

## Related Files

- **Modified**: `src/ram/segs.rs` - Added `madvise_cold()` function
- **Modified**: `src/ram/tiered/eviction.rs` - Two-phase madvise approach
- **Related**: `src/ram/tiered/manager.rs` - Eviction orchestration
- **Related**: `src/ram/tiered/page_fault_tracker.rs` - Access tracking

## Future Enhancements

1. **Per-page tracking**: Track which pages in cold segments are frequently accessed
2. **Partial eviction**: Keep hot pages, evict only cold pages within a segment
3. **MADV_PAGEOUT**: Use on Linux 5.4+ to swap pages without invalidating them
4. **mincore monitoring**: Periodically check actual physical memory usage
5. **Adaptive thresholds**: Adjust COLD vs DONTNEED based on access patterns

