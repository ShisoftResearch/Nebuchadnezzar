# Tiered Memory System

## Overview

Nebuchadnezzar now supports a **tiered memory system** that enables the database to handle datasets larger than physical memory. The system automatically manages data between "hot" (in-memory) and "cold" (file-backed mmap) storage tiers based on access patterns and memory pressure.

## Architecture

### Hot vs Cold Segments

- **Hot Segments**: Stored in anonymous memory (RAM), providing fast access
- **Cold Segments**: Backed by memory-mapped files, freeing physical memory while maintaining virtual address stability

### Key Features

1. **Automatic Eviction**: When hot memory usage exceeds configured limits, segments are automatically evicted to cold storage
2. **Transparent Promotion**: Cold segments are automatically promoted to hot on access
3. **CLOCK Eviction Policy**: Uses the CLOCK algorithm for intelligent victim selection
4. **Address Stability**: Uses `MAP_FIXED` to ensure segment addresses remain stable during hot/cold transitions
5. **Cleaner Integration**: Cleaners automatically skip cold segments; promoted segments become eligible for cleaning

## Configuration

The tiered memory system is configured via environment variables:

### `NEB_TIERED_MEMORY_ENABLED`

Enables or disables the tiered memory system.

- **Type**: Boolean (`1`, `true`, `0`, `false`)
- **Default**: `false` (disabled)
- **Example**: `export NEB_TIERED_MEMORY_ENABLED=1`

### `NEB_TIERED_MEMORY_THRESHOLD`

The percentage threshold (0.0 to 1.0) of the physical memory limit at which eviction is triggered.

- **Type**: Float (0.0 to 1.0)
- **Default**: `0.8` (80%)
- **Example**: `export NEB_TIERED_MEMORY_THRESHOLD=0.7`

When hot memory usage exceeds `physical_memory_limit * threshold`, eviction begins.

### `NEB_TIERED_PHYSICAL_MEMORY_LIMIT`

The maximum physical memory (in bytes) that hot segments can occupy.

- **Type**: Unsigned integer (bytes)
- **Default**: `None` (uses chunk capacity)
- **Example**: `export NEB_TIERED_PHYSICAL_MEMORY_LIMIT=1073741824` (1GB)

This is the **most important setting** for controlling memory usage. When set, Nebuchadnezzar will keep hot segments within this limit by evicting to cold storage.

## Usage Examples

### Example 1: 1GB Physical Memory Limit

```bash
# Enable tiered memory with 1GB limit
export NEB_TIERED_MEMORY_ENABLED=1
export NEB_TIERED_PHYSICAL_MEMORY_LIMIT=1073741824  # 1GB
export NEB_TIERED_MEMORY_THRESHOLD=0.8  # Evict when >800MB used

# Start Nebuchadnezzar
./neb
```

### Example 2: Conservative Memory Usage

```bash
# Enable tiered memory with aggressive eviction
export NEB_TIERED_MEMORY_ENABLED=1
export NEB_TIERED_PHYSICAL_MEMORY_LIMIT=536870912   # 512MB
export NEB_TIERED_MEMORY_THRESHOLD=0.6  # Evict when >307MB used

./neb
```

### Example 3: Large Memory Server

```bash
# Enable tiered memory for datasets > 16GB
export NEB_TIERED_MEMORY_ENABLED=1
export NEB_TIERED_PHYSICAL_MEMORY_LIMIT=17179869184  # 16GB
export NEB_TIERED_MEMORY_THRESHOLD=0.9  # Evict when >14.4GB used

./neb
```

## How It Works

### Eviction Process

1. **Trigger**: When hot memory exceeds `physical_memory_limit * threshold`
2. **Victim Selection**: CLOCK algorithm selects unreferenced segments
3. **Archive**: Segment data is written to backup file
4. **Remap**: Anonymous memory is replaced with read-only file-backed mmap using `MAP_FIXED`
5. **Physical Memory Freed**: Kernel automatically releases physical pages

**Important**: During eviction, segment data remains identical, so no locks are needed on individual cells.

### Promotion Process

1. **Trigger**: Read access to a cell in a cold segment (via `location_for_read()`)
2. **Acquire Lock**: Atomic `promoting` flag prevents concurrent promotions
3. **Copy Data**: Segment data copied to temporary buffer
4. **Remap**: File-backed mmap replaced with anonymous memory using `MAP_FIXED`
5. **Copy Back**: Data copied from buffer to anonymous memory
6. **Release Lock**: Other threads can now access the segment

**Important**: The `promoting` flag protects against the "empty window" that occurs during `MAP_FIXED` remapping.

### CLOCK Eviction Algorithm

The CLOCK algorithm provides efficient LRU-approximation:

- Each segment has a `reference_bit` (set on access)
- CLOCK scans segments circularly
- Referenced segments get a "second chance" (bit cleared)
- Unreferenced segments are evicted
- Head segments and protected segments are skipped

### Address Stability

Segment addresses must remain stable because:

1. `SegmentAllocator::id_by_addr()` uses address arithmetic
2. `cell_index` stores raw memory addresses

Solution: `MAP_FIXED` replaces mappings at the exact same address, maintaining address stability.

## Performance Considerations

### When to Use Tiered Memory

✅ **Good use cases:**
- Datasets larger than available RAM
- Read-heavy workloads with locality
- Batch processing with sequential access
- Development/testing with limited memory

❌ **Avoid for:**
- Datasets that fit in memory
- Random access patterns across entire dataset
- Extremely latency-sensitive applications

### Tuning

#### Physical Memory Limit

- **Too Low**: Excessive eviction/promotion thrashing
- **Too High**: Risk of OOM, not enough memory for other processes
- **Recommended**: 60-80% of available RAM

#### Threshold

- **Low (0.5-0.7)**: More aggressive eviction, lower memory usage
- **High (0.8-0.9)**: Less frequent eviction, better performance
- **Recommended**: 0.7-0.8 for most workloads

#### Eviction Target

When eviction is triggered, the system evicts down to `threshold * 0.7` to avoid immediate re-eviction.

### Monitoring

Check segment status in logs:

```
Memory pressure detected: 10 hot segments (80 MB), limit: 100 MB, threshold: 80 MB, evicting 3 segments
```

Cold segment indicators:

```
Evicting segment 123 to cold storage
Segment 123 evicted. Now cold, backed by file: /path/to/backup
```

Promotion indicators:

```
Promoting segment 123 to hot storage
Segment 123 promoted and unlocked
```

## Architecture Details

### File Structure

```
src/ram/tiered/
├── mod.rs           # Module declaration
├── clock.rs         # CLOCK eviction policy
├── eviction.rs      # Eviction logic (hot → cold)
├── promotion.rs     # Promotion logic (cold → hot)
├── manager.rs       # TieredMemoryManager (coordinator)
└── tests.rs         # Comprehensive tests
```

### Segment State

Each segment tracks its state with atomic fields:

```rust
pub struct Segment {
    // ...
    pub cold_file_fd: AtomicI32,      // -1 = hot, ≥0 = fd for cold
    pub reference_bit: AtomicBool,    // For CLOCK algorithm
    pub promoting: AtomicBool,        // Serializes promotions
}
```

### Integration Points

#### Chunk

- `tiered_manager: Option<TieredMemoryManager>` - Manages eviction/promotion
- `location_for_read()` - Checks if segment is cold, triggers promotion
- `segs_for_compact_cleaner()` - Filters out cold segments
- `segs_for_combine_cleaner()` - Filters out cold segments

#### Segment

- `is_hot()` / `is_cold()` - State queries
- `mark_referenced()` - Mark for CLOCK
- `archive()` - Skip if already cold

## Testing

Three comprehensive tests verify the system:

### `test_eviction_on_memory_overflow`

- Fills memory beyond physical limit
- Verifies automatic eviction
- Checks cold segment file descriptors
- Tests promotion by reading cold cells

### `test_cold_segment_promotion`

- Creates cold segments via explicit eviction
- Reads all cells to trigger promotion
- Verifies data integrity after promotion

### `test_cleaner_ignores_cold_segments`

- Creates cold segments
- Verifies cleaners only see hot segments
- Ensures cold segments are not compacted/combined

Run tests:

```bash
cargo test --lib tiered::tests
```

## Implementation Notes

### Why Not Per-Cell Locking?

Initially considered locking all cells in a segment during promotion, but:

1. `WordMap` (cell index) doesn't expose iteration API
2. Would require extensive changes to expose cell enumeration
3. Atomic `promoting` flag is simpler and equally safe

### Why MAP_FIXED?

`MAP_FIXED` is required to:

1. Replace mappings at the exact same address
2. Maintain address stability for `id_by_addr()`
3. Avoid changing cell_index entries

Alternative (`MAP_FIXED_NOREPLACE`) would fail if address is already mapped.

### Log-Structured Storage Benefits

The log-structured design simplifies tiered memory:

1. Cold segments are immutable (read-only mmap)
2. Head segment is never evicted
3. No need for write-back on eviction (already archived)

## Future Enhancements

Potential improvements:

1. **Adaptive Thresholds**: Adjust threshold based on eviction/promotion frequency
2. **Multi-Level Tiers**: Add SSD tier between RAM and HDD
3. **Prefetching**: Promote segments before they're accessed
4. **Statistics API**: Expose eviction/promotion metrics
5. **Compression**: Compress cold segments to save disk space
6. **NUMA Awareness**: Pin hot segments to local NUMA nodes

## Troubleshooting

### OOM Despite Tiered Memory

**Symptom**: Process killed by OOM even with tiered memory enabled

**Causes**:
- Physical memory limit set too high
- Too many hot segments protected from eviction
- Head segment(s) consuming excessive memory

**Solutions**:
- Lower `NEB_TIERED_PHYSICAL_MEMORY_LIMIT`
- Lower `NEB_TIERED_MEMORY_THRESHOLD`
- Monitor head segment size

### Excessive Promotion/Eviction

**Symptom**: Logs show constant eviction and promotion

**Causes**:
- Working set larger than physical memory limit
- Random access pattern
- Threshold too aggressive

**Solutions**:
- Increase `NEB_TIERED_PHYSICAL_MEMORY_LIMIT`
- Increase `NEB_TIERED_MEMORY_THRESHOLD`
- Improve access locality in application

### Slow Cold Reads

**Symptom**: First access to cold data is very slow

**Expected**: First access to cold segment triggers promotion (copying data)

**Solutions**:
- This is expected behavior
- Consider prefetching if access pattern is predictable
- Increase physical memory limit to keep more data hot

## License

Same as Nebuchadnezzar project.

