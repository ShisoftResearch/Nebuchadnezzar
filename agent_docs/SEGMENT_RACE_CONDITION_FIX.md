# Fix for Segment Head Race Condition

## Issue
The system was experiencing panics with the error:
```
Cannot get header segment with id: 2, have ids [0]
```

## Root Cause
A race condition existed in the `try_acquire` method in `src/ram/chunk.rs` between:

1. **Reading the head segment ID** (`head_seg_id`)
2. **Retrieving the segment from the map** (`segs.get()`)

### Race Condition Sequence:

1. **Thread A** reads `head_seg_id` as 2
2. **Thread B** allocates a new segment 3 and updates `head_seg_id` to 3
3. **Cleaner Thread** sees that segment 2 is no longer the head segment and removes it from the `segs` map
4. **Thread A** tries to get segment 2 from the map, but it's been removed → **PANIC!**

The issue occurs because:
- When a new head segment is allocated, the old head segment is no longer protected from cleanup
- The cleaner actively removes segments that are not the current head and have no references
- There's a window between reading `head_seg_id` and accessing the segment where the segment can be removed

## Solution
Modified `try_acquire` in `/home/shisoft/Code/OSS Projects/Nebuchadnezzar/src/ram/chunk.rs` to handle the case where the head segment has been removed:

**Before (lines 365-371):**
```rust
let head = self.segs.get(&(head_seg_id as usize)).unwrap_or_else(|| {
    panic!(
        "Cannot get header segment with id: {}, have ids {:?}",
        head_seg_id,
        self.segs.iter_front_keys().collect::<Vec<_>>()
    );
});
```

**After (lines 365-378):**
```rust
// Try to get the head segment. If it's been removed (e.g., by cleaner after
// a new head was allocated), retry with the updated head_seg_id.
let head = match self.segs.get(&(head_seg_id as usize)) {
    Some(seg) => seg,
    None => {
        // Segment was removed (likely by cleaner after a new head was allocated)
        // Retry the loop to get the current head segment
        debug!(
            "Head segment {} was removed, retrying with current head",
            head_seg_id
        );
        continue;
    }
};
```

### Key Changes:
1. **Replaced `unwrap_or_else` with `match`** to handle the missing segment gracefully
2. **Added `continue`** to retry the loop when the segment is not found
3. **Added debug logging** to track when this race condition occurs

This ensures that if the head segment is removed between reading `head_seg_id` and accessing it, the function simply retries with the new head segment ID instead of panicking.

## Testing
Created comprehensive tests to verify the fix:

### 1. `test_multiple_segments_in_chunk` 
Tests concurrent writes across multiple threads that trigger multiple segment allocations.

### 2. `test_concurrent_segment_allocation_and_cleanup` (New Stress Test)
Aggressively tests the race condition by:
- Running 8 writer threads continuously allocating cells
- Running a dedicated cleaner thread that constantly triggers garbage collection
- Deleting cells to create dead space for the cleaner to work with
- Ensuring the system remains stable under concurrent segment allocation and cleanup

## Verification
All 12 chunk tests pass, including the new stress test that specifically targets this race condition.

## Impact
- **No breaking changes** to the API
- **Performance impact**: Minimal - only adds a retry loop in the rare case where a segment is removed
- **Safety**: Eliminates a potential panic in production systems under heavy load with garbage collection enabled

