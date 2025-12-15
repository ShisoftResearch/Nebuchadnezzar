# Eviction System Fixes

## Problem
The eviction system was struggling to find victims, resulting in 258GB hot memory when the physical limit was set to 8GB. Eviction was almost completely failing.

## Root Causes Identified

### 1. **No Background Eviction Task**
**Impact:** Critical - Primary cause of the issue
- Eviction only ran when allocating new segments
- Once data was loaded (e.g., during recovery), if no new allocations occurred, eviction never ran
- Hot memory could grow indefinitely with no automatic cleanup

### 2. **CLOCK Policy Too Restrictive**
**Impact:** High
- CLOCK policy required segments to be archived before selecting them as victims
- Many hot segments weren't archived yet, making them ineligible for eviction
- This significantly reduced the pool of evictable segments

### 3. **Reference Leak in Combine Cleaner**
**Impact:** Medium
- Segment references were incremented to prevent eviction during combine operations
- Early return path didn't decrement references, causing leaks
- Leaked references made `no_references()` return false, blocking eviction

### 4. **Eviction Gave Up Too Easily**
**Impact:** Medium
- Eviction tried each segment only once
- If references were held temporarily, eviction wouldn't retry
- No mechanism to wait for references to be released

## Fixes Applied

### Fix 1: Added Background Eviction to Cleaner Thread
**File:** `src/ram/cleaner/mod.rs`
**Changes:**
- Added periodic eviction check to the cleaner's main loop
- Runs every 100ms (configurable via `NEB_CLEANER_SLEEP_INTERVAL_MS`)
- Each chunk is checked and evicted if over its memory limit
- Properly gated with `#[cfg(feature = "tiered_memory")]`

**Code:**
```rust
// Background eviction: check and evict if memory limit exceeded
#[cfg(feature = "tiered_memory")]
checks_ref_clone.list.par_iter().for_each(|chunk| {
    if let Some(ref tiered_manager) = chunk.tiered_manager {
        match tiered_manager.evict_for_allocation(chunk) {
            Ok(evicted) => {
                if evicted > 0 {
                    debug!(
                        "Background eviction: evicted {} segments from chunk {}",
                        evicted, chunk.id
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Background eviction failed for chunk {}: {}",
                    chunk.id, e
                );
            }
        }
    }
});
```

### Fix 2: Removed `is_archived()` Check from CLOCK Policy
**File:** `src/ram/tiered/clock.rs`
**Changes:**
- Removed the requirement for segments to be archived before selection
- Eviction process already handles archiving (lines 64-93 in `eviction.rs`)
- Updated documentation to clarify this behavior
- Removed unused `std::sync::Arc` import

**Rationale:** The `is_archived()` check was overly restrictive. The eviction process will archive segments if needed, so there's no reason to exclude unarchived segments from victim selection.

### Fix 3: Fixed Reference Leak in Combine Cleaner
**File:** `src/ram/cleaner/combine.rs`
**Changes:**
- Added reference decrement on early return path (line 209)
- Ensures references are properly released even when combine is aborted

**Before:**
```rust
if pending_segments_len >= segments_to_combine_len {
    for seg in segments.iter() {
        seg.set_hot();
    }
    return 0;  // LEAK: references not decremented
}
```

**After:**
```rust
if pending_segments_len >= segments_to_combine_len {
    // Release references before returning (they were acquired at line 88)
    for seg in segments.iter() {
        seg.references.fetch_sub(1, Ordering::Relaxed);
        seg.set_hot();
    }
    return 0;
}
```

### Fix 4: Made Eviction More Aggressive with Retry Logic
**File:** `src/ram/tiered/manager.rs`
**Changes:**
- Added retry mechanism that attempts up to `target * 2` times
- Gives up only after 3 consecutive attempts without progress
- Uses `thread::yield_now()` to allow references to be released
- Better logging to diagnose eviction failures

**Benefits:**
- Can evict segments whose references are temporarily held
- More resilient to transient conditions
- Better visibility into eviction struggles via warning logs

## Expected Results

After these fixes, the eviction system should:

1. **Proactively manage memory:** Background eviction runs every 100ms, keeping hot memory near the limit even without new allocations

2. **Find more victims:** Removing the `is_archived()` check greatly expands the pool of evictable segments

3. **Avoid reference leaks:** The combine cleaner properly releases references, preventing segments from being permanently marked as "in use"

4. **Be more persistent:** Retry logic allows eviction to succeed even when references are temporarily held

## Monitoring

To verify the fixes are working, monitor:

1. **Hot vs Cold memory ratio:** Should converge toward the configured limit
2. **Eviction logs:** Look for `"Background eviction: evicted N segments"` messages
3. **Warning logs:** If you see frequent `"Eviction stalled"` warnings, segments may be legitimately protected or have persistent references

## Configuration

The cleaner (and thus background eviction) runs at an interval controlled by:
```bash
export NEB_CLEANER_SLEEP_INTERVAL_MS=100  # Default: 100ms
```

Shorter intervals mean more aggressive eviction but higher CPU usage.

## Related Issues

- Reference counting: Ensure `PendingEntry` drop always releases references
- Transaction protection: Long-running transactions can legitimately block eviction
- Cleaner interactions: Compaction and combining acquire references, which can temporarily block eviction

