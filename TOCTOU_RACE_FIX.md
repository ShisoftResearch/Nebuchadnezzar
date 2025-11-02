# TOCTOU Race Condition Fix - Cleaner vs Cell Updates

## Summary

Fixed a critical **Time-Of-Check-Time-Of-Use (TOCTOU)** race condition between the compact cleaner and concurrent cell updates that caused panics with corrupted entry headers.

**Commits:**
- `2938e606` - Improved entry decode error diagnostics
- `fcab42d5` - Fixed TOCTOU race condition in compact cleaner

## The Bug

### Error Manifestation
```
Cannot decode entry header: invalid entry_type_bits=2099407214 (0x7d22696e) 
at address 0x00007498aecb9678. 
Valid types are: UNDECIDED(0), CELL(1), TOMBSTONE(2).
```

**Key Observations:**
- Address `0x00007498aecb9678` is properly 8-byte aligned
- Invalid bits `0x7d22696e` decode to ASCII: `}`, `"`, `i`, `n` → `}"in`
- This indicates memory was overwritten with text data (likely JSON)
- Classic **use-after-free** scenario

### Root Cause: TOCTOU Race Condition

The compact cleaner in `src/ram/cleaner/compact.rs` had a dangerous sequence:

```rust
// OLD BUGGY CODE (lines 47-119)
let entries = chunk.live_entries(seg)  // ← Check (no lock)
    .collect_vec();

entries.into_iter().for_each(|entry| {
    let cell_migration = Some(chunk.cell_index.lock(hash));  // ← Lock acquired AFTER check
    
    unsafe {
        libc::memmove(cursor, entry_pos, entry_size);  // ← Use (moves memory)
    }
    
    if let Some(mut cell_guard) = cell_migration {
        if *cell_guard == old_addr {
            *cell_guard = new_addr;  // ← Update cell_index
        } else {
            chunk.mark_dead_entry_with_seg(new_addr, seg);  // ← ERROR: Decodes garbage!
        }
    }
});
```

### The Race Window

1. **Thread A (Cleaner):** Calls `live_entries(seg)` 
   - Checks cell at address `0x...9678` IS live ✓
   - NO LOCK HELD during this check!

2. **Thread B (User):** Calls `update_cell_by()`
   - Locks cell_index
   - Writes new cell version to different address
   - Updates cell_index to point to new address
   - Marks `0x...9678` as dead
   - Releases lock

3. **Thread A (Cleaner):** NOW acquires lock
   - Performs `memmove(cursor, entry_pos, ...)` - moves stale data!
   - Checks if address still matches - NO!
   - Calls `mark_dead_entry_with_seg(new_addr, seg)`
   - Tries to decode entry at `new_addr` - **reads garbage!**

### Why Garbage?

By the time the cleaner calls `mark_dead_entry_with_seg`:
- Original address `0x...9678` was marked dead by Thread B
- Memory at `0x...9678` may have been reused for other data
- Cleaner moved this garbage to `cursor` position
- Trying to decode the garbage as an entry header fails

The hex `0x7d22696e` ("}\"in") suggests the memory was reused for JSON strings, confirming use-after-free.

## The Fix

### New Logic in compact.rs

```rust
// NEW FIXED CODE (lines 70-161)
entries.into_iter().for_each(|entry| {
    if entry.meta.entry_header.entry_type == EntryType::CELL {
        let header = entry.content.as_cell_header();
        let cell_guard = chunk.cell_index.lock(header.hash as usize);  // ← Lock FIRST
        
        // Check WHILE HOLDING LOCK
        if let Some(mut guard) = cell_guard {
            let actual_addr = *guard;
            if actual_addr == entry_pos {  // ← Check address hasn't changed
                // SAFE: Address verified under lock, can move now
                unsafe {
                    libc::memmove(new_addr, old_addr, entry_size);
                }
                *guard = new_addr;  // Update cell_index
                cursor += entry_size;
            } else {
                // Cell was updated by another thread - skip this stale entry
                trace!("Skipping stale entry at {}", entry_pos);
                // Don't advance cursor - reuse this space
            }
        } else {
            // Cell was deleted - skip
            trace!("Skipping deleted entry at {}", entry_pos);
        }
    }
});
```

### Key Changes

1. **Lock acquired BEFORE any memory operations**
   - Prevents cell_index from changing during validation

2. **Address check performed UNDER LOCK**
   - Eliminates the race window
   - Guarantees address cannot change between check and memmove

3. **Skip stale entries instead of moving them**
   - If address changed, don't move the entry at all
   - Reuse the space for subsequent entries
   - Never try to decode stale data

4. **Proper synchronization**
   - Cleaner and user threads now properly coordinate via cell_index lock
   - No more use-after-free scenarios

## Why This Fixes the Bug

### Before (Buggy):
```
Time    Thread A (Cleaner)           Thread B (User)           Memory State
----    -------------------          ----------------          ------------
t0      Check: addr=X is live        -                        X: valid cell
t1      -                            Lock cell_index          X: valid cell
t2      -                            Write new cell at Y      X: valid, Y: valid
t3      -                            *cell_index = Y          cell_index → Y
t4      -                            Mark X as dead           X: DEAD
t5      -                            Release lock             X: may be reused!
t6      Lock cell_index              -                        X: garbage!
t7      memmove(cursor, X, ...)      -                        cursor: garbage!
t8      mark_dead(cursor)            -                        ❌ PANIC!
        decode garbage!
```

### After (Fixed):
```
Time    Thread A (Cleaner)           Thread B (User)           Memory State
----    -------------------          ----------------          ------------
t0      Lock cell_index              -                        X: valid cell
t1      Check: *cell_index == X?     -                        cell_index → X
t2      YES → memmove(cursor, X)     -                        cursor: valid!
t3      *cell_index = cursor         -                        cell_index → cursor
t4      Release lock                 -                        X: now dead
        ✓ Success!

OR if cell was updated:

t0      Lock cell_index              Lock cell_index          X: valid cell
                                     (blocks)
t1      Check: *cell_index == X?     (waiting...)             cell_index → X
        YES                          
t2      memmove(cursor, X)           (waiting...)             cursor: valid!
t3      *cell_index = cursor         (waiting...)             cell_index → cursor
t4      Release lock                 Acquired!                X: now dead
t5      -                            Write new at Y           Y: valid
t6      -                            *cell_index = Y          cell_index → Y
        
Next entry:
t7      Lock cell_index              -                        -
t8      Check: *cell_index == X?     -                        -
        NO → Skip!                                            ✓ No garbage!
```

## Impact

### Before
- ❌ Random panics during WikiData import
- ❌ Corruption when cleaner runs concurrently with updates
- ❌ Difficult to debug (race condition dependent on timing)

### After
- ✓ Proper synchronization between cleaner and updates
- ✓ No use-after-free scenarios
- ✓ Cleaner safely skips entries updated by other threads
- ✓ Improved error messages if corruption still occurs

## Testing Recommendations

1. **Run WikiData import with concurrent load**
   - Previously: Panicked with "Cannot decode entry header"
   - Now: Should complete successfully

2. **Stress test cleaner + updates**
   ```bash
   # High concurrency + active cleaner
   NEB_CLEANER_SLEEP_INTERVAL_MS=10 ./wikidata_cli_debug import --workers 64
   ```

3. **Monitor for trace logs**
   - Look for "Skipping stale entry" messages
   - Confirms cleaner is properly detecting concurrent updates

## Related Files

- **src/ram/cleaner/compact.rs** - Fixed TOCTOU race condition
- **src/ram/cleaner/combine.rs** - Already correct (different design)
- **src/ram/entry.rs** - Improved error diagnostics
- **src/ram/chunk.rs** - Added validation
- **BUG_REPORT_CORRUPTION.md** - Original investigation

## Notes

### Why combine.rs doesn't have this bug

The combine cleaner creates NEW segments and copies entries there. If a cell is updated during combine:
- The copy in the new segment is marked as dead (line 270)
- This is safe because it's in a different segment
- The user's updated cell is in yet another location

The compact cleaner works IN-PLACE within the same segment, so the race condition is more severe.

### Memory ordering

The fix relies on the mutex provided by `cell_index.lock()`, which provides:
- Acquire semantics on lock
- Release semantics on unlock
- Full memory barrier between threads

This guarantees that all updates to the cell are visible when we check the address under lock.

## Future Improvements

1. Consider using epoch-based reclamation for cells
2. Add metrics for "skipped stale entries" count
3. Consider making `live_entries()` acquire locks during iteration
4. Add integration test that reproduces this race condition

## Conclusion

This was a classic concurrent programming bug:
- Check (is entry live?) and Use (move entry) were not atomic
- Proper locking was added but too late in the sequence
- Fixed by moving lock acquisition before all operations

The fix is minimal, focused, and addresses the root cause without changing the overall cleaner architecture.

