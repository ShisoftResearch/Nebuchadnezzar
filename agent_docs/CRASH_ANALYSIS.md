# Crash Analysis - Consistent Behavior Confirmed

## Crash Addresses

1. **First crash**: `0x6c395a4000e6`
2. **Second crash**: `0x6c51974000e6`
3. **Third crash**: `0x75b917a000e6` ← **NEW**

## Pattern Analysis

All three addresses show **EXACTLY the same pattern**:

| Address | Last Byte | 8-byte Offset | 4-byte Offset | Correct Address |
|---------|-----------|---------------|---------------|-----------------|
| `0x6c395a4000e6` | `0xE6` (230) | 6 | 2 | `0x6c395a4000e0` |
| `0x6c51974000e6` | `0xE6` (230) | 6 | 2 | `0x6c51974000e0` |
| `0x75b917a000e6` | `0xE6` (230) | 6 | 2 | `0x75b917a000e0` |

## Consistency Confirmed ✅

- ✅ **Same last byte**: `0xE6` (230 decimal) in all crashes
- ✅ **Same 8-byte offset**: All have offset 6 from 8-byte boundary
- ✅ **Same 4-byte offset**: All have offset 2 from 4-byte boundary (causes u32 read panic)
- ✅ **Same root cause**: **Something is consistently adding +6 bytes to correct addresses**

## Why Assertions Didn't Catch This?

Looking at the stack trace:
```
25: neb::ram::chunk::Chunk::read_cell
24: SharedData::from_chunk_raw
23: SharedCellData::from_chunk_raw
```

The user ran: `./wikidata_cli_asan_debug` 

**Critical Issue**: The assertions are `#[cfg(debug_assertions)]`, but ASAN builds might not have `debug_assertions` enabled!

## Next Steps

1. **Verify build configuration**: Ensure `debug_assertions` is enabled in the ASAN build
2. **Check if assertions ran**: Add unconditional logging before assertions
3. **If assertions didn't run**: The corruption happens, but we're not catching it at the right point

## The Math

For address `0x75b917a000e6`:
```
Crash:   0x75b917a000e6  (offset: 6 from 8-byte boundary)
Correct: 0x75b917a000e0  (offset: 0, 8-byte aligned)
Difference: +6 bytes
```

This proves the corruption is **systematic and consistent**.

## What This Tells Us

1. **NOT random corruption** - would have varying offsets
2. **Systematic bug** - always adds exactly +6 bytes
3. **Happens consistently** - not race-dependent (though might be triggered by races)
4. **Same root cause** - all three crashes share identical pattern

## Investigation Focus

Since the pattern is consistent, we should:

1. **Check if assertions actually ran** in the ASAN build
2. **If they did run**: Why didn't they catch it? (Maybe corruption happens between assertion and use?)
3. **If they didn't run**: Enable them properly in ASAN builds

## Stack Trace Analysis

The crash happens at:
- Frame 20: `read_field` - reading a field from schema
- Frame 19: `get_shared_prim_array_val` - reading primitive array
- Frame 18: `string_io::read_slice` - reading string slice
- Frame 4: `u32_io::read` - **PANIC HERE** trying to read u32 at misaligned address

The address was retrieved from `cell_index` via `read_cell` → `location_for_read`.

**If assertions were enabled**, they should have caught it at `location_for_read`!

## Recommendation

Add unconditional logging to trace the address flow:

```rust
pub fn location_for_read<'a>(&self, hash: u64) -> Result<CellReadGuard<'_>, ReadError> {
    let guard = self.cell_index.lock(hash as usize);
    match guard {
        Some(index) => {
            let addr = *index;
            // UNCONDITIONAL logging (not behind debug_assertions)
            error!("TRACE location_for_read: hash={}, addr=0x{:016x}, aligned={}", 
                   hash, addr, addr % 8 == 0);
            // ... rest of function
```

This will help us see if the address is already corrupted when retrieved.

