# Dovahkiin String Array Alignment Bug

## Problem Summary

The `string_io::read_slice` function in dovahkiin has a critical alignment bug when reading arrays of variable-length strings. This causes segfaults when reading UTF-8 strings or any strings with byte lengths that don't result in 4-byte-aligned sizes.

## Root Cause

**File**: `dovahkiin/src/types/macros.rs` (line 168-183)

The `read_slice` function for variable-length types (String, Bytes, SmallBytes) does this:

```rust
pub fn read_slice<'a>(mut mem_ptr: usize, len: usize) -> (Slice<'a>, usize) {
    let origin_ptr = mem_ptr;
    let align = type_align();  // For String: 4 bytes
    let res = (0..len)
        .map(|_| {
            // Align pointer to required alignment before reading
            mem_ptr = (mem_ptr + align - 1) & !(align - 1);
            let current_ptr = mem_ptr;
            let v = read(current_ptr);
            // advance by encoded size to be robust to decoding issues
            mem_ptr += size_at(current_ptr);  // <-- BUG: No re-alignment!
            v
        })
        .collect::<Vec<_>>();
    (res, mem_ptr - origin_ptr)
}
```

## The Bug

**Line 178**: After advancing `mem_ptr` by `size_at(current_ptr)`, the function does NOT re-align the pointer before the next iteration.

### Why This Causes Problems

For `String` type:
- `size_at(mem_ptr)` returns `str_len + 4` (where 4 is the u32 length prefix)
- This size is only 4-byte-aligned when `str_len % 4 == 0`
- When `str_len % 4 != 0`, the next iteration tries to read a u32 length from a misaligned address

### Example

**Working case (ASCII 4-byte strings)**:
```
String: "aaaa" -> len=4 -> size_at=8 -> 8%4=0 ✓ (aligned)
```

**Failing case (UTF-8 variable-length)**:
```
String: ""          -> len=0 -> size_at=4 -> 4%4=0 ✓ (aligned)
String: "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ" -> len=41 -> size_at=45 -> 45%4=1 ✗ (MISALIGNED!)
Next read tries to read u32 from misaligned address -> SEGFAULT
```

## Impact

- **ASCII 4-byte strings**: Work by coincidence
- **ASCII with varying lengths**: Crash
- **UTF-8 strings**: Crash  
- **All variable-length strings**: Crash when length mod 4 != 0

## Fix Required

The `read_slice` function needs to re-align `mem_ptr` after advancing by `size_at`:

```rust
pub fn read_slice<'a>(mut mem_ptr: usize, len: usize) -> (Slice<'a>, usize) {
    let origin_ptr = mem_ptr;
    let align = type_align();
    let res = (0..len)
        .map(|_| {
            // Align pointer to required alignment before reading
            mem_ptr = (mem_ptr + align - 1) & !(align - 1);
            let current_ptr = mem_ptr;
            let v = read(current_ptr);
            // advance by encoded size
            mem_ptr += size_at(current_ptr);
            // FIX: Re-align for next element
            mem_ptr = (mem_ptr + align - 1) & !(align - 1);
            v
        })
        .collect::<Vec<_>>();
    (res, mem_ptr - origin_ptr)
}
```

## Alternative: Writer Fix

Alternatively, the writer should ensure 4-byte padding after each string:

```rust
pub fn write(val: &str, mem_ptr: usize) {
    let bytes = val.as_bytes();
    let len = bytes.len();
    u32_io::write(&(len as u32), mem_ptr);
    let mut smem_ptr = mem_ptr + u32_io::type_size();
    unsafe {
        for b in bytes {
            ptr::write(smem_ptr as *mut u8, *b);
            smem_ptr += 1;
        }
    }
    // FIX: Pad to 4-byte boundary
    let total_size = len + 4;
    let padding = (4 - (total_size % 4)) % 4;
    for _ in 0..padding {
        ptr::write(smem_ptr as *mut u8, 0);
        smem_ptr += 1;
    }
}
```

Then `val_size` would need to return the padded size instead of just `len + 4`.

## Recommendation

**Prefer the reader fix** (re-alignment in `read_slice`) because:
1. It handles existing data correctly
2. More efficient than padding all strings
3. Maintains backward compatibility
4. Only affects variable-length primitive array reading

## Test Case

A working test case demonstrating the bug:

```rust
let strings = vec![
    String::from(""),
    String::from("ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"),  // 41 bytes
    String::from("中文测试文本"),       // 18 bytes
];
```

This test should pass after the fix.

