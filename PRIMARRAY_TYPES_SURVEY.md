# PrimArray Types Survey

## Summary

**`PrimArray` DOES support String arrays!** My previous "fix" was incorrect.

## Complete List of Types in PrimArray

Based on `dovahkiin/src/types/mod.rs:254-275`, the `define_types!` macro generates PrimArray variants for ALL these types:

### Primitive Fixed-Size Types:
1. **bool** (Bool) - 1 byte
2. **char** (Char) - 4 bytes
3. **i8** (I8) - 1 byte
4. **i16** (I16) - 2 bytes
5. **i32** (I32) - 4 bytes
6. **i64** (I64) - 8 bytes
7. **u8** (U8) - 1 byte
8. **u16** (U16) - 2 bytes
9. **u32** (U32) - 4 bytes
10. **u64** (U64) - 8 bytes
11. **f32** (F32) - 4 bytes
12. **f64** (F64) - 8 bytes

### Composite Fixed-Size Types:
13. **Pos2d32** - 8 bytes (2 × f32)
14. **Pos2d64** - 16 bytes (2 × f64)
15. **Pos3d32** - 12 bytes (3 × f32)
16. **Pos3d64** - 24 bytes (3 × f64)
17. **Id** - 16 bytes (2 × u64: higher, lower)

### Variable-Length Types:
18. **String** ✅ - Variable length (u32 length + UTF-8 bytes)
19. **Bytes** - Variable length (u32 length + raw bytes)
20. **SmallBytes** - Variable length (u16 length + raw bytes)

## How PrimArray Works

The `define_types!` macro at `dovahkiin/src/types/mod.rs:254` generates:

```rust
pub enum OwnedPrimArray {
    Bool(Vec<bool>),
    Char(Vec<char>),
    I8(Vec<i8>),
    // ... all fixed-size types
    String(Vec<String>),  // ✅ String arrays ARE PrimArrays!
    Bytes(Vec<Bytes>),
    SmallBytes(Vec<SmallBytes>),
}
```

And correspondingly:

```rust
pub enum SharedPrimArray<'a> {
    Bool(bool_io::Slice<'a>),
    Char(char_io::Slice<'a>),
    // ...
    String(string_io::Slice<'a>),  // ✅ String slices ARE supported!
    // ...
}
```

## The Test That Broke

`ram::tests::chunk::complex_cell_sel_read` uses:

```rust
strings: OwnedValue::PrimArray(OwnedPrimArray::String(vec![
    String::from("aaaa"),
    String::from("bbbb"),
    String::from("cccc")
]))
```

This is **CORRECT and valid usage**! String arrays ARE PrimArrays.

## Why My "Fix" Was Wrong

I changed the pattern from:
```rust
(_, true, _, None) => { /* read as PrimArray */ }
```

To:
```rust
(false, true, _, None) => { /* read as PrimArray */ }
```

This excluded `field_var_base_ty=true` which includes Strings, Bytes, and SmallBytes.

But these types ARE valid PrimArray types! They're just variable-length primitives.

## The Real Question

If the original code was correct and supported string arrays properly, then **what was actually causing the UTF-8 errors**?

The errors we saw:
```
[2025-10-31T03:21:35Z ERROR dovahkiin::types::string_io] string_io: invalid UTF-8 at ptr=138164346881148 len=1700921344
```

The stack trace showed:
```
15: dovahkiin::types::string_io::read_slice
16: dovahkiin::types::get_shared_prim_array_val
17: neb::ram::io::reader::read_field
```

This path is CORRECT for reading string arrays! The problem must be:

1. **Misaligned `base_ptr`** passed to `read_field()` - causing strings to be read from wrong addresses
2. **Schema mismatch** - writer and reader using different schemas (static_bound=44 vs 48)
3. **Corrupted pointer values** - the u32 offsets stored in the static region are wrong
4. **Transaction recovery issue** - reading cells without proper schema context

## Recommendation

Revert my "fix" to `src/ram/io/reader.rs` - it was incorrect. The real bugs are:

1. ✅ **Schema alignment bug** (FIXED) - `static_bound` using 4-byte instead of 8-byte alignment
2. ❓ **Base pointer corruption** - Need to investigate where `base_ptr` comes from when calling `read_by_schema()`
3. ❓ **Schema version mismatch** - Old cells vs new schemas (user said "everything starts new" though)
4. ❓ **Transaction recovery** - Graph ID list operations reading cells with wrong/missing schema

The pattern matching in `read_field()` was CORRECT as originally written.

