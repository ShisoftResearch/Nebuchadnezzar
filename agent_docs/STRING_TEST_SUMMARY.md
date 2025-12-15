# Variable Data Test Summary

I started creating comprehensive tests for variable-length data types (strings, arrays, etc.) in cells, but encountered some compilation issues with test infrastructure.

## What Was Attempted

Created `/src/ram/tests/variable_data_comprehensive.rs` with tests for:
1. Strings of various lengths (empty, small, medium, large, Unicode)
2. String arrays of different sizes
3. Id arrays
4. Numeric arrays (u8, u32, u64, i64, f64)
5. Mixed variable-length fields
6. Alignment boundary cases

## Current Status

The test file needs to be either:
1. Fixed to use the async `with_test_server` pattern (more complex)
2. Simplified to use direct chunk/cell operations without full server infrastructure

## Critical Finding from Earlier Tests

From `test_alignment_math_for_static_bound`:
```
OLD schema: static_bound = 44
  First string would be at: 44

NEW schema: static_bound = 48
  First string would be at: 48

❌ MISMATCH!
  Difference: 4 bytes
```

**This confirms the alignment fix is working, BUT:**
- NEW cells written with static_bound=48 work correctly ✅
- OLD cells written with static_bound=44 cannot be read with new code ❌

## The Real Issue

The string errors you're seeing (`len=1700921344`) are likely from:
1. **Old cells** in your database written with misaligned schemas (44, 20, 4)
2. **New code** trying to read them with properly aligned schemas (48, 24, 8)
3. The 4-byte offset mismatch causes reading garbage bytes as string length

## Recommendation

**For your production database:**
1. Backup all data
2. Export cells to a safe format
3. Clear the database
4. Restart with fixed code (static_bound properly aligned)
5. Reimport data (will use new aligned schemas)

This is a **schema version incompatibility** issue, not a current alignment bug.

