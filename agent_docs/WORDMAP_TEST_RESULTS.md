# WordMap Testing Results

## Hypothesis
User suggested: "WordMap might have offset problem, which could be off for 6 bytes"

## Tests Performed

Created comprehensive tests in `src/ram/tests/wordmap_test.rs`:

1. **test_wordmap_stores_correct_addresses** - Basic storage/retrieval test
2. **test_wordmap_concurrent_stress** - 10,000 concurrent writes across 10 threads
3. **test_wordmap_with_actual_segment_addresses** - Using real allocated segment addresses

## Results

**ALL TESTS PASSED ✅**

```
test ram::tests::wordmap_test::test_wordmap_stores_correct_addresses ... ok
test ram::tests::wordmap_test::test_wordmap_with_actual_segment_addresses ... ok  
test ram::tests::wordmap_test::test_wordmap_concurrent_stress ... ok
```

Specific findings:
- ✅ 5/5 test addresses stored and retrieved correctly
- ✅ 10/10 segment addresses stored and retrieved correctly  
- ✅ 10,000/10,000 concurrent addresses stored and retrieved correctly
- ✅ No alignment corruption observed
- ✅ No offset issues found

## Conclusion

**WordMap is NOT the source of the 6-byte offset corruption.**

The lightning `WordMap` implementation correctly:
- Stores `usize` values without modification
- Retrieves exact same values that were stored
- Handles concurrent access without corruption
- Works correctly with real segment addresses

## Next Investigation Steps

Since WordMap is not the problem, the corruption must occur in:

1. **Address calculation BEFORE storage in WordMap**
   - Check if wrong address is being calculated when writing cells
   - Verify `pending_entry.addr` is correct
   - Check if there's confusion between entry_addr vs content_addr vs data_addr

2. **Address usage AFTER retrieval from WordMap**
   - Check if retrieved address is used incorrectly
   - Verify pointer arithmetic after retrieval
   - Look for off-by-N calculations

3. **Concurrent modification race conditions**
   - Check for TOCTOU (Time of Check Time of Use) bugs
   - Verify atomic operations
   - Look for missing memory barriers

4. **Memory corruption from other sources**
   - Buffer overflows
   - Use-after-free
   - Double-free
   - Wild pointers

5. **Serialization/Recovery bugs**
   - Check WAL replay logic
   - Verify backup/restore doesn't corrupt addresses
   - Check segment recovery code

## The 0xE6 Pattern

All crash addresses end in `0xE6` (offset 6 from 8-byte boundary):
- `0x6c395a4000e6`
- `0x6c51974000e6`

This consistent pattern suggests:
- **NOT random corruption** (would have varying offsets)
- **Systematic bug** adding/subtracting 6 bytes somewhere
- **OR** reading from wrong struct field offset

## Recommended Focus

Since WordMap is proven correct, focus investigation on:

1. The exact point where addresses are stored in cell_index
2. The exact point where addresses are retrieved from cell_index  
3. Any pointer arithmetic between allocation and storage
4. Any pointer arithmetic between retrieval and use

Look for patterns involving:
- Struct field offsets
- Pointer casts
- Manual offset calculations
- Sizeof/offsetof operations

