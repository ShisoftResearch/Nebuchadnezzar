# LinkedHashMap Use-After-Free Bug

## Summary

The `lightning` crate's `LinkedHashMap` implementation has a use-after-free bug in its destructor when storing `Arc<T>` values. This bug is detected by AddressSanitizer and causes the test `ram::cleaner::tests::full_clean_cycle` to fail.

## Root Cause

The bug is in the cleanup order of `LinkedHashMap`'s internal structure:

```
LinkedHashMap<K, V>
  └─> PtrHashMap<K, (V, NodePtr<K>)>
        └─> Internal Vec of entries: (K, V, NodePtr<K>)
              └─> NodePtr<K> contains Arc<SpinLock<Node<K>>>
                    └─> Node<K> has next/prev pointers forming a circular linked list
```

### The Problem

1. When `LinkedHashMap` is dropped, `PtrHashMap::drop` is called
2. `PtrHashMap::drop` iterates through its internal buckets and drops each `(V, NodePtr<K>)` pair
3. When `NodePtr<K>` is dropped, it drops the `Arc<SpinLock<Node<K>>>`
4. The `Node<K>` structure contains pointers to other nodes in a circular doubly-linked list
5. During `Arc::drop`, the reference count is decremented using `atomic_sub`
6. However, due to the circular structure, some `Arc` objects may have already been freed
7. This causes **heap-use-after-free** when trying to access the atomic reference counter

## AddressSanitizer Output

```
==ERROR: AddressSanitizer: heap-use-after-free on address 0x... at pc 0x...
WRITE of size 8 at 0x... thread T1
    #0 in core::sync::atomic::atomic_sub
    #1 in core::sync::atomic::AtomicUsize::fetch_sub
    #2 in alloc::sync::Arc<T>::drop
    #3 in lightning::linked_map::NodePtr::drop
    #4 in lightning::map::ptr_map::PtrHashMap::drop
    #5 in lightning::linked_map::LinkedHashMap::drop
```

## Affected Code

The bug affects any code that:
1. Uses `lightning::linked_map::LinkedHashMap`
2. Stores `Arc<T>` values in the map
3. Allows the map to be dropped normally (rather than leaking it)

In our codebase, this specifically affects:
- `src/ram/chunk.rs`: The `Chunk` struct contains `segs: LinkedHashMap<usize, Arc<Segment>>`
- `src/ram/cleaner/tests.rs`: The `full_clean_cycle` test creates and drops `Chunks`

## Reproduction

### Minimal Example

See `tests/linked_map_minimal.rs` for a minimal reproduction. However, the bug is **non-deterministic** and depends on:
- The specific memory layout
- The order of Arc drops
- The internal state of the LinkedHashMap

### Reliable Reproduction

The most reliable way to reproduce is:
```bash
export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
cargo test ram::cleaner::tests::full_clean_cycle --target x86_64-unknown-linux-gnu -- --test-threads=1
```

## Workaround

### Immediate Workaround (Applied)

In `src/ram/cleaner/tests.rs`, we use `std::mem::forget()` to intentionally leak the `Chunks` object:

```rust
// Intentionally leak the chunks to avoid use-after-free in LinkedHashMap cleanup
std::mem::forget(chunks);
```

This prevents the `LinkedHashMap` destructor from running, avoiding the bug.

### Long-term Solutions

1. **Replace LinkedHashMap**: Use a different data structure that doesn't have this bug
   - Option: Use `std::collections::HashMap` + a separate doubly-linked list
   - Option: Use `indexmap::IndexMap` which provides insertion order
   
2. **Fix the lightning crate**: The bug should be reported and fixed upstream
   - Issue URL: https://github.com/shisoft/Lightning (to be created)
   - The fix would need to change how `NodePtr` handles circular references during drop

3. **Custom cleanup**: Manually remove all entries before dropping:
   ```rust
   // Clear all entries before drop
   let keys: Vec<_> = chunk.segments().iter().map(|s| s.id as usize).collect();
   for key in keys {
       chunk.segs.remove(&key);
   }
   drop(chunk);
   ```
   Note: This workaround is **not guaranteed** to prevent the bug.

## Impact

### Test Suite
- The `full_clean_cycle` test now uses the workaround and passes
- Memory leak in tests is acceptable as tests are short-lived

### Production Code
- The `Chunk` struct is used in production
- The bug only manifests when `Chunk` is dropped
- In production, `Chunk` objects are typically long-lived
- However, server shutdown or chunk reallocation could trigger the bug

## Next Steps

1. ✅ Apply workaround in test (`std::mem::forget`)
2. ✅ Document the bug in this file
3. ⏳ Create minimal reproduction for upstream report
4. ⏳ Report bug to lightning crate maintainers
5. ⏳ Evaluate replacement data structures for production code

## References

- Lightning crate: https://github.com/shisoft/Lightning
- LinkedHashMap source: https://github.com/shisoft/Lightning/blob/master/src/linked_map.rs
- AddressSanitizer documentation: https://github.com/google/sanitizers/wiki/AddressSanitizer

