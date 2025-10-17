# Bug Report: LinkedHashMap Use-After-Free in Drop Implementation

## Issue Summary

The `lightning::linked_map::LinkedHashMap` has a critical memory safety bug in its `Drop` implementation that causes heap-use-after-free errors when the map is dropped. This bug is reliably detected by AddressSanitizer and occurs due to improper handling of circular `Arc` references in the internal doubly-linked list structure.

---

## Reproduction

### Environment
- Rust: nightly (required for AddressSanitizer)
- Target: x86_64-unknown-linux-gnu
- Sanitizer: AddressSanitizer

### Minimal Reproduction Code

```rust
use lightning::linked_map::LinkedHashMap;
use std::sync::Arc;

#[test]
fn test_linked_map_use_after_free() {
    let map = LinkedHashMap::with_capacity(4);
    
    // Insert Arc values
    for i in 0..10 {
        map.insert_back(i, Arc::new(format!("value_{}", i)));
    }
    
    // Access values
    for i in 0..10 {
        assert!(map.get(&i).is_some());
    }
    
    // Drop triggers use-after-free
    drop(map);
}
```

### How to Run

```bash
export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
cargo test test_linked_map_use_after_free --target x86_64-unknown-linux-gnu -- --test-threads=1
```

### Expected Result
Test passes without errors.

### Actual Result
```
ERROR: AddressSanitizer: heap-use-after-free on address 0x...
WRITE of size 8 at 0x... thread T1
    #0 in core::sync::atomic::atomic_sub
    #1 in core::sync::atomic::AtomicUsize::fetch_sub
    #2 in alloc::sync::Arc<T>::drop
    #3 in lightning::linked_map::NodePtr::drop
    #4 in lightning::map::ptr_map::PtrHashMap::drop
    #5 in lightning::linked_map::LinkedHashMap::drop
```

---

## Root Cause Analysis

### Data Structure

The `LinkedHashMap` maintains:
1. A `PtrHashMap<K, (V, NodePtr<K>)>` for key-value storage
2. A doubly-linked list via `NodePtr<K>` which contains `Arc<SpinLock<Node<K>>>`
3. Each `Node<K>` has `next` and `prev` pointers to other nodes, forming a circular structure

```
LinkedHashMap<K, V>
  └─> PtrHashMap<K, (V, NodePtr<K>)>
      └─> Buckets containing: (K, V, NodePtr<K>)
          └─> NodePtr<K> = Arc<SpinLock<Node<K>>>
              └─> Node<K> {
                    next: Option<Arc<SpinLock<Node<K>>>>,
                    prev: Option<Arc<SpinLock<Node<K>>>>,
                  }
```

### The Bug

When `LinkedHashMap` is dropped, the `PtrHashMap::drop` iterates through its buckets and drops each `(V, NodePtr<K>)` pair. The problem:

**Scenario:**
```
Node1 ←Arc→ Node2 ←Arc→ Node3 ←Arc→ Node4
  ↑                               ↓
  └───────────Arc─────────────────┘
```

**Drop sequence:**
1. Iteration drops `NodePtr` for Node2
   - `Arc<SpinLock<Node2>>::drop` is called
   - Reference count reaches 0
   - **Memory for Node2's Arc is freed**

2. Iteration drops `NodePtr` for Node1
   - Node1 still has an `Arc` reference to Node2 in its `next` field
   - When dropping Node1's internal `Arc`, it tries to drop the Arc to Node2
   - **Tries to call `atomic_sub` on Node2's already-freed Arc counter**
   - 💥 **heap-use-after-free**

### Why This Happens

The `LinkedHashMap::drop` implementation (or lack thereof) doesn't:
1. Explicitly unlink the circular references before dropping
2. Clear the `next`/`prev` pointers in the nodes
3. Ensure proper drop order that avoids accessing freed memory

The nodes are dropped in hash map iteration order, NOT in a safe order that respects the circular Arc dependencies.

---

## Task for Coding Agent

**Please fix the use-after-free bug in `lightning::linked_map::LinkedHashMap` by implementing a proper `Drop` implementation that safely cleans up the circular Arc references.**

### Requirements

1. **Prevent Use-After-Free**: Ensure that no `Arc` reference counter is accessed after its memory has been freed

2. **Preserve Functionality**: The fix should not break any existing functionality or API

3. **No Memory Leaks**: All allocated memory should be properly freed (verified by LeakSanitizer)

4. **Thread Safety**: Maintain thread safety guarantees if any

### Suggested Approach

The fix likely needs to:

1. **Implement a custom `Drop` for `LinkedHashMap`** that:
   - Walks the linked list in order (not hash map order)
   - Breaks circular references by clearing `next`/`prev` pointers
   - Only then allows the Arcs to be dropped

2. **OR modify `NodePtr::drop`** to:
   - Check if it's the last reference before accessing neighbor nodes
   - Safely unlink itself from the list

3. **OR modify `PtrHashMap::drop`** to:
   - First pass: unlink all nodes (clear next/prev pointers)
   - Second pass: drop all NodePtr values

### Pseudocode Example

```rust
impl<K, V> Drop for LinkedHashMap<K, V> {
    fn drop(&mut self) {
        // Option 1: Walk the linked list and unlink all nodes first
        if let Some(head) = self.head.take() {
            let mut current = Some(head);
            while let Some(node) = current {
                let next = {
                    let mut locked = node.lock();
                    locked.prev = None;  // Break circular refs
                    locked.next.take()   // Get next and clear
                };
                current = next;
            }
        }
        
        // Option 2: Or clear all next/prev pointers in the map
        // for (_, (_, node_ptr)) in self.map.iter_mut() {
        //     if let Some(node) = node_ptr.try_lock() {
        //         node.next = None;
        //         node.prev = None;
        //     }
        // }
        
        // Now safe to drop the PtrHashMap
        // (happens automatically)
    }
}
```

### Files to Examine

- `src/linked_map.rs` - Main LinkedHashMap implementation
- `src/linked_list.rs` - LinkedList internal structure
- `src/map/ptr_map.rs` - PtrHashMap implementation

### Testing

After implementing the fix:

1. Run with AddressSanitizer:
```bash
export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
cargo test --target x86_64-unknown-linux-gnu -- --test-threads=1
```

2. Verify no use-after-free errors

3. Run with LeakSanitizer to ensure no memory leaks:
```bash
export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
export ASAN_OPTIONS=detect_leaks=1
cargo test --target x86_64-unknown-linux-gnu
```

4. Run all existing tests to ensure no regressions:
```bash
cargo test
```

### Success Criteria

- ✅ No AddressSanitizer errors (heap-use-after-free)
- ✅ No LeakSanitizer errors (memory leaks)
- ✅ All existing tests pass
- ✅ The minimal reproduction test passes with sanitizers enabled
- ✅ No performance regression in normal operations

---

## Additional Context

This bug affects any code that:
1. Uses `LinkedHashMap` with `Arc<T>` values (or any values containing Arcs)
2. Allows the map to be dropped normally
3. Has multiple entries in the map (single entry doesn't trigger the bug)

### Current Workaround

Users can work around this bug by:
```rust
// Intentionally leak the map to avoid the buggy drop
std::mem::forget(map);
```

However, this causes memory leaks and is not a proper solution.

---

## References

- Lightning crate: https://github.com/shisoft/Lightning
- Similar issues in other linked structures with Arc: 
  - [Rust RFC: Proper cleanup of circular references](https://github.com/rust-lang/rfcs/issues/1993)
  - Best practice: break cycles before dropping

---

## Priority

**Critical** - This is a memory safety bug that causes undefined behavior and can lead to crashes or security vulnerabilities in production code.

