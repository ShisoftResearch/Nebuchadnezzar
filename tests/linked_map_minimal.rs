// Minimal reproducer for the LinkedHashMap use-after-free bug
//
// This reproduces the exact issue seen in the cleaner test.
// The bug is related to how LinkedHashMap handles Arc<T> with complex internal structures.
//
// Run with: export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
//           cargo test --test linked_map_minimal --target x86_64-unknown-linux-gnu -- --test-threads=1

use std::sync::Arc;
use lightning::linked_map::LinkedHashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

// Simplified version of the Segment struct
#[derive(Default, Clone)]
struct SimpleSegment {
    id: u64,
    references: Arc<AtomicUsize>,  // This Arc is important for triggering the bug
}

#[test]
fn test_linked_map_with_nested_arc() {
    let map = LinkedHashMap::with_capacity(4);
    
    // Create segments with Arc inside
    for i in 0..10 {
        let seg = SimpleSegment {
            id: i,
            references: Arc::new(AtomicUsize::new(0)),
        };
        map.insert_back(i as usize, Arc::new(seg));
    }
    
    // Access the segments
    for i in 0..10 {
        let seg = map.get(&(i as usize)).unwrap();
        seg.references.fetch_add(1, Ordering::Relaxed);
        assert_eq!(seg.id, i);
    }
    
    // The drop triggers use-after-free when:
    // 1. LinkedHashMap drops its internal PtrHashMap
    // 2. PtrHashMap drops the NodePtr entries
    // 3. NodePtr contains Arc<SpinLock<Node<usize>>>
    // 4. The Arc reference counting gets confused during circular cleanup
    drop(map);
}

#[test]
fn test_workaround_with_forget() {
    let map = LinkedHashMap::with_capacity(4);
    
    for i in 0..10 {
        let seg = SimpleSegment {
            id: i,
            references: Arc::new(AtomicUsize::new(0)),
        };
        map.insert_back(i as usize, Arc::new(seg));
    }
    
    // Workaround: forget the map to avoid the buggy drop implementation
    std::mem::forget(map);
    // This test will pass without AddressSanitizer errors
}

#[test]
fn test_explanation() {
    // The bug is in the lightning crate's LinkedHashMap implementation.
    // The issue is in the drop order of:
    //
    // LinkedHashMap
    //   └─> PtrHashMap<K, (V, NodePtr<K>)>
    //         └─> Vec of entries containing (Arc<V>, NodePtr<K>)
    //               └─> NodePtr<K> which contains Arc<SpinLock<Node<K>>>
    //
    // When dropping:
    // 1. PtrHashMap::drop iterates and drops each (Arc<V>, NodePtr<K>) pair
    // 2. When NodePtr is dropped, it drops Arc<SpinLock<Node<K>>>
    // 3. The Node contains pointers to other nodes in a circular structure
    // 4. During Arc::drop, it tries to decrement the reference count
    // 5. But the memory might have already been freed by another Arc::drop
    // 6. This causes heap-use-after-free in atomic_sub
    //
    // The root cause is that LinkedHashMap's internal structure has circular
    // Arc references that aren't properly handled during cleanup.
    
    println!("This test explains the bug but doesn't trigger it");
    println!("The actual trigger requires specific timing/ordering of drops");
    println!("See the full_clean_cycle test for a real reproduction");
}







