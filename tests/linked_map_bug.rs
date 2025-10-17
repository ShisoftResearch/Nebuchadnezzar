// Minimal example to reproduce the LinkedHashMap use-after-free bug
// 
// To run with address sanitizer:
// export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
// cargo test linked_map --target x86_64-unknown-linux-gnu -- --test-threads=1

use std::sync::Arc;
use lightning::linked_map::LinkedHashMap;

#[test]
fn test_linked_map_basic_use_after_free() {
    // Create a LinkedHashMap with Arc values
    let map = LinkedHashMap::with_capacity(4);
    
    // Insert some Arc values
    for i in 0..10 {
        map.insert_back(i, Arc::new(format!("value_{}", i)));
    }
    
    // Access the values
    for i in 0..10 {
        assert!(map.get(&i).is_some());
    }
    
    // The drop of the map here triggers the use-after-free
    // The issue is in the LinkedHashMap's internal cleanup of its
    // linked list structure with circular Arc references
    drop(map);
}

#[test]
fn test_linked_map_with_struct() {
    // Create a structure similar to Segment in the main codebase
    #[derive(Default, Clone)]
    struct TestSegment {
        id: u64,
        data: Vec<u8>,
    }
    
    let map = LinkedHashMap::with_capacity(4);
    
    // Insert Arc<TestSegment> similar to how Chunk stores Arc<Segment>
    for i in 0..10 {
        map.insert_back(
            i as usize, 
            Arc::new(TestSegment {
                id: i,
                data: vec![0u8; 1024 * 100], // 100KB per entry
            })
        );
    }
    
    // Access the map
    for i in 0..10 {
        let seg = map.get(&(i as usize)).unwrap();
        assert_eq!(seg.id, i);
    }
    
    // This drop triggers the use-after-free in LinkedHashMap's destructor
    // The AddressSanitizer will detect:
    // "heap-use-after-free on address ... in core::sync::atomic::atomic_sub"
    drop(map);
}

#[test]
fn test_linked_map_leak_workaround() {
    // The only reliable workaround: intentionally leak the memory
    let map = LinkedHashMap::with_capacity(4);
    
    for i in 0..10 {
        map.insert_back(i, Arc::new(format!("value_{}", i)));
    }
    
    // Intentionally leak to avoid the use-after-free in drop
    // This is what we're doing in the actual test
    std::mem::forget(map);
}

