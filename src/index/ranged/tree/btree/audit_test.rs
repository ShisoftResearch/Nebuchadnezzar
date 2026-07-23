// Tests added during the correctness audit of the B+ tree.
// Each test targets a specific defect found by inspection; see the audit report.
use super::*;
use dovahkiin::types::custom_types::id::Id;
use lightning::map::HashSet;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread;

extern crate env_logger;

const TINY_PAGE_SIZE: usize = 5;
type TinyKeySlice = [EntryKey; TINY_PAGE_SIZE];
type TinyPtrSlice = [NodeCellRef; TINY_PAGE_SIZE + 1];
type TinyTree = BPlusTree<TinyKeySlice, TinyPtrSlice>;

fn deletion_set() -> Arc<DeletionSet> {
    Arc::new(HashSet::with_capacity(16))
}

fn key_of(n: u64) -> EntryKey {
    EntryKey::from_id(&Id::new(1, n))
}

// A backward seek on an empty tree must yield an empty cursor, not a
// phantom all-zero key read from an uninitialized slot.
#[test]
fn backward_seek_on_empty_tree_is_empty() {
    let _ = env_logger::try_init();
    let tree = TinyTree::new(&deletion_set());
    let cursor = tree.seek(&max_entry_key(), Ordering::Backward);
    assert_eq!(
        cursor.current(),
        None,
        "backward seek on empty tree must not fabricate a key"
    );
}

#[test]
fn forward_seek_on_empty_tree_is_empty() {
    let _ = env_logger::try_init();
    let tree = TinyTree::new(&deletion_set());
    let cursor = tree.seek(&min_entry_key(), Ordering::Forward);
    assert_eq!(cursor.current(), None);
}

// Merging a key that already exists into a full page must not create a
// duplicate through the split path (the in-place path already dedups).
#[test]
fn merge_existing_key_into_full_page_no_duplicate() {
    let _ = env_logger::try_init();
    let tree = TinyTree::new(&deletion_set());
    // Fill the single root leaf completely: 0, 2, 4, 6, 8
    for n in [0u64, 2, 4, 6, 8] {
        assert!(tree.insert(&key_of(n)));
    }
    // Merge a key that is already present while the page is full.
    tree.merge_with_keys_(vec![key_of(4)]);
    assert!(
        verification::is_tree_in_order(&tree, 0),
        "duplicate key introduced by merge split path"
    );
    // Full scan must see each key exactly once.
    let mut cursor = tree.seek(&min_entry_key(), Ordering::Forward);
    let mut seen = vec![];
    while let Some(k) = cursor.next() {
        seen.push(k);
    }
    let expected: Vec<_> = [0u64, 2, 4, 6, 8].iter().map(|&n| key_of(n)).collect();
    assert_eq!(seen, expected, "scan must yield each key exactly once");
}

// A backward seek for a key that falls in the gap between two pages must
// return the predecessor (largest key <= seek key), not the successor.
#[test]
fn backward_seek_gap_key_returns_predecessor() {
    let _ = env_logger::try_init();
    let tree = TinyTree::new(&deletion_set());
    const N: u64 = 200;
    // Insert only even keys; odd keys are gaps.
    for n in 0..N {
        assert!(tree.insert(&key_of(n * 2)));
    }
    for n in 0..N - 1 {
        let gap = key_of(n * 2 + 1); // between 2n and 2n+2
        let cursor = tree.seek(&gap, Ordering::Backward);
        assert_eq!(
            cursor.current(),
            Some(&key_of(n * 2)),
            "backward seek for gap key {} must return {}",
            n * 2 + 1,
            n * 2
        );
    }
}

// A backward seek for a key smaller than every key in the tree must yield
// an empty cursor, not the smallest key (which is > the seek key).
#[test]
fn backward_seek_before_min_is_empty() {
    let _ = env_logger::try_init();
    let tree = TinyTree::new(&deletion_set());
    for n in 0..50u64 {
        assert!(tree.insert(&key_of(n)));
    }
    // key_of uses Id::new(1, n); an id with higher=0 sorts before all of them.
    let before_min = EntryKey::from_id(&Id::new(0, 42));
    let cursor = tree.seek(&before_min, Ordering::Backward);
    assert_eq!(
        cursor.current(),
        None,
        "no key is <= the seek key, cursor must be empty"
    );
}

// A full forward scan must never skip keys while writers touch pages.
// Rationale: NodeWriteGuard::drop bumps the node version even when nothing
// was mutated, which forces optimistic readers to retry; the cursor closure
// must be idempotent under those retries.
#[test]
fn concurrent_scan_does_not_skip_keys() {
    let _ = env_logger::try_init();
    const NUM: u64 = 2_000;
    let tree = Arc::new(TinyTree::new(&deletion_set()));
    for n in 0..NUM {
        assert!(tree.insert(&key_of(n)));
    }
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = vec![];
    for w in 0..2 {
        let tree = tree.clone();
        let stop = stop.clone();
        writers.push(thread::spawn(move || {
            let mut n = w;
            while !stop.load(AtomicOrdering::Relaxed) {
                // Re-inserting an existing key is a no-op that still write-latches
                // the leaf, so it churns node versions without changing content.
                tree.insert(&key_of(n % NUM));
                n += 1;
            }
        }));
    }
    let mut min_count = usize::MAX;
    for _ in 0..300 {
        let mut cursor = tree.seek(&min_entry_key(), Ordering::Forward);
        let mut count = 0usize;
        let mut last: Option<EntryKey> = None;
        while let Some(k) = cursor.next() {
            if let Some(prev) = &last {
                assert!(prev < &k, "scan out of order: {:?} >= {:?}", prev, k);
            }
            last = Some(k);
            count += 1;
        }
        min_count = min_count.min(count);
    }
    stop.store(true, AtomicOrdering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }
    assert_eq!(
        min_count, NUM as usize,
        "a concurrent scan skipped {} key(s)",
        NUM as usize - min_count
    );
}

// Tombstone compaction: remove_contains under the page latch must remove the
// keys physically AND drop their tombstones (docs/tla/DeletionReclaim.tla).
#[test]
fn tombstone_compaction_reclaims_set() {
    let _ = env_logger::try_init();
    let deletion = deletion_set();
    let tree = TinyTree::new(&deletion);
    const N: u64 = 40;
    for n in 0..N {
        assert!(tree.insert(&key_of(n)));
    }
    // Tombstone the even keys.
    for n in (0..N).step_by(2) {
        assert!(deletion.insert(key_of(n)));
    }
    // Compact every leaf page under its write latch, walking the chain.
    let mut node_ref = tree.get_root();
    loop {
        let next = match &*read_unchecked::<TinyKeySlice, TinyPtrSlice>(&node_ref) {
            &NodeData::Internal(ref n) => n.ptrs.as_slice_immute()[0].clone(),
            &NodeData::External(_) => break,
            _ => unreachable!(),
        };
        node_ref = next;
    }
    let mut page_ref = node_ref;
    while !page_ref.is_default() {
        let mut guard = write_node::<TinyKeySlice, TinyPtrSlice>(&page_ref);
        let next = guard.extnode_mut_no_persist().next.clone();
        guard
            .extnode_mut_no_persist()
            .remove_contains(&deletion);
        drop(guard);
        page_ref = next;
    }
    // Tombstones are gone from the set.
    for n in (0..N).step_by(2) {
        assert!(
            !deletion.contains(&key_of(n)),
            "tombstone for {} must be reclaimed",
            n
        );
    }
    // Scan yields exactly the odd keys.
    let mut cursor = tree.seek(&min_entry_key(), Ordering::Forward);
    let mut seen = vec![];
    while let Some(k) = cursor.next() {
        seen.push(k);
    }
    let expected: Vec<_> = (1..N).step_by(2).map(key_of).collect();
    assert_eq!(seen, expected);
    // Reclaimed keys can be re-inserted and become visible again.
    assert!(tree.insert(&key_of(0)));
    let cursor = tree.seek(&key_of(0), Ordering::Forward);
    assert_eq!(cursor.current(), Some(&key_of(0)));
}

// The bulk-merge path calls remove_contains too; tombstones must be dropped.
#[test]
fn merge_compaction_drops_tombstones() {
    let _ = env_logger::try_init();
    let deletion = deletion_set();
    let tree = TinyTree::new(&deletion);
    for n in [0u64, 2, 4, 6, 8] {
        assert!(tree.insert(&key_of(n)));
    }
    assert!(deletion.insert(key_of(2)));
    assert!(deletion.insert(key_of(6)));
    tree.merge_with_keys_(vec![key_of(1)]);
    assert!(!deletion.contains(&key_of(2)));
    assert!(!deletion.contains(&key_of(6)));
    let mut cursor = tree.seek(&min_entry_key(), Ordering::Forward);
    let mut seen = vec![];
    while let Some(k) = cursor.next() {
        seen.push(k);
    }
    let expected: Vec<_> = [0u64, 1, 4, 8].iter().map(|&n| key_of(n)).collect();
    assert_eq!(seen, expected);
    assert!(verification::is_tree_in_order(&tree, 0));
}

// Structural split: split_off must partition the tree exactly — source keeps
// keys < pivot, the returned tree gets keys >= pivot, both valid and in order,
// union == original, disjoint. Modeled in docs/tla/StructuralSplit.tla.
#[test]
fn structural_split_off_partitions_exactly() {
    use super::split_off::split_off;
    let _ = env_logger::try_init();
    // Cover clean-cut pivots (page boundaries) and mid-leaf pivots across a
    // multi-level tree.
    for &n in &[1u64, 2, 5, 6, 50, 200, 1000] {
        for pivot_n in [0u64, 1, n / 3, n / 2, n.saturating_sub(1), n] {
            let deletion = deletion_set();
            let tree = TinyTree::new(&deletion);
            for i in 0..n {
                assert!(tree.insert(&key_of(i * 2))); // even keys 0,2,4,...
            }
            let pivot = key_of(pivot_n * 2); // may be a real key or a gap
            let orig_len = tree.len();
            let res = split_off(&tree, &pivot);

            // Collect the source's remaining keys.
            let mut src_keys = vec![];
            let mut c = tree.seek(&min_entry_key(), Ordering::Forward);
            while let Some(k) = c.next() {
                src_keys.push(k);
            }
            assert!(
                verification::is_tree_in_order(&tree, 0),
                "source not in order after split n={} pivot={}",
                n,
                pivot_n
            );
            assert!(
                src_keys.iter().all(|k| k < &pivot),
                "source kept a key >= pivot (n={}, pivot={})",
                n,
                pivot_n
            );

            let mut moved_keys = vec![];
            if let Some(so) = res {
                let new_tree = TinyTree::from_root(
                    so.new_root,
                    so.new_head_id,
                    so.moved_len,
                    so.new_height,
                    &deletion,
                );
                assert!(
                    verification::is_tree_in_order(&new_tree, 0),
                    "moved tree not in order (n={}, pivot={})",
                    n,
                    pivot_n
                );
                let mut c = new_tree.seek(&min_entry_key(), Ordering::Forward);
                while let Some(k) = c.next() {
                    moved_keys.push(k);
                }
                assert!(
                    moved_keys.iter().all(|k| k >= &pivot),
                    "moved tree has a key < pivot (n={}, pivot={})",
                    n,
                    pivot_n
                );
                assert_eq!(
                    moved_keys.len(),
                    so.moved_len,
                    "moved_len mismatch (n={}, pivot={})",
                    n,
                    pivot_n
                );
            }

            // Union is the original set, disjoint, no loss.
            assert_eq!(
                src_keys.len() + moved_keys.len(),
                orig_len,
                "key count changed: {} + {} != {} (n={}, pivot={})",
                src_keys.len(),
                moved_keys.len(),
                orig_len,
                n,
                pivot_n
            );
            let mut all: Vec<_> = src_keys.into_iter().chain(moved_keys).collect();
            all.sort();
            let expected: Vec<_> = (0..n).map(|i| key_of(i * 2)).collect();
            assert_eq!(all, expected, "union != original (n={}, pivot={})", n, pivot_n);
        }
    }
}

// Randomized structural split: random key sets and random pivots (including
// values not present as keys), many rounds, against a Vec model.
#[test]
fn structural_split_off_randomized() {
    use super::split_off::split_off;
    use rand::prelude::*;
    let _ = env_logger::try_init();
    let mut rng = rand::rng();
    for _round in 0..300 {
        let n = rng.random_range(0..400u64);
        let deletion = deletion_set();
        let tree = TinyTree::new(&deletion);
        let mut model: Vec<u64> = Vec::new();
        for _ in 0..n {
            let k = rng.random_range(0..2000u64);
            if !model.contains(&k) {
                model.push(k);
                assert!(tree.insert(&key_of(k)));
            }
        }
        model.sort();
        // Pivot: a value in [0, 2001), often between keys.
        let pivot_v = rng.random_range(0..2001u64);
        let pivot = key_of(pivot_v);
        let res = split_off(&tree, &pivot);

        let mut src: Vec<u64> = vec![];
        let mut c = tree.seek(&min_entry_key(), Ordering::Forward);
        while let Some(k) = c.next() {
            src.push(k.id().lower);
        }
        assert!(verification::is_tree_in_order(&tree, 0), "src not in order");
        let mut moved: Vec<u64> = vec![];
        if let Some(so) = res {
            let nt = TinyTree::from_root(so.new_root, so.new_head_id, so.moved_len, so.new_height, &deletion);
            assert!(verification::is_tree_in_order(&nt, 0), "moved not in order");
            let mut c = nt.seek(&min_entry_key(), Ordering::Forward);
            while let Some(k) = c.next() {
                moved.push(k.id().lower);
            }
            assert_eq!(moved.len(), so.moved_len, "moved_len");
        }
        let exp_src: Vec<u64> = model.iter().cloned().filter(|&k| key_of(k) < pivot).collect();
        let exp_moved: Vec<u64> = model.iter().cloned().filter(|&k| key_of(k) >= pivot).collect();
        assert_eq!(src, exp_src, "source partition wrong (n={}, pivot={})", n, pivot_v);
        assert_eq!(moved, exp_moved, "moved partition wrong (n={}, pivot={})", n, pivot_v);
    }
}
