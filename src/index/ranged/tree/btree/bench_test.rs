// Benchmarks for the B+ tree, written as ignored tests so they can use the
// in-crate Slice instantiations (the production 128-key page from tree.rs).
//
// Run with:
//   cargo test --release --lib index::ranged::tree::btree::bench_test:: \
//       -- --ignored --nocapture --test-threads=1
use super::*;
use crate::index::ranged::tree::btree::level::BTREE_NODE_SIZE;
use dovahkiin::types::custom_types::id::Id;
use lightning::map::HashSet;
use rand::prelude::*;
use rayon::prelude::*;
use std::env;
use std::sync::Arc;
use std::time::Instant;

type BenchKeySlice = [EntryKey; BTREE_NODE_SIZE];
type BenchPtrSlice = [NodeCellRef; BTREE_NODE_SIZE + 1];
type BenchTree = BPlusTree<BenchKeySlice, BenchPtrSlice>;

fn deletion_set() -> Arc<DeletionSet> {
    Arc::new(HashSet::with_capacity(16))
}

fn key_of(n: u64) -> EntryKey {
    EntryKey::from_id(&Id::new(1, n))
}

fn bench_n() -> u64 {
    env::var("NEB_BENCH_N")
        .unwrap_or("1000000".to_string())
        .parse()
        .unwrap()
}

fn report(name: &str, ops: u64, start: Instant) {
    let secs = start.elapsed().as_secs_f64();
    println!(
        "BENCH {:24} {:>10} ops in {:>8.3}s = {:>12.0} ops/s",
        name,
        ops,
        secs,
        ops as f64 / secs
    );
}

#[test]
#[ignore]
fn bench_insert_sequential() {
    let n = bench_n();
    let tree = BenchTree::new(&deletion_set());
    let start = Instant::now();
    for i in 0..n {
        tree.insert(&key_of(i));
    }
    report("insert_sequential", n, start);
    assert_eq!(tree.len(), n as usize);
}

#[test]
#[ignore]
fn bench_insert_random() {
    let n = bench_n();
    let mut keys: Vec<u64> = (0..n).collect();
    keys.shuffle(&mut rand::rng());
    let tree = BenchTree::new(&deletion_set());
    let start = Instant::now();
    for i in &keys {
        tree.insert(&key_of(*i));
    }
    report("insert_random", n, start);
    assert_eq!(tree.len(), n as usize);
}

#[test]
#[ignore]
fn bench_insert_parallel_random() {
    let n = bench_n();
    let mut keys: Vec<u64> = (0..n).collect();
    keys.shuffle(&mut rand::rng());
    let tree = Arc::new(BenchTree::new(&deletion_set()));
    let start = Instant::now();
    keys.par_iter().for_each(|i| {
        tree.insert(&key_of(*i));
    });
    report("insert_parallel_rand", n, start);
    assert_eq!(tree.len(), n as usize);
}

#[test]
#[ignore]
fn bench_point_seek() {
    let n = bench_n();
    let tree = BenchTree::new(&deletion_set());
    for i in 0..n {
        tree.insert(&key_of(i));
    }
    let mut order: Vec<u64> = (0..n).collect();
    order.shuffle(&mut rand::rng());
    let start = Instant::now();
    for i in &order {
        let k = key_of(*i);
        let cursor = tree.seek(&k, Ordering::Forward);
        assert_eq!(cursor.current(), Some(&k));
    }
    report("point_seek", n, start);
}

#[test]
#[ignore]
fn bench_scan() {
    let n = bench_n();
    let tree = BenchTree::new(&deletion_set());
    for i in 0..n {
        tree.insert(&key_of(i));
    }
    const ROUNDS: u64 = 10;
    let start = Instant::now();
    for _ in 0..ROUNDS {
        let mut cursor = tree.seek(&min_entry_key(), Ordering::Forward);
        let mut count = 0u64;
        while cursor.next().is_some() {
            count += 1;
        }
        assert_eq!(count, n);
    }
    report("scan_full", n * ROUNDS, start);
}
