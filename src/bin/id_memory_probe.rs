//! Measures storage memory per cell for id-heavy payloads.
//!
//! Populates N cells whose payload carries `refs` cell-id references
//! (the graph-adjacency shape the compact-id design targets) and
//! reports process RSS growth per cell. Identical source builds
//! against both the 128-bit and 64-bit id trees for comparison.

use dovahkiin::types::{Map, OwnedMap, OwnedPrimArray, OwnedValue};
use neb::ram::cell::OwnedCell;
use neb::ram::chunk::Chunks;
use neb::ram::schema::{Field, Schema};
use neb::ram::types::{Id, RandValue, Type};

fn rss_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .expect("parse VmRSS");
        }
    }
    panic!("VmRSS not found");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cells: usize = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(200_000);
    let refs: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(8);

    let fields = Field::new_schema(vec![
        Field::new_unindexed("weight", Type::U64),
        Field::new_unindexed_array("refs", Type::Id),
    ]);
    let schema = Schema::new_with_id(7, &String::from("id_probe"), None, fields, false, false);

    let chunks = Chunks::new_dummy(4, 512 * 1024 * 1024);
    for chunk in &chunks.list {
        chunk.meta.schemas.register_internal_schema(schema.clone());
    }

    // Warm allocators/lazy statics with a small prelude, then measure.
    for _ in 0..1_000 {
        let mut map = OwnedMap::new();
        map.insert("weight", OwnedValue::U64(1));
        map.insert(
            "refs",
            OwnedValue::PrimArray(OwnedPrimArray::Id(
                (0..refs).map(|_| Id::rand()).collect(),
            )),
        );
        let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(map));
        chunks.write_cell(&mut cell).expect("prelude write");
    }

    let rss_before = rss_kib();
    for i in 0..cells {
        let mut map = OwnedMap::new();
        map.insert("weight", OwnedValue::U64(i as u64));
        map.insert(
            "refs",
            OwnedValue::PrimArray(OwnedPrimArray::Id(
                (0..refs).map(|_| Id::rand()).collect(),
            )),
        );
        let mut cell = OwnedCell::new_with_id(schema.id, &Id::rand(), OwnedValue::Map(map));
        chunks.write_cell(&mut cell).expect("probe write");
    }
    let rss_after = rss_kib();

    let delta_bytes = (rss_after.saturating_sub(rss_before)) * 1024;
    println!(
        "cells={} refs_per_cell={} id_size={}B rss_before_kib={} rss_after_kib={} bytes_per_cell={:.1}",
        cells,
        refs,
        std::mem::size_of::<Id>(),
        rss_before,
        rss_after,
        delta_bytes as f64 / cells as f64
    );
}
