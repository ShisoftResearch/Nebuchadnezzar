//! Read-path cost harness.
//!
//! `Chunks::read_cell` goes through `Segment::incr_references` /
//! `decr_references`, which is where QSBR's quiescent-state bookkeeping was
//! hooked. This measures that path directly, away from client, network and
//! scheduler noise, so a regression in it is attributable rather than inferred
//! from an import's wall clock.
//!
//! Not a #[test] assertion: it reports timings, and a timing threshold in CI
//! would either be too loose to catch anything or flaky. Run it explicitly:
//!
//!   cargo test --release --test read_path_bench -- --ignored --nocapture

use std::sync::Arc;
use std::time::Instant;

use dovahkiin::data_map;
use dovahkiin::data_map_value;
use dovahkiin::types::{Map, Type};
use neb::ram::cell::{CellHeader, OwnedCell};
use neb::ram::chunk::Chunks;
use neb::ram::schema::SchemaVid;
use neb::ram::schema::{Field, LocalSchemasCache, Schema};
use neb::ram::types::Id;
use neb::server::ServerMeta;

const CHUNK_SIZE: usize = 512 * 1024 * 1024;
const CELLS: usize = 20_000;
const READS_PER_THREAD: usize = 200_000;

fn setup() -> (Arc<Chunks>, Schema) {
    let fields = Field::new_schema(vec![
        Field::new_unindexed("id", Type::I32),
        Field::new_unindexed_array("data", Type::U8),
    ]);
    let schema = Schema::new("read_path_bench", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local("");
    // `debug_only_new_schema` panics in release builds, and a read-path
    // measurement is only meaningful in release.
    schemas.register_internal_schema(schema.clone());
    let chunks = Chunks::new(
        1,
        CHUNK_SIZE,
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    );
    (chunks, schema)
}

fn write_cells(chunks: &Arc<Chunks>, schema_id: u32) -> Vec<Id> {
    let mut ids = Vec::with_capacity(CELLS);
    for i in 0..CELLS {
        let id = Id::allocated(1, 0, i as u64 + 1);
        let data: Vec<u8> = std::iter::repeat(i as u8).take(64).collect();
        let mut cell = OwnedCell {
            header: CellHeader::new(SchemaVid(schema_id), &id),
            data: data_map_value!(id: i as i32, data: data),
        };
        chunks.write_cell(&mut cell).expect("write bench cell");
        ids.push(id);
    }
    ids
}

fn run(threads: usize) {
    let (chunks, schema) = setup();
    let ids = Arc::new(write_cells(&chunks, schema.vid.get()));

    // Warm the index and the pages before timing.
    for id in ids.iter().take(1000) {
        let _ = chunks.read_cell(id).expect("warmup read");
    }

    let started = Instant::now();
    std::thread::scope(|scope| {
        for t in 0..threads {
            let chunks = Arc::clone(&chunks);
            let ids = Arc::clone(&ids);
            scope.spawn(move || {
                let mut cursor = t * 7919;
                for _ in 0..READS_PER_THREAD {
                    cursor = (cursor + 1) % ids.len();
                    let cell = chunks.read_cell(&ids[cursor]).expect("bench read");
                    std::hint::black_box(&cell);
                }
            });
        }
    });
    let elapsed = started.elapsed();

    let total = threads * READS_PER_THREAD;
    let per_read = elapsed.as_nanos() as f64 / total as f64;
    println!(
        "READ_PATH threads={:>3} reads={:>9} wall={:>8.3}s ns_per_read={:>8.1} reads_per_s={:>12.0}",
        threads,
        total,
        elapsed.as_secs_f64(),
        per_read,
        total as f64 / elapsed.as_secs_f64(),
    );
}

#[test]
#[ignore = "timing harness, run explicitly with --ignored"]
fn read_path_cost() {
    // A single-threaded run is the only one whose instruction count is
    // meaningful: above one thread, `incr_references` backs off under
    // contention and the spin dominates any difference being measured.
    if let Ok(only) = std::env::var("READ_PATH_BENCH_THREADS") {
        let threads: usize = only
            .parse()
            .expect("READ_PATH_BENCH_THREADS must be a number");
        run(threads);
        return;
    }
    for threads in [1usize, 4, 16, 64] {
        run(threads);
    }
}
