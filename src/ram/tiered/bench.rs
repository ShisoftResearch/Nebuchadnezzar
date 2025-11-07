/// Benchmarks for tiered memory system
///
/// Run with: `cargo test --lib tiered::bench -- --nocapture --ignored --test-threads=1`
///
/// Tests:
/// - Hot segment reads (baseline)
/// - Cold segment reads (with promotion overhead)
/// - Mixed workload with uniform distribution
/// - Mixed workload with Zipf distribution (realistic access pattern)
use crate::ram::cell::*;
use crate::ram::chunk::Chunks;
use crate::ram::schema::*;
use crate::ram::segs::SEGMENT_SIZE;
use crate::ram::types::*;
use crate::server::ServerMeta;
use log::info;
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn default_fields() -> Field {
    use dovahkiin::types::Type;
    Field::new_schema(vec![
        Field::new_indexed("id", Type::I64, vec![IndexType::Statistics]),
        Field::new_unindexed("name", Type::String),
        Field::new_unindexed("data", Type::String),
    ])
}

/// Simple Zipf distribution generator
///
/// Zipf distribution: probability of rank k item is proportional to 1/k^s
/// where s is the skewness parameter (typically 0.99 for realistic workloads)
struct ZipfGenerator {
    n: usize,            // number of items
    s: f64,              // skewness parameter
    sum_probs: Vec<f64>, // cumulative distribution
}

impl ZipfGenerator {
    fn new(n: usize, s: f64) -> Self {
        // Calculate normalization constant (generalized harmonic number)
        let mut sum = 0.0;
        for i in 1..=n {
            sum += 1.0 / (i as f64).powf(s);
        }

        // Build cumulative distribution
        let mut sum_probs = Vec::with_capacity(n);
        let mut cumulative = 0.0;
        for i in 1..=n {
            let prob = (1.0 / (i as f64).powf(s)) / sum;
            cumulative += prob;
            sum_probs.push(cumulative);
        }

        ZipfGenerator { n, s, sum_probs }
    }

    fn sample(&self, rng: &mut impl Rng) -> usize {
        let u: f64 = rng.gen();
        // Binary search for the rank
        match self
            .sum_probs
            .binary_search_by(|p| p.partial_cmp(&u).unwrap())
        {
            Ok(i) => i,
            Err(i) => i.min(self.n - 1),
        }
    }
}

struct BenchResult {
    name: String,
    total_ops: usize,
    duration: Duration,
    avg_latency_us: f64,
    p50_latency_us: f64,
    p95_latency_us: f64,
    p99_latency_us: f64,
    throughput_ops: f64,
}

impl BenchResult {
    fn from_latencies(name: String, latencies: Vec<Duration>) -> Self {
        let total_ops = latencies.len();
        let duration: Duration = latencies.iter().sum();

        let mut sorted_latencies = latencies.clone();
        sorted_latencies.sort();

        let avg_latency_us = duration.as_micros() as f64 / total_ops as f64;
        let p50_latency_us = sorted_latencies[total_ops * 50 / 100].as_micros() as f64;
        let p95_latency_us = sorted_latencies[total_ops * 95 / 100].as_micros() as f64;
        let p99_latency_us = sorted_latencies[total_ops * 99 / 100].as_micros() as f64;
        let throughput_ops = total_ops as f64 / duration.as_secs_f64();

        BenchResult {
            name,
            total_ops,
            duration,
            avg_latency_us,
            p50_latency_us,
            p95_latency_us,
            p99_latency_us,
            throughput_ops,
        }
    }

    fn print(&self) {
        info!("");
        info!("╔══════════════════════════════════════════════════════════════╗");
        info!("║ Benchmark: {:<50} ║", self.name);
        info!("╠══════════════════════════════════════════════════════════════╣");
        info!(
            "║ Total Operations:    {:>10}                             ║",
            self.total_ops
        );
        info!(
            "║ Total Duration:      {:>10.2} s                           ║",
            self.duration.as_secs_f64()
        );
        info!(
            "║ Throughput:          {:>10.2} ops/s                       ║",
            self.throughput_ops
        );
        info!("╠══════════════════════════════════════════════════════════════╣");
        info!("║ Latency Statistics:                                          ║");
        info!(
            "║   Average:           {:>10.2} μs                          ║",
            self.avg_latency_us
        );
        info!(
            "║   p50 (median):      {:>10.2} μs                          ║",
            self.p50_latency_us
        );
        info!(
            "║   p95:               {:>10.2} μs                          ║",
            self.p95_latency_us
        );
        info!(
            "║   p99:               {:>10.2} μs                          ║",
            self.p99_latency_us
        );
        info!("╚══════════════════════════════════════════════════════════════╝");
        info!("");
    }
}

/// Benchmark: Reading from hot segments only (baseline)
#[test]
#[ignore] // Run with --ignored
fn bench_hot_segment_reads() {
    let _ = env_logger::try_init();

    info!("=== Benchmark: Hot Segment Reads (Baseline) ===");

    let backup_dir = "/tmp/neb_bench_hot";
    let wal_dir = "/tmp/neb_bench_hot_wal";
    let schema_dir = "/tmp/neb_bench_hot_schema";

    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);

    // No tiered memory - all segments stay hot
    let chunk_capacity = 20 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("bench_hot", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local(schema_dir);
    schemas.new_schema(schema.clone());

    let chunks = Chunks::new(
        1,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        Some(crate::ram::tiered::TieredConfig::with_memory_limit(
            chunk_capacity / 2,
        )),
    );

    // Create 3 segments worth of data
    let large_data = "x".repeat(1024);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    let num_cells = cells_per_segment * 3;

    info!("Writing {} cells ({} segments)...", num_cells, 3);

    for i in 0..num_cells {
        let id = Id::new(schema.id as u64, 1000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(1000 + i as i64));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(format!("hot_{}", i)),
        );
        data_map.insert(
            &String::from("data"),
            OwnedValue::String(large_data.clone()),
        );

        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };

        let _ = chunks.write_cell(&mut cell);
    }

    info!("Created {} hot segments", chunks.list[0].segments().len());

    // Warm up
    for i in 0..(num_cells / 10) {
        let id = Id::new(schema.id as u64, 1000 + i as u64);
        let _ = chunks.read_cell(&id);
    }

    // Benchmark: Sequential reads
    let mut latencies = Vec::new();
    let num_reads = 100000;

    info!("Running {} sequential reads...", num_reads);

    for i in 0..num_reads {
        let cell_idx = i % num_cells;
        let id = Id::new(schema.id as u64, 1000 + cell_idx as u64);

        let start = Instant::now();
        let _ = chunks.read_cell(&id);
        let elapsed = start.elapsed();

        latencies.push(elapsed);
    }

    let result = BenchResult::from_latencies("Hot Segment Reads".to_string(), latencies);
    result.print();

    // Clean up
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
}

/// Benchmark: Reading from cold segments (pure mmap reads, no promotion)
#[test]
#[ignore] // Run with --ignored
fn bench_cold_segment_reads() {
    let _ = env_logger::try_init();

    info!("=== Benchmark: Cold Segment Reads (mmap, no promotion) ===");

    let backup_dir = "/tmp/neb_bench_cold";
    let wal_dir = "/tmp/neb_bench_cold_wal";
    let schema_dir = "/tmp/neb_bench_cold_schema";

    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);

    // Enable tiered memory with tight limit to force cold segments
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.5");
    std::env::set_var(
        "NEB_TIERED_PHYSICAL_MEMORY_LIMIT",
        &format!("{}", 1 * SEGMENT_SIZE),
    );

    let chunk_capacity = 20 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("bench_cold", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local(schema_dir);
    schemas.new_schema(schema.clone());

    let chunks = Chunks::new(
        1,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        Some(crate::ram::tiered::TieredConfig::with_memory_limit(
            chunk_capacity / 2,
        )),
    );

    // Create 5 segments worth of data (most will be cold)
    let large_data = "x".repeat(1024);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    let num_cells = cells_per_segment * 5;

    info!("Writing {} cells ({} segments)...", num_cells, 5);

    for i in 0..num_cells {
        let id = Id::new(schema.id as u64, 2000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(2000 + i as i64));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(format!("cold_{}", i)),
        );
        data_map.insert(
            &String::from("data"),
            OwnedValue::String(large_data.clone()),
        );

        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };

        let _ = chunks.write_cell(&mut cell);

        // Trigger eviction periodically
        if i > 0 && i % (cells_per_segment / 2) == 0 {
            for chunk in &chunks.list {
                if let Some(ref manager) = chunk.tiered_manager {
                    let _ = manager.check_and_evict(chunk);
                }
            }
        }
    }

    let cold_count = chunks.list[0]
        .segments()
        .iter()
        .filter(|s| s.is_cold())
        .count();
    info!(
        "Created {} cold segments (out of {})",
        cold_count,
        chunks.list[0].segments().len()
    );

    // Disable promotion to measure pure cold segment read performance
    // (reads from mmap'd files without copying to anonymous memory)
    for chunk in &chunks.list {
        if let Some(ref manager) = chunk.tiered_manager {
            manager
                .disable_promotion
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }
    info!("Disabled promotion for pure cold segment read benchmark");

    // Benchmark: Reading from cold segments (WITHOUT promotion)
    let mut latencies = Vec::new();
    let num_reads = 100000;

    info!(
        "Running {} reads from cold segments (mmap reads, no promotion)...",
        num_reads
    );

    let mut rng = rand::thread_rng();
    for _ in 0..num_reads {
        let cell_idx = rng.gen_range(0..num_cells);
        let id = Id::new(schema.id as u64, 2000 + cell_idx as u64);

        let start = Instant::now();
        let _ = chunks.read_cell(&id);
        let elapsed = start.elapsed();

        latencies.push(elapsed);
    }

    let result = BenchResult::from_latencies(
        "Cold Segment Reads (mmap, no promotion)".to_string(),
        latencies,
    );
    result.print();

    let final_cold = chunks.list[0]
        .segments()
        .iter()
        .filter(|s| s.is_cold())
        .count();
    info!(
        "After benchmark: {} segments remain cold (no promotion occurred)",
        final_cold
    );

    // Clean up
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
}

/// Benchmark: Mixed workload with uniform distribution
#[test]
#[ignore] // Run with --ignored
fn bench_mixed_uniform() {
    let _ = env_logger::try_init();

    info!("=== Benchmark: Mixed Workload (Uniform Distribution) ===");

    let backup_dir = "/tmp/neb_bench_mixed_uniform";
    let wal_dir = "/tmp/neb_bench_mixed_uniform_wal";
    let schema_dir = "/tmp/neb_bench_mixed_uniform_schema";

    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);

    // Enable tiered memory with moderate limit
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.8");
    std::env::set_var(
        "NEB_TIERED_PHYSICAL_MEMORY_LIMIT",
        &format!("{}", 2 * SEGMENT_SIZE),
    );

    let chunk_capacity = 20 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("bench_mixed_uniform", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local(schema_dir);
    schemas.new_schema(schema.clone());

    let chunks = Chunks::new(
        1,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        Some(crate::ram::tiered::TieredConfig::with_memory_limit(
            chunk_capacity / 2,
        )),
    );

    // Create 5 segments worth of data
    let large_data = "x".repeat(1024);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    let num_cells = cells_per_segment * 5;

    info!("Writing {} cells ({} segments)...", num_cells, 5);

    for i in 0..num_cells {
        let id = Id::new(schema.id as u64, 3000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(3000 + i as i64));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(format!("mixed_{}", i)),
        );
        data_map.insert(
            &String::from("data"),
            OwnedValue::String(large_data.clone()),
        );

        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };

        let _ = chunks.write_cell(&mut cell);

        // Trigger eviction periodically
        if i > 0 && i % (cells_per_segment / 2) == 0 {
            for chunk in &chunks.list {
                if let Some(ref manager) = chunk.tiered_manager {
                    let _ = manager.check_and_evict(chunk);
                }
            }
        }
    }

    let hot_count = chunks.list[0]
        .segments()
        .iter()
        .filter(|s| s.is_hot())
        .count();
    let cold_count = chunks.list[0]
        .segments()
        .iter()
        .filter(|s| s.is_cold())
        .count();
    info!(
        "Created {} hot segments, {} cold segments",
        hot_count, cold_count
    );

    // Benchmark: Uniform random access
    let mut latencies = Vec::new();
    let num_reads = 100000;

    info!("Running {} uniform random reads...", num_reads);

    let mut rng = rand::thread_rng();
    for _ in 0..num_reads {
        let cell_idx = rng.gen_range(0..num_cells);
        let id = Id::new(schema.id as u64, 3000 + cell_idx as u64);

        let start = Instant::now();
        let _ = chunks.read_cell(&id);
        let elapsed = start.elapsed();

        latencies.push(elapsed);
    }

    let result = BenchResult::from_latencies("Mixed Uniform".to_string(), latencies);
    result.print();

    let final_hot = chunks.list[0]
        .segments()
        .iter()
        .filter(|s| s.is_hot())
        .count();
    let final_cold = chunks.list[0]
        .segments()
        .iter()
        .filter(|s| s.is_cold())
        .count();
    info!(
        "After benchmark: {} hot segments, {} cold segments",
        final_hot, final_cold
    );

    // Clean up
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
}

/// Benchmark: Mixed workload with Zipf distribution (realistic)
#[test]
#[ignore] // Run with --ignored
fn bench_mixed_zipf() {
    let _ = env_logger::try_init();

    info!("=== Benchmark: Mixed Workload (Zipf Distribution, s=0.99) ===");

    let backup_dir = "/tmp/neb_bench_mixed_zipf";
    let wal_dir = "/tmp/neb_bench_mixed_zipf_wal";
    let schema_dir = "/tmp/neb_bench_mixed_zipf_schema";

    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);

    // Enable tiered memory with moderate limit
    std::env::set_var("NEB_TIERED_MEMORY_ENABLED", "1");
    std::env::set_var("NEB_TIERED_MEMORY_THRESHOLD", "0.8");
    std::env::set_var(
        "NEB_TIERED_PHYSICAL_MEMORY_LIMIT",
        &format!("{}", 2 * SEGMENT_SIZE),
    );

    let chunk_capacity = 20 * SEGMENT_SIZE;
    let fields = default_fields();
    let schema = Schema::new("bench_mixed_zipf", None, fields, false, false);
    let schemas = LocalSchemasCache::new_local(schema_dir);
    schemas.new_schema(schema.clone());

    let chunks = Chunks::new(
        1,
        chunk_capacity,
        Arc::new(ServerMeta { schemas }),
        None,
        Some(backup_dir.to_string()),
        Some(wal_dir.to_string()),
        Some(crate::ram::tiered::TieredConfig::with_memory_limit(
            chunk_capacity / 2,
        )),
    );

    // Create 5 segments worth of data
    let large_data = "x".repeat(1024);
    let cells_per_segment = SEGMENT_SIZE / 2048;
    let num_cells = cells_per_segment * 5;

    info!("Writing {} cells ({} segments)...", num_cells, 5);

    for i in 0..num_cells {
        let id = Id::new(schema.id as u64, 4000 + i as u64);
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(4000 + i as i64));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(format!("zipf_{}", i)),
        );
        data_map.insert(
            &String::from("data"),
            OwnedValue::String(large_data.clone()),
        );

        let data = OwnedValue::Map(data_map);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.id, &id),
            data,
        };

        let _ = chunks.write_cell(&mut cell);

        // Trigger eviction periodically
        if i > 0 && i % (cells_per_segment / 2) == 0 {
            for chunk in &chunks.list {
                if let Some(ref manager) = chunk.tiered_manager {
                    let _ = manager.check_and_evict(chunk);
                }
            }
        }
    }

    let hot_count = chunks.list[0]
        .segments()
        .iter()
        .filter(|s| s.is_hot())
        .count();
    let cold_count = chunks.list[0]
        .segments()
        .iter()
        .filter(|s| s.is_cold())
        .count();
    info!(
        "Created {} hot segments, {} cold segments",
        hot_count, cold_count
    );

    // Benchmark: Zipf distributed access (s=0.99 - realistic for caching workloads)
    let zipf = ZipfGenerator::new(num_cells, 0.99);
    let mut latencies = Vec::new();
    let num_reads = 100000;

    info!("Running {} Zipf-distributed reads (s=0.99)...", num_reads);
    info!("(Zipf means: ~20% of keys get ~80% of accesses)");

    let mut rng = rand::thread_rng();
    for _ in 0..num_reads {
        let cell_idx = zipf.sample(&mut rng);
        let id = Id::new(schema.id as u64, 4000 + cell_idx as u64);

        let start = Instant::now();
        let _ = chunks.read_cell(&id);
        let elapsed = start.elapsed();

        latencies.push(elapsed);
    }

    let result = BenchResult::from_latencies("Mixed Zipf (s=0.99)".to_string(), latencies);
    result.print();

    let final_hot = chunks.list[0]
        .segments()
        .iter()
        .filter(|s| s.is_hot())
        .count();
    let final_cold = chunks.list[0]
        .segments()
        .iter()
        .filter(|s| s.is_cold())
        .count();
    info!(
        "After benchmark: {} hot segments, {} cold segments",
        final_hot, final_cold
    );
    info!("(Hot segments should contain frequently accessed data)");

    // Clean up
    std::env::remove_var("NEB_TIERED_MEMORY_ENABLED");
    std::env::remove_var("NEB_TIERED_MEMORY_THRESHOLD");
    std::env::remove_var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT");
    let _ = std::fs::remove_dir_all(backup_dir);
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(schema_dir);
}

/// Run all benchmarks in sequence
#[test]
#[ignore] // Run with --ignored
fn bench_all() {
    let _ = env_logger::try_init();

    info!("");
    info!("╔══════════════════════════════════════════════════════════════╗");
    info!("║               TIERED MEMORY BENCHMARK SUITE                  ║");
    info!("╚══════════════════════════════════════════════════════════════╝");
    info!("");

    bench_hot_segment_reads();
    bench_cold_segment_reads();
    bench_mixed_uniform();
    bench_mixed_zipf();

    info!("╔══════════════════════════════════════════════════════════════╗");
    info!("║                  ALL BENCHMARKS COMPLETE                     ║");
    info!("╚══════════════════════════════════════════════════════════════╝");
}
