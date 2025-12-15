# Memory Status API

This document describes how to use the memory status functionality to monitor your Nebuchadnezzar server's memory usage.

## Overview

The memory status API provides comprehensive information about:
- Total number of chunks
- Hot and cold segments for each chunk
- Memory usage breakdown (hot vs cold)
- Total cell count across all chunks
- Physical memory limit configuration

This is especially useful when debugging memory issues, monitoring memory pressure, or understanding how the tiered memory system is performing.

## Quick Start

```rust
use nebuchadnezzar::server::NebServer;

// Get memory status from your server instance
let status = server.memory_status();

// Print a human-readable summary
status.print_summary();
```

## Example Output

```
╔════════════════════════════════════════════════════════════════╗
║           Nebuchadnezzar Memory Status Report                 ║
╚════════════════════════════════════════════════════════════════╝

📊 Overall Statistics:
  • Total Chunks:        4
  • Total Cells:         1000000
  • Total Segments:      100 (Hot: 80, Cold: 20)

💾 Memory Usage:
  • Hot Memory:          640.00 MB
  • Cold Memory:         160.00 MB
  • Total Memory:        800.00 MB
  • Physical Limit:      512.00 MB
  • Limit Usage:         125.00%
  ⚠️  WARNING: Hot memory exceeds configured physical limit!

🔧 Configuration:
  • Tiered Memory:       Enabled

📋 Per-Chunk Details:
  ┌────────┬────────┬──────┬──────────┬─────────────┬──────────────┬───────────┐
  │ Chunk  │  Hot   │ Cold │  Total   │  Hot Memory │  Cold Memory │   Cells   │
  │   ID   │  Segs  │ Segs │   Segs   │             │              │           │
  ├────────┼────────┼──────┼──────────┼─────────────┼──────────────┼───────────┤
  │      0 │     20 │    5 │       25 │   160.00 MB │     40.00 MB │    250000 │
  │      1 │     20 │    5 │       25 │   160.00 MB │     40.00 MB │    250000 │
  │      2 │     20 │    5 │       25 │   160.00 MB │     40.00 MB │    250000 │
  │      3 │     20 │    5 │       25 │   160.00 MB │     40.00 MB │    250000 │
  └────────┴────────┴──────┴──────────┴─────────────┴──────────────┴───────────┘
```

## API Reference

### `NebServer::memory_status()`

Returns a `ServerMemoryStatus` struct containing comprehensive memory statistics.

**Returns:** `ServerMemoryStatus`

### `ServerMemoryStatus`

Main status structure containing overall statistics.

**Fields:**
- `total_chunks: usize` - Number of chunks in the server
- `chunk_details: Vec<ChunkMemoryStatus>` - Per-chunk statistics
- `total_hot_segments: usize` - Total hot segments across all chunks
- `total_cold_segments: usize` - Total cold segments across all chunks
- `total_segments: usize` - Total segments across all chunks
- `total_hot_memory_bytes: usize` - Total hot memory in bytes
- `total_cold_memory_bytes: usize` - Total cold memory in bytes
- `total_memory_bytes: usize` - Total memory allocated in bytes
- `total_cells: usize` - Total cells stored
- `physical_memory_limit_bytes: Option<usize>` - Configured physical memory limit (if tiered memory is enabled)
- `tiered_memory_enabled: bool` - Whether tiered memory is enabled

**Methods:**
- `print_summary(&self)` - Print a human-readable summary to stdout

### `ChunkMemoryStatus`

Per-chunk memory statistics.

**Fields:**
- `chunk_id: usize` - Chunk identifier
- `hot_segments: usize` - Number of hot segments
- `cold_segments: usize` - Number of cold segments
- `total_segments: usize` - Total segments
- `hot_memory_bytes: usize` - Hot memory in bytes
- `cold_memory_bytes: usize` - Cold memory in bytes
- `total_memory_bytes: usize` - Total memory in bytes
- `cell_count: usize` - Number of cells in this chunk

## Programmatic Usage

### Accessing Fields

```rust
let status = server.memory_status();

// Check if memory usage is too high
if let Some(limit) = status.physical_memory_limit_bytes {
    let usage_pct = (status.total_hot_memory_bytes as f64 / limit as f64) * 100.0;
    if usage_pct > 80.0 {
        eprintln!("Warning: Hot memory usage at {:.1}%", usage_pct);
    }
}

// Find chunks with many cold segments
for chunk in &status.chunk_details {
    if chunk.cold_segments > 10 {
        println!("Chunk {} has {} cold segments", 
                 chunk.chunk_id, chunk.cold_segments);
    }
}
```

### JSON Serialization

Both `ServerMemoryStatus` and `ChunkMemoryStatus` implement `Serialize` and `Deserialize`, so you can easily export the data:

```rust
use serde_json;

let status = server.memory_status();
let json = serde_json::to_string_pretty(&status)?;
println!("{}", json);
```

## Use Cases

### 1. Debugging Memory Issues

If your application is using more memory than expected (like the 461GB issue mentioned), you can use this API to:

```rust
let status = server.memory_status();
println!("Total memory allocated: {} GB", 
         status.total_memory_bytes / (1024 * 1024 * 1024));
println!("Hot segments: {}", status.total_hot_segments);
println!("Cold segments: {}", status.total_cold_segments);

// Check which chunks are using the most memory
let mut sorted_chunks = status.chunk_details.clone();
sorted_chunks.sort_by_key(|c| std::cmp::Reverse(c.total_memory_bytes));

println!("\nTop 5 chunks by memory usage:");
for chunk in sorted_chunks.iter().take(5) {
    println!("  Chunk {}: {} MB ({} segments, {} cells)",
             chunk.chunk_id,
             chunk.total_memory_bytes / (1024 * 1024),
             chunk.total_segments,
             chunk.cell_count);
}
```

### 2. Monitoring Memory Pressure

```rust
let status = server.memory_status();

if let Some(limit) = status.physical_memory_limit_bytes {
    let hot_memory_mb = status.total_hot_memory_bytes / (1024 * 1024);
    let limit_mb = limit / (1024 * 1024);
    let usage_pct = (status.total_hot_memory_bytes as f64 / limit as f64) * 100.0;
    
    println!("Memory pressure: {} / {} MB ({:.1}%)",
             hot_memory_mb, limit_mb, usage_pct);
    
    if usage_pct > 90.0 {
        println!("Critical: Memory pressure is very high!");
    } else if usage_pct > 80.0 {
        println!("Warning: Memory pressure is high");
    }
}
```

### 3. Verifying Tiered Memory Configuration

```rust
let status = server.memory_status();

if status.tiered_memory_enabled {
    println!("Tiered memory is enabled");
    if let Some(limit) = status.physical_memory_limit_bytes {
        println!("Physical limit: {} GB", limit / (1024 * 1024 * 1024));
    }
    
    // Check if eviction is working
    if status.total_cold_segments > 0 {
        let eviction_rate = (status.total_cold_segments as f64 / 
                            status.total_segments as f64) * 100.0;
        println!("Eviction working: {:.1}% of segments are cold", eviction_rate);
    } else {
        println!("No segments have been evicted yet");
    }
} else {
    println!("Tiered memory is disabled");
}
```

## Integration with Monitoring Systems

You can integrate this with monitoring systems like Prometheus:

```rust
use prometheus::{Gauge, register_gauge};

let hot_memory_gauge = register_gauge!(
    "neb_hot_memory_bytes", 
    "Hot memory usage in bytes"
)?;

let cold_memory_gauge = register_gauge!(
    "neb_cold_memory_bytes",
    "Cold memory usage in bytes"
)?;

// Update metrics periodically
loop {
    let status = server.memory_status();
    hot_memory_gauge.set(status.total_hot_memory_bytes as f64);
    cold_memory_gauge.set(status.total_cold_memory_bytes as f64);
    
    tokio::time::sleep(Duration::from_secs(10)).await;
}
```

## Troubleshooting

### High Memory Usage

If `total_memory_bytes` is much higher than expected:

1. Check `total_segments` - each segment is 8MB
2. Look at per-chunk breakdown to find outliers
3. Check if garbage collection is running (`cell_count` vs `total_segments`)
4. Verify your physical memory limit is configured correctly

### Memory Limit Not Working

If `total_hot_memory_bytes` exceeds `physical_memory_limit_bytes`:

1. Verify tiered memory is enabled (`tiered_memory_enabled: true`)
2. Check if segments are being evicted (`total_cold_segments > 0`)
3. Look for log messages about eviction failures
4. Ensure the limit is divided correctly among chunks (see OOM_FIX.md)

## See Also

- [TIERED_MEMORY.md](TIERED_MEMORY.md) - Tiered memory system documentation
- [OOM_FIX.md](OOM_FIX.md) - Memory limit bug fixes
- [examples/memory_status.rs](examples/memory_status.rs) - Complete example

