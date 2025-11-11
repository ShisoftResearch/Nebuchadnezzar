# How to Check Memory Usage

This guide shows you how to add memory status checking to your Nebuchadnezzar application.

## Quick Start: Add to Your Application

### Option 1: Print Summary Periodically

Add this to your server initialization code:

```rust
use std::time::Duration;
use tokio::time;

// After creating your NebServer
let server_clone = server.clone();
tokio::spawn(async move {
    let mut interval = time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        
        let status = server_clone.memory_status();
        status.print_summary();
    }
});
```

This will print a status report every 60 seconds to your console.

### Option 2: Check for Memory Issues

Add memory monitoring with alerts:

```rust
use std::time::Duration;
use tokio::time;

let server_clone = server.clone();
tokio::spawn(async move {
    let mut interval = time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        
        let status = server_clone.memory_status();
        
        // Check if memory usage is too high
        if let Some(limit) = status.physical_memory_limit_bytes {
            let usage_pct = (status.total_hot_memory_bytes as f64 / limit as f64) * 100.0;
            
            if usage_pct > 90.0 {
                error!("🔴 CRITICAL: Memory usage at {:.1}% of limit!", usage_pct);
                error!("Hot memory: {} MB / {} MB",
                       status.total_hot_memory_bytes / (1024*1024),
                       limit / (1024*1024));
            } else if usage_pct > 80.0 {
                warn!("⚠️  WARNING: Memory usage at {:.1}% of limit", usage_pct);
            } else {
                info!("✓ Memory usage OK: {:.1}%", usage_pct);
            }
        }
        
        // Check if eviction is working (if using tiered memory)
        if status.tiered_memory_enabled {
            if status.total_cold_segments == 0 && status.total_segments > 10 {
                warn!("⚠️  No segments have been evicted despite having {} segments", 
                      status.total_segments);
            } else if status.total_cold_segments > 0 {
                info!("✓ Eviction working: {} cold segments", status.total_cold_segments);
            }
        }
    }
});
```

### Option 3: Expose via HTTP Endpoint

If you have an HTTP server, expose memory status via REST API:

```rust
use axum::{Router, Json};
use axum::routing::get;

async fn memory_status_handler(
    server: Arc<NebServer>,
) -> Json<ServerMemoryStatus> {
    Json(server.memory_status())
}

let app = Router::new()
    .route("/api/status/memory", get(memory_status_handler))
    .with_state(server.clone());
```

Then check it with:
```bash
curl http://localhost:8080/api/status/memory | jq
```

### Option 4: Log to File

Write status to a log file for later analysis:

```rust
use std::fs::OpenOptions;
use std::io::Write;
use chrono::Local;

let server_clone = server.clone();
tokio::spawn(async move {
    let mut interval = time::interval(Duration::from_secs(300)); // Every 5 minutes
    loop {
        interval.tick().await;
        
        let status = server_clone.memory_status();
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
        
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("memory_status.log")
            .unwrap();
        
        writeln!(file, "[{}] Total: {} MB, Hot: {} MB, Cold: {} MB, Segments: {} (H:{} C:{})",
                 timestamp,
                 status.total_memory_bytes / (1024*1024),
                 status.total_hot_memory_bytes / (1024*1024),
                 status.total_cold_memory_bytes / (1024*1024),
                 status.total_segments,
                 status.total_hot_segments,
                 status.total_cold_segments).unwrap();
    }
});
```

## Investigating Your 461GB Issue

Given your specific problem (memory growing to 461GB despite an 8GB limit), add this diagnostic code:

```rust
use std::time::Duration;
use tokio::time;

let server_clone = server.clone();
tokio::spawn(async move {
    let mut interval = time::interval(Duration::from_secs(10)); // Check frequently
    let mut last_total = 0;
    
    loop {
        interval.tick().await;
        
        let status = server_clone.memory_status();
        
        // Calculate total memory in GB
        let total_gb = status.total_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let hot_gb = status.total_hot_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let cold_gb = status.total_cold_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        
        // Log current state
        info!("Memory: Total={:.2}GB (Hot={:.2}GB, Cold={:.2}GB), Segments={} (H:{} C:{})",
              total_gb, hot_gb, cold_gb,
              status.total_segments, status.total_hot_segments, status.total_cold_segments);
        
        // Detect if memory is growing rapidly
        if status.total_memory_bytes > last_total {
            let growth = status.total_memory_bytes - last_total;
            let growth_mb = growth / (1024 * 1024);
            if growth_mb > 100 {
                warn!("🔴 Memory grew by {} MB in 10 seconds!", growth_mb);
                warn!("   Segments added: {}", 
                      (growth / 8388608)); // 8MB per segment
            }
        }
        last_total = status.total_memory_bytes;
        
        // Check if limit is configured
        if let Some(limit) = status.physical_memory_limit_bytes {
            let limit_gb = limit as f64 / (1024.0 * 1024.0 * 1024.0);
            info!("Physical limit: {:.2}GB, Usage: {:.1}%",
                  limit_gb,
                  (hot_gb / limit_gb) * 100.0);
        } else {
            error!("🔴 NO MEMORY LIMIT CONFIGURED! This is likely your problem.");
            error!("   Set NEB_TIERED_PHYSICAL_MEMORY_LIMIT or pass TieredConfig");
        }
        
        // Check for issues
        if total_gb > 10.0 {
            error!("🔴 MEMORY EXCEEDED 10GB! Current: {:.2}GB", total_gb);
            error!("   This suggests either:");
            error!("   1. Memory limit is not configured properly");
            error!("   2. Tiered memory is disabled");
            error!("   3. Eviction is not working");
            
            // Print detailed chunk breakdown
            error!("   Per-chunk breakdown:");
            for chunk in &status.chunk_details {
                error!("     Chunk {}: {} segments ({:.2}GB)",
                       chunk.chunk_id,
                       chunk.total_segments,
                       chunk.total_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
            }
        }
    }
});
```

## Expected Behavior

With an 8GB physical limit and tiered memory enabled, you should see:

1. **Initial state**: A few hot segments (bootstrap segments)
   ```
   Total: 32 MB, Hot: 32 MB, Cold: 0 MB
   ```

2. **As you add data**: Hot segments increase until reaching ~80% of limit
   ```
   Total: 6.5 GB, Hot: 6.5 GB, Cold: 0 GB (82% of 8GB limit)
   ```

3. **After eviction triggers**: Cold segments appear, hot stays under limit
   ```
   Total: 15 GB, Hot: 7.5 GB, Cold: 7.5 GB (94% of 8GB limit)
   ```

4. **Normal operation**: Hot oscillates around threshold, cold grows
   ```
   Total: 100 GB, Hot: 7.8 GB, Cold: 92.2 GB (98% of 8GB limit)
   ```

## If Memory Keeps Growing

If you see memory continuously growing past the limit without eviction:

### Check 1: Is tiered memory enabled?

```rust
let status = server.memory_status();
if !status.tiered_memory_enabled {
    eprintln!("ERROR: Tiered memory is NOT enabled!");
    eprintln!("Set NEB_TIERED_MEMORY_ENABLED=1");
}
```

### Check 2: Is the limit configured?

```rust
if status.physical_memory_limit_bytes.is_none() {
    eprintln!("ERROR: No physical memory limit configured!");
    eprintln!("Set NEB_TIERED_PHYSICAL_MEMORY_LIMIT=8589934592  # 8GB in bytes");
}
```

### Check 3: Is eviction happening?

```rust
if status.total_cold_segments == 0 && status.total_hot_memory_bytes > limit * 0.8 {
    eprintln!("ERROR: Memory is high but no eviction is happening!");
    eprintln!("Check logs for eviction errors");
}
```

### Check 4: Per-chunk limit calculation

```rust
if let Some(total_limit) = status.physical_memory_limit_bytes {
    let per_chunk_limit = total_limit / status.total_chunks;
    println!("Total limit: {} GB", total_limit / (1024*1024*1024));
    println!("Per-chunk limit: {} MB", per_chunk_limit / (1024*1024));
    println!("This should match what's in the code (see OOM_FIX.md)");
}
```

## Complete Example

Here's a complete example you can copy-paste:

```rust
use nebuchadnezzar::server::{NebServer, ServerOptions};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

#[tokio::main]
async fn main() {
    env_logger::init();
    
    let opts = ServerOptions {
        chunk_count: 4,
        total_size: 32 * 1024 * 1024 * 1024, // 32 GB virtual
        tiered_config: Some(nebuchadnezzar::ram::tiered::TieredConfig::with_memory_limit(
            8 * 1024 * 1024 * 1024, // 8 GB physical limit
        )),
        backup_storage: Some("/data/backup".to_string()),
        wal_storage: Some("/data/wal".to_string()),
        undo_log_storage: None,
        raft_storage: Some("/data/raft".to_string()),
        services: vec![],
        index_enabled: false,
        enable_recovery: false,
    };
    
    let server = NebServer::new_from_opts(
        &opts,
        "127.0.0.1:9000",
        "my_cluster",
        |_| async {},
    ).await;
    
    // Start memory monitor
    let server_clone = server.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let status = server_clone.memory_status();
            status.print_summary();
        }
    });
    
    // Your application logic here...
    
    // Keep running
    tokio::signal::ctrl_c().await.unwrap();
}
```

## See Also

- [MEMORY_STATUS.md](MEMORY_STATUS.md) - Complete API documentation
- [MEMORY_STATUS_IMPLEMENTATION.md](MEMORY_STATUS_IMPLEMENTATION.md) - Implementation details
- [OOM_FIX.md](OOM_FIX.md) - Known memory limit bugs and fixes
- [TIERED_MEMORY.md](TIERED_MEMORY.md) - How tiered memory works

