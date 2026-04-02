// Example: How to use the memory status functionality
//
// Run with: cargo run --example memory_status
//
// This example demonstrates how to get and display memory statistics
// from a NebServer instance, showing chunk allocation, hot/cold segments,
// and overall memory usage.

use neb::server::{NebServer, ServerOptions, Service};

#[tokio::main]
async fn main() {
    // Initialize logger
    env_logger::init();

    // Configure server options
    let opts = ServerOptions {
        chunk_size: 256 * 1024 * 1024, // 256 MB per chunk
        db_size: 1024 * 1024 * 1024,   // 1 GB total
        tiered_config: Some(neb::ram::tiered::TieredConfig::with_memory_limit(
            512 * 1024 * 1024, // 512 MB physical limit
        )),
        backup_storage: None,
        wal_storage: None,
        undo_log_storage: None,
        raft_storage: None,
        services: vec![Service::Cell],
        index_enabled: false,
        enable_recovery: false,
    };

    // Create server instance
    let server =
        NebServer::new_from_opts(&opts, "127.0.0.1:9000", "test_group", async |_| {}).await;

    // Get memory status
    let status = server.memory_status();

    // Print summary in human-readable format
    status.print_summary();

    // Access individual fields programmatically
    println!("\n📌 Programmatic Access Example:");
    println!(
        "  Total memory allocated: {} bytes",
        status.total_memory_bytes
    );
    println!(
        "  Hot memory usage: {} bytes",
        status.total_hot_memory_bytes
    );
    println!(
        "  Cold memory usage: {} bytes",
        status.total_cold_memory_bytes
    );

    if let Some(limit) = status.physical_memory_limit_bytes {
        let usage_percentage = (status.total_hot_memory_bytes as f64 / limit as f64) * 100.0;
        println!("  Memory limit: {} bytes", limit);
        println!("  Usage: {:.2}%", usage_percentage);

        if usage_percentage > 80.0 {
            println!("  ⚠️  WARNING: Memory usage is high!");
        }
    }

    // Access per-chunk details
    println!("\n📌 Per-Chunk Analysis:");
    for chunk_status in &status.chunk_details {
        if chunk_status.hot_segments > 0 {
            println!(
                "  Chunk {}: {} hot segments using {} bytes",
                chunk_status.chunk_id, chunk_status.hot_segments, chunk_status.hot_memory_bytes
            );
        }
    }

    // JSON serialization example
    println!("\n📌 JSON Serialization Example:");
    if let Ok(json) = serde_json::to_string_pretty(&status) {
        println!("{}", json);
    }
}
