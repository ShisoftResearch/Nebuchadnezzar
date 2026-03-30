use crate::ram::segs::SEGMENT_SIZE;
use crate::server::NebServer;
/// Per-chunk memory statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMemoryStatus {
    pub chunk_id: usize,
    pub hot_segments: usize,
    pub cold_segments: usize,
    pub total_segments: usize,
    pub hot_memory_bytes: usize,
    pub cold_memory_bytes: usize,
    pub total_memory_bytes: usize,
    pub cell_count: usize,
}

/// Overall server memory status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerMemoryStatus {
    pub total_chunks: usize,
    pub chunk_details: Vec<ChunkMemoryStatus>,
    pub total_hot_segments: usize,
    pub total_cold_segments: usize,
    pub total_segments: usize,
    pub total_hot_memory_bytes: usize,
    pub total_cold_memory_bytes: usize,
    pub total_memory_bytes: usize,
    pub total_cells: usize,
    pub living_transactions: usize,
    pub physical_memory_limit_bytes: Option<usize>,
    pub tiered_memory_enabled: bool,
}

impl ServerMemoryStatus {
    /// Format memory size in human-readable format (B, KB, MB, GB)
    fn format_bytes(bytes: usize) -> String {
        const KB: usize = 1024;
        const MB: usize = KB * 1024;
        const GB: usize = MB * 1024;

        if bytes >= GB {
            format!("{:.2} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }

    /// Print the status in a human-readable format
    pub fn print_summary(&self) {
        println!("\n╔════════════════════════════════════════════════════════════════╗");
        println!("║           Nebuchadnezzar Memory Status Report                 ║");
        println!("╚════════════════════════════════════════════════════════════════╝");

        println!("\n📊 Overall Statistics:");
        println!("  • Total Chunks:        {}", self.total_chunks);
        println!("  • Total Cells:         {}", self.total_cells);
        println!("  • Living Transactions: {}", self.living_transactions);
        println!(
            "  • Total Segments:      {} (Hot: {}, Cold: {})",
            self.total_segments, self.total_hot_segments, self.total_cold_segments
        );

        println!("\n💾 Memory Usage:");
        println!(
            "  • Hot Memory:          {}",
            Self::format_bytes(self.total_hot_memory_bytes)
        );
        println!(
            "  • Cold Memory:         {}",
            Self::format_bytes(self.total_cold_memory_bytes)
        );
        println!(
            "  • Total Memory:        {}",
            Self::format_bytes(self.total_memory_bytes)
        );

        if let Some(limit) = self.physical_memory_limit_bytes {
            let usage_percent = (self.total_hot_memory_bytes as f64 / limit as f64) * 100.0;
            println!("  • Physical Limit:      {}", Self::format_bytes(limit));
            println!("  • Limit Usage:         {:.2}%", usage_percent);

            if usage_percent > 100.0 {
                println!("  ⚠️  WARNING: Hot memory exceeds configured physical limit!");
            }
        } else {
            println!("  • Physical Limit:      Not configured");
        }

        println!("\n🔧 Configuration:");
        println!(
            "  • Tiered Memory:       {}",
            if self.tiered_memory_enabled {
                "Enabled"
            } else {
                "Disabled"
            }
        );

        println!("\n📋 Per-Chunk Details:");
        println!(
            "  ┌────────┬────────┬──────┬──────────┬─────────────┬──────────────┬───────────┐"
        );
        println!(
            "  │ Chunk  │  Hot   │ Cold │  Total   │  Hot Memory │  Cold Memory │   Cells   │"
        );
        println!(
            "  │   ID   │  Segs  │ Segs │   Segs   │             │              │           │"
        );
        println!(
            "  ├────────┼────────┼──────┼──────────┼─────────────┼──────────────┼───────────┤"
        );

        for chunk in &self.chunk_details {
            println!(
                "  │ {:6} │ {:6} │ {:4} │ {:8} │ {:>11} │ {:>12} │ {:>9} │",
                chunk.chunk_id,
                chunk.hot_segments,
                chunk.cold_segments,
                chunk.total_segments,
                Self::format_bytes(chunk.hot_memory_bytes),
                Self::format_bytes(chunk.cold_memory_bytes),
                chunk.cell_count
            );
        }

        println!(
            "  └────────┴────────┴──────┴──────────┴─────────────┴──────────────┴───────────┘"
        );
        println!();
    }
}

impl NebServer {
    /// Get comprehensive memory status including chunk and segment statistics
    ///
    /// This function provides detailed information about:
    /// - Total number of chunks
    /// - Hot and cold segments for each chunk
    /// - Memory usage breakdown (hot vs cold)
    /// - Total cell count across all chunks
    /// - Physical memory limit configuration
    pub fn memory_status(&self) -> ServerMemoryStatus {
        let chunks = self.chunks();
        let total_chunks = chunks.list.len();
        let mut chunk_details = Vec::with_capacity(total_chunks);

        let mut total_hot_segments = 0;
        let mut total_cold_segments = 0;
        let mut total_segments = 0;
        let mut total_cells = 0;

        // Collect statistics for each chunk
        for chunk in &chunks.list {
            let segments = chunk.segments();
            let hot_count = segments.iter().filter(|s| s.is_hot()).count();
            let cold_count = segments.iter().filter(|s| s.is_cold()).count();
            let seg_count = segments.len();
            let cell_count = chunk.cell_count();

            let hot_memory = hot_count * SEGMENT_SIZE;
            let cold_memory = cold_count * SEGMENT_SIZE;
            let total_memory = seg_count * SEGMENT_SIZE;

            chunk_details.push(ChunkMemoryStatus {
                chunk_id: chunk.id,
                hot_segments: hot_count,
                cold_segments: cold_count,
                total_segments: seg_count,
                hot_memory_bytes: hot_memory,
                cold_memory_bytes: cold_memory,
                total_memory_bytes: total_memory,
                cell_count,
            });

            total_hot_segments += hot_count;
            total_cold_segments += cold_count;
            total_segments += seg_count;
            total_cells += cell_count;
        }

        // The shared pool holds the server-wide limit directly — no per-chunk multiplication.
        let (total_physical_limit, tiered_enabled) =
            if let Some(ref manager) = self.chunks().tiered_manager {
            (
                Some(manager.shared_pool().physical_memory_limit),
                manager.is_enabled(),
            )
        } else {
            (None, false)
        };

        let living_transactions = self
            .txn_manager()
            .as_ref()
            .map(|tm| tm.transaction_count())
            .unwrap_or(0);

        ServerMemoryStatus {
            total_chunks,
            chunk_details,
            total_hot_segments,
            total_cold_segments,
            total_segments,
            total_hot_memory_bytes: total_hot_segments * SEGMENT_SIZE,
            total_cold_memory_bytes: total_cold_segments * SEGMENT_SIZE,
            total_memory_bytes: total_segments * SEGMENT_SIZE,
            total_cells,
            living_transactions,
            physical_memory_limit_bytes: total_physical_limit,
            tiered_memory_enabled: tiered_enabled,
        }
    }
}
