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
    pub shared_hot_segments: usize,
    pub hot_segment_counter_drift: isize,
    pub total_cold_segments: usize,
    pub total_segments: usize,
    pub total_hot_memory_bytes: usize,
    pub shared_hot_memory_bytes: usize,
    pub total_cold_memory_bytes: usize,
    pub total_memory_bytes: usize,
    pub total_cells: usize,
    pub living_transactions: usize,
    pub physical_memory_limit_bytes: Option<usize>,
    pub tiered_memory_enabled: bool,
    /// Server-wide eviction counters. `churns` counts promotions of segments
    /// evicted within the cooldown -- eviction of data still in use, where each
    /// event costs a write out and a read back.
    pub promotions: u64,
    pub evictions: u64,
    pub churns: u64,
    pub lower_watermark_evictions: u64,
    /// Promotions refused because the hot tier was at the hard limit.
    pub promotions_declined: u64,
    /// Reads served from a cold backup without promoting the segment.
    pub cold_block_reads: u64,
    /// Cold segments whose block cache was handed back under memory pressure.
    /// Cold residency is bounded by this and nothing else.
    pub cold_blocks_reclaimed: u64,
    /// Cold-read amplification, as bytes moved rather than a ratio, so the
    /// denominator can be chosen at analysis time.
    pub cold_block_hits: u64,
    pub cold_block_misses: u64,
    pub cold_block_file_bytes: u64,
    pub cold_block_plain_bytes: u64,
    pub cold_block_opens: u64,
    pub cold_index_loads: u64,
    /// Bytes faulted into cold segments, included in the pressure calculation.
    pub cold_resident_bytes: u64,
    /// Durability write accounting, counted where the writes are issued.
    pub archive_count: u64,
    pub archive_bytes: u64,
    /// Archives of segments that already had a backup file -- rewrites, not
    /// first writes.
    pub archive_rewrites: u64,
    pub wal_bytes: u64,
    pub wal_syncs: u64,
    /// Entries journalled. Divided by live cells this is the rewrite factor.
    pub wal_writes: u64,
    /// Per-segment WAL lock: how long writers waited for it against how long it
    /// was held, and how often acquisition was contended.
    pub wal_lock_wait_ms: u64,
    pub wal_lock_held_ms: u64,
    pub wal_lock_contended: u64,
    /// Per-phase cost of a cell write, microseconds per cell.
    pub write_cells: u64,
    pub write_plan_us: u64,
    pub write_alloc_us: u64,
    pub write_copy_us: u64,
    pub write_index_us: u64,
    pub write_secondary_us: u64,
    pub write_stats_us: u64,
    /// Index tasks by route, and time spent waiting for the global backlog lock.
    pub index_task_local: u64,
    pub index_task_global: u64,
    pub index_global_wait_ms: u64,
    /// Breakdown of the secondary-index phase.
    pub idx_probe_us: u64,
    pub idx_key_us: u64,
    pub idx_spawn_us: u64,
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
        println!("  • Shared Hot Counter:  {}", self.shared_hot_segments);
        println!(
            "  • Counter Drift:       {}",
            self.hot_segment_counter_drift
        );

        println!("\n💾 Memory Usage:");
        println!(
            "  • Hot Memory:          {}",
            Self::format_bytes(self.total_hot_memory_bytes)
        );
        println!(
            "  • Shared Hot Memory:   {}",
            Self::format_bytes(self.shared_hot_memory_bytes)
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
        let shared_hot_segments = self
            .chunks()
            .tiered_manager
            .as_ref()
            .map(|manager| manager.shared_hot_segments())
            .unwrap_or(total_hot_segments);
        let hot_segment_counter_drift = shared_hot_segments as isize - total_hot_segments as isize;

        let tiered_counters = self
            .chunks()
            .tiered_manager
            .as_ref()
            .map(|manager| manager.global_counters())
            .unwrap_or_default();

        let living_transactions = self
            .txn_manager()
            .as_ref()
            .map(|tm| tm.transaction_count())
            .unwrap_or(0);

        ServerMemoryStatus {
            total_chunks,
            chunk_details,
            total_hot_segments,
            shared_hot_segments,
            hot_segment_counter_drift,
            total_cold_segments,
            total_segments,
            total_hot_memory_bytes: total_hot_segments * SEGMENT_SIZE,
            shared_hot_memory_bytes: shared_hot_segments * SEGMENT_SIZE,
            total_cold_memory_bytes: total_cold_segments * SEGMENT_SIZE,
            total_memory_bytes: total_segments * SEGMENT_SIZE,
            total_cells,
            living_transactions,
            physical_memory_limit_bytes: total_physical_limit,
            tiered_memory_enabled: tiered_enabled,
            promotions: tiered_counters.promotions,
            evictions: tiered_counters.evictions,
            churns: tiered_counters.churns,
            lower_watermark_evictions: tiered_counters.lower_watermark_evictions,
            promotions_declined: tiered_counters.promotions_declined,
            cold_block_reads: tiered_counters.cold_block_reads,
            cold_blocks_reclaimed: tiered_counters.cold_blocks_reclaimed,
            cold_block_hits: crate::ram::segs::COLD_BLOCK_HITS.load(std::sync::atomic::Ordering::Relaxed),
            cold_block_misses: crate::ram::segs::COLD_BLOCK_MISSES.load(std::sync::atomic::Ordering::Relaxed),
            cold_block_file_bytes: crate::ram::segs::COLD_BLOCK_FILE_BYTES.load(std::sync::atomic::Ordering::Relaxed),
            cold_block_plain_bytes: crate::ram::segs::COLD_BLOCK_PLAIN_BYTES.load(std::sync::atomic::Ordering::Relaxed),
            cold_block_opens: crate::ram::segs::COLD_BLOCK_OPENS.load(std::sync::atomic::Ordering::Relaxed),
            cold_index_loads: crate::ram::segs::COLD_INDEX_LOADS.load(std::sync::atomic::Ordering::Relaxed),
            cold_resident_bytes: tiered_counters.cold_resident_bytes,
            archive_count: crate::ram::segs::ARCHIVE_COUNT.load(std::sync::atomic::Ordering::Relaxed),
            archive_bytes: crate::ram::segs::ARCHIVE_BYTES.load(std::sync::atomic::Ordering::Relaxed),
            archive_rewrites: crate::ram::segs::ARCHIVE_REWRITES.load(std::sync::atomic::Ordering::Relaxed),
            wal_bytes: crate::ram::segs::WAL_BYTES.load(std::sync::atomic::Ordering::Relaxed),
            wal_syncs: crate::ram::segs::WAL_SYNCS.load(std::sync::atomic::Ordering::Relaxed),
            wal_writes: crate::ram::segs::WAL_WRITES.load(std::sync::atomic::Ordering::Relaxed),
            wal_lock_wait_ms: crate::ram::segs::WAL_LOCK_WAIT_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1_000_000,
            wal_lock_held_ms: crate::ram::segs::WAL_LOCK_HELD_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1_000_000,
            wal_lock_contended: crate::ram::segs::WAL_LOCK_CONTENDED.load(std::sync::atomic::Ordering::Relaxed),
            write_cells: crate::ram::chunk::WRITE_CELLS.load(std::sync::atomic::Ordering::Relaxed),
            write_plan_us: crate::ram::chunk::WRITE_PLAN_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            write_alloc_us: crate::ram::chunk::WRITE_ALLOC_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            write_copy_us: crate::ram::chunk::WRITE_COPY_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            write_index_us: crate::ram::chunk::WRITE_INDEX_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            write_secondary_us: crate::ram::chunk::WRITE_SECONDARY_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            write_stats_us: crate::ram::chunk::WRITE_STATS_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            index_task_local: crate::index::builder::INDEX_TASK_LOCAL.load(std::sync::atomic::Ordering::Relaxed),
            index_task_global: crate::index::builder::INDEX_TASK_GLOBAL.load(std::sync::atomic::Ordering::Relaxed),
            index_global_wait_ms: crate::index::builder::INDEX_GLOBAL_WAIT_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1_000_000,
            idx_probe_us: crate::index::builder::IDX_PROBE_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            idx_key_us: crate::index::builder::IDX_KEY_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            idx_spawn_us: crate::index::builder::IDX_SPAWN_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
        }
    }
}
