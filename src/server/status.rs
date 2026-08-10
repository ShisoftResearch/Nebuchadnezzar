use crate::ram::segs::SEGMENT_SIZE;

/// Heap totals from the global-allocator shim (`crate::mem_shim`): exact
/// live bytes from two relaxed atomics per alloc — genuinely always-on.
///
/// This replaces `mallinfo2`, whose "cheap bookkeeping read" is anything
/// but at scale: it walks every arena's bins WITH THE ARENA LOCK HELD,
/// which at a few hundred GB of heap takes seconds-to-minutes and stalls
/// every allocating thread. Measured on the TB12 import: status polls put
/// the busiest thread 78-99% inside `int_mallinfo` and each poll knocked
/// the ingest rate into a trough. The shim keeps the same JSON shape:
/// in_use = live bytes; the free/arena/mmap split was a glibc concept —
/// arena now mirrors live, free reports 0, and the large-block band is
/// served exactly by the >=128K histogram buckets (`heap_buckets`).
fn heap_info() -> (usize, usize, usize, usize) {
    let live = crate::mem_shim::live_bytes();
    let buckets = crate::mem_shim::bucket_stats();
    let large: usize = buckets[1..].iter().map(|(b, _)| b).sum();
    (live, 0, live, large)
}

/// Whether to walk the maps with `mincore` for per-structure residency.
///
/// Off by default. The walk is cheap in CPU (~0.5 ms per 1.5 GiB map, ~70 ms
/// for 128 chunks) but it takes the process `mmap_lock` for read, and chunk
/// retirement takes that lock for write to sentinel-remap and `MADV_DONTNEED`
/// released tables. A hard-polled status endpoint would contend with exactly
/// the reclamation path this accounting exists to verify, so it is opt-in:
/// set `NEB_STATUS_RESIDENCY=1` when diagnosing.
fn residency_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("NEB_STATUS_RESIDENCY").is_ok())
}

/// Resident set size of this process, from `/proc/self/statm` (pages).
fn process_rss_bytes() -> usize {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1).and_then(|v| v.parse::<usize>().ok()))
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}
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
    /// Real resident memory of this chunk's `cell_index`, from `mincore` over
    /// the live table structures. Attributes index cost directly instead of
    /// inferring it as "RSS minus segments", which lumps in every other map,
    /// the ranged index, and the heap.
    pub cell_index_resident_bytes: usize,
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
    /// Sum of every chunk's `cell_index_resident_bytes`. Compare against RSS
    /// minus segments: the remainder is the ranged index, full-text shards,
    /// transaction state and plain heap -- none of which the cell index
    /// explains, and which has been the largest unattributed term.
    pub total_cell_index_resident_bytes: usize,
    /// Segment sequence index, one `WordMap` per chunk.
    pub total_seg_index_resident_bytes: usize,
    /// Schema lookup maps (id -> schema, name -> id).
    pub schema_maps_resident_bytes: usize,
    /// Tiered manager's per-chunk state map.
    pub tiered_states_resident_bytes: usize,
    /// Full-text in-memory caches (`field_stats`, `doc_metadata`). Covers the
    /// PtrHashMap tables and their node buffers only -- `doc_metadata` is keyed
    /// per (schema, field, doc) and each value is an `Arc<Mutex<DocMeta>>` whose
    /// payload lives on the heap and lands in `unattributed` instead.
    pub fulltext_maps_resident_bytes: usize,
    /// Heap accounting from the global-allocator shim (`mem_shim`): exact
    /// live bytes, no allocator walk, no arena lock (see `heap_info`).
    ///   * `heap_in_use`  - live allocation bytes (layout sizes)
    ///   * `heap_free`    - always 0 (allocator retention is an RSS concern,
    ///                      visible as `process_rss - attributed`, not a
    ///                      demand gauge)
    ///   * `heap_arena`   - mirrors `heap_in_use` (field kept for samplers)
    ///   * `heap_mmap`    - live bytes in allocations >= 128 KiB (the
    ///                      "large-block band" of the TB post-mortems)
    /// `heap_buckets` below splits live bytes by size class.
    /// Ranged index: registry-map residency, tree count, and total live keys.
    /// The B-tree nodes are plain heap (`NodeCellRef`), so mincore cannot see
    /// them -- the key count is what sizes this index, and it lands in
    /// `heap_in_use` below.
    pub ranged_maps_resident_bytes: usize,
    pub ranged_tree_count: usize,
    pub ranged_key_count: usize,
    pub heap_in_use_bytes: usize,
    pub heap_free_bytes: usize,
    pub heap_arena_bytes: usize,
    pub heap_mmap_bytes: usize,
    /// Live-allocation histogram by size class: bucket name -> (bytes,
    /// count). Continuously answers "which band is growing" — the question
    /// that previously took smaps walks (which hold `mmap_lock` and
    /// perturb the workload being observed).
    pub heap_buckets: std::collections::BTreeMap<String, (usize, usize)>,
    /// Everything above, i.e. the Lightning tables this build can reach.
    pub attributed_map_resident_bytes: usize,
    /// Process RSS, so the remainder is explicit rather than inferred.
    pub process_rss_bytes: usize,
    /// `process_rss - resident_segments - attributed_maps`, where resident
    /// segments are the hot tier plus faulted-in cold bytes. Do NOT use
    /// `total_segments * SEGMENT_SIZE` here: that counts cold segments whose
    /// data lives on disk, overstates residency once tiering engages, and
    /// floors this to zero. Whatever this holds is
    /// neither tiered data nor a map we account for: the ranged-index B-trees,
    /// full-text posting shards, transaction state, allocator retention and
    /// plain heap. This has been the largest unexplained term at scale, so it
    /// is reported rather than left to subtraction by hand.
    pub unattributed_resident_bytes: usize,
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
    /// Unscoped index tasks spawned but not yet finished. Growth here is
    /// execution falling behind production; each task retains its payload.
    pub index_tasks_inflight: i64,
    pub index_global_wait_ms: u64,
    /// Breakdown of the secondary-index phase.
    pub idx_probe_us: u64,
    pub idx_key_us: u64,
    pub idx_spawn_us: u64,
    /// Where a vertex-create request spends its time, filled by the gateway.
    pub vc_calls: u64,
    pub vc_schema_us: u64,
    pub vc_norm_us: u64,
    pub vc_engine_us: u64,
    pub vc_resp_us: u64,
    /// Index work a write waits on before returning.
    pub idx_scope_calls: u64,
    pub idx_scope_empty: u64,
    pub idx_scope_tasks: u64,
    pub idx_scope_wait_us: u64,
    pub idx_task_exec_us: u64,
    /// Per-index-type insert cost and count: ranged, hashed, vector, fulltext, embedding.
    pub idx_type_us: Vec<u64>,
    pub idx_type_count: Vec<u64>,
    /// Hashed-index bucket sizes: the cost of read-modify-write scales with these.
    pub hash_bucket_len_sum: u64,
    pub hash_bucket_samples: u64,
    pub hash_bucket_max: u64,
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

        let mut total_index_resident = 0usize;
        let mut total_seg_index_resident = 0usize;
        // Collect statistics for each chunk
        for chunk in &chunks.list {
            let segments = chunk.segments();
            let hot_count = segments.iter().filter(|s| s.is_hot()).count();
            let cold_count = segments.iter().filter(|s| s.is_cold()).count();
            let seg_count = segments.len();
            let cell_count = chunk.cell_count();
            let index_resident = if residency_enabled() {
                chunk.cell_index.resident_pages() * 4096
            } else {
                0
            };

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
                cell_index_resident_bytes: index_resident,
            });

            total_hot_segments += hot_count;
            total_cold_segments += cold_count;
            total_segments += seg_count;
            total_cells += cell_count;
            total_index_resident += index_resident;
            if residency_enabled() {
                total_seg_index_resident += chunk.segs.index_resident_bytes();
            }
        }

        let schema_resident = if residency_enabled() {
            self.meta().schemas.resident_bytes()
        } else {
            0
        };
        let tiered_states_resident = if residency_enabled() {
            chunks
                .tiered_manager
                .as_ref()
                .map(|m| m.states_resident_bytes())
                .unwrap_or(0)
        } else {
            0
        };
        let fulltext_resident = if residency_enabled() {
            self.indexer()
                .and_then(|b| b.clients.fulltext_indexer())
                .map(|f| f.resident_bytes())
                .unwrap_or(0)
        } else {
            0
        };
        let (ranged_trees, ranged_keys, ranged_maps) = if residency_enabled() {
            crate::index::ranged::tree::service::LOCAL_TREE_SERVICE
                .get()
                .map(|svc| svc.index_stats())
                .unwrap_or((0, 0, 0))
        } else {
            (0, 0, 0)
        };
        let attributed = total_index_resident
            + total_seg_index_resident
            + schema_resident
            + tiered_states_resident
            + fulltext_resident
            + ranged_maps;
        let process_rss = process_rss_bytes();
        let (heap_in_use, heap_free, heap_arena, heap_mmap) = heap_info();

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
            total_cell_index_resident_bytes: total_index_resident,
            total_seg_index_resident_bytes: total_seg_index_resident,
            schema_maps_resident_bytes: schema_resident,
            tiered_states_resident_bytes: tiered_states_resident,
            fulltext_maps_resident_bytes: fulltext_resident,
            ranged_maps_resident_bytes: ranged_maps,
            ranged_tree_count: ranged_trees,
            ranged_key_count: ranged_keys,
            heap_in_use_bytes: heap_in_use,
            heap_free_bytes: heap_free,
            heap_arena_bytes: heap_arena,
            heap_mmap_bytes: heap_mmap,
            heap_buckets: {
                let stats = crate::mem_shim::bucket_stats();
                crate::mem_shim::BUCKET_NAMES
                    .iter()
                    .zip(stats.iter())
                    .map(|(name, (bytes, count))| (name.to_string(), (*bytes, *count)))
                    .collect()
            },
            attributed_map_resident_bytes: attributed,
            process_rss_bytes: process_rss,
            unattributed_resident_bytes: process_rss
                .saturating_sub(total_hot_segments * SEGMENT_SIZE)
                .saturating_sub(tiered_counters.cold_resident_bytes as usize)
                .saturating_sub(attributed),
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
            index_tasks_inflight: crate::index::builder::INDEX_TASKS_INFLIGHT.load(std::sync::atomic::Ordering::Relaxed),
            index_global_wait_ms: crate::index::builder::INDEX_GLOBAL_WAIT_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1_000_000,
            idx_probe_us: crate::index::builder::IDX_PROBE_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            idx_key_us: crate::index::builder::IDX_KEY_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            idx_spawn_us: crate::index::builder::IDX_SPAWN_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            // Gateway-level phases; neb has no view of them, so they are zero
            // here and filled in by the caller that owns the HTTP handler.
            vc_calls: 0,
            vc_schema_us: 0,
            vc_norm_us: 0,
            vc_engine_us: 0,
            vc_resp_us: 0,
            idx_scope_calls: crate::index::builder::IDX_SCOPE_CALLS.load(std::sync::atomic::Ordering::Relaxed),
            idx_scope_empty: crate::index::builder::IDX_SCOPE_EMPTY.load(std::sync::atomic::Ordering::Relaxed),
            idx_scope_tasks: crate::index::builder::IDX_SCOPE_TASKS.load(std::sync::atomic::Ordering::Relaxed),
            idx_scope_wait_us: crate::index::builder::IDX_SCOPE_WAIT_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            idx_task_exec_us: crate::index::builder::IDX_TASK_EXEC_NANOS.load(std::sync::atomic::Ordering::Relaxed) / 1000,
            idx_type_us: crate::index::builder::IDX_BY_TYPE_NANOS.iter().map(|v| v.load(std::sync::atomic::Ordering::Relaxed) / 1000).collect(),
            idx_type_count: crate::index::builder::IDX_BY_TYPE_COUNT.iter().map(|v| v.load(std::sync::atomic::Ordering::Relaxed)).collect(),
            hash_bucket_len_sum: crate::index::hash::HASH_BUCKET_LEN_SUM.load(std::sync::atomic::Ordering::Relaxed),
            hash_bucket_samples: crate::index::hash::HASH_BUCKET_SAMPLES.load(std::sync::atomic::Ordering::Relaxed),
            hash_bucket_max: crate::index::hash::HASH_BUCKET_MAX.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}
