// Temporary diagnostic probe for the crash-churn corpse. NOT part of the
// harness; safe to delete. Starts a server over an existing corpse dir in
// recovery mode, then cross-references cells vs ranged-index entries for
// the churn schema: which seqs have a durable cell but no index entry, and
// vice versa.
use std::process::ExitCode;

const CHURN_SCHEMA_ID: u32 = 7301;
const KEY_FIELD: &str = "seq";

struct ChurnLogger(log::LevelFilter);
impl log::Log for ChurnLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= self.0
    }
    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("[{}] {} - {}", record.level(), record.target(), record.args());
        }
    }
    fn flush(&self) {}
}

fn init_diag_logging() {
    let Ok(level) = std::env::var("NEB_CHURN_LOG") else {
        return;
    };
    let filter = match level.to_ascii_lowercase().as_str() {
        "error" => log::LevelFilter::Error,
        "warn" => log::LevelFilter::Warn,
        "debug" => log::LevelFilter::Debug,
        "trace" => log::LevelFilter::Trace,
        _ => log::LevelFilter::Info,
    };
    let _ = log::set_boxed_logger(Box::new(ChurnLogger(filter)));
    log::set_max_level(filter);
}

fn main() -> ExitCode {
    init_diag_logging();
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).expect("addr").clone();
    let group = args.get(2).expect("group").clone();
    let base_dir = args.get(3).expect("base_dir").clone();
    let max_seq: u64 = args.get(4).expect("max_seq").parse().expect("max_seq");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(probe(addr, group, base_dir, max_seq)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            println!("FATAL {}", e);
            ExitCode::from(1)
        }
    }
}

async fn probe(addr: String, group: String, base_dir: String, max_seq: u64) -> Result<(), String> {
    use neb::index::ranged::tree::btree::Ordering as TreeOrdering;
    use neb::query::data_client::{ValueRange, ValueRangeTerm};
    use neb::ram::types::Id;
    use neb::server::{NebServer, ServerOptions, Service};
    use bifrost_hasher::hash_str;
    use dovahkiin::expr::serde::Expr;
    use dovahkiin::types::*;

    let dir = std::path::Path::new(&base_dir);
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 256 * 1024 * 1024,
            tiered_config: None,
            backup_storage: Some(dir.join("backup").to_string_lossy().into_owned()),
            wal_storage: Some(dir.join("wal").to_string_lossy().into_owned()),
            undo_log_storage: Some(dir.join("undo").to_string_lossy().into_owned()),
            raft_storage: Some(dir.join("raft").to_string_lossy().into_owned()),
            index_enabled: true,
            services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
            enable_recovery: true,
            disable_storage_locks: true,
        },
        &addr,
        &group,
        async |_| {},
    )
    .await
    .map_err(|e| format!("server start: {:?}", e))?;

    let client = std::sync::Arc::new(
        server
            .data_client(&vec![addr.clone()])
            .await
            .map_err(|e| format!("data client: {:?}", e))?,
    );

    // 1. Which cells exist?
    let mut missing_cells = Vec::new();
    for seq in 0..max_seq {
        let id = Id::from_parts(9, seq);
        match client.read_cell(id).await {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => missing_cells.push(seq),
            Err(e) => println!("RPC error reading {}: {:?}", seq, e),
        }
    }
    println!("MISSING_CELLS count={} sample={:?}", missing_cells.len(),
        &missing_cells[..missing_cells.len().min(50)]);
    if missing_cells.len() > 50 {
        println!("MISSING_CELLS_FULL {:?}", missing_cells);
    }

    // 2. Which index entries exist?
    let idx_client = server.indexed_data_client();
    let field_id = hash_str(KEY_FIELD);
    let val_range = ValueRange {
        start: ValueRangeTerm::inclusive_from(&OwnedValue::U64(0).shared()),
        end: ValueRangeTerm::inclusive_from(&OwnedValue::U64(u64::MAX).shared()),
    };
    let mut seen = vec![false; (max_seq + 1000) as usize];
    let mut count = 0u64;
    let mut cursor = idx_client
        .range_index_scan(
            CHURN_SCHEMA_ID,
            field_id,
            val_range,
            vec![],
            Expr::nothing(),
            Expr::nothing(),
            TreeOrdering::Forward,
        )
        .await
        .map_err(|e| format!("scan open: {:?}", e))?;
    loop {
        match cursor.next().await {
            Ok(Some(cell)) => {
                count += 1;
                if let Some(seq) = cell.data[hash_str(KEY_FIELD)].u64() {
                    if (*seq as usize) < seen.len() {
                        seen[*seq as usize] = true;
                    }
                }
            }
            Ok(None) => break,
            Err(e) => return Err(format!("scan: {:?}", e)),
        }
    }
    let missing_idx: Vec<u64> = (0..max_seq)
        .filter(|s| !seen[*s as usize])
        .collect();
    println!("SCANNED n={}", count);
    println!("MISSING_INDEX count={} sample={:?}", missing_idx.len(),
        &missing_idx[..missing_idx.len().min(100)]);
    if missing_idx.len() > 100 {
        // Print ranges compactly.
        let mut ranges = Vec::new();
        let mut start = missing_idx[0];
        let mut prev = missing_idx[0];
        for &s in &missing_idx[1..] {
            if s != prev + 1 {
                ranges.push((start, prev));
                start = s;
            }
            prev = s;
        }
        ranges.push((start, prev));
        println!("MISSING_INDEX_RANGES {:?}", ranges);
    }

    // 3. Cross reference.
    let cells_missing: std::collections::HashSet<u64> = missing_cells.iter().cloned().collect();
    let idx_only: Vec<u64> = missing_idx
        .iter()
        .filter(|s| !cells_missing.contains(s))
        .cloned()
        .collect();
    println!(
        "CELL_PRESENT_INDEX_MISSING count={} sample={:?}",
        idx_only.len(),
        &idx_only[..idx_only.len().min(100)]
    );
    std::process::exit(0);
}
