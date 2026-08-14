// Crash-churn fuzzer for the ranged index.
//
// The durability corpses this hunts (TB14/TB16/TB17) share three
// ingredients none of which needs a terabyte: structural churn (page and
// tree splits under write load), a kill at an adversarial moment, and a
// reload that is VERIFIED rather than trusted. TB-scale runs embed two or
// three restarts per six hours; this harness runs hundreds per hour.
//
// Two modes in one binary:
//
//   child <addr> <group> <base_dir> <first|recover> <churn_secs>
//     Starts a real server (WAL, backup, raft dirs under base_dir) with the
//     ranged indexer enabled, scans the indexed schema and reports
//     "SCANNED n=<count>", then churns: writer tasks insert cells with a
//     monotonically increasing indexed key, streaming "ACK_TO <n>"
//     high-water lines. SIGTERM triggers a graceful shutdown;
//     SIGKILL is the parent's other weapon.
//
//   parent <base_dir> <cycles>
//     Loop: spawn child, read SCANNED, enforce the invariants, let it
//     churn for a random window, kill it (random SIGKILL/SIGTERM),
//     repeat. Invariants:
//       - the child must reach SCANNED within a deadline (a hang or a
//         refused tree load is a caught corpse),
//       - the scanned count NEVER regresses below the best count verified
//         by any earlier cycle (a regression means previously-served keys
//         vanished: the corpse class),
//       - after a graceful SIGTERM cycle, the scanned count must be at
//         least the last acked high-water (the shutdown flush contract).
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("child") => child_main(&args[2..]),
        Some("parent") => parent_main(&args[2..]),
        _ => {
            eprintln!("usage: neb_crash_churn child <addr> <group> <base_dir> <first|recover> <churn_secs>");
            eprintln!("       neb_crash_churn parent <base_dir> <cycles>");
            ExitCode::from(64)
        }
    }
}

// ---------------------------------------------------------------- parent --

struct ParentState {
    next_key: u64,
    best_verified: u64,
}

fn parent_main(args: &[String]) -> ExitCode {
    let base_dir = args.get(0).expect("base_dir").clone();
    let cycles: u64 = args.get(1).expect("cycles").parse().expect("cycles number");
    let addr_base: u16 = 42200;
    let exe = std::env::current_exe().expect("own path");

    std::fs::create_dir_all(&base_dir).expect("base dir");
    let mut state = ParentState {
        next_key: 0,
        best_verified: 0,
    };
    // Deterministic-ish per-run seed without wall-clock dependence beyond
    // the pid; each cycle derives its choices from this.
    let mut seed = (std::process::id() as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let mut rand = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    for cycle in 0..cycles {
        let first = cycle == 0;
        // Identity is deployment identity: the same addr and group every
        // cycle, exactly like a real server restarting. Rotating the group
        // name here once "found" a schema-recovery bug that was actually
        // this harness impersonating a different cluster.
        let addr = format!("127.0.0.1:{}", addr_base);
        let group = "crash-churn".to_string();
        let churn_secs = 2 + rand() % 6;
        let graceful = rand() % 2 == 0;

        println!(
            "=== cycle {}/{} addr={} churn={}s kill={} best_verified={} next_key={}",
            cycle + 1,
            cycles,
            addr,
            churn_secs,
            if graceful { "TERM" } else { "KILL" },
            state.best_verified,
            state.next_key,
        );

        let mut child = Command::new(&exe)
            .args([
                "child",
                &addr,
                &group,
                &base_dir,
                if first { "first" } else { "recover" },
                &churn_secs.to_string(),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn child");
        let child_pid = child.id() as i32;
        let stdout = BufReader::new(child.stdout.take().expect("child stdout"));

        let started = Instant::now();
        let deadline = Duration::from_secs(180);
        let mut scanned: Option<u64> = None;
        let mut acked_high: u64 = state.next_key;
        let mut failed: Option<String> = None;

        // Reader thread streams child lines; main thread enforces deadline.
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        let reader = std::thread::spawn(move || {
            for line in stdout.lines() {
                match line {
                    Ok(line) => {
                        if tx.send(line).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let mut kill_at: Option<Instant> = None;
        loop {
            if let Some(at) = kill_at {
                if Instant::now() >= at {
                    if graceful {
                        unsafe { libc::kill(child_pid, libc::SIGTERM) };
                        // A graceful shutdown that hangs is also a finding.
                        let term_deadline = Instant::now() + Duration::from_secs(120);
                        loop {
                            match child.try_wait() {
                                Ok(Some(_)) => break,
                                Ok(None) if Instant::now() > term_deadline => {
                                    failed = Some("graceful shutdown hung 120s".into());
                                    unsafe { libc::kill(child_pid, libc::SIGKILL) };
                                    break;
                                }
                                _ => std::thread::sleep(Duration::from_millis(50)),
                            }
                        }
                    } else {
                        unsafe { libc::kill(child_pid, libc::SIGKILL) };
                    }
                    break;
                }
            }
            if scanned.is_none() && started.elapsed() > deadline {
                failed = Some(format!(
                    "child did not report SCANNED within {:?} (hang or refused load)",
                    deadline
                ));
                unsafe { libc::kill(child_pid, libc::SIGKILL) };
                break;
            }
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(line) => {
                    if let Some(rest) = line.strip_prefix("SCANNED n=") {
                        let n: u64 = rest.trim().parse().unwrap_or(u64::MAX);
                        scanned = Some(n);
                        // The two invariants that define the corpse class.
                        if n < state.best_verified {
                            failed = Some(format!(
                                "REGRESSION: scanned {} < best verified {}",
                                n, state.best_verified
                            ));
                            unsafe { libc::kill(child_pid, libc::SIGKILL) };
                            break;
                        }
                        state.best_verified = n;
                        println!("    scanned n={} (best={})", n, state.best_verified);
                        kill_at = Some(Instant::now() + Duration::from_secs(churn_secs));
                    } else if let Some(rest) = line.strip_prefix("ACK_TO ") {
                        acked_high = rest.trim().parse().unwrap_or(acked_high);
                    } else if line.starts_with("SCAN_ERROR") || line.starts_with("FATAL") {
                        failed = Some(line);
                        unsafe { libc::kill(child_pid, libc::SIGKILL) };
                        break;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    if scanned.is_none() {
                        failed = Some("child exited before SCANNED".into());
                    }
                    break;
                }
            }
        }
        // Drain any remaining lines (ACK_TO sent right before death).
        while let Ok(line) = rx.recv_timeout(Duration::from_millis(500)) {
            if let Some(rest) = line.strip_prefix("ACK_TO ") {
                acked_high = rest.trim().parse().unwrap_or(acked_high);
            }
        }
        let _ = child.wait();
        let _ = reader.join();

        state.next_key = acked_high.max(state.next_key);
        if graceful && failed.is_none() {
            // The next cycle's scan must cover every acked key: record the
            // expectation now; enforcement happens when that scan reports.
            if state.next_key > state.best_verified {
                state.best_verified = state.next_key;
                println!(
                    "    graceful shutdown: expecting >= {} on next load",
                    state.best_verified
                );
            }
        }

        if let Some(reason) = failed {
            eprintln!("CYCLE {} FAILED: {}", cycle + 1, reason);
            eprintln!(
                "state: next_key={} best_verified={} dir={}",
                state.next_key, state.best_verified, base_dir
            );
            return ExitCode::from(1);
        }
    }
    println!(
        "ALL {} CYCLES PASSED (final verified count {})",
        cycles, state.best_verified
    );
    ExitCode::SUCCESS
}

// ----------------------------------------------------------------- child --

fn child_main(args: &[String]) -> ExitCode {
    let addr = args.get(0).expect("addr").clone();
    let group = args.get(1).expect("group").clone();
    let base_dir = args.get(2).expect("base_dir").clone();
    let first = args.get(3).map(|s| s == "first").unwrap_or(false);
    let churn_secs: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(3600);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(child_async(addr, group, base_dir, first, churn_secs)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            println!("FATAL {}", e);
            ExitCode::from(1)
        }
    }
}

const CHURN_SCHEMA_ID: u32 = 7301;
const KEY_FIELD: &str = "seq";
const PAD_FIELD: &str = "pad";

async fn child_async(
    addr: String,
    group: String,
    base_dir: String,
    first: bool,
    churn_secs: u64,
) -> Result<(), String> {
    use neb::index::ranged::tree::btree::Ordering as TreeOrdering;
    use neb::query::data_client::{ValueRange, ValueRangeTerm};
    use neb::ram::cell::OwnedCell;
    use neb::ram::schema::{Field, IndexType, Schema};
    use neb::ram::types::{Id, Type};
    use neb::server::{NebServer, ServerOptions, Service};
    use bifrost_hasher::hash_str;
    use dovahkiin::expr::serde::Expr;
    use dovahkiin::types::*;

    let dir = std::path::Path::new(&base_dir);
    for sub in ["backup", "wal", "undo", "raft"] {
        std::fs::create_dir_all(dir.join(sub)).map_err(|e| e.to_string())?;
    }

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
            enable_recovery: !first,
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

    if first {
        let fields = Field::new_schema(vec![
            Field::new_indexed(KEY_FIELD, Type::U64, vec![IndexType::Ranged]),
            Field::new_unindexed(PAD_FIELD, Type::String),
        ]);
        let schema = Schema::new_with_id(CHURN_SCHEMA_ID, "churn", None, fields, false, true);
        client
            .new_schema_with_id(schema)
            .await
            .map_err(|e| format!("schema rpc: {:?}", e))?
            .map_err(|e| format!("schema: {:?}", e))?;
    }

    // Verification scan: walk the whole indexed range. A refused tree load
    // surfaces here as an error (or, historically, a hang -- the parent's
    // deadline catches that). Count keys, resuming across cursor batches.
    let idx_client = server.indexed_data_client();
    let field_id = hash_str(KEY_FIELD);
    let val_range = ValueRange {
        start: ValueRangeTerm::inclusive_from(&OwnedValue::U64(0).shared()),
        end: ValueRangeTerm::inclusive_from(&OwnedValue::U64(u64::MAX).shared()),
    };
    let mut count: u64 = 0;
    match idx_client
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
    {
        Ok(mut cursor) => loop {
            match cursor.next().await {
                Ok(Some(_)) => count += 1,
                Ok(None) => break,
                Err(e) => {
                    println!("SCAN_ERROR {:?}", e);
                    return Err("scan failed".into());
                }
            }
        },
        Err(e) => {
            println!("SCAN_ERROR {:?}", e);
            return Err("scan failed to open".into());
        }
    }
    println!("SCANNED n={}", count);
    flush_stdout();

    // Churn: writers append monotone keys; a reporter streams the acked
    // high-water mark. Padding gives pages realistic weight so splits come
    // fast. Keys continue from the scanned count -- duplicates with an
    // earlier incarnation's unacked tail are harmless upserts.
    let next = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(count));
    let acked = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    const WRITERS: usize = 4;
    let mut writer_handles = Vec::new();
    for _ in 0..WRITERS {
        let client = client.clone();
        let next = next.clone();
        let acked = acked.clone();
        writer_handles.push(tokio::spawn(async move {
            loop {
                let seq = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let id = Id::from_parts(9, seq);
                let mut value = OwnedValue::Map(OwnedMap::new());
                value[KEY_FIELD] = OwnedValue::U64(seq);
                value[PAD_FIELD] = OwnedValue::String(format!("pad-{:0>200}", seq));
                let cell = OwnedCell::new_with_id(CHURN_SCHEMA_ID, &id, value);
                if let Ok(Ok(_)) = client.write_cell(cell).await {
                    acked.fetch_max(seq + 1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }));
    }
    let acked_reporter = acked.clone();
    tokio::spawn(async move {
        let mut last = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let now = acked_reporter.load(std::sync::atomic::Ordering::Relaxed);
            if now != last {
                println!("ACK_TO {}", now);
                flush_stdout();
                last = now;
            }
        }
    });

    // Serve until SIGTERM (graceful) or the churn window elapses; SIGKILL
    // needs no cooperation.
    let mut term =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|e| e.to_string())?;
    tokio::select! {
        _ = term.recv() => {
            println!("TERM: graceful shutdown");
            flush_stdout();
            // Ingress stops first, as it does on a real server where RPC
            // teardown precedes the flush; writers left running would grow
            // the write-back backlog faster than the drain barrier closes.
            for handle in &writer_handles {
                handle.abort();
            }
            server.shutdown().await;
            println!("SHUTDOWN_COMPLETE");
            flush_stdout();
        }
        _ = tokio::time::sleep(Duration::from_secs(churn_secs + 300)) => {
            // Parent should have killed us long ago; exit to avoid zombies.
        }
    }
    Ok(())
}

fn flush_stdout() {
    let _ = std::io::stdout().flush();
}
