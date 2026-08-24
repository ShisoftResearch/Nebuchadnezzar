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
            eprintln!("usage: neb_crash_churn child <addr> <group> <base_dir> <first|recover> <churn_secs> [delete_rate] [delete_from]");
            eprintln!("       neb_crash_churn parent <base_dir> <cycles>");
            ExitCode::from(64)
        }
    }
}

// ---------------------------------------------------------------- parent --

struct ParentState {
    next_key: u64,
    best_verified: u64,
    txn_committed_high: u64,
    txn_verify_from: u64,
    /// Contiguous prefix of keys the store has been told to delete.
    deleted: u64,
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
        txn_committed_high: 0,
        txn_verify_from: 0,
        deleted: 0,
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

    let mut mutilated_last = false;
    let mut exhausted_last = false;
    for cycle in 0..cycles {
        let first = cycle == 0;
        if mutilated_last {
            println!("    (previous cycle damaged files: loss is allowed, a crash is not)");
        }
        // Identity is deployment identity: the same addr and group every
        // cycle, exactly like a real server restarting. Rotating the group
        // name here once "found" a schema-recovery bug that was actually
        // this harness impersonating a different cluster.
        let addr = format!("127.0.0.1:{}", addr_base);
        let group = "crash-churn".to_string();
        let churn_secs = 2 + rand() % 6;
        let graceful = rand() % 2 == 0;
        // Deletes on most cycles once there is a backlog worth retiring.
        let delete_rate = if state.next_key > 5_000 && rand() % 4 != 0 {
            20 + rand() % 60
        } else {
            0
        };
        // Transactions on most cycles: the lane that puts kills inside an
        // apply burst and inside the decision window, which is where a head
        // lease changes behaviour.
        let txn_rate = if rand() % 5 != 0 { 40 + rand() % 60 } else { 0 };
        // Damage files on some hard-kill cycles: a kill alone leaves clean
        // files, which is not what a power cut leaves behind.
        let mutilate = std::env::var("NEB_CHURN_NO_MUTILATE").is_err()
            && !graceful
            && cycle > 0
            && rand() % 3 == 0;

        println!(
            "=== cycle {}/{} addr={} churn={}s kill={} delete_rate={} txn_rate={} \
             best_verified={} next_key={} deleted={}",
            cycle + 1,
            cycles,
            addr,
            churn_secs,
            if graceful { "TERM" } else { "KILL" },
            delete_rate,
            txn_rate,
            state.best_verified,
            state.next_key,
            state.deleted,
        );

        let mut child = Command::new(&exe)
            .args([
                "child",
                &addr,
                &group,
                &base_dir,
                if first { "first" } else { "recover" },
                &churn_secs.to_string(),
                &delete_rate.to_string(),
                &state.deleted.to_string(),
                &state.next_key.to_string(),
                &txn_rate.to_string(),
                &state.txn_committed_high.to_string(),
                &state.txn_verify_from.to_string(),
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
        let mut txn_high: u64 = state.txn_committed_high;
        let mut deleted_high: u64 = state.deleted;
        let mut exhausted_this_cycle = false;
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
                                "REGRESSION: scanned {} < best verified {}{}",
                                n,
                                state.best_verified,
                                if exhausted_last {
                                    " -- BUT the previous cycle ran the store OUT OF SPACE, \
                                     which stops index write-back and loses entries by \
                                     design. Re-run with a larger NEB_CHURN_DB_GB before \
                                     reading this as a durability bug."
                                } else {
                                    ""
                                }
                            ));
                            unsafe { libc::kill(child_pid, libc::SIGKILL) };
                            break;
                        }
                        state.best_verified = n;
                        println!("    scanned n={} (best={})", n, state.best_verified);
                        kill_at = Some(Instant::now() + Duration::from_secs(churn_secs));
                    } else if let Some(rest) = line.strip_prefix("ACK_TO ") {
                        acked_high = rest.trim().parse().unwrap_or(acked_high);
                    } else if let Some(rest) = line.strip_prefix("DELETED_TO ") {
                        deleted_high = rest.trim().parse().unwrap_or(deleted_high);
                    } else if line.starts_with("STORE_EXHAUSTED") {
                        exhausted_this_cycle = true;
                        println!("    STORE RAN OUT OF SPACE during the previous cycle");
                    } else if let Some(rest) = line.strip_prefix("TXN_COMMITTED_TO ") {
                        if let Ok(round) = rest.trim().parse::<u64>() {
                            txn_high = txn_high.max(round);
                        }
                    } else if line.starts_with("TXN_TORN ") {
                        // A committed transaction that came back in pieces.
                        // Nothing about a kill excuses this: the contract is
                        // all-or-nothing, and unlike a lost tail it cannot be
                        // written off as "not yet durable".
                        failed = Some(format!("TORN TRANSACTION: {}", line.trim()));
                    } else if let Some(rest) = line.strip_prefix("TXN_ATOMIC ") {
                        println!("    transactions: {}", rest.trim());
                    } else if let Some(rest) = line.strip_prefix("SCRUB ") {
                        // Reported, never failed on: see the child's comment.
                        // Indexes are rebuildable by contract, so entries a
                        // kill caught in flight are allowed to be missing.
                        // The number's SIZE is the signal.
                        println!("    scrub: {}", rest.trim());
                    } else if let Some(rest) = line.strip_prefix("SCRUB_ERROR ") {
                        println!("    scrub FAILED: {}", rest.trim());
                    } else if let Some(rest) = line.strip_prefix("CELLS_PRESENT ") {
                        // Printed for every cycle, so a regression can be
                        // read as "index lost entries" or "store lost cells"
                        // without a second run.
                        println!("    cells present (sampled): {}", rest.trim());
                    } else if line.starts_with("SCAN_ERROR") && mutilated_last {
                        // Refusing to scan a deliberately-damaged store is
                        // the CORRECT answer -- better than serving whatever
                        // the damaged bytes decode into. What it also shows
                        // is that there is no way back: the index stays
                        // unscannable for every later cycle.
                        //
                        // A scrub now EXISTS (neb::index::scrub) and can
                        // refill a tree that still loads. It cannot help
                        // here: damage to the page chain leaves the tree
                        // unloadable, and repair inserts into a tree that
                        // must first load. Closing this needs the reset --
                        // pointing the metadata cell at a fresh chain --
                        // which is the operation that cost 31 of 40 trees on
                        // TB14 and is deliberately not built yet.
                        println!(
                            "    scan REFUSED after file damage (correct, but the index \
                             cannot be rebuilt): {}",
                            line.trim()
                        );
                        failed = Some(
                            "index unrecoverable after file damage -- the scrub cannot \
                             reset an unloadable tree"
                                .to_string(),
                        );
                        unsafe { libc::kill(child_pid, libc::SIGKILL) };
                        break;
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
        // Drain any remaining lines (the last cursor reports are sent right
        // before death, and dropping them UNDERSTATES what the child got
        // done -- which for the transaction lane means verifying nothing).
        while let Ok(line) = rx.recv_timeout(Duration::from_millis(500)) {
            if let Some(rest) = line.strip_prefix("ACK_TO ") {
                acked_high = rest.trim().parse().unwrap_or(acked_high);
            } else if let Some(rest) = line.strip_prefix("DELETED_TO ") {
                deleted_high = rest.trim().parse().unwrap_or(deleted_high);
            } else if let Some(rest) = line.strip_prefix("TXN_COMMITTED_TO ") {
                if let Ok(round) = rest.trim().parse::<u64>() {
                    txn_high = txn_high.max(round);
                }
            }
        }
        let _ = child.wait();
        let _ = reader.join();

        let mut mutilated_this_cycle_was_empty = false;
        if mutilate {
            let victims = mutilate_files(&base_dir, &mut rand);
            if victims.is_empty() {
                println!("    (nothing to mutilate this cycle)");
                mutilated_this_cycle_was_empty = true;
            } else {
                for victim in &victims {
                    println!("    MUTILATED {}", victim);
                }
                // Damaged files may legitimately cost data: the contract for
                // this cycle is that the store SURVIVES and serves only what
                // it can vouch for -- it starts, it scans, it does not panic,
                // and it never hands back a corrupt cell. So the no-regression
                // bar is lifted, and the next scan sets a new one.
                state.best_verified = 0;
            }
        }

        mutilated_last = mutilate && !mutilated_this_cycle_was_empty;
        exhausted_last = exhausted_this_cycle;
        // THIS cycle's cursor, not the highest ever seen.
        //
        // The child rebases its key numbering on the count its own scan
        // reported (`next = AtomicU64::new(count)`), so the cursor it ends
        // with is measured from that base. Carrying a global maximum across
        // cycles silently assumed the key space stays dense -- but an
        // ungraceful kill legitimately loses acked keys, leaving holes that
        // are never refilled, and the expectation then sits permanently
        // above anything the store can hold. That produced a "REGRESSION:
        // scanned 910,521 < best verified 1,197,420" on cycle 19 of a soak
        // whose three preceding cycles had all agreed on ~910k, with the
        // cells intact: the store was consistent and the harness was not.
        //
        // Rebased each cycle, `acked_high - newly_deleted` is exact whether
        // or not the space is dense, because the base cancels: keys added
        // this cycle = cursor - scan-at-start.
        // What the child just wrote is what the next cycle verifies: its
        // range starts one past the child's own start_round, which mirrors
        // the child's `txn_verify_to + 10_000`.
        if txn_rate > 0 {
            state.txn_verify_from = state.txn_committed_high + 10_000 + 1;
        }
        state.txn_committed_high = txn_high;
        state.next_key = acked_high;
        // Every key the store was told to delete is one fewer the next scan
        // owes us. A delete lost to a SIGKILL only leaves the key in place,
        // which shows up as MORE than expected -- never less -- so lowering
        // the bar by the acked deletions keeps the invariant one-sided.
        let newly_deleted = deleted_high.saturating_sub(state.deleted);
        if newly_deleted > 0 {
            state.deleted = deleted_high;
        }
        if graceful && failed.is_none() {
            // The next cycle's scan must cover every acked key: record the
            // expectation now; enforcement happens when that scan reports.
            // ONE expression, and it must include the deletions.
            //
            // Computing the delete-adjusted bar and then letting the
            // graceful branch overwrite it with the bare cursor put the
            // deletions back: cycle 3 of a soak lowered the bar to
            // 1,193,864 for 1,038 deletes and then raised it again to
            // 1,194,902, and cycle 4's scan of 1,193,881 -- which CLEARS
            // the correct bar -- was reported as a regression.
            //
            // The child rebases numbering on its scan count, so keys added
            // this cycle is cursor minus that base, and the base cancels:
            // live at the end of a graceful cycle is cursor - deletions.
            let live = state.next_key.saturating_sub(newly_deleted);
            state.best_verified = live;
            println!(
                "    graceful shutdown: expecting >= {} on next load ({} written, {} deleted)",
                state.best_verified, state.next_key, newly_deleted
            );
        }

        if !graceful {
            // A hard kill RELEASES the bar; it does not merely lower it.
            //
            // The durability contract this campaign settled on is
            // asymmetric on purpose: a graceful shutdown must preserve
            // every acked write, while a crash may lose whatever sat inside
            // the group-commit window (10 ms / 1 MB of WAL). Carrying the
            // previous scan as a hard floor across a SIGKILL therefore
            // enforces the GRACEFUL contract on a crash, which the product
            // never promised -- and it duly reported a shortfall of ONE key
            // in 1,199,792 as a durability regression.
            //
            // So after a kill the next scan sets the bar, and the strong
            // "nothing that was ever served may vanish" invariant is
            // enforced exactly where the contract is strong: across
            // graceful boundaries.
            if newly_deleted > 0 || state.best_verified > 0 {
                println!(
                    "    hard kill: bar released (was {}, {} deleted); the next scan sets it",
                    state.best_verified, newly_deleted
                );
            }
            state.best_verified = 0;
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

/// Damage the tail of some on-disk files, the way a power cut does.
///
/// A kill leaves clean files: the process stops, but every byte it wrote
/// is intact. Real power loss does not do that -- it leaves half-written
/// records, tails of zeros where blocks were allocated but never written,
/// and occasionally bytes from nowhere. Those are precisely the inputs the
/// record checksums and truncate-at-tear rules exist for, and nothing in a
/// SIGKILL harness ever produces them.
///
/// Returns a description of what was damaged, for the cycle log.
fn mutilate_files(base_dir: &str, rand: &mut impl FnMut() -> u64) -> Vec<String> {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    let mut victims = Vec::new();
    for sub in ["wal", "undo", "backup"] {
        let dir = std::path::Path::new(base_dir).join(sub);
        let mut files = Vec::new();
        collect_files(&dir, &mut files);
        if files.is_empty() {
            continue;
        }
        let file = files[(rand() % files.len() as u64) as usize].clone();
        let Ok(len) = std::fs::metadata(&file).map(|m| m.len()) else {
            continue;
        };
        if len < 64 {
            continue;
        }
        let how = rand() % 3;
        let result = match how {
            // Truncate the tail: the record that was mid-write is cut off.
            0 => {
                let cut = 1 + rand() % (len / 4).max(1);
                OpenOptions::new()
                    .write(true)
                    .open(&file)
                    .and_then(|f| f.set_len(len - cut))
                    .map(|_| format!("truncated {} bytes", cut))
            }
            // Zero the tail: what an allocated-but-unwritten block reads back
            // as, and what a length-only check cannot tell from real data.
            1 => {
                let zeros = ((rand() % (len / 4).max(1)) + 1).min(len) as usize;
                OpenOptions::new()
                    .write(true)
                    .open(&file)
                    .and_then(|mut f| {
                        f.seek(SeekFrom::End(-(zeros as i64)))?;
                        f.write_all(&vec![0u8; zeros])
                    })
                    .map(|_| format!("zeroed last {} bytes", zeros))
            }
            // Scribble inside: same lengths, wrong bytes. Only a checksum
            // catches this one.
            _ => {
                let at = rand() % len;
                OpenOptions::new()
                    .write(true)
                    .open(&file)
                    .and_then(|mut f| {
                        f.seek(SeekFrom::Start(at))?;
                        f.write_all(&[0x5A, 0xA5, 0x5A, 0xA5])
                    })
                    .map(|_| format!("scribbled 4 bytes at {}", at))
            }
        };
        match result {
            Ok(what) => victims.push(format!(
                "{}: {}",
                file.file_name().unwrap_or_default().to_string_lossy(),
                what
            )),
            Err(e) => victims.push(format!("{}: mutilation failed: {}", sub, e)),
        }
    }
    victims
}

fn collect_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, out);
        } else if path.extension().map(|e| e == "nlog" || e == "nbackup").unwrap_or(false) {
            out.push(path);
        }
    }
}

// ----------------------------------------------------------------- child --

/// Minimal stderr logger for diagnostics: active only when NEB_CHURN_LOG is
/// set (to a level name), silent otherwise. env_logger is a dev-dependency,
/// so the bin carries its own ~20 lines.
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

fn child_main(args: &[String]) -> ExitCode {
    init_diag_logging();
    let addr = args.get(0).expect("addr").clone();
    let group = args.get(1).expect("group").clone();
    let base_dir = args.get(2).expect("base_dir").clone();
    let first = args.get(3).map(|s| s == "first").unwrap_or(false);
    let churn_secs: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(3600);
    let delete_rate: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(0);
    let delete_from: u64 = args.get(6).and_then(|s| s.parse().ok()).unwrap_or(0);
    let probe_high: u64 = args.get(7).and_then(|s| s.parse().ok()).unwrap_or(0);
    let txn_rate: u64 = args.get(8).and_then(|s| s.parse().ok()).unwrap_or(0);
    let txn_verify_to: u64 = args.get(9).and_then(|s| s.parse().ok()).unwrap_or(0);
    let txn_verify_from: u64 = args.get(10).and_then(|s| s.parse().ok()).unwrap_or(0);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(child_async(
        addr,
        group,
        base_dir,
        first,
        churn_secs,
        delete_rate,
        delete_from,
        probe_high,
        txn_rate,
        txn_verify_to,
        txn_verify_from,
    )) {
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
    delete_rate: u64,
    delete_from: u64,
    probe_high: u64,
    txn_rate: u64,
    txn_verify_to: u64,
    txn_verify_from: u64,
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
            // Sized for the INDEX, not the cells.
            //
            // Half a million padded cells is only ~120 MB, but each one also
            // writes ranged-index pages, and every flush rewrites the dirty
            // ones -- so page versions accumulate far faster than the cells
            // do, and the cleaner cannot always keep up under continuous
            // kill-and-restart churn. At 2 GB a run reliably hit "No space
            // left for chunk N" (5,982 times in one soak), which made index
            // write-back batches fail, which left the barrier unestablished,
            // which lost index entries -- and the harness reported that as a
            // durability REGRESSION when it was really the store being full.
            // Configurable so a deliberately-small run can still study that.
            chunk_size: std::env::var("NEB_CHURN_CHUNK_MB")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(256)
                * 1024
                * 1024,
            db_size: std::env::var("NEB_CHURN_DB_GB")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(16)
                * 1024
                * 1024
                * 1024,
            tiered_config: None,
            backup_storage: Some(dir.join("backup").to_string_lossy().into_owned()),
            wal_storage: Some(dir.join("wal").to_string_lossy().into_owned()),
            raft_storage: Some(dir.join("raft").to_string_lossy().into_owned()),
            index_enabled: true,
            services: vec![
                Service::Cell,
                Service::Query,
                Service::RangedIndexer,
                // The transaction lane needs a participant to talk to.
                Service::Transaction,
            ],
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
    // What recovery decided about transactions, every run. These counters
    // existed but nothing read them, which is the same as not having them:
    // the torn-transaction hunt would have been far shorter with this line.
    println!(
        "RECOVERY brackets_applied={} brackets_discarded={} damaged_entries={} leases_expired={}",
        neb::ram::recovery::BRACKETS_APPLIED.load(std::sync::atomic::Ordering::Relaxed),
        neb::ram::recovery::BRACKETS_DISCARDED.load(std::sync::atomic::Ordering::Relaxed),
        neb::ram::recovery::RECOVERY_DAMAGED_ENTRIES.load(std::sync::atomic::Ordering::Relaxed),
        neb::ram::chunk::TXN_LEASES_EXPIRED.load(std::sync::atomic::Ordering::Relaxed),
    );
    flush_stdout();
    println!("SCANNED n={}", count);
    flush_stdout();

    // A different question than the scanned count asks. That count only
    // notices loss that drops BELOW the previous best, so a hole refilled by
    // later inserts is invisible to it; this asks whether every cell still
    // in the store has the index entry its contents imply.
    //
    // REPORTED, NOT ENFORCED, deliberately. The Phase 0 contract makes
    // indexes rebuildable and never authoritative, so a crash is ALLOWED to
    // leave cells whose entries had not been inserted yet -- index insertion
    // is async and outside the bracket. Failing on missing>0 would therefore
    // manufacture failures out of contracted behaviour. What the line is for
    // is the size of the number: a handful after a kill is the expected lag,
    // thousands is a bug, and nothing in the harness could tell those apart
    // before. Same reasoning as the RECOVERY counters above -- a number
    // nothing reads is a number nobody has.
    if std::env::var("NEB_CHURN_SCRUB").as_deref() != Ok("0") {
        let began = std::time::Instant::now();
        match client.scrub_ranged_index(false).await {
            Ok(report) => println!(
                "SCRUB missing={} unreachable={} present={} derived={} cells={} took_ms={}",
                report.entries_missing,
                report.entries_unreachable,
                report.entries_present,
                report.entries_derived,
                report.cells_scanned,
                began.elapsed().as_millis()
            ),
            Err(e) => println!("SCRUB_ERROR {}", e),
        }
        flush_stdout();
    }
    // A store that ran out of room is a CONFIGURATION outcome, not a
    // durability one. Index write-back cannot allocate, its batches are
    // abandoned, the barrier is never established and entries are lost --
    // all correct behaviour for a full store, and all indistinguishable
    // from real loss unless the harness is told which it is looking at.
    if neb::ram::chunk::ALLOCATION_EXHAUSTED.load(std::sync::atomic::Ordering::Relaxed) > 0 {
        println!("STORE_EXHAUSTED");
        flush_stdout();
    }

    // Which layer lost it?
    //
    // The scan above counts RANGED INDEX entries. If keys go missing, that
    // alone cannot say whether the index lost entries for cells that are
    // still there, or the store lost the cells themselves -- two different
    // bugs with two different fixes. So sample the cell store directly for
    // keys the parent believes are durable, and report both numbers.
    if probe_high > delete_from + 1 {
        const SAMPLES: u64 = 400;
        let span = probe_high - delete_from;
        let stride = (span / SAMPLES).max(1);
        let mut present = 0u64;
        let mut probed = 0u64;
        let mut seq = delete_from;
        while seq < probe_high && probed < SAMPLES {
            let id = Id::from_parts(9 + (seq % 64), seq);
            if let Ok(Ok(_)) = client.read_cell(id).await {
                present += 1;
            }
            probed += 1;
            seq += stride;
        }
        println!("CELLS_PRESENT {}/{} below {}", present, probed, probe_high);
        flush_stdout();
    }

    // Atomicity, which is the whole reason the transaction lane exists.
    //
    // Every round the previous cycle reported committed must come back with
    // ALL of its cells; a round that was cut off mid-apply must come back
    // with NONE. A round that returns some but not all is a torn
    // transaction -- the one failure the plain lanes structurally cannot
    // produce, because a single-cell write has no partial state.
    if txn_verify_to >= txn_verify_from && txn_verify_from > 0 {
        const TXN_CELLS: u64 = 4;
        const TXN_SAMPLES: u64 = 200;
        // Only rounds a previous cycle actually reported as committed. Probing
        // outside that range counts never-attempted rounds as absent, which
        // looks exactly like lost transactions and is the harness lying.
        let span = txn_verify_to - txn_verify_from + 1;
        let stride = (span / TXN_SAMPLES).max(1);
        let mut whole = 0u64;
        let mut torn = 0u64;
        let mut missing = 0u64;
        let mut round = txn_verify_from;
        while round <= txn_verify_to {
            let base = round * TXN_CELLS;
            let mut found = 0u64;
            for i in 0..TXN_CELLS {
                let seq = base + i;
                let id = Id::from_parts(72 + (seq % 8), seq);
                if let Ok(Ok(_)) = client.read_cell(id).await {
                    found += 1;
                }
            }
            if found == TXN_CELLS {
                whole += 1;
            } else if found == 0 {
                missing += 1;
            } else {
                torn += 1;
                // WHICH cells are gone matters more than how many: the four
                // land in four different partitions, so the pattern says
                // whether one chunk lost a bracket or one position always
                // loses.
                let mut lost = Vec::new();
                for i in 0..TXN_CELLS {
                    let seq = base + i;
                    let id = Id::from_parts(72 + (seq % 8), seq);
                    if !matches!(client.read_cell(id).await, Ok(Ok(_))) {
                        lost.push(format!("i={} part={} seq={}", i, 72 + (seq % 8), seq));
                    }
                }
                println!(
                    "TXN_TORN round={} found={}/{} missing[{}] -- a committed transaction came \
                     back in pieces",
                    round,
                    found,
                    TXN_CELLS,
                    lost.join(", ")
                );
            }
            round += stride;
        }
        println!(
            "TXN_ATOMIC whole={} torn={} absent={} range={}..={}",
            whole, torn, missing, txn_verify_from, txn_verify_to
        );
        flush_stdout();
    }

    // Churn: writers append monotone keys; a reporter streams the acked
    // high-water mark. Padding gives pages realistic weight so splits come
    // fast. Keys continue from the scanned count -- duplicates with an
    // earlier incarnation's unacked tail are harmless upserts.
    let next = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(count));
    // The acked mark is the CONTIGUOUS prefix, not the high-water.
    //
    // Keys are handed out by `fetch_add` across several writers, so a write
    // can fail while a higher-numbered one succeeds -- at any interruption,
    // and especially once shutdown starts refusing writes. A high-water mark
    // then claims keys that were never written, and the parent reports a
    // regression of a few keys against a server that lost nothing. Only "every
    // key below this point is durable" is a property the store actually owes,
    // and holes above the cursor are the harness's business, not the store's.
    let acked = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(count));
    let pending_acks =
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::<u64>::new()));
    const WRITERS: usize = 4;
    let mut writer_handles = Vec::new();
    for _ in 0..WRITERS {
        let client = client.clone();
        let next = next.clone();
        let acked = acked.clone();
        let pending_acks = pending_acks.clone();
        writer_handles.push(tokio::spawn(async move {
            loop {
                let seq = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // Spread across partitions: every cell sharing one high part
                // lands in a single chunk, so the store fills at ~150k keys
                // no matter how large db_size is.
                let id = Id::from_parts(9 + (seq % 64), seq);
                let mut value = OwnedValue::Map(OwnedMap::new());
                value[KEY_FIELD] = OwnedValue::U64(seq);
                value[PAD_FIELD] = OwnedValue::String(format!("pad-{:0>200}", seq));
                let cell = OwnedCell::new_with_id(CHURN_SCHEMA_ID, &id, value);
                if let Ok(Ok(_)) = client.write_cell(cell).await {
                    // Advance the cursor over whatever contiguous run this
                    // completes; anything above it waits for its predecessor.
                    let mut pending = pending_acks.lock().unwrap();
                    pending.insert(seq);
                    let mut cursor = acked.load(std::sync::atomic::Ordering::Relaxed);
                    while pending.remove(&cursor) {
                        cursor += 1;
                    }
                    acked.store(cursor, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }));
    }
    // Transaction lane: multi-cell transactions, so kills land inside an
    // apply burst and inside the decision window.
    //
    // The plain writers above only ever exercise single-cell writes, which
    // is the one shape that needs no atomicity: nothing can be half-applied.
    // A transaction holds a write head from its first entry until its
    // decision, so this lane is what puts kills in the windows that lease
    // actually changes -- between two entries of one transaction, and
    // between the last entry and the commit.
    //
    // Keys live in their OWN partition range (72+) and are never counted
    // toward the contiguous ack cursor: a transaction is all-or-nothing, so
    // its keys are not a contiguous prefix of anything, and the parent's
    // arithmetic must not see them. What the lane asserts instead is the
    // property that matters and that no other lane can see: every committed
    // transaction's cells are ALL present after recovery, and an
    // uncommitted one leaves NONE.
    let txn_committed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut txn_handles = Vec::new();
    if txn_rate > 0 {
        const TXN_CELLS: u64 = 4;
        let client = client.clone();
        let txn_committed = txn_committed.clone();
        // Start well ABOVE anything a previous cycle could have written.
        //
        // Continuing from the last REPORTED round is not enough: reports are
        // sampled, so a cycle commits rounds past its last report and a kill
        // drops the rest. The next cycle then re-attempts rounds whose cells
        // already exist, and those transactions abort against committed data
        // -- which makes any residue look like a torn transaction when it is
        // really the harness colliding with itself. A gap costs nothing here
        // and keeps "torn" meaning exactly one thing.
        let start_round = txn_verify_to + 10_000;
        txn_handles.push(tokio::spawn(async move {
            let mut round: u64 = start_round;
            let mut reporting = true;
            loop {
                round += 1;
                let base = round * TXN_CELLS;
                let cells: Vec<OwnedCell> = (0..TXN_CELLS)
                    .map(|i| {
                        let seq = base + i;
                        let id = Id::from_parts(72 + (seq % 8), seq);
                        let mut value = OwnedValue::Map(OwnedMap::new());
                        value[KEY_FIELD] = OwnedValue::U64(seq);
                        value[PAD_FIELD] = OwnedValue::String(format!("txn-{:0>200}", seq));
                        OwnedCell::new_with_id(CHURN_SCHEMA_ID, &id, value)
                    })
                    .collect();
                let result = client
                    .transaction(move |txn| {
                        let cells = cells.clone();
                        async move {
                            for cell in cells {
                                txn.write(cell).await?;
                            }
                            Ok(())
                        }
                    })
                    .await;
                if let Err(ref e) = result {
                    if round % 50 == 1 {
                        println!("TXN_FAILED round={} {:?}", round, e);
                        flush_stdout();
                    }
                }
                if result.is_err() {
                    // Stop advancing the mark: it must mean "every round up
                    // to here committed", or the parent cannot tell a lost
                    // transaction from one that was never attempted.
                    reporting = false;
                }
                if result.is_ok() && reporting {
                    // Published AFTER the commit returns, so the count is a
                    // lower bound on what is durable -- the same one-sided
                    // discipline the ack cursor uses. A transaction whose
                    // commit was cut off by the kill is simply not counted,
                    // and its cells are expected to be absent.
                    let done = txn_committed.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    // A high-water mark, so reporting every commit is just
                    // noise the parent has to keep up with. Every eighth is
                    // frequent enough to lose almost nothing to a kill.
                    if done <= 4 || done % 8 == 0 {
                        println!("TXN_COMMITTED_TO {}", round);
                        flush_stdout();
                    }
                }
                if txn_rate < 100 {
                    tokio::time::sleep(Duration::from_millis(100 - txn_rate)).await;
                }
            }
        }));
    }

    // Delete lane: retire a contiguous prefix of the keys that already
    // exist, well behind the write cursor.
    //
    // Deletions are the shape no durability test had: a page whose keys are
    // all tombstoned persists with ZERO keys, and a run of those inside the
    // page chain is a legal on-disk state that reconstruction has to
    // tolerate. That exact state made a complete 179,423-key tree serve
    // nothing after a restart, and nothing here would have produced it,
    // because every writer only ever appended.
    //
    // The cursor is contiguous like the ack cursor, so the parent can do
    // exact arithmetic: live keys = acked writes - deleted prefix.
    let deleted = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(delete_from));
    let mut delete_handles = Vec::new();
    if delete_rate > 0 {
        let client = client.clone();
        let deleted = deleted.clone();
        let acked_for_delete = acked.clone();
        delete_handles.push(tokio::spawn(async move {
            loop {
                let cursor = deleted.load(std::sync::atomic::Ordering::Relaxed);
                // Stay a safe distance behind the durable write cursor:
                // deleting a key whose write has not been acked would race
                // its own creation.
                let safe_limit = acked_for_delete
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .saturating_sub(1_000);
                if cursor >= safe_limit {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                let id = Id::from_parts(9 + (cursor % 64), cursor);
                // Publish the INTENT, before the delete, so the reported
                // cursor is an upper bound on what has actually been
                // removed.
                //
                // Reporting after the fact made it a lower bound, and the
                // reporter only publishes every 100 ms: a SIGKILL landing
                // between a delete and its report left the store with fewer
                // keys than the parent's bar allowed for, which read as a
                // 7-key durability regression across 1.1 million. An
                // over-reported delete is harmless in the other direction --
                // the store simply holds MORE than expected, and the
                // invariant is one-sided.
                deleted.store(cursor + 1, std::sync::atomic::Ordering::Relaxed);
                match client.remove_cell(id).await {
                    // Gone is gone: a key an earlier incarnation deleted
                    // before dying is still progress for this cursor.
                    Ok(Ok(())) | Ok(Err(_)) => {}
                    Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
                }
                if delete_rate < 100 {
                    tokio::time::sleep(Duration::from_millis(
                        ((100 - delete_rate) / 10).max(1) as u64,
                    ))
                    .await;
                }
            }
        }));
    }

    let acked_reporter = acked.clone();
    let deleted_reporter = deleted.clone();
    tokio::spawn(async move {
        let mut last = 0;
        let mut last_deleted = u64::MAX;
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let now = acked_reporter.load(std::sync::atomic::Ordering::Relaxed);
            if now != last {
                println!("ACK_TO {}", now);
                last = now;
            }
            let gone = deleted_reporter.load(std::sync::atomic::Ordering::Relaxed);
            if gone != last_deleted {
                println!("DELETED_TO {}", gone);
                last_deleted = gone;
            }
            flush_stdout();
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
            // Deleters are ingress too: leaving them running through the
            // shutdown keeps issuing writes at a server that is trying to
            // stop, which is not what a real deployment does.
            for handle in writer_handles
                .iter()
                .chain(delete_handles.iter())
                .chain(txn_handles.iter())
            {
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
