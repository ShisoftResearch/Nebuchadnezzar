//! Process-scoped filesystem paths for tests.
//!
//! Tests used to write fixtures to hardcoded `/tmp/...` literals, which has
//! three problems. The paths ignore `TMPDIR`, so a run cannot be redirected
//! away from a small or quota-limited `/tmp` — and these tests write 8 MiB
//! segments, so a full tiered suite moves hundreds of megabytes through it. The
//! names are fixed, so two users on a shared machine, or two test binaries in
//! parallel, collide on the same directory. And a panicking test strands its
//! fixture under a name the next run reuses, which then starts against dirty
//! state.
//!
//! Paths here live under `std::env::temp_dir()` and carry the process id, so
//! concurrent runs never share one. They stay stable within a process, so the
//! common pattern of naming a directory at setup and removing it at teardown
//! still refers to the same place.

use std::path::PathBuf;

/// Path under the platform temp directory for `label`, unique to this process.
///
/// Repeated calls with the same `label` in one process return the same path.
pub fn temp_path(label: &str) -> String {
    temp_path_buf(label).to_string_lossy().into_owned()
}

/// [`temp_path`] as a `PathBuf`.
pub fn temp_path_buf(label: &str) -> PathBuf {
    sweep_stale_fixtures_once();
    let mut path = std::env::temp_dir();
    path.push(format!("neb-{}-{}", std::process::id(), label));
    path
}

fn sweep_stale_fixtures_once() {
    static SWEPT: std::sync::Once = std::sync::Once::new();
    SWEPT.call_once(|| {
        let removed = sweep_stale_fixtures();
        if removed > 0 {
            // Not a warning: finding leftovers is the normal case, because the
            // runs that leave them are the ones that failed.
            log::info!("Removed {} stranded test fixture director(ies)", removed);
        }
    });
}

/// Remove `neb-<pid>-*` fixtures whose owning process is gone.
///
/// Naming by pid stops concurrent runs from colliding, but it also means no
/// run ever reuses a name -- so nothing is ever overwritten and every crashed
/// or killed test leaks its fixture permanently. On this machine that reached
/// **554 stranded directories and filled a 15 GB tmpfs to its quota**, at
/// which point writes across the whole box began failing with
/// `disk quota exceeded`. A suite whose failures accumulate until unrelated
/// software breaks is a suite that has to clean up after itself, and teardown
/// cannot do it: the runs that leak are precisely the ones that never reach
/// their teardown.
///
/// Sweeping at startup instead is what makes it reliable -- it collects the
/// damage from runs that are already over, including ones killed by SIGKILL,
/// which no in-process handler can catch.
///
/// Liveness decides, never age: a directory is removed only when its pid has
/// no process. `kill(pid, 0)` failing with `EPERM` means the process exists
/// and is someone else's, so that fixture is KEPT. Pid reuse can therefore
/// spare a dead fixture, which costs one directory until the next sweep;
/// deleting a live run's fixture would corrupt a concurrent test, so the bias
/// runs that way deliberately.
pub fn sweep_stale_fixtures() -> usize {
    let root = std::env::temp_dir();
    let Ok(entries) = std::fs::read_dir(&root) else {
        return 0;
    };
    let own = std::process::id();
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(pid) = fixture_owner_pid(&name) else {
            continue;
        };
        if pid == own || process_exists(pid) {
            continue;
        }
        if std::fs::remove_dir_all(entry.path()).is_ok() {
            removed += 1;
        }
    }
    removed
}

/// The pid in `neb-<pid>-<label>`, or None if the name is not one of ours.
///
/// Matched structurally rather than by prefix alone: a directory called
/// `neb-something` that carries no pid is not ours to delete.
fn fixture_owner_pid(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("neb-")?;
    let (digits, remainder) = rest.split_at(rest.find('-')?);
    if remainder.len() < 2 || digits.is_empty() {
        return None;
    }
    digits.parse::<u32>().ok()
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    // The cast is guarded because `kill` reads NEGATIVE pids as broadcasts:
    // -1 is "every process you may signal" and -N is "process group N". A u32
    // above i32::MAX casts straight into that space, so an absurd directory
    // name like `neb-4294967295-x` would call kill(-1, 0), get success, and
    // report the process alive. It cost this test one failure to find; with a
    // real signal in place of 0 it would have cost far more.
    if pid == 0 || pid > i32::MAX as u32 {
        return true; // not a pid we can ask about: keep the directory
    }
    // ESRCH -- and only ESRCH -- means gone. EPERM means it exists under
    // another user, which is still alive.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 || *libc::__errno_location() == libc::EPERM }
}

#[cfg(not(unix))]
fn process_exists(_pid: u32) -> bool {
    // Without a cheap liveness check, keep everything: leaking a directory is
    // survivable, deleting a running test's fixture is not.
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_label_is_stable_within_a_process() {
        // Setup and teardown name the directory separately; they must agree.
        assert_eq!(temp_path("fixture"), temp_path("fixture"));
    }

    #[test]
    fn different_labels_do_not_collide() {
        assert_ne!(temp_path("backup"), temp_path("wal"));
    }

    #[test]
    fn path_is_under_the_platform_temp_dir_and_carries_the_pid() {
        let p = temp_path_buf("probe");
        assert!(p.starts_with(std::env::temp_dir()));
        assert!(p
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains(&std::process::id().to_string()));
    }

    /// The sweep must recognise our own names and nothing else. Getting this
    /// wrong deletes another program's temp directory.
    #[test]
    fn only_our_own_fixture_names_are_claimed() {
        assert_eq!(fixture_owner_pid("neb-1234-backup"), Some(1234));
        assert_eq!(fixture_owner_pid("neb-7-a"), Some(7));
        // Not ours: no pid, no label, or not our prefix at all.
        assert_eq!(fixture_owner_pid("neb-backup"), None);
        assert_eq!(fixture_owner_pid("neb-1234-"), None);
        assert_eq!(fixture_owner_pid("neb-"), None);
        assert_eq!(fixture_owner_pid("nebula-1234-x"), None);
        assert_eq!(fixture_owner_pid("something-else"), None);
    }

    /// `kill` reads negative pids as broadcasts, so a u32 that casts negative
    /// must never reach it. Asserted directly because the consequence is
    /// invisible at this call site -- `kill(-1, 0)` simply returns success --
    /// and only turns dangerous if someone later passes a real signal.
    #[cfg(unix)]
    #[test]
    fn pids_that_would_cast_into_broadcast_range_are_never_asked_about() {
        assert!(process_exists(u32::MAX), "-1 would broadcast to all processes");
        assert!(process_exists(i32::MAX as u32 + 1), "casts negative");
        assert!(process_exists(0), "pid 0 is the caller's process group");
        // A real, live pid still answers truthfully.
        assert!(process_exists(std::process::id()));
    }

    /// A dead process's fixture goes; a live one's stays. The second half is
    /// the one that matters -- a sweep that took a running test's directory
    /// would corrupt it mid-run.
    #[test]
    fn the_sweep_spares_live_fixtures_and_takes_dead_ones() {
        let root = std::env::temp_dir();
        // Pid 1 always exists. The dead one is a VALID positive pid_t that is
        // far above any real pid_max -- not u32::MAX, which is a broadcast
        // once cast and would be kept by the guard in `process_exists`.
        const DEAD_PID: u32 = 0x7FFF_FFFE;
        let live = root.join("neb-1-sweep_probe_live");
        let dead = root.join(format!("neb-{}-sweep_probe_dead", DEAD_PID));
        let ours = temp_path_buf("sweep_probe_ours");
        for d in [&live, &dead, &ours] {
            std::fs::create_dir_all(d).unwrap();
        }

        sweep_stale_fixtures();

        assert!(live.exists(), "a live process's fixture was deleted");
        assert!(ours.exists(), "this process's own fixture was deleted");
        assert!(!dead.exists(), "a dead process's fixture was left behind");

        let _ = std::fs::remove_dir_all(&live);
        let _ = std::fs::remove_dir_all(&ours);
    }

    #[test]
    fn honours_tmpdir_rather_than_assuming_slash_tmp() {
        // The whole point: a run must be redirectable off a quota-limited /tmp.
        // env::temp_dir() reads TMPDIR on unix, so assert we go through it.
        assert!(temp_path_buf("x").starts_with(std::env::temp_dir()));
    }
}
