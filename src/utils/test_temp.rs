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
    let mut path = std::env::temp_dir();
    path.push(format!("neb-{}-{}", std::process::id(), label));
    path
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

    #[test]
    fn honours_tmpdir_rather_than_assuming_slash_tmp() {
        // The whole point: a run must be redirectable off a quota-limited /tmp.
        // env::temp_dir() reads TMPDIR on unix, so assert we go through it.
        assert!(temp_path_buf("x").starts_with(std::env::temp_dir()));
    }
}
