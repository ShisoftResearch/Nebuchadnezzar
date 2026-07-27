use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

fn parent_directory(path: &Path) -> Option<&Path> {
    path.parent().map(|parent| {
        if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        }
    })
}

/// Create a directory tree and durably publish every newly-created directory
/// entry before returning success.
pub(crate) fn ensure_directory(path: &Path) -> io::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    if path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} exists but is not a directory", path.display()),
        ));
    }

    let mut missing = Vec::new();
    let mut cursor = path;
    while !cursor.exists() {
        missing.push(cursor.to_path_buf());
        let parent = cursor.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("directory {} has no existing ancestor", path.display()),
            )
        })?;
        if parent.as_os_str().is_empty() {
            break;
        }
        cursor = parent;
    }

    for directory in missing.iter().rev() {
        let created = match fs::create_dir(directory) {
            Ok(()) => true,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && directory.is_dir() => {
                false
            }
            Err(error) => return Err(error),
        };
        if let Some(parent) = parent_directory(directory) {
            if let Err(error) = sync_directory(parent) {
                // No durable file can have been created below this directory
                // yet. Remove the uncertain entry so a caller can safely retry
                // the publication instead of taking the existing-dir fast path.
                if created {
                    let _ = fs::remove_dir(directory);
                }
                // A concurrent creator may have populated the directory.
                // Best-effort publication keeps that race from stranding an
                // existing but unsynced entry.
                let _ = sync_directory(parent);
                return Err(error);
            }
        }
    }
    Ok(())
}

/// Open a read/write file, creating and durably publishing its directory entry
/// when it does not already exist.
pub(crate) fn open_or_create(path: &Path, truncate_existing: bool) -> io::Result<File> {
    open_or_create_with(path, truncate_existing, false)
}

/// Open an append-only file, creating and durably publishing its directory
/// entry when needed.
pub(crate) fn open_or_create_append(path: &Path) -> io::Result<File> {
    open_or_create_with(path, false, true)
}

fn open_or_create_with(path: &Path, truncate_existing: bool, append: bool) -> io::Result<File> {
    if let Some(parent) = parent_directory(path) {
        ensure_directory(parent)?;
    }

    let mut new_options = OpenOptions::new();
    new_options
        .read(true)
        .write(!append)
        .append(append)
        .create_new(true);
    match new_options.open(path) {
        Ok(file) => {
            #[cfg(test)]
            record_event_for_test(DurabilityEvent::FileCreated(path.to_path_buf()));
            if let Err(error) = sync_parent(path) {
                drop(file);
                let _ = fs::remove_file(path);
                let _ = sync_parent(path);
                return Err(error);
            }
            Ok(file)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let mut existing_options = OpenOptions::new();
            existing_options
                .read(true)
                .write(!append)
                .append(append)
                .truncate(truncate_existing);
            existing_options.open(path)
        }
        Err(error) => Err(error),
    }
}

/// Remove a file and durably publish the removal. A missing file is already in
/// the requested state.
pub(crate) fn remove_file(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            #[cfg(test)]
            record_event_for_test(DurabilityEvent::FileRemoved(path.to_path_buf()));
            sync_parent(path)
        }
        // A prior unlink may have succeeded while its directory sync failed.
        // Re-syncing on a missing retry makes that error recoverable.
        Err(error) if error.kind() == io::ErrorKind::NotFound => sync_parent(path),
        Err(error) => Err(error),
    }
}

/// Rename a file and durably publish the changed directory entries.
pub(crate) fn rename(from: &Path, to: &Path) -> io::Result<()> {
    if let Some(parent) = parent_directory(to) {
        ensure_directory(parent)?;
    }
    #[cfg(test)]
    if should_fail_rename_for_test(to) {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("injected rename failure for {}", to.to_string_lossy()),
        ));
    }
    fs::rename(from, to)?;
    #[cfg(test)]
    record_event_for_test(DurabilityEvent::FileRenamed {
        from: from.to_path_buf(),
        to: to.to_path_buf(),
    });
    #[cfg(test)]
    if should_fail_rename_directory_sync_for_test(to) {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "injected directory sync failure after rename to {}",
                to.to_string_lossy()
            ),
        ));
    }

    let from_parent = parent_directory(from);
    let to_parent = parent_directory(to);
    if let Some(parent) = to_parent {
        sync_directory(parent)?;
    }
    if from_parent != to_parent {
        if let Some(parent) = from_parent {
            sync_directory(parent)?;
        }
    }
    Ok(())
}

pub(crate) fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = parent_directory(path).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path {} has no parent directory", path.display()),
        )
    })?;
    sync_directory(parent)
}

pub(crate) fn sync_file(file: &File, path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if should_fail_file_sync_for_test(path) {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("injected file sync failure for {}", path.to_string_lossy()),
        ));
    }
    file.sync_all()?;
    #[cfg(test)]
    record_event_for_test(DurabilityEvent::FileSynced(path.to_path_buf()));
    Ok(())
}

pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if should_fail_directory_sync_for_test(path) {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "injected directory sync failure for {}",
                path.to_string_lossy()
            ),
        ));
    }

    File::open(path)?.sync_all()?;

    #[cfg(test)]
    record_directory_sync_for_test(path);

    Ok(())
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DurabilityEvent {
    FileCreated(PathBuf),
    FileSynced(PathBuf),
    FileRemoved(PathBuf),
    FileRenamed { from: PathBuf, to: PathBuf },
    DirectorySynced(PathBuf),
}

#[cfg(test)]
#[derive(Default)]
struct TestDurabilityState {
    directory_syncs: std::collections::HashMap<PathBuf, usize>,
    failed_directory_syncs: std::collections::HashMap<PathBuf, std::collections::VecDeque<usize>>,
    failed_file_syncs: std::collections::HashMap<PathBuf, usize>,
    failed_renames: std::collections::HashMap<PathBuf, usize>,
    failed_rename_directory_syncs: std::collections::HashMap<PathBuf, usize>,
    events: Vec<DurabilityEvent>,
}

#[cfg(test)]
fn test_durability_state() -> &'static std::sync::Mutex<TestDurabilityState> {
    static STATE: std::sync::OnceLock<std::sync::Mutex<TestDurabilityState>> =
        std::sync::OnceLock::new();
    STATE.get_or_init(|| std::sync::Mutex::new(TestDurabilityState::default()))
}

#[cfg(test)]
fn should_fail_directory_sync_for_test(path: &Path) -> bool {
    let mut state = test_durability_state().lock().unwrap();
    let Some(scheduled) = state.failed_directory_syncs.get_mut(path) else {
        return false;
    };
    let Some(remaining) = scheduled.front_mut() else {
        return false;
    };
    if *remaining > 0 {
        *remaining -= 1;
        return false;
    }
    scheduled.pop_front();
    true
}

#[cfg(test)]
fn take_scheduled_path_failure(
    scheduled: &mut std::collections::HashMap<PathBuf, usize>,
    path: &Path,
) -> bool {
    let Some(remaining) = scheduled.get_mut(path) else {
        return false;
    };
    *remaining -= 1;
    if *remaining == 0 {
        scheduled.remove(path);
    }
    true
}

#[cfg(test)]
fn should_fail_file_sync_for_test(path: &Path) -> bool {
    take_scheduled_path_failure(
        &mut test_durability_state().lock().unwrap().failed_file_syncs,
        path,
    )
}

#[cfg(test)]
fn should_fail_rename_for_test(path: &Path) -> bool {
    take_scheduled_path_failure(
        &mut test_durability_state().lock().unwrap().failed_renames,
        path,
    )
}

#[cfg(test)]
fn should_fail_rename_directory_sync_for_test(path: &Path) -> bool {
    take_scheduled_path_failure(
        &mut test_durability_state()
            .lock()
            .unwrap()
            .failed_rename_directory_syncs,
        path,
    )
}

#[cfg(test)]
fn record_directory_sync_for_test(path: &Path) {
    let mut state = test_durability_state().lock().unwrap();
    *state.directory_syncs.entry(path.to_path_buf()).or_default() += 1;
    state
        .events
        .push(DurabilityEvent::DirectorySynced(path.to_path_buf()));
}

#[cfg(test)]
fn record_event_for_test(event: DurabilityEvent) {
    test_durability_state().lock().unwrap().events.push(event);
}

#[cfg(test)]
pub(crate) fn directory_sync_count_for_test(path: &Path) -> usize {
    test_durability_state()
        .lock()
        .unwrap()
        .directory_syncs
        .get(path)
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn fail_next_directory_sync_for_test(path: &Path) {
    test_durability_state()
        .lock()
        .unwrap()
        .failed_directory_syncs
        .entry(path.to_path_buf())
        .or_default()
        .push_back(0);
}

#[cfg(test)]
pub(crate) fn fail_directory_sync_after_for_test(path: &Path, successful_syncs: usize) {
    test_durability_state()
        .lock()
        .unwrap()
        .failed_directory_syncs
        .entry(path.to_path_buf())
        .or_default()
        .push_back(successful_syncs);
}

#[cfg(test)]
pub(crate) fn fail_next_file_sync_for_test(path: &Path) {
    *test_durability_state()
        .lock()
        .unwrap()
        .failed_file_syncs
        .entry(path.to_path_buf())
        .or_default() += 1;
}

#[cfg(test)]
pub(crate) fn fail_next_rename_for_test(path: &Path) {
    *test_durability_state()
        .lock()
        .unwrap()
        .failed_renames
        .entry(path.to_path_buf())
        .or_default() += 1;
}

#[cfg(test)]
pub(crate) fn fail_next_rename_directory_sync_for_test(path: &Path) {
    *test_durability_state()
        .lock()
        .unwrap()
        .failed_rename_directory_syncs
        .entry(path.to_path_buf())
        .or_default() += 1;
}

#[cfg(test)]
pub(crate) fn durability_events_for_test() -> Vec<DurabilityEvent> {
    test_durability_state().lock().unwrap().events.clone()
}

#[cfg(test)]
mod tests {
    use super::{ensure_directory, fail_next_directory_sync_for_test};
    use std::path::PathBuf;

    #[test]
    fn failed_new_directory_publication_can_be_retried_safely() {
        let temp = tempfile::TempDir::new().expect("temp directory should be created");
        let durable_root = temp.path().join("durable-root");
        fail_next_directory_sync_for_test(temp.path());

        ensure_directory(&durable_root)
            .expect_err("an injected parent sync failure must reject directory publication");
        assert!(
            !durable_root.exists(),
            "the unsynced directory entry must be removed before returning the failure"
        );

        ensure_directory(&durable_root)
            .expect("directory publication should succeed on a clean retry");
        assert!(durable_root.is_dir());
    }

    #[test]
    fn bare_relative_directory_is_durably_created() {
        let relative = PathBuf::from(format!(
            ".neb-durable-relative-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should follow the Unix epoch")
                .as_nanos()
        ));

        ensure_directory(&relative).expect("bare relative storage path should be supported");
        assert!(relative.is_dir());
        std::fs::remove_dir(&relative).expect("relative test directory should be removable");
    }
}
