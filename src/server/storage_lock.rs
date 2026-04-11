use super::database::DatabaseStorageLayout;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

const LOCK_FILE_NAME: &str = ".neb.lock";

#[derive(Debug)]
pub struct StorageDirectoryLocks {
    _guards: Vec<DirectoryLockGuard>,
}

#[derive(Debug)]
struct DirectoryLockGuard {
    _directory: PathBuf,
    _lock_path: PathBuf,
    _file: File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageLockError {
    Io {
        directory: PathBuf,
        source: String,
    },
    AlreadyLocked {
        directory: PathBuf,
        lock_path: PathBuf,
        pid: Option<u32>,
    },
}

impl Display for StorageLockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageLockError::Io { directory, source } => {
                write!(f, "failed to lock storage directory {}: {source}", directory.display())
            }
            StorageLockError::AlreadyLocked {
                directory,
                lock_path,
                pid,
            } => {
                if let Some(pid) = pid {
                    write!(
                        f,
                        "storage directory {} is already locked by pid {} (lock file: {})",
                        directory.display(),
                        pid,
                        lock_path.display()
                    )
                } else {
                    write!(
                        f,
                        "storage directory {} is already locked by another process (lock file: {})",
                        directory.display(),
                        lock_path.display()
                    )
                }
            }
        }
    }
}

impl std::error::Error for StorageLockError {}

impl StorageDirectoryLocks {
    pub fn acquire(layout: &DatabaseStorageLayout) -> Result<Self, StorageLockError> {
        if should_skip_storage_locks() {
            return Ok(Self { _guards: Vec::new() });
        }

        Self::acquire_impl(layout)
    }

    fn acquire_impl(layout: &DatabaseStorageLayout) -> Result<Self, StorageLockError> {
        let mut directories = BTreeSet::new();
        directories.extend(layout.backup_storage.iter().cloned());
        directories.extend(layout.wal_storage.iter().cloned());
        directories.extend(layout.undo_log_storage.iter().cloned());
        directories.extend(layout.raft_storage.iter().cloned());

        let mut guards = Vec::with_capacity(directories.len());
        for directory in directories {
            guards.push(DirectoryLockGuard::acquire(Path::new(&directory))?);
        }

        Ok(Self { _guards: guards })
    }
}

fn should_skip_storage_locks() -> bool {
    cfg!(test)
}

impl DirectoryLockGuard {
    fn acquire(directory: &Path) -> Result<Self, StorageLockError> {
        std::fs::create_dir_all(directory).map_err(|e| StorageLockError::Io {
            directory: directory.to_path_buf(),
            source: e.to_string(),
        })?;

        let lock_path = directory.join(LOCK_FILE_NAME);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| StorageLockError::Io {
                directory: directory.to_path_buf(),
                source: e.to_string(),
            })?;

        match try_lock_exclusive(&file) {
            Ok(()) => {
                let previous_pid = read_lock_pid(&mut file).map_err(|e| StorageLockError::Io {
                    directory: directory.to_path_buf(),
                    source: e.to_string(),
                })?;
                if let Some(pid) = previous_pid {
                    if pid != std::process::id() && !is_pid_running(pid) {
                        info!(
                            "Reclaiming stale storage lock at {} from dead pid {}",
                            lock_path.display(),
                            pid
                        );
                    }
                }

                write_lock_pid(&mut file, directory).map_err(|e| StorageLockError::Io {
                    directory: directory.to_path_buf(),
                    source: e.to_string(),
                })?;

                Ok(Self {
                    _directory: directory.to_path_buf(),
                    _lock_path: lock_path,
                    _file: file,
                })
            }
            Err(err) if is_lock_contention(&err) => {
                let pid = read_lock_pid(&mut file).ok().flatten();
                Err(StorageLockError::AlreadyLocked {
                    directory: directory.to_path_buf(),
                    lock_path,
                    pid,
                })
            }
            Err(err) => Err(StorageLockError::Io {
                directory: directory.to_path_buf(),
                source: err.to_string(),
            }),
        }
    }
}

fn read_lock_pid(file: &mut File) -> io::Result<Option<u32>> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let contents = String::from_utf8_lossy(&bytes);

    for line in contents.lines() {
        let candidate = line.trim();
        let candidate = candidate.strip_prefix("pid=").unwrap_or(candidate);
        if let Ok(pid) = candidate.parse::<u32>() {
            return Ok(Some(pid));
        }
    }

    Ok(None)
}

fn write_lock_pid(file: &mut File, directory: &Path) -> io::Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    writeln!(file, "pid={}", std::process::id())?;
    writeln!(file, "directory={}", directory.display())?;
    file.sync_all()
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> io::Result<()> {
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn try_lock_exclusive(_file: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "filesystem locking is only supported on unix hosts",
    ))
}

fn is_lock_contention(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN
    )
}

#[cfg(unix)]
fn is_pid_running(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }

    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if result == 0 {
        return true;
    }

    !matches!(io::Error::last_os_error().raw_os_error(), Some(code) if code == libc::ESRCH)
}

#[cfg(not(unix))]
fn is_pid_running(_pid: u32) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::{read_lock_pid, DirectoryLockGuard, StorageDirectoryLocks, LOCK_FILE_NAME};
    use crate::server::database::DatabaseStorageLayout;
    use std::fs::OpenOptions;

    fn layout_with_raft_path(path: &std::path::Path) -> DatabaseStorageLayout {
        DatabaseStorageLayout {
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: Some(path.to_string_lossy().to_string()),
        }
    }

    #[test]
    fn lock_file_records_current_pid() {
        let temp = tempfile::TempDir::new().expect("tempdir should be created");
        let storage_path = temp.path().join("raft");

        let _locks = StorageDirectoryLocks::acquire_impl(&layout_with_raft_path(&storage_path))
            .expect("lock should be acquired");

        let contents = std::fs::read_to_string(storage_path.join(LOCK_FILE_NAME))
            .expect("lock file should be readable");
        assert!(contents.contains(&format!("pid={}", std::process::id())));
    }

    #[test]
    fn stale_pid_metadata_is_rewritten() {
        let temp = tempfile::TempDir::new().expect("tempdir should be created");
        let storage_path = temp.path().join("raft");
        std::fs::create_dir_all(&storage_path).expect("storage path should be created");
        std::fs::write(storage_path.join(LOCK_FILE_NAME), b"pid=999999\n")
            .expect("stale lock file should be created");

        let _guard = DirectoryLockGuard::acquire(&storage_path)
            .expect("stale pid should not block lock acquisition");

        let contents = std::fs::read_to_string(storage_path.join(LOCK_FILE_NAME))
            .expect("lock file should be readable");
        assert!(contents.contains(&format!("pid={}", std::process::id())));
    }

    #[test]
    fn second_acquire_reports_existing_pid() {
        let temp = tempfile::TempDir::new().expect("tempdir should be created");
        let storage_path = temp.path().join("raft");
        let _first = DirectoryLockGuard::acquire(&storage_path)
            .expect("first lock acquisition should succeed");

        let error = DirectoryLockGuard::acquire(&storage_path)
            .expect_err("second lock acquisition should fail");
        match error {
            super::StorageLockError::AlreadyLocked { pid, .. } => {
                assert_eq!(pid, Some(std::process::id()));
            }
            other => panic!("unexpected lock error: {other}"),
        }
    }

    #[test]
    fn read_lock_pid_parses_pid_prefix() {
        let temp = tempfile::TempDir::new().expect("tempdir should be created");
        let lock_path = temp.path().join(LOCK_FILE_NAME);
        std::fs::write(&lock_path, b"pid=1234\n").expect("lock file should be written");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .expect("lock file should open");

        assert_eq!(read_lock_pid(&mut file).expect("pid should parse"), Some(1234));
    }
}