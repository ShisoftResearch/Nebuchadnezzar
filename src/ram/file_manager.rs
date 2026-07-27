use crate::ram::compression;
use crate::ram::durable_fs;
use std::fs::{self, create_dir_all, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Unified file manager for segment file operations
/// Centralizes all file path generation, directory management, and file I/O
pub struct SegmentFileManager {
    backup_storage: Option<String>,
    wal_storage: Option<String>,
}

pub(crate) struct StagedBackupFile {
    file: File,
    staging_path: PathBuf,
    final_path: PathBuf,
}

impl StagedBackupFile {
    pub(crate) fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

impl SegmentFileManager {
    pub fn new(backup_storage: Option<String>, wal_storage: Option<String>) -> Self {
        Self {
            backup_storage: backup_storage.map(|p| Self::normalize_path(&p)),
            wal_storage: wal_storage.map(|p| Self::normalize_path(&p)),
        }
    }

    fn normalize_path(path: &str) -> String {
        let normalized = PathBuf::from(path)
            .components()
            .collect::<PathBuf>()
            .to_string_lossy()
            .into_owned();

        if normalized != path {
            warn!(
                "Storage path normalized: '{}' -> '{}' (check config for double slashes)",
                path, normalized
            );
        }

        normalized
    }

    /// Get backup storage path
    pub fn backup_storage(&self) -> Option<&str> {
        self.backup_storage.as_deref()
    }

    /// Get WAL storage path
    pub fn wal_storage(&self) -> Option<&str> {
        self.wal_storage.as_deref()
    }

    /// Initialize storage directories
    pub fn init_directories(&self) -> io::Result<()> {
        if let Some(backup_storage) = &self.backup_storage {
            durable_fs::ensure_directory(Path::new(backup_storage))?;
        }
        if let Some(wal_storage) = &self.wal_storage {
            durable_fs::ensure_directory(Path::new(wal_storage))?;
        }
        Ok(())
    }

    /// Generate backup file path for a segment
    pub fn backup_path(&self, chunk_id: usize, seg_id: u64, seq_id: u64) -> Option<String> {
        self.backup_storage
            .as_ref()
            .map(|path| format!("{}/{}-{}-{}.nbackup", path, chunk_id, seg_id, seq_id))
    }

    /// Generate WAL file path for a segment
    pub fn wal_path(&self, chunk_id: usize, seg_id: u64, seq_id: u64) -> Option<String> {
        self.wal_storage
            .as_ref()
            .map(|path| format!("{}/{}-{}-{}.nlog", path, chunk_id, seg_id, seq_id))
    }

    /// Create a WAL file (unbuffered for memory efficiency)
    pub fn create_wal_file(
        &self,
        chunk_id: usize,
        seg_id: u64,
        seq_id: u64,
    ) -> io::Result<Option<File>> {
        if let Some(wal_path) = self.wal_path(chunk_id, seg_id, seq_id) {
            let mut file = durable_fs::open_or_create(Path::new(&wal_path), false)?;
            file.seek(SeekFrom::End(0))?;
            Ok(Some(file))
        } else {
            Ok(None)
        }
    }

    /// Open an existing backup file for reading
    pub fn open_backup_file(
        &self,
        chunk_id: usize,
        seg_id: u64,
        seq_id: u64,
    ) -> io::Result<Option<File>> {
        if let Some(backup_path) = self.backup_path(chunk_id, seg_id, seq_id) {
            if Path::new(&backup_path).exists() {
                Ok(Some(File::open(&backup_path)?))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) fn stage_backup_file(
        &self,
        chunk_id: usize,
        seg_id: u64,
        seq_id: u64,
    ) -> io::Result<Option<StagedBackupFile>> {
        let Some(final_path) = self.backup_path(chunk_id, seg_id, seq_id) else {
            return Ok(None);
        };
        let final_path = PathBuf::from(final_path);
        let staging_path = PathBuf::from(format!("{}.staging", final_path.display()));
        let file = durable_fs::open_or_create(&staging_path, true)?;
        Ok(Some(StagedBackupFile {
            file,
            staging_path,
            final_path,
        }))
    }

    pub(crate) fn publish_staged_backup(&self, staged: StagedBackupFile) -> io::Result<()> {
        let StagedBackupFile {
            file,
            staging_path,
            final_path,
        } = staged;
        durable_fs::sync_file(&file, &staging_path)?;
        drop(file);
        durable_fs::rename(&staging_path, &final_path)
    }

    /// Open an existing WAL file for reading
    pub fn open_wal_file(
        &self,
        chunk_id: usize,
        seg_id: u64,
        seq_id: u64,
    ) -> io::Result<Option<File>> {
        if let Some(wal_path) = self.wal_path(chunk_id, seg_id, seq_id) {
            if Path::new(&wal_path).exists() {
                Ok(Some(File::open(&wal_path)?))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Check if backup file exists
    pub fn backup_exists(&self, chunk_id: usize, seg_id: u64, seq_id: u64) -> bool {
        self.backup_path(chunk_id, seg_id, seq_id)
            .map(|path| Path::new(&path).exists())
            .unwrap_or(false)
    }

    /// Check if WAL file exists
    pub fn wal_exists(&self, chunk_id: usize, seg_id: u64, seq_id: u64) -> bool {
        self.wal_path(chunk_id, seg_id, seq_id)
            .map(|path| Path::new(&path).exists())
            .unwrap_or(false)
    }

    /// Delete backup file
    pub fn delete_backup(&self, chunk_id: usize, seg_id: u64, seq_id: u64) -> io::Result<()> {
        if let Some(backup_path) = self.backup_path(chunk_id, seg_id, seq_id) {
            let path = Path::new(&backup_path);
            durable_fs::remove_file(path)?;
        }
        Ok(())
    }

    /// Delete WAL file
    pub fn delete_wal(&self, chunk_id: usize, seg_id: u64, seq_id: u64) -> io::Result<()> {
        if let Some(wal_path) = self.wal_path(chunk_id, seg_id, seq_id) {
            let path = Path::new(&wal_path);
            durable_fs::remove_file(path)?;
        }
        Ok(())
    }

    /// Delete both backup and WAL files
    pub fn delete_all(&self, chunk_id: usize, seg_id: u64, seq_id: u64) -> io::Result<()> {
        self.delete_backup(chunk_id, seg_id, seq_id)?;
        self.delete_wal(chunk_id, seg_id, seq_id)?;
        Ok(())
    }

    /// Copy WAL to backup with optional padding
    pub fn copy_wal_to_backup(
        &self,
        chunk_id: usize,
        seg_id: u64,
        seq_id: u64,
        pad_to_size: Option<usize>,
    ) -> io::Result<bool> {
        let wal_path = match self.wal_path(chunk_id, seg_id, seq_id) {
            Some(path) => path,
            None => return Ok(false),
        };

        // Check if WAL exists
        let wal_path_ref = Path::new(&wal_path);
        if !wal_path_ref.exists() {
            return Ok(false);
        }

        // Read WAL file
        let mut wal_file = File::open(&wal_path)?;
        let mut wal_data = Vec::new();
        wal_file.read_to_end(&mut wal_data)?;
        let wal_size = wal_data.len();

        // Write an ignored staging name. Only a fully synced file is renamed
        // to the recovery-visible backup name.
        let Some(mut staged) = self.stage_backup_file(chunk_id, seg_id, seq_id)? else {
            return Ok(false);
        };
        staged.file_mut().write_all(&wal_data)?;

        // Pad if requested
        if let Some(target_size) = pad_to_size {
            if wal_size < target_size {
                let padding_size = target_size - wal_size;
                let padding = vec![0u8; padding_size];
                staged.file_mut().write_all(&padding)?;
            }
        }

        self.publish_staged_backup(staged)?;
        Ok(true)
    }

    /// Read file content into memory
    /// Automatically decompresses backup files if they are compressed
    pub fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        // Check if this is a backup file by extension
        if let Some(extension) = path.extension() {
            if extension == "nbackup" {
                // Decompress backup files (auto-detects compression)
                return compression::decompress_if_compressed(&buffer);
            }
        }

        // WAL files are not compressed, return as-is
        Ok(buffer)
    }

    /// Get file size
    pub fn file_size(&self, path: &Path) -> io::Result<u64> {
        Ok(fs::metadata(path)?.len())
    }

    /// Discover all segment files in storage directories
    pub fn discover_files(&self) -> io::Result<Vec<SegmentFileInfo>> {
        let mut files = Vec::new();

        // Scan backup storage recursively
        if let Some(backup_dir) = &self.backup_storage {
            self.scan_dir_recursive(Path::new(backup_dir), &mut files, true)?;
        }

        // Scan WAL storage recursively
        if let Some(wal_dir) = &self.wal_storage {
            self.scan_dir_recursive(Path::new(wal_dir), &mut files, false)?;
        }

        // Deduplicate by (chunk_id, seg_id), keeping only the highest seq_id
        // This is critical because a segment may be archived multiple times,
        // and we only want the most recent version.
        use std::collections::HashMap;
        let mut best_files: HashMap<(usize, u64), SegmentFileInfo> = HashMap::new();

        for file in files {
            let key = (file.chunk_id, file.seg_id);
            let should_replace = match best_files.get(&key) {
                None => true,
                Some(existing) => {
                    // Prefer higher seq_id (more recent)
                    // If seq_id is the same, prefer backup over WAL
                    if file.seq_id > existing.seq_id {
                        true
                    } else if file.seq_id == existing.seq_id
                        && file.is_backup
                        && !existing.is_backup
                    {
                        true
                    } else {
                        false
                    }
                }
            };

            if should_replace {
                info!(
                    "Segment file for chunk {} seg {}: keeping seq {} (path: {})",
                    file.chunk_id,
                    file.seg_id,
                    file.seq_id,
                    file.path.display()
                );
                best_files.insert(key, file);
            } else {
                debug!(
                    "Segment file for chunk {} seg {} seq {}: superseded by newer version",
                    file.chunk_id, file.seg_id, file.seq_id
                );
            }
        }

        let mut deduped: Vec<SegmentFileInfo> = best_files.into_values().collect();

        // Sort by seq_id (chronological order for proper version resolution)
        deduped.sort_by_key(|f| f.seq_id);

        info!(
            "Discovered {} unique segment files for recovery",
            deduped.len()
        );

        Ok(deduped)
    }

    /// Helper function to recursively scan directories for segment files
    fn scan_dir_recursive(
        &self,
        dir: &Path,
        files: &mut Vec<SegmentFileInfo>,
        is_backup_scan: bool,
    ) -> io::Result<()> {
        if !dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                // Recursively scan subdirectories
                self.scan_dir_recursive(&path, files, is_backup_scan)?;
            } else if path.is_file() {
                if let Some(info) = SegmentFileInfo::parse_filename(&path) {
                    if info.is_backup == is_backup_scan {
                        files.push(info);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Information about a segment file
#[derive(Debug, Clone)]
pub struct SegmentFileInfo {
    pub chunk_id: usize,
    pub seg_id: u64,
    pub seq_id: u64,
    pub path: PathBuf,
    pub size: u64,
    pub is_backup: bool,
}

impl SegmentFileInfo {
    /// Parse filename pattern: {chunk_id}-{seg_id}-{seq_id}.{nlog|nbackup}
    pub fn parse_filename(path: &Path) -> Option<Self> {
        let stem = path.file_stem()?.to_str()?;

        // Check extension
        let extension = path.extension()?.to_str()?;
        let is_backup = match extension {
            "nbackup" => true,
            "nlog" => false,
            _ => return None,
        };

        // Parse {chunk_id}-{seg_id}-{seq_id}
        let parts: Vec<&str> = stem.split('-').collect();
        if parts.len() != 3 {
            return None;
        }

        let chunk_id = parts[0].parse::<usize>().ok()?;
        let seg_id = parts[1].parse::<u64>().ok()?;
        let seq_id = parts[2].parse::<u64>().ok()?;

        let metadata = fs::metadata(path).ok()?;
        let size = metadata.len();

        Some(SegmentFileInfo {
            chunk_id,
            seg_id,
            seq_id,
            path: path.to_path_buf(),
            size,
            is_backup,
        })
    }

    /// Get the file path as a string
    pub fn path_str(&self) -> &str {
        self.path.to_str().unwrap_or("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::durable_fs::{
        directory_sync_count_for_test, durability_events_for_test,
        fail_next_directory_sync_for_test, fail_next_file_sync_for_test, DurabilityEvent,
    };
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn final_backup_publication_is_sealed_behind_staging() {
        let source = include_str!("file_manager.rs");
        let production_source = source
            .split_once("\n#[cfg(test)]\nmod tests {")
            .map(|(production_source, _)| production_source)
            .expect("file_manager.rs should contain its private test module");

        for forbidden_api in [
            "pub fn create_backup_file(",
            "pub fn open_or_create_backup_writer(",
        ] {
            assert!(
                !production_source.contains(forbidden_api),
                "final backups must not expose direct writer API `{forbidden_api}`"
            );
        }

        for forbidden_writer in [
            "durable_fs::open_or_create(Path::new(&backup_path)",
            "File::create(",
            "OpenOptions::new(",
            "fs::write(",
            "std::fs::write(",
        ] {
            assert!(
                !production_source.contains(forbidden_writer),
                "final backups must not have a direct writer path containing `{forbidden_writer}`"
            );
        }

        assert_eq!(
            production_source
                .matches("durable_fs::open_or_create(")
                .count(),
            2,
            "file creation must remain limited to the WAL and ignored backup staging paths"
        );
        for staged_publication_step in [
            "durable_fs::open_or_create(&staging_path, true)?",
            "durable_fs::sync_file(&file, &staging_path)?",
            "durable_fs::rename(&staging_path, &final_path)",
        ] {
            assert!(
                production_source.contains(staged_publication_step),
                "atomic backup publication must retain `{staged_publication_step}`"
            );
        }
    }

    #[test]
    fn new_wal_publication_syncs_directory_once_and_existing_fast_path_does_not_resync() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let manager = SegmentFileManager::new(None, Some(wal_dir.to_string_lossy().into_owned()));
        manager.init_directories().unwrap();
        let before = directory_sync_count_for_test(&wal_dir);

        let mut wal = manager
            .create_wal_file(1, 2, 3)
            .unwrap()
            .expect("configured WAL");
        wal.write_all(b"first").unwrap();
        wal.sync_all().unwrap();
        drop(wal);
        assert_eq!(
            directory_sync_count_for_test(&wal_dir),
            before + 1,
            "publishing a new WAL filename must sync its containing directory"
        );

        drop(
            manager
                .create_wal_file(1, 2, 3)
                .unwrap()
                .expect("existing configured WAL"),
        );
        assert_eq!(
            directory_sync_count_for_test(&wal_dir),
            before + 1,
            "opening an existing WAL must not add a directory sync per transaction"
        );
    }

    #[test]
    fn wal_publication_propagates_directory_sync_failure() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let manager = SegmentFileManager::new(None, Some(wal_dir.to_string_lossy().into_owned()));
        manager.init_directories().unwrap();
        fail_next_directory_sync_for_test(&wal_dir);

        let error = manager
            .create_wal_file(4, 5, 6)
            .expect_err("directory publication failure must reject WAL creation");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(error
            .to_string()
            .contains("injected directory sync failure"));
    }

    #[test]
    fn backup_publication_syncs_staging_contents_before_final_rename() {
        let temp_dir = TempDir::new().unwrap();
        let backup_dir = temp_dir.path().join("backup");
        let wal_dir = temp_dir.path().join("wal");
        let manager = SegmentFileManager::new(
            Some(backup_dir.to_string_lossy().into_owned()),
            Some(wal_dir.to_string_lossy().into_owned()),
        );
        manager.init_directories().unwrap();
        let mut wal = manager
            .create_wal_file(3, 4, 5)
            .unwrap()
            .expect("configured WAL");
        wal.write_all(b"durable-wal").unwrap();
        wal.sync_all().unwrap();
        drop(wal);
        let final_path = PathBuf::from(manager.backup_path(3, 4, 5).unwrap());
        fs::write(&final_path, b"prior-final").unwrap();
        let event_start = durability_events_for_test().len();

        assert!(manager.copy_wal_to_backup(3, 4, 5, Some(64)).unwrap());

        let events = durability_events_for_test();
        let events = &events[event_start..];
        let renames = events
            .iter()
            .enumerate()
            .filter_map(|(index, event)| match event {
                DurabilityEvent::FileRenamed { from, to } if to == &final_path => {
                    Some((index, from.clone()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            renames.len(),
            1,
            "replacement must use one staging-to-final rename: {events:?}"
        );
        let (rename_index, staging_path) = &renames[0];
        let sync_index = events
            .iter()
            .position(|event| event == &DurabilityEvent::FileSynced(staging_path.clone()))
            .expect("staging contents must be synced");
        assert!(
            sync_index < *rename_index,
            "staging file contents must be durable before final-name publication: {events:?}"
        );
        assert!(final_path.exists());
        assert!(!staging_path.exists());
        assert!(!PathBuf::from(format!("{}.old", final_path.display())).exists());
        let mut expected = b"durable-wal".to_vec();
        expected.resize(64, 0);
        assert_eq!(manager.read_file(&final_path).unwrap(), expected);
    }

    #[test]
    fn wal_to_backup_staging_failure_preserves_existing_final_and_retry_converges() {
        let temp_dir = TempDir::new().unwrap();
        let backup_dir = temp_dir.path().join("backup");
        let wal_dir = temp_dir.path().join("wal");
        let manager = SegmentFileManager::new(
            Some(backup_dir.to_string_lossy().into_owned()),
            Some(wal_dir.to_string_lossy().into_owned()),
        );
        manager.init_directories().unwrap();
        let mut wal = manager
            .create_wal_file(6, 7, 8)
            .unwrap()
            .expect("configured WAL");
        wal.write_all(b"replacement").unwrap();
        wal.sync_all().unwrap();
        drop(wal);
        let final_path = PathBuf::from(manager.backup_path(6, 7, 8).unwrap());
        let staging_path = PathBuf::from(format!("{}.staging", final_path.display()));
        fs::write(&final_path, b"prior-final").unwrap();
        fail_next_file_sync_for_test(&staging_path);

        let error = manager
            .copy_wal_to_backup(6, 7, 8, Some(32))
            .expect_err("injected staging sync failure");
        assert!(error.to_string().contains("injected file sync failure"));
        assert_eq!(fs::read(&final_path).unwrap(), b"prior-final");
        assert!(
            SegmentFileInfo::parse_filename(&staging_path).is_none(),
            "recovery discovery must ignore staging"
        );

        assert!(manager.copy_wal_to_backup(6, 7, 8, Some(32)).unwrap());
        let mut expected = b"replacement".to_vec();
        expected.resize(32, 0);
        assert_eq!(manager.read_file(&final_path).unwrap(), expected);
        assert!(!staging_path.exists());
    }

    #[test]
    fn test_path_generation() {
        let mgr = SegmentFileManager::new(Some("/backup".to_string()), Some("/wal".to_string()));

        assert_eq!(
            mgr.backup_path(1, 2, 3),
            Some("/backup/1-2-3.nbackup".to_string())
        );
        assert_eq!(mgr.wal_path(1, 2, 3), Some("/wal/1-2-3.nlog".to_string()));
    }

    #[test]
    fn test_filename_parsing() {
        let temp_dir = TempDir::new().unwrap();
        let backup_path = temp_dir.path().join("1-2-3.nbackup");
        File::create(&backup_path).unwrap();

        let info = SegmentFileInfo::parse_filename(&backup_path).unwrap();
        assert_eq!(info.chunk_id, 1);
        assert_eq!(info.seg_id, 2);
        assert_eq!(info.seq_id, 3);
        assert_eq!(info.is_backup, true);

        let wal_path = temp_dir.path().join("4-5-6.nlog");
        File::create(&wal_path).unwrap();

        let info = SegmentFileInfo::parse_filename(&wal_path).unwrap();
        assert_eq!(info.chunk_id, 4);
        assert_eq!(info.seg_id, 5);
        assert_eq!(info.seq_id, 6);
        assert_eq!(info.is_backup, false);
    }

    #[test]
    fn test_file_discovery() {
        let temp_dir = TempDir::new().unwrap();
        let backup_dir = temp_dir.path().join("backup");
        let wal_dir = temp_dir.path().join("wal");

        create_dir_all(&backup_dir).unwrap();
        create_dir_all(&wal_dir).unwrap();

        // Create some test files
        File::create(backup_dir.join("0-1-1.nbackup")).unwrap();
        File::create(backup_dir.join("0-2-2.nbackup")).unwrap();
        File::create(wal_dir.join("0-3-3.nlog")).unwrap();

        let mgr = SegmentFileManager::new(
            Some(backup_dir.to_str().unwrap().to_string()),
            Some(wal_dir.to_str().unwrap().to_string()),
        );

        let files = mgr.discover_files().unwrap();
        assert_eq!(files.len(), 3);

        // Check files are sorted by seq_id
        assert_eq!(files[0].seq_id, 1);
        assert_eq!(files[1].seq_id, 2);
        assert_eq!(files[2].seq_id, 3);
    }

    #[test]
    fn test_path_normalization() {
        let mgr = SegmentFileManager::new(
            Some("/mnt/data//backup".to_string()),
            Some("/mnt/data//wal".to_string()),
        );

        assert_eq!(
            mgr.backup_path(1, 2, 3),
            Some("/mnt/data/backup/1-2-3.nbackup".to_string())
        );
        assert_eq!(
            mgr.wal_path(1, 2, 3),
            Some("/mnt/data/wal/1-2-3.nlog".to_string())
        );

        let mgr_trailing = SegmentFileManager::new(
            Some("/mnt/data/backup/".to_string()),
            Some("/mnt/data/wal/".to_string()),
        );

        assert_eq!(
            mgr_trailing.backup_path(1, 2, 3),
            Some("/mnt/data/backup/1-2-3.nbackup".to_string())
        );
    }
}
