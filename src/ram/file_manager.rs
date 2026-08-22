use crate::ram::compression;
use std::fs::{self, create_dir_all, remove_file, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Unified file manager for segment file operations
/// Centralizes all file path generation, directory management, and file I/O
pub struct SegmentFileManager {
    backup_storage: Option<String>,
    wal_storage: Option<String>,
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
            create_dir_all(backup_storage)?;
        }
        if let Some(wal_storage) = &self.wal_storage {
            create_dir_all(wal_storage)?;
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
            // APPEND, not truncate. `File::create` here destroyed the durable
            // copy of a segment recovered from a WAL alone: recovery replays
            // the log into memory, and the first post-restart write then
            // truncated the very file it was replayed from, so a crash before
            // the next archive lost every cell in it. Appending can only ever
            // preserve more history, and a seq id is never reused (segments
            // that have a backup are sealed, and dispense deletes both files),
            // so there is nothing here that truncation was protecting against.
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)?;
            Ok(Some(file))
        } else {
            Ok(None)
        }
    }

    /// Create a backup file
    pub fn create_backup_file(
        &self,
        chunk_id: usize,
        seg_id: u64,
        seq_id: u64,
    ) -> io::Result<Option<File>> {
        if let Some(backup_path) = self.backup_path(chunk_id, seg_id, seq_id) {
            // Ensure parent directory exists
            if let Some(parent) = Path::new(&backup_path).parent() {
                create_dir_all(parent)?;
            }
            let file = File::create(&backup_path)?;
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

    /// Open or create a backup file for writing (unbuffered for memory efficiency)
    /// If the file exists, it opens for read/write without truncating
    /// If the file doesn't exist, it creates a new one
    pub fn open_or_create_backup_writer(
        &self,
        chunk_id: usize,
        seg_id: u64,
        seq_id: u64,
    ) -> io::Result<Option<File>> {
        if let Some(backup_path) = self.backup_path(chunk_id, seg_id, seq_id) {
            // Ensure parent directory exists
            if let Some(parent) = Path::new(&backup_path).parent() {
                create_dir_all(parent)?;
            }
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&backup_path)?;
            Ok(Some(file))
        } else {
            Ok(None)
        }
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
            if path.exists() {
                remove_file(path)?;
            }
        }
        Ok(())
    }

    /// Delete WAL file
    pub fn delete_wal(&self, chunk_id: usize, seg_id: u64, seq_id: u64) -> io::Result<()> {
        if let Some(wal_path) = self.wal_path(chunk_id, seg_id, seq_id) {
            let path = Path::new(&wal_path);
            if path.exists() {
                remove_file(path)?;
            }
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

        let backup_path = match self.backup_path(chunk_id, seg_id, seq_id) {
            Some(path) => path,
            None => return Ok(false),
        };

        // Check if WAL exists
        let wal_path_ref = Path::new(&wal_path);
        if !wal_path_ref.exists() {
            return Ok(false);
        }

        // Ensure backup parent directory exists
        if let Some(parent) = Path::new(&backup_path).parent() {
            create_dir_all(parent)?;
        }

        // Read WAL file
        let mut wal_file = File::open(&wal_path)?;
        let mut wal_data = Vec::new();
        wal_file.read_to_end(&mut wal_data)?;
        let wal_size = wal_data.len();

        // Create backup file and write data
        let mut backup_file = File::create(&backup_path)?;
        backup_file.write_all(&wal_data)?;

        // Pad if requested
        if let Some(target_size) = pad_to_size {
            if wal_size < target_size {
                let padding_size = target_size - wal_size;
                let padding = vec![0u8; padding_size];
                backup_file.write_all(&padding)?;
            }
        }

        backup_file.sync_all()?;
        Ok(true)
    }

    /// Read file content into memory
    /// Automatically decompresses backup files if they are compressed
    pub fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.read_file_with_used_len(path).map(|(data, _)| data)
    }

    /// Read a segment file, along with the live extent its image declares.
    ///
    /// `None` means the file does not record one -- a WAL, whose live extent
    /// is its length by construction, or a plain uncompressed buffer.
    pub fn read_file_with_used_len(&self, path: &Path) -> io::Result<(Vec<u8>, Option<usize>)> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        // Check if this is a backup file by extension
        if let Some(extension) = path.extension() {
            if extension == "nbackup" {
                let used_len = compression::declared_used_len(&buffer);
                // Decompress backup files (auto-detects compression)
                return compression::decompress_if_compressed(&buffer).map(|data| (data, used_len));
            }
        }

        // WAL files are not compressed, return as-is
        Ok((buffer, None))
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
            self.scan_storage_root(Path::new(backup_dir), &mut files, true)?;
        }

        // Scan WAL storage recursively
        if let Some(wal_dir) = &self.wal_storage {
            self.scan_storage_root(Path::new(wal_dir), &mut files, false)?;
        }

        // Deduplicate by (chunk_id, seg_id), keeping only the highest seq_id
        // This is critical because a segment may be archived multiple times,
        // and we only want the most recent version.
        use std::collections::HashMap;
        let mut best_files: HashMap<(usize, u64), SegmentFileInfo> = HashMap::new();

        for file in files {
            // A zero-byte backup is always a torn archive: a kill landed
            // between creating the file and writing its image (legitimate
            // backups always carry at least a compression header, and an
            // uncompressed one is a full segment). Letting it into the
            // dedup below would SHADOW the complete WAL at the same seq id
            // and silently drop every cell in the segment. Newly written
            // backups are rename-installed and can no longer be torn; this
            // guard covers stores written before that fix.
            if file.is_backup && file.size == 0 {
                warn!(
                    "Ignoring zero-byte backup '{}' (chunk {} seg {} seq {}): torn archive; \
                     recovery falls back to the WAL or an earlier file for this segment",
                    file.path.display(),
                    file.chunk_id,
                    file.seg_id,
                    file.seq_id
                );
                continue;
            }
            let key = (file.chunk_id, file.seg_id);
            let should_replace = match best_files.get(&key) {
                None => true,
                Some(existing) => {
                    // A backup and a WAL at the SAME seq id are twins, and the
                    // preference below assumes the backup is the complete
                    // image. That assumption used to be violated: archiving
                    // the open head deleted its WAL, and appending afterwards
                    // re-created one at the same seq, leaving a prefix image
                    // and a suffix log that only make sense concatenated.
                    // Preferring the backup then silently dropped every write
                    // after the archive.
                    //
                    // Segments with a backup are sealed now, so a store this
                    // build wrote cannot produce twins. One still on disk was
                    // written by an older build: say so, loudly and per pair,
                    // because the loss is otherwise invisible. Recovery still
                    // takes the backup -- a prefix is the safe half, and the
                    // seam between them is not reliably recoverable (a merge
                    // that guessed it wrong trimmed the WAL to nothing).
                    if file.seq_id == existing.seq_id && file.is_backup != existing.is_backup {
                        let (backup, wal) = if file.is_backup {
                            (&file, existing)
                        } else {
                            (existing, &file)
                        };
                        error!(
                            "TWIN SEGMENT FILES for chunk {} seg {} seq {}: backup '{}' ({} bytes) \
                             and WAL '{}' ({} bytes) share one seq id. This store was written by a \
                             build that appended to an archived segment; the WAL holds writes made \
                             after the backup and they CANNOT be recovered. Taking the backup. \
                             Re-import to get a clean store.",
                            file.chunk_id,
                            file.seg_id,
                            file.seq_id,
                            backup.path.display(),
                            backup.size,
                            wal.path.display(),
                            wal.size
                        );
                    }
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

    /// Scan one storage ROOT, skipping the reserved `databases/` child.
    ///
    /// `<root>/databases/<scoped_name>/` is where the server scopes every
    /// dynamically-bound database's storage (see the server's storage-root
    /// scoping), and each of those stores has its OWN file manager rooted
    /// there. Descending into it from the unscoped root ingested every other
    /// database's segments into this one's recovery: files are named only
    /// `{chunk}-{seg}-{seq}` with no database identity, so the dedup treated
    /// another store's WAL as a twin of this store's backup -- measured as a
    /// dynamic database's WAL being shadowed (its writes unrecoverable) and
    /// cross-database cells surfacing under the wrong schemas.
    fn scan_storage_root(
        &self,
        root: &Path,
        files: &mut Vec<SegmentFileInfo>,
        is_backup_scan: bool,
    ) -> io::Result<()> {
        if !root.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().and_then(|n| n.to_str()) == Some("databases") {
                    continue;
                }
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
    use std::fs::File;
    use tempfile::TempDir;

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

        // Create some test files. Backups carry content on purpose: a
        // zero-byte backup is a torn archive and discovery now ignores it,
        // so an empty fixture would be testing the guard, not discovery.
        use std::io::Write as _;
        File::create(backup_dir.join("0-1-1.nbackup"))
            .unwrap()
            .write_all(b"x")
            .unwrap();
        File::create(backup_dir.join("0-2-2.nbackup"))
            .unwrap()
            .write_all(b"x")
            .unwrap();
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
