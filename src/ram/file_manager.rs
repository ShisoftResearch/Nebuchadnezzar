use std::fs::{self, create_dir_all, remove_file, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use crate::ram::compression;

/// Unified file manager for segment file operations
/// Centralizes all file path generation, directory management, and file I/O
pub struct SegmentFileManager {
    backup_storage: Option<String>,
    wal_storage: Option<String>,
}

impl SegmentFileManager {
    pub fn new(backup_storage: Option<String>, wal_storage: Option<String>) -> Self {
        Self {
            backup_storage,
            wal_storage,
        }
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
            let file = File::create(&wal_path)?;
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

        // Deduplicate: prefer backup over WAL for same (chunk_id, seg_id, seq_id)
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut deduped = Vec::new();

        // Sort: backup files first (is_backup = true)
        files.sort_by(|a, b| b.is_backup.cmp(&a.is_backup));

        for file in files {
            let key = (file.chunk_id, file.seg_id, file.seq_id);
            if seen.insert(key) {
                deduped.push(file);
            }
        }

        // Sort by seq_id (chronological order)
        deduped.sort_by_key(|f| f.seq_id);

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
}
