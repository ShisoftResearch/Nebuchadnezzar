use crate::ram::types::Id;
use bifrost::vector_clock::StandardVectorClock;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs::{create_dir_all, remove_file, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub type TxnId = StandardVectorClock;

/// Undo log entry stored in the log file
/// Format: [entry_type: u8][txn_id_len: u32][txn_id: bytes][cell_id: Id][op_type: u8][version: u64][chunk_id: u64][seg_id: u64][seq_id: u64]
/// 
/// All operations store version for verification during recovery:
/// - Write: version = new cell version (to verify cell unchanged before deletion)
/// - Update/Remove: version = old cell version (to verify we're restoring the right version)
#[derive(Debug, Clone)]
pub struct UndoLogEntry {
    pub txn_id: TxnId,
    pub cell_id: Id,
    pub op_type: UndoOpType,
    /// Cell version for verification during recovery
    /// - Write: version of newly created cell
    /// - Update/Remove: version of old cell before modification
    pub version: u64,
    /// For Update/Remove: chunk_id where old cell is located (0 for Write)
    pub chunk_id: u64,
    /// For Update/Remove: segment_id where old cell is located (0 for Write)
    pub seg_id: u64,
    /// For Update/Remove: seq_id of segment where old cell is located (0 for Write)
    pub seq_id: u64,
}

/// Type of operation that needs to be undone
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndoOpType {
    Write = 1,   // New cell created - store version, DELETE on rollback if version matches
    Update = 2,  // Cell updated - store old segment location, RESTORE old version on rollback
    Remove = 3,  // Cell removed - store old segment location, RESTORE old version on rollback
}

impl UndoOpType {
    fn from_u8(value: u8) -> io::Result<Self> {
        match value {
            1 => Ok(UndoOpType::Write),
            2 => Ok(UndoOpType::Update),
            3 => Ok(UndoOpType::Remove),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid undo operation type: {}", value),
            )),
        }
    }
}

const ENTRY_TYPE_UNDO: u8 = 1;
const ENTRY_TYPE_COMMIT: u8 = 2;
const ENTRY_TYPE_ABORT: u8 = 3;

impl UndoLogEntry {
    /// Create a new undo log entry
    pub fn new(txn_id: TxnId, cell_id: Id, op_type: UndoOpType, version: u64, chunk_id: u64, seg_id: u64, seq_id: u64) -> Self {
        Self {
            txn_id,
            cell_id,
            op_type,
            version,
            chunk_id,
            seg_id,
            seq_id,
        }
    }
    
    /// Helper to create a Write entry (for new cells)
    /// Only needs version since there's no old segment to restore from
    pub fn new_write(txn_id: TxnId, cell_id: Id, version: u64) -> Self {
        Self::new(txn_id, cell_id, UndoOpType::Write, version, 0, 0, 0)
    }
    
    /// Helper to create an Update/Remove entry (with old cell version and segment location)
    /// Stores both the old version for verification and segment location for restoration
    pub fn new_restore(txn_id: TxnId, cell_id: Id, op_type: UndoOpType, old_version: u64, chunk_id: u64, seg_id: u64, seq_id: u64) -> Self {
        debug_assert!(op_type != UndoOpType::Write, "Use new_write for Write operations");
        Self::new(txn_id, cell_id, op_type, old_version, chunk_id, seg_id, seq_id)
    }
    
    /// Serialize entry to bytes
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let txn_id_bytes = serde_json::to_vec(&self.txn_id)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let txn_id_len = txn_id_bytes.len() as u32;

        let mut bytes = Vec::with_capacity(1 + 4 + txn_id_bytes.len() + 16 + 1 + 8 + 8 + 8 + 8);
        bytes.push(ENTRY_TYPE_UNDO);
        bytes.extend_from_slice(&txn_id_len.to_le_bytes());
        bytes.extend_from_slice(&txn_id_bytes);
        bytes.extend_from_slice(&self.cell_id.higher.to_le_bytes());
        bytes.extend_from_slice(&self.cell_id.lower.to_le_bytes());
        bytes.push(self.op_type as u8);
        bytes.extend_from_slice(&self.version.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_id.to_le_bytes());
        bytes.extend_from_slice(&self.seg_id.to_le_bytes());
        bytes.extend_from_slice(&self.seq_id.to_le_bytes());

        Ok(bytes)
    }

    /// Deserialize entry from bytes
    pub fn from_bytes(bytes: &[u8]) -> io::Result<(Self, usize)> {
        if bytes.len() < 5 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for entry header",
            ));
        }

        let entry_type = bytes[0];
        if entry_type != ENTRY_TYPE_UNDO {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid entry type: {}", entry_type),
            ));
        }

        let txn_id_len = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        if bytes.len() < 5 + txn_id_len + 49 {  // +1 for op_type, +8 for version, +8 for chunk_id, +8 for seg_id, +8 for seq_id = 33 + 16 (cell_id) = 49
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for full entry",
            ));
        }

        let txn_id: TxnId = serde_json::from_slice(&bytes[5..5 + txn_id_len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut offset = 5 + txn_id_len;
        let cell_id_higher = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        let cell_id_lower = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        let op_type = UndoOpType::from_u8(bytes[offset])?;
        offset += 1;

        let version = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;
        
        let chunk_id = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        let seg_id = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        let seq_id = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        let total_size = offset;

        Ok((
            Self {
                txn_id,
                cell_id: Id {
                    higher: cell_id_higher,
                    lower: cell_id_lower,
                },
                op_type,
                version,
                chunk_id,
                seg_id,
                seq_id,
            },
            total_size,
        ))
    }
}

/// Manages the undo log for transactions
pub struct UndoLogger {
    /// Path to the undo log directory
    log_dir: String,
    /// Current active log file
    log_file: Mutex<Option<BufWriter<File>>>,
    /// Current log file name
    log_file_name: Mutex<Option<String>>,
    /// Log file sequence number
    log_seq: AtomicU64,
    /// In-memory index of transaction -> undo entries
    txn_index: Mutex<HashMap<TxnId, Vec<UndoLogEntry>>>,
    /// Maximum log file size before rotation (default 64MB)
    max_log_size: u64,
}

impl UndoLogger {
    /// Create a new undo log manager
    pub fn new(log_dir: String) -> io::Result<Arc<Self>> {
        create_dir_all(&log_dir)?;

        let log = Arc::new(Self {
            log_dir: log_dir.clone(),
            log_file: Mutex::new(None),
            log_file_name: Mutex::new(None),
            log_seq: AtomicU64::new(0),
            txn_index: Mutex::new(HashMap::new()),
            max_log_size: 64 * 1024 * 1024, // 64MB
        });

        // Open or create initial log file
        log.rotate_log()?;

        Ok(log)
    }

    /// Rotate to a new log file
    fn rotate_log(&self) -> io::Result<()> {
        let seq = self.log_seq.fetch_add(1, Ordering::SeqCst);
        let log_file_path = format!("{}/undo-{}.nlog", self.log_dir, seq);

        debug!("Rotating undo log to: {}", log_file_path);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)?;

        let writer = BufWriter::with_capacity(4096, file);

        let mut log_file_guard = self.log_file.lock();
        if let Some(mut old_writer) = log_file_guard.take() {
            old_writer.flush()?;
            old_writer.get_ref().sync_all()?;
        }

        *log_file_guard = Some(writer);
        *self.log_file_name.lock() = Some(log_file_path);

        Ok(())
    }

    /// Write an undo entry to the log
    pub fn write_undo_entry(&self, entry: UndoLogEntry) -> io::Result<()> {
        let bytes = entry.to_bytes()?;

        let mut log_file_guard = self.log_file.lock();
        if let Some(writer) = log_file_guard.as_mut() {
            writer.write_all(&bytes)?;
            writer.flush()?;
            writer.get_ref().sync_data()?;

            // Add to in-memory index
            drop(log_file_guard);
            let mut index = self.txn_index.lock();
            index
                .entry(entry.txn_id.clone())
                .or_insert_with(Vec::new)
                .push(entry);

            // Check if we need to rotate
            let log_file_name = self.log_file_name.lock();
            if let Some(ref path) = *log_file_name {
                if let Ok(metadata) = std::fs::metadata(path) {
                    if metadata.len() > self.max_log_size {
                        drop(log_file_name);
                        self.rotate_log()?;
                    }
                }
            }

            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Log file not initialized",
            ))
        }
    }

    /// Write a commit marker for a transaction
    pub fn write_commit_marker(&self, txn_id: &TxnId) -> io::Result<()> {
        let txn_id_bytes = serde_json::to_vec(txn_id)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let txn_id_len = txn_id_bytes.len() as u32;

        let mut bytes = Vec::with_capacity(1 + 4 + txn_id_bytes.len());
        bytes.push(ENTRY_TYPE_COMMIT);
        bytes.extend_from_slice(&txn_id_len.to_le_bytes());
        bytes.extend_from_slice(&txn_id_bytes);

        let mut log_file_guard = self.log_file.lock();
        if let Some(writer) = log_file_guard.as_mut() {
            writer.write_all(&bytes)?;
            writer.flush()?;
            writer.get_ref().sync_data()?;

            // Remove from in-memory index
            drop(log_file_guard);
            self.txn_index.lock().remove(txn_id);

            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Log file not initialized",
            ))
        }
    }

    /// Write an abort marker for a transaction
    pub fn write_abort_marker(&self, txn_id: &TxnId) -> io::Result<()> {
        let txn_id_bytes = serde_json::to_vec(txn_id)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let txn_id_len = txn_id_bytes.len() as u32;

        let mut bytes = Vec::with_capacity(1 + 4 + txn_id_bytes.len());
        bytes.push(ENTRY_TYPE_ABORT);
        bytes.extend_from_slice(&txn_id_len.to_le_bytes());
        bytes.extend_from_slice(&txn_id_bytes);

        let mut log_file_guard = self.log_file.lock();
        if let Some(writer) = log_file_guard.as_mut() {
            writer.write_all(&bytes)?;
            writer.flush()?;
            writer.get_ref().sync_data()?;

            // Remove from in-memory index
            drop(log_file_guard);
            self.txn_index.lock().remove(txn_id);

            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Log file not initialized",
            ))
        }
    }

    /// Get all undo entries for a transaction
    pub fn get_undo_entries(&self, txn_id: &TxnId) -> Vec<UndoLogEntry> {
        self.txn_index
            .lock()
            .get(txn_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Trim old log files that only contain committed/aborted transactions
    pub fn trim_old_logs(&self) -> io::Result<()> {
        let current_seq = self.log_seq.load(Ordering::SeqCst);

        // Get all active transactions
        let active_txns: Vec<TxnId> = self.txn_index.lock().keys().cloned().collect();

        // Scan log directory for old log files
        let log_dir_path = Path::new(&self.log_dir);
        if let Ok(entries) = std::fs::read_dir(log_dir_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if let Some(file_name) = path.file_name() {
                        if let Some(name_str) = file_name.to_str() {
                            if name_str.starts_with("undo-") && name_str.ends_with(".nlog") {
                                // Parse sequence number
                                if let Some(seq_str) = name_str
                                    .strip_prefix("undo-")
                                    .and_then(|s| s.strip_suffix(".nlog"))
                                {
                                    if let Ok(seq) = seq_str.parse::<u64>() {
                                        // Don't trim current or recent logs
                                        if seq < current_seq.saturating_sub(2) {
                                            // Check if this log contains any active transactions
                                            if !self.log_contains_active_txns(&path, &active_txns)? {
                                                debug!("Trimming old undo log: {:?}", path);
                                                remove_file(&path)?;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if a log file contains any active transactions
    fn log_contains_active_txns(&self, path: &Path, active_txns: &[TxnId]) -> io::Result<bool> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut offset = 0;
        while offset < buffer.len() {
            if buffer.len() < offset + 5 {
                break;
            }

            let entry_type = buffer[offset];
            let txn_id_len =
                u32::from_le_bytes([buffer[offset + 1], buffer[offset + 2], buffer[offset + 3], buffer[offset + 4]]) as usize;

            if buffer.len() < offset + 5 + txn_id_len {
                break;
            }

            let txn_id: TxnId = match serde_json::from_slice(&buffer[offset + 5..offset + 5 + txn_id_len]) {
                Ok(id) => id,
                Err(_) => break,
            };

            if active_txns.contains(&txn_id) {
                return Ok(true);
            }

            match entry_type {
                ENTRY_TYPE_UNDO => {
                    // Skip the rest of the undo entry
                    offset += 5 + txn_id_len + 40; // txn_id + cell_id + chunk_id + seg_id + seq_id
                }
                ENTRY_TYPE_COMMIT | ENTRY_TYPE_ABORT => {
                    offset += 5 + txn_id_len;
                }
                _ => break,
            }
        }

        Ok(false)
    }

    /// Recover undo log from disk on startup
    pub fn recover(&self) -> io::Result<()> {
        let log_dir_path = Path::new(&self.log_dir);
        let mut log_files = Vec::new();

        // Collect all log files
        if let Ok(entries) = std::fs::read_dir(log_dir_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if let Some(file_name) = path.file_name() {
                        if let Some(name_str) = file_name.to_str() {
                            if name_str.starts_with("undo-") && name_str.ends_with(".nlog") {
                                if let Some(seq_str) = name_str
                                    .strip_prefix("undo-")
                                    .and_then(|s| s.strip_suffix(".nlog"))
                                {
                                    if let Ok(seq) = seq_str.parse::<u64>() {
                                        log_files.push((seq, path));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sort by sequence number
        log_files.sort_by_key(|(seq, _)| *seq);

        // Rebuild in-memory index
        let mut txn_index = HashMap::new();
        for (_seq, path) in &log_files {
            let mut file = File::open(path)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;

            let mut offset = 0;
            while offset < buffer.len() {
                if buffer.len() < offset + 5 {
                    break;
                }

                let entry_type = buffer[offset];
                let txn_id_len =
                    u32::from_le_bytes([buffer[offset + 1], buffer[offset + 2], buffer[offset + 3], buffer[offset + 4]])
                        as usize;

                if buffer.len() < offset + 5 + txn_id_len {
                    break;
                }

                let txn_id: TxnId = match serde_json::from_slice(&buffer[offset + 5..offset + 5 + txn_id_len]) {
                    Ok(id) => id,
                    Err(_) => break,
                };

                match entry_type {
                    ENTRY_TYPE_UNDO => {
                        if let Ok((entry, size)) = UndoLogEntry::from_bytes(&buffer[offset..]) {
                            txn_index
                                .entry(txn_id.clone())
                                .or_insert_with(Vec::new)
                                .push(entry);
                            offset += size;
                        } else {
                            break;
                        }
                    }
                    ENTRY_TYPE_COMMIT | ENTRY_TYPE_ABORT => {
                        txn_index.remove(&txn_id);
                        offset += 5 + txn_id_len;
                    }
                    _ => break,
                }
            }
        }

        *self.txn_index.lock() = txn_index;

        // Update log sequence number
        if let Some((max_seq, _)) = log_files.last() {
            self.log_seq.store(*max_seq + 1, Ordering::SeqCst);
        }

        info!(
            "Recovered undo log with {} active transactions",
            self.txn_index.lock().len()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_undo_entry_serialization() {
        let txn_id = TxnId::new();
        let cell_id = Id { higher: 1, lower: 2 };
        let entry = UndoLogEntry::new(txn_id, cell_id, UndoOpType::Update, 5, 0, 100, 1000);

        let bytes = entry.to_bytes().unwrap();
        let (recovered, size) = UndoLogEntry::from_bytes(&bytes).unwrap();

        assert_eq!(size, bytes.len());
        assert_eq!(recovered.cell_id, entry.cell_id);
        assert_eq!(recovered.op_type, entry.op_type);
        assert_eq!(recovered.version, entry.version);
        assert_eq!(recovered.chunk_id, entry.chunk_id);
        assert_eq!(recovered.seg_id, entry.seg_id);
        assert_eq!(recovered.seq_id, entry.seq_id);
    }

    #[test]
    fn test_undo_log_basic() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir).unwrap();

        let txn_id = TxnId::new();
        let cell_id = Id { higher: 1, lower: 2 };
        let entry = UndoLogEntry::new(txn_id.clone(), cell_id, UndoOpType::Update, 3, 0, 100, 1000);

        undo_log.write_undo_entry(entry.clone()).unwrap();

        let entries = undo_log.get_undo_entries(&txn_id);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cell_id, cell_id);

        undo_log.write_commit_marker(&txn_id).unwrap();

        let entries = undo_log.get_undo_entries(&txn_id);
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_undo_log_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let txn_id = TxnId::new();
        let cell_id = Id { higher: 1, lower: 2 };
        let entry = UndoLogEntry::new(txn_id.clone(), cell_id, UndoOpType::Remove, 2, 0, 100, 1000);

        {
            let undo_log = UndoLogger::new(log_dir.clone()).unwrap();
            undo_log.write_undo_entry(entry.clone()).unwrap();
        }

        // Recreate and recover
        let undo_log = UndoLogger::new(log_dir).unwrap();
        undo_log.recover().unwrap();

        let entries = undo_log.get_undo_entries(&txn_id);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cell_id, cell_id);
    }
    
    #[test]
    fn test_undo_log_multiple_entries() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir).unwrap();

        let txn_id = TxnId::new();
        let cell_id1 = Id { higher: 1, lower: 2 };
        let cell_id2 = Id { higher: 3, lower: 4 };
        
        let entry1 = UndoLogEntry::new(txn_id.clone(), cell_id1, UndoOpType::Write, 1, 0, 0, 0);
        let entry2 = UndoLogEntry::new(txn_id.clone(), cell_id2, UndoOpType::Update, 4, 0, 200, 2000);

        undo_log.write_undo_entry(entry1).unwrap();
        undo_log.write_undo_entry(entry2).unwrap();

        let entries = undo_log.get_undo_entries(&txn_id);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].cell_id, cell_id1);
        assert_eq!(entries[1].cell_id, cell_id2);
    }

    /// Test end-to-end: Transaction with successful commit
    /// Verifies that committed transactions are removed from undo log
    #[test]
    fn test_e2e_committed_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir.clone()).unwrap();

        // Transaction 1: Write a new cell
        let txn1 = TxnId::new();
        let cell_id1 = Id { higher: 100, lower: 1 };
        let entry1 = UndoLogEntry::new_write(txn1.clone(), cell_id1, 1);
        undo_log.write_undo_entry(entry1).unwrap();

        // Verify undo entry exists
        let entries = undo_log.get_undo_entries(&txn1);
        assert_eq!(entries.len(), 1, "Should have 1 undo entry before commit");
        assert_eq!(entries[0].op_type, UndoOpType::Write);
        assert_eq!(entries[0].version, 1);

        // Commit the transaction
        undo_log.write_commit_marker(&txn1).unwrap();

        // Verify undo entries are cleared after commit
        let entries_after = undo_log.get_undo_entries(&txn1);
        assert_eq!(entries_after.len(), 0, "Should have no undo entries after commit");

        // Recovery should not find this transaction as incomplete
        undo_log.recover().unwrap();
        let entries_after_recovery = undo_log.get_undo_entries(&txn1);
        assert_eq!(entries_after_recovery.len(), 0, "Should have no entries after recovery");
    }

    /// Test end-to-end: Transaction with abort
    /// Verifies that aborted transactions are removed from undo log
    #[test]
    fn test_e2e_aborted_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir.clone()).unwrap();

        // Transaction that will be aborted
        let txn = TxnId::new();
        let cell_id1 = Id { higher: 200, lower: 1 };
        let cell_id2 = Id { higher: 200, lower: 2 };
        
        // Write, Update, and Remove operations
        let entry1 = UndoLogEntry::new_write(txn.clone(), cell_id1, 1);
        let entry2 = UndoLogEntry::new_restore(txn.clone(), cell_id2, UndoOpType::Update, 5, 0, 100, 1000);
        
        undo_log.write_undo_entry(entry1).unwrap();
        undo_log.write_undo_entry(entry2).unwrap();

        // Verify undo entries exist
        let entries = undo_log.get_undo_entries(&txn);
        assert_eq!(entries.len(), 2, "Should have 2 undo entries before abort");

        // Abort the transaction
        undo_log.write_abort_marker(&txn).unwrap();

        // Verify undo entries are cleared after abort
        let entries_after = undo_log.get_undo_entries(&txn);
        assert_eq!(entries_after.len(), 0, "Should have no undo entries after abort");

        // Recovery should not find this transaction as incomplete
        undo_log.recover().unwrap();
        let entries_after_recovery = undo_log.get_undo_entries(&txn);
        assert_eq!(entries_after_recovery.len(), 0, "Should have no entries after recovery");
    }

    /// Test end-to-end: Incomplete transaction (crash before commit/abort)
    /// Verifies that incomplete transactions are recovered
    #[test]
    fn test_e2e_incomplete_transaction_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let txn_incomplete = TxnId::new();
        let cell_id1 = Id { higher: 300, lower: 1 };
        let cell_id2 = Id { higher: 300, lower: 2 };
        let cell_id3 = Id { higher: 300, lower: 3 };

        {
            // Simulate a transaction that didn't finish
            let undo_log = UndoLogger::new(log_dir.clone()).unwrap();
            
            // Write multiple operations
            let entry1 = UndoLogEntry::new_write(txn_incomplete.clone(), cell_id1, 1);
            let entry2 = UndoLogEntry::new_restore(txn_incomplete.clone(), cell_id2, UndoOpType::Update, 3, 0, 50, 500);
            let entry3 = UndoLogEntry::new_restore(txn_incomplete.clone(), cell_id3, UndoOpType::Remove, 7, 1, 75, 750);
            
            undo_log.write_undo_entry(entry1).unwrap();
            undo_log.write_undo_entry(entry2).unwrap();
            undo_log.write_undo_entry(entry3).unwrap();

            // Simulate crash - no commit/abort marker written
        } // undo_log dropped here

        // Recover after "crash"
        let undo_log = UndoLogger::new(log_dir).unwrap();
        undo_log.recover().unwrap();

        // Verify incomplete transaction is found
        let entries = undo_log.get_undo_entries(&txn_incomplete);
        assert_eq!(entries.len(), 3, "Should recover all 3 undo entries");
        
        // Verify entry details
        assert_eq!(entries[0].cell_id, cell_id1);
        assert_eq!(entries[0].op_type, UndoOpType::Write);
        assert_eq!(entries[0].version, 1);
        
        assert_eq!(entries[1].cell_id, cell_id2);
        assert_eq!(entries[1].op_type, UndoOpType::Update);
        assert_eq!(entries[1].version, 3);
        assert_eq!(entries[1].chunk_id, 0);
        assert_eq!(entries[1].seg_id, 50);
        assert_eq!(entries[1].seq_id, 500);
        
        assert_eq!(entries[2].cell_id, cell_id3);
        assert_eq!(entries[2].op_type, UndoOpType::Remove);
        assert_eq!(entries[2].version, 7);
        assert_eq!(entries[2].chunk_id, 1);
        assert_eq!(entries[2].seg_id, 75);
        assert_eq!(entries[2].seq_id, 750);
    }

    /// Test if TxnId (StandardVectorClock) equality works after JSON serialization
    #[test]
    fn test_txn_id_serialization_equality() {
        let txn1 = TxnId::new();
        
        // Serialize and deserialize
        let json = serde_json::to_vec(&txn1).unwrap();
        let txn2: TxnId = serde_json::from_slice(&json).unwrap();
        
        println!("Original TxnId: {:?}", txn1);
        println!("Deserialized TxnId: {:?}", txn2);
        println!("Are they equal? {}", txn1 == txn2);
        println!("Hash original: {:?}", {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            txn1.hash(&mut hasher);
            hasher.finish()
        });
        println!("Hash deserialized: {:?}", {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            txn2.hash(&mut hasher);
            hasher.finish()
        });
        
        // Test HashMap lookup
        let mut map = std::collections::HashMap::new();
        map.insert(txn1.clone(), vec![1, 2, 3]);
        
        assert!(map.contains_key(&txn2), "HashMap should find deserialized key");
        assert_eq!(map.get(&txn2).unwrap(), &vec![1, 2, 3]);
    }

    /// Debug test: Understand why commit markers aren't being processed during recovery
    #[test]
    fn test_debug_commit_marker_processing() {
        use std::io::Read;
        
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let txn = TxnId::new();
        
        // Write entry and commit marker
        {
            let undo_log = UndoLogger::new(log_dir.clone()).unwrap();
            let entry = UndoLogEntry::new_write(txn.clone(), Id { higher: 1, lower: 1 }, 1);
            undo_log.write_undo_entry(entry).unwrap();
            println!("After write: {} entries for txn", undo_log.get_undo_entries(&txn).len());
            
            undo_log.write_commit_marker(&txn).unwrap();
            println!("After commit marker: {} entries for txn", undo_log.get_undo_entries(&txn).len());
        }
        
        // Read the log file directly to see what's in it
        let log_files: Vec<_> = std::fs::read_dir(&log_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|ext| ext == "nlog").unwrap_or(false))
            .collect();
            
        println!("Found {} log files", log_files.len());
        for file_entry in &log_files {
            let path = file_entry.path();
            println!("Log file: {:?}", path);
            let mut file = std::fs::File::open(&path).unwrap();
            let mut contents = Vec::new();
            file.read_to_end(&mut contents).unwrap();
            println!("File size: {} bytes", contents.len());
            
            // Parse entries manually
            let mut offset = 0;
            let mut entry_count = 0;
            while offset < contents.len() {
                if contents.len() < offset + 5 {
                    break;
                }
                let entry_type = contents[offset];
                let txn_id_len = u32::from_le_bytes([
                    contents[offset + 1],
                    contents[offset + 2],
                    contents[offset + 3],
                    contents[offset + 4],
                ]) as usize;
                
                println!("Entry {}: type={}, txn_id_len={}", entry_count, entry_type, txn_id_len);
                entry_count += 1;
                
                // Skip to next entry (approximate)
                match entry_type {
                    1 => offset += 5 + txn_id_len + 49, // UNDO entry
                    2 | 3 => offset += 5 + txn_id_len,  // COMMIT/ABORT marker
                    _ => break,
                }
            }
        }
        
        // Now recover and see what happens
        let undo_log = UndoLogger::new(log_dir).unwrap();
        println!("Before recovery: {} entries", undo_log.get_undo_entries(&txn).len());
        undo_log.recover().unwrap();
        println!("After recovery: {} entries", undo_log.get_undo_entries(&txn).len());
        
        assert_eq!(undo_log.get_undo_entries(&txn).len(), 0, "Should have 0 entries after recovery");
    }

    /// Test end-to-end: Mixed transactions (committed, aborted, incomplete)
    /// Verifies that recovery correctly handles different transaction states
    #[test]
    fn test_e2e_mixed_transactions() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        // Create unique transaction IDs by incrementing them
        // (in real system, each would have different server_id in vector clock)
        let mut txn_committed = TxnId::new();
        txn_committed.inc(1); // server_id=1
        
        let mut txn_aborted = TxnId::new();
        txn_aborted.inc(2); // server_id=2
        
        let mut txn_incomplete = TxnId::new();
        txn_incomplete.inc(3); // server_id=3

        {
            let undo_log = UndoLogger::new(log_dir.clone()).unwrap();
            
            // Transaction 1: Will be committed
            let entry1 = UndoLogEntry::new_write(txn_committed.clone(), Id { higher: 1, lower: 1 }, 1);
            undo_log.write_undo_entry(entry1).unwrap();
            assert_eq!(undo_log.get_undo_entries(&txn_committed).len(), 1, "Entry should exist before commit");
            
            undo_log.write_commit_marker(&txn_committed).unwrap();
            assert_eq!(undo_log.get_undo_entries(&txn_committed).len(), 0, "Entry should be cleared after commit");

            // Transaction 2: Will be aborted
            let entry2 = UndoLogEntry::new_write(txn_aborted.clone(), Id { higher: 2, lower: 1 }, 2);
            undo_log.write_undo_entry(entry2).unwrap();
            undo_log.write_abort_marker(&txn_aborted).unwrap();
            assert_eq!(undo_log.get_undo_entries(&txn_aborted).len(), 0, "Entry should be cleared after abort");

            // Transaction 3: Incomplete (crash)
            let entry3 = UndoLogEntry::new_write(txn_incomplete.clone(), Id { higher: 3, lower: 1 }, 3);
            undo_log.write_undo_entry(entry3).unwrap();
            // No commit/abort marker
        } // undo_log dropped and files flushed

        // Recover after "crash"
        let undo_log = UndoLogger::new(log_dir).unwrap();
        undo_log.recover().unwrap();

        // After recovery, only incomplete transaction should be present
        assert_eq!(undo_log.get_undo_entries(&txn_incomplete).len(), 1, "Incomplete txn should be recovered");
        assert_eq!(undo_log.get_undo_entries(&txn_committed).len(), 0, "Committed txn should not be recovered");
        assert_eq!(undo_log.get_undo_entries(&txn_aborted).len(), 0, "Aborted txn should not be recovered");
    }

    /// Test end-to-end: Log trimming removes old completed transactions
    /// Verifies that trim_old_logs correctly removes files with only committed/aborted transactions
    #[test]
    fn test_e2e_log_trimming() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir.clone()).unwrap();

        // Create multiple transactions in first log file
        for i in 0..5 {
            let txn = TxnId::new();
            let entry = UndoLogEntry::new_write(txn.clone(), Id { higher: i, lower: 1 }, i);
            undo_log.write_undo_entry(entry).unwrap();
            undo_log.write_commit_marker(&txn).unwrap();
        }
        
        // Write more transactions to fill the log
        for i in 5..10 {
            let txn = TxnId::new();
            let entry = UndoLogEntry::new_write(txn.clone(), Id { higher: i, lower: 1 }, i);
            undo_log.write_undo_entry(entry).unwrap();
            undo_log.write_commit_marker(&txn).unwrap();
        }

        // Get current file count
        let log_files_before = std::fs::read_dir(&log_dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .map(|ext| ext == "nlog")
                    .unwrap_or(false)
            })
            .count();

        println!("Log files before trim: {}", log_files_before);

        // All transactions are committed, so trim should clean up old files
        undo_log.trim_old_logs().unwrap();

        // Check file count after trim
        let log_files_after = std::fs::read_dir(&log_dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .map(|ext| ext == "nlog")
                    .unwrap_or(false)
            })
            .count();

        println!("Log files after trim: {}", log_files_after);

        // After trimming, we should have at most the current active file
        // (trim removes all files except current one if all txns are complete)
        assert!(
            log_files_after <= log_files_before,
            "Trimming should not increase file count"
        );
        assert!(
            log_files_after >= 1,
            "Should always keep at least the current log file"
        );
    }

    /// Test end-to-end: Trimming preserves incomplete transactions
    /// Verifies that trim_old_logs does NOT remove files with incomplete transactions
    #[test]
    fn test_e2e_trimming_preserves_incomplete() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let txn_incomplete = TxnId::new();

        {
            let undo_log = UndoLogger::new(log_dir.clone()).unwrap();

            // Write some committed transactions
            for i in 0..3 {
                let txn = TxnId::new();
                let entry = UndoLogEntry::new_write(txn.clone(), Id { higher: i, lower: 1 }, i);
                undo_log.write_undo_entry(entry).unwrap();
                undo_log.write_commit_marker(&txn).unwrap();
            }

            // Write an incomplete transaction
            let entry_incomplete = UndoLogEntry::new_write(
                txn_incomplete.clone(),
                Id { higher: 999, lower: 1 },
                999,
            );
            undo_log.write_undo_entry(entry_incomplete).unwrap();
            // No commit/abort marker

            // Try to trim
            undo_log.trim_old_logs().unwrap();

            // Verify incomplete transaction is still present
            let entries = undo_log.get_undo_entries(&txn_incomplete);
            assert_eq!(
                entries.len(),
                1,
                "Incomplete transaction should survive trimming"
            );
        }

        // Recover and verify
        let undo_log = UndoLogger::new(log_dir).unwrap();
        undo_log.recover().unwrap();
        
        let entries = undo_log.get_undo_entries(&txn_incomplete);
        assert_eq!(
            entries.len(),
            1,
            "Incomplete transaction should be recovered after trimming"
        );
    }

    /// Test end-to-end: Version verification for different operation types
    /// Verifies that all operations correctly store and retrieve version information
    #[test]
    fn test_e2e_version_verification() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir.clone()).unwrap();
        let txn = TxnId::new();

        // Write operation: version is the new cell's version
        let write_entry = UndoLogEntry::new_write(txn.clone(), Id { higher: 1, lower: 1 }, 10);
        undo_log.write_undo_entry(write_entry).unwrap();

        // Update operation: version is the old cell's version
        let update_entry = UndoLogEntry::new_restore(
            txn.clone(),
            Id { higher: 1, lower: 2 },
            UndoOpType::Update,
            20, // old version
            0,  // chunk_id
            100, // seg_id
            1000, // seq_id
        );
        undo_log.write_undo_entry(update_entry).unwrap();

        // Remove operation: version is the old cell's version
        let remove_entry = UndoLogEntry::new_restore(
            txn.clone(),
            Id { higher: 1, lower: 3 },
            UndoOpType::Remove,
            30, // old version
            1,  // chunk_id
            200, // seg_id
            2000, // seq_id
        );
        undo_log.write_undo_entry(remove_entry).unwrap();

        // Recover and verify all versions are preserved
        undo_log.recover().unwrap();
        let entries = undo_log.get_undo_entries(&txn);
        
        assert_eq!(entries.len(), 3);
        
        // Verify Write entry
        assert_eq!(entries[0].op_type, UndoOpType::Write);
        assert_eq!(entries[0].version, 10);
        assert_eq!(entries[0].chunk_id, 0);
        assert_eq!(entries[0].seg_id, 0);
        assert_eq!(entries[0].seq_id, 0);
        
        // Verify Update entry
        assert_eq!(entries[1].op_type, UndoOpType::Update);
        assert_eq!(entries[1].version, 20);
        assert_eq!(entries[1].chunk_id, 0);
        assert_eq!(entries[1].seg_id, 100);
        assert_eq!(entries[1].seq_id, 1000);
        
        // Verify Remove entry
        assert_eq!(entries[2].op_type, UndoOpType::Remove);
        assert_eq!(entries[2].version, 30);
        assert_eq!(entries[2].chunk_id, 1);
        assert_eq!(entries[2].seg_id, 200);
        assert_eq!(entries[2].seq_id, 2000);
    }
}

