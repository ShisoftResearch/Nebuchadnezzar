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
/// Format: [entry_type: u8][txn_id_len: u32][txn_id: bytes][cell_id: Id][op_type: u8][version_or_chunk: u64][seg_id: u64][seq_id: u64]
/// 
/// For Write operations: version_or_chunk = cell version (to verify during recovery)
/// For Update/Remove operations: version_or_chunk = chunk_id (to locate old segment)
#[derive(Debug, Clone)]
pub struct UndoLogEntry {
    pub txn_id: TxnId,
    pub cell_id: Id,
    pub op_type: UndoOpType,
    /// For Write: cell version to check during recovery
    /// For Update/Remove: chunk_id where old cell is located
    pub version_or_chunk: u64,
    /// For Update/Remove: segment_id where old cell is located (unused for Write)
    pub seg_id: u64,
    /// For Update/Remove: seq_id of segment where old cell is located (unused for Write)
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
    /// For Write operations: version_or_chunk should be the cell version
    /// For Update/Remove operations: version_or_chunk should be the chunk_id
    pub fn new(txn_id: TxnId, cell_id: Id, op_type: UndoOpType, version_or_chunk: u64, seg_id: u64, seq_id: u64) -> Self {
        Self {
            txn_id,
            cell_id,
            op_type,
            version_or_chunk,
            seg_id,
            seq_id,
        }
    }
    
    /// Helper to create a Write entry (for new cells)
    pub fn new_write(txn_id: TxnId, cell_id: Id, version: u64) -> Self {
        Self::new(txn_id, cell_id, UndoOpType::Write, version, 0, 0)
    }
    
    /// Helper to create an Update/Remove entry (with old segment location)
    pub fn new_restore(txn_id: TxnId, cell_id: Id, op_type: UndoOpType, chunk_id: u64, seg_id: u64, seq_id: u64) -> Self {
        debug_assert!(op_type != UndoOpType::Write, "Use new_write for Write operations");
        Self::new(txn_id, cell_id, op_type, chunk_id, seg_id, seq_id)
    }
    
    /// Serialize entry to bytes
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let txn_id_bytes = serde_json::to_vec(&self.txn_id)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let txn_id_len = txn_id_bytes.len() as u32;

        let mut bytes = Vec::with_capacity(1 + 4 + txn_id_bytes.len() + 16 + 1 + 8 + 8 + 8);
        bytes.push(ENTRY_TYPE_UNDO);
        bytes.extend_from_slice(&txn_id_len.to_le_bytes());
        bytes.extend_from_slice(&txn_id_bytes);
        bytes.extend_from_slice(&self.cell_id.higher.to_le_bytes());
        bytes.extend_from_slice(&self.cell_id.lower.to_le_bytes());
        bytes.push(self.op_type as u8);
        bytes.extend_from_slice(&self.version_or_chunk.to_le_bytes());
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
        if bytes.len() < 5 + txn_id_len + 41 {  // +1 for op_type
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

        let version_or_chunk = u64::from_le_bytes([
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
                version_or_chunk,
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
                                .entry(txn_id)
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
        let entry = UndoLogEntry::new(txn_id, cell_id, UndoOpType::Update, 0, 100, 1000);

        let bytes = entry.to_bytes().unwrap();
        let (recovered, size) = UndoLogEntry::from_bytes(&bytes).unwrap();

        assert_eq!(size, bytes.len());
        assert_eq!(recovered.cell_id, entry.cell_id);
        assert_eq!(recovered.op_type, entry.op_type);
        assert_eq!(recovered.version_or_chunk, entry.version_or_chunk);
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
        let entry = UndoLogEntry::new(txn_id.clone(), cell_id, UndoOpType::Update, 0, 100, 1000);

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
        let entry = UndoLogEntry::new(txn_id.clone(), cell_id, UndoOpType::Remove, 0, 100, 1000);

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
        
        let entry1 = UndoLogEntry::new(txn_id.clone(), cell_id1, UndoOpType::Write, 0, 100, 1000);
        let entry2 = UndoLogEntry::new(txn_id.clone(), cell_id2, UndoOpType::Update, 0, 200, 2000);

        undo_log.write_undo_entry(entry1).unwrap();
        undo_log.write_undo_entry(entry2).unwrap();

        let entries = undo_log.get_undo_entries(&txn_id);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].cell_id, cell_id1);
        assert_eq!(entries[1].cell_id, cell_id2);
    }
}

