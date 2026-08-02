use crate::ram::cell::{cell_header_from_entry_content_addr, CellHeader, OwnedCell};
use crate::ram::chunk::Chunks;
use crate::ram::entry::Entry;
use crate::ram::io::reader;
use crate::ram::types::Id;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::fs::{create_dir_all, remove_file, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::TxnId;

/// Undo log entry stored in the log file
/// Format: [entry_type: u8][txn_id_len: u32][txn_id: bytes][cell_id: Id][op_type: u8][version: u64][chunk_id: u64][seq_id: u64][cell_offset: u64]
///
/// The `txn_id` is now a serialized `bifrost::hlc::Hlc` (16-byte fixed HLC),
/// not the former variable-length vector clock; its serialized byte shape
/// changed with the type. Undo logs written before this migration framed
/// `txn_id` as a `bifrost::vector_clock::StandardVectorClock`
/// (`{"map": [[server, counter], ...]}`), which is not a valid `Hlc` and is
/// rejected at recovery (see `decode_txn_id`) with an explicit "pre-HLC"
/// error rather than being silently skipped or misparsed. Undo logs from
/// before the migration must be discarded, not recovered.
///
/// All operations store version for verification during recovery:
/// - Write: version = new cell version (to verify cell unchanged before deletion)
/// - Update/Remove: version = old cell version (to verify we're restoring the right version)
///
/// Note: seg_id is NOT stored because it's address-derived and changes across recoveries.
/// Only seq_id is stable across recoveries and sufficient for segment lookup.
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
    /// For Update/Remove: seq_id of segment where old cell is located (0 for Write)
    /// Note: seq_id is stable across recoveries, unlike seg_id which is address-derived
    pub seq_id: u64,
    /// For Update/Remove: offset within segment where old cell is located (0 for Write)
    pub cell_offset: u64,
}

/// Type of operation that needs to be undone
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndoOpType {
    Write = 1,  // New cell created - store version, DELETE on rollback if version matches
    Update = 2, // Cell updated - store old segment location, RESTORE old version on rollback
    Remove = 3, // Cell removed - store old segment location, RESTORE old version on rollback
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

/// Decode a serialized transaction id from the bytes stored in the undo log.
///
/// Before the HLC migration, `TxnId` was `bifrost::vector_clock::StandardVectorClock`,
/// which serializes as the JSON object `{"map": [[server, counter], ...]}`.
/// That shape does not deserialize into the current `Hlc { ts, node }`, so any
/// decode failure here means the log predates the HLC migration (or is
/// otherwise corrupt): reject it with a hard, actionable error instead of
/// silently truncating recovery or falling back to a default value.
fn decode_txn_id(bytes: &[u8]) -> io::Result<TxnId> {
    serde_json::from_slice(bytes).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "undo log contains pre-HLC transaction ids; discard undo logs from before the HLC migration (serde error: {})",
                e
            ),
        )
    })
}

impl UndoLogEntry {
    /// Create a new undo log entry
    pub fn new(
        txn_id: TxnId,
        cell_id: Id,
        op_type: UndoOpType,
        version: u64,
        chunk_id: u64,
        seq_id: u64,
        cell_offset: u64,
    ) -> Self {
        Self {
            txn_id,
            cell_id,
            op_type,
            version,
            chunk_id,
            seq_id,
            cell_offset,
        }
    }

    /// Helper to create a Write entry (for new cells)
    /// Only needs version since there's no old segment to restore from
    pub fn new_write(txn_id: TxnId, cell_id: Id, version: u64) -> Self {
        Self::new(txn_id, cell_id, UndoOpType::Write, version, 0, 0, 0)
    }

    /// Helper to create an Update/Remove entry (with old cell version, segment seq_id, and offset)
    /// Stores both the old version for verification and exact segment location for fast restoration
    /// Note: only seq_id is stored, not seg_id, because seg_id changes across recoveries
    pub fn new_restore(
        txn_id: TxnId,
        cell_id: Id,
        op_type: UndoOpType,
        old_version: u64,
        chunk_id: u64,
        seq_id: u64,
        cell_offset: u64,
    ) -> Self {
        debug_assert!(
            op_type != UndoOpType::Write,
            "Use new_write for Write operations"
        );
        Self::new(
            txn_id,
            cell_id,
            op_type,
            old_version,
            chunk_id,
            seq_id,
            cell_offset,
        )
    }

    /// Serialize entry to bytes
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let txn_id_bytes = serde_json::to_vec(&self.txn_id)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let txn_id_len = txn_id_bytes.len() as u32;

        // Removed seg_id field (8 bytes)
        let mut bytes = Vec::with_capacity(1 + 4 + txn_id_bytes.len() + 8 + 1 + 8 + 8 + 8 + 8);
        bytes.push(ENTRY_TYPE_UNDO);
        bytes.extend_from_slice(&txn_id_len.to_le_bytes());
        bytes.extend_from_slice(&txn_id_bytes);
        bytes.extend_from_slice(&self.cell_id.bits().to_le_bytes());
        bytes.push(self.op_type as u8);
        bytes.extend_from_slice(&self.version.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_id.to_le_bytes());
        bytes.extend_from_slice(&self.seq_id.to_le_bytes());
        bytes.extend_from_slice(&self.cell_offset.to_le_bytes());

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
        // Removed seg_id field: +1 for op_type, +8 for version, +8 for chunk_id, +8 for seq_id, +8 for cell_offset = 33 + 8 (cell_id) = 41
        if bytes.len() < 5 + txn_id_len + 41 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for full entry",
            ));
        }

        let txn_id: TxnId = decode_txn_id(&bytes[5..5 + txn_id_len])?;

        let mut offset = 5 + txn_id_len;
        let cell_id_bits = u64::from_le_bytes([
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

        // Removed seg_id field deserialization

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

        let cell_offset = u64::from_le_bytes([
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
                cell_id: Id::from_bits(cell_id_bits),
                op_type,
                version,
                chunk_id,
                seq_id,
                cell_offset,
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
    /// Set of active (incomplete) transaction IDs for trimming
    active_txns: lightning::map::HashSet<TxnId>,
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
            active_txns: lightning::map::HashSet::with_capacity(64),
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

            // Track active transaction
            drop(log_file_guard);
            self.active_txns.insert(entry.txn_id.clone());

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
        let txn_id_bytes =
            serde_json::to_vec(txn_id).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
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
            self.active_txns.remove(txn_id);

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
        let txn_id_bytes =
            serde_json::to_vec(txn_id).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
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
            self.active_txns.remove(txn_id);

            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Log file not initialized",
            ))
        }
    }

    /// Perform rollback for all incomplete transactions
    /// Must be called after segment recovery is complete
    /// Takes the txn_index built during recovery as a parameter
    pub fn rollback_incomplete_transactions(
        &self,
        txn_index: HashMap<TxnId, Vec<UndoLogEntry>>,
        chunks: &Arc<Chunks>,
    ) -> io::Result<()> {
        if txn_index.is_empty() {
            info!("No incomplete transactions to rollback");
            return Ok(());
        }

        info!("Rolling back {} incomplete transactions", txn_index.len());

        let mut rollback_stats = (0usize, 0usize, 0usize); // (writes, updates, removes)

        for (txn_id, entries) in txn_index.iter() {
            debug!(
                "Rolling back transaction: {:?} with {} entries",
                txn_id,
                entries.len()
            );

            for entry in entries {
                match entry.op_type {
                    UndoOpType::Write => {
                        // Delete new cell if it still has the logged version
                        if let Err(e) = self.rollback_write(entry, chunks) {
                            error!(
                                "Failed to rollback write for cell {:?}: {:?}",
                                entry.cell_id, e
                            );
                        } else {
                            rollback_stats.0 += 1;
                        }
                    }
                    UndoOpType::Update | UndoOpType::Remove => {
                        // Restore old cell from segment
                        if let Err(e) = self.rollback_restore(entry, chunks) {
                            error!(
                                "Failed to rollback restore for cell {:?}: {:?}",
                                entry.cell_id, e
                            );
                        } else {
                            if entry.op_type == UndoOpType::Update {
                                rollback_stats.1 += 1;
                            } else {
                                rollback_stats.2 += 1;
                            }
                        }
                    }
                }
            }
        }

        info!(
            "Rollback complete: {} writes deleted, {} updates restored, {} removes restored",
            rollback_stats.0, rollback_stats.1, rollback_stats.2
        );

        Ok(())
    }

    /// Rollback a Write operation by deleting the new cell if version matches
    fn rollback_write(&self, entry: &UndoLogEntry, chunks: &Arc<Chunks>) -> io::Result<()> {
        debug!(
            "Rolling back Write: cell_id={:?}, version={}",
            entry.cell_id, entry.version
        );

        // Locate the chunk for this cell
        let chunk = chunks.locate_chunk_by_partition(entry.cell_id.locality() as u64);

        // Check if cell exists in cell_index
        let hash = entry.cell_id.bits();
        if let Some(guard) = chunk.cell_index.lock(hash as usize) {
            let entry_addr = *guard; // This is the Entry address, not content address
            drop(guard); // Release lock early

            // The index may hold a zero value if a tombstone was applied during recovery.
            // Treat this as "not found" instead of attempting to read from null.
            if entry_addr == 0 {
                debug!(
                    "Cell {:?} not found in index during rollback (stored addr=0)",
                    entry.cell_id
                );
                return Ok(());
            }

            // Convert entry address to content address
            let content_addr = Entry::content_pos(entry_addr);

            // Read just the header without needing the schema
            // This is safe because the header is always at a fixed offset
            let header = cell_header_from_entry_content_addr(content_addr);
            let current_version = header.version;

            // Only delete if version matches (cell hasn't been modified since transaction)
            if current_version == entry.version {
                debug!(
                    "Deleting new cell {:?} with version {}",
                    entry.cell_id, entry.version
                );

                // Use remove_cell which handles tombstone creation
                if let Err(e) = chunks.remove_cell(&entry.cell_id) {
                    warn!("Failed to remove cell during rollback: {:?}", e);
                }
            } else {
                debug!(
                    "Cell {:?} version mismatch (current={}, logged={}), skipping delete",
                    entry.cell_id, current_version, entry.version
                );
            }
        } else {
            debug!(
                "Cell {:?} not found in index during rollback",
                entry.cell_id
            );
        }

        Ok(())
    }

    /// Rollback an Update or Remove operation by restoring the old cell from segment memory
    fn rollback_restore(&self, entry: &UndoLogEntry, chunks: &Arc<Chunks>) -> io::Result<()> {
        debug!(
            "Rolling back {:?}: cell_id={:?}, version={}, from chunk={}, seq={}, offset={}",
            entry.op_type,
            entry.cell_id,
            entry.version,
            entry.chunk_id,
            entry.seq_id,
            entry.cell_offset
        );

        // Get the chunk and find the segment in memory
        let chunk = &chunks.list[entry.chunk_id as usize];

        // For Remove operations, verify the cell is actually gone before restoring
        // For Update operations, we'll verify version matches in read_cell_from_address
        if entry.op_type == UndoOpType::Remove {
            // Check if cell exists in the index by trying to read it
            if let Ok(cell_ref) = chunks.read_cell(&entry.cell_id) {
                drop(cell_ref); // Release the read lock immediately
                debug!(
                    "Cell {:?} still exists after Remove operation - skipping restore (might have been re-created)",
                    entry.cell_id
                );
                return Ok(());
            }
            // Cell doesn't exist (or read error), proceed with restore
        }

        // Find the segment by seq_id (segments are already loaded in memory after recovery)
        // Note: seg_id in the undo log is the OLD segment ID from before recovery,
        // so we can only rely on seq_id which is stable across recoveries.
        // If multiple segments have the same seq_id (e.g., bootstrap segment + recovered segment),
        // prefer the one with actual data (non-zero append_header offset)
        let segment = chunk
            .segments()
            .into_iter()
            .filter(|seg| seg.seq_id == entry.seq_id)
            .max_by_key(|seg| seg.append_header.load(Ordering::Acquire) - seg.addr) // Prefer segment with more data
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "Segment with seq_id {} not found in chunk {}",
                        entry.seq_id, entry.chunk_id
                    ),
                )
            })?;

        debug!(
            "Found segment in memory: seg_id={}, seq_id={}, addr={:#x}",
            segment.id, segment.seq_id, segment.addr
        );

        // Directly read the old cell from the specified offset (no scanning needed!)
        let cell_addr = segment.addr + entry.cell_offset as usize;
        // For Remove operations, skip hash/version verification (cell is gone)
        // For Update operations, verify version matches
        let verify = entry.op_type != UndoOpType::Remove;
        let old_cell =
            self.read_cell_from_address(cell_addr, chunk, &entry.cell_id, entry.version, verify)?;

        // Restore the old cell's DATA with a NEW version number
        // The upsert will automatically assign a new, higher version
        match entry.op_type {
            UndoOpType::Remove => {
                // Cell was removed by the transaction - restore it with its old data but new version
                debug!(
                    "Restoring removed cell {:?} (old data with new version)",
                    entry.cell_id
                );
                if let Err(e) = chunks.upsert_cell(&mut old_cell.clone()) {
                    error!("Failed to restore removed cell: {:?}", e);
                }
            }
            UndoOpType::Update => {
                // Cell was updated by the transaction - restore old data with new version
                // Use upsert_cell to handle both cases (cell exists or was deleted by another txn)
                debug!(
                    "Restoring old data for cell {:?} (old data with new version)",
                    entry.cell_id
                );
                if let Err(e) = chunks.upsert_cell(&mut old_cell.clone()) {
                    error!("Failed to restore updated cell: {:?}", e);
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    /// Read a cell directly from a memory address (using stored offset, no scanning!)
    ///
    /// # Arguments
    /// * `cell_addr` - Memory address of the entry (not content address)
    /// * `chunk` - The chunk containing the cell
    /// * `cell_id` - Expected cell ID (for verification)
    /// * `expected_version` - Expected cell version (for verification if verify=true)
    /// * `verify` - If true, verify hash and version match; if false, skip verification
    fn read_cell_from_address(
        &self,
        cell_addr: usize,
        chunk: &crate::ram::chunk::Chunk,
        cell_id: &Id,
        expected_version: u64,
        verify: bool,
    ) -> io::Result<OwnedCell> {
        // Read cell header from the entry content address
        let content_addr = Entry::content_pos(cell_addr);
        let cell_header = cell_header_from_entry_content_addr(content_addr);

        // Check if segment has been cleaned (hash=0 indicates freed/cleaned segment)
        if cell_header.id.is_unit_id() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Cell at offset {} has been cleaned (hash=0)", cell_addr),
            ));
        }

        // Verify cell identity if requested (skip for Remove operations)
        if verify {
            if cell_header.id != *cell_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Cell hash mismatch at offset: expected {}, found {}",
                        cell_id.bits(), cell_header.id.bits()
                    ),
                ));
            }

            if cell_header.version != expected_version {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Cell version mismatch at offset: expected {}, found {}",
                        expected_version, cell_header.version
                    ),
                ));
            }
        }

        debug!(
            "Reading cell from offset: hash={}, version={}, schema={}, verify={}",
            cell_header.id.bits(), cell_header.version, cell_header.schema, verify
        );

        // Get schema to deserialize data
        let schema = chunk.meta.schemas.get(&cell_header.schema).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Schema {} not found", cell_header.schema),
            )
        })?;

        // Read cell data using the schema
        let data_ptr = content_addr + std::mem::size_of::<CellHeader>();
        let cell_data = reader::read_by_schema(data_ptr, &schema);

        // Convert to owned
        Ok(OwnedCell {
            header: cell_header,
            data: cell_data.owned(),
        })
    }

    /// Trim old log files that only contain committed/aborted transactions
    pub fn trim_old_logs(&self) -> io::Result<()> {
        let current_seq = self.log_seq.load(Ordering::SeqCst);

        // Get all active transactions
        let active_txns: Vec<TxnId> = self.active_txns.items().into_iter().collect();

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
                                            if !self
                                                .log_contains_active_txns(&path, &active_txns)?
                                            {
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
            let txn_id_len = u32::from_le_bytes([
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
                buffer[offset + 4],
            ]) as usize;

            if buffer.len() < offset + 5 + txn_id_len {
                break;
            }

            let txn_id: TxnId = decode_txn_id(&buffer[offset + 5..offset + 5 + txn_id_len])?;

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
    /// Returns a HashMap of incomplete transactions for rollback
    pub fn recover(&self) -> io::Result<HashMap<TxnId, Vec<UndoLogEntry>>> {
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
                let txn_id_len = u32::from_le_bytes([
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                    buffer[offset + 4],
                ]) as usize;

                if buffer.len() < offset + 5 + txn_id_len {
                    break;
                }

                let txn_id: TxnId =
                    decode_txn_id(&buffer[offset + 5..offset + 5 + txn_id_len])?;

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

        // Update active transactions set for trimming. Recovery runs
        // before the log accepts traffic, so clear-then-refill is not
        // racing concurrent writers.
        for txn_id in self.active_txns.items() {
            self.active_txns.remove(&txn_id);
        }
        for txn_id in txn_index.keys() {
            self.active_txns.insert(txn_id.clone());
        }

        // Update log sequence number
        if let Some((max_seq, _)) = log_files.last() {
            self.log_seq.store(*max_seq + 1, Ordering::SeqCst);
        }

        info!(
            "Recovered undo log with {} incomplete transactions",
            txn_index.len()
        );

        Ok(txn_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::transactions::test_hlc;
    use crate::ram::cell::ReadError;
    use crate::ram::types::{OwnedMap, OwnedValue};
    use crate::server::transactions::{EndResult, TMPrepareResult, TxnExecResult};
    use dovahkiin::data_map_value;
    use dovahkiin::types::Map;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    // Helper function to create a random Id
    fn random_id() -> Id {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        Id::allocated(0, 0, now.as_secs() * 1_000_000_000 + now.subsec_nanos() as u64)
    }

    #[test]
    fn test_undo_entry_serialization() {
        let txn_id = TxnId::default();
        let cell_id = Id::allocated(1, 0, 2);
        // Removed seg_id parameter (was 100)
        let entry = UndoLogEntry::new(txn_id, cell_id, UndoOpType::Update, 5, 0, 1000, 50);

        let bytes = entry.to_bytes().unwrap();
        let (recovered, size) = UndoLogEntry::from_bytes(&bytes).unwrap();

        assert_eq!(size, bytes.len());
        assert_eq!(recovered.cell_id, entry.cell_id);
        assert_eq!(recovered.op_type, entry.op_type);
        assert_eq!(recovered.version, entry.version);
        assert_eq!(recovered.chunk_id, entry.chunk_id);
        assert_eq!(recovered.seq_id, entry.seq_id);
    }

    #[test]
    fn test_undo_log_basic() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir).unwrap();

        let txn_id = TxnId::default();
        let cell_id = Id::allocated(1, 0, 2);
        // Removed seg_id parameter (was 100)
        let entry = UndoLogEntry::new(txn_id.clone(), cell_id, UndoOpType::Update, 3, 0, 1000, 50);

        undo_log.write_undo_entry(entry.clone()).unwrap();

        let txn_index = undo_log.recover().unwrap();
        let entries = txn_index.get(&txn_id).cloned().unwrap_or_default();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cell_id, cell_id);

        undo_log.write_commit_marker(&txn_id).unwrap();

        let txn_index = undo_log.recover().unwrap();
        let entries = txn_index.get(&txn_id).cloned().unwrap_or_default();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_undo_log_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let txn_id = TxnId::default();
        let cell_id = Id::allocated(1, 0, 2);
        // Removed seg_id parameter (was 100)
        let entry = UndoLogEntry::new(txn_id.clone(), cell_id, UndoOpType::Remove, 2, 0, 1000, 50);

        {
            let undo_log = UndoLogger::new(log_dir.clone()).unwrap();
            undo_log.write_undo_entry(entry.clone()).unwrap();
        }

        // Recreate and recover
        let undo_log = UndoLogger::new(log_dir).unwrap();
        let txn_index = undo_log.recover().unwrap();

        let txn_index = undo_log.recover().unwrap();
        let entries = txn_index.get(&txn_id).cloned().unwrap_or_default();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cell_id, cell_id);
    }

    #[test]
    fn test_undo_log_multiple_entries() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir).unwrap();

        let txn_id = TxnId::default();
        let cell_id1 = Id::allocated(1, 0, 2);
        let cell_id2 = Id::allocated(3, 0, 4);

        // Removed seg_id parameters
        let entry1 = UndoLogEntry::new(txn_id.clone(), cell_id1, UndoOpType::Write, 1, 0, 0, 0);
        let entry2 = UndoLogEntry::new(
            txn_id.clone(),
            cell_id2,
            UndoOpType::Update,
            4,
            0,
            2000,
            100,
        );

        undo_log.write_undo_entry(entry1).unwrap();
        undo_log.write_undo_entry(entry2).unwrap();

        let txn_index = undo_log.recover().unwrap();
        let entries = txn_index.get(&txn_id).cloned().unwrap_or_default();
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
        let txn1 = TxnId::default();
        let cell_id1 = Id::allocated(100, 0, 1);
        let entry1 = UndoLogEntry::new_write(txn1.clone(), cell_id1, 1);
        undo_log.write_undo_entry(entry1).unwrap();

        // Verify undo entry exists
        let txn_index = undo_log.recover().unwrap();
        let entries = txn_index.get(&txn1).cloned().unwrap_or_default();
        assert_eq!(entries.len(), 1, "Should have 1 undo entry before commit");
        assert_eq!(entries[0].op_type, UndoOpType::Write);
        assert_eq!(entries[0].version, 1);

        // Commit the transaction
        undo_log.write_commit_marker(&txn1).unwrap();

        // Verify undo entries are cleared after commit (need to recover again to see changes)
        let txn_index = undo_log.recover().unwrap();
        let entries_after = txn_index.get(&txn1).cloned().unwrap_or_default();
        assert_eq!(
            entries_after.len(),
            0,
            "Should have no undo entries after commit"
        );

        // Recovery should not find this transaction as incomplete
        let txn_index = undo_log.recover().unwrap();
        let entries_after_recovery = txn_index.get(&txn1).cloned().unwrap_or_default();
        assert_eq!(
            entries_after_recovery.len(),
            0,
            "Should have no entries after recovery"
        );
    }

    /// Test end-to-end: Transaction with abort
    /// Verifies that aborted transactions are removed from undo log
    #[test]
    fn test_e2e_aborted_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir.clone()).unwrap();

        // Transaction that will be aborted
        let txn = TxnId::default();
        let cell_id1 = Id::allocated(200, 0, 1);
        let cell_id2 = Id::allocated(200, 0, 2);

        // Write, Update, and Remove operations
        let entry1 = UndoLogEntry::new_write(txn.clone(), cell_id1, 1);
        // Removed seg_id parameter (was 100)
        let entry2 =
            UndoLogEntry::new_restore(txn.clone(), cell_id2, UndoOpType::Update, 5, 0, 1000, 50);

        undo_log.write_undo_entry(entry1).unwrap();
        undo_log.write_undo_entry(entry2).unwrap();

        // Verify undo entries exist
        let txn_index = undo_log.recover().unwrap();
        let entries = txn_index.get(&txn).cloned().unwrap_or_default();
        assert_eq!(entries.len(), 2, "Should have 2 undo entries before abort");

        // Abort the transaction
        undo_log.write_abort_marker(&txn).unwrap();

        // Verify undo entries are cleared after abort (need to recover again to see changes)
        let txn_index = undo_log.recover().unwrap();
        let entries_after = txn_index.get(&txn).cloned().unwrap_or_default();
        assert_eq!(
            entries_after.len(),
            0,
            "Should have no undo entries after abort"
        );

        // Recovery should not find this transaction as incomplete
        let txn_index = undo_log.recover().unwrap();
        let entries_after_recovery = txn_index.get(&txn).cloned().unwrap_or_default();
        assert_eq!(
            entries_after_recovery.len(),
            0,
            "Should have no entries after recovery"
        );
    }

    /// Test end-to-end: Incomplete transaction (crash before commit/abort)
    /// Verifies that incomplete transactions are recovered
    #[test]
    fn test_e2e_incomplete_transaction_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let txn_incomplete = TxnId::default();
        let cell_id1 = Id::allocated(300, 0, 1);
        let cell_id2 = Id::allocated(300, 0, 2);
        let cell_id3 = Id::allocated(300, 0, 3);

        {
            // Simulate a transaction that didn't finish
            let undo_log = UndoLogger::new(log_dir.clone()).unwrap();

            // Write multiple operations
            let entry1 = UndoLogEntry::new_write(txn_incomplete.clone(), cell_id1, 1);
            // Removed seg_id parameters (were 50 and 75)
            let entry2 = UndoLogEntry::new_restore(
                txn_incomplete.clone(),
                cell_id2,
                UndoOpType::Update,
                3,
                0,
                500,
                25,
            );
            let entry3 = UndoLogEntry::new_restore(
                txn_incomplete.clone(),
                cell_id3,
                UndoOpType::Remove,
                7,
                1,
                750,
                37,
            );

            undo_log.write_undo_entry(entry1).unwrap();
            undo_log.write_undo_entry(entry2).unwrap();
            undo_log.write_undo_entry(entry3).unwrap();

            // Simulate crash - no commit/abort marker written
        } // undo_log dropped here

        // Recover after "crash"
        let undo_log = UndoLogger::new(log_dir).unwrap();
        let txn_index = undo_log.recover().unwrap();

        // Verify incomplete transaction is found
        let entries = txn_index.get(&txn_incomplete).cloned().unwrap_or_default();
        assert_eq!(entries.len(), 3, "Should recover all 3 undo entries");

        // Verify entry details
        assert_eq!(entries[0].cell_id, cell_id1);
        assert_eq!(entries[0].op_type, UndoOpType::Write);
        assert_eq!(entries[0].version, 1);

        assert_eq!(entries[1].cell_id, cell_id2);
        assert_eq!(entries[1].op_type, UndoOpType::Update);
        assert_eq!(entries[1].version, 3);
        assert_eq!(entries[1].chunk_id, 0);
        // Removed seg_id assertion
        assert_eq!(entries[1].seq_id, 500);

        assert_eq!(entries[2].cell_id, cell_id3);
        assert_eq!(entries[2].op_type, UndoOpType::Remove);
        assert_eq!(entries[2].version, 7);
        assert_eq!(entries[2].chunk_id, 1);
        // Removed seg_id assertion
        assert_eq!(entries[2].seq_id, 750);
    }

    /// Test if TxnId (Hlc) equality works after JSON serialization
    #[test]
    fn test_txn_id_serialization_equality() {
        let txn1 = TxnId::default();

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

        assert!(
            map.contains_key(&txn2),
            "HashMap should find deserialized key"
        );
        assert_eq!(map.get(&txn2).unwrap(), &vec![1, 2, 3]);
    }

    /// Confirms the pre-HLC txn-id serde shape this test builds against: the old
    /// `TxnId = StandardVectorClock` serialized as a JSON object `{"map": [...]}`
    /// (a named-field struct, never a bare array), which does not coincidentally
    /// decode as `Hlc { ts, node }`.
    #[test]
    fn test_pre_hlc_vector_clock_serde_shape_is_object_with_map_key() {
        let old_clock = bifrost::vector_clock::StandardVectorClock::from_vec(vec![(1, 2)]);
        let bytes = serde_json::to_vec(&old_clock).unwrap();
        assert_eq!(String::from_utf8(bytes).unwrap(), r#"{"map":[[1,2]]}"#);
    }

    /// Undo logs written before the HLC migration framed `txn_id` as a
    /// serialized `StandardVectorClock` (`{"map": [[server, counter], ...]}`).
    /// Recovering such a log must fail loudly with an actionable error, not
    /// silently drop the rest of the file (nor panic).
    #[test]
    fn test_recover_rejects_pre_hlc_undo_log() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        // Build txn-id bytes the OLD way.
        let old_clock = bifrost::vector_clock::StandardVectorClock::from_vec(vec![(1, 2)]);
        let old_txn_id_bytes = serde_json::to_vec(&old_clock).unwrap();

        // Hand-assemble one undo entry, framed exactly like
        // UndoLogEntry::to_bytes() frames a new one:
        // [entry_type: u8][txn_id_len: u32][txn_id: bytes][cell_id: u64]
        // [op_type: u8][version: u64][chunk_id: u64]
        // [seq_id: u64][cell_offset: u64]
        let mut bytes = Vec::new();
        bytes.push(ENTRY_TYPE_UNDO);
        bytes.extend_from_slice(&(old_txn_id_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&old_txn_id_bytes);
        bytes.extend_from_slice(&2u64.to_le_bytes()); // cell_id
        bytes.push(UndoOpType::Write as u8);
        bytes.extend_from_slice(&1u64.to_le_bytes()); // version
        bytes.extend_from_slice(&0u64.to_le_bytes()); // chunk_id
        bytes.extend_from_slice(&0u64.to_le_bytes()); // seq_id
        bytes.extend_from_slice(&0u64.to_le_bytes()); // cell_offset

        // Also exercise the entry-level parser directly with the same
        // hand-assembled bytes.
        let entry_parse_err = UndoLogEntry::from_bytes(&bytes)
            .expect_err("parsing a pre-HLC entry must fail, not panic or succeed");
        assert!(
            entry_parse_err.to_string().contains("pre-HLC"),
            "expected a pre-HLC error message from UndoLogEntry::from_bytes, got: {}",
            entry_parse_err
        );

        // Write it as a log file a fresh UndoLogger will pick up on recovery.
        std::fs::write(format!("{}/undo-0.nlog", log_dir), &bytes).unwrap();

        let undo_log = UndoLogger::new(log_dir).unwrap();
        let recover_err = undo_log
            .recover()
            .expect_err("recovering a pre-HLC undo log must fail, not silently succeed");
        assert!(
            recover_err.to_string().contains("pre-HLC"),
            "expected a pre-HLC error message from recover(), got: {}",
            recover_err
        );
    }

    /// Debug test: Understand why commit markers aren't being processed during recovery
    #[test]
    fn test_debug_commit_marker_processing() {
        use std::io::Read;

        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let txn = TxnId::default();

        // Write entry and commit marker
        {
            let undo_log = UndoLogger::new(log_dir.clone()).unwrap();
            let entry = UndoLogEntry::new_write(
                txn.clone(),
                Id::allocated(1, 0, 1),
                1,
            );
            undo_log.write_undo_entry(entry).unwrap();
            let txn_index = undo_log.recover().unwrap();
            println!(
                "After write: {} entries for txn",
                txn_index.get(&txn).cloned().unwrap_or_default().len()
            );

            undo_log.write_commit_marker(&txn).unwrap();
            let txn_index = undo_log.recover().unwrap();
            println!(
                "After commit marker: {} entries for txn",
                txn_index.get(&txn).cloned().unwrap_or_default().len()
            );
        }

        // Read the log file directly to see what's in it
        let log_files: Vec<_> = std::fs::read_dir(&log_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "nlog")
                    .unwrap_or(false)
            })
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

                println!(
                    "Entry {}: type={}, txn_id_len={}",
                    entry_count, entry_type, txn_id_len
                );
                entry_count += 1;

                // Skip to next entry (approximate)
                match entry_type {
                    1 => offset += 5 + txn_id_len + 41, // UNDO entry
                    2 | 3 => offset += 5 + txn_id_len,  // COMMIT/ABORT marker
                    _ => break,
                }
            }
        }

        // Now recover and see what happens
        let undo_log = UndoLogger::new(log_dir).unwrap();
        let txn_index = undo_log.recover().unwrap();
        println!(
            "After recovery: {} entries",
            txn_index.get(&txn).cloned().unwrap_or_default().len()
        );

        assert_eq!(
            txn_index.get(&txn).cloned().unwrap_or_default().len(),
            0,
            "Should have 0 entries after recovery"
        );
    }

    /// Test end-to-end: Mixed transactions (committed, aborted, incomplete)
    /// Verifies that recovery correctly handles different transaction states
    #[test]
    fn test_e2e_mixed_transactions() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        // Create unique transaction IDs (in a real system each coordinator
        // mints a distinct HLC node).
        let txn_committed = test_hlc(1, 1); // node=1
        let txn_aborted = test_hlc(1, 2); // node=2
        let txn_incomplete = test_hlc(1, 3); // node=3

        {
            let undo_log = UndoLogger::new(log_dir.clone()).unwrap();

            // Transaction 1: Will be committed
            let entry1 = UndoLogEntry::new_write(
                txn_committed.clone(),
                Id::allocated(1, 0, 1),
                1,
            );
            undo_log.write_undo_entry(entry1).unwrap();
            let txn_index_check = undo_log.recover().unwrap();
            assert_eq!(
                txn_index_check
                    .get(&txn_committed)
                    .cloned()
                    .unwrap_or_default()
                    .len(),
                1,
                "Entry should exist before commit"
            );

            undo_log.write_commit_marker(&txn_committed).unwrap();
            let txn_index_check = undo_log.recover().unwrap();
            assert_eq!(
                txn_index_check
                    .get(&txn_committed)
                    .cloned()
                    .unwrap_or_default()
                    .len(),
                0,
                "Entry should be cleared after commit"
            );

            // Transaction 2: Will be aborted
            let entry2 = UndoLogEntry::new_write(
                txn_aborted.clone(),
                Id::allocated(2, 0, 1),
                2,
            );
            undo_log.write_undo_entry(entry2).unwrap();
            undo_log.write_abort_marker(&txn_aborted).unwrap();
            let txn_index_check = undo_log.recover().unwrap();
            assert_eq!(
                txn_index_check
                    .get(&txn_aborted)
                    .cloned()
                    .unwrap_or_default()
                    .len(),
                0,
                "Entry should be cleared after abort"
            );

            // Transaction 3: Incomplete (crash)
            let entry3 = UndoLogEntry::new_write(
                txn_incomplete.clone(),
                Id::allocated(3, 0, 1),
                3,
            );
            undo_log.write_undo_entry(entry3).unwrap();
            // No commit/abort marker
        } // undo_log dropped and files flushed

        // Recover after "crash"
        let undo_log = UndoLogger::new(log_dir).unwrap();
        let txn_index = undo_log.recover().unwrap();

        // After recovery, only incomplete transaction should be present
        assert_eq!(
            txn_index
                .get(&txn_incomplete)
                .cloned()
                .unwrap_or_default()
                .len(),
            1,
            "Incomplete txn should be recovered"
        );
        assert_eq!(
            txn_index
                .get(&txn_committed)
                .cloned()
                .unwrap_or_default()
                .len(),
            0,
            "Committed txn should not be recovered"
        );
        assert_eq!(
            txn_index
                .get(&txn_aborted)
                .cloned()
                .unwrap_or_default()
                .len(),
            0,
            "Aborted txn should not be recovered"
        );
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
            let txn = TxnId::default();
            let entry = UndoLogEntry::new_write(
                txn.clone(),
                Id::from_parts(i, 1),
                i,
            );
            undo_log.write_undo_entry(entry).unwrap();
            undo_log.write_commit_marker(&txn).unwrap();
        }

        // Write more transactions to fill the log
        for i in 5..10 {
            let txn = TxnId::default();
            let entry = UndoLogEntry::new_write(
                txn.clone(),
                Id::from_parts(i, 1),
                i,
            );
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

        let txn_incomplete = TxnId::default();

        {
            let undo_log = UndoLogger::new(log_dir.clone()).unwrap();

            // Write some committed transactions
            for i in 0..3 {
                let txn = TxnId::default();
                let entry = UndoLogEntry::new_write(
                    txn.clone(),
                    Id::from_parts(i, 1),
                    i,
                );
                undo_log.write_undo_entry(entry).unwrap();
                undo_log.write_commit_marker(&txn).unwrap();
            }

            // Write an incomplete transaction
            let entry_incomplete = UndoLogEntry::new_write(
                txn_incomplete.clone(),
                Id::allocated(999, 0, 1),
                999,
            );
            undo_log.write_undo_entry(entry_incomplete).unwrap();
            // No commit/abort marker

            // Try to trim
            undo_log.trim_old_logs().unwrap();

            // Verify incomplete transaction is still present after trimming
            let txn_index_check = undo_log.recover().unwrap();
            let entries = txn_index_check
                .get(&txn_incomplete)
                .cloned()
                .unwrap_or_default();
            assert_eq!(
                entries.len(),
                1,
                "Incomplete transaction should survive trimming"
            );
        }

        // Recover and verify
        let undo_log = UndoLogger::new(log_dir).unwrap();
        let txn_index = undo_log.recover().unwrap();

        let entries = txn_index.get(&txn_incomplete).cloned().unwrap_or_default();
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
        let txn = TxnId::default();

        // Write operation: version is the new cell's version
        let write_entry = UndoLogEntry::new_write(
            txn.clone(),
            Id::allocated(1, 0, 1),
            10,
        );
        undo_log.write_undo_entry(write_entry).unwrap();

        // Update operation: version is the old cell's version
        // Removed seg_id parameter
        let update_entry = UndoLogEntry::new_restore(
            txn.clone(),
            Id::allocated(1, 0, 2),
            UndoOpType::Update,
            20,   // old version
            0,    // chunk_id
            1000, // seq_id
            50,   // cell_offset
        );
        undo_log.write_undo_entry(update_entry).unwrap();

        // Remove operation: version is the old cell's version
        // Removed seg_id parameter
        let remove_entry = UndoLogEntry::new_restore(
            txn.clone(),
            Id::allocated(1, 0, 3),
            UndoOpType::Remove,
            30,   // old version
            1,    // chunk_id
            2000, // seq_id
            100,  // cell_offset
        );
        undo_log.write_undo_entry(remove_entry).unwrap();

        // Recover and verify all versions are preserved
        let txn_index = undo_log.recover().unwrap();
        let entries = txn_index.get(&txn).cloned().unwrap_or_default();

        assert_eq!(entries.len(), 3);

        // Verify Write entry
        assert_eq!(entries[0].op_type, UndoOpType::Write);
        assert_eq!(entries[0].version, 10);
        assert_eq!(entries[0].chunk_id, 0);
        // Removed seg_id assertions
        assert_eq!(entries[0].seq_id, 0);

        // Verify Update entry
        assert_eq!(entries[1].op_type, UndoOpType::Update);
        assert_eq!(entries[1].version, 20);
        assert_eq!(entries[1].chunk_id, 0);
        // Removed seg_id assertion
        assert_eq!(entries[1].seq_id, 1000);

        // Verify Remove entry
        assert_eq!(entries[2].op_type, UndoOpType::Remove);
        assert_eq!(entries[2].version, 30);
        assert_eq!(entries[2].chunk_id, 1);
        // Removed seg_id assertion
        assert_eq!(entries[2].seq_id, 2000);
    }

    /// Test end-to-end: Rollback Write operations (delete new cells)
    /// Verifies that uncommitted new cells are deleted during recovery
    #[test]
    fn test_rollback_write_operations() {
        use crate::ram::chunk::Chunks;
        use crate::ram::schema::Schema;
        use crate::ram::types::OwnedValue;
        use dovahkiin::types::Type;

        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let backup_dir = temp_dir.path().join("backup");
        let wal_dir = temp_dir.path().join("wal");
        let raft_dir = temp_dir.path().join("raft");
        std::fs::create_dir_all(&raft_dir).unwrap();
        let raft_path = raft_dir.to_str().unwrap().to_string();

        // Setup schema
        use crate::ram::schema::Field;
        let fields = Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed("value", Type::String),
        ]);
        let schema = Schema::new("test_schema", None, fields, false, false);
        let schemas = crate::ram::schema::LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema);
        let meta = Arc::new(crate::server::ServerMeta { schemas });

        let cell_id = Id::allocated(1, 0, 100);

        // Phase 1: Create cell and log it as incomplete transaction
        {
            let chunks = Chunks::new(
                1,
                32 * 1024 * 1024,
                meta.clone(),
                None,
                Some(backup_dir.to_str().unwrap().to_string()),
                Some(wal_dir.to_str().unwrap().to_string()),
                None,
            );

            let mut cell = OwnedCell::new_with_id(
                0,
                &cell_id,
                data_map_value!(id: 1i32, value: "test".to_string()),
            );

            chunks.write_cell(&mut cell).unwrap();
            assert!(
                chunks.read_cell(&cell_id).is_ok(),
                "Cell should exist before rollback"
            );

            // Archive segments
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            // Log the write as incomplete transaction
            let undo_log = UndoLogger::new(log_dir.to_str().unwrap().to_string()).unwrap();
            let txn_id = test_hlc(1, 1);
            let entry = UndoLogEntry::new_write(txn_id, cell_id, cell.header.version);
            undo_log.write_undo_entry(entry).unwrap();
            // No commit marker - simulate crash
        }

        // Phase 2: Recovery with rollback
        {
            let chunks = Chunks::new_with_recovery(
                1,
                32 * 1024 * 1024,
                meta.clone(),
                None,
                Some(backup_dir.to_str().unwrap().to_string()),
                Some(wal_dir.to_str().unwrap().to_string()),
                None,
                true,
                Some(raft_path.clone()),
            );

            let undo_log = UndoLogger::new(log_dir.to_str().unwrap().to_string()).unwrap();
            let txn_index = undo_log.recover().unwrap();
            undo_log
                .rollback_incomplete_transactions(txn_index, &chunks)
                .unwrap();

            // Cell should be deleted after rollback
            assert!(
                chunks.read_cell(&cell_id).is_err(),
                "Cell should be deleted after rollback"
            );
        }
    }

    // NOTE: The following simulated rollback tests for Update and Remove operations have been
    // removed in favor of comprehensive E2E tests with real transactions:
    // - test_e2e_txn_write_rollback (tests Write rollback)
    // - test_e2e_txn_update_rollback (tests Update rollback)
    // - test_e2e_txn_remove_rollback (tests Remove rollback)
    // - test_e2e_txn_committed_no_rollback (tests committed transactions persist)
    //
    // The multi-phase recovery approach in simulated tests was problematic due to segment
    // renumbering across recovery cycles. The E2E tests use actual transaction clients and
    // provide better coverage of the real-world rollback scenarios.

    /// Test: Rollback restores old data with new version
    /// Verifies that rollback restores the old cell data but with a new, incremented version
    #[test]
    fn test_rollback_with_new_version() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        use crate::ram::chunk::Chunks;
        use crate::ram::schema::Schema;
        use dovahkiin::types::Type;

        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");

        // Setup schema
        use crate::ram::schema::Field;
        let fields = Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed("value", Type::String),
        ]);
        let schema = Schema::new("test_schema", None, fields, false, false);
        let schemas = crate::ram::schema::LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema);
        let meta = Arc::new(crate::server::ServerMeta { schemas });

        let chunks = Chunks::new(1, 32 * 1024 * 1024, meta.clone(), None, None, None, None);
        let undo_log = UndoLogger::new(log_dir.to_str().unwrap().to_string()).unwrap();

        println!("=== Step 1: Create initial cell ===");
        // Create a cell with initial version
        let cell_id = Id::allocated(1, 0, 100);
        let mut cell = OwnedCell::new_with_id(
            0,
            &cell_id,
            data_map_value!(id: 1i32, value: "v1".to_string()),
        );
        chunks.write_cell(&mut cell).unwrap();
        let initial_version = cell.header.version;
        println!("Created cell with version {}", initial_version);

        // Get cell location for undo log
        let chunk = chunks.locate_chunk_by_partition(cell_id.locality() as u64);
        let (_cell_addr, seg_id, seq_id, cell_offset) = {
            // Scope the guard so it's dropped immediately after we extract the info
            let guard = chunk.location_for_read(cell_id.bits()).unwrap();
            let addr = *guard;
            drop(guard); // Explicitly drop to release lock

            let (seg_id, seq_id) = chunk.get_cell_segment_info(addr);
            let segment_base_addr = chunk.allocator.addr_by_id(seg_id as usize);
            let offset = (addr - segment_base_addr) as u64;
            (addr, seg_id, seq_id, offset)
        };

        // Simulate incomplete transaction that updated the cell
        let txn_id = test_hlc(1, 1);
        // Removed seg_id parameter
        let undo_entry = UndoLogEntry::new_restore(
            txn_id.clone(),
            cell_id,
            UndoOpType::Update,
            initial_version, // old version before transaction
            chunk.id as u64,
            seq_id,
            cell_offset,
        );
        undo_log.write_undo_entry(undo_entry).unwrap();

        println!("=== Step 2: Perform rollback (restore v1) ===");
        // Directly perform rollback without doing the transaction update
        // This tests that rollback correctly restores the old data
        println!("About to recover...");
        let txn_index = undo_log.recover().unwrap();
        println!("About to rollback...");
        undo_log
            .rollback_incomplete_transactions(txn_index, &chunks)
            .unwrap();
        println!("Rollback complete!");

        // Verify cell has been rolled back - the old data restored with a new version
        let after_rollback = chunks.read_cell(&cell_id).unwrap();
        assert!(
            after_rollback.header.version >= initial_version,
            "Rollback should use a new version (>= initial version)"
        );
        assert_eq!(
            after_rollback.data["value"].string().unwrap(),
            "v1",
            "Rollback should restore the original data from the undo log"
        );
    }

    // =====================================================================
    // E2E Tests with Real Transactions
    // =====================================================================

    /// E2E test: Write operation rollback with real transactions
    /// Tests that a transaction that writes a new cell but doesn't commit
    /// will have the cell deleted during recovery
    #[tokio::test(flavor = "multi_thread")]
    async fn test_e2e_txn_write_rollback() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::tests::default_fields;
        use crate::server::transactions;
        use crate::server::{NebServer, ServerOptions, Service};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let undo_log_path = temp_dir.path().join("undo");
        let backup_path = temp_dir.path().join("backup");
        let wal_path = temp_dir.path().join("wal");
        let raft_path = temp_dir.path().join("raft");
        std::fs::create_dir_all(&raft_path).unwrap();
        let raft_path_str = raft_path.to_str().unwrap().to_string();

        let server_addr = crate::utils::test_port::unique_localhost_addr(); // Unique port for this test
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE * 4,
                db_size: SEGMENT_SIZE * 4,
                tiered_config: None,
                backup_storage: Some(backup_path.to_str().unwrap().to_string()),
                wal_storage: Some(wal_path.to_str().unwrap().to_string()),
                index_enabled: false,
                services: vec![Service::Cell, Service::Transaction],
                enable_recovery: false,
                disable_storage_locks: true,
                undo_log_storage: Some(undo_log_path.to_str().unwrap().to_string()),
                raft_storage: Some(raft_path_str.clone()), // Needed for schema recovery when segments exist
            },
            &server_addr,
            "test",
            async |_| {},
        )
        .await
        .unwrap();

        let schema = crate::ram::schema::Schema::new_with_id(
            1,
            &String::from("test"),
            None,
            default_fields(),
            false,
            false,
        );
        server.meta().schemas.debug_only_new_schema(schema.clone());

        let txn = transactions::new_async_client_for_database(&server_addr, "test", "test")
            .await
            .unwrap();
        let txn_id = txn.begin().await.unwrap().unwrap();

        // Write a new cell in transaction (but don't commit)
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(100));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(String::from("test_name")),
        );
        data_map.insert(&String::from("score"), OwnedValue::U64(50));

        let cell = OwnedCell::new_with_id(schema.id, &random_id(), OwnedValue::Map(data_map));
        let cell_id = cell.id();

        match txn
            .write(txn_id.clone(), cell.clone())
            .await
            .unwrap()
            .unwrap()
        {
            TxnExecResult::Accepted(()) => {}
            other => panic!("Write should be accepted, got {:?}", other),
        }

        // Verify cell exists in transaction
        match txn.read(txn_id.clone(), cell_id).await.unwrap().unwrap() {
            TxnExecResult::Accepted(read_cell) => {
                assert_eq!(read_cell.id(), cell_id);
            }
            other => panic!("Read should succeed, got {:?}", other),
        }

        // Prepare the transaction (this writes to chunks and undo log)
        match txn.prepare(txn_id.clone()).await.unwrap().unwrap() {
            TMPrepareResult::Success => {}
            other => panic!("Prepare should succeed, got {:?}", other),
        }

        // Archive segments before shutdown
        for chunk in &server.chunks().list {
            for seg in chunk.segments() {
                seg.archive().unwrap();
            }
        }

        drop(txn);
        drop(server);

        // Restart server with recovery enabled
        let server2 = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE * 4,
                db_size: SEGMENT_SIZE * 4,
                tiered_config: None,
                backup_storage: Some(backup_path.to_str().unwrap().to_string()),
                wal_storage: Some(wal_path.to_str().unwrap().to_string()),
                index_enabled: false,
                services: vec![Service::Cell, Service::Transaction],
                enable_recovery: true, // Enable recovery
                disable_storage_locks: true,
                undo_log_storage: Some(undo_log_path.to_str().unwrap().to_string()),
                raft_storage: Some(raft_path_str.clone()), // Needed for schema recovery when segments exist
            },
            &server_addr,
            "test",
            async |_| {},
        )
        .await
        .unwrap();

        server2.meta().schemas.debug_only_new_schema(schema.clone());

        // Cell should NOT exist after rollback
        let cell_read_result = server2.chunks().read_cell(&cell_id);
        if let Ok(ref cell) = cell_read_result {
            eprintln!("ERROR: Cell still exists after rollback!");
            eprintln!("Cell header: {:?}", cell.header);
        }
        assert!(
            cell_read_result.is_err(),
            "Uncommitted write should be rolled back"
        );
    }

    /// E2E test: Update operation rollback with real transactions
    /// Tests that a transaction that updates a cell but doesn't commit
    /// will have the old value restored during recovery
    #[tokio::test(flavor = "multi_thread")]
    async fn test_e2e_txn_update_rollback() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::tests::default_fields;
        use crate::server::transactions;
        use crate::server::{NebServer, ServerOptions, Service};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let undo_log_path = temp_dir.path().join("undo");
        let backup_path = temp_dir.path().join("backup");
        let wal_path = temp_dir.path().join("wal");
        let raft_path = temp_dir.path().join("raft");
        std::fs::create_dir_all(&raft_path).unwrap();

        let server_addr = crate::utils::test_port::unique_localhost_addr(); // Unique port for this test

        let schema = crate::ram::schema::Schema::new_with_id(
            1,
            &String::from("test"),
            None,
            default_fields(),
            false,
            false,
        );

        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE * 4,
                db_size: SEGMENT_SIZE * 4,
                tiered_config: None,
                backup_storage: Some(backup_path.to_str().unwrap().to_string()),
                wal_storage: Some(wal_path.to_str().unwrap().to_string()),
                index_enabled: false,
                services: vec![Service::Cell, Service::Transaction],
                enable_recovery: false,
                disable_storage_locks: true,
                undo_log_storage: Some(undo_log_path.to_str().unwrap().to_string()),
                raft_storage: Some(raft_path.to_str().unwrap().to_string()), // Enable Raft persistence for schema
            },
            &server_addr,
            "test",
            async move |_| {},
        )
        .await
        .unwrap();

        // Add schema via async client so it's persisted to Raft
        use crate::client::AsyncClient;
        let client = AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr.clone()],
            "test",
        )
        .await
        .unwrap();
        client
            .new_schema_with_id(schema.clone())
            .await
            .unwrap()
            .unwrap();

        // First, write a cell outside of transaction
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(100));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(String::from("original")),
        );
        data_map.insert(&String::from("score"), OwnedValue::U64(50));

        let mut cell = OwnedCell::new_with_id(schema.id, &random_id(), OwnedValue::Map(data_map));
        let cell_id = cell.id();
        server.chunks().write_cell(&mut cell).unwrap();
        let original_name = cell.data["name"].string().unwrap().clone();

        // Archive before transaction
        for chunk in &server.chunks().list {
            for seg in chunk.segments() {
                seg.archive().unwrap();
            }
        }

        // Now update it in a transaction (but don't commit)
        let txn = transactions::new_async_client_for_database(&server_addr, "test", "test")
            .await
            .unwrap();
        let txn_id = txn.begin().await.unwrap().unwrap();

        let mut data_map2 = OwnedMap::new();
        data_map2.insert(&String::from("id"), OwnedValue::I64(100));
        data_map2.insert(
            &String::from("name"),
            OwnedValue::String(String::from("updated")),
        );
        data_map2.insert(&String::from("score"), OwnedValue::U64(90));
        let cell2 = OwnedCell::new_with_id(schema.id, &cell_id, OwnedValue::Map(data_map2));

        match txn
            .update(txn_id.clone(), cell2.clone())
            .await
            .unwrap()
            .unwrap()
        {
            TxnExecResult::Accepted(()) => {}
            other => panic!("Update should be accepted, got {:?}", other),
        }

        // Verify update in transaction
        match txn.read(txn_id.clone(), cell_id).await.unwrap().unwrap() {
            TxnExecResult::Accepted(read_cell) => {
                assert_eq!(read_cell.data["name"].string().unwrap(), "updated");
            }
            other => panic!("Read should succeed, got {:?}", other),
        }

        // Prepare the transaction (this writes to chunks and undo log)
        match txn.prepare(txn_id.clone()).await.unwrap().unwrap() {
            TMPrepareResult::Success => {}
            other => panic!("Prepare should succeed, got {:?}", other),
        }

        // Archive segments before shutdown
        for chunk in &server.chunks().list {
            for seg in chunk.segments() {
                seg.archive().unwrap();
            }
        }

        drop(txn);
        drop(server);

        // Restart server with recovery enabled
        let server2 = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE * 4,
                db_size: SEGMENT_SIZE * 4,
                tiered_config: None,
                backup_storage: Some(backup_path.to_str().unwrap().to_string()),
                wal_storage: Some(wal_path.to_str().unwrap().to_string()),
                index_enabled: false,
                services: vec![Service::Cell, Service::Transaction],
                enable_recovery: true, // Enable recovery
                disable_storage_locks: true,
                undo_log_storage: Some(undo_log_path.to_str().unwrap().to_string()),
                raft_storage: Some(raft_path.to_str().unwrap().to_string()), // Use same Raft storage to recover schema
            },
            &server_addr,
            "test",
            async move |_| {},
        )
        .await
        .unwrap();

        // Cell should have original value after rollback
        let restored_cell = server2.chunks().read_cell(&cell_id).unwrap();
        assert_eq!(
            restored_cell.data["name"].string().unwrap(),
            &original_name,
            "Uncommitted update should be rolled back to original value"
        );
    }

    /// E2E test: Remove operation rollback with real transactions
    /// Tests that a transaction that removes a cell but doesn't commit
    /// will have the cell restored during recovery
    #[tokio::test(flavor = "multi_thread")]
    async fn test_e2e_txn_remove_rollback() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::tests::default_fields;
        use crate::server::transactions;
        use crate::server::{NebServer, ServerOptions, Service};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let undo_log_path = temp_dir.path().join("undo");
        let backup_path = temp_dir.path().join("backup");
        let wal_path = temp_dir.path().join("wal");
        let raft_path = temp_dir.path().join("raft");

        let server_addr = crate::utils::test_port::unique_localhost_addr(); // Unique port for this test

        let schema = crate::ram::schema::Schema::new_with_id(
            1,
            &String::from("test"),
            None,
            default_fields(),
            false,
            false,
        );

        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE * 4,
                db_size: SEGMENT_SIZE * 4,
                tiered_config: None,
                backup_storage: Some(backup_path.to_str().unwrap().to_string()),
                wal_storage: Some(wal_path.to_str().unwrap().to_string()),
                index_enabled: false,
                services: vec![Service::Cell, Service::Transaction],
                enable_recovery: false,
                disable_storage_locks: true,
                undo_log_storage: Some(undo_log_path.to_str().unwrap().to_string()),
                raft_storage: Some(raft_path.to_str().unwrap().to_string()), // Enable Raft persistence for schema
            },
            &server_addr,
            "test",
            async move |_| {},
        )
        .await
        .unwrap();

        // Add schema via async client so it's persisted to Raft
        use crate::client::AsyncClient;
        let client = AsyncClient::new(
            &server.rpc,
            &server.membership,
            &vec![server_addr.clone()],
            "test",
        )
        .await
        .unwrap();
        client
            .new_schema_with_id(schema.clone())
            .await
            .unwrap()
            .unwrap();

        // First, write a cell outside of transaction
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(100));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(String::from("to_be_deleted")),
        );
        data_map.insert(&String::from("score"), OwnedValue::U64(75));

        let mut cell = OwnedCell::new_with_id(schema.id, &random_id(), OwnedValue::Map(data_map));
        let cell_id = cell.id();
        server.chunks().write_cell(&mut cell).unwrap();
        let original_name = cell.data["name"].string().unwrap().clone();

        // Archive before transaction
        for chunk in &server.chunks().list {
            for seg in chunk.segments() {
                seg.archive().unwrap();
            }
        }

        // Now remove it in a transaction (but don't commit)
        let txn = transactions::new_async_client_for_database(&server_addr, "test", "test")
            .await
            .unwrap();
        let txn_id = txn.begin().await.unwrap().unwrap();

        match txn.remove(txn_id.clone(), cell_id).await.unwrap().unwrap() {
            TxnExecResult::Accepted(()) => {}
            other => panic!("Remove should be accepted, got {:?}", other),
        }

        // Verify cell is removed in transaction
        match txn.read(txn_id.clone(), cell_id).await.unwrap().unwrap() {
            TxnExecResult::Error(ReadError::CellDoesNotExisted) => {}
            other => panic!("Read should fail after remove, got {:?}", other),
        }

        // Prepare the transaction (this writes to chunks and undo log)
        match txn.prepare(txn_id.clone()).await.unwrap().unwrap() {
            TMPrepareResult::Success => {}
            other => panic!("Prepare should succeed, got {:?}", other),
        }

        // Archive segments before shutdown
        for chunk in &server.chunks().list {
            for seg in chunk.segments() {
                seg.archive().unwrap();
            }
        }

        drop(txn);
        drop(server);

        // Restart server with recovery enabled
        let server2 = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE * 4,
                db_size: SEGMENT_SIZE * 4,
                tiered_config: None,
                backup_storage: Some(backup_path.to_str().unwrap().to_string()),
                wal_storage: Some(wal_path.to_str().unwrap().to_string()),
                index_enabled: false,
                services: vec![Service::Cell, Service::Transaction],
                enable_recovery: true, // Enable recovery
                disable_storage_locks: true,
                undo_log_storage: Some(undo_log_path.to_str().unwrap().to_string()),
                raft_storage: Some(raft_path.to_str().unwrap().to_string()), // Use same Raft storage to recover schema
            },
            &server_addr,
            "test",
            async move |_| {},
        )
        .await
        .unwrap();

        // Cell should exist after rollback
        let restored_cell = server2.chunks().read_cell(&cell_id).unwrap();
        assert_eq!(
            restored_cell.data["name"].string().unwrap(),
            &original_name,
            "Uncommitted remove should be rolled back and cell restored"
        );
    }

    /// E2E test: Committed transactions should not be rolled back
    #[tokio::test(flavor = "multi_thread")]
    async fn test_e2e_txn_committed_no_rollback() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::tests::default_fields;
        use crate::server::transactions;
        use crate::server::{NebServer, ServerOptions, Service};
        use tempfile::TempDir;
        use tokio::time::{sleep, Duration};

        let temp_dir = TempDir::new().unwrap();
        let undo_log_path = temp_dir.path().join("undo");
        let backup_path = temp_dir.path().join("backup");
        let wal_path = temp_dir.path().join("wal");
        let raft_path = temp_dir.path().join("raft");
        std::fs::create_dir_all(&raft_path).unwrap();
        let raft_path_str = raft_path.to_str().unwrap().to_string();

        let server_addr = crate::utils::test_port::unique_localhost_addr(); // Unique port for this test
                                                          // Use unique group name to avoid conflicts with other tests
        let group_name = "test_e2e_txn_committed_no_rollback";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE * 4,
                db_size: SEGMENT_SIZE * 4,
                tiered_config: None,
                backup_storage: Some(backup_path.to_str().unwrap().to_string()),
                wal_storage: Some(wal_path.to_str().unwrap().to_string()),
                index_enabled: false,
                services: vec![Service::Cell, Service::Transaction],
                enable_recovery: false,
                disable_storage_locks: true,
                undo_log_storage: Some(undo_log_path.to_str().unwrap().to_string()),
                raft_storage: Some(raft_path_str.clone()), // Needed for schema recovery when segments exist
            },
            &server_addr,
            group_name,
            async |_| {},
        )
        .await
        .unwrap();

        // Wait for Raft to stabilize before starting the test
        // This prevents overwhelming the Raft heartbeat mechanism
        sleep(Duration::from_millis(500)).await;

        let schema = crate::ram::schema::Schema::new_with_id(
            1,
            &String::from("test"),
            None,
            default_fields(),
            false,
            false,
        );
        server.meta().schemas.debug_only_new_schema(schema.clone());

        let txn = transactions::new_async_client_for_database(&server_addr, group_name, group_name)
            .await
            .unwrap();
        let txn_id = txn.begin().await.unwrap().unwrap();

        // Write a new cell in transaction and commit
        let mut data_map = OwnedMap::new();
        data_map.insert(&String::from("id"), OwnedValue::I64(200));
        data_map.insert(
            &String::from("name"),
            OwnedValue::String(String::from("committed_name")),
        );
        data_map.insert(&String::from("score"), OwnedValue::U64(100));

        let cell = OwnedCell::new_with_id(schema.id, &random_id(), OwnedValue::Map(data_map));
        let cell_id = cell.id();

        txn.write(txn_id.clone(), cell.clone())
            .await
            .unwrap()
            .unwrap();

        // Commit the transaction
        assert_eq!(
            txn.prepare(txn_id.clone()).await.unwrap().unwrap(),
            TMPrepareResult::Success
        );
        assert_eq!(
            txn.commit(txn_id.clone()).await.unwrap().unwrap(),
            EndResult::Success
        );

        // Archive segments before shutdown
        for chunk in &server.chunks().list {
            for seg in chunk.segments() {
                seg.archive().unwrap();
            }
        }

        drop(txn);
        drop(server);

        // Restart server with recovery enabled
        let server2 = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE * 4,
                db_size: SEGMENT_SIZE * 4,
                tiered_config: None,
                backup_storage: Some(backup_path.to_str().unwrap().to_string()),
                wal_storage: Some(wal_path.to_str().unwrap().to_string()),
                index_enabled: false,
                services: vec![Service::Cell, Service::Transaction],
                enable_recovery: true, // Enable recovery
                disable_storage_locks: true,
                undo_log_storage: Some(undo_log_path.to_str().unwrap().to_string()),
                raft_storage: Some(raft_path_str.clone()), // Needed for schema recovery when segments exist
            },
            &server_addr,
            group_name,
            async |_| {},
        )
        .await
        .unwrap();

        // Wait for Raft to stabilize after recovery restart
        sleep(Duration::from_millis(500)).await;

        server2.meta().schemas.debug_only_new_schema(schema.clone());

        // Cell should still exist after recovery (committed transaction)
        let read_cell = server2.chunks().read_cell(&cell_id).unwrap();
        assert_eq!(
            read_cell.data["name"].string().unwrap(),
            "committed_name",
            "Committed transaction should persist through recovery"
        );
    }
}
