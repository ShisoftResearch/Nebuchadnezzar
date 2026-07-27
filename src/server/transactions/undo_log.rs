use crate::ram::cell::OwnedCell;
use crate::ram::chunk::Chunks;
use crate::ram::durable_fs;
use crate::ram::types::Id;
use bifrost::hlc::Hlc;
use log::{debug, error, info};
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::{TxnId, TxnResolution, TxnState};

/// Undo log entry stored in the log file
/// Format: [entry_type: u8 = 7][txn_id_len: u32][txn_id: bytes][cell_id: Id]
/// [op_type: u8][installed_revision_ts: u64][prior_cell_len: u32]
/// [prior_owned_cell: bytes]
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
/// All operations store the installed revision for exact recovery ownership.
/// Update and remove own the complete immutable prior cell, so recovery is
/// independent of cleaner relocation and source-segment lifetime.
#[derive(Debug, Clone)]
pub struct UndoLogEntry {
    pub txn_id: TxnId,
    pub cell_id: Id,
    pub op_type: UndoOpType,
    /// Revision installed by the incomplete transaction.
    pub installed_revision_ts: u64,
    /// Complete prior immutable cell for Update/Remove. Inserts have no prior.
    pub prior_cell: Option<OwnedCell>,
}

/// Type of operation that needs to be undone
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndoOpType {
    Write = 1,  // Compensate the installed revision with a newer tombstone.
    Update = 2, // Compensate the installed revision with the prior immutable contents.
    Remove = 3, // Compensate the installed tombstone with the prior immutable contents.
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

// Types 1 and 4 were raw-location undo layouts. Reusing either would make
// framing ambiguous and could silently reinterpret an adjacent record.
const ENTRY_TYPE_UNDO: u8 = 7;
const ENTRY_TYPE_COMMIT: u8 = 2;
const ENTRY_TYPE_ABORT: u8 = 3;
const ENTRY_TYPE_COORDINATOR_COMMIT: u8 = 5;
const ENTRY_TYPE_COORDINATOR_ABORT: u8 = 6;
const UNDO_FIXED_PAYLOAD_LEN: usize = 16 + 1 + 8 + 4;
const MAX_UNDO_PRIOR_CELL_LEN: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct CoordinatorDecisionRecord {
    pub resolution: TxnResolution,
    pub participants: BTreeSet<u64>,
}

fn collect_undo_log_paths<I>(log_dir_path: &Path, entries: I) -> io::Result<Vec<(u64, PathBuf)>>
where
    I: IntoIterator<Item = io::Result<PathBuf>>,
{
    let mut log_files = Vec::new();
    for path in entries {
        let path = path.map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "cannot enumerate entry in undo log directory {}: {}",
                    log_dir_path.display(),
                    error
                ),
            )
        })?;
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
    log_files.sort_by_key(|(seq, _)| *seq);
    Ok(log_files)
}

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

fn coordinator_commit_record_size(
    bytes: &[u8],
    offset: usize,
    txn_id_len: usize,
) -> io::Result<(Hlc, BTreeSet<u64>, usize)> {
    let commit_len_offset = offset
        .checked_add(5)
        .and_then(|value| value.checked_add(txn_id_len))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "record size overflow"))?;
    if bytes.len() < commit_len_offset + 4 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "incomplete coordinator commit timestamp length",
        ));
    }
    let commit_hlc_len = u32::from_le_bytes([
        bytes[commit_len_offset],
        bytes[commit_len_offset + 1],
        bytes[commit_len_offset + 2],
        bytes[commit_len_offset + 3],
    ]) as usize;
    let commit_hlc_offset = commit_len_offset + 4;
    let commit_hlc_end = commit_hlc_offset
        .checked_add(commit_hlc_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "record size overflow"))?;
    if bytes.len() < commit_hlc_end {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "incomplete coordinator commit timestamp",
        ));
    }
    let commit_hlc = serde_json::from_slice(&bytes[commit_hlc_offset..commit_hlc_end])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let (participants, record_end) =
        decode_coordinator_participants(bytes, commit_hlc_end, "commit")?;
    Ok((commit_hlc, participants, record_end - offset))
}

fn coordinator_abort_record_size(
    bytes: &[u8],
    offset: usize,
    txn_id_len: usize,
) -> io::Result<(BTreeSet<u64>, usize)> {
    let participants_len_offset = offset
        .checked_add(5)
        .and_then(|value| value.checked_add(txn_id_len))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "record size overflow"))?;
    let (participants, record_end) =
        decode_coordinator_participants(bytes, participants_len_offset, "abort")?;
    Ok((participants, record_end - offset))
}

fn decode_coordinator_participants(
    bytes: &[u8],
    participants_len_offset: usize,
    decision: &str,
) -> io::Result<(BTreeSet<u64>, usize)> {
    if bytes.len() < participants_len_offset + 4 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("incomplete coordinator {decision} participant length"),
        ));
    }
    let participants_len = u32::from_le_bytes([
        bytes[participants_len_offset],
        bytes[participants_len_offset + 1],
        bytes[participants_len_offset + 2],
        bytes[participants_len_offset + 3],
    ]) as usize;
    let participants_offset = participants_len_offset + 4;
    let record_end = participants_offset
        .checked_add(participants_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "record size overflow"))?;
    if bytes.len() < record_end {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("incomplete coordinator {decision} participants"),
        ));
    }
    let participants = serde_json::from_slice(&bytes[participants_offset..record_end])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok((participants, record_end))
}

impl UndoLogEntry {
    /// Create a new undo log entry
    pub fn new(
        txn_id: TxnId,
        cell_id: Id,
        op_type: UndoOpType,
        installed_revision_ts: u64,
        prior_cell: Option<OwnedCell>,
    ) -> Self {
        Self {
            txn_id,
            cell_id,
            op_type,
            installed_revision_ts,
            prior_cell,
        }
    }

    /// Helper to create a Write entry (for new cells)
    /// Only needs revision_ts since there's no old segment to restore from
    pub fn new_write(txn_id: TxnId, cell_id: Id, installed_revision_ts: u64) -> Self {
        Self::new(
            txn_id,
            cell_id,
            UndoOpType::Write,
            installed_revision_ts,
            None,
        )
    }

    /// Helper to create an Update/Remove entry that owns the immutable prior
    /// cell independently of its physical source segment.
    pub fn new_restore(
        txn_id: TxnId,
        cell_id: Id,
        op_type: UndoOpType,
        installed_revision_ts: u64,
        prior_cell: OwnedCell,
    ) -> Self {
        debug_assert!(
            op_type != UndoOpType::Write,
            "Use new_write for Write operations"
        );
        Self::new(
            txn_id,
            cell_id,
            op_type,
            installed_revision_ts,
            Some(prior_cell),
        )
    }

    fn validate(&self) -> io::Result<()> {
        match (self.op_type, self.prior_cell.as_ref()) {
            (UndoOpType::Write, None) => Ok(()),
            (UndoOpType::Write, Some(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "insert undo entry unexpectedly contains a prior cell",
            )),
            (UndoOpType::Update | UndoOpType::Remove, None) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{:?} undo entry has no prior cell", self.op_type),
            )),
            (UndoOpType::Update | UndoOpType::Remove, Some(prior_cell)) => {
                if prior_cell.header.id() != self.cell_id {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "undo prior cell identity mismatch: expected {:?}, found {:?}",
                            self.cell_id,
                            prior_cell.header.id()
                        ),
                    ));
                }
                if prior_cell.header.revision_ts == 0
                    || prior_cell.header.revision_ts >= self.installed_revision_ts
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "invalid undo revision order: prior {}, installed {}",
                            prior_cell.header.revision_ts, self.installed_revision_ts
                        ),
                    ));
                }
                Ok(())
            }
        }
    }

    /// Serialize entry to bytes
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        self.validate()?;
        let txn_id_bytes = serde_json::to_vec(&self.txn_id)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let txn_id_len = u32::try_from(txn_id_bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "transaction id too large"))?;
        let prior_cell_bytes = self
            .prior_cell
            .as_ref()
            .map(|prior_cell| {
                bincode::serde::encode_to_vec(prior_cell, bincode::config::standard())
            })
            .transpose()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            .unwrap_or_default();
        if prior_cell_bytes.len() > MAX_UNDO_PRIOR_CELL_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "undo prior cell exceeds maximum encoded size",
            ));
        }
        let prior_cell_len = u32::try_from(prior_cell_bytes.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "undo prior cell is too large")
        })?;

        let mut bytes = Vec::with_capacity(
            1 + 4 + txn_id_bytes.len() + UNDO_FIXED_PAYLOAD_LEN + prior_cell_bytes.len(),
        );
        bytes.push(ENTRY_TYPE_UNDO);
        bytes.extend_from_slice(&txn_id_len.to_le_bytes());
        bytes.extend_from_slice(&txn_id_bytes);
        bytes.extend_from_slice(&self.cell_id.higher.to_le_bytes());
        bytes.extend_from_slice(&self.cell_id.lower.to_le_bytes());
        bytes.push(self.op_type as u8);
        bytes.extend_from_slice(&self.installed_revision_ts.to_le_bytes());
        bytes.extend_from_slice(&prior_cell_len.to_le_bytes());
        bytes.extend_from_slice(&prior_cell_bytes);

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
        if bytes.len() < 5 + txn_id_len + UNDO_FIXED_PAYLOAD_LEN {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for full entry",
            ));
        }

        let txn_id: TxnId = decode_txn_id(&bytes[5..5 + txn_id_len])?;

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

        let installed_revision_ts = u64::from_le_bytes([
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

        let prior_cell_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;
        if prior_cell_len > MAX_UNDO_PRIOR_CELL_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "undo prior cell exceeds maximum encoded size",
            ));
        }
        let total_size = offset.checked_add(prior_cell_len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "undo record size overflow")
        })?;
        if bytes.len() < total_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete undo prior cell payload",
            ));
        }
        let prior_cell = if prior_cell_len == 0 {
            None
        } else {
            let (prior_cell, decoded_len): (OwnedCell, usize) = bincode::serde::decode_from_slice(
                &bytes[offset..total_size],
                bincode::config::standard(),
            )
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if decoded_len != prior_cell_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "undo prior cell payload has trailing bytes",
                ));
            }
            Some(prior_cell)
        };

        let entry = Self {
            txn_id,
            cell_id: Id {
                higher: cell_id_higher,
                lower: cell_id_lower,
            },
            op_type,
            installed_revision_ts,
            prior_cell,
        };
        entry.validate()?;
        Ok((entry, total_size))
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
    active_txns: Mutex<HashSet<TxnId>>,
    /// Maximum log file size before rotation (default 64MB)
    max_log_size: u64,
    #[cfg(test)]
    fail_next_undo_write: AtomicBool,
    #[cfg(test)]
    fail_next_commit_marker: AtomicBool,
    #[cfg(test)]
    fail_next_abort_marker: AtomicBool,
    #[cfg(test)]
    fail_next_coordinator_commit_decision: AtomicBool,
    #[cfg(test)]
    fail_next_coordinator_abort_decision: AtomicBool,
    #[cfg(test)]
    rotate_before_next_record: AtomicBool,
}

impl UndoLogger {
    /// Create a new undo log manager
    pub fn new(log_dir: String) -> io::Result<Arc<Self>> {
        let log_dir_path = Path::new(&log_dir);
        durable_fs::ensure_directory(log_dir_path)?;
        let existing_logs = collect_undo_log_paths(
            log_dir_path,
            std::fs::read_dir(log_dir_path)?.map(|entry| entry.map(|entry| entry.path())),
        )?;
        let next_log_seq = existing_logs
            .last()
            .map(|(seq, _)| {
                seq.checked_add(1).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "undo log sequence number is exhausted",
                    )
                })
            })
            .transpose()?
            .unwrap_or(0);

        let log = Arc::new(Self {
            log_dir: log_dir.clone(),
            log_file: Mutex::new(None),
            log_file_name: Mutex::new(None),
            log_seq: AtomicU64::new(next_log_seq),
            active_txns: Mutex::new(HashSet::new()),
            max_log_size: 64 * 1024 * 1024, // 64MB
            #[cfg(test)]
            fail_next_undo_write: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_commit_marker: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_abort_marker: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_coordinator_commit_decision: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_coordinator_abort_decision: AtomicBool::new(false),
            #[cfg(test)]
            rotate_before_next_record: AtomicBool::new(false),
        });

        // Open or create initial log file
        log.rotate_log()?;

        Ok(log)
    }

    /// Rotate to a new log file
    fn rotate_log(&self) -> io::Result<()> {
        // Serialize rotation through the active writer so a failed publication
        // cannot age the still-active file into the trimmer's old-log window.
        let mut log_file_guard = self.log_file.lock();
        let seq = self.log_seq.load(Ordering::SeqCst);
        let next_seq = seq.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "undo log sequence number is exhausted",
            )
        })?;
        let log_file_path = format!("{}/undo-{}.nlog", self.log_dir, seq);

        debug!("Rotating undo log to: {}", log_file_path);

        let file = durable_fs::open_or_create_append(Path::new(&log_file_path))?;
        let writer = BufWriter::with_capacity(4096, file);

        if let Some(old_writer) = log_file_guard.as_mut() {
            old_writer.flush()?;
            old_writer.get_ref().sync_all()?;
        }

        *log_file_guard = Some(writer);
        *self.log_file_name.lock() = Some(log_file_path);
        self.log_seq.store(next_seq, Ordering::SeqCst);

        Ok(())
    }

    /// Write an undo entry to the log
    pub fn write_undo_entry(&self, entry: UndoLogEntry) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_next_undo_write.swap(false, Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "injected undo write failure",
            ));
        }
        let bytes = entry.to_bytes()?;

        let mut log_file_guard = self.log_file.lock();
        if let Some(writer) = log_file_guard.as_mut() {
            writer.write_all(&bytes)?;
            writer.flush()?;
            writer.get_ref().sync_data()?;

            // Track active transaction
            drop(log_file_guard);
            self.active_txns.lock().insert(entry.txn_id.clone());

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

    #[cfg(test)]
    pub(crate) fn fail_next_undo_write_for_test(&self) {
        self.fail_next_undo_write.store(true, Ordering::SeqCst);
    }

    /// Write a commit marker for a transaction
    pub fn write_commit_marker(&self, txn_id: &TxnId) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_next_commit_marker.swap(false, Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "injected commit marker failure",
            ));
        }
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
            self.active_txns.lock().remove(txn_id);

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
        #[cfg(test)]
        if self.fail_next_abort_marker.swap(false, Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "injected abort marker failure",
            ));
        }
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
            self.active_txns.lock().remove(txn_id);

            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Log file not initialized",
            ))
        }
    }

    /// Persist the distributed coordinator's irrevocable commit decision.
    ///
    /// This record is intentionally distinct from a participant commit marker:
    /// it must remain visible to resolution RPCs without suppressing local
    /// participant undo until recovery has proved the installed output durable.
    pub fn write_coordinator_commit_decision(
        &self,
        txn_id: &TxnId,
        commit_hlc: Hlc,
        participants: &[u64],
    ) -> io::Result<()> {
        #[cfg(test)]
        self.rotate_before_record_if_requested_for_test()?;
        #[cfg(test)]
        if self
            .fail_next_coordinator_commit_decision
            .swap(false, Ordering::SeqCst)
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "injected coordinator commit decision failure",
            ));
        }
        let txn_id_bytes = serde_json::to_vec(txn_id)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
        let commit_hlc_bytes = serde_json::to_vec(&commit_hlc)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
        let participants: BTreeSet<_> = participants.iter().copied().collect();
        let participants_bytes = serde_json::to_vec(&participants)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
        let txn_id_len = u32::try_from(txn_id_bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "transaction id too large"))?;
        let commit_hlc_len = u32::try_from(commit_hlc_bytes.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "commit timestamp too large")
        })?;
        let participants_len = u32::try_from(participants_bytes.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "participant list too large")
        })?;

        let mut bytes = Vec::with_capacity(
            1 + 4 + txn_id_bytes.len() + 4 + commit_hlc_bytes.len() + 4 + participants_bytes.len(),
        );
        bytes.push(ENTRY_TYPE_COORDINATOR_COMMIT);
        bytes.extend_from_slice(&txn_id_len.to_le_bytes());
        bytes.extend_from_slice(&txn_id_bytes);
        bytes.extend_from_slice(&commit_hlc_len.to_le_bytes());
        bytes.extend_from_slice(&commit_hlc_bytes);
        bytes.extend_from_slice(&participants_len.to_le_bytes());
        bytes.extend_from_slice(&participants_bytes);

        let mut log_file_guard = self.log_file.lock();
        let writer = log_file_guard
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Log file not initialized"))?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        writer.get_ref().sync_data()
    }

    /// Persist the distributed coordinator's irrevocable abort decision.
    ///
    /// Like the coordinator commit record, this does not suppress participant
    /// undo. Recovery must first compensate the participant output and then
    /// write the ordinary participant abort marker.
    pub fn write_coordinator_abort_decision(
        &self,
        txn_id: &TxnId,
        participants: &[u64],
    ) -> io::Result<()> {
        #[cfg(test)]
        self.rotate_before_record_if_requested_for_test()?;
        #[cfg(test)]
        if self
            .fail_next_coordinator_abort_decision
            .swap(false, Ordering::SeqCst)
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "injected coordinator abort decision failure",
            ));
        }
        let txn_id_bytes = serde_json::to_vec(txn_id)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
        let participants: BTreeSet<_> = participants.iter().copied().collect();
        let participants_bytes = serde_json::to_vec(&participants)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
        let txn_id_len = u32::try_from(txn_id_bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "transaction id too large"))?;
        let participants_len = u32::try_from(participants_bytes.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "participant list too large")
        })?;
        let mut bytes =
            Vec::with_capacity(1 + 4 + txn_id_bytes.len() + 4 + participants_bytes.len());
        bytes.push(ENTRY_TYPE_COORDINATOR_ABORT);
        bytes.extend_from_slice(&txn_id_len.to_le_bytes());
        bytes.extend_from_slice(&txn_id_bytes);
        bytes.extend_from_slice(&participants_len.to_le_bytes());
        bytes.extend_from_slice(&participants_bytes);

        let mut log_file_guard = self.log_file.lock();
        let writer = log_file_guard
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Log file not initialized"))?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        writer.get_ref().sync_data()
    }

    pub fn coordinator_decision(&self, txn_id: &TxnId) -> io::Result<Option<TxnResolution>> {
        self.coordinator_decision_record(txn_id)
            .map(|record| record.map(|record| record.resolution))
    }

    pub(crate) fn coordinator_decision_record(
        &self,
        txn_id: &TxnId,
    ) -> io::Result<Option<CoordinatorDecisionRecord>> {
        let log_dir_path = Path::new(&self.log_dir);
        let entries = std::fs::read_dir(log_dir_path)?;
        let log_files = collect_undo_log_paths(
            log_dir_path,
            entries.map(|entry| entry.map(|entry| entry.path())),
        )?;
        let mut decision = None;

        for (_seq, path) in log_files {
            let mut buffer = Vec::new();
            File::open(&path)?.read_to_end(&mut buffer)?;
            let mut offset = 0;
            while offset < buffer.len() {
                if buffer.len() < offset + 5 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "cannot decode undo log {} at byte offset {}: incomplete entry header",
                            path.display(),
                            offset
                        ),
                    ));
                }
                let entry_type = buffer[offset];
                let encoded_txn_id_len = u32::from_le_bytes([
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                    buffer[offset + 4],
                ]) as usize;
                if buffer.len() < offset + 5 + encoded_txn_id_len {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "cannot decode undo log {} at byte offset {}: incomplete transaction id",
                            path.display(),
                            offset
                        ),
                    ));
                }
                let encoded_txn_id =
                    decode_txn_id(&buffer[offset + 5..offset + 5 + encoded_txn_id_len])?;
                match entry_type {
                    ENTRY_TYPE_UNDO => {
                        let (_, size) = UndoLogEntry::from_bytes(&buffer[offset..])?;
                        offset += size;
                    }
                    ENTRY_TYPE_COMMIT | ENTRY_TYPE_ABORT => {
                        offset += 5 + encoded_txn_id_len;
                    }
                    ENTRY_TYPE_COORDINATOR_COMMIT => {
                        let (commit_hlc, participants, size) =
                            coordinator_commit_record_size(&buffer, offset, encoded_txn_id_len)?;
                        if encoded_txn_id == *txn_id {
                            decision = Some(CoordinatorDecisionRecord {
                                resolution: TxnResolution::Commit(commit_hlc),
                                participants,
                            });
                        }
                        offset += size;
                    }
                    ENTRY_TYPE_COORDINATOR_ABORT => {
                        let (participants, size) =
                            coordinator_abort_record_size(&buffer, offset, encoded_txn_id_len)?;
                        if encoded_txn_id == *txn_id {
                            decision = Some(CoordinatorDecisionRecord {
                                resolution: TxnResolution::Abort,
                                participants,
                            });
                        }
                        offset += size;
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "cannot decode undo log {} at byte offset {}: invalid entry type {}",
                                path.display(),
                                offset,
                                entry_type
                            ),
                        ));
                    }
                }
            }
        }

        Ok(decision)
    }

    /// Read the durable outcome written by a participant's `end` operation.
    ///
    /// Coordinator decision records are deliberately ignored here: they prove
    /// only the global choice, not that this participant finished promotion and
    /// lock release.
    pub fn participant_completion(&self, txn_id: &TxnId) -> io::Result<Option<TxnState>> {
        let log_dir_path = Path::new(&self.log_dir);
        let entries = std::fs::read_dir(log_dir_path)?;
        let log_files = collect_undo_log_paths(
            log_dir_path,
            entries.map(|entry| entry.map(|entry| entry.path())),
        )?;
        let mut completion = None;

        for (_seq, path) in log_files {
            let mut buffer = Vec::new();
            File::open(&path)?.read_to_end(&mut buffer)?;
            let mut offset = 0;
            while offset < buffer.len() {
                if buffer.len() < offset + 5 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "cannot decode undo log {} at byte offset {}: incomplete entry header",
                            path.display(),
                            offset
                        ),
                    ));
                }
                let entry_type = buffer[offset];
                let encoded_txn_id_len = u32::from_le_bytes([
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                    buffer[offset + 4],
                ]) as usize;
                if buffer.len() < offset + 5 + encoded_txn_id_len {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "cannot decode undo log {} at byte offset {}: incomplete transaction id",
                            path.display(),
                            offset
                        ),
                    ));
                }
                let encoded_txn_id =
                    decode_txn_id(&buffer[offset + 5..offset + 5 + encoded_txn_id_len])?;
                match entry_type {
                    ENTRY_TYPE_UNDO => {
                        let (_, size) = UndoLogEntry::from_bytes(&buffer[offset..])?;
                        offset += size;
                    }
                    ENTRY_TYPE_COMMIT | ENTRY_TYPE_ABORT => {
                        if encoded_txn_id == *txn_id {
                            completion = Some(if entry_type == ENTRY_TYPE_COMMIT {
                                TxnState::Committed
                            } else {
                                TxnState::Aborted
                            });
                        }
                        offset += 5 + encoded_txn_id_len;
                    }
                    ENTRY_TYPE_COORDINATOR_COMMIT => {
                        let (_, _, size) =
                            coordinator_commit_record_size(&buffer, offset, encoded_txn_id_len)?;
                        offset += size;
                    }
                    ENTRY_TYPE_COORDINATOR_ABORT => {
                        let (_, size) =
                            coordinator_abort_record_size(&buffer, offset, encoded_txn_id_len)?;
                        offset += size;
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "cannot decode undo log {} at byte offset {}: invalid entry type {}",
                                path.display(),
                                offset,
                                entry_type
                            ),
                        ));
                    }
                }
            }
        }

        Ok(completion)
    }

    pub fn resolve_recovered_transactions(
        &self,
        mut txn_index: HashMap<TxnId, Vec<UndoLogEntry>>,
        resolutions: &HashMap<TxnId, TxnResolution>,
    ) -> io::Result<HashMap<TxnId, Vec<UndoLogEntry>>> {
        for txn_id in txn_index.keys() {
            match resolutions
                .get(txn_id)
                .copied()
                .unwrap_or(TxnResolution::Unknown)
            {
                TxnResolution::Commit(_) | TxnResolution::Abort => {}
                TxnResolution::InProgress | TxnResolution::Unknown => {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!("transaction {txn_id:?} has no final durable coordinator decision"),
                    ));
                }
            }
        }

        let committed: Vec<_> = txn_index
            .keys()
            .filter(|txn_id| matches!(resolutions.get(txn_id), Some(TxnResolution::Commit(_))))
            .copied()
            .collect();
        for txn_id in committed {
            self.write_commit_marker(&txn_id)?;
            txn_index.remove(&txn_id);
        }
        Ok(txn_index)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_commit_marker_for_test(&self) {
        self.fail_next_commit_marker.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_abort_marker_for_test(&self) {
        self.fail_next_abort_marker.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_coordinator_commit_decision_for_test(&self) {
        self.fail_next_coordinator_commit_decision
            .store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_coordinator_abort_decision_for_test(&self) {
        self.fail_next_coordinator_abort_decision
            .store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn rotate_before_next_record_for_test(&self) {
        self.rotate_before_next_record.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn log_directory_for_test(&self) -> PathBuf {
        PathBuf::from(&self.log_dir)
    }

    #[cfg(test)]
    fn rotate_before_record_if_requested_for_test(&self) -> io::Result<()> {
        if self.rotate_before_next_record.swap(false, Ordering::SeqCst) {
            self.rotate_log()?;
        }
        Ok(())
    }

    /// Perform rollback for all incomplete transactions
    /// Must be called after segment recovery is complete
    /// Takes the txn_index built during recovery as a parameter
    pub fn rollback_incomplete_transactions(
        &self,
        txn_index: HashMap<TxnId, Vec<UndoLogEntry>>,
        chunks: &Arc<Chunks>,
    ) -> io::Result<()> {
        if let Some(max_installed_revision_ts) = txn_index
            .values()
            .flat_map(|entries| entries.iter())
            .map(|entry| entry.installed_revision_ts)
            .max()
        {
            chunks
                .revision_clock()
                .try_observe(bifrost::hlc::Hlc {
                    ts: max_installed_revision_ts,
                    node: chunks.revision_clock().node(),
                })
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        "undo installed revision clock is exhausted",
                    )
                })?;
        }
        if txn_index.is_empty() {
            info!("No incomplete transactions to rollback");
            return Ok(());
        }

        info!("Rolling back {} incomplete transactions", txn_index.len());

        let mut rollback_stats = (0usize, 0usize, 0usize); // (writes, updates, removes)
        let mut rollback_failures = Vec::new();

        for (txn_id, entries) in txn_index.iter() {
            debug!(
                "Rolling back transaction: {:?} with {} entries",
                txn_id,
                entries.len()
            );

            let failures_before = rollback_failures.len();
            for entry in entries {
                match entry.op_type {
                    UndoOpType::Write => {
                        if let Err(e) = self.rollback_write(entry, chunks) {
                            error!(
                                "Failed to rollback write for cell {:?}: {:?}",
                                entry.cell_id, e
                            );
                            rollback_failures.push(format!(
                                "transaction {txn_id:?} {:?} compensation for cell {:?} failed: {e}",
                                entry.op_type, entry.cell_id
                            ));
                        } else {
                            rollback_stats.0 += 1;
                        }
                    }
                    UndoOpType::Update | UndoOpType::Remove => {
                        if let Err(e) = self.rollback_restore(entry, chunks) {
                            error!(
                                "Failed to rollback restore for cell {:?}: {:?}",
                                entry.cell_id, e
                            );
                            rollback_failures.push(format!(
                                "transaction {txn_id:?} {:?} compensation for cell {:?} failed: {e}",
                                entry.op_type, entry.cell_id
                            ));
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
            if rollback_failures.len() == failures_before {
                if let Err(error) = self.write_abort_marker(txn_id) {
                    rollback_failures.push(format!(
                        "transaction {txn_id:?} participant abort marker failed after durable compensation: {error}"
                    ));
                }
            }
        }

        info!(
            "Rollback complete: {} writes deleted, {} updates restored, {} removes restored",
            rollback_stats.0, rollback_stats.1, rollback_stats.2
        );

        if rollback_failures.is_empty() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "{} rollback compensation(s) failed: {}",
                    rollback_failures.len(),
                    rollback_failures.join("; ")
                ),
            ))
        }
    }

    /// Compensate a recovered insert with a newer tombstone when the recovered
    /// logical head is exactly the revision installed by the incomplete
    /// transaction.
    fn rollback_write(&self, entry: &UndoLogEntry, chunks: &Arc<Chunks>) -> io::Result<()> {
        debug!(
            "Compensating recovered Write: cell_id={:?}, installed_revision_ts={}",
            entry.cell_id, entry.installed_revision_ts
        );
        if chunks.current_revision_ts(&entry.cell_id) != Some(entry.installed_revision_ts) {
            debug!(
                "Recovered cell {:?} no longer has installed revision {}; compensation is already complete or the mutation was never durable",
                entry.cell_id, entry.installed_revision_ts
            );
            return Ok(());
        }
        chunks
            .compensate_recovered(&entry.cell_id, entry.installed_revision_ts, None)
            .and_then(|compensation| {
                chunks
                    .force_sync_installed_revisions([&compensation])
                    .map_err(|error| {
                        crate::ram::cell::WriteError::DurabilityFailure(error.to_string())
                    })
            })
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "insert compensation for cell {:?} at installed revision {} failed: {:?}",
                        entry.cell_id, entry.installed_revision_ts, error
                    ),
                )
            })
    }

    /// Compensate a recovered update or remove by restoring the immutable prior
    /// contents as a newer committed revision.
    fn rollback_restore(&self, entry: &UndoLogEntry, chunks: &Arc<Chunks>) -> io::Result<()> {
        entry.validate()?;
        let old_cell = entry.prior_cell.clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{:?} undo entry for {:?} has no prior cell",
                    entry.op_type, entry.cell_id
                ),
            )
        })?;
        debug!(
            "Compensating recovered {:?}: cell_id={:?}, installed_revision_ts={}, prior_revision_ts={}",
            entry.op_type,
            entry.cell_id,
            entry.installed_revision_ts,
            old_cell.header.revision_ts,
        );
        if chunks.current_revision_ts(&entry.cell_id) != Some(entry.installed_revision_ts) {
            debug!(
                "Recovered cell {:?} no longer has installed revision {}; compensation is already complete or a later revision won",
                entry.cell_id, entry.installed_revision_ts
            );
            return Ok(());
        }

        chunks
            .compensate_recovered(&entry.cell_id, entry.installed_revision_ts, Some(old_cell))
            .and_then(|compensation| {
                chunks
                    .force_sync_installed_revisions([&compensation])
                    .map_err(|error| {
                        crate::ram::cell::WriteError::DurabilityFailure(error.to_string())
                    })
            })
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "restore compensation for cell {:?} failed: {:?}",
                        entry.cell_id, error
                    ),
                )
            })
    }

    /// Trim old log files that only contain committed/aborted transactions
    pub fn trim_old_logs(&self) -> io::Result<()> {
        let current_seq = self.log_seq.load(Ordering::SeqCst);

        // Get all active transactions
        let active_txns: Vec<TxnId> = self.active_txns.lock().iter().cloned().collect();

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
                                                durable_fs::remove_file(&path)?;
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
                    let (_, size) = UndoLogEntry::from_bytes(&buffer[offset..])?;
                    offset += size;
                }
                ENTRY_TYPE_COMMIT | ENTRY_TYPE_ABORT => {
                    // Participant completion evidence is retained until a
                    // future compaction protocol can atomically prove that
                    // every earlier undo record is gone.
                    return Ok(true);
                }
                ENTRY_TYPE_COORDINATOR_COMMIT => {
                    let _ = coordinator_commit_record_size(&buffer, offset, txn_id_len)?;
                    // A delayed retry or restarting participant may still
                    // require this final distributed decision.
                    return Ok(true);
                }
                ENTRY_TYPE_COORDINATOR_ABORT => {
                    let _ = coordinator_abort_record_size(&buffer, offset, txn_id_len)?;
                    return Ok(true);
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

        // Collect all log files
        let entries = std::fs::read_dir(log_dir_path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "cannot read undo log directory {}: {}",
                    log_dir_path.display(),
                    error
                ),
            )
        })?;
        let log_files = collect_undo_log_paths(
            log_dir_path,
            entries.map(|entry| entry.map(|entry| entry.path())),
        )?;

        // Rebuild in-memory index
        let mut txn_index = HashMap::new();
        for (_seq, path) in &log_files {
            let mut file = File::open(path)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;

            let mut offset = 0;
            while offset < buffer.len() {
                if buffer.len() < offset + 5 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "cannot decode undo log {} at byte offset {}: incomplete entry header",
                            path.display(),
                            offset
                        ),
                    ));
                }

                let entry_type = buffer[offset];
                let txn_id_len = u32::from_le_bytes([
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                    buffer[offset + 4],
                ]) as usize;

                if buffer.len() < offset + 5 + txn_id_len {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "cannot decode undo log {} at byte offset {}: incomplete transaction id",
                            path.display(),
                            offset
                        ),
                    ));
                }

                let txn_id: TxnId = decode_txn_id(&buffer[offset + 5..offset + 5 + txn_id_len])
                    .map_err(|error| {
                        io::Error::new(
                            error.kind(),
                            format!(
                                "cannot decode undo log {} at byte offset {}: {}",
                                path.display(),
                                offset,
                                error
                            ),
                        )
                    })?;

                match entry_type {
                    ENTRY_TYPE_UNDO => {
                        let (entry, size) =
                            UndoLogEntry::from_bytes(&buffer[offset..]).map_err(|error| {
                                io::Error::new(
                                    error.kind(),
                                    format!(
                                        "cannot decode undo log {} at byte offset {}: {}",
                                        path.display(),
                                        offset,
                                        error
                                    ),
                                )
                            })?;
                        txn_index
                            .entry(txn_id.clone())
                            .or_insert_with(Vec::new)
                            .push(entry);
                        offset += size;
                    }
                    ENTRY_TYPE_COMMIT | ENTRY_TYPE_ABORT => {
                        txn_index.remove(&txn_id);
                        offset += 5 + txn_id_len;
                    }
                    ENTRY_TYPE_COORDINATOR_COMMIT => {
                        let (_, _, size) = coordinator_commit_record_size(
                            &buffer, offset, txn_id_len,
                        )
                        .map_err(|error| {
                            io::Error::new(
                                error.kind(),
                                format!(
                                    "cannot decode undo log {} at byte offset {}: {}",
                                    path.display(),
                                    offset,
                                    error
                                ),
                            )
                        })?;
                        offset += size;
                    }
                    ENTRY_TYPE_COORDINATOR_ABORT => {
                        let (_, size) = coordinator_abort_record_size(&buffer, offset, txn_id_len)
                            .map_err(|error| {
                                io::Error::new(
                                    error.kind(),
                                    format!(
                                        "cannot decode undo log {} at byte offset {}: {}",
                                        path.display(),
                                        offset,
                                        error
                                    ),
                                )
                            })?;
                        offset += size;
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "cannot decode undo log {} at byte offset {}: invalid entry type {}",
                                path.display(),
                                offset,
                                entry_type
                            ),
                        ));
                    }
                }
            }
        }

        // Update active transactions set for trimming
        let active_txns_set: HashSet<TxnId> = txn_index.keys().cloned().collect();
        *self.active_txns.lock() = active_txns_set;

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
    use crate::ram::cell::ReadError;
    use crate::ram::durable_fs::{
        directory_sync_count_for_test, fail_next_directory_sync_for_test,
    };
    use crate::ram::types::{OwnedMap, OwnedValue};
    use crate::server::transactions::test_hlc;
    use crate::server::transactions::{
        AbortResult, CheckError, EndResult, TMPrepareResult, TxnExecResult,
    };
    use dovahkiin::data_map_value;
    use dovahkiin::types::Map;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    // Helper function to create a random Id
    fn random_id() -> Id {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        Id {
            higher: now.as_secs(),
            lower: now.subsec_nanos() as u64,
        }
    }

    fn test_prior_cell(id: Id, revision_ts: u64) -> OwnedCell {
        let mut cell = OwnedCell::new_with_id(0, &id, OwnedValue::Null);
        cell.header.revision_ts = revision_ts;
        cell
    }

    #[test]
    fn undo_owned_prior_payload_preserves_ieee_float_bits() {
        let id = Id::new(91, 92);
        let f64_nan_bits = 0x7ff8_0000_0000_0042;
        let f32_nan_bits = 0x7fc0_0042;
        let mut prior = OwnedCell::new_with_id(
            0,
            &id,
            OwnedValue::Array(vec![
                OwnedValue::F64(f64::from_bits(f64_nan_bits)),
                OwnedValue::F64(f64::INFINITY),
                OwnedValue::F64(-0.0),
                OwnedValue::F32(f32::from_bits(f32_nan_bits)),
                OwnedValue::F32(f32::NEG_INFINITY),
                OwnedValue::F32(0.0),
            ]),
        );
        prior.header.revision_ts = 100;
        let encoded =
            UndoLogEntry::new_restore(test_hlc(99, 7), id, UndoOpType::Update, 101, prior)
                .to_bytes()
                .expect("valid IEEE values must be encodable");
        let (decoded, consumed) =
            UndoLogEntry::from_bytes(&encoded).expect("owned prior payload must decode");
        assert_eq!(consumed, encoded.len());

        let OwnedValue::Array(values) = decoded
            .prior_cell
            .expect("update must retain its prior cell")
            .data
        else {
            panic!("expected float array");
        };
        assert_eq!(values[0].f64().expect("f64 NaN").to_bits(), f64_nan_bits);
        assert_eq!(
            values[1].f64().expect("f64 infinity").to_bits(),
            f64::INFINITY.to_bits()
        );
        assert_eq!(
            values[2].f64().expect("f64 signed zero").to_bits(),
            (-0.0f64).to_bits()
        );
        assert_eq!(values[3].f32().expect("f32 NaN").to_bits(), f32_nan_bits);
        assert_eq!(
            values[4].f32().expect("f32 infinity").to_bits(),
            f32::NEG_INFINITY.to_bits()
        );
        assert_eq!(
            values[5].f32().expect("f32 signed zero").to_bits(),
            0.0f32.to_bits()
        );
    }

    fn compensation_test_chunks(name: &str) -> (crate::ram::schema::Schema, Arc<Chunks>) {
        use crate::ram::schema::{Field, Schema};
        use dovahkiin::types::Type;

        let schema = Schema::new(
            name,
            None,
            Field::new_schema(vec![
                Field::new_unindexed("id", Type::I32),
                Field::new_unindexed("value", Type::String),
            ]),
            false,
            false,
        );
        let schemas = crate::ram::schema::LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            1,
            32 * 1024 * 1024,
            Arc::new(crate::server::ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        (schema, chunks)
    }

    #[test]
    fn undo_log_creation_and_rotation_durably_publish_each_new_filename_once() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let before = directory_sync_count_for_test(&log_dir);

        let undo =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("initial undo log");
        assert_eq!(directory_sync_count_for_test(&log_dir), before + 1);

        undo.write_undo_entry(UndoLogEntry::new_write(test_hlc(1, 1), Id::new(1, 1), 2))
            .unwrap();
        assert_eq!(
            directory_sync_count_for_test(&log_dir),
            before + 1,
            "ordinary records on an existing undo file must not resync its directory"
        );

        undo.rotate_log().expect("rotate undo log");
        assert_eq!(
            directory_sync_count_for_test(&log_dir),
            before + 2,
            "a newly rotated filename must be durably published"
        );
    }

    #[test]
    fn undo_log_creation_propagates_directory_sync_failure() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        std::fs::create_dir_all(&log_dir).unwrap();
        fail_next_directory_sync_for_test(&log_dir);

        let error = match UndoLogger::new(log_dir.to_string_lossy().into_owned()) {
            Ok(_) => panic!("undo logger must reject an undurable initial filename"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(error
            .to_string()
            .contains("injected directory sync failure"));
    }

    #[test]
    fn failed_rotations_do_not_make_active_log_trim_eligible() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).unwrap();
        let active_path = PathBuf::from(
            undo.log_file_name
                .lock()
                .clone()
                .expect("active log filename"),
        );

        for _ in 0..3 {
            fail_next_directory_sync_for_test(&log_dir);
            undo.rotate_log()
                .expect_err("injected rotation publication must fail");
        }
        undo.trim_old_logs().unwrap();

        assert!(
            active_path.exists(),
            "failed rotations must not age the still-active log into the trim window"
        );
        let tid = test_hlc(5, 9);
        undo.write_coordinator_abort_decision(&tid, &[17]).unwrap();
        drop(undo);
        let reopened = UndoLogger::new(log_dir.to_string_lossy().into_owned()).unwrap();
        assert_eq!(
            reopened.coordinator_decision(&tid).unwrap(),
            Some(TxnResolution::Abort),
            "records appended after failed rotations must remain crash-visible"
        );
    }

    #[test]
    fn coordinator_decision_rotation_directory_failure_prevents_durable_decision() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).unwrap();
        let tid = test_hlc(8, 2);
        undo.rotate_before_next_record_for_test();
        fail_next_directory_sync_for_test(&log_dir);

        let error = undo
            .write_coordinator_commit_decision(&tid, test_hlc(9, 2), &[11])
            .expect_err("undurable rotated filename must reject coordinator decision");
        assert!(error
            .to_string()
            .contains("injected directory sync failure"));
        assert_eq!(undo.coordinator_decision(&tid).unwrap(), None);
    }

    #[test]
    fn test_undo_entry_serialization() {
        let txn_id = TxnId::default();
        let cell_id = Id {
            higher: 1,
            lower: 2,
        };
        let entry = UndoLogEntry::new_restore(
            txn_id,
            cell_id,
            UndoOpType::Update,
            6,
            test_prior_cell(cell_id, 5),
        );

        let bytes = entry.to_bytes().unwrap();
        let (recovered, size) = UndoLogEntry::from_bytes(&bytes).unwrap();

        assert_eq!(size, bytes.len());
        assert_eq!(recovered.cell_id, entry.cell_id);
        assert_eq!(recovered.op_type, entry.op_type);
        assert_eq!(recovered.installed_revision_ts, entry.installed_revision_ts);
        assert_eq!(
            serde_json::to_vec(&recovered.prior_cell).unwrap(),
            serde_json::to_vec(&entry.prior_cell).unwrap()
        );
    }

    #[test]
    fn undo_owned_prior_round_trips_by_operation_without_a_raw_location() {
        let id = Id::new(1, 2);
        let mut prior = OwnedCell::new_with_id(
            7,
            &id,
            data_map_value!(id: 1i32, value: "prior".to_string()),
        );
        prior.header.revision_ts = 100;

        for operation in [UndoOpType::Update, UndoOpType::Remove] {
            let entry =
                UndoLogEntry::new_restore(test_hlc(10, 1), id, operation, 200, prior.clone());
            let decoded = UndoLogEntry::from_bytes(&entry.to_bytes().unwrap())
                .unwrap()
                .0;
            assert_eq!(decoded.op_type, operation);
            let decoded_prior = decoded
                .prior_cell
                .expect("update/remove undo must own prior bytes");
            assert_eq!(
                serde_json::to_vec(&decoded_prior).unwrap(),
                serde_json::to_vec(&prior).unwrap()
            );
        }

        let insert = UndoLogEntry::new_write(test_hlc(11, 1), id, 201);
        assert!(
            UndoLogEntry::from_bytes(&insert.to_bytes().unwrap())
                .unwrap()
                .0
                .prior_cell
                .is_none(),
            "insert undo has no prior bytes"
        );
    }

    #[test]
    fn undo_decoder_rejects_legacy_single_revision_layout() {
        let txn_id_bytes = serde_json::to_vec(&test_hlc(10, 1)).unwrap();
        let mut bytes = Vec::new();
        bytes.push(1);
        bytes.extend_from_slice(&(txn_id_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&txn_id_bytes);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&2u64.to_le_bytes());
        bytes.push(UndoOpType::Update as u8);
        bytes.extend_from_slice(&100u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&9u64.to_le_bytes());
        bytes.extend_from_slice(&64u64.to_le_bytes());

        UndoLogEntry::from_bytes(&bytes)
            .expect_err("the legacy single-revision undo layout must not be decoded");
    }

    #[test]
    fn undo_decoder_rejects_legacy_layout_even_when_an_adjacent_record_supplies_extra_bytes() {
        let txn_id = test_hlc(10, 1);
        let txn_id_bytes = serde_json::to_vec(&txn_id).unwrap();
        let mut bytes = Vec::new();
        bytes.push(1);
        bytes.extend_from_slice(&(txn_id_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&txn_id_bytes);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&2u64.to_le_bytes());
        bytes.push(UndoOpType::Update as u8);
        bytes.extend_from_slice(&100u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&9u64.to_le_bytes());
        bytes.extend_from_slice(&64u64.to_le_bytes());

        bytes.push(ENTRY_TYPE_COMMIT);
        bytes.extend_from_slice(&(txn_id_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&txn_id_bytes);

        UndoLogEntry::from_bytes(&bytes).expect_err(
            "an adjacent record must not make the legacy single-revision layout look current",
        );
    }

    #[test]
    fn undo_bytes_record_installed_and_prior_revisions() {
        let id = Id::new(1, 2);
        let entry = UndoLogEntry::new_restore(
            test_hlc(10, 1),
            id,
            UndoOpType::Update,
            200,
            test_prior_cell(id, 100),
        );
        let decoded = UndoLogEntry::from_bytes(&entry.to_bytes().unwrap())
            .unwrap()
            .0;
        assert_eq!(decoded.installed_revision_ts, 200);
        assert_eq!(decoded.prior_cell.unwrap().header.revision_ts, 100);
    }

    #[test]
    fn coordinator_commit_decision_is_durable_without_suppressing_participant_undo() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(300, 7);
        let commit_hlc = test_hlc(400, 7);
        let cell_id = Id::new(1, 2);

        {
            let undo =
                UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
            undo.write_undo_entry(UndoLogEntry::new_write(tid, cell_id, commit_hlc.ts))
                .unwrap();
            undo.write_coordinator_commit_decision(&tid, commit_hlc, &[11, 12])
                .unwrap();

            assert_eq!(
                undo.coordinator_decision(&tid).unwrap(),
                Some(super::super::TxnResolution::Commit(commit_hlc))
            );
            assert_eq!(
                undo.coordinator_decision_record(&tid)
                    .unwrap()
                    .unwrap()
                    .participants,
                BTreeSet::from([11, 12])
            );
            assert!(
                undo.recover().unwrap().contains_key(&tid),
                "a coordinator decision is not participant completion and must not suppress undo"
            );
        }

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert_eq!(
            reopened.coordinator_decision(&tid).unwrap(),
            Some(super::super::TxnResolution::Commit(commit_hlc))
        );
        assert!(reopened.recover().unwrap().contains_key(&tid));
    }

    #[test]
    fn committed_recovery_resolution_preserves_installed_output_instead_of_compensating() {
        let temp_dir = TempDir::new().unwrap();
        let (schema, chunks) = compensation_test_chunks("committed_recovery_resolution");
        let id = Id::new(0, 706);
        let mut installed = OwnedCell::new_with_id(
            schema.id,
            &id,
            data_map_value!(id: 1i32, value: "chosen-commit".to_string()),
        );
        let installed_revision_ts = chunks.write_cell(&mut installed).unwrap().revision_ts;
        let tid = test_hlc(301, 8);
        let commit_hlc = bifrost::hlc::Hlc {
            ts: installed_revision_ts,
            node: tid.node,
        };
        let undo =
            UndoLogger::new(temp_dir.path().join("undo").to_string_lossy().into_owned()).unwrap();
        undo.write_undo_entry(UndoLogEntry::new_write(tid, id, installed_revision_ts))
            .unwrap();

        let unresolved = undo
            .resolve_recovered_transactions(
                undo.recover().unwrap(),
                &HashMap::from([(tid, super::super::TxnResolution::Commit(commit_hlc))]),
            )
            .unwrap();
        assert!(unresolved.is_empty());
        undo.rollback_incomplete_transactions(unresolved, &chunks)
            .unwrap();

        let retained = chunks.read_cell(&id).unwrap().to_owned();
        assert_eq!(retained.header.revision_ts, installed_revision_ts);
        assert_eq!(retained.data, installed.data);
        assert!(
            !undo.recover().unwrap().contains_key(&tid),
            "participant commit outcome must become durable before recovery continues"
        );
    }

    #[test]
    fn unknown_recovery_resolution_is_conservative_and_leaves_pending_bytes_and_undo_intact() {
        let temp_dir = TempDir::new().unwrap();
        let (schema, chunks) = compensation_test_chunks("unknown_recovery_resolution");
        let id = Id::new(0, 707);
        let mut installed = OwnedCell::new_with_id(
            schema.id,
            &id,
            data_map_value!(id: 1i32, value: "unresolved".to_string()),
        );
        let installed_revision_ts = chunks.write_cell(&mut installed).unwrap().revision_ts;
        let tid = test_hlc(302, 9);
        let undo =
            UndoLogger::new(temp_dir.path().join("undo").to_string_lossy().into_owned()).unwrap();
        undo.write_undo_entry(UndoLogEntry::new_write(tid, id, installed_revision_ts))
            .unwrap();

        let error = undo
            .resolve_recovered_transactions(
                undo.recover().unwrap(),
                &HashMap::from([(tid, super::super::TxnResolution::Unknown)]),
            )
            .expect_err("unknown coordinator state must stop recovery");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        let retained = chunks.read_cell(&id).unwrap().to_owned();
        assert_eq!(retained.header.revision_ts, installed_revision_ts);
        assert_eq!(retained.data, installed.data);
        assert!(
            undo.recover().unwrap().contains_key(&tid),
            "unknown resolution must not emit a completion marker or discard undo"
        );
    }

    #[test]
    fn reopened_logger_orders_recovery_completion_after_existing_log_sequences() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_string_lossy().into_owned();
        let tid = test_hlc(303, 10);

        {
            let undo = UndoLogger::new(log_dir.clone()).unwrap();
            undo.rotate_log().unwrap();
            undo.write_undo_entry(UndoLogEntry::new_write(tid, Id::new(1, 158), 304))
                .unwrap();
        }

        {
            let reopened = UndoLogger::new(log_dir.clone()).unwrap();
            let pending = reopened.recover().unwrap();
            assert!(pending.contains_key(&tid));
            reopened
                .resolve_recovered_transactions(
                    pending,
                    &HashMap::from([(tid, super::super::TxnResolution::Commit(test_hlc(305, 10)))]),
                )
                .unwrap();
        }

        let reopened = UndoLogger::new(log_dir).unwrap();
        assert!(
            !reopened.recover().unwrap().contains_key(&tid),
            "a startup completion marker must sort after every pre-existing undo record"
        );
        assert_eq!(
            reopened.participant_completion(&tid).unwrap(),
            Some(TxnState::Committed)
        );
    }

    #[test]
    fn trimming_retains_distributed_decisions_and_participant_completion_evidence() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_string_lossy().into_owned();
        let decision_tid = test_hlc(306, 11);
        let participant_tid = test_hlc(307, 11);
        let commit_hlc = test_hlc(308, 11);

        {
            let undo = UndoLogger::new(log_dir.clone()).unwrap();
            undo.write_coordinator_commit_decision(&decision_tid, commit_hlc, &[21, 22])
                .unwrap();
            undo.write_undo_entry(UndoLogEntry::new_write(
                participant_tid,
                Id::new(1, 159),
                309,
            ))
            .unwrap();
            undo.write_commit_marker(&participant_tid).unwrap();
            for _ in 0..4 {
                undo.rotate_log().unwrap();
            }
            undo.trim_old_logs().unwrap();
        }

        let reopened = UndoLogger::new(log_dir).unwrap();
        assert_eq!(
            reopened.coordinator_decision_record(&decision_tid).unwrap(),
            Some(CoordinatorDecisionRecord {
                resolution: TxnResolution::Commit(commit_hlc),
                participants: BTreeSet::from([21, 22]),
            })
        );
        assert_eq!(
            reopened.participant_completion(&participant_tid).unwrap(),
            Some(TxnState::Committed),
            "delayed end retries must keep durable participant completion evidence"
        );
        assert!(
            !reopened.recover().unwrap().contains_key(&participant_tid),
            "retained completion evidence must continue suppressing participant undo"
        );
    }

    #[test]
    fn trimming_follows_owned_prior_payload_to_participant_completion() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_string_lossy().into_owned();
        let participant_tid = test_hlc(310, 11);
        let cell_id = Id::new(1, 160);

        {
            let undo = UndoLogger::new(log_dir.clone()).unwrap();
            undo.write_undo_entry(UndoLogEntry::new_restore(
                participant_tid,
                cell_id,
                UndoOpType::Update,
                312,
                test_prior_cell(cell_id, 311),
            ))
            .unwrap();
            undo.write_commit_marker(&participant_tid).unwrap();
            for _ in 0..4 {
                undo.rotate_log().unwrap();
            }
            undo.trim_old_logs().unwrap();
        }

        let reopened = UndoLogger::new(log_dir).unwrap();
        assert_eq!(
            reopened.participant_completion(&participant_tid).unwrap(),
            Some(TxnState::Committed),
            "trimming must step over the variable owned-prior payload before retaining completion"
        );
    }

    #[test]
    fn test_undo_log_basic() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir).unwrap();

        let txn_id = TxnId::default();
        let cell_id = Id {
            higher: 1,
            lower: 2,
        };
        let entry = UndoLogEntry::new_restore(
            txn_id.clone(),
            cell_id,
            UndoOpType::Update,
            4,
            test_prior_cell(cell_id, 3),
        );

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
        let cell_id = Id {
            higher: 1,
            lower: 2,
        };
        let entry = UndoLogEntry::new_restore(
            txn_id.clone(),
            cell_id,
            UndoOpType::Remove,
            3,
            test_prior_cell(cell_id, 2),
        );

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
    fn recovery_propagates_undo_directory_open_failure() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let undo_log = UndoLogger::new(log_dir.to_string_lossy().into_owned()).unwrap();
        std::fs::rename(&log_dir, temp_dir.path().join("moved-undo")).unwrap();

        let error = undo_log
            .recover()
            .expect_err("a missing undo directory must fail recovery");
        let message = error.to_string();

        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(
            message.contains("cannot read undo log directory"),
            "{message}"
        );
        assert!(
            message.contains(log_dir.to_string_lossy().as_ref()),
            "{message}"
        );
    }

    #[test]
    fn undo_path_collection_propagates_iterator_entry_failure() {
        let log_dir = Path::new("/injected/undo");
        let error = collect_undo_log_paths(
            log_dir,
            [Err::<std::path::PathBuf, _>(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected iterator entry failure",
            ))],
        )
        .expect_err("an iterator entry failure must fail path collection");
        let message = error.to_string();

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            message.contains("cannot enumerate entry in undo log directory"),
            "{message}"
        );
        assert!(message.contains("/injected/undo"), "{message}");
        assert!(
            message.contains("injected iterator entry failure"),
            "{message}"
        );
    }

    #[test]
    fn test_undo_log_multiple_entries() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir).unwrap();

        let txn_id = TxnId::default();
        let cell_id1 = Id {
            higher: 1,
            lower: 2,
        };
        let cell_id2 = Id {
            higher: 3,
            lower: 4,
        };

        let entry1 = UndoLogEntry::new_write(txn_id.clone(), cell_id1, 1);
        let entry2 = UndoLogEntry::new_restore(
            txn_id.clone(),
            cell_id2,
            UndoOpType::Update,
            5,
            test_prior_cell(cell_id2, 4),
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
        let cell_id1 = Id {
            higher: 100,
            lower: 1,
        };
        let entry1 = UndoLogEntry::new_write(txn1.clone(), cell_id1, 1);
        undo_log.write_undo_entry(entry1).unwrap();

        // Verify undo entry exists
        let txn_index = undo_log.recover().unwrap();
        let entries = txn_index.get(&txn1).cloned().unwrap_or_default();
        assert_eq!(entries.len(), 1, "Should have 1 undo entry before commit");
        assert_eq!(entries[0].op_type, UndoOpType::Write);
        assert_eq!(entries[0].installed_revision_ts, 1);
        assert!(entries[0].prior_cell.is_none());

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
        let cell_id1 = Id {
            higher: 200,
            lower: 1,
        };
        let cell_id2 = Id {
            higher: 200,
            lower: 2,
        };

        // Write, Update, and Remove operations
        let entry1 = UndoLogEntry::new_write(txn.clone(), cell_id1, 1);
        let entry2 = UndoLogEntry::new_restore(
            txn.clone(),
            cell_id2,
            UndoOpType::Update,
            6,
            test_prior_cell(cell_id2, 5),
        );

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
        let cell_id1 = Id {
            higher: 300,
            lower: 1,
        };
        let cell_id2 = Id {
            higher: 300,
            lower: 2,
        };
        let cell_id3 = Id {
            higher: 300,
            lower: 3,
        };

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
                4,
                test_prior_cell(cell_id2, 3),
            );
            let entry3 = UndoLogEntry::new_restore(
                txn_incomplete.clone(),
                cell_id3,
                UndoOpType::Remove,
                8,
                test_prior_cell(cell_id3, 7),
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
        assert_eq!(entries[0].installed_revision_ts, 1);
        assert!(entries[0].prior_cell.is_none());

        assert_eq!(entries[1].cell_id, cell_id2);
        assert_eq!(entries[1].op_type, UndoOpType::Update);
        assert_eq!(entries[1].installed_revision_ts, 4);
        assert_eq!(
            entries[1].prior_cell.as_ref().unwrap().header.revision_ts,
            3
        );

        assert_eq!(entries[2].cell_id, cell_id3);
        assert_eq!(entries[2].op_type, UndoOpType::Remove);
        assert_eq!(entries[2].installed_revision_ts, 8);
        assert_eq!(
            entries[2].prior_cell.as_ref().unwrap().header.revision_ts,
            7
        );
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
        // [entry_type: u8][txn_id_len: u32][txn_id: bytes][cell_id.higher: u64]
        // [cell_id.lower: u64][op_type: u8][installed_revision_ts: u64]
        // [prior_cell_len: u32][prior_owned_cell: bytes]
        let mut bytes = Vec::new();
        bytes.push(ENTRY_TYPE_UNDO);
        bytes.extend_from_slice(&(old_txn_id_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&old_txn_id_bytes);
        bytes.extend_from_slice(&1u64.to_le_bytes()); // cell_id.higher
        bytes.extend_from_slice(&2u64.to_le_bytes()); // cell_id.lower
        bytes.push(UndoOpType::Write as u8);
        bytes.extend_from_slice(&1u64.to_le_bytes()); // installed_revision_ts
        bytes.extend_from_slice(&0u32.to_le_bytes()); // prior_cell_len (None)

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
                Id {
                    higher: 1,
                    lower: 1,
                },
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

                match entry_type {
                    ENTRY_TYPE_UNDO => {
                        let (_, size) = UndoLogEntry::from_bytes(&contents[offset..]).unwrap();
                        offset += size;
                    }
                    ENTRY_TYPE_COMMIT | ENTRY_TYPE_ABORT => offset += 5 + txn_id_len,
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
                Id {
                    higher: 1,
                    lower: 1,
                },
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
                Id {
                    higher: 2,
                    lower: 1,
                },
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
                Id {
                    higher: 3,
                    lower: 1,
                },
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
                Id {
                    higher: i,
                    lower: 1,
                },
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
                Id {
                    higher: i,
                    lower: 1,
                },
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
                    Id {
                        higher: i,
                        lower: 1,
                    },
                    i,
                );
                undo_log.write_undo_entry(entry).unwrap();
                undo_log.write_commit_marker(&txn).unwrap();
            }

            // Write an incomplete transaction
            let entry_incomplete = UndoLogEntry::new_write(
                txn_incomplete.clone(),
                Id {
                    higher: 999,
                    lower: 1,
                },
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

    /// Test end-to-end: installed and prior revision persistence by operation.
    #[test]
    fn test_e2e_version_verification() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap().to_string();

        let undo_log = UndoLogger::new(log_dir.clone()).unwrap();
        let txn = TxnId::default();

        // Write records only the revision installed by the incomplete transaction.
        let write_entry = UndoLogEntry::new_write(
            txn.clone(),
            Id {
                higher: 1,
                lower: 1,
            },
            10,
        );
        undo_log.write_undo_entry(write_entry).unwrap();

        // Update records both installed and prior revisions.
        let update_id = Id {
            higher: 1,
            lower: 2,
        };
        let update_entry = UndoLogEntry::new_restore(
            txn.clone(),
            update_id,
            UndoOpType::Update,
            21,
            test_prior_cell(update_id, 20),
        );
        undo_log.write_undo_entry(update_entry).unwrap();

        // Remove records the installed tombstone and prior present revision.
        let remove_id = Id {
            higher: 1,
            lower: 3,
        };
        let remove_entry = UndoLogEntry::new_restore(
            txn.clone(),
            remove_id,
            UndoOpType::Remove,
            31,
            test_prior_cell(remove_id, 30),
        );
        undo_log.write_undo_entry(remove_entry).unwrap();

        // Recover and verify all versions are preserved
        let txn_index = undo_log.recover().unwrap();
        let entries = txn_index.get(&txn).cloned().unwrap_or_default();

        assert_eq!(entries.len(), 3);

        // Verify Write entry
        assert_eq!(entries[0].op_type, UndoOpType::Write);
        assert_eq!(entries[0].installed_revision_ts, 10);
        assert!(entries[0].prior_cell.is_none());

        // Verify Update entry
        assert_eq!(entries[1].op_type, UndoOpType::Update);
        assert_eq!(entries[1].installed_revision_ts, 21);
        assert_eq!(
            entries[1].prior_cell.as_ref().unwrap().header.revision_ts,
            20
        );

        // Verify Remove entry
        assert_eq!(entries[2].op_type, UndoOpType::Remove);
        assert_eq!(entries[2].installed_revision_ts, 31);
        assert_eq!(
            entries[2].prior_cell.as_ref().unwrap().header.revision_ts,
            30
        );
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

        let cell_id = Id {
            higher: 1,
            lower: 100,
        };

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
            let entry = UndoLogEntry::new_write(txn_id, cell_id, cell.header.revision_ts);
            undo_log.write_undo_entry(entry).unwrap();
            // No commit marker - simulate crash
        }

        // Phase 2: Recovery with rollback
        {
            let (chunks, recovery) = Chunks::recover_with_clock(
                1,
                32 * 1024 * 1024,
                meta.clone(),
                None,
                Some(backup_dir.to_str().unwrap().to_string()),
                Some(wal_dir.to_str().unwrap().to_string()),
                None,
                Some(raft_path.clone()),
                Arc::new(bifrost::hlc::HlcSource::new(0)),
                300_000,
            )
            .unwrap();
            chunks
                .revision_clock()
                .try_observe(bifrost::hlc::Hlc {
                    ts: recovery.max_revision_ts,
                    node: chunks.revision_clock().node(),
                })
                .unwrap();
            assert!(
                chunks
                    .list
                    .iter()
                    .all(|chunk| chunk.history.recovery_floor() == 0),
                "undo must run before the recovery floor is established"
            );

            let undo_log = UndoLogger::new(log_dir.to_str().unwrap().to_string()).unwrap();
            let txn_index = undo_log.recover().unwrap();
            undo_log
                .rollback_incomplete_transactions(txn_index, &chunks)
                .unwrap();
            let floor = chunks.establish_recovery_floor().unwrap();
            assert!(chunks
                .list
                .iter()
                .all(|chunk| chunk.history.recovery_floor() == floor));

            // Cell should be deleted after rollback
            assert!(
                chunks.read_cell(&cell_id).is_err(),
                "Cell should be deleted after rollback"
            );
        }
    }

    #[test]
    fn rollback_write_failure_is_returned_after_other_records_are_attempted() {
        use crate::ram::schema::{Field, Schema};
        use dovahkiin::types::Type;

        let temp_dir = TempDir::new().unwrap();
        let fields = Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed("value", Type::String),
        ]);
        let schema = Schema::new("rollback_write_failure", None, fields, false, false);
        let schemas = crate::ram::schema::LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            1,
            32 * 1024 * 1024,
            Arc::new(crate::server::ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        let failed_id = Id::new(1, 501);
        let successful_id = Id::new(1, 502);
        let mut failed_cell = OwnedCell::new_with_id(
            schema.id,
            &failed_id,
            data_map_value!(id: 1i32, value: "failed".to_string()),
        );
        let mut successful_cell = OwnedCell::new_with_id(
            schema.id,
            &successful_id,
            data_map_value!(id: 2i32, value: "successful".to_string()),
        );
        chunks.write_cell(&mut failed_cell).unwrap();
        chunks.write_cell(&mut successful_cell).unwrap();

        let txn_id = test_hlc(10, 1);
        let txn_index = HashMap::from([(
            txn_id,
            vec![
                UndoLogEntry::new_write(txn_id, failed_id, failed_cell.header.revision_ts),
                UndoLogEntry::new_write(txn_id, successful_id, successful_cell.header.revision_ts),
            ],
        )]);
        chunks.fail_next_allocation_for_test(&failed_id);
        let undo_log =
            UndoLogger::new(temp_dir.path().join("undo").to_string_lossy().into_owned()).unwrap();

        let error = undo_log
            .rollback_incomplete_transactions(txn_index, &chunks)
            .expect_err("a failed compensation must reach the recovery caller");

        assert!(
            error.to_string().contains("Write")
                && error.to_string().contains(&format!("{failed_id:?}")),
            "rollback failure should retain operation and cell context: {error}"
        );
        assert!(
            chunks.read_cell(&failed_id).is_ok(),
            "the failed compensation must leave its cell present"
        );
        assert!(
            chunks.read_cell(&successful_id).is_err(),
            "independent rollback records must still be attempted"
        );
    }

    #[test]
    fn rollback_restore_failure_is_returned_to_the_caller() {
        use crate::ram::schema::{Field, Schema};
        use dovahkiin::types::Type;

        let temp_dir = TempDir::new().unwrap();
        let fields = Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed("value", Type::String),
        ]);
        let schema = Schema::new("rollback_restore_failure", None, fields, false, false);
        let schemas = crate::ram::schema::LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            2,
            32 * 1024 * 1024,
            Arc::new(crate::server::ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        let cell_id = Id::new(1, 601);
        let mut cell = OwnedCell::new_with_id(
            schema.id,
            &cell_id,
            data_map_value!(id: 1i32, value: "restore".to_string()),
        );
        chunks.write_cell(&mut cell).unwrap();
        let prior_cell = cell.clone();
        chunks.remove_cell(&cell_id).unwrap();
        let installed_revision_ts = chunks.current_revision_ts(&cell_id).unwrap();
        let entry = UndoLogEntry::new_restore(
            test_hlc(11, 1),
            cell_id,
            UndoOpType::Remove,
            installed_revision_ts,
            prior_cell,
        );
        chunks.fail_next_allocation_for_test(&cell_id);
        let unrelated_id = Id::new(2, 602);
        let mut unrelated_cell = OwnedCell::new_with_id(
            schema.id,
            &unrelated_id,
            data_map_value!(id: 2i32, value: "unrelated".to_string()),
        );
        chunks
            .write_cell(&mut unrelated_cell)
            .expect("a different chunk must not consume the injected failure");
        let undo_log =
            UndoLogger::new(temp_dir.path().join("undo").to_string_lossy().into_owned()).unwrap();

        let error = undo_log
            .rollback_restore(&entry, &chunks)
            .expect_err("a failed restore compensation must reach the recovery caller");

        assert!(
            error.to_string().contains("restore"),
            "restore failure should retain compensation context: {error}"
        );
        assert!(chunks.read_cell(&cell_id).is_err());
        let follow_up_id = Id::new(1, 603);
        let mut follow_up_cell = OwnedCell::new_with_id(
            schema.id,
            &follow_up_id,
            data_map_value!(id: 3i32, value: "one-shot".to_string()),
        );
        chunks
            .write_cell(&mut follow_up_cell)
            .expect("the injected failure must be one-shot");
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

    /// Test: Rollback restores old data with new revision_ts
    /// Verifies that rollback restores the old cell data but with a new, incremented revision_ts
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
        // Create a cell with initial revision_ts
        let cell_id = Id {
            higher: 1,
            lower: 100,
        };
        let mut cell = OwnedCell::new_with_id(
            0,
            &cell_id,
            data_map_value!(id: 1i32, value: "v1".to_string()),
        );
        chunks.write_cell(&mut cell).unwrap();
        let initial_revision_ts = cell.header.revision_ts;
        let prior_cell = cell.clone();
        println!("Created cell with revision_ts {}", initial_revision_ts);

        let mut failed = OwnedCell::new_with_id(
            0,
            &cell_id,
            data_map_value!(id: 1i32, value: "v2".to_string()),
        );
        let installed_revision_ts = chunks.update_cell(&mut failed).unwrap().revision_ts;

        // Simulate incomplete transaction that updated the cell.
        let txn_id = test_hlc(1, 1);
        let undo_entry = UndoLogEntry::new_restore(
            txn_id.clone(),
            cell_id,
            UndoOpType::Update,
            installed_revision_ts,
            prior_cell,
        );
        undo_log.write_undo_entry(undo_entry).unwrap();

        println!("=== Step 2: Perform rollback (restore v1) ===");
        println!("About to recover...");
        let txn_index = undo_log.recover().unwrap();
        println!("About to rollback...");
        undo_log
            .rollback_incomplete_transactions(txn_index, &chunks)
            .unwrap();
        println!("Rollback complete!");

        // Verify cell has been rolled back - the old data restored with a new revision_ts
        let after_rollback = chunks.read_cell(&cell_id).unwrap();
        assert!(
            after_rollback.header.revision_ts > installed_revision_ts,
            "Rollback should use a newer compensation revision"
        );
        assert_eq!(
            after_rollback.data["value"].string().unwrap(),
            "v1",
            "Rollback should restore the original data from the undo log"
        );
    }

    #[test]
    fn recovery_compensates_insert_with_newer_tombstone_idempotently() {
        let temp_dir = TempDir::new().unwrap();
        let (schema, chunks) = compensation_test_chunks("recovery_insert_compensation");
        let id = Id::new(0, 701);
        let mut failed = OwnedCell::new_with_id(
            schema.id,
            &id,
            data_map_value!(id: 1i32, value: "failed".to_string()),
        );
        let installed_revision_ts = chunks.write_cell(&mut failed).unwrap().revision_ts;
        let tid = test_hlc(20, 1);
        let entry = UndoLogEntry::new_write(tid, id, installed_revision_ts);
        let undo =
            UndoLogger::new(temp_dir.path().join("undo").to_string_lossy().into_owned()).unwrap();

        undo.rollback_incomplete_transactions(HashMap::from([(tid, vec![entry.clone()])]), &chunks)
            .unwrap();
        assert!(chunks.read_cell(&id).is_err());
        let compensation_ts = chunks.current_revision_ts(&id).unwrap();
        assert!(compensation_ts > installed_revision_ts);
        assert!(matches!(
            chunks.read_cell_snapshot(&id, compensation_ts).unwrap(),
            crate::ram::cell::SnapshotRead::Absent(None)
        ));

        undo.rollback_incomplete_transactions(HashMap::from([(tid, vec![entry])]), &chunks)
            .unwrap();
        assert_eq!(chunks.current_revision_ts(&id), Some(compensation_ts));
    }

    #[test]
    fn recovery_observes_undo_installed_timestamp_even_when_mutation_was_not_durable() {
        let temp_dir = TempDir::new().unwrap();
        let (_schema, chunks) = compensation_test_chunks("recovery_undo_clock_floor");
        let undo =
            UndoLogger::new(temp_dir.path().join("undo").to_string_lossy().into_owned()).unwrap();
        let tid = test_hlc(24, 1);
        let cell_id = Id::new(0, 705);
        let physical_floor = chunks.revision_clock().try_now().unwrap().ts;
        let undo_only_installed_ts = physical_floor.checked_add(1_000_000).unwrap();
        undo.write_undo_entry(UndoLogEntry::new_write(
            tid,
            cell_id,
            undo_only_installed_ts,
        ))
        .unwrap();

        let recovered = undo.recover().unwrap();
        undo.rollback_incomplete_transactions(recovered, &chunks)
            .unwrap();
        let next = chunks.next_revision_ts(0).unwrap();
        assert!(
            next > undo_only_installed_ts,
            "new traffic must start above every durable undo installed timestamp"
        );
    }

    #[test]
    fn recovery_compensates_update_with_newer_content_idempotently() {
        let temp_dir = TempDir::new().unwrap();
        let (schema, chunks) = compensation_test_chunks("recovery_update_compensation");
        let id = Id::new(0, 702);
        let mut original = OwnedCell::new_with_id(
            schema.id,
            &id,
            data_map_value!(id: 1i32, value: "original".to_string()),
        );
        let prior_revision_ts = chunks.write_cell(&mut original).unwrap().revision_ts;
        let prior_cell = original.clone();
        let mut failed = OwnedCell::new_with_id(
            schema.id,
            &id,
            data_map_value!(id: 1i32, value: "failed".to_string()),
        );
        let installed_revision_ts = chunks.update_cell(&mut failed).unwrap().revision_ts;
        let tid = test_hlc(21, 1);
        let entry = UndoLogEntry::new_restore(
            tid,
            id,
            UndoOpType::Update,
            installed_revision_ts,
            prior_cell,
        );
        let undo =
            UndoLogger::new(temp_dir.path().join("undo").to_string_lossy().into_owned()).unwrap();

        undo.rollback_incomplete_transactions(HashMap::from([(tid, vec![entry.clone()])]), &chunks)
            .unwrap();
        let compensated = chunks.read_cell(&id).unwrap().to_owned();
        assert_eq!(compensated.data, original.data);
        assert!(compensated.header.revision_ts > installed_revision_ts);
        let compensation_ts = compensated.header.revision_ts;
        assert!(matches!(
            chunks.read_cell_snapshot(&id, compensation_ts).unwrap(),
            crate::ram::cell::SnapshotRead::Present(ref cell)
                if cell.header.revision_ts == prior_revision_ts
                    && cell.data == original.data
        ));

        undo.rollback_incomplete_transactions(HashMap::from([(tid, vec![entry.clone()])]), &chunks)
            .unwrap();
        assert_eq!(
            chunks.read_cell(&id).unwrap().header.revision_ts,
            compensation_ts
        );

        let mut later = OwnedCell::new_with_id(
            schema.id,
            &id,
            data_map_value!(id: 1i32, value: "later".to_string()),
        );
        let later_header = chunks.update_cell(&mut later).unwrap();
        undo.rollback_incomplete_transactions(HashMap::from([(tid, vec![entry])]), &chunks)
            .unwrap();
        let retained = chunks.read_cell(&id).unwrap().to_owned();
        assert_eq!(retained.header.revision_ts, later_header.revision_ts);
        assert_eq!(retained.data, later.data);
    }

    #[test]
    fn restart_compensation_uses_owned_payload_after_source_wal_is_removed() {
        use crate::ram::schema::{Field, Schema};
        use crate::ram::segs::SEGMENT_SIZE;
        use dovahkiin::types::Type;

        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let undo_dir = temp_dir.path().join("undo");
        let raft_dir = temp_dir.path().join("raft");
        std::fs::create_dir_all(&raft_dir).unwrap();
        let schema = Schema::new(
            "restart_owned_undo_payload",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("id", Type::I32),
                Field::new_unindexed("value", Type::String),
            ]),
            false,
            false,
        );
        let schemas = crate::ram::schema::LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let meta = Arc::new(crate::server::ServerMeta { schemas });
        let id = Id::new(0, 705);
        let tid = test_hlc(24, 1);
        let installed_revision_ts;

        {
            let chunks = Chunks::new(
                1,
                SEGMENT_SIZE * 4,
                meta.clone(),
                None,
                None,
                Some(wal_dir.to_string_lossy().into_owned()),
                None,
            );
            let mut original = OwnedCell::new_with_id(
                schema.id,
                &id,
                data_map_value!(id: 1i32, value: "owned-prior".to_string()),
            );
            chunks.write_cell(&mut original).unwrap();
            let prior_cell = original.clone();
            let chunk = chunks.locate_chunk_by_partition(id.higher);
            let source = chunk
                .locate_segment(chunks.address_of(&id))
                .expect("prior source segment");
            let source_wal = chunk
                .file_manager
                .wal_path(chunk.id, source.id, source.seq_id)
                .expect("prior source WAL");
            source.append_header.store(source.bound, Ordering::Release);

            let mut failed = OwnedCell::new_with_id(
                schema.id,
                &id,
                data_map_value!(id: 1i32, value: "failed-update".to_string()),
            );
            installed_revision_ts = chunks
                .next_revision_ts(prior_cell.header.revision_ts)
                .unwrap();
            let installed = chunks
                .update_cell_at_revision(
                    &mut failed,
                    crate::ram::cell::RevisionWrite::committed(installed_revision_ts),
                )
                .unwrap();
            chunks.force_sync_installed_revisions([&installed]).unwrap();
            let undo = UndoLogger::new(undo_dir.to_string_lossy().into_owned()).unwrap();
            undo.write_undo_entry(UndoLogEntry::new_restore(
                tid,
                id,
                UndoOpType::Update,
                installed_revision_ts,
                prior_cell,
            ))
            .unwrap();

            assert!(chunk.remove_segment(source.id).unwrap());
            source.mem_drop(chunk);
            assert!(
                !std::path::Path::new(&source_wal).exists(),
                "the old physical source must be unavailable before restart"
            );
        }

        let (recovered, recovery) = Chunks::recover_with_clock(
            1,
            SEGMENT_SIZE * 4,
            meta,
            None,
            None,
            Some(wal_dir.to_string_lossy().into_owned()),
            None,
            Some(raft_dir.to_string_lossy().into_owned()),
            Arc::new(bifrost::hlc::HlcSource::new(0)),
            300_000,
        )
        .unwrap();
        recovered
            .revision_clock()
            .try_observe(test_hlc(recovery.max_revision_ts, 0))
            .unwrap();
        assert_eq!(
            recovered.current_revision_ts(&id),
            Some(installed_revision_ts)
        );
        let undo = UndoLogger::new(undo_dir.to_string_lossy().into_owned()).unwrap();
        let entries = undo.recover().unwrap();
        undo.rollback_incomplete_transactions(entries, &recovered)
            .unwrap();

        let restored = recovered.read_cell(&id).unwrap().to_owned();
        assert_eq!(restored.data["value"].string().unwrap(), "owned-prior");
        assert!(restored.header.revision_ts > installed_revision_ts);
    }

    #[test]
    fn recovery_rejects_prior_source_with_same_hash_and_revision_but_wrong_partition() {
        let temp_dir = TempDir::new().unwrap();
        let (schema, chunks) = compensation_test_chunks("recovery_full_id_validation");
        let wrong_source_id = Id::new(2, 704);
        let target_id = Id::new(4, 704);
        let prior_revision_ts = 100;
        let installed_revision_ts = 200;

        let mut wrong_source = OwnedCell::new_with_id(
            schema.id,
            &wrong_source_id,
            data_map_value!(id: 1i32, value: "wrong-partition".to_string()),
        );
        chunks
            .write_cell_at_revision(
                &mut wrong_source,
                crate::ram::cell::RevisionWrite::committed(prior_revision_ts),
            )
            .unwrap();
        chunks
            .remove_cell_at_revision(
                &wrong_source_id,
                crate::ram::cell::RevisionWrite::committed(150),
            )
            .unwrap();

        let mut original = OwnedCell::new_with_id(
            schema.id,
            &target_id,
            data_map_value!(id: 2i32, value: "target-original".to_string()),
        );
        chunks
            .write_cell_at_revision(
                &mut original,
                crate::ram::cell::RevisionWrite::committed(prior_revision_ts),
            )
            .unwrap();
        let mut failed = OwnedCell::new_with_id(
            schema.id,
            &target_id,
            data_map_value!(id: 3i32, value: "target-failed".to_string()),
        );
        chunks
            .update_cell_at_revision(
                &mut failed,
                crate::ram::cell::RevisionWrite::committed(installed_revision_ts),
            )
            .unwrap();

        let tid = test_hlc(23, 1);
        let entry = UndoLogEntry::new_restore(
            tid,
            target_id,
            UndoOpType::Update,
            installed_revision_ts,
            wrong_source.clone(),
        );
        let undo =
            UndoLogger::new(temp_dir.path().join("undo").to_string_lossy().into_owned()).unwrap();

        let error = undo
            .rollback_incomplete_transactions(HashMap::from([(tid, vec![entry])]), &chunks)
            .expect_err("the wrong partition must make the restore source invalid");
        assert!(
            error.to_string().contains("partition")
                || error.to_string().contains("identity")
                || error.to_string().contains("restore")
        );
        let retained = chunks.read_cell(&target_id).unwrap().to_owned();
        assert_eq!(retained.header.revision_ts, installed_revision_ts);
        assert_eq!(retained.data, failed.data);
        assert_eq!(
            chunks
                .locate_chunk_by_partition(target_id.higher)
                .history
                .current(&target_id)
                .unwrap()
                .load()
                .0,
            crate::ram::history::RevisionState::CommittedPresent,
            "source validation must happen before invalidating the failed installed head"
        );
    }

    #[test]
    fn recovery_compensates_delete_with_newer_content_idempotently() {
        let temp_dir = TempDir::new().unwrap();
        let (schema, chunks) = compensation_test_chunks("recovery_delete_compensation");
        let id = Id::new(0, 703);
        let mut original = OwnedCell::new_with_id(
            schema.id,
            &id,
            data_map_value!(id: 1i32, value: "original".to_string()),
        );
        let prior_revision_ts = chunks.write_cell(&mut original).unwrap().revision_ts;
        let prior_cell = original.clone();
        chunks.remove_cell(&id).unwrap();
        let installed_revision_ts = chunks.current_revision_ts(&id).unwrap();
        let tid = test_hlc(22, 1);
        let entry = UndoLogEntry::new_restore(
            tid,
            id,
            UndoOpType::Remove,
            installed_revision_ts,
            prior_cell,
        );
        let undo =
            UndoLogger::new(temp_dir.path().join("undo").to_string_lossy().into_owned()).unwrap();

        undo.rollback_incomplete_transactions(HashMap::from([(tid, vec![entry.clone()])]), &chunks)
            .unwrap();
        let compensated = chunks.read_cell(&id).unwrap().to_owned();
        assert_eq!(compensated.data, original.data);
        assert!(compensated.header.revision_ts > installed_revision_ts);
        let compensation_ts = compensated.header.revision_ts;

        undo.rollback_incomplete_transactions(HashMap::from([(tid, vec![entry])]), &chunks)
            .unwrap();
        assert_eq!(
            chunks.read_cell(&id).unwrap().header.revision_ts,
            compensation_ts
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

        let server_addr = String::from("127.0.0.1:5310"); // Unique port for this test
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE * 4,
                db_size: SEGMENT_SIZE * 4,
                history_retention_ms: 300_000,
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
        let abort_failure =
            transactions::data_site::install_abort_cannot_end_for_cell(txn_id, cell_id);
        assert_eq!(
            txn.abort(txn_id).await.unwrap().unwrap(),
            AbortResult::CheckFailed(CheckError::CannotEnd)
        );
        drop(abort_failure);

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
                history_retention_ms: 300_000,
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

        let server_addr = String::from("127.0.0.1:5311"); // Unique port for this test

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
                history_retention_ms: 300_000,
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
        let abort_failure =
            transactions::data_site::install_abort_cannot_end_for_cell(txn_id, cell_id);
        assert_eq!(
            txn.abort(txn_id).await.unwrap().unwrap(),
            AbortResult::CheckFailed(CheckError::CannotEnd)
        );
        drop(abort_failure);

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
                history_retention_ms: 300_000,
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

        let server_addr = String::from("127.0.0.1:5312"); // Unique port for this test

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
                history_retention_ms: 300_000,
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
        let abort_failure =
            transactions::data_site::install_abort_cannot_end_for_cell(txn_id, cell_id);
        assert_eq!(
            txn.abort(txn_id).await.unwrap().unwrap(),
            AbortResult::CheckFailed(CheckError::CannotEnd)
        );
        drop(abort_failure);

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
                history_retention_ms: 300_000,
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

        let server_addr = String::from("127.0.0.1:5320"); // Unique port for this test
                                                          // Use unique group name to avoid conflicts with other tests
        let group_name = "test_e2e_txn_committed_no_rollback";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE * 4,
                db_size: SEGMENT_SIZE * 4,
                history_retention_ms: 300_000,
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
        server
            .current_database()
            .undo_log()
            .unwrap()
            .fail_next_commit_marker_for_test();
        assert_eq!(
            txn.commit(txn_id.clone()).await.unwrap().unwrap(),
            EndResult::CheckFailed(CheckError::CannotEnd),
            "crash fixture must stop after the durable coordinator decision but before participant completion"
        );
        assert_eq!(
            server
                .current_database()
                .txn_manager()
                .unwrap()
                .transaction_count(),
            1,
            "the unresolved durable commit must remain owned by the coordinator"
        );
        assert!(matches!(
            server
                .current_database()
                .undo_log()
                .unwrap()
                .coordinator_decision(&txn_id)
                .unwrap(),
            Some(TxnResolution::Commit(_))
        ));

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
                history_retention_ms: 300_000,
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

        // Startup resolves the incomplete participant from the durable local
        // coordinator record and must not compensate its installed output.
        let read_cell = server2.chunks().read_cell(&cell_id).unwrap();
        assert_eq!(
            read_cell.data["name"].string().unwrap(),
            "committed_name",
            "Committed transaction should persist through recovery"
        );
    }
}
