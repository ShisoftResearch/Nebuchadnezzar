use crate::ram::cell::OwnedCell;
use crate::ram::chunk::Chunks;
use crate::ram::durable_fs;
use crate::ram::types::Id;
use bifrost::hlc::Hlc;
use log::{debug, error, info};
use parking_lot::{Mutex, RwLock};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{File, OpenOptions};
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
const ENTRY_TYPE_COORDINATOR_COMPLETION: u8 = 8;
const ENTRY_TYPE_PARTICIPANT_RETIREMENT: u8 = 9;
const ENTRY_TYPE_COMPACTION_SNAPSHOT: u8 = 10;
const ENTRY_TYPE_COORDINATOR_DISPATCH_INTENT: u8 = 11;
const UNDO_FIXED_PAYLOAD_LEN: usize = 16 + 1 + 8 + 4;
const MAX_UNDO_PRIOR_CELL_LEN: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct CoordinatorDecisionRecord {
    pub resolution: TxnResolution,
    pub participants: BTreeSet<u64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct CoordinatorCompletionRecord {
    pub resolution: TxnResolution,
    pub participants: BTreeSet<u64>,
    pub expires_at_ms: i64,
    /// Participants whose durable retirement-prepared response the
    /// coordinator has durably acknowledged.
    pub retired_participants: BTreeSet<u64>,
    /// Participants that acknowledged the coordinator's finalize message.
    pub finalized_participants: BTreeSet<u64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum CoordinatorStatus {
    Decided(CoordinatorDecisionRecord),
    Completed(CoordinatorCompletionRecord),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ParticipantRetirementRecord {
    pub outcome: TxnState,
    pub expires_at_ms: i64,
    /// False means the participant must retain this per-transaction proof for
    /// idempotent retire retries. True means the coordinator durably recorded
    /// the prepare acknowledgement and the proof may expire at the deadline.
    pub finalized: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CompactionSnapshotRecord {
    covered_through_seq: u64,
}

#[derive(Clone, Default)]
struct CanonicalLogState {
    participant_undo: HashMap<TxnId, Vec<UndoLogEntry>>,
    participant_undo_records: HashMap<TxnId, BTreeSet<Vec<u8>>>,
    participant_completion: HashMap<TxnId, TxnState>,
    participant_retirement: HashMap<TxnId, ParticipantRetirementRecord>,
    coordinator_status: HashMap<TxnId, CoordinatorStatus>,
    incomplete_retirement_members: BTreeSet<TxnId>,
    incomplete_retirement_cursor: Option<TxnId>,
    abort_cleanup_members: BTreeSet<TxnId>,
    abort_cleanup_cursor: Option<TxnId>,
}

impl CanonicalLogState {
    fn apply_undo(&mut self, entry: UndoLogEntry, record_bytes: Vec<u8>) {
        let txn_id = entry.txn_id;
        self.participant_completion.remove(&txn_id);
        self.participant_retirement.remove(&txn_id);
        if self
            .participant_undo_records
            .entry(txn_id)
            .or_default()
            .insert(record_bytes)
        {
            self.participant_undo.entry(txn_id).or_default().push(entry);
        }
    }

    fn apply_participant_completion(&mut self, txn_id: TxnId, outcome: TxnState) {
        self.participant_undo.remove(&txn_id);
        self.participant_undo_records.remove(&txn_id);
        self.participant_retirement.remove(&txn_id);
        self.participant_completion.insert(txn_id, outcome);
    }

    fn apply_participant_retirement(&mut self, txn_id: TxnId, record: ParticipantRetirementRecord) {
        self.participant_undo.remove(&txn_id);
        self.participant_undo_records.remove(&txn_id);
        self.participant_completion.remove(&txn_id);
        self.participant_retirement.insert(txn_id, record);
    }

    fn apply_coordinator_decision(&mut self, txn_id: TxnId, record: CoordinatorDecisionRecord) {
        if !matches!(
            self.coordinator_status.get(&txn_id),
            Some(CoordinatorStatus::Completed(_))
        ) {
            if matches!(
                record.resolution,
                TxnResolution::InProgress | TxnResolution::Abort
            ) {
                self.abort_cleanup_members.insert(txn_id);
            } else {
                self.abort_cleanup_members.remove(&txn_id);
            }
            self.coordinator_status
                .insert(txn_id, CoordinatorStatus::Decided(record));
        }
    }

    fn apply_coordinator_completion(&mut self, txn_id: TxnId, record: CoordinatorCompletionRecord) {
        if record.finalized_participants != record.participants {
            self.incomplete_retirement_members.insert(txn_id);
        } else {
            self.incomplete_retirement_members.remove(&txn_id);
            if self.incomplete_retirement_members.is_empty() {
                self.incomplete_retirement_cursor = None;
            }
        }
        self.abort_cleanup_members.remove(&txn_id);
        self.coordinator_status
            .insert(txn_id, CoordinatorStatus::Completed(record));
    }

    fn rebuild_incomplete_retirements(&mut self) {
        self.incomplete_retirement_members.clear();
        self.incomplete_retirement_cursor = None;
        for (tid, status) in &self.coordinator_status {
            if matches!(
                status,
                CoordinatorStatus::Completed(record)
                    if record.finalized_participants != record.participants
            ) {
                self.incomplete_retirement_members.insert(*tid);
            }
        }
        self.abort_cleanup_members.clear();
        self.abort_cleanup_cursor = None;
        for (tid, status) in &self.coordinator_status {
            if matches!(
                status,
                CoordinatorStatus::Decided(CoordinatorDecisionRecord {
                    resolution: TxnResolution::InProgress | TxnResolution::Abort,
                    ..
                })
            ) {
                self.abort_cleanup_members.insert(*tid);
            }
        }
    }

    fn retirement_candidates(&mut self, limit: usize) -> Vec<TxnId> {
        if self.incomplete_retirement_members.is_empty() || limit == 0 {
            return Vec::new();
        }
        let mut candidates =
            Vec::with_capacity(limit.min(self.incomplete_retirement_members.len()));
        if let Some(after) = self.incomplete_retirement_cursor {
            candidates.extend(
                self.incomplete_retirement_members
                    .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                    .take(limit)
                    .copied(),
            );
            if candidates.len() < limit {
                candidates.extend(
                    self.incomplete_retirement_members
                        .range(..=after)
                        .take(limit - candidates.len())
                        .copied(),
                );
            }
        } else {
            candidates.extend(
                self.incomplete_retirement_members
                    .iter()
                    .take(limit)
                    .copied(),
            );
        }
        if let Some(last) = candidates.last() {
            self.incomplete_retirement_cursor = Some(*last);
        }
        candidates
    }

    #[cfg(test)]
    fn retirement_discovery_storage_len(&self) -> usize {
        self.incomplete_retirement_members.len()
    }

    fn abort_cleanup_candidates(&mut self, limit: usize) -> Vec<TxnId> {
        if self.abort_cleanup_members.is_empty() || limit == 0 {
            return Vec::new();
        }
        let mut candidates = Vec::with_capacity(limit.min(self.abort_cleanup_members.len()));
        if let Some(after) = self.abort_cleanup_cursor {
            candidates.extend(
                self.abort_cleanup_members
                    .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                    .take(limit)
                    .copied(),
            );
            if candidates.len() < limit {
                candidates.extend(
                    self.abort_cleanup_members
                        .range(..=after)
                        .take(limit - candidates.len())
                        .copied(),
                );
            }
        } else {
            candidates.extend(self.abort_cleanup_members.iter().take(limit).copied());
        }
        if let Some(last) = candidates.last() {
            self.abort_cleanup_cursor = Some(*last);
        }
        candidates
    }

    fn retained_at(&self, now_ms: i64) -> Self {
        let mut retained = self.clone();
        retained
            .participant_retirement
            .retain(|_, record| !record.finalized || now_ms < record.expires_at_ms);
        retained
            .coordinator_status
            .retain(|_, status| match status {
                CoordinatorStatus::Decided(_) => true,
                CoordinatorStatus::Completed(record) => {
                    record.finalized_participants != record.participants
                        || now_ms < record.expires_at_ms
                }
            });
        retained.rebuild_incomplete_retirements();
        retained
    }
}

struct CanonicalLogScan {
    state: CanonicalLogState,
    highest_barrier: Option<(u64, u64)>,
    newest_tail_repair: Option<(PathBuf, u64)>,
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

fn remove_abandoned_compacting_files(log_dir_path: &Path) -> io::Result<()> {
    let mut first_error = None;
    for entry in std::fs::read_dir(log_dir_path)? {
        let path = entry?.path();
        let is_compacting = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("undo-") && name.ends_with(".nlog.compacting"));
        if is_compacting {
            if let Err(error) = durable_fs::remove_file(&path) {
                first_error.get_or_insert_with(|| {
                    io::Error::new(
                        error.kind(),
                        format!(
                            "cannot durably remove abandoned compaction file {}: {}",
                            path.display(),
                            error
                        ),
                    )
                });
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(test)]
static PERSISTENT_LOG_REMOVE_FAILURES: std::sync::OnceLock<Mutex<HashSet<PathBuf>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
fn persistent_log_remove_failures() -> &'static Mutex<HashSet<PathBuf>> {
    PERSISTENT_LOG_REMOVE_FAILURES.get_or_init(|| Mutex::new(HashSet::new()))
}

#[cfg(test)]
struct PersistentLogRemoveFailureHandle {
    path: PathBuf,
}

#[cfg(test)]
impl Drop for PersistentLogRemoveFailureHandle {
    fn drop(&mut self) {
        persistent_log_remove_failures().lock().remove(&self.path);
    }
}

#[cfg(test)]
fn install_persistent_log_remove_failure(path: PathBuf) -> PersistentLogRemoveFailureHandle {
    persistent_log_remove_failures().lock().insert(path.clone());
    PersistentLogRemoveFailureHandle { path }
}

fn remove_log_file(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if persistent_log_remove_failures().lock().contains(path) {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "injected persistent log remove failure for {}",
                path.display()
            ),
        ));
    }
    durable_fs::remove_file(path)
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

fn encode_json_payload_record<T: Serialize>(
    entry_type: u8,
    txn_id: &TxnId,
    payload: &T,
) -> io::Result<Vec<u8>> {
    let txn_id_bytes =
        serde_json::to_vec(txn_id).map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
    let payload_bytes =
        serde_json::to_vec(payload).map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
    let txn_id_len = u32::try_from(txn_id_bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "transaction id too large"))?;
    let payload_len = u32::try_from(payload_bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "record payload too large"))?;

    let mut bytes = Vec::with_capacity(1 + 4 + txn_id_bytes.len() + 4 + payload_bytes.len());
    bytes.push(entry_type);
    bytes.extend_from_slice(&txn_id_len.to_le_bytes());
    bytes.extend_from_slice(&txn_id_bytes);
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload_bytes);
    Ok(bytes)
}

fn decode_json_payload_record<T: DeserializeOwned>(
    bytes: &[u8],
    offset: usize,
    txn_id_len: usize,
) -> io::Result<(T, usize)> {
    let payload_len_offset = offset
        .checked_add(5)
        .and_then(|value| value.checked_add(txn_id_len))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "record size overflow"))?;
    if bytes.len() < payload_len_offset + 4 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "incomplete record payload length",
        ));
    }
    let payload_len = u32::from_le_bytes([
        bytes[payload_len_offset],
        bytes[payload_len_offset + 1],
        bytes[payload_len_offset + 2],
        bytes[payload_len_offset + 3],
    ]) as usize;
    let payload_offset = payload_len_offset + 4;
    let payload_end = payload_offset
        .checked_add(payload_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "record size overflow"))?;
    if bytes.len() < payload_end {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "incomplete record payload",
        ));
    }
    let payload = serde_json::from_slice(&bytes[payload_offset..payload_end])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok((payload, payload_end - offset))
}

fn encode_compaction_snapshot_record(covered_through_seq: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(13);
    bytes.push(ENTRY_TYPE_COMPACTION_SNAPSHOT);
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&covered_through_seq.to_le_bytes());
    bytes
}

fn encode_participant_completion_record(txn_id: &TxnId, outcome: TxnState) -> io::Result<Vec<u8>> {
    validate_participant_outcome(outcome)?;
    let txn_id_bytes =
        serde_json::to_vec(txn_id).map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
    let txn_id_len = u32::try_from(txn_id_bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "transaction id too large"))?;
    let mut bytes = Vec::with_capacity(5 + txn_id_bytes.len());
    bytes.push(match outcome {
        TxnState::Committed => ENTRY_TYPE_COMMIT,
        TxnState::Aborted => ENTRY_TYPE_ABORT,
        _ => unreachable!(),
    });
    bytes.extend_from_slice(&txn_id_len.to_le_bytes());
    bytes.extend_from_slice(&txn_id_bytes);
    Ok(bytes)
}

fn encode_coordinator_decision_record(
    txn_id: &TxnId,
    record: &CoordinatorDecisionRecord,
) -> io::Result<Vec<u8>> {
    if record.resolution != TxnResolution::InProgress {
        validate_final_resolution(record.resolution)?;
    }
    let txn_id_bytes =
        serde_json::to_vec(txn_id).map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
    let participants_bytes = serde_json::to_vec(&record.participants)
        .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
    let txn_id_len = u32::try_from(txn_id_bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "transaction id too large"))?;
    let participants_len = u32::try_from(participants_bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "participant list too large"))?;
    let mut bytes = Vec::new();
    match record.resolution {
        TxnResolution::Commit(commit_hlc) => {
            let commit_hlc_bytes = serde_json::to_vec(&commit_hlc)
                .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
            let commit_hlc_len = u32::try_from(commit_hlc_bytes.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "commit timestamp too large")
            })?;
            bytes.reserve(
                1 + 4
                    + txn_id_bytes.len()
                    + 4
                    + commit_hlc_bytes.len()
                    + 4
                    + participants_bytes.len(),
            );
            bytes.push(ENTRY_TYPE_COORDINATOR_COMMIT);
            bytes.extend_from_slice(&txn_id_len.to_le_bytes());
            bytes.extend_from_slice(&txn_id_bytes);
            bytes.extend_from_slice(&commit_hlc_len.to_le_bytes());
            bytes.extend_from_slice(&commit_hlc_bytes);
        }
        TxnResolution::Abort => {
            bytes.reserve(1 + 4 + txn_id_bytes.len() + 4 + participants_bytes.len());
            bytes.push(ENTRY_TYPE_COORDINATOR_ABORT);
            bytes.extend_from_slice(&txn_id_len.to_le_bytes());
            bytes.extend_from_slice(&txn_id_bytes);
        }
        TxnResolution::InProgress => {
            return encode_json_payload_record(
                ENTRY_TYPE_COORDINATOR_DISPATCH_INTENT,
                txn_id,
                record,
            );
        }
        TxnResolution::Unknown => unreachable!(),
    }
    bytes.extend_from_slice(&participants_len.to_le_bytes());
    bytes.extend_from_slice(&participants_bytes);
    Ok(bytes)
}

fn decode_compaction_snapshot_record(
    bytes: &[u8],
    offset: usize,
    txn_id_len: usize,
) -> io::Result<(CompactionSnapshotRecord, usize)> {
    if txn_id_len != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "compaction snapshot unexpectedly contains a transaction id",
        ));
    }
    if bytes.len() < offset + 13 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "incomplete compaction snapshot",
        ));
    }
    let covered_through_seq = u64::from_le_bytes([
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
        bytes[offset + 8],
        bytes[offset + 9],
        bytes[offset + 10],
        bytes[offset + 11],
        bytes[offset + 12],
    ]);
    Ok((
        CompactionSnapshotRecord {
            covered_through_seq,
        },
        13,
    ))
}

fn validate_final_resolution(resolution: TxnResolution) -> io::Result<()> {
    if matches!(resolution, TxnResolution::Commit(_) | TxnResolution::Abort) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "durable completion requires a final Commit or Abort decision",
        ))
    }
}

fn validate_participant_outcome(outcome: TxnState) -> io::Result<()> {
    if matches!(outcome, TxnState::Committed | TxnState::Aborted) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "participant retirement requires a committed or aborted outcome",
        ))
    }
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

fn read_canonical_log_state(log_files: &[(u64, PathBuf)]) -> io::Result<CanonicalLogScan> {
    let mut state = CanonicalLogState::default();
    let mut highest_barrier = None;
    let mut newest_tail_repair = None;
    for (file_index, (seq, path)) in log_files.iter().enumerate() {
        let is_newest_generation = file_index + 1 == log_files.len();
        let mut buffer = Vec::new();
        File::open(path)?.read_to_end(&mut buffer)?;
        let mut offset = 0;
        let mut saw_snapshot = false;
        'records: while offset < buffer.len() {
            if buffer.len() < offset + 5 {
                if is_newest_generation
                    && buffer.get(offset).copied() != Some(ENTRY_TYPE_COMPACTION_SNAPSHOT)
                {
                    newest_tail_repair = Some((path.clone(), offset as u64));
                    break;
                }
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
                if is_newest_generation && entry_type != ENTRY_TYPE_COMPACTION_SNAPSHOT {
                    newest_tail_repair = Some((path.clone(), offset as u64));
                    break;
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "cannot decode undo log {} at byte offset {}: incomplete transaction id",
                        path.display(),
                        offset
                    ),
                ));
            }
            if entry_type == ENTRY_TYPE_COMPACTION_SNAPSHOT {
                if offset != 0 || saw_snapshot {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "compaction snapshot in sequence {} must be the first and only barrier record",
                            seq
                        ),
                    ));
                }
                let (snapshot, size) =
                    decode_compaction_snapshot_record(&buffer, offset, txn_id_len)?;
                if snapshot.covered_through_seq.checked_add(1) != Some(*seq) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "compaction snapshot in sequence {} must immediately follow covered sequence {}",
                            seq, snapshot.covered_through_seq
                        ),
                    ));
                }
                if let Some((previous_snapshot_seq, previous_covered_through)) = highest_barrier {
                    if snapshot.covered_through_seq < previous_snapshot_seq
                        || snapshot.covered_through_seq <= previous_covered_through
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "compaction snapshot in sequence {} does not advance beyond prior barrier {} covering {}",
                                seq, previous_snapshot_seq, previous_covered_through
                            ),
                        ));
                    }
                }
                state = CanonicalLogState::default();
                highest_barrier = Some((*seq, snapshot.covered_through_seq));
                saw_snapshot = true;
                offset += size;
                continue;
            }

            let txn_id = decode_txn_id(&buffer[offset + 5..offset + 5 + txn_id_len])?;
            macro_rules! decode_active_record {
                ($decode:expr) => {
                    match $decode {
                        Ok(record) => record,
                        Err(error)
                            if is_newest_generation
                                && error.kind() == io::ErrorKind::UnexpectedEof =>
                        {
                            newest_tail_repair = Some((path.clone(), offset as u64));
                            break 'records;
                        }
                        Err(error) => return Err(error),
                    }
                };
            }
            match entry_type {
                ENTRY_TYPE_UNDO => {
                    let (entry, size) =
                        decode_active_record!(UndoLogEntry::from_bytes(&buffer[offset..]));
                    let record_bytes = buffer[offset..offset + size].to_vec();
                    // Preserve chronological restart semantics even if a
                    // transaction identifier is reused: a later undo record
                    // supersedes older participant completion evidence.
                    state.participant_completion.remove(&txn_id);
                    state.participant_retirement.remove(&txn_id);
                    if state
                        .participant_undo_records
                        .entry(txn_id)
                        .or_default()
                        .insert(record_bytes)
                    {
                        state
                            .participant_undo
                            .entry(txn_id)
                            .or_default()
                            .push(entry);
                    }
                    offset += size;
                }
                ENTRY_TYPE_COMMIT | ENTRY_TYPE_ABORT => {
                    state.participant_undo.remove(&txn_id);
                    state.participant_undo_records.remove(&txn_id);
                    state.participant_retirement.remove(&txn_id);
                    state.participant_completion.insert(
                        txn_id,
                        if entry_type == ENTRY_TYPE_COMMIT {
                            TxnState::Committed
                        } else {
                            TxnState::Aborted
                        },
                    );
                    offset += 5 + txn_id_len;
                }
                ENTRY_TYPE_COORDINATOR_COMMIT => {
                    let (commit_hlc, participants, size) = decode_active_record!(
                        coordinator_commit_record_size(&buffer, offset, txn_id_len)
                    );
                    if !matches!(
                        state.coordinator_status.get(&txn_id),
                        Some(CoordinatorStatus::Completed(_))
                    ) {
                        state.coordinator_status.insert(
                            txn_id,
                            CoordinatorStatus::Decided(CoordinatorDecisionRecord {
                                resolution: TxnResolution::Commit(commit_hlc),
                                participants,
                            }),
                        );
                    }
                    offset += size;
                }
                ENTRY_TYPE_COORDINATOR_ABORT => {
                    let (participants, size) = decode_active_record!(
                        coordinator_abort_record_size(&buffer, offset, txn_id_len)
                    );
                    if !matches!(
                        state.coordinator_status.get(&txn_id),
                        Some(CoordinatorStatus::Completed(_))
                    ) {
                        state.coordinator_status.insert(
                            txn_id,
                            CoordinatorStatus::Decided(CoordinatorDecisionRecord {
                                resolution: TxnResolution::Abort,
                                participants,
                            }),
                        );
                    }
                    offset += size;
                }
                ENTRY_TYPE_COORDINATOR_DISPATCH_INTENT => {
                    let (record, size): (CoordinatorDecisionRecord, _) = decode_active_record!(
                        decode_json_payload_record(&buffer, offset, txn_id_len)
                    );
                    if record.resolution != TxnResolution::InProgress {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "coordinator dispatch intent must be InProgress",
                        ));
                    }
                    if !matches!(
                        state.coordinator_status.get(&txn_id),
                        Some(CoordinatorStatus::Completed(_))
                    ) {
                        state
                            .coordinator_status
                            .insert(txn_id, CoordinatorStatus::Decided(record));
                    }
                    offset += size;
                }
                ENTRY_TYPE_COORDINATOR_COMPLETION => {
                    let (record, size): (CoordinatorCompletionRecord, _) = decode_active_record!(
                        decode_json_payload_record(&buffer, offset, txn_id_len)
                    );
                    validate_final_resolution(record.resolution)?;
                    if !record.retired_participants.is_subset(&record.participants)
                        || !record
                            .finalized_participants
                            .is_subset(&record.retired_participants)
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid coordinator completion acknowledgement sets",
                        ));
                    }
                    state
                        .coordinator_status
                        .insert(txn_id, CoordinatorStatus::Completed(record));
                    offset += size;
                }
                ENTRY_TYPE_PARTICIPANT_RETIREMENT => {
                    let (record, size): (ParticipantRetirementRecord, _) = decode_active_record!(
                        decode_json_payload_record(&buffer, offset, txn_id_len)
                    );
                    validate_participant_outcome(record.outcome)?;
                    state.participant_undo.remove(&txn_id);
                    state.participant_undo_records.remove(&txn_id);
                    state.participant_completion.remove(&txn_id);
                    state.participant_retirement.insert(txn_id, record);
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
    state.rebuild_incomplete_retirements();
    Ok(CanonicalLogScan {
        state,
        highest_barrier,
        newest_tail_repair,
    })
}

#[cfg(test)]
struct ScanPauseState {
    entered: std::sync::Mutex<bool>,
    entered_cv: std::sync::Condvar,
    released: std::sync::Mutex<bool>,
    released_cv: std::sync::Condvar,
}

#[cfg(test)]
impl ScanPauseState {
    fn new() -> Self {
        Self {
            entered: std::sync::Mutex::new(false),
            entered_cv: std::sync::Condvar::new(),
            released: std::sync::Mutex::new(false),
            released_cv: std::sync::Condvar::new(),
        }
    }

    fn wait_until_entered(&self) {
        let mut entered = self.entered.lock().unwrap();
        while !*entered {
            entered = self.entered_cv.wait(entered).unwrap();
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.released_cv.notify_all();
    }
}

/// Manages the undo log for transactions
pub struct UndoLogger {
    /// Path to the undo log directory
    log_dir: String,
    /// Current active log file
    log_file: Mutex<Option<BufWriter<File>>>,
    /// Keeps full-generation scanners on one stable file set while compaction
    /// publishes and removes a generation.
    log_generation: RwLock<()>,
    /// Current log file name
    log_file_name: Mutex<Option<String>>,
    /// A snapshot rename completed, but publishing its directory entry has not
    /// yet been confirmed. The snapshot is already the only safe writer;
    /// appends stay fail-closed until the parent directory sync succeeds.
    publication_sync_pending: Mutex<Option<PathBuf>>,
    /// Log file sequence number
    log_seq: AtomicU64,
    /// Set of active (incomplete) transaction IDs for trimming
    active_txns: Mutex<HashSet<TxnId>>,
    /// Canonical targeted lookup index, rebuilt once at startup and updated
    /// only after the corresponding record is durably appended.
    canonical_state: Mutex<CanonicalLogState>,
    /// Covered old sequences whose durable unlink has not yet converged.
    /// Compaction must clear this before publishing another snapshot.
    pending_cleanup_through: Mutex<Option<u64>>,
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
    fail_next_coordinator_completion: AtomicBool,
    #[cfg(test)]
    fail_next_coordinator_completions_read: AtomicBool,
    #[cfg(test)]
    rotate_before_next_record: AtomicBool,
    #[cfg(test)]
    pause_before_snapshot_adoption: Mutex<Option<Arc<ScanPauseState>>>,
    #[cfg(test)]
    coordinator_completion_writes: AtomicU64,
    #[cfg(test)]
    full_log_scans: AtomicU64,
}

impl UndoLogger {
    /// Create a new undo log manager
    pub fn new(log_dir: String) -> io::Result<Arc<Self>> {
        let log_dir_path = Path::new(&log_dir);
        durable_fs::ensure_directory(log_dir_path)?;
        remove_abandoned_compacting_files(log_dir_path)?;
        let existing_logs = collect_undo_log_paths(
            log_dir_path,
            std::fs::read_dir(log_dir_path)?.map(|entry| entry.map(|entry| entry.path())),
        )?;
        let scan = read_canonical_log_state(&existing_logs)?;
        if let Some((_, newest_path)) = existing_logs.last() {
            let newest_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(newest_path)?;
            if let Some((repair_path, valid_len)) = scan.newest_tail_repair.as_ref() {
                debug_assert_eq!(repair_path, newest_path);
                newest_file.set_len(*valid_len)?;
            }
            // A prior process may have returned an error after flushing but
            // before syncing its final record. Make every complete record
            // still visible after restart durable before recovery can trust
            // it, and durably publish any newest-tail repair for later
            // restarts.
            durable_fs::sync_file(&newest_file, newest_path)?;
        }
        let mut pending_cleanup_through = None;
        if let Some((_snapshot_seq, covered_through_seq)) = scan.highest_barrier {
            for (seq, path) in &existing_logs {
                if *seq <= covered_through_seq {
                    if let Err(error) = remove_log_file(path) {
                        error!(
                            "Could not finish covered undo-log cleanup for {} at startup: {:?}",
                            path.display(),
                            error
                        );
                        pending_cleanup_through = Some(covered_through_seq);
                    }
                }
            }
        }
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
        let pending_active_path = pending_cleanup_through
            .and_then(|_| existing_logs.last().map(|(_, path)| path.clone()));

        let log = Arc::new(Self {
            log_dir: log_dir.clone(),
            log_file: Mutex::new(None),
            log_generation: RwLock::new(()),
            log_file_name: Mutex::new(None),
            publication_sync_pending: Mutex::new(None),
            log_seq: AtomicU64::new(next_log_seq),
            active_txns: Mutex::new(scan.state.participant_undo.keys().copied().collect()),
            canonical_state: Mutex::new(scan.state),
            pending_cleanup_through: Mutex::new(pending_cleanup_through),
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
            fail_next_coordinator_completion: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_coordinator_completions_read: AtomicBool::new(false),
            #[cfg(test)]
            rotate_before_next_record: AtomicBool::new(false),
            #[cfg(test)]
            pause_before_snapshot_adoption: Mutex::new(None),
            #[cfg(test)]
            coordinator_completion_writes: AtomicU64::new(0),
            #[cfg(test)]
            full_log_scans: AtomicU64::new(1),
        });

        // Reuse the latest authoritative generation while covered-file cleanup
        // is pending. Repeated crash/restart prefixes must not create an empty
        // active generation each time the same durable unlink keeps failing.
        if let Some(active_path) = pending_active_path {
            let file = durable_fs::open_or_create_append(&active_path)?;
            *log.log_file.lock() = Some(BufWriter::with_capacity(4096, file));
            *log.log_file_name.lock() = Some(active_path.to_string_lossy().into_owned());
        } else {
            log.rotate_log()?;
        }

        Ok(log)
    }

    /// Rotate to a new log file
    fn rotate_log(&self) -> io::Result<()> {
        // Serialize rotation through the active writer so a failed publication
        // cannot age the still-active file into the trimmer's old-log window.
        let mut log_file_guard = self.log_file.lock();
        self.finish_pending_publication()?;
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
        self.finish_pending_publication()?;
        if let Some(writer) = log_file_guard.as_mut() {
            writer.write_all(&bytes)?;
            writer.flush()?;
            writer.get_ref().sync_data()?;

            self.canonical_state.lock().apply_undo(entry.clone(), bytes);
            self.active_txns.lock().insert(entry.txn_id);
            drop(log_file_guard);

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
        self.finish_pending_publication()?;
        if let Some(writer) = log_file_guard.as_mut() {
            writer.write_all(&bytes)?;
            writer.flush()?;
            writer.get_ref().sync_data()?;

            self.canonical_state
                .lock()
                .apply_participant_completion(*txn_id, TxnState::Committed);
            self.active_txns.lock().remove(txn_id);
            drop(log_file_guard);

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
        self.finish_pending_publication()?;
        if let Some(writer) = log_file_guard.as_mut() {
            writer.write_all(&bytes)?;
            writer.flush()?;
            writer.get_ref().sync_data()?;

            self.canonical_state
                .lock()
                .apply_participant_completion(*txn_id, TxnState::Aborted);
            self.active_txns.lock().remove(txn_id);
            drop(log_file_guard);

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
        self.finish_pending_publication()?;
        let writer = log_file_guard
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Log file not initialized"))?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        writer.get_ref().sync_data()?;
        self.canonical_state.lock().apply_coordinator_decision(
            *txn_id,
            CoordinatorDecisionRecord {
                resolution: TxnResolution::Commit(commit_hlc),
                participants,
            },
        );
        Ok(())
    }

    /// Persist the exact participant set immediately before concurrent
    /// prepare/commit dispatch. A restart treats this non-final decision as
    /// Abort cleanup work until a final Commit/Abort record supersedes it.
    pub(crate) fn write_coordinator_dispatch_intent(
        &self,
        txn_id: &TxnId,
        participants: &[u64],
    ) -> io::Result<()> {
        #[cfg(test)]
        self.rotate_before_record_if_requested_for_test()?;
        let record = CoordinatorDecisionRecord {
            resolution: TxnResolution::InProgress,
            participants: participants.iter().copied().collect(),
        };
        let bytes =
            encode_json_payload_record(ENTRY_TYPE_COORDINATOR_DISPATCH_INTENT, txn_id, &record)?;
        self.write_synced_record(&bytes, |state| {
            state.apply_coordinator_decision(*txn_id, record)
        })
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
        self.finish_pending_publication()?;
        let writer = log_file_guard
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Log file not initialized"))?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        writer.get_ref().sync_data()?;
        self.canonical_state.lock().apply_coordinator_decision(
            *txn_id,
            CoordinatorDecisionRecord {
                resolution: TxnResolution::Abort,
                participants,
            },
        );
        Ok(())
    }

    /// During startup, turn this server's exact pre-dispatch intent into the
    /// final Abort decision that makes participant compensation safe.
    ///
    /// The writer lock serializes the status recheck with every decision
    /// append and compaction. Canonical state remains InProgress until the
    /// Abort record is fully synced, so a failed promotion cannot expose Abort
    /// to recovery or overwrite a concurrently durable final decision.
    pub(crate) fn promote_local_dispatch_intent_to_abort(
        &self,
        txn_id: &TxnId,
    ) -> io::Result<Option<CoordinatorDecisionRecord>> {
        let mut log_file_guard = self.log_file.lock();
        self.finish_pending_publication()?;
        let status = self
            .canonical_state
            .lock()
            .coordinator_status
            .get(txn_id)
            .cloned();
        let Some(status) = status else {
            return Ok(None);
        };
        let CoordinatorStatus::Decided(record) = status else {
            return Ok(match status {
                CoordinatorStatus::Completed(record)
                    if bifrost::utils::time::get_time() < record.expires_at_ms =>
                {
                    Some(CoordinatorDecisionRecord {
                        resolution: record.resolution,
                        participants: record.participants,
                    })
                }
                CoordinatorStatus::Completed(_) => None,
                CoordinatorStatus::Decided(_) => unreachable!(),
            });
        };
        if record.resolution != TxnResolution::InProgress {
            return Ok(Some(record));
        }

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
        let abort = CoordinatorDecisionRecord {
            resolution: TxnResolution::Abort,
            participants: record.participants,
        };
        let bytes = encode_coordinator_decision_record(txn_id, &abort)?;
        let writer = log_file_guard
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Log file not initialized"))?;
        let active_path =
            self.log_file_name.lock().clone().ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "Log file path not initialized")
            })?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        durable_fs::sync_file(writer.get_ref(), Path::new(&active_path))?;
        self.canonical_state
            .lock()
            .apply_coordinator_decision(*txn_id, abort.clone());
        Ok(Some(abort))
    }

    pub(crate) fn coordinator_recovery_decision(
        &self,
        txn_id: &TxnId,
        local_server_id: u64,
    ) -> io::Result<Option<CoordinatorDecisionRecord>> {
        if txn_id.node != local_server_id {
            return self.coordinator_decision_record(txn_id);
        }
        self.promote_local_dispatch_intent_to_abort(txn_id)
    }

    fn write_synced_record<F>(&self, bytes: &[u8], update: F) -> io::Result<()>
    where
        F: FnOnce(&mut CanonicalLogState),
    {
        let mut log_file_guard = self.log_file.lock();
        self.finish_pending_publication()?;
        let writer = log_file_guard
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Log file not initialized"))?;
        writer.write_all(bytes)?;
        writer.flush()?;
        writer.get_ref().sync_data()?;
        update(&mut self.canonical_state.lock());
        Ok(())
    }

    fn finish_pending_publication(&self) -> io::Result<()> {
        let mut pending = self.publication_sync_pending.lock();
        let Some(path) = pending.as_ref() else {
            return Ok(());
        };
        durable_fs::sync_parent(path)?;
        *pending = None;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn write_coordinator_completion(
        &self,
        txn_id: &TxnId,
        resolution: TxnResolution,
        participants: &[u64],
        expires_at_ms: i64,
    ) -> io::Result<()> {
        self.write_coordinator_completion_record(
            txn_id,
            &CoordinatorCompletionRecord {
                resolution,
                participants: participants.iter().copied().collect(),
                expires_at_ms,
                retired_participants: BTreeSet::new(),
                finalized_participants: BTreeSet::new(),
            },
        )
    }

    pub(crate) fn write_coordinator_completion_record(
        &self,
        txn_id: &TxnId,
        record: &CoordinatorCompletionRecord,
    ) -> io::Result<()> {
        #[cfg(test)]
        if self
            .fail_next_coordinator_completion
            .swap(false, Ordering::SeqCst)
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "injected coordinator completion failure",
            ));
        }
        validate_final_resolution(record.resolution)?;
        if !record.retired_participants.is_subset(&record.participants)
            || !record
                .finalized_participants
                .is_subset(&record.retired_participants)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "coordinator retirement acknowledgements are not subsets of participants",
            ));
        }
        let bytes = encode_json_payload_record(ENTRY_TYPE_COORDINATOR_COMPLETION, txn_id, record)?;
        self.write_synced_record(&bytes, |state| {
            state.apply_coordinator_completion(*txn_id, record.clone())
        })?;
        #[cfg(test)]
        self.coordinator_completion_writes
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn coordinator_completion_write_count_for_test(&self) -> u64 {
        self.coordinator_completion_writes.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_coordinator_completion_for_test(&self) {
        self.fail_next_coordinator_completion
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn write_participant_retirement(
        &self,
        txn_id: &TxnId,
        outcome: TxnState,
        expires_at_ms: i64,
    ) -> io::Result<()> {
        self.write_participant_retirement_record(
            txn_id,
            &ParticipantRetirementRecord {
                outcome,
                expires_at_ms,
                finalized: false,
            },
        )
    }

    pub(crate) fn finalize_participant_retirement(
        &self,
        txn_id: &TxnId,
        outcome: TxnState,
        expires_at_ms: i64,
    ) -> io::Result<()> {
        self.write_participant_retirement_record(
            txn_id,
            &ParticipantRetirementRecord {
                outcome,
                expires_at_ms,
                finalized: true,
            },
        )
    }

    fn write_participant_retirement_record(
        &self,
        txn_id: &TxnId,
        record: &ParticipantRetirementRecord,
    ) -> io::Result<()> {
        validate_participant_outcome(record.outcome)?;
        let bytes = encode_json_payload_record(ENTRY_TYPE_PARTICIPANT_RETIREMENT, txn_id, record)?;
        self.write_synced_record(&bytes, |state| {
            state.apply_participant_retirement(*txn_id, record.clone())
        })
    }

    pub fn coordinator_decision(&self, txn_id: &TxnId) -> io::Result<Option<TxnResolution>> {
        self.coordinator_decision_record(txn_id)
            .map(|record| record.map(|record| record.resolution))
    }

    pub(crate) fn coordinator_decision_record(
        &self,
        txn_id: &TxnId,
    ) -> io::Result<Option<CoordinatorDecisionRecord>> {
        self.coordinator_status(txn_id).map(|status| {
            status.and_then(|status| match status {
                CoordinatorStatus::Decided(record) => Some(record),
                CoordinatorStatus::Completed(record)
                    if bifrost::utils::time::get_time() < record.expires_at_ms =>
                {
                    Some(CoordinatorDecisionRecord {
                        resolution: record.resolution,
                        participants: record.participants,
                    })
                }
                CoordinatorStatus::Completed(_) => None,
            })
        })
    }

    pub(crate) fn coordinator_status(
        &self,
        txn_id: &TxnId,
    ) -> io::Result<Option<CoordinatorStatus>> {
        Ok(self
            .canonical_state
            .lock()
            .coordinator_status
            .get(txn_id)
            .cloned())
    }

    pub(crate) fn coordinator_retirement_candidates(&self, limit: usize) -> io::Result<Vec<TxnId>> {
        #[cfg(test)]
        if self
            .fail_next_coordinator_completions_read
            .swap(false, Ordering::SeqCst)
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "injected coordinator completion discovery failure",
            ));
        }
        Ok(self.canonical_state.lock().retirement_candidates(limit))
    }

    pub(crate) fn coordinator_abort_cleanup_candidates(
        &self,
        limit: usize,
    ) -> io::Result<Vec<TxnId>> {
        Ok(self.canonical_state.lock().abort_cleanup_candidates(limit))
    }

    #[cfg(test)]
    pub(crate) fn fail_next_coordinator_completions_read_for_test(&self) {
        self.fail_next_coordinator_completions_read
            .store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn full_log_scan_count_for_test(&self) -> u64 {
        self.full_log_scans.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn pause_before_snapshot_adoption_for_test(&self) -> Arc<ScanPauseState> {
        let state = Arc::new(ScanPauseState::new());
        *self.pause_before_snapshot_adoption.lock() = Some(state.clone());
        state
    }

    #[cfg(test)]
    fn pause_snapshot_adoption_if_requested_for_test(&self) {
        let Some(state) = self.pause_before_snapshot_adoption.lock().take() else {
            return;
        };
        *state.entered.lock().unwrap() = true;
        state.entered_cv.notify_all();
        let mut released = state.released.lock().unwrap();
        while !*released {
            released = state.released_cv.wait(released).unwrap();
        }
    }

    /// Read the durable outcome written by a participant's `end` operation.
    ///
    /// Coordinator decision records are deliberately ignored here: they prove
    /// only the global choice, not that this participant finished promotion and
    /// lock release.
    pub fn participant_completion(&self, txn_id: &TxnId) -> io::Result<Option<TxnState>> {
        self.participant_state(txn_id)
            .map(|(completion, retirement)| retirement.map(|record| record.outcome).or(completion))
    }

    pub(crate) fn participant_retirement(
        &self,
        txn_id: &TxnId,
    ) -> io::Result<Option<ParticipantRetirementRecord>> {
        self.participant_state(txn_id)
            .map(|(_, retirement)| retirement)
    }

    pub(crate) fn participant_completion_cache_at(
        &self,
        now_ms: i64,
    ) -> io::Result<BTreeMap<TxnId, (TxnState, Option<i64>)>> {
        let state = self.canonical_state.lock();
        let mut completions = state
            .participant_completion
            .iter()
            .map(|(tid, outcome)| (*tid, (*outcome, None)))
            .collect::<BTreeMap<_, _>>();
        for (tid, retirement) in &state.participant_retirement {
            if !retirement.finalized || now_ms < retirement.expires_at_ms {
                completions.insert(
                    *tid,
                    (
                        retirement.outcome,
                        retirement.finalized.then_some(retirement.expires_at_ms),
                    ),
                );
            }
        }
        Ok(completions)
    }

    pub(crate) fn has_active_undo(&self, txn_id: &TxnId) -> bool {
        self.active_txns.lock().contains(txn_id)
    }

    fn participant_state(
        &self,
        txn_id: &TxnId,
    ) -> io::Result<(Option<TxnState>, Option<ParticipantRetirementRecord>)> {
        let state = self.canonical_state.lock();
        Ok((
            state.participant_completion.get(txn_id).copied(),
            state.participant_retirement.get(txn_id).cloned(),
        ))
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
        self.compact_logs_at(bifrost::utils::time::get_time())
    }

    fn remove_covered_log_files(
        &self,
        log_files: &[(u64, PathBuf)],
        covered_through_seq: u64,
    ) -> io::Result<()> {
        let mut first_error = None;
        for (seq, path) in log_files {
            if *seq > covered_through_seq {
                continue;
            }
            debug!("Trimming superseded undo log: {:?}", path);
            if let Err(error) = remove_log_file(path) {
                first_error.get_or_insert_with(|| {
                    io::Error::new(
                        error.kind(),
                        format!(
                            "cannot durably remove covered undo log {}: {}",
                            path.display(),
                            error
                        ),
                    )
                });
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn compact_logs_at(&self, now_ms: i64) -> io::Result<()> {
        // Holding the writer lock freezes the complete input set. The new
        // higher-sequence snapshot is synced and published before any source
        // file is unlinked, so a crash observes either the old logs or the
        // canonical snapshot plus harmless older files.
        let _generation_guard = self.log_generation.write();
        let mut log_file_guard = self.log_file.lock();
        self.finish_pending_publication()?;
        let current_writer = log_file_guard
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Log file not initialized"))?;
        current_writer.flush()?;
        current_writer.get_ref().sync_all()?;

        let log_dir_path = Path::new(&self.log_dir);
        let mut log_files = collect_undo_log_paths(
            log_dir_path,
            std::fs::read_dir(log_dir_path)?.map(|entry| entry.map(|entry| entry.path())),
        )?;
        let pending_cleanup = *self.pending_cleanup_through.lock();
        if let Some(covered_through_seq) = pending_cleanup {
            self.remove_covered_log_files(&log_files, covered_through_seq)?;
            *self.pending_cleanup_through.lock() = None;
            log_files = collect_undo_log_paths(
                log_dir_path,
                std::fs::read_dir(log_dir_path)?.map(|entry| entry.map(|entry| entry.path())),
            )?;
        }
        let Some((covered_through_seq, _)) = log_files.last() else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "undo log has no active file",
            ));
        };
        let snapshot_seq = covered_through_seq.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "undo log sequence number is exhausted",
            )
        })?;
        let next_seq = snapshot_seq.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "undo log sequence number is exhausted",
            )
        })?;
        let state = self.canonical_state.lock().retained_at(now_ms);

        let staging_path = log_dir_path.join(format!("undo-{}.nlog.compacting", snapshot_seq));
        let snapshot_path = log_dir_path.join(format!("undo-{}.nlog", snapshot_seq));
        let mut snapshot_file = durable_fs::open_or_create(&staging_path, true)?;
        snapshot_file.write_all(&encode_compaction_snapshot_record(*covered_through_seq))?;

        for entries in state.participant_undo.values() {
            for entry in entries {
                snapshot_file.write_all(&entry.to_bytes()?)?;
            }
        }
        for (txn_id, outcome) in &state.participant_completion {
            snapshot_file.write_all(&encode_participant_completion_record(txn_id, *outcome)?)?;
        }
        for (txn_id, record) in &state.participant_retirement {
            if !record.finalized || now_ms < record.expires_at_ms {
                snapshot_file.write_all(&encode_json_payload_record(
                    ENTRY_TYPE_PARTICIPANT_RETIREMENT,
                    txn_id,
                    record,
                )?)?;
            }
        }
        for (txn_id, status) in &state.coordinator_status {
            match status {
                CoordinatorStatus::Decided(record) => {
                    snapshot_file
                        .write_all(&encode_coordinator_decision_record(txn_id, record)?)?;
                }
                CoordinatorStatus::Completed(record)
                    if record.finalized_participants != record.participants
                        || now_ms < record.expires_at_ms =>
                {
                    snapshot_file.write_all(&encode_json_payload_record(
                        ENTRY_TYPE_COORDINATOR_COMPLETION,
                        txn_id,
                        record,
                    )?)?;
                }
                CoordinatorStatus::Completed(_) => {}
            }
        }
        snapshot_file.flush()?;
        durable_fs::sync_file(&snapshot_file, &staging_path)?;

        let mut publication_error = None;
        if let Err(rename_error) = durable_fs::rename(&staging_path, &snapshot_path) {
            // `rename` may have completed while publishing the directory entry
            // failed. Once the final path exists, the higher barrier must
            // become the active writer even if the recovery sync also fails;
            // subsequent appends stay fail-closed until that sync succeeds.
            if snapshot_path.exists() && !staging_path.exists() {
                if let Err(sync_error) = durable_fs::sync_parent(&snapshot_path) {
                    publication_error = Some(io::Error::new(
                        sync_error.kind(),
                        format!(
                            "snapshot rename failed ({rename_error}) and adoption sync failed: {sync_error}"
                        ),
                    ));
                }
            } else {
                drop(snapshot_file);
                let _ = durable_fs::remove_file(&staging_path);
                return Err(rename_error);
            }
        }

        #[cfg(test)]
        self.pause_snapshot_adoption_if_requested_for_test();
        // Keep the already-synced staging handle across rename. Once the
        // higher barrier is published, falling back to the old lower-sequence
        // writer would make later appends invisible; adopting this exact handle
        // has no post-publication reopen failure window.
        let snapshot_writer = BufWriter::with_capacity(4096, snapshot_file);
        *log_file_guard = Some(snapshot_writer);
        *self.log_file_name.lock() = Some(snapshot_path.to_string_lossy().into_owned());
        self.log_seq.store(next_seq, Ordering::SeqCst);
        *self.active_txns.lock() = state.participant_undo.keys().copied().collect();
        *self.canonical_state.lock() = state;
        *self.pending_cleanup_through.lock() = Some(*covered_through_seq);

        if let Some(error) = publication_error {
            *self.publication_sync_pending.lock() = Some(snapshot_path);
            return Err(error);
        }

        self.remove_covered_log_files(&log_files, *covered_through_seq)?;
        *self.pending_cleanup_through.lock() = None;
        Ok(())
    }

    /// Recover undo log from disk on startup
    /// Returns a HashMap of incomplete transactions for rollback
    pub fn recover(&self) -> io::Result<HashMap<TxnId, Vec<UndoLogEntry>>> {
        let txn_index: HashMap<_, _> = self
            .canonical_state
            .lock()
            .participant_undo
            .iter()
            .map(|(tid, entries)| (*tid, entries.clone()))
            .collect();
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
        directory_sync_count_for_test, fail_directory_sync_after_for_test,
        fail_next_directory_sync_for_test, fail_next_file_sync_for_test,
        fail_next_rename_directory_sync_for_test,
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
    fn failed_rotations_do_not_lose_active_log_during_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).unwrap();
        let before_tid = test_hlc(4, 9);
        undo.write_coordinator_abort_decision(&before_tid, &[17])
            .unwrap();

        for _ in 0..3 {
            fail_next_directory_sync_for_test(&log_dir);
            undo.rotate_log()
                .expect_err("injected rotation publication must fail");
        }
        undo.trim_old_logs().unwrap();

        let active_path = PathBuf::from(
            undo.log_file_name
                .lock()
                .clone()
                .expect("active snapshot filename"),
        );
        assert!(
            active_path.exists(),
            "compaction must adopt an existing active snapshot after failed rotations"
        );
        assert_eq!(
            undo.coordinator_decision(&before_tid).unwrap(),
            Some(TxnResolution::Abort),
            "the active log contents must survive failed rotations and compaction"
        );
        let after_tid = test_hlc(5, 9);
        undo.write_coordinator_abort_decision(&after_tid, &[17])
            .unwrap();
        drop(undo);
        let reopened = UndoLogger::new(log_dir.to_string_lossy().into_owned()).unwrap();
        assert_eq!(
            reopened.coordinator_decision(&before_tid).unwrap(),
            Some(TxnResolution::Abort),
            "records compacted after failed rotations must remain crash-visible"
        );
        assert_eq!(
            reopened.coordinator_decision(&after_tid).unwrap(),
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
    fn coordinator_dispatch_intent_is_restartable_abort_work_until_completion() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(300, 8);

        {
            let undo =
                UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
            undo.write_coordinator_dispatch_intent(&tid, &[11, 12])
                .unwrap();
            assert_eq!(
                undo.coordinator_status(&tid).unwrap(),
                Some(CoordinatorStatus::Decided(CoordinatorDecisionRecord {
                    resolution: TxnResolution::InProgress,
                    participants: BTreeSet::from([11, 12]),
                }))
            );
            assert_eq!(
                undo.coordinator_abort_cleanup_candidates(8).unwrap(),
                vec![tid],
                "an in-progress dispatch intent is canonical restartable Abort work"
            );
        }

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert_eq!(
            reopened.coordinator_abort_cleanup_candidates(8).unwrap(),
            vec![tid],
            "restart must rediscover a crash after intent persistence"
        );
        reopened.compact_logs_at(0).unwrap();
        drop(reopened);
        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("compacted undo logger");
        assert_eq!(
            reopened.coordinator_abort_cleanup_candidates(8).unwrap(),
            vec![tid],
            "compaction must retain an unresolved dispatch intent"
        );
        reopened
            .write_coordinator_abort_decision(&tid, &[11, 12])
            .unwrap();
        assert_eq!(
            reopened.coordinator_abort_cleanup_candidates(8).unwrap(),
            vec![tid],
            "durable Abort remains cleanup work until completion"
        );
        reopened
            .write_coordinator_completion(&tid, TxnResolution::Abort, &[11, 12], i64::MAX)
            .unwrap();
        assert!(
            reopened
                .coordinator_abort_cleanup_candidates(8)
                .unwrap()
                .is_empty(),
            "completion must physically remove canonical Abort cleanup work"
        );
    }

    #[test]
    fn failed_startup_intent_promotion_preserves_output_undo_and_retryability() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let (schema, chunks) = compensation_test_chunks("failed_startup_intent_promotion");
        let tid = test_hlc(300, 81);
        let cell_id = Id::new(0, 708);
        let mut installed = OwnedCell::new_with_id(
            schema.id,
            &cell_id,
            data_map_value!(id: 1i32, value: "pending-local-intent".to_string()),
        );
        let installed_revision_ts = chunks.write_cell(&mut installed).unwrap().revision_ts;

        {
            let undo =
                UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
            undo.write_coordinator_dispatch_intent(&tid, &[81, 82])
                .unwrap();
            undo.write_undo_entry(UndoLogEntry::new_write(tid, cell_id, installed_revision_ts))
                .unwrap();
            undo.fail_next_coordinator_abort_decision_for_test();

            assert!(
                undo.promote_local_dispatch_intent_to_abort(&tid).is_err(),
                "a failed decision sync must stop before compensation"
            );
            assert_eq!(
                undo.coordinator_status(&tid).unwrap(),
                Some(CoordinatorStatus::Decided(CoordinatorDecisionRecord {
                    resolution: TxnResolution::InProgress,
                    participants: BTreeSet::from([81, 82]),
                }))
            );
            assert!(undo.recover().unwrap().contains_key(&tid));
            assert_eq!(
                chunks.read_cell(&cell_id).unwrap().header.revision_ts,
                installed_revision_ts
            );
        }

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert_eq!(
            reopened.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Decided(CoordinatorDecisionRecord {
                resolution: TxnResolution::InProgress,
                participants: BTreeSet::from([81, 82]),
            })),
            "a later restart must retry the exact durable intent"
        );
        assert_eq!(
            reopened
                .promote_local_dispatch_intent_to_abort(&tid)
                .unwrap(),
            Some(CoordinatorDecisionRecord {
                resolution: TxnResolution::Abort,
                participants: BTreeSet::from([81, 82]),
            })
        );
        let unresolved = reopened
            .resolve_recovered_transactions(
                reopened.recover().unwrap(),
                &HashMap::from([(tid, TxnResolution::Abort)]),
            )
            .unwrap();
        reopened
            .rollback_incomplete_transactions(unresolved, &chunks)
            .unwrap();
        assert!(chunks.read_cell(&cell_id).is_err());
        assert!(!reopened.recover().unwrap().contains_key(&tid));
        drop(reopened);

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert_eq!(
            reopened.coordinator_decision(&tid).unwrap(),
            Some(TxnResolution::Abort),
            "the retry must durably supersede the older InProgress record"
        );
    }

    #[test]
    fn startup_repairs_only_the_newest_incomplete_record() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path();
        let tid = test_hlc(304, 81);
        let intent = CoordinatorDecisionRecord {
            resolution: TxnResolution::InProgress,
            participants: BTreeSet::from([81, 82]),
        };
        let abort = CoordinatorDecisionRecord {
            resolution: TxnResolution::Abort,
            participants: intent.participants.clone(),
        };
        std::fs::write(
            log_dir.join("undo-0.nlog"),
            encode_coordinator_decision_record(&tid, &intent).unwrap(),
        )
        .unwrap();
        let abort_bytes = encode_coordinator_decision_record(&tid, &abort).unwrap();
        std::fs::write(
            log_dir.join("undo-1.nlog"),
            &abort_bytes[..abort_bytes.len() - 1],
        )
        .unwrap();

        let recovered =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("repairable tail");
        assert_eq!(
            recovered.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Decided(intent.clone()))
        );
        assert_eq!(
            std::fs::metadata(log_dir.join("undo-1.nlog"))
                .unwrap()
                .len(),
            0,
            "startup must durably discard only the incomplete newest record"
        );
        drop(recovered);

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("repaired reopen");
        assert_eq!(
            reopened.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Decided(intent))
        );
    }

    #[test]
    fn startup_rejects_an_older_incomplete_record_and_complete_corruption() {
        let older_tail_dir = TempDir::new().unwrap();
        let tid = test_hlc(305, 81);
        let intent = CoordinatorDecisionRecord {
            resolution: TxnResolution::InProgress,
            participants: BTreeSet::from([81, 82]),
        };
        std::fs::write(
            older_tail_dir.path().join("undo-0.nlog"),
            encode_coordinator_decision_record(&tid, &intent).unwrap(),
        )
        .unwrap();
        let abort_bytes = encode_coordinator_decision_record(
            &tid,
            &CoordinatorDecisionRecord {
                resolution: TxnResolution::Abort,
                participants: intent.participants.clone(),
            },
        )
        .unwrap();
        std::fs::write(
            older_tail_dir.path().join("undo-1.nlog"),
            &abort_bytes[..abort_bytes.len() - 1],
        )
        .unwrap();
        std::fs::write(older_tail_dir.path().join("undo-2.nlog"), []).unwrap();
        let older_error =
            match UndoLogger::new(older_tail_dir.path().to_string_lossy().into_owned()) {
                Ok(_) => panic!("an older incomplete generation must remain fatal"),
                Err(error) => error,
            };
        assert_eq!(older_error.kind(), io::ErrorKind::UnexpectedEof);

        let corrupt_dir = TempDir::new().unwrap();
        std::fs::write(
            corrupt_dir.path().join("undo-0.nlog"),
            encode_coordinator_decision_record(&tid, &intent).unwrap(),
        )
        .unwrap();
        std::fs::write(
            corrupt_dir.path().join("undo-1.nlog"),
            [u8::MAX, 0, 0, 0, 0],
        )
        .unwrap();
        let corrupt_error = match UndoLogger::new(corrupt_dir.path().to_string_lossy().into_owned())
        {
            Ok(_) => panic!("complete newest-generation corruption must remain fatal"),
            Err(error) => error,
        };
        assert_eq!(corrupt_error.kind(), io::ErrorKind::InvalidData);

        let partial_snapshot_dir = TempDir::new().unwrap();
        let snapshot = encode_compaction_snapshot_record(0);
        std::fs::write(
            partial_snapshot_dir.path().join("undo-1.nlog"),
            &snapshot[..snapshot.len() - 1],
        )
        .unwrap();
        let snapshot_error =
            match UndoLogger::new(partial_snapshot_dir.path().to_string_lossy().into_owned()) {
                Ok(_) => panic!("an incomplete compaction snapshot must remain fatal"),
                Err(error) => error,
            };
        assert_eq!(snapshot_error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn startup_syncs_a_recovered_complete_decision_before_exposing_it() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path();
        let tid = test_hlc(306, 81);
        let intent = CoordinatorDecisionRecord {
            resolution: TxnResolution::InProgress,
            participants: BTreeSet::from([81, 82]),
        };
        let abort = CoordinatorDecisionRecord {
            resolution: TxnResolution::Abort,
            participants: intent.participants.clone(),
        };
        std::fs::write(
            log_dir.join("undo-0.nlog"),
            encode_coordinator_decision_record(&tid, &intent).unwrap(),
        )
        .unwrap();
        let newest_path = log_dir.join("undo-1.nlog");
        std::fs::write(
            &newest_path,
            encode_coordinator_decision_record(&tid, &abort).unwrap(),
        )
        .unwrap();
        fail_next_file_sync_for_test(&newest_path);

        let error = match UndoLogger::new(log_dir.to_string_lossy().into_owned()) {
            Ok(_) => panic!("an unsynced recovered final decision must not be exposed"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("injected file sync failure"),
            "{error}"
        );

        let retried = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("sync retry");
        assert_eq!(
            retried.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Decided(abort))
        );
    }

    #[test]
    fn startup_tail_repair_sync_failure_is_retryable() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path();
        let tid = test_hlc(307, 81);
        let intent = CoordinatorDecisionRecord {
            resolution: TxnResolution::InProgress,
            participants: BTreeSet::from([81, 82]),
        };
        std::fs::write(
            log_dir.join("undo-0.nlog"),
            encode_coordinator_decision_record(&tid, &intent).unwrap(),
        )
        .unwrap();
        let abort_bytes = encode_coordinator_decision_record(
            &tid,
            &CoordinatorDecisionRecord {
                resolution: TxnResolution::Abort,
                participants: intent.participants.clone(),
            },
        )
        .unwrap();
        let newest_path = log_dir.join("undo-1.nlog");
        std::fs::write(&newest_path, &abort_bytes[..abort_bytes.len() - 1]).unwrap();
        fail_next_file_sync_for_test(&newest_path);

        let error = match UndoLogger::new(log_dir.to_string_lossy().into_owned()) {
            Ok(_) => panic!("a failed tail-repair sync must stop startup"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("injected file sync failure"),
            "{error}"
        );

        let retried =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("repair sync retry");
        assert_eq!(
            retried.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Decided(intent))
        );
    }

    #[test]
    fn startup_intent_promotion_never_downgrades_a_final_decision() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let commit_tid = test_hlc(301, 81);
        let commit_hlc = test_hlc(401, 81);
        let abort_tid = test_hlc(302, 81);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");

        undo.write_coordinator_dispatch_intent(&commit_tid, &[81, 82])
            .unwrap();
        undo.write_coordinator_commit_decision(&commit_tid, commit_hlc, &[81, 82])
            .unwrap();
        assert_eq!(
            undo.promote_local_dispatch_intent_to_abort(&commit_tid)
                .unwrap(),
            Some(CoordinatorDecisionRecord {
                resolution: TxnResolution::Commit(commit_hlc),
                participants: BTreeSet::from([81, 82]),
            })
        );

        undo.write_coordinator_dispatch_intent(&abort_tid, &[81, 83])
            .unwrap();
        undo.write_coordinator_abort_decision(&abort_tid, &[81, 83])
            .unwrap();
        assert_eq!(
            undo.promote_local_dispatch_intent_to_abort(&abort_tid)
                .unwrap(),
            Some(CoordinatorDecisionRecord {
                resolution: TxnResolution::Abort,
                participants: BTreeSet::from([81, 83]),
            })
        );
        drop(undo);

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert_eq!(
            reopened.coordinator_decision(&commit_tid).unwrap(),
            Some(TxnResolution::Commit(commit_hlc))
        );
        assert_eq!(
            reopened.coordinator_decision(&abort_tid).unwrap(),
            Some(TxnResolution::Abort)
        );
    }

    #[test]
    fn foreign_dispatch_intent_remains_inprogress_during_local_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let local_server_id = 81;
        let foreign_tid = test_hlc(303, 82);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
        undo.write_coordinator_dispatch_intent(&foreign_tid, &[81, 82])
            .unwrap();

        assert_eq!(
            undo.coordinator_recovery_decision(&foreign_tid, local_server_id)
                .unwrap(),
            Some(CoordinatorDecisionRecord {
                resolution: TxnResolution::InProgress,
                participants: BTreeSet::from([81, 82]),
            })
        );
        assert_eq!(
            undo.coordinator_status(&foreign_tid).unwrap(),
            Some(CoordinatorStatus::Decided(CoordinatorDecisionRecord {
                resolution: TxnResolution::InProgress,
                participants: BTreeSet::from([81, 82]),
            })),
            "recovery must never infer Abort for a foreign coordinator"
        );
        drop(undo);

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert_eq!(
            reopened.coordinator_decision(&foreign_tid).unwrap(),
            Some(TxnResolution::InProgress)
        );
    }

    #[test]
    fn later_coordinator_completion_masks_older_decision_across_restart() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(301, 70);
        let commit_hlc = test_hlc(401, 70);
        let expires_at_ms = 900_000;

        {
            let undo =
                UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
            undo.write_coordinator_commit_decision(&tid, commit_hlc, &[11, 12])
                .unwrap();
            undo.rotate_before_next_record_for_test();
            undo.write_coordinator_completion(
                &tid,
                TxnResolution::Commit(commit_hlc),
                &[11, 12],
                expires_at_ms,
            )
            .unwrap();
        }

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert_eq!(
            reopened.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Completed(CoordinatorCompletionRecord {
                resolution: TxnResolution::Commit(commit_hlc),
                participants: BTreeSet::from([11, 12]),
                expires_at_ms,
                retired_participants: BTreeSet::new(),
                finalized_participants: BTreeSet::new(),
            })),
            "the later completion must be the only authoritative restart status"
        );
    }

    #[test]
    fn expired_completion_masks_older_decision_before_compaction_and_across_restart() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(301, 71);

        {
            let undo =
                UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
            undo.write_coordinator_abort_decision(&tid, &[11]).unwrap();
            undo.write_coordinator_completion_record(
                &tid,
                &CoordinatorCompletionRecord {
                    resolution: TxnResolution::Abort,
                    participants: BTreeSet::from([11]),
                    expires_at_ms: 0,
                    retired_participants: BTreeSet::from([11]),
                    finalized_participants: BTreeSet::from([11]),
                },
            )
            .unwrap();
            assert_eq!(
                undo.coordinator_decision(&tid).unwrap(),
                None,
                "expiry must not fall back to the older unresolved decision"
            );
        }

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert_eq!(
            reopened.coordinator_decision(&tid).unwrap(),
            None,
            "restart must preserve the completion tombstone until compaction installs a barrier"
        );
        reopened.compact_logs_at(0).unwrap();
        assert_eq!(
            reopened.coordinator_decision(&tid).unwrap(),
            None,
            "compaction at the exact expiry boundary must not reveal the older decision"
        );
        drop(reopened);

        let compacted =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("compacted undo logger");
        assert_eq!(
            compacted.coordinator_decision(&tid).unwrap(),
            None,
            "restart after compaction must not resurrect the older durable decision"
        );
    }

    #[test]
    fn participant_retirement_round_trip_preserves_completion_evidence_until_expiry() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(302, 70);
        let cell_id = Id::new(1, 203);
        let expires_at_ms = 901_000;

        {
            let undo =
                UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
            undo.write_undo_entry(UndoLogEntry::new_write(tid, cell_id, 500))
                .unwrap();
            undo.write_commit_marker(&tid).unwrap();
            undo.write_participant_retirement(&tid, TxnState::Committed, expires_at_ms)
                .unwrap();
        }

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert_eq!(
            reopened.participant_retirement(&tid).unwrap(),
            Some(ParticipantRetirementRecord {
                outcome: TxnState::Committed,
                expires_at_ms,
                finalized: false,
            })
        );
        assert!(
            !reopened.recover().unwrap().contains_key(&tid),
            "retirement evidence must continue suppressing older undo until compaction"
        );
    }

    #[test]
    fn participant_completion_cache_rebuilds_once_with_strict_retirement_expiry() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let ordinary_tid = test_hlc(320, 70);
        let pending_tid = test_hlc(321, 70);
        let live_tid = test_hlc(322, 70);
        let expired_tid = test_hlc(323, 70);
        let now_ms = 1_000_000;
        let live_expiry_ms = now_ms + 1;

        {
            let undo =
                UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
            undo.write_commit_marker(&ordinary_tid).unwrap();
            undo.write_abort_marker(&pending_tid).unwrap();
            undo.write_participant_retirement(&pending_tid, TxnState::Aborted, now_ms)
                .unwrap();
            undo.write_commit_marker(&live_tid).unwrap();
            undo.finalize_participant_retirement(&live_tid, TxnState::Committed, live_expiry_ms)
                .unwrap();
            undo.write_abort_marker(&expired_tid).unwrap();
            undo.finalize_participant_retirement(&expired_tid, TxnState::Aborted, now_ms)
                .unwrap();
        }

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert_eq!(
            reopened.participant_completion_cache_at(now_ms).unwrap(),
            BTreeMap::from([
                (ordinary_tid, (TxnState::Committed, None)),
                (pending_tid, (TxnState::Aborted, None)),
                (live_tid, (TxnState::Committed, Some(live_expiry_ms)),),
            ]),
            "startup cache rebuild must retain ordinary and pending proof, attach the exact \
             finalized deadline, and exclude proof at equality"
        );
    }

    #[test]
    fn targeted_participant_state_follows_later_undo_and_completion_chronology() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(324, 70);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).unwrap();

        undo.write_commit_marker(&tid).unwrap();
        undo.finalize_participant_retirement(&tid, TxnState::Committed, 2_000_000)
            .unwrap();
        assert!(undo.participant_retirement(&tid).unwrap().is_some());

        undo.write_undo_entry(UndoLogEntry::new_write(tid, Id::new(1, 324), 1_324))
            .unwrap();
        assert_eq!(
            undo.participant_retirement(&tid).unwrap(),
            None,
            "a later undo must supersede stale retirement proof for the same TID"
        );
        assert_eq!(
            undo.participant_completion(&tid).unwrap(),
            None,
            "a later undo must also supersede stale participant completion"
        );

        undo.write_abort_marker(&tid).unwrap();
        assert_eq!(
            undo.participant_retirement(&tid).unwrap(),
            None,
            "a later completion must not reveal the retirement it superseded"
        );
        assert_eq!(
            undo.participant_completion(&tid).unwrap(),
            Some(TxnState::Aborted)
        );
    }

    #[test]
    fn startup_rejects_invalid_or_out_of_order_snapshot_barriers() {
        let invalid_dir = TempDir::new().unwrap();
        std::fs::write(
            invalid_dir.path().join("undo-0.nlog"),
            encode_compaction_snapshot_record(0),
        )
        .unwrap();
        let invalid = UndoLogger::new(invalid_dir.path().to_string_lossy().into_owned());
        assert!(
            invalid.is_err(),
            "a snapshot may not cover its own containing sequence"
        );

        let out_of_order_dir = TempDir::new().unwrap();
        std::fs::write(
            out_of_order_dir.path().join("undo-1.nlog"),
            encode_compaction_snapshot_record(0),
        )
        .unwrap();
        std::fs::write(
            out_of_order_dir.path().join("undo-2.nlog"),
            encode_compaction_snapshot_record(0),
        )
        .unwrap();
        let out_of_order = UndoLogger::new(out_of_order_dir.path().to_string_lossy().into_owned());
        assert!(
            out_of_order.is_err(),
            "snapshot barriers must advance monotonically across generations"
        );

        let under_covering_dir = TempDir::new().unwrap();
        std::fs::write(
            under_covering_dir.path().join("undo-1.nlog"),
            encode_participant_completion_record(&test_hlc(12, 34), TxnState::Aborted).unwrap(),
        )
        .unwrap();
        std::fs::write(
            under_covering_dir.path().join("undo-100.nlog"),
            encode_compaction_snapshot_record(0),
        )
        .unwrap();
        let under_covering =
            UndoLogger::new(under_covering_dir.path().to_string_lossy().into_owned());
        assert!(
            under_covering.is_err(),
            "a snapshot barrier must cover the immediately preceding generation before resetting canonical state"
        );
    }

    #[test]
    fn startup_durably_removes_abandoned_compacting_snapshots() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        std::fs::create_dir_all(&log_dir).unwrap();
        let abandoned = [
            log_dir.join("undo-7.nlog.compacting"),
            log_dir.join("undo-900.nlog.compacting"),
        ];
        for path in &abandoned {
            std::fs::write(path, b"crash prefix").unwrap();
        }

        let _undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).unwrap();

        for path in abandoned {
            assert!(
                !path.exists(),
                "startup must remove ignored compaction staging file {}",
                path.display()
            );
        }
    }

    #[test]
    fn global_compaction_expires_acknowledged_evidence_but_retains_unresolved_state() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let unresolved_tid = test_hlc(303, 70);
        let decision_tid = test_hlc(304, 70);
        let completed_tid = test_hlc(305, 70);
        let retired_tid = test_hlc(306, 70);
        let pending_retirement_tid = test_hlc(307, 70);
        let now_ms = 1_000_000;

        {
            let undo =
                UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
            undo.write_undo_entry(UndoLogEntry::new_write(
                unresolved_tid,
                Id::new(1, 204),
                600,
            ))
            .unwrap();
            undo.write_coordinator_abort_decision(&decision_tid, &[71])
                .unwrap();
            undo.write_coordinator_abort_decision(&completed_tid, &[71])
                .unwrap();
            undo.write_coordinator_completion_record(
                &completed_tid,
                &CoordinatorCompletionRecord {
                    resolution: TxnResolution::Abort,
                    participants: BTreeSet::from([71]),
                    expires_at_ms: now_ms,
                    retired_participants: BTreeSet::from([71]),
                    finalized_participants: BTreeSet::from([71]),
                },
            )
            .unwrap();
            undo.write_abort_marker(&retired_tid).unwrap();
            undo.finalize_participant_retirement(&retired_tid, TxnState::Aborted, now_ms)
                .unwrap();
            undo.write_abort_marker(&pending_retirement_tid).unwrap();
            undo.write_participant_retirement(&pending_retirement_tid, TxnState::Aborted, now_ms)
                .unwrap();

            undo.compact_logs_at(now_ms).unwrap();
        }

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert!(reopened.recover().unwrap().contains_key(&unresolved_tid));
        assert_eq!(
            reopened.coordinator_decision(&decision_tid).unwrap(),
            Some(TxnResolution::Abort)
        );
        assert_eq!(reopened.coordinator_status(&completed_tid).unwrap(), None);
        assert_eq!(reopened.participant_completion(&retired_tid).unwrap(), None);
        assert_eq!(
            reopened
                .participant_retirement(&pending_retirement_tid)
                .unwrap(),
            Some(ParticipantRetirementRecord {
                outcome: TxnState::Aborted,
                expires_at_ms: now_ms,
                finalized: false,
            }),
            "unacknowledged participant proof must never expire"
        );
    }

    #[test]
    fn repeated_global_compaction_keeps_mixed_completed_log_bounded() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let now_ms = 1_000_000;
        let unresolved_tid = test_hlc(450, 70);
        let incomplete_completion_tid = test_hlc(451, 70);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");

        for ts in 400..450 {
            let tid = test_hlc(ts, 70);
            undo.write_undo_entry(UndoLogEntry::new_write(tid, Id::new(2, ts), ts + 1_000))
                .unwrap();
            undo.write_commit_marker(&tid).unwrap();
            undo.finalize_participant_retirement(&tid, TxnState::Committed, now_ms)
                .unwrap();
            undo.write_coordinator_abort_decision(&tid, &[70]).unwrap();
            undo.write_coordinator_completion_record(
                &tid,
                &CoordinatorCompletionRecord {
                    resolution: TxnResolution::Abort,
                    participants: BTreeSet::from([70]),
                    expires_at_ms: now_ms,
                    retired_participants: BTreeSet::from([70]),
                    finalized_participants: BTreeSet::from([70]),
                },
            )
            .unwrap();
        }
        undo.write_undo_entry(UndoLogEntry::new_write(
            unresolved_tid,
            Id::new(2, 500),
            1_500,
        ))
        .unwrap();
        undo.write_coordinator_completion_record(
            &incomplete_completion_tid,
            &CoordinatorCompletionRecord {
                resolution: TxnResolution::Abort,
                participants: BTreeSet::from([70]),
                expires_at_ms: now_ms,
                retired_participants: BTreeSet::new(),
                finalized_participants: BTreeSet::new(),
            },
        )
        .unwrap();

        let mut compacted_size = None;
        for _ in 0..3 {
            undo.compact_logs_at(now_ms).unwrap();
            let logs = collect_undo_log_paths(
                &log_dir,
                std::fs::read_dir(&log_dir)
                    .unwrap()
                    .map(|entry| entry.map(|entry| entry.path())),
            )
            .unwrap();
            assert_eq!(logs.len(), 1, "each generation must replace every old file");
            let size = std::fs::metadata(&logs[0].1).unwrap().len();
            if let Some(expected) = compacted_size {
                assert_eq!(
                    size, expected,
                    "repeated compaction must not re-accumulate completed records"
                );
            }
            compacted_size = Some(size);
        }

        assert!(undo.recover().unwrap().contains_key(&unresolved_tid));
        assert!(matches!(
            undo.coordinator_status(&incomplete_completion_tid).unwrap(),
            Some(CoordinatorStatus::Completed(_))
        ));
    }

    #[test]
    fn persistent_unlink_failure_allows_only_one_pending_snapshot_generation() {
        fn log_footprint(log_dir: &Path) -> (usize, u64) {
            let paths = collect_undo_log_paths(
                log_dir,
                std::fs::read_dir(log_dir)
                    .unwrap()
                    .map(|entry| entry.map(|entry| entry.path())),
            )
            .unwrap();
            let bytes = paths
                .iter()
                .map(|(_, path)| std::fs::metadata(path).unwrap().len())
                .sum();
            (paths.len(), bytes)
        }

        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(460, 70);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
        undo.write_coordinator_abort_decision(&tid, &[70]).unwrap();
        undo.rotate_log().unwrap();
        let failed_path = log_dir.join("undo-0.nlog");
        let failure = install_persistent_log_remove_failure(failed_path.clone());

        assert!(
            undo.compact_logs_at(1_000_000).is_err(),
            "the adopted snapshot must report covered-file cleanup failure"
        );
        assert!(
            !log_dir.join("undo-1.nlog").exists(),
            "cleanup must attempt later safe files after the oldest unlink fails"
        );
        let first_failed_footprint = log_footprint(&log_dir);
        for _ in 0..8 {
            assert!(
                undo.compact_logs_at(1_000_000).is_err(),
                "pending cleanup must be retried before another snapshot is published"
            );
            assert_eq!(
                log_footprint(&log_dir),
                first_failed_footprint,
                "persistent unlink failure must not accumulate snapshot generations or bytes"
            );
        }
        drop(undo);

        for _ in 0..4 {
            let reopened = UndoLogger::new(log_dir.to_string_lossy().into_owned())
                .expect("reopened undo logger during persistent unlink failure");
            assert!(matches!(
                reopened.coordinator_status(&tid).unwrap(),
                Some(CoordinatorStatus::Decided(_))
            ));
            assert_eq!(
                log_footprint(&log_dir),
                first_failed_footprint,
                "repeated crash/restart prefixes must not create empty active generations"
            );
            drop(reopened);
        }

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert!(matches!(
            reopened.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Decided(_))
        ));
        let restarted_footprint = log_footprint(&log_dir);
        for _ in 0..4 {
            assert!(
                reopened.compact_logs_at(1_000_000).is_err(),
                "restart must recover and retry the highest barrier cleanup first"
            );
            assert_eq!(
                log_footprint(&log_dir),
                restarted_footprint,
                "restart retries must remain bounded while unlink failure persists"
            );
        }

        drop(failure);
        reopened
            .compact_logs_at(1_000_000)
            .expect("cleanup and compaction should converge after unlink recovers");
        assert_eq!(log_footprint(&log_dir).0, 1);
        assert!(!failed_path.exists());
        assert!(matches!(
            reopened.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Decided(_))
        ));
    }

    #[test]
    fn global_compaction_deduplicates_retried_unresolved_undo() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(452, 70);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).unwrap();
        let entry = UndoLogEntry::new_write(tid, Id::new(2, 452), 1_452);

        for _ in 0..64 {
            undo.write_undo_entry(entry.clone()).unwrap();
        }
        undo.compact_logs_at(1_000_000).unwrap();

        let recovered = undo.recover().unwrap();
        assert_eq!(
            recovered.get(&tid).map(Vec::len),
            Some(1),
            "compaction must retain one canonical copy of identical unresolved undo evidence"
        );
    }

    #[test]
    fn compaction_snapshot_masks_a_superseded_file_left_after_publication() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(308, 70);
        let now_ms = 1_000_000;

        {
            let undo =
                UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
            undo.write_coordinator_completion_record(
                &tid,
                &CoordinatorCompletionRecord {
                    resolution: TxnResolution::Abort,
                    participants: BTreeSet::new(),
                    expires_at_ms: now_ms,
                    retired_participants: BTreeSet::new(),
                    finalized_participants: BTreeSet::new(),
                },
            )
            .unwrap();
            let superseded_path =
                PathBuf::from(undo.log_file_name.lock().clone().expect("active log path"));
            let superseded_bytes = std::fs::read(&superseded_path).unwrap();

            undo.compact_logs_at(now_ms).unwrap();
            std::fs::write(&superseded_path, superseded_bytes).unwrap();
        }

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert_eq!(
            reopened.coordinator_status(&tid).unwrap(),
            None,
            "the higher snapshot barrier must mask a lower file even if deletion was incomplete"
        );
    }

    #[test]
    fn failed_snapshot_sync_leaves_the_old_log_authoritative_and_retryable() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(309, 70);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
        undo.write_undo_entry(UndoLogEntry::new_write(tid, Id::new(1, 205), 601))
            .unwrap();
        let staging_path = log_dir.join(format!(
            "undo-{}.nlog.compacting",
            undo.log_seq.load(Ordering::SeqCst)
        ));
        fail_next_file_sync_for_test(&staging_path);

        assert!(undo.compact_logs_at(1_000_000).is_err());
        assert!(
            undo.recover().unwrap().contains_key(&tid),
            "an unpublished snapshot must not replace the old authoritative log"
        );

        undo.compact_logs_at(1_000_000).unwrap();
        drop(undo);
        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert!(reopened.recover().unwrap().contains_key(&tid));
    }

    #[test]
    fn post_rename_directory_sync_failure_adopts_the_synced_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(310, 70);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
        undo.write_undo_entry(UndoLogEntry::new_write(tid, Id::new(1, 206), 602))
            .unwrap();
        let snapshot_path =
            log_dir.join(format!("undo-{}.nlog", undo.log_seq.load(Ordering::SeqCst)));
        fail_next_rename_directory_sync_for_test(&snapshot_path);

        undo.compact_logs_at(1_000_000)
            .expect("a completed rename is adoptable after retrying the parent sync");
        drop(undo);
        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert!(reopened.recover().unwrap().contains_key(&tid));
    }

    #[test]
    fn expired_coordinator_visibility_does_not_drop_incomplete_retirement_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(310, 74);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
        undo.write_coordinator_completion_record(
            &tid,
            &CoordinatorCompletionRecord {
                resolution: TxnResolution::Abort,
                participants: BTreeSet::from([74]),
                expires_at_ms: 10_000,
                retired_participants: BTreeSet::new(),
                finalized_participants: BTreeSet::new(),
            },
        )
        .unwrap();

        undo.compact_logs_at(10_000).unwrap();
        assert!(matches!(
            undo.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Completed(ref record))
                if record.finalized_participants.is_empty()
        ));
        drop(undo);

        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        assert!(matches!(
            reopened.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Completed(ref record))
                if record.finalized_participants.is_empty()
        ));
    }

    #[test]
    fn double_post_rename_sync_failure_adopts_and_fails_closed_until_resynced() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let original_tid = test_hlc(310, 71);
        let blocked_tid = test_hlc(310, 72);
        let resumed_tid = test_hlc(310, 73);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
        undo.write_undo_entry(UndoLogEntry::new_write(original_tid, Id::new(1, 207), 603))
            .unwrap();
        let snapshot_path =
            log_dir.join(format!("undo-{}.nlog", undo.log_seq.load(Ordering::SeqCst)));
        fail_next_rename_directory_sync_for_test(&snapshot_path);
        // Allow the staging-file directory sync, then fail the recovery sync
        // after rename has physically published the final path.
        fail_directory_sync_after_for_test(&log_dir, 1);

        assert!(
            undo.compact_logs_at(1_000_000).is_err(),
            "a second parent-sync failure must be reported after adopting the snapshot"
        );
        let snapshot_name = snapshot_path.to_string_lossy().into_owned();
        assert_eq!(
            undo.log_file_name.lock().as_deref(),
            Some(snapshot_name.as_str()),
            "the old lower-sequence writer must never be restored"
        );

        fail_next_directory_sync_for_test(&log_dir);
        assert!(undo
            .write_undo_entry(UndoLogEntry::new_write(blocked_tid, Id::new(1, 208), 604,))
            .is_err());
        undo.write_undo_entry(UndoLogEntry::new_write(resumed_tid, Id::new(1, 209), 605))
            .expect("the next append retries publication before writing");

        drop(undo);
        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        let recovered = reopened.recover().unwrap();
        assert!(recovered.contains_key(&original_tid));
        assert!(!recovered.contains_key(&blocked_tid));
        assert!(recovered.contains_key(&resumed_tid));
    }

    #[test]
    fn targeted_resolution_and_retirement_reads_never_rescan_log_files() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let tid = test_hlc(311, 70);
        let participant_tid = test_hlc(311, 71);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
        undo.write_coordinator_abort_decision(&tid, &[70]).unwrap();
        undo.write_abort_marker(&participant_tid).unwrap();
        undo.write_participant_retirement(&participant_tid, TxnState::Aborted, 0)
            .unwrap();
        let scans_after_startup = undo.full_log_scan_count_for_test();

        for unknown_offset in 0..256 {
            let unknown_tid = test_hlc(500 + unknown_offset, 99);
            assert!(undo.coordinator_status(&unknown_tid).unwrap().is_none());
            assert!(undo.participant_completion(&unknown_tid).unwrap().is_none());
            assert!(undo.participant_retirement(&unknown_tid).unwrap().is_none());
            assert!(matches!(
                undo.coordinator_status(&tid).unwrap(),
                Some(CoordinatorStatus::Decided(_))
            ));
            assert!(undo
                .participant_retirement(&participant_tid)
                .unwrap()
                .is_some());
        }
        assert_eq!(
            undo.full_log_scan_count_for_test(),
            scans_after_startup,
            "targeted hit/miss and retirement retries must use only the canonical index"
        );
    }

    #[test]
    fn retirement_discovery_is_bounded_round_robin_and_releases_index_before_append() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
        for offset in 0..600 {
            let tid = test_hlc(1_000 + offset, 70);
            undo.write_coordinator_completion_record(
                &tid,
                &CoordinatorCompletionRecord {
                    resolution: TxnResolution::Abort,
                    participants: BTreeSet::from([70]),
                    expires_at_ms: 2_000_000,
                    retired_participants: BTreeSet::new(),
                    finalized_participants: BTreeSet::new(),
                },
            )
            .unwrap();
        }
        let scans_after_startup = undo.full_log_scan_count_for_test();
        let first: HashSet<_> = undo
            .coordinator_retirement_candidates(37)
            .unwrap()
            .into_iter()
            .collect();
        let second: HashSet<_> = undo
            .coordinator_retirement_candidates(37)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(first.len(), 37);
        assert_eq!(second.len(), 37);
        assert!(
            first.is_disjoint(&second),
            "bounded discovery must rotate rather than starve later incomplete TIDs"
        );

        let appended_tid = test_hlc(9_999, 71);
        undo.write_coordinator_abort_decision(&appended_tid, &[71])
            .expect("candidate extraction must release the canonical index before append");
        assert!(matches!(
            undo.coordinator_status(&appended_tid).unwrap(),
            Some(CoordinatorStatus::Decided(_))
        ));
        assert_eq!(
            undo.full_log_scan_count_for_test(),
            scans_after_startup,
            "bounded retirement discovery and a normal append must not rescan log files"
        );

        undo.compact_logs_at(1_000_000).unwrap();
        drop(undo);
        let reopened =
            UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("reopened undo logger");
        let after_reopen: BTreeSet<_> = reopened
            .coordinator_retirement_candidates(37)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            after_reopen.len(),
            37,
            "compaction/reopen must rebuild only live canonical retirement work"
        );
    }

    #[test]
    fn durable_retirement_discovery_has_no_completed_tombstones_or_live_prefix_starvation() {
        let mut state = CanonicalLogState::default();
        for offset in 0..2_048 {
            let tid = test_hlc(30_000 + offset, 70);
            state.apply_coordinator_completion(
                tid,
                CoordinatorCompletionRecord {
                    resolution: TxnResolution::Abort,
                    participants: BTreeSet::from([70]),
                    expires_at_ms: i64::MAX,
                    retired_participants: BTreeSet::new(),
                    finalized_participants: BTreeSet::new(),
                },
            );
            state.apply_coordinator_completion(
                tid,
                CoordinatorCompletionRecord {
                    resolution: TxnResolution::Abort,
                    participants: BTreeSet::from([70]),
                    expires_at_ms: i64::MAX,
                    retired_participants: BTreeSet::from([70]),
                    finalized_participants: BTreeSet::from([70]),
                },
            );
        }
        let live = BTreeSet::from([
            test_hlc(50_001, 70),
            test_hlc(50_002, 70),
            test_hlc(50_003, 70),
        ]);
        for tid in &live {
            state.apply_coordinator_completion(
                *tid,
                CoordinatorCompletionRecord {
                    resolution: TxnResolution::Abort,
                    participants: BTreeSet::from([70]),
                    expires_at_ms: i64::MAX,
                    retired_participants: BTreeSet::new(),
                    finalized_participants: BTreeSet::new(),
                },
            );
        }

        assert_eq!(
            state.retirement_discovery_storage_len(),
            live.len(),
            "completed durable retirements must leave no scheduling tombstones"
        );
        let mut observed = BTreeSet::new();
        for _ in 0..3 {
            observed.extend(state.retirement_candidates(1));
        }
        assert_eq!(
            observed, live,
            "bounded canonical rotation must reach every remaining live retirement"
        );
    }

    #[test]
    fn append_and_rotation_wait_for_snapshot_adoption() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let base_tid = test_hlc(312, 70);
        let appended_tid = test_hlc(313, 70);
        let undo = UndoLogger::new(log_dir.to_string_lossy().into_owned()).expect("undo logger");
        undo.write_undo_entry(UndoLogEntry::new_write(base_tid, Id::new(1, 207), 603))
            .unwrap();
        let pause = undo.pause_before_snapshot_adoption_for_test();

        let compacting_undo = undo.clone();
        let compactor = std::thread::spawn(move || compacting_undo.compact_logs_at(1_000_000));
        pause.wait_until_entered();

        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let appending_undo = undo.clone();
        let append_finished = finished_tx.clone();
        let appender = std::thread::spawn(move || {
            let result = appending_undo.write_undo_entry(UndoLogEntry::new_write(
                appended_tid,
                Id::new(1, 208),
                604,
            ));
            append_finished.send(("append", result)).unwrap();
        });
        let rotating_undo = undo.clone();
        let rotator = std::thread::spawn(move || {
            let result = rotating_undo.rotate_log();
            finished_tx.send(("rotate", result)).unwrap();
        });
        assert!(
            matches!(
                finished_rx.recv_timeout(std::time::Duration::from_millis(50)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ),
            "active append and rotation must wait while the snapshot is unpublished as active"
        );

        pause.release();
        compactor.join().unwrap().unwrap();
        for _ in 0..2 {
            finished_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("writer operation should resume after adoption")
                .1
                .unwrap();
        }
        appender.join().unwrap();
        rotator.join().unwrap();

        let recovered = undo.recover().unwrap();
        assert!(recovered.contains_key(&base_tid));
        assert!(recovered.contains_key(&appended_tid));
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
    fn recovery_uses_the_startup_index_after_undo_directory_is_removed() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().join("undo");
        let undo_log = UndoLogger::new(log_dir.to_string_lossy().into_owned()).unwrap();
        let tid = test_hlc(601, 70);
        undo_log
            .write_undo_entry(UndoLogEntry::new_write(tid, Id::new(7, 8), 9))
            .unwrap();
        let scans_after_startup = undo_log.full_log_scan_count_for_test();
        std::fs::rename(&log_dir, temp_dir.path().join("moved-undo")).unwrap();

        let recovered = undo_log
            .recover()
            .expect("recovery must use the canonical index built at startup");
        assert!(
            recovered.contains_key(&tid),
            "removing the directory after startup must not force a synchronous recovery rescan"
        );
        assert_eq!(undo_log.full_log_scan_count_for_test(), scans_after_startup);
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

        let recover_err = match UndoLogger::new(log_dir) {
            Ok(_) => panic!("startup must reject a pre-HLC undo log, not silently succeed"),
            Err(error) => error,
        };
        assert!(
            recover_err.to_string().contains("pre-HLC"),
            "expected a pre-HLC error message from startup index rebuild, got: {}",
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
            transactions::data_site::install_persistent_abort_cannot_end_for_cell(txn_id, cell_id);
        assert_eq!(
            txn.abort(txn_id).await.unwrap().unwrap(),
            AbortResult::CheckFailed(CheckError::CannotEnd)
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
        drop(abort_failure);

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
            transactions::data_site::install_persistent_abort_cannot_end_for_cell(txn_id, cell_id);
        assert_eq!(
            txn.abort(txn_id).await.unwrap().unwrap(),
            AbortResult::CheckFailed(CheckError::CannotEnd)
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
        drop(abort_failure);

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
            transactions::data_site::install_persistent_abort_cannot_end_for_cell(txn_id, cell_id);
        assert_eq!(
            txn.abort(txn_id).await.unwrap().unwrap(),
            AbortResult::CheckFailed(CheckError::CannotEnd)
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
        drop(abort_failure);

        // Cell should exist after rollback
        let restored_cell = server2.chunks().read_cell(&cell_id).unwrap();
        assert_eq!(
            restored_cell.data["name"].string().unwrap(),
            &original_name,
            "Uncommitted remove should be rolled back and cell restored"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_promotes_local_dispatch_intent_to_abort_before_recovery_compensation() {
        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::tests::default_fields;
        use crate::server::transactions;
        use crate::server::{NebServer, ServerOptions, Service};
        use tempfile::TempDir;
        use tokio::time::{sleep, timeout, Duration};

        let temp_dir = TempDir::new().unwrap();
        let undo_log_path = temp_dir.path().join("undo");
        let backup_path = temp_dir.path().join("backup");
        let wal_path = temp_dir.path().join("wal");
        let raft_path = temp_dir.path().join("raft");
        std::fs::create_dir_all(&raft_path).unwrap();
        let server_addr = String::from("127.0.0.1:5321");
        let group_name = "startup_promotes_local_dispatch_intent";
        let options = |enable_recovery, services| ServerOptions {
            chunk_size: SEGMENT_SIZE * 4,
            db_size: SEGMENT_SIZE * 4,
            history_retention_ms: 300_000,
            tiered_config: None,
            backup_storage: Some(backup_path.to_string_lossy().into_owned()),
            wal_storage: Some(wal_path.to_string_lossy().into_owned()),
            index_enabled: false,
            services,
            enable_recovery,
            disable_storage_locks: true,
            undo_log_storage: Some(undo_log_path.to_string_lossy().into_owned()),
            raft_storage: Some(raft_path.to_string_lossy().into_owned()),
        };

        let server = NebServer::new_from_opts(
            &options(false, vec![]),
            &server_addr,
            group_name,
            async |_| {},
        )
        .await
        .unwrap();
        let server_id = server.server_id;
        let schema = crate::ram::schema::Schema::new_with_id(
            1,
            &String::from("startup_dispatch_intent"),
            None,
            default_fields(),
            false,
            false,
        );
        server.meta().schemas.debug_only_new_schema(schema.clone());
        let original_runtime = server.current_database();
        let participant =
            transactions::data_site::DataManager::new(original_runtime.clone(), server.hlc.clone());
        let participant_weak = Arc::downgrade(&participant);
        let tid = server.hlc.try_now().unwrap();
        let cell = OwnedCell::new_with_id(
            schema.id,
            &random_id(),
            data_map_value!(
                id: 5321i64,
                name: "installed-before-crash".to_string(),
                score: 1u64
            ),
        );
        let cell_id = cell.id();
        let original_runtime_weak = Arc::downgrade(&original_runtime);
        let original_undo = original_runtime.undo_log().unwrap().clone();
        original_undo
            .write_coordinator_dispatch_intent(&tid, &[server_id])
            .unwrap();
        assert_eq!(
            <transactions::data_site::DataManager as transactions::data_site::Service>::prepare(
                &participant,
                server_id,
                tid,
                tid,
                vec![super::super::PrepareOp {
                    id: cell_id,
                    expectation: super::super::CellExpectation::Absent(None),
                    intent: super::super::PrepareIntent::Write,
                }],
            )
            .await
            .payload,
            super::super::DMPrepareResult::Success
        );
        assert_eq!(
            <transactions::data_site::DataManager as transactions::data_site::Service>::commit(
                &participant,
                server.hlc.try_now().unwrap(),
                tid,
                vec![super::super::CommitOp::Write(cell)],
            )
            .await
            .payload,
            super::super::DMCommitResult::Success
        );

        assert_eq!(
            original_undo.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Decided(CoordinatorDecisionRecord {
                resolution: TxnResolution::InProgress,
                participants: BTreeSet::from([server_id]),
            }))
        );
        assert!(original_undo.recover().unwrap().contains_key(&tid));
        assert!(
            transactions::data_site::participant_owner_for_test(
                server_id, group_name, group_name, &cell_id,
            )
            .is_some(),
            "the installed participant output must still be owned before the crash"
        );

        server.shutdown().await;
        drop(participant);
        drop(original_runtime);
        drop(server);
        drop(original_undo);
        timeout(Duration::from_secs(2), async {
            while participant_weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the original participant and all of its workers must be destroyed");
        assert!(
            original_runtime_weak.upgrade().is_none(),
            "the original storage runtime must be destroyed after its participant is gone"
        );

        let restarted = NebServer::new_from_opts(
            &options(true, vec![Service::Cell, Service::Transaction]),
            &server_addr,
            group_name,
            async |_| {},
        )
        .await
        .expect("startup recovery must not loop forever on a local durable dispatch intent");
        restarted
            .meta()
            .schemas
            .debug_only_new_schema(schema.clone());
        assert!(
            restarted.chunks().read_cell(&cell_id).is_err(),
            "startup must compensate the installed output after durably deciding Abort"
        );
        assert!(
            !restarted
                .current_database()
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "participant recovery must durably complete local undo"
        );
        assert_eq!(
            transactions::data_site::participant_owner_for_test(
                server_id, group_name, group_name, &cell_id,
            ),
            None,
            "the recovered participant must not retain the crashed owner"
        );

        let restarted_undo = restarted.current_database().undo_log().unwrap().clone();
        timeout(Duration::from_secs(15), async {
            loop {
                if matches!(
                    restarted_undo.coordinator_status(&tid).unwrap(),
                    Some(CoordinatorStatus::Completed(ref record))
                        if record.resolution == TxnResolution::Abort
                            && record.participants == BTreeSet::from([server_id])
                ) {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the restarted manager must finish canonical participant cleanup");
        restarted.shutdown().await;
        drop(restarted_undo);
        drop(restarted);

        let completed =
            UndoLogger::new(undo_log_path.to_string_lossy().into_owned()).expect("completed log");
        assert!(
            matches!(
                completed.coordinator_status(&tid).unwrap(),
                Some(CoordinatorStatus::Completed(ref record))
                    if record.resolution == TxnResolution::Abort
                        && record.participants == BTreeSet::from([server_id])
            ),
            "reopen must retain Completed(Abort) and mask the older InProgress intent"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup_sync_failure_preserves_distributed_dispatch_intent_for_retry() {
        use crate::ram::segs::SEGMENT_SIZE;
        use crate::ram::tests::default_fields;
        use crate::server::transactions;
        use crate::server::{NebServer, ServerOptions, Service};
        use bifrost::rpc::ServiceClient;
        use tempfile::TempDir;
        use tokio::time::{sleep, timeout, Duration};

        let temp_dir = TempDir::new().unwrap();
        let coordinator_root = temp_dir.path().join("coordinator");
        let remote_root = temp_dir.path().join("remote");
        let coordinator_undo = coordinator_root.join("undo");
        let addresses = [
            String::from("127.0.0.1:5522"),
            String::from("127.0.0.1:5523"),
        ];
        let meta_servers = addresses.to_vec();
        let group_name = "startup_distributed_dispatch_intent_sync_failure";
        let options = |root: &Path,
                       raft_root: &Path,
                       enable_recovery: bool,
                       services: Vec<Service>| ServerOptions {
            chunk_size: SEGMENT_SIZE * 4,
            db_size: SEGMENT_SIZE * 4,
            history_retention_ms: 300_000,
            tiered_config: None,
            backup_storage: Some(root.join("backup").to_string_lossy().into_owned()),
            wal_storage: Some(root.join("wal").to_string_lossy().into_owned()),
            index_enabled: false,
            services,
            enable_recovery,
            disable_storage_locks: true,
            undo_log_storage: Some(root.join("undo").to_string_lossy().into_owned()),
            raft_storage: Some(raft_root.to_string_lossy().into_owned()),
        };

        let remote = NebServer::new_cluster_from_opts(
            &options(
                &remote_root,
                &remote_root.join("raft"),
                false,
                vec![Service::Cell, Service::Transaction],
            ),
            &addresses[0],
            &meta_servers,
            group_name,
            async |_| {},
        )
        .await
        .unwrap();
        let coordinator = NebServer::new_cluster_from_opts(
            &options(
                &coordinator_root,
                &coordinator_root.join("raft-initial"),
                false,
                vec![],
            ),
            &addresses[1],
            &meta_servers,
            group_name,
            async |_| {},
        )
        .await
        .unwrap();
        let coordinator_id = coordinator.server_id;
        let remote_id = remote.server_id;
        assert_ne!(coordinator_id, remote_id);
        timeout(Duration::from_secs(5), async {
            loop {
                let coordinator_sees_remote = coordinator
                    .conshash()
                    .to_server_name_option(Some(remote_id))
                    .is_some();
                let remote_sees_coordinator = remote
                    .conshash()
                    .to_server_name_option(Some(coordinator_id))
                    .is_some();
                if coordinator_sees_remote && remote_sees_coordinator {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("both servers must observe the distributed membership");

        let schema = crate::ram::schema::Schema::new_with_id(
            1,
            &String::from("distributed_dispatch_intent"),
            None,
            default_fields(),
            false,
            false,
        );
        coordinator
            .meta()
            .schemas
            .debug_only_new_schema(schema.clone());
        remote.meta().schemas.debug_only_new_schema(schema.clone());
        let coordinator_runtime = coordinator.current_database();
        let coordinator_participant = transactions::data_site::DataManager::new(
            coordinator_runtime.clone(),
            coordinator.hlc.clone(),
        );
        let coordinator_participant_weak = Arc::downgrade(&coordinator_participant);
        let coordinator_runtime_weak = Arc::downgrade(&coordinator_runtime);
        let tid = coordinator.hlc.try_now().unwrap();
        let local_id = Id::new(5523, 1);
        let remote_cell_id = Id::new(5522, 1);
        let local_cell = OwnedCell::new_with_id(
            schema.id,
            &local_id,
            data_map_value!(
                id: 5523i64,
                name: "local-installed-before-crash".to_string(),
                score: 1u64
            ),
        );
        let remote_cell = OwnedCell::new_with_id(
            schema.id,
            &remote_cell_id,
            data_map_value!(
                id: 5522i64,
                name: "remote-installed-before-crash".to_string(),
                score: 2u64
            ),
        );
        let original_undo = coordinator_runtime.undo_log().unwrap().clone();
        original_undo
            .write_coordinator_dispatch_intent(&tid, &[coordinator_id, remote_id])
            .unwrap();
        assert_eq!(
            <transactions::data_site::DataManager as transactions::data_site::Service>::prepare(
                &coordinator_participant,
                coordinator_id,
                tid,
                tid,
                vec![super::super::PrepareOp {
                    id: local_id,
                    expectation: super::super::CellExpectation::Absent(None),
                    intent: super::super::PrepareIntent::Write,
                }],
            )
            .await
            .payload,
            super::super::DMPrepareResult::Success
        );
        assert_eq!(
            <transactions::data_site::DataManager as transactions::data_site::Service>::commit(
                &coordinator_participant,
                coordinator.hlc.try_now().unwrap(),
                tid,
                vec![super::super::CommitOp::Write(local_cell)],
            )
            .await
            .payload,
            super::super::DMCommitResult::Success
        );

        let remote_rpc = bifrost::rpc::DEFAULT_CLIENT_POOL
            .get(&addresses[0])
            .await
            .unwrap();
        let remote_participant = transactions::data_site::AsyncServiceClient::new_with_service_id(
            transactions::data_site::generate_scoped_service_id(group_name, group_name),
            &remote_rpc,
        );
        assert_eq!(
            remote_participant
                .prepare(
                    coordinator_id,
                    tid,
                    tid,
                    vec![super::super::PrepareOp {
                        id: remote_cell_id,
                        expectation: super::super::CellExpectation::Absent(None),
                        intent: super::super::PrepareIntent::Write,
                    }],
                )
                .await
                .unwrap()
                .payload,
            super::super::DMPrepareResult::Success
        );
        assert_eq!(
            remote_participant
                .commit(
                    coordinator.hlc.try_now().unwrap(),
                    tid,
                    vec![super::super::CommitOp::Write(remote_cell)],
                )
                .await
                .unwrap()
                .payload,
            super::super::DMCommitResult::Success
        );
        let expected_owner = Some(super::super::TxnPriority::new(tid, coordinator_id));
        assert_eq!(
            transactions::data_site::participant_owner_for_test(
                remote_id,
                group_name,
                group_name,
                &remote_cell_id,
            ),
            expected_owner
        );
        assert!(matches!(
            remote
                .chunks()
                .head_snapshot(&remote_cell_id, u64::MAX)
                .unwrap(),
            crate::ram::cell::SnapshotRead::Wait
        ));
        let failed_generation = coordinator_undo.join(format!(
            "undo-{}.nlog",
            original_undo.log_seq.load(Ordering::SeqCst)
        ));

        coordinator.shutdown().await;
        drop(coordinator_participant);
        drop(coordinator_runtime);
        drop(coordinator);
        drop(original_undo);
        timeout(Duration::from_secs(2), async {
            while coordinator_participant_weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the original coordinator participant must be destroyed");
        assert!(
            coordinator_runtime_weak.upgrade().is_none(),
            "the original coordinator runtime must be destroyed"
        );

        fail_next_file_sync_for_test(&failed_generation);
        let failed_restart = NebServer::new_cluster_from_opts(
            &options(
                &coordinator_root,
                &coordinator_root.join("raft-failed-restart"),
                true,
                vec![Service::Cell, Service::Transaction],
            ),
            &addresses[1],
            &meta_servers,
            group_name,
            async |_| {},
        )
        .await;
        let startup_error = match failed_restart {
            Ok(unexpected_server) => {
                unexpected_server.shutdown().await;
                panic!("an injected Abort-decision sync failure must fail startup");
            }
            Err(error) => error,
        };
        let startup_error_message = startup_error.to_string();
        assert!(
            startup_error_message.contains("injected file sync failure")
                && startup_error_message.contains(failed_generation.to_string_lossy().as_ref()),
            "startup must fail at the injected Abort-decision sync boundary: \
             {startup_error_message}"
        );
        assert_eq!(
            transactions::data_site::participant_owner_for_test(
                remote_id,
                group_name,
                group_name,
                &remote_cell_id,
            ),
            expected_owner,
            "failed startup must not release the remote participant owner"
        );
        assert!(matches!(
            remote
                .chunks()
                .head_snapshot(&remote_cell_id, u64::MAX)
                .unwrap(),
            crate::ram::cell::SnapshotRead::Wait
        ));

        let failed_bytes = std::fs::read(&failed_generation).unwrap_or_else(|error| {
            panic!(
                "failed startup did not reach the expected promotion generation: \
                 startup={startup_error:?}, file={error:?}"
            )
        });
        assert!(
            failed_bytes.len() > 1,
            "the failed generation must contain the flushed Abort record"
        );
        let failed_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&failed_generation)
            .unwrap();
        failed_file
            .set_len((failed_bytes.len() - 1) as u64)
            .unwrap();
        failed_file.sync_all().unwrap();
        drop(failed_file);
        let reopened = UndoLogger::new(coordinator_undo.to_string_lossy().into_owned()).unwrap();
        assert_eq!(
            reopened.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Decided(CoordinatorDecisionRecord {
                resolution: TxnResolution::InProgress,
                participants: BTreeSet::from([coordinator_id, remote_id]),
            })),
            "after crash loss of the unsynced generation, reopen must retry the exact intent"
        );
        assert!(reopened.recover().unwrap().contains_key(&tid));
        drop(reopened);

        let restarted = NebServer::new_cluster_from_opts(
            &options(
                &coordinator_root,
                &coordinator_root.join("raft-failed-restart"),
                true,
                vec![Service::Cell, Service::Transaction],
            ),
            &addresses[1],
            &meta_servers,
            group_name,
            async |_| {},
        )
        .await
        .expect("a later startup must durably promote and recover the intent");
        restarted
            .meta()
            .schemas
            .debug_only_new_schema(schema.clone());
        assert!(restarted.chunks().read_cell(&local_id).is_err());
        let restarted_undo = restarted.current_database().undo_log().unwrap().clone();
        timeout(Duration::from_secs(15), async {
            loop {
                let remote_released = transactions::data_site::participant_owner_for_test(
                    remote_id,
                    group_name,
                    group_name,
                    &remote_cell_id,
                )
                .is_none();
                let remote_compensated = remote.chunks().read_cell(&remote_cell_id).is_err();
                let completed = matches!(
                    restarted_undo.coordinator_status(&tid).unwrap(),
                    Some(CoordinatorStatus::Completed(ref record))
                        if record.resolution == TxnResolution::Abort
                            && record.participants
                                == BTreeSet::from([coordinator_id, remote_id])
                );
                if remote_released && remote_compensated && completed {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the restarted manager must rediscover and clean the exact remote participant");

        restarted.shutdown().await;
        remote.shutdown().await;
        drop(remote_participant);
        drop(restarted_undo);
        drop(restarted);
        drop(remote);

        let completed = UndoLogger::new(coordinator_undo.to_string_lossy().into_owned()).unwrap();
        assert!(matches!(
            completed.coordinator_status(&tid).unwrap(),
            Some(CoordinatorStatus::Completed(ref record))
                if record.resolution == TxnResolution::Abort
                    && record.participants == BTreeSet::from([coordinator_id, remote_id])
        ));
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
