//! The durable record of participant fences (D29).
//!
//! A site in doubt about a multi-site transaction FENCES every participant
//! before it aborts: a fenced site refuses the coordinator's `end` for that
//! transaction, and refuses a `prepare` of it. The site records the fence
//! here, and syncs it, BEFORE it answers `Fenced`: the answer is a promise
//! that must survive a crash and a power loss.
//!
//! Records use 64 independent append-only files beside the WAL files.
//! Version 3 adds an abort settlement after every participant excludes commit.
//! A fence alone does not prove abort. Recovery excludes a bracket only when
//! its abort settlement is durable. Other versions are refused.
//!
//! ```text
//! header (8 bytes)
//!   magic    [u8; 4]  b"NEBF"
//!   version  u8       FENCE_LOG_VERSION
//!   reserved [u8; 3]
//! record (24 bytes), repeated
//!   txn ts   u64
//!   txn node u64
//!   aborted  u32      0 = fence, 1 = settled abort
//!   crc      u32      CRC32C over the 20 bytes before it
//! ```
//!
//! A malformed header or record is a typed error that stops the start.
//! Recovery never discards an unreadable abort settlement.
//!
//! A fence must outlive the transaction at this site (TLA+ F3, F6): a site
//! with no bracket of the transaction answers `Unknown` and must still refuse
//! a `prepare` that a coordinator sends later, and a site that aborted after
//! a fence must refuse a duplicate `prepare` after a compaction and a
//! restart. So a fence is never dropped while the process runs. At each start
//! the log is rewritten with the fences of the transactions still in doubt
//! and every fence younger than `fence_retention` (24 h by default; a
//! coordinator gives a transaction up long before), so the log does not grow
//! without limit.

use crate::server::transactions::TxnId;
use crc_fast::{CrcAlgorithm, Digest};
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

pub const FENCE_LOG_MAGIC: [u8; 4] = *b"NEBF";
pub const FENCE_LOG_VERSION: u8 = 3;
const FENCE_SHARDS: usize = 64;
const HEADER_SIZE: usize = 8;
const RECORD_SIZE: usize = 24;
pub const FENCE_LOG_FILE: &str = "txn-fences.nfence";

/// Why a fence log cannot be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceLogError {
    /// The file does not start with `FENCE_LOG_MAGIC`.
    NotAFenceLog,
    /// The file has a version that this build does not read.
    UnsupportedVersion(u8),
    /// A record has an unknown decision tag.
    InvalidDecision(u32),
    /// The file has an incomplete header.
    TruncatedHeader,
    /// The file ends inside a record.
    TruncatedRecord,
    /// A record fails its checksum.
    ChecksumMismatch,
}

impl std::fmt::Display for FenceLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FenceLogError::NotAFenceLog => write!(f, "the file is not a fence log"),
            FenceLogError::UnsupportedVersion(version) => write!(
                f,
                "the fence log has version {version}; this build reads only version \
                 {FENCE_LOG_VERSION}"
            ),
            FenceLogError::InvalidDecision(tag) => {
                write!(f, "the fence record has decision tag {tag}")
            }
            FenceLogError::TruncatedHeader => write!(f, "the fence log header is incomplete"),
            FenceLogError::TruncatedRecord => write!(f, "the fence log ends inside a record"),
            FenceLogError::ChecksumMismatch => {
                write!(f, "the fence record checksum does not match")
            }
        }
    }
}

impl std::error::Error for FenceLogError {}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut digest = Digest::new(CrcAlgorithm::Crc32Iscsi);
    digest.update(bytes);
    digest.finalize() as u32
}

fn header() -> [u8; HEADER_SIZE] {
    let mut header = [0u8; HEADER_SIZE];
    header[..4].copy_from_slice(&FENCE_LOG_MAGIC);
    header[4] = FENCE_LOG_VERSION;
    header
}

fn record(txn: &TxnId, aborted: bool) -> [u8; RECORD_SIZE] {
    let mut bytes = [0u8; RECORD_SIZE];
    bytes[..8].copy_from_slice(&txn.ts.to_le_bytes());
    bytes[8..16].copy_from_slice(&txn.node.to_le_bytes());
    bytes[16..20].copy_from_slice(&u32::from(aborted).to_le_bytes());
    let crc = crc32c(&bytes[..20]);
    bytes[20..].copy_from_slice(&crc.to_le_bytes());
    bytes
}

/// The durable fences and the subset with a settled abort.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct FenceRecords {
    pub fenced: BTreeSet<TxnId>,
    pub aborted: BTreeSet<TxnId>,
}

/// Read every fence and abort settlement a log holds.
fn parse(data: &[u8]) -> Result<FenceRecords, FenceLogError> {
    if data.len() < HEADER_SIZE {
        return Err(FenceLogError::TruncatedHeader);
    }
    if data[..4] != FENCE_LOG_MAGIC {
        return Err(FenceLogError::NotAFenceLog);
    }
    if data[4] != FENCE_LOG_VERSION {
        return Err(FenceLogError::UnsupportedVersion(data[4]));
    }
    if (data.len() - HEADER_SIZE) % RECORD_SIZE != 0 {
        return Err(FenceLogError::TruncatedRecord);
    }
    let mut records = FenceRecords::default();
    let mut cursor = HEADER_SIZE;
    while cursor + RECORD_SIZE <= data.len() {
        let bytes = &data[cursor..cursor + RECORD_SIZE];
        let crc = u32::from_le_bytes(bytes[20..].try_into().unwrap());
        if crc != crc32c(&bytes[..20]) {
            return Err(FenceLogError::ChecksumMismatch);
        }
        let txn = TxnId {
            ts: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            node: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        };
        match u32::from_le_bytes(bytes[16..20].try_into().unwrap()) {
            0 => {}
            1 => {
                records.aborted.insert(txn);
            }
            tag => return Err(FenceLogError::InvalidDecision(tag)),
        }
        records.fenced.insert(txn);
        cursor += RECORD_SIZE;
    }
    Ok(records)
}

struct State {
    file: Option<File>,
    records: FenceRecords,
}

struct FenceShard {
    path: Option<PathBuf>,
    state: Mutex<State>,
}

fn fence_shard(txn: &TxnId) -> usize {
    // Keep the shard mapping independent of Rust's Hash.
    let mut key = txn.ts ^ txn.node.rotate_left(23);
    key = (key ^ (key >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    key = (key ^ (key >> 27)).wrapping_mul(0x94d049bb133111eb);
    ((key ^ (key >> 31)) as usize) % FENCE_SHARDS
}

/// The fences of one store. Each mutex owns exactly one durable file.
pub struct FenceLog {
    shards: [FenceShard; FENCE_SHARDS],
    count: AtomicUsize,
}

impl FenceLog {
    /// A log in `wal_storage`, or a memory-only one for a store with no WAL:
    /// such a store keeps nothing over a restart anyway.
    pub fn new(wal_storage: Option<&str>) -> Self {
        FenceLog {
            shards: std::array::from_fn(|shard| FenceShard {
                path: wal_storage.map(|dir| {
                    Path::new(dir).join(if shard == 0 {
                        FENCE_LOG_FILE.to_owned()
                    } else {
                        format!("txn-fences-{shard:02}.nfence")
                    })
                }),
                state: Mutex::new(State {
                    file: None,
                    records: FenceRecords::default(),
                }),
            }),
            count: AtomicUsize::new(0),
        }
    }

    /// Read the fences of the last run. Called once at start, before the
    /// store serves. `recover` false means a fresh store: the old log is
    /// dropped.
    pub fn load(&self, recover: bool) -> io::Result<FenceRecords> {
        let mut records = FenceRecords::default();
        if !recover {
            return Ok(records);
        }
        for shard in &self.shards {
            let Some(path) = &shard.path else { continue };
            let mut file = match File::open(path) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            let found =
                parse(&data).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            records.fenced.extend(found.fenced);
            records.aborted.extend(found.aborted);
        }
        Ok(records)
    }

    /// Keep `records` and only those: write them to a new log, sync it, and
    /// put it in place of the old one.
    pub fn reset(&self, records: FenceRecords) -> io::Result<()> {
        let count = records.fenced.len();
        let mut partitions: [FenceRecords; FENCE_SHARDS] =
            std::array::from_fn(|_| FenceRecords::default());
        for txn in records.fenced {
            let partition = &mut partitions[fence_shard(&txn)];
            if records.aborted.contains(&txn) {
                partition.aborted.insert(txn);
            }
            partition.fenced.insert(txn);
        }
        for (i, (shard, records)) in self.shards.iter().zip(partitions).enumerate() {
            let mut state = crate::profile_lock!(H14, shard.state.lock());
            if let Some(path) = &shard.path {
                // Keep shard zero's header as the layout marker, even empty.
                if i != 0 && records.fenced.is_empty() && !path.exists() {
                    state.file = None;
                    state.records = records;
                    continue;
                }
                if let Some(dir) = path.parent() {
                    std::fs::create_dir_all(dir)?;
                }
                let staging = path.with_extension("nfence-new");
                {
                    let mut file = File::create(&staging)?;
                    file.write_all(&header())?;
                    for txn in &records.fenced {
                        file.write_all(&record(txn, records.aborted.contains(txn)))?;
                    }
                    file.sync_all()?;
                }
                std::fs::rename(&staging, path)?;
                if let Some(dir) = path.parent() {
                    File::open(dir)?.sync_all()?;
                }
                #[cfg(test)]
                crate::ram::segs::power_loss_model::record_synced_len(
                    &Some(path.to_string_lossy().to_string()),
                    (HEADER_SIZE + records.fenced.len() * RECORD_SIZE) as u64,
                );
                state.file = Some(OpenOptions::new().append(true).open(path)?);
            }
            state.records = records;
        }
        self.count.store(count, Ordering::Release);
        Ok(())
    }

    /// Record a fence of `txn`, synced. Returns only when a power loss can
    /// no longer drop it.
    pub fn record(&self, txn: &TxnId) -> io::Result<()> {
        self.record_decision(txn, false)
    }

    /// Sync an abort settlement before the recovered bracket releases its pins.
    /// The caller first excludes commit at every participant, or applies an
    /// explicit operator decision. A local fence alone does not permit this.
    pub(crate) fn record_abort(&self, txn: &TxnId) -> io::Result<()> {
        self.record_decision(txn, true)
    }

    fn record_decision(&self, txn: &TxnId, aborted: bool) -> io::Result<()> {
        let shard = &self.shards[fence_shard(txn)];
        let mut state = crate::profile_lock!(H14, shard.state.lock());
        if state.records.aborted.contains(txn) || (!aborted && state.records.fenced.contains(txn)) {
            return Ok(());
        }
        #[cfg(test)]
        if aborted
            && crate::ram::segs::power_loss_model::wal_sync_fails(
                &shard
                    .path
                    .as_ref()
                    .map(|path| path.to_string_lossy().to_string()),
            )
        {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        if state.file.is_none() {
            if let Some(path) = &shard.path {
                if let Some(dir) = path.parent() {
                    std::fs::create_dir_all(dir)?;
                }
                let fresh = !path.exists();
                let mut file = OpenOptions::new().create(true).append(true).open(path)?;
                if fresh {
                    file.write_all(&header())?;
                    file.sync_all()?;
                    if let Some(dir) = path.parent() {
                        File::open(dir)?.sync_all()?;
                    }
                }
                state.file = Some(file);
            }
        }
        if let Some(file) = state.file.as_mut() {
            file.write_all(&record(txn, aborted))?;
            #[cfg(not(test))]
            file.sync_data()?;
            #[cfg(test)]
            if !FENCE_SYNC_SKIPPED_FOR_TEST.load(Ordering::Acquire) {
                file.sync_data()?;
                crate::ram::segs::power_loss_model::record_synced(
                    &shard
                        .path
                        .as_ref()
                        .map(|path| path.to_string_lossy().to_string()),
                    file,
                );
            }
        }
        if aborted {
            state.records.aborted.insert(*txn);
        }
        if state.records.fenced.insert(*txn) {
            self.count.fetch_add(1, Ordering::AcqRel);
        }
        Ok(())
    }

    pub fn is_fenced(&self, txn: &TxnId) -> bool {
        if self.count.load(Ordering::Acquire) == 0 {
            return false;
        }
        crate::profile_lock!(H14, self.shards[fence_shard(txn)].state.lock())
            .records
            .fenced
            .contains(txn)
    }

    pub fn len(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }
}

/// How long a start keeps the fence of a transaction that is not in doubt.
pub fn fence_retention() -> std::time::Duration {
    std::env::var("NEB_FENCE_RETENTION_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(24 * 60 * 60))
}

/// The fences a start keeps: those of the transactions in doubt, and those
/// younger than `fence_retention` by the transaction's clock.
pub fn fences_to_keep(
    previous: &FenceRecords,
    in_doubt: &BTreeSet<TxnId>,
    now_ms: u64,
) -> FenceRecords {
    let horizon = now_ms.saturating_sub(fence_retention().as_millis() as u64);
    // Use the physical brackets before settled aborts are excluded. Otherwise
    // an old settlement could expire while its BEGIN still survives.
    let fenced: BTreeSet<_> = previous
        .fenced
        .iter()
        .filter(|txn| in_doubt.contains(*txn) || txn.wall_ms() >= horizon)
        .copied()
        .collect();
    let aborted = previous.aborted.intersection(&fenced).copied().collect();
    FenceRecords { fenced, aborted }
}

/// Test hook for a reverted control: record fences without the sync.
#[cfg(test)]
pub(crate) static FENCE_SYNC_SKIPPED_FOR_TEST: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::transactions::test_hlc;

    #[test]
    fn fences_survive_a_reload_and_a_reset_keeps_only_the_given_ones() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        let log = FenceLog::new(Some(&path));
        log.reset(FenceRecords::default()).unwrap();
        log.record(&test_hlc(1, 1)).unwrap();
        log.record(&test_hlc(2, 1)).unwrap();
        assert!(log.is_fenced(&test_hlc(1, 1)));
        let reloaded = FenceLog::new(Some(&path));
        let fences = reloaded.load(true).unwrap();
        assert_eq!(fences.fenced.len(), 2);
        reloaded
            .reset(FenceRecords {
                fenced: [test_hlc(2, 1)].into_iter().collect(),
                aborted: BTreeSet::new(),
            })
            .unwrap();
        let again = FenceLog::new(Some(&path)).load(true).unwrap();
        assert_eq!(again.fenced, [test_hlc(2, 1)].into_iter().collect());
    }

    #[test]
    fn independent_fence_files_do_not_wait_for_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let log = std::sync::Arc::new(FenceLog::new(dir.path().to_str()));
        log.reset(FenceRecords::default()).unwrap();
        let first = test_hlc(1, 1);
        let second = (2..1000)
            .map(|n| test_hlc(n, 1))
            .find(|tid| fence_shard(tid) != fence_shard(&first))
            .unwrap();
        let held = log.shards[fence_shard(&first)].state.lock();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = log.clone();
        let second_copy = second.clone();
        let thread = std::thread::spawn(move || {
            worker.record(&second_copy).unwrap();
            assert!(worker.is_fenced(&second_copy));
            done_tx.send(()).unwrap();
        });
        let result = done_rx.recv_timeout(std::time::Duration::from_secs(2));
        drop(held);
        thread.join().unwrap();
        assert!(result.is_ok());
        assert_eq!(
            FenceLog::new(dir.path().to_str())
                .load(true)
                .unwrap()
                .fenced,
            [second].into_iter().collect()
        );
    }

    #[test]
    fn a_single_file_version_one_store_is_refused_with_a_typed_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut old = header();
        old[4] = 1;
        std::fs::write(dir.path().join(FENCE_LOG_FILE), old).unwrap();
        let error = FenceLog::new(dir.path().to_str()).load(true).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.get_ref().unwrap().downcast_ref::<FenceLogError>(),
            Some(&FenceLogError::UnsupportedVersion(1))
        );
    }

    #[test]
    fn a_torn_record_and_another_version_are_refused() {
        let mut data = header().to_vec();
        data.extend_from_slice(&record(&test_hlc(3, 3), false));
        data.extend_from_slice(&record(&test_hlc(4, 4), false)[..10]);
        assert_eq!(parse(&data), Err(FenceLogError::TruncatedRecord));
        let mut other = header().to_vec();
        other[4] = FENCE_LOG_VERSION + 1;
        assert_eq!(
            parse(&other),
            Err(FenceLogError::UnsupportedVersion(FENCE_LOG_VERSION + 1))
        );
        assert_eq!(parse(b"WALRxxxx"), Err(FenceLogError::NotAFenceLog));
    }
    #[test]
    fn g11_abort_settlement_survives_reload_reset_and_retention() {
        let dir = tempfile::tempdir().unwrap();
        let log = FenceLog::new(dir.path().to_str());
        log.reset(FenceRecords::default()).unwrap();
        let aborted = test_hlc(1, 1);
        let fenced = test_hlc(2, 1);
        log.record(&aborted).unwrap();
        log.record(&fenced).unwrap();
        assert!(log.load(true).unwrap().aborted.is_empty());
        log.record_abort(&aborted).unwrap();
        // Repeated fences cannot erase an abort or count it twice.
        log.record(&aborted).unwrap();
        log.record_abort(&aborted).unwrap();
        assert_eq!(log.len(), 2);
        let recovered = log.load(true).unwrap();
        assert_eq!(recovered.aborted, [aborted].into_iter().collect());
        let now_ms = fence_retention().as_millis() as u64 + 1_000;
        let retained = fences_to_keep(&recovered, &[aborted].into_iter().collect(), now_ms);
        assert_eq!(retained.fenced, [aborted].into_iter().collect());
        assert_eq!(retained.aborted, [aborted].into_iter().collect());
        let reloaded = FenceLog::new(dir.path().to_str());
        reloaded.reset(retained).unwrap();
        let recovered = reloaded.load(true).unwrap();
        assert_eq!(recovered.aborted, [aborted].into_iter().collect());
        assert!(fences_to_keep(&recovered, &BTreeSet::new(), now_ms)
            .aborted
            .is_empty());
    }

    #[test]
    fn g11_unreadable_settlements_and_version_two_are_typed_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FENCE_LOG_FILE);
        let load_error = |bytes: &[u8]| {
            std::fs::write(&path, bytes).unwrap();
            let error = FenceLog::new(dir.path().to_str()).load(true).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            error
                .get_ref()
                .unwrap()
                .downcast_ref::<FenceLogError>()
                .unwrap()
                .clone()
        };
        let mut old = header();
        old[4] = 2;
        assert_eq!(load_error(&old), FenceLogError::UnsupportedVersion(2));
        assert_eq!(load_error(&header()[..4]), FenceLogError::TruncatedHeader);
        let mut bytes = header().to_vec();
        bytes.extend_from_slice(&record(&test_hlc(1, 1), true));
        bytes[HEADER_SIZE] ^= 1;
        assert_eq!(load_error(&bytes), FenceLogError::ChecksumMismatch);
        bytes[HEADER_SIZE] ^= 1;
        bytes[HEADER_SIZE + 16..HEADER_SIZE + 20].copy_from_slice(&2u32.to_le_bytes());
        let crc = crc32c(&bytes[HEADER_SIZE..HEADER_SIZE + 20]);
        bytes[HEADER_SIZE + 20..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(load_error(&bytes), FenceLogError::InvalidDecision(2));
        assert_eq!(
            load_error(&bytes[..bytes.len() - 1]),
            FenceLogError::TruncatedRecord
        );
    }
}
