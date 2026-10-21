//! The durable record of participant fences (D29).
//!
//! A site in doubt about a multi-site transaction FENCES every participant
//! before it aborts: a fenced site refuses the coordinator's `end` for that
//! transaction, and refuses a `prepare` of it. The site records the fence
//! here, and syncs it, BEFORE it answers `Fenced`: the answer is a promise
//! that must survive a crash and a power loss.
//!
//! Records use 64 independent append-only files beside the WAL files.
//! Version 2 assigns each transaction to a file with `fence_shard`.
//! Version 1 is refused; this code does not read or migrate its single log.
//!
//! ```text
//! header (8 bytes)
//!   magic    [u8; 4]  b"NEBF"
//!   version  u8       FENCE_LOG_VERSION
//!   reserved [u8; 3]
//! record (20 bytes), repeated
//!   txn ts   u64
//!   txn node u64
//!   crc      u32      CRC32C over the 16 bytes before it
//! ```
//!
//! A record that is cut short or fails its CRC ends the log: it was not
//! synced, so the site never answered `Fenced` for it. Another version, or
//! another magic, is a typed error that stops the start (§10.1).
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
pub const FENCE_LOG_VERSION: u8 = 2;
const FENCE_SHARDS: usize = 64;
const HEADER_SIZE: usize = 8;
const RECORD_SIZE: usize = 20;
pub const FENCE_LOG_FILE: &str = "txn-fences.nfence";

/// Why a fence log cannot be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceLogError {
    /// The file does not start with `FENCE_LOG_MAGIC`.
    NotAFenceLog,
    /// The file has a version that this build does not read.
    UnsupportedVersion(u8),
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

fn record(txn: &TxnId) -> [u8; RECORD_SIZE] {
    let mut bytes = [0u8; RECORD_SIZE];
    bytes[..8].copy_from_slice(&txn.ts.to_le_bytes());
    bytes[8..16].copy_from_slice(&txn.node.to_le_bytes());
    let crc = crc32c(&bytes[..16]);
    bytes[16..].copy_from_slice(&crc.to_le_bytes());
    bytes
}

/// Read every fence a log holds.
fn parse(data: &[u8]) -> Result<BTreeSet<TxnId>, FenceLogError> {
    if data.len() < HEADER_SIZE {
        // The header is synced when the file is created; a shorter file
        // was created and never synced, so it holds no fence.
        return Ok(BTreeSet::new());
    }
    if data[..4] != FENCE_LOG_MAGIC {
        return Err(FenceLogError::NotAFenceLog);
    }
    if data[4] != FENCE_LOG_VERSION {
        return Err(FenceLogError::UnsupportedVersion(data[4]));
    }
    let mut fences = BTreeSet::new();
    let mut cursor = HEADER_SIZE;
    while cursor + RECORD_SIZE <= data.len() {
        let bytes = &data[cursor..cursor + RECORD_SIZE];
        let crc = u32::from_le_bytes(bytes[16..].try_into().unwrap());
        if crc != crc32c(&bytes[..16]) {
            break;
        }
        fences.insert(TxnId {
            ts: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            node: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        });
        cursor += RECORD_SIZE;
    }
    Ok(fences)
}

struct State {
    file: Option<File>,
    fenced: BTreeSet<TxnId>,
}

struct FenceShard {
    path: Option<PathBuf>,
    state: Mutex<State>,
}

fn fence_shard(txn: &TxnId) -> usize {
    // This mapping is part of version 2. Keep it independent of Rust's Hash.
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
                    fenced: BTreeSet::new(),
                }),
            }),
            count: AtomicUsize::new(0),
        }
    }

    /// Read the fences of the last run. Called once at start, before the
    /// store serves. `recover` false means a fresh store: the old log is
    /// dropped.
    pub fn load(&self, recover: bool) -> io::Result<BTreeSet<TxnId>> {
        let mut fences = BTreeSet::new();
        if !recover {
            return Ok(fences);
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
            fences.extend(
                parse(&data).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
            );
        }
        Ok(fences)
    }

    /// Keep `fences` and only those: write them to a new log, sync it, and
    /// put it in place of the old one.
    pub fn reset(&self, fences: BTreeSet<TxnId>) -> io::Result<()> {
        let count = fences.len();
        let mut partitions: [BTreeSet<TxnId>; FENCE_SHARDS] =
            std::array::from_fn(|_| BTreeSet::new());
        for txn in fences {
            partitions[fence_shard(&txn)].insert(txn);
        }
        for (i, (shard, fences)) in self.shards.iter().zip(partitions).enumerate() {
            let mut state = crate::profile_lock!(H14, shard.state.lock());
            if let Some(path) = &shard.path {
                // Keep shard zero's header as the layout marker, even empty.
                if i != 0 && fences.is_empty() && !path.exists() {
                    state.file = None;
                    state.fenced = fences;
                    continue;
                }
                if let Some(dir) = path.parent() {
                    std::fs::create_dir_all(dir)?;
                }
                let staging = path.with_extension("nfence-new");
                {
                    let mut file = File::create(&staging)?;
                    file.write_all(&header())?;
                    for txn in &fences {
                        file.write_all(&record(txn))?;
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
                    (HEADER_SIZE + fences.len() * RECORD_SIZE) as u64,
                );
                state.file = Some(OpenOptions::new().append(true).open(path)?);
            }
            state.fenced = fences;
        }
        self.count.store(count, Ordering::Release);
        Ok(())
    }

    /// Record a fence of `txn`, synced. Returns only when a power loss can
    /// no longer drop it.
    pub fn record(&self, txn: &TxnId) -> io::Result<()> {
        let shard = &self.shards[fence_shard(txn)];
        let mut state = crate::profile_lock!(H14, shard.state.lock());
        if state.fenced.contains(txn) {
            return Ok(());
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
            file.write_all(&record(txn))?;
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
        state.fenced.insert(txn.clone());
        self.count.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn is_fenced(&self, txn: &TxnId) -> bool {
        if self.count.load(Ordering::Acquire) == 0 {
            return false;
        }
        crate::profile_lock!(H14, self.shards[fence_shard(txn)].state.lock())
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
    previous: &BTreeSet<TxnId>,
    in_doubt: &BTreeSet<TxnId>,
    now_ms: u64,
) -> BTreeSet<TxnId> {
    let horizon = now_ms.saturating_sub(fence_retention().as_millis() as u64);
    previous
        .iter()
        .filter(|txn| in_doubt.contains(*txn) || txn.wall_ms() >= horizon)
        .cloned()
        .collect()
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
        log.reset(BTreeSet::new()).unwrap();
        log.record(&test_hlc(1, 1)).unwrap();
        log.record(&test_hlc(2, 1)).unwrap();
        assert!(log.is_fenced(&test_hlc(1, 1)));
        let reloaded = FenceLog::new(Some(&path));
        let fences = reloaded.load(true).unwrap();
        assert_eq!(fences.len(), 2);
        reloaded
            .reset([test_hlc(2, 1)].into_iter().collect())
            .unwrap();
        let again = FenceLog::new(Some(&path)).load(true).unwrap();
        assert_eq!(again, [test_hlc(2, 1)].into_iter().collect());
    }

    #[test]
    fn independent_fence_files_do_not_wait_for_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let log = std::sync::Arc::new(FenceLog::new(dir.path().to_str()));
        log.reset(BTreeSet::new()).unwrap();
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
            FenceLog::new(dir.path().to_str()).load(true).unwrap(),
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
    fn a_torn_record_ends_the_log_and_another_version_is_refused() {
        let mut data = header().to_vec();
        data.extend_from_slice(&record(&test_hlc(3, 3)));
        data.extend_from_slice(&record(&test_hlc(4, 4))[..10]);
        assert_eq!(parse(&data).unwrap().len(), 1);
        let mut other = header().to_vec();
        other[4] = FENCE_LOG_VERSION + 1;
        assert_eq!(
            parse(&other),
            Err(FenceLogError::UnsupportedVersion(FENCE_LOG_VERSION + 1))
        );
        assert_eq!(parse(b"WALRxxxx"), Err(FenceLogError::NotAFenceLog));
    }
}
