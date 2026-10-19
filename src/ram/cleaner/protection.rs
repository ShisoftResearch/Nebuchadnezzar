//! Segment ownership that survives lease release and head-slot reservation.

use crate::ram::durability::Receipt;
use crate::server::transactions::TxnId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Test only: restore the pin without pruning, as the first fix had it.
#[cfg(test)]
pub(crate) static NO_PRUNE_ON_PIN: AtomicBool = AtomicBool::new(false);

const CLEANING: usize = 1 << (usize::BITS - 1);
const RETIRED: usize = 1 << (usize::BITS - 2);

#[derive(Default)]
pub(crate) struct Protection {
    /// Remains set while a pool slot temporarily contains ALLOCATING.
    pub head: AtomicBool,
    /// Set before rotation unpublishes the head; cleared after the seal job.
    pub seal_pending: AtomicBool,
    /// Set when the seal job has drained the segment and synced its WAL. From
    /// then on, the cleaner may reclaim the segment before its archive.
    pub synced: AtomicBool,
    /// Seal attempts that left a synced, mostly dead segment to the cleaner.
    pub reclaim_waits: AtomicUsize,
    /// Set when the seal job closes the segment for appends. It never clears:
    /// a rotated segment never takes another append, so nothing reopens its
    /// journal gate.
    pub rotated: AtomicBool,
    /// Set while the segment is counted in `SEAL_WAITING_SEGMENTS`.
    waiting: AtomicBool,
    activity: AtomicUsize,
    transactions: Mutex<HashMap<TxnId, Option<Arc<Vec<Receipt>>>>>,
}

impl Protection {
    pub fn retain_transaction(&self, txn: &TxnId) {
        self.transactions.lock().insert(txn.clone(), None);
    }

    pub fn decide_transaction(&self, txn: &TxnId, receipts: Arc<Vec<Receipt>>) {
        for receipt in receipts.iter() {
            receipt.file.request_sync(receipt.offset, receipt.epoch);
        }
        self.transactions.lock().insert(txn.clone(), Some(receipts));
    }

    /// Keep this segment in progress until `tokens` are covered by a sync.
    ///
    /// Used for the segment of an OLD version that `txn` replaced or removed.
    /// If the COMMIT of `txn` is lost in a power loss, recovery rolls the
    /// transaction back, and the old version is the cell. The entry stays
    /// until the COMMIT is durable.
    ///
    /// `tokens` hold no WAL descriptor (`Receipt::token`). Covered entries
    /// are removed here, so a segment that the cleaner never examines keeps
    /// only the pins that are still uncovered, plus the new one.
    pub fn pin_until_covered(&self, txn: &TxnId, tokens: Arc<Vec<Receipt>>) {
        let mut transactions = self.transactions.lock();
        #[cfg(test)]
        let prune = !NO_PRUNE_ON_PIN.load(Ordering::Relaxed);
        #[cfg(not(test))]
        let prune = true;
        if prune {
            Self::prune(&mut transactions);
        }
        transactions.insert(txn.clone(), Some(tokens));
    }

    /// The number of transactions that keep this segment in progress.
    #[cfg(test)]
    pub fn pin_count(&self) -> usize {
        self.transactions.lock().len()
    }

    fn prune(transactions: &mut HashMap<TxnId, Option<Arc<Vec<Receipt>>>>) {
        transactions.retain(|_, decision| {
            decision.as_ref().map_or(true, |receipts| {
                !receipts
                    .iter()
                    .all(|receipt| receipt.file.covers_receipt(receipt.offset, receipt.epoch))
            })
        });
    }

    pub fn abort_transaction(&self, txn: &TxnId) {
        self.transactions.lock().remove(txn);
    }

    /// Whether `txn` still keeps this segment in progress: in flight, or
    /// decided with receipts that a sync has not covered yet.
    pub fn holds_transaction(&self, txn: &TxnId) -> bool {
        self.transaction_pending();
        self.transactions.lock().contains_key(txn)
    }

    pub fn transaction_pending(&self) -> bool {
        let mut transactions = self.transactions.lock();
        Self::prune(&mut transactions);
        !transactions.is_empty()
    }

    /// Count this segment as waiting for the cleaner. True on the first call.
    pub fn enter_wait(&self) -> bool {
        let first = !self.waiting.swap(true, Ordering::AcqRel);
        if first {
            let now = crate::ram::segs::SEAL_WAITING_SEGMENTS.fetch_add(1, Ordering::Relaxed) + 1;
            crate::ram::segs::SEAL_WAITING_PEAK.fetch_max(now, Ordering::Relaxed);
        }
        first
    }

    /// Stop counting this segment as waiting. Safe to call more than once.
    pub fn leave_wait(&self) {
        if self.waiting.swap(false, Ordering::AcqRel) {
            crate::ram::segs::SEAL_WAITING_SEGMENTS.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn in_progress(&self) -> bool {
        self.head.load(Ordering::Acquire)
            || self.seal_pending.load(Ordering::Acquire)
            || self.activity.load(Ordering::Acquire) & !CLEANING != 0
            || self.transaction_pending()
    }

    /// The cleaner must not select, claim or remove this segment.
    ///
    /// Like `in_progress`, except that a seal job blocks the cleaner only until
    /// it has drained the segment and synced its WAL. The seal job closes the
    /// journal gate of the segment before that drain, and the gate stays closed
    /// (`rotated`). So after `synced`, the segment takes no append, and its WAL
    /// holds every acknowledged write. A reclaim then makes its archive
    /// unnecessary. The cleaner claim and the archive exclude each other.
    pub fn blocks_cleaner(&self) -> bool {
        self.head.load(Ordering::Acquire)
            || (self.seal_pending.load(Ordering::Acquire) && !self.synced.load(Ordering::Acquire))
            || self.activity.load(Ordering::Acquire) & !CLEANING != 0
            || self.transaction_pending()
    }

    /// Exclude jobs before the cleaner tests ownership. Failure means skip.
    pub fn claim_cleaner(&self) -> bool {
        self.activity
            .compare_exchange(0, CLEANING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn maintenance(&self) -> Option<Maintenance<'_>> {
        self.claim_cleaner().then(|| Maintenance(self))
    }

    pub fn release_cleaner(&self) {
        self.activity.fetch_and(!CLEANING, Ordering::Release);
    }

    pub fn retire(&self) {
        self.activity.fetch_or(RETIRED, Ordering::AcqRel);
    }

    pub fn jobs_drained(&self) -> bool {
        self.activity.load(Ordering::Acquire) & !RETIRED == 0
    }

    /// A sync can race maintenance. Wait before taking the file lock.
    pub fn sync_job(&self, timeout: std::time::Duration) -> std::io::Result<Job<'_>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(job) = self.job() {
                return Ok(job);
            }
            if self.activity.load(Ordering::Acquire) & RETIRED != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "segment retired",
                ));
            }
            if std::time::Instant::now() >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "segment maintenance did not finish",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// Jobs share a count. The cleaner excludes their start until it releases.
    pub fn job(&self) -> Option<Job<'_>> {
        self.activity
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state & (CLEANING | RETIRED) == 0).then_some(state + 1)
            })
            .ok()?;
        Some(Job(self))
    }
}

pub(crate) struct Job<'a>(&'a Protection);

impl Drop for Job<'_> {
    fn drop(&mut self) {
        self.0.activity.fetch_sub(1, Ordering::Release);
    }
}

pub(crate) struct Maintenance<'a>(&'a Protection);

impl Drop for Maintenance<'_> {
    fn drop(&mut self) {
        self.0.release_cleaner();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decided_uncovered_pin_requests_sync_without_an_ack_waiter() {
        use std::io::Write;
        let directory = tempfile::tempdir().unwrap();
        let mut descriptor = std::fs::File::create(directory.path().join("wal")).unwrap();
        descriptor.write_all(&[7; 16]).unwrap();
        let descriptor = Arc::new(descriptor);
        let file = crate::ram::durability::DurableFile::new(0, 1, 1);
        file.open(&descriptor, 0, directory.path().to_str().unwrap())
            .unwrap();
        file.observe(16);
        let protection = Protection::default();
        let txn = crate::server::transactions::test_hlc(9843, 1);
        protection.retain_transaction(&txn);
        protection.decide_transaction(
            &txn,
            Arc::new(vec![Receipt {
                file: file.clone(),
                offset: 16,
                epoch: 0,
                descriptor: Some(descriptor),
            }]),
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while protection.transaction_pending() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(
            !protection.transaction_pending(),
            "a decided pin needs a sync trigger"
        );
        assert!(file.covers_receipt(16, 0));
    }

    #[test]
    fn refused_maintenance_keeps_the_existing_claim() {
        let protection = Protection::default();
        let claim = protection.maintenance().unwrap();
        assert!(protection.maintenance().is_none());
        assert!(protection.job().is_none());
        drop(claim);
        assert!(protection.job().is_some());
    }

    #[test]
    fn sync_waits_for_maintenance_and_has_a_deadline() {
        let protection = Protection::default();
        let claim = protection.maintenance().unwrap();
        std::thread::scope(|scope| {
            let (sent, received) = std::sync::mpsc::channel();
            let protection = &protection;
            scope.spawn(move || {
                sent.send(
                    protection
                        .sync_job(std::time::Duration::from_secs(2))
                        .map(|_| ()),
                )
                .unwrap();
            });
            assert!(received
                .recv_timeout(std::time::Duration::from_millis(20))
                .is_err());
            drop(claim);
            received
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap()
                .unwrap();
        });
        let _claim = protection.maintenance().unwrap();
        assert_eq!(
            protection
                .sync_job(std::time::Duration::from_millis(1))
                .err()
                .unwrap()
                .kind(),
            std::io::ErrorKind::TimedOut
        );
    }

    /// A pin holds no WAL descriptor: an unlinked WAL file is not kept open.
    #[test]
    fn a_pin_token_holds_no_wal_descriptor() {
        use std::io::Write;
        let directory = tempfile::tempdir().unwrap();
        let mut file = std::fs::File::create(directory.path().join("wal")).unwrap();
        file.write_all(&[7; 16]).unwrap();
        let descriptor = Arc::new(file);
        let durable = crate::ram::durability::DurableFile::new(0, 1, 1);
        durable
            .open(&descriptor, 0, directory.path().to_str().unwrap())
            .unwrap();
        durable.observe(16);
        let receipt = Receipt {
            file: durable,
            offset: 16,
            epoch: 0,
            descriptor: Some(descriptor.clone()),
        };
        let token = receipt.token();
        assert!(token.descriptor.is_none());
        let protection = Protection::default();
        let txn = crate::server::transactions::test_hlc(9844, 1);
        protection.pin_until_covered(&txn, Arc::new(vec![token]));
        drop(receipt);
        // The test holds one reference. A pin that kept the receipt would
        // hold a second one.
        assert_eq!(Arc::strong_count(&descriptor), 1);
    }
}
