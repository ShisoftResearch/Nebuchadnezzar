//! Segment ownership that survives lease release and head-slot reservation.

use crate::ram::durability::Receipt;
use crate::server::transactions::TxnId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

const CLEANING: usize = 1 << (usize::BITS - 1);
const RETIRED: usize = 1 << (usize::BITS - 2);

#[derive(Default)]
pub(crate) struct Protection {
    /// Remains set while a pool slot temporarily contains ALLOCATING.
    pub head: AtomicBool,
    /// Set before rotation unpublishes the head; cleared after the seal job.
    pub seal_pending: AtomicBool,
    activity: AtomicUsize,
    transactions: Mutex<HashMap<TxnId, Option<Arc<Vec<Receipt>>>>>,
}

impl Protection {
    pub fn retain_transaction(&self, txn: &TxnId) {
        self.transactions.lock().insert(txn.clone(), None);
    }

    pub fn decide_transaction(&self, txn: &TxnId, receipts: Arc<Vec<Receipt>>) {
        self.transactions.lock().insert(txn.clone(), Some(receipts));
    }

    pub fn abort_transaction(&self, txn: &TxnId) {
        self.transactions.lock().remove(txn);
    }

    pub fn transaction_pending(&self) -> bool {
        let mut transactions = self.transactions.lock();
        transactions.retain(|_, decision| {
            decision.as_ref().map_or(true, |receipts| {
                !receipts
                    .iter()
                    .all(|receipt| receipt.file.covers_receipt(receipt.offset, receipt.epoch))
            })
        });
        !transactions.is_empty()
    }

    pub fn in_progress(&self) -> bool {
        self.head.load(Ordering::Acquire)
            || self.seal_pending.load(Ordering::Acquire)
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
        self.claim_cleaner().then_some(Maintenance(self))
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
