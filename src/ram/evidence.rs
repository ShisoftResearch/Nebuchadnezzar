//! Unsettled multi-site transactions: the decision answer and the release
//! round.
//!
//! A multi-site transaction is SETTLED when every participant holds its
//! outcome durably and does not hold it open any more: no site can ask about
//! it again. Until then:
//!
//! * The segments that hold its parts on every participant stay in progress
//!   (D20 extended by D28): the cleaner never compacts them. That is
//!   `crate::ram::cleaner::protection::Protection`, the one pin system; a
//!   decided multi-site transaction stays there after its COMMIT is synced,
//!   until it is settled.
//! * The commit point answers `Committed` for it after its bounded decision
//!   memory evicted the answer. This module is that answer, and it is never
//!   evicted while the transaction is unsettled.
//!
//! A transaction of one site cannot tear across sites. It is not kept here,
//! and its COMMIT is not retained after recovery.
//!
//! The participants come from prepare, and after a restart from the BEGIN
//! records (`crate::ram::bracket::encode_begin`). A release round
//! (`DataManager::release_decision_evidence`) asks only them.

use crate::server::transactions::TxnId;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};

/// A segment as recovery and `Protection` name it: chunk id and segment id.
pub type SegmentRef = (usize, u64);

/// The unsettled multi-site transactions of one store.
pub struct DecisionEvidence {
    state: Mutex<EvidenceState>,
}

#[derive(Default)]
struct EvidenceState {
    /// Committed here, not settled yet.
    retained: BTreeMap<TxnId, Retained>,
    /// In doubt here after recovery, and not resolved yet. The value is the
    /// participant set.
    resolving: BTreeMap<TxnId, Vec<u64>>,
}

struct Retained {
    /// The whole participant set, this site included.
    participants: BTreeSet<u64>,
    /// The segments whose `Protection` holds the transaction until it is
    /// settled.
    segments: Vec<SegmentRef>,
}

/// A retained transaction and the sites that must report it closed.
pub struct ReleaseCandidate {
    pub txn: TxnId,
    pub peers: BTreeSet<u64>,
}

/// A committed transaction as recovery found it.
pub struct RecoveredCommit {
    pub txn: TxnId,
    /// The segments with a BEGIN of the transaction.
    pub segments: Vec<SegmentRef>,
    /// The participant set its BEGIN records name. Empty when no BEGIN of
    /// it remains.
    pub participants: Vec<u64>,
}

impl DecisionEvidence {
    pub fn new() -> Self {
        DecisionEvidence {
            state: Mutex::new(EvidenceState::default()),
        }
    }

    /// Keep a multi-site transaction this store committed until it is
    /// settled. Returns false, and keeps nothing, for a transaction of one
    /// site.
    pub fn retain_commit(
        &self,
        txn: TxnId,
        participants: BTreeSet<u64>,
        mut segments: Vec<SegmentRef>,
    ) -> bool {
        if participants.len() < 2 {
            return false;
        }
        let mut state = self.state.lock();
        let retained = state.retained.entry(txn).or_insert_with(|| Retained {
            participants: BTreeSet::new(),
            segments: Vec::new(),
        });
        retained.participants.extend(participants);
        retained.segments.append(&mut segments);
        retained.segments.sort_unstable();
        retained.segments.dedup();
        true
    }

    /// A transaction in doubt after recovery.
    pub fn begin_resolving(&self, txn: TxnId, participants: Vec<u64>) {
        self.state.lock().resolving.insert(txn, participants);
    }

    /// Whether this store committed `txn` and it is not settled yet.
    pub fn holds_commit(&self, txn: &TxnId) -> bool {
        self.state.lock().retained.contains_key(txn)
    }

    /// Recovery resolution decided `txn`; this store no longer asks about
    /// it.
    pub fn end_resolving(&self, txn: &TxnId) {
        self.state.lock().resolving.remove(txn);
    }

    /// The transactions this store is resolving after recovery.
    pub fn resolving(&self) -> Vec<TxnId> {
        self.state.lock().resolving.keys().cloned().collect()
    }

    /// The participant set of a transaction this store is resolving.
    pub fn resolving_participants(&self, txn: &TxnId) -> Option<Vec<u64>> {
        self.state.lock().resolving.get(txn).cloned()
    }

    /// The retained transactions, for one release round of site `me`.
    pub fn release_candidates(&self, me: u64) -> Vec<ReleaseCandidate> {
        self.state
            .lock()
            .retained
            .iter()
            .map(|(txn, retained)| ReleaseCandidate {
                txn: txn.clone(),
                peers: retained
                    .participants
                    .iter()
                    .copied()
                    .filter(|peer| *peer != me)
                    .collect(),
            })
            .collect()
    }

    /// `txns` are settled. Returns each one with the segments to settle in
    /// `Protection`.
    pub fn release(&self, txns: &[TxnId]) -> Vec<(TxnId, Vec<SegmentRef>)> {
        let mut state = self.state.lock();
        txns.iter()
            .filter_map(|txn| {
                state
                    .retained
                    .remove(txn)
                    .map(|retained| (txn.clone(), retained.segments))
            })
            .collect()
    }

    /// Stop retaining everything: this store runs no transaction service,
    /// so no peer can ask it anything.
    pub fn release_all(&self) -> Vec<(TxnId, Vec<SegmentRef>)> {
        std::mem::take(&mut self.state.lock().retained)
            .into_iter()
            .map(|(txn, retained)| (txn, retained.segments))
            .collect()
    }

    /// `(retained, resolving)` transaction counts.
    pub fn counts(&self) -> (usize, usize) {
        let state = self.state.lock();
        (state.retained.len(), state.resolving.len())
    }
}

impl Default for DecisionEvidence {
    fn default() -> Self {
        Self::new()
    }
}
