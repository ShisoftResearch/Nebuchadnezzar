//! Unsettled multi-site transactions, and the cleaner invariant that keeps
//! them whole.
//!
//! THE INVARIANT (D20, extended by D28): the cleaner never compacts a segment
//! that is in progress. A segment is in progress while it holds any part --
//! BEGIN, data entries or COMMIT -- of a multi-site transaction that is not
//! SETTLED at every participant. Settled means every other participant
//! answered a release round and does not hold the transaction open: it holds
//! the outcome durably (its COMMIT synced, or its abort final), so no site
//! can ask about the transaction again. Until then compaction could remove
//! the evidence a site needs to decide, and tear the transaction. After it,
//! compaction turns the entries into plain cells, as for any transaction.
//!
//! A transaction of one site cannot tear across sites. It is never pinned,
//! and its COMMIT is not retained after recovery.
//!
//! Three kinds of entry hold pins:
//!
//! * RETAINED: a multi-site transaction this site committed. Every
//!   participant keeps one, not only the commit point: the segments that hold
//!   its parts stay pinned, and the commit point answers `Committed` from it
//!   when its bounded decision memory has evicted the answer. It goes in a
//!   release round, when every other participant reports that it does not
//!   hold the transaction open (see `DataManager::release_decision_evidence`).
//!   The participants come from prepare, and after a restart from the BEGIN
//!   records (`crate::ram::bracket::encode_begin`).
//! * FRAGILE: a transaction committed through a COMMIT that is not in the
//!   segment of an open part. A site that resolved an in-doubt transaction
//!   after a restart writes such a COMMIT into a new segment, because a
//!   recovered segment never becomes a write head again; a close that fell
//!   back to a standalone COMMIT does the same. If the cleaner compacted the
//!   COMMIT segment first, the next restart would find the BEGIN without a
//!   COMMIT and put the transaction in doubt again, after the commit point
//!   forgot it. So the COMMIT segments stay pinned until no segment with an
//!   open part remains, also after the transaction is settled.
//! * RESOLVING: a transaction in doubt after a restart. Its cells are not in
//!   the index, so the cleaner would drop them as dead. The open parts stay
//!   pinned until resolution ends.

use crate::ram::chunk::Chunk;
use crate::server::transactions::TxnId;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// A segment as a commit manifest names it: chunk id and segment seq id.
pub type SegmentRef = (usize, u64);

/// The unsettled multi-site transactions of one store.
pub struct DecisionEvidence {
    state: Mutex<EvidenceState>,
}

#[derive(Default)]
struct EvidenceState {
    retained: BTreeMap<TxnId, Retained>,
    fragile: BTreeMap<TxnId, Fragile>,
    resolving: BTreeMap<TxnId, Resolving>,
}

struct Retained {
    /// The whole participant set, this site included.
    participants: BTreeSet<u64>,
    /// Pinned: the segments that hold a part of the transaction here.
    segments: Vec<SegmentRef>,
}

struct Fragile {
    /// Pinned: the segments with a COMMIT that closes no part in its own
    /// segment.
    commits: Vec<SegmentRef>,
    /// The segments with the parts those COMMITs decide.
    open: Vec<SegmentRef>,
}

struct Resolving {
    participants: Vec<u64>,
    /// Pinned until resolution ends.
    open: Vec<SegmentRef>,
}

/// A retained transaction and the sites that must report it closed.
pub struct ReleaseCandidate {
    pub txn: TxnId,
    pub peers: BTreeSet<u64>,
}

/// A committed transaction as recovery found it.
pub struct RecoveredCommit {
    pub txn: TxnId,
    /// Segments that hold a COMMIT of the transaction.
    pub commits: Vec<SegmentRef>,
    /// Segments that hold a part of the transaction whose BEGIN no COMMIT
    /// in the same segment closes. Not empty means fragile.
    pub open: Vec<SegmentRef>,
    /// The participant set its BEGIN records name. Empty when no BEGIN of
    /// it remains: then the transaction was settled and compacted, and only
    /// a fragile COMMIT can be left.
    pub participants: Vec<u64>,
}

/// A transaction in doubt after recovery, as the evidence keeps it.
pub struct RecoveredInDoubt {
    pub txn: TxnId,
    pub participants: Vec<u64>,
    pub open: Vec<SegmentRef>,
}

fn pin_all(chunks: &[Chunk], segments: &[SegmentRef]) {
    for (chunk_id, seq) in segments {
        if let Some(chunk) = chunks.get(*chunk_id) {
            chunk.pin_in_progress_segment(*seq);
        }
    }
}

fn unpin_all(chunks: &[Chunk], segments: &[SegmentRef]) {
    for (chunk_id, seq) in segments {
        if let Some(chunk) = chunks.get(*chunk_id) {
            chunk.unpin_in_progress_segment(*seq);
        }
    }
}

fn dedup(mut segments: Vec<SegmentRef>) -> Vec<SegmentRef> {
    segments.sort_unstable();
    segments.dedup();
    segments
}

impl DecisionEvidence {
    pub fn new() -> Self {
        DecisionEvidence {
            state: Mutex::new(EvidenceState::default()),
        }
    }

    /// Keep a multi-site transaction this store committed until it is
    /// settled. `segments` are the segments that hold its parts here. A
    /// transaction of one site is not kept.
    ///
    /// The caller must make sure that the cleaner cannot take a segment in
    /// `segments` before the pin is in place: it holds the transaction's
    /// write heads, or the segments are pinned already.
    pub fn retain_commit(
        &self,
        chunks: &[Chunk],
        txn: TxnId,
        participants: BTreeSet<u64>,
        segments: Vec<SegmentRef>,
    ) {
        if participants.len() < 2 {
            return;
        }
        let segments = dedup(segments);
        let mut state = self.state.lock();
        match state.retained.get_mut(&txn) {
            Some(existing) => {
                existing.participants.extend(participants);
                let added: Vec<SegmentRef> = segments
                    .into_iter()
                    .filter(|segment| !existing.segments.contains(segment))
                    .collect();
                pin_all(chunks, &added);
                existing.segments.extend(added);
            }
            None => {
                pin_all(chunks, &segments);
                state.retained.insert(
                    txn,
                    Retained {
                        participants,
                        segments,
                    },
                );
            }
        }
    }

    /// Keep this store's own COMMIT segments of `txn` until no segment in
    /// `open` remains.
    pub fn retain_fragile(
        &self,
        chunks: &[Chunk],
        txn: TxnId,
        commits: Vec<SegmentRef>,
        open: Vec<SegmentRef>,
    ) {
        let commits = dedup(commits);
        let open = dedup(open);
        let mut state = self.state.lock();
        if let Some(existing) = state.fragile.remove(&txn) {
            unpin_all(chunks, &existing.commits);
        }
        pin_all(chunks, &commits);
        state.fragile.insert(txn, Fragile { commits, open });
    }

    /// Install what recovery found. Called once, before the cleaner runs.
    pub fn retain_recovered(
        &self,
        chunks: &[Chunk],
        committed: Vec<RecoveredCommit>,
        in_doubt: Vec<RecoveredInDoubt>,
    ) {
        for commit in committed {
            if !commit.open.is_empty() {
                self.retain_fragile(
                    chunks,
                    commit.txn.clone(),
                    commit.commits.clone(),
                    commit.open.clone(),
                );
            }
            let segments: Vec<SegmentRef> =
                commit.commits.into_iter().chain(commit.open).collect();
            self.retain_commit(
                chunks,
                commit.txn,
                commit.participants.into_iter().collect(),
                segments,
            );
        }
        let mut state = self.state.lock();
        for bracket in in_doubt {
            let open = dedup(bracket.open);
            pin_all(chunks, &open);
            if let Some(existing) = state.resolving.insert(
                bracket.txn,
                Resolving {
                    participants: bracket.participants,
                    open,
                },
            ) {
                unpin_all(chunks, &existing.open);
            }
        }
    }

    /// Whether this store committed `txn` and still keeps it.
    pub fn holds_commit(&self, txn: &TxnId) -> bool {
        let state = self.state.lock();
        state.retained.contains_key(txn) || state.fragile.contains_key(txn)
    }

    /// Recovery resolution decided `txn`; this store no longer asks about
    /// it. The open parts stay pinned if a commit retained them.
    pub fn end_resolving(&self, chunks: &[Chunk], txn: &TxnId) {
        if let Some(resolving) = self.state.lock().resolving.remove(txn) {
            unpin_all(chunks, &resolving.open);
        }
    }

    /// The transactions this store is resolving after recovery.
    pub fn resolving(&self) -> Vec<TxnId> {
        self.state.lock().resolving.keys().cloned().collect()
    }

    /// The participant set of a transaction this store is resolving.
    pub fn resolving_participants(&self, txn: &TxnId) -> Option<Vec<u64>> {
        self.state
            .lock()
            .resolving
            .get(txn)
            .map(|resolving| resolving.participants.clone())
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

    /// `txns` are settled: unpin their segments.
    pub fn release(&self, chunks: &[Chunk], txns: &[TxnId]) -> usize {
        let mut state = self.state.lock();
        let mut released = 0;
        for txn in txns {
            if let Some(retained) = state.retained.remove(txn) {
                unpin_all(chunks, &retained.segments);
                released += 1;
            }
        }
        released
    }

    /// Stop retaining everything: this store runs no transaction service,
    /// so no peer can ask it anything.
    pub fn release_all_retained(&self, chunks: &[Chunk]) {
        let mut state = self.state.lock();
        for (_, retained) in std::mem::take(&mut state.retained) {
            unpin_all(chunks, &retained.segments);
        }
    }

    /// Release the fragile transactions whose open parts are all gone.
    ///
    /// A part's segment goes only by compaction, which drops its BEGIN with
    /// it, so from then on no restart can find that part open.
    pub fn release_settled_fragile(&self, chunks: &[Chunk]) -> usize {
        let mut present: HashMap<usize, BTreeSet<u64>> = HashMap::new();
        let mut state = self.state.lock();
        if state.fragile.is_empty() {
            return 0;
        }
        let settled: Vec<TxnId> = state
            .fragile
            .iter()
            .filter(|(_, fragile)| {
                fragile.open.iter().all(|(chunk_id, seq)| {
                    let seqs = present.entry(*chunk_id).or_insert_with(|| {
                        chunks
                            .get(*chunk_id)
                            .map(|chunk| chunk.segments().iter().map(|seg| seg.seq_id).collect())
                            .unwrap_or_default()
                    });
                    !seqs.contains(seq)
                })
            })
            .map(|(txn, _)| txn.clone())
            .collect();
        for txn in &settled {
            if let Some(fragile) = state.fragile.remove(txn) {
                unpin_all(chunks, &fragile.commits);
            }
        }
        settled.len()
    }

    /// `(retained, fragile, resolving)` transaction counts.
    pub fn counts(&self) -> (usize, usize, usize) {
        let state = self.state.lock();
        (
            state.retained.len(),
            state.fragile.len(),
            state.resolving.len(),
        )
    }
}

impl Default for DecisionEvidence {
    fn default() -> Self {
        Self::new()
    }
}
