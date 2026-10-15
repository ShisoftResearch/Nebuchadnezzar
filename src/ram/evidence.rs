//! Decision evidence for transactions that span sites.
//!
//! A participant that is in doubt asks its peers what became of a
//! transaction. A peer that committed must answer `Committed` for as long as
//! any participant can still ask. Two things used to end that answer early:
//!
//! * The live answer came from a bounded memory of recent decisions, so later
//!   transactions evicted it.
//! * After a restart the answer came from the COMMIT records that recovery
//!   found, and the cleaner drops COMMIT records when it compacts a segment.
//!
//! This module keeps the answer until no participant can ask. A retained
//! transaction PINS every segment that holds one of its COMMIT records, and
//! the cleaner does not compact a pinned segment. The pin goes when every
//! site that could ask reports that it does not hold the transaction open.
//!
//! A second kind of pin protects a site's own evidence. A site that resolved
//! an in-doubt transaction after a restart writes its COMMIT into a new
//! segment, because a recovered segment never becomes a write head again. The
//! transaction's BEGIN stays in the old segment. If the cleaner compacted the
//! new segment first, the next restart would find the BEGIN without a COMMIT
//! and put the transaction in doubt again. Such a transaction is "fragile":
//! its COMMIT segments stay pinned until no segment with an open part of it
//! remains.

use crate::ram::chunk::Chunk;
use crate::server::transactions::TxnId;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// A segment as a commit manifest names it: chunk id and segment seq id.
pub type SegmentRef = (usize, u64);

/// What a store keeps about committed transactions that other sites, or its
/// own next recovery, may still need.
pub struct DecisionEvidence {
    state: Mutex<EvidenceState>,
}

#[derive(Default)]
struct EvidenceState {
    /// Committed here. A peer may still be in doubt about it.
    retained: BTreeMap<TxnId, Retained>,
    /// Committed here through a COMMIT in another segment than an open part.
    fragile: BTreeMap<TxnId, Fragile>,
    /// In doubt here after recovery, and not resolved yet.
    resolving: BTreeSet<TxnId>,
}

struct Retained {
    /// The sites that can ask. `None` after a restart: the participant set
    /// lived in memory, so every member of the group can ask.
    participants: Option<BTreeSet<u64>>,
    commits: Vec<SegmentRef>,
}

struct Fragile {
    commits: Vec<SegmentRef>,
    open: Vec<SegmentRef>,
}

/// A retained transaction and the sites that must report it closed.
pub struct ReleaseCandidate {
    pub txn: TxnId,
    pub participants: Option<BTreeSet<u64>>,
}

/// A committed transaction as recovery found it.
pub struct RecoveredCommit {
    pub txn: TxnId,
    /// Segments that hold a COMMIT of the transaction.
    pub commits: Vec<SegmentRef>,
    /// Segments that hold a part of the transaction whose BEGIN no COMMIT
    /// in the same segment closes. Not empty means fragile.
    pub open: Vec<SegmentRef>,
}

fn pin_all(chunks: &[Chunk], segments: &[SegmentRef]) {
    for (chunk_id, seq) in segments {
        if let Some(chunk) = chunks.get(*chunk_id) {
            chunk.pin_evidence_segment(*seq);
        }
    }
}

fn unpin_all(chunks: &[Chunk], segments: &[SegmentRef]) {
    for (chunk_id, seq) in segments {
        if let Some(chunk) = chunks.get(*chunk_id) {
            chunk.unpin_evidence_segment(*seq);
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

    /// Keep the answer for a transaction this store committed.
    ///
    /// The caller must hold the transaction's write heads, so that no
    /// segment in `commits` can be compacted before the pin is in place.
    pub fn retain_commit(
        &self,
        chunks: &[Chunk],
        txn: TxnId,
        participants: Option<BTreeSet<u64>>,
        commits: Vec<SegmentRef>,
    ) {
        let commits = dedup(commits);
        let mut state = self.state.lock();
        match state.retained.get_mut(&txn) {
            Some(existing) => {
                existing.participants = match (existing.participants.take(), participants) {
                    (Some(mut known), Some(more)) => {
                        known.extend(more);
                        Some(known)
                    }
                    _ => None,
                };
                let added: Vec<SegmentRef> = commits
                    .into_iter()
                    .filter(|segment| !existing.commits.contains(segment))
                    .collect();
                pin_all(chunks, &added);
                existing.commits.extend(added);
            }
            None => {
                pin_all(chunks, &commits);
                state.retained.insert(
                    txn,
                    Retained {
                        participants,
                        commits,
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
        in_doubt: impl IntoIterator<Item = TxnId>,
    ) {
        for commit in committed {
            if !commit.open.is_empty() {
                self.retain_fragile(
                    chunks,
                    commit.txn.clone(),
                    commit.commits.clone(),
                    commit.open,
                );
            }
            self.retain_commit(chunks, commit.txn, None, commit.commits);
        }
        self.state.lock().resolving.extend(in_doubt);
    }

    /// Whether this store committed `txn` and still keeps the evidence.
    pub fn holds_commit(&self, txn: &TxnId) -> bool {
        let state = self.state.lock();
        state.retained.contains_key(txn) || state.fragile.contains_key(txn)
    }

    /// Recovery resolution decided `txn`; this store no longer asks about it.
    pub fn end_resolving(&self, txn: &TxnId) {
        self.state.lock().resolving.remove(txn);
    }

    /// The transactions this store is resolving after recovery.
    pub fn resolving(&self) -> Vec<TxnId> {
        self.state.lock().resolving.iter().cloned().collect()
    }

    /// The retained transactions, for one release round.
    pub fn release_candidates(&self) -> Vec<ReleaseCandidate> {
        self.state
            .lock()
            .retained
            .iter()
            .map(|(txn, retained)| ReleaseCandidate {
                txn: txn.clone(),
                participants: retained.participants.clone(),
            })
            .collect()
    }

    /// Stop retaining `txns`: no site can ask about them any more.
    pub fn release(&self, chunks: &[Chunk], txns: &[TxnId]) -> usize {
        let mut state = self.state.lock();
        let mut released = 0;
        for txn in txns {
            if let Some(retained) = state.retained.remove(txn) {
                unpin_all(chunks, &retained.commits);
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
            unpin_all(chunks, &retained.commits);
        }
        state.resolving.clear();
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

    /// `(retained, fragile)` transaction counts.
    pub fn counts(&self) -> (usize, usize) {
        let state = self.state.lock();
        (state.retained.len(), state.fragile.len())
    }
}

impl Default for DecisionEvidence {
    fn default() -> Self {
        Self::new()
    }
}
