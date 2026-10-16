//! The status cell of a multi-site transaction (D31, TLA+ variant E2).
//!
//! Each participant of a multi-site transaction appends one STATUS CELL
//! inside the transaction's bracket, before the COMMIT, under the same sync.
//! The cell says "this site committed T" and names the participants. It is an
//! ordinary cell in a hidden system schema, so:
//!
//! * the cleaner carries it like any live cell: compaction drops the bracket
//!   markers but keeps the decision (no pin lifetime beyond D20);
//! * recovery installs it like any committed cell, and decides an open
//!   bracket of T from it;
//! * a peer in doubt gets the answer from it by id (`txn_outcome`).
//!
//! It is tombstoned only when EVERY participant's COMMIT is synced (TLA+ F4):
//! each participant reports `synced(T)` to the others after its own sync, and
//! a slow sweep asks `txn_status` for the rest.
//!
//! Ids: `Id::allocated(chunk, STATUS_ORIGIN, sequence)`. The origin
//! `STATUS_ORIGIN` is never given to an allocator (`id_alloc_sm::claim_origin`
//! skips it), and the sequence is a 36-bit hash of the transaction id plus a
//! probe number, so two transactions whose hashes collide take different
//! probes. The cell lives in a chunk the transaction already writes, so it
//! adds no bracket part. Lookups probe every chunk.
//!
//! Hidden: cell scans, cell counts and slot migration skip status cells
//! (`Chunks::is_status_cell`, used by `Chunks::live_cell_ids_of_chunk` and
//! `Chunks::cell_ids_in_slots`);
//! the schema has no indexed field and is not scannable, so the indexer and
//! the scrub ignore it. Morpheus excludes it from dumps by name.

use crate::ram::cell::OwnedCell;
use crate::ram::schema::{Field, Schema, SchemaVid};
use crate::ram::types::{Id, Map, OwnedMap, OwnedPrimArray, OwnedValue, Type};
use crate::server::transactions::TxnId;
use bifrost_hasher::hash_str;
use dovahkiin::types::key_hash;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashSet};

pub const TXN_STATUS_SCHEMA: &str = "_NEB_TXN_STATUS";
const TS_FIELD: &str = "TS";
const NODE_FIELD: &str = "NODE";
const PARTICIPANTS_FIELD: &str = "PARTICIPANTS";

/// The id origin reserved for status cells.
pub const STATUS_ORIGIN: u16 = 0xFFF;
/// How many sequence slots a lookup probes per chunk.
pub const STATUS_PROBES: u64 = 8;

lazy_static! {
    pub static ref TXN_STATUS_SCHEMA_ID: SchemaVid =
        SchemaVid(key_hash(TXN_STATUS_SCHEMA) as u32);
}

/// The hidden schema of status cells. Registered locally on every store,
/// before recovery: it is the same everywhere and needs no consensus.
pub fn txn_status_schema() -> Schema {
    Schema::new_with_id(
        TXN_STATUS_SCHEMA_ID.get(),
        &TXN_STATUS_SCHEMA.to_string(),
        None,
        Field::new_schema(vec![
            Field::new_unindexed(TS_FIELD, Type::U64),
            Field::new_unindexed(NODE_FIELD, Type::U64),
            Field::new_unindexed_array(PARTICIPANTS_FIELD, Type::U64),
        ]),
        false,
        false,
    )
}

fn sequence_of(txn: &TxnId) -> u64 {
    hash_str(&format!("TXN-STATUS-{}-{}", txn.ts, txn.node))
}

/// The id of probe `probe` for `txn` in chunk `chunk`.
pub fn status_id(txn: &TxnId, chunk: usize, probe: u64) -> Id {
    Id::allocated(
        chunk as u16,
        STATUS_ORIGIN,
        sequence_of(txn).wrapping_add(probe),
    )
}

/// The status cell of `txn` at `id`.
pub fn status_cell(txn: &TxnId, id: &Id, participants: &[u64]) -> OwnedCell {
    let mut map = OwnedMap::new();
    map.insert(TS_FIELD, OwnedValue::U64(txn.ts));
    map.insert(NODE_FIELD, OwnedValue::U64(txn.node));
    map.insert(
        PARTICIPANTS_FIELD,
        OwnedValue::PrimArray(OwnedPrimArray::U64(participants.to_vec())),
    );
    OwnedCell::new_with_id(*TXN_STATUS_SCHEMA_ID, id, OwnedValue::Map(map))
}

/// The transaction and participants a status cell names.
pub fn decode_status_cell(cell: &OwnedCell) -> Option<(TxnId, Vec<u64>)> {
    if cell.header.schema != *TXN_STATUS_SCHEMA_ID {
        return None;
    }
    let ts = *cell.data[TS_FIELD].u64()?;
    let node = *cell.data[NODE_FIELD].u64()?;
    let participants = match &cell.data[PARTICIPANTS_FIELD] {
        OwnedValue::PrimArray(OwnedPrimArray::U64(ids)) => ids.clone(),
        OwnedValue::Array(values) => values.iter().filter_map(|v| v.u64().copied()).collect(),
        _ => Vec::new(),
    };
    Some((TxnId { ts, node }, participants))
}

/// A status cell recovery found live in the index.
pub struct FoundStatusCell {
    pub txn: TxnId,
    pub chunk: usize,
    pub id: Id,
    pub participants: Vec<u64>,
}

/// A segment as `Protection` names it: chunk id and segment id.
pub type SegmentRef = (usize, u64);

/// A status cell this store holds.
#[derive(Clone)]
pub struct StatusEntry {
    pub id: Id,
    pub chunk: usize,
    pub participants: Vec<u64>,
    /// This site's COMMIT and status cell are synced.
    pub self_synced: bool,
    /// The other participants that reported their COMMIT synced.
    pub synced_peers: BTreeSet<u64>,
    /// The segments of this site's bracket parts. `self_synced` becomes true
    /// when none of them holds the transaction in `Protection` any more.
    pub segments: Vec<SegmentRef>,
    /// `synced(T)` was sent to the other participants.
    pub announced: bool,
    pub since: std::time::Instant,
}

#[derive(Default)]
struct State {
    live: BTreeMap<TxnId, StatusEntry>,
    ids: HashSet<u64>,
    /// In doubt after recovery, and not resolved yet: the participants.
    resolving: BTreeMap<TxnId, Vec<u64>>,
}

/// The status cells of one store, and the transactions it resolves.
#[derive(Default)]
pub struct TxnStatusRegistry {
    state: Mutex<State>,
}

impl TxnStatusRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, txn: TxnId, entry: StatusEntry) {
        let mut state = self.state.lock();
        state.ids.insert(entry.id.bits());
        if let Some(old) = state.live.insert(txn, entry) {
            if !state.live.values().any(|other| other.id == old.id) {
                state.ids.remove(&old.id.bits());
            }
        }
    }

    pub fn get(&self, txn: &TxnId) -> Option<StatusEntry> {
        self.state.lock().live.get(txn).cloned()
    }

    pub fn remove(&self, txn: &TxnId) -> Option<StatusEntry> {
        let mut state = self.state.lock();
        let entry = state.live.remove(txn)?;
        state.ids.remove(&entry.id.bits());
        Some(entry)
    }

    /// Whether `id` is a status cell of this store: scans and migration skip
    /// it.
    pub fn is_status_id(&self, id: u64) -> bool {
        let state = self.state.lock();
        !state.ids.is_empty() && state.ids.contains(&id)
    }

    pub fn note_peer_synced(&self, txn: &TxnId, peer: u64) {
        if let Some(entry) = self.state.lock().live.get_mut(txn) {
            entry.synced_peers.insert(peer);
        }
    }

    pub fn mark_self_synced(&self, txn: &TxnId) {
        if let Some(entry) = self.state.lock().live.get_mut(txn) {
            entry.self_synced = true;
        }
    }

    pub fn mark_announced(&self, txn: &TxnId) {
        if let Some(entry) = self.state.lock().live.get_mut(txn) {
            entry.announced = true;
        }
    }

    pub fn entries(&self) -> Vec<(TxnId, StatusEntry)> {
        self.state
            .lock()
            .live
            .iter()
            .map(|(txn, entry)| (txn.clone(), entry.clone()))
            .collect()
    }

    pub fn begin_resolving(&self, txn: TxnId, participants: Vec<u64>) {
        self.state.lock().resolving.insert(txn, participants);
    }

    pub fn end_resolving(&self, txn: &TxnId) {
        self.state.lock().resolving.remove(txn);
    }

    pub fn resolving(&self) -> Vec<TxnId> {
        self.state.lock().resolving.keys().cloned().collect()
    }

    pub fn resolving_participants(&self, txn: &TxnId) -> Option<Vec<u64>> {
        self.state.lock().resolving.get(txn).cloned()
    }

    /// `(status cells, resolving)` counts.
    pub fn counts(&self) -> (usize, usize) {
        let state = self.state.lock();
        (state.live.len(), state.resolving.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::transactions::test_hlc;

    #[test]
    fn a_status_cell_round_trips_and_its_ids_are_reserved() {
        let txn = test_hlc(77, 5);
        let id = status_id(&txn, 3, 0);
        assert_eq!(id.origin(), STATUS_ORIGIN);
        assert_eq!(id.locality(), 3);
        assert!(!id.is_hashed());
        assert_ne!(status_id(&txn, 3, 1), id);
        let cell = status_cell(&txn, &id, &[9, 4]);
        let (decoded, participants) = decode_status_cell(&cell).unwrap();
        assert_eq!(decoded, txn);
        assert_eq!(participants, vec![9, 4]);
    }
}
