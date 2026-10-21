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
use crate::utils::keyed_state::KeyedState;
use bifrost_hasher::hash_str;
use dovahkiin::types::key_hash;
use std::collections::BTreeSet;

pub const TXN_STATUS_SCHEMA: &str = "_NEB_TXN_STATUS";
const TS_FIELD: &str = "TS";
const NODE_FIELD: &str = "NODE";
const PARTICIPANTS_FIELD: &str = "PARTICIPANTS";

/// The id origin reserved for status cells.
pub const STATUS_ORIGIN: u16 = 0xFFF;
/// How many sequence slots a lookup probes per chunk.
pub const STATUS_PROBES: u64 = 8;

lazy_static! {
    pub static ref TXN_STATUS_SCHEMA_ID: SchemaVid = SchemaVid(key_hash(TXN_STATUS_SCHEMA) as u32);
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

/// A cell in the status schema that does not decode as a status cell. It
/// stops the start: a status cell decides transactions (AGENTS.md §10.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusCellError {
    /// The cell is not in the status schema.
    NotAStatusCell,
    /// A field is missing or has another type.
    Field(&'static str),
    /// A status cell names at least two participants.
    Participants(usize),
}

impl std::fmt::Display for StatusCellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatusCellError::NotAStatusCell => write!(f, "the cell is not a status cell"),
            StatusCellError::Field(name) => {
                write!(
                    f,
                    "the status cell field {} is missing or has another type",
                    name
                )
            }
            StatusCellError::Participants(count) => write!(
                f,
                "the status cell names {} participant(s); a status cell names at least 2",
                count
            ),
        }
    }
}

impl std::error::Error for StatusCellError {}

/// The transaction and participants a status cell names. The writer
/// (`status_cell`) makes one encoding only, and only that one decodes.
pub fn decode_status_cell(cell: &OwnedCell) -> Result<(TxnId, Vec<u64>), StatusCellError> {
    if cell.header.schema != *TXN_STATUS_SCHEMA_ID {
        return Err(StatusCellError::NotAStatusCell);
    }
    let ts = *cell.data[TS_FIELD]
        .u64()
        .ok_or(StatusCellError::Field(TS_FIELD))?;
    let node = *cell.data[NODE_FIELD]
        .u64()
        .ok_or(StatusCellError::Field(NODE_FIELD))?;
    let OwnedValue::PrimArray(OwnedPrimArray::U64(participants)) = &cell.data[PARTICIPANTS_FIELD]
    else {
        return Err(StatusCellError::Field(PARTICIPANTS_FIELD));
    };
    if participants.len() < 2 {
        return Err(StatusCellError::Participants(participants.len()));
    }
    Ok((TxnId { ts, node }, participants.clone()))
}

/// A slot that holds a cell of a bracket in doubt after a restart. Its
/// cells must not move: the resolution installs the committed version at
/// this site, so a copy made now would keep the version before the
/// transaction for good (review R2). The caller retries after the
/// resolution.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SlotInDoubt {
    pub slot: u32,
    pub txn: TxnId,
}

impl std::fmt::Display for SlotInDoubt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "slot {} holds a cell of transaction {:?}, which is in doubt here",
            self.slot, self.txn
        )
    }
}

impl std::error::Error for SlotInDoubt {}

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

struct Resolving {
    participants: Vec<u64>,
    cells: Vec<u64>,
}

/// Each transaction owns its status and recovery state. Ordered side indexes
/// support sweeps. Cell counters use keyed lookups only.
pub struct TxnStatusRegistry {
    live: KeyedState<TxnId, StatusEntry>,
    ids: KeyedState<u64, usize>,
    resolving: KeyedState<TxnId, Resolving>,
    in_doubt_cells: KeyedState<u64, usize>,
    in_doubt_cell_count: std::sync::atomic::AtomicUsize,
}
impl Default for TxnStatusRegistry {
    fn default() -> Self {
        Self {
            live: KeyedState::ordered(),
            ids: KeyedState::default(),
            resolving: KeyedState::ordered(),
            in_doubt_cells: KeyedState::default(),
            in_doubt_cell_count: Default::default(),
        }
    }
}
impl TxnStatusRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, txn: TxnId, entry: StatusEntry) {
        let mut state = crate::profile_lock!(H09, self.live.entry(&txn));
        *self.ids.entry(&entry.id.bits()).get_or_insert(0) += 1;
        if let Some(old) = state.replace(entry) {
            Self::release_id(&self.ids, old.id.bits());
        }
    }

    fn release_id(map: &KeyedState<u64, usize>, id: u64) -> bool {
        let mut count = map.entry(&id);
        if let Some(value) = count.as_mut() {
            *value -= 1;
            if *value == 0 {
                count.take();
                return true;
            }
        }
        false
    }

    pub fn get(&self, txn: &TxnId) -> Option<StatusEntry> {
        self.live.get(txn)?.as_ref().cloned()
    }

    pub fn remove(&self, txn: &TxnId) -> Option<StatusEntry> {
        let mut state = crate::profile_lock!(H09, self.live.get(txn)?);
        let entry = state.take()?;
        Self::release_id(&self.ids, entry.id.bits());
        // Trace for the open torn-batch item (FIX_graceful_prepare.md).
        log::info!(
            "TRACE status entry removed {:?} participants {:?} synced peers {:?} self synced {}",
            txn,
            entry.participants,
            entry.synced_peers,
            entry.self_synced
        );
        Some(entry)
    }

    pub fn is_status_id(&self, id: u64) -> bool {
        self.ids.get(&id).is_some_and(|entry| entry.is_some())
    }

    pub fn note_peer_synced(&self, txn: &TxnId, peer: u64) {
        if let Some(mut state) = self.live.get(txn) {
            if let Some(entry) = state.as_mut() {
                entry.synced_peers.insert(peer);
            }
        }
    }

    pub fn mark_self_synced(&self, txn: &TxnId) {
        if let Some(mut state) = self.live.get(txn) {
            if let Some(entry) = state.as_mut() {
                entry.self_synced = true;
            }
        }
    }

    pub fn mark_announced(&self, txn: &TxnId) {
        if let Some(mut state) = self.live.get(txn) {
            if let Some(entry) = state.as_mut() {
                entry.announced = true;
            }
        }
    }

    pub fn entries(&self) -> Vec<(TxnId, StatusEntry)> {
        self.live
            .keys()
            .filter_map(|txn| self.get(&txn).map(|entry| (txn, entry)))
            .collect()
    }

    /// Publish protection before publishing the transaction in the sweep index.
    pub fn begin_resolving(&self, txn: TxnId, participants: Vec<u64>, mut cells: Vec<u64>) {
        cells.sort_unstable();
        cells.dedup();
        let mut state = crate::profile_lock!(H09, self.resolving.entry(&txn));
        for id in &cells {
            let mut count = self.in_doubt_cells.entry(id);
            if count.is_none() {
                self.in_doubt_cell_count
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
            }
            *count.get_or_insert(0) += 1;
        }
        if let Some(old) = state.replace(Resolving {
            participants,
            cells,
        }) {
            self.forget_cells(&old.cells);
        }
    }

    pub fn end_resolving(&self, txn: &TxnId) {
        let Some(mut state) = self.resolving.get(txn) else {
            return;
        };
        if let Some(old) = state.take() {
            self.forget_cells(&old.cells);
        }
    }

    fn forget_cells(&self, cells: &[u64]) {
        for id in cells {
            if Self::release_id(&self.in_doubt_cells, *id) {
                self.in_doubt_cell_count
                    .fetch_sub(1, std::sync::atomic::Ordering::Release);
            }
        }
    }

    pub fn in_doubt_slot(&self, slots: &std::collections::HashSet<u16>) -> Option<SlotInDoubt> {
        if self
            .in_doubt_cell_count
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            return None;
        }
        self.resolving.keys().find_map(|txn| {
            let state = self.resolving.get(&txn)?;
            state.as_ref()?.cells.iter().find_map(|id| {
                let slot = Id::from_bits(*id).locality();
                slots.contains(&slot).then(|| SlotInDoubt {
                    slot: slot as u32,
                    txn: txn.clone(),
                })
            })
        })
    }

    pub fn is_in_doubt_cell(&self, id: u64) -> bool {
        if self
            .in_doubt_cell_count
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            return false;
        }
        self.in_doubt_cells
            .get(&id)
            .is_some_and(|entry| entry.is_some())
    }

    pub fn resolving(&self) -> Vec<TxnId> {
        self.resolving.keys().collect()
    }

    pub fn resolving_participants(&self, txn: &TxnId) -> Option<Vec<u64>> {
        self.resolving
            .get(txn)?
            .as_ref()
            .map(|entry| entry.participants.clone())
    }

    pub fn counts(&self) -> (usize, usize) {
        (self.live.keys().count(), self.resolving.keys().count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::transactions::test_hlc;

    #[test]
    fn transaction_status_entries_resolve_independently() {
        let registry = std::sync::Arc::new(TxnStatusRegistry::new());
        let first = test_hlc(1, 1);
        let second = test_hlc(2, 1);
        let cell = Id::from_parts(7, 8).bits();
        registry.begin_resolving(first.clone(), vec![1, 2], vec![cell]);
        let held = registry.resolving.entry(&first);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = registry.clone();
        let second_copy = second.clone();
        let thread = std::thread::spawn(move || {
            worker.begin_resolving(second_copy.clone(), vec![1, 3], vec![cell]);
            assert_eq!(
                worker.resolving_participants(&second_copy),
                Some(vec![1, 3])
            );
            done_tx.send(()).unwrap();
        });
        let result = done_rx.recv_timeout(std::time::Duration::from_secs(2));
        drop(held);
        thread.join().unwrap();
        assert!(result.is_ok());
        registry.end_resolving(&first);
        assert!(
            registry.is_in_doubt_cell(cell),
            "another transaction still protects this cell"
        );
        registry.end_resolving(&second);
        assert!(!registry.is_in_doubt_cell(cell));
        assert_eq!(registry.counts(), (0, 0));
    }

    #[test]
    fn concurrent_resolvers_keep_shared_cell_protection_until_the_last_owner() {
        let registry = TxnStatusRegistry::new();
        let cell = Id::from_parts(7, 8).bits();
        let last = test_hlc(100, 1);
        registry.begin_resolving(last.clone(), vec![1, 2], vec![cell]);
        let ready = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for node in 1..=8 {
                let registry = &registry;
                let ready = &ready;
                scope.spawn(move || {
                    let txn = test_hlc(node, 1);
                    registry.begin_resolving(txn.clone(), vec![1, 2], vec![cell, cell]);
                    ready.wait();
                    // Replacing one resolver must retain the other references.
                    registry.begin_resolving(txn.clone(), vec![2, 3], vec![cell]);
                    registry.end_resolving(&txn);
                    assert!(registry.is_in_doubt_cell(cell));
                });
            }
        });
        assert_eq!(registry.resolving(), vec![last.clone()]);
        assert_eq!(
            registry
                .in_doubt_cell_count
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        registry.end_resolving(&last);
        assert!(!registry.is_in_doubt_cell(cell));
        assert_eq!(registry.counts(), (0, 0));
        assert_eq!(
            registry
                .in_doubt_cell_count
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
    }

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
        let lone = status_cell(&txn, &id, &[9]);
        assert_eq!(
            decode_status_cell(&lone),
            Err(StatusCellError::Participants(1))
        );
    }
}
