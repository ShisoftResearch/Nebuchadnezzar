//! Repair records for failed index tasks (model-check finding D-F11).
//!
//! An index task runs after its cell write. When it fails, the cell is
//! written and its index entries are not, so an index query misses the row
//! with no error. Before this module the failure was only logged and
//! counted, and the index stayed wrong until the cell was written again or
//! the store restarted.
//!
//! Now a failed task records a repair for its cell, and a worker re-asserts
//! the entries that the cell implies. While a schema has a pending repair, an
//! index query of that schema answers [`IndexRepairPending`], not fewer rows.
//! The design is the shipped configuration of `docs/tla/IndexRepair.tla`.
//!
//! * The records are memory. A crash loses them, and the recovery reconcile
//!   (`scrub::reconcile_index_before_serving`, Repair by default) derives the
//!   whole index again before the store serves. With the reconcile off, a
//!   crash can lose a repair (`IndexRepairNoReconcile` fails), as it can lose
//!   pending index tasks without this module.
//! * The worker only inserts. A stale entry (a remove that failed) stays, and
//!   a query drops it when it reads the cell of the candidate; the full scrub
//!   prunes it. A remove by the worker could race a write that implies the
//!   entry again.
//! * A record has a generation. The worker removes a record only if no new
//!   failure of the cell came while it worked (`IndexRepairNoGen` fails).
//! * Full-text entries are not re-asserted: their insert is not idempotent.
//!   A failed task of a full-text cell is repaired for its other index types.
//! * The records are per process. A query served by the indexer clients of
//!   another process does not see them.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bifrost::rpc::RPCError;
use dovahkiin::types::Id;

use crate::ram::schema::SchemaUid;
use crate::utils::keyed_state::KeyedState;

/// One pending repair of a cell.
struct CellRepair {
    schema: SchemaUid,
    /// Incremented by each new failure of the cell. The worker removes the
    /// record only at the generation it read before it read the cell.
    generation: u64,
}

/// The pending repairs of one database, per cell and per schema.
pub struct IndexRepairs {
    cells: KeyedState<Id, CellRepair>,
    /// Pending repairs per schema family. Updated under the entry of the
    /// cell, so it is never below the number of records of the schema.
    schemas: KeyedState<u64, usize>,
    /// All pending repairs: the fast path of a query is one atomic load.
    pending: AtomicUsize,
}

impl Default for IndexRepairs {
    fn default() -> Self {
        Self {
            cells: KeyedState::ordered(),
            schemas: KeyedState::default(),
            pending: AtomicUsize::new(0),
        }
    }
}

impl IndexRepairs {
    /// Record that an index task of `cell` (of the schema family `schema`)
    /// failed.
    pub fn record(&self, cell: Id, schema: SchemaUid) {
        let mut entry = self.cells.entry(&cell);
        match entry.as_mut() {
            Some(repair) => repair.generation += 1,
            None => {
                *entry = Some(CellRepair {
                    schema,
                    generation: 0,
                });
                *self
                    .schemas
                    .entry(&(schema.get() as u64))
                    .get_or_insert(0) += 1;
                self.pending.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    /// The cells with a pending repair, and the generation of each.
    pub(crate) fn snapshot(&self) -> Vec<(Id, u64)> {
        self.cells
            .keys()
            .filter_map(|cell| {
                let entry = self.cells.get(&cell)?;
                entry.as_ref().map(|repair| (cell, repair.generation))
            })
            .collect()
    }

    /// Remove the record of `cell` if it is still at `generation`. Returns
    /// whether it was removed.
    pub(crate) fn clear(&self, cell: &Id, generation: u64) -> bool {
        let mut entry = self.cells.entry(cell);
        let Some(repair) = entry.as_ref() else {
            return false;
        };
        if repair.generation != generation {
            return false;
        }
        let schema = repair.schema;
        *entry = None;
        let mut count = self.schemas.entry(&(schema.get() as u64));
        match count.as_mut() {
            Some(n) if *n > 1 => *n -= 1,
            _ => *count = None,
        }
        self.pending.fetch_sub(1, Ordering::AcqRel);
        true
    }

    /// How many cells have a pending repair.
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    /// Whether the schema family `schema` has a pending repair.
    pub fn is_pending(&self, schema: SchemaUid) -> bool {
        if self.pending.load(Ordering::Acquire) == 0 {
            return false;
        }
        self.schemas
            .get(&(schema.get() as u64))
            .is_some_and(|count| count.is_some())
    }

    /// `Err(IndexRepairPending)` if the schema family `schema` has a pending
    /// repair: an index answer for it can miss rows.
    pub fn refuse_if_pending(&self, schema: SchemaUid) -> Result<(), RPCError> {
        if self.is_pending(schema) {
            return Err(IndexRepairPending { schema }.into_rpc_error());
        }
        Ok(())
    }
}

/// An index query of a schema whose index has a pending repair: a failed
/// index task left a cell without its entries, so the answer could miss rows.
/// Retryable when the repair is done. It is carried inside
/// `RPCError::IOError`; use [`IndexRepairPending::of`] to find it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexRepairPending {
    pub schema: SchemaUid,
}

impl IndexRepairPending {
    /// This refusal as the error of a query API.
    pub fn into_rpc_error(self) -> RPCError {
        RPCError::IOError(std::io::Error::other(self))
    }

    /// The refusal inside an error from a query API, if it is one.
    pub fn of(error: &RPCError) -> Option<&IndexRepairPending> {
        match error {
            RPCError::IOError(error) => error.get_ref()?.downcast_ref(),
            _ => None,
        }
    }
}

impl std::fmt::Display for IndexRepairPending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the index of schema {} has a pending repair of a failed index task; the query \
             is refused until the repair is done",
            self.schema.get()
        )
    }
}

impl std::error::Error for IndexRepairPending {}

/// Shared handle, as `IndexerClients` keeps it.
pub type SharedIndexRepairs = Arc<IndexRepairs>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_is_cleared_only_at_its_generation() {
        let repairs = IndexRepairs::default();
        let cell = Id::allocated(0, 1, 7);
        let schema = SchemaUid(5);
        assert!(!repairs.is_pending(schema));
        repairs.record(cell, schema);
        let (_, generation) = repairs.snapshot()[0];
        // A new failure of the same cell while the worker works.
        repairs.record(cell, schema);
        assert!(!repairs.clear(&cell, generation));
        assert!(repairs.is_pending(schema));
        assert!(repairs.refuse_if_pending(schema).is_err());
        assert!(IndexRepairPending::of(&repairs.refuse_if_pending(schema).unwrap_err()).is_some());
        let (_, generation) = repairs.snapshot()[0];
        assert!(repairs.clear(&cell, generation));
        assert!(!repairs.is_pending(schema));
        assert_eq!(repairs.pending(), 0);
        assert!(repairs.refuse_if_pending(SchemaUid(6)).is_ok());
    }
}
