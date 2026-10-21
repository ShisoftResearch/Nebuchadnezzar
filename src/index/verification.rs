//! Query checks use the same keys as the writer and recovery reconcile.
use crate::index::full_text::ToOwnedValue;
use crate::index::{
    builder::probe_cell_indices,
    ranged::tree::service::{Range, RangeTerm},
};
use crate::ram::{
    cell::{Cell, ReadError},
    schema::{Schema, SchemaUid},
    types::{values_semantically_equal, Id, OwnedValue, Value},
};
use serde::{Deserialize, Serialize};

static VERIFIED_READ_BATCHES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
static VERIFIED_CELL_READS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Candidate cell reads requested for index verification.
pub fn verified_cell_reads() -> usize {
    VERIFIED_CELL_READS.load(std::sync::atomic::Ordering::Relaxed)
}

/// RPC batches used for index verification.
pub fn verified_read_batches() -> usize {
    VERIFIED_READ_BATCHES.load(std::sync::atomic::Ordering::Relaxed)
}

pub(crate) fn record_verified_cell_reads(count: usize) {
    VERIFIED_READ_BATCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    VERIFIED_CELL_READS.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
}

/// Verification for hash and range index candidates.
/// Existence permits stale predicates but never returns a missing cell.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum Verify {
    #[default]
    Exists,
    Predicate,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum IndexCheck {
    Schema(SchemaUid),
    Ranged(Range),
    Hashed {
        buckets: Vec<Id>,
        field: u64,
        value: OwnedValue,
    },
}

pub(crate) fn matches<C: Cell>(
    cell: &C,
    schema: &Schema,
    checks: &[IndexCheck],
) -> Result<(), ReadError>
where
    <C::Value as Value>::Out: ToOwnedValue,
{
    #[cfg(test)]
    if control("NEB_TEST_IDCONTRACT_SKIP_VERIFY") {
        return Ok(());
    }
    if checks
        .iter()
        .any(|check| matches!(check, IndexCheck::Schema(uid) if schema.uid != *uid))
    {
        return Err(ReadError::NotMatch);
    }
    if checks
        .iter()
        .all(|check| matches!(check, IndexCheck::Schema(_)))
    {
        return Ok(());
    }
    let entries = probe_cell_indices(cell, schema);
    let scannable = schema
        .is_scannable
        .then(|| crate::index::EntryKey::for_scannable(&cell.id(), schema.uid));
    for check in checks {
        let matched = match check {
            IndexCheck::Schema(uid) => schema.uid == *uid,
            IndexCheck::Ranged(range) => entries
                .iter()
                .flat_map(|entry| entry.ranged_keys())
                .chain(scannable.iter())
                .any(|key| {
                    let lower = match &range.start {
                        RangeTerm::Inclusive(bound) => key >= bound,
                        RangeTerm::Exclusive(bound) => key > bound,
                        RangeTerm::Open => true,
                    };
                    let upper = match &range.end {
                        RangeTerm::Inclusive(bound) => key <= bound,
                        RangeTerm::Exclusive(bound) => key < bound,
                        RangeTerm::Open => true,
                    };
                    lower && upper
                }),
            IndexCheck::Hashed {
                buckets,
                field,
                value,
            } => {
                entries
                    .iter()
                    .flat_map(|entry| entry.hash_buckets())
                    .any(|id| buckets.contains(id))
                    && schema.id_index.get(field).is_some_and(|path| {
                        values_semantically_equal(
                            &cell.data().get_in_by_ids(path).to_owned_value(),
                            value,
                        )
                    })
            }
        };
        if !matched {
            return Err(ReadError::NotMatch);
        }
    }
    Ok(())
}

/// Select the rules needed by this query. Key derivation remains in the writer.
pub(crate) fn query_schema(schema: &Schema, checks: &[IndexCheck]) -> Schema {
    use crate::ram::schema::IndexType;
    let mut selected = schema.clone();
    selected.compound_index_fields.clear();
    selected.index_fields.retain(|field, indices| {
        indices.retain(|index| {
            checks.iter().any(|check| match (check, index) {
                (
                    IndexCheck::Hashed { field: queried, .. },
                    IndexType::Hashed | IndexType::Null,
                ) => field == queried,
                (IndexCheck::Ranged(range), IndexType::Ranged) => {
                    match (&range.start, &range.end) {
                        (
                            RangeTerm::Inclusive(start) | RangeTerm::Exclusive(start),
                            RangeTerm::Inclusive(end) | RangeTerm::Exclusive(end),
                        ) if start.as_slice()[..8] == end.as_slice()[..8] => {
                            (field.to_owned() as u32).to_be_bytes() == start.as_slice()[4..8]
                        }
                        _ => true,
                    }
                }
                _ => false,
            })
        });
        !indices.is_empty()
    });
    selected
}

/// Isolated negative controls are enabled before each test process starts.
#[inline]
pub(crate) fn control(name: &str) -> bool {
    #[cfg(test)]
    {
        std::env::var_os(name).is_some()
    }
    #[cfg(not(test))]
    {
        let _ = name;
        false
    }
}
