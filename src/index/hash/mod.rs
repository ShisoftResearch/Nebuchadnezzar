use crate::ram::cell::{Cell, ReadError, WriteError, MAX_CELL_SIZE};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Bucket-size accounting for the hashed index.
pub static HASH_BUCKET_LEN_SUM: AtomicU64 = AtomicU64::new(0);
pub static HASH_BUCKET_SAMPLES: AtomicU64 = AtomicU64::new(0);
pub static HASH_BUCKET_MAX: AtomicU64 = AtomicU64::new(0);
use crate::ram::schema::{Field, Schema, SchemaUid, SchemaVid};
use crate::ram::types::*;
use crate::{client::AsyncClient, ram::cell::OwnedCell};
use bifrost::rpc::RPCError;
use bifrost_hasher::hash_str;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

use super::Feature;
use crate::index::full_text::ToOwnedValue;

const MAX_CAS_RETRIES: u32 = 1000;

const HASH_BUCKET_FORMAT_VERSION: u32 = 3;

const HASH_SCHEMA: &'static str = "HASH_INDEX_SCHEMA";
const HASH_INDEX_FIELD: &'static str = "CELL_ID";
/// Link to the rest of a bucket that has outgrown one cell.
const HASH_NEXT_FIELD: &'static str = "NEXT";
/// The head of the chain this segment belongs to.
///
/// A bucket id is a one-way hash of (schema, field, value feature), and a
/// spilled segment's id is a one-way hash of (head, version), so nothing in a
/// segment's own id says which bucket it is part of. The recovery reconcile
/// must ask "does the cell this entry names still imply THIS bucket", and it
/// can only ask that of the bucket, not of a segment. `OWNER` is what makes an
/// entry self-describing, the way a ranged key already is.
const HASH_OWNER_FIELD: &'static str = "OWNER";

/// Runaway guard when walking a bucket chain. A well-formed chain is
/// `bucket_len / BUCKET_CAPACITY` cells; anything beyond this is a cycle or
/// corruption, and walking it forever would hang a query.
pub(crate) const MAX_CHAIN_HOPS: usize = 1_000_000;

lazy_static! {
    // The physical schema id is the format marker in each persisted cell header.
    pub(crate) static ref HASH_INDEX_SCHEMA_V1_ID: SchemaVid = SchemaVid(key_hash(HASH_SCHEMA) as u32);
    pub(crate) static ref HASH_INDEX_SCHEMA_V2_ID: SchemaVid = SchemaVid(key_hash("HASH_INDEX_SCHEMA:2") as u32);
    pub static ref HASH_INDEX_SCHEMA_ID: SchemaVid = SchemaVid(key_hash(&format!("{HASH_SCHEMA}:{HASH_BUCKET_FORMAT_VERSION}")) as u32);
    pub static ref HASH_INDEX_FIELD_ID: u64 = hash_str(HASH_INDEX_FIELD);
    pub static ref HASH_NEXT_FIELD_ID: u64 = hash_str(HASH_NEXT_FIELD);
    pub static ref HASH_OWNER_FIELD_ID: u64 = hash_str(HASH_OWNER_FIELD);
    /// Ids one bucket cell holds before the bucket spills into a chain.
    ///
    /// A cell is capped at `MAX_CELL_SIZE`, so before chaining a bucket could
    /// hold ~131k ids and every insert past that failed `CellIsTooLarge` --
    /// silently, because a failed index task was only logged. Chaining removes
    /// the ceiling; this bound decides how much gets rewritten per append.
    ///
    /// It is a HARD SAFETY CAP, not a tuning hint, and it is deliberately not
    /// derived from `Type::Id.size()`. The encoded cost per id exceeds the
    /// nominal one, so arithmetic from the nominal figure puts the cap ABOVE
    /// what actually fits -- precisely the failure the cap exists to prevent.
    /// It is anchored to a measurement instead, and divided down. The env
    /// override can only lower it.
    ///
    /// Smaller means cheaper appends and longer chains; a walk is one read per
    /// cell and a query re-reads every member anyway.
    pub static ref BUCKET_CAPACITY: usize = {
        /// Most ids a single cell was ever observed to hold before the store
        /// refused the next append -- that refusal reported
        /// `CellIsTooLarge(1252352)` against a 1 MiB limit, so the true
        /// encoded cost is ~9.55 B/id rather than the nominal 8.
        const OBSERVED_MAX_IDS_PER_CELL: usize = 131_066;
        // A QUARTER of a cell measured full, and the divisor is set by churn
        // rather than by the size limit. Segment size is what an append
        // rewrites, so it decides how fast dead versions pile into the one
        // chunk holding a hot bucket. Halving the observed max clears
        // CellIsTooLarge but doubles that churn, and on a 2.79M-edge import
        // over 16 values that was enough to exhaust a chunk again:
        //
        //   cap 65,533  ->  89,132 entries lost, 791 alloc failures,  72K/s
        //   cap 32,768  ->  0 lost,              0 alloc failures,   112K/s
        //
        // Quartering is both correct and faster. Do not raise this to "as
        // much as fits" -- what fits is not the binding constraint.
        let ceiling = OBSERVED_MAX_IDS_PER_CELL / 4;
        std::env::var("NEB_HASH_BUCKET_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(ceiling)
            .min(ceiling)
            .max(1)
    };
}

/// Id of the cell a bucket's contents are frozen into when the head spills.
///
/// Keyed by the head version the contents were read at, so two writers racing
/// to spill the same head write DIFFERENT cells and neither can publish the
/// other's contents under the winning pointer.
fn spilled_cell_id(index_id: &Id, version: u64) -> Id {
    Id::from_obj(&(*index_id, version, "HASH_BUCKET_SPILL"))
}

/// Coalescing state for one hash bucket.
///
/// `pending` is guarded by a plain mutex and is only ever held for an insert
/// or a drain, never across an await; `apply` is the async latch that makes
/// one caller the applier for the whole queue.
#[derive(Default)]
struct Bucket {
    pending: Mutex<HashSet<Id>>,
    apply: TokioMutex<()>,
    blocking_removals: AtomicUsize,
    #[cfg(test)]
    removal_handoffs: AtomicUsize,
}

pub struct HashIndexer {
    neb_client: Arc<AsyncClient>,
    /// Per-bucket coalescing slots. Deliberately per-indexer rather than
    /// process-wide: bucket ids derive from (schema, field, value) and so can
    /// repeat across databases, and folding two databases' inserts into one
    /// apply would write them through the wrong client.
    buckets: Mutex<HashMap<Id, Arc<Bucket>>>,
}

impl HashIndexer {
    fn retryable_write_error(err: &WriteError) -> bool {
        matches!(
            err,
            WriteError::CellVersionMismatch
                | WriteError::UserCanceledUpdate
                | WriteError::DeletionPredictionFailed
                | WriteError::NetworkingError
        )
    }

    fn retryable_read_error(err: &ReadError) -> bool {
        matches!(
            err,
            ReadError::NetworkingError
                | ReadError::SegmentPromotionFailed
                | ReadError::DecompressionFailed(_)
        )
    }

    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        HashIndexer {
            neb_client: neb_client.clone(),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Coalescing slot for one bucket, created on demand.
    fn bucket(&self, index_id: &Id) -> Arc<Bucket> {
        crate::profile_lock!(H19, self.buckets
            .lock())
            .entry(*index_id)
            .or_insert_with(|| Arc::new(Bucket::default()))
            .clone()
    }

    /// Drop a bucket's slot once nothing is queued on it, so the map does not
    /// accumulate an entry per distinct indexed value -- which, for the
    /// near-unique fields this index is built for, would be one per cell.
    /// Call only after releasing your own handle: a strong count of 1 then
    /// means the map is the last owner. Racing a newcomer is harmless -- it
    /// gets a fresh slot, coalesces with nobody, and its CAS still applies.
    fn release_bucket(&self, index_id: &Id) {
        let mut buckets = crate::profile_lock!(H19, self.buckets.lock());
        let drop_it = buckets
            .get(index_id)
            .is_some_and(|b| Arc::strong_count(b) == 1 && b.pending.lock().is_empty());
        if drop_it {
            buckets.remove(index_id);
        }
    }

    /// Add `cell_id` to the bucket at `index_id`.
    ///
    /// Concurrent inserts into the SAME bucket are coalesced: a burst folds
    /// into one read-modify-CAS carrying every queued id, instead of one
    /// rewrite per id.
    ///
    /// Why that matters. A bucket is a single cell holding a flat array which
    /// is read, scanned and written whole on every update, so appending N ids
    /// one at a time rewrites ~N^2/2 ids. That is invisible while the design
    /// assumption below holds -- indexed values near-unique, so buckets are
    /// singletons -- and catastrophic when it does not: a low-cardinality
    /// indexed field (a region id, a status enum) funnels millions of updates
    /// into a handful of cells, and the dead versions saturate the segment
    /// table of the one chunk that holds each of them. Allocation then fails
    /// `CannotAllocateSpace` and the index write is DROPPED, losing the entry
    /// silently. Measured before this coalescing, on a 2.79M-row import over
    /// 16 distinct values at 64-way concurrency: 145,099 entries lost.
    /// Batching divides the quadratic term by the batch size, and the batch
    /// grows with exactly the contention that causes the problem.
    ///
    /// Coalescing is an optimisation only -- correctness never depends on two
    /// callers finding the same slot, because the apply is the same CAS loop
    /// it always was. The caller's contract is unchanged: this returns only
    /// once `cell_id` is durably in the bucket, so a write's index entry is
    /// still visible by the time the write completes. That is why this is a
    /// combining latch and not a timed buffer -- a buffer would have to
    /// either break that guarantee or block on a flush timer.
    pub async fn add_index(&self, cell_id: &Id, index_id: &Id) -> Result<(), WriteError> {
        debug!(
            "Attempting to add index for cell_id: {:?}, index_id: {:?}",
            cell_id, index_id
        );

        let bucket = self.bucket(index_id);
        bucket.pending.lock().insert(*cell_id);

        let result = {
            // One applier at a time per bucket; everyone else queues here and
            // their ids ride the batch of whoever is already inside.
            let _apply = bucket.apply.lock().await;

            // Take whatever accumulated while we waited. If our own id is
            // gone, an earlier applier committed it -- a failing applier puts
            // its batch back before releasing this lock, so "absent" can only
            // mean "applied".
            let batch: Vec<Id> = {
                let mut pending = bucket.pending.lock();
                if pending.contains(cell_id) {
                    pending.drain().collect()
                } else {
                    Vec::new()
                }
            };

            if batch.is_empty() {
                Ok(())
            } else {
                let applied = self.apply_batch(index_id, &batch).await;
                if applied.is_err() {
                    // Hand the batch back so the callers queued behind us
                    // retry it instead of inheriting our failure.
                    bucket.pending.lock().extend(batch);
                }
                applied
            }
        };

        drop(bucket);
        self.release_bucket(index_id);
        result
    }

    /// Fold a batch of ids into the bucket cell in one read-modify-CAS.
    ///
    /// `batch` must be free of duplicates; `add_index` drains it from a set.
    /// Ids held directly in one bucket cell.
    fn cell_ids(cell: &OwnedCell) -> Option<&Vec<Id>> {
        match &cell[*HASH_INDEX_FIELD_ID] {
            OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) => Some(ids),
            _ => None,
        }
    }

    /// Next cell in the bucket chain, if this one has spilled. Null, NA and a
    /// missing field all mean "end of chain".
    fn cell_next(cell: &OwnedCell) -> Option<Id> {
        match &cell[*HASH_NEXT_FIELD_ID] {
            OwnedValue::Id(id) => Some(*id),
            _ => None,
        }
    }

    /// `owner` is the head of the chain this cell belongs to; a head is its own
    /// owner.
    fn bucket_cell(owner: &Id, index_id: &Id, ids: Vec<Id>, next: Option<Id>) -> OwnedCell {
        let mut map = OwnedMap::new();
        map.insert_key_id(
            *HASH_INDEX_FIELD_ID,
            OwnedValue::PrimArray(OwnedPrimArray::Id(ids)),
        );
        map.insert_key_id(
            *HASH_NEXT_FIELD_ID,
            match next {
                Some(id) => OwnedValue::Id(id),
                None => OwnedValue::Null,
            },
        );
        map.insert_key_id(*HASH_OWNER_FIELD_ID, OwnedValue::Id(*owner));
        OwnedCell::new_with_id(*HASH_INDEX_SCHEMA_ID, index_id, OwnedValue::Map(map))
    }

    /// Read one segment of a bucket chain: its ids, and the link to the next.
    ///
    /// The unit of a chain walk is deliberately ONE segment. A whole bucket is
    /// unbounded -- that is the entire point of chaining it -- so a caller
    /// that materialised every segment before doing anything would put the
    /// bucket back into memory in one piece, which is the shape chaining
    /// exists to avoid.
    async fn read_segment(
        &self,
        segment_id: Id,
    ) -> Result<Result<Option<(Vec<Id>, Option<Id>)>, ReadError>, RPCError> {
        match self.neb_client.read_cell(segment_id).await {
            Ok(Ok(mut cell)) => {
                let next = Self::cell_next(&cell);
                let ids = match &mut cell[*HASH_INDEX_FIELD_ID] {
                    OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) => std::mem::take(ids),
                    _ => Vec::new(),
                };
                Ok(Ok(Some((ids, next))))
            }
            Ok(Err(ReadError::CellDoesNotExisted)) => Ok(Ok(None)),
            Ok(Err(e)) => Ok(Err(e)),
            Err(e) => Err(e),
        }
    }

    /// Fold a batch of ids into the bucket, spilling into a chain as needed.
    ///
    /// `batch` must be free of duplicates; `add_index` drains it from a set.
    async fn apply_batch(&self, index_id: &Id, batch: &[Id]) -> Result<(), WriteError> {
        // A batch bigger than one cell is split, so a chunk can always fit a
        // freshly spilled head. Concurrency-sized batches never reach this.
        for chunk in batch.chunks(*BUCKET_CAPACITY) {
            self.apply_chunk(index_id, chunk).await?;
        }
        Ok(())
    }

    /// Apply at most `BUCKET_CAPACITY` ids to the bucket head.
    ///
    /// Inserts always touch the HEAD only, so an append stays O(1) reads no
    /// matter how long the chain is. When the head is full its contents are
    /// frozen into a new cell and the head is reset to just the incoming ids,
    /// pointing at the frozen one -- so the chain grows behind the head and
    /// nothing has to walk it to write.
    async fn apply_chunk(&self, index_id: &Id, batch: &[Id]) -> Result<(), WriteError> {
        let cap = *BUCKET_CAPACITY;
        // Set when a supposedly-fitting append is refused for size. The cap is
        // an estimate of what fits, and an estimate that is ever wrong must
        // degrade into a spill rather than into a lost index entry.
        let mut force_spill = false;
        for retry in 0..MAX_CAS_RETRIES {
            // Try to create the bucket before reading it.
            //
            // Buckets are overwhelmingly singletons -- measured mean length at
            // insert was 0.0, with a single non-empty sample across 23k inserts
            // a second, because indexed values are near-unique. Reading first
            // therefore spent an entire round trip learning the cell does not
            // exist, on essentially every insert. Creating first collapses the
            // common case to one round trip; a bucket that does exist costs a
            // rejected create and falls through to the merge below, which the
            // measurement says is the rare path.
            if retry == 0 {
                let cell = Self::bucket_cell(index_id, index_id, batch.to_vec(), None);
                match self.neb_client.write_cell(cell).await {
                    Ok(Ok(_)) => return Ok(()),
                    // Bucket already there: fall through and merge into it.
                    Ok(Err(WriteError::CellAlreadyExisted)) => {}
                    Ok(Err(e)) if Self::retryable_write_error(&e) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                }
            }

            match self.neb_client.read_cell(*index_id).await {
                Ok(Ok(mut cell)) => {
                    let version = cell.header.version;
                    let next = Self::cell_next(&cell);
                    // Take the head's ids out rather than copying them: the
                    // cell is dropped as soon as the write is issued, so a
                    // clone bought nothing and cost a copy of the whole head.
                    let head: Vec<Id> = match &mut cell[*HASH_INDEX_FIELD_ID] {
                        OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) => std::mem::take(ids),
                        other => {
                            return Err(WriteError::DataMismatchSchema(
                                Field::new_unindexed_array(HASH_INDEX_FIELD, Type::Id),
                                other.clone(),
                            ))
                        }
                    };

                    // Head size drives the cost of this path: it is read,
                    // scanned and written whole on every append.
                    HASH_BUCKET_LEN_SUM.fetch_add(head.len() as u64, Ordering::Relaxed);
                    HASH_BUCKET_SAMPLES.fetch_add(1, Ordering::Relaxed);
                    HASH_BUCKET_MAX.fetch_max(head.len() as u64, Ordering::Relaxed);

                    // Membership resolved once for the whole batch, against
                    // the head only -- see `collect_bucket` for why that is
                    // sufficient. The single-id case keeps the linear scan: it
                    // is the common one, and hashing the whole head to place
                    // one id would cost more than it saves.
                    let fresh: Vec<Id> = if batch.len() == 1 {
                        if head.contains(&batch[0]) {
                            Vec::new()
                        } else {
                            vec![batch[0]]
                        }
                    } else {
                        let existing: HashSet<Id> = head.iter().copied().collect();
                        batch
                            .iter()
                            .copied()
                            .filter(|id| !existing.contains(id))
                            .collect()
                    };
                    if fresh.is_empty() {
                        debug!("All of batch already in head of index {:?}", index_id);
                        return Ok(());
                    }

                    if !force_spill && head.len() + fresh.len() <= cap {
                        let mut ids = head;
                        ids.extend(fresh);
                        match self
                            .neb_client
                            .compare_version_and_set_field(
                                *index_id,
                                version,
                                *HASH_INDEX_FIELD_ID,
                                OwnedValue::PrimArray(OwnedPrimArray::Id(ids)),
                            )
                            .await
                        {
                            Ok(Ok(_)) => return Ok(()),
                            // The cap said this fits and the store disagrees.
                            // The store is right: spill instead, and say so --
                            // reaching here means BUCKET_CAPACITY is sized too
                            // close to the limit and wants lowering.
                            Ok(Err(WriteError::CellIsTooLarge(size))) => {
                                warn!(
                                    "hash bucket {:?} refused a {}-id append at {} bytes despite a \
                                     capacity of {}; spilling. Lower NEB_HASH_BUCKET_CAPACITY.",
                                    index_id,
                                    batch.len(),
                                    size,
                                    cap
                                );
                                force_spill = true;
                                continue;
                            }
                            Ok(Err(e)) if Self::retryable_write_error(&e) => {
                                tokio::task::yield_now().await;
                                continue;
                            }
                            Ok(Err(e)) => return Err(e),
                            Err(_) => {
                                tokio::task::yield_now().await;
                                continue;
                            }
                        }
                    }

                    // ---- spill ----
                    // Freeze the head's contents into a version-keyed cell
                    // that nothing points at yet, then swing the head onto it
                    // in one CAS. The CAS is the sole publish point: a racing
                    // writer bumps the version, we lose, and the frozen cell
                    // was never reachable. Two racing spillers key their
                    // frozen cells by the same version they both read, so the
                    // contents are identical by construction.
                    let frozen_id = spilled_cell_id(index_id, version);
                    let frozen = Self::bucket_cell(index_id, &frozen_id, head, next);
                    match self.neb_client.write_cell(frozen).await {
                        Ok(Ok(_)) | Ok(Err(WriteError::CellAlreadyExisted)) => {}
                        Ok(Err(e)) if Self::retryable_write_error(&e) => {
                            tokio::task::yield_now().await;
                            continue;
                        }
                        Ok(Err(e)) => return Err(e),
                        Err(_) => {
                            tokio::task::yield_now().await;
                            continue;
                        }
                    }

                    let new_head = Self::bucket_cell(index_id, index_id, fresh, Some(frozen_id));
                    match self
                        .neb_client
                        .compare_version_and_update_cell(*index_id, version, new_head)
                        .await
                    {
                        Ok(Ok(_)) => {
                            debug!("Bucket {:?} spilled into {:?}", index_id, frozen_id);
                            return Ok(());
                        }
                        Ok(Err(e)) if Self::retryable_write_error(&e) => {
                            // Our frozen cell was never reachable; drop it and
                            // re-derive from whatever the head is now.
                            let _ = self.neb_client.remove_cell(frozen_id).await;
                            tokio::task::yield_now().await;
                            continue;
                        }
                        Ok(Err(e)) => {
                            let _ = self.neb_client.remove_cell(frozen_id).await;
                            return Err(e);
                        }
                        Err(_) => {
                            let _ = self.neb_client.remove_cell(frozen_id).await;
                            tokio::task::yield_now().await;
                            continue;
                        }
                    }
                }
                Ok(Err(ReadError::CellDoesNotExisted)) => {
                    let cell = Self::bucket_cell(index_id, index_id, batch.to_vec(), None);
                    match self.neb_client.write_cell(cell).await {
                        Ok(Ok(_)) => return Ok(()),
                        Ok(Err(WriteError::CellAlreadyExisted)) => continue,
                        Ok(Err(e)) if Self::retryable_write_error(&e) => {
                            tokio::task::yield_now().await;
                            continue;
                        }
                        Ok(Err(e)) => return Err(e),
                        Err(_) => {
                            tokio::task::yield_now().await;
                            continue;
                        }
                    }
                }
                Ok(Err(e)) if Self::retryable_read_error(&e) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(Err(e)) => return Err(WriteError::ReadError(e)),
                Err(_) => {
                    tokio::task::yield_now().await;
                    continue;
                }
            }
        }

        warn!(
            "Max CAS retries exceeded for add_index batch of {} into {:?}",
            batch.len(),
            index_id
        );
        Err(WriteError::CellVersionMismatch)
    }

    /// Remove `cell_id` from whichever cell of the bucket chain holds it.
    ///
    /// An emptied segment is left linked rather than unlinked: unlinking means
    /// updating its predecessor, which races with a concurrent spill onto that
    /// same predecessor. An empty segment costs one small cell and one read on
    /// the walk, which is cheaper than getting that race wrong.
    pub async fn remove_index(&self, cell_id: &Id, index_id: &Id) -> Result<(), WriteError> {
        let bucket = self.bucket(index_id);
        let mut inline = bucket
            .apply
            .try_lock()
            .ok()
            .filter(|_| bucket.blocking_removals.load(Ordering::Acquire) == 0);
        let result = if let Some(_guard) = inline.take() {
            Self::remove_index_inner(&self.neb_client, cell_id, index_id).await
        } else {
            // Count queued jobs too. A cancelled future must not clear this count
            // while its blocking worker still uses the bucket.
            struct RemovalJob(Arc<Bucket>);
            impl Drop for RemovalJob {
                fn drop(&mut self) {
                    self.0.blocking_removals.fetch_sub(1, Ordering::Release);
                }
            }
            bucket.blocking_removals.fetch_add(1, Ordering::AcqRel);
            #[cfg(test)]
            bucket.removal_handoffs.fetch_add(1, Ordering::Relaxed);
            let job = RemovalJob(bucket.clone());
            let client = self.neb_client.clone();
            let cell_id = *cell_id;
            let index_id = *index_id;
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                let _job = job;
                runtime.block_on(Self::remove_index_inner(&client, &cell_id, &index_id))
            })
            .await
            .map_err(|_| WriteError::NetworkingError)?
        };
        drop(inline);
        drop(bucket);
        self.release_bucket(index_id);
        result
    }

    async fn remove_index_inner(
        neb_client: &AsyncClient,
        cell_id: &Id,
        index_id: &Id,
    ) -> Result<(), WriteError> {
        debug!(
            "Attempting to remove index for cell_id: {:?}, index_id: {:?}",
            cell_id, index_id
        );

        for retry in 0..MAX_CAS_RETRIES {
            // Locate the segment holding it.
            let mut cursor = Some(*index_id);
            let mut hops = 0usize;
            let mut holder: Option<(Id, u64, Vec<Id>)> = None;
            loop {
                let Some(cid) = cursor else { break };
                match neb_client.read_cell(cid).await {
                    Ok(Ok(cell)) => {
                        let version = cell.header.version;
                        if let Some(ids) = Self::cell_ids(&cell) {
                            if ids.contains(cell_id) {
                                holder = Some((cid, version, ids.clone()));
                                break;
                            }
                        }
                        cursor = Self::cell_next(&cell);
                    }
                    Ok(Err(ReadError::CellDoesNotExisted)) => break,
                    Ok(Err(e)) if Self::retryable_read_error(&e) => {
                        tokio::task::yield_now().await;
                        cursor = None;
                        holder = None;
                        break;
                    }
                    Ok(Err(e)) => return Err(WriteError::ReadError(e)),
                    Err(_) => {
                        tokio::task::yield_now().await;
                        cursor = None;
                        holder = None;
                        break;
                    }
                }
                hops += 1;
                if hops > MAX_CHAIN_HOPS {
                    error!(
                        "hash bucket {:?} chain exceeded {} hops during remove",
                        index_id, MAX_CHAIN_HOPS
                    );
                    break;
                }
            }

            let Some((seg_id, version, mut ids)) = holder else {
                debug!(
                    "Cell {:?} not in index {:?}, nothing to remove",
                    cell_id, index_id
                );
                return Ok(());
            };
            ids.retain(|id| id != cell_id);

            match neb_client
                .compare_version_and_set_field(
                    seg_id,
                    version,
                    *HASH_INDEX_FIELD_ID,
                    OwnedValue::PrimArray(OwnedPrimArray::Id(ids)),
                )
                .await
            {
                Ok(Ok(_)) => {
                    debug!(
                        "Successfully removed cell {:?} from index {:?}",
                        cell_id, index_id
                    );
                    return Ok(());
                }
                Ok(Err(e)) if Self::retryable_write_error(&e) => {
                    debug!("CAS retry {} for remove_index", retry + 1);
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    tokio::task::yield_now().await;
                    continue;
                }
            }
        }

        warn!(
            "Max CAS retries exceeded for remove_index({:?}, {:?})",
            cell_id, index_id
        );
        Err(WriteError::CellVersionMismatch)
    }

    /// Remove a member from the exact segment that the prune inspected.
    /// Orphan spills are not reachable from the head after a lost head CAS.
    pub(crate) async fn remove_segment_member(
        &self,
        member: &Id,
        segment: &Id,
        owner: &Id,
    ) -> Result<(), WriteError> {
        for _ in 0..MAX_CAS_RETRIES {
            let cell = match self.neb_client.read_cell(*segment).await {
                Ok(Ok(cell)) => cell,
                Ok(Err(ReadError::CellDoesNotExisted)) => return Ok(()),
                Ok(Err(e)) if Self::retryable_read_error(&e) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(Err(e)) => return Err(WriteError::ReadError(e)),
                Err(_) => {
                    tokio::task::yield_now().await;
                    continue;
                }
            };
            let mut contents = bucket_segment(&cell).map_err(WriteError::ReadError)?;
            if contents.owner != *owner {
                return Err(WriteError::ReadError(ReadError::ExecError(
                    "hash segment owner changed".into(),
                )));
            }
            let previous = contents.ids.len();
            contents.ids.retain(|id| id != member);
            if contents.ids.len() == previous {
                return Ok(());
            }
            match self
                .neb_client
                .compare_version_and_set_field(
                    *segment,
                    cell.header.version,
                    *HASH_INDEX_FIELD_ID,
                    OwnedValue::PrimArray(OwnedPrimArray::Id(contents.ids)),
                )
                .await
            {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(e)) if Self::retryable_write_error(&e) => tokio::task::yield_now().await,
                Ok(Err(e)) => return Err(e),
                Err(_) => tokio::task::yield_now().await,
            }
        }
        Err(WriteError::CellVersionMismatch)
    }

    /// Cells whose `field_id` equals `value`, walking the bucket chain.
    ///
    /// Streams: one segment is held at a time, its members are verified and
    /// emitted, and only then is the next segment read. Peak memory is one
    /// segment plus the matches, not the whole bucket -- which for a
    /// low-cardinality indexed value is the difference between half a megabyte
    /// and however large that value's population has grown.
    ///
    /// De-duplication is against what has already been EMITTED rather than
    /// against every candidate seen. That is both sufficient -- the contract
    /// is a result without duplicates -- and free, since the result is
    /// materialised anyway, whereas a candidate-side set would be as large as
    /// the bucket. Duplicates are possible at all because write-side dedup
    /// checks only the head segment: scanning the chain on every insert would
    /// put a chain-length read on the write path.
    async fn query_ids(
        &self,
        index_id: Id,
        check: Option<&crate::index::verification::IndexCheck>,
    ) -> Result<Result<Vec<Id>, ReadError>, RPCError> {
        let mut result = Vec::new();
        let mut emitted = HashSet::new();
        let mut cursor = Some(index_id);
        let mut hops = 0usize;

        while let Some(cid) = cursor {
            let segment = match self.read_segment(cid).await {
                Ok(Ok(Some(segment))) => segment,
                // A missing HEAD is an empty bucket. A missing LINK is a
                // broken chain: say so rather than quietly returning a short
                // answer that reads like a complete one.
                Ok(Ok(None)) => {
                    if cid != index_id {
                        error!(
                            "hash bucket {:?} links to missing cell {:?}; entries after it are unreachable",
                            index_id, cid
                        );
                    }
                    break;
                }
                Ok(Err(e)) => return Ok(Err(e)),
                Err(e) => return Err(e),
            };
            let (ids, next) = segment;
            cursor = next;

            for id in ids {
                if emitted.contains(&id) {
                    continue;
                }
                if let Some(check) = check {
                    if !matches!(
                        self.neb_client.read_indexed_cell(id, check.clone()).await,
                        Ok(Ok(_))
                    ) {
                        continue;
                    }
                }
                emitted.insert(id);
                result.push(id);
            }

            hops += 1;
            if hops > MAX_CHAIN_HOPS {
                error!(
                    "hash bucket {:?} chain exceeded {} hops; truncating walk",
                    index_id, MAX_CHAIN_HOPS
                );
                break;
            }
        }

        Ok(Ok(result))
    }
    pub(crate) async fn candidates(
        &self,
        index_id: Id,
    ) -> Result<Result<Vec<Id>, ReadError>, RPCError> {
        self.query_ids(index_id, None).await
    }

    pub async fn query(
        &self,
        index_id: Id,
        field_id: u64,
        value: &OwnedValue,
    ) -> Result<Result<Vec<Id>, ReadError>, RPCError> {
        let check = crate::index::verification::IndexCheck::Hashed {
            buckets: vec![index_id],
            field: field_id,
            value: value.clone(),
        };
        self.query_ids(index_id, Some(&check)).await
    }
}

pub fn hash_index_schema() -> Schema {
    Schema::new_with_id(
        HASH_INDEX_SCHEMA_ID.get(),
        &HASH_SCHEMA.to_string(),
        None,
        Field::new_schema(vec![
            Field::new_unindexed_array(HASH_INDEX_FIELD, Type::Id),
            // Nullable: a bucket that never outgrew one cell has no link.
            Field::new_unindexed_nullable(HASH_NEXT_FIELD, Type::Id),
            Field::new_unindexed(HASH_OWNER_FIELD, Type::Id),
        ]),
        false,
        false,
    )
}

/// What one bucket segment holds.
///
/// The recovery reconcile reads bucket cells in both cell forms -- `OwnedCell`
/// when the segment sits on another member and `SharedCell` when it sits in
/// this member's own chunks -- so the decode is written once, against the
/// `Cell` trait, rather than twice.
pub(crate) struct BucketSegment {
    /// The head of the chain this segment belongs to.
    pub(crate) owner: Id,
    /// The member ids this segment holds.
    pub(crate) ids: Vec<Id>,
    /// The older segment of the chain, if this one spilled.
    pub(crate) next: Option<Id>,
}

fn bucket_field<C>(cell: &C, field_id: u64) -> OwnedValue
where
    C: Cell,
    <C::Value as Value>::Out: ToOwnedValue,
{
    cell.data().get_in_by_ids(&vec![field_id]).to_owned_value()
}

/// Read a bucket cell out of any cell form.
pub(crate) fn bucket_segment<C>(cell: &C) -> Result<BucketSegment, ReadError>
where
    C: Cell,
    <C::Value as Value>::Out: ToOwnedValue,
{
    let ids = match bucket_field(cell, *HASH_INDEX_FIELD_ID) {
        OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) => ids,
        _ => return Err(ReadError::ExecError("invalid hash member array".into())),
    };
    let owner = match bucket_field(cell, *HASH_OWNER_FIELD_ID) {
        OwnedValue::Id(id) => id,
        _ => return Err(ReadError::ExecError("invalid hash owner".into())),
    };
    let next = match bucket_field(cell, *HASH_NEXT_FIELD_ID) {
        OwnedValue::Id(id) => Some(id),
        OwnedValue::Null => None,
        _ => return Err(ReadError::ExecError("invalid hash next link".into())),
    };
    Ok(BucketSegment { owner, ids, next })
}

/// Validate recovered bucket records before startup can repair or serve them.
pub(crate) fn validate_recovered_buckets(
    chunks: &Arc<crate::ram::chunk::Chunks>,
) -> std::io::Result<()> {
    use crate::ram::entry::EntryContent;
    for chunk in &chunks.list {
        for segment in chunk.segments() {
            for entry in chunk.live_entries(&segment) {
                let EntryContent::Cell(header) = entry.content else {
                    continue;
                };
                if header.schema == *HASH_INDEX_SCHEMA_V1_ID
                    || header.schema == *HASH_INDEX_SCHEMA_V2_ID
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "hash bucket format does not match format 3",
                    ));
                }
                if header.schema != *HASH_INDEX_SCHEMA_ID {
                    continue;
                }
                let invalid =
                    |message: String| std::io::Error::new(std::io::ErrorKind::InvalidData, message);
                if chunk.meta.schemas.get(&header.schema).is_none() {
                    return Err(invalid("hash bucket schema is absent".into()));
                }
                let cell = chunk
                    .read_cell(header.id().bits())
                    .map_err(|e| invalid(format!("hash bucket read failed: {e:?}")))?;
                bucket_segment(&cell)
                    .map_err(|e| invalid(format!("hash bucket decode failed: {e:?}")))?;
            }
        }
    }
    Ok(())
}

pub struct HashedIndexClient {
    pub client: Arc<AsyncClient>,
    pub indexer: HashIndexer,
}

impl HashedIndexClient {
    pub fn new(client: &Arc<AsyncClient>) -> Self {
        let indexer = HashIndexer::new(&client);
        HashedIndexClient {
            client: client.clone(),
            indexer,
        }
    }

    pub async fn insert(&self, hash_id: &Id, cell_id: &Id) -> Result<(), WriteError> {
        self.indexer.add_index(cell_id, hash_id).await
    }

    pub async fn query(
        &self,
        index_id: Id,
        field_id: u64,
        value: &OwnedValue,
    ) -> Result<Result<Vec<Id>, ReadError>, RPCError> {
        self.indexer.query(index_id, field_id, value).await
    }
}

/// The id of a hash-index bucket cell.
///
/// Keyed by the schema FAMILY: a bucket holds the cells of one schema's field
/// whatever generation each of them was written under, so evolving the schema
/// must not move the bucket out from under the entries already in it. Passing
/// the raw number keeps the derivation independent of `SchemaUid` staying
/// `#[serde(transparent)]`, since these ids are durable.
pub fn get_hash_id(schema: SchemaUid, field: u64, hash_feat: Feature) -> Id {
    Id::from_obj(&(schema.get(), field, hash_feat))
}

pub fn get_null_hash_id(schema: SchemaUid, field: u64) -> Id {
    Id::from_obj(&(schema.get(), field, "NULL_BUCKET"))
}

pub fn get_hash_id_from_value(schema: SchemaUid, field: u64, value: &OwnedValue) -> Id {
    let hash_feat = hash_indexable_owned_value(value)
        .expect("hash index values must be scalar values or flat scalar arrays");
    get_hash_id(schema, field, hash_feat)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AsyncClient;
    use crate::server::{NebServer, ServerOptions, Service};
    use std::sync::Arc;
    use tokio::task::JoinSet;

    /// Helper function to create a test server
    async fn create_test_server(name: &str) -> (Arc<NebServer>, Arc<AsyncClient>) {
        let _ = env_logger::try_init();
        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = format!("hash_index_test_{}", name);

        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 16 * 1024 * 1024,
                db_size: 16 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();

        let client = Arc::new(
            AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                &server_group,
            )
            .await
            .unwrap(),
        );

        // Initialize hash index schema
        let _ = client.new_schema_with_id(hash_index_schema()).await;

        (server, client)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_index_new_cell() {
        let (_server, client) = create_test_server("add_index_new").await;
        let indexer = HashIndexer::new(&client);

        let cell_id = Id::rand();
        let index_id = Id::rand();

        // Add index to a non-existent cell (should create it)
        let result = indexer.add_index(&cell_id, &index_id).await;
        assert!(result.is_ok(), "Failed to add index: {:?}", result);

        // Verify the cell was created with the correct data
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 1);
            assert_eq!(ids[0], cell_id);
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_index_existing_cell() {
        let (_server, client) = create_test_server("add_index_existing").await;
        let indexer = HashIndexer::new(&client);

        let cell_id_1 = Id::rand();
        let cell_id_2 = Id::rand();
        let index_id = Id::rand();

        // Add first cell
        indexer.add_index(&cell_id_1, &index_id).await.unwrap();

        // Add second cell to the same index
        indexer.add_index(&cell_id_2, &index_id).await.unwrap();

        // Verify both cells are in the index
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 2);
            assert!(ids.contains(&cell_id_1));
            assert!(ids.contains(&cell_id_2));
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_index_duplicate() {
        let (_server, client) = create_test_server("add_index_duplicate").await;
        let indexer = HashIndexer::new(&client);

        let cell_id = Id::rand();
        let index_id = Id::rand();

        // Add same cell twice
        indexer.add_index(&cell_id, &index_id).await.unwrap();
        indexer.add_index(&cell_id, &index_id).await.unwrap();

        // Verify only one entry exists (no duplicates)
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 1);
            assert_eq!(ids[0], cell_id);
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_index() {
        let (_server, client) = create_test_server("remove_index").await;
        let indexer = HashIndexer::new(&client);

        let cell_id = Id::rand();
        let index_id = Id::rand();

        // Add and then remove
        indexer.add_index(&cell_id, &index_id).await.unwrap();
        indexer.remove_index(&cell_id, &index_id).await.unwrap();

        let result = client.read_cell(index_id).await;
        match result {
            Ok(Err(ReadError::CellDoesNotExisted)) => {}
            Ok(Ok(cell)) => {
                let ids = &cell[*HASH_INDEX_FIELD_ID];
                if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
                    assert!(ids.is_empty(), "Expected empty ids, got {:?}", ids);
                } else {
                    panic!("Expected Id array, got {:?}", ids);
                }
            }
            other => panic!("Expected empty index, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn uncontended_removal_makes_no_blocking_handoffs() {
        let (server, client) = create_test_server("remove_inline").await;
        let indexer = HashIndexer::new(&client);
        let index_id = Id::rand();
        let member = Id::rand();
        indexer.add_index(&member, &index_id).await.unwrap();
        let bucket = indexer.bucket(&index_id);
        indexer.remove_index(&member, &index_id).await.unwrap();
        assert_eq!(bucket.removal_handoffs.load(Ordering::Relaxed), 0);
        let stored = client.read_cell(index_id).await.unwrap().unwrap();
        assert!(HashIndexer::cell_ids(&stored).unwrap().is_empty());
        server.shutdown().await;
    }

    #[tokio::test]
    async fn removal_does_not_serialize_on_the_bucket_applier() {
        let (server, client) = create_test_server("remove_no_applier_queue").await;
        let indexer = HashIndexer::new(&client);
        let index_id = Id::rand();
        let member = Id::rand();
        indexer.add_index(&member, &index_id).await.unwrap();
        let bucket = indexer.bucket(&index_id);
        let apply = bucket.apply.lock().await;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            indexer.remove_index(&member, &index_id),
        )
        .await;
        drop(apply);
        assert!(
            result.is_ok(),
            "removal unnecessarily waited for the bucket applier"
        );
        result.unwrap().unwrap();
        let stored = client.read_cell(index_id).await.unwrap().unwrap();
        assert!(HashIndexer::cell_ids(&stored).unwrap().is_empty());
        server.shutdown().await;
    }

    #[tokio::test]
    async fn contended_removal_waits_for_the_cell_word_without_stalling_the_runtime() {
        use std::time::Duration;

        let (server, client) = create_test_server("remove_applier_progress").await;
        let indexer = HashIndexer::new(&client);
        let index_id = Id::rand();
        let other_index = Id::rand();
        let member = Id::rand();
        indexer.add_index(&member, &index_id).await.unwrap();
        indexer.add_index(&member, &other_index).await.unwrap();
        let bucket = indexer.bucket(&index_id);
        let apply = bucket.apply.lock().await;
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let chunks = server.chunks().clone();
        let owner = std::thread::spawn(move || {
            let word = chunks.lock_cell_for_write(&index_id, true).unwrap();
            locked_tx.send(()).unwrap();
            // Bound the negative control even if removal blocks the runtime.
            let _ = release_rx.recv_timeout(Duration::from_secs(2));
            drop(word);
        });
        locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        indexer.remove_index(&member, &other_index).await.unwrap();
        let removal = indexer.remove_index(&member, &index_id);
        tokio::pin!(removal);
        let waiting = tokio::time::timeout(Duration::from_millis(50), removal.as_mut()).await;
        let _ = release_tx.send(());
        owner.join().unwrap();
        drop(apply);
        assert!(
            waiting.is_err(),
            "removal must await the cell word and let the runtime timer run"
        );
        removal.await.unwrap();
        let stored = client.read_cell(index_id).await.unwrap().unwrap();
        let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = &stored[*HASH_INDEX_FIELD_ID] else {
            panic!("expected bucket IDs");
        };
        assert!(ids.is_empty());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_index_multiple_entries() {
        let (_server, client) = create_test_server("remove_multiple").await;
        let indexer = HashIndexer::new(&client);

        let cell_id_1 = Id::rand();
        let cell_id_2 = Id::rand();
        let cell_id_3 = Id::rand();
        let index_id = Id::rand();

        // Add three cells
        indexer.add_index(&cell_id_1, &index_id).await.unwrap();
        indexer.add_index(&cell_id_2, &index_id).await.unwrap();
        indexer.add_index(&cell_id_3, &index_id).await.unwrap();

        // Remove one
        indexer.remove_index(&cell_id_2, &index_id).await.unwrap();

        // Verify two remain
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 2);
            assert!(ids.contains(&cell_id_1));
            assert!(ids.contains(&cell_id_3));
            assert!(!ids.contains(&cell_id_2));
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_nonexistent() {
        let (_server, client) = create_test_server("remove_nonexistent").await;
        let indexer = HashIndexer::new(&client);

        let cell_id = Id::rand();
        let index_id = Id::rand();

        // Remove from non-existent index (should succeed silently)
        let result = indexer.remove_index(&cell_id, &index_id).await;
        assert!(
            result.is_ok(),
            "Remove from non-existent index should succeed"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_empty() {
        let (_server, client) = create_test_server("query_empty").await;
        let indexer = HashIndexer::new(&client);

        let index_id = Id::rand();
        let field_id = 123u64;
        let value = OwnedValue::I64(42);

        // Query non-existent index
        let result = indexer.query(index_id, field_id, &value).await;
        assert!(result.is_ok());
        let ids = result.unwrap().unwrap();
        assert_eq!(ids.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_adds() {
        let (_server, client) = create_test_server("concurrent_adds").await;
        let indexer = Arc::new(HashIndexer::new(&client));
        let index_id = Id::rand();

        // Spawn 10 concurrent tasks adding different cells to the same index
        let mut tasks = JoinSet::new();
        let cell_ids: Vec<Id> = (0..10).map(|_| Id::rand()).collect();

        for cell_id in cell_ids.iter() {
            let indexer = indexer.clone();
            let cell_id = *cell_id;
            let index_id = index_id;
            tasks.spawn(async move { indexer.add_index(&cell_id, &index_id).await });
        }

        // Wait for all tasks to complete
        while let Some(result) = tasks.join_next().await {
            assert!(result.is_ok(), "Task failed: {:?}", result);
            assert!(result.unwrap().is_ok(), "Add index failed");
        }

        // Verify all 10 cells are in the index
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 10, "Expected 10 cells, got {}", ids.len());
            for cell_id in cell_ids.iter() {
                assert!(
                    ids.contains(cell_id),
                    "Cell {:?} not found in index",
                    cell_id
                );
            }
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    /// A bucket that outgrows one cell must chain, and must read back whole.
    ///
    /// This is the silent-truncation case. Before chaining, the head hit
    /// `MAX_CELL_SIZE`, every further insert failed `CellIsTooLarge`, and the
    /// bucket then answered with whatever had happened to fit -- a 174,166-id
    /// bucket returning 131,066. Sized just past one segment so the spill path
    /// runs without paying for a full-size bucket, and driven concurrently so
    /// it also covers spilling under contention.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_bucket_chains_past_one_segment() {
        // Shrink the segment so the spill path runs against the 16 MiB test
        // store. The isolated runner gives each test its own process, so this
        // is read before anything else touches the index; the assert makes it
        // fail loudly rather than silently testing nothing if that changes.
        std::env::set_var("NEB_HASH_BUCKET_CAPACITY", "64");
        let (_server, client) = create_test_server("bucket_chain").await;
        let indexer = Arc::new(HashIndexer::new(&client));
        let index_id = Id::rand();

        assert!(
            *BUCKET_CAPACITY <= 64,
            "BUCKET_CAPACITY already initialised to {}; this test needs its own process",
            *BUCKET_CAPACITY
        );
        let total = *BUCKET_CAPACITY + 500;
        let cell_ids: Vec<Id> = (0..total as u64)
            .map(|i| Id::from_parts(7, i + 1))
            .collect();

        let mut tasks = JoinSet::new();
        for chunk in cell_ids.chunks(total.div_ceil(32)) {
            let indexer = indexer.clone();
            let chunk: Vec<Id> = chunk.to_vec();
            tasks.spawn(async move {
                for cell_id in chunk {
                    indexer.add_index(&cell_id, &index_id).await?;
                }
                Ok::<(), WriteError>(())
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.expect("task panicked").expect("add_index failed");
        }

        // Walk the chain the way a reader does.
        let mut seen = std::collections::HashSet::new();
        let mut segments = 0usize;
        let mut cursor = Some(index_id);
        while let Some(cid) = cursor {
            let cell = client.read_cell(cid).await.unwrap().unwrap();
            match &cell[*HASH_INDEX_FIELD_ID] {
                OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) => {
                    assert!(
                        ids.len() <= *BUCKET_CAPACITY,
                        "segment {:?} holds {} ids, over the {} cap",
                        cid,
                        ids.len(),
                        *BUCKET_CAPACITY
                    );
                    seen.extend(ids.iter().copied());
                }
                other => panic!("segment {:?} has no id array: {:?}", cid, other),
            }
            cursor = match &cell[*HASH_NEXT_FIELD_ID] {
                OwnedValue::Id(id) => Some(*id),
                _ => None,
            };
            segments += 1;
            assert!(segments < 1000, "chain did not terminate");
        }

        assert!(
            segments > 1,
            "a bucket of {} past a {} cap must spill, but stayed in {} segment(s)",
            total,
            *BUCKET_CAPACITY,
            segments
        );
        assert_eq!(seen.len(), total, "every id must survive the spill");
        for cell_id in &cell_ids {
            assert!(
                seen.contains(cell_id),
                "id {:?} lost across the chain",
                cell_id
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_removes() {
        let (_server, client) = create_test_server("concurrent_removes").await;
        let indexer = Arc::new(HashIndexer::new(&client));
        let index_id = Id::rand();

        // Add 20 cells first
        let cell_ids: Vec<Id> = (0..20).map(|_| Id::rand()).collect();
        for cell_id in cell_ids.iter() {
            indexer.add_index(cell_id, &index_id).await.unwrap();
        }

        // Concurrently remove 10 of them
        let mut tasks = JoinSet::new();
        for cell_id in cell_ids.iter().take(10) {
            let indexer = indexer.clone();
            let cell_id = *cell_id;
            let index_id = index_id;
            tasks.spawn(async move { indexer.remove_index(&cell_id, &index_id).await });
        }

        // Wait for all removals to complete
        while let Some(result) = tasks.join_next().await {
            assert!(result.is_ok(), "Task failed: {:?}", result);
            assert!(result.unwrap().is_ok(), "Remove index failed");
        }

        // Verify 10 cells remain
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 10, "Expected 10 cells, got {}", ids.len());
            // Verify the remaining 10 are the ones we didn't remove
            for cell_id in cell_ids.iter().skip(10) {
                assert!(
                    ids.contains(cell_id),
                    "Cell {:?} should still be in index",
                    cell_id
                );
            }
            // Verify the removed 10 are gone
            for cell_id in cell_ids.iter().take(10) {
                assert!(
                    !ids.contains(cell_id),
                    "Cell {:?} should have been removed",
                    cell_id
                );
            }
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_mixed_operations() {
        let (_server, client) = create_test_server("concurrent_mixed").await;
        let indexer = Arc::new(HashIndexer::new(&client));
        let index_id = Id::rand();

        // Spawn mixed add/remove operations concurrently
        let mut tasks = JoinSet::new();
        let cell_ids: Vec<Id> = (0..20).map(|_| Id::rand()).collect();

        // Add all cells
        for (i, cell_id) in cell_ids.iter().enumerate() {
            let indexer = indexer.clone();
            let cell_id = *cell_id;
            let index_id = index_id;

            if i % 2 == 0 {
                tasks.spawn(async move {
                    let mut last_err = WriteError::CellVersionMismatch;
                    for _ in 0..5 {
                        match indexer.add_index(&cell_id, &index_id).await {
                            Ok(()) => return Ok(()),
                            Err(e) => {
                                last_err = e;
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                    Err(last_err)
                });
            } else {
                tasks.spawn(async move {
                    let mut last_err = WriteError::CellVersionMismatch;
                    for _ in 0..5 {
                        let op = match indexer.add_index(&cell_id, &index_id).await {
                            Ok(()) => indexer.remove_index(&cell_id, &index_id).await,
                            Err(e) => Err(e),
                        };
                        match op {
                            Ok(()) => return Ok(()),
                            Err(e) => {
                                last_err = e;
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                    Err(last_err)
                });
            }
        }

        // Wait for all operations
        while let Some(result) = tasks.join_next().await {
            assert!(result.is_ok(), "Task panicked: {:?}", result);
            assert!(result.unwrap().is_ok(), "Operation failed");
        }

        // Verify only even-indexed cells remain
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 10, "Expected 10 cells, got {}", ids.len());
            for (i, cell_id) in cell_ids.iter().enumerate() {
                if i % 2 == 0 {
                    assert!(
                        ids.contains(cell_id),
                        "Even cell {:?} should be in index",
                        cell_id
                    );
                } else {
                    assert!(
                        !ids.contains(cell_id),
                        "Odd cell {:?} should not be in index",
                        cell_id
                    );
                }
            }
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stress_cas_retries() {
        let (_server, client) = create_test_server("stress_cas").await;
        let indexer = Arc::new(HashIndexer::new(&client));
        let index_id = Id::rand();

        // Create very high contention: 50 concurrent operations on the same index
        let mut tasks = JoinSet::new();
        for i in 0..50 {
            let indexer = indexer.clone();
            let cell_id = Id::rand();
            let index_id = index_id;

            tasks.spawn(async move {
                let mut last_err = WriteError::CellVersionMismatch;
                for _ in 0..5 {
                    let mut op = indexer.add_index(&cell_id, &index_id).await;
                    if op.is_ok() && i % 3 == 0 {
                        op = indexer.remove_index(&cell_id, &index_id).await;
                    }
                    match op {
                        Ok(()) => return Ok(()),
                        Err(e) => {
                            last_err = e;
                            tokio::task::yield_now().await;
                        }
                    }
                }
                Err(last_err)
            });
        }

        // Wait for all operations
        let mut success_count = 0;
        while let Some(result) = tasks.join_next().await {
            if result.is_ok() && result.unwrap().is_ok() {
                success_count += 1;
            }
        }

        // With CAS retries, all operations should eventually succeed
        assert_eq!(
            success_count, 50,
            "Not all operations succeeded despite CAS retries"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hashed_index_client() {
        let (_server, client) = create_test_server("client_test").await;
        let hashed_client = HashedIndexClient::new(&client);

        let cell_id = Id::rand();
        let hash_id = Id::rand();

        // Test insert via client
        let result = hashed_client.insert(&hash_id, &cell_id).await;
        assert!(result.is_ok(), "Client insert failed: {:?}", result);

        // Verify via direct read
        let cell = client.read_cell(hash_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 1);
            assert_eq!(ids[0], cell_id);
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_hash_id_functions() {
        let schema_id = SchemaUid(123);
        let field_id = 456u64;
        let hash_feat: Feature = [1, 2, 3, 4, 5, 6, 7, 8];

        let hash_id_1 = get_hash_id(schema_id, field_id, hash_feat);
        let hash_id_2 = get_hash_id(schema_id, field_id, hash_feat);

        // Same inputs should produce same hash ID
        assert_eq!(hash_id_1, hash_id_2);

        // A different family must get a different bucket.
        let hash_id_3 = get_hash_id(SchemaUid(schema_id.get() + 1), field_id, hash_feat);
        assert_ne!(hash_id_1, hash_id_3);
    }
}
