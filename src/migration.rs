//! Moving a slot's cells from one member of a group to another.
//!
//! [`crate::slots`] says which slot a cell belongs to and
//! `bifrost::conshash::slots` records who owns each slot; neither moves a byte.
//! This module is the part that does.
//!
//! ## The commit point
//!
//! Everything here is arranged around one property of the placement state
//! machine: **a slot's table entry is the migration's only commit point.**
//! Before `complete_slot_migration` the donor answers for the slot; after it
//! the recipient does. There is no interval in which the slot is owned by
//! neither member or by both, so an interrupted migration leaves the group
//! correct and merely unfinished. Batching, the delta rounds and the deferred
//! drop all exist to make the data have arrived before that single write.
//!
//! ## Why the donor's copy is dropped last, and separately
//!
//! [`reclaim_donor_copy`] is a distinct call, not the tail of
//! [`migrate_slot`], and it re-reads the table before destroying anything. Two
//! things fall out of that, and the second is the more valuable:
//!
//! 1. Nothing is destroyed until the flip has been *read back*, so a
//!    migration that reported success on a lost response cannot have deleted
//!    the only copy.
//! 2. A client whose cached placement table is stale still routes to the
//!    donor, and while the donor's copy is intact that client gets the right
//!    answer instead of a spurious miss. Deferring the drop is what turns the
//!    cache-staleness window from a correctness problem into a latency one.
//!
//! ## A migration is per database, not per server
//!
//! Cell RPC services are scoped per database
//! (`cell_rpc::generate_scoped_service_id`), so the [`AsyncClient`] handed to
//! [`migrate_slot`] decides *which database's* cells move. Moving a slot for a
//! whole server means driving this once per database hosted there.
//!
//! Worth stating because the failure mode is quiet: a migration driven by the
//! wrong database's client enumerates that database's chunks, finds the slot
//! empty, transfers nothing, and reports success.
//!
//! ## Memory on the receiving side
//!
//! A slot's worth of cells arriving as ordinary writes lands in the recipient's
//! memory tier, so a bulk transfer can grow its hot tier far faster than a
//! normal write workload would. Three things bound it, and none of them
//! required a second ingest path:
//!
//! - batches bound how much is in flight at once;
//! - a segment is archived the moment it seals, so the durable copy exists
//!   without anything special;
//! - after each batch the driver asks the recipient to settle
//!   (`settle_bulk_receive`), which runs the tier's own eviction pass so the
//!   received data becomes disk-resident at the pace of the migration rather
//!   than at the background cleaner's cadence.
//!
//! The recipient reports its hot-tier size back with every settle, and
//! [`SlotHandover::recipient_peak_hot_bytes`] carries the peak — so the driver
//! measures the thing that could blow up instead of assuming it cannot.
//!
//! ## What this guarantees, and what it does not
//!
//! A slot with no concurrent writes moves exactly and losslessly. Under
//! concurrent writes:
//!
//! - **New cells** written to the donor mid-transfer are caught: each delta
//!   round re-enumerates the donor, and the reclaim carries over anything the
//!   recipient turns out to be missing rather than dropping it.
//! - **Updates to already-transferred cells** are not. A writer holding a
//!   stale table can also write to a former owner after the flip. Both need
//!   the same fix — a member must refuse writes for a slot the table no longer
//!   assigns to it, so a stale client is told to refresh instead of silently
//!   writing somewhere that will be discarded. That is deliberately not done
//!   here: it puts a check on the write path, and the cluster tests are not yet
//!   trustworthy enough to attribute a regression there. It is the next step,
//!   with the racing-write failure model that proves it.

use crate::client::AsyncClient;
use crate::ram::cell::{OwnedCell, ReadError, WriteError};
use crate::server::cell_rpc::AsyncServiceClient as CellServiceClient;
use crate::slots::{slot_group_id, SLOT_COUNT};
use bifrost::conshash::slots::client::SMClient as SlotsSMClient;
use bifrost::conshash::slots::SlotState;
use bifrost::rpc::RPCError;
use dovahkiin::types::Id;
use futures::stream::{self, StreamExt};
use std::fmt;
use std::sync::Arc;

/// Cells per round trip.
///
/// Small enough that a batch's cells are a bounded amount of memory on both
/// sides at once, large enough that the per-call dispatch cost is amortized.
/// It is a tuning knob, not a correctness one — every batch is independently
/// applied, so any value moves the same set of cells.
pub const DEFAULT_BATCH_CELLS: usize = 1024;

/// How many times to re-enumerate the donor looking for cells that arrived
/// after the previous pass read the slot.
///
/// Bounded on purpose: under sustained writes to the slot being moved the delta
/// never empties, and looping until it does would never commit. Committing with
/// a non-empty delta is the better answer — after the flip new writes go to the
/// recipient, so the delta stops growing, and the reclaim carries over whatever
/// is left.
pub const DEFAULT_DELTA_ROUNDS: usize = 4;

#[derive(Debug, Clone, Copy)]
pub struct MigrationPlan {
    pub batch_cells: usize,
    pub delta_rounds: usize,
    /// Ask the recipient to push what it just received towards disk after each
    /// batch. On by default: the cost is one cheap round trip per batch, and
    /// the thing it prevents is an unbounded hot tier on the receiving side.
    pub settle_recipient_per_batch: bool,
    /// How many slots to move at once.
    ///
    /// A slot migration is dominated by round trips -- two raft commands, a
    /// handful of RPCs, and a batch read plus write per batch -- so a sequential
    /// driver spends nearly all of its time waiting. Slots are independent
    /// (separate table entries, disjoint cells, per-slot commit points), so they
    /// pipeline without any cross-slot ordering to preserve.
    ///
    /// Bounded rather than unlimited, and the bound matters for two reasons that
    /// pull in the same direction: the raft leader serialises log appends however
    /// many callers there are, and in-flight data on the recipient is
    /// `concurrent_slots x batch_cells`, so raising this raises the recipient's
    /// peak memory proportionally. Defaults to [`default_concurrent_slots`].
    pub concurrent_slots: usize,
}

/// Slot concurrency scaled to the machine, clamped at both ends.
///
/// A floor because even one core benefits from pipelining calls that are waiting
/// on the network, and a ceiling because past a few dozen the raft leader's
/// serialised appends -- and the recipient's in-flight memory -- become the limit
/// rather than the caller's parallelism.
pub fn default_concurrent_slots() -> usize {
    std::thread::available_parallelism()
        .map(|cores| cores.get())
        .unwrap_or(4)
        .clamp(4, 64)
}

impl Default for MigrationPlan {
    fn default() -> Self {
        Self {
            batch_cells: DEFAULT_BATCH_CELLS,
            delta_rounds: DEFAULT_DELTA_ROUNDS,
            settle_recipient_per_batch: true,
            concurrent_slots: default_concurrent_slots(),
        }
    }
}

/// What one committed slot move did.
#[derive(Debug, Clone)]
pub struct SlotHandover {
    pub slot: u32,
    pub from: u64,
    pub to: u64,
    pub cells_transferred: usize,
    pub batches: usize,
    /// How many enumeration passes were needed. More than one means cells
    /// appeared on the donor while the transfer was running.
    pub delta_rounds_used: usize,
    /// Enumerated but gone by the time the batch read it — removed between the
    /// two steps. Not an error; recorded because a large count means something
    /// else is deleting from this slot concurrently.
    pub vanished_before_transfer: usize,
    /// Largest hot-tier size the recipient reported during the transfer, in
    /// bytes. Zero when the recipient has no memory tier configured.
    pub recipient_peak_hot_bytes: u64,
    /// Segments the recipient evicted while receiving. Zero alongside a growing
    /// `recipient_peak_hot_bytes` is the signature of a tier that is being asked
    /// to shed and declining -- which is the difference between "bounded" and
    /// "happens to fit".
    pub recipient_evicted_segments: u64,
}

/// What dropping the donor's copy did.
#[derive(Debug, Clone)]
pub struct Reclaim {
    pub slot: u32,
    pub from: u64,
    pub to: u64,
    pub dropped: usize,
    /// Cells the new owner turned out not to have, so they were transferred
    /// before being dropped. Non-zero means writes reached the donor after the
    /// last delta pass.
    pub carried_over: usize,
    /// Cells left on the donor because they could not be safely dropped. Always
    /// the safe outcome, and always worth investigating.
    pub retained: usize,
}

#[derive(Debug)]
pub enum MigrationError {
    /// The move was never attempted, or the plan asked for something that
    /// cannot be a migration.
    Invalid(String),
    /// The placement state machine refused, or could not be reached.
    Placement(String),
    /// A donor or recipient could not be reached.
    Rpc(RPCError),
    /// Cells could not be read from the donor or written to the recipient. The
    /// slot is left with its donor.
    Transfer(String),
    /// The table does not say what the caller believed it says. Nothing was
    /// destroyed.
    NotCommitted(String),
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationError::Invalid(reason) => write!(f, "invalid migration: {reason}"),
            MigrationError::Placement(reason) => write!(f, "placement refused: {reason}"),
            MigrationError::Rpc(error) => write!(f, "member unreachable: {error:?}"),
            MigrationError::Transfer(reason) => write!(f, "transfer failed: {reason}"),
            MigrationError::NotCommitted(reason) => write!(f, "not committed: {reason}"),
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<RPCError> for MigrationError {
    fn from(error: RPCError) -> Self {
        MigrationError::Rpc(error)
    }
}

pub(crate) fn placement_client(client: &AsyncClient) -> SlotsSMClient {
    SlotsSMClient::new(crate::server::SLOTS_SM_ID, &client.raft_client)
}

/// Cut a slot's ids into batches that never straddle an id class.
///
/// A slot is two contiguous spans of the id space — one per class tag — and
/// this keeps each batch inside one of them, ascending. That is worth the sort:
/// a batch is then a *range*, which is what makes an interrupted transfer
/// resumable by naming where it got to rather than by replaying a cursor, and
/// it keeps the donor's reads clustered instead of scattered across the space.
fn range_batches(ids: Vec<Id>, batch_cells: usize) -> Vec<Vec<Id>> {
    let batch_cells = batch_cells.max(1);
    let (mut hashed, mut allocated): (Vec<Id>, Vec<Id>) =
        ids.into_iter().partition(|id| id.is_hashed());
    allocated.sort_unstable_by_key(|id| id.bits());
    hashed.sort_unstable_by_key(|id| id.bits());
    allocated
        .chunks(batch_cells)
        .chain(hashed.chunks(batch_cells))
        .map(|batch| batch.to_vec())
        .collect()
}

/// Ids of every live cell a member holds in one slot.
async fn enumerate_slot(member: &Arc<CellServiceClient>, slot: u32) -> Result<Vec<Id>, RPCError> {
    member.cell_ids_in_slots(&vec![slot]).await
}

/// Every live cell a member holds across many slots, bucketed by slot, in ONE
/// pass over its cell index.
///
/// The distinction from [`enumerate_slot`] is asymptotic, not stylistic.
/// `cell_ids_in_slots` scans the donor's whole index whatever it is asked for,
/// so asking per slot makes a bulk move **O(slots x cells)**: measured on .239,
/// resharding 4096 slots of a 4.19M-cell store meant ~12 000 full index passes,
/// each allocating a fresh ~67 MB vector — roughly 800 GB of allocation churn to
/// move 16 GB of data. Asking once for the whole set makes it O(cells + slots).
///
/// This is why the RPC takes a slot *set*. Any caller moving more than one slot
/// must come through here.
async fn enumerate_slots(
    member: &Arc<CellServiceClient>,
    slots: &[u32],
) -> Result<std::collections::HashMap<u32, Vec<Id>>, RPCError> {
    let mut held: std::collections::HashMap<u32, Vec<Id>> = Default::default();
    if slots.is_empty() {
        return Ok(held);
    }
    for id in member.cell_ids_in_slots(&slots.to_vec()).await? {
        held.entry(crate::slots::slot_of(id_ref(&id))).or_default().push(id);
    }
    Ok(held)
}

#[inline]
fn id_ref(id: &Id) -> &Id {
    id
}

struct BatchOutcome {
    /// Exactly the ids the recipient now holds a copy of. Returned as ids
    /// rather than a count because the reclaim decides what it may destroy from
    /// this, and a count cannot say *which* cells arrived.
    transferred: Vec<Id>,
    vanished: usize,
}

/// Move one batch, donor -> recipient directly.
///
/// The coordinator names the cells and the target and gets back only the ids that
/// landed; the bodies never pass through it. That matters because the byte path is
/// what limits a transfer at realistic cell sizes — routing bodies through here
/// would double both the wire traffic and the serialization work for no gain.
async fn transfer_batch(
    donor: &Arc<CellServiceClient>,
    to: u64,
    batch: &Vec<Id>,
) -> Result<BatchOutcome, MigrationError> {
    let landed = donor
        .push_cells_to(batch, to)
        .await?
        .map_err(MigrationError::Transfer)?;
    // Anything the donor did not send is a cell that vanished between being
    // enumerated and being read. Not an error: a write failure comes back as one.
    let vanished = batch.len().saturating_sub(landed.len());
    Ok(BatchOutcome {
        transferred: landed,
        vanished,
    })
}

/// Stream exactly these cells of one slot to the recipient.
///
/// Does **not** begin, commit, or look for more work. Delta rounds are the
/// caller's job, deliberately: the enumeration behind a delta round scans the
/// donor's whole index whatever it is asked for, so a delta *inside* here would
/// reintroduce a per-slot full pass — the exact quadratic term that
/// `enumerate_slots` exists to remove. Measured: leaving the delta in here kept a
/// 512-slot reshard at 67 s where the work itself takes ~30 s.
async fn transfer_slot(
    donor: &Arc<CellServiceClient>,
    recipient: &Arc<CellServiceClient>,
    slot: u32,
    from: u64,
    to: u64,
    plan: &MigrationPlan,
    ids: Vec<Id>,
) -> Result<SlotHandover, MigrationError> {
    let mut handover = SlotHandover {
        slot,
        from,
        to,
        cells_transferred: 0,
        batches: 0,
        delta_rounds_used: if ids.is_empty() { 0 } else { 1 },
        vanished_before_transfer: 0,
        recipient_peak_hot_bytes: 0,
        recipient_evicted_segments: 0,
    };
    for batch in range_batches(ids, plan.batch_cells) {
        let outcome = transfer_batch(donor, to, &batch).await?;
        handover.cells_transferred += outcome.transferred.len();
        handover.vanished_before_transfer += outcome.vanished;
        handover.batches += 1;

        if plan.settle_recipient_per_batch {
            match recipient.settle_bulk_receive().await {
                Ok(report) => {
                    let observed = if report.hot_bytes_scanned > 0 {
                        report.hot_bytes_scanned
                    } else {
                        report.hot_bytes
                    };
                    handover.recipient_peak_hot_bytes =
                        handover.recipient_peak_hot_bytes.max(observed);
                    handover.recipient_evicted_segments += report.evicted_segments;
                }
                // A settle is an optimisation, never a precondition: the cells are
                // already written and durable.
                Err(error) => warn!(
                    "recipient {to} could not settle after a batch of slot {slot}: {error:?}"
                ),
            }
        }
    }
    Ok(handover)
}

/// Fold a later round's handover into the running one for the same slot.
fn accumulate(into: &mut SlotHandover, extra: SlotHandover) {
    into.cells_transferred += extra.cells_transferred;
    into.batches += extra.batches;
    into.vanished_before_transfer += extra.vanished_before_transfer;
    into.delta_rounds_used += extra.delta_rounds_used;
    into.recipient_peak_hot_bytes = into
        .recipient_peak_hot_bytes
        .max(extra.recipient_peak_hot_bytes);
    into.recipient_evicted_segments += extra.recipient_evicted_segments;
}

/// Move one slot's cells and flip its owner.
///
/// The donor keeps its copy: call [`reclaim_donor_copy`] to drop it, once the
/// caller is ready to give up the fallback that copy provides.
pub async fn migrate_slot(
    client: &Arc<AsyncClient>,
    slot: u32,
    from: u64,
    to: u64,
    plan: &MigrationPlan,
) -> Result<SlotHandover, MigrationError> {
    // Enumerates for this one slot. Callers moving a SET of slots must not loop
    // over this -- see `enumerate_slots` for why that is quadratic -- and should
    // use `reshard_slots` or the drain instead.
    let donor = client.client_by_server_id(from).await?;
    let ids = enumerate_slot(&donor, slot).await?;
    migrate_slot_prepared(client, slot, from, to, plan, ids).await
}

/// [`migrate_slot`], with this slot's cell ids already known.
///
/// Split out so a bulk caller can enumerate the donor **once** for every slot it
/// intends to move and then drive each one from that single pass.
async fn migrate_slot_prepared(
    client: &Arc<AsyncClient>,
    slot: u32,
    from: u64,
    to: u64,
    plan: &MigrationPlan,
    known: Vec<Id>,
) -> Result<SlotHandover, MigrationError> {
    if (slot as usize) >= SLOT_COUNT {
        return Err(MigrationError::Invalid(format!(
            "slot {slot} is outside the {SLOT_COUNT}-slot space"
        )));
    }
    if from == to {
        return Err(MigrationError::Invalid(format!(
            "slot {slot} cannot migrate from {from} to itself"
        )));
    }
    if from == 0 || to == 0 {
        return Err(MigrationError::Invalid(
            "server id 0 is not a member".to_string(),
        ));
    }

    // Both members are resolved BEFORE the slot is marked migrating, so an
    // unreachable member leaves the table untouched rather than needing an
    // abort to undo.
    let donor = client.client_by_server_id(from).await?;
    let recipient = client.client_by_server_id(to).await?;

    let placement = placement_client(client);
    let group = slot_group_id(client.group_name());
    placement
        .begin_slot_migration(&group, &slot, &from, &to)
        .await
        .map_err(|error| MigrationError::Placement(format!("{error:?}")))?
        .map_err(MigrationError::Placement)?;

    let mut handover = SlotHandover {
        slot,
        from,
        to,
        cells_transferred: 0,
        batches: 0,
        delta_rounds_used: 0,
        vanished_before_transfer: 0,
        recipient_peak_hot_bytes: 0,
        recipient_evicted_segments: 0,
    };
    let mut moved: std::collections::HashSet<Id> = Default::default();

    let outcome = async {
        for round in 0..plan.delta_rounds.max(1) {
            // Round 0 uses what the caller already enumerated; later rounds
            // have to look again, because their whole purpose is to catch cells
            // that arrived since. A slot that was empty to begin with therefore
            // costs no enumeration at all.
            let pending: Vec<Id> = if round == 0 {
                known.iter().copied().filter(|id| !moved.contains(id)).collect()
            } else {
                enumerate_slot(&donor, slot)
                    .await?
                    .into_iter()
                    .filter(|id| !moved.contains(id))
                    .collect()
            };
            if pending.is_empty() {
                // The pass that finds nothing is the one that proves the slot
                // is caught up, so it is not counted as a round of work.
                break;
            }
            handover.delta_rounds_used = round + 1;
            for batch in range_batches(pending, plan.batch_cells) {
                let outcome = transfer_batch(&donor, to, &batch).await?;
                handover.cells_transferred += outcome.transferred.len();
                handover.vanished_before_transfer += outcome.vanished;
                handover.batches += 1;
                // The whole batch, not just what transferred: a vanished cell
                // does not need looking for again either.
                moved.extend(batch.iter().copied());

                if plan.settle_recipient_per_batch {
                    match recipient.settle_bulk_receive().await {
                        Ok(report) => {
                            // Prefer the scanned figure when the recipient was
                            // asked to compute it: the counter is corrected by
                            // reconciliation rather than by each eviction, so
                            // right after a pass it can still include segments
                            // that have just gone cold. The scan is off by
                            // default because it costs a full sweep, so fall back
                            // to the counter -- an overestimate is the safe
                            // direction for a number used to spot a blow-up.
                            let observed = if report.hot_bytes_scanned > 0 {
                                report.hot_bytes_scanned
                            } else {
                                report.hot_bytes
                            };
                            handover.recipient_peak_hot_bytes =
                                handover.recipient_peak_hot_bytes.max(observed);
                            handover.recipient_evicted_segments += report.evicted_segments;
                        }
                        // A settle is an optimisation, never a precondition:
                        // the cells are already written and durable. Failing
                        // the migration over it would abandon a completed
                        // transfer for a hint we did not get.
                        Err(error) => warn!(
                            "recipient {to} could not settle after a batch of slot {slot}: {error:?}"
                        ),
                    }
                }
            }
        }
        Ok::<(), MigrationError>(())
    }
    .await;

    if let Err(error) = outcome {
        // Give the slot back to its donor. Whatever reached the recipient is
        // harmless: nothing routes to it, and a retry upserts over it.
        if let Err(abort_error) = placement.abort_slot_migration(&group, &slot).await {
            warn!(
                "slot {slot} transfer failed ({error}) and the migration could not be aborted \
                 ({abort_error:?}); the slot stays with donor {from} but its table entry still \
                 reads as migrating"
            );
        }
        return Err(error);
    }

    // The commit point.
    let (owner, applied_index) = placement
        .complete_slot_migration_with_index(&group, &slot)
        .await
        .map_err(|error| MigrationError::Placement(format!("{error:?}")))?;
    let owner = owner.map_err(MigrationError::NotCommitted)?;
    if owner != to {
        return Err(MigrationError::NotCommitted(format!(
            "slot {slot} committed to {owner}, not to the intended recipient {to}"
        )));
    }

    // Follow our own commit immediately, so the caller's next read of a
    // migrated cell goes to the new owner rather than to a donor we are about
    // to ask to drop it. Applied from the command's own answer rather than by
    // re-reading the table -- see `note_slot_owner` for why a read-back here
    // can legitimately return the state before the commit.
    client.note_slot_owner(slot, owner, applied_index);
    // And push it to the two members that must not be wrong about this slot:
    // the new owner, which is about to start answering for it, and the donor,
    // which must stop. Pushed rather than left for them to re-read, for the
    // same reason -- a query can be served the state this commit replaced.
    //
    // Best-effort: the table is already committed, so a member that misses this
    // is stale rather than wrong, and its copy of the data is still intact
    // because the drop is deferred.
    for (member, member_client) in [(from, &donor), (to, &recipient)] {
        if let Err(error) = member_client
            .note_slot_owner(slot, owner, applied_index)
            .await
        {
            warn!(
                "slot {slot} committed to {owner} but member {member} could not be told \
                 ({error:?}); it will route by a table one migration behind until it refreshes"
            );
        }
    }

    info!(
        "slot {} migrated {} -> {}: {} cells in {} batches over {} pass(es), \
         recipient hot tier peaked at {} MB",
        slot,
        from,
        to,
        handover.cells_transferred,
        handover.batches,
        handover.delta_rounds_used,
        handover.recipient_peak_hot_bytes / (1024 * 1024)
    );
    Ok(handover)
}

/// How long to keep asking whether the flip has been applied, and how often.
///
/// A placement *query* can be served by a member that holds the committing log
/// entry but has not applied it yet, so the state machine can honestly answer
/// with the state immediately before a commit that has already happened.
const HANDOVER_CONFIRM_ATTEMPTS: usize = 20;
const HANDOVER_CONFIRM_DELAY_MS: u64 = 50;

/// Establish, from the state machine, that the slot really is the recipient's.
///
/// Tolerates exactly one other answer: `Migrating { from, to }` for *this*
/// migration, which is the state the commit replaced and therefore the only one
/// a lagging replica can legitimately still be showing. Anything else is a real
/// disagreement and is reported rather than waited out — a slot that is stable
/// on somebody else is not a slot whose donor copy may be destroyed.
async fn confirm_handover(
    placement: &SlotsSMClient,
    group: u64,
    slot: u32,
    from: u64,
    to: u64,
) -> Result<(), MigrationError> {
    let mut last = None;
    for attempt in 0..HANDOVER_CONFIRM_ATTEMPTS {
        let state = placement
            .slot_state(&group, &slot)
            .await
            .map_err(|error| MigrationError::Placement(format!("{error:?}")))?;
        match state {
            Some(SlotState::Stable { owner }) if owner == to => return Ok(()),
            Some(SlotState::Migrating {
                from: active_from,
                to: active_to,
            }) if active_from == from && active_to == to => {
                last = state;
                if attempt + 1 < HANDOVER_CONFIRM_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        HANDOVER_CONFIRM_DELAY_MS,
                    ))
                    .await;
                }
            }
            other => {
                return Err(MigrationError::NotCommitted(format!(
                    "slot {slot} must be stable on {to} before donor {from} may drop it; \
                     the table says {other:?}"
                )))
            }
        }
    }
    Err(MigrationError::NotCommitted(format!(
        "slot {slot} still reads as {last:?} after {HANDOVER_CONFIRM_ATTEMPTS} attempts; \
         donor {from} keeps its copy"
    )))
}

/// Drop the donor's copy of a slot that has already been handed over.
///
/// Refuses unless the table says the slot is stable on `to` — read back from
/// the state machine, not taken from the caller. Anything the recipient turns
/// out to be missing is transferred first and only then dropped, so a write
/// that reached the donor after the last delta pass is carried over rather than
/// destroyed.
pub async fn reclaim_donor_copy(
    client: &Arc<AsyncClient>,
    slot: u32,
    from: u64,
    to: u64,
    plan: &MigrationPlan,
) -> Result<Reclaim, MigrationError> {
    let donor = client.client_by_server_id(from).await?;
    let ids = enumerate_slot(&donor, slot).await?;
    reclaim_donor_copy_prepared(client, slot, from, to, plan, ids).await
}

/// [`reclaim_donor_copy`], with the donor's remaining ids for this slot already
/// known, so a bulk caller enumerates once for the whole set.
async fn reclaim_donor_copy_prepared(
    client: &Arc<AsyncClient>,
    slot: u32,
    from: u64,
    to: u64,
    plan: &MigrationPlan,
    known: Vec<Id>,
) -> Result<Reclaim, MigrationError> {
    let placement = placement_client(client);
    let group = slot_group_id(client.group_name());
    confirm_handover(&placement, group, slot, from, to).await?;
    let donor = client.client_by_server_id(from).await?;
    let recipient = client.client_by_server_id(to).await?;
    reclaim_slot_confirmed(&donor, &recipient, slot, from, to, plan, known).await
}

/// The reclaim itself, for a handover already known to have committed.
///
/// A bulk caller learns that from `complete_slot_migrations`, whose return value
/// is produced by its own apply and is therefore authoritative — better than a
/// query, which can be served by a member that has not applied the commit yet.
/// So the bulk path skips `confirm_handover` entirely rather than paying a query
/// per slot to re-learn something it already knows.
async fn reclaim_slot_confirmed(
    donor: &Arc<CellServiceClient>,
    recipient: &Arc<CellServiceClient>,
    slot: u32,
    from: u64,
    to: u64,
    plan: &MigrationPlan,
    known: Vec<Id>,
) -> Result<Reclaim, MigrationError> {
    let mut reclaim = Reclaim {
        slot,
        from,
        to,
        dropped: 0,
        carried_over: 0,
        retained: 0,
    };

    for batch in range_batches(known, plan.batch_cells) {
        // Freshness at the new owner, without moving any bodies to find out.
        //
        // Existence alone is not enough, and that gap was data loss: the donor
        // stays the serving owner for the whole transfer, so it legitimately
        // accepts writes *after* the transfer has read a cell. Those updates are
        // invisible to the delta rounds, which look for new ids rather than new
        // versions, and a carry-over that only asks "does the recipient have it?"
        // then drops the newer copy. Demonstrated by
        // `an_update_during_transfer_is_lost`.
        //
        // Comparing versions is sound *because* migration preserves them: a
        // transferred cell lands on the version it had, so the two sides agree
        // unless one of them has since taken a write. Before that fix the
        // recipient was always one ahead and this comparison would have been
        // meaningless -- which is why an earlier version of this code did not try.
        let heads = recipient.head_all_cells(&batch).await?;
        let donor_heads = donor.head_all_cells(&batch).await?;
        let donor_versions: std::collections::HashMap<Id, u64> = batch
            .iter()
            .zip(donor_heads.into_iter())
            .filter_map(|(id, head)| head.ok().map(|header| (*id, header.version)))
            .collect();
        let mut droppable: Vec<Id> = Vec::with_capacity(batch.len());
        let mut missing: Vec<Id> = Vec::new();
        for (id, head) in batch.iter().zip(heads.into_iter()) {
            match head {
                // The recipient has it, but the donor's copy is NEWER: a write
                // landed after the transfer read this cell. Carry it over rather
                // than destroy it. `missing` is really "needs sending", and a
                // stale copy needs sending exactly as much as an absent one does.
                Ok(header)
                    if donor_versions
                        .get(id)
                        .is_some_and(|donor_version| *donor_version > header.version) =>
                {
                    missing.push(*id)
                }
                Ok(_) => droppable.push(*id),
                Err(ReadError::CellDoesNotExisted) => missing.push(*id),
                Err(error) => {
                    // We cannot establish that the new owner has it, so we do
                    // not destroy the only copy we know about.
                    warn!(
                        "slot {slot}: could not check {id:?} on new owner {to} ({error:?}); \
                         keeping the donor's copy"
                    );
                    reclaim.retained += 1;
                }
            }
        }

        if !missing.is_empty() {
            // The new owner is authoritative for this slot now, so this is a
            // plain repair rather than a migration step: send what it is
            // missing, then it is safe to drop.
            let outcome = transfer_batch(&donor, to, &missing).await?;
            reclaim.carried_over += outcome.transferred.len();
            // Vanished ids are already gone from the donor; counting them as
            // dropped keeps the totals honest against the enumeration.
            reclaim.dropped += outcome.vanished;
            let unaccounted = missing.len() - outcome.transferred.len() - outcome.vanished;
            reclaim.retained += unaccounted;
            // Only what demonstrably reached the new owner may be destroyed.
            droppable.extend(outcome.transferred);
        }

        if droppable.is_empty() {
            continue;
        }
        // Likewise exempt: by now the slot belongs to the recipient, so the
        // donor is deliberately deleting cells it no longer owns.
        let removals = donor.drop_migrated_cells(&droppable).await?;
        for (id, removal) in droppable.iter().zip(removals.into_iter()) {
            match removal {
                Ok(()) => reclaim.dropped += 1,
                // Already gone: the goal was for the donor not to hold it.
                Err(WriteError::CellDoesNotExisted) => reclaim.dropped += 1,
                Err(error) => {
                    warn!("slot {slot}: donor {from} could not drop {id:?}: {error:?}");
                    reclaim.retained += 1;
                }
            }
        }
    }

    if reclaim.retained > 0 {
        // A retained copy means the reclaim could not establish that the new
        // owner holds some cell, so the donor kept it. Nothing is lost -- that
        // is the point of retaining -- but the slot now has cells the placement
        // table does not route to, and only a re-run reclaim will converge it.
        // An operator watching a manual reshard reads this and re-runs; an
        // automatic balancer must treat it as a stop signal (Phase 4 decision:
        // `retained > 0` halts the balancer rather than continuing to move
        // slots on a cluster that cannot verify its transfers).
        warn!(
            "slot {} reclaim on {} RETAINED {} cell(s) it could not verify on {}: \
             {} dropped, {} carried over. The donor keeps the unverified copies; \
             re-run the reclaim to converge. An automatic balancer must stop here.",
            slot, from, reclaim.retained, to, reclaim.dropped, reclaim.carried_over
        );
    } else {
        info!(
            "slot {} donor copy reclaimed on {}: {} dropped, {} carried over to {}, {} retained",
            slot, from, reclaim.dropped, reclaim.carried_over, to, reclaim.retained
        );
    }
    Ok(reclaim)
}

/// What an operator-driven reshard did, slot by slot.
///
/// Not a `Result`: each slot commits independently, so a reshard that fails
/// partway has genuinely moved the slots it reports and genuinely left the rest
/// where they were. Collapsing that into one error would throw away the only
/// information an operator needs.
#[derive(Debug, Default)]
pub struct Reshard {
    pub handovers: Vec<SlotHandover>,
    pub reclaims: Vec<Reclaim>,
    pub failed: Vec<(u32, String)>,
}

/// Move a named set of slots between two members.
///
/// The drop is deferred across the *whole* set, not just each slot: every slot
/// is handed over first, then the donor copies are reclaimed. So for the
/// duration of the reshard every cell involved exists on both members, and a
/// client with a stale table still reads correctly no matter how far the
/// reshard has got.
pub async fn reshard_slots(
    client: &Arc<AsyncClient>,
    slots: &[u32],
    from: u64,
    to: u64,
    plan: &MigrationPlan,
) -> Reshard {
    let mut reshard = Reshard::default();
    if slots.is_empty() {
        return reshard;
    }
    let concurrency = plan.concurrent_slots.max(1);

    macro_rules! fail_all {
        ($slots:expr, $reason:expr) => {{
            for slot in $slots {
                reshard.failed.push((slot, $reason.clone()));
            }
            return reshard;
        }};
    }

    let donor = match client.client_by_server_id(from).await {
        Ok(donor) => donor,
        Err(error) => fail_all!(slots.to_vec(), format!("donor unreachable: {error:?}")),
    };
    let recipient = match client.client_by_server_id(to).await {
        Ok(recipient) => recipient,
        Err(error) => fail_all!(slots.to_vec(), format!("recipient unreachable: {error:?}")),
    };
    let placement = placement_client(client);
    let group = slot_group_id(client.group_name());

    // ONE raft command to begin them all, before any enumeration, so a slot
    // refused by placement is never transferred.
    let moves: Vec<(u32, u64, u64)> = slots.iter().map(|slot| (*slot, from, to)).collect();
    let refused = match placement.begin_slot_migrations(&group, &moves).await {
        Ok(refused) => refused,
        Err(error) => fail_all!(slots.to_vec(), format!("bulk begin failed: {error:?}")),
    };
    let refused_slots: std::collections::HashSet<u32> =
        refused.iter().map(|(slot, _)| *slot).collect();
    for (slot, reason) in refused {
        reshard.failed.push((slot, format!("begin refused: {reason}")));
    }
    let began: Vec<u32> = slots
        .iter()
        .copied()
        .filter(|slot| !refused_slots.contains(slot))
        .collect();

    // Transfer as a SWEEP-level loop, not a per-slot one. Every enumeration scans
    // the donor's whole index, so a delta round per slot is a full pass per slot --
    // the quadratic term. One pass per round covers every slot at once, and the
    // rounds converge for the same reason a drain's do: a round that finds nothing
    // new is the proof that the set is caught up.
    let mut handovers: std::collections::HashMap<u32, SlotHandover> = Default::default();
    let mut moved_ids: std::collections::HashSet<Id> = Default::default();
    let mut aborted: Vec<u32> = Vec::new();
    for round in 0..plan.delta_rounds.max(1) {
        let held = match enumerate_slots(&donor, &began).await {
            Ok(held) => held,
            Err(error) if round == 0 => fail_all!(
                began.clone(),
                format!("could not enumerate donor: {error:?}")
            ),
            // A later round failing is not fatal: the earlier rounds transferred,
            // and the reclaim's carry-over is the backstop for anything missed.
            Err(error) => {
                warn!("delta round {round} enumeration failed ({error:?}); committing what moved");
                break;
            }
        };
        let pending: Vec<(u32, Vec<Id>)> = began
            .iter()
            .filter_map(|slot| {
                let ids: Vec<Id> = held
                    .get(slot)
                    .map(|ids| {
                        ids.iter()
                            .copied()
                            .filter(|id| !moved_ids.contains(id))
                            .collect()
                    })
                    .unwrap_or_default();
                (!ids.is_empty()).then_some((*slot, ids))
            })
            .collect();
        if pending.is_empty() {
            break;
        }
        for (_, ids) in &pending {
            moved_ids.extend(ids.iter().copied());
        }

        // Spawned, not merely `buffer_unordered`. The distinction is the whole
        // point of scaling with cores: `buffer_unordered` interleaves futures
        // *within one task*, which only helps where the work actually yields. Two
        // members in one process talk over the local RPC shortcut, which completes
        // synchronously and never returns Pending -- so an unspawned fan-out runs
        // strictly one slot at a time. Measured: 36.0s at concurrency 1, 36.0s at
        // 32. Spawning puts each slot on the runtime's thread pool, so the work
        // parallelises whether the peer is a socket or a shortcut.
        //
        // A semaphore bounds it rather than spawning everything at once: in-flight
        // data on the recipient is `permits x batch_cells`, and the tier's peak
        // moves with it.
        let permits = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let mut tasks = Vec::with_capacity(pending.len());
        for (slot, ids) in pending {
            let permits = permits.clone();
            let donor = donor.clone();
            let recipient = recipient.clone();
            let plan = *plan;
            tasks.push(tokio::spawn(async move {
                let _permit = permits.acquire().await;
                (
                    slot,
                    transfer_slot(&donor, &recipient, slot, from, to, &plan, ids).await,
                )
            }));
        }
        let mut transferred: Vec<(u32, Result<SlotHandover, MigrationError>)> =
            Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(outcome) => transferred.push(outcome),
                // A panicking transfer task must not be silently dropped: the slot
                // would look untouched while its migration is half-done.
                Err(join_error) => {
                    warn!("a slot transfer task failed to join: {join_error:?}");
                }
            }
        }
        for (slot, outcome) in transferred {
            match outcome {
                Ok(handover) => match handovers.get_mut(&slot) {
                    Some(existing) => accumulate(existing, handover),
                    None => {
                        handovers.insert(slot, handover);
                    }
                },
                Err(error) => {
                    reshard.failed.push((slot, error.to_string()));
                    aborted.push(slot);
                    handovers.remove(&slot);
                }
            }
        }
    }

    // Slots that began but held nothing still have to change hands.
    for slot in &began {
        if !handovers.contains_key(slot) && !aborted.contains(slot) {
            handovers.insert(
                *slot,
                SlotHandover {
                    slot: *slot,
                    from,
                    to,
                    cells_transferred: 0,
                    batches: 0,
                    delta_rounds_used: 0,
                    vanished_before_transfer: 0,
                    recipient_peak_hot_bytes: 0,
                    recipient_evicted_segments: 0,
                },
            );
        }
    }

    // A slot whose transfer failed goes back to its donor. Whatever reached the
    // recipient is harmless: nothing routes to it, and a retry upserts over it.
    for slot in &aborted {
        if let Err(error) = placement.abort_slot_migration(&group, slot).await {
            warn!("slot {slot} transfer failed and could not be aborted: {error:?}");
        }
    }
    let handovers: Vec<SlotHandover> = handovers.into_values().collect();

    // ONE raft command to commit them all. Its return value is produced by its own
    // apply, so it is authoritative about what committed -- no follow-up query,
    // which also sidesteps a query being served the pre-commit state.
    let ready: Vec<u32> = handovers.iter().map(|handover| handover.slot).collect();
    let (committed, applied_index) = match placement
        .complete_slot_migrations_with_index(&group, &ready)
        .await
    {
        Ok(committed) => committed,
        Err(error) => {
            for slot in ready {
                reshard
                    .failed
                    .push((slot, format!("bulk commit failed: {error:?}")));
            }
            return reshard;
        }
    };
    let committed_map: std::collections::HashMap<u32, u64> = committed.iter().copied().collect();
    for handover in handovers {
        match committed_map.get(&handover.slot) {
            Some(owner) if *owner == to => reshard.handovers.push(handover),
            other => reshard.failed.push((
                handover.slot,
                format!("transferred but committed to {other:?} rather than {to}"),
            )),
        }
    }

    // Follow our own commit locally, then push it to both members in one call
    // each rather than one per slot.
    for (slot, owner) in &committed {
        client.note_slot_owner(*slot, *owner, applied_index);
    }
    for (member, member_client) in [(from, &donor), (to, &recipient)] {
        if let Err(error) = member_client
            .note_slot_owners(&committed, applied_index)
            .await
        {
            warn!(
                "committed {} slots but member {member} could not be told ({error:?}); \
                 it will route by a table one migration behind until it refreshes",
                committed.len()
            );
        }
    }

    // The drop is deferred across the WHOLE set, so for the duration of the
    // reshard every cell involved exists on both members and a client with a stale
    // table still reads correctly however far the reshard has got.
    //
    // Re-enumerated once, because the reclaim's job is precisely to catch what
    // reached the donor after the transfer read it.
    let moved: Vec<u32> = reshard.handovers.iter().map(|h| h.slot).collect();
    let after = match enumerate_slots(&donor, &moved).await {
        Ok(after) => after,
        Err(error) => {
            for slot in moved {
                reshard
                    .failed
                    .push((slot, format!("reclaim enumeration failed: {error:?}")));
            }
            return reshard;
        }
    };
    // Spawned for the same reason as the transfer above.
    let permits = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut tasks = Vec::with_capacity(moved.len());
    for slot in &moved {
        let slot = *slot;
        let ids = after.get(&slot).cloned().unwrap_or_default();
        let permits = permits.clone();
        let donor = donor.clone();
        let recipient = recipient.clone();
        let plan = *plan;
        tasks.push(tokio::spawn(async move {
            let _permit = permits.acquire().await;
            (
                slot,
                reclaim_slot_confirmed(&donor, &recipient, slot, from, to, &plan, ids).await,
            )
        }));
    }
    let mut reclaimed: Vec<(u32, Result<Reclaim, MigrationError>)> =
        Vec::with_capacity(tasks.len());
    for task in tasks {
        match task.await {
            Ok(outcome) => reclaimed.push(outcome),
            Err(join_error) => warn!("a reclaim task failed to join: {join_error:?}"),
        }
    }
    for (slot, outcome) in reclaimed {
        match outcome {
            Ok(reclaim) => reshard.reclaims.push(reclaim),
            Err(error) => reshard.failed.push((slot, format!("reclaim: {error}"))),
        }
    }

    reshard.handovers.sort_unstable_by_key(|handover| handover.slot);
    reshard.reclaims.sort_unstable_by_key(|reclaim| reclaim.slot);
    reshard.failed.sort_unstable_by_key(|(slot, _)| *slot);
    reshard
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MEASUREMENT: how much of a transfer's per-cell cost is bifrost's codec?
    ///
    /// ```text
    /// cargo test --release --lib codec_share_of_transfer_cost -- --ignored --nocapture
    /// ```
    ///
    /// Asked because "the transfer is slow" is not an actionable statement until
    /// the cost is attributed. A migration batch pays: read from the donor's
    /// segment, `to_owned`, serialize, ship, deserialize, write into the
    /// recipient's chunk, WAL, index. Only two of those are bifrost's, and the
    /// answer decides whether the next optimisation belongs in the RPC layer or in
    /// the storage path.
    ///
    /// **Must be run in release.** bifrost picks its codec by build profile --
    /// `serde_cbor` in release, `serde_json` under `debug_assertions` -- so a debug
    /// measurement is measuring JSON and is not a statement about production.
    #[test]
    #[ignore]
    fn codec_share_of_transfer_cost() {
        use crate::ram::types::{Map, OwnedMap, OwnedValue};

        const CELLS: usize = 1024;
        const PAYLOAD: usize = 4096;

        assert!(
            !cfg!(debug_assertions),
            "run this with --release: bifrost serializes with JSON under \
             debug_assertions and CBOR otherwise, so a debug number says nothing \
             about production"
        );

        let payload = "x".repeat(PAYLOAD);
        let cells: Vec<crate::ram::cell::OwnedCell> = (0..CELLS)
            .map(|seq| {
                let mut value = OwnedMap::new();
                value.insert(&String::from("id"), OwnedValue::I64(seq as i64));
                value.insert(&String::from("score"), OwnedValue::U64(seq as u64));
                value.insert(&String::from("name"), OwnedValue::String(payload.clone()));
                crate::ram::cell::OwnedCell::new_with_id(
                    1,
                    &Id::from_parts(1, seq as u64),
                    OwnedValue::Map(value),
                )
            })
            .collect();

        let started = std::time::Instant::now();
        let encoded = bifrost::utils::serde::serialize(&cells);
        let encode = started.elapsed();

        let started = std::time::Instant::now();
        let decoded: Option<Vec<crate::ram::cell::OwnedCell>> =
            bifrost::utils::serde::deserialize(&encoded);
        let decode = started.elapsed();
        assert_eq!(decoded.map(|cells| cells.len()), Some(CELLS));

        let bytes = (CELLS * PAYLOAD) as f64;
        let round_trip = encode + decode;
        println!(
            "CODEC: {} cells x {} B -> {} B wire ({:.2}x)",
            CELLS,
            PAYLOAD,
            encoded.len(),
            encoded.len() as f64 / bytes
        );
        println!(
            "CODEC: encode {:.1} us/cell ({:.0} MB/s), decode {:.1} us/cell ({:.0} MB/s)",
            encode.as_secs_f64() * 1e6 / CELLS as f64,
            bytes / (1024.0 * 1024.0) / encode.as_secs_f64(),
            decode.as_secs_f64() * 1e6 / CELLS as f64,
            bytes / (1024.0 * 1024.0) / decode.as_secs_f64()
        );
        println!(
            "CODEC: round trip {:.1} us/cell -- compare against the transfer's \
             measured per-cell cost to get the codec's share",
            round_trip.as_secs_f64() * 1e6 / CELLS as f64
        );
    }

    #[test]
    fn batches_never_straddle_an_id_class() {
        // A slot is two spans of the id space, not one. A batch that mixes them
        // is not a range, and the whole reason for sorting is to be able to say
        // where a transfer got to.
        let ids = vec![
            Id::hashed(0xffff_ffff_ffff_ffff),
            Id::from_parts(7, 3),
            Id::hashed(0x8000_0000_0000_0001),
            Id::from_parts(7, 1),
            Id::from_parts(7, 2),
        ];
        let batches = range_batches(ids, 2);
        for batch in &batches {
            let classes: std::collections::HashSet<bool> =
                batch.iter().map(|id| id.is_hashed()).collect();
            assert_eq!(
                classes.len(),
                1,
                "batch {batch:?} mixes hashed and allocated ids"
            );
            let sorted = batch.windows(2).all(|w| w[0].bits() <= w[1].bits());
            assert!(sorted, "batch {batch:?} is not ascending");
        }
    }

    #[test]
    fn batching_partitions_the_input_exactly() {
        // Nothing lost and nothing duplicated: a dropped id is a cell left
        // behind on the donor, and a duplicated one is wasted transfer.
        let ids: Vec<Id> = (0..37)
            .map(|seq| Id::from_parts(9, seq))
            .chain((0..11).map(|seq| Id::hashed(0xdead_0000_0000_0000 + seq)))
            .collect();
        let flattened: Vec<Id> = range_batches(ids.clone(), 5).into_iter().flatten().collect();
        assert_eq!(flattened.len(), ids.len());
        let expected: std::collections::HashSet<Id> = ids.into_iter().collect();
        let got: std::collections::HashSet<Id> = flattened.into_iter().collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn a_zero_batch_size_still_makes_progress() {
        // A misconfigured plan must not silently produce zero batches and
        // report a slot as migrated without moving anything.
        let ids: Vec<Id> = (0..4).map(|seq| Id::from_parts(3, seq)).collect();
        let batches = range_batches(ids, 0);
        assert_eq!(batches.iter().map(|b| b.len()).sum::<usize>(), 4);
        assert!(batches.iter().all(|batch| !batch.is_empty()));
    }

    #[test]
    fn an_empty_slot_produces_no_batches() {
        assert!(range_batches(vec![], 8).is_empty());
    }
}

/// Two-member tests: the mechanism that actually moves data.
///
/// These lean on a property Phase 1 bought and it is worth naming, because it
/// is what makes them deterministic where the historical cluster tests are not:
/// the first member to come up adopts the whole slot table, and a later joiner
/// claims nothing. So **every slot is on the first server** no matter how the
/// ring formed, and the tests can name a donor and a recipient instead of
/// waiting for placement to settle.
#[cfg(test)]
mod cluster_tests {
    use super::*;
    use crate::client;
    use crate::ram::schema::Schema;
    use crate::ram::tests::default_fields;
    use crate::ram::types::{Map, OwnedMap, OwnedValue};
    use crate::server::*;
    use std::collections::HashSet;
    use std::time::Duration;

    const SCHEMA_ID: u32 = 1300;

    async fn start_pair(group: &str) -> (Vec<Arc<NebServer>>, Arc<AsyncClient>) {
        start_cluster(group, 2).await
    }

    /// Start `members` servers in one group, with a client bound to the first.
    ///
    /// Returns them in start order, which is also ownership order: the first to
    /// come up adopts the whole slot table and every later member claims nothing.
    /// That is the property these tests rest on, and it is what makes them
    /// deterministic without waiting for a ring to settle.
    async fn start_cluster(group: &str, members: usize) -> (Vec<Arc<NebServer>>, Arc<AsyncClient>) {
        start_cluster_with(group, members, vec![Service::Cell]).await
    }

    /// As [`start_cluster`], with the services spelled out. Transaction tests need
    /// `Service::Transaction`, which the cell-only default does not start.
    async fn start_cluster_with(
        group: &str,
        members: usize,
        services: Vec<Service>,
    ) -> (Vec<Arc<NebServer>>, Arc<AsyncClient>) {
        let _ = env_logger::try_init();
        let addresses: Vec<String> = (0..members)
            .map(|_| crate::utils::test_port::unique_localhost_addr())
            .collect();
        let opts = ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services,
            enable_recovery: false,
            disable_storage_locks: true,
        };
        let mut servers = Vec::with_capacity(addresses.len());
        for address in &addresses {
            servers.push(
                NebServer::new_cluster_from_opts(&opts, address, &addresses, group, async |_| {})
                    .await
                    .unwrap(),
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = Arc::new(
            client::AsyncClient::new(&servers[0].rpc, &servers[0].membership, &addresses, group)
                .await
                .unwrap(),
        );
        client.reload_slot_owners().await;
        let schema = Schema::new_with_id(
            SCHEMA_ID,
            &String::from("migration_schema"),
            None,
            default_fields(),
            false,
            false,
        );
        client.new_schema_with_id(schema).await.unwrap().unwrap();
        (servers, client)
    }

    fn cell_in_slot(slot: u16, seq: u64, name: &str) -> (Id, crate::ram::cell::OwnedCell) {
        let id = Id::from_parts(slot as u64, seq);
        let mut value = OwnedMap::new();
        value.insert(&String::from("id"), OwnedValue::I64(seq as i64));
        value.insert(&String::from("score"), OwnedValue::U64(seq));
        value.insert(&String::from("name"), OwnedValue::String(name.to_string()));
        (
            id,
            crate::ram::cell::OwnedCell::new_with_id(SCHEMA_ID, &id, OwnedValue::Map(value)),
        )
    }

    fn held_in_slot(server: &Arc<NebServer>, slot: u16) -> HashSet<Id> {
        server
            .chunks()
            .cell_ids_in_slots(&HashSet::from([slot]))
            .into_iter()
            .collect()
    }

    /// The whole sequence: transfer, flip, and only then drop.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_slot_moves_to_its_new_owner_and_the_donor_keeps_its_copy_until_reclaimed() {
        let (servers, client) = start_pair("migration_handover_test").await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;
        assert_ne!(donor_id, recipient_id);

        const MOVING: u16 = 77;
        const STAYING: u16 = 78;
        const PER_SLOT: u64 = 9;

        let mut moving_ids = HashSet::new();
        let mut staying_ids = HashSet::new();
        for seq in 0..PER_SLOT {
            let (id, cell) = cell_in_slot(MOVING, seq, "moving");
            client.write_cell(cell).await.unwrap().unwrap();
            moving_ids.insert(id);
            let (id, cell) = cell_in_slot(STAYING, seq, "staying");
            client.write_cell(cell).await.unwrap().unwrap();
            staying_ids.insert(id);
        }

        // The premise: everything starts on the first member. If this fails the
        // rest of the test is measuring something else.
        assert_eq!(held_in_slot(&servers[0], MOVING), moving_ids);
        assert!(held_in_slot(&servers[1], MOVING).is_empty());

        // The per-slot byte counters agree with the premise -- these are what
        // the Phase 4 balancer will steer by, so a migration must move them
        // exactly as it moves the cells.
        let donor_moving_bytes = servers[0].chunks().slot_bytes.get(MOVING as u32);
        let donor_staying_bytes = servers[0].chunks().slot_bytes.get(STAYING as u32);
        assert!(donor_moving_bytes > 0, "the donor holds cells, so it must hold bytes");
        assert!(donor_staying_bytes > 0);
        assert_eq!(servers[1].chunks().slot_bytes.get(MOVING as u32), 0);

        // And over the wire, which is the only view the node manager will
        // ever have: positional, aligned with the asked slots, junk slot -> 0.
        let donor_rpc = client.client_by_server_id(donor_id).await.unwrap();
        assert_eq!(
            donor_rpc
                .slot_live_bytes(&vec![MOVING as u32, STAYING as u32, 999_999])
                .await
                .unwrap(),
            vec![donor_moving_bytes, donor_staying_bytes, 0]
        );
        assert!(donor_rpc.total_live_bytes().await.unwrap() >= donor_moving_bytes + donor_staying_bytes);

        // Deliberately smaller than the slot, so the batching and the settle
        // step are exercised rather than skipped.
        let plan = MigrationPlan {
            batch_cells: 4,
            ..Default::default()
        };
        let handover = migrate_slot(&client, MOVING as u32, donor_id, recipient_id, &plan)
            .await
            .expect("slot should migrate");
        assert_eq!(handover.cells_transferred, PER_SLOT as usize);
        assert!(
            handover.batches >= 3,
            "9 cells in batches of 4 should take at least 3 batches, took {}",
            handover.batches
        );
        assert_eq!(handover.delta_rounds_used, 1, "nothing was writing to the slot");

        // Transferred, and the donor still holds its copy: the drop is a
        // separate, later decision, and until it happens a client with a stale
        // table still reads correct data.
        assert_eq!(held_in_slot(&servers[1], MOVING), moving_ids);
        assert_eq!(
            held_in_slot(&servers[0], MOVING),
            moving_ids,
            "the donor must keep its copy until it is explicitly reclaimed"
        );

        // The commit point itself, read back from the state machine rather than
        // inferred from the transfer's own report.
        //
        // Through `confirm_handover` rather than a bare `slot_state`, for the
        // reason the reclaim uses it too: a query issued straight after the
        // committing command can be served by a member that has the log entry
        // and has not applied it, so a raw read here fails about one run in
        // three -- and it fails by reporting the state the commit replaced.
        let group = slot_group_id(client.group_name());
        confirm_handover(
            &placement_client(&client),
            group,
            MOVING as u32,
            donor_id,
            recipient_id,
        )
        .await
        .expect("the table must name the recipient once the migration has committed");

        // Placement followed the commit, and only for the slot that moved.
        for id in &moving_ids {
            assert_eq!(
                client.locate_server_id(id).unwrap(),
                recipient_id,
                "{id:?} in slot {MOVING} should route to the recipient (donor {donor_id})"
            );
        }
        for id in &staying_ids {
            assert_eq!(
                client.locate_server_id(id).unwrap(),
                donor_id,
                "moving one slot must not move any other"
            );
        }

        // And the cells are readable through the ordinary hashed path, which is
        // the only thing a caller actually cares about.
        for id in &moving_ids {
            let cell = client.read_cell(*id).await.unwrap().unwrap();
            assert_eq!(cell.header.id, *id);
        }

        // The transfer copied every cell, so the recipient's counter reaches
        // exactly the donor's, and the donor's is untouched until the reclaim.
        assert_eq!(
            servers[1].chunks().slot_bytes.get(MOVING as u32),
            donor_moving_bytes,
            "the recipient's slot bytes must equal what the donor was holding"
        );
        assert_eq!(
            servers[0].chunks().slot_bytes.get(MOVING as u32),
            donor_moving_bytes,
            "the donor's counter must not move before its copy is reclaimed"
        );

        let reclaim = reclaim_donor_copy(&client, MOVING as u32, donor_id, recipient_id, &plan)
            .await
            .expect("reclaim should be allowed once the slot is stable on the recipient");
        assert_eq!(reclaim.dropped, PER_SLOT as usize);
        assert_eq!(reclaim.carried_over, 0);
        assert_eq!(reclaim.retained, 0);

        assert!(
            held_in_slot(&servers[0], MOVING).is_empty(),
            "the donor should hold nothing in a slot it has given up"
        );
        assert_eq!(
            held_in_slot(&servers[1], MOVING),
            moving_ids,
            "reclaiming the donor copy must not touch the recipient's"
        );
        assert_eq!(
            held_in_slot(&servers[0], STAYING),
            staying_ids,
            "the reclaim must be confined to the migrated slot"
        );
        assert_eq!(
            servers[0].chunks().slot_bytes.get(MOVING as u32),
            0,
            "reclaiming the donor copy must return its slot bytes"
        );
        assert_eq!(
            servers[1].chunks().slot_bytes.get(MOVING as u32),
            donor_moving_bytes,
            "the reclaim must not touch the recipient's counter"
        );
        assert_eq!(
            servers[0].chunks().slot_bytes.get(STAYING as u32),
            donor_staying_bytes,
            "the untouched slot's counter must not move at all"
        );

        for id in &moving_ids {
            let cell = client.read_cell(*id).await.unwrap().unwrap();
            assert_eq!(cell.header.id, *id);
        }
        for id in &staying_ids {
            let cell = client.read_cell(*id).await.unwrap().unwrap();
            assert_eq!(cell.header.id, *id);
        }
    }

    /// A migration that cannot legitimately start must change nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_migration_leaves_both_placement_and_data_alone() {
        let (servers, client) = start_pair("migration_refusal_test").await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        const SLOT: u16 = 91;
        let (id, cell) = cell_in_slot(SLOT, 1, "refused");
        client.write_cell(cell).await.unwrap().unwrap();

        let plan = MigrationPlan::default();

        // Same member on both ends is not a migration.
        assert!(matches!(
            migrate_slot(&client, SLOT as u32, donor_id, donor_id, &plan).await,
            Err(MigrationError::Invalid(_))
        ));

        // A donor that does not own the slot: a planner working from a stale
        // table would otherwise move a slot out from under its real owner.
        assert!(matches!(
            migrate_slot(&client, SLOT as u32, recipient_id, donor_id, &plan).await,
            Err(MigrationError::Placement(_))
        ));

        // Outside the slot space.
        assert!(matches!(
            migrate_slot(&client, SLOT_COUNT as u32, donor_id, recipient_id, &plan).await,
            Err(MigrationError::Invalid(_))
        ));

        // And a reclaim before any handover must refuse rather than delete the
        // only copy of anything.
        assert!(matches!(
            reclaim_donor_copy(&client, SLOT as u32, donor_id, recipient_id, &plan).await,
            Err(MigrationError::NotCommitted(_))
        ));

        assert_eq!(client.locate_server_id(&id).unwrap(), donor_id);
        assert_eq!(held_in_slot(&servers[0], SLOT), HashSet::from([id]));
        client.read_cell(id).await.unwrap().unwrap();
    }

    /// A cell that reaches the donor after the flip is carried over, not
    /// destroyed. This is the reason the reclaim asks the new owner what it has
    /// instead of trusting the transfer's own record.
    #[tokio::test(flavor = "multi_thread")]
    async fn reclaim_carries_over_a_cell_the_new_owner_never_received() {
        let (servers, client) = start_pair("migration_carryover_test").await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        const SLOT: u16 = 64;
        let (moved_id, cell) = cell_in_slot(SLOT, 1, "moved");
        client.write_cell(cell).await.unwrap().unwrap();

        let plan = MigrationPlan::default();
        migrate_slot(&client, SLOT as u32, donor_id, recipient_id, &plan)
            .await
            .expect("slot should migrate");

        // Stands in for a write that the donor accepted microseconds *before* the
        // flip committed: the ownership guard checks at request time, so a
        // request that passed the check and was still writing when the table
        // changed lands anyway. That is the window the carry-over exists for,
        // and it is why the reclaim asks the new owner what it holds instead of
        // trusting the transfer's own record.
        //
        // Placed through the migration-exempt receive path because an ordinary
        // write to the donor is now (correctly) refused -- the guard closes the
        // *stale client* case, not this one.
        let (late_id, late_cell) = cell_in_slot(SLOT, 2, "late");
        let donor = client.client_by_server_id(donor_id).await.unwrap();
        donor
            .receive_migrated_cells(vec![late_cell])
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .unwrap();
        assert!(
            !held_in_slot(&servers[1], SLOT).contains(&late_id),
            "the premise is that the new owner has never seen this cell"
        );

        let reclaim = reclaim_donor_copy(&client, SLOT as u32, donor_id, recipient_id, &plan)
            .await
            .expect("reclaim should be allowed");
        assert_eq!(
            reclaim.carried_over, 1,
            "the late cell must be carried to the new owner, not dropped"
        );
        assert_eq!(reclaim.dropped, 2);
        assert_eq!(reclaim.retained, 0);

        assert_eq!(
            held_in_slot(&servers[1], SLOT),
            HashSet::from([moved_id, late_id])
        );
        assert!(held_in_slot(&servers[0], SLOT).is_empty());
        client.read_cell(late_id).await.unwrap().unwrap();
    }

    /// A member refuses writes for a slot it no longer owns, and names the owner.
    ///
    /// This is the difference between a stale placement table costing a hop and
    /// costing data. A write accepted by a former owner succeeds, satisfies the
    /// client, and lands where nothing will read it again.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_former_owner_refuses_writes_and_says_who_owns_the_slot() {
        let (servers, client) = start_pair("migration_refuse_foreign_test").await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        const SLOT: u16 = 55;
        let (seed_id, seed) = cell_in_slot(SLOT, 1, "seed");
        client.write_cell(seed).await.unwrap().unwrap();

        let plan = MigrationPlan::default();
        migrate_slot(&client, SLOT as u32, donor_id, recipient_id, &plan)
            .await
            .expect("slot should migrate");

        // Addressed directly at the former owner, which is exactly what a client
        // holding a table one migration behind would do.
        let donor = client.client_by_server_id(donor_id).await.unwrap();
        let (_, late) = cell_in_slot(SLOT, 2, "late");
        // `CellHeader` has no PartialEq, so match rather than compare.
        assert!(
            matches!(
                donor.upsert_cell(late).await.unwrap(),
                Err(crate::ram::cell::WriteError::NotSlotOwner { owner, .. })
                    if owner == recipient_id
            ),
            "the former owner must refuse and name the member that took over"
        );
        assert!(
            matches!(
                donor.remove_cell(seed_id).await.unwrap(),
                Err(crate::ram::cell::WriteError::NotSlotOwner { owner, .. })
                    if owner == recipient_id
            ),
            "removals are writes too -- a delete accepted here would be lost"
        );

        // The new owner accepts the same write.
        let (accepted_id, accepted) = cell_in_slot(SLOT, 3, "accepted");
        client
            .client_by_server_id(recipient_id)
            .await
            .unwrap()
            .upsert_cell(accepted)
            .await
            .unwrap()
            .expect("the owner must accept");
        assert!(held_in_slot(&servers[1], SLOT).contains(&accepted_id));

        // And a slot that did NOT move is still writable at the donor, so the
        // guard is per slot rather than a blanket refusal.
        let (untouched_id, untouched) = cell_in_slot(56, 1, "untouched");
        client
            .client_by_server_id(donor_id)
            .await
            .unwrap()
            .upsert_cell(untouched)
            .await
            .unwrap()
            .expect("a slot this member still owns must stay writable");
        assert!(held_in_slot(&servers[0], 56).contains(&untouched_id));
    }

    /// A client with a stale table writes successfully anyway, via the redirect.
    ///
    /// The refusal is only half the fix; without the client following it, the
    /// guard would convert silent data loss into loud failure rather than into
    /// correct behaviour.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_client_is_redirected_to_the_new_owner() {
        let (servers, client) = start_pair("migration_redirect_test").await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        const SLOT: u16 = 61;
        let (seed_id, seed) = cell_in_slot(SLOT, 1, "seed");
        client.write_cell(seed).await.unwrap().unwrap();

        migrate_slot(
            &client,
            SLOT as u32,
            donor_id,
            recipient_id,
            &MigrationPlan::default(),
        )
        .await
        .expect("slot should migrate");

        // Wind this client's placement back to before the migration, which is
        // what a member that missed the push looks like.
        client.force_slot_owner_for_test(SLOT as u32, donor_id);
        assert_eq!(client.locate_server_id(&seed_id).unwrap(), donor_id);

        let (late_id, late) = cell_in_slot(SLOT, 2, "late-but-redirected");
        client
            .upsert_cell(late)
            .await
            .unwrap()
            .expect("a stale client should still succeed, by being redirected");

        // It landed on the real owner, and the client learned the placement.
        assert!(held_in_slot(&servers[1], SLOT).contains(&late_id));
        assert!(!held_in_slot(&servers[0], SLOT).contains(&late_id));
        assert_eq!(client.locate_server_id(&late_id).unwrap(), recipient_id);
        client.read_cell(late_id).await.unwrap().unwrap();
    }

    /// A refusal from an intermediate owner cannot wind an already newer client
    /// back to that intermediate placement for the retry itself.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_refusal_uses_the_clients_newer_redirect_target() {
        let (servers, client) =
            start_cluster_with("migration_stale_refusal_test", 3, vec![Service::Cell]).await;

        const SLOT: u16 = 62;
        let (seed_id, seed) = cell_in_slot(SLOT, 1, "seed");
        let first_owner = client.locate_server_id(&seed_id).unwrap();
        let mut other_owners = servers
            .iter()
            .map(|server| server.server_id)
            .filter(|owner| *owner != first_owner);
        let intermediate_owner = other_owners.next().unwrap();
        let newest_owner = other_owners.next().unwrap();

        client.write_cell(seed).await.unwrap().unwrap();
        migrate_slot(
            &client,
            SLOT as u32,
            first_owner,
            intermediate_owner,
            &MigrationPlan::default(),
        )
        .await
        .expect("first migration should commit");
        let (_, intermediate_index) = client
            .conshash
            .slot_override_with_index(SLOT as u64)
            .expect("first migration should version the owner");

        migrate_slot(
            &client,
            SLOT as u32,
            intermediate_owner,
            newest_owner,
            &MigrationPlan::default(),
        )
        .await
        .expect("second migration should commit");
        let (cached_owner, newest_index) = client
            .conshash
            .slot_override_with_index(SLOT as u64)
            .expect("second migration should version the owner");
        assert_eq!(cached_owner, newest_owner);
        assert!(newest_index > intermediate_index);

        // This is the reply from a request sent before the second migration:
        // it names the then-current intermediate owner at its older index.
        let redirect = client
            .redirect_to_slot_owner(&seed_id, intermediate_owner, intermediate_index)
            .await
            .unwrap();
        let (late_id, late) = cell_in_slot(SLOT, 2, "newer-than-refusal");
        redirect
            .upsert_cell(late)
            .await
            .unwrap()
            .expect("the retry must use the newer cached owner");

        let newest_server = servers
            .iter()
            .find(|server| server.server_id == newest_owner)
            .unwrap();
        assert!(held_in_slot(newest_server, SLOT).contains(&late_id));
    }

    /// Phase 5's racing-write model: sustained writes across a migration lose
    /// nothing.
    ///
    /// The writer never pauses and never learns about the migration except by
    /// being refused. Every acknowledged write must be readable afterwards --
    /// that is the property the guard exists for, and the one that could not
    /// hold before it.
    #[tokio::test(flavor = "multi_thread")]
    async fn writes_racing_a_migration_are_never_lost() {
        let (servers, client) = start_pair("migration_racing_writes_test").await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        const SLOT: u16 = 71;
        const WRITES: u64 = 60;

        // Seed enough that the transfer has real work to do while writes land.
        for seq in 0..20 {
            let (_, cell) = cell_in_slot(SLOT, seq, "seed");
            client.write_cell(cell).await.unwrap().unwrap();
        }

        let writer_client = client.clone();
        let writer = tokio::spawn(async move {
            let mut acknowledged = Vec::new();
            for seq in 100..(100 + WRITES) {
                let (id, cell) = cell_in_slot(SLOT, seq, "racing");
                match writer_client.upsert_cell(cell).await {
                    // Acknowledged: from here on the cluster owes us this cell.
                    Ok(Ok(_)) => acknowledged.push(id),
                    // A refusal the client could not follow, or an RPC failure:
                    // nothing was written and nothing is owed, which is the
                    // honest outcome. Recorded so the test can say how many.
                    Ok(Err(_)) | Err(_) => {}
                }
                tokio::task::yield_now().await;
            }
            acknowledged
        });

        let handover = migrate_slot(
            &client,
            SLOT as u32,
            donor_id,
            recipient_id,
            &MigrationPlan {
                batch_cells: 8,
                ..Default::default()
            },
        )
        .await
        .expect("the slot should migrate while writes are in flight");

        let acknowledged = writer.await.expect("writer task should not panic");
        assert!(
            acknowledged.len() as u64 >= WRITES / 2,
            "only {} of {} writes were acknowledged; the guard should redirect,              not reject wholesale",
            acknowledged.len(),
            WRITES
        );

        // Carry over anything that reached the donor after the last delta pass,
        // then drop the donor's copy. This is the step that makes the property
        // hold, so the test must run it rather than assume it.
        let reclaim = reclaim_donor_copy(
            &client,
            SLOT as u32,
            donor_id,
            recipient_id,
            &MigrationPlan::default(),
        )
        .await
        .expect("reclaim should be allowed");
        assert_eq!(reclaim.retained, 0);

        // The property: every acknowledged write is still there.
        for id in &acknowledged {
            let cell = client
                .read_cell(*id)
                .await
                .unwrap()
                .unwrap_or_else(|error| {
                    panic!("acknowledged write {id:?} was lost by the migration: {error:?}")
                });
            assert_eq!(cell.header.id, *id);
        }
        assert!(
            held_in_slot(&servers[0], SLOT).is_empty(),
            "the donor should hold nothing in a slot it gave up"
        );
        assert!(handover.cells_transferred >= 20);
    }

    /// A drained member owns nothing and has lost nothing.
    ///
    /// The safety property Phase 3 exists for: a *planned* departure never loses
    /// data. Both halves are asserted, because either alone is worthless -- a
    /// member that owns nothing because its data was dropped would satisfy the
    /// first half and be a catastrophe.
    #[tokio::test(flavor = "multi_thread")]
    async fn draining_a_member_moves_everything_it_owns_and_loses_nothing() {
        let (servers, client) = start_pair("migration_drain_test").await;
        let departing = servers[0].server_id;
        let remaining = servers[1].server_id;

        // Several slots with several cells each, so the drain has to enumerate
        // and move a set rather than a single lucky slot.
        const SLOTS: [u16; 4] = [201, 202, 203, 204];
        let mut expected: HashSet<Id> = HashSet::new();
        for slot in SLOTS {
            for seq in 0..4 {
                let (id, cell) = cell_in_slot(slot, seq, "drain");
                client.write_cell(cell).await.unwrap().unwrap();
                expected.insert(id);
            }
        }
        assert!(!crate::migration::drain::owns_nothing(&client, departing)
            .await
            .unwrap());

        let drain = crate::migration::drain::drain_member(
            &client,
            departing,
            &[remaining],
            &MigrationPlan::default(),
        )
        .await
        .expect("the drain should run");

        assert!(
            drain.is_complete(),
            "drain left slots stranded: {:?}",
            drain.stranded
        );
        // Every slot the member owned moved, which in a two-member cluster is the
        // whole space -- the four holding data through the careful path and the
        // rest in bulk. Asserting the total is what catches a split that silently
        // drops the empty majority.
        assert_eq!(drain.moved.len(), crate::slots::SLOT_COUNT);
        assert_eq!(drain.cells_transferred, expected.len());

        // Owns nothing -- read back from the state machine, not from the report.
        assert!(crate::migration::drain::owns_nothing(&client, departing)
            .await
            .unwrap());

        // And every cell survived, readable through the ordinary hashed path.
        for id in &expected {
            let cell = client.read_cell(*id).await.unwrap().unwrap_or_else(|error| {
                panic!("{id:?} was lost by the drain: {error:?}")
            });
            assert_eq!(cell.header.id, *id);
        }
        // The departing member holds none of it.
        for slot in SLOTS {
            assert!(held_in_slot(&servers[0], slot).is_empty());
        }
        let received: HashSet<Id> = SLOTS
            .iter()
            .flat_map(|slot| held_in_slot(&servers[1], *slot))
            .collect();
        assert_eq!(received, expected);
    }

    /// A drain with nowhere to send data refuses, rather than half-emptying a
    /// member and reporting success.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_drain_with_no_destination_refuses() {
        let (servers, client) = start_pair("migration_drain_refusal_test").await;
        let departing = servers[0].server_id;

        const SLOT: u16 = 211;
        let (id, cell) = cell_in_slot(SLOT, 1, "kept");
        client.write_cell(cell).await.unwrap().unwrap();

        assert!(matches!(
            crate::migration::drain::drain_member(
                &client,
                departing,
                &[],
                &MigrationPlan::default()
            )
            .await,
            Err(MigrationError::Invalid(_))
        ));
        // Draining onto itself is not a drain either.
        assert!(matches!(
            crate::migration::drain::drain_member(
                &client,
                departing,
                &[departing],
                &MigrationPlan::default()
            )
            .await,
            Err(MigrationError::Invalid(_))
        ));

        // Nothing moved and nothing was lost.
        assert!(!crate::migration::drain::owns_nothing(&client, departing)
            .await
            .unwrap());
        assert_eq!(held_in_slot(&servers[0], SLOT), HashSet::from([id]));
        client.read_cell(id).await.unwrap().unwrap();
    }

    /// Phase 5: a drain under sustained writes loses nothing.
    ///
    /// The drain analogue of `writes_racing_a_migration_are_never_lost`, and the
    /// harder case: a drain moves *every* slot the member owns, so a writer aimed
    /// at that member is writing into ground that is being taken away underneath
    /// it for the whole run. Every acknowledged write must still be readable, and
    /// the member must end up owning nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_drain_under_sustained_writes_loses_nothing() {
        let (servers, client) = start_pair("migration_drain_racing_writes_test").await;
        let departing = servers[0].server_id;
        let remaining = servers[1].server_id;

        const SLOTS: [u16; 3] = [221, 222, 223];
        const WRITES: u64 = 45;

        let mut seeded: HashSet<Id> = HashSet::new();
        for slot in SLOTS {
            for seq in 0..4 {
                let (id, cell) = cell_in_slot(slot, seq, "seed");
                client.write_cell(cell).await.unwrap().unwrap();
                seeded.insert(id);
            }
        }

        let writer_client = client.clone();
        let writer = tokio::spawn(async move {
            let mut acknowledged = Vec::new();
            for seq in 200..(200 + WRITES) {
                for slot in SLOTS {
                    let (id, cell) = cell_in_slot(slot, seq, "during-drain");
                    // Acknowledged means the cluster owes us this cell from here
                    // on. A refusal or an RPC error means nothing was written and
                    // nothing is owed -- the honest outcome, not a loss.
                    if let Ok(Ok(_)) = writer_client.upsert_cell(cell).await {
                        acknowledged.push(id);
                    }
                }
                tokio::task::yield_now().await;
            }
            acknowledged
        });

        let drain = crate::migration::drain::drain_member(
            &client,
            departing,
            &[remaining],
            &MigrationPlan::default(),
        )
        .await
        .expect("the drain should run while writes are in flight");

        let acknowledged = writer.await.expect("writer task should not panic");
        assert!(
            !acknowledged.is_empty(),
            "the writer never got a single write through; this proves nothing"
        );

        // Every acknowledged write, and every seeded cell, still readable.
        for id in acknowledged.iter().chain(seeded.iter()) {
            let cell = client.read_cell(*id).await.unwrap().unwrap_or_else(|error| {
                panic!("{id:?} was lost by a drain under load: {error:?}")
            });
            assert_eq!(cell.header.id, *id);
        }

        // A drain racing a writer may legitimately not finish in one call -- the
        // writer keeps handing it new work. What must not happen is data loss,
        // which is asserted above.
        //
        // The one directional claim worth making: if it says complete, the member
        // really owns nothing. Polled, because the check is a query and a query can
        // be served by a member that has not applied the last reassignment yet.
        //
        // Deliberately NOT the converse. `stranded` is computed from a table read
        // at the end of the drain, and a lagging read makes it report slots that
        // have in fact moved -- so an "incomplete" drain can be complete moments
        // later. That is the safe direction (it fails closed, and a caller gated on
        // `is_complete` simply retries), and asserting the converse made this test
        // fail about one run in three on nothing but replica lag.
        if drain.is_complete() {
            let mut empty = false;
            for _ in 0..40 {
                if crate::migration::drain::owns_nothing(&client, departing)
                    .await
                    .unwrap()
                {
                    empty = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert!(
                empty,
                "a drain that reported itself complete must leave the member owning nothing"
            );
        }
    }

    /// Phase 5: an unreachable recipient leaves the slot with its donor.
    ///
    /// The donor stays authoritative for the whole transfer, so losing the
    /// recipient must cost nothing but the attempt. Asserted on placement *and*
    /// on the data, because a slot correctly left with its donor while the cells
    /// were dropped would satisfy the first and be a catastrophe.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unreachable_recipient_leaves_the_slot_with_its_donor() {
        let (servers, client) = start_pair("migration_recipient_death_test").await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        const SLOT: u16 = 231;
        let mut expected: HashSet<Id> = HashSet::new();
        for seq in 0..5 {
            let (id, cell) = cell_in_slot(SLOT, seq, "kept");
            client.write_cell(cell).await.unwrap().unwrap();
            expected.insert(id);
        }

        // The recipient goes away before the transfer can reach it.
        servers[1].shutdown().await;

        let outcome = migrate_slot(
            &client,
            SLOT as u32,
            donor_id,
            recipient_id,
            &MigrationPlan::default(),
        )
        .await;
        assert!(
            outcome.is_err(),
            "a migration to a member that is gone must fail, not report success"
        );

        // Placement is back with the donor -- either never moved, or aborted --
        // and never left mid-flight claiming a recipient that cannot answer.
        let state = placement_client(&client)
            .slot_state(&slot_group_id(client.group_name()), &(SLOT as u32))
            .await
            .unwrap();
        assert_eq!(
            state,
            Some(SlotState::Stable { owner: donor_id }),
            "the slot must be left stable on its donor, not stuck migrating"
        );
        assert_eq!(client.locate_server_id(expected.iter().next().unwrap()).unwrap(), donor_id);

        // And the data is all still there, on the donor.
        assert_eq!(held_in_slot(&servers[0], SLOT), expected);
        for id in &expected {
            client.read_cell(*id).await.unwrap().unwrap();
        }
    }

    /// Phase 5: a drain that cannot finish says so, rather than reporting success.
    ///
    /// The safety gate is only worth having if it fails closed: a drain whose
    /// destination cannot take the data must leave the member owning it, and must
    /// report `is_complete() == false` so a caller gated on that never removes it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_drain_to_a_member_that_cannot_take_it_fails_closed() {
        let (servers, client) = start_pair("migration_drain_aborted_test").await;
        let departing = servers[0].server_id;

        const SLOT: u16 = 241;
        let (id, cell) = cell_in_slot(SLOT, 1, "undrainable");
        client.write_cell(cell).await.unwrap().unwrap();

        // A member id that is not in this cluster at all.
        const GHOST: u64 = 0xDEAD_BEEF_CAFE;
        let drain = crate::migration::drain::drain_member(
            &client,
            departing,
            &[GHOST],
            &MigrationPlan::default(),
        )
        .await
        .expect("the drain should report, not error out");

        assert!(
            !drain.is_complete(),
            "a drain that moved nothing must not report itself complete"
        );
        assert!(
            !drain.stranded.is_empty(),
            "it must name what it could not move"
        );
        assert!(!crate::migration::drain::owns_nothing(&client, departing)
            .await
            .unwrap());

        // Nothing lost.
        assert_eq!(held_in_slot(&servers[0], SLOT), HashSet::from([id]));
        client.read_cell(id).await.unwrap().unwrap();
    }

    /// A migrated cell keeps its version.
    ///
    /// Not a cosmetic property. Callers derive *cell ids* from a container's
    /// version -- Morpheus's id lists compute their segment ids from
    /// `(container, field, schema, root, root_version)` -- so a migration that
    /// bumps the version silently repoints every derived id at a cell that does
    /// not exist. The symptom is far from the cause: an edge append fails with
    /// "root segment cell does not exist" on a vertex that migrated perfectly.
    #[tokio::test(flavor = "multi_thread")]
    async fn migration_preserves_cell_versions() {
        let (servers, client) = start_pair("migration_version_preservation_test").await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        const SLOT: u16 = 281;
        let (id, cell) = cell_in_slot(SLOT, 1, "versioned");
        client.write_cell(cell).await.unwrap().unwrap();
        // Update a few times so the version is something specific rather than the
        // value a fresh insert happens to produce.
        for seq in 0..3 {
            let (_, mut updated) = cell_in_slot(SLOT, 1, &format!("update-{seq}"));
            updated.header.id = id;
            client.upsert_cell(updated).await.unwrap().unwrap();
        }
        let before = client.read_cell(id).await.unwrap().unwrap().header.version;

        migrate_slot(
            &client,
            SLOT as u32,
            donor_id,
            recipient_id,
            &MigrationPlan::default(),
        )
        .await
        .expect("slot should migrate");

        let after = client.read_cell(id).await.unwrap().unwrap().header.version;
        assert_eq!(
            after, before,
            "migration changed the cell version from {before} to {after}; \
             any id derived from a container's version now points at a cell that \
             does not exist"
        );
    }

    /// Reproduction target: a cell read THROUGH A TRANSACTION right after its slot
    /// migrated.
    ///
    /// Every other test here reads with `AsyncClient::read_cell`, and those pass.
    /// The Morpheus failure reads through a distributed transaction instead, which
    /// is a different path with its own placement lookup, its own per-transaction
    /// cache, and a participant RPC to the owning site. This narrows the search to
    /// that difference: same migration, same reclaim, same refresh, but the read
    /// and the write go through `txn`.
    ///
    /// Repeated because the symptom is intermittent under load; a single pass
    /// proving nothing is exactly how this class of bug survives.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_transaction_reads_a_migrated_cell() {
        let (servers, client) = start_cluster_with(
            "migration_txn_read_test",
            2,
            vec![Service::Cell, Service::Transaction],
        )
        .await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        const SLOTS: [u16; 6] = [301, 302, 303, 304, 305, 306];
        let mut ids = Vec::new();
        for slot in SLOTS {
            for seq in 0..4 {
                let (id, cell) = cell_in_slot(slot, seq, "before");
                client.write_cell(cell).await.unwrap().unwrap();
                ids.push(id);
            }
        }

        let plan = MigrationPlan::default();
        for slot in SLOTS {
            migrate_slot(&client, slot as u32, donor_id, recipient_id, &plan)
                .await
                .expect("slot should migrate");
            if std::env::var("NEB_DIAG_NO_RECLAIM").is_err() {
                reclaim_donor_copy(&client, slot as u32, donor_id, recipient_id, &plan)
                    .await
                    .expect("donor copy should be reclaimable");
            }
        }
        for server in &servers {
            server.refresh_slot_placement().await;
        }

        // Read every migrated cell inside a transaction, then update it, which is
        // the shape of the Morpheus link that fails: read a container, decide,
        // write back.
        for id in &ids {
            let id = *id;
            let outcome = client
                .transaction(|txn| {
                    Box::pin(async move {
                        let mut cell = txn
                            .read(id)
                            .await?
                            .expect("the migrated cell must still exist after placement refresh");
                        if let OwnedValue::Map(ref mut map) = cell.data {
                            map.insert(
                                &String::from("name"),
                                OwnedValue::String("after".to_string()),
                            );
                        }
                        txn.update(cell).await?;
                        Ok(())
                    })
                })
                .await;
            outcome.unwrap_or_else(|error| {
                panic!("a transaction could not read+update migrated {id:?}: {error:?}")
            });
        }

        // And the updates are visible afterwards, on the new owner.
        for id in &ids {
            let cell = client.read_cell(*id).await.unwrap().unwrap();
            assert_eq!(
                cell.data["name"].string().map(|s| s.as_str()),
                Some("after"),
                "{id:?} did not keep the transactional update"
            );
        }
    }

    /// CONTROL for `a_transaction_reads_a_migrated_cell`: the same cluster and the
    /// same transactions, with no migration at all.
    ///
    /// Without this the other test cannot attribute anything. If both hang under
    /// load then the hang belongs to two-member transactions, not to migration,
    /// and chasing it inside the migration code would be chasing the wrong thing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_transaction_reads_an_unmigrated_cell() {
        let (_servers, client) = start_cluster_with(
            "migration_txn_control_test",
            2,
            vec![Service::Cell, Service::Transaction],
        )
        .await;

        const SLOTS: [u16; 6] = [311, 312, 313, 314, 315, 316];
        let mut ids = Vec::new();
        for slot in SLOTS {
            for seq in 0..4 {
                let (id, cell) = cell_in_slot(slot, seq, "before");
                client.write_cell(cell).await.unwrap().unwrap();
                ids.push(id);
            }
        }

        for id in &ids {
            let id = *id;
            client
                .transaction(|txn| {
                    Box::pin(async move {
                        let cell = txn.read(id).await?;
                        let mut cell = cell.ok_or(
                            crate::client::transaction::TxnError::NotRealizable(
                                crate::client::transaction::NotRealizableReason::ReadTooLate(id),
                            ),
                        )?;
                        if let OwnedValue::Map(ref mut map) = cell.data {
                            map.insert(
                                &String::from("name"),
                                OwnedValue::String("after".to_string()),
                            );
                        }
                        txn.update(cell).await?;
                        Ok(())
                    })
                })
                .await
                .unwrap_or_else(|error| {
                    panic!("a transaction could not read+update {id:?}: {error:?}")
                });
        }
        for id in &ids {
            let cell = client.read_cell(*id).await.unwrap().unwrap();
            assert_eq!(cell.data["name"].string().map(|s| s.as_str()), Some("after"));
        }
    }

    /// Does a migrated cell stay findable through the RANGED INDEX?
    ///
    /// Every other migration test here runs with `index_enabled: false`, which is
    /// why none of them ever exercised this — and why the deterministic Morpheus
    /// failure ("enumerated no vertices ... the index could not be enumerated"
    /// after any migration) has no Neb-level counterpart yet.
    ///
    /// Migration writes each cell on the recipient through the ordinary upsert
    /// path, so `ensure_indices` should add its entries there, and the reclaim
    /// removes the donor's through `remove_indices`. If that holds, a scan sees
    /// the same set before and after. If it does not, this is the Neb-level
    /// reproduction of the last open blocker.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_migrated_cell_is_still_found_by_a_ranged_scan() {
        let _ = env_logger::try_init();
        let group = "migration_ranged_scan_test";
        let addresses = vec![
            crate::utils::test_port::unique_localhost_addr(),
            crate::utils::test_port::unique_localhost_addr(),
        ];
        let opts = ServerOptions {
            chunk_size: 16 * 1024 * 1024,
            db_size: 16 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            // The whole point: every other migration test leaves this off.
            index_enabled: true,
            services: vec![Service::Cell, Service::RangedIndexer],
            enable_recovery: false,
            disable_storage_locks: true,
        };
        let mut servers = Vec::new();
        for address in &addresses {
            servers.push(
                NebServer::new_cluster_from_opts(&opts, address, &addresses, group, async |_| {})
                    .await
                    .unwrap(),
            );
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let client = Arc::new(
            client::AsyncClient::new(&servers[0].rpc, &servers[0].membership, &addresses, group)
                .await
                .unwrap(),
        );
        client.reload_slot_owners().await;
        const SCHEMA: u32 = 1600;
        client
            .new_schema_with_id(Schema::new_with_id(
                SCHEMA,
                &String::from("ranged_scan_schema"),
                None,
                default_fields(),
                false,
                true,
            ))
            .await
            .unwrap()
            .unwrap();

        const SLOT: u16 = 321;
        let mut written: HashSet<Id> = HashSet::new();
        for seq in 0..6 {
            let (id, mut cell) = cell_in_slot(SLOT, seq, "indexed");
            cell.header.schema = SCHEMA;
            client.write_cell(cell).await.unwrap().unwrap();
            written.insert(id);
        }
        let _ = crate::index::builder::IndexBuilder::await_all_indices().await;

        async fn scan_ids(client: &Arc<AsyncClient>, schema: u32) -> HashSet<Id> {
            let mut found = HashSet::new();
            if let Ok(Some(mut cursor)) = client.ranged().scan_schema(schema, 64).await {
                loop {
                    match cursor.next().await {
                        Ok(Some(id)) => {
                            found.insert(id);
                        }
                        _ => break,
                    }
                }
            }
            found
        }

        let before = scan_ids(&client, SCHEMA).await;
        assert!(
            written.is_subset(&before),
            "the scan must find the cells before any migration, or this proves nothing: \
             wrote {}, scan found {}",
            written.len(),
            before.len()
        );

        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;
        let plan = MigrationPlan::default();
        migrate_slot(&client, SLOT as u32, donor_id, recipient_id, &plan)
            .await
            .expect("slot should migrate");
        reclaim_donor_copy(&client, SLOT as u32, donor_id, recipient_id, &plan)
            .await
            .expect("donor copy should be reclaimable");
        for server in &servers {
            server.refresh_slot_placement().await;
        }
        let _ = crate::index::builder::IndexBuilder::await_all_indices().await;

        let after = scan_ids(&client, SCHEMA).await;
        let lost: Vec<&Id> = written.iter().filter(|id| !after.contains(id)).collect();
        // Is this the residual stale pointer? Ranged index pages ARE cells, so a
        // single zeroed page makes a scan return nothing -- which is exactly what
        // this test reports when it fails. If a stale pointer was recorded in
        // this process, that is the first thing to look at; if none was, this
        // failure is something else and the tier is not the place to look.
        let (stale_seen, verdict) = crate::ram::cell::stale_pointer_record::snapshot();
        if !lost.is_empty() {
            println!(
                "RANGED SCAN FAILURE: {} stale pointers recorded process-wide; most recent: {}",
                stale_seen,
                verdict
                    .as_deref()
                    .unwrap_or("none -- so this is NOT the tier stale-pointer bug")
            );
        }
        assert!(
            lost.is_empty(),
            "{} of {} migrated cells vanished from the ranged index \
             (scan found {} before, {} after): {:?}",
            lost.len(),
            written.len(),
            before.len(),
            after.len(),
            lost.iter().take(3).collect::<Vec<_>>()
        );
    }

    /// THE BUG, in isolation: an update to an already-transferred cell is lost.
    ///
    /// The migration doc has always said delta rounds catch *new* cells and not
    /// updates to ones already copied. This shows what that costs, without a graph
    /// or a transaction anywhere near it: transfer a cell, update it on the donor
    /// while the slot is still `Migrating` (so the donor is the serving owner and
    /// the write is legitimately accepted), then commit.
    ///
    /// The recipient ends up with the pre-update value, and because the reclaim
    /// only carries over cells the recipient is *missing* — never ones whose donor
    /// copy is newer — dropping the donor's copy destroys the newer version.
    ///
    /// This is the shape behind the Morpheus id-list failure: the cell that gets
    /// updated there is a type list, and the pointer it gains names a segment that
    /// the recipient never receives, so a read that resolves the pointer reports
    /// "root segment cell does not exist".
    #[tokio::test(flavor = "multi_thread")]
    async fn an_update_during_transfer_is_lost() {
        let (servers, client) = start_pair("migration_lost_update_test").await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        const SLOT: u16 = 291;
        let (id, cell) = cell_in_slot(SLOT, 1, "original");
        client.write_cell(cell).await.unwrap().unwrap();

        let placement = placement_client(&client);
        let group = slot_group_id(client.group_name());
        let donor = client.client_by_server_id(donor_id).await.unwrap();
        let recipient = client.client_by_server_id(recipient_id).await.unwrap();

        // Drive the sequence by hand so the update lands in the window the driver
        // cannot see: after the transfer has read the cell, before the flip.
        placement
            .begin_slot_migration(&group, &(SLOT as u32), &donor_id, &recipient_id)
            .await
            .unwrap()
            .unwrap();
        let ids = donor.cell_ids_in_slots(&vec![SLOT as u32]).await.unwrap();
        donor.push_cells_to(&ids, recipient_id).await.unwrap().unwrap();

        // The donor is still the serving owner, so this write is correct to accept.
        let (_, updated) = cell_in_slot(SLOT, 1, "UPDATED-mid-transfer");
        donor.upsert_cell(updated).await.unwrap().unwrap();

        let (owner, applied_index) = placement
            .complete_slot_migration_with_index(&group, &(SLOT as u32))
            .await
            .unwrap();
        assert_eq!(owner.unwrap(), recipient_id);
        client.note_slot_owner(SLOT as u32, recipient_id, applied_index);

        // What the new owner holds is the value from before the update.
        let landed = recipient.read_cell(id).await.unwrap().unwrap();
        let landed_name = landed.data["name"].string().cloned().unwrap_or_default();

        // And the reclaim will not rescue it: the recipient HAS the cell, so the
        // carry-over sees nothing missing and the donor's newer copy is dropped.
        let reclaim = reclaim_donor_copy(
            &client,
            SLOT as u32,
            donor_id,
            recipient_id,
            &MigrationPlan::default(),
        )
        .await
        .expect("reclaim should run");
        assert_eq!(
            reclaim.carried_over, 1,
            "the carry-over must notice the donor holds a newer version and send it"
        );

        let after = client.read_cell(id).await.unwrap().unwrap();
        let after_name = after.data["name"].string().cloned().unwrap_or_default();
        assert_eq!(
            after_name, "UPDATED-mid-transfer",
            "an update accepted by the donor mid-transfer was lost: the cluster now \
             serves {after_name:?} (transferred value was {landed_name:?})"
        );
    }

    /// The whole distributed lifecycle at scale, in one run: a cluster under
    /// tier pressure, a member joining and being filled AUTOMATICALLY, and a
    /// member drained away again -- with every cell accounted for at each step.
    ///
    /// The unit tests prove each mechanism; this proves they compose at a size
    /// where the tier actually spills, the reshard is disk-bound, and the
    /// automatic fill is racing real eviction. Three members rather than two,
    /// because the plane-join fix and the "largest current holder" donor choice
    /// are both trivially satisfied by a pair.
    ///
    ///   NEB_LIFECYCLE_TIER_MB, NEB_LIFECYCLE_SLOTS, NEB_LIFECYCLE_CELLS_PER_SLOT,
    ///   NEB_LIFECYCLE_PAYLOAD_BYTES, NEB_LIFECYCLE_DB_GB, NEB_LIFECYCLE_SAMPLE
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn the_distributed_lifecycle_holds_at_scale() {
        fn env_usize(name: &str, default: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        let _ = env_logger::try_init();
        let tier_limit = env_usize("NEB_LIFECYCLE_TIER_MB", 1024) * 1024 * 1024;
        let slots = env_usize("NEB_LIFECYCLE_SLOTS", 512).min(crate::slots::SLOT_COUNT) as u16;
        let cells_per_slot = env_usize("NEB_LIFECYCLE_CELLS_PER_SLOT", 256) as u64;
        let payload_bytes = env_usize("NEB_LIFECYCLE_PAYLOAD_BYTES", 4096);
        let db_size = env_usize("NEB_LIFECYCLE_DB_GB", 16) * 1024 * 1024 * 1024;
        // Reading back every cell over RPC dominates the run at scale, so the
        // full sweep is sampled. Set to 0 to read them all.
        let sample_every = env_usize("NEB_LIFECYCLE_SAMPLE", 16).max(1);
        let total_cells = slots as u64 * cells_per_slot;
        const CHUNK_SIZE: usize = 64 * 1024 * 1024;

        let group = "distributed_lifecycle";
        let addresses: Vec<String> = (0..3)
            .map(|_| crate::utils::test_port::unique_localhost_addr())
            .collect();
        // Where the tier spills to. Driven by TMPDIR, which on a developer box
        // is very often a tmpfs -- i.e. RAM. A tier test whose "disk" is memory
        // measures nothing and dies by OOM partway through, so say where the
        // data is going and refuse a root that cannot hold it.
        let storage_root = std::env::temp_dir()
            .join(format!("neb-lifecycle-{}", std::process::id()));
        std::fs::create_dir_all(&storage_root).expect("storage root should be creatable");
        {
            let path = std::ffi::CString::new(storage_root.to_string_lossy().as_bytes()).unwrap();
            let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
            assert_eq!(
                unsafe { libc::statvfs(path.as_ptr(), &mut stat) },
                0,
                "cannot stat the storage root {}",
                storage_root.display()
            );
            let available = (stat.f_bavail as u64) * (stat.f_frsize as u64);
            let payload_total = total_cells * payload_bytes as u64;
            println!(
                "LIFECYCLE: storage root {} has {} MB available for a {} MB payload",
                storage_root.display(),
                available / (1024 * 1024),
                payload_total / (1024 * 1024)
            );
            assert!(
                available > payload_total * 3,
                "storage root {} has only {} MB free for a {} MB payload; set TMPDIR to a \
                 real disk with room (a tmpfs here means the tier spills into RAM and the \
                 run dies by OOM rather than measuring anything)",
                storage_root.display(),
                available / (1024 * 1024),
                payload_total / (1024 * 1024)
            );
        }
        let opts_for = |index: usize| ServerOptions {
            chunk_size: CHUNK_SIZE,
            db_size,
            tiered_config: Some(crate::ram::tiered::TieredConfig {
                threshold: 0.8,
                lower_watermark: 0.72,
                physical_memory_limit: tier_limit,
                promotion_cooldown_ms: 2000,
            }),
            backup_storage: Some(
                storage_root
                    .join(format!("member-{index}/backup"))
                    .to_string_lossy()
                    .to_string(),
            ),
            wal_storage: Some(
                storage_root
                    .join(format!("member-{index}/wal"))
                    .to_string_lossy()
                    .to_string(),
            ),
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        };

        // Two members to start. The third joins later, which is the point.
        let mut servers = Vec::new();
        for index in 0..2 {
            servers.push(
                NebServer::new_cluster_from_opts(
                    &opts_for(index),
                    &addresses[index],
                    &addresses,
                    group,
                    async |_| {},
                )
                .await
                .unwrap(),
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = Arc::new(
            client::AsyncClient::new(&servers[0].rpc, &servers[0].membership, &addresses, group)
                .await
                .unwrap(),
        );
        client.reload_slot_owners().await;
        client
            .new_schema_with_id(Schema::new_with_id(
                SCHEMA_ID,
                &String::from("migration_schema"),
                None,
                default_fields(),
                false,
                false,
            ))
            .await
            .unwrap()
            .unwrap();

        // The payload rides in `name`: `default_fields` is id/name/score, so a
        // separate data field would simply not be in the schema.
        let payload = "x".repeat(payload_bytes);
        let started = std::time::Instant::now();
        let mut ids = Vec::with_capacity(total_cells as usize);
        // Slot and sequence both start at 1: `Id::from_parts(0, 0)` is the unit
        // id, which the router rejects outright.
        for slot in 1..=slots {
            for seq in 1..=cells_per_slot {
                let id = Id::from_parts(slot as u64, seq);
                let mut value = OwnedMap::new();
                value.insert(&String::from("id"), OwnedValue::I64(seq as i64));
                value.insert(&String::from("score"), OwnedValue::U64(seq));
                value.insert(&String::from("name"), OwnedValue::String(payload.clone()));
                client
                    .write_cell(crate::ram::cell::OwnedCell::new_with_id(
                        SCHEMA_ID,
                        &id,
                        OwnedValue::Map(value),
                    ))
                    .await
                    .unwrap_or_else(|e| panic!("rpc writing {id:?}: {e:?}"))
                    .unwrap_or_else(|e| panic!("writing {id:?}: {e:?}"));
                ids.push(id);
            }
        }
        let written_bytes: u64 = servers.iter().map(|s| s.chunks().total_live_bytes()).sum();
        println!(
            "LIFECYCLE: wrote {} cells ({} MB live) across {} slots in {:.1}s",
            total_cells,
            written_bytes / (1024 * 1024),
            slots,
            started.elapsed().as_secs_f64()
        );
        assert!(written_bytes > 0, "the counters saw none of the write load");

        let verify = |client: Arc<AsyncClient>, ids: Vec<Id>, phase: &'static str| async move {
            let mut unreadable = Vec::new();
            for id in ids.iter().step_by(sample_every) {
                match client.read_cell(*id).await {
                    Ok(Ok(cell)) => assert_eq!(cell.header.id, *id),
                    Ok(Err(e)) => unreadable.push((*id, format!("{e:?}"))),
                    Err(e) => unreadable.push((*id, format!("rpc {e:?}"))),
                }
            }
            assert!(
                unreadable.is_empty(),
                "{}: {} of {} sampled cells unreadable; first few: {:?}",
                phase,
                unreadable.len(),
                ids.len() / sample_every,
                &unreadable[..unreadable.len().min(5)]
            );
            println!("LIFECYCLE: {} -- all sampled cells readable", phase);
        };
        verify(client.clone(), ids.clone(), "after the initial load").await;

        // ---- the join, and the fill nobody asks for ----
        let joiner = NebServer::new_cluster_from_opts(
            &opts_for(2),
            &addresses[2],
            &addresses,
            group,
            async |_| {},
        )
        .await
        .unwrap();
        let join_at = std::time::Instant::now();

        // Converge: wait for the joiner to stop gaining bytes, which is the
        // fill finishing rather than a fixed sleep guessing at it.
        let mut last = 0u64;
        let mut stable_rounds = 0;
        let mut joiner_bytes = 0u64;
        let settle_secs = env_usize("NEB_LIFECYCLE_SETTLE_S", 600);
        for _ in 0..settle_secs {
            tokio::time::sleep(Duration::from_secs(1)).await;
            joiner_bytes = joiner.chunks().total_live_bytes();
            if joiner_bytes > 0 && joiner_bytes == last {
                stable_rounds += 1;
                if stable_rounds >= 10 {
                    break;
                }
            } else {
                stable_rounds = 0;
            }
            last = joiner_bytes;
        }
        let held: Vec<u64> = servers
            .iter()
            .map(|s| s.chunks().total_live_bytes())
            .chain(std::iter::once(joiner_bytes))
            .collect();
        let cluster_bytes: u64 = held.iter().sum();
        let mean = cluster_bytes / held.len() as u64;
        println!(
            "LIFECYCLE: auto-fill settled after {:.1}s -- holdings {:?} MB, mean {} MB",
            join_at.elapsed().as_secs_f64(),
            held.iter().map(|b| b / (1024 * 1024)).collect::<Vec<_>>(),
            mean / (1024 * 1024)
        );
        assert!(
            joiner_bytes > 0,
            "nobody filled the joining member: it holds nothing after the fill window"
        );
        assert!(
            joiner_bytes <= mean,
            "the automatic fill overshot the mean: joiner {joiner_bytes} vs mean {mean}"
        );
        client.reload_slot_owners().await;
        verify(client.clone(), ids.clone(), "after the automatic fill").await;

        // Placement and the counters must tell the same story.
        let placement = placement_client(&client);
        let group_id = slot_group_id(client.group_name());
        let joiner_slots = placement
            .slots_owned_by(&group_id, &joiner.server_id)
            .await
            .unwrap();
        assert!(
            !joiner_slots.is_empty(),
            "the joiner holds bytes but the table says it owns no slots"
        );
        let joiner_slot_bytes: u64 = joiner.chunks().slot_live_bytes(&joiner_slots).iter().sum();
        assert_eq!(
            joiner_slot_bytes, joiner_bytes,
            "the joiner's per-slot counters disagree with its total"
        );
        println!(
            "LIFECYCLE: joiner owns {} slots holding {} MB",
            joiner_slots.len(),
            joiner_bytes / (1024 * 1024)
        );

        // ---- and a member leaving again ----
        let drain_at = std::time::Instant::now();
        let leaving = servers[1].server_id;
        let destinations: Vec<u64> = vec![servers[0].server_id, joiner.server_id];
        let outcome = crate::migration::drain::drain_member(
            &client,
            leaving,
            &destinations,
            &MigrationPlan::default(),
        )
        .await;
        println!(
            "LIFECYCLE: drain of {} finished in {:.1}s: {:?}",
            leaving,
            drain_at.elapsed().as_secs_f64(),
            outcome.as_ref().map(|o| (o.moved.len(), o.cells_transferred, o.stranded.len()))
        );
        let outcome = outcome.expect("draining a live member should succeed");
        assert!(
            outcome.stranded.is_empty(),
            "the drain stranded {} slots: {:?}",
            outcome.stranded.len(),
            &outcome.stranded[..outcome.stranded.len().min(8)]
        );
        assert!(
            placement
                .slots_owned_by(&group_id, &leaving)
                .await
                .unwrap()
                .is_empty(),
            "a drained member still owns slots"
        );
        client.reload_slot_owners().await;
        verify(client.clone(), ids.clone(), "after the drain").await;

        println!(
            "LIFECYCLE: PASSED -- {} cells survived load, an automatic fill and a drain",
            total_cells
        );
    }

    /// Phase 5: a donor that dies mid-migration is DETECTED, not survived.
    ///
    /// This one is data loss and the plan says so: there is no replication, so a
    /// member that vanishes takes its slots' contents with it. The property worth
    /// having is therefore not recovery but honesty — the migration must fail
    /// loudly, and placement must never end up claiming the recipient holds data
    /// that never arrived. A silent success here would be the worst outcome in the
    /// whole design: the cluster would believe the cells are somewhere they are
    /// not, and the reclaim would happily delete a donor copy that no longer
    /// exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_donor_that_dies_mid_migration_is_detected() {
        let (servers, client) = start_pair("migration_donor_death_test").await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        const SLOT: u16 = 251;
        for seq in 0..5 {
            let (_, cell) = cell_in_slot(SLOT, seq, "doomed");
            client.write_cell(cell).await.unwrap().unwrap();
        }

        // The donor goes away with the data still on it.
        servers[0].shutdown().await;

        let outcome = migrate_slot(
            &client,
            SLOT as u32,
            donor_id,
            recipient_id,
            &MigrationPlan::default(),
        )
        .await;
        assert!(
            outcome.is_err(),
            "a migration from a member that is gone must fail loudly; \
             reporting success would tell the cluster the cells are somewhere they are not"
        );

        // Placement must not claim the recipient owns this slot: it holds nothing.
        //
        // Read through the SURVIVOR's own raft client rather than the test
        // client. The test client's freshness cursor sits at the dead leader's
        // last acknowledgement, and in a two-member cluster the survivor can
        // never learn that final commit -- so an honest freshness gate refuses
        // it forever, correctly (the old gate served it by comparing log
        // receipt instead of application, which is the stale-read bug). This
        // assertion is a post-mortem: "what does the cluster still know" is
        // the question, and a fresh cursor is the honest way to ask it.
        let state = SlotsSMClient::new(crate::server::SLOTS_SM_ID, &servers[1].raft_client)
            .slot_state(&slot_group_id(client.group_name()), &(SLOT as u32))
            .await
            .unwrap();
        assert_ne!(
            state,
            Some(SlotState::Stable {
                owner: recipient_id
            }),
            "placement must never commit a handover whose data never arrived"
        );
        assert!(held_in_slot(&servers[1], SLOT).is_empty());

        // And the reclaim must refuse, because refusing is what stops it deleting
        // a donor copy on the strength of a handover that did not happen.
        assert!(reclaim_donor_copy(
            &client,
            SLOT as u32,
            donor_id,
            recipient_id,
            &MigrationPlan::default()
        )
        .await
        .is_err());
    }

    /// Phase 5: a second member joining during a join moves nothing.
    ///
    /// The plan lists this as a failure model because under computed placement it
    /// was one: each join reshuffled the space, so joins racing each other
    /// reshuffled it twice and orphaned whatever was written in between. With
    /// placement stored, a joining member owns nothing and the question becomes
    /// trivial — which is the point, and worth pinning so it stays trivial.
    #[tokio::test(flavor = "multi_thread")]
    async fn members_joining_a_running_cluster_own_nothing_and_move_nothing() {
        let (servers, client) = start_cluster("migration_double_join_test", 3).await;
        let first = servers[0].server_id;

        // Written while all three are up, so this is about placement rather than
        // about a write that happened to precede a join.
        let mut expected: HashSet<Id> = HashSet::new();
        for slot in [261u16, 262, 263, 264] {
            for seq in 0..3 {
                let (id, cell) = cell_in_slot(slot, seq, "joined");
                client.write_cell(cell).await.unwrap().unwrap();
                expected.insert(id);
            }
        }

        let placement = placement_client(&client);
        let group = slot_group_id(client.group_name());
        assert_eq!(
            placement.slots_owned_by(&group, &first).await.unwrap().len(),
            crate::slots::SLOT_COUNT,
            "the first member up should still own the whole space after two joins"
        );
        for later in &servers[1..] {
            assert!(
                placement
                    .slots_owned_by(&group, &later.server_id)
                    .await
                    .unwrap()
                    .is_empty(),
                "a member that joined a running cluster must own nothing"
            );
        }

        // Everything readable, and all of it on the first member.
        for id in &expected {
            let cell = client.read_cell(*id).await.unwrap().unwrap_or_else(|error| {
                panic!("{id:?} became unreachable across two joins: {error:?}")
            });
            assert_eq!(cell.header.id, *id);
            assert_eq!(client.locate_server_id(id).unwrap(), first);
        }
    }

    /// Phase 5: losing a member does not silently re-map its slots.
    ///
    /// The ring-version-regression model, restated for stored placement. Under a
    /// computed ring this was the whole disease: membership changes, `jump_hash`
    /// answers differently, and cells are looked up at addresses they were never
    /// written to. Now the table is what answers, so a member leaving must change
    /// **nothing** about placement — its slots keep naming it.
    ///
    /// That looks unhelpful (those cells are unreachable, and without replication
    /// they are gone) and it is exactly right: placement that quietly re-pointed
    /// the slots at a survivor would claim data had moved when nothing had. An
    /// operator gets a slot that names a dead member, which is a fact they can act
    /// on, instead of a lie they cannot.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_member_leaving_does_not_re_map_its_slots() {
        let (servers, client) = start_pair("migration_ring_regression_test").await;
        let first = servers[0].server_id;
        let second = servers[1].server_id;

        const MOVED: u16 = 271;
        let (moved_id, cell) = cell_in_slot(MOVED, 1, "on-the-second-member");
        client.write_cell(cell).await.unwrap().unwrap();
        migrate_slot(
            &client,
            MOVED as u32,
            first,
            second,
            &MigrationPlan::default(),
        )
        .await
        .expect("slot should migrate so the second member owns something to lose");
        assert_eq!(client.locate_server_id(&moved_id).unwrap(), second);

        let placement = placement_client(&client);
        let group = slot_group_id(client.group_name());

        // The member holding that slot goes away ungracefully.
        servers[1].shutdown().await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Asserted as a PROPERTY of the final table rather than by diffing two
        // snapshots. Two `all_slots` queries round-robin over members and can be
        // served by replicas one apply apart, so a snapshot diff reports lag as
        // change -- it did, showing slot 271 as `Migrating` in the "before" read
        // and `Stable` in the "after" one, for a commit that had already happened
        // before either. Polling for a settled table and then checking what it
        // says is immune to that, and is the claim worth making anyway.
        let mut owned_by_departed = Vec::new();
        for _ in 0..40 {
            owned_by_departed = placement.slots_owned_by(&group, &second).await.unwrap();
            if owned_by_departed == vec![MOVED as u32] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            owned_by_departed,
            vec![MOVED as u32],
            "the departed member must still own exactly the slot it was given; \
             re-pointing it at a survivor would claim data moved when nothing did"
        );
        assert_eq!(
            placement.slots_owned_by(&group, &first).await.unwrap().len(),
            crate::slots::SLOT_COUNT - 1,
            "and the survivor must own exactly what it owned before, no more"
        );
        assert_eq!(
            placement.slot_state(&group, &(MOVED as u32)).await.unwrap(),
            Some(SlotState::Stable { owner: second }),
            "the slot must name the departed member, so the loss is visible \
             rather than silently papered over"
        );

        // Everything the surviving member owns is still readable -- the loss is
        // confined to the departed member's slots rather than spread by a reshuffle.
        let (survivor_id, cell) = cell_in_slot(272, 1, "still-here");
        client.write_cell(cell).await.unwrap().unwrap();
        client.read_cell(survivor_id).await.unwrap().unwrap();
    }

    /// MEASUREMENT: how fast does a drain actually move data?
    ///
    /// ```text
    /// cargo test --release --lib drain_throughput -- --ignored --nocapture
    /// ```
    ///
    /// **Release only.** bifrost picks its codec by build profile -- `serde_cbor`
    /// in release, `serde_json` under `debug_assertions` -- so a debug number is a
    /// statement about JSON and nothing else.
    ///
    /// A drain is the operator-facing operation of this whole campaign: it is what
    /// runs when a machine has to leave. `reshard_slots` has a measured number and
    /// the drain did not, and the two paths do NOT share their fan-out, so the
    /// reshard's figure says nothing about this one.
    ///
    ///   NEB_DRAIN_SLOTS, NEB_DRAIN_CELLS_PER_SLOT, NEB_DRAIN_PAYLOAD_BYTES,
    ///   NEB_DRAIN_CONCURRENCY, NEB_DRAIN_DB_GB
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn drain_throughput_measurement() {
        let _ = env_logger::try_init();
        fn env_usize(name: &str, default: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        let slots = env_usize("NEB_DRAIN_SLOTS", 256).min(crate::slots::SLOT_COUNT) as u16;
        let cells_per_slot = env_usize("NEB_DRAIN_CELLS_PER_SLOT", 64) as u64;
        let payload_bytes = env_usize("NEB_DRAIN_PAYLOAD_BYTES", 4096);
        let db_size = env_usize("NEB_DRAIN_DB_GB", 4) * 1024 * 1024 * 1024;
        let concurrency = env_usize("NEB_DRAIN_CONCURRENCY", default_concurrent_slots());

        let group = "migration_drain_throughput";
        let addresses = vec![
            crate::utils::test_port::unique_localhost_addr(),
            crate::utils::test_port::unique_localhost_addr(),
        ];
        let opts = ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell],
            enable_recovery: false,
            disable_storage_locks: true,
        };
        let mut servers = Vec::new();
        for address in &addresses {
            servers.push(
                NebServer::new_cluster_from_opts(&opts, address, &addresses, group, async |_| {})
                    .await
                    .unwrap(),
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        let client = Arc::new(
            client::AsyncClient::new(&servers[0].rpc, &servers[0].membership, &addresses, group)
                .await
                .unwrap(),
        );
        client.reload_slot_owners().await;
        client
            .new_schema_with_id(Schema::new_with_id(
                SCHEMA_ID,
                &String::from("migration_schema"),
                None,
                default_fields(),
                false,
                false,
            ))
            .await
            .unwrap()
            .unwrap();

        let payload = "x".repeat(payload_bytes);
        let mut written = 0usize;
        for slot in 0..slots {
            for seq in 0..cells_per_slot {
                let id = Id::from_parts(slot as u64, 2_000_000 + seq);
                let mut value = OwnedMap::new();
                value.insert(&String::from("id"), OwnedValue::I64(seq as i64));
                value.insert(&String::from("score"), OwnedValue::U64(seq));
                value.insert(&String::from("name"), OwnedValue::String(payload.clone()));
                client
                    .write_cell(crate::ram::cell::OwnedCell::new_with_id(
                        SCHEMA_ID,
                        &id,
                        OwnedValue::Map(value),
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                written += 1;
            }
        }
        let moved_bytes = written * payload_bytes;
        println!(
            "DRAIN MEASUREMENT: {} cells (~{} MB) across {} slots, concurrency {}",
            written,
            moved_bytes / (1024 * 1024),
            slots,
            concurrency
        );

        let departing = servers[0].server_id;
        let remaining = servers[1].server_id;
        let started = std::time::Instant::now();
        let drain = crate::migration::drain::drain_member(
            &client,
            departing,
            &[remaining],
            &MigrationPlan {
                concurrent_slots: concurrency,
                ..Default::default()
            },
        )
        .await
        .expect("the drain should run");
        let elapsed = started.elapsed();

        println!(
            "DRAIN MEASUREMENT: {:.1}s ({:.1} MB/s, {:.0} cells/s); {} slots moved, {} cells, {} stranded",
            elapsed.as_secs_f64(),
            moved_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
            written as f64 / elapsed.as_secs_f64(),
            drain.moved.len(),
            drain.cells_transferred,
            drain.stranded.len()
        );
        // A measurement of a drain that lost data is not a measurement of a drain.
        assert!(drain.is_complete(), "drain stranded {:?}", drain.stranded);
        assert_eq!(drain.cells_transferred, written);
    }

    /// MEASUREMENT, not a unit test. Run explicitly, on a machine with room:
    ///
    /// ```text
    /// cargo test --lib recipient_memory_stays_bounded -- --ignored --nocapture
    /// ```
    ///
    /// Answers the one question the migration design left open: is the
    /// recipient's resident memory bounded by its own tier while it receives a
    /// large transfer, or does bulk ingest blow it up? Every other test here runs
    /// with `tiered_config: None`, so they report zeroes and cannot answer it.
    ///
    /// Ignored rather than deleted because the answer is a property of the
    /// *configuration*, not of the code: it has to be re-measured whenever the
    /// tier's pacing or the migration's batching changes. See
    /// [[tiered-eviction-collapse]] and [[per-segment-resources-dont-scale]] for
    /// why this class of claim does not survive being reasoned about.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn recipient_memory_stays_bounded_across_a_large_reshard() {
        let _ = env_logger::try_init();

        // The tier limit is set far BELOW the data being moved on purpose. If the
        // recipient's hot tier tracked the transfer rather than its own bound,
        // that shows up as peak hot bytes climbing past the limit.
        // Sized so the payload is several times the tier limit. If it were below
        // the limit the tier would never need to shed and this would pass without
        // testing anything -- the trap the whole measurement exists to avoid.
        //
        // Parameterised by environment, because the answer depends on the size of
        // the limit and not only on the code: a 256 MB limit is 32 segments, which
        // is few enough that eviction's give-up rule dominates. Re-run with a
        // realistic limit before treating any of this as a sizing fact.
        //
        //   NEB_MEASURE_TIER_MB, NEB_MEASURE_SLOTS, NEB_MEASURE_CELLS_PER_SLOT,
        //   NEB_MEASURE_PAYLOAD_BYTES, NEB_MEASURE_DB_GB
        fn env_usize(name: &str, default: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        }
        let tier_limit = env_usize("NEB_MEASURE_TIER_MB", 256) * 1024 * 1024;
        let slots = env_usize("NEB_MEASURE_SLOTS", 512).min(crate::slots::SLOT_COUNT) as u16;
        let cells_per_slot = env_usize("NEB_MEASURE_CELLS_PER_SLOT", 512) as u64;
        let payload_bytes = env_usize("NEB_MEASURE_PAYLOAD_BYTES", 4096);
        let db_size = env_usize("NEB_MEASURE_DB_GB", 8) * 1024 * 1024 * 1024;
        const CHUNK_SIZE: usize = 64 * 1024 * 1024;
        let (tier_limit, db_size) = (tier_limit, db_size);
        let (slots, cells_per_slot, payload_bytes) = (slots, cells_per_slot, payload_bytes);

        let group = "migration_memory_measurement";
        let addresses = vec![
            crate::utils::test_port::unique_localhost_addr(),
            crate::utils::test_port::unique_localhost_addr(),
        ];
        let storage_root = std::env::temp_dir().join(format!(
            "neb-migration-memory-{}",
            std::process::id()
        ));

        let mut servers = Vec::new();
        for (index, address) in addresses.iter().enumerate() {
            let member_root = storage_root.join(format!("member-{index}"));
            let opts = ServerOptions {
                chunk_size: CHUNK_SIZE,
                db_size: db_size,
                tiered_config: Some(crate::ram::tiered::TieredConfig {
                    threshold: 0.8,
                    lower_watermark: 0.72,
                    physical_memory_limit: tier_limit,
                    promotion_cooldown_ms: 2000,
                }),
                // Eviction needs somewhere to put a segment it is demoting;
                // without backing storage the tier cannot shed at all and the
                // measurement would be of nothing.
                backup_storage: Some(member_root.join("backup").to_string_lossy().to_string()),
                wal_storage: Some(member_root.join("wal").to_string_lossy().to_string()),
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            };
            servers.push(
                NebServer::new_cluster_from_opts(&opts, address, &addresses, group, async |_| {})
                    .await
                    .unwrap(),
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = Arc::new(
            client::AsyncClient::new(&servers[0].rpc, &servers[0].membership, &addresses, group)
                .await
                .unwrap(),
        );
        client.reload_slot_owners().await;
        client
            .new_schema_with_id(Schema::new_with_id(
                1500,
                &String::from("migration_memory_schema"),
                None,
                default_fields(),
                false,
                false,
            ))
            .await
            .unwrap()
            .unwrap();

        let payload = "x".repeat(payload_bytes);
        let mut written: Vec<Id> = Vec::new();
        let write_started = std::time::Instant::now();
        for slot in 0..slots {
            for seq in 0..cells_per_slot {
                let id = Id::from_parts(slot as u64, 1_000_000 + seq);
                let mut value = OwnedMap::new();
                value.insert(&String::from("id"), OwnedValue::I64(seq as i64));
                value.insert(&String::from("score"), OwnedValue::U64(seq));
                value.insert(&String::from("name"), OwnedValue::String(payload.clone()));
                client
                    .write_cell(crate::ram::cell::OwnedCell::new_with_id(
                        1500,
                        &id,
                        OwnedValue::Map(value),
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                written.push(id);
            }
        }
        let write_elapsed = write_started.elapsed();
        let moved_bytes = written.len() * payload_bytes;
        println!(
            "MEASUREMENT: write phase {:.1}s ({:.0} cells/s)",
            write_elapsed.as_secs_f64(),
            written.len() as f64 / write_elapsed.as_secs_f64()
        );
        println!(
            "MEASUREMENT: wrote {} cells (~{} MB of payload) across {} slots; tier limit {} MB",
            written.len(),
            moved_bytes / (1024 * 1024),
            slots,
            tier_limit / (1024 * 1024)
        );

        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        // The BASELINE, and the reason this test can answer anything at all. The
        // donor has just taken the same volume through the ordinary write path
        // with the same tier configuration, so whatever the tier does under that
        // load is not migration's doing. The question is therefore not "is the
        // recipient near its limit" but "is receiving a migration worse than
        // being written to".
        let baseline_hot = client
            .client_by_server_id(donor_id)
            .await
            .unwrap()
            .settle_bulk_receive()
            .await
            .unwrap();
        // Scanned when available, counter otherwise -- the same preference the
        // driver applies, so baseline and peak are always measured the same way.
        // Which one was used is printed, because the counter can overstate right
        // after an eviction pass.
        let baseline_hot = if baseline_hot.hot_bytes_scanned > 0 {
            baseline_hot.hot_bytes_scanned
        } else {
            baseline_hot.hot_bytes
        };
        println!(
            "MEASUREMENT: hot figures from {} (set NEB_MEASURE_SCAN_HOT to force the scan)",
            if std::env::var("NEB_MEASURE_SCAN_HOT").is_ok() {
                "a full segment scan"
            } else {
                "the shared counter"
            }
        );
        println!(
            "MEASUREMENT: baseline -- donor hot tier {} MB after taking {} MB through the ordinary write path",
            baseline_hot / (1024 * 1024),
            moved_bytes / (1024 * 1024)
        );

        let slots: Vec<u32> = (0..slots).map(|slot| slot as u32).collect();
        // Overridable so the effect of concurrency and of the per-batch settle can
        // be measured rather than assumed: NEB_MEASURE_CONCURRENCY=1 gives the
        // sequential baseline, NEB_MEASURE_SETTLE=0 removes the settle.
        let concurrent_slots = env_usize("NEB_MEASURE_CONCURRENCY", default_concurrent_slots());
        let plan_batch_cells = MigrationPlan::default().batch_cells;
        println!("MEASUREMENT: resharding with concurrent_slots={concurrent_slots}");
        let reshard_started = std::time::Instant::now();
        let reshard = reshard_slots(
            &client,
            &slots,
            donor_id,
            recipient_id,
            &MigrationPlan {
                concurrent_slots,
                settle_recipient_per_batch: env_usize("NEB_MEASURE_SETTLE", 1) != 0,
                ..Default::default()
            },
        )
        .await;
        let reshard_elapsed = reshard_started.elapsed();
        println!(
            "MEASUREMENT: reshard phase {:.1}s ({:.1} MB/s, {:.0} cells/s)",
            reshard_elapsed.as_secs_f64(),
            moved_bytes as f64 / (1024.0 * 1024.0) / reshard_elapsed.as_secs_f64(),
            written.len() as f64 / reshard_elapsed.as_secs_f64()
        );

        let peak_hot = reshard
            .handovers
            .iter()
            .map(|handover| handover.recipient_peak_hot_bytes)
            .max()
            .unwrap_or(0);
        let evicted: u64 = reshard
            .handovers
            .iter()
            .map(|handover| handover.recipient_evicted_segments)
            .sum();
        let transferred: usize = reshard
            .handovers
            .iter()
            .map(|handover| handover.cells_transferred)
            .sum();
        let vanished: usize = reshard
            .handovers
            .iter()
            .map(|handover| handover.vanished_before_transfer)
            .sum();
        println!(
            "MEASUREMENT: {} slots handed over, {} cells transferred, {} vanished before transfer, {} failures",
            reshard.handovers.len(),
            transferred,
            vanished,
            reshard.failed.len()
        );
        println!(
            "MEASUREMENT: recipient peak hot tier {} MB against a {} MB limit while receiving {} MB",
            peak_hot / (1024 * 1024),
            tier_limit / (1024 * 1024),
            moved_bytes / (1024 * 1024)
        );
        println!(
            "MEASUREMENT: recipient evicted {} segments during receive ({} MB); \
             donor hot tier now {} MB",
            evicted,
            evicted as usize * crate::ram::segs::SEGMENT_SIZE / (1024 * 1024),
            servers[0]
                .chunks()
                .tiered_manager
                .as_ref()
                .map(|m| m.shared_hot_segments() * crate::ram::segs::SEGMENT_SIZE / (1024 * 1024))
                .unwrap_or(0)
        );


        // The property under test, stated against the baseline rather than
        // against the tier limit.
        //
        // Measured 2026-08-17, 1 GB payload against a 256 MB limit: the tier
        // overshoots its limit substantially under *either* load -- ~1256 MB from
        // ordinary writes, ~1032 MB while receiving a migration. That overshoot
        // is real and deserves its own investigation, but it is a property of the
        // tier under sustained write pressure rather than something migration
        // introduces. Receiving was no worse than being written to, and slightly
        // better, because batches bound how much is in flight where an
        // unthrottled writer does not.
        //
        // So the claim defended here is the one that matters for migration: a
        // transfer must not cost the recipient MORE than the same volume of
        // ordinary writes costs. If that ever breaks, migration needs its own
        // cold-append path after all.
        assert!(
            baseline_hot > 0,
            "no tier reported anything; this configuration measures nothing"
        );

        // The bound is derived, not a magic multiplier, because two of its terms
        // are consequences of the configuration rather than of the code:
        //
        //  * `baseline` -- what the same volume of ordinary writes leaves resident.
        //    The tier overshoots its own limit under either load; that is a tier
        //    property, not migration's doing, so it belongs in the baseline.
        //  * in-flight data -- `concurrent_slots x batch_cells` cells are being
        //    received at once by construction, so raising concurrency raises the
        //    peak. This is the cost side of the throughput win and is worth
        //    stating in the assertion rather than discovering later.
        //  * slack -- eviction is threshold-driven and lags a burst; run-to-run
        //    peaks vary by a couple of hundred MB at this size.
        let in_flight = (concurrent_slots * plan_batch_cells * payload_bytes) as u64;
        let allowance = baseline_hot + in_flight + baseline_hot / 2;
        println!(
            "MEASUREMENT: peak {} MB against an allowance of {} MB \
             (baseline {} + in-flight {} + slack)",
            peak_hot / (1024 * 1024),
            allowance / (1024 * 1024),
            baseline_hot / (1024 * 1024),
            in_flight / (1024 * 1024)
        );
        assert!(
            peak_hot <= allowance,
            "receiving a migration cost the recipient {} MB of hot tier where the same volume of \
             ordinary writes cost {} MB and {} MB was in flight by design: bulk receive is \
             materially worse than writing, so it needs an explicit cold-append path",
            peak_hot / (1024 * 1024),
            baseline_hot / (1024 * 1024),
            in_flight / (1024 * 1024)
        );

        // The survey runs BEFORE the count assertions on purpose. "The transfer
        // moved 129 fewer cells than were written" is a fact about a counter;
        // "those cells are on neither member" is a fact about the data, and only
        // the second one says whether anything was actually lost.
        //
        // And nothing was lost moving it -- as a SURVEY, not a first-failure
        // panic. At this scale "one cell is missing" and "an entire slot is
        // missing" are completely different bugs with completely different
        // causes, and stopping at the first missing id cannot tell them apart.
        // Measured 2026-08-18 on .239 at a 4 GB tier limit with 16 GB of payload:
        // the reshard reported 4194304 of 4194304 cells transferred and 0
        // failures, and a read still came back CellDoesNotExisted.
        let mut lost: Vec<Id> = Vec::new();
        for id in &written {
            match client.read_cell(*id).await {
                Ok(Ok(cell)) => assert_eq!(cell.header.id, *id),
                Ok(Err(_)) | Err(_) => lost.push(*id),
            }
        }
        if !lost.is_empty() {
            let mut by_slot: std::collections::BTreeMap<u32, usize> = Default::default();
            for id in &lost {
                *by_slot.entry(crate::slots::slot_of(id)).or_default() += 1;
            }
            println!(
                "MEASUREMENT: {} of {} cells UNREADABLE after the reshard, across {} slots",
                lost.len(),
                written.len(),
                by_slot.len()
            );
            // Whole-slot losses mean routing or a dropped handover; scattered
            // ones mean the transfer itself skipped cells. Print enough of both
            // to tell which, and say who actually holds each sample -- "neither"
            // is data loss, "the donor" is a reclaim that ran too early, and
            // "the recipient" is a read that went to the wrong member.
            for (slot, count) in by_slot.iter().take(8) {
                let donor_holds = held_in_slot(&servers[0], *slot as u16).len();
                let recipient_holds = held_in_slot(&servers[1], *slot as u16).len();
                println!(
                    "MEASUREMENT:   slot {slot}: {count} unreadable, donor holds {donor_holds}, recipient holds {recipient_holds}"
                );
            }
            for id in lost.iter().take(4) {
                let slot = crate::slots::slot_of(id);
                println!(
                    "MEASUREMENT:   sample {id:?} (slot {slot}) donor_has={} recipient_has={}",
                    held_in_slot(&servers[0], slot as u16).contains(id),
                    held_in_slot(&servers[1], slot as u16).contains(id)
                );
            }
            // Does a FULL promotion recover them?
            //
            // This is the question the warning text cannot answer. A cold read
            // faults in one block from the backup; a promotion restores the whole
            // segment image. If the cell comes back after promotion, the block
            // path was putting it in the wrong place or missing it. If it stays
            // zeroed, the backup genuinely never contained it and the archiver is
            // where to look. Everything else about this failure has been
            // inference from a fingerprint; this is a measurement.
            for id in lost.iter().take(3) {
                let chunks = servers[0].chunks();
                let chunk = chunks.locate_chunk_by_partition(id.locality() as u64);
                let before_promotion = chunks.read_cell(id).is_ok();
                // The address the index holds for this cell, which is what the
                // read path is dereferencing when it finds zeros.
                let addr = chunks.location_for_read(id).ok();
                let promoted = match addr.and_then(|guard| chunk.locate_segment(*guard)) {
                    Some(segment) => {
                        crate::ram::tiered::promotion::promote_segment(&segment);
                        true
                    }
                    None => false,
                };
                println!(
                    "MEASUREMENT:   PROMOTION PROBE {id:?}: readable_before={before_promotion}, \
                     promotion_attempted={promoted}, readable_after={}",
                    chunks.read_cell(id).is_ok()
                );
            }
        }
        assert!(
            lost.is_empty(),
            "the reshard lost {} of {} cells ({} were never transferred, {} vanished before \
             transfer, {} slots reported failures)",
            lost.len(),
            written.len(),
            written.len().saturating_sub(transferred),
            vanished,
            reshard.failed.len()
        );
        println!(
            "MEASUREMENT: all {} cells readable after the reshard",
            written.len()
        );
        assert!(
            reshard.failed.is_empty(),
            "reshard reported failures: {:?}",
            reshard.failed
        );
        assert_eq!(
            transferred,
            written.len(),
            "the reshard moved {} cells where {} were written, and every cell is still \
             readable -- so the shortfall is an accounting bug rather than data loss",
            transferred,
            written.len()
        );

        let _ = std::fs::remove_dir_all(&storage_root);
    }

    /// Moving several slots at once: each commits on its own, and every donor
    /// copy survives until the last slot has been handed over.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reshard_hands_over_every_slot_before_dropping_any() {
        let (servers, client) = start_pair("migration_reshard_test").await;
        let donor_id = servers[0].server_id;
        let recipient_id = servers[1].server_id;

        const SLOTS: [u16; 3] = [101, 102, 103];
        let mut expected: HashSet<Id> = HashSet::new();
        for slot in SLOTS {
            for seq in 0..3 {
                let (id, cell) = cell_in_slot(slot, seq, "reshard");
                client.write_cell(cell).await.unwrap().unwrap();
                expected.insert(id);
            }
        }

        let slots: Vec<u32> = SLOTS.iter().map(|slot| *slot as u32).collect();
        let reshard = reshard_slots(
            &client,
            &slots,
            donor_id,
            recipient_id,
            &MigrationPlan::default(),
        )
        .await;
        assert!(
            reshard.failed.is_empty(),
            "reshard reported failures: {:?}",
            reshard.failed
        );
        assert_eq!(reshard.handovers.len(), SLOTS.len());
        assert_eq!(reshard.reclaims.len(), SLOTS.len());

        for slot in SLOTS {
            assert!(held_in_slot(&servers[0], slot).is_empty());
        }
        let received: HashSet<Id> = SLOTS
            .iter()
            .flat_map(|slot| held_in_slot(&servers[1], *slot))
            .collect();
        assert_eq!(received, expected);
        for id in &expected {
            assert_eq!(client.locate_server_id(id).unwrap(), recipient_id);
            client.read_cell(*id).await.unwrap().unwrap();
        }
    }
}


/// Draining a member: moving everything it owns elsewhere, so it can leave
/// without taking data with it.
///
/// This is the phase that turns a planned departure into a safe one, and it is
/// deliberately built before any automatic rebalancing, because its policy is
/// trivial and unambiguous — *all* of this member's slots, spread over whoever
/// is left — while auto-balance is an optimisation with a scheduling policy that
/// can be got wrong.
///
/// It is genuinely symmetric with growing the cluster, which is the second
/// payoff of storing placement: nothing is recomputed, so the only slots that
/// move are the ones being drained.
///
/// **Ungraceful loss is still loss.** There is no replication anywhere in Neb, so
/// a member that vanishes takes its slots' data with it and no amount of
/// draining afterwards can help. What this guarantees is narrower and worth
/// stating exactly: a member that is drained *first* loses nothing.
pub mod drain {
    use super::*;

    /// What draining one member did.
    #[derive(Debug, Default)]
    pub struct Drain {
        pub departing: u64,
        /// Slots successfully handed over, with their new owner.
        pub moved: Vec<(u32, u64)>,
        /// Slots still owned by the departing member, with why. **Non-empty means
        /// it is not safe to remove the member.**
        pub stranded: Vec<(u32, String)>,
        pub cells_transferred: usize,
    }

    impl Drain {
        /// Whether the departing member can now be removed without losing data.
        ///
        /// The only question that matters, and the reason it is a method rather
        /// than left to the caller to work out from two vectors.
        pub fn is_complete(&self) -> bool {
            self.stranded.is_empty()
        }
    }

    /// Spread a departing member's slots over the remaining ones.
    ///
    /// Round-robin over the destinations in a stable order, so the assignment is
    /// reproducible and an interrupted drain resumed later makes the same choices
    /// for the slots it has left.
    fn assign(slots: &[u32], destinations: &[u64]) -> Vec<(u32, u64)> {
        slots
            .iter()
            .enumerate()
            .map(|(index, slot)| (*slot, destinations[index % destinations.len()]))
            .collect()
    }

    /// How many empty slots to reassign per raft command.
    ///
    /// The whole table is 32768 entries; proposing it as one command makes a
    /// ~400 KB raft entry. Chunking keeps entries small, and because each chunk
    /// only moves slots still stable on the departing member, a partly applied
    /// drain is not a broken one -- the next pass picks up whatever is left.
    const REASSIGN_CHUNK: usize = 4096;

    /// How many times to sweep before giving up.
    ///
    /// A drain is written as a convergent loop rather than a single pass, and
    /// that shape earns its keep three separate ways: a placement *query* can be
    /// served by a member that has not applied the command we just committed
    /// (see `AsyncClient::note_slot_owner`), a cell can be written to a slot
    /// after we enumerated it, and an individual transfer can fail for its own
    /// reasons. All three look identical from here -- the member still owns
    /// something -- and all three are answered by sweeping again.
    const DRAIN_PASSES: usize = 6;

    /// Move every slot a member owns to the other members, then report whether it
    /// is safe to remove.
    ///
    /// Does **not** remove the member from the group. That is the caller's step
    /// and it must be gated on [`Drain::is_complete`], which is the entire safety
    /// property this exists to establish. Splitting them is deliberate: a drain
    /// that half-succeeded should leave a cluster that is correct and still has
    /// all its data, not one that has already said goodbye.
    ///
    /// ## Two paths, because a drain has two quite different jobs
    ///
    /// A departing member owns every slot it was ever given, which in a small
    /// cluster is the whole 32768-slot space -- and the great majority hold
    /// nothing. The first version walked all of them through the full migration
    /// sequence: ~200 000 round trips, almost all to move nothing. Correct, and
    /// unusable. So each pass enumerates the member's cells once and splits:
    ///
    /// - **Slots holding data** go through `migrate_slot` + `reclaim_donor_copy`,
    ///   unchanged. The careful path is what makes an interrupted drain safe, and
    ///   it is used wherever there is anything to be careful about.
    /// - **Empty slots** move in bulk via `reassign_slots`. No data to strand, so
    ///   no commit point to protect -- and that command only moves slots still
    ///   stable on the departing member, so it cannot steal one from a third
    ///   member or disturb a transfer in flight.
    pub async fn drain_member(
        client: &Arc<AsyncClient>,
        departing: u64,
        destinations: &[u64],
        plan: &MigrationPlan,
    ) -> Result<Drain, MigrationError> {
        if destinations.is_empty() {
            return Err(MigrationError::Invalid(format!(
                "cannot drain member {departing}: there is nowhere to put its slots"
            )));
        }
        if destinations.contains(&departing) {
            return Err(MigrationError::Invalid(format!(
                "member {departing} cannot be a destination for its own drain"
            )));
        }

        let placement = placement_client(client);
        let group = slot_group_id(client.group_name());
        let departing_client = client.client_by_server_id(departing).await?;

        // Sorted and deduped so the assignment does not depend on the order a
        // caller happened to enumerate members in.
        let mut sorted = destinations.to_vec();
        sorted.sort_unstable();
        sorted.dedup();

        let mut drain = Drain {
            departing,
            ..Default::default()
        };
        let mut failed: Vec<(u32, String)> = Vec::new();
        let mut remaining = usize::MAX;

        for pass in 0..DRAIN_PASSES {
            let owned = placement
                .slots_owned_by(&group, &departing)
                .await
                .map_err(|error| MigrationError::Placement(format!("{error:?}")))?;
            if owned.is_empty() {
                break;
            }
            // No progress means sweeping again will not help: either every
            // remaining slot has a real reason to be stuck, or something is
            // writing to this member faster than we can drain it. Either way the
            // caller needs to see it rather than wait.
            if owned.len() >= remaining {
                debug!(
                    "drain of {} made no progress on pass {} ({} slots remain)",
                    departing,
                    pass,
                    owned.len()
                );
                break;
            }
            remaining = owned.len();

            // One pass, bucketed by slot, and its results are handed down to
            // each per-slot migration. The drain already enumerated once per
            // sweep; feeding those ids through means the migrations do not
            // enumerate again, which is what keeps a drain linear in store size
            // rather than O(slots x cells).
            let held = enumerate_slots(&departing_client, &owned).await?;
            let cell_count: usize = held.values().map(|ids| ids.len()).sum();
            info!(
                "drain pass {} for member {}: {} slots owned, {} hold data ({} cells)",
                pass,
                departing,
                owned.len(),
                held.len(),
                cell_count
            );

            // The careful path, for slots with something to lose.
            let with_data: Vec<u32> = owned
                .iter()
                .copied()
                .filter(|slot| held.contains_key(slot))
                .collect();
            // Concurrent across slots, same as a reshard: independent commit
            // points, disjoint cells, and nearly all of the time spent waiting.
            //
            // `buffer_unordered`, NOT spawned, and that is a measured decision.
            // Spawning is 8x faster here (8.9 s -> 1.1 s at concurrency 32 on
            // .239, `drain_throughput_measurement`) and it was reverted anyway,
            // because it made the drain STRAND SLOTS. A/B of
            // `draining_a_member_moves_everything_it_owns_and_loses_nothing`, 10
            // runs each on a 32-core machine: **10/10 interleaved, 8/10 spawned**,
            // the failures leaving ~4000 slots "still owned after the drain".
            // Invisible on .239's 192 cores, which is 10/10 both ways -- so the
            // reshard's fan-out numbers do not transfer to this path, in either
            // direction.
            //
            // Whoever wants the 8x back: the drain is a convergent loop that
            // stops on no progress, and the spawned version reaches that bail-out
            // instead of finishing. Find out why the bulk `reassign_slots` pass
            // stops making progress under it before changing this again, and A/B
            // on a machine with few enough cores to see it.
            let concurrency = plan.concurrent_slots.max(1);
            type SlotOutcome = (u32, u64, Result<(SlotHandover, Result<Reclaim, MigrationError>), MigrationError>);
            let outcomes: Vec<SlotOutcome> = stream::iter(
                assign(&with_data, &sorted).into_iter().map(|(slot, destination)| {
                    let ids = held.get(&slot).cloned().unwrap_or_default();
                    async move {
                        match migrate_slot_prepared(client, slot, departing, destination, plan, ids)
                            .await
                        {
                            Ok(handover) => {
                                // Reclaimed per slot, not deferred to the end: a
                                // drain exists so a member can leave, and it
                                // cannot leave while it still holds the data.
                                // `reshard_slots` defers every drop because there
                                // the donor is staying; here that would defeat
                                // the purpose.
                                let reclaim = reclaim_donor_copy(
                                    client, slot, departing, destination, plan,
                                )
                                .await;
                                (slot, destination, Ok((handover, reclaim)))
                            }
                            Err(error) => (slot, destination, Err(error)),
                        }
                    }
                }),
            )
            .buffer_unordered(concurrency)
            .collect()
            .await;

            for (slot, destination, outcome) in outcomes {
                match outcome {
                    Ok((handover, reclaim)) => {
                        drain.cells_transferred += handover.cells_transferred;
                        match reclaim {
                            Ok(reclaim) if reclaim.retained == 0 => {
                                drain.moved.push((slot, destination))
                            }
                            Ok(reclaim) => failed.push((
                                slot,
                                format!(
                                    "handed over to {destination} but {} cells could not be dropped",
                                    reclaim.retained
                                ),
                            )),
                            Err(error) => failed.push((
                                slot,
                                format!("migrated to {destination}, reclaim failed: {error}"),
                            )),
                        }
                    }
                    Err(error) => failed.push((slot, error.to_string())),
                }
            }

            // The bulk path, for slots with nothing in them.
            let empty: Vec<u32> = owned
                .iter()
                .copied()
                .filter(|slot| !held.contains_key(slot))
                .collect();
            for chunk in assign(&empty, &sorted).chunks(REASSIGN_CHUNK) {
                let batch = chunk.to_vec();
                let moved = placement
                    .reassign_slots(&group, &batch, &departing)
                    .await
                    .map_err(|error| MigrationError::Placement(format!("{error:?}")))?;
                for (slot, destination) in batch.into_iter().take(moved) {
                    drain.moved.push((slot, destination));
                }
            }
        }

        // Whatever is still owned when the sweeping stops is the honest answer to
        // "can this member leave", read from the state machine rather than
        // inferred from what we believe we did.
        let still_owned = placement
            .slots_owned_by(&group, &departing)
            .await
            .map_err(|error| MigrationError::Placement(format!("{error:?}")))?;
        drain.moved.sort_unstable();
        drain.moved.dedup();
        drain.moved.retain(|(slot, _)| !still_owned.contains(slot));
        for slot in still_owned {
            let reason = failed
                .iter()
                .find(|(stranded, _)| *stranded == slot)
                .map(|(_, reason)| reason.clone())
                .unwrap_or_else(|| "still owned after the drain".to_string());
            drain.stranded.push((slot, reason));
        }

        if drain.is_complete() {
            info!(
                "member {} drained: {} slots moved, {} cells transferred; safe to remove",
                departing,
                drain.moved.len(),
                drain.cells_transferred
            );
        } else {
            warn!(
                "member {} is NOT drained: {} slots moved but {} remain. \
                 Do not remove it -- retry the drain instead.",
                departing,
                drain.moved.len(),
                drain.stranded.len()
            );
        }
        Ok(drain)
    }

    /// Confirm from the state machine that a member owns nothing.
    ///
    /// The gate to check before removing a member, and it deliberately re-reads
    /// placement rather than trusting a [`Drain`] the caller is holding: that
    /// report describes what one drain did, while this answers the question that
    /// actually matters, which is whether anything is still there *now*.
    pub async fn owns_nothing(
        client: &Arc<AsyncClient>,
        member: u64,
    ) -> Result<bool, MigrationError> {
        let placement = placement_client(client);
        let group = slot_group_id(client.group_name());
        Ok(placement
            .slots_owned_by(&group, &member)
            .await
            .map_err(|error| MigrationError::Placement(format!("{error:?}")))?
            .is_empty())
    }
}
