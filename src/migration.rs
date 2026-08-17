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
}

impl Default for MigrationPlan {
    fn default() -> Self {
        Self {
            batch_cells: DEFAULT_BATCH_CELLS,
            delta_rounds: DEFAULT_DELTA_ROUNDS,
            settle_recipient_per_batch: true,
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

fn placement_client(client: &AsyncClient) -> SlotsSMClient {
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

struct BatchOutcome {
    /// Exactly the ids the recipient now holds a copy of. Returned as ids
    /// rather than a count because the reclaim decides what it may destroy from
    /// this, and a count cannot say *which* cells arrived.
    transferred: Vec<Id>,
    vanished: usize,
}

/// Move one batch.
async fn transfer_batch(
    donor: &Arc<CellServiceClient>,
    recipient: &Arc<CellServiceClient>,
    batch: &Vec<Id>,
) -> Result<BatchOutcome, MigrationError> {
    let read = donor.read_all_cells(batch).await?;
    let mut cells: Vec<OwnedCell> = Vec::with_capacity(read.len());
    let mut vanished = 0usize;
    for (id, result) in batch.iter().zip(read.into_iter()) {
        match result {
            Ok(cell) => cells.push(cell),
            // Enumerated a moment ago and gone now: something removed it
            // between the two steps. There is nothing to move and nothing
            // wrong; moving on is the only correct response.
            Err(ReadError::CellDoesNotExisted) => vanished += 1,
            Err(error) => {
                return Err(MigrationError::Transfer(format!(
                    "donor could not read {id:?}: {error:?}"
                )))
            }
        }
    }
    if cells.is_empty() {
        return Ok(BatchOutcome {
            transferred: Vec::new(),
            vanished,
        });
    }

    let transferred: Vec<Id> = cells.iter().map(|cell| cell.header.id).collect();
    let written = recipient.upsert_all_cells(cells).await?;
    // `upsert_all_cells` stops at the first failure and reports BatchAborted
    // for the rest, so the first error that is not BatchAborted is the real
    // cause and the only one worth reporting.
    if let Some(error) = written
        .iter()
        .filter_map(|result| result.as_ref().err())
        .find(|error| !matches!(error, WriteError::BatchAborted))
    {
        return Err(MigrationError::Transfer(format!(
            "recipient rejected a batch of {} cells: {error:?}",
            transferred.len()
        )));
    }
    Ok(BatchOutcome {
        transferred,
        vanished,
    })
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
    };
    let mut moved: std::collections::HashSet<Id> = Default::default();

    let outcome = async {
        for round in 0..plan.delta_rounds.max(1) {
            let pending: Vec<Id> = enumerate_slot(&donor, slot)
                .await?
                .into_iter()
                .filter(|id| !moved.contains(id))
                .collect();
            if pending.is_empty() {
                // The pass that finds nothing is the one that proves the slot
                // is caught up, so it is not counted as a round of work.
                break;
            }
            handover.delta_rounds_used = round + 1;
            for batch in range_batches(pending, plan.batch_cells) {
                let outcome = transfer_batch(&donor, &recipient, &batch).await?;
                handover.cells_transferred += outcome.transferred.len();
                handover.vanished_before_transfer += outcome.vanished;
                handover.batches += 1;
                // The whole batch, not just what transferred: a vanished cell
                // does not need looking for again either.
                moved.extend(batch.iter().copied());

                if plan.settle_recipient_per_batch {
                    match recipient.settle_bulk_receive().await {
                        Ok(report) => {
                            handover.recipient_peak_hot_bytes =
                                handover.recipient_peak_hot_bytes.max(report.hot_bytes);
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
    let owner = placement
        .complete_slot_migration(&group, &slot)
        .await
        .map_err(|error| MigrationError::Placement(format!("{error:?}")))?
        .map_err(MigrationError::NotCommitted)?;
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
    client.note_slot_owner(slot, owner);

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
    let placement = placement_client(client);
    let group = slot_group_id(client.group_name());
    confirm_handover(&placement, group, slot, from, to).await?;

    let donor = client.client_by_server_id(from).await?;
    let recipient = client.client_by_server_id(to).await?;

    let mut reclaim = Reclaim {
        slot,
        from,
        to,
        dropped: 0,
        carried_over: 0,
        retained: 0,
    };

    for batch in range_batches(enumerate_slot(&donor, slot).await?, plan.batch_cells) {
        // Presence at the new owner, without moving any bodies to find out.
        let heads = recipient.head_all_cells(&batch).await?;
        let mut droppable: Vec<Id> = Vec::with_capacity(batch.len());
        let mut missing: Vec<Id> = Vec::new();
        for (id, head) in batch.iter().zip(heads.into_iter()) {
            match head {
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
            let outcome = transfer_batch(&donor, &recipient, &missing).await?;
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
        let removals = donor.remove_all_cells(&droppable).await?;
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

    info!(
        "slot {} donor copy reclaimed on {}: {} dropped, {} carried over to {}, {} retained",
        slot, from, reclaim.dropped, reclaim.carried_over, to, reclaim.retained
    );
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
    for slot in slots {
        match migrate_slot(client, *slot, from, to, plan).await {
            Ok(handover) => reshard.handovers.push(handover),
            Err(error) => reshard.failed.push((*slot, error.to_string())),
        }
    }
    for handover in &reshard.handovers {
        match reclaim_donor_copy(client, handover.slot, from, to, plan).await {
            Ok(reclaim) => reshard.reclaims.push(reclaim),
            // A slot whose donor copy could not be dropped is still correctly
            // migrated -- it just costs space until the reclaim is retried.
            Err(error) => reshard
                .failed
                .push((handover.slot, format!("reclaim: {error}"))),
        }
    }
    reshard
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let _ = env_logger::try_init();
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
            index_enabled: false,
            services: vec![Service::Cell],
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

        // Stands in for a writer holding a stale placement table: it addresses
        // the former owner directly, after the flip, which is exactly what such
        // a client would do.
        let (late_id, late_cell) = cell_in_slot(SLOT, 2, "late");
        client
            .client_by_server_id(donor_id)
            .await
            .unwrap()
            .upsert_cell(late_cell)
            .await
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
