//! What a slot means for Neb, and how a group adopts a slot table.
//!
//! bifrost's `conshash::slots` is deliberately generic about what a slot *is* —
//! it cannot depend on dovahkiin and must not learn our id layout. This module
//! is the other half: it says that **a Neb slot is an id's locality**, and it
//! seeds the table from the placement the ring already computes.
//!
//! Adoption is the step that makes the table safe to introduce. Every member
//! proposes the placement it already derives from `jump_hash`, and
//! `adopt_slots` takes only the slots that have no owner yet — so whoever
//! commits first defines the table, re-running changes nothing, and a cluster
//! switching over does not appear to move a single cell.

use bifrost::conshash::slots::client::SMClient as SlotsSMClient;
use bifrost::conshash::slots::SlotState;
use bifrost::conshash::ConsistentHashing;
use bifrost::raft::client::RaftClient;
use bifrost_hasher::hash_str;
use dovahkiin::types::custom_types::id::ID_LOCALITY_MASK;
use dovahkiin::types::Id;
use std::sync::Arc;

/// Number of distinct slots, which is the number of distinct localities.
///
/// `Id::locality()` is 15 bits for both id classes (bits 62..48, below the
/// class tag), so this is the entire placement space and it never grows.
pub const SLOT_COUNT: usize = (ID_LOCALITY_MASK as usize) + 1;

/// One raft command per chunk rather than one for the whole table.
///
/// The full table is 32768 entries; proposing it as a single command makes a
/// ~400KB raft entry, and every member proposes one at startup. Chunking keeps
/// individual entries small, and because `adopt_slots` is first-writer-wins per
/// slot, a partially-applied adoption is not a broken one — the next member's
/// proposal, or the next restart, fills whatever is still unowned.
const ADOPT_CHUNK: usize = 4096;

/// The slot an id belongs to. Fixed at id creation and never changes: *"stored
/// cells never change locality after creation"*.
pub fn slot_of(id: &Id) -> u32 {
    id.locality() as u32
}

/// The group key the slot table is partitioned by, matching how `conshash`
/// scopes its own per-group state.
pub fn slot_group_id(group_name: &str) -> u64 {
    hash_str(group_name)
}

/// Seed the slot table from the placement the ring currently computes.
///
/// Returns how many slots this call actually claimed. Zero is the normal and
/// expected answer on every member after the first, and on every restart —
/// it means the table was already populated, not that anything failed.
///
/// Deliberately best-effort: a failure here leaves the table unpopulated,
/// which callers must treat as "fall back to the ring" rather than as "nothing
/// is placed". Refusing to start because the table could not be seeded would
/// trade a placement problem for an availability one.
pub async fn adopt_from_ring(
    group_name: &str,
    conshash: &Arc<ConsistentHashing>,
    raft_client: &Arc<RaftClient>,
    slots_sm_id: u64,
) -> Result<usize, String> {
    let client = SlotsSMClient::new(slots_sm_id, raft_client);
    let group = slot_group_id(group_name);

    // Nothing to seed from: with no members in the ring, `get_server_id`
    // answers None for every slot and we would write an empty table that
    // claims to be authoritative.
    if conshash.server_count() == 0 {
        return Err("cannot adopt slot placement from an empty ring".to_string());
    }

    let mut adopted = 0usize;
    let mut chunk: Vec<(u32, u64)> = Vec::with_capacity(ADOPT_CHUNK);
    for slot in 0..SLOT_COUNT {
        // The same call `locate_server_id` makes today, for every slot rather
        // than for one id -- which is what makes the table reproduce current
        // placement exactly instead of merely plausibly.
        if let Some(owner) = conshash.get_server_id(slot as u64) {
            chunk.push((slot as u32, owner));
        }
        if chunk.len() >= ADOPT_CHUNK || slot + 1 == SLOT_COUNT {
            if chunk.is_empty() {
                continue;
            }
            let proposed = std::mem::take(&mut chunk);
            adopted += client
                .adopt_slots(&group, &proposed)
                .await
                .map_err(|error| format!("slot adoption command failed: {error:?}"))?;
            chunk = Vec::with_capacity(ADOPT_CHUNK);
        }
    }
    Ok(adopted)
}

/// The table as a slot-indexed vector plus the command's applied Raft log index,
/// or `None` when the group has no table.
///
/// Flattened to a vector rather than kept as a map because this is read once per
/// cell lookup: the representation should be an index, not a hash. A slot with
/// no owner stays 0 and the caller falls back to the ring.
pub async fn load_owner_vec(
    group_name: &str,
    raft_client: &Arc<RaftClient>,
    slots_sm_id: u64,
) -> Result<(Option<Vec<u64>>, u64), String> {
    let (table, applied_index) = load_table(group_name, raft_client, slots_sm_id).await?;
    Ok((
        table.map(|table| {
            let mut owners = vec![0u64; SLOT_COUNT];
            for (slot, state) in table {
                if let Some(entry) = owners.get_mut(slot as usize) {
                    // The *serving* owner: during a migration that is still the
                    // donor, which is what keeps an interrupted transfer
                    // unambiguous.
                    *entry = state.serving_owner();
                }
            }
            owners
        }),
        applied_index,
    ))
}

/// Read the whole table through an ordered Raft command, returning that command's
/// applied log index, or `None` when the group has never been seeded.
///
/// `None` and an empty table are different answers and callers must not
/// conflate them: the first means "fall back to the ring", the second would
/// mean "nothing is placed anywhere", which is never true of a running group.
pub async fn load_table(
    group_name: &str,
    raft_client: &Arc<RaftClient>,
    slots_sm_id: u64,
) -> Result<
    (
        Option<std::collections::HashMap<u32, SlotState>>,
        u64,
    ),
    String,
> {
    let client = SlotsSMClient::new(slots_sm_id, raft_client);
    client
        .all_slots_consistent_with_index(&slot_group_id(group_name))
        .await
        .map_err(|error| format!("consistent slot table command failed: {error:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_slot_space_is_the_locality_space() {
        // Slots ARE localities. If these ever diverge some ids become
        // unplaceable, so it is pinned rather than assumed.
        assert_eq!(SLOT_COUNT, 32_768);
        assert_eq!(SLOT_COUNT - 1, ID_LOCALITY_MASK as usize);
    }

    #[test]
    fn every_id_maps_into_the_slot_space() {
        // Both id classes carry locality in the same bits, so a hashed id must
        // land in range just as an allocated one does -- including the extremes,
        // where an off-by-one would put a slot outside the table.
        let cases = [
            Id::from_parts(0, 0),
            Id::from_parts(1, 1),
            Id::from_parts(ID_LOCALITY_MASK, u64::MAX),
            Id::hashed(u64::MAX),
            Id::hashed(0),
            Id::max_id(),
            Id::unit_id(),
        ];
        for id in cases {
            let slot = slot_of(&id);
            assert!(
                (slot as usize) < SLOT_COUNT,
                "id {id:?} mapped to slot {slot}, outside the {SLOT_COUNT}-slot space"
            );
        }
    }

    #[test]
    fn a_slot_is_stable_for_an_id() {
        // Placement is only stable if the key -> slot half never moves. This is
        // the half the table relies on and does not control.
        let id = Id::from_parts(1234, 5678);
        assert_eq!(slot_of(&id), slot_of(&id));
        assert_eq!(slot_of(&id), id.locality() as u32);
    }

    #[test]
    fn group_ids_separate_groups() {
        assert_ne!(slot_group_id("alpha"), slot_group_id("beta"));
        assert_eq!(slot_group_id("alpha"), slot_group_id("alpha"));
    }
}

/// The slot of an id given only its bits, for call sites that carry the
/// index key (`id.bits()`) rather than the `Id`.
#[inline]
pub fn slot_of_bits(bits: u64) -> u32 {
    Id::from_bits(bits).locality() as u32
}

/// Live bytes per slot, maintained on the write path and read by whoever
/// needs a placement decision.
///
/// This exists because slots-per-member is nearly meaningless as a balance
/// metric: a slot is a locality, and localities are not uniformly full — a hub
/// vertex's container and its adjacency live in one slot on purpose. Moving
/// data sensibly needs bytes, and bytes must be a *counter*, never a scan:
/// an inline O(cells) refresh on the write path froze shard workers for 25+
/// minutes (the statistics-refresh wedge). Every update here is one atomic
/// add.
///
/// What is counted: the **content length of the live entry** each cell
/// currently resolves to — the same measure the dead-space accounting uses,
/// so insert/update/remove arithmetic can be checked against it. Dead space,
/// tombstones and abandoned race-loser entries are deliberately excluded:
/// a balancer moves live cells, and compaction debt is the cleaner's concern,
/// not placement's.
///
/// Counts are signed so a transient add/sub race cannot wrap; reads clamp at
/// zero. All mutations of one cell serialise on its cell lock, so every add
/// is paired with exactly one sub when the entry is superseded or removed —
/// the counter drifts only if a hook is missed, not by racing.
pub struct SlotLiveBytes {
    counts: Box<[std::sync::atomic::AtomicI64]>,
}

impl SlotLiveBytes {
    pub fn new() -> Self {
        let counts = (0..SLOT_COUNT)
            .map(|_| std::sync::atomic::AtomicI64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { counts }
    }

    #[inline]
    pub fn add(&self, id_bits: u64, bytes: u32) {
        self.counts[slot_of_bits(id_bits) as usize]
            .fetch_add(bytes as i64, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn sub(&self, id_bits: u64, bytes: u32) {
        self.counts[slot_of_bits(id_bits) as usize]
            .fetch_sub(bytes as i64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Live bytes in one slot. Clamped at zero: a reader racing an
    /// update/remove can observe the sub before the add.
    #[inline]
    pub fn get(&self, slot: u32) -> u64 {
        self.counts
            .get(slot as usize)
            .map_or(0, |c| c.load(std::sync::atomic::Ordering::Relaxed).max(0) as u64)
    }

    /// Every slot's live bytes, indexed by slot.
    pub fn snapshot(&self) -> Vec<u64> {
        self.counts
            .iter()
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed).max(0) as u64)
            .collect()
    }

    /// Total live bytes across all slots.
    pub fn total(&self) -> u64 {
        self.counts
            .iter()
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed).max(0) as u64)
            .sum()
    }
}

#[cfg(test)]
mod slot_bytes_tests {
    use super::*;

    #[test]
    fn slot_arithmetic_follows_the_id_locality() {
        let counter = SlotLiveBytes::new();
        let a = Id::allocated(7, 0, 1);
        let b = Id::allocated(7, 0, 2);
        let c = Id::allocated(9, 0, 1);
        assert_eq!(slot_of(&a), 7);
        assert_eq!(slot_of_bits(a.bits()), 7);

        counter.add(a.bits(), 100);
        counter.add(b.bits(), 50);
        counter.add(c.bits(), 30);
        assert_eq!(counter.get(7), 150);
        assert_eq!(counter.get(9), 30);
        assert_eq!(counter.total(), 180);

        counter.sub(a.bits(), 100);
        assert_eq!(counter.get(7), 50);

        // Transient underflow clamps on read instead of wrapping.
        counter.sub(b.bits(), 80);
        assert_eq!(counter.get(7), 0);
        counter.add(b.bits(), 30);
        assert_eq!(counter.get(7), 0, "the deficit must not eat later adds");
        let snap = counter.snapshot();
        assert_eq!(snap.len(), SLOT_COUNT);
        assert_eq!(snap[9], 30);
    }

    #[test]
    fn hashed_ids_land_in_slots_too() {
        let id = Id::hashed(0xDEAD_BEEF_CAFE_F00D);
        let counter = SlotLiveBytes::new();
        counter.add(id.bits(), 64);
        assert_eq!(counter.get(slot_of(&id)), 64);
        assert!((slot_of(&id) as usize) < SLOT_COUNT);
    }
}
