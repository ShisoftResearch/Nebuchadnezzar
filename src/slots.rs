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

/// Read the whole table for a group, or `None` when it has never been seeded.
///
/// `None` and an empty table are different answers and callers must not
/// conflate them: the first means "fall back to the ring", the second would
/// mean "nothing is placed anywhere", which is never true of a running group.
pub async fn load_table(
    group_name: &str,
    raft_client: &Arc<RaftClient>,
    slots_sm_id: u64,
) -> Result<Option<std::collections::HashMap<u32, SlotState>>, String> {
    let client = SlotsSMClient::new(slots_sm_id, raft_client);
    client
        .all_slots(&slot_group_id(group_name))
        .await
        .map_err(|error| format!("slot table query failed: {error:?}"))
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
