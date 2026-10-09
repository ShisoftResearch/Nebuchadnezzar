//! Consensus-backed origin-slot and block-lease authority for the
//! compact-id allocator (design:
//! `docs/superpowers/specs/2026-08-02-compact-cell-id-design.md`).
//!
//! One state machine per database, on the shared meta plane. It owns
//! the two distributed invariants:
//!
//! - *Single ownership of each origin slot*: `claim_origin` assigns a
//!   slot to a server and bumps the slot epoch; re-claiming (after a
//!   presumed death) bumps the epoch again, fencing the previous
//!   holder. Exhausted slots retire permanently and are never
//!   re-assigned.
//! - *Durable-lease-before-issue*: `lease_block` is a raft command, so
//!   a granted lease is replicated before the caller sees it — at
//!   least as durable as any data that can reference the ids.

use crate::utils::persisted::decode_persisted_or_halt;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::utils;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use dovahkiin::types::custom_types::id::{ID_ORIGIN_MASK, ID_SEQUENCE_MASK};

pub const DEFAULT_ID_ALLOC_SM_ID: u64 = hash_ident!("COMPACT_ID_ALLOC_SM_ID") as u64;

pub fn generate_scoped_sm_id(group_name: &str, database_name: &str) -> u64 {
    if group_name == database_name {
        DEFAULT_ID_ALLOC_SM_ID
    } else {
        hash_str(&format!(
            "COMPACT_ID_ALLOC_SM_ID-{}-{}",
            group_name, database_name
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantedLease {
    pub origin: u16,
    pub epoch: u64,
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct OriginState {
    epoch: u64,
    lease_end: u64,
    holder: u64,
    retired: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AllocState {
    origins: HashMap<u16, OriginState>,
    next_slot: u16,
}

pub struct IdAllocSM {
    state: AllocState,
    sm_id: u64,
}

raft_state_machine! {
    def cmd claim_origin(holder: u64) -> Result<(u16, u64), String>;
    def cmd reclaim_origin(origin: u16, holder: u64) -> Result<u64, String>;
    def cmd lease_block(origin: u16, epoch: u64, floor: u64, span: u64) -> Result<GrantedLease, String>;
    def cmd reserve_floor(origin: u16, floor: u64) -> Result<u64, String>;
    def qry last_lease_end(origin: u16) -> Option<u64>;
}

impl StateMachineCmds for IdAllocSM {
    fn claim_origin(&mut self, holder: u64) -> BoxFuture<'_, Result<(u16, u64), String>> {
        // Idempotent per holder: a restarting server re-claims the slot it
        // already owns (epoch bump fences its previous incarnation) instead
        // of burning a fresh slot every boot — 4096 restarts must not
        // exhaust the origin space.
        if let Some((&slot, _)) = self
            .state
            .origins
            .iter()
            .find(|(_, entry)| entry.holder == holder && !entry.retired && entry.epoch > 0)
        {
            let entry = self.state.origins.get_mut(&slot).expect("slot just found");
            entry.epoch += 1;
            return future::ready(Ok((slot, entry.epoch))).boxed();
        }
        let result = loop {
            let slot = self.state.next_slot;
            if slot as u64 > ID_ORIGIN_MASK {
                break Err("origin slot space exhausted".to_string());
            }
            self.state.next_slot += 1;
            // Origin 0 is reserved: legacy `Id::from_parts` shim ids carry
            // zeros in the origin bits, so an origin-0 allocation could
            // collide with a shim-composed id (locality, seq) pair.
            if slot == 0 {
                continue;
            }
            let entry = self.state.origins.entry(slot).or_default();
            if entry.retired {
                continue;
            }
            entry.epoch += 1;
            entry.holder = holder;
            break Ok((slot, entry.epoch));
        };
        future::ready(result).boxed()
    }

    fn reclaim_origin(&mut self, origin: u16, holder: u64) -> BoxFuture<'_, Result<u64, String>> {
        let result = match self.state.origins.get_mut(&origin) {
            Some(entry) if !entry.retired => {
                // Re-claiming bumps the epoch, fencing any previous
                // holder that may still be alive with a warm block.
                entry.epoch += 1;
                entry.holder = holder;
                Ok(entry.epoch)
            }
            Some(_) => Err(format!("origin {} is retired", origin)),
            None => Err(format!("origin {} was never claimed", origin)),
        };
        future::ready(result).boxed()
    }

    fn lease_block(
        &mut self,
        origin: u16,
        epoch: u64,
        floor: u64,
        span: u64,
    ) -> BoxFuture<'_, Result<GrantedLease, String>> {
        let result = (|| {
            let entry = self
                .state
                .origins
                .get_mut(&origin)
                .ok_or_else(|| format!("origin {} was never claimed", origin))?;
            if entry.retired {
                return Err(format!("origin {} is retired", origin));
            }
            if epoch != entry.epoch {
                return Err(format!(
                    "fenced: lease at epoch {} but origin {} is at epoch {}",
                    epoch, origin, entry.epoch
                ));
            }
            let start = entry.lease_end.max(floor);
            let end = start
                .checked_add(span)
                .filter(|end| *end <= ID_SEQUENCE_MASK)
                .ok_or_else(|| {
                    entry.retired = true;
                    format!("origin {} sequence space exhausted; slot retired", origin)
                })?;
            entry.lease_end = end;
            Ok(GrantedLease {
                origin,
                epoch,
                start,
                end,
            })
        })();
        future::ready(result).boxed()
    }

    /// Raise an origin's durable lease end to at least `floor`.
    ///
    /// This is the restore-side half of the allocator: ids imported from
    /// another database name `(origin, sequence)` pairs this authority never
    /// issued, and any later lease for that origin must start above them or it
    /// would reissue a restored id. Unlike `lease_block` it needs no holder or
    /// epoch -- the origin may belong to a server that has not joined yet -- so
    /// it creates the slot's record if there is none. Monotone and idempotent:
    /// a floor below the current lease end changes nothing.
    ///
    /// Origin 0 is refused (reserved for the shim plane, see `claim_origin`)
    /// and so is a floor past the sequence space.
    fn reserve_floor(&mut self, origin: u16, floor: u64) -> BoxFuture<'_, Result<u64, String>> {
        let result = (|| {
            if origin == 0 {
                return Err("origin 0 is reserved and has no lease to raise".to_string());
            }
            if origin as u64 > ID_ORIGIN_MASK {
                return Err(format!("origin {} exceeds the origin field", origin));
            }
            if floor > ID_SEQUENCE_MASK {
                return Err(format!(
                    "floor {} exceeds the sequence space of origin {}",
                    floor, origin
                ));
            }
            let entry = self.state.origins.entry(origin).or_default();
            if entry.retired {
                return Err(format!("origin {} is retired", origin));
            }
            entry.lease_end = entry.lease_end.max(floor);
            Ok(entry.lease_end)
        })();
        future::ready(result).boxed()
    }

    fn last_lease_end(&self, origin: u16) -> BoxFuture<'_, Option<u64>> {
        future::ready(
            self.state
                .origins
                .get(&origin)
                .map(|entry| entry.lease_end),
        )
        .boxed()
    }
}

impl StateMachineCtl for IdAllocSM {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        self.sm_id
    }
    fn snapshot(&self) -> Vec<u8> {
        utils::serde::serialize(&self.state)
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<'_, ()> {
        self.state = decode_persisted_or_halt("id allocator snapshot", &data);
        future::ready(()).boxed()
    }
    fn recoverable(&self) -> bool {
        true
    }
}

impl IdAllocSM {
    pub fn new(sm_id: u64) -> Self {
        Self {
            state: AllocState::default(),
            sm_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn recover_round_trips_a_snapshot() {
        let mut sm = IdAllocSM::new(1);
        let (slot, epoch) = block_on(sm.claim_origin(5)).unwrap();
        let snapshot = sm.snapshot();
        let mut restored = IdAllocSM::new(1);
        block_on(restored.recover(snapshot));
        assert_eq!(restored.state.origins.len(), 1);
        assert!(restored.state.origins.contains_key(&slot));
        assert_eq!(restored.state.origins[&slot].epoch, epoch);
    }

    #[test]
    fn recover_halts_on_an_undecodable_snapshot() {
        let mut sm = IdAllocSM::new(1);
        let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            block_on(sm.recover(b"{\"origins\":{}}".to_vec()));
        }))
        .unwrap_err();
        let error = error
            .downcast::<crate::utils::persisted::PersistedDecodeError>()
            .unwrap();
        assert_eq!(error.what, "id allocator snapshot");
    }

    #[test]
    fn origin_zero_is_reserved_for_the_shim_plane() {
        // `Id::from_parts` composes ids with zero origin bits; an origin-0
        // allocation of (locality, seq) would collide with the shim id of
        // the same pair. The allocator must never issue slot 0.
        let mut sm = IdAllocSM::new(1);
        for holder in 0..8u64 {
            let (slot, _) = block_on(sm.claim_origin(holder + 100)).unwrap();
            assert_ne!(slot, 0, "origin 0 must stay reserved");
        }
    }

    #[test]
    fn claim_lease_fence_lifecycle() {
        let mut sm = IdAllocSM::new(1);
        let (slot, epoch) = block_on(sm.claim_origin(11)).unwrap();
        assert_eq!(epoch, 1);

        let lease = block_on(sm.lease_block(slot, epoch, 0, 1 << 20)).unwrap();
        assert_eq!(lease.start, 0);
        assert_eq!(lease.end, 1 << 20);

        // A second holder reclaims the slot; the old epoch is fenced.
        let new_epoch = block_on(sm.reclaim_origin(slot, 22)).unwrap();
        assert!(new_epoch > epoch);
        assert!(block_on(sm.lease_block(slot, epoch, 0, 1 << 20)).is_err());
        let lease2 = block_on(sm.lease_block(slot, new_epoch, 0, 1 << 20)).unwrap();
        assert_eq!(lease2.start, 1 << 20, "resumes above the prior lease");

        assert_eq!(block_on(sm.last_lease_end(slot)), Some(2 << 20));
    }

    #[test]
    fn distinct_claims_get_distinct_slots() {
        let mut sm = IdAllocSM::new(2);
        let (a, _) = block_on(sm.claim_origin(1)).unwrap();
        let (b, _) = block_on(sm.claim_origin(2)).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn reclaim_by_same_holder_reuses_slot_with_epoch_bump() {
        let mut sm = IdAllocSM::new(5);
        let (slot, epoch) = block_on(sm.claim_origin(77)).unwrap();
        // A restart claims again with the same holder id: same slot, higher
        // epoch (fencing the previous incarnation), no slot burn.
        let (slot2, epoch2) = block_on(sm.claim_origin(77)).unwrap();
        assert_eq!(slot, slot2);
        assert!(epoch2 > epoch);
        // Leases continue above the prior lease end.
        let lease = block_on(sm.lease_block(slot, epoch2, 0, 1 << 20)).unwrap();
        assert_eq!(lease.start, 0);
        let (slot3, epoch3) = block_on(sm.claim_origin(77)).unwrap();
        assert_eq!(slot, slot3);
        let lease2 = block_on(sm.lease_block(slot, epoch3, 0, 1 << 20)).unwrap();
        assert_eq!(lease2.start, 1 << 20);
    }

    #[test]
    fn exhausted_slot_retires_permanently() {
        let mut sm = IdAllocSM::new(3);
        let (slot, epoch) = block_on(sm.claim_origin(1)).unwrap();
        let near_end = ID_SEQUENCE_MASK - 10;
        block_on(sm.lease_block(slot, epoch, near_end, 10)).unwrap();
        assert!(block_on(sm.lease_block(slot, epoch, 0, 1 << 20)).is_err());
        assert!(block_on(sm.reclaim_origin(slot, 9)).is_err(), "retired slots never re-lease");
    }

    #[test]
    fn snapshot_round_trip() {
        let mut sm = IdAllocSM::new(4);
        let (slot, epoch) = block_on(sm.claim_origin(1)).unwrap();
        block_on(sm.lease_block(slot, epoch, 0, 1 << 20)).unwrap();
        let snap = StateMachineCtl::snapshot(&sm);
        let mut restored = IdAllocSM::new(4);
        block_on(StateMachineCtl::recover(&mut restored, snap));
        assert_eq!(block_on(restored.last_lease_end(slot)), Some(1 << 20));
        // The restored authority still fences and still resumes above.
        let lease = block_on(restored.lease_block(slot, epoch, 0, 1 << 20)).unwrap();
        assert_eq!(lease.start, 1 << 20);
    }

    #[test]
    fn reserve_floor_raises_lease_end_for_unclaimed_and_claimed_origins() {
        let mut sm = IdAllocSM::new(6);
        // An origin nobody has claimed: the record is created.
        assert_eq!(block_on(sm.reserve_floor(9, 5_000)).unwrap(), 5_000);
        assert_eq!(block_on(sm.last_lease_end(9)), Some(5_000));
        // Monotone and idempotent.
        assert_eq!(block_on(sm.reserve_floor(9, 100)).unwrap(), 5_000);
        assert_eq!(block_on(sm.reserve_floor(9, 7_000)).unwrap(), 7_000);

        // A server that later claims slots up to 9 gets leases above the floor.
        let mut holder_slot = None;
        for holder in 0..32u64 {
            let (slot, epoch) = block_on(sm.claim_origin(holder + 1)).unwrap();
            if slot == 9 {
                holder_slot = Some((slot, epoch));
                break;
            }
        }
        let (slot, epoch) = holder_slot.expect("slot 9 is reached by sequential claims");
        let lease = block_on(sm.lease_block(slot, epoch, 0, 1 << 20)).unwrap();
        assert!(lease.start >= 7_000, "a lease may not start below a reserved floor");
    }

    #[test]
    fn reserve_floor_refuses_reserved_and_out_of_range_input() {
        let mut sm = IdAllocSM::new(7);
        assert!(block_on(sm.reserve_floor(0, 1)).is_err());
        assert!(block_on(sm.reserve_floor(1 << 12, 1)).is_err());
        assert!(block_on(sm.reserve_floor(3, ID_SEQUENCE_MASK + 1)).is_err());
    }

    #[test]
    fn reserve_floor_survives_a_snapshot() {
        let mut sm = IdAllocSM::new(8);
        block_on(sm.reserve_floor(4, 123_456)).unwrap();
        let snap = StateMachineCtl::snapshot(&sm);
        let mut restored = IdAllocSM::new(8);
        block_on(StateMachineCtl::recover(&mut restored, snap));
        assert_eq!(block_on(restored.last_lease_end(4)), Some(123_456));
    }
}
