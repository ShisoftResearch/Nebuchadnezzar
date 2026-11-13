//! A per-database seal over slots that are in the last step of a handover.
//!
//! A slot migration copies the cells of a slot while the donor still takes
//! writes. Before the placement table flips to the recipient, the donor must
//! stop: a write that the donor accepts after the last copy read its cell is
//! on no member that the table names after the flip. The seal is how the donor
//! stops, and how it proves that no accepted write is still in flight.
//!
//! # Why it cannot be raced
//!
//! The seal has no writer count of its own. Each write is already counted by
//! the write fence of the database ([`super::write_fence`]), in one of 2
//! counter banks. The seal uses that count:
//!
//! * a writer adds itself to the current bank of the fence, **then** reads
//!   the sealed set;
//! * a sealer publishes the new sealed set, **then** rotates the banks, and
//!   waits for the old bank to become empty.
//!
//! A writer that stays in the old bank is one that the sealer waits for. A
//! writer in the new bank read the rotation after the sealer moved it, and
//! the sealer published the set before that, so the writer sees the seal and
//! is refused. No write is in neither group.
//!
//! Sealers take turns through a claim flag, because the bank that a sealer
//! makes current must be empty first. The claim is not on the write path.
//!
//! With no seal, the cost for a write above the cost of the fence is 2 loads
//! of the rotation and 1 load of the sealed set.

use super::write_fence::{WriteFence, WriteGuard};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Why a seal could not be engaged. In each case no slot of the request stays
/// sealed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealError {
    /// Another seal did not finish its drain in time, so this one could not
    /// start.
    Busy { waited: Duration },
    /// Writes that were already in flight did not finish in time.
    DrainTimeout {
        in_flight_writes: i64,
        waited: Duration,
    },
}

impl std::fmt::Display for SealError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SealError::Busy { waited } => {
                write!(f, "another slot seal was still draining after {waited:?}")
            }
            SealError::DrainTimeout {
                in_flight_writes,
                waited,
            } => write!(
                f,
                "{in_flight_writes} write(s) still in flight after {waited:?}; slots unsealed"
            ),
        }
    }
}

impl std::error::Error for SealError {}

pub struct SlotSeal {
    /// The write fence of the same database. It counts the writes.
    fence: Arc<WriteFence>,
    /// The sealed slots, each with the token of the handover that sealed it.
    /// Published before the banks rotate and read only after a writer is
    /// counted, so the rotation orders it.
    sealed: arc_swap::ArcSwap<HashMap<u32, u64>>,
    /// Held by the one sealer that moves the generation and drains.
    sealing: AtomicBool,
    refused: AtomicU64,
    /// Wakes the writers that wait for a seal to end.
    unsealed: tokio::sync::Notify,
}

struct SealingClaim<'a>(&'a SlotSeal);

impl Drop for SealingClaim<'_> {
    fn drop(&mut self) {
        self.0.sealing.store(false, Ordering::SeqCst);
    }
}

impl SlotSeal {
    pub fn new(fence: Arc<WriteFence>) -> Self {
        Self {
            fence,
            sealed: arc_swap::ArcSwap::from_pointee(HashMap::new()),
            sealing: AtomicBool::new(false),
            refused: AtomicU64::new(0),
            unsealed: tokio::sync::Notify::new(),
        }
    }

    /// Admit one write to `slot`, or refuse it because the slot is sealed.
    ///
    /// For a caller that holds no guard of the write fence. The caller keeps
    /// the guard until the write is complete.
    pub fn enter(&self, slot: u32) -> Option<WriteGuard<'_>> {
        let guard = self
            .fence
            .count_if_open()
            .unwrap_or_else(|| self.fence.count_passed());
        self.admits(slot).then_some(guard)
    }

    /// Tells if a write to `slot` can proceed. The caller must hold a counted
    /// guard of the write fence from before this call until the write is
    /// complete. Without the guard the answer protects nothing.
    pub fn admits(&self, slot: u32) -> bool {
        let sealed = self.sealed.load();
        if !sealed.is_empty() && sealed.contains_key(&slot) {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// Tells if `slot` is sealed now. For a caller that needs the answer
    /// without a guard.
    pub fn is_sealed(&self, slot: u32) -> bool {
        let sealed = self.sealed.load();
        !sealed.is_empty() && sealed.contains_key(&slot)
    }

    /// Tells if any slot is sealed now.
    pub fn any_sealed(&self) -> bool {
        !self.sealed.load().is_empty()
    }

    /// The sealed slots, in ascending order.
    pub fn sealed_slots(&self) -> Vec<u32> {
        let mut slots: Vec<u32> = self.sealed.load().keys().copied().collect();
        slots.sort_unstable();
        slots
    }

    /// The slots of `slots` that the handover `token` still seals.
    pub fn sealed_by(&self, slots: &[u32], token: u64) -> Vec<u32> {
        let sealed = self.sealed.load();
        slots
            .iter()
            .copied()
            .filter(|slot| sealed.get(slot) == Some(&token))
            .collect()
    }

    /// Writes refused by a seal since this value was created.
    pub fn refused_writes(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// Seal `slots` and wait until each write that was admitted before the
    /// seal is complete.
    ///
    /// When this returns `Ok`, no write to these slots is in flight and none
    /// starts until [`Self::unseal`]. On an error the slots of this call are
    /// not sealed. A slot that an earlier call sealed stays sealed, and keeps
    /// the token of that call.
    ///
    /// `token` names the handover. The donor uses it to end only its own seal
    /// when a lease runs out, and not a later seal of the same slot.
    pub async fn seal(
        &self,
        slots: &[u32],
        token: u64,
        drain_timeout: Duration,
    ) -> Result<(), SealError> {
        let started = Instant::now();
        let newly_sealed: Vec<u32> = {
            let current = self.sealed.load();
            slots
                .iter()
                .copied()
                .filter(|slot| !current.contains_key(slot))
                .collect()
        };

        while self
            .sealing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            if started.elapsed() >= drain_timeout {
                return Err(SealError::Busy {
                    waited: started.elapsed(),
                });
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let _claim = SealingClaim(self);

        // The bank that becomes current must be empty. It holds writers only
        // if an earlier seal gave up its drain.
        let old_bank = self.fence.current_bank();
        let new_bank = old_bank ^ 1;
        loop {
            let in_flight = self.fence.bank_in_flight(new_bank);
            if in_flight <= 0 {
                break;
            }
            if started.elapsed() >= drain_timeout {
                return Err(SealError::DrainTimeout {
                    in_flight_writes: in_flight,
                    waited: started.elapsed(),
                });
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        self.sealed.rcu(|current| {
            let mut next = HashMap::clone(current);
            for slot in slots {
                next.entry(*slot).or_insert(token);
            }
            next
        });
        self.fence.rotate();

        loop {
            let in_flight = self.fence.bank_in_flight(old_bank);
            if in_flight <= 0 {
                return Ok(());
            }
            if started.elapsed() >= drain_timeout {
                self.unseal_token(&newly_sealed, token);
                return Err(SealError::DrainTimeout {
                    in_flight_writes: in_flight,
                    waited: started.elapsed(),
                });
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Let writes to `slots` through again. A slot that is not sealed is
    /// ignored.
    pub fn unseal(&self, slots: &[u32]) {
        if slots.is_empty() {
            return;
        }
        self.sealed.rcu(|current| {
            let mut next = HashMap::clone(current);
            for slot in slots {
                next.remove(slot);
            }
            next
        });
        self.unsealed.notify_waiters();
    }

    /// Let writes through again for the slots of `slots` that the handover
    /// `token` sealed. A slot that another handover sealed stays sealed.
    pub fn unseal_token(&self, slots: &[u32], token: u64) {
        if slots.is_empty() {
            return;
        }
        self.sealed.rcu(|current| {
            let mut next = HashMap::clone(current);
            for slot in slots {
                if next.get(slot) == Some(&token) {
                    next.remove(slot);
                }
            }
            next
        });
        self.unsealed.notify_waiters();
    }

    /// Wait until `slot` is not sealed or `deadline` passes.
    ///
    /// A wake-up can be missed between the check and the wait, so the wait
    /// also ends after `UNSEAL_POLL`. The caller checks the slot again.
    pub async fn unsealed(&self, slot: u32, deadline: tokio::time::Instant) {
        const UNSEAL_POLL: Duration = Duration::from_millis(5);
        if !self.is_sealed(slot) {
            return;
        }
        let poll = tokio::time::Instant::now() + UNSEAL_POLL;
        tokio::select! {
            _ = self.unsealed.notified() => {}
            _ = tokio::time::sleep_until(poll.min(deadline)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    const DRAIN: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn a_sealed_slot_refuses_writes_and_other_slots_do_not() {
        let seal = SlotSeal::new(Arc::new(WriteFence::new()));
        assert!(seal.enter(7).is_some());
        seal.seal(&[7], 1, DRAIN).await.expect("seal");

        assert!(seal.enter(7).is_none());
        assert!(seal.enter(8).is_some());
        assert!(seal.is_sealed(7));
        assert_eq!(seal.sealed_slots(), vec![7]);
        assert_eq!(seal.refused_writes(), 1);

        seal.unseal(&[7]);
        assert!(seal.enter(7).is_some());
        assert!(seal.sealed_slots().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_seal_waits_for_a_write_that_was_admitted_before_it() {
        let seal = Arc::new(SlotSeal::new(Arc::new(WriteFence::new())));
        let guard = seal.enter(7).expect("admitted before the seal");

        let sealed = Arc::new(AtomicBool::new(false));
        let task = {
            let seal = seal.clone();
            let sealed = sealed.clone();
            tokio::spawn(async move {
                seal.seal(&[7], 1, DRAIN).await.expect("seal");
                sealed.store(true, Ordering::SeqCst);
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !sealed.load(Ordering::SeqCst),
            "the seal returned while an admitted write was in flight"
        );
        assert!(
            seal.enter(7).is_none(),
            "a write that starts during the drain must be refused"
        );
        drop(guard);
        task.await.expect("seal task");
        assert!(sealed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_drain_timeout_leaves_the_slot_open_and_the_next_seal_correct() {
        let seal = SlotSeal::new(Arc::new(WriteFence::new()));
        let straggler = seal.enter(7).expect("admitted");

        let error = seal
            .seal(&[7], 1, Duration::from_millis(20))
            .await
            .expect_err("the straggler keeps the drain from ending");
        assert!(matches!(
            error,
            SealError::DrainTimeout {
                in_flight_writes: 1,
                ..
            }
        ));
        assert!(!seal.is_sealed(7), "a failed seal must not stay engaged");
        assert!(seal.enter(7).is_some());

        // The straggler is still in flight and saw no seal. The next seal of
        // the same slot must wait for it too.
        let error = seal
            .seal(&[7], 1, Duration::from_millis(20))
            .await
            .expect_err("the straggler is still in flight");
        assert!(matches!(error, SealError::DrainTimeout { .. }));
        drop(straggler);
        seal.seal(&[7], 1, DRAIN)
            .await
            .expect("seal after the straggler left");
        assert!(seal.enter(7).is_none());
    }

    #[tokio::test]
    async fn seals_overlap_and_end_independently() {
        let seal = SlotSeal::new(Arc::new(WriteFence::new()));
        seal.seal(&[1, 2], 1, DRAIN).await.expect("first seal");
        seal.seal(&[3], 1, DRAIN).await.expect("second seal");
        assert_eq!(seal.sealed_slots(), vec![1, 2, 3]);

        // A lease of another handover must not end this seal.
        seal.unseal_token(&[1, 2, 3], 99);
        assert_eq!(seal.sealed_slots(), vec![1, 2, 3]);
        assert_eq!(seal.sealed_by(&[1, 2, 3, 4], 1), vec![1, 2, 3]);
        seal.unseal_token(&[2], 1);
        assert!(seal.enter(1).is_none());
        assert!(seal.enter(2).is_some());
        assert!(seal.enter(3).is_none());

        // A repeated seal of a sealed slot keeps it sealed when a new slot of
        // the same call cannot drain.
        let straggler = seal.enter(9).expect("admitted");
        let error = seal
            .seal(&[1, 9], 1, Duration::from_millis(20))
            .await
            .expect_err("slot 9 cannot drain");
        assert!(matches!(error, SealError::DrainTimeout { .. }));
        assert!(seal.is_sealed(1));
        assert!(!seal.is_sealed(9));
        drop(straggler);
    }

    /// Writers increment a counter for each admitted write to the slot. After
    /// the seal returns, the counter must not change: a change is a write
    /// that the seal neither waited for nor refused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn no_write_lands_after_the_seal_returns() {
        for _ in 0..200 {
            let seal = Arc::new(SlotSeal::new(Arc::new(WriteFence::new())));
            let stop = Arc::new(AtomicBool::new(false));
            let landed = Arc::new(AtomicUsize::new(0));
            let mut writers = Vec::new();
            for _ in 0..6 {
                let (seal, stop, landed) = (seal.clone(), stop.clone(), landed.clone());
                writers.push(std::thread::spawn(move || {
                    while !stop.load(Ordering::SeqCst) {
                        if let Some(_guard) = seal.enter(7) {
                            landed.fetch_add(1, Ordering::SeqCst);
                        }
                        let _other = seal.enter(8);
                    }
                }));
            }
            tokio::time::sleep(Duration::from_micros(200)).await;
            seal.seal(&[7], 1, DRAIN).await.expect("seal");
            let at_seal = landed.load(Ordering::SeqCst);
            tokio::time::sleep(Duration::from_micros(200)).await;
            let later = landed.load(Ordering::SeqCst);
            stop.store(true, Ordering::SeqCst);
            for writer in writers {
                writer.join().expect("writer");
            }
            assert_eq!(
                at_seal, later,
                "a write to the sealed slot landed after the seal returned"
            );
        }
    }
}
