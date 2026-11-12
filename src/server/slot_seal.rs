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
//! This is the store-then-load handshake of [`super::write_fence`], with one
//! change. A write fence admits one fencer at a time, so one counter set is
//! enough. Seals overlap: a reshard seals a slot while another slot is still
//! sealed, and writes to all other slots must keep their full protection.
//! Thus the seal keeps 2 counter banks and a generation:
//!
//! * a writer reads the generation, adds itself to the bank of that
//!   generation, and reads the generation again. If it changed, the writer
//!   removes itself and repeats. **Then** it reads the sealed set;
//! * a sealer publishes the new sealed set, **then** moves the generation to
//!   the other bank, and waits for the old bank to become empty.
//!
//! A writer that stays in the old bank is one that the sealer waits for. A
//! writer in the new bank read the generation after the sealer moved it, and
//! the sealer published the set before that, so the writer sees the seal and
//! is refused. No write is in neither group.
//!
//! Sealers take turns through a claim flag, because the bank that a sealer
//! makes current must be empty first. The claim is not on the write path.
//!
//! The banks are split into shards on separate cache lines. With no seal the
//! cost for a write is 2 uncontended atomic read-modify-writes, 2 loads of the
//! generation and 1 load of the sealed set.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SHARDS: usize = 32;

#[repr(align(128))]
struct Shard(AtomicI64);

type Bank = Box<[Shard; SHARDS]>;

fn new_bank() -> Bank {
    Box::new(std::array::from_fn(|_| Shard(AtomicI64::new(0))))
}

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
    /// Its lowest bit names the bank that new writers enter.
    generation: AtomicU64,
    banks: [Bank; 2],
    /// The sealed slots. Published before the generation moves and read only
    /// after a writer is counted, so the generation orders it.
    sealed: arc_swap::ArcSwap<HashSet<u32>>,
    /// Held by the one sealer that moves the generation and drains.
    sealing: AtomicBool,
    refused: AtomicU64,
}

impl Default for SlotSeal {
    fn default() -> Self {
        Self::new()
    }
}

/// Held for the duration of one write. A seal that engages while this guard
/// lives waits for it.
pub struct SlotWriteGuard<'a> {
    counted: Option<(&'a SlotSeal, usize, usize)>,
}

impl SlotWriteGuard<'_> {
    /// A guard for a write that no seal can apply to.
    pub fn uncounted() -> Self {
        Self { counted: None }
    }
}

impl Drop for SlotWriteGuard<'_> {
    fn drop(&mut self) {
        if let Some((seal, bank, shard)) = self.counted.take() {
            seal.banks[bank][shard].0.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

struct SealingClaim<'a>(&'a SlotSeal);

impl Drop for SealingClaim<'_> {
    fn drop(&mut self) {
        self.0.sealing.store(false, Ordering::SeqCst);
    }
}

fn shard_of_current_thread() -> usize {
    thread_local! {
        static SHARD: usize = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            std::thread::current().id().hash(&mut hasher);
            (hasher.finish() as usize) % SHARDS
        };
    }
    SHARD.with(|shard| *shard)
}

impl SlotSeal {
    pub fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            banks: [new_bank(), new_bank()],
            sealed: arc_swap::ArcSwap::from_pointee(HashSet::new()),
            sealing: AtomicBool::new(false),
            refused: AtomicU64::new(0),
        }
    }

    /// Admit one write to `slot`, or refuse it because the slot is sealed.
    ///
    /// The caller keeps the guard until the write is complete.
    pub fn enter(&self, slot: u32) -> Option<SlotWriteGuard<'_>> {
        let shard = shard_of_current_thread();
        let bank = loop {
            let generation = self.generation.load(Ordering::SeqCst);
            let bank = (generation & 1) as usize;
            self.banks[bank][shard].0.fetch_add(1, Ordering::SeqCst);
            if self.generation.load(Ordering::SeqCst) == generation {
                break bank;
            }
            self.banks[bank][shard].0.fetch_sub(1, Ordering::SeqCst);
        };
        let sealed = self.sealed.load();
        if !sealed.is_empty() && sealed.contains(&slot) {
            self.banks[bank][shard].0.fetch_sub(1, Ordering::SeqCst);
            self.refused.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(SlotWriteGuard {
            counted: Some((self, bank, shard)),
        })
    }

    /// Tells if `slot` is sealed now. For a caller that needs the answer
    /// without a guard.
    pub fn is_sealed(&self, slot: u32) -> bool {
        let sealed = self.sealed.load();
        !sealed.is_empty() && sealed.contains(&slot)
    }

    /// The sealed slots, in ascending order.
    pub fn sealed_slots(&self) -> Vec<u32> {
        let mut slots: Vec<u32> = self.sealed.load().iter().copied().collect();
        slots.sort_unstable();
        slots
    }

    /// Writes refused by a seal since this value was created.
    pub fn refused_writes(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    fn bank_in_flight(&self, bank: usize) -> i64 {
        self.banks[bank]
            .iter()
            .map(|shard| shard.0.load(Ordering::SeqCst))
            .sum()
    }

    /// Seal `slots` and wait until each write that was admitted before the
    /// seal is complete.
    ///
    /// When this returns `Ok`, no write to these slots is in flight and none
    /// starts until [`Self::unseal`]. On an error the slots of this call are
    /// not sealed. A slot that an earlier call sealed stays sealed.
    pub async fn seal(&self, slots: &[u32], drain_timeout: Duration) -> Result<(), SealError> {
        let started = Instant::now();
        let newly_sealed: Vec<u32> = {
            let current = self.sealed.load();
            slots
                .iter()
                .copied()
                .filter(|slot| !current.contains(slot))
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
        let old_bank = (self.generation.load(Ordering::SeqCst) & 1) as usize;
        let new_bank = old_bank ^ 1;
        loop {
            let in_flight = self.bank_in_flight(new_bank);
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
            let mut next = HashSet::clone(current);
            next.extend(slots.iter().copied());
            next
        });
        self.generation.fetch_add(1, Ordering::SeqCst);

        loop {
            let in_flight = self.bank_in_flight(old_bank);
            if in_flight <= 0 {
                return Ok(());
            }
            if started.elapsed() >= drain_timeout {
                self.unseal(&newly_sealed);
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
            let mut next = HashSet::clone(current);
            for slot in slots {
                next.remove(slot);
            }
            next
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    const DRAIN: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn a_sealed_slot_refuses_writes_and_other_slots_do_not() {
        let seal = SlotSeal::new();
        assert!(seal.enter(7).is_some());
        seal.seal(&[7], DRAIN).await.expect("seal");

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
        let seal = Arc::new(SlotSeal::new());
        let guard = seal.enter(7).expect("admitted before the seal");

        let sealed = Arc::new(AtomicBool::new(false));
        let task = {
            let seal = seal.clone();
            let sealed = sealed.clone();
            tokio::spawn(async move {
                seal.seal(&[7], DRAIN).await.expect("seal");
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
        let seal = SlotSeal::new();
        let straggler = seal.enter(7).expect("admitted");

        let error = seal
            .seal(&[7], Duration::from_millis(20))
            .await
            .expect_err("the straggler keeps the drain from ending");
        assert!(matches!(error, SealError::DrainTimeout { in_flight_writes: 1, .. }));
        assert!(!seal.is_sealed(7), "a failed seal must not stay engaged");
        assert!(seal.enter(7).is_some());

        // The straggler is still in flight and saw no seal. The next seal of
        // the same slot must wait for it too.
        let error = seal
            .seal(&[7], Duration::from_millis(20))
            .await
            .expect_err("the straggler is still in flight");
        assert!(matches!(error, SealError::DrainTimeout { .. }));
        drop(straggler);
        seal.seal(&[7], DRAIN).await.expect("seal after the straggler left");
        assert!(seal.enter(7).is_none());
    }

    #[tokio::test]
    async fn seals_overlap_and_end_independently() {
        let seal = SlotSeal::new();
        seal.seal(&[1, 2], DRAIN).await.expect("first seal");
        seal.seal(&[3], DRAIN).await.expect("second seal");
        assert_eq!(seal.sealed_slots(), vec![1, 2, 3]);

        seal.unseal(&[2]);
        assert!(seal.enter(1).is_none());
        assert!(seal.enter(2).is_some());
        assert!(seal.enter(3).is_none());

        // A repeated seal of a sealed slot keeps it sealed when a new slot of
        // the same call cannot drain.
        let straggler = seal.enter(9).expect("admitted");
        let error = seal
            .seal(&[1, 9], Duration::from_millis(20))
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
            let seal = Arc::new(SlotSeal::new());
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
            seal.seal(&[7], DRAIN).await.expect("seal");
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
